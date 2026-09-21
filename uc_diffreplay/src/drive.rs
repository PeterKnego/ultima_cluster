// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The in-process replay driver (spec §6, plan A erratum): install the
//! corpus's artifact at P (or start from genesis), walk the journal span,
//! call `apply`/`on_timer` with the RECORDED headers — position, time_ns,
//! term — and capture every surface into a [`Trace`]. Deterministic by
//! construction: the same corpus through the same build yields the same
//! trace, which is what the `determinism` mode checks.
//!
//! Mirrors the dispatch rules of `uc_service/src/replay.rs` (MESSAGE above
//! the SM's frontier → apply; TIMER naming this identity → on_timer; anything
//! else skipped) without the live-rejoin machinery — there is no node here.
//!
//! **The block walk is `replay.rs`'s, not the live ring reader's.** An
//! archived block's frames lie back to back at their full ALIGNED span,
//! padding included — `uc_log::archive`'s own walkers (`observe_terms`,
//! `max_frame_time_ns`, the `ReplayFrame` iterator) and `replay.rs` all
//! advance `off += align_frame_len(length)` for every frame and need no
//! padding special case. `uc_log::reader::FrameIter` differs (a padding
//! frame's buffer footprint there is the 32-byte header while the stream
//! position advances the full span) because that is the LIVE ring, where
//! padding is the wrap filler; applying that rule to a journal block would
//! desync the walk. Likewise the span bound is the frame's END against
//! `end`, and a sub-header / over-running length ends THIS BLOCK (`break`),
//! both verbatim from `replay.rs`.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use anyhow::{Context, bail};
use uc_journal::TailReader;
use uc_protocol::identity::FsmIdentity;
use uc_protocol::v2::frame::{
    self, FLAG_TIMER_TABLE, FRAME_TYPE_MESSAGE, FRAME_TYPE_TIMER, HEADER_LEN, align_frame_len,
};
use uc_protocol::v2::ipc::SchedOp;
use uc_service::snapshots::verify_snapshot_envelope;
// `RawStateMachine`'s items (`apply`, `on_timer`, `last_applied`, `IDENTITY`,
// `VERSION`) reach us through the `SnapshotStateMachine: RawStateMachine`
// supertrait bound, so it needs no import of its own.
use uc_service::{ApplyCtx, SnapshotStateMachine, TimerEvent};

use crate::corpus::Corpus;
use crate::trace::{Entry, EntryKind, Sched, Trace};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// Install the corpus artifact at P, replay `[P, end)`. The correct path.
    Artifact,
    /// Skip the install, replay `[0, end)` — the §2.3 counterfactual.
    Genesis,
}

fn project_string<S: SnapshotStateMachine>(sm: &S) -> anyhow::Result<String> {
    let mut out = Vec::new();
    sm.project(&mut out)
        .map_err(|e| anyhow::anyhow!("project(): {e}"))?;
    String::from_utf8(out).context("projection is not UTF-8")
}

/// Install `artifact` (tagged `position`) into `sm`: strip and check the
/// framework envelope, then hand the payload to the SM. Returns `(got,
/// artifact_version)`: the position the SM reported (must equal `position`)
/// and the version stamped in the artifact's envelope.
fn install<S: SnapshotStateMachine>(
    sm: &mut S,
    artifact: &Path,
    position: u64,
) -> anyhow::Result<(u64, u32)> {
    let mut f =
        BufReader::new(File::open(artifact).with_context(|| artifact.display().to_string())?);
    install_from(sm, &mut f, position)
}

/// [`install`] minus the file: envelope, install, and the two post-install
/// checks. Split out so the refusals are unit-testable over an in-memory
/// artifact — no scratch directory, no I/O. `None` for the envelope's
/// expected version: this driver reports what the artifact was built with
/// rather than asserting it (that cross-check is Tasks 3/4's).
fn install_from<S: SnapshotStateMachine>(
    sm: &mut S,
    src: &mut dyn std::io::Read,
    position: u64,
) -> anyhow::Result<(u64, u32)> {
    let f: &mut dyn std::io::Read = src;
    let env = verify_snapshot_envelope(f, position, None)
        .map_err(|e| anyhow::anyhow!("envelope: {e}"))?;
    let got = sm
        .install_snapshot(position, f)
        .map_err(|e| anyhow::anyhow!("install_snapshot({position}): {e}"))?;
    if got != position {
        bail!("install_snapshot returned {got}, expected {position}");
    }
    // The returned position is the trait's self-report; the frontier it LEFT
    // is what the walk above P actually keys on, and the two can disagree.
    // `scan_from` always yields the covering segment, so every frame of the
    // block holding P is offered to `Some(pos) > last_applied()`:
    //
    // * a cursor at or above P silently DROPS the frame starting at P (the
    //   artifact tag is an EXCLUSIVE frontier — `SnapshotStateMachine::
    //   install_snapshot`'s contract, ruling P6);
    // * `None` silently RE-APPLIES the covering segment from its base.
    //
    // Either way the trace is quietly wrong, which is the one output this
    // harness must never produce. Both are named refusals here. (`None <
    // Some(_)` in `Option`'s ordering, so the first check alone would let the
    // `None` case through — it needs its own.)
    anyhow::ensure!(
        sm.last_applied().is_some(),
        "install_snapshot({position}) left last_applied() = None; the image must restore its cursor"
    );
    anyhow::ensure!(
        sm.last_applied() < Some(position),
        "install_snapshot({position}) left last_applied at {:?}; the image's cursor must be strictly below the artifact tag (P is an exclusive frontier)",
        sm.last_applied()
    );
    Ok((got, env.version))
}

pub fn project_artifact<S: SnapshotStateMachine>(
    mut sm: S,
    artifact: &Path,
    position: u64,
) -> anyhow::Result<String> {
    let (_got, _version) = install(&mut sm, artifact, position)?;
    project_string(&sm)
}

pub fn drive<S: SnapshotStateMachine>(
    mut sm: S,
    corpus: &Corpus,
    origin: Origin,
) -> anyhow::Result<Trace> {
    let m = &corpus.manifest;
    let (start, projection_at_origin, artifact_version) = match origin {
        Origin::Artifact => {
            let (_got, version) = install(&mut sm, &corpus.artifact(), m.origin)?;
            (m.origin, Some(project_string(&sm)?), Some(version))
        }
        Origin::Genesis => (0, None, None),
    };

    let reader = TailReader::open(&corpus.journal_dir())?;
    let identity = S::IDENTITY;
    let mut entries = Vec::new();
    let mut resp = Vec::with_capacity(256);
    let end = m.end;

    reader.scan_from(start, |_seq, base, block| {
        walk_block(&mut sm, identity, base, block, end, &mut resp, &mut entries)
    })?;

    Ok(Trace {
        row: m.row,
        version: S::VERSION,
        artifact_version,
        origin: start,
        end,
        projection_at_origin,
        projection_at_end: Some(project_string(&sm)?),
        entries,
    })
}

/// Walk ONE archived block's frames, dispatching into `sm` and appending to
/// `entries`. `base` is the block's base stream position, `end` the corpus's
/// exclusive frontier. Returns what the `scan_from` visitor should return:
/// `false` to stop the whole scan (the span's end was reached), `true` to
/// continue with the next block.
///
/// Extracted from the closure so it can be unit-tested over a hand-laid
/// block — the TIMER arm and the schedule capture are unreachable from a
/// `RegisterSm` corpus, which never schedules and never receives a timer.
fn walk_block<S: SnapshotStateMachine>(
    sm: &mut S,
    identity: FsmIdentity,
    base: u64,
    block: &[u8],
    end: u64,
    resp: &mut Vec<u8>,
    entries: &mut Vec<Entry>,
) -> bool {
    let mut off = 0usize;
    while off + HEADER_LEN <= block.len() {
        let hdr = frame::read_header(&block[off..]);
        let total = hdr.length as usize;
        let aligned = align_frame_len(total);
        // `replay.rs`'s defensive stop: a sub-header or over-running length
        // would desync the walk (archived blocks are frame-aligned and
        // CRC-validated, so unreachable on real input). End THIS block rather
        // than spin — and, unlike a whole-scan abort, leave the following
        // blocks to be walked.
        if total < HEADER_LEN || off + aligned > block.len() {
            break;
        }
        let pos = base + off as u64;
        // The span bound is the frame's END against `end` (exclusive
        // frontier), exactly as `replay.rs` bounds a replayed span by
        // `min(commit, durable)`. A frame that straddles `end` is not
        // replayed and stops the whole scan.
        if pos.saturating_add(aligned as u64) > end {
            return false;
        }
        // PADDING needs no arm: it matches neither dispatch arm and the walk
        // advances over it like any other frame.
        let payload = &block[off + HEADER_LEN..off + total];
        // The apply loop's idempotency guard, verbatim
        // (`uc_service/src/apply.rs`, `replay.rs`): re-walking an overlap
        // already reflected in the SM applies nothing. After
        // `install_snapshot(P)` the SM's cursor is the one its image recorded
        // — strictly BELOW P, which `install` now checks — so the frame
        // starting exactly at P is applied, which is what makes P an
        // exclusive frontier.
        let above = Some(pos) > sm.last_applied();
        match hdr.frame_type {
            FRAME_TYPE_MESSAGE if above => {
                let mut ctx = ApplyCtx::new(pos, identity)
                    .with_time(hdr.time_ns)
                    .with_term(hdr.leadership_term_id);
                resp.clear();
                sm.apply(&mut ctx, payload, resp);
                entries.push(Entry {
                    pos,
                    kind: EntryKind::Message,
                    // 32 bytes, not 4: a `Sessioned<S>` service's payload
                    // opens with a 16-byte `client_id ‖ seq` envelope
                    // (`uc_service::session::SESSION_HEADER_LEN`), so a
                    // 4-byte tag would capture a client id and never reach
                    // the app's own op byte. The declaration's `tag_offset`
                    // says how much of this prefix to skip.
                    tag: payload[..payload.len().min(32)].to_vec(),
                    response: resp.clone(),
                    sched: sched_of(&mut ctx),
                });
            }
            FRAME_TYPE_TIMER if above => {
                // A TIMER frame naming another row's identity is skipped —
                // but the walk still advances over it below.
                if let Some(body) = frame::read_timer_body(payload)
                    && body.identity_hash == identity.hash()
                {
                    let table = hdr.flags & FLAG_TIMER_TABLE != 0;
                    let mut ctx = ApplyCtx::new(pos, identity)
                        .with_time(hdr.time_ns)
                        .with_term(hdr.leadership_term_id);
                    sm.on_timer(
                        &mut ctx,
                        TimerEvent::new(body.timer_id, body.deadline_ns, table),
                    );
                    entries.push(Entry {
                        pos,
                        kind: EntryKind::Timer {
                            id: body.timer_id,
                            deadline_ns: body.deadline_ns,
                            table,
                        },
                        tag: Vec::new(),
                        response: Vec::new(),
                        sched: sched_of(&mut ctx),
                    });
                }
            }
            _ => {}
        }
        off += aligned;
    }
    true
}

/// The wire name of a schedule op, as a trace records it. Split out of
/// [`sched_of`] because `Consumed` / `TableConsumed` are pushed by
/// `pub(crate)` `ApplyCtx` methods (`consumed`, `consumed_table` — `Timed`'s
/// only), so no test outside `uc_service` can produce them through a real
/// apply; the mapping itself is still testable here.
fn op_name(op: SchedOp) -> &'static str {
    match op {
        SchedOp::Schedule => "schedule",
        SchedOp::Cancel => "cancel",
        SchedOp::Consumed => "consumed",
        SchedOp::TableConsumed => "table_consumed",
    }
}

fn sched_of(ctx: &mut ApplyCtx) -> Vec<Sched> {
    ctx.take_sched_records_for_test()
        .into_iter()
        .map(|r| Sched {
            op: op_name(r.op).into(),
            id: r.timer_id,
            deadline_ns: r.deadline_ns,
        })
        .collect()
}

/// What an app binary's `replay` subcommand calls (spec plan A erratum):
/// open the corpus, drive this SM, write the trace.
pub fn run_replay_cli<S: SnapshotStateMachine>(
    sm: S,
    corpus_dir: &Path,
    out: &Path,
    from_genesis: bool,
) -> anyhow::Result<()> {
    let corpus = Corpus::open(corpus_dir)?;
    let origin = if from_genesis {
        Origin::Genesis
    } else {
        Origin::Artifact
    };
    let trace = drive(sm, &corpus, origin)?;
    let f = File::create(out).with_context(|| out.display().to_string())?;
    trace.write_json(f)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc_protocol::v2::frame::{
        FRAME_TYPE_PADDING, FrameHeader, OFF_LENGTH, TIMER_BODY_LEN, TimerBody,
        write_header_except_length, write_timer_body,
    };
    use uc_service::RawStateMachine;
    use uc_service::snapshots::write_snapshot_envelope;

    /// What `install_snapshot` should leave `last_applied()` at — the lever
    /// the frontier-refusal tests pull.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Frontier {
        /// The contract: the cursor the image recorded, strictly below P.
        BelowTag,
        /// Swallows the frame at P.
        AtTag,
        /// Re-applies the covering segment from its base.
        Nothing,
    }

    /// A minimal SM that records everything the driver hands it. `RegisterSm`
    /// cannot exercise the TIMER arm or the schedule capture (it never
    /// schedules and never receives a timer), so the walk tests drive this.
    struct ProbeSm {
        last: Option<u64>,
        frontier: Frontier,
        applied: Vec<(u64, Vec<u8>, u64, u32)>,
        timers: Vec<(TimerEvent, u64)>,
    }

    impl Default for ProbeSm {
        fn default() -> ProbeSm {
            ProbeSm {
                last: None,
                frontier: Frontier::BelowTag,
                applied: Vec::new(),
                timers: Vec::new(),
            }
        }
    }

    impl uc_service::RawStateMachine for ProbeSm {
        const NAME: &'static str = "probe";

        fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
            self.last = Some(ctx.position);
            self.applied
                .push((ctx.position, cmd.to_vec(), ctx.time_ns, ctx.term));
            // Two requests, in this order, so the trace's `sched` list is
            // order-sensitive and both op names are covered.
            ctx.schedule(7, 100);
            ctx.cancel(3);
            out.extend_from_slice(b"ok");
        }

        fn query(&self, _q: &[u8], _out: &mut Vec<u8>) {}

        fn last_applied(&self) -> Option<u64> {
            self.last
        }

        fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {
            self.last = Some(ctx.position);
            self.timers.push((ev, ctx.position));
        }
    }

    impl SnapshotStateMachine for ProbeSm {
        type SnapshotHandle = ();

        fn freeze(&self) -> Result<((), u64), uc_service::SnapshotError> {
            Ok(((), self.last.unwrap_or(0)))
        }

        fn stream_snapshot(
            _h: (),
            _dst: &mut dyn std::io::Write,
        ) -> Result<(), uc_service::SnapshotError> {
            Ok(())
        }

        fn install_snapshot(
            &mut self,
            position: u64,
            src: &mut dyn std::io::Read,
        ) -> Result<u64, uc_service::SnapshotError> {
            let mut buf = Vec::new();
            std::io::Read::read_to_end(src, &mut buf)?;
            self.last = match self.frontier {
                Frontier::BelowTag => Some(position.saturating_sub(64)),
                Frontier::AtTag => Some(position),
                Frontier::Nothing => None,
            };
            Ok(position)
        }

        fn project(&self, out: &mut dyn std::io::Write) -> Result<(), uc_service::SnapshotError> {
            writeln!(out, "last={:?}", self.last)?;
            Ok(())
        }
    }

    /// An in-memory artifact: the framework envelope at `p`, stamped with
    /// `version`, plus an empty payload (`ProbeSm` reads its image from
    /// nowhere).
    fn artifact_bytes(p: u64, version: u32) -> Vec<u8> {
        let mut v = Vec::new();
        write_snapshot_envelope(&mut v, p, version).unwrap();
        v
    }

    // ---- Finding 1: the post-install frontier checks ----

    #[test]
    fn install_accepts_a_cursor_strictly_below_the_tag() {
        let mut sm = ProbeSm::default();
        let art = artifact_bytes(512, 7);
        let (got, version) = install_from(&mut sm, &mut &art[..], 512).unwrap();
        assert_eq!(got, 512);
        assert_eq!(version, 7, "the artifact's own stamp is reported back");
        assert_eq!(sm.last_applied(), Some(448));
    }

    #[test]
    fn install_refuses_a_cursor_at_the_tag() {
        let mut sm = ProbeSm {
            frontier: Frontier::AtTag,
            ..Default::default()
        };
        let art = artifact_bytes(512, 0);
        let e = install_from(&mut sm, &mut &art[..], 512)
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("left last_applied at Some(512)") && e.contains("exclusive frontier"),
            "{e}"
        );
    }

    #[test]
    fn install_refuses_a_cursor_left_at_none() {
        let mut sm = ProbeSm {
            frontier: Frontier::Nothing,
            ..Default::default()
        };
        let art = artifact_bytes(512, 0);
        let e = install_from(&mut sm, &mut &art[..], 512)
            .unwrap_err()
            .to_string();
        assert!(e.contains("left last_applied() = None"), "{e}");
    }

    // ---- Finding 2: the block walk, over a hand-laid block ----

    /// Lay one frame at `off` in `block`: header (length written last, as the
    /// runtime does) plus `payload`. Returns the frame's ALIGNED span.
    fn lay(block: &mut [u8], off: usize, ftype: u8, flags: u8, payload: &[u8]) -> usize {
        let total = HEADER_LEN + payload.len();
        write_header_except_length(
            &mut block[off..],
            &FrameHeader {
                length: total as u32,
                frame_type: ftype,
                flags,
                leadership_term_id: 4,
                client_id: 0,
                seq: 0,
                time_ns: 9_000,
            },
        );
        block[off + OFF_LENGTH..off + OFF_LENGTH + 4]
            .copy_from_slice(&(total as u32).to_le_bytes());
        block[off + HEADER_LEN..off + total].copy_from_slice(payload);
        align_frame_len(total)
    }

    fn timer_payload(hash: u64, id: u64, deadline_ns: u64) -> [u8; TIMER_BODY_LEN] {
        let mut b = [0u8; TIMER_BODY_LEN];
        write_timer_body(
            &mut b,
            &TimerBody {
                identity_hash: hash,
                timer_id: id,
                deadline_ns,
            },
        );
        b
    }

    const BASE: u64 = 1024;

    fn probe_identity() -> FsmIdentity {
        <ProbeSm as uc_service::RawStateMachine>::IDENTITY
    }

    /// MESSAGE ‖ TIMER(ours, table) ‖ TIMER(foreign) ‖ PADDING(96 B) ‖ MESSAGE.
    /// Offsets: 0, 64, 128, 192, 288 — the last frame ends at 352.
    ///
    /// The PADDING frame is deliberately 96 bytes, not the 32-byte minimum:
    /// `align_frame_len(32) == HEADER_LEN`, so a header-only advance (the
    /// LIVE ring's rule, `uc_log::reader::FrameIter`) would be
    /// indistinguishable from the archived-block rule at 32. At 96 they
    /// differ, and the body below is filled with `0xFF` so a wrong advance
    /// reads a bogus `length` and `break`s out — which the tests' assertions
    /// on what follows the padding rule out.
    fn hand_laid_block() -> Vec<u8> {
        let mut b = vec![0u8; 352];
        let mut off = 0;
        off += lay(&mut b, off, FRAME_TYPE_MESSAGE, 0, b"CMD\x01abcd");
        assert_eq!(off, 64);
        let ours = timer_payload(probe_identity().hash(), 9, 77);
        off += lay(&mut b, off, FRAME_TYPE_TIMER, FLAG_TIMER_TABLE, &ours);
        assert_eq!(off, 128);
        let theirs = timer_payload(0xDEAD_BEEF_0BAD_F00D, 11, 88);
        off += lay(&mut b, off, FRAME_TYPE_TIMER, 0, &theirs);
        assert_eq!(off, 192);
        let pad = [0xFFu8; 64];
        off += lay(&mut b, off, FRAME_TYPE_PADDING, 0, &pad);
        assert_eq!(off, 288);
        off += lay(&mut b, off, FRAME_TYPE_MESSAGE, 0, b"CMD\x02wxyz");
        assert_eq!(off, 352);
        b
    }

    #[test]
    fn walk_dispatches_message_and_own_timer_skips_foreign_and_stops_at_end() {
        let block = hand_laid_block();
        let mut sm = ProbeSm::default();
        let (mut resp, mut entries) = (Vec::new(), Vec::new());
        // The last MESSAGE starts at 1312 and ends at 1376; an `end` of 1340
        // makes it straddle the frontier, so it must not be dispatched.
        let cont = walk_block(
            &mut sm,
            probe_identity(),
            BASE,
            &block,
            BASE + 316,
            &mut resp,
            &mut entries,
        );

        // Stopped AT the straddling frame — not `break`-ed out of a desynced
        // walk, which would have returned true.
        assert!(!cont, "the straddling frame must stop the scan");
        assert_eq!(entries.len(), 2, "{entries:?}");

        // The MESSAGE: recorded headers, tag = the first 32 payload bytes
        // (this payload is 8, so all of it).
        assert_eq!(entries[0].pos, BASE);
        assert_eq!(entries[0].kind, EntryKind::Message);
        assert_eq!(entries[0].tag, b"CMD\x01abcd".to_vec());
        assert_eq!(entries[0].response, b"ok".to_vec());
        assert_eq!(
            entries[0].sched,
            vec![
                Sched {
                    op: "schedule".into(),
                    id: 7,
                    deadline_ns: 100
                },
                Sched {
                    op: "cancel".into(),
                    id: 3,
                    deadline_ns: 0
                },
            ]
        );
        // The RECORDED header values reached apply: time_ns and term.
        assert_eq!(sm.applied, vec![(BASE, b"CMD\x01abcd".to_vec(), 9_000, 4)]);

        // OUR timer, table flag carried, id/deadline in the right slots.
        assert_eq!(
            entries[1].kind,
            EntryKind::Timer {
                id: 9,
                deadline_ns: 77,
                table: true
            }
        );
        assert_eq!(entries[1].pos, BASE + 64);
        assert!(entries[1].tag.is_empty() && entries[1].response.is_empty());

        // The FOREIGN timer produced nothing — and only one timer arrived.
        assert_eq!(sm.timers.len(), 1);
        assert_eq!(sm.timers[0].0, TimerEvent::new(9, 77, true));
        assert_eq!(sm.timers[0].1, BASE + 64);

        // The straddling MESSAGE never reached the SM, and the frontier
        // advanced only over what was dispatched.
        assert_eq!(sm.applied.len(), 1);
        assert_eq!(sm.last_applied(), Some(BASE + 64));
    }

    #[test]
    fn walk_runs_the_whole_block_when_end_is_above_it() {
        let block = hand_laid_block();
        let mut sm = ProbeSm::default();
        let (mut resp, mut entries) = (Vec::new(), Vec::new());
        let cont = walk_block(
            &mut sm,
            probe_identity(),
            BASE,
            &block,
            u64::MAX,
            &mut resp,
            &mut entries,
        );
        // Ran off the end of the block, so the scan continues to the next one.
        assert!(cont);
        // Both MESSAGEs and our one TIMER; the 96-byte PADDING was walked at
        // its ALIGNED span (a header-only advance would have desynced into
        // its 0xFF body and broken out before the second MESSAGE).
        assert_eq!(entries.len(), 3, "{entries:?}");
        assert_eq!(entries[2].pos, BASE + 288);
        assert_eq!(entries[2].tag, b"CMD\x02wxyz".to_vec());
        assert_eq!(sm.applied.len(), 2);
    }

    #[test]
    fn walk_skips_frames_at_or_below_the_frontier() {
        let block = hand_laid_block();
        let mut sm = ProbeSm {
            last: Some(BASE + 64),
            ..Default::default()
        };
        let (mut resp, mut entries) = (Vec::new(), Vec::new());
        walk_block(
            &mut sm,
            probe_identity(),
            BASE,
            &block,
            u64::MAX,
            &mut resp,
            &mut entries,
        );
        // The first MESSAGE (at BASE) and our TIMER (at BASE+64) are history;
        // only the last MESSAGE is above the frontier.
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_eq!(entries[0].pos, BASE + 288);
    }

    // ---- `sched_of`'s op names ----

    #[test]
    fn every_sched_op_has_its_wire_name() {
        assert_eq!(op_name(SchedOp::Schedule), "schedule");
        assert_eq!(op_name(SchedOp::Cancel), "cancel");
        assert_eq!(op_name(SchedOp::Consumed), "consumed");
        assert_eq!(op_name(SchedOp::TableConsumed), "table_consumed");
    }

    #[test]
    fn sched_of_drains_the_ctx_in_request_order() {
        let mut ctx = ApplyCtx::for_sm::<ProbeSm>(64);
        ctx.schedule(7, 100);
        ctx.cancel(3);
        let got = sched_of(&mut ctx);
        assert_eq!(
            got,
            vec![
                Sched {
                    op: "schedule".into(),
                    id: 7,
                    deadline_ns: 100
                },
                Sched {
                    op: "cancel".into(),
                    id: 3,
                    deadline_ns: 0
                },
            ]
        );
        // Drained: a second frame cannot inherit the first's records.
        assert!(sched_of(&mut ctx).is_empty());
    }
}
