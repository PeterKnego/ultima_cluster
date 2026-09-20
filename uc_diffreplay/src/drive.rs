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
/// framework envelope, then hand the payload to the SM. Returns the position
/// the SM reported, which must equal `position`.
fn install<S: SnapshotStateMachine>(
    sm: &mut S,
    artifact: &Path,
    position: u64,
) -> anyhow::Result<u64> {
    let mut f =
        BufReader::new(File::open(artifact).with_context(|| artifact.display().to_string())?);
    verify_snapshot_envelope(&mut f, position).map_err(|e| anyhow::anyhow!("envelope: {e}"))?;
    let got = sm
        .install_snapshot(position, &mut f)
        .map_err(|e| anyhow::anyhow!("install_snapshot({position}): {e}"))?;
    if got != position {
        bail!("install_snapshot returned {got}, expected {position}");
    }
    Ok(got)
}

pub fn project_artifact<S: SnapshotStateMachine>(
    mut sm: S,
    artifact: &Path,
    position: u64,
) -> anyhow::Result<String> {
    install(&mut sm, artifact, position)?;
    project_string(&sm)
}

pub fn drive<S: SnapshotStateMachine>(
    mut sm: S,
    corpus: &Corpus,
    origin: Origin,
) -> anyhow::Result<Trace> {
    let m = &corpus.manifest;
    let (start, projection_at_origin) = match origin {
        Origin::Artifact => {
            install(&mut sm, &corpus.artifact(), m.origin)?;
            (m.origin, Some(project_string(&sm)?))
        }
        Origin::Genesis => (0, None),
    };

    let reader = TailReader::open(&corpus.journal_dir())?;
    let identity = S::IDENTITY;
    let mut entries = Vec::new();
    let mut resp = Vec::with_capacity(256);
    let end = m.end;

    reader.scan_from(start, |_seq, base, block| {
        let mut off = 0usize;
        while off + HEADER_LEN <= block.len() {
            let hdr = frame::read_header(&block[off..]);
            let total = hdr.length as usize;
            let aligned = align_frame_len(total);
            // `replay.rs`'s defensive stop: a sub-header or over-running
            // length would desync the walk (archived blocks are frame-aligned
            // and CRC-validated, so unreachable on real input). End THIS
            // block rather than spin — and, unlike a whole-scan abort, leave
            // the following blocks to be walked.
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
            // PADDING needs no arm: it matches neither dispatch arm and the
            // walk advances over it like any other frame.
            let payload = &block[off + HEADER_LEN..off + total];
            // The apply loop's idempotency guard, verbatim
            // (`uc_service/src/apply.rs`, `replay.rs`): re-walking an overlap
            // already reflected in the SM applies nothing. After
            // `install_snapshot(P)` the SM's cursor is the one its image
            // recorded — strictly BELOW P — so the frame starting exactly at
            // P is applied, which is what makes P an exclusive frontier.
            let above = Some(pos) > sm.last_applied();
            match hdr.frame_type {
                FRAME_TYPE_MESSAGE if above => {
                    let mut ctx = ApplyCtx::new(pos, identity)
                        .with_time(hdr.time_ns)
                        .with_term(hdr.leadership_term_id);
                    resp.clear();
                    sm.apply(&mut ctx, payload, &mut resp);
                    entries.push(Entry {
                        pos,
                        kind: EntryKind::Message,
                        tag: payload[..payload.len().min(4)].to_vec(),
                        response: resp.clone(),
                        sched: sched_of(&mut ctx),
                    });
                }
                FRAME_TYPE_TIMER if above => {
                    // A TIMER frame naming another row's identity is skipped
                    // — but the walk still advances over it below.
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
    })?;

    Ok(Trace {
        row: m.row,
        version: S::VERSION,
        origin: start,
        end,
        projection_at_origin,
        projection_at_end: Some(project_string(&sm)?),
        entries,
    })
}

fn sched_of(ctx: &mut ApplyCtx) -> Vec<Sched> {
    ctx.take_sched_records_for_test()
        .into_iter()
        .map(|r| Sched {
            op: match r.op {
                SchedOp::Schedule => "schedule",
                SchedOp::Cancel => "cancel",
                SchedOp::Consumed => "consumed",
                SchedOp::TableConsumed => "table_consumed",
            }
            .into(),
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
