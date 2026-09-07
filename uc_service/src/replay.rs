// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Journal-replay reconstruction (spec §7, task14 semantics). When the live
//! log buffer has scrolled past a follower's cursor — a fresh service attaching
//! to a long-running node, or a node cold-start whose ring predates the
//! service's `last_applied` — the apply agent degrades to replaying the
//! ARCHIVED log (the journal) into the state machine, then rejoins the live
//! buffer at the byte position replay reached.
//!
//! The journal is read through [`uc_journal::TailReader`]: strictly
//! read-only, no lock on the node's writer, tolerant of a concurrently-appending
//! archive (see its module doc for the safety argument). Each journal record is
//! one archived BLOCK whose `meta` is the block's base stream position and whose
//! payload is the raw frames of that block concatenated (exactly as they lay in
//! the ring). Replay walks those frames and dispatches each `MESSAGE`, and
//! each `TIMER` frame addressed to this row (spec §4.8).

use std::sync::Mutex;

use uc_journal::TailReader;
use uc_log::cnc::CncPage;
use uc_protocol::v2::cnc::NODE_FLAG_LEARNER;
use uc_protocol::v2::frame::{
    self, FLAG_SNAPSHOT_STANDBY, FLAG_TIMER_TABLE, FRAME_TYPE_MESSAGE, FRAME_TYPE_SNAPSHOT,
    FRAME_TYPE_TIMER, HEADER_LEN, align_frame_len,
};

use crate::apply::{SnapshotRestore, SnapshotTrigger, on_snapshot_frame};
use crate::config::ServiceError;
use crate::traits::{ApplyCtx, RawStateMachine, TimerEvent};

/// What a replay pass produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Replay {
    /// Replay ran: the byte cursor after the last applied/skipped frame — the
    /// point at which the live [`LogFollower`] resumes.
    ///
    /// [`LogFollower`]: uc_log::reader::LogFollower
    Rejoin(u64),
    /// The gap guard found a covering artifact, but ABOVE the live apply
    /// target `min(commit, durable)`: installing it would put the SM ahead of
    /// what this node has committed and durable. Not a gap — the counters
    /// are still climbing (a two-row snapshot set adopts the floor at the
    /// `min` of its rows' positions, so the other row's artifact sits above
    /// the floor until the tail catches up; nightly 33488022809). Nothing was
    /// applied; the caller retries next cycle.
    AwaitArtifact { artifact: u64, target: u64 },
}

/// Ruling P10's inputs: everything a replayed span needs in order to act on
/// its last `SNAPSHOT` frame. Bundled rather than passed as three more
/// parameters because they travel together and mean one thing.
///
/// `trigger` is `None` on a row started with plain `start()` — not
/// snapshot-capable, so pass 1 is skipped outright and the walk is
/// byte-for-byte what it was before P10.
pub(crate) struct ReplayInstant<'a, S: RawStateMachine> {
    pub trigger: &'a mut Option<SnapshotTrigger<S>>,
    /// The node-written status flags word, read ONCE per apply cycle by the
    /// caller — the same value the live arm uses, so a role change mid-cycle
    /// cannot make the two paths disagree about whether this node is a
    /// learner.
    pub node_flags: u64,
    /// This row's cnc slot, for the "already held" guard below.
    pub service_id: u8,
}

/// Ruling P10, pass 1: the START position of the LAST `SNAPSHOT` frame in the
/// span this replay is about to walk that this row should freeze at, or
/// `None`.
///
/// **Why a pre-pass and not an inline decision.** "Act on the last one" cannot
/// be decided while walking: the freeze has to happen BEFORE any frame at or
/// above P is applied (P6), and whether a given instant is the last is only
/// known once the span has been read to its end. Freezing at each in turn and
/// keeping the newest would pay `freeze()` — O(state), on the apply thread,
/// under the SM lock — once per instant in the span. Deciding first costs one
/// extra walk that applies nothing and decodes no user bytes, and this whole
/// path is the overrun path, never the live walk.
///
/// **Cost, measured honestly** (fix round 3): this is NOT a header-only scan.
/// `TailReader::scan_from` hands each journal BLOCK to its visitor with the
/// payload already read and CRC-checked, so pass 1 costs a second full read of
/// the span — ~2x the journal I/O per overrun, bounded by the tail above the
/// row's start position. Cheaper than the alternative it replaces (an O(state)
/// `freeze()` per instant in the span, under the SM lock) and confined to the
/// overrun path, but not free. Two bounded alternatives if it ever matters: a
/// REVERSE segment scan that stops at the first `SNAPSHOT` frame it finds, or
/// a cnc word carrying the newest commanded instant's position so pass 1
/// becomes one load.
///
/// Three declines beyond `on_snapshot_frame`'s own three:
///
/// * **an instant at or below the row's APPLIED frontier** — the guard fix
///   round 3 added, and the one that matters for correctness. `scan_from`
///   always yields the COVERING segment, so a span routinely contains frames
///   the SM is already past; freezing at one of those would pin state ABOVE P
///   and tag it P, which the envelope check cannot catch (the tag IS P) and
///   which makes this node's artifact for that instant differ from every live
///   replica's — a joiner installing it double-applies `(P, applied]`. A
///   `SNAPSHOT` frame at or below the frontier is history, exactly as it is
///   for the `MESSAGE` and `TIMER` arms.
/// * an instant at or below the artifact this row already holds (its cnc
///   `snapshot_pos`) — a restart replaying the span that contains its own
///   installed artifact's frame would otherwise rebuild the same file;
/// * a standby instant on a node that is not a learner (spec §5.7), checked
///   here as well as in `on_snapshot_frame` so a standby-flagged frame cannot
///   mask a plain instant EARLIER in the same span by being picked as "last"
///   and then declined.
fn last_actionable_instant<S: RawStateMachine>(
    reader: &TailReader,
    start_pos: u64,
    applied: Option<u64>,
    target: u64,
    cnc: &CncPage,
    instant: &ReplayInstant<'_, S>,
) -> Result<Option<u64>, ServiceError> {
    let held = crate::attach::slot(cnc, instant.service_id)
        .snapshot_pos
        .load_acquire();
    let mut last = None;
    reader
        .scan_from(start_pos, |_seq, base, payload| {
            let mut off = 0usize;
            while off + HEADER_LEN <= payload.len() {
                let hdr = frame::read_header(&payload[off..]);
                let total = hdr.length as usize;
                let aligned = align_frame_len(total);
                if total < HEADER_LEN || off + aligned > payload.len() {
                    break;
                }
                let pos = base + off as u64;
                let end = pos + aligned as u64;
                // The SAME `target` value pass 2 uses — captured once by the
                // caller and shared, so the two passes provably walk the same
                // span. See `replay_into`'s capture for why that matters.
                if end > target {
                    return false;
                }
                if hdr.frame_type == FRAME_TYPE_SNAPSHOT
                    && Some(pos) > applied
                    && end > held
                    && !(hdr.flags & FLAG_SNAPSHOT_STANDBY != 0
                        && instant.node_flags & NODE_FLAG_LEARNER == 0)
                {
                    last = Some(pos);
                }
                off += aligned;
            }
            true
        })
        .map_err(|e| ServiceError::Replay(e.to_string()))?;
    Ok(last)
}

/// Replay archived journal blocks into `sm` (see [`Replay`] for the two
/// outcomes).
///
/// For each block (`meta` = base stream position) it walks the block's frames
/// and dispatches every `MESSAGE` frame — and every `TIMER` frame naming this
/// row's identity hash — whose position is `> sm.last_applied()`
/// (idempotent-skip: re-walking an overlap already reflected in the SM applies
/// nothing) AND whose frame END is `<= target`, where `target = min(commit,
/// durable)` is captured ONCE by [`replay_into`] and shared with pass 1 — fix
/// round 3's whole point, so the pre-pass's choice of instant and the apply
/// pass's bound cannot disagree about where the span ends. (It said "RE-READ
/// per block" until the final wave's M6; re-reading is what made a replayed
/// instant able to freeze state above P.)
///
/// # Invariants
/// * NEVER apply above the live `min(commit, durable)` — the per-frame target
///   guard stops the walk at the first frame that would cross it, leaving the
///   returned cursor at that frame's start (a frame boundary the live follower
///   resumes from).
/// * Dispatch is by POSITION, so re-walking already-applied frames is safe.
/// * Leader-publish is SUPPRESSED here: these responses were already answered by
///   the previous incarnation; re-emitting them onto the egress ring would be
///   harmless (at-least-once) but noisy, so replay applies without publishing.
///
/// [`LogFollower`]: uc_log::reader::LogFollower
pub(crate) fn replay_into<S: RawStateMachine>(
    sm: &Mutex<S>,
    cnc: &CncPage,
    journal_dir: &std::path::Path,
    restore: Option<&SnapshotRestore<S>>,
    instant: ReplayInstant<'_, S>,
) -> Result<Replay, ServiceError> {
    let reader = TailReader::open(journal_dir).map_err(|e| ServiceError::Replay(e.to_string()))?;
    let mut guard = sm.lock().unwrap();
    // The live rejoin point: advances over every frame walked (applied,
    // idempotently-skipped, or padding). Stays a frame boundary throughout.
    let mut cursor = 0u64;
    // Reused response scratch: replay never publishes (see the doc), so the
    // response bytes are written and dropped — one buffer for the whole pass.
    let mut scratch = Vec::with_capacity(256);

    // M6 Task 5 — the GAP GUARD. `needed` is the position tail replay would
    // start dispatching from; `first` is the journal's lowest replayable
    // position (base of its first retained block, 0 if unpurged). If the journal
    // has been purged ABOVE what the SM needs (`first > needed`), the tail alone
    // cannot rebuild a contiguous state — the frames in `(needed, first)` are
    // gone. Without this guard, `scan_from` would silently start at `first` and
    // "succeed" with a hole in the middle of the state (the silent-gap bug
    // class). Instead: install a covering snapshot (if the SM can), else
    // fail-stop with the contract named.
    let mut start_pos = guard.last_applied().unwrap_or(0);
    let first = reader
        .first_meta()
        .map_err(|e| ServiceError::Replay(e.to_string()))?
        .unwrap_or(0);
    if first > start_pos {
        // A covering snapshot must reach at least `first` (so the snapshot's
        // prefix `[0, S]` and the journal's tail `[first, target]` overlap and
        // leave no hole). Pick the newest snapshot no higher than the live apply
        // target; require `S >= first`.
        let target = {
            let c = cnc.counters();
            c.commit.load_acquire().min(c.durable.load_acquire())
        };
        let covering = match restore {
            Some(r) => r
                .store
                .newest(target)
                .map_err(|e| ServiceError::Replay(e.to_string()))?,
            None => None,
        };
        match (restore, covering) {
            (Some(r), Some((s_pos, path))) if s_pos >= first => {
                let mut file =
                    std::fs::File::open(&path).map_err(|e| ServiceError::Replay(e.to_string()))?;
                // Ruling P6, BEFORE a byte reaches the state machine: strip and
                // check the framework's 16-byte envelope. The file NAME is what
                // `newest` picked `s_pos` from, and a name is only a name — a
                // `uc2ctl restore` of a mis-copied backup, or any rename, can
                // present an artifact built at `P0` as `P`, and installing it
                // would leave `(P0, P)` unapplied. That is a silent state gap,
                // so it is a NAMED refusal here rather than an install. The
                // joiner path lands here too: the receiver writes the shipped
                // bytes verbatim under `snapshots/<row>/`, so the artifact a
                // snapshot session produced carries the shipper's envelope and
                // is checked by this same line.
                crate::snapshots::verify_snapshot_envelope(&mut file, s_pos).map_err(|e| {
                    ServiceError::MistaggedSnapshot {
                        path: path.display().to_string(),
                        source: e,
                    }
                })?;
                let installed = (r.install)(&mut guard, s_pos, &mut file)
                    .map_err(|e| ServiceError::Replay(format!("snapshot install: {e}")))?;
                // A self-check on the TRAIT contract ("returns the post-install
                // position, which MUST equal `position`"), not on the artifact:
                // the mis-tag guarantee is the envelope check above, which is
                // the framework's and cannot be weakened by an SM.
                debug_assert_eq!(installed, s_pos, "install must land at the artifact's tag");
                // The SM is now at `installed`; tail replay continues from there.
                // (`installed >= first`, so the journal's retained tail is a
                // contiguous continuation — no hole.)
                start_pos = installed;
                cursor = installed;
            }
            // A covering artifact exists but sits ABOVE the live target: the
            // journal's tail `[first, target]` cannot yet meet it. Not a gap —
            // wait for the counters (nightly 33488022809: the fail-stop here
            // killed a learner's second FSM whose artifact was above the
            // two-row set's `min` floor while its tail was still arriving).
            (Some(r), _) => {
                let above = r
                    .store
                    .newest(u64::MAX)
                    .map_err(|e| ServiceError::Replay(e.to_string()))?;
                match above {
                    Some((s_pos, _)) if s_pos >= first && s_pos > target => {
                        return Ok(Replay::AwaitArtifact {
                            artifact: s_pos,
                            target,
                        });
                    }
                    _ => {
                        return Err(ServiceError::SnapshotRequired {
                            needed: start_pos,
                            first_available: first,
                        });
                    }
                }
            }
            // No install capability: the gap is unbridgeable. Fail-stop,
            // contract named (kills the silent-gap class).
            (None, _) => {
                return Err(ServiceError::SnapshotRequired {
                    needed: start_pos,
                    first_available: first,
                });
            }
        }
    }

    // Skip whole segment FILES entirely below what the SM has already applied.
    // Replay only dispatches frames with `pos > last_applied`, so a segment
    // whose records all end at or below `last_applied` contributes nothing.
    //
    // This is pure perf plumbing. `scan_from` still yields the COVERING segment
    // (the one holding `last_applied`), and the per-frame `> last_applied` skip
    // below is unchanged — so the applied set and the returned `cursor` are
    // identical to the old full `scan`. All that is removed is the wasted
    // re-read of purged or already-applied leading segments (the
    // O(journal)-per-overrun M5 carry).

    // Ruling P10, PASS 1: which `SNAPSHOT` frame in the span this pass is
    // about to walk should be frozen at (see [`last_actionable_instant`] for
    // why there is a pre-pass at all, and what it costs). Skipped entirely for
    // a row that is not snapshot-capable.
    //
    // The applied frontier is sampled HERE, after the install block above may
    // have moved it, and handed to pass 1 so both passes apply the identical
    // "at or below the frontier is history" bound.
    let applied_frontier = guard.last_applied();
    // ONE apply frontier for BOTH passes (fix round 3). It used to be re-read
    // per block by pass 2 so a long replay could pick up bytes that committed
    // while it ran; that made the two passes disagree about where the span
    // ENDS, and an instant in the difference was walked past and lost. The
    // per-block refresh bought at most a few extra frames per pass and cost a
    // correctness argument, so it is gone: replay now stops at the frontier it
    // started with, and the caller's `Re-loop` (which either reads live or
    // degrades again from a strictly higher cursor) picks up whatever
    // committed meanwhile. Forward progress is unchanged — the cursor still
    // advances every pass — and pass 1 and pass 2 now provably walk the same
    // frames.
    let target = {
        let c = cnc.counters();
        c.commit.load_acquire().min(c.durable.load_acquire())
    };
    let freeze_at = if instant.trigger.is_some() {
        last_actionable_instant(&reader, start_pos, applied_frontier, target, cnc, &instant)?
    } else {
        None
    };

    reader
        .scan_from(start_pos, |_seq, base, payload| {
            // `target` is the ONE frontier captured above, shared with pass 1
            // (fix round 3) — not re-read per block any more.
            let mut off = 0usize;
            while off + HEADER_LEN <= payload.len() {
                let hdr = frame::read_header(&payload[off..]);
                let total = hdr.length as usize;
                let aligned = align_frame_len(total);
                // Defensive: a sub-header or over-running length would desync the
                // walk (archived blocks are frame-aligned + CRC-validated, so
                // unreachable on real input). Stop this block rather than spin.
                if total < HEADER_LEN || off + aligned > payload.len() {
                    break;
                }
                let pos = base + off as u64;
                let end = pos + aligned as u64;
                // The load-bearing guard: never apply a frame whose END exceeds
                // the live min(commit, durable). Stop the WHOLE scan here — the
                // cursor stays at this frame's start, and the live follower
                // resumes exactly there.
                if end > target {
                    return false;
                }
                // Dispatch MESSAGE frames, and TIMER frames addressed to THIS
                // row, that are not already reflected in the SM. PADDING /
                // NEW_TERM / CONFIG (and any future type that is neither) are
                // not user data.
                //
                // SNAPSHOT (type 7) is ACTED ON — at the ONE position pass 1
                // picked (Ruling P10). It replaces the pre-P10 rule, which
                // ignored every instant here on the argument that "freezing at
                // an instant the cluster passed long ago is meaningless work".
                // It is not: the leader is still waiting on the NEWEST one, and
                // ignoring it made a ring small enough to lap this row silently
                // cost instants — a set that could never complete, at any
                // timeout. Only the EARLIER instants in a span are meaningless,
                // and pass 1 is what drops those.
                //
                // Leader-publish suppressed: apply only (see the
                // doc), so the response bytes land in the throwaway scratch. A
                // typed SM decodes inside its blanket `RawStateMachine` impl and
                // fail-stops there on a committed, archived frame that will not
                // decode — unrecoverable corruption, never a silent skip of
                // user data.
                //
                // The cheap frame-type test comes FIRST in each arm
                // (final-review M1): `last_applied()` is a trait call and must
                // not run for a frame the arm is going to skip anyway.
                if hdr.frame_type == FRAME_TYPE_MESSAGE && Some(pos) > guard.last_applied() {
                    scratch.clear();
                    guard.apply(
                        &mut ApplyCtx::new(pos, S::IDENTITY)
                            .with_time(hdr.time_ns)
                            .with_term(hdr.leadership_term_id),
                        &payload[off + HEADER_LEN..off + total],
                        &mut scratch,
                    );
                } else if hdr.frame_type == FRAME_TYPE_TIMER
                    && Some(pos) > guard.last_applied()
                    && let Some(body) =
                        frame::read_timer_body(&payload[off + HEADER_LEN..off + total])
                    && body.identity_hash == S::IDENTITY.hash()
                {
                    // Deliver exactly as the live loop would (same guard, same
                    // hash check); any `on_timer` requests made here are
                    // dropped — the re-announce after replay
                    // (`ApplyState::announce_pending`, set once this pass
                    // rejoins the live buffer) covers them, which is the
                    // whole point of §4.8.
                    let mut ctx = ApplyCtx::new(pos, S::IDENTITY)
                        .with_time(hdr.time_ns)
                        .with_term(hdr.leadership_term_id);
                    guard.on_timer(
                        &mut ctx,
                        TimerEvent {
                            id: body.timer_id,
                            deadline_ns: body.deadline_ns,
                            table: hdr.flags & FLAG_TIMER_TABLE != 0,
                        },
                    );
                    let _ = ctx.take_sched_records();
                } else if hdr.frame_type == FRAME_TYPE_SNAPSHOT && Some(pos) > guard.last_applied()
                {
                    // Fix round 3: the SAME `> last_applied` bound the two arms
                    // above carry, and for a sharper reason. `scan_from` always
                    // yields the COVERING segment, so a span routinely holds
                    // frames the SM is already past; freezing at one of those
                    // would pin state ABOVE P and tag it P — undetectable (the
                    // tag IS P, so the envelope verifies), different from every
                    // live replica's artifact for that instant, and a joiner
                    // installing it double-applies `(P, applied]`. A `SNAPSHOT`
                    // frame at or below the frontier is history.
                    if Some(pos) == freeze_at {
                        // Ruling P10 + P6: here, and only here — after every
                        // frame below P has applied and before any frame at or
                        // above P does, so the artifact is a function of the
                        // log strictly below P: the same function the live loop
                        // computes, which is what makes one instant's artifacts
                        // position-aligned across rows. The decision
                        // (capability, standby, one build in flight) is
                        // `on_snapshot_frame`'s, unchanged — this path must not
                        // grow a second opinion about any of it.
                        //
                        // Every OTHER `SNAPSHOT` frame in the span is skipped
                        // silently, which is P10's "act on the last one only":
                        // pass 1 walked these same frames, under the same
                        // `target` and the same `> last_applied` bound, so a
                        // frame it did not pick is either an earlier instant
                        // (superseded) or one it declined for a reason that
                        // has not changed.
                        let slot = crate::attach::slot(cnc, instant.service_id);
                        on_snapshot_frame(
                            instant.trigger,
                            &guard,
                            pos,
                            &hdr,
                            instant.node_flags,
                            slot,
                        );
                    }
                }
                cursor = end;
                off += aligned;
            }
            true
        })
        .map_err(|e| ServiceError::Replay(e.to_string()))?;

    Ok(Replay::Rejoin(cursor))
}
