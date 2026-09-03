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
use uc_protocol::v2::frame::{
    self, FLAG_TIMER_TABLE, FRAME_TYPE_MESSAGE, FRAME_TYPE_TIMER, HEADER_LEN, align_frame_len,
};

use crate::apply::SnapshotRestore;
use crate::config::ServiceError;
use crate::traits::{ApplyCtx, RawStateMachine, TimerEvent};

/// Replay archived journal blocks into `sm`, returning the byte cursor after the
/// last applied/skipped frame — the point at which the live [`LogFollower`] can
/// resume.
///
/// For each block (`meta` = base stream position) it walks the block's frames
/// and dispatches every `MESSAGE` frame — and every `TIMER` frame naming this
/// row's identity hash — whose position is `> sm.last_applied()`
/// (idempotent-skip: re-walking an overlap already reflected in the SM applies
/// nothing) AND whose frame END is `<= target`, where `target = min(commit,
/// durable)` is RE-READ per block (both counters can advance while replay runs).
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
) -> Result<u64, ServiceError> {
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
                let installed = (r.install)(&mut guard, s_pos, &mut file)
                    .map_err(|e| ServiceError::Replay(format!("snapshot install: {e}")))?;
                debug_assert_eq!(installed, s_pos, "install must land at the artifact's tag");
                // The SM is now at `installed`; tail replay continues from there.
                // (`installed >= first`, so the journal's retained tail is a
                // contiguous continuation — no hole.)
                start_pos = installed;
                cursor = installed;
            }
            // No install capability, or no snapshot covers the floor: the gap is
            // unbridgeable. Fail-stop, contract named (kills the silent-gap class).
            _ => {
                return Err(ServiceError::SnapshotRequired {
                    needed: start_pos,
                    first_available: first,
                });
            }
        }
    }

    // Skip whole segment FILES entirely below what the SM has already applied:
    // replay only dispatches frames with `pos > last_applied`, so a segment
    // whose records all end at/below `last_applied` contributes nothing. This is
    // pure perf plumbing — `scan_from` still yields the COVERING segment (the
    // one holding `last_applied`) and the per-frame `> last_applied` skip below
    // is unchanged, so the applied set and the returned `cursor` are identical
    // to the old full `scan`; only the wasted re-read of purged/applied leading
    // segments is removed (the O(journal)-per-overrun M5 carry).

    reader
        .scan_from(start_pos, |_seq, base, payload| {
            // Re-read the live apply frontier PER BLOCK: both commit and durable
            // can advance while we replay, so a later block may legitimately be
            // (partly) applicable that an earlier snapshot would have gated.
            let counters = cnc.counters();
            let target = counters
                .commit
                .load_acquire()
                .min(counters.durable.load_acquire());

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
                // not user data. Leader-publish suppressed: apply only (see the
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
                }
                cursor = end;
                off += aligned;
            }
            true
        })
        .map_err(|e| ServiceError::Replay(e.to_string()))?;

    Ok(cursor)
}
