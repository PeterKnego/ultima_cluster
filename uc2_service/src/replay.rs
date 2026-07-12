// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Journal-replay reconstruction (spec §7, task14 semantics). When the live
//! log buffer has scrolled past a follower's cursor — a fresh service attaching
//! to a long-running node, or a node cold-start whose ring predates the
//! service's `last_applied` — the apply agent degrades to replaying the
//! ARCHIVED log (the journal) into the state machine, then rejoins the live
//! buffer at the byte position replay reached.
//!
//! The journal is read through [`ultima_journal::TailReader`]: strictly
//! read-only, no lock on the node's writer, tolerant of a concurrently-appending
//! archive (see its module doc for the safety argument). Each journal record is
//! one archived BLOCK whose `meta` is the block's base stream position and whose
//! payload is the raw frames of that block concatenated (exactly as they lay in
//! the ring). Replay walks those frames and dispatches each `MESSAGE`.

use std::sync::Mutex;

use uc2_log::cnc::CncPage;
use uc_protocol::v2::frame::{self, FRAME_TYPE_MESSAGE, HEADER_LEN, align_frame_len};
use ultima_journal::TailReader;

use crate::config::ServiceError;
use crate::traits::StateMachine;

/// Replay archived journal blocks into `sm`, returning the byte cursor after the
/// last applied/skipped frame — the point at which the live [`LogFollower`] can
/// resume.
///
/// For each block (`meta` = base stream position) it walks the block's frames
/// and dispatches every `MESSAGE` frame whose position is `> sm.last_applied()`
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
/// [`LogFollower`]: uc2_log::reader::LogFollower
pub(crate) fn replay_into<S: StateMachine>(
    sm: &Mutex<S>,
    cnc: &CncPage,
    journal_dir: &std::path::Path,
) -> Result<u64, ServiceError> {
    let reader = TailReader::open(journal_dir).map_err(|e| ServiceError::Replay(e.to_string()))?;
    let mut guard = sm.lock().unwrap();
    // The live rejoin point: advances over every frame walked (applied,
    // idempotently-skipped, or padding). Stays a frame boundary throughout.
    let mut cursor = 0u64;
    let mut decode_error = false;

    // Skip whole segment FILES entirely below what the SM has already applied:
    // replay only dispatches frames with `pos > last_applied`, so a segment
    // whose records all end at/below `last_applied` contributes nothing. This is
    // pure perf plumbing — `scan_from` still yields the COVERING segment (the
    // one holding `last_applied`) and the per-frame `> last_applied` skip below
    // is unchanged, so the applied set and the returned `cursor` are identical
    // to the old full `scan`; only the wasted re-read of purged/applied leading
    // segments is removed (the O(journal)-per-overrun M5 carry). The gap GUARD
    // (below-floor detection) lands in Task 5.
    let start_pos = guard.last_applied().unwrap_or(0);

    reader
        .scan_from(start_pos, |_seq, base, payload| {
            // Re-read the live apply frontier PER BLOCK: both commit and durable
            // can advance while we replay, so a later block may legitimately be
            // (partly) applicable that an earlier snapshot would have gated.
            let counters = cnc.counters();
            let target = counters.commit.load_acquire().min(counters.durable.load_acquire());

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
                // Dispatch MESSAGE frames not already reflected in the SM. NEW_TERM
                // / PADDING (and any future non-MESSAGE type) are not user data.
                if hdr.frame_type == FRAME_TYPE_MESSAGE && Some(pos) > guard.last_applied() {
                    match bincode::serde::decode_from_slice::<S::Command, _>(
                        &payload[off + HEADER_LEN..off + total],
                        bincode::config::standard(),
                    ) {
                        // Leader-publish suppressed: apply only (see the doc).
                        Ok((cmd, _)) => {
                            let _ = guard.apply(pos, cmd);
                        }
                        Err(_) => {
                            // A committed, archived frame that will not decode is
                            // unrecoverable corruption — surface it as fail-stop
                            // rather than silently skipping user data.
                            decode_error = true;
                            return false;
                        }
                    }
                }
                cursor = end;
                off += aligned;
            }
            true
        })
        .map_err(|e| ServiceError::Replay(e.to_string()))?;

    if decode_error {
        return Err(ServiceError::Replay(
            "corrupt archived MESSAGE frame (fail-stop)".to_string(),
        ));
    }
    Ok(cursor)
}
