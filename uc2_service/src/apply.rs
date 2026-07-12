// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The apply agent duty cycle (spec §7). A single polling thread that follows
//! the committed log, applies each `MESSAGE` frame to the user's state machine,
//! and (while leader) publishes the response onto the egress broadcast. On an
//! `Overrun` (the live buffer scrolled past the cursor) it degrades to journal
//! replay (Task 9) and rejoins the live buffer at the byte position replay
//! reached.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use uc2_log::cnc::CncPage;
use uc2_log::reader::{Batch, LogFollower};
use uc_protocol::ring::SpscConsumer;
use uc_protocol::v2::cnc::NODE_FLAG_LEADER;
use uc_protocol::v2::frame::FRAME_TYPE_MESSAGE;

use crate::egress::Egress;
use crate::replay::replay_into;
use crate::traits::StateMachine;

/// Everything the apply thread owns. Fields are accessed directly (not through
/// `&self` methods) inside the frames loop so the borrow checker sees the
/// per-field disjoint borrows (`follower` iterated while `sm`/`egress`/`cnc`
/// are touched).
pub(crate) struct ApplyState<S: StateMachine> {
    pub(crate) follower: LogFollower,
    /// The user state machine, behind `Arc<Mutex<S>>`. `Arc` (shared, not owned)
    /// so the `Service` handle can reach it for direct queries (the test/embedded
    /// query path until the client query ring lands in Task 10/11); `Mutex`
    /// (not `RwLock`) so sharing needs only `S: Send` — the `StateMachine` bound
    /// is `Send + 'static`, with no `Sync`. Task 11's `drain_queries` runs on
    /// THIS same apply thread right after the applies, taking the lock the same
    /// way the apply path does (single-threaded, so the read/write distinction a
    /// `RwLock` would give buys nothing here).
    pub(crate) sm: Arc<Mutex<S>>,
    pub(crate) cnc: Arc<CncPage>,
    pub(crate) egress: Egress,
    /// The node's journal directory — the archived-log source the replay path
    /// reconstructs from on `Overrun`.
    pub(crate) journal_dir: PathBuf,
    /// The node→service query ring consumer half. Drained by Task 11's
    /// `drain_queries`; held here so the apply thread owns it (single reader).
    pub(crate) svc_query: SpscConsumer,
    /// Observability: set while a batch has surfaced `Overrun` and the replay
    /// reconstruction is degrading the follower back onto the live buffer.
    /// Cleared once replay rejoins.
    pub(crate) needs_replay: bool,
}

/// One apply duty cycle. Returns `true` iff it made progress (drove the idle
/// strategy). Follows the plan skeleton exactly:
/// target = `min(commit, durable)`; apply every committed `MESSAGE` up to it;
/// skip already-applied positions (idempotent re-entry) and non-`MESSAGE`
/// frames; publish responses only while leader. On `Overrun`, reconstruct via
/// journal replay and rejoin the live buffer.
pub(crate) fn apply_cycle<S: StateMachine>(st: &mut ApplyState<S>) -> bool {
    let c = st.cnc.counters();
    // Apply frontier = the lesser of quorum-commit and local durability. Both
    // acquire-loaded from the shared cnc page.
    let target = c.commit.load_acquire().min(c.durable.load_acquire());
    let mut progressed = false;
    loop {
        // is_leader read inline (a direct field access, not a `&self` method)
        // so it does not conflict with the `follower` borrow the batch holds.
        let is_leader =
            st.cnc.status().flags.load_acquire() & NODE_FLAG_LEADER != 0;
        let cursor_before = st.follower.cursor;
        // Resolve the batch to a plain enum before touching other fields, so the
        // mutable borrow of `st.follower` the batch holds ends before the
        // replay/publish arms mutate `st.follower.cursor`.
        let overrun = match st.follower.next_batch(target) {
            Batch::CaughtUp => break,
            // The live buffer scrolled past the cursor (or these bytes live only
            // in the journal after a restart prime) — degrade to replay below.
            Batch::Overrun => true,
            Batch::Frames(frames) => {
                let mut sm = st.sm.lock().unwrap();
                for (pos, hdr, payload) in frames {
                    // NEW_TERM (and any future non-MESSAGE type) is not user data.
                    if hdr.frame_type != FRAME_TYPE_MESSAGE {
                        continue;
                    }
                    // Idempotent re-entry: skip anything at or below what the SM
                    // has already applied (a restart replays from last_applied).
                    if Some(pos) <= sm.last_applied() {
                        continue;
                    }
                    // The ONE decode at the apply boundary. Committed bytes are
                    // trusted; a decode failure is unrecoverable corruption.
                    let (cmd, _) = bincode::serde::decode_from_slice::<S::Command, _>(
                        payload,
                        bincode::config::standard(),
                    )
                    .expect("corrupt committed frame (fail-stop)");
                    let resp = sm.apply(pos, cmd);
                    if is_leader {
                        st.egress.publish(hdr.session_id, hdr.correlation_id, pos, &resp);
                    }
                }
                drop(sm);
                // Publish the new applied frontier for barrier readers / clients.
                st.cnc.service().service_applied.store_release(st.follower.cursor);
                if st.follower.cursor == cursor_before {
                    // No frame cleared the target guard (target between the
                    // cursor and the next frame's end). Nothing more to do this
                    // cycle; avoids spinning on a partially-committed frame.
                    break;
                }
                progressed = true;
                false
            }
        };
        if overrun {
            // Task 9: reconstruct from the journal, then rejoin the live buffer
            // at the byte position replay reached. Livelock-free: each replay
            // pass strictly ADVANCES the cursor toward the archived frontier
            // captured at that pass's start (a monotonic byte position), so the
            // inner loop cannot spin in place — it either catches up (next
            // `next_batch` is `CaughtUp`/`Frames`) or, if the ring lapped the new
            // cursor while replay ran, degrades once more from a strictly higher
            // cursor. Forward progress every time.
            st.needs_replay = true;
            let cursor = replay_into(&st.sm, &st.cnc, &st.journal_dir)
                .expect("service journal replay fail-stop");
            st.follower.cursor = cursor;
            st.cnc.service().service_applied.store_release(cursor);
            st.needs_replay = false;
            progressed = true;
            // Re-loop: read live from the rejoin point (may CaughtUp, apply
            // more, or Overrun again if the ring lapped us during replay).
        }
    }
    // Liveness: a wall-clock heartbeat the node compares against its own clock.
    st.cnc.status().service_heartbeat_ns.store_release(unix_ns());
    drain_queries(st);
    progressed
}

/// Query bounded drain per apply cycle — the read-side analog of the apply
/// batch cap, so a burst of queries can never starve the apply loop.
const QUERY_DRAIN_PER_CYCLE: usize = 64;

/// Drain `svc_query.ring` (bounded): decode each read's `expected_epoch` prefix,
/// REFUSE any stamped for a superseded incarnation with `MSG_V2_RETRY`, and
/// answer the rest by querying the SM and publishing `MSG_V2_RESPONSE`
/// (`FLAG_V2_IS_QUERY`) onto the egress broadcast. Runs on the apply thread
/// right after the batch loop, taking the SM lock the same way `apply` does
/// (single-threaded — the read/write distinction a `RwLock` would give buys
/// nothing here).
///
/// Payload contract (`ipc.rs`): `expected_epoch: u64 LE ++ query bytes`.
/// `expected_epoch == 0` means "skip the check" — a snapshot read the node
/// forwarded unconditionally. Both this RETRY site and the barrier's are
/// PRE-query / side-effect-free: a query never mutates the SM (the
/// cross-task RETRY-is-side-effect-free invariant, Task 10 review).
fn drain_queries<S: StateMachine>(st: &mut ApplyState<S>) {
    // This incarnation's epoch (fixed at attach; a newer incarnation would have
    // bumped it — and would have joined this thread first, so a live mismatch
    // means the read was routed for a different incarnation than the one now
    // reading it: refuse it).
    let my_epoch = st.cnc.service().service_epoch.load_acquire();
    let mut buf = Vec::new();
    for _ in 0..QUERY_DRAIN_PER_CYCLE {
        match st.svc_query.try_read(&mut buf) {
            Ok(Some(rec)) => {
                // Payload = expected_epoch u64 LE ++ query bytes. A record too
                // short to hold the prefix is a protocol violation — drop it (a
                // query has no recovery contract; the client times out/retries).
                if buf.len() < 8 {
                    continue;
                }
                let expected_epoch = u64::from_le_bytes(buf[..8].try_into().unwrap());
                if expected_epoch != 0 && expected_epoch != my_epoch {
                    // Stale incarnation (task14 TOCTOU close): the read was routed
                    // for a superseded service epoch. Refuse with RETRY rather
                    // than answer with THIS incarnation's (different) state. The
                    // SM is NOT touched — side-effect-free.
                    st.egress.publish_retry(rec.header_extra);
                    continue;
                }
                let (q, _) = bincode::serde::decode_from_slice::<S::Query, _>(
                    &buf[8..],
                    bincode::config::standard(),
                )
                .expect("corrupt query frame (fail-stop)");
                let qr = st.sm.lock().unwrap().query(q);
                st.egress.publish_query_answer(rec.header_extra, &qr);
            }
            Ok(None) => break,
            // Corrupt record (bad crc/magic): stop this cycle; the next retries
            // at the same unread position.
            Err(_) => break,
        }
    }
}

fn unix_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}
