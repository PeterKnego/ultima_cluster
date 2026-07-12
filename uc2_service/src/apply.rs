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
    /// The node `instance_id` this incarnation attached to (M5 final review
    /// #2c). A change means the node restarted and recreated the cnc page in
    /// place — this attachment is invalidated and this thread must fail-stop
    /// rather than keep writing `service_applied`/`service_heartbeat_ns` onto the
    /// NEW generation's page (a single-writer violation, and the enabler of the
    /// epoch-0 barrier collision guarded node-side in #1).
    pub(crate) instance_id: u128,
    /// Consecutive-cycle counter for the instance-mismatch derace (#2c): a node
    /// recreate is truncate → set_len → rewrite-in-place, so a single cycle can
    /// catch a torn/stale header. Only TWO consecutive confirmed mismatches
    /// fail-stop; any match or torn (`None`) read resets it.
    pub(crate) instance_mismatch_streak: u8,
    /// This incarnation's service epoch, captured at attach (M5 final review
    /// #5). Fixed for the life of this service incarnation — a newer incarnation
    /// would bump `service_epoch` on the shared page, but THIS thread must keep
    /// comparing forwarded reads against ITS OWN epoch, not whatever the page now
    /// holds (re-reading live would make an old incarnation answer reads stamped
    /// for a newer one). #2c fail-stops on the node-restart case; this closes the
    /// same-node service-restart case.
    pub(crate) my_epoch: u64,
}

/// One apply duty cycle. Returns `true` iff it made progress (drove the idle
/// strategy). Follows the plan skeleton exactly:
/// target = `min(commit, durable)`; apply every committed `MESSAGE` up to it;
/// skip already-applied positions (idempotent re-entry) and non-`MESSAGE`
/// frames; publish responses only while leader. On `Overrun`, reconstruct via
/// journal replay and rejoin the live buffer.
pub(crate) fn apply_cycle<S: StateMachine>(st: &mut ApplyState<S>) -> bool {
    // Node-restart fail-stop (M5 final review #2c, plan decision #9): a node
    // restart recreates the cnc page in place with a fresh random `instance_id`,
    // invalidating every attachment. Detect it before doing any work so this
    // apply thread stops being a zombie writer of `service_applied`/heartbeats on
    // the NEW generation's page. Run once per duty cycle.
    check_node_instance(&st.cnc, st.instance_id, &mut st.instance_mismatch_streak);

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
    // This incarnation's epoch, CAPTURED AT ATTACH (M5 final review #5) — fixed
    // for this incarnation's life, NOT re-read live from the page. A newer
    // incarnation attaching to the same page bumps `service_epoch`; if we
    // re-read it live, an old (still-running) incarnation would start answering
    // reads stamped for the NEW epoch with ITS OWN (stale) state — a
    // linearizability hole. Comparing against the fixed attach-time epoch makes
    // any read not stamped for THIS incarnation fall through to RETRY. (#2c
    // fail-stops the node-restart case; this is the same-node service-restart
    // case, where `instance_id` is unchanged so #2c does not fire.)
    let my_epoch = st.my_epoch;
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

/// Fail-stop this service thread if the node it attached to has restarted (M5
/// final review #2c; shared by the apply and output loops).
///
/// A node restart recreates the cnc page IN PLACE with a fresh random
/// `instance_id` (the v2.0 contract: a restart invalidates every attachment;
/// there is no live re-attach — an external supervisor respawns the service).
/// The recreate is truncate → `set_len` → rewrite-in-place, so a probe can catch
/// a torn/zeroed header ([`CncPage::try_instance_id`] → `None`); we therefore
/// require TWO CONSECUTIVE confirmed mismatches before fail-stopping so a single
/// torn read cannot false-trip a live cluster. On confirmation we `panic!` — the
/// documented fail-stop: it unwinds this agent thread (stopping the zombie
/// writer), and the crashtest harness / a real process supervisor, which already
/// respawns the service on node restart, takes the process the rest of the way
/// down. A matching id (or a torn `None` read) resets the streak.
pub(crate) fn check_node_instance(cnc: &CncPage, attached: u128, streak: &mut u8) {
    match cnc.try_instance_id() {
        Some(id) if id != attached => {
            *streak += 1;
            if *streak >= 2 {
                panic!(
                    "uc2_service: node instance_id changed ({attached:#x} -> {id:#x}) — this \
                     attachment is invalidated by a node restart (v2.0 contract, plan decision \
                     #9). Fail-stop; the supervisor respawns the service."
                );
            }
        }
        // Matching id, or a torn/None read (node mid-recreate — re-probed next
        // cycle): not a confirmed change, so reset the consecutive-mismatch run.
        _ => *streak = 0,
    }
}

#[cfg(test)]
mod tests {
    use super::check_node_instance;
    use std::sync::Arc;
    use uc2_log::cnc::{CncMeta, CncPage};

    fn page(instance_id: u128) -> Arc<CncPage> {
        CncPage::heap(&CncMeta {
            node_id: 1,
            instance_id,
            app_id: "svc-test".into(),
            buffer_bytes: 1 << 20,
            max_payload: 256,
        })
    }

    // A matching instance_id is the steady state: it resets any transient
    // mismatch streak and never fail-stops.
    #[test]
    fn matching_instance_resets_streak_and_does_not_fail_stop() {
        let p = page(0xAAAA);
        let mut streak = 1; // a prior torn/transient bump
        check_node_instance(&p, 0xAAAA, &mut streak);
        assert_eq!(streak, 0, "a matching instance resets the streak");
    }

    // The derace: ONE mismatching cycle only arms the streak — a single torn
    // read during a node recreate must not fail-stop a live cluster.
    #[test]
    fn a_single_mismatch_arms_but_does_not_fire() {
        let p = page(0xBBBB); // page instance != attached
        let mut streak = 0;
        check_node_instance(&p, 0xAAAA, &mut streak);
        assert_eq!(streak, 1, "one mismatch arms but does not fire");
    }

    // TWO consecutive confirmed mismatches fail-stop (the node genuinely
    // restarted with a fresh instance_id) — the documented panic.
    #[test]
    #[should_panic(expected = "node instance_id changed")]
    fn two_consecutive_mismatches_fail_stop() {
        let p = page(0xBBBB);
        let mut streak = 0;
        check_node_instance(&p, 0xAAAA, &mut streak); // streak -> 1
        check_node_instance(&p, 0xAAAA, &mut streak); // streak -> 2 -> panic
    }
}
