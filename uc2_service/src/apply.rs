// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The apply agent duty cycle (spec §7). A single polling thread that follows
//! the committed log, applies each `MESSAGE` frame to the user's state machine,
//! and (while leader) publishes the response onto the egress broadcast.

use std::sync::Arc;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use uc2_log::cnc::CncPage;
use uc2_log::reader::{Batch, LogFollower};
use uc_protocol::ring::SpscConsumer;
use uc_protocol::v2::cnc::NODE_FLAG_LEADER;
use uc_protocol::v2::frame::FRAME_TYPE_MESSAGE;

use crate::egress::Egress;
use crate::traits::StateMachine;

/// Everything the apply thread owns. Fields are accessed directly (not through
/// `&self` methods) inside the frames loop so the borrow checker sees the
/// per-field disjoint borrows (`follower` iterated while `sm`/`egress`/`cnc`
/// are touched).
pub(crate) struct ApplyState<S: StateMachine> {
    pub(crate) follower: LogFollower,
    /// The user state machine. `RwLock` (not a bare `S`) so Task 11's
    /// `drain_queries` — which runs on THIS same apply thread, right after the
    /// applies — can take a read lock while the apply path takes the write
    /// lock; a bare `&mut`/`&` split would fight the borrow checker across the
    /// two phases. Owned (not `Arc`), because the SM never leaves this thread,
    /// so the `StateMachine: Send + 'static` bound (no `Sync`) is enough.
    pub(crate) sm: RwLock<S>,
    pub(crate) cnc: Arc<CncPage>,
    pub(crate) egress: Egress,
    /// The node→service query ring consumer half. Drained by Task 11's
    /// `drain_queries`; held here so the apply thread owns it (single reader).
    #[allow(dead_code)]
    pub(crate) svc_query: SpscConsumer,
    /// Set when a batch surfaced `Overrun` — Task 9 wires the journal-replay
    /// reconstruction that clears it. Here it only records the condition.
    pub(crate) needs_replay: bool,
}

/// One apply duty cycle. Returns `true` iff it made progress (drove the idle
/// strategy). Follows the plan skeleton exactly:
/// target = `min(commit, durable)`; apply every committed `MESSAGE` up to it;
/// skip already-applied positions (idempotent re-entry) and non-`MESSAGE`
/// frames; publish responses only while leader.
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
        match st.follower.next_batch(target) {
            Batch::CaughtUp => break,
            // Task 9 wires the replay reconstruction; here we record and stop.
            Batch::Overrun => {
                st.needs_replay = true;
                break;
            }
            Batch::Frames(frames) => {
                let mut sm = st.sm.write().unwrap();
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
            }
        }
    }
    // Liveness: a wall-clock heartbeat the node compares against its own clock.
    st.cnc.status().service_heartbeat_ns.store_release(unix_ns());
    drain_queries(st);
    progressed
}

/// Task 11 fills this in (drain `svc_query`, answer linearizable reads onto the
/// egress broadcast). No query producer is wired to `svc_query.ring` yet, so
/// this is a no-op stub.
fn drain_queries<S: StateMachine>(_st: &mut ApplyState<S>) {}

fn unix_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}
