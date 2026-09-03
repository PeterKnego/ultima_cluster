// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Per-row timer heap (time-and-timers spec §4.5). Kept on EVERY node from the
//! row's service `svc_sched` records; only the leader pops by time. No
//! persistence: the heap is a cache of what the service knows and converges
//! from the service's re-announce after a restart. At-least-once by design —
//! `rearm` after a leadership loss may fire an instance twice; `Timed<S>` on
//! the service side drops the duplicate.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::AtomicU64;

use uc_protocol::v2::cnc::CNC_MAX_SERVICES;

pub struct RowTimers {
    hash: u64,
    /// id → deadline of the pending instance (one per id).
    pending: HashMap<u64, u64>,
    /// (deadline, id), lazily deleted: an entry whose deadline no longer
    /// matches `pending[id]` is stale and skipped.
    heap: BinaryHeap<Reverse<(u64, u64)>>,
    /// Appended by this node as leader, not yet reported consumed.
    in_flight: HashMap<u64, u64>,
}

impl RowTimers {
    pub fn new(identity_hash: u64) -> Self {
        Self {
            hash: identity_hash,
            pending: HashMap::new(),
            heap: BinaryHeap::new(),
            in_flight: HashMap::new(),
        }
    }
    pub fn hash(&self) -> u64 {
        self.hash
    }
    pub fn schedule(&mut self, id: u64, deadline_ns: u64) {
        self.in_flight.remove(&id); // a newer instance supersedes an in-flight one
        self.pending.insert(id, deadline_ns);
        self.heap.push(Reverse((deadline_ns, id)));
    }
    pub fn cancel(&mut self, id: u64) {
        self.pending.remove(&id);
        self.in_flight.remove(&id);
    }
    pub fn consumed(&mut self, id: u64, deadline_ns: u64) {
        if self.in_flight.get(&id) == Some(&deadline_ns) {
            self.in_flight.remove(&id);
        }
        if self.pending.get(&id) == Some(&deadline_ns) {
            self.pending.remove(&id);
        }
    }
    /// Earliest pending instance with `deadline <= now_ns`, or `None`.
    pub fn peek_due(&mut self, now_ns: u64) -> Option<(u64, u64)> {
        while let Some(Reverse((dl, id))) = self.heap.peek().copied() {
            if self.pending.get(&id) != Some(&dl) {
                self.heap.pop(); // stale
                continue;
            }
            return if dl <= now_ns { Some((id, dl)) } else { None };
        }
        None
    }
    /// After the leader appended `(id, deadline)`: pending → in-flight.
    pub fn take_in_flight(&mut self, id: u64, deadline_ns: u64) {
        if self.pending.get(&id) == Some(&deadline_ns) {
            self.pending.remove(&id);
            let popped = self.heap.pop(); // it was the head `peek_due` returned
            debug_assert_eq!(popped, Some(Reverse((deadline_ns, id))));
            self.in_flight.insert(id, deadline_ns);
        }
    }
    /// Leadership lost: every in-flight instance is pending again.
    pub fn rearm(&mut self) -> usize {
        let n = self.in_flight.len();
        for (id, dl) in self.in_flight.drain() {
            self.pending.insert(id, dl);
            self.heap.push(Reverse((dl, id)));
        }
        n
    }
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }
    /// The companion to [`RowTimers::pending_len`]. Only the tests read it
    /// today — the cnc slot word and `/metrics` publish the PENDING count,
    /// which is the operator-meaningful half (an in-flight instance is
    /// already on the log).
    #[allow(dead_code)]
    pub fn in_flight_len(&self) -> usize {
        self.in_flight.len()
    }
}

/// Process-local counters `/metrics` renders per row (spec §6).
#[derive(Default)]
pub struct TimerStats {
    pub fired: [AtomicU64; CNC_MAX_SERVICES],
    pub late: [AtomicU64; CNC_MAX_SERVICES],
    pub rearmed: [AtomicU64; CNC_MAX_SERVICES],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_replace_cancel_and_due_order_across_ids() {
        let mut t = RowTimers::new(0xabc);
        t.schedule(1, 500);
        t.schedule(2, 300);
        t.schedule(1, 400); // replace
        assert_eq!(t.pending_len(), 2);
        assert_eq!(t.peek_due(299), None);
        assert_eq!(t.peek_due(300), Some((2, 300)));
        t.cancel(2);
        assert_eq!(
            t.peek_due(1_000),
            Some((1, 400)),
            "stale heap entry for (2,300) and (1,500) skipped"
        );
        t.take_in_flight(1, 400);
        assert_eq!(t.peek_due(1_000), None);
        assert_eq!((t.pending_len(), t.in_flight_len()), (0, 1));
    }

    #[test]
    fn consumed_clears_in_flight_on_the_leader_and_pending_on_a_follower() {
        let mut leader = RowTimers::new(1);
        leader.schedule(9, 100);
        assert_eq!(leader.peek_due(100), Some((9, 100)));
        leader.take_in_flight(9, 100);
        leader.consumed(9, 100);
        assert_eq!((leader.pending_len(), leader.in_flight_len()), (0, 0));

        let mut follower = RowTimers::new(1);
        follower.schedule(9, 100);
        follower.consumed(9, 100); // never fired here; the log delivered it
        assert_eq!(follower.pending_len(), 0);
        assert_eq!(follower.peek_due(u64::MAX), None);

        let mut stale = RowTimers::new(1);
        stale.schedule(9, 200); // re-scheduled after the fire the consumed refers to
        stale.consumed(9, 100);
        assert_eq!(
            stale.pending_len(),
            1,
            "a consumed for an older instance leaves the new one"
        );
    }

    #[test]
    fn rearm_moves_in_flight_back_and_they_fire_again() {
        let mut t = RowTimers::new(1);
        t.schedule(4, 50);
        t.schedule(5, 60);
        t.take_in_flight(4, 50);
        t.take_in_flight(5, 60);
        assert_eq!(t.rearm(), 2);
        assert_eq!(t.peek_due(100), Some((4, 50)));
        t.take_in_flight(4, 50);
        assert_eq!(t.peek_due(100), Some((5, 60)));
        assert_eq!(t.rearm(), 1, "only the still in-flight one");
    }

    #[test]
    fn reschedule_of_an_in_flight_id_supersedes_it() {
        let mut t = RowTimers::new(1);
        t.schedule(7, 10);
        t.take_in_flight(7, 10);
        t.schedule(7, 20); // the FSM re-armed it from on_timer before consumed arrived
        assert_eq!(
            t.in_flight_len(),
            0,
            "the old instance can no longer be re-armed"
        );
        assert_eq!(t.peek_due(20), Some((7, 20)));
    }
}
