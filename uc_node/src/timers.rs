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
use uc_protocol::v2::schedule::ScheduleRule;

/// A table entry (spec §5, plan 2): unlike a programmatic `schedule`d
/// instance, a table entry never leaves on fire — it advances to its next
/// occurrence and stays keyed by `id` in `RowTimers::table`. `next == None`
/// means parked: a `Once` rule already delivered, holding no deadline and
/// not counted as pending.
struct TableEntry {
    rule: ScheduleRule,
    next: Option<u64>,
    last_delivered: Option<u64>,
}

pub struct RowTimers {
    hash: u64,
    /// id → deadline of the pending instance (one per id).
    pending: HashMap<u64, u64>,
    /// id → table entry (spec §5, plan 2). Same id space as `pending`, but a
    /// disjoint kind: the heap's `bool` tag tells them apart.
    table: HashMap<u64, TableEntry>,
    /// (deadline, id, is_table_entry), lazily deleted: a programmatic entry
    /// is stale when its deadline no longer matches `pending[id]`; a table
    /// entry is stale when it no longer matches `table[id].next`. The `bool`
    /// keeps the two kinds from colliding on the same `(deadline, id)`.
    heap: BinaryHeap<Reverse<(u64, u64, bool)>>,
    /// Appended by this node as leader, not yet reported consumed.
    in_flight: HashMap<u64, u64>,
}

impl RowTimers {
    pub fn new(identity_hash: u64) -> Self {
        Self {
            hash: identity_hash,
            pending: HashMap::new(),
            table: HashMap::new(),
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
        self.heap.push(Reverse((deadline_ns, id, false)));
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
    /// Replaces this row's table wholesale: an id not in `entries` is
    /// dropped (its heap entries lazily discarded on the next `peek_due`); a
    /// kept id keeps its `last_delivered`. Every entry's `next` is
    /// (re)computed via `rule.arm(last_delivered, log_time_ns)` (spec §5
    /// one-tick catch-up) and pushed if `Some`.
    pub fn adopt_table(&mut self, entries: &[(u64, ScheduleRule)], log_time_ns: u64) {
        let mut new_table = HashMap::with_capacity(entries.len());
        for &(id, rule) in entries {
            let last_delivered = self.table.get(&id).and_then(|e| e.last_delivered);
            let next = rule.arm(last_delivered, log_time_ns);
            if let Some(n) = next {
                self.heap.push(Reverse((n, id, true)));
            }
            new_table.insert(
                id,
                TableEntry {
                    rule,
                    next,
                    last_delivered,
                },
            );
        }
        self.table = new_table;
    }
    /// The deadline a DUE table head must actually be fired at (spec §5's
    /// one-tick catch-up, enforced at FIRE time): the LATEST occurrence at or
    /// before `now_ns`, never below the armed `next`. `None` when the entry
    /// is absent or parked.
    ///
    /// Why here and not at arming: a node arms from the log's clock, which
    /// after a restart is the clock as of the last recorded frame and can be
    /// hours behind the new leader's wall clock. Advancing one period per
    /// fire from that point would replay the whole downtime
    /// (`fired + period`, then again, then again) — the backlog spec §5
    /// promises never happens. Which occurrence is due is the one
    /// clock-driven choice the determinism rule allows, and the chosen
    /// deadline rides the TIMER frame, so every replica advances from the
    /// same value.
    pub fn table_fire_deadline(&self, id: u64, now_ns: u64) -> Option<u64> {
        let e = self.table.get(&id)?;
        let n = e.next?;
        Some(n.max(e.rule.latest_at_or_before(now_ns).unwrap_or(n)))
    }
    /// Leader, after a successful append of a table tick: advances to the
    /// next occurrence, or parks (`next = None`) when nothing follows (a
    /// `Once` already fired). Never touches `in_flight` — a table tick is
    /// never in flight.
    ///
    /// `deadline_ns` is the deadline that was APPENDED, which
    /// [`RowTimers::table_fire_deadline`] may have moved forward past the
    /// armed `next`. Hence the `<=` guard rather than an equality: anything
    /// at or after what we hold is a fire of this instance, and the next
    /// occurrence is computed from the deadline that actually rode the frame.
    pub fn table_fired(&mut self, id: u64, deadline_ns: u64) {
        if let Some(e) = self.table.get_mut(&id)
            && e.next.is_some_and(|n| n <= deadline_ns)
        {
            e.next = e.rule.next_after(deadline_ns);
            if let Some(n) = e.next {
                self.heap.push(Reverse((n, id, true)));
            }
        }
    }
    /// From the service's `TableConsumed` (follower path, and the
    /// post-attach announce): raises `last_delivered` monotonically, and
    /// advances `next` past `deadline_ns` if it hadn't already.
    pub fn table_delivered(&mut self, id: u64, deadline_ns: u64) {
        if let Some(e) = self.table.get_mut(&id) {
            e.last_delivered = Some(e.last_delivered.map_or(deadline_ns, |l| l.max(deadline_ns)));
            if e.next.is_none_or(|n| n <= deadline_ns) {
                e.next = e.rule.next_after(deadline_ns);
                if let Some(n) = e.next {
                    self.heap.push(Reverse((n, id, true)));
                }
            }
        }
    }
    /// Earliest due instance across both kinds with `deadline <= now_ns`, or
    /// `None`. The third field says whether the head is a table entry.
    pub fn peek_due(&mut self, now_ns: u64) -> Option<(u64, u64, bool)> {
        while let Some(Reverse((dl, id, table))) = self.heap.peek().copied() {
            let current = if table {
                self.table.get(&id).and_then(|e| e.next)
            } else {
                self.pending.get(&id).copied()
            };
            if current != Some(dl) {
                self.heap.pop(); // stale
                continue;
            }
            return if dl <= now_ns {
                Some((id, dl, table))
            } else {
                None
            };
        }
        None
    }
    /// After the leader appended `(id, deadline)`: pending → in-flight.
    /// Must NOT be called for a table head — a table entry is never
    /// in-flight; `table_fired` is its equivalent.
    pub fn take_in_flight(&mut self, id: u64, deadline_ns: u64) {
        if self.pending.get(&id) == Some(&deadline_ns) {
            self.pending.remove(&id);
            let popped = self.heap.pop(); // it was the head `peek_due` returned
            debug_assert_eq!(popped, Some(Reverse((deadline_ns, id, false))));
            self.in_flight.insert(id, deadline_ns);
        }
    }
    /// Leadership lost: every in-flight instance is pending again. Table
    /// entries are never in `in_flight`, so this ignores them by
    /// construction.
    pub fn rearm(&mut self) -> usize {
        let n = self.in_flight.len();
        for (id, dl) in self.in_flight.drain() {
            self.pending.insert(id, dl);
            self.heap.push(Reverse((dl, id, false)));
        }
        n
    }
    /// Programmatic + table entries that currently hold a deadline (a
    /// parked `Once` does not count).
    pub fn pending_len(&self) -> usize {
        self.pending.len() + self.table.values().filter(|e| e.next.is_some()).count()
    }
    /// Every table entry, parked ones included.
    pub fn table_len(&self) -> usize {
        self.table.len()
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
        assert_eq!(t.peek_due(300), Some((2, 300, false)));
        t.cancel(2);
        assert_eq!(
            t.peek_due(1_000),
            Some((1, 400, false)),
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
        assert_eq!(leader.peek_due(100), Some((9, 100, false)));
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
        assert_eq!(t.peek_due(100), Some((4, 50, false)));
        t.take_in_flight(4, 50);
        assert_eq!(t.peek_due(100), Some((5, 60, false)));
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
        assert_eq!(t.peek_due(20), Some((7, 20, false)));
    }

    #[test]
    fn table_entries_advance_on_fire_and_never_enter_in_flight() {
        use uc_protocol::v2::schedule::ScheduleRule;
        let mut t = RowTimers::new(1);
        let r = ScheduleRule::Every {
            period_ns: 100,
            anchor_ns: 1_000,
        };
        t.adopt_table(&[(7, r)], 1_250); // log clock 1_250: latest missed occurrence 1_200 (one-tick catch-up)
        assert_eq!(t.peek_due(1_199), None);
        assert_eq!(t.peek_due(1_200), Some((7, 1_200, true)));
        t.table_fired(7, 1_200);
        assert_eq!(t.in_flight_len(), 0, "table ticks are never in flight");
        assert_eq!(t.peek_due(1_299), None);
        assert_eq!(
            t.peek_due(1_300),
            Some((7, 1_300, true)),
            "advanced from the fired deadline, not the clock"
        );
        assert_eq!(t.rearm(), 0);
        assert_eq!(t.table_len(), 1);
    }

    /// Spec §5's one-tick catch-up, enforced at FIRE time (plan-2 fix round
    /// 1): an entry armed against a log clock that is far behind the leader's
    /// fires the LATEST due occurrence, not the head of a backlog, and then
    /// advances from THAT.
    #[test]
    fn a_due_table_head_fires_at_the_latest_occurrence_not_the_backlog() {
        use uc_protocol::v2::schedule::ScheduleRule;
        let mut t = RowTimers::new(1);
        let r = ScheduleRule::Every {
            period_ns: 100,
            anchor_ns: 1_000,
        };
        t.adopt_table(&[(7, r)], 1_250);
        assert_eq!(t.peek_due(u64::MAX), Some((7, 1_200, true)));
        assert_eq!(
            t.table_fire_deadline(7, 1_950),
            Some(1_900),
            "the latest occurrence at or before the pass clock"
        );
        assert_eq!(
            t.table_fire_deadline(7, 1_150),
            Some(1_200),
            "never below the armed next"
        );
        t.table_fired(7, 1_900);
        assert_eq!(
            t.peek_due(u64::MAX),
            Some((7, 2_000, true)),
            "advanced from the deadline that was appended, not from 1_200"
        );
        // A parked `once` has no fire deadline, and neither has an id the
        // table does not hold.
        let mut o = RowTimers::new(1);
        o.adopt_table(&[(3, ScheduleRule::Once { at_ns: 500 })], 0);
        assert_eq!(o.table_fire_deadline(3, 9_999), Some(500));
        o.table_fired(3, 500);
        assert_eq!(o.table_fire_deadline(3, 9_999), None, "parked");
        assert_eq!(o.table_fire_deadline(99, 9_999), None, "absent id");
    }

    #[test]
    fn table_delivered_advances_a_follower_and_adopt_keeps_last_delivered() {
        use uc_protocol::v2::schedule::ScheduleRule;
        let r = ScheduleRule::Every {
            period_ns: 100,
            anchor_ns: 0,
        };
        let mut f = RowTimers::new(1);
        f.adopt_table(&[(7, r)], 0);
        assert_eq!(
            f.peek_due(u64::MAX),
            Some((7, 0, true)),
            "first occurrence at the anchor"
        );
        f.table_delivered(7, 300); // the log delivered ticks up to 300
        assert_eq!(f.peek_due(u64::MAX), Some((7, 400, true)));
        f.table_delivered(7, 100); // an old report never moves it back
        assert_eq!(f.peek_due(u64::MAX), Some((7, 400, true)));
        // re-adoption of the same id keeps last_delivered and re-arms from the clock
        f.adopt_table(&[(7, r), (8, r)], 950);
        assert_eq!(
            f.peek_due(u64::MAX),
            Some((7, 900, true)),
            "one-tick catch-up above last_delivered=300"
        );
        assert_eq!(f.table_len(), 2);
        // an id dropped from the table disappears
        f.adopt_table(&[(8, r)], 950);
        f.table_fired(8, 900);
        assert_eq!(f.peek_due(u64::MAX), Some((8, 1_000, true)));
        assert_eq!(f.table_len(), 1);
    }

    #[test]
    fn programmatic_and_table_share_the_heap_in_deadline_order() {
        use uc_protocol::v2::schedule::ScheduleRule;
        let mut t = RowTimers::new(1);
        t.schedule(1, 500);
        t.adopt_table(
            &[(
                1,
                ScheduleRule::Every {
                    period_ns: 1_000,
                    anchor_ns: 400,
                },
            )],
            0,
        ); // same id 1: distinct kinds
        assert_eq!(t.peek_due(1_000), Some((1, 400, true)));
        t.table_fired(1, 400);
        assert_eq!(t.peek_due(1_000), Some((1, 500, false)));
        t.take_in_flight(1, 500);
        assert_eq!(t.peek_due(1_000), None);
        assert_eq!(
            t.pending_len(),
            1,
            "the table entry (next 1_400) still counts as pending"
        );
    }

    #[test]
    fn once_entries_fire_once_park_and_survive_re_adoption() {
        use uc_protocol::v2::schedule::ScheduleRule;
        let mut t = RowTimers::new(1);
        let r = ScheduleRule::Once { at_ns: 500 };
        t.adopt_table(&[(3, r)], 0);
        assert_eq!(t.pending_len(), 1);
        assert_eq!(t.peek_due(499), None);
        assert_eq!(t.peek_due(500), Some((3, 500, true)));
        t.table_fired(3, 500);
        assert_eq!(t.peek_due(u64::MAX), None, "nothing follows a once");
        assert_eq!(t.pending_len(), 0, "a parked once is not pending");
        assert_eq!(t.table_len(), 1, "but it is still in the table");
        t.table_delivered(3, 500); // the service reported it (leader or follower)
        t.adopt_table(&[(3, r)], 9_000);
        assert_eq!(
            t.peek_due(u64::MAX),
            None,
            "re-applying the same once does not re-fire a delivered one"
        );
        t.adopt_table(&[(3, ScheduleRule::Once { at_ns: 7_000 })], 9_000);
        assert_eq!(
            t.peek_due(u64::MAX),
            Some((3, 7_000, true)),
            "a newer deadline for the same id fires (late, once)"
        );
    }
}
