// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Snapshot-lifecycle spec §6–§7: the background AUTO-FETCH decision. Pure —
//! no I/O, no clock, no locks; the consensus agent feeds it the newest agreed
//! set, its durable frontier, the free-space reading, a candidate list and
//! the pass clock, and issues the existing store-only fetch when it says so.
//! One fetch at a time per node (`PendingFetch` is the node's), chasing the
//! newest agreed set only.

use std::sync::atomic::{AtomicU64, Ordering};

use uc_consensus::election::NodeId;

/// Spec §6: the first attempt for a given set waits `node_id × 250 ms`.
pub const AUTO_FETCH_STAGGER_NS: u64 = 250_000_000;
/// Spec §6: the retry ladder — 1 s, doubling, to 30 s.
pub const AUTO_FETCH_BACKOFF_MIN_NS: u64 = 1_000_000_000;
pub const AUTO_FETCH_BACKOFF_MAX_NS: u64 = 30_000_000_000;
/// Plan ruling P9: how often a WAITING decision (set above durable, or still
/// being built here) is looked at again — not every pass.
pub const AUTO_FETCH_RECHECK_NS: u64 = 100_000_000;
/// Controller ruling PF7: how long a set this node is still BUILDING holds
/// the fetch, measured from the first sighting of that set. A row that never
/// freezes (a wedged or slow builder) must not park auto-fetch forever.
pub const AUTO_FETCH_BUILD_GUARD_NS: u64 = 30_000_000_000;
/// Spec §7.3: the headroom floor, a fixed default (not a setting).
pub const FETCH_HEADROOM_MIN_BYTES: u64 = 1 << 30;

/// Spec §7.3: `free_bytes >= total + max(total / 4, 1 GiB)`. Saturating: a
/// total near `u64::MAX` never fits.
pub fn fits(free_bytes: u64, total: u64) -> bool {
    free_bytes >= total.saturating_add((total / 4).max(FETCH_HEADROOM_MIN_BYTES))
}

/// Final review M6: the `uc2-holdings` probe's free-bytes figure before its
/// first successful `statvfs` (or after every one has failed) — UNKNOWN, not
/// "no space". The probe stores a real reading as at least 1, so 0 is never a
/// measurement.
pub const FREE_BYTES_UNKNOWN: u64 = 0;

/// Spec §7.3/§7.4 and plan ruling PF11: the one "won't fit" predicate the
/// space check and `uc2_snapshot_wont_fit` share. An unknown size or an
/// unknown free figure ([`FREE_BYTES_UNKNOWN`]) never reads as won't fit.
pub fn wont_fit(free_bytes: u64, total: u64) -> bool {
    total != 0 && free_bytes != FREE_BYTES_UNKNOWN && !fits(free_bytes, total)
}

/// Spec §6: `uc2_snapshot_auto_fetch_total{outcome}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    Refused,
    Timeout,
    NoSpace,
    NoHolder,
}

impl Outcome {
    pub const ALL: [Outcome; 5] = [
        Outcome::Ok,
        Outcome::Refused,
        Outcome::Timeout,
        Outcome::NoSpace,
        Outcome::NoHolder,
    ];
    pub const fn label(self) -> &'static str {
        match self {
            Outcome::Ok => "ok",
            Outcome::Refused => "refused",
            Outcome::Timeout => "timeout",
            Outcome::NoSpace => "no_space",
            Outcome::NoHolder => "no_holder",
        }
    }
}

/// The counter family's storage — the consensus agent bumps it, `/metrics`
/// reads it at scrape (Relaxed both ways: a counter, not a gate).
#[derive(Debug, Default)]
pub struct AutoFetchStats {
    counts: [AtomicU64; 5],
}

impl AutoFetchStats {
    pub fn bump(&self, o: Outcome) {
        self.counts[o as usize].fetch_add(1, Ordering::Relaxed);
    }
    pub fn get(&self, o: Outcome) -> u64 {
        self.counts[o as usize].load(Ordering::Relaxed)
    }
}

/// Spec §7.3: the answer of [`AutoFetch::check_space`]. `first` is `true`
/// the first time a given set gets that answer — the caller names it once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpaceCheck {
    Fits,
    /// Final review M6: the free figure is not known yet (or `statvfs`
    /// fails) — fetched without the check, like an unknown size, and
    /// silently: a boot-time race must not spend the set's one `no_space`
    /// warning on a false reading.
    FreeUnknown,
    Unknown {
        first: bool,
    },
    NoSpace {
        first: bool,
    },
}

/// One node's auto-fetch decision state.
#[derive(Debug)]
pub struct AutoFetch {
    node_id: NodeId,
    /// The newest agreed set being chased (`0` = none yet).
    target: u64,
    /// Pass-clock ns at which `target` was first seen — the start of the
    /// PF7 build guard.
    first_seen_ns: u64,
    /// Pass-clock ns before which nothing is attempted for `target`.
    next_attempt_ns: u64,
    /// The current rung of the 1 s → 30 s ladder (`0` = not backing off).
    backoff_ns: u64,
    /// Controller ruling R8 (plan ruling P15): pass-clock ns before which NO
    /// fetch is issued — `now + 1 s` on every fetch timeout this node
    /// observes, auto or operator. Survives a retarget: a new fetch clears
    /// the receiver's parked expired-fetch slot, so the floor is not the
    /// target's but the node's.
    not_before_ns: u64,
    /// Controller ruling R7: `target` was fetched `ok` — never re-issued for
    /// it in this incarnation, whatever `holdings_held` says.
    fetched_ok: bool,
    /// Candidates already tried for `target`, in order (plan ruling P10).
    tried: Vec<NodeId>,
    no_space_named: bool,
    size_unknown_named: bool,
    thin_named: bool,
}

impl AutoFetch {
    pub fn new(node_id: NodeId) -> Self {
        AutoFetch {
            node_id,
            target: 0,
            first_seen_ns: 0,
            next_attempt_ns: 0,
            backoff_ns: 0,
            not_before_ns: 0,
            fetched_ok: false,
            tried: Vec::new(),
            no_space_named: false,
            size_unknown_named: false,
            thin_named: false,
        }
    }

    pub fn target(&self) -> u64 {
        self.target
    }

    /// The steady-pass test: nothing to do for `n` before the next attempt.
    #[inline]
    pub fn quiet(&self, n: u64, now_ns: u64) -> bool {
        n == self.target && now_ns < self.next_attempt_ns
    }

    /// Spec §6: is an attempt for the newest agreed set `n` due now? A new
    /// `n` resets the chase (tried list, ladder, once-per-set names) and
    /// starts the stagger. Waiting — `n` above `durable`, or still being
    /// built here (for at most [`AUTO_FETCH_BUILD_GUARD_NS`] from the first
    /// sighting of `n`, controller ruling PF7) — re-checks after
    /// [`AUTO_FETCH_RECHECK_NS`].
    pub fn due(&mut self, n: u64, durable: u64, building: bool, now_ns: u64) -> bool {
        if n != self.target {
            self.target = n;
            self.first_seen_ns = now_ns;
            self.tried.clear();
            self.backoff_ns = 0;
            self.no_space_named = false;
            self.size_unknown_named = false;
            self.thin_named = false;
            self.fetched_ok = false;
            self.next_attempt_ns = now_ns
                .saturating_add(u64::from(self.node_id) * AUTO_FETCH_STAGGER_NS)
                .max(self.not_before_ns);
        }
        if self.fetched_ok || now_ns < self.next_attempt_ns {
            return false;
        }
        let guarded =
            building && now_ns.saturating_sub(self.first_seen_ns) < AUTO_FETCH_BUILD_GUARD_NS;
        if n > durable || guarded {
            self.next_attempt_ns = now_ns.saturating_add(AUTO_FETCH_RECHECK_NS);
            return false;
        }
        true
    }

    /// Spec §7.3 before every attempt. A set of unknown size (`total == 0`)
    /// is fetched without the check (review focus 4); a set that does not fit
    /// backs off (plan ruling P11).
    pub fn check_space(&mut self, total: u64, free_bytes: u64, now_ns: u64) -> SpaceCheck {
        if total == 0 {
            let first = !self.size_unknown_named;
            self.size_unknown_named = true;
            return SpaceCheck::Unknown { first };
        }
        if free_bytes == FREE_BYTES_UNKNOWN {
            return SpaceCheck::FreeUnknown;
        }
        if !wont_fit(free_bytes, total) {
            return SpaceCheck::Fits;
        }
        let first = !self.no_space_named;
        self.no_space_named = true;
        self.back_off(now_ns);
        SpaceCheck::NoSpace { first }
    }

    /// The first candidate not yet tried for the target; `None` when every
    /// one has been (or there are none) — `no_holder`: the list starts over
    /// after the 30 s ceiling (plan ruling P10).
    pub fn pick(&mut self, candidates: &[NodeId], now_ns: u64) -> Option<NodeId> {
        match candidates.iter().copied().find(|c| !self.tried.contains(c)) {
            Some(c) => {
                self.tried.push(c);
                Some(c)
            }
            None => {
                self.tried.clear();
                self.backoff_ns = AUTO_FETCH_BACKOFF_MAX_NS;
                self.next_attempt_ns = now_ns.saturating_add(AUTO_FETCH_BACKOFF_MAX_NS);
                None
            }
        }
    }

    /// Spec §6: `ok` resets the ladder and ends the chase for this target —
    /// controller ruling R7: never re-issued for the same target, so the
    /// steady pass reads [`Self::quiet`] until a newer set is agreed (this
    /// subsumes PF8's recheck pause for the same target). `refused` backs
    /// off; `timeout` backs off and sets the node's floor
    /// ([`Self::note_timeout`]). The next attempt picks the next untried
    /// candidate.
    pub fn on_result(&mut self, o: Outcome, now_ns: u64) {
        match o {
            Outcome::Ok => {
                self.backoff_ns = 0;
                self.tried.clear();
                self.fetched_ok = true;
                self.next_attempt_ns = u64::MAX;
            }
            Outcome::Refused => self.back_off(now_ns),
            Outcome::Timeout => {
                self.back_off(now_ns);
                self.note_timeout(now_ns);
            }
            Outcome::NoSpace | Outcome::NoHolder => {}
        }
    }

    /// Controller ruling R8 (plan ruling P15): a fetch — auto OR operator —
    /// timed out at `now_ns`: issue nothing for [`AUTO_FETCH_BACKOFF_MIN_NS`],
    /// for this target and any newer one.
    pub fn note_timeout(&mut self, now_ns: u64) {
        self.not_before_ns = now_ns.saturating_add(AUTO_FETCH_BACKOFF_MIN_NS);
        self.next_attempt_ns = self.next_attempt_ns.max(self.not_before_ns);
    }

    /// Plan ruling P12: `true` the first time per set.
    pub fn first_thin(&mut self) -> bool {
        let first = !self.thin_named;
        self.thin_named = true;
        first
    }

    fn back_off(&mut self, now_ns: u64) {
        self.backoff_ns = if self.backoff_ns == 0 {
            AUTO_FETCH_BACKOFF_MIN_NS
        } else {
            (self.backoff_ns * 2).min(AUTO_FETCH_BACKOFF_MAX_NS)
        };
        self.next_attempt_ns = now_ns.saturating_add(self.backoff_ns);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    /// Spec §7.3: `free >= total + max(total / 4, 1 GiB)`, exactly.
    #[test]
    fn fits_is_exact_at_its_threshold() {
        assert!(
            fits(5 * GIB, 4 * GIB),
            "4 GiB needs 1 GiB headroom: 5 GiB fits"
        );
        assert!(!fits(5 * GIB - 1, 4 * GIB), "one byte short");
        assert!(
            fits(10 * GIB, 8 * GIB),
            "8 GiB: headroom is total/4 = 2 GiB"
        );
        assert!(!fits(10 * GIB - 1, 8 * GIB));
        assert!(
            fits(100 + GIB, 100),
            "a small set still needs the 1 GiB floor"
        );
        assert!(!fits(100 + GIB - 1, 100));
        assert!(!fits(u64::MAX - 1, u64::MAX), "saturates, never wraps");
    }

    /// Spec §6: the first attempt for a given N waits node_id × 250 ms.
    #[test]
    fn the_first_attempt_is_staggered_by_node_id() {
        let mut a = AutoFetch::new(3);
        assert!(!a.due(1000, 2000, false, 0));
        assert!(!a.due(1000, 2000, false, 749_999_999));
        assert!(a.due(1000, 2000, false, 750_000_000));
        let mut z = AutoFetch::new(0);
        assert!(z.due(1000, 2000, false, 0), "node 0 does not wait");
    }

    /// Spec §6 trigger: a set ahead of this node's durable frontier waits,
    /// and so does one this node is still building (plan ruling P8); the
    /// wait is re-checked every AUTO_FETCH_RECHECK_NS, not every pass (P9).
    #[test]
    fn a_set_above_durable_or_still_being_built_waits() {
        let mut a = AutoFetch::new(0);
        assert!(!a.due(1000, 999, false, 0), "above durable");
        assert!(
            a.quiet(1000, AUTO_FETCH_RECHECK_NS - 1),
            "re-checked only after the recheck delay"
        );
        assert!(
            !a.due(1000, 2000, true, AUTO_FETCH_RECHECK_NS),
            "still building"
        );
        assert!(a.due(1000, 2000, false, 2 * AUTO_FETCH_RECHECK_NS));
    }

    /// Controller ruling PF7: a local build holds the fetch for at most
    /// AUTO_FETCH_BUILD_GUARD_NS from the first sighting of N; after that the
    /// fetch proceeds even though a row still reads as building (a row that
    /// never freezes must not park auto-fetch forever).
    #[test]
    fn a_local_build_holds_the_fetch_for_at_most_the_build_guard() {
        let mut a = AutoFetch::new(0);
        assert!(!a.due(1000, 2000, true, 0), "building: held");
        assert!(
            !a.due(
                1000,
                2000,
                true,
                AUTO_FETCH_BUILD_GUARD_NS - AUTO_FETCH_RECHECK_NS
            ),
            "still inside the guard"
        );
        assert!(
            a.due(1000, 2000, true, AUTO_FETCH_BUILD_GUARD_NS),
            "the guard has expired: the fetch proceeds"
        );
        // A newer N restarts the guard from its own first sighting.
        let t = AUTO_FETCH_BUILD_GUARD_NS + 1;
        assert!(!a.due(2000, 3000, true, t), "a new N is held again");
    }

    /// Spec §6: a refusal or timeout moves to the NEXT holder, backing off
    /// 1 s, 2 s, 4 s …; when every candidate has been tried the outcome is
    /// no_holder and the next attempt waits the 30 s ceiling (plan ruling P10).
    #[test]
    fn a_refusal_or_timeout_moves_to_the_next_holder_with_doubling_backoff() {
        let mut a = AutoFetch::new(0);
        let c = [7, 8, 9];
        let mut t = 0;
        assert!(a.due(1000, 2000, false, t));
        assert_eq!(a.pick(&c, t), Some(7));
        a.on_result(Outcome::Timeout, t);
        assert!(!a.due(1000, 2000, false, t + AUTO_FETCH_BACKOFF_MIN_NS - 1));
        t += AUTO_FETCH_BACKOFF_MIN_NS;
        assert!(a.due(1000, 2000, false, t));
        assert_eq!(a.pick(&c, t), Some(8));
        a.on_result(Outcome::Refused, t);
        assert!(!a.due(1000, 2000, false, t + 2 * AUTO_FETCH_BACKOFF_MIN_NS - 1));
        t += 2 * AUTO_FETCH_BACKOFF_MIN_NS;
        assert!(a.due(1000, 2000, false, t));
        assert_eq!(a.pick(&c, t), Some(9));
        a.on_result(Outcome::Timeout, t);
        t += 4 * AUTO_FETCH_BACKOFF_MIN_NS;
        assert!(a.due(1000, 2000, false, t));
        assert_eq!(a.pick(&c, t), None, "every candidate tried: no_holder");
        assert!(!a.due(1000, 2000, false, t + AUTO_FETCH_BACKOFF_MAX_NS - 1));
        assert!(a.due(1000, 2000, false, t + AUTO_FETCH_BACKOFF_MAX_NS));
        assert_eq!(
            a.pick(&c, t + AUTO_FETCH_BACKOFF_MAX_NS),
            Some(7),
            "the list starts over"
        );
    }

    /// Review focus 3: a holder whose soft entry is stale but still listed
    /// FIRST is not retried before every other candidate has had its turn.
    #[test]
    fn a_timed_out_holder_is_not_tried_again_before_every_other_candidate() {
        let mut a = AutoFetch::new(0);
        assert!(a.due(1000, 2000, false, 0));
        assert_eq!(a.pick(&[5, 6], 0), Some(5));
        a.on_result(Outcome::Timeout, 0);
        assert!(a.due(1000, 2000, false, AUTO_FETCH_BACKOFF_MIN_NS));
        assert_eq!(
            a.pick(&[5, 6], AUTO_FETCH_BACKOFF_MIN_NS),
            Some(6),
            "5 is still listed first; 6 goes next"
        );
    }

    /// Review focus 3: a lone dead holder costs one timeout, then a 30 s
    /// ceiling — never a back-to-back series of 60 s timeouts.
    #[test]
    fn a_lone_dead_holder_costs_one_timeout_per_backoff_ceiling() {
        let mut a = AutoFetch::new(0);
        assert!(a.due(1000, 2000, false, 0));
        assert_eq!(a.pick(&[5], 0), Some(5));
        a.on_result(Outcome::Timeout, 60_000_000_000);
        let t = 60_000_000_000 + AUTO_FETCH_BACKOFF_MIN_NS;
        assert!(a.due(1000, 2000, false, t));
        assert_eq!(a.pick(&[5], t), None, "not 5 again straight away");
        assert!(a.quiet(1000, t + AUTO_FETCH_BACKOFF_MAX_NS - 1));
    }

    /// Spec §6 "only the newest": a newer agreed set moves the target, clears
    /// the tried list and restarts the stagger.
    #[test]
    fn chasing_the_newest_resets_the_tried_list() {
        let mut a = AutoFetch::new(0);
        assert!(a.due(1000, 9000, false, 0));
        assert_eq!(a.pick(&[5], 0), Some(5));
        a.on_result(Outcome::Timeout, 0);
        // Controller ruling R8: a retarget does not jump the 1 s floor a
        // timeout set (plan ruling P15) — the stagger is max'd with it.
        assert!(
            !a.due(2000, 9000, false, 1),
            "not before the floor, even for a new target"
        );
        assert_eq!(a.target(), 2000);
        assert!(a.due(2000, 9000, false, AUTO_FETCH_BACKOFF_MIN_NS));
        assert_eq!(
            a.pick(&[5], AUTO_FETCH_BACKOFF_MIN_NS),
            Some(5),
            "5 is untried for the new target"
        );
    }

    /// Controller ruling R8 (plan ruling P15): a retarget right after a
    /// timeout waits at least the 1 s floor, whatever the stagger says.
    #[test]
    fn a_retarget_right_after_a_timeout_waits_the_floor() {
        let mut a = AutoFetch::new(0);
        assert!(a.due(1000, 9000, false, 0));
        assert_eq!(a.pick(&[5], 0), Some(5));
        let t = 5_000_000_000;
        a.on_result(Outcome::Timeout, t);
        assert!(!a.due(2000, 9000, false, t), "same pass: held");
        assert!(a.quiet(2000, t + AUTO_FETCH_BACKOFF_MIN_NS - 1));
        assert!(!a.due(2000, 9000, false, t + AUTO_FETCH_BACKOFF_MIN_NS - 1));
        assert!(a.due(2000, 9000, false, t + AUTO_FETCH_BACKOFF_MIN_NS));
        // A stagger longer than the floor still wins.
        let mut b = AutoFetch::new(8);
        b.note_timeout(t);
        assert!(!b.due(2000, 9000, false, t), "first sighting at t");
        assert!(!b.due(2000, 9000, false, t + AUTO_FETCH_BACKOFF_MIN_NS));
        assert!(b.due(2000, 9000, false, t + 8 * AUTO_FETCH_STAGGER_NS));
    }

    /// Controller ruling R8: ANY fetch timeout this node observes — an
    /// operator's too — sets the floor, for the current target and the next.
    #[test]
    fn an_operator_timeout_sets_the_floor() {
        let mut a = AutoFetch::new(0);
        assert!(!a.due(1000, 999, false, 0), "seen, waiting on durable");
        let t = 5_000_000_000;
        a.note_timeout(t);
        assert!(!a.due(1000, 9000, false, t), "same target: held");
        assert!(!a.due(1000, 9000, false, t + AUTO_FETCH_BACKOFF_MIN_NS - 1));
        assert!(a.due(1000, 9000, false, t + AUTO_FETCH_BACKOFF_MIN_NS));
        a.note_timeout(2 * t);
        assert!(!a.due(2000, 9000, false, 2 * t), "next target: held too");
        assert!(a.due(2000, 9000, false, 2 * t + AUTO_FETCH_BACKOFF_MIN_NS));
    }

    /// Review focus 4 + spec §7.3: an unknown size (0) is fetched without the
    /// check; the log line is named once per set.
    #[test]
    fn an_unknown_size_is_fetched_without_the_check() {
        let mut a = AutoFetch::new(0);
        assert!(a.due(1000, 9000, false, 0));
        assert_eq!(a.check_space(0, 0, 0), SpaceCheck::Unknown { first: true });
        assert_eq!(a.check_space(0, 0, 0), SpaceCheck::Unknown { first: false });
    }

    /// Final review M6: before the probe's first `statvfs` (or when every one
    /// fails) the free figure is unknown, not zero — the fetch is never
    /// refused `no_space` on it, nothing backs off, and the set's one
    /// `no_space` warning is still unspent for a real reading.
    #[test]
    fn an_unknown_free_figure_never_refuses_with_no_space() {
        let mut a = AutoFetch::new(0);
        assert!(a.due(1000, 9000, false, 0));
        assert_eq!(
            a.check_space(4 * GIB, FREE_BYTES_UNKNOWN, 0),
            SpaceCheck::FreeUnknown
        );
        assert!(!a.quiet(1000, 1), "no backoff on an unknown");
        assert!(!wont_fit(FREE_BYTES_UNKNOWN, 4 * GIB));
        assert_eq!(
            a.check_space(4 * GIB, GIB, 0),
            SpaceCheck::NoSpace { first: true },
            "the first REAL shortfall is still the one named"
        );
    }

    /// Spec §7.3 + plan ruling P11: no space → skipped, named once per set,
    /// and backed off; a newer set is named again.
    #[test]
    fn no_space_is_named_once_per_set_and_backs_off() {
        let mut a = AutoFetch::new(0);
        assert!(a.due(1000, 9000, false, 0));
        assert_eq!(
            a.check_space(4 * GIB, GIB, 0),
            SpaceCheck::NoSpace { first: true }
        );
        assert!(a.quiet(1000, AUTO_FETCH_BACKOFF_MIN_NS - 1));
        assert!(a.due(1000, 9000, false, AUTO_FETCH_BACKOFF_MIN_NS));
        assert_eq!(
            a.check_space(4 * GIB, GIB, AUTO_FETCH_BACKOFF_MIN_NS),
            SpaceCheck::NoSpace { first: false }
        );
        assert!(a.due(2000, 9000, false, 10 * AUTO_FETCH_BACKOFF_MIN_NS));
        assert_eq!(
            a.check_space(4 * GIB, GIB, 0),
            SpaceCheck::NoSpace { first: true }
        );
        assert_eq!(a.check_space(4 * GIB, 6 * GIB, 0), SpaceCheck::Fits);
    }

    /// Controller ruling R7 (belt and braces; supersedes PF8's recheck for
    /// the SAME target): an `ok` for N is never re-issued for N in this
    /// incarnation, even if `holdings_held` never lists it. A newer target
    /// starts fresh — tried list and ladder reset.
    #[test]
    fn an_ok_is_never_reissued_for_the_same_target() {
        let mut a = AutoFetch::new(0);
        assert!(a.due(1000, 9000, false, 0));
        assert_eq!(a.pick(&[5], 0), Some(5));
        a.on_result(Outcome::Ok, 7);
        assert!(a.quiet(1000, 7), "the same pass decides nothing");
        assert!(a.quiet(1000, 7 + AUTO_FETCH_RECHECK_NS));
        assert!(a.quiet(1000, 7 + 10 * AUTO_FETCH_BACKOFF_MAX_NS));
        assert!(
            !a.due(1000, 9000, false, 7 + 10 * AUTO_FETCH_BACKOFF_MAX_NS),
            "N fetched ok: never again"
        );
        let t = 8 + 10 * AUTO_FETCH_BACKOFF_MAX_NS;
        assert!(
            a.due(2000, 9000, false, t),
            "a newer target is due (node 0)"
        );
        assert_eq!(a.pick(&[5], t), Some(5), "fresh tried list");
        a.on_result(Outcome::Timeout, t);
        assert!(
            a.due(2000, 9000, false, t + AUTO_FETCH_BACKOFF_MIN_NS),
            "the ladder starts at its 1 s floor"
        );
    }

    #[test]
    fn outcomes_have_the_spec_labels_and_count_independently() {
        assert_eq!(
            Outcome::ALL.map(Outcome::label),
            ["ok", "refused", "timeout", "no_space", "no_holder"]
        );
        let s = AutoFetchStats::default();
        s.bump(Outcome::NoSpace);
        s.bump(Outcome::NoSpace);
        s.bump(Outcome::Ok);
        assert_eq!(
            (
                s.get(Outcome::NoSpace),
                s.get(Outcome::Ok),
                s.get(Outcome::Timeout)
            ),
            (2, 1, 0)
        );
    }
}
