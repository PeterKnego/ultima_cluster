//! Group-key rotation policy (spec §5 "Key rotation").
//!
//! Pure decision logic, no clock reads, no I/O — like every other unit in
//! this crate (see `lib.rs`'s module docs), [`RotationState`] is fed events
//! plus an explicit `now_ns` and asked what is due
//! ([`RotationState::take_due`]), the same driven-transition-function shape
//! as [`crate::handshake::Peers`] and [`crate::group::GroupPlane`]. It never
//! calls [`crate::group::GroupPlane::mint`] itself — the node layer (T9)
//! owns wiring "rotation is due" to "actually mint and distribute a new
//! epoch", so this module can stay entirely clock- and socket-free and
//! `uc2_sim`-drivable.
//!
//! # Why three triggers, and why they are not weighted equally
//!
//! 1. **`BecameLeader`.** A fresh leader always mints a fresh epoch. This
//!    single rule is deliberately load-bearing for a whole class of cases
//!    that would otherwise need individual handling: the outgoing leader in
//!    a self-removal steps down at the very commit that would have told IT
//!    to rotate, so it structurally cannot be the one to do it; a crashed
//!    leader may have missed a periodic or removal trigger entirely; and a
//!    handoff mid-rotation needs a clean restart of the rotation state
//!    anyway. Elections are rare enough that "just always rotate" costs
//!    nothing measurable and removes an entire category of missed-rotation
//!    bugs at the seam between leaders.
//! 2. **`Periodic`.** Ordinary key hygiene: whichever of a time interval or
//!    a bytes-sealed budget is hit first. Both counters are cleared on ANY
//!    rotation (not just a periodic one) — see `take_due` — because the
//!    budget's whole purpose is bounding exposure since the key last
//!    changed, and that purpose is served just as well by a rotation that
//!    happened to fire for a different reason.
//! 3. **`Removal`.** The security-relevant trigger. The group key is
//!    symmetric and shared cluster-wide; a removed node keeps the ability to
//!    decrypt any group-sealed traffic it captured before removal until the
//!    key itself changes — dropping the node's public key from the
//!    allowlist only stops FUTURE handshakes, it does nothing about a key
//!    already held. Demote is explicitly excluded: a demoted voter is still
//!    a cluster member (a learner), still legitimately replicated to, and
//!    rotating on demote would just make it re-fetch a key it never lost the
//!    right to have. The signal that distinguishes the two is the
//!    TOMBSTONE COUNT growing, not "a config change happened" — see
//!    `on_committed_config`.
//!
//! # Priority order and latch-and-clear
//!
//! `BecameLeader` and `Removal` are latched booleans: the event may occur
//! well before the caller next polls `take_due`, and the caller must not
//! lose it by polling at the wrong instant (the node's agents are
//! busy-spin polling loops, not an event queue). `take_due` checks in a
//! fixed priority order — `BecameLeader`, then `Removal`, then `Periodic` —
//! and reports only the highest-priority reason that is due, never more
//! than one per call. Priority matters operationally: a `Removal` firing in
//! the same instant as a due `Periodic` must surface as `Removal` in any
//! audit log built from these reasons, because "the interval happened to
//! also be up" is not the story an operator needs — "a node was just
//! removed" is. Whichever reason fires, ALL latches and counters are
//! cleared together: one rotation satisfies every outstanding trigger at
//! once, there is no such thing as "still owing a periodic rotation" right
//! after a leader-triggered one.
//!
//! # Baseline vs. growth
//!
//! [`RotationState::on_committed_config`] is fed the tombstone count on
//! every committed config change, demotes included — it has to see demotes
//! to correctly NOT rotate on them, so it cannot only be called for
//! removals. The very first call has no prior count to compare against and
//! must not itself be treated as growth (there is no way to tell "0 tombstones,
//! first observation" from "the count went from nothing to 0" without a
//! stored baseline) — it seeds `last_tombstones` instead. Every call after
//! that latches `Removal` only when the new count is STRICTLY greater than
//! the stored one, then updates the stored count regardless of whether it
//! grew, so a later demote (unchanged count) never re-triggers off a stale
//! comparison point.

use std::time::Duration;

/// How the rotation clock and byte budget are configured.
#[derive(Debug, Clone, Copy)]
pub struct RotationPolicy {
    pub interval_ns: u64,
    pub bytes: u64,
}

impl Default for RotationPolicy {
    /// 1 hour / 1 TiB — ordinary key hygiene, not a security boundary in
    /// itself (that is what `Removal` is for); generous enough that the
    /// periodic trigger essentially never fires ahead of a real
    /// membership event in practice.
    fn default() -> Self {
        RotationPolicy {
            interval_ns: Duration::from_secs(3600).as_nanos() as u64,
            bytes: 1u64 << 40,
        }
    }
}

/// Why a rotation is due. Carried through to whatever the caller logs — see
/// the module docs on why `Removal` must never be silently reported as
/// `Periodic` just because both happened to be due at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotationReason {
    BecameLeader,
    Periodic,
    Removal,
}

/// Rotation decision state. See the module docs for the full trigger
/// rationale and the latch-and-clear contract of [`RotationState::take_due`].
pub struct RotationState {
    policy: RotationPolicy,
    became_leader: bool,
    removal: bool,
    last_rotate_ns: u64,
    bytes_since: u64,
    /// The last tombstone count observed via `on_committed_config`, or
    /// `None` before the first observation — see the module docs' "Baseline
    /// vs. growth" section for why the first call must not be judged as
    /// growth against an assumed-zero baseline.
    last_tombstones: Option<usize>,
}

impl RotationState {
    pub fn new(policy: RotationPolicy) -> RotationState {
        RotationState {
            policy,
            became_leader: false,
            removal: false,
            last_rotate_ns: 0,
            bytes_since: 0,
            last_tombstones: None,
        }
    }

    /// A new leader always rotates. Latched — see the module docs.
    pub fn on_became_leader(&mut self) {
        self.became_leader = true;
    }

    /// Feed the tombstone count observed on every committed config change
    /// (promotes and demotes included, not just removals — the demote case
    /// is what proves this trigger is silent on non-departures). Latches
    /// `Removal` only when the count strictly grows past the last
    /// observation; the very first call seeds the baseline instead of
    /// firing.
    pub fn on_committed_config(&mut self, tombstone_count: usize) {
        if let Some(last) = self.last_tombstones
            && tombstone_count > last
        {
            self.removal = true;
        }
        self.last_tombstones = Some(tombstone_count);
    }

    /// Feed the byte count of every seal under the group key, for the
    /// bytes-sealed half of the periodic budget.
    pub fn on_bytes_sealed(&mut self, n: u64) {
        self.bytes_since = self.bytes_since.saturating_add(n);
    }

    /// Returns the highest-priority reason a rotation is due right now, if
    /// any, and — if it returns `Some` — clears every latch and counter in
    /// the same call: `BecameLeader`/`Removal` are un-latched and the
    /// periodic clock/byte budget both restart from `now_ns`/0. One
    /// rotation satisfies everything outstanding at once; see the module
    /// docs.
    pub fn take_due(&mut self, now_ns: u64) -> Option<RotationReason> {
        let reason = if self.became_leader {
            Some(RotationReason::BecameLeader)
        } else if self.removal {
            Some(RotationReason::Removal)
        } else if now_ns.saturating_sub(self.last_rotate_ns) >= self.policy.interval_ns
            || self.bytes_since >= self.policy.bytes
        {
            Some(RotationReason::Periodic)
        } else {
            None
        };

        if reason.is_some() {
            self.became_leader = false;
            self.removal = false;
            self.last_rotate_ns = now_ns;
            self.bytes_since = 0;
        }

        reason
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn becoming_leader_always_rotates() {
        let mut r = RotationState::new(RotationPolicy::default());
        r.on_became_leader();
        assert_eq!(r.take_due(0), Some(RotationReason::BecameLeader));
        assert_eq!(r.take_due(0), None, "consumed exactly once");
    }

    #[test]
    fn a_growing_tombstone_set_rotates_but_a_demote_does_not() {
        let mut r = RotationState::new(RotationPolicy::default());
        r.on_committed_config(0);
        assert_eq!(r.take_due(0), None, "baseline observation is not a trigger");
        // A demote leaves the tombstone set unchanged: the node stays in the
        // cluster and must keep replicating.
        r.on_committed_config(0);
        assert_eq!(r.take_due(0), None);
        // A Remove* tombstones an id.
        r.on_committed_config(1);
        assert_eq!(r.take_due(0), Some(RotationReason::Removal));
    }

    #[test]
    fn periodic_fires_on_the_interval_and_on_the_byte_budget() {
        let p = RotationPolicy {
            interval_ns: 1_000,
            bytes: 500,
        };
        let mut r = RotationState::new(p);
        assert_eq!(r.take_due(999), None);
        assert_eq!(r.take_due(1_001), Some(RotationReason::Periodic));

        let mut r2 = RotationState::new(p);
        r2.on_bytes_sealed(499);
        assert_eq!(r2.take_due(0), None);
        r2.on_bytes_sealed(2);
        assert_eq!(r2.take_due(0), Some(RotationReason::Periodic));
    }

    #[test]
    fn a_removal_outranks_a_simultaneously_due_periodic() {
        let mut r = RotationState::new(RotationPolicy {
            interval_ns: 1,
            bytes: u64::MAX,
        });
        r.on_committed_config(0);
        r.on_committed_config(1);
        assert_eq!(
            r.take_due(1_000),
            Some(RotationReason::Removal),
            "report the security event"
        );
    }

    // -- Beyond the mandated four. Own mutation testing on this task (per the
    // task brief's standing finding that the mandated suite has failed to
    // discriminate on five prior tasks) found two wrong implementations the
    // four tests above pass anyway. Each test below is paired with the
    // mutant it was written to kill.

    #[test]
    fn any_rotation_reason_restarts_the_periodic_clock_and_byte_budget() {
        // A wrong `take_due` might only reset `last_rotate_ns`/`bytes_since`
        // when the reason it returns is `Periodic` itself, leaving them
        // untouched on a `BecameLeader`/`Removal` rotation. That mutant
        // passes all four mandated tests (none of them mixes a non-periodic
        // trigger with a periodic clock left running) — the fresh key just
        // minted on election immediately looks overdue for a periodic
        // rotation too, and the interval never really means "since the key
        // last changed".
        let p = RotationPolicy {
            interval_ns: 1_000,
            bytes: u64::MAX,
        };
        let mut r = RotationState::new(p);
        assert_eq!(r.take_due(500), None, "not yet due by any trigger");
        r.on_became_leader();
        assert_eq!(r.take_due(600), Some(RotationReason::BecameLeader));
        assert_eq!(
            r.take_due(1_000),
            None,
            "the periodic clock must restart from the t=600 rotation, not stay pinned at t=0 \
             (only 400ns have elapsed since the last actual rotation, not 1000ns)"
        );
    }

    #[test]
    fn a_nonzero_first_observation_seeds_the_baseline_rather_than_reading_as_growth() {
        // A wrong implementation might default the "last seen" tombstone
        // count to a bare `0` instead of `Option<usize>`, folding "no prior
        // observation" and "previously observed exactly 0" into the same
        // state. That mutant passes the mandated demote/growth test — which
        // only ever starts its baseline at 0 — but misfires the very first
        // time `on_committed_config` is called with a cluster that ALREADY
        // has tombstones (e.g. `RotationState` constructed mid-life, after
        // some nodes were already removed): it would read that first
        // nonzero count as growth from an assumed-zero baseline and rotate
        // on a call that observed no new departure at all.
        let mut r = RotationState::new(RotationPolicy::default());
        r.on_committed_config(3);
        assert_eq!(
            r.take_due(0),
            None,
            "first observation is the baseline, even when it is nonzero"
        );
        r.on_committed_config(3);
        assert_eq!(r.take_due(0), None, "unchanged count, still no rotation");
        r.on_committed_config(4);
        assert_eq!(r.take_due(0), Some(RotationReason::Removal));
    }
}
