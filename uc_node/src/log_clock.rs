// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The leader's log clock: `CLOCK_MONOTONIC` plus a sampled epoch offset
//! (spec `docs/superpowers/specs/2026-09-08-uc2-monotonic-log-clock-design.md`
//! §2, §5).
//!
//! `REALTIME − MONOTONIC` is piecewise constant on Linux — NTP slews BOTH
//! clocks and steps only `REALTIME` (`clock_gettime(2)`) — so the offset moves
//! only at step events. A forward step is adopted at the next resample; a
//! backward step is never adopted as a step: the clock re-anchors at its own
//! current value and retires the difference by running `SMEAR_PPM` slow
//! (§5.3). The appender's `max(now, last_stamp)` clamp stays; this module
//! just never hands it a value that goes backwards.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Slew rate at which a backward step is retired: the derived clock runs
/// `SMEAR_PPM` parts per million slow until the step is paid back. 500 ppm is
/// `ntpd`'s maximum slew; a 1 s step retires in 2 000 s.
pub(crate) const SMEAR_PPM: u64 = 500;
/// How often the held offset is checked against `CLOCK_REALTIME`. The offset
/// does not drift (slew cancels), so this only has to catch STEPS.
pub(crate) const RESAMPLE_INTERVAL_NS: u64 = 1_000_000_000;
/// A disagreement smaller than this is sampling noise, not a step. Not
/// re-anchored, so if sub-tolerance disagreements accumulate past it they
/// are caught at a later resample.
pub(crate) const STEP_TOLERANCE_NS: u64 = 10_000;
/// The one-time sample at construction (Agrona's `OffsetEpochNanoClock`).
const INIT_RETRIES: u32 = 100;
const INIT_THRESHOLD_NS: u64 = 250;
/// The in-pass resample: bounded so the pass that resamples stays short.
const RESAMPLE_RETRIES: u32 = 4;
const RESAMPLE_THRESHOLD_NS: u64 = 1_000;

/// A detected step of the wall clock, in ns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    Forward(u64),
    Backward(u64),
}

/// Pure arithmetic: `wall = anchor_wall + d − retired(d)`, with
/// `d = mono − anchor_mono` and `retired(d) = min(smear, d × SMEAR_PPM / 10⁶)`.
/// Monotone in `mono` by construction (the derivative is `1 − SMEAR_PPM/10⁶`
/// while smearing, `1` after), and every re-anchor sets `anchor_wall` to a
/// value ≥ the last emitted one.
#[derive(Debug, Clone)]
pub(crate) struct LogClockCore {
    anchor_mono_ns: u64,
    anchor_wall_ns: u64,
    /// Total ns still to be retired by running slow, measured from the anchor.
    smear_ns: u64,
    next_resample_mono_ns: u64,
    /// The last emitted value — asserted against in debug builds only, so
    /// neither the field nor its store exist in a release build.
    #[cfg(debug_assertions)]
    last_ns: u64,
}

impl LogClockCore {
    pub(crate) fn new(mono_ns: u64, wall_ns: u64) -> Self {
        Self {
            anchor_mono_ns: mono_ns,
            anchor_wall_ns: wall_ns,
            smear_ns: 0,
            next_resample_mono_ns: mono_ns.saturating_add(RESAMPLE_INTERVAL_NS),
            #[cfg(debug_assertions)]
            last_ns: wall_ns,
        }
    }

    #[inline(always)]
    fn retired(&self, d: u64) -> u64 {
        if self.smear_ns == 0 {
            return 0;
        }
        ((d as u128 * SMEAR_PPM as u128 / 1_000_000) as u64).min(self.smear_ns)
    }

    #[inline(always)]
    fn derived(&self, mono_ns: u64) -> u64 {
        let d = mono_ns.saturating_sub(self.anchor_mono_ns);
        self.anchor_wall_ns + d - self.retired(d)
    }

    /// The log clock's value at monotonic instant `mono_ns`. Never less than
    /// any earlier answer.
    #[inline(always)]
    pub(crate) fn wall_at(&mut self, mono_ns: u64) -> u64 {
        let v = self.derived(mono_ns);
        #[cfg(debug_assertions)]
        {
            debug_assert!(
                v >= self.last_ns,
                "log clock went backwards: {} -> {v}",
                self.last_ns
            );
            self.last_ns = v;
        }
        v
    }

    #[inline(always)]
    pub(crate) fn due_for_resample(&self, mono_ns: u64) -> bool {
        mono_ns >= self.next_resample_mono_ns
    }

    /// Defer the next resample one interval without taking a sample (the
    /// wrapper calls this when every bracket was too wide to trust).
    pub(crate) fn skip_resample(&mut self, mono_ns: u64) {
        self.next_resample_mono_ns = mono_ns.saturating_add(RESAMPLE_INTERVAL_NS);
    }

    /// Ns of smear still to be retired at `mono_ns` (0 when the clock agrees
    /// with wall time).
    pub(crate) fn remaining_smear_ns(&self, mono_ns: u64) -> u64 {
        let d = mono_ns.saturating_sub(self.anchor_mono_ns);
        self.smear_ns - self.retired(d)
    }

    /// Compare a fresh `(mono, wall)` sample against what the held offset
    /// predicts. During a smear our derived clock is deliberately AHEAD of
    /// the wall by `remaining_smear_ns` — that is expected, not a step — so
    /// the baseline for "did the wall move" is `derived − remaining`, not
    /// `derived` (controller ruling R7). Forward step: adopt (re-anchor at
    /// the sample, drop any smear). Backward step: re-anchor at the DERIVED
    /// value and add the difference to the smear. Within tolerance: nothing.
    pub(crate) fn resample(&mut self, mono_ns: u64, wall_ns: u64) -> Option<Step> {
        self.next_resample_mono_ns = mono_ns.saturating_add(RESAMPLE_INTERVAL_NS);
        let remaining = self.remaining_smear_ns(mono_ns);
        let derived = self.derived(mono_ns);
        // Signed gap between our clock and the wall; positive = we are ahead.
        // A smear in flight is EXPECTED to leave us ahead by `remaining`, so
        // the step is what changed beyond that — not the gap itself.
        let gap = derived as i128 - wall_ns as i128;
        let step = gap - remaining as i128;
        if step.unsigned_abs() <= STEP_TOLERANCE_NS as u128 {
            return None;
        }
        self.anchor_mono_ns = mono_ns;
        if gap <= 0 {
            // The wall is at or above us: adopt it, nothing left to retire.
            self.anchor_wall_ns = wall_ns;
            self.smear_ns = 0;
            Some(Step::Forward((-step) as u64))
        } else {
            // We are still ahead of the wall: re-anchor at our own value
            // (never backwards) and retire the whole gap. A forward step
            // smaller than the remaining smear lands here too — it shrinks
            // the smear rather than being adopted.
            self.anchor_wall_ns = derived;
            self.smear_ns = gap as u64;
            if step > 0 {
                Some(Step::Backward(step as u64))
            } else {
                Some(Step::Forward((-step) as u64))
            }
        }
    }
}

#[inline(always)]
fn mono_since(base: Instant) -> u64 {
    base.elapsed().as_nanos() as u64
}

fn wall_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Bracket one `SystemTime` read between two monotonic reads, up to `retries`
/// times, and return `(mono_mid_ns, wall_ns, width_ns)` of the NARROWEST
/// bracket seen — stopping early once a bracket is at or under
/// `threshold_ns`. The caller decides whether `width_ns` is good enough.
///
/// `wall_now_ns() == 0` (a `SystemTime` before the epoch — the only way it
/// can return 0) is treated as an INVALID bracket: its `width` is forced to
/// `u64::MAX` so it can never be `best` and never satisfies `threshold_ns`,
/// which keeps a bogus 0 sample from ever being adopted as a ~56-year step.
pub(crate) fn bracket_sample(base: Instant, retries: u32, threshold_ns: u64) -> (u64, u64, u64) {
    let mut best: Option<(u64, u64, u64)> = None;
    for _ in 0..retries.max(1) {
        let m0 = mono_since(base);
        let w = wall_now_ns();
        let m1 = mono_since(base);
        let width = if w == 0 {
            u64::MAX
        } else {
            m1.saturating_sub(m0)
        };
        if best.is_none_or(|b| width < b.2) {
            best = Some((m0 + m1.saturating_sub(m0) / 2, w, width));
        }
        if width <= threshold_ns {
            break;
        }
    }
    best.expect("retries >= 1")
}

/// The clock the consensus agent owns: an `Instant` origin, the core, and
/// the resample-in-pass policy.
pub(crate) struct LogClock {
    base: Instant,
    core: LogClockCore,
    pending: Option<Step>,
}

impl LogClock {
    /// One bracketed sample at construction; blocks for at most
    /// `INIT_RETRIES` brackets (microseconds), never longer.
    pub(crate) fn new() -> Self {
        let base = Instant::now();
        let (m, w, _width) = bracket_sample(base, INIT_RETRIES, INIT_THRESHOLD_NS);
        // `bracket_sample` picks the best (narrowest-width) try regardless of
        // width here, so if every try saw `SystemTime` before the epoch this
        // is the process's only usable clock reading — not a case we should
        // silently accept in a debug build.
        debug_assert!(w != 0, "SystemTime before the epoch");
        Self {
            base,
            core: LogClockCore::new(m, w),
            pending: None,
        }
    }

    /// The pass's ONE clock read.
    #[inline(always)]
    pub(crate) fn mono_now(&self) -> u64 {
        mono_since(self.base)
    }

    /// The log clock at `mono_ns`. One compare on the steady path; the
    /// resample is out of line and runs once per `RESAMPLE_INTERVAL_NS`.
    #[inline(always)]
    pub(crate) fn wall_at(&mut self, mono_ns: u64) -> u64 {
        if self.core.due_for_resample(mono_ns) {
            self.resample_slow(mono_ns);
        }
        self.core.wall_at(mono_ns)
    }

    #[inline(never)]
    fn resample_slow(&mut self, mono_ns: u64) {
        let sample = bracket_sample(self.base, RESAMPLE_RETRIES, RESAMPLE_THRESHOLD_NS);
        self.apply_sample(mono_ns, sample);
    }

    /// The resample DECISION, separated from the sampling so it can be
    /// tested with a synthetic `(mono_mid, wall, width)`: a bracket wider
    /// than `RESAMPLE_THRESHOLD_NS` is skipped (deferred one interval,
    /// nothing adopted); otherwise the core compares it against the held
    /// offset and any step it reports is parked for `take_step`.
    fn apply_sample(&mut self, mono_ns: u64, (m, w, width): (u64, u64, u64)) {
        if width > RESAMPLE_THRESHOLD_NS {
            self.core.skip_resample(mono_ns);
            return;
        }
        if let Some(step) = self.core.resample(m, w) {
            self.pending = Some(step);
        }
    }

    /// The step detected by the most recent resample, once.
    pub(crate) fn take_step(&mut self) -> Option<Step> {
        self.pending.take()
    }

    pub(crate) fn remaining_smear_ns(&self, mono_ns: u64) -> u64 {
        self.core.remaining_smear_ns(mono_ns)
    }

    /// Test seam: feed a synthetic `(mono_mid, wall, width)` bracket into the
    /// resample decision, exactly as `resample_slow` would.
    #[cfg(test)]
    pub(crate) fn inject_sample(&mut self, mono_ns: u64, sample: (u64, u64, u64)) {
        self.apply_sample(mono_ns, sample);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: u64 = 1_000_000_000;

    #[test]
    fn steady_state_advances_one_to_one_with_monotonic() {
        let mut c = LogClockCore::new(100 * S, 1_700_000_000 * S);
        assert_eq!(c.wall_at(100 * S), 1_700_000_000 * S);
        assert_eq!(c.wall_at(100 * S + 1), 1_700_000_000 * S + 1);
        assert_eq!(c.wall_at(160 * S), 1_700_000_060 * S);
        assert_eq!(c.remaining_smear_ns(160 * S), 0);
    }

    #[test]
    fn a_forward_step_is_adopted_at_the_resample() {
        let mut c = LogClockCore::new(0, 1000 * S);
        assert!(!c.due_for_resample(RESAMPLE_INTERVAL_NS - 1));
        assert!(c.due_for_resample(RESAMPLE_INTERVAL_NS));
        // wall jumped 3 s ahead of where the offset predicts
        let at = RESAMPLE_INTERVAL_NS;
        let predicted = 1000 * S + at;
        assert_eq!(
            c.resample(at, predicted + 3 * S),
            Some(Step::Forward(3 * S))
        );
        assert_eq!(c.wall_at(at), predicted + 3 * S);
        assert_eq!(c.wall_at(at + S), predicted + 4 * S, "1:1 after adoption");
        assert_eq!(c.remaining_smear_ns(at + S), 0);
    }

    #[test]
    fn a_backward_step_never_moves_the_clock_backwards_and_is_smeared() {
        let mut c = LogClockCore::new(0, 1000 * S);
        let at = RESAMPLE_INTERVAL_NS;
        let before = c.wall_at(at);
        assert_eq!(c.resample(at, before - S), Some(Step::Backward(S)));
        // the instant after the step: unchanged, not backwards
        assert_eq!(c.wall_at(at), before);
        assert_eq!(c.remaining_smear_ns(at), S);
        // running SMEAR_PPM slow: after 1000 s of monotonic time, 0.5 s retired
        let later = at + 1000 * S;
        assert_eq!(c.wall_at(later), before + 1000 * S - S / 2);
        assert_eq!(c.remaining_smear_ns(later), S / 2);
        // after 2000 s the whole second is retired and the clock agrees with
        // real wall time again (real wall = before - 1 s + elapsed)
        let done = at + 2000 * S;
        assert_eq!(c.wall_at(done), before - S + 2000 * S);
        assert_eq!(c.remaining_smear_ns(done), 0);
        // and is 1:1 thereafter
        assert_eq!(c.wall_at(done + 7), before - S + 2000 * S + 7);
    }

    #[test]
    fn smear_retires_at_exactly_smear_ppm() {
        // 500 ppm: 1 ms of monotonic time retires 500 ns
        let mut c = LogClockCore::new(0, 1000 * S);
        let at = RESAMPLE_INTERVAL_NS;
        let before = c.wall_at(at);
        c.resample(at, before - 10_000_000); // 10 ms back
        assert_eq!(c.wall_at(at + 1_000_000), before + 1_000_000 - 500);
        assert_eq!(SMEAR_PPM, 500, "the test above encodes the constant");
    }

    #[test]
    fn sub_tolerance_disagreement_is_ignored_but_accumulation_is_caught() {
        let mut c = LogClockCore::new(0, 1000 * S);
        let at1 = RESAMPLE_INTERVAL_NS;
        let p1 = c.wall_at(at1);
        // 8 µs back: under STEP_TOLERANCE_NS, ignored, NOT re-anchored
        assert_eq!(c.resample(at1, p1 - 8_000), None);
        assert_eq!(c.remaining_smear_ns(at1), 0);
        let at2 = 2 * RESAMPLE_INTERVAL_NS;
        let p2 = c.wall_at(at2);
        // the wall is now 16 µs behind what the held offset predicts: a step
        assert_eq!(c.resample(at2, p2 - 16_000), Some(Step::Backward(16_000)));
        assert_eq!(c.remaining_smear_ns(at2), 16_000);
    }

    #[test]
    fn a_forward_step_during_a_smear_clears_the_smear() {
        let mut c = LogClockCore::new(0, 1000 * S);
        let at = RESAMPLE_INTERVAL_NS;
        let before = c.wall_at(at);
        c.resample(at, before - S);
        let at2 = at + 1000 * S; // 0.5 s retired, 0.5 s remaining
        let derived = c.wall_at(at2);
        assert_eq!(c.remaining_smear_ns(at2), S / 2);
        assert_eq!(
            c.resample(at2, derived + 5 * S),
            Some(Step::Forward(5 * S + S / 2))
        );
        assert_eq!(c.remaining_smear_ns(at2), 0);
        assert_eq!(c.wall_at(at2), derived + 5 * S);
    }

    #[test]
    fn a_second_backward_step_during_a_smear_adds_the_remaining_smear() {
        let mut c = LogClockCore::new(0, 1000 * S);
        let at = RESAMPLE_INTERVAL_NS;
        let before = c.wall_at(at);
        c.resample(at, before - S); // smear 1 s
        let at2 = at + 1000 * S; // 0.5 s retired, 0.5 s remaining
        let derived = c.wall_at(at2);
        assert_eq!(c.remaining_smear_ns(at2), S / 2);
        assert_eq!(
            c.resample(at2, derived - 2 * S),
            Some(Step::Backward(S + S / 2))
        );
        assert_eq!(c.remaining_smear_ns(at2), 2 * S);
        assert_eq!(c.wall_at(at2), derived, "still never backwards");
    }

    #[test]
    fn a_smear_in_flight_is_not_re_reported_and_the_gauge_counts_down() {
        // An INDEPENDENT wall: true_wall(mono) = base + mono, stepped back by
        // 1 s once at t = 1 s, then running 1:1. Every resample after the
        // step must see NO new step, and the remaining smear must fall
        // strictly toward 0 without undershoot.
        let base = 1000 * S;
        let true_wall = |mono: u64| base + mono - if mono >= S { S } else { 0 };
        let mut c = LogClockCore::new(0, base);
        let mut steps = Vec::new();
        let mut last_remaining = u64::MAX;
        let mut mono = 0;
        for _ in 0..3000 {
            mono += RESAMPLE_INTERVAL_NS;
            if let Some(s) = c.resample(mono, true_wall(mono)) {
                steps.push((mono, s));
            }
            let rem = c.remaining_smear_ns(mono);
            assert!(
                rem <= last_remaining,
                "smear grew at mono {mono}: {last_remaining} -> {rem}"
            );
            last_remaining = rem;
            let v = c.wall_at(mono);
            assert!(v >= true_wall(mono), "undershoot at mono {mono}");
        }
        assert_eq!(
            steps,
            vec![(S, Step::Backward(S))],
            "exactly one step reported: {steps:?}"
        );
        assert_eq!(c.remaining_smear_ns(mono), 0, "fully retired after 3000 s");
        assert_eq!(
            c.wall_at(mono),
            true_wall(mono),
            "converged to the wall, no residue"
        );
    }

    #[test]
    fn a_forward_step_smaller_than_the_remaining_smear_shrinks_it_without_going_backwards() {
        let mut c = LogClockCore::new(0, 1000 * S);
        let at = RESAMPLE_INTERVAL_NS;
        let before = c.wall_at(at);
        c.resample(at, before - S); // smear 1 s
        let at2 = 2 * RESAMPLE_INTERVAL_NS;
        let derived = c.wall_at(at2);
        let remaining = c.remaining_smear_ns(at2);
        // the wall jumps forward by a quarter second: still below us
        let wall = derived - remaining + S / 4;
        assert_eq!(c.resample(at2, wall), Some(Step::Forward(S / 4)));
        assert_eq!(c.wall_at(at2), derived, "never backwards");
        assert_eq!(c.remaining_smear_ns(at2), remaining - S / 4);
    }

    #[test]
    fn skip_resample_defers_one_interval_without_sampling() {
        let mut c = LogClockCore::new(0, 1000 * S);
        let at = RESAMPLE_INTERVAL_NS;
        assert!(c.due_for_resample(at));
        c.skip_resample(at);
        assert!(!c.due_for_resample(at + RESAMPLE_INTERVAL_NS - 1));
        assert!(c.due_for_resample(at + RESAMPLE_INTERVAL_NS));
    }

    #[test]
    fn output_is_monotone_under_a_seeded_random_step_sequence() {
        // LCG, no crates: forward and backward steps of random size at random
        // resample instants; the emitted series must never decrease.
        let mut c = LogClockCore::new(0, 1000 * S);
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            x >> 11
        };
        let mut mono = 0u64;
        let mut last = c.wall_at(0);
        for _ in 0..20_000 {
            mono += 1 + next() % (3 * RESAMPLE_INTERVAL_NS);
            if c.due_for_resample(mono) {
                let derived = c.wall_at(mono);
                let mag = next() % (5 * S);
                let wall = if next() % 2 == 0 {
                    derived + mag
                } else {
                    derived.saturating_sub(mag)
                };
                let _ = c.resample(mono, wall);
            }
            let v = c.wall_at(mono);
            assert!(v >= last, "went backwards: {last} -> {v} at mono {mono}");
            last = v;
        }
    }

    #[test]
    fn bracket_sample_returns_the_narrowest_bracket_and_a_sane_wall_value() {
        let base = std::time::Instant::now();
        let (mono_mid, wall, width) = bracket_sample(base, 100, 250);
        // wall is epoch ns, i.e. after 2020-01-01
        assert!(wall > 1_577_836_800 * S, "wall = {wall}");
        assert!(
            mono_mid < 10 * S,
            "mono since base should be small: {mono_mid}"
        );
        // 100 tries on an idle thread comfortably produce a sub-microsecond bracket
        assert!(width < 1_000_000, "width = {width} ns");
    }

    #[test]
    fn log_clock_wrapper_is_monotone_and_close_to_system_time() {
        let mut c = LogClock::new();
        let mut last = 0u64;
        for _ in 0..1000 {
            let m = c.mono_now();
            let v = c.wall_at(m);
            assert!(v >= last);
            last = v;
        }
        let sys = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let diff = sys.abs_diff(last);
        assert!(
            diff < 50_000_000,
            "clock and SystemTime differ by {diff} ns"
        );
        assert_eq!(c.take_step(), None, "no step on an undisturbed box");
    }

    #[test]
    fn a_wide_bracket_is_skipped_not_adopted() {
        let mut c = LogClock::new();
        let m = c.mono_now();
        let before = c.wall_at(m);
        // due now; the sample says "3 s ahead" but its bracket is too wide to trust
        c.apply_sample(m, (m, before + 3 * S, RESAMPLE_THRESHOLD_NS + 1));
        assert_eq!(c.take_step(), None, "a wide bracket must not be adopted");
        assert_eq!(c.wall_at(m), before, "nothing changed");
        assert!(
            !c.core.due_for_resample(m + RESAMPLE_INTERVAL_NS - 1),
            "deferred one interval"
        );
        assert!(c.core.due_for_resample(m + RESAMPLE_INTERVAL_NS));
    }

    #[test]
    fn a_narrow_bracket_forward_step_is_adopted_and_reported_once() {
        let mut c = LogClock::new();
        let m = c.mono_now();
        let before = c.wall_at(m);
        c.apply_sample(m, (m, before + 3 * S, RESAMPLE_THRESHOLD_NS));
        assert_eq!(c.take_step(), Some(Step::Forward(3 * S)));
        assert_eq!(c.take_step(), None, "reported once");
        assert_eq!(c.wall_at(m), before + 3 * S);
    }

    #[test]
    fn a_narrow_bracket_backward_step_is_smeared_and_reported() {
        let mut c = LogClock::new();
        let m = c.mono_now();
        let before = c.wall_at(m);
        c.apply_sample(m, (m, before - S, 100));
        assert_eq!(c.take_step(), Some(Step::Backward(S)));
        assert_eq!(c.wall_at(m), before, "never backwards");
        assert_eq!(c.remaining_smear_ns(m), S);
    }

    #[test]
    fn resample_slow_on_a_real_clock_reports_no_step_when_undisturbed() {
        let mut c = LogClock::new();
        let m = c.mono_now();
        let before = c.wall_at(m);
        c.resample_slow(m); // real bracket against real SystemTime; no step expected
        let after = c.wall_at(m);
        assert!(after >= before);
        assert_eq!(c.take_step(), None);
    }
}
