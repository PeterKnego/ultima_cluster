# Monotonic, wall-clock-anchored log clock — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The leader's log-time stamp is read from `CLOCK_MONOTONIC` plus a sampled epoch offset, so a backward NTP step **slows** the log clock (smeared at 500 ppm) instead of **freezing** it, a forward step is adopted immediately, and the consensus pass takes **one** clock read instead of two.

**Architecture:** A private `uc_node::log_clock` module holds `LogClockCore` — pure integer arithmetic over `(mono_ns, wall_ns)` pairs: `wall = anchor_wall + (mono − anchor_mono) − retired_smear` — and a thin `LogClock` wrapper that owns the `Instant` origin, takes the bracketed `SystemTime` sample (Agrona's `OffsetEpochNanoClock` method), and resamples on a 1 s cadence out of line. `Consensus::do_work` reads `Instant` once at the top; the stamp (`pass_now_ns`) and `Event::Tick`'s `now_ns` are both derived from that one reading. The appender's `max(now, last_stamp)` clamp is untouched. One new gauge (`uc2_log_clock_smear_ns`) and one obs event (`log_clock_step`) make a step visible; `Uc2LogTimeFrozen` keeps its rule and narrows to "the appender is stalled".

**Tech Stack:** Rust 1.96 (MSRV 1.89), `std::time::{Instant, SystemTime}` only — no new crates, no `unsafe`, no arch intrinsics. Tests: `cargo test -p uc_node`, the in-file `Harness` in `uc_node/src/node.rs`, `uc_node/src/obs/metrics.rs`'s render tests, `cargo clippy --workspace --all-targets -- -D warnings`.

**Spec:** `docs/superpowers/specs/2026-09-08-uc2-monotonic-log-clock-design.md` — §2 (the offset), §5 (the design; §5.2 one read, §5.3 step/smear), §7 (observability), §8 (acceptance: the fleet A/B and its three pre-committed readings), §9 (open parameters — this plan settles them in Task 0). Read §3–§4 for *why* approach A and why B-lite is deferred; nothing in them is implemented here.

## Global Constraints

- **Rebase first.** This worktree (`claude-2`) is at `0ef7ddc`; `main` has since tagged `2.11.0` (`4855f36`). Task 0 rebases onto `main`. **Every `node.rs` line number in this plan is from `0ef7ddc`; on `main` the same code sits 18 lines lower for anything past line 480** (`git diff -U0 0ef7ddc main -- uc_node/src/node.rs` shows exactly two hunks: `+18` at 480, and 6941). Re-find each anchor with the `grep` given beside it rather than trusting the number.
- **Whole workspace green after every task:** `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test -p uc_node`, and after Task 2 also `cargo test -p uc_node --test lin_v2` and `cargo test -p uc_node --test timers`. Use a private `CARGO_TARGET_DIR` from this worktree (CLAUDE.md "Benchmarking discipline").
- **The consensus hot loop gains no code size.** `do_work`'s top goes from two clock reads to one read plus one `#[inline(always)]` add and one `#[inline(always)]` compare; the resample body and the step-event body are `#[inline(never)]`. The M14a lesson (CLAUDE.md "Finding a performance bottleneck") is binding: inline code costs even on paths that never run.
- **No `unsafe`, no arch-specific code, no new dependency.** Approach B-lite is a separate future spec (spec §4).
- **Parameters, fixed here (spec §9) — each a named `pub(crate) const` in `log_clock.rs` so a change is one line:** `SMEAR_PPM = 500` (ntpd's maximum slew; a 1 s step retires in 2 000 s), `RESAMPLE_INTERVAL_NS = 1_000_000_000`, `STEP_TOLERANCE_NS = 10_000` (10 µs — a disagreement below it is sampling noise, not a step; sub-tolerance disagreements are NOT re-anchored, so if they accumulate past it they are caught), `INIT_RETRIES = 100` / `INIT_THRESHOLD_NS = 250` (Agrona's, for the one-time sample at construction), `RESAMPLE_RETRIES = 4` / `RESAMPLE_THRESHOLD_NS = 1_000` (the in-pass resample: ≤ 4 brackets ≈ ≤ 250 ns worst case; if none is under the threshold the resample is **skipped** until the next interval, never taken from a wide bracket).
- **Wire, cnc, `ApplyCtx`, `uc_log::Appender`: untouched.** `test_now_ns` (the `#[cfg(test)]` override) keeps overriding the **wall** value only; `Event::Tick` keeps getting a real monotonic value in tests, so no existing test changes behaviour.
- **Every new or changed test is watched red first.** Commit subjects: `type(scope): imperative summary`.
- **Fleet spend is user-gated.** Task 5's fleet A/B is written into the gate doc as pre-committed and *not run*; only the dev-box smoke runs here. Never write scratch to `/tmp`.

---

## File structure

| file | responsibility | task |
|---|---|---|
| spec §5.2, §7, §9 | errata: Tick-only derivation, lag saturates at 0 under a smear (hence the gauge), parameters chosen | 0 |
| `uc_node/src/log_clock.rs` (**new**), `uc_node/src/lib.rs` | `LogClockCore` (pure), `LogClock` (owns `Instant`, samples, resamples), `Step`, `bracket_sample` | 1 |
| `uc_node/src/node.rs` | `Consensus.clock`, `pass_mono_ns`, one read at the top of `do_work`, `pass_clock(mono)`, Tick from the pass reading, `wall_now_ns` deleted | 2 |
| `uc_node/src/node.rs`, `uc_node/src/obs/{mod,metrics}.rs`, `packaging/prometheus/uc2-alerts.yml` | `log_clock_step` event, `uc2_log_clock_smear_ns` gauge, `uc2_log_time_lag_seconds` help + alert comment narrowed | 3 |
| `docs/reference/limits.md`, `docs/notes/uc2-log-time-and-timers-explained.md`, `docs/ops/uc2-runbook.md`, `RELEASES.md`, `docs/releases.md`, `CLAUDE.md` | the shipped-limitation row rewritten; explainer section; runbook; the 2.12.0 writeup skeleton | 4 |
| `docs/benchmarks/uc2-log-clock-gate-2026-09-08.md` (**new**) | the pre-committed fleet A/B (three readings) + the dev-box smoke record | 5 |

---

### Task 0: Rebase onto `main` and record the spec errata

**Files:**
- Modify: `docs/superpowers/specs/2026-09-08-uc2-monotonic-log-clock-design.md` (§5.2, §7, §9)

**Interfaces:**
- Produces: a worktree whose `HEAD` contains `4855f36` ("2.11.0 is tagged"); the spec's §9 parameters replaced by the values in Global Constraints.

- [ ] **Step 1: Rebase**

```bash
git -C /home/claude/ultima/ultima_cluster/.claude/worktrees/claude-2 rebase main
git log --oneline -3   # expect the two spec commits (6e38205, 7e19d25 — new SHAs after rebase) on top of 4855f36's descendants
```

Expected: clean rebase (the spec commits touch only `docs/superpowers/specs/`). If it conflicts, stop and report — nothing else in this plan touched `main`.

- [ ] **Step 2: Append the errata section to the spec**

Append at the end of the spec file:

```markdown

## Errata (plan, as built)

- **§5.2, which sites take the pass reading.** Only `Event::Tick`'s `now_ns`
  (`node.rs:3497`) is derived from the pass's one `Instant` reading. The
  three conditional `now_ns()` sites (`:4549`, `:4612`, `:6306`) keep their
  own `Instant` read: they are rare, they are intervals against a deadline
  set from the same source, and folding them in would mean threading the
  pass value into three cold paths for no measurable gain.
- **§7, what the lag series shows during a smear.** The first draft said
  `uc2_log_time_lag_seconds` "shows the smear retiring". It cannot: the
  exporter computes `wall.saturating_sub(log_time)`, and during a smear the
  log clock is AHEAD of wall, so the series reads 0. That is the right
  behaviour for the alert (a smeared-but-healthy leader must not fire
  `Uc2LogTimeFrozen`) but leaves the smear invisible, so the plan adds one
  gauge, `uc2_log_clock_smear_ns` (leader-only, remaining nanoseconds still
  to retire; 0 when none), and one obs event, `log_clock_step`
  (`direction`, `step_ns`, `smear_ns`), emitted once per detected step.
  §9's "whether to count steps" is answered: no counter — the event carries
  the count and the gauge carries the state.
- **§9, parameters.** Fixed in the plan's Global Constraints:
  `SMEAR_PPM = 500`, `RESAMPLE_INTERVAL_NS = 1 s`, `STEP_TOLERANCE_NS =
  10 µs`, bracket retries/thresholds 100/250 ns at construction and
  4/1 000 ns in-pass (skip, never adopt a wide bracket). Suspend reads as a
  forward step and is adopted — asserted, covered by the core's forward-step
  test, not by a real suspend.
- **§8, the local smoke harness.** `scripts/hop1_ab.sh` measures the client
  hop against `dummy-node` — the consensus agent is not in its path, so it
  cannot A/B this change. The dev-box smoke is `m12_gate --arm direct` (three
  in-process nodes, the real consensus agent), two binaries alternated; the
  acceptance A/B is `m14_fleet_gate.py` row a on the fleet, as written.
```

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-09-08-uc2-monotonic-log-clock-design.md
git commit -m "docs(spec): monotonic log clock — plan errata: Tick-only derivation, lag saturates under a smear (gauge added), parameters fixed, smoke harness corrected"
```

---

### Task 1: `log_clock.rs` — the pure core, the sampling wrapper, and their tests

**Files:**
- Create: `uc_node/src/log_clock.rs`
- Modify: `uc_node/src/lib.rs` (add `mod log_clock;` beside `mod node;` at `lib.rs:50`)

**Interfaces:**
- Produces (all `pub(crate)`):
  - `const SMEAR_PPM: u64`, `RESAMPLE_INTERVAL_NS: u64`, `STEP_TOLERANCE_NS: u64`
  - `enum Step { Forward(u64), Backward(u64) }` — the step size in ns
  - `struct LogClockCore` with `fn new(mono_ns: u64, wall_ns: u64) -> Self`, `fn wall_at(&mut self, mono_ns: u64) -> u64`, `fn due_for_resample(&self, mono_ns: u64) -> bool`, `fn skip_resample(&mut self, mono_ns: u64)`, `fn resample(&mut self, mono_ns: u64, wall_ns: u64) -> Option<Step>`, `fn remaining_smear_ns(&self, mono_ns: u64) -> u64`
  - `struct LogClock` with `fn new() -> Self`, `fn mono_now(&self) -> u64`, `fn wall_at(&mut self, mono_ns: u64) -> u64`, `fn take_step(&mut self) -> Option<Step>`, `fn remaining_smear_ns(&self, mono_ns: u64) -> u64`
  - `fn bracket_sample(base: Instant, retries: u32, threshold_ns: u64) -> (u64, u64, u64)` — `(mono_mid_ns, wall_ns, bracket_width_ns)` of the narrowest bracket

- [ ] **Step 1: Write the failing tests (core arithmetic)**

Create `uc_node/src/log_clock.rs` with ONLY the module doc and the test module first, so the tests fail to compile against a missing API:

```rust
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
        assert_eq!(c.resample(at, predicted + 3 * S), Some(Step::Forward(3 * S)));
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
        let at2 = 2 * RESAMPLE_INTERVAL_NS;
        let derived = c.wall_at(at2);
        assert_eq!(c.resample(at2, derived + 5 * S), Some(Step::Forward(5 * S)));
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
        assert_eq!(c.resample(at2, derived - 2 * S), Some(Step::Backward(2 * S)));
        assert_eq!(c.remaining_smear_ns(at2), S / 2 + 2 * S);
        assert_eq!(c.wall_at(at2), derived, "still never backwards");
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
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            x >> 11
        };
        let mut mono = 0u64;
        let mut last = c.wall_at(0);
        for _ in 0..20_000 {
            mono += 1 + next() % (3 * RESAMPLE_INTERVAL_NS);
            if c.due_for_resample(mono) {
                let derived = c.wall_at(mono);
                let mag = next() % (5 * S);
                let wall = if next() % 2 == 0 { derived + mag } else { derived.saturating_sub(mag) };
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
        assert!(mono_mid < 10 * S, "mono since base should be small: {mono_mid}");
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
        assert!(diff < 50_000_000, "clock and SystemTime differ by {diff} ns");
        assert_eq!(c.take_step(), None, "no step on an undisturbed box");
    }
}
```

Add to `uc_node/src/lib.rs` beside `mod node;`:

```rust
mod log_clock;
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_node --lib log_clock 2>&1 | tail -20`
Expected: compile errors — `LogClockCore`, `LogClock`, `Step`, `bracket_sample`, the constants: "cannot find".

- [ ] **Step 3: Implement the module**

Insert above the `#[cfg(test)]` module:

```rust
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
    /// The last emitted value — asserted against in debug builds only.
    last_ns: u64,
}

impl LogClockCore {
    pub(crate) fn new(mono_ns: u64, wall_ns: u64) -> Self {
        Self {
            anchor_mono_ns: mono_ns,
            anchor_wall_ns: wall_ns,
            smear_ns: 0,
            next_resample_mono_ns: mono_ns.saturating_add(RESAMPLE_INTERVAL_NS),
            last_ns: wall_ns,
        }
    }

    #[inline(always)]
    fn retired(&self, d: u64) -> u64 {
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
        debug_assert!(v >= self.last_ns, "log clock went backwards: {} -> {v}", self.last_ns);
        self.last_ns = v;
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
    /// predicts. Forward step: adopt (re-anchor at the sample, drop any
    /// smear). Backward step: re-anchor at the DERIVED value and add the
    /// difference to the smear. Within tolerance: nothing.
    pub(crate) fn resample(&mut self, mono_ns: u64, wall_ns: u64) -> Option<Step> {
        self.next_resample_mono_ns = mono_ns.saturating_add(RESAMPLE_INTERVAL_NS);
        let derived = self.derived(mono_ns);
        if wall_ns > derived.saturating_add(STEP_TOLERANCE_NS) {
            let by = wall_ns - derived;
            self.anchor_mono_ns = mono_ns;
            self.anchor_wall_ns = wall_ns;
            self.smear_ns = 0;
            Some(Step::Forward(by))
        } else if wall_ns.saturating_add(STEP_TOLERANCE_NS) < derived {
            let by = derived - wall_ns;
            let remaining = self.remaining_smear_ns(mono_ns);
            self.anchor_mono_ns = mono_ns;
            self.anchor_wall_ns = derived;
            self.smear_ns = remaining + by;
            Some(Step::Backward(by))
        } else {
            None
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
pub(crate) fn bracket_sample(base: Instant, retries: u32, threshold_ns: u64) -> (u64, u64, u64) {
    let mut best: Option<(u64, u64, u64)> = None;
    for _ in 0..retries.max(1) {
        let m0 = mono_since(base);
        let w = wall_now_ns();
        let m1 = mono_since(base);
        let width = m1.saturating_sub(m0);
        if best.is_none_or(|b| width < b.2) {
            best = Some((m0 + width / 2, w, width));
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
        let (m, w, width) = bracket_sample(self.base, RESAMPLE_RETRIES, RESAMPLE_THRESHOLD_NS);
        if width > RESAMPLE_THRESHOLD_NS {
            // Every bracket was preempted or otherwise wide: do not adopt a
            // reading whose error could itself look like a step.
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
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc_node --lib log_clock 2>&1 | tail -20`
Expected: `test result: ok. 11 passed`.

If `output_is_monotone_under_a_seeded_random_step_sequence` fails, the arithmetic has a monotonicity hole — do NOT loosen the assertion; fix `resample` (the invariant is "every re-anchor sets `anchor_wall_ns` ≥ the last emitted value").

- [ ] **Step 5: fmt + clippy**

Run: `cargo fmt --all && cargo clippy -p uc_node --all-targets -- -D warnings 2>&1 | tail -5`
Expected: no warnings. (`is_none_or` is stable since 1.82; MSRV is 1.89.)

- [ ] **Step 6: Commit**

```bash
git add uc_node/src/log_clock.rs uc_node/src/lib.rs
git commit -m "feat(uc_node): log_clock — CLOCK_MONOTONIC + sampled epoch offset; forward steps adopted, backward steps smeared at 500 ppm"
```

---

### Task 2: One clock read per consensus pass

**Files:**
- Modify: `uc_node/src/node.rs` — the `Consensus` struct (`base: Instant` at `:2894`, `pass_now_ns` at `:2686`), both constructors (`base: Instant::now()` at `:1914` and `:9926`; `pass_now_ns: 0` at `:1861` and `:9873`), `wall_now_ns` (`:3254–3259`), `pass_clock` (`:3268`, `:3276`), the top of `do_work` (`:3308–3311`), the Tick feed (`:3497`), `now_ns` (`:4726`), the `use std::time` line (`:12`)

**Interfaces:**
- Consumes: `crate::log_clock::LogClock::{new, mono_now, wall_at}` (Task 1).
- Produces: `Consensus.clock: LogClock`, `Consensus.pass_mono_ns: u64` (the pass's monotonic reading, set at the top of `do_work`), `fn pass_clock(&mut self, mono_ns: u64) -> u64`. Task 3 reads `self.clock` and `self.pass_mono_ns` from `publish_status`.

- [ ] **Step 1: Write the failing harness test**

In `uc_node/src/node.rs`'s test module, next to the test at `:11328` that sets `h.cons.test_now_ns = Some(50)` (find it with `grep -n "test_now_ns = Some(50)" uc_node/src/node.rs`), add:

```rust
    /// Monotonic log clock (spec §5.2): a pass takes ONE clock read. The
    /// stamp (`pass_now_ns`) and the Tick (`pass_mono_ns`) both come from it,
    /// both advance across passes, and the wall value tracks `SystemTime`.
    #[test]
    fn a_pass_reads_the_clock_once_and_both_derived_values_advance() {
        let mut h = harness();
        h.cons.do_work();
        let (w1, m1) = (h.cons.pass_now_ns, h.cons.pass_mono_ns);
        assert!(m1 > 0, "pass_mono_ns is set by the pass");
        std::thread::sleep(std::time::Duration::from_millis(2));
        h.cons.do_work();
        let (w2, m2) = (h.cons.pass_now_ns, h.cons.pass_mono_ns);
        assert!(m2 > m1 && w2 > w1, "both derived values advance: {m1}->{m2}, {w1}->{w2}");
        assert_eq!(
            (w2 - w1) / 1_000_000,
            (m2 - m1) / 1_000_000,
            "wall and mono advanced by the same amount (to the ms)"
        );
        let sys = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        assert!(sys.abs_diff(w2) < 50_000_000, "stamp is wall-anchored: off by {}", sys.abs_diff(w2));
    }

    /// The test seam overrides the WALL value only; the Tick still gets a
    /// real monotonic reading, so pinning the pass clock cannot stall an
    /// election timeout.
    #[test]
    fn test_now_ns_overrides_the_stamp_but_not_the_monotonic_reading() {
        let mut h = harness();
        h.cons.test_now_ns = Some(50);
        h.cons.do_work();
        assert_eq!(h.cons.pass_now_ns, 50);
        assert!(h.cons.pass_mono_ns > 50, "mono is real, not the pinned 50");
    }
```

`harness()` is the free fn at `node.rs:9599` (`fn harness() -> Harness`) the neighbouring tests use; `h.cons` is the `Consensus`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_node --lib a_pass_reads_the_clock_once 2>&1 | tail -8`
Expected: compile error — `no field pass_mono_ns`.

- [ ] **Step 3: Rewire `Consensus`**

(a) The struct. Replace the field at `:2894` (`grep -n "^    base: Instant," uc_node/src/node.rs`):

```rust
    /// The pass's one clock (spec 2026-09-08 §5): `Instant` origin + sampled
    /// epoch offset. `pass_now_ns` (the stamp) and `pass_mono_ns` (the Tick)
    /// are both derived from ONE `mono_now()` at the top of `do_work`.
    clock: crate::log_clock::LogClock,
```

and add beside `pass_now_ns: u64` at `:2686`:

```rust
    /// The pass's one MONOTONIC reading (ns since `clock`'s origin) — the
    /// value `Event::Tick` carries, and what `record_pass_interval`'s and the
    /// lateness histogram's subtraction would be against if they moved off
    /// wall time (they have not; see Task 3's note).
    pass_mono_ns: u64,
```

(b) Both constructors: replace `base: Instant::now(),` (`:1914`, `:9926`) with `clock: crate::log_clock::LogClock::new(),`, and beside each `pass_now_ns: 0,` (`:1861`, `:9873`) add `pass_mono_ns: 0,`.

(c) Delete `wall_now_ns` (`:3248–3259`, the doc comment and the fn) and replace the two `pass_clock` bodies (`:3261–3278`) with:

```rust
    /// The pass's one WALL value, derived from the pass's one monotonic
    /// reading. In a release build this is `LogClock::wall_at` — one compare
    /// and one add on the steady path — so `do_work`'s hot top gains no code.
    #[cfg(not(test))]
    #[inline(always)]
    fn pass_clock(&mut self, mono_ns: u64) -> u64 {
        self.clock.wall_at(mono_ns)
    }

    /// Test build: honour `test_now_ns` when the test has placed the pass at a
    /// chosen instant, else derive the real value. Only the WALL value is
    /// overridden; `pass_mono_ns` is always the real reading.
    #[cfg(test)]
    fn pass_clock(&mut self, mono_ns: u64) -> u64 {
        match self.test_now_ns {
            Some(t) => t,
            None => self.clock.wall_at(mono_ns),
        }
    }
```

(d) The top of `do_work` (`:3308–3311`, `grep -n "let now_wall = self.pass_clock" uc_node/src/node.rs`). Replace

```rust
        let now_wall = self.pass_clock();
        self.pass_now_ns = now_wall;
```

with

```rust
        let mono = self.clock.mono_now();
        self.pass_mono_ns = mono;
        let now_wall = self.pass_clock(mono);
        self.pass_now_ns = now_wall;
```

and update the comment above it: "ONE clock read per pass" is now literally one `Instant` read; the `SystemTime` read is gone.

(e) The Tick (`:3497`, `grep -n "Feed the tick" uc_node/src/node.rs`). Replace `let now = self.now_ns();` with

```rust
        // Spec 2026-09-08 §5.2: the Tick takes the pass's one monotonic
        // reading — the second per-pass `Instant::now()` this replaced was
        // the whole of approach A's measurable saving.
        let now = self.pass_mono_ns;
```

(f) `now_ns` (`:4726`): body becomes `self.clock.mono_now()`. Same origin, same semantics; the three conditional callers are untouched (spec errata, Task 0).

(g) `use std::time::{Duration, Instant, SystemTime};` (`:12`): remove `SystemTime` if `grep -n "SystemTime::" uc_node/src/node.rs` now shows only comments; keep `Instant` (it is still used by tests and deadlines).

- [ ] **Step 4: Run the new tests, then the whole crate**

Run: `cargo test -p uc_node --lib a_pass_reads_the_clock_once test_now_ns_overrides 2>&1 | tail -6`
Expected: 2 passed.

Run: `cargo test -p uc_node 2>&1 | tail -5` and `cargo test -p uc_node --test timers 2>&1 | tail -3` and `cargo test -p uc_node --test lin_v2 2>&1 | tail -3`
Expected: all green — no existing test changes behaviour (the seam still overrides the wall value; the Tick got a real monotonic value before and gets one now).

- [ ] **Step 5: fmt + clippy + the unused-import check**

Run: `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -5`
Expected: clean. A `SystemTime` unused-import warning here means step (g) was skipped.

- [ ] **Step 6: Confirm the hot top did not grow (M14a)**

Run:
```bash
CARGO_TARGET_DIR=$HOME/.cache/cargo-target-logclock cargo build --release -p uc_node --example m5_gate
objdump -d -C $HOME/.cache/cargo-target-logclock/release/examples/m5_gate | grep -c "clock_gettime\|__vdso_clock_gettime"
```
Expected: the count is LOWER than the same command on the pre-Task-2 binary (build `HEAD~1` into a second private target dir and compare). Record both numbers in the commit message. This is a codegen sanity check, not a rate measurement.

- [ ] **Step 7: Commit**

```bash
git add uc_node/src/node.rs
git commit -m "perf(uc_node): one clock read per consensus pass — stamp and Tick both derived from one Instant reading via LogClock; wall_now_ns deleted"
```

---

### Task 3: The step event, the smear gauge, and the narrowed alert

**Files:**
- Modify: `uc_node/src/node.rs` — `publish_status` (`:4714–4724`, `grep -n "status.node_heartbeat_ns.store_release" uc_node/src/node.rs`); the `Node` struct field band around `schedule_entries_pub` (`:843`, `:1790`, `:1898`, `:2027`, `:2435`) and the `Consensus` field at `:2858`
- Modify: `uc_node/src/obs/mod.rs` (`ObsSources`, `:45–70`), `uc_node/src/obs/metrics.rs` (name list `:94`, the lag gauge `:805–825`, the test constructor `:1477`)
- Modify: `packaging/prometheus/uc2-alerts.yml` (`Uc2LogTimeFrozen`, `:142–154`)

**Interfaces:**
- Consumes: `Consensus.clock`, `Consensus.pass_mono_ns` (Task 2); `LogClock::{take_step, remaining_smear_ns}`, `Step` (Task 1).
- Produces: `ObsSources.log_clock_smear_ns: Arc<AtomicU64>`; the series `uc2_log_clock_smear_ns`; the obs event `log_clock_step { node, direction, step_ns, smear_ns }`.

- [ ] **Step 1: Write the failing metrics test**

In `uc_node/src/obs/metrics.rs`'s test module, beside the `uc2_log_time_lag_seconds` render test (`grep -n "uc2_log_time_lag_seconds" uc_node/src/obs/metrics.rs` — the test near `:1620–1680`), add:

```rust
    #[test]
    fn log_clock_smear_gauge_renders_the_published_value() {
        let s = synthetic_sources();
        s.log_clock_smear_ns.store(123_456, Ordering::Relaxed);
        let text = render_prometheus(&s);
        assert!(
            text.contains("\n# TYPE uc2_log_clock_smear_ns gauge\n"),
            "{text}"
        );
        assert!(text.contains("\nuc2_log_clock_smear_ns 123456\n"), "{text}");
        assert!(CONTRACT_SERIES.contains(&"uc2_log_clock_smear_ns"));
    }
```

`synthetic_sources()` is the test fixture at `metrics.rs:1438` (`fn synthetic_sources() -> ObsSources`); `CONTRACT_SERIES` is the name-list constant at `metrics.rs:39`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_node --lib log_clock_smear_gauge 2>&1 | tail -6`
Expected: compile error — `no field log_clock_smear_ns`.

- [ ] **Step 3: Plumb the atomic, mirroring `schedule_entries_pub` exactly**

Do for `log_clock_smear_pub` what the tree does for `schedule_entries_pub` at each of these sites (`grep -n "schedule_entries" uc_node/src/node.rs uc_node/src/obs/*.rs`):

- `Node` field (`node.rs:843`): `log_clock_smear_pub: Arc<AtomicU64>,`
- creation in `Node::start` (`:1790`): `let log_clock_smear_pub = Arc::new(AtomicU64::new(0));`
- into `Consensus` (`:1898`): `log_clock_smear_pub: Arc::clone(&log_clock_smear_pub),`
- stored on `Node` (`:2027`): `log_clock_smear_pub,`
- `observability()` (`:2435`): `log_clock_smear_ns: Arc::clone(&self.log_clock_smear_pub),`
- `Consensus` field (`:2858`), with the doc: "Spec 2026-09-08 §7: remaining smear ns, leader-written once per pass from `publish_status`; `uc2_log_clock_smear_ns`."
- the `Consensus` test constructor (`:9870–9930`): `log_clock_smear_pub: Arc::new(AtomicU64::new(0)),`
- `ObsSources` (`obs/mod.rs:45`): `pub log_clock_smear_ns: Arc<AtomicU64>,` with the doc "Spec 2026-09-08 §7: ns of backward wall-clock step still being retired by the log clock (0 = none). The lag series saturates at 0 while the log clock is AHEAD of wall time, so this is the only sign of a smear."
- the metrics test constructor (`metrics.rs:1477`): `log_clock_smear_ns: Arc::new(AtomicU64::new(0)),`

- [ ] **Step 4: Export the gauge and narrow the lag text**

In `metrics.rs`, add `"uc2_log_clock_smear_ns",` to `CONTRACT_SERIES` (`:39`) directly after `"uc2_log_time_lag_seconds",` (`:94`). Then directly after the `uc2_log_time_lag_seconds` `push_gauge` (`:820–825`) add:

```rust
    push_gauge(
        out,
        "uc2_log_clock_smear_ns",
        "Leader only (0 elsewhere): nanoseconds of a backward wall-clock step the log clock is still retiring by running 500 ppm slow (spec 2026-09-08 §5.3). The log clock is AHEAD of wall time while this is nonzero, so uc2_log_time_lag_seconds reads 0 — this gauge is the only sign of a smear.",
        s.log_clock_smear_ns.load(Ordering::Relaxed),
    );
```

and change the lag gauge's help string (`:821`) to:

```rust
        "Leader only (0 elsewhere): wall clock minus the log's clock, floored at 0. Grows only when nothing is being appended — since 2.12.0 a backward wall-clock step no longer parks it (the log clock smears instead; see uc2_log_clock_smear_ns). Alert: Uc2LogTimeFrozen.",
```

- [ ] **Step 5: Publish from `publish_status` and emit the step event**

In `publish_status`, directly after `self.last_wall_ns = now_ns;` (`:4721`), add:

```rust
        // Spec 2026-09-08 §7: the smear gauge and the step event. Both are
        // off the hot top of the pass — this runs once per pass in step 6,
        // beside the other status stores — and the event body is out of line.
        let smear = self.clock.remaining_smear_ns(self.pass_mono_ns);
        if smear != 0 || self.log_clock_smear_pub.load(Ordering::Relaxed) != 0 {
            self.log_clock_smear_pub.store(smear, Ordering::Relaxed);
        }
        if let Some(step) = self.clock.take_step() {
            self.on_log_clock_step(step, smear);
        }
```

and add the method beside `record_pass_interval` (`:4811`):

```rust
    /// Spec 2026-09-08 §5.3: one line per detected wall-clock step. A forward
    /// step was adopted (every timer due in the skipped interval fires now —
    /// `docs/reference/limits.md`'s unchanged half); a backward step is being
    /// smeared, `smear_ns` remaining.
    #[inline(never)]
    fn on_log_clock_step(&mut self, step: crate::log_clock::Step, smear_ns: u64) {
        use crate::log_clock::Step;
        let (direction, step_ns) = match step {
            Step::Forward(n) => ("forward", n),
            Step::Backward(n) => ("backward", n),
        };
        crate::obs_event!(
            Info,
            "log_clock_step",
            node = self.id as u64,
            direction = direction,
            step_ns = step_ns,
            smear_ns = smear_ns
        );
    }
```

(`obs_event!` takes any `FieldValue::from`-able value — `uc_obs/src/log.rs:261` — and the tree already passes `&str` fields, e.g. `reason = "fetch_expired"` at `node.rs:4188`.)

- [ ] **Step 6: Narrow the alert's comment and annotation**

In `packaging/prometheus/uc2-alerts.yml`, replace the `Uc2LogTimeFrozen` comment block and annotation (`:143–154`) with:

```yaml
  - alert: Uc2LogTimeFrozen
    # Time-and-timers spec §3 + the 2026-09-08 log-clock spec §7:
    # `uc2_log_time_lag_seconds` is leader-only (rendered 0 on followers), so
    # the `uc2_is_leader == 1` join picks out the one instance where a
    # nonzero lag is meaningful. Since 2.12.0 a backward wall-clock step no
    # longer parks the log clock (it smears — see uc2_log_clock_smear_ns), so
    # a grown lag has ONE cause left: nothing is being appended. The rule and
    # threshold are unchanged; the meaning narrowed.
    expr: uc2_log_time_lag_seconds > 5 and on(instance) uc2_is_leader == 1
    for: 30s
    labels: { severity: warning }
    annotations: { summary: "log time on the leader is {{ $value }}s behind wall time — the appender is stalled" }
```

`scripts/m10_alert_fire.sh`'s `log_time_frozen` scenario is unchanged: it parks the cnc word behind wall time, which is still exactly the "stalled appender" state the rule fires on.

- [ ] **Step 7: Run the tests**

Run: `cargo test -p uc_node --lib log_clock_smear_gauge 2>&1 | tail -4` — Expected: 1 passed.
Run: `cargo test -p uc_node 2>&1 | tail -4` — Expected: green (the metrics name-list contract test will have caught a missing name in Step 4).
Run: `promtool check rules packaging/prometheus/uc2-alerts.yml` if `promtool` is installed (`which promtool`); if it is not, say "not verified" in the commit message rather than skipping silently.

- [ ] **Step 8: fmt + clippy, commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -3
git add uc_node/src/node.rs uc_node/src/obs/mod.rs uc_node/src/obs/metrics.rs packaging/prometheus/uc2-alerts.yml
git commit -m "feat(obs): uc2_log_clock_smear_ns gauge + log_clock_step event; Uc2LogTimeFrozen narrows to 'appender stalled'"
```

---

### Task 4: Documentation

**Files:**
- Modify: `docs/reference/limits.md` (`:88`, the "Leader clock discipline" row)
- Modify: `docs/notes/uc2-log-time-and-timers-explained.md` (the "Failure modes" section — `grep -n "^## \|^### " docs/notes/uc2-log-time-and-timers-explained.md` to find it)
- Modify: `docs/ops/uc2-runbook.md` (the metrics/alerts section)
- Modify: `RELEASES.md` (new top section, "2.12.0 (unreleased)"), `docs/releases.md` (matching entry), `CLAUDE.md` (one sentence in the time-and-timers standing-facts bullet)

**Interfaces:** none (prose).

- [ ] **Step 1: Rewrite the limits row**

Replace the row at `limits.md:88` with:

```markdown
| Leader clock discipline is still the operator's, but a step no longer stalls the log. The leader's log clock is `CLOCK_MONOTONIC` plus a sampled epoch offset (resampled every 1 s). A **forward** step is adopted at the next resample and fires every timer due in the skipped interval — unchanged, and not detectable in-band. A **backward** step is never adopted: the log clock keeps advancing, 500 ppm slow, until it has retired the step (a 1 s step takes 2 000 s), visible as `uc2_log_clock_smear_ns` and one `log_clock_step` line. `Uc2LogTimeFrozen` now means only "the appender is stalled". NTP is still the answer, as it is in Aeron; the difference is what happens when it steps | [Log time and timers, explained](../notes/uc2-log-time-and-timers-explained.md#the-log-clock) |
```

- [ ] **Step 2: Add "The log clock" to the explainer**

Add a section `## The log clock` to `docs/notes/uc2-log-time-and-timers-explained.md`, placed before its "Failure modes" section, containing in order: (1) the two-clocks problem (`REALTIME` jumps, `MONOTONIC` has no calendar), (2) `now = MONOTONIC + offset` and WHY the offset is piecewise constant (slew cancels — quote `clock_gettime(2)`: `CLOCK_MONOTONIC` "is affected by frequency adjustments"), (3) forward adopted / backward smeared, with the reframe "the old design already smeared — by stopping", (4) what it costs: one `Instant` read per pass instead of `SystemTime` + `Instant`, (5) what it does NOT do: cross-node agreement (still the leader's clock; `log_time_ns` seeds a new leader), and why the raw TSC was not used (one paragraph pointing at spec §4). Then edit the "Failure modes" section's backward-step entry to say the clock slows instead of freezing, and add `uc2_log_clock_smear_ns` to its observability list. Copy the spec's §2 and §5.3 prose rather than paraphrasing loosely — the spec is the source and the explainer must not drift from it.

- [ ] **Step 3: Runbook, RELEASES, releases, CLAUDE.md**

- `docs/ops/uc2-runbook.md`: in the metrics table add `uc2_log_clock_smear_ns` (leader-only; nonzero = a backward wall step being retired; expect it to fall to 0 at 500 ppm) and in the alerts list change `Uc2LogTimeFrozen`'s one-line meaning to "the appender is stalled (a clock step no longer causes it since 2.12.0)".
- `RELEASES.md`: a new top section `## 2.12.0 (unreleased)` with one feature bullet — "**Monotonic log clock.** The leader's log-time stamp comes from `CLOCK_MONOTONIC` plus a sampled epoch offset: a backward NTP step slows the log clock (500 ppm) instead of freezing it, and the consensus pass takes one clock read instead of two. New gauge `uc2_log_clock_smear_ns`, new event `log_clock_step`; `Uc2LogTimeFrozen` now means only a stalled appender. [Explainer](docs/notes/uc2-log-time-and-timers-explained.md#the-log-clock), [spec](docs/superpowers/specs/2026-09-08-uc2-monotonic-log-clock-design.md)." — and a performance bullet that says the fleet A/B is **pre-committed and not yet run**, linking the Task 5 gate doc. Follow the section shape CLAUDE.md "Release documentation" prescribes.
- `docs/releases.md`: the matching engineering entry — what changed, the two reads → one, the smear parameters, the gate doc link, and "acceptance: fleet A/B, unrun".
- `CLAUDE.md`: in the "Log time and timers (plan 1)" standing-facts sub-bullet, after the sentence about the leader reading its clock once per pass, add one sentence: "Since the (unreleased) `2.12.0` that read is `CLOCK_MONOTONIC` plus a sampled epoch offset (`uc_node::log_clock`): a backward wall step is smeared at 500 ppm, never frozen — spec `docs/superpowers/specs/2026-09-08-uc2-monotonic-log-clock-design.md`."

- [ ] **Step 4: Check the links resolve and commit**

Run: `grep -n "the-log-clock" docs/reference/limits.md RELEASES.md` and `grep -n "^## The log clock" docs/notes/uc2-log-time-and-timers-explained.md` — Expected: the anchor exists.

```bash
git add docs/reference/limits.md docs/notes/uc2-log-time-and-timers-explained.md docs/ops/uc2-runbook.md RELEASES.md docs/releases.md CLAUDE.md
git commit -m "docs: monotonic log clock — limits row rewritten, explainer section, runbook, 2.12.0 writeup skeleton"
```

---

### Task 5: The gate doc — pre-committed fleet A/B, and the dev-box smoke

**Files:**
- Create: `docs/benchmarks/uc2-log-clock-gate-2026-09-08.md`

**Interfaces:** none. Reads spec §8 verbatim for the three readings.

- [ ] **Step 1: Write the gate doc with the bars pre-committed and every result cell "not run"**

Follow the shape of `docs/benchmarks/uc2-time-and-timers-gate-2026-09-03.md` (its preamble on the honest-failure protocol, a rows table, a Results table). Rows:

```markdown
| row | what | bar (pre-committed) | result |
|---|---|---|---|
| a | **fleet A/B**: `m14_fleet_gate.py` rows a/b/e (steady window, `WARMUP_SECS, MEASURE_SECS = 2, 8`), this tree vs its parent commit (the last commit before Task 2), on the same rig, same day, after a same-source rebuild control run FIRST to record the day's resolution (the M14b lesson; 1.12 % on 2026-09-07) | three readings, each a result (spec §8): **gain outside the resolution** (ceiling 2.2 %) → A is a perf win, consensus is the limiter, B-lite gets its own spec; **within the resolution** → null for throughput, ships on behaviour alone, B-lite closed; **regression outside the resolution** → FAIL, does not ship until the cause is found (`objdump -d -C` both binaries' `do_work`) | not run — fleet, user-gated |
| b | **codegen sanity**: count of `clock_gettime` call sites in `do_work`'s reachable code, this tree vs parent (`objdump -d -C` + `readelf -rW`, the row-d playbook) | strictly fewer in this tree | filled in by Task 2 step 6 |
| c | **behaviour**: `cargo test -p uc_node --lib log_clock` — steady state, forward adopted, backward smeared at 500 ppm and fully retired at 2 000 s/s, monotone under 20 000 seeded random steps | all green | filled in by Task 1 |
| d | **dev-box SMOKE, not a gate**: `m12_gate --arm direct --secs 8` alternated A/B/B/A/A/B on an idle box, private target dirs, sha256 of each binary recorded | reported, **no bar** (CLAUDE.md: a local rate is smoke; the same dip measured 7× spanned 0–18 % on a dev box) | filled in by step 2 below |
```

Add a "Procedure" section that spells out row a's exact commands, copied from the time-and-timers gate doc's own "run procedure" for rows a/b/e, with `--tt-rows` omitted and the two arms named by commit SHA; and a "Why `hop1_ab.sh` is not the harness" paragraph (it drives `dummy-node`; the consensus agent is not in its path).

- [ ] **Step 2: Run the dev-box smoke and record it**

```bash
W=/home/claude/ultima/ultima_cluster/.claude/worktrees/claude-2
BASE=$(git -C $W log --format=%H -1 HEAD~4)   # the commit before Task 1 — check with git log that it is
for arm in base head; do
  sha=$([ $arm = base ] && echo $BASE || echo HEAD)
  git -C $W worktree add -f $HOME/scratch/logclock-$arm $sha 2>/dev/null || true
  CARGO_TARGET_DIR=$HOME/.cache/cargo-target-logclock-$arm cargo build --release --manifest-path $HOME/scratch/logclock-$arm/Cargo.toml -p uc_gateway --example m12_gate
  cp $HOME/.cache/cargo-target-logclock-$arm/release/examples/m12_gate $HOME/scratch/m12_gate.$arm
  sha256sum $HOME/scratch/m12_gate.$arm
done
for arm in base head head base base head; do
  $HOME/scratch/m12_gate.$arm --arm direct --secs 8 --root $HOME/scratch/logclock-smoke-$arm 2>&1 | grep '^RESULT' | sed "s/^/$arm /"
done
```

Record in row d's result cell: the six `responses_per_sec` values by arm, the two means and the ratio, both sha256s, and the words **"dev-box smoke, not a gate"**. Then remove the two scratch worktrees (`git worktree remove --force $HOME/scratch/logclock-{base,head}`) and the `logclock-smoke-*` roots.

- [ ] **Step 3: Commit**

```bash
git add docs/benchmarks/uc2-log-clock-gate-2026-09-08.md
git commit -m "docs(gate): log-clock gate doc — fleet A/B pre-committed with three readings (unrun), codegen and behaviour rows filled, dev-box smoke recorded"
```

---

## Self-review

**Spec coverage.** §2 (offset, slew cancels) → Task 1 module doc + `LogClockCore`. §5.1 (bracket sampling, Agrona constants) → `bracket_sample`, `INIT_*`, `RESAMPLE_*`. §5.2 (one read; Tick derived; conditional sites left alone — errata) → Task 2 (c)(d)(e)(f). §5.3 (forward adopt, backward smear, never backwards) → `resample`, tests 2–7 and the seeded-random test. §5.4 (private module in `uc_node`) → Task 1 `mod log_clock;`. §7 (lag keeps its meaning; alert narrows; gauge/event per errata) → Task 3. §8 (2.12.0; fleet A/B three readings; codegen check) → Task 5 row a, Task 2 step 6 / row b. §9 parameters → Global Constraints + Task 0 errata; suspend → covered as a forward step (stated as asserted-not-tested in the errata). Out of scope (§9): cross-node agreement, B-lite, the clamp — no task touches them.

**Placeholder scan.** No TBD/TODO. Every identifier a task uses from the existing tree was read this session and is cited with its line (`harness()` at `node.rs:9599`, `synthetic_sources()` at `metrics.rs:1438`, `CONTRACT_SERIES` at `metrics.rs:39`, `obs_event!` at `uc_obs/src/log.rs:254`). Task 4 step 2 gives the section's ordered contents rather than its full prose, deliberately — the spec is the prose source and the step says to copy from it.

**Type consistency.** `LogClockCore::{new(u64,u64), wall_at(&mut,u64)->u64, due_for_resample(&,u64)->bool, skip_resample(&mut,u64), resample(&mut,u64,u64)->Option<Step>, remaining_smear_ns(&,u64)->u64}` and `LogClock::{new(), mono_now(&)->u64, wall_at(&mut,u64)->u64, take_step(&mut)->Option<Step>, remaining_smear_ns(&,u64)->u64}` are used with exactly those shapes in Tasks 2 and 3. `pass_clock(&mut self, mono_ns: u64) -> u64` is `&mut` in both cfgs because `wall_at` resamples. `Step::{Forward(u64), Backward(u64)}` matches `on_log_clock_step`.
