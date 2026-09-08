# UC v2 — A monotonic, wall-clock-anchored log clock

**Date:** 2026-09-08
**Status:** design brainstormed in chat 2026-09-08, with a measurement probe
run between the approaches section and this document (§3); **amended the same
day** after the maintainer's challenge re-priced the saving against fleet
numbers (§3, §4, §8 — the first draft called the perf case "null" from a local
run three orders of magnitude below peak; it is a conditional 2.2 %). Approved
by the maintainer 2026-09-08 ("sounds reasonable"). Next: the implementation
plan.
**Baseline:** local `main` / worktree `claude-2` at `0ef7ddc` — the `2.11.0`
flag day merged but unreleased (wire `0.7.0`, cnc `3.1`); `2.10.0` is what
is shipped.
**Requested by:** the maintainer, 2026-09-08 ("a fast, monotonic,
wall-clock-anchored ns counter, that works both on Arm in Intel"). Not a
ranked `docs/BACKLOG.md` item.
**Release:** `2.12.0`. `2.11.0` is **tagged** on `main` (`4855f36`); this
touches the consensus agent's hot loop and ships in the next minor — §8.

## 1. Goal and locked decisions

Since the (unreleased) time-and-timers work, every log frame carries a
`time_ns` stamp the leader writes at append, and every replica applies that
same value as `ctx.time_ns`. The stamp is read from the wall clock —
`wall_now_ns()` at `uc_node/src/node.rs:3255`, one reading per consensus
pass — and made monotonic downstream by a clamp in the appender,
`stamp = max(now, last_stamp)` (`uc_log/src/buffer.rs:632`, and identically
at `:701` and `:783`; the TIMER frame at `:846` takes the same clamp against
its own deadline, `body.deadline_ns.max(last_stamp)`).

That clamp is the whole monotonicity guarantee, and it works. What it costs
is recorded as a shipped limitation in `docs/reference/limits.md:88`:

> Leader clock discipline is the operator's. A backward step is clamped (the
> log's time freezes until wall time catches up) and alerted
> (`Uc2LogTimeFrozen`); a forward step is not detectable in-band and fires
> every timer in between. NTP is the answer, as it is in Aeron.

So a backward NTP step does not corrupt the log — it **freezes** it. For the
duration of the step every frame takes the same stamp, every timer due in
that window fires late, and `uc2_log_time_lag_seconds` grows until wall time
walks back up to `last_stamp`.

This spec replaces the clock the stamp is read from, so that a backward step
**slows** the log clock instead of **stopping** it, and so that the log
clock still converges back to UTC afterwards. The clamp stays exactly as it
is; nothing about the frame, the wire, cnc, or the FSM surface changes.

| decision | choice | why (§) |
|---|---|---|
| clock shape | monotonic source + a sampled epoch **offset**: `now = MONOTONIC + offset` | §2 |
| monotonic source | the kernel's `CLOCK_MONOTONIC` (`std::time::Instant`), **not** a raw hardware counter | §3, §4 |
| why not the raw counter first | A is the safe, portable half with a **2.2 % ceiling**; the raw counter ("B-lite", re-anchored every K passes) adds ~1.3 % on top for an `unsafe` per-arch surface, and is a **follow-on gated on A's fleet result** | §3, §4 |
| what A actually removes | the consensus agent's **second** unconditional clock read per pass — `pass_now_ns` (wall) and `now_ns` (monotonic) are both derived from ONE `Instant` read | §3, §5.2 |
| offset sampling | bracket a wall read between two monotonic reads; keep the narrowest bracket | §5.1 |
| forward step | **adopt immediately** — monotonic-safe, and identical to today's behaviour | §5.3 |
| backward step | **never adopt as a step**; smear — run the derived clock slow until it re-converges with UTC | §5.3 |
| cross-arch | free: `Instant` is portable, no intrinsics, no `unsafe`, no calibration, no fallback path | §4 |
| wire / cnc / FSM surface | **unchanged**. No frame change, no cnc word, no `ApplyCtx` change, no new metric series required | §1, §7 |
| release | the minor **after** `2.11.0` | §8 |

### What does not change

- The `max(now, last_stamp)` clamp in `uc_log::Appender`. It remains the
  monotonicity guarantee of record; this spec only stops relying on it as a
  step-absorber.
- The one-reading-per-pass discipline (time-and-timers spec §3.2). The new
  clock is read in exactly the same place, `pass_clock()` at
  `uc_node/src/node.rs:3268`, once per pass.
- Seeding at leader open from the cnc `log_time_ns` word
  (`Archive::recovered_log_time_ns()`, `uc_log/src/archive.rs:254`), and the
  archive's never-lower rule.
- `uc2_log_time_lag_seconds` and the `Uc2LogTimeFrozen` alert
  (`packaging/prometheus/uc2-alerts.yml:142`). The metric keeps its meaning;
  what changes is that a healthy node stops producing the condition.
- Followers. They write frames verbatim and read no clock; this is a
  leader-side change only.

## 2. Why a monotonic source plus an offset

Linux offers two clocks and neither one is sufficient alone:

- **`CLOCK_REALTIME`** (`SystemTime`) answers *what time is it* — epoch
  nanoseconds — but can **jump** in either direction when NTP corrects it or
  an operator sets the clock. Not monotonic.
- **`CLOCK_MONOTONIC`** (`Instant`) never jumps, but carries no calendar
  meaning. The schedule table's `at {secs_of_day}` rule is a time of day, so
  an unanchored counter cannot serve it.

The log stamp needs both properties, so the clock is built by composition:

```
offset = REALTIME − MONOTONIC        // sampled, §5.1
now()  = MONOTONIC + offset          // read once per consensus pass
```

The result counts like `CLOCK_MONOTONIC` and reads like `CLOCK_REALTIME`.

**The offset is nearly constant, and that is the load-bearing fact.** NTP
corrects a clock two ways: by **slewing** (running it imperceptibly fast or
slow, continuously) and by **stepping** (a discontinuous jump, rare). Linux
applies **slew to both** clocks — `clock_gettime(2)` states that
`CLOCK_MONOTONIC` "is not affected by discontinuous jumps in the system time
but is affected by frequency adjustments" — and applies **steps only to
`REALTIME`**. In the subtraction `REALTIME − MONOTONIC`, the slew cancels.

So the offset is piecewise constant: it moves only at step events and across
suspend. There is no continuous drift to chase, which is why this design is
a subtraction and not a control loop.

## 3. The measurements, and what they bound

Run on the dev box 2026-09-08 (AMD Ryzen AI MAX+ 395, clocksource `tsc`,
CPU flags `constant_tsc nonstop_tsc rdtscp`), pinned to one core, N=20 M
iterations × 5 reps, throwaway probe under a private `CARGO_TARGET_DIR`.
**Dev-box smoke per CLAUDE.md's benchmarking discipline — no rate bar is set
by it, and it must be re-read on the fleet before any bar quotes it.**

Cost of one clock read:

| | mean | vs today |
|---|---|---|
| `SystemTime::now()` → ns — today | 20.11 ns | — |
| **A**: `Instant` + offset | **19.46 ns** | −0.65 ns (null) |
| raw `rdtsc` | 7.66 ns | |
| raw `lfence`+`rdtsc` | 13.16 ns | |
| raw `rdtscp` | 15.27 ns | |
| **B**: `rdtsc`·mult>>32 + offset | **7.41 ns** | −12.70 ns (2.7×) |
| B, fenced | 13.83 ns | −6.28 ns |

Measured counter rate 3.0001 GHz (0.333 ns/tick).

**How many reads a pass takes today — corrected from the first draft, which
counted one.** The consensus agent takes **two** unconditional clock reads
per pass: `SystemTime::now()` for the log stamp (`pass_clock()`,
`node.rs:3268`) and `Instant::now()` for `Event::Tick`'s `now_ns`
(`node.rs:3497` → `now_ns()` at `:4726`); three more `now_ns()` calls at
`:4549`, `:4612` and `:6306` are conditional. The sender takes one
unconditional `Instant::now()` per pass (`uc_net/src/sender.rs:1043`, the
heartbeat check); the receiver's reads are all conditional. So the
consensus agent pays **≈ 38.7 ns per pass** today, not 20.

Drift of B against the wall clock with no resampling: **−293 ns/s ≈ 0.29 ppm
≈ 1.05 ms/hour**.

Consensus pass rate, three nodes + three services + one `client-direct`,
all on the one box, leader read off `uc2_consensus_pass_ns`:

| | pass rate | mean pass | today's read | B would save |
|---|---|---|---|---|
| idle | 2.121 M/s | 471.5 ns | 4.3 % of a pass | 2.7 % |
| under load (165 983 resp/s) | 1.401 M/s | 713.9 ns | 2.82 % of a pass | **1.78 %** |

**What the local run does and does not say.** Under load the consensus
agent ran 1.401 M passes to deliver 165 983 responses — 8.4 passes per
response, i.e. not saturated. The first draft read that as "the clock
converts to no throughput." That inference does **not** transfer: 166 k/s
is three orders of magnitude below the fleet's peak, and at peak the agent
is doing real work every pass. The local run bounds the *per-read cost*; it
says nothing about whether the consensus agent is the limiter at peak.

**The fleet framing, from numbers already on `main`.** Under row b's load
the leader's pass was **mean 881.59 ns** (the time-and-timers gate's row c
results cell), on a rig doing **2 625 636 resp/s** the same day (its
`hop1_ab.sh` resolution cell), i.e. ~1.13 M passes/s and **~2.3 commands
per pass** — the leader batches (`INGRESS_PER_CYCLE = 256`,
`node.rs:306`), so the clock is read once for all of them. The tree's peak
is 3 437 529 resp/s (`uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md`, c8id
unpinned). Amortized either way, today's two reads are

```
38.7 ns / 881.59 ns per pass  =  4.4 %
```

of the pass — the ceiling with *zero* reads, which is not achievable. Against
the rig's measured same-source rebuild resolution of **1.12 %**:

| | reads/pass | ns/pass | saving | ceiling, IF consensus-bound |
|---|---|---|---|---|
| today | 2 (`SystemTime` + `Instant`) | 38.7 | — | — |
| **A**: one `Instant` read, derive both | 1 | ~19 | 19.7 ns | **2.2 %** |
| **B-lite**: one `rdtsc`/`CNTVCT`, re-anchor every K passes | 1 + 1/K | ~7.6 | 31 ns | **3.5 %** |
| zero reads | 0 | 0 | 38.7 | 4.4 % |

**Whether the ceiling is realised is unknown and is the thing the fleet A/B
in §8 exists to find out.** Nothing in the gate docs isolates the consensus
hop; if it is the limiter at peak, pass length *is* throughput and the
saving converts one-for-one; if it is not, the saving is null, per
CLAUDE.md's hop-isolation doctrine. Both A and B-lite clear the 1.12 %
resolution, so the A/B can tell.

**Stated limits.** The per-read costs are one dev box, x86 only, pinned. The
ARM read cost was not measured; it is irrelevant to A (`Instant` is
portable) and is the first thing B-lite would need.

## 4. A vs B — the two approaches, why A goes first, and B-lite

Both approaches have the same shape: a monotonic counter plus a fixed offset
that turns it into epoch nanoseconds. They differ in **one substitution** —
where the monotonic counter comes from — and everything expensive about B
follows from that swap.

**A — anchor the kernel's `CLOCK_MONOTONIC`.** `now = Instant + offset`, as
§2. One conversion (the epoch offset), sampled once and refreshed only at
step events, because slew cancels out of the subtraction.

**B — anchor the raw hardware counter.** Read `rdtsc` on x86 or
`CNTVCT_EL0` on ARM and skip the kernel entirely. That yields a raw tick
count which is neither nanoseconds nor an epoch, so B needs **two**
conversions: a **rate** (ticks → ns, as an integer `mult`/`shift` pair) and
an **epoch offset**. ARM self-describes its rate in `CNTFRQ_EL0`; x86 does
not, so x86 must calibrate against a reference clock.

B is genuinely faster — 7.41 ns against 20.11 ns — because today's
`SystemTime::now()` already reads the TSC (this box's clocksource *is*
`tsc`); what B removes is the vDSO's seqlock read of the kernel timekeeping
struct, the mult/shift, and `Duration` construction.

**B as first drafted — anchor once, resample on agrona's hourly cadence —
is rejected on two grounds:**

1. **B reintroduces the failure mode this spec exists to remove.** B's
   `mult` is a snapshot of the rate; the kernel's is continuously
   NTP-corrected. So B drifts against UTC — measured 0.29 ppm, ~1 ms/hour —
   and correcting that by resampling the *offset* produces a step. A
   **backward** resample step hits `max(now, last_stamp)` and freezes the
   log. So B may not step; it must correct the **rate** and smear, from a
   noisy continuous measurement — a phase-locked loop. That is what `ntpd`
   is, and B would run a second one inside the node, in disagreement with
   the first.
2. **Arch surface.** Intrinsics and ordering fences (`lfence`/`rdtscp` on
   x86 — which halves the saving to 6.28 ns; `isb` on ARM), invariant-TSC
   detection with a fallback path, cross-socket synchronisation, and suspend
   handling. All `unsafe`, all per-arch, all needing their own tests.

**A is a subtraction; B-as-drafted is a control system.** A takes the entire
behavioural win — no freeze, UTC re-convergence, identical on ARM and Intel,
no `unsafe`, no calibration, no fallback — and removes one of the two reads
(§5.2), for a 2.2 % ceiling.

**B-lite — the variant the first draft missed, kept as a follow-on.** The
PLL objection assumed agrona's hourly resample. Re-anchor the raw counter to
the vDSO clock every **K passes** instead — K = 100 is ~90 µs at the fleet's
pass length — and the resample step is bounded by
`drift × interval ≈ 0.29 ppm × 90 µs ≈ 0.03 ns`: zero in integer
nanoseconds, so the appender clamp never sees a backward step and there is
no drift to chase. No PLL. What remains of B's cost is exactly ground 2, the
`unsafe` per-arch surface, bought for **~1.3 % over A** — about one
resolution unit. That is not worth doing blind, and it is not worth doing at
all if A's fleet A/B reads null (which would show the consensus agent is not
the limiter). So: **A ships first; B-lite is taken up only if A's A/B shows a
gain**, and then as its own spec-and-plan with the ARM read cost measured
first.

**One consequence worth recording:** choosing A first removed the need for
the ARM fleet probe that was planned. The maintainer's report that Graviton's
`CNTFRQ_EL0` runs at 1 GHz (1 ns/tick, against the 24–100 MHz that the
public surveys quote) could not be confirmed from public sources and remains
**unverified**; it matters only for B-lite, and `Instant::now()` is
portable with no hardware-counter question in A. If B-lite is taken up,
reading `CNTFRQ_EL0` on one Graviton host is the first step.

## 5. The design

### 5.1 Sampling the offset

Two clocks cannot be read at the same instant, so the offset is sampled by
**bracketing**:

```
m0   = MONOTONIC
w    = REALTIME
m1   = MONOTONIC
```

The true offset lies in `[w − m1, w − m0]`, and `m1 − m0` is the sample's
error bound. Take several samples and keep the one with the narrowest
bracket; accept early once a bracket comes in under a threshold. This is
Agrona's `OffsetEpochNanoClock` method (§6), whose constants —
`DEFAULT_MAX_MEASUREMENT_RETRIES = 100`,
`DEFAULT_MEASUREMENT_THRESHOLD_NS = 250` — are the starting point, to be
confirmed against a measured bracket distribution on this platform rather
than adopted on faith.

The offset is `i128` or a signed pair, not `u64`: `REALTIME − MONOTONIC` is
a difference between two unrelated origins and its sign is not guaranteed a
priori.

### 5.2 Reading it

`pass_clock()` (`uc_node/src/node.rs:3268`, with the `#[cfg(test)]`
twin at `:3276`) becomes one monotonic read plus one add, in exactly the
place `wall_now_ns()` is read today — once per pass, at the top of
`do_work`. No caller changes; `pass_now_ns`, `set_now`, timer deadline
comparisons and `record_pass_interval` all consume the same `u64` they
consume now.

**And the pass's second read goes away.** `Event::Tick`'s `now_ns`
(`node.rs:3497`, today a separate `Instant::now()` via `now_ns()` at
`:4726`) is derived from the **same** monotonic reading the stamp was — the
raw `Instant` delta before the offset is added. One read per pass yields
both values; that single removed read is the whole of A's measurable
saving (§3). The conditional `now_ns()` sites (`:4549`, `:4612`, `:6306`)
may keep their own reads or take the pass value — the plan decides, with the
constraint that a value used for an interval must stay monotonic (it is
either way, since the source is `Instant`).

A side effect worth naming: `uc2_consensus_pass_ns` and
`uc2_timer_lateness_ns` are today *differences of `CLOCK_REALTIME`*
(`record_pass_interval`, `node.rs:4811`; lateness at `:4913`). Under A they
become differences of a monotonic-sourced value, which is what a duration
should be; a forward wall-clock step no longer lands in either histogram.

The `#[cfg(test)]` override on `pass_clock` stays as it is, so every
existing test keeps its behaviour and the new step/smear paths get the same
deterministic seam.

### 5.3 Step detection and response

Periodically — on a cadence, not on the hot path — re-sample the offset and
compare with the held one. The difference is the step (slew having cancelled
out, §2). Three cases:

- **No meaningful change** (inside the sampling bracket's noise): keep the
  held offset. This is the steady state.
- **Forward step** (wall time jumped ahead): **adopt immediately.** The
  derived clock jumps forward with it. This is monotonic-safe and is exactly
  what happens today, including the documented consequence that every timer
  due in the skipped interval fires at once (`limits.md:88`).
- **Backward step** (wall time jumped back): **do not adopt.** Adopting
  would move the derived clock backwards, the clamp would absorb it, and the
  log would freeze — the failure mode this spec removes. Instead **smear**:
  hold a correction term and retire it gradually by running the derived
  clock slightly slow, until the derived clock and UTC agree again.

**Why smear rather than simply hold the old offset.** Holding leaves the log
clock permanently ahead of UTC by the step size, which the `at
{secs_of_day}` daily rule cannot tolerate — a daily job would fire at the
wrong time of day, forever. Smearing keeps the log clock monotonic *and*
returns it to UTC. The rate and the ceiling on the smear are open
parameters — see §9.

The framing worth keeping in the explainer: **today's design already smears,
by stopping.** `max(now, last_stamp)` is the crudest possible smear — it
halts the clock until UTC catches up. This spec replaces "stop dead" with
"run slightly slow," same destination, no stall.

### 5.4 Where the clock lives

A private module in `uc_node` — **not** a new publishable crate. `uc_node`
is the only reader (`uc_log::Appender` is *given* the reading, it does not
take one), the workspace already publishes 13 crates in lockstep, and the
publish order flipped once this flag day already. If a second consumer ever
appears, promoting it is a later, cheap decision.

## 6. External software and solutions considered

Searched 2026-09-08. **No existing crate does all three of monotonic,
wall-clock-anchored, and cross-arch**; the two that anchor to the epoch are
x86-only, and the two that cover ARM are raw counters with no epoch and no
discipline.

| candidate | source | arch | wall-anchored | verdict |
|---|---|---|---|---|
| `std::time::SystemTime` | vDSO `clock_gettime(CLOCK_REALTIME)` | all | yes | **what UC uses today.** Not monotonic — the whole problem |
| `std::time::Instant` | `CLOCK_MONOTONIC` | all | no | **the monotonic half of approach A** |
| [`coarsetime`](https://github.com/jedisct1/rust-coarsetime) | `CLOCK_MONOTONIC_COARSE`, cached | all | via offset | **rejected**: fastest of all (a load), but tick-granularity (~1–4 ms). Coarser than the timer deadlines it would stamp |
| [`minstant`](https://docs.rs/minstant/) | TSC, with an `Anchor` for unix nanos | **Linux x86/x86_64 only** | yes | **rejected**: closest in intent, wrong arch coverage — "only the Linux on `x86` or `x86_64` is backed by TSC", falls back to coarse elsewhere, i.e. silently coarse on the ARM fleet hosts |
| [`quanta`](https://docs.rs/quanta/) | TSC, calibrated against a reference clock | **x86_64 + SSE2 only** | yes | **rejected**: same arch gap, plus documented "raw values may time warp", first-`Clock` construction blocking up to 200 ms, and "does not track time across system suspends" |
| [`tick_counter`](https://github.com/sheroz/tick_counter), [`tsc-trace`](https://github.com/koeninger/tsc-trace) | `rdtsc` **and** `CNTVCT_EL0` | x86_64 + aarch64 | **no** | **rejected as a clock**: the only crates covering both arches, but they are raw tick counters for benchmarking — no epoch, no monotonicity contract, no discipline. They would be the *input* to approach B, not a substitute for it |
| the Linux vDSO itself | — | all | yes | **kept, by choosing A.** The vDSO already does what a fast-clock crate does — reads the hardware counter in userspace, no syscall — with the kernel's NTP-disciplined mult/shift. Approach B's entire saving is the cost of that discipline |

**The design precedent, and the closest complete match, is not a Rust crate:
Aeron's own** [`OffsetEpochNanoClock`](https://github.com/aeron-io/agrona/blob/master/agrona/src/main/java/org/agrona/concurrent/OffsetEpochNanoClock.java)
(agrona), which is structurally approach A: anchor `nanoTime()` to the epoch
via a sampled offset, bracket the wall read between two monotonic reads,
keep the narrowest bracket, accept early under a 250 ns threshold, give up
after 100 retries, and resample on a 1 hour interval **or immediately when
the adjustment goes negative**. UC is already Aeron-shaped elsewhere and the
time-and-timers spec §2 already reasons against Aeron's clock behaviour, so
following it here is consistent.

**Where this design departs from agrona:** agrona resamples on a negative
adjustment and takes the correction as a **step**. UC cannot — a backward
step is absorbed by the appender clamp and freezes the log (§5.3). Hence the
smear, which is UC's own addition and the one genuinely novel piece.

## 7. Observability

No new series is required. Two changes in meaning, both improvements:

- `uc2_log_time_lag_seconds` (`uc_node/src/obs/metrics.rs:820`) keeps its
  definition — wall clock minus the log's clock, leader-only. Under this
  design a backward step no longer parks it at the step size for the step's
  duration; it shows the smear retiring.
- `Uc2LogTimeFrozen` (`packaging/prometheus/uc2-alerts.yml:142`, `> 5s for
  30s`) keeps its rule and its threshold. What changes is that a healthy
  node with a stepped clock stops firing it, so the alert narrows to what it
  should have meant all along: **the appender is stalled**.

Whether to add a counter for adopted/smeared steps is left open (§9); the
argument against is that steps are rare and the lag series already shows
them.

## 8. Release and acceptance

**`2.12.0`.** `2.11.0` is tagged on `main` (`4855f36`, "2.11.0 is tagged");
this worktree's baseline `0ef7ddc` predates that, so the plan rebases onto
the tagged `main` first. This change touches the consensus agent's hot loop
— the M14a lesson, re-learned in the 2026-09-07 row-d regression, is that
such changes must be A/B'd on exact binaries because codegen alone has cost
9 % for arms that never execute. No wire, cnc, or API change, so it is an
ordinary minor by `docs/reference/semver-policy.md`.

**Acceptance is a fleet A/B, and it is pre-committed here.** Under
`m14_fleet_gate.py`'s row a load (steady window), this tree against its
parent, measured the way `scripts/hop1_ab.sh` measures — exact binaries,
back to back, with a same-source rebuild control run first to establish the
day's resolution (the M14b lesson; 1.12 % on 2026-09-07). Three readings are
possible and each is a result, not a failure of the method:

- **Gain outside the resolution** (ceiling 2.2 %, §3) — A is a perf win, the
  consensus agent is the limiter at peak, and **B-lite is worth its own
  spec** (§4).
- **Within the resolution** — A is null for throughput and ships on its
  behavioural merits alone (no freeze, one read, monotonic durations);
  **B-lite is closed**, since the agent is not the limiter.
- **Regression outside the resolution** — a FAIL, recorded as such; the
  change does not ship until the cause is found (the row-d playbook:
  `objdump` the two binaries' `do_work`, since the probes could not see the
  last regression and the machine code could).

Behavioural acceptance is separate and deterministic: the step/smear paths
under the `pass_clock()` test seam (§5.2), plus a synthetic
`Uc2LogTimeFrozen` scenario in `m10_alerts.rs` showing the alert no longer
fires on a stepped-but-healthy leader.

## 9. Open parameters and out of scope

Open, to be settled in the plan with a measurement rather than a guess:

- **The smear rate**, and its ceiling. Too slow and a large step takes hours
  to retire; too fast and the log clock's rate is visibly wrong. `ntpd`'s own
  slew rate is the obvious reference point.
- **The resample cadence.** agrona uses 1 hour. UC's constraint is different
  — it needs to *detect* steps, not track drift — so a shorter cadence may be
  right, and it must stay off the hot path either way.
- **The bracket threshold and retry count** (§5.1), against a measured
  bracket distribution rather than agrona's constants adopted on faith.
- **Whether to count adopted/smeared steps** as a metric (§7).
- **Suspend.** `CLOCK_MONOTONIC` does not advance across suspend, so the
  offset shifts by the suspend duration and reads as a forward step, which
  §5.3 adopts. That is believed correct — the log clock should reflect the
  time that really passed — but it is asserted here, not tested, and the plan
  must cover it.

Out of scope:

- **Cross-node clock agreement.** Nothing here makes two nodes' clocks agree;
  the log clock is still the *leader's* clock, and `max(now, last_stamp)`
  plus the `log_time_ns` seed is still what carries it across a failover.
  Bounded cross-node skew would be a different feature.
- **B-lite** (§4). Taken up only if A's fleet A/B (§8) reads a gain outside
  the resolution — that reading is what shows the consensus agent is the
  limiter. Its own spec and plan; first step, reading `CNTFRQ_EL0` on a
  Graviton host and measuring the ARM read cost.
- **The clamp.** `max(now, last_stamp)` stays. It is the guarantee of
  record and this design deliberately does not replace it — it stops
  *depending* on it for step absorption.

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
- **§7 / the plan's errata bullet above, "leader-only".** Wrong as written:
  `LogClock` runs on every node and persists across promotion, so a
  follower's smear is the clock it would lead with after a failover.
  `uc2_log_clock_smear_ns` and `log_clock_step` are therefore **per-node**,
  deliberately not leader-gated (Ruling R6, Task 3 review): hiding a
  follower's smear would discard the one reading an operator wants before a
  failover. Nothing about stamps changes — only the leader's clock ever
  reaches the log.
- **§5.2, "no longer lands in either histogram".** False as built. The first
  draft claimed a forward step "no longer lands in either histogram" once
  the clock read moved to `LogClock`. It does: `record_pass_interval`
  (`uc2_consensus_pass_ns`) and the timer-lateness histogram
  (`timer_stats.lateness_ns`, `node.rs:4989`) both subtract `pass_now_ns`
  from a prior or deadline value, and `pass_now_ns` is exactly the value a
  forward step ADOPTS — so an adopted forward step still lands in both
  series as one outsized sample, same as before this change. Separately,
  the first errata bullet above undercounted the conditional `now_ns()`
  call sites at **three**; the actual count in `node.rs` is **five**
  (`:4591`, `:4654`, `:6190`, `:6316`, `:6382` — the read-barrier timeout,
  the timer-heap arm path, and related cold callers), none of them folded
  into the pass reading, for the same reason the original three were not.
- **§8, the synthetic `m10_alerts.rs` scenario.** Not built, and none exists
  in this release. §8 sketched a
  `Uc2LogTimeFrozen`-no-longer-fires-on-a-stepped-but-healthy-leader
  scenario for `scripts/m10_alert_fire.sh`'s harness; it was never written.
  It would need a manipulated system clock to drive a real step through
  `bracket_sample`, and the exporter's lag arithmetic
  (`wall.saturating_sub(log_time)`) is unchanged by this feature — a
  synthetic scenario built on the existing frozen-log-time fault injector
  would prove only that the arithmetic it already proves still holds, not
  anything new about the smear. The gate doc
  (`docs/benchmarks/uc2-log-clock-gate-2026-09-08.md`) says so plainly in
  its coverage statement, and an alert on `uc2_log_clock_smear_ns` itself is
  a `docs/BACKLOG.md` item, not part of this release.
- **§9, no smear ceiling in this release.** §9 raised, and left open, whether
  a smear above some ceiling should page. Decided: none, for `2.12.0`. A
  backward step of any size is smeared at the fixed `SMEAR_PPM = 500` rate —
  a 1 h step takes roughly 83 days to fully retire (`3600 s / (500 / 1e6)`)
  — during which the log's time runs increasingly off true UTC and any
  `at {secs_of_day}` schedule-table rule fires late until the smear is
  retired (§5's daily rule, unaffected in kind, only in how far off UTC the
  log clock is meanwhile). `uc2_log_clock_smear_ns` is the only signal an
  operator has for this, and nothing pages on it — the alert plus its
  `scripts/m10_alert_fire.sh` builder and scenario is the same
  `docs/BACKLOG.md` item named in the bullet above.
- **C1, the resample baseline bug and its fix.** As first built,
  `LogClockCore::resample` compared the fresh wall sample against `derived`
  — the clock's own current value — rather than against `derived −
  remaining_smear_ns`. During a smear the derived clock is DELIBERATELY
  ahead of the wall by the remaining smear (that is the whole point of
  smearing), so comparing against `derived` re-detected that same, expected
  gap as a brand-new backward step on every 1 s resample: the smear grew
  instead of counting down, `log_clock_step` fired every second instead of
  once, and a forward step arriving mid-smear was never adopted (it was
  read as an even bigger backward step). The fix subtracts the remaining
  smear from the gap before deciding whether anything changed —
  `step = (derived − wall) − remaining` — so a smear in flight is silent
  unless the wall moves by more than what the smear already accounts for; see
  `uc_node/src/log_clock.rs`'s `resample` for the corrected algorithm and
  its doc comment for the reasoning.
