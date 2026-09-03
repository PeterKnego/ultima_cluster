# uc2 time-and-timers gate — SKELETON, no fleet run yet

**Date:** 2026-09-03 (bars committed). **Fleet run: NOT RUN — release on
hold.** Log time and timers (plan 1) land on the same unreleased `2.11.0`
flag day as FSM identity; the maintainer has not green-lit fleet spend for
either gate.

> **Decide rule committed before any run.** This document's bar table is
> committed, with every result cell empty, **before** any fleet run
> against it — the honest-failure protocol carried forward from
> M7/M9/M10/M11/M12/M13/M14/M14c2 and from
> [the FSM identity gate skeleton](uc2-fsm-identity-gate-2026-09-02.md).
> Nothing in the bar may be edited to match a result: a run that misses the
> bar is recorded as a FAIL and keeps the bar. This document itself is a
> placeholder — its own commit message says so — and must not be read as
> "gated" until a fleet run fills in the results table below.

## What the gate measures

Spec:
[`docs/superpowers/specs/2026-09-02-uc2-time-and-timers-design.md`](../superpowers/specs/2026-09-02-uc2-time-and-timers-design.md)
§8 ("Fleet gate"), §9. Plan 1 puts a leader-written `time_ns` stamp in every
log frame header and adds a scheduler: a `TIMER` frame type, a per-row node
heap, `ApplyCtx::{schedule, cancel}`, a provided `on_timer` on both tiers,
and `uc_service::Timed<S>` for exactly-once delivery. Plain-language
explainer:
[`docs/notes/uc2-log-time-and-timers-explained.md`](../notes/uc2-log-time-and-timers-explained.md).

Two claims need a fleet to test, and this gate exists for exactly those:

1. **The stamp is free.** The spec's cost claim is "one vDSO clock read and
   one heap peek per leader pass" (§3.2), explicitly recorded as *to be
   measured, not asserted*.
2. **The scheduler is precise enough to be useful, and says so honestly.**
   The spec deliberately promises no per-timer precision guarantee (§10);
   the contract is "never early; on time or marked late". So the gate
   *measures* the distribution rather than asserting a number.

**Where the code lands matters for row d.** Unlike FSM identity, this work
does touch two hot loops:

- The leader's consensus pass (`uc_node/src/node.rs`) gains one wall-clock
  read per pass, one heap peek per pass per declared row, and a drain of the
  per-row `svc_sched` SPSC rings (an empty SPSC poll is one load).
- The service's apply loop (`uc_service/src/apply.rs`) gains the `time_ns`/
  `term` fill on every `ApplyCtx`, a `TIMER`-type branch in the frame-type
  dispatch, and the `take_sched_records` drain after each call.

CLAUDE.md's standing M14a lesson is that **code in a hot loop's body costs
even on paths that never run** (a wait ladder added inline to the apply loop
cost 9 % at N=1, a path N=1 never executes, through codegen alone; out of
line it cost 1.5 % — [`uc2-m14a-apply-hop-2026-08-27.md`](uc2-m14a-apply-hop-2026-08-27.md)).
That lesson is why row d exists: an isolated apply-hop A/B, run under
`scripts/hop1_ab.sh`'s same-source rebuild discipline, is the only honest way
to say the added frame-loop body is free.

**Coverage statement.** This gate measures throughput cost and timer
precision on a fleet. It is not a substitute for the correctness tier, which
is where the ordering and exactly-once properties are actually proved:
`uc_log`'s pass-order property test, `uc_sim::timers`'s five-rule pass model
across seeds, `uc_node/tests/timers.rs`, and the two capstones
(`two_fsm_timer_churn_under_failover` in `lin_v2`,
`two_fsm_timer_service_sigkill` in the hard-crash harness), both adjudicated
by the shared `uc_lincheck::timer::assert_timer_report` oracle. See
[VERIFICATION §2, §3, §4](../VERIFICATION.md) for those rows and
[VERIFICATION §11](../VERIFICATION.md#11-what-is-not-verified) for what none
of it covers.

## The bar

Pre-committed. Rows a and b run on the same fleet shape as the M14 gate
(4 × `c6id.2xlarge`, `m12_gate` roles +
[`bench-infra/scripts/m14_fleet_gate.py`](/bench-infra/scripts/m14_fleet_gate.py)),
reusing that driver's rows a/b/e and its steady window (`WARMUP_SECS,
MEASURE_SECS = 2, 8`). Row c is new. Row d is a dev-box-legal isolated A/B,
not a fleet rate.

Rows a, b and d are **null bars against measurement noise, not ratios
against a target**: "within the same-source rebuild resolution measured by
`scripts/hop1_ab.sh` on the day". `scripts/hop1_ab.sh` A/B's two builds of
the *same* source against one fixed sink and reports the spread that comes
from build noise alone. **Record that number first, before comparing
anything to it** (CLAUDE.md: "M14b's client-hop A/B read −4.2 % on one binary
pair; fresh builds of the same two commits read ±0.3 %, and two builds of the
*same* commit differed by 1 %").

| row | measure | bar | result |
|---|---|---|---|
| a | `m14_fleet_gate.py` rows a/b/e with every service wrapped in `Timed<..>` and **no timers scheduled**, steady window, against the same rows on the pre-time-and-timers binary | within the same-source rebuild resolution measured by `scripts/hop1_ab.sh` on the day (record the number first) | not run — release on hold |
| b | the same three rows with one declared FSM scheduling **1 000 timers/s** sustained through the measure window | throughput within the same resolution as row a; **`uc2_timers_late_total == 0`** on every node after the warm-up window | not run — release on hold |
| c | timer precision: the distribution of `time_ns − deadline_ns` over **≥ 10 000 on-time fires** under row b's load | **p99 ≤ 2 × the measured consensus-pass length on the rig.** Measure the pass length first, on the day, and write it into the results table before comparing anything to it | not run — release on hold |
| d | apply-hop A/B: `uc_node/examples/apply_bench`, this branch vs. `17d5c6b` (the pre-time-and-timers baseline), run under `scripts/hop1_ab.sh`'s same-source rebuild control, at N=1 and N=2, bounded lag | within the measured same-source rebuild resolution (the control arm of the same run) | not run — release on hold |

### Reading the rules

**Rate** follows the M14 gate's conventions unchanged: the direct `Engine`
client's completed operations per second over the middle `MEASURE_SECS` of
the steady window, `--inflight 4096`, 64-byte payload, session envelope on,
fan-in (`try_submit_all`) whenever two FSMs are declared, client on the
leader host and shmem-attached.

**Row b's timer load** is chosen to be visible without being the workload:
1 000 timers/s is ~16 timers per second per pass at a plausible pass rate,
well under the `TIMERS_PER_PASS = 64` bound, so step 3 of the leader pass is
never skipped and the row measures the *steady* cost of firing rather than
the backpressure path. A run in which `uc2_timers_late_total` is nonzero
after warm-up has either hit that bound or has a leader whose clock is
misbehaving; either way the row does not pass, and the cause is diagnosed
rather than the bar moved.

**Row c's bar is derived, not hoped for.** An on-time fire is stamped with
its deadline, so `time_ns − deadline_ns` is `0` for every on-time fire by
construction. What row c actually measures is *wall-clock* lateness: the
delay between the deadline passing and the leader pass that notices it. That
is bounded below by the pass length, so a p99 above `2 ×` the measured pass
length means passes are being delayed, not that the timer logic is slow.
Measure the pass length on the rig first, with the `uc2_*` cycle metrics or a
one-off probe, and write it down before the comparison.

**Row d is the M14a lesson made a bar.** It is not a fleet row and does not
need fleet spend: `apply_bench` isolates the FSM hop on one host. It may be
run on the dev box, and it is smoke rather than a rate gate in the usual
sense, but its *bar* is a ratio against a control arm measured in the same
run, which is exactly the construction the dev-box-is-not-a-bench rule
permits.

**What would fail this gate, if it ran.** Row a failing would mean the
per-pass clock read, the ring drain, or the stamp write costs measurable
throughput, which would be a surprise worth investigating rather than a
tuning target. Row b failing while row a passes would isolate the cost to
firing itself. Row c failing points at pass scheduling, not at the timer
heap. Row d failing while row a passes would be the M14a codegen effect
again, and the fix would be moving the added frame-loop body out of line, as
M14a's `lockstep_wait` was.

## Results

**Not run.** No fleet spend has been authorized for this gate — the `2.11.0`
release is on hold pending further work on this branch. This table stays
empty until the maintainer green-lights a run:

| row | result |
|---|---|
| a | not run — release on hold |
| b | not run — release on hold |
| c | not run — release on hold |
| d | not run — release on hold |

Numbers to record on the day, before any comparison:

| measurement | value |
|---|---|
| `scripts/hop1_ab.sh` same-source rebuild resolution, on the rig | not measured |
| consensus-pass length on the rig, under row b's load | not measured |

## When this gate is run

1. Record `scripts/hop1_ab.sh`'s same-source rebuild resolution on the day,
   on the fleet host shape, before running anything else. Rows a, b and d
   compare against that number, not against a fixed percentage.
2. Measure the consensus-pass length on the rig under load, and write it into
   the table above. Row c's bar is `2 ×` it.
3. Run `bench-infra/scripts/m14_fleet_gate.py`'s rows a/b/e against this
   branch's binaries with `Timed<..>` services and no timers (row a), then
   again with the 1 000 timers/s arm (rows b and c).
4. Run `scripts/hop1_ab.sh` over `apply_bench` for row d, including its
   same-source control arm.
5. Fill in the results table above; do not edit the bar table to match
   whatever the run produced.
6. Only after this gate, the FSM identity gate, and the maintainer's
   version-number decision
   ([the semver policy](../reference/semver-policy.md)) does
   [Cut a release](../how-to/cut-a-release.md) apply.

## Related

- [FSM identity gate skeleton](uc2-fsm-identity-gate-2026-09-02.md) — the
  other gate on this same unreleased flag day; its rows a/b/e are the same
  rows, so a single fleet trip can adjudicate both.
- [M14 gate](uc2-m14-gate-2026-08-29.md) — where rows a/b/e come from, and
  the steady-window convention they carry.
- [M14a apply-hop bench](uc2-m14a-apply-hop-2026-08-27.md) — the isolated
  apply-hop harness row d uses, and the codegen lesson row d exists for.
- [M14c client hop](uc2-m14c-client-hop-2026-08-28.md) — where
  `scripts/hop1_ab.sh`'s same-source rebuild control came from.
