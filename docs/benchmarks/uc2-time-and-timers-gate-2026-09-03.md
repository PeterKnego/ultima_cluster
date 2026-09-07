# uc2 time-and-timers gate — SKELETON, no fleet run yet

**Date:** 2026-09-03 (bars committed; row e added the same day with plan 2;
rows **f, g and h** added 2026-09-07 with the cluster-FSM and
coordinated-snapshot work, their bars pre-committed the same way, and row d
given the runner it was missing).
**Fleet run: NOT RUN — release on hold.** Log time and timers (plan 1 **and**
plan 2, the replicated schedule table) land on the same unreleased `2.11.0`
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
and `uc_service::Timed<S>` for exactly-once delivery. Plan 2 (§5) adds the
**replicated schedule table**: `FRAME_TYPE_SCHEDULE_TABLE`, an admin verb
(`uc2ctl schedule apply`), adoption from the archive walk, and table ticks
that fire through the same heap and the same frame — row e is its bar.
Plain-language explainer:
[`docs/notes/uc2-log-time-and-timers-explained.md`](../notes/uc2-log-time-and-timers-explained.md).

Rows **f, g and h** are the three gate rows the cluster-FSM /
coordinated-snapshot spec
([`2026-09-05-uc2-cluster-fsm-and-coordinated-snapshot-design.md`](../superpowers/specs/2026-09-05-uc2-cluster-fsm-and-coordinated-snapshot-design.md)
§11) asks this document to carry, because that work lands on the same
unreleased flag day and shares this gate's fleet trip: the apply-loop arm
commanded instants add (f), a below-floor join with the shipper restarted
mid-window (g), and freeze duration measured against commit stall (h).

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
That lesson is why row d exists: an isolated apply-hop A/B with a same-source
rebuild control is the only honest way to say the added frame-loop body is
free. Row **f** is the same construction applied to the one arm the
coordinated-snapshot work adds to that same loop (the `FRAME_TYPE_SNAPSHOT`
type test plus an out-of-line call — out of line precisely because of the
M14a lesson).

Both rows run under **[`scripts/apply_ab.sh`](/scripts/apply_ab.sh)**, added
2026-09-07 for exactly this. It takes two commit-ishes, builds each in its
own temporary `git worktree` with its own private `CARGO_TARGET_DIR`
(`--locked`), builds the head sha a **third** time into a separate dir as the
same-source control arm, copies all three binaries out and records their
`sha256`, then runs them interleaved and reports `head vs base` against `head
vs head′` (**the resolution**, which is the only bar) — plus a per-arm
standard error that decides whether the run can resolve that bar at all, and
so whether the answer is `within`, `outside`, or `inconclusive (noisy run)`.
Its `--selftest` pins that arithmetic and all three verdicts on fixed inputs;
the `APPLY-JSON` line it parses is pinned on the Rust side by
`uc_node/examples/apply_bench.rs`'s `apply_json_line_shape_is_pinned`. The
verdict rule is stated in the script's header and reproduced under "Reading
the rules" below.

**Coverage statement.** This gate measures throughput cost and timer
precision on a fleet. It is not a substitute for the correctness tier, which
is where the ordering and exactly-once properties are actually proved:
`uc_log`'s pass-order property test, `uc_sim::timers`'s five-rule pass model
across seeds, `uc_node/tests/timers.rs`, and the two capstones
(`two_fsm_timer_churn_under_failover` in `lin_v2`,
`two_fsm_timer_service_sigkill` in the hard-crash harness), both adjudicated
by the shared `uc_lincheck::timer::assert_timer_report` oracle. Plan 2's own
correctness rows are
`uc_node/tests/timers.rs::a_schedule_table_ticks_exactly_once_per_deadline_and_advances_from_the_tick`
and `::a_restarted_node_resumes_the_table_with_one_catch_up_tick`,
`uc_node/tests/admin_auth.rs::schedule_apply_is_signed_digest_checked_leader_only_and_audited`,
the `RowTimers` unit tests in `uc_node/src/timers.rs`, the oracle's clause (7)
exercised by the capstone, and the `uc_protocol_schedule_table` fuzz target.
See
[VERIFICATION §2, §3, §4](../VERIFICATION.md) for those rows and
[VERIFICATION §11](../VERIFICATION.md#11-what-is-not-verified) for what none
of it covers.

## The bar

Pre-committed. Rows a and b run on the same fleet shape as the M14 gate
(4 × `c6id.2xlarge`, `m12_gate` roles +
[`bench-infra/scripts/m14_fleet_gate.py`](/bench-infra/scripts/m14_fleet_gate.py)),
reusing that driver's rows a/b/e and its steady window (`WARMUP_SECS,
MEASURE_SECS = 2, 8`). Row e here runs the same three driver rows again with a
schedule table live, so it needs no new driver either — only the one
`uc2ctl schedule apply` before the window opens. (The driver's rows a/b/e and
this document's rows a–e are different namespaces; where it matters below, the
driver's are called "the driver's rows a/b/e".) Row c is new. Rows d and f
are dev-box-legal isolated A/Bs, not fleet rates. Rows g and h are fleet
rows and are **user-gated**: they cost a fleet trip and are not run until the
maintainer green-lights this gate.

Rows a, b, d, e and f are **null bars against measurement noise, not ratios
against a target** — "within the same-source rebuild resolution measured on
the day". There are two such resolutions and they are not interchangeable:

- Rows a, b and e compare fleet throughput, so their resolution is
  **`scripts/hop1_ab.sh`**'s: it A/Bs two builds of the *same* source against
  one fixed sink and reports the spread that comes from build noise alone.
- Rows d and f compare the apply hop, so their resolution is the **control
  arm of their own run** — `scripts/apply_ab.sh`'s third arm (B′), a second
  build of the head source, measured back to back with the other two on the
  same box in the same minutes. That is strictly better than importing a
  number from another harness, and it is why those two rows have no separate
  "record first" step: the bar is produced by the run it judges.

**Record the resolution first, before comparing anything to it** (CLAUDE.md:
"M14b's client-hop A/B read −4.2 % on one binary pair; fresh builds of the
same two commits read ±0.3 %, and two builds of the *same* commit differed by
1 %").

| row | measure | bar | result |
|---|---|---|---|
| a | `m14_fleet_gate.py` rows a/b/e with every service wrapped in `Timed<..>` and **no timers scheduled**, steady window, against the same rows on the pre-time-and-timers binary | within the same-source rebuild resolution measured by `scripts/hop1_ab.sh` on the day (record the number first) | not run — release on hold |
| b | the same three rows with one declared FSM scheduling **1 000 timers/s** sustained through the measure window | throughput within the same resolution as row a; **`uc2_timers_late_total == 0`** on every node after the warm-up window | not run — release on hold |
| c | timer precision: the distribution of `time_ns − deadline_ns` over **≥ 10 000 on-time fires** under row b's load | **p99 ≤ 2 × the measured consensus-pass length on the rig.** Measure the pass length first, on the day, and write it into the results table before comparing anything to it | not run — release on hold |
| d | apply-hop A/B: `uc_node/examples/apply_bench`, this branch vs. `17d5c6b` (the pre-time-and-timers baseline), at N=1 and N=2, bounded lag, under [`scripts/apply_ab.sh`](/scripts/apply_ab.sh) — `scripts/apply_ab.sh 17d5c6b HEAD --fsms 1 --pairs 6`, then `--fsms 2`. This pair straddles the `Appender::new`/`append` arity change, so the `--harness` overlay is NOT available on it: each arm builds the harness its own commit carries, and the two differ in the fake DRIVER, not in the measured apply loop. Say so when quoting the number | **the guard is `pace_stalls > 0` on every arm** (Ruling Q9; `scripts/apply_ab.sh`'s `inconclusive (driver-bound)` verdict, computed from `apply_bench`'s exported `pace_stalls` counter). The driver paces itself on the slowest FSM, so in steady state a driver that never stalls IS the limiter and that arm's `min_rate` measures the driver, not the apply hop — and because a paced driver's mean equals `min_rate` by construction, checking that the two arms' `driver_mean`s agree proves nothing (it is the verdict restated, not an independent guard). Once `pace_stalls` clears on both arms, row d still cannot supply the flag day's **arm-cost verdict**: this pair's two arms compile *different* `Appender::new`/`append` code, so a driver-side cost from the arity change lands straight in `min_rate` — that verdict is row f's, whose arms share the `--harness` overlay and so differ only in the library under it. Row d's `within`/`outside resolution` reading, once ungated, is reported as what it is: a same-source rebuild check across a harness asymmetry, not the flag day's cost number | **not run** — the runner it was missing exists since 2026-09-07, and `pace_stalls` since the same fix wave that closed Ruling Q9; the 2026-09-03 no-runner finding it closes is kept in the Results table below |
| e | a 32-entry schedule table (`MAX_SCHEDULE_ENTRIES`, the cap) with **100 ms** `every` rules, all on **one** declared FSM, applied with `uc2ctl schedule apply` and left running through row a's three rows | **`uc2_timers_late_total` == 0** on every node after the warm-up window, and throughput within row a's resolution (the same-source rebuild number recorded on the day) | not run — release on hold |
| f | **commanded instants under the throughput load** (cluster-FSM spec §11): the cost of the one arm coordinated snapshots add to the apply hot loop — the `FRAME_TYPE_SNAPSHOT` type test plus an out-of-line call. `scripts/apply_ab.sh 627eb4e a64a6ed --harness uc_node/examples/apply_bench.rs --pairs 6 --fsms 1`, then `--fsms 2`, bounded lag. `627eb4e` is the last commit before that arm entered the loop; `a64a6ed` is the merge that carries it. `--harness` is **required** on this pair: both arms predate the `svc_sched`-ring harness fix, so each arm's own `apply_bench` cannot run at all — with the overlay every arm runs the identical harness and only the library under it differs | within the run's own rebuild resolution (the B′ arm), at both N | dev-box **SMOKE** run 2026-09-07 at N=1, verdict `inconclusive (noisy run)` — see the Results table. Not a gate, and not a pass: the run's arms were noisier than the resolution they would have been judged against, and the row's own procedure (`--pairs 6`, N=1 **and** N=2) has not been run |
| g | **a below-floor join with the shipper restarted mid-window** (cluster-FSM spec §11): `uc_node/tests/learner.rs::a_joiner_served_by_a_leader_restarted_before_its_first_commit_advance_still_installs_the_table` scaled to the fleet — purge the leader, restart it, and have a fresh learner join it before its first commit advance, under `m14_fleet_gate.py`'s row a load. Measure time to converge | **≤ 60 s** to converge and `snapshot_installed` observed — matching the [FSM-identity gate's row j](uc2-fsm-identity-gate-2026-09-02.md) | not run — fleet, user-gated |
| h | **freeze duration vs commit stall** (cluster-FSM spec §11): a `CountSm` with a deliberately large state — a `Vec<u8>` of 256 MiB the FSM carries — under `m14_fleet_gate.py`'s row a load. Command an all-nodes instant and record `uc2_snapshot_freeze_seconds_max` and the longest gap in `commit` advance during it; then a `--standby` instant and record both again | the **standby** instant's commit gap is **≤ the pass length measured on the day** — i.e. no stall attributable to the instant. The **all-nodes** instant's gap is **reported, no bar**: it is the number this row exists to produce | not run — fleet, user-gated |

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
need fleet spend: `apply_bench` isolates the FSM hop on one host — it needs
only an idle host, the same as row f. It may be run on the dev box, and it is
smoke rather than a rate gate in the usual sense, but its *bar* is a ratio
against a control arm measured in the same run, which is exactly the
construction the dev-box-is-not-a-bench rule permits. Its `pace_stalls`
guard (Ruling Q9) decides whether the run is readable at all, but even a
clean guard does not make row d the flag day's arm-cost number — that is row
f's, for the harness-asymmetry reason under "Rows d and f: the verdict rule".

**Row e is the schedule table at its cap, not at a plausible setting.** 32
entries is `MAX_SCHEDULE_ENTRIES` and 100 ms is fast for an operator-declared
recurrence, so together they are 320 table ticks per second on one row. That
is *below* row b's 1 000 programmatic timers/s deliberately: the rate is not
what row e is probing — row b already covers the firing rate, and 320/s is
comfortably under `TIMERS_PER_PASS = 64` per pass, so step 3 of the leader
pass is never skipped here either. The point is to put every table-specific
path under sustained load at once — the leader's advance-at-append, the
followers' advance-on-`TableConsumed`, and
`Timed`'s `table_last` dedup — while the throughput rows are being measured
beside them. All 32 entries sit on **one** FSM deliberately: that is the
worst case for a single row's heap, and it is the row whose
`uc2_timers_pending` and `uc2_timers_late_total` the bar reads. A nonzero
`uc2_timers_late_total` after warm-up is a fail and is diagnosed, not
accommodated — the likely causes are the same two row b names (the per-pass
bound, or delayed passes), plus one of row e's own: an entry armed from a log
clock that had fallen behind, which the one-tick catch-up should absorb into a
single late fire rather than a run of them.

**Plan 3 (the schedule table on the snapshot session) leaves row e's rationale
unchanged — checked 2026-09-03, and recorded here so it is not re-derived.**
Row e is a steady-state firing row on a live cluster: no purge floor, no
joiner, no snapshot session, so the `SNAP_TABLE` path is never entered while it
runs. What plan 3 adds on the paths row e *does* touch is one `Mutex` cache
refresh per adoption — a table is applied once, at the start of the row, not
per tick — and one extra clone at `snapshot_set_for`, which a snapshot session
reaches, not a timer pass. Neither the leader's advance-at-append nor the
followers' advance-on-`TableConsumed` nor `Timed`'s `table_last` dedup changed.
The bars stand as committed, and no row is added: a below-floor joiner's table
install is a correctness claim, adjudicated by
`a_promoted_below_floor_joiner_keeps_the_schedule_ticking_when_it_leads` and
the two `learner.rs` scenarios beside it, not by a rate.

**Rows d and f: the verdict rule, stated once.** `scripts/apply_ab.sh`
measures three arms — A (base), B (head) and B′ (head, built a second time
into its own target dir from its own worktree) — interleaved, `--pairs K`
runs each, and computes, on the per-arm means of `apply_bench`'s `min_rate`
(the slowest FSM's applied frames/s):

```text
head_vs_base = (mean(B)  - mean(A)) / mean(A) * 100      the candidate
resolution   = |mean(B') - mean(B)| / mean(B) * 100      the bar
sem(X)       = stdev(X) / (mean(X) * sqrt(K)) * 100      run quality, per arm
pace_stalls(X) = sum of the per-rep pace_stalls counter, per arm   driver-bound guard (Ruling Q9)

if pace_stalls(A) == 0 or pace_stalls(B) == 0 or pace_stalls(B') == 0:
    verdict = inconclusive (driver-bound)
elif K < 2 or max(sem(A), sem(B), sem(B')) > resolution:
    verdict = inconclusive (noisy run)
elif |head_vs_base| <= resolution:  verdict = within resolution
else:                               verdict = outside resolution
```

**The driver-bound guard runs first, and it is the ONLY guard that can tell
row d's arms apart (Ruling Q9).** `apply_bench`'s driver thread paces itself
on the slowest FSM (spins while `append − min(applied) > window`), so a
driver that is the limiter never stalls: `pace_stalls == 0` on that arm means
its `min_rate` is the driver's own ceiling, not the apply hop's. This
supersedes the earlier draft of this row, which asked the operator to check
that the two arms' `driver_mean`s agree — that check cannot fail while the
verdict passes, because a paced driver's mean equals `min_rate` by
construction, so it was the verdict restated rather than an independent
signal. `pace_stalls` is independent of `min_rate`, which is what makes it a
real guard.

**Once the driver-bound guard clears, the rebuild resolution is the only
bar, and run noise never widens it.** `resolution` is build noise: B and B′
are the same source, so whatever separates them is not semantics — that is
M14b's rule exactly. Rows d and f are **null** bars ("the added code is
free"), so anything added to the right-hand side would only make it easier
to bless a real regression, which is the wrong direction to be wrong in.

Noise is a **separate gate whose answer is "no answer"**. `sem` is the
standard error of the arm's *mean* — the quantity the verdict actually
compares — and unlike a min/max spread it shrinks as `1/√K`, so `--pairs` is
a real remedy rather than a knob that cannot move the number. A run whose
arms are noisier than the resolution they are being judged against is
reported `inconclusive (noisy run)` and claims neither "within" nor
"outside". `--selftest` pins the arithmetic and all **four** verdicts
(`within`, `outside`, `inconclusive (noisy run)`, `inconclusive
(driver-bound)`) on fixed inputs, with no cargo and no git.

"outside resolution" is an instruction to measure the hop on the fleet, never
a claim that the code regressed by that percentage.

The runner also prints `/proc/loadavg`'s 1-minute figure and a count of other
`cargo`/`rustc` processes into both the run header and its `AB-JSON` line,
and warns (it does not refuse) when the load exceeds 1.0 or another build is
running. A busy box does not bias an arm — the interleave sees to that — but
it inflates every arm's `sem`, which is what turns a run inconclusive.

**Rows d and f differ in one thing worth stating: whether the harness is
identical across arms.** Row f's pair is inside the window in which
`apply_bench` could not run at all — `uc_service::attach` opens a per-row
`svc_sched.<row>.ring` that the fake node never created (fixed 2026-09-07,
in the same commit as the `APPLY-JSON` pin) — so row f **must** use
`--harness`, and gets the stronger construction for free: every arm runs
byte-identical harness source and only the library under it differs, which is
`hop1_ab.sh`'s one-fixed-sink discipline generalized. Row d's pair straddles
the `Appender::new`/`append` arity change of the same flag day, so no single
harness compiles on both sides; each arm builds its own, and the two differ
in the fake **driver** that paces the run, not in the apply loop being
measured. That is a real, unavoidable asymmetry between the two rows and it
is recorded here rather than discovered later.

**Rows g and h are the two coordinated-snapshot claims a fleet is needed
for.** Row g is the residual the spec was written to close, moved from a
single-box test to a real join: a leader that ships a set and is then
restarted used to serve `(0, 0, [])`, so the joiner installed nothing. The
bar is deliberately the FSM-identity gate's row j number (≤ 60 s) rather than
a new one — nothing about the join path's *mechanics* changed, only what it
now carries, so a slower join would be a regression in the mechanics and not
a cost of the feature. Row h exists because §5.7's stall argument is
currently an argument: a freeze on a quorum that outlasts `fsm_lag` of
appended log stalls commit **by design**, and standby instants exist to avoid
it. 256 MiB of FSM state is chosen to make the freeze long enough to see
against a live commit stream; the standby arm is the one with a bar, because
"a standby instant does not stall commit" is the claim, and the all-nodes
arm's gap is reported bare because it is the cost the operator is choosing
between, not a target to hit.

**What would fail this gate, if it ran.** Row a failing would mean the
per-pass clock read, the ring drain, or the stamp write costs measurable
throughput, which would be a surprise worth investigating rather than a
tuning target. Row b failing while row a passes would isolate the cost to
firing itself. Row c failing points at pass scheduling, not at the timer
heap. Row d failing while row a passes would be the M14a codegen effect
again, and the fix would be moving the added frame-loop body out of line, as
M14a's `lockstep_wait` was. Row e failing while rows a and b pass would
isolate the cost to the table path specifically — most plausibly the
`TableConsumed` round trip through `svc_sched`, since that is the one hop a
programmatic timer does not have on the firing side. Row f failing would say the
same thing about the `FRAME_TYPE_SNAPSHOT` arm, whose fix is the same move
(it is already out of line, so the next step would be the type-dispatch
shape itself). Row g failing would mean the join path regressed, not that
the shipped set is wrong — the set's contents are a correctness claim with
its own tests. Row h has no way to "fail" on the all-nodes arm; only its
standby arm can, and a standby instant that stalls commit would mean a voter
froze when the spec says only the learner should.

## Results

**Fleet rows not run.** No fleet spend has been authorized for this gate — the
`2.11.0` release is on hold pending further work on this branch. The fleet
rows stay empty until the maintainer green-lights a run. Rows d and f have a
dev-box **smoke** reading (2026-09-07, `scripts/apply_ab.sh`, idle box where
noted): it is explicitly not a gate result, but under the honest-failure
protocol it is recorded as what it is — **both rows miss their bar**, the
bars stay, and the root cause is bisected to four inline hot-loop additions
and fixed on a branch (row d's entry has the ledger). The fleet run decides.

| row | result |
|---|---|
| a | not run — release on hold |
| b | not run — release on hold |
| c | not run — release on hold |
| d | **dev-box SMOKE, 2026-09-07, and the bar is MISSED: −26.8 % at N=1, −26.1 % at N=2.** Runs `20260907T102101Z-17d5c6b-4417c42` (N=1) and `20260907T102431Z-…` (N=2), `--pairs 6 --secs 6`, each arm its own harness (the pair straddles the `Appender` arity change, so the overlay is unavailable — see the row). N=1 arm means, `min_rate` applied frames/s: A `17d5c6b` **21 149 526** (sem 0.131 %), B `4417c42` **15 476 388** (sem 0.107 %), B′ 15 480 424 (sem 0.091 %); head vs base **−26.824 %**, resolution 0.026 %. N=2: A 20 775 089 (sem 0.619 %), B 15 353 653, B′ 15 421 759; head vs base **−26.096 %**, resolution 0.444 %. Both runs printed `inconclusive (noisy run)` under the Q8 rule of the day (a 27 % delta against a 0.03 % resolution, called inconclusive because the arms' sem exceeded the resolution) — that is the runner defect Ruling Q8' fixed: the signal bar is now `max(resolution, 2·sem)`, under which both read `outside resolution`. Ruling Q9's driver guard is **not checkable on the base arm**: `17d5c6b`'s driver predates the `pace_stalls` counter (the runner shows it as n/a since Q8'); the head arms stalled ≈ 2.95 × 10⁹ times per run, so the head side is apply-bound. **Root cause, by bisect** (N=1, idle box, one `apply_ab.sh` pair per step, logs under the maintainer's `scratch/apply_ab/gate-2026-09-07/`): the loss is four inline additions to the apply loop body, exactly the M14a lesson — timer core `4cbe926` **−13.7 %** (a per-frame `ApplyCtx` with three `Vec`s, an unconditional sched-record drain, and the `TIMER` arm's decode inline); `e282a0a` **−2.9 %** (a third `ApplyCtx` `Vec`); cluster-FSM plan 1 T7 `c0cd424` **−9.8 %** (leader-only heap bookkeeping in the loop); cluster-FSM plan 2 T8 `c3dfd7c` **−2.1 %** (the `SNAPSHOT` arm's inline cnc-slot lookup); every other commit in the range, including the `SNAPSHOT` arm itself (`931f794`) and the schedule table (`3776f3a`), measured within its run's resolution. **Fixes** (branch `perf/apply-arm-slot-out-of-loop`, three commits, each one hot-loop change): `34f339d` moves the slot lookup out of line (+0.7 % vs `a64a6ed`); `3636d76` puts the drain behind `ApplyCtx::has_sched_records()` and moves the `TIMER` arm into `on_timer_frame`; `4ed0335` boxes the three lists behind one lazily built `Option` so a frame that schedules nothing allocates nothing. Together vs main `a64a6ed`: **+16.15 %** (15 456 470 → 17 953 354, resolution 0.095 %, `outside resolution`; run `20260907T…-a64a6ed-4ed0335`); vs the pre-plan-2 `627eb4e`: **+13.42 %** (15 839 103 → 17 965 012, resolution 0.020 %, run `20260907T114007Z-627eb4e-4ed0335`) — the branch is above where the cluster-FSM plans started. **Residual pass, same day** (branch `perf/apply-hop-residual`, three commits, each its own A/B). With the arm fixes merged (`d0f126a`) the row re-read **−15.19 %** (21 170 971 → 17 955 099, resolution 0.214 %, run `20260907T140008Z`). The `apply-profile` rdtsc probes (3 reps per arm, stable to the ns) put the whole difference inside the batch loop — per frame A `sm_apply` 8 / `publish` 35 / `batch_arm` 51 ns vs B 8 / 38 / 57 — but after the first fix they read per-frame PARITY while the plain A/B still read −12 %: the probes mask what was left. The machine code did not. `objdump -d -C` of the runner's kept `apply_bench.{a,b}` binaries, with `readelf -rW` resolving each `call *slot(%rip)`, showed that at `17d5c6b` the loop's per-frame callees were inlined into it, and at `d0f126a` `FrameIter::next`, `read_header` and `BroadcastProducer::write` had each become a GOT-indirect call per frame with its result round-tripping the stack — LLVM stopped inlining them once `apply_cycle` (now ≈ 3 500 instructions × 4 monomorphs) outgrew its budget. `373cbc6` (`#[inline(always)]` on `FrameIter::next` + `Egress::publish`): **+3.12 %** vs `d0f126a` (res 0.062 %); `4a2f454` (the same on `read_header` + `BroadcastProducer::write`): **+15.99 %** vs `373cbc6` (18 541 884 → 21 507 315, res 0.775 %, head arms noisy but the signal is 20× the bar); `bfa97c0` (one `ApplyCtx` per batch, rebound per frame): **null** (−1.21 % within a 1.47 % resolution; kept as a neutral simplification, no gain claimed). **Final, `bfa97c0` vs `17d5c6b`: N=1 +1.61 %** (21 165 064 → 21 506 002, resolution 0.148 %, worst sem 0.088 %, `outside resolution` on the right side; run `20260907T142…`), **N=2 +3.31 %** (20 745 208 → 21 432 786, resolution 2.465 % — N=2 is noisy on this box at every arm, ≈ 5 % spread — `outside resolution`). **The −26.8 % is closed on the dev box.** The bar stays and the fleet row still decides: this is smoke, on one host, and the M14a/M14b caveats (build-to-build resolution, an idle box) all apply. |
| e | not run — release on hold |
| f | **dev-box SMOKE, 2026-09-07: N=1 −2.66 % `outside resolution`; N=2 −2.30 % `inconclusive (noisy run)`.** Runs `20260907T100243Z-627eb4e-a64a6ed` (N=1) and `20260907T100609Z-…` (N=2), `--pairs 6 --secs 6`, harness overlay on every arm. N=1 arm means, `min_rate` applied frames/s: A `627eb4e` **15 909 478** (sem 0.054 %), B `a64a6ed` **15 486 962** (sem 0.047 %), B′ 15 499 637 (sem 0.042 %); head vs base **−2.656 %**, resolution 0.082 %, worst sem 0.054 % — the box was idle and the run resolves its own bar. N=2: A 15 816 503 (sem 0.931 %), B 15 453 579, B′ 15 421 378; head vs base **−2.295 %**, resolution 0.208 %, worst sem 0.931 % (the box was not idle: load 1.7 at start). **The bar (within resolution) is MISSED at N=1** — but not by the arm the row was written to measure: bisecting plan 2 shows the `SNAPSHOT` arm commit `931f794` and the P10 `ReplayInstant` change each within their run's resolution, and the whole −2.1 % in T8 `c3dfd7c`, whose inline `attach::slot(..)` computation in the `SNAPSHOT` arm the loop body paid for on every frame (M14a). Fixed on `perf/apply-arm-slot-out-of-loop` (`34f339d`, +0.7 % vs `a64a6ed` on its own; the branch's cumulative reading is in row d). The earlier 3-pair smoke (`20260907T065532Z`, −0.24 %, `inconclusive (noisy run)`) is superseded by these runs and kept only in git history. |
| g | not run — fleet, user-gated |
| h | not run — fleet, user-gated |

Numbers to record on the day, before any comparison:

| measurement | value |
|---|---|
| `scripts/hop1_ab.sh` same-source rebuild resolution, on the rig (rows a, b, e) | not measured |
| consensus-pass length on the rig, under row b's load (rows c and h) | not measured |
| `scripts/apply_ab.sh` B′ resolution, row d's run | not measured on the rig; dev-box smoke 2026-09-07: **0.026 %** at N=1 (worst sem 0.131 %), 0.444 % at N=2 (worst sem 0.695 %) on the morning run; **0.148 %** at N=1 and 2.465 % at N=2 on the final `bfa97c0` run |
| `scripts/apply_ab.sh` B′ resolution, row f's run | not measured on the rig; dev-box smoke 2026-09-07: **0.082 %** at N=1 (worst sem 0.054 % — resolvable), 0.208 % at N=2 (worst sem 0.931 % — not) |

## When this gate is run

1. Record `scripts/hop1_ab.sh`'s same-source rebuild resolution on the day,
   on the fleet host shape, before running anything else. Rows a, b and e
   compare against that number, not against a fixed percentage. (Rows d and f
   do **not**: they carry their own control arm — step 5.)
2. The consensus-pass length comes off the leader's `/metrics` during row
   b's arms — `uc2_consensus_pass_ns` (a histogram the leader fills from the
   one clock reading it already takes per pass; `_sum / _count` is the mean,
   `_max` the worst) — and the driver writes it into its `GATE-JSON` for row
   c. Copy it into the table above.
3. Rows a, b and c, one invocation (the harness arms exist since 2026-09-07:
   `m12_gate service --timed --timers-per-sec N`, `m12_gate node
   --metrics-listen`, and `m14_fleet_gate.py`'s `--tt-rows`; the driver
   refuses at the door without `--base-tree` + `--resolution-pct`, because
   the bar IS the A/B against the pre-time-and-timers tree):

   ```bash
   R=<step 1's resolution, in %>
   python3 bench-infra/scripts/m14_fleet_gate.py --fleet --rows '' --tt-rows abc \
       --timed --timers-per-sec 1000 --base-tree <checkout of 17d5c6b> \
       --resolution-pct "$R" --ab-reps 3
   ```

   Row a is the head tree (every service `Timed<..>`, no timers) against
   the base tree on the same four rate arms (`n1 n2eq slow1 pair`),
   interleaved A/B on fresh clusters; row b re-runs them with FSM 0 as the
   self-sustaining 1 000 timers/s state machine and sweeps every voter's
   `uc2_timers_late_total`; row c reads the leader's `uc2_timer_lateness_ns`
   p99 (the upper bound of the bucket holding it — the ladder is 100 ns …
   100 ms in 1-2-5 steps, so the answer's resolution is one bucket) against
   `2 ×` the pass mean, and needs `_count ≥ 10 000`.
4. Row e (needs row a's head rates in the same process, so it repeats `a`):

   ```bash
   python3 bench-infra/scripts/m14_fleet_gate.py --fleet --rows '' --tt-rows ae \
       --timed --schedule-table 32 --base-tree <checkout of 17d5c6b> \
       --resolution-pct "$R" --ab-reps 3
   ```

   The driver writes the 32-entry, 100 ms `every` table naming row 0's FSM,
   applies it on the leader with `uc2ctl schedule apply` before the client
   starts, and waits for every voter's `uc2_schedule_table_position` to agree
   (30 s) before the measure window opens; then the four rate arms run with
   it live, under row b's late == 0 sweep.
5. Run rows d and f's apply-hop A/Bs with
   [`scripts/apply_ab.sh`](/scripts/apply_ab.sh), which builds and measures
   the control arm itself — that arm IS the bar, so there is nothing to
   record first. On an otherwise **idle** box (check `uptime` / `top`; a
   busy box makes the noise margin swamp the resolution, as the dev-box
   smoke shows):

   ```bash
   scripts/apply_ab.sh 17d5c6b HEAD --pairs 6 --fsms 1      # row d
   scripts/apply_ab.sh 17d5c6b HEAD --pairs 6 --fsms 2
   scripts/apply_ab.sh 627eb4e a64a6ed --pairs 6 --fsms 1 \
       --harness uc_node/examples/apply_bench.rs            # row f
   scripts/apply_ab.sh 627eb4e a64a6ed --pairs 6 --fsms 2 \
       --harness uc_node/examples/apply_bench.rs
   ```

   Row f **needs** `--harness` (both its arms predate the `svc_sched`-ring
   harness fix and cannot run otherwise); row d **cannot use it** (its pair
   straddles the `Appender::new` arity change). Record the run id, the three
   binary `sha256`s and, for row f, the harness `sha256` beside every number.
   These two rows do not need fleet spend — `apply_bench` isolates the FSM
   hop on one host — but they do need an idle host, and the runner will say
   so: a verdict of `inconclusive (noisy run)` is **not** a pass and not a
   fail, it means the host could not resolve the bar. Raise `--pairs` or move
   to a quieter host and run it again; do not record an inconclusive run as
   either outcome. The runner checks a **third** verdict before either of
   those, on every row: `inconclusive (driver-bound)` (Ruling Q9) fires when
   any arm's summed `pace_stalls` is zero — the driver paces itself on the
   slowest FSM, so a driver that never stalls is the one setting `min_rate`,
   and that arm's number is measuring the driver, not the apply hop. This is
   the guard that replaced the earlier "check the two arms' `driver_mean`s
   agree" instruction: that check could not fail while the verdict passed
   (a paced driver's mean equals `min_rate` by construction, so it was the
   verdict restated), where `pace_stalls` is independent of `min_rate` and can
   actually catch it. Even with `pace_stalls > 0` on both arms, row d's
   `within`/`outside resolution` reading is **not** the flag day's arm-cost
   verdict — its two arms compile different `Appender::new`/`append` code, so
   a driver-side cost from the arity change would land straight in `min_rate`
   with no way to separate it out. The arm-cost verdict is row f's, whose
   arms share the `--harness` overlay and so differ only in the library code
   under it; quote row d only for the rebuild-resolution check itself, and
   note the harness asymmetry as a limitation whenever you do.
6. Rows g and h, which **do** need the fleet and are user-gated:

   ```bash
   python3 bench-infra/scripts/m14_fleet_gate.py --fleet --rows '' --tt-rows gh \
       --timed --k <K from step 3> --state-bytes 268435456 --pass-ns <row c's pass mean>
   ```

   - **g**: the M14 join arm (a fresh learner joins a purged leader under the
     row a load) with the LEADER's node unit restarted ~2 s after
     `add-learner`; the joiner must still converge (`snapshot_installed` in
     its log) within 60 s, and `uc2_snapshot_set_position` must agree on all
     four hosts at the end.
   - **h**: every service carries the 256 MiB ballast; under the row a load
     the driver commands `uc2ctl snapshot` on the leader, reads the instant
     P off the leader's `uc2_snapshot_instant_position` (leader-local by
     ruling P13), waits for every voter's `uc2_snapshot_set_position` to
     reach P, and records each voter's `uc2_snapshot_freeze_seconds_max`
     plus the longest commit stall from the client's per-second timeline;
     then it joins the learner, commands `--standby`, waits on the learner's
     `uc2_snapshot_standby_instant_position`, and records both again. The
     timeline is 1 s buckets, so the standby bar (gap ≤ a pass length) reads
     as "zero stalled buckets"; the all-nodes gap is reported with no bar.
     `uc2ctl snapshot show` is the diagnostic if an instant does not
     complete. This arm is ~6 minutes of continuous load; run it last.
7. Fill in the results table above; do not edit the bar table to match
   whatever the run produced.
8. Only after this gate, the FSM identity gate, and the maintainer's
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
- [Cluster FSM and coordinated snapshots
  spec](../superpowers/specs/2026-09-05-uc2-cluster-fsm-and-coordinated-snapshot-design.md)
  §11 — where rows f, g and h come from, and the correctness rows they are
  deliberately **not** a substitute for.
