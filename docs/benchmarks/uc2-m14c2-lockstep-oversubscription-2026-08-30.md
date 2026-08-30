# M14c2 T8 — lockstep under CPU oversubscription

**SMOKE, never a gate.** Every number here is a dev-box measurement
(CLAUDE.md "Benchmarking discipline").
Rate bars are fleet-only; nothing below moves a bar, and nothing below is a
bar. What this record adjudicates is a *ratio* and a *mechanism*, both of which
a dev box can carry.

Date: 2026-08-30. Branch `worktree-uc2-m14c2`, HEAD `11d4ecc`.

## The question

The M14 fleet gate's **row e** measured lockstep (`fsm_lag = "lockstep"`) on a
3-voter cluster whose 8-vCPU leader host also ran the client at **~21.7 k
ops/s** — 60× under its bounded twin
(`docs/benchmarks/uc2-m14-gate-2026-08-29.md`). The dev-box apply-hop harness
(`uc2_node/examples/apply_bench`) had measured the same mode at **631 k/s at
N=2** with the FSMs alone on the box
(`docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md`). Two numbers 30× apart for
the same code path.

**Pre-registered hypothesis** (spec §16.4, M14a's doc comment at
`uc2_service/src/apply.rs`): under CPU oversubscription a descheduled sibling
makes the barrier ladder (`LAG_WAIT_SPINS = 256` spins, then
`LAG_WAIT_YIELDS = 2048` yields) *exhaust*, the waiter falls through to the
apply agent's idle strategy — `APPLY_IDLE = Sleep(50 µs)`,
`uc2_service/src/lib.rs` — and the set cascades into sleeping in lockstep.

## Method

`scripts/lockstep_oversub.sh` (new). Each invocation runs
`apply_bench --fsms 2 --mode lockstep --secs 8 --warmup-secs 1` twice: once
unconstrained, once `taskset`-pinned to `--cores` with `--spinners N`
`stress-ng --cpu` threads pinned to the same cores. Auxiliary arms (bounded
mode, N=1, unconstrained-only) were run through an out-of-tree helper that
takes the same shape with `--fsms`/`--mode` free; the repo script stays exactly
the shape the brief specified.

The harness's three busy threads are the driver (append + fake
archive/consensus) and the two `uc2_service` apply agents, so `--cores 0` is
already 3 runnable threads on 1 CPU before any spinner is added.

Box: **16C/32T x86_64 dev box, HT siblings `N, N+16`** (so `--cores 0-1` is two
distinct *physical* cores and `--cores 0,16` is the two logical CPUs of one
physical core). Linux, stock scheduler, otherwise idle (load average ≈ 2.3 of
32 during the run). rustc 1.96.0, `--release`.

Every ladder and bisect rung ran **3×** and all three numbers are reported;
two isolation arms (bounded N=2 at `--cores 0-1 --spinners 2`, lockstep N=1 at
`--cores 0`) are **n=1** — they are order-of-magnitude checks, not deltas, and
are marked as such in their table.

### Provenance

`~/.cache/cargo-target` is shared with every other checkout, so each build got
a private `CARGO_TARGET_DIR` and every binary is identified by sha256
(the script prints it per invocation).

| build | `CARGO_TARGET_DIR` | sha256 |
|---|---|---|
| baseline (HEAD `11d4ecc`) | `…-m14c2-t8` | `4f10dc7354f237edca8a0ad631086ec822ccd3bd580a8a83414f729707c405b7` |
| control A (same source, separate dir) | `…-m14c2-t8-ctrl-a` | `4f10dc73…c405b7` (identical) |
| control B (same source, separate dir, sccache bypassed) | `…-m14c2-t8-ctrl-b` | `4f10dc73…c405b7` (identical) |
| variant (a1) `LAG_WAIT_YIELDS = 8192` | `…-m14c2-t8-a1` | `71cb34dca96d3610270cdc20a5650b6623076aa4800aca25498b4072996babbd` |
| variant (a2) `LAG_WAIT_YIELDS = 32768` | `…-m14c2-t8-a2` | `80408ac9246bd12e3f52d4bb1d612b308ba312dda4edb638c466bfdb463e69d0` |
| variant (b) yield-until-sibling-dead | `…-m14c2-t8-b` | `e60c67c4e50ca2fa2f1171183408389031875745053dd05bd861f87c35fc3e4e` |
| variant (c) futex on the sibling's `applied` | `…-m14c2-t8-c` | `9c138693cb19539af35d6474ab85585f3dd6d561344a5cef2a7457b10a7e9ce4` |

## The same-source rebuild control (= the resolution)

M14b's lesson (CLAUDE.md): measure the harness's build-to-build resolution with
a same-source rebuild before attributing any delta to code. Two independent
builds of HEAD into two separate target dirs, 3 runs each:

| | run 1 | run 2 | run 3 |
|---|---|---|---|
| ctrl A — unconstrained N=2 lockstep | 624 276 | 623 031 | 629 333 |
| ctrl B — unconstrained N=2 lockstep | 628 007 | 624 764 | 627 684 |
| ctrl A — pinned `0-1`, 2 spinners | 741 | 1 323 | 2 008 |
| ctrl B — pinned `0-1`, 2 spinners | 392 | 1 011 | 465 |

**Resolution, unconstrained: 1.0 % peak-to-peak** (623 031 – 629 333 across six
runs of two separately-built binaries — i.e. ±0.5 % about the mean; "±1.0 %"
would overstate it). A delta smaller than that is not readable.

**Resolution at a spinner rung: a factor of ~5** (392 – 2 008). The
`stress-ng` rungs are *not* usable for reading a bisect delta; only an
order-of-magnitude change is legible there. The `--cores 0 --spinners 0` rung
(below) is stable to 0.1 % and is what the bisect was read on.

**Caveat on the control.** This box builds through `sccache` into a shared
compilation cache, and all three separately-built binaries came out
**bit-identical** (same sha256) — including one built with the wrapper
bypassed. So this control measures **run-to-run noise only**; the codegen
component M14b saw between two builds of one commit cannot appear here. It is
the weaker of the two controls, and it is the honest description of what was
measured.

**And note where the control pair was run:** at the unconstrained rung and at
the `0-1 --spinners 2` rung, *not* at the `--cores 0` rung the bisect is read
on. That rung's quoted resolution (709 / 709 / 710, 0.1 %) is **n=3 of one
binary**. Since the separately-built binaries here are bit-identical, a rebuild
control at that rung would be identical by construction, so nothing is lost —
but the 0.1 % figure is a run-to-run repeatability number, not a rebuild
control.

## The ladder

Baseline binary `4f10dc73…`. `hop: min applied_frames/s`, 3 runs per rung.
Reproduction target was ≤ 30 k frames/s.

| rung | runnable threads / CPUs | run 1 | run 2 | run 3 | vs unconstrained | ≤ 30 k? |
|---|---|---:|---:|---:|---:|---|
| unconstrained | 3 / 32 | 624 276 | 623 031 | 629 333 | 1.00× | — |
| `--cores 0-1 --spinners 0` | 3 / 2 | 148 278 | 167 076 | 147 825 | 0.25× | no |
| `--cores 0-1 --spinners 1` | 4 / 2 | 1 204 | 1 080 | 3 749 | 0.003× | **YES** |
| `--cores 0-1 --spinners 2` | 5 / 2 | 741 | 1 323 | 2 008 | 0.002× | **YES** |
| `--cores 0-3 --spinners 4` | 7 / 4 | 7 031 | 6 545 | 1 224 | 0.008× | **YES** |
| `--cores 0` (widened) | 3 / 1 | **709** | **709** | **710** | 0.0011× | **YES** |
| `--cores 0,16` (HT pair, widened) | 3 / 2 logical, 1 physical | 86 729 | 87 518 | 88 862 | 0.14× | no |

**Reproduced, hugely.** The worst rung is 880× down from unconstrained, deeper
than the fleet's row e (60×) on a harsher rung — consistent with row e, pending
Task 9's pinned re-measure, which is what would establish that they are the
same phenomenon. The
`--cores 0` rung is the cleanest: 709 / 709 / 710 frames/s, a 0.1 % spread,
i.e. **1.41 ms per frame**.

`lag_waits` (the barrier's give-up-episode counter) was **0 on essentially
every collapsed run** — 0 at `--cores 0` on all three runs, 0 at
`--cores 0-1 --spinners 1/2`, and 0 and 4 at `--cores 0-3 --spinners 4`. Over
eight seconds at 709 f/s that is ~5 700 barrier waits per FSM with zero
give-ups.

### Two controls that isolate the barrier

| arm | rung | run 1 | run 2 | run 3 |
|---|---|---:|---:|---:|
| **bounded**, N=2 | `--cores 0` (same 3 threads / 1 CPU) | 7 410 565 | 7 043 294 | 7 441 768 |
| **bounded**, N=2 | `--cores 0-1 --spinners 2` | 7 215 190 | — | — |
| **lockstep**, N=1 (no sibling) | `--cores 0` | 338 512 | — | — |

The last two rows are **n=1** (marked `—`): they are three-order-of-magnitude
sanity checks against the 709 f/s on the same rung, not deltas, and nothing in
the verdict rests on their exact value.

Bounded mode on a *single* CPU with the same three threads runs at **7.4 M
frames/s** — 10 000× the lockstep number on the identical rung, and a third of
its own unconstrained 21.5 M. Lockstep with no sibling to wait for does not
collapse either. The collapse is **specific to the lockstep barrier's
cross-thread wait**, not to CPU starvation of the apply hop.

## The mechanism — and the refutation of the pre-registered hypothesis

The pre-registered story was "the ladder exhausts and the set cascades into the
50 µs sleep". **That is not what happens.** `lag_waits = 0` says the ladder
*never* exhausted, so `lockstep_wait` never returned `None`, so the apply
agent's `Sleep(50 µs)` was never reached on any collapsed run.

What the numbers say instead: the ladder's **yields are the collapse**.
`std::thread::yield_now()` does not hand the CPU to a specific thread; under
the stock Linux scheduler a yielding thread with a competitive vruntime is
routinely re-picked, so a waiting FSM burns whole scheduling slices spinning
through its 2 048-yield budget while the sibling it is waiting for sits
runnable-but-not-running. The barrier opens when the *scheduler* eventually
preempts the waiter — one timeslice-scale event per frame. 1.41 ms/frame is a
scheduler quantum, not a handshake.

**What is instrumented and what is inferred.** The ladder's yields were *not*
counted, so "burns whole scheduling slices" is an inference. The bound that
does follow from the data: `lag_waits = 0` means every barrier returned inside
the budget, so **≤ 2 047 yield iterations per barrier**; at 1.41 ms per frame
that is a **mean of ≥ 0.69 µs per yield iteration** — several times the cost of
a bare `sched_yield` that does not switch (tens of ns), so the iterations are
not free spins and the waiter is being descheduled or is re-picked after real
work elsewhere. Pinning that down properly needs a yields-consumed counter
alongside `lag_waits`; that is the instrument to add if this is revisited.

That also predicts, correctly, that (a) and (b) below are no-ops: both change
what happens *after* the ladder, on a path that never executes.

## The bisect

Ordered (a) → (b) → (c) per the rule, each a separate build, measured at the
reproducing rung `--cores 0 --spinners 0` (the 0.1 %-stable one), 3 runs.

| variant | change | run 1 | run 2 | run 3 | vs baseline 709 |
|---|---|---:|---:|---:|---:|
| baseline | — | 709 | 709 | 710 | 1.00× |
| (a1) | `LAG_WAIT_YIELDS` 2048 → 8192 (×4) | 709 | 708 | 709 | **1.00×** |
| (a2) | `LAG_WAIT_YIELDS` 2048 → 32768 (×16) | 709 | 709 | 707 | **1.00×** |
| (b) | ladder ends only when a sibling's `heartbeat_ns` is > 1 s stale (never sleep on a live sibling) | 708 | 710 | 709 | **1.00×** |
| (c) | after `LAG_WAIT_SPINS`, `futex_wait` on the blocking sibling's `applied` word (500 µs timeout backstop, 64 waits) + `futex_wake` after each `applied` store while in lockstep | **82 639** | **82 231** | **82 309** | **116×** |

**(a1), (a2) and (b) were measured only at the reproducing rung**, not
unconstrained as the brief's Step 3 asks; only (c) got the full unconstrained
set below. That is immaterial to the verdict: the rule's clauses are
conjunctive, and a variant that reads 1.00× at the reproducing rung fails the
"≥ 50 % of the unconstrained rate" clause whatever its unconstrained numbers
are, so no unconstrained measurement could have changed its outcome. It would
have mattered only if one of them had cleared the rate bar.

(a) and (b) are null to within one frame per second — exactly as the mechanism
predicts. (c) is a 116× improvement, and it is the only variant that touches
the path that actually runs.

### Variant (c) across the other reproducing rungs and unconstrained

| arm | rung | run 1 | run 2 | run 3 | baseline (same rung) |
|---|---|---:|---:|---:|---|
| (c) N=2 lockstep | `--cores 0` | 82 639 | 82 231 | 82 309 | 709 / 709 / 710 |
| (c) N=2 lockstep | `--cores 0-1 --spinners 2` | 79 433 | 80 237 | 79 741 | 741 / 1 323 / 2 008 |
| (c) N=2 lockstep | `--cores 0-3 --spinners 4` | 56 722 | 67 931 | 70 977 | 7 031 / 6 545 / 1 224 |
| (c) N=1 **bounded**, unconstrained | — | 21 825 839 | 21 293 606 | 21 836 225 | 21 459 487 / 21 294 198 / 21 228 775 |
| (c) N=2 **bounded**, unconstrained | — | 21 984 040 | 22 038 232 | 21 904 080 | 21 563 026 / 21 552 018 / 21 568 752 |
| (c) N=2 lockstep, unconstrained | — | 584 195 | 569 337 | 601 831 | 624 276 / 623 031 / 629 333 |

- No regression on either bounded arm: **medians +2.5 % at N=1**
  (21 825 839 vs 21 294 198) and **+2.0 % at N=2** (21 984 040 vs 21 563 026).
  By means the N=1 figure is +1.5 % and by maxima +1.8 %; the statistic is
  named because they differ. Both are in the *favourable* direction, so read as
  "no regression", not as a win.
- **But the bounded arms cannot bind this variant.** (c)'s `futex_wake` is
  gated on `LagMode::Lockstep`, so a bounded run never executes the added
  syscall — the bounded N=1/N=2 arms exercise none of (c)'s cost, and the
  rule's regression clause is therefore vacuous against this particular
  implementation. Its real cost shows only where the syscall runs:
- **Unconstrained N=2 lockstep regresses 6.4 %** (medians 584 195 vs 624 276),
  outside the 1.0 % peak-to-peak control. The syscall pair per frame is not
  free at full speed. (The rule's regression clause names only N=1/N=2
  *bounded*, so this is recorded as a finding, not as the disqualifier — the
  verdict below turns on the 50 % clause, which (c) fails outright.)
- **(c) restores 13 % of the unconstrained rate** (82.6 k of 624 k) at the best
  rung, 13 % at `0-1/2`, and 11 % at `0-3/4`.

## The rule

Quoted verbatim from the plan's Global Constraints
(`docs/superpowers/plans/2026-08-30-uc2-m14c2-proof-pass.md`), restating
spec §16.4 (pre-committed before the run):

> product defect iff it reproduces only under oversubscription *and* one of
> (a) ladder ×4/×16, (b) yield-not-sleep while a sibling is live, (c) futex
> wait on the sibling's applied word restores ≥ 50 % of the unconstrained rate
> under the same oversubscription without regressing unconstrained N=1/N=2
> bounded by more than a same-source-rebuild control; otherwise an
> operating-envelope fact stated with the number.

Applied:

- "reproduces only under oversubscription": **yes** — unconstrained is
  623–629 k on every run, the collapse appears only when the runnable set
  exceeds the CPUs.
- (a) ×4 and ×16: **null** (1.00×). Does not clear 50 %.
- (b): **null** (1.00×). Does not clear 50 %.
- (c): **116×**, but **13 %** of the unconstrained rate — below the 50 % bar
  (which would need ≥ 312 k). It also costs 6.4 % of unconstrained N=2
  lockstep.

No variant clears the bar, so the second clause fires.

**Verdict: an operating-envelope fact, not a product defect — lockstep needs a
free CPU per declared FSM plus the node's own agents, and once the runnable set
exceeds the CPUs it collapses by up to ~880× (624 k → 709 frames/s at N=2 with
3 threads on 1 CPU), while bounded mode on the identical rung is unaffected at
7.4 M frames/s.**

Landed: the envelope sentence with the number, in
`docs/reference/configuration.md` (`[services]`, the `fsm_lag` row) and
`docs/reference/limits.md`. **No behavioural code change**:
`uc2_service/src/apply.rs` gains only comments — a one-line "do not retune"
pointer at `LAG_WAIT_YIELDS` and a paragraph on `lockstep_wait` recording that
the ladder never exhausts here — so that nobody rediscovers the 1.00× the hard
way.

## What a future attempt should know

Recorded so the next person does not re-derive it:

1. The M14a doc comment's mechanism ("a lockstep FSM must never SLEEP on a live
   sibling") is real but is **not** what row e is. Row e is the yield ladder
   itself under oversubscription; the sleep is never reached (`lag_waits = 0`).
   Lengthening the ladder or making it unbounded is therefore provably null —
   both were measured, both are 1.00×.
2. A blocking handoff (variant (c)) is the only lever that moved the number,
   and it moved it 116×. Its patch is small: a `wake_word()` accessor on
   `PaddedAtomicU64` (the low-32-bits trick already used by
   `RingHeader::wake_word`), `pub mod futex` in `uc_protocol::ring`, a
   `futex_wait` phase in `lockstep_wait` after the spins, and a `futex_wake`
   after the `applied` store *while in lockstep mode*. It was built and
   measured, then **discarded** — it does not clear the pre-committed bar, and
   the honest-failure protocol keeps the bar.
3. The 50 % bar may be physically unreachable at these rungs by *any* blocking
   design: a lockstep frame at N=2 needs the sibling and the log producer to
   each be scheduled, so on 1–2 CPUs it costs ~2–3 context switches per frame
   (~12 µs at the measured 82 k/s). 50 % of 624 k would be 3.2 µs per frame
   with those switches still mandatory. If the question is revisited, the bar
   should be re-specified against a *scheduling-aware* ceiling, not against the
   unconstrained rate.
4. `--cores 0,16` (both HTs of one physical core) reads 87 k, ~120× the
   single-CPU rung: two logical CPUs are enough to keep the handshake off the
   scheduler even though there is only one physical core behind them. The
   thing lockstep needs is a *runnable slot*, not a core.
5. Task 9's pinned fleet rig should re-measure row e with the FSM threads
   pinned to dedicated vCPUs; this record predicts that pinning alone recovers
   most of the 60×.
