# uc2 log-clock gate — SKELETON, no fleet run yet

**Date:** 2026-09-08 (bars committed). **Fleet run: NOT RUN — user-gated.**
The maintainer has not green-lit fleet spend for this gate; row a's result
cell stays "not run" until that happens, per the honest-failure protocol.

> **Decide rule committed before any run.** This document's bar table is
> committed, with every result cell filled with either a real result (rows
> b, c, d — all off-fleet) or "not run" (row a), **before** any fleet run
> against row a — the honest-failure protocol carried forward from
> M7/M9/M10/M11/M12/M13/M14/M14c2 and from the FSM identity and
> time-and-timers gate skeletons. Nothing in the bar may be edited to match
> a result: a run that misses the bar is recorded as a FAIL and keeps the
> bar. This document itself is a placeholder for row a — its own commit
> message says so — and must not be read as "gated" until a fleet run fills
> in that row's result.

## What the gate measures

Spec:
[`docs/superpowers/specs/2026-09-08-uc2-monotonic-log-clock-design.md`](../superpowers/specs/2026-09-08-uc2-monotonic-log-clock-design.md)
§8 ("Release and acceptance"), §9. The change anchors the leader's per-pass
log-time stamp on `CLOCK_MONOTONIC` plus a periodically resampled epoch
offset, instead of reading the wall clock every pass: a forward step against
wall time is adopted at the next resample, a backward step is smeared at a
fixed rate (never moved backwards), and the existing `max(now, last_stamp)`
appender clamp is kept as the guarantee of record rather than depended on for
step absorption. It touches the consensus agent's hot loop (`LogClock` is
read once per leader pass in place of the deleted `wall_now_ns()` read) —
the M14a lesson, re-learned in the 2026-09-07 time-and-timers row-d
regression, is that such changes must be A/B'd on exact binaries, because
codegen alone has cost 9 % for arms that never execute. No wire, cnc, or API
change, so this is an ordinary minor (`2.12.0`) by
[the semver policy](../reference/semver-policy.md).

**Coverage statement.** This gate measures throughput cost only. It is not a
substitute for the behavioural tier — the step/smear paths under the
`pass_clock()` test seam and the seeded-random monotonicity property — which
is `uc_node`'s `log_clock` unit suite (row c below) plus the synthetic
`Uc2LogTimeFrozen`-no-longer-fires-on-a-stepped-but-healthy-leader scenario
in `m10_alerts.rs`. See
[VERIFICATION §11](../VERIFICATION.md#11-what-is-not-verified) for what
neither tier covers.

Two claims need a fleet to test, and row a exists for exactly those — spec
§8's three pre-committed readings, reproduced verbatim below:

- **Gain outside the resolution** (ceiling 2.2 %, spec §3) — A is a perf
  win, the consensus agent is the limiter at peak, and **B-lite is worth its
  own spec** (spec §4).
- **Within the resolution** — A is null for throughput and ships on its
  behavioural merits alone (no freeze, one read, monotonic durations);
  **B-lite is closed**, since the agent is not the limiter.
- **Regression outside the resolution** — a FAIL, recorded as such; the
  change does not ship until the cause is found (the row-d playbook:
  `objdump` the two binaries' `do_work`, since the probes could not see the
  last regression and the machine code could).

## The bar

Pre-committed. Row a is the only fleet row and is **user-gated**: it costs a
fleet trip and is not run until the maintainer green-lights this gate. Rows
b–d are dev-box-legal and have already been run (this document is not a pure
skeleton for those three).

| row | what | bar (pre-committed) | result |
|---|---|---|---|
| a | **fleet A/B**: `m14_fleet_gate.py` rows a/b/e (steady window, `WARMUP_SECS, MEASURE_SECS = 2, 8`), this tree vs its parent commit (`10c014d`, the last commit before Task 1's code), on the same rig, same day, after a same-source rebuild control run FIRST to record the day's resolution (the M14b lesson; 1.12 % on 2026-09-07) | three readings, each a result (spec §8, reproduced above): **gain outside the resolution** (ceiling 2.2 %) → A is a perf win, consensus is the limiter, B-lite gets its own spec; **within the resolution** → null for throughput, ships on behaviour alone, B-lite closed; **regression outside the resolution** → FAIL, does not ship until the cause is found (`objdump -d -C` both binaries' `do_work`) | **not run — fleet, user-gated** |
| b | **codegen sanity**: count of `clock_gettime`/`__vdso_clock_gettime` call sites reachable from `m5_gate`, this tree vs parent, `objdump -d -C \| grep -c` (the time-and-timers row-d playbook) | strictly fewer in this tree | **filled in by Task 2, step 6 (controller rulings R1/R2): parent = 2, HEAD = 2 — not lower.** Both sites are libstd's shared `Timespec::now` (the one function every `Instant`/`SystemTime` read routes through) plus `m5_gate::await_single_leader` in the harness; a static call-site count cannot see per-pass frequency — diagnostic, not a bar (ruling R2); the acceptance bar is row a |
| c | **behaviour**: `cargo test -p uc_node --lib log_clock` — steady state, forward adopted, backward smeared at 500 ppm and fully retired at 2 000 s/s, monotone under 20 000 seeded random steps | all green | **filled in by Task 1: 15 passed (11 planned + 4 added in review, covering the resample decision — wide bracket skipped, narrow forward adopted and reported once, narrow backward smeared, real-clock resample reports no step). Re-run 2026-09-08 to confirm (`CARGO_TARGET_DIR=$HOME/.cache/cargo-target-logclock`): the same test filter also incidentally matches one unrelated test whose name contains the substring `log_clock` (`obs::metrics::tests::log_clock_smear_gauge_renders_the_published_value`, Task 3's gauge test), so the raw summary line reads `test result: ok. 16 passed; 0 failed; 0 ignored; 0 measured; 275 filtered out` — 15 in the `log_clock` module itself (all green) plus that one incidental match (also green). The module's own count is unchanged at 15/15** |
| d | **dev-box SMOKE, not a gate**: `m12_gate --arm direct --secs 8` alternated A/B/B/A/A/B on an idle box, private target dirs, sha256 of each binary recorded | reported, **no bar** (CLAUDE.md: a local rate is smoke; the same dip measured 7× spanned 0–18 % on a dev box) | **filled in by step 2 below — see [Results](#results)** |

## Why `hop1_ab.sh` is not the harness

`scripts/hop1_ab.sh` measures the client hop against `dummy-node` — a fixed
sink standing in for the node, chosen precisely so the harness does not run
a real consensus agent. This change lives entirely inside the consensus
agent's per-pass loop (`LogClock::mono_now`/`wall_at` replacing the deleted
`wall_now_ns()` read), which `hop1_ab.sh` never executes. It is still the
right tool for recording row a's resolution number (spec §8's "measured the
way `scripts/hop1_ab.sh` measures" refers to its same-source rebuild
control discipline, not to running the client hop as a stand-in for the
consensus pass), but it cannot itself A/B this change — that needs the real
in-process cluster `m12_gate` runs (row d, off-fleet) and
`m14_fleet_gate.py`'s rows a/b/e (row a, on-fleet), both of which exercise
the real consensus agent, exactly as the time-and-timers gate's row d/f
"Why the harness differs" reasoning already established for the same class
of change.

## Procedure

**Row a (fleet, when green-lit).** Copied from the time-and-timers gate
doc's run procedure for its rows a/b/e, with `--tt-rows` omitted (this gate
has no timer/schedule-table arms) and the two commit SHAs named explicitly:

1. Record `scripts/hop1_ab.sh`'s same-source rebuild resolution on the day,
   on the fleet host shape, before running anything else — that is the `R`
   below, not a fixed percentage.
2. Run the fleet A/B:

   ```bash
   R=<step 1's resolution, in %>
   python3 bench-infra/scripts/m14_fleet_gate.py --fleet --rows abe \
       --base-tree <checkout of 10c014d> --resolution-pct "$R" --ab-reps 3
   ```

   (this tree, `589451b` or later, is the head tree; `--rows abe` reuses the
   driver's own rows a/b/e under its steady window, `WARMUP_SECS,
   MEASURE_SECS = 2, 8` — the same convention the M14, FSM-identity, and
   time-and-timers gates all use, so a single fleet trip can adjudicate this
   gate alongside any other still-open row from those).
3. Read the three per-rep paired deltas and their standard error (Ruling on
   rows a/b/e in the time-and-timers gate doc, 2026-09-08: judge the PAIRED
   delta the driver already computes from interleaved base/head reps within
   each rep, not an unpaired arm-mean difference) against `R` and classify
   the result under spec §8's three readings, reproduced above.
4. Fill in row a's result cell; do not edit the bar table to match whatever
   the run produced. If the rig cannot resolve `R` against fleet arm-to-arm
   variance (the time-and-timers gate's row a/b/e experience: 15–43 %
   arm-to-arm spread against a 1.12 % build-noise bar), record that as its
   own finding rather than forcing a within/outside verdict — this is a bar
   question for the maintainer, not a silent reinterpretation.

**Row d (dev-box smoke, already run — see Results).**

```bash
W=/home/claude/ultima/ultima_cluster/.claude/worktrees/claude-2
BASE=10c014d   # the last commit before Task 1's code, NOT HEAD~4 — fix
               # rounds added commits after Task 1, so HEAD~4 lands inside
               # the feature
git -C $W worktree add --detach $HOME/scratch/logclock-base $BASE
git -C $W worktree add --detach $HOME/scratch/logclock-head HEAD
for arm in base head; do
  CARGO_TARGET_DIR=$HOME/.cache/cargo-target-logclock-$arm \
    cargo build --release \
    --manifest-path $HOME/scratch/logclock-$arm/Cargo.toml \
    -p uc_gateway --example m12_gate
  cp $HOME/.cache/cargo-target-logclock-$arm/release/examples/m12_gate \
    $HOME/scratch/m12_gate.$arm
  sha256sum $HOME/scratch/m12_gate.$arm
done
for i in 1 2 3 4 5 6; do
  arm=$(echo "base head head base base head" | cut -d' ' -f$i)
  $HOME/scratch/m12_gate.$arm --arm direct --secs 8 \
    --root $HOME/scratch/logclock-smoke-run$i-$arm
done
git -C $W worktree remove --force $HOME/scratch/logclock-base
git -C $W worktree remove --force $HOME/scratch/logclock-head
```

**Deviation from the brief's exact command, noted here rather than silently
worked around.** The brief's `grep '^RESULT'` assumes a machine-readable
`RESULT {...}` JSON line on every arm. Reading `m12_gate.rs`, that line is
only emitted by the fleet client roles (`client-direct`/`client-remote`,
`print_result_json`, `m12_gate.rs:2341`); the top-level `--arm direct`
default (in-process smoke) path never calls it — it prints a human-readable
report only (`print_report`, confirmed by reading `main()`'s branch at
`m12_gate.rs:400` and the direct-arm runner at `m12_gate.rs:1245`). Both
binaries were confirmed to produce no `RESULT` line under `--arm direct`
before the six measured runs were taken; the `responses/s` line from the
human-readable report was captured instead — same binary, same flags, same
number the JSON line would have carried, just parsed from the other output
the binary actually produces in this mode. This is not a different harness,
only a different grep.

## Results

**RUN 2026-09-08 on the dev box (idle: load 0.18–0.19 at start; a short
load-average bump to ~5 immediately after the two release builds settled
back down to ~2.5 before the six measured runs, confirmed no competing
`cargo`/`rustc` process was running).**

Binaries: `m12_gate.base` (built from worktree at `10c014d`,
`CARGO_TARGET_DIR=$HOME/.cache/cargo-target-logclock-base`) sha256
`6622f165d4ff38f727fa61c6531250936509272a4831cb1bcfc3a0a88dfdd096`;
`m12_gate.head` (built from worktree at `589451b` = `HEAD`,
`CARGO_TARGET_DIR=$HOME/.cache/cargo-target-logclock-head`) sha256
`c1540f5e048b9de242f108e27733ef765e51b7063d774a187c2e75cc3133fd38`.

Six runs, `--arm direct --secs 8`, each its own `--root`, alternated
base/head/head/base/base/head:

| run | arm | responses/s |
|---|---|---|
| 1 | base | 183 285 |
| 2 | head | 184 208 |
| 3 | head | 177 569 |
| 4 | base | 176 806 |
| 5 | base | 169 493 |
| 6 | head | 176 593 |

Means: base (runs 1/4/5) = (183 285 + 176 806 + 169 493) / 3 = **176 528**;
head (runs 2/3/6) = (184 208 + 177 569 + 176 593) / 3 = **179 456.67**.
**Ratio head/base = 1.01659 (+1.66 %) — dev-box smoke, not a gate.**

**Dev-box smoke, not a gate.** Per CLAUDE.md's standing rule, this number is
reported and carries no bar — a dev box's own dip has been measured 7× to
span 0–18 % against a real 10 % bar, and a +1.66 % reading here is well
inside that noise band. It is consistent with, but does not substitute for,
spec §8's "within the resolution" reading; only row a's fleet run can
adjudicate that.

## Related

- [Time-and-timers gate](uc2-time-and-timers-gate-2026-09-03.md) — where the
  M14a codegen lesson, the same-source rebuild control discipline, and the
  paired-delta ruling for rows a/b/e all come from; the rig shape and driver
  this gate's row a reuses.
- [FSM identity gate skeleton](uc2-fsm-identity-gate-2026-09-02.md) — the
  other gate that reuses `m14_fleet_gate.py`'s rows a/b/e.
- [M14a apply-hop bench](uc2-m14a-apply-hop-2026-08-27.md) — the codegen
  lesson ("code in a hot loop's body costs even on paths that never run")
  this gate's row d exists to guard against.
- [`docs/superpowers/specs/2026-09-08-uc2-monotonic-log-clock-design.md`](../superpowers/specs/2026-09-08-uc2-monotonic-log-clock-design.md)
  §8 — the source of row a's three pre-committed readings and this gate's
  release target (`2.12.0`).
