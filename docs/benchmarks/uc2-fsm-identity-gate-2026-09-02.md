# uc2 FSM identity gate — SKELETON, no fleet run yet

**Date:** 2026-09-02 (bars committed). **Fleet run: NOT RUN — release on
hold.** More changes are planned on `uc2/fsm-identity` before a release; the
maintainer has not green-lit fleet spend for this gate.

> **Decide rule committed before any run.** This document's bar table is
> committed, with every result cell empty, **before** any fleet run
> against it — the honest-failure protocol carried forward from
> M7/M9/M10/M11/M12/M13/M14/M14c2. Nothing in the bar may be edited to
> match a result: a run that misses the bar is recorded as a FAIL and
> keeps the bar. This document itself is a placeholder — its own commit
> message says so — and must not be read as "gated" until a fleet run
> fills in the results table below.

## What the gate measures

Spec: `docs/superpowers/specs/2026-09-02-uc2-fsm-identity-design.md` §9
("Fleet gate"), §10. FSM identity replaces M14's numeric declared-set
bitmask with named, positionally-checked rows: identity in code (`const
NAME`/`const VERSION`), `[services] names` required, `SNAP_BEGIN` 0.7.0
carrying per-row identity hashes and versions, cnc 3.1. **Consensus, the log
frame, the ingress/egress rings, and the client `Engine` internals are
untouched**; the apply loop is NOT fully untouched — it now constructs a
48-byte `ApplyCtx` (`uc_service/src/apply.rs`, `ApplyCtx::new(pos,
S::IDENTITY)`) once per frame, where the pre-identity binary passed a bare
`position`. The expected throughput delta against the M14 gate's own numbers
is **null only if that construction inlines away**, per M14a's lesson that
code added to a hot loop's body can cost even on paths that don't touch it
(`docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md`) — this gate does not get
to assume it does. Before any null claim, the run must include an
`apply_bench` A/B (pre-identity vs. this branch) with `scripts/hop1_ab.sh`'s
same-source rebuild control, not just cite this section's prose.

**Coverage statement.** This gate measures only what a fleet can measure:
whether attaching, running and joining **by name** costs anything against
the identical scenario run **by number** on the pre-identity binary. It is
not a substitute for the unit tier, the two negative snapshot-session
scenarios (`uc_node/tests/learner.rs`), or the capstones — see
[VERIFICATION §11](../VERIFICATION.md#11-what-is-not-verified) for those
(all dev-box smoke as of this skeleton's commit, per the standing
dev-box-is-not-a-bench rule; not a substitute for a fleet run either).

## The bar

Pre-committed. Rows a/b/e mirror the M14 gate's rows of the same letter
(`docs/benchmarks/uc2-m14-gate-2026-08-29.md`), on the same fleet shape
(4 × `c6id.2xlarge`, `m12_gate` roles + `bench-infra/scripts/m14_fleet_gate.py`,
whose `fsm_name`/`node_args`/`service_args` already speak names — Task 9),
with FSM names substituted for the row numbers the M14 gate used. Row j is
new to this gate.

Because the hot path is untouched, **the bar for a/b/e is not a throughput
ratio against a target — it is a bound against the harness's own
measurement noise**: "within the same-source rebuild resolution measured by
`scripts/hop1_ab.sh` on the day (record the number first, before comparing
anything to it)." `scripts/hop1_ab.sh` A/B's two builds of the *same*
source against one fixed sink and reports the ratio's spread from build
noise alone (CLAUDE.md "M14b's client-hop A/B read −4.2% on one binary
pair... fresh builds of the same two commits read ±0.3%, and two builds of
the *same* commit differed by 1%" — the standing lesson this bar is built
on). Measure that resolution on the gate day, record it in this doc's
results table, and only then judge whether this release's by-name numbers
sit inside it.

| row | measure | bar | result |
|---|---|---|---|
| a | `n2eq` (two `CountSm`-shaped FSMs, declared by name, bounded lag) vs `n1` (one FSM, declared by name), same run, steady window (`WARMUP_SECS, MEASURE_SECS = 2, 8`, `m14_fleet_gate.py`'s `arm_rates`) | within the same-source rebuild resolution measured by `scripts/hop1_ab.sh` on the day (record first) | not run — release on hold |
| b | `pair` (`count` + `spin`, bounded, both declared by name) vs `slow1` (`spin` alone), steady window | within the same-source rebuild resolution measured by `scripts/hop1_ab.sh` on the day (record first) | not run — release on hold |
| e | lockstep pairs (`count`+`spin` declared by name, `fsm_lag = "lockstep"`) vs their bounded twins, steady window | reported, **no bar** (unchanged from the M14 gate: lockstep's cost is an operating-envelope fact, not something this release could move) | not run — release on hold |
| j | a learner, declared `{count, fsm1}` by name, joins a purged two-FSM leader under load (the M14 gate's row f, run with names, and asserting the new positional/by-name refusal machinery never fires on a matched cluster) | **≤ 60 s** to converge; `Node::snapshot_session_refusals() == (0, 0, 0)` on every node (legacy-peer, identity, version — the third slot is new since wire 0.7.0); both artifacts present on the learner; row c's (M14 gate) all-FSMs-agree check on the learner; ≥ 1 `snapshot_installed` observed | not run — release on hold |

### Reading the rules

Same conventions as the M14 gate: **rate** is the direct `Engine` client's
completed operations per second over the middle `MEASURE_SECS` of the
steady window, `--inflight 4096`, 64-byte payload, session envelope on,
fan-in (`try_submit_all`) whenever two FSMs are declared. The client runs on
the leader host, shmem-attached. Row j's join budget (≤ 60 s) is carried
forward from the M14 gate's row f unchanged — nothing about the join path's
*mechanics* changed, only what a mismatch is checked against and how it is
named when it fires.

**What would fail this gate, if it ran.** A regression here would mean the
by-name lookup at attach, the eight-name scan on the snapshot path, or the
version-comparison branch added measurable per-frame or per-session cost —
none of which sits in the hot commit/apply loop, so a real regression would
be a surprise worth its own investigation, not a tuning target. Row j
failing would mean either the join budget regressed (unlikely — the
snapshot session's byte-for-byte shape is unchanged, only the header
fields) or the new refusal counter fired on a cluster that should have
matched (a real defect in the positional comparison, not a bar to relax).

## Results

**Not run.** No fleet spend has been authorized for this gate — the release
itself is on hold pending further changes to `uc2/fsm-identity`. This table
stays empty until the maintainer green-lights a run:

| row | result |
|---|---|
| a | not run — release on hold |
| b | not run — release on hold |
| e | not run — release on hold |
| j | not run — release on hold |

## When this gate is run

1. Record `scripts/hop1_ab.sh`'s same-source rebuild resolution on the day,
   on the fleet host shape, before running anything else — this is the
   number rows a/b compare against, not a fixed percentage.
2. Run `bench-infra/scripts/m14_fleet_gate.py`'s rows a/b/e/f (row f driven
   with named FSMs — the driver already speaks names since Task 9) against
   this branch's binaries.
3. Fill in the results table above; do not edit the bar table to match
   whatever the run produced.
4. Only after this gate (and the maintainer's version-number decision,
   `docs/reference/semver-policy.md`) does `docs/how-to/cut-a-release.md`
   apply.
