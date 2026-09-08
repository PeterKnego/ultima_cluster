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

**Row j's join now installs the cluster artifact too** (added 2026-09-07, no
new row and no change to row j's bar): since the coordinated-snapshot work,
the set a below-floor joiner installs carries the cluster FSM's artifact —
membership, the schedule table and the settings record — alongside the
service artifacts row j already checks for. The ≤ 60 s budget and the
`snapshot_session_refusals() == (0, 0, 0)` check stand as committed; what
changed is that a joiner which converges is now also holding the cluster's
table and membership before it can serve or lead, which is
[the time-and-timers gate's row g](uc2-time-and-timers-gate-2026-09-03.md),
not this one.

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

**RUN 2026-09-07 on the fleet.** 4 × `c6id.2xlarge`, us-east-1a (node0
`54.208.131.240` leader/client, node1 `34.228.73.79`, node2 `34.230.85.94`,
node3 `54.221.97.88` learner). Head tree `d9483c2`, working tree clean.
Driver `m14_fleet_gate.py --fleet --rows abef --metrics-port 0` (the M14
gate's own conditions: no `--metrics-listen`, since this gate's rows mirror
M14's). Calibration picked `K = 500`, the same rung the M14 gate used.
Log kept off-tree.

**Rows a/b/e are judged against the same-source rebuild resolution measured
on the day: `1.12 %`** — see the "numbers to record" note below.

| row | result |
|---|---|
| a | **not a regression, and NOT adjudicable against this bar.** Measured `n2eq`/`n1` = **0.539** (1 007 609 / 1 869 148 ops/s), which also misses the M14 gate's own ≥ 0.90 ratio bar the driver checks. The same-day A/B in [the time-and-timers gate](uc2-time-and-timers-gate-2026-09-03.md) settles it: on this rig, on the same hosts in the same minutes, the **pre-identity, pre-time-and-timers baseline tree `17d5c6b` straddles the same bar** — per-rep `n2eq`/`n1` of 0.678 / 0.734 / 0.626 and 0.717 / 0.810 / 1.362, against the head tree's 0.815 / 0.842 / 0.613 and 0.822 / 0.683 / 0.524. Neither tree sits cleanly above 0.90 here, and this gate's own arms are SINGLE SAMPLES in a distribution that wide. The 0.539 is where one sample landed, not a cost of FSM identity. **The bar itself is unreachable on this rig** (see row a of the time-and-timers gate: arm-to-arm spread 15–43 %, worst sem 13–21 %, against a 1.12 % build-noise bar; resolving would need ~430 reps per arm). Bar unchanged, per the honest-failure protocol; restating it is a maintainer decision |
| b | **PASS** — `pair`/`slow1` = **1.037** (738 288 / 712 009 ops/s), inside the driver's [0.9, 1.1] band and within the 1.12 % resolution's spirit. Same single-sample caveat as row a applies to the precision of the number, but the row is not close to its edge |
| e | **reported, no bar** (as committed) — `n2eq-ls` 22 478 ops/s = **0.0223×** its bounded twin; `pair-ls` 22 376 ops/s = **0.0303×**. Consistent in shape with the M14 gate's 0.0166× / 0.0282×: lockstep's cost remains an operating-envelope fact, unmoved by this release |
| j | **PASS** — joined at **24.61 s** (bar ≤ 60 s); `snapshot_session_refusals()` **`(0,0,0,0,0)` on every one of the four hosts**; 9 artifacts under each of ids 0 and 1 on the learner; ≥ 1 `snapshot_installed` observed (1); the all-FSMs-agree check passed on the learner; `client_lost` 0. **Note the bar is satisfied in a STRICTER form than committed**: this document's bar names a 3-tuple `(0, 0, 0)`, but the coordinated-snapshot work made `Node::snapshot_session_refusals()` a **5-tuple** (adding `position_mismatch` and `fetch_expired`), and all five read zero. Stricter, never weaker |

**Row j took three runs to adjudicate, and the first two failures were the
harness reading itself, not the row.** Recorded here because a future reader
will otherwise re-derive them: (1) the driver's `STATS_RE` matched THREE
refusal counters while `m12_gate.rs:2036` prints FIVE, so it never matched
the real line and every host scored `(-1,-1,-1)`, which the verdict read as
"not zero"; (2) once the regex was widened, `node_stats` still read only the
last 400 log lines, and `m12_gate` prints that line ONLY WHEN THE TUPLE
CHANGES — so on a healthy run it appears exactly once, at line 2, and had
scrolled out of the window on the two hosts whose logs had grown past 400
lines (the leader's was 742, node2's 498) while the two quieter hosts read
the true zeros. Every substantive clause of row j was passing throughout.
Both are fixed in `bench-infra/scripts/m14_fleet_gate.py` (commit `0ef7ddc`),
with a selftest asserting that an unreadable `-1` reading can never pass —
"could not read" and "is bad" must not be the same outcome.

## When this gate is run

1. Record `scripts/hop1_ab.sh`'s same-source rebuild resolution on the day,
   on the fleet host shape, before running anything else — this is the
   number rows a/b compare against, not a fixed percentage.
   **Done 2026-09-07: `1.12 %`** — node0, `--reps 6 --secs 6`, sink fixed,
   A `hb-1` `de953a8d6654` (mean 2 625 636 resp/s) vs B `hb-2b`
   `920ae7f73c8b` (mean 2 654 928), ranges OVERLAP.
   **Gotcha worth not re-learning: two builds of the same source into two
   different `CARGO_TARGET_DIR`s are BYTE-IDENTICAL** (both came out
   `de953a8d6654`), so that pair measures run noise while claiming to measure
   build noise — and produces an artificially tight bar, i.e. wrong in the
   direction that blesses a real regression. The second arm must be built from
   a tree at a DIFFERENT ABSOLUTE PATH, which is `scripts/apply_ab.sh`'s own
   discipline ("different absolute paths get baked into the binary"). The
   1.12 % it then produced sits right on CLAUDE.md's independently-observed
   ~1 % same-commit build spread.
2. Run `bench-infra/scripts/m14_fleet_gate.py`'s rows a/b/e/f (row f driven
   with named FSMs — the driver already speaks names since Task 9) against
   this branch's binaries.
3. Fill in the results table above; do not edit the bar table to match
   whatever the run produced.
4. Only after this gate (and the maintainer's version-number decision,
   `docs/reference/semver-policy.md`) does `docs/how-to/cut-a-release.md`
   apply.
