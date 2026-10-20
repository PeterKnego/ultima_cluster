# uc2 wire-level rolling-upgrade gate — 2026-10-19

**Status: NOT RUN.** This document is a PRE-COMMITMENT, not a record. The
driver is `bench-infra/scripts/wire_roll_gate.py`; the maintainer runs it on
the fleet. Spec: `docs/superpowers/specs/2026-10-19-uc2-wire-level-design.md`
§6.4. Functional clauses only: there is **no rate bar**.

> **Decide rule committed before any run.** This document's bar table is
> committed, with every result cell **UNRUN**, before any fleet run against
> it — the honest-failure protocol carried forward from
> M7/M9/M10/M11/M12/M13/M14/M14c2 and every 2.11.0/2.12.0 gate skeleton
> since (FSM identity, time-and-timers, the log-clock gate). Nothing in the
> bar table may be edited to match a result: a run that misses a bar is
> recorded as a **FAIL** and the bar is **kept, unmoved**. This document
> itself is a PRE-COMMITMENT, not a record.

## Command

```
python3 bench-infra/scripts/wire_roll_gate.py --selftest   # verdict arithmetic, no ssh
python3 bench-infra/scripts/wire_roll_gate.py --run [--hosts pub/priv,...] [--tree <checkout>]
```

Exit 0 = PASS, 1 = FAIL.

## Procedure

Three real `uc2-node` daemons, built with `--features wire-level-override`,
start with `UC2_WIRE_RANGE=12,12` (the old side). A fourth host drives load
(`client-remote` through each node's edge, `count` FSM) for the whole roll.
Each node in turn is killed and restarted with `UC2_WIRE_RANGE=12,13`; the
driver waits for its `uc2_agent_alive{agent="cluster"}` to read 1 and samples
`uc2_commit_position` on the surviving nodes, recording the longest interval
over which the sampled position did not rise above its high-water mark as
that restart's gap. Only a rise ends a stall: a dip in the sampled max (one
survivor silent for a tick, or a failover stepping the exported position
back before it climbs again) is not movement, so it cannot split one stall
into two short ones ([#183](https://github.com/PeterKnego/ultima_cluster/issues/183)).
Then every node's `uc2_wire_level` must read 13 within 60 s. Commit sampling
starts before each kill and runs through the cluster-agent wait, scraping
both surviving nodes and taking the max; a tick on which neither answers is
skipped, and the per-roll log line counts skipped and partial ticks so a
sampler that lost its observers is visible. A stall is measured from its
first sample at the mark to the first sample above it, the upper bound: it
over-reads by at most one ssh scrape tick (~1 s, the resolution), a blind
stretch of skipped ticks counts as stall because nothing proves commit moved
while no observer answered, and a stall shorter than a tick is invisible.
Finally the load stops and `m12_gate check-fsms` checks FSM state agreement
across nodes. Linearizability under the roll is
proven by the two-binary crashtest
(`testing/uc_crashtest/tests/rolling_upgrade.rs`, WGL on a real history); this
row proves the cross-host roll, the commit gap and the level.

For `2.15.0` the old side is **the same tree** with `UC2_WIRE_RANGE=12,12`.
From `2.16.0` the old side is the **previous release**; parameterize the
levels then.

## Clauses and pre-committed bars

| clause | bar | result |
|---|---|---|
| level | `uc2_wire_level == 13` on every node within 60 s of the last restart | NOT RUN |
| gaps | longest commit stall per node restart `<= 5.0 s` (`ROW_COMMIT_GAP_BAR_SECS`) | NOT RUN |
| fsm_agreement | FSM state agreement across nodes after the roll (`m12_gate check-fsms`) | NOT RUN |

Row verdict: PASS only if all three clauses hold.
