# Multi-process hard crash

Part of [Verification](../VERIFICATION.md), the map of what is proved, checked, and only bug-hunted.

This tier runs real node and service processes and kills them with `SIGKILL` while they are under load. A `SIGKILL` cannot be caught, so the process gets no chance to clean up. Recovery must stay linearizable. Starting up again is not enough. The tests live in [`testing/uc_crashtest/`](/testing/uc_crashtest).

Linearizable means every operation appears to take effect at one instant between its call and its return. The tests record each history and check it with WGL. WGL is the Wing-Gong-Lowe search, which looks for an order of the operations that explains every result. An oracle is the rule that decides pass or fail. Several tests also use the replication-equivalence oracle. It counts places where replicas disagree, and `equiv == 0` means it found none.

## Two FSMs

An FSM is a finite state machine, the user's replicated application logic. The two-FSM tests shipped in `2.8.1`, as part of M14c2. These two scenarios run a node with `--services 0,1`, so one node hosts two FSMs. Each FSM sits in its own row, and the rows are numbered 0 and 1. The tests check *both* FSMs' histories after every restart. They also check the replication-equivalence oracle across every restart.

| test | what it kills |
|---|---|
| `two_fsm_service_sigkill` | FSM 1's process, under load. The test then respawns it. |
| `two_fsm_node_sigkill` | The node and both services together. The test then brings the node back and reattaches both services. |

The two tests run six kill cycles between them. Every FSM history is `Linearizable`. The replication-equivalence oracle reads `equiv == 0` throughout.

## Timers

The timer test `two_fsm_timer_service_sigkill` shipped in `2.11.0`. It is the twin of `two_fsm_service_sigkill`. The difference is that the row-1 service runs `Timed<TimerSm>`. The test does a real `kill -9` of the timer FSM's process. It does this three times, under sustained schedule and cancel load.

The process comes back with an empty `Timed<TimerSm>`. Two things must then be rebuilt: the FSM's timer record and `Timed`'s own pending set. Both are rebuilt entirely by journal replay of `TIMER` frames. The in-process capstones exercise this reconstruction path too, but they never cross a process boundary on it. This test does.

The result is checked by the `uc_lincheck::timer::assert_timer_report` oracle. That is the same oracle the [linearizability capstones](linearizability.md) use.

## Snapshot instants

A snapshot instant is a log position at which every row freezes and builds a snapshot. The set at that position is complete when every row has built its part. The snapshot-instant test shipped in `2.11.0`. Its name is `snapshot_instant_abandoned_on_service_sigkill_mid_build_and_the_next_completes`.

### How the kill lands inside the build

The test runs a real two-row node process. Three worker threads put load on it. The test commands an instant `P1` through the cnc admin band. The row-1 service writes a sentinel as it **enters** `freeze()`. The test waits for that sentinel and then sends `kill -9`. So the kill lands inside the build by construction, not by timing luck.

### The causality gate

After the kill, a causality gate waits out the whole freeze window. It asserts two things:

- Row 1 published nothing at `P1`.
- The set at `P1` is still incomplete.

**That gate exists because the test once passed with the SIGKILL removed.** The artificially slow freeze alone had produced the abandonment. The slow-freeze wrapper section below covers that freeze. The gate makes sure the kill is what causes it.

### Recovery and the final counts

`P2` is commanded while row 1 is down, and it supersedes `P1`. Row 1 is then respawned and caught up. `P3` completes the set.

The final counts are asserted exactly: `(abandoned, row0_incomplete, row1_incomplete) == (2, 0, 2)`. The abandoned count is two rather than one because a catching-up row reaches `P1` first and declines `P2` as busy. That is spec §10 behaviour. Both FSMs' histories are still `Linearizable`, with `equiv == 0`.

### The slow-freeze wrapper

The wrapper that makes the freeze artificially slow lives in the crashtest crate only. It is opt-in, through `--snapshots`. So all six pre-existing hard-crash tests attach byte-for-byte as before.

## Rolling wire-level upgrade

The wire level is the version of the node-to-node protocol that a cluster has committed to speak. The rolling-upgrade test shipped in `2.15.0`. It is `rolling_upgrade_one_node_at_a_time_under_load_is_linearizable_and_raises`, in `testing/uc_crashtest/tests/rolling_upgrade.rs`. It runs wire-level spec §6.3 as real processes.

### What the test does

The test boots a 3-process cluster on the old binary, called the old side. A three-worker CAS-register load runs against it. The test rolls the three nodes to the new side one at a time. The new side is this tree's bin. Nodes 0 and 1 roll by a restart. Node 2 rolls by a `SIGKILL` mid-roll.

The test writes no level down. Instead it encodes the level rule for ANY adjacent pair of binaries. `L` is read from the old cluster's cnc `wire_level` word after it boots. The new side's top level is this tree's `WIRE_MAX`. The outcome depends on how `WIRE_MAX` compares with `L`.

### The step case

A **step** is `WIRE_MAX == L + 1`. Three clauses must hold:

- Every node's `wire_level` word must still read `L` while an old member remains.
- All three nodes must read `L + 1` within 30 s of the last roll. The leader raises the level on its own.
- The old binary is then started on node 2's directory. It must be refused by name within 20 s. The refusal is `WireLevelAboveBinary` on its stderr and exit status 1. Exit status 1 is the cluster agent's fail-stop exit.

### The same-level case

The same-level case is `WIRE_MAX == L`. This is a release that kept the wire minor. The roll completes. The level never moves. The test checks this again at the end of the run. The refusal clause is skipped, with a stated reason.

In both the step and the same-level shapes, the history across the whole roll is `Linearizable`. WGL checks it.

### The refusal case

The level rule admits adjacent levels only. So the test fails at once when the two sides are two or more levels apart. It also fails at once when the old side is above the new side. In both cases the failure names both sides' top levels.

### Where the old binary comes from

The old side is `UC2_BASE_NODE_BIN` when that variable is set. It points at the previous release's `uc_crashtest-node`, run with no override env. The release checklist runs the test this way for every release.

When `UC2_BASE_NODE_BIN` is unset, the test uses this tree's bin for both sides. The old side is pinned to `UC2_WIRE_RANGE=WIRE_MAX,WIRE_MAX`. The new side is pinned to `WIRE_MAX,WIRE_MAX+1`. This is the nightly's one-level step, run on every tree.

The node bin honours `UC2_WIRE_RANGE` only when built with `wire-level-override`. So the test is gated on both `hard-crash-tests` and `wire-level-override`.

## How to run it

```bash
cargo test -p uc_crashtest --features hard-crash-tests
cargo test -p uc_crashtest --features hard-crash-tests,wire-level-override --test rolling_upgrade
```
