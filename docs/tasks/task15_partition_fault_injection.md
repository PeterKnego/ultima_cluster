# Task 15 — Partition / Quorum-Loss Fault Injection

Canonical record for the network-partition fault-injection feature: an in-process
QUIC fault layer (behind a non-default cargo feature) driven by the WGL
linearizability harness to prove the **consensus core** — no split-brain, no stale
reads on a partitioned-away node, clean failure under total quorum loss — not just
the service-reconstruction path (task14).

Design history (retained, not required reading):
`docs/superpowers/specs/2026-06-08-partition-fault-injection-design.md` and
`docs/superpowers/plans/2026-06-08-partition-fault-injection.md`.

## Problem

The inter-node transport is QUIC (`quinn`). Before this feature, nothing could
make node A unable to reach node B while both kept running — the existing harness
faults were process/task kills (node-kill, service-crash), never a *network*
partition. So split-brain prevention and quorum-loss behavior were untested. The
shmem/Raft architecture is designed for the node split; a partition test only
needs a way to drop inter-node RPCs by node-pair plus harness hooks to drive it.

## Mechanism

### Send hook (the chokepoint)

Every consensus RPC — `append_entries` (incl. heartbeats), `vote`,
`install_snapshot` — is sent from one of the three `RaftNetwork` methods of
`QuicRaftNetwork` (`uc_node/src/network/instance.rs`). Each instance knows its
**target** `NodeId` (from `new_client(target)`) and its **source** `NodeId` (from
the factory). A check at the top of each method consults an optional fault table;
if the `(source, target)` pair is blocked it returns
`Err(rpc_err(NetworkError::Disconnected))` **before** encoding the body or touching
the connection pool. openraft already treats `RPCError::Network` as a transient
unreachable-peer (retry + rely on election timeout), so a dropped pair is
indistinguishable from a real partition. Dropping at the **sender** on both nodes
(they share one table) gives a clean symmetric partition; no server-side inbound
drop and no connection eviction are needed (idle QUIC connections, 30 s timeout,
survive a short partition and are reused on heal).

### `FaultTable` (`uc_node/src/network/fault.rs`)

An `Arc`-shared `Mutex<HashSet<(NodeId, NodeId)>>` of blocked ordered pairs:
- `is_blocked(src, dst)` — the hot-path check.
- `set_partition(&[Vec<NodeId>])` — replace: block every cross-group ordered pair
  (expresses any split, incl. three-way quorum loss). Same-group pairs stay open.
- `isolate(node, all)` — additive; block `node` ↔ every other (both directions).
- `heal()` — clear.

### Behind cargo feature `fault-injection` (zero production surface)

The whole mechanism is `#[cfg(feature = "fault-injection")]`-gated: the
`network::fault` module, the `source`/`fault_table` fields on `QuicRaftNetwork`
and `QuicRaftNetworkFactory`, and the wiring. With the feature off (production and
the default `cargo test`) none of it is compiled — verified by clippy + build with
the feature off. Partition tests run with `--features fault-injection`.

### Threaded via `NodeBuilder`, NOT `NodeConfig`

The fault table is passed in through `NodeBuilder::with_fault_table(Arc<FaultTable>)`
(cfg-gated), threaded through the free `finish()` fn into
`QuicRaftNetworkFactory::set_fault_injection(node_id, table)`. This deliberately
avoids adding a field to `NodeConfig`: `NodeConfig` is constructed as an explicit
struct literal in every `m2/m3/m4` test, so a (cfg) field there would force all of
them to add it under the feature. `NodeBuilder::new` is a constructor, so a cfg
field defaulted in `new` plus a cfg setter touches nothing else. (Three setter
names by layer: `NodeBuilder::with_fault_table` → `QuicRaftNetworkFactory::set_fault_injection`
→ `QuicRaftNetwork::with_fault`.)

## Harness (`uc_node/tests/lincheck/cluster.rs`)

`LinCluster` holds the shared `Arc<FaultTable>` (cfg-gated), wired into every node
it builds — in `start_3()` and on the `kill_and_restart_leader` respawn path, so a
restarted node still honors partitions. New cfg-gated methods:
`partition_minority()` (isolate a follower), `partition_leader()` (isolate the
leader), `partition_quorum_loss()` (three-way split), `heal()`, `read_from(node_id)`
(linearizable read addressed to a specific node — probes a partitioned-away node),
`a_follower_id()`, `node_ids()`, `last_applied_of(node_id)`.

## Tests

- `uc_node/tests/lin_partition.rs` (feature-gated) — three targeted scenarios on a
  3-node cluster, each driving 3 leader-routed workers, recording a
  `uc_lincheck::History`, then asserting WGL-`Linearizable` (Inconclusive tolerated,
  Violation fails) plus a scenario property:
  - **minority + heal**: isolate a follower; the majority keeps committing
    (progress assertion); reads against the isolated node are recorded so the WGL
    checker is the oracle for "no stale read"; heal → catch up.
  - **leader isolation**: isolate the leader; the majority must elect a NEW,
    commit-capable leader — detected by probing the non-isolated nodes with
    `read_from` (an `Ok` linearizable read passes a ReadIndex quorum barrier, so it
    proves both election AND commit-capability; `leader_id()` is unusable here
    because an isolated openraft leader keeps claiming leadership in its own view).
    Hard assertion that the isolated old leader does **not** serve an `Ok` read
    (no split-brain).
  - **total quorum loss**: three-way split; `ok_count` must stay FLAT during the
    no-quorum window (a hard assertion — any committed `Ok` is a false ack /
    split-brain); heal → progress resumes.
- `uc_node/tests/lin_register.rs` — under the feature, the seeded capstone's fault
  scheduler gains partition/heal as a third fault kind (1-in-3), mixed with
  kill-leader and crash-service under heavy churn, across seeds. The non-feature
  build runs the existing two-fault capstone unchanged.

Run: `cargo test -p uc_node --features fault-injection -- --test-threads=1`
(both `lin_partition` and the partition-augmented capstone). `--test-threads=1` as
with all the in-process cluster tests.

### Flake handling (the openraft apply-assert)

Post-partition elections exercise an openraft-0.10-alpha `#[cfg(debug_assertions)]`
invariant (`sm/worker.rs:214`, `assert_eq!(end - 1, got_last_index)`) that
intermittently panics a node's apply-worker during apply/convergence — the same
deferred upstream flake as the m3 convergence race. It is NOT a linearizability
violation (the WGL checker never reports `Violation`). Two layers absorb it without
ever masking a real bug:
- **`start_3()` bounded boot-retry** — a boot that fails to reach a stable
  3-node cluster (node-start timeout / task panic / no-leader) tears down and
  retries with fresh temp dirs, up to 4 attempts. Helps the capstone too.
- **Scenario-level retry** in `lin_partition.rs` — each scenario is
  `run_*(seed) -> Result<(), String>` wrapped in a 3-attempt loop. ONLY
  transient liveness/convergence failures (no progress, election probe timeout,
  no resume after heal, vacuous `< 30` Ok) retry; every SAFETY failure (WGL
  `Violation`, isolated-node `Ok` read = split-brain, `ok_count` rising during
  quorum loss = false ack, worker `Fatal`) panics IMMEDIATELY and is never
  retried. Teardown runs on every path so a retry can't leak ports/shmem.

After the openraft alpha.20 → alpha.21 bump, the assert became much rarer
(0 occurrences in 9 `lin_partition` runs + 4 feature-on capstone runs); the
retries remain as cheap insurance. See `docs/openraft-known-issues.md`.

## Correctness rationale

- **No split-brain:** only a quorum can advance the Raft log; a minority (or a
  no-majority split) cannot commit. Any two-leaders divergence would surface as a
  WGL non-linearizable result.
- **No stale read under partition:** linearizable reads go through
  `Raft::ensure_linearizable` (ReadIndex), which needs a quorum confirm — on a
  partitioned-away node it fails instead of returning a stale value. This is the
  task14 read barrier; the partition tests are its adversarial proof.
- **Clean failure under quorum loss:** with no quorum, `client_write` /
  `ensure_linearizable` error within the client timeout; the op is recorded
  `Indeterminate`/error, never `Ok`. The checker confirms nothing committed that
  shouldn't have.

## Scope / deferred

- In-process only (the in-process lincheck cluster). Multi-process OS-level
  partitions (`uc-crashtest` + SIGSTOP/packet-filter) are out of scope for v1.
- Fault model is a full **symmetric** drop of a node-pair; no delay, partial loss,
  or asymmetric/one-way links.
- 3-node topology covers all three chosen scenarios; the `FaultTable` API is
  N-node-general.
