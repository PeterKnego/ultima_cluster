# Partition / quorum-loss fault injection — design

## Goal

Prove the **consensus core** of `ultima_cluster` is correct under network
partitions, not just the service-reconstruction path: no split-brain (only a
quorum ever commits), no stale reads on a partitioned-away node, and clean
failure under total quorum loss (no phantom commits, recover on heal). Achieved
with an in-process network-fault layer keyed by node-pair, exercised by the
existing WGL linearizability harness.

## Why this needs new infrastructure

The cluster's inter-node transport is QUIC (`quinn`). Today nothing can make node
A unable to reach node B while both keep running — the existing harness faults are
process/task kills (node-kill, service-crash), never a *network* partition.
Split-brain prevention and quorum-loss behavior are therefore untested. Every
inter-node consensus RPC already funnels through a single place per peer; we add a
test-only drop hook there and drive it from the harness.

## Scope (v1)

- **Fault model:** full **symmetric** drop of a node-pair (block A↔B entirely).
  No artificial delay, no partial/probabilistic loss, no one-way/asymmetric links
  — those add model complexity without changing the core correctness claims;
  defer if ever needed.
- **Topology:** 3 nodes for all scenarios. The fault API is N-node-general, but
  the chosen scenarios (minority isolation, leader isolation, three-way quorum
  loss) are all expressible and decisive on 3 nodes; 5-node adds nothing they
  need.
- **Level:** in-process only (the existing in-process lincheck cluster). The
  multi-process `uc-crashtest` harness and OS-level partitions are out of scope
  for v1.

## The fault-injection mechanism

### Send hook (the chokepoint)

Every consensus RPC — `append_entries` (including heartbeats, which are
empty-entry append RPCs), `vote`, `install_snapshot` — is sent from one of the
three methods of `QuicRaftNetwork` in `uc_node/src/network/instance.rs`. Each
`QuicRaftNetwork` instance knows its **target** `NodeId` (from
`new_client(target)`) and can capture its **source** `NodeId` from the factory.

Add one early check at the top of each of the three send methods: if a fault
table is present and reports the `(source, target)` pair blocked, return
`Err(rpc_err(NetworkError::Disconnected))` immediately — before touching the
connection pool. openraft already treats `RPCError::Network` as a transient
unreachable-peer condition (retry + rely on election timeout), so a dropped pair
looks exactly like a real partition to the rest of the system.

Dropping at the **sender** on both nodes (they share one table) yields a clean
symmetric partition: A→B is dropped by A's hook, B→A by B's hook. No server-side
inbound drop and no connection eviction are required — existing idle QUIC
connections simply go unused during the (short) partition and are reusable on
heal (30s idle timeout >> test partition duration).

### Production stays pristine: cargo feature `fault-injection`

The entire mechanism is behind a **non-default** cargo feature `fault-injection`
on `uc_node`:

- `uc_node/src/network/fault.rs` — `#[cfg(feature = "fault-injection")]` module
  defining `FaultTable`.
- The send-method check in `instance.rs` and the `source`/table fields on
  `QuicRaftNetworkFactory` / `QuicRaftNetwork` — `#[cfg(feature = "fault-injection")]`.
- The injection point on `NodeConfig` (see below) — `#[cfg(feature = "fault-injection")]`.

With the feature off (production builds and the default `cargo test`), none of it
is compiled: zero runtime cost, zero API surface. Partition tests and the
partition-augmented capstone run with `cargo test -p uc_node --features fault-injection`.

### `FaultTable`

`network/fault.rs`:

- `FaultTable` — wraps interior-mutable shared state (e.g.
  `Mutex<HashSet<(NodeId, NodeId)>>` storing blocked ordered pairs; symmetric
  inserts store both directions) behind an `Arc`.
- `is_blocked(&self, src: NodeId, dst: NodeId) -> bool` — consulted by the hook.
- `set_partition(&self, groups: &[Vec<NodeId>])` — block every pair whose
  endpoints fall in different groups (expresses any split, incl. three-way).
- `isolate(&self, node: NodeId, all: &[NodeId])` — convenience: `{node}` vs rest.
- `heal(&self)` — clear all blocks.

All in-process test nodes are built pointing at the **same** `Arc<FaultTable>`,
so the harness changes the partition for the whole cluster atomically.

### Wiring the table into nodes

`NodeConfig` gains a `#[cfg(feature = "fault-injection")]`,
`#[doc(hidden)]` field (test-only): `pub fault_table: Option<Arc<FaultTable>>`.
`runtime/builder.rs` (also cfg-gated) passes it into
`QuicRaftNetworkFactory::new(..)` alongside `config.node_id` (the source). The
factory clones `(source_node_id, Option<Arc<FaultTable>>)` into each
`QuicRaftNetwork` it creates.

## Harness extension

`uc_node/tests/lincheck/cluster.rs` (`LinCluster`):

- Construct the cluster with a shared `Arc<FaultTable>`, injected into all three
  `NodeConfig`s. Use the existing `tight_raft_tuning()` (heartbeat 100ms,
  election 500–1000ms) so a partition triggers an election within the test window.
- New fault methods (all `&self`, internally locking like the existing ones):
  - `partition_minority(&self)` — isolate one follower (`{follower}` vs the other
    two, which retain quorum).
  - `partition_leader(&self)` — isolate the current leader into the minority,
    forcing the majority to elect a new leader.
  - `partition_quorum_loss(&self)` — three-way split `[[n1],[n2],[n3]]` so no
    side has a majority.
  - `heal(&self)` — `fault_table.heal()`.

The existing `History`/`Outcome`/`check_register` recording API is unchanged;
partition-killed in-flight ops are recorded `Indeterminate` exactly as
crash-killed ones are.

## Tests

### `uc_node/tests/lin_partition.rs` (feature-gated) — targeted scenarios

Each spins a 3-node `LinCluster`, drives a few workers, applies one partition,
holds it long enough to elect/observe, heals, and asserts WGL-linearizable plus
its specific property:

1. **Minority partition + heal.** Isolate a follower. Assert: the majority keeps
   committing (ok_count climbs on the majority); the isolated node serves **no
   stale read** — a linearizable read on it fails (the task14 ReadIndex barrier
   can't confirm a quorum) rather than returning an old value; after `heal()` the
   isolated node catches up and the full history is linearizable.
2. **Leader isolation.** Isolate the current leader. Assert: the majority elects a
   new leader and resumes committing; the old (isolated) leader cannot commit and
   steps down (no two leaders commit — guaranteed by quorum, confirmed by
   linearizability); on heal it rejoins as a follower.
3. **Total quorum loss + recovery.** Three-way split. Assert: writes and
   linearizable reads **fail cleanly** — each returns an error or is recorded
   `Indeterminate` within the client timeout, and **none is recorded `Ok`** (no
   false ack / phantom commit); after `heal()` the cluster re-forms and progress
   resumes (ok_count climbs); the full history is linearizable.

### Capstone integration (`uc_node/tests/lin_register.rs`)

Under `#[cfg(feature = "fault-injection")]`, add partition/heal to the seeded
fault scheduler's menu alongside the existing kill-leader and crash-service
faults: pick a random fault each round (including "partition a random
group split, hold ~1.5–2s, heal"), under heavy concurrent worker load, across the
existing seed set. Assert WGL-linearizable + the existing liveness gate (a
sufficient fraction of ops complete `Ok`, proving the cluster recovers after each
partition). The non-feature build keeps running the existing capstone unchanged.

## Correctness rationale

- **No split-brain:** only a quorum can advance the Raft log; a minority (or a
  no-majority split) cannot commit. The WGL checker would flag any two-leaders
  divergence as non-linearizable.
- **No stale reads under partition:** linearizable reads go through
  `Raft::ensure_linearizable` (ReadIndex), which needs a quorum confirm — on a
  partitioned-away node it fails instead of returning a stale value (the task14
  read barrier is exactly what closes this).
- **Clean failure under quorum loss:** with no quorum, `client_write` /
  `ensure_linearizable` error out within the client timeout; the worker records
  `Indeterminate`/error, never `Ok`. The checker confirms nothing committed that
  shouldn't have.

## Components / files

- `uc_node/Cargo.toml` — add `[features] fault-injection = []`.
- `uc_node/src/network/fault.rs` — NEW (cfg): `FaultTable` + helpers.
- `uc_node/src/network/mod.rs` — cfg `pub mod fault;`.
- `uc_node/src/network/instance.rs` — cfg send-hook check in the 3 RPC methods;
  cfg `source` + `fault_table` fields.
- `uc_node/src/network/factory.rs` — cfg `source` + `fault_table`, threaded into
  `QuicRaftNetwork`.
- `uc_node/src/config.rs` — cfg `#[doc(hidden)] fault_table` on `NodeConfig`.
- `uc_node/src/runtime/builder.rs` — cfg wiring into the factory.
- `uc_node/tests/lincheck/cluster.rs` — shared `FaultTable`, new fault methods.
- `uc_node/tests/lin_partition.rs` — NEW (feature-gated): the 3 scenario tests.
- `uc_node/tests/lin_register.rs` — cfg partition fault in the capstone scheduler.
- `docs/tasks/task15_partition_fault_injection.md` — canonical record (written at
  consolidation time).

## Risks / open points

- **Election timing vs partition duration.** Partitions must outlast
  `election_timeout_max` to trigger a new election; with tight tuning (~1s max)
  hold partitions ~1.5–2s. Too-short partitions are a flaky no-op; bounded by the
  scenario constants.
- **CPU contention flakiness.** Like the existing capstone, partition tests run
  `--test-threads=1`; the seeded capstone already serializes via `CLUSTER_SERIAL`.
- **Quorum-loss client behavior.** A former leader's `client_write` must *return*
  (error) within the client timeout rather than hang; verified by the clean-fail
  assertion. If a path hangs, that's a real bug this test surfaces.
- **Connection reuse on heal.** Relies on QUIC idle timeout (30s) >> partition
  duration so connections survive; if a future change shortens idle timeout, heal
  may need an explicit reconnect nudge. Documented, not handled in v1.
- **Feature-gated test surface.** The partition tests and the capstone's partition
  branch only compile/run under `--features fault-injection`; the default
  `cargo test` path is unchanged. CI must add the feature run to exercise them.
