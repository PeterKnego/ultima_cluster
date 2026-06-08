# Partition / Quorum-Loss Fault Injection — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the consensus core never splits-brain or serves stale reads under network partitions, and fails cleanly under total quorum loss, by adding a test-only in-process QUIC fault layer driven by the WGL linearizability harness.

**Architecture:** A `FaultTable` (set of blocked node-pairs) is consulted at the single inter-node send chokepoint (`QuicRaftNetwork`'s three RPC methods). A blocked `(source, target)` pair returns `NetworkError::Disconnected` before the wire, which openraft reads as a normal unreachable peer. The whole mechanism is behind a non-default cargo feature `fault-injection`, threaded into nodes via `NodeBuilder` (NOT `NodeConfig`, to avoid touching the many `NodeConfig` literals). The `LinCluster` test harness gains partition/heal fault methods; targeted scenario tests + the seeded capstone exercise them.

**Tech Stack:** Rust, openraft 0.10, quinn (QUIC), the existing `uc-lincheck` WGL checker + `LinCluster` harness.

**Spec:** `docs/superpowers/specs/2026-06-08-partition-fault-injection-design.md`

**Branch:** `feat/partition-fault-injection` (already created).

---

## Key facts about the existing code (read before starting)

- Send chokepoint: `uc_node/src/network/instance.rs` — `QuicRaftNetwork` has fields `target: NodeId`, and three `RaftNetwork` methods (`append_entries`, `install_snapshot`, `vote`), each calling `self.get_or_connect()` then `conn.request(...)`. `rpc_err(NetworkError) -> RPCError<...>` is the error mapper. `QuicRaftNetwork::new(target, peer_addr, endpoint, client_cfg, pool, app_id)`.
- Factory: `uc_node/src/network/factory.rs` — `QuicRaftNetworkFactory::new(endpoint, client_cfg, app_id)`; `new_client(target, node)` builds a `QuicRaftNetwork` and calls `.into_v2()`.
- `NetworkError::Disconnected` already exists (`uc_node/src/network/mod.rs:36`). Module decls are `pub mod client; ... pub mod instance; ...`.
- Node build path: `uc_node/src/runtime/builder.rs` — `NodeBuilder<S>{ config, state_machine }`, `NodeBuilder::new(config, sm)`, `start(self)`. `start` calls the free fn `finish<A,S>(config, log_storage, handles_for_node, adapter, handle_sm, ...3×None, RaftHandle)` in BOTH the `Embedded` and `Shmem` match arms. Inside `finish` (~line 360): `let network = QuicRaftNetworkFactory::new(client_endpoint, client_tls_cfg, config.app_id.clone());` then `Raft::new(config.node_id, raft_config, network, ...)`.
- `NodeId = u64` (`uc_node/src/raft/mod.rs`).
- Harness: `uc_node/tests/lincheck/cluster.rs` — `LinCluster{ nodes: Mutex<Vec<Node>>, _serial }`; `node_config(id, instance, data, addr, peers) -> NodeConfig` uses `RaftTuning::default()` (election 1000–2000 ms); `start_3()` spawns `NodeBuilder::new(cfg, RegisterSm::default()).start()` per node; `leader_id()`, `wait_for_stable_leader(timeout) -> NodeId`, `client_for(id)`, `submit_cmd`/`read` (leader-routed), `SubmitOutcome`/`ReadOutcome { Ok, Indeterminate, Fatal }`. `kill_and_restart_leader()` / `crash_and_restart_leader_service()` are the existing fault methods.
- Capstone: `uc_node/tests/lin_register.rs` — `worker(...)` + a fault scheduler `while History::ok_count(&history.snapshot()) < target_ops { sleep(fault_period); if rng.random_bool(0.5) { kill } else { crash } }`, then `check_register(&entries)` asserting `Verdict::Linearizable` + an 80% liveness gate.

**cfg note:** `#[cfg(...)]` attributes are valid on struct fields, function parameters, function-call arguments, and `let` rebindings — this plan relies on that to thread the feature cleanly. Production library code (feature off) compiles the fault layer out entirely.

---

## Task 1: `fault-injection` feature + `FaultTable`

**Files:**
- Modify: `uc_node/Cargo.toml` (add `[features]`)
- Create: `uc_node/src/network/fault.rs`
- Modify: `uc_node/src/network/mod.rs` (cfg module decl)

- [ ] **Step 1: Add the feature to `uc_node/Cargo.toml`.** Find the `[dependencies]` section; add ABOVE it (or after `[package]`):

```toml
[features]
# Test-only in-process network fault injection (partition / quorum-loss).
# Off by default: zero production surface. Enable for partition tests:
#   cargo test -p uc_node --features fault-injection
fault-injection = []
```

- [ ] **Step 2: Create `uc_node/src/network/fault.rs`** with the `FaultTable` and its unit tests:

```rust
//! Test-only network fault injection (cargo feature `fault-injection`).
//!
//! A [`FaultTable`] is a shared set of blocked node-pairs, consulted at the QUIC
//! send chokepoint ([`super::instance::QuicRaftNetwork`]) to simulate a network
//! partition: a blocked `(src, dst)` pair makes the outbound RPC fail with
//! [`super::NetworkError::Disconnected`] before it reaches the wire, which
//! openraft treats as a normal unreachable peer.
//!
//! Partitions are **symmetric**: [`FaultTable::set_partition`] / [`FaultTable::isolate`]
//! insert both directions, so blocking A↔B drops A→B and B→A. The whole module is
//! compiled out without the `fault-injection` feature.

use std::collections::HashSet;
use std::sync::Mutex;

use crate::raft::NodeId;

/// Shared table of blocked ordered node-pairs. Clone the `Arc` into every node in
/// a test cluster so the harness can change the partition for all nodes at once.
#[derive(Debug, Default)]
pub struct FaultTable {
    blocked: Mutex<HashSet<(NodeId, NodeId)>>,
}

impl FaultTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// True if `src` is currently forbidden from sending to `dst`.
    pub fn is_blocked(&self, src: NodeId, dst: NodeId) -> bool {
        self.blocked.lock().unwrap().contains(&(src, dst))
    }

    /// Replace the partition with one where nodes in different `groups` cannot
    /// talk (both directions), while nodes within a group can. Expresses any
    /// split, including a three-way `[[1],[2],[3]]` total quorum loss.
    pub fn set_partition(&self, groups: &[Vec<NodeId>]) {
        let mut b = self.blocked.lock().unwrap();
        b.clear();
        for (gi, g) in groups.iter().enumerate() {
            for (hi, h) in groups.iter().enumerate() {
                if gi == hi {
                    continue;
                }
                for &a in g {
                    for &c in h {
                        b.insert((a, c));
                    }
                }
            }
        }
    }

    /// Isolate `node` from every other node in `all` (both directions). Leaves
    /// any existing blocks in place (additive).
    pub fn isolate(&self, node: NodeId, all: &[NodeId]) {
        let mut b = self.blocked.lock().unwrap();
        for &other in all {
            if other == node {
                continue;
            }
            b.insert((node, other));
            b.insert((other, node));
        }
    }

    /// Clear all blocks — heal the network.
    pub fn heal(&self) {
        self.blocked.lock().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_blocks_nothing() {
        let t = FaultTable::new();
        assert!(!t.is_blocked(1, 2));
        assert!(!t.is_blocked(2, 1));
    }

    #[test]
    fn isolate_blocks_both_directions_only_for_node() {
        let t = FaultTable::new();
        t.isolate(1, &[1, 2, 3]);
        assert!(t.is_blocked(1, 2));
        assert!(t.is_blocked(2, 1));
        assert!(t.is_blocked(1, 3));
        assert!(t.is_blocked(3, 1));
        // Other pair untouched.
        assert!(!t.is_blocked(2, 3));
        assert!(!t.is_blocked(3, 2));
    }

    #[test]
    fn set_partition_two_groups() {
        let t = FaultTable::new();
        t.set_partition(&[vec![1], vec![2, 3]]);
        assert!(t.is_blocked(1, 2));
        assert!(t.is_blocked(2, 1));
        assert!(t.is_blocked(1, 3));
        assert!(t.is_blocked(3, 1));
        // Within the {2,3} group: allowed.
        assert!(!t.is_blocked(2, 3));
        assert!(!t.is_blocked(3, 2));
    }

    #[test]
    fn set_partition_three_way_blocks_all_cross_pairs() {
        let t = FaultTable::new();
        t.set_partition(&[vec![1], vec![2], vec![3]]);
        for (a, b) in [(1, 2), (2, 1), (1, 3), (3, 1), (2, 3), (3, 2)] {
            assert!(t.is_blocked(a, b), "({a},{b}) should be blocked");
        }
    }

    #[test]
    fn set_partition_replaces_and_heal_clears() {
        let t = FaultTable::new();
        t.isolate(1, &[1, 2, 3]);
        // set_partition replaces (clears first).
        t.set_partition(&[vec![1, 2, 3]]); // single group → nothing blocked
        assert!(!t.is_blocked(1, 2));
        t.set_partition(&[vec![1], vec![2, 3]]);
        assert!(t.is_blocked(1, 2));
        t.heal();
        assert!(!t.is_blocked(1, 2));
    }
}
```

- [ ] **Step 3: Add the cfg module decl in `uc_node/src/network/mod.rs`.** After the existing `pub mod client;` line add:

```rust
#[cfg(feature = "fault-injection")]
pub mod fault;
```

- [ ] **Step 4: Run the unit tests (feature on):**

Run: `cargo test -p uc_node --features fault-injection network::fault`
Expected: 5 tests pass (`empty_blocks_nothing`, `isolate_...`, `set_partition_two_groups`, `set_partition_three_way_...`, `set_partition_replaces_and_heal_clears`).

- [ ] **Step 5: Confirm zero surface without the feature:**

Run: `cargo build -p uc_node` then `cargo clippy -p uc_node --all-targets -- -D warnings`
Expected: builds clean; `network::fault` is not compiled.

- [ ] **Step 6: Commit.**

```bash
git add uc_node/Cargo.toml uc_node/src/network/fault.rs uc_node/src/network/mod.rs
git commit -m "feat(net): FaultTable + fault-injection feature (test-only partition table)"
```

---

## Task 2: Thread `FaultTable` into the send path

**Files:**
- Modify: `uc_node/src/network/instance.rs` (cfg fields + cfg `with_fault` + cfg send-hook in 3 methods)
- Modify: `uc_node/src/network/factory.rs` (cfg fields + cfg `set_fault_injection` + pass into `new_client`)
- Modify: `uc_node/src/runtime/builder.rs` (cfg field on `NodeBuilder` + cfg `with_fault_table` + thread into `finish`)

- [ ] **Step 1: `QuicRaftNetwork` — add cfg fields + a cfg setter (`instance.rs`).** Add two cfg fields to the struct (after `app_id`):

```rust
pub struct QuicRaftNetwork {
    target: NodeId,
    peer_addr: SocketAddr,
    endpoint: Endpoint,
    client_cfg: Arc<ClientConfig>,
    pool: PeerPool,
    app_id: String,
    #[cfg(feature = "fault-injection")]
    source: NodeId,
    #[cfg(feature = "fault-injection")]
    fault_table: Option<Arc<super::fault::FaultTable>>,
}
```

In `QuicRaftNetwork::new`, initialize the cfg fields in the returned `Self { .. }` (add these two lines, cfg-gated, after `app_id,`):

```rust
            app_id,
            #[cfg(feature = "fault-injection")]
            source: 0,
            #[cfg(feature = "fault-injection")]
            fault_table: None,
        }
```

Add a cfg-gated builder method inside `impl QuicRaftNetwork` (after `new`):

```rust
    #[cfg(feature = "fault-injection")]
    pub(crate) fn with_fault(
        mut self,
        source: NodeId,
        fault_table: Option<Arc<super::fault::FaultTable>>,
    ) -> Self {
        self.source = source;
        self.fault_table = fault_table;
        self
    }
```

- [ ] **Step 2: Add the send-hook check to all three RPC methods (`instance.rs`).** At the very top of `append_entries`, `install_snapshot`, and `vote` (before encoding the body), insert the cfg-gated block:

```rust
        #[cfg(feature = "fault-injection")]
        if let Some(t) = &self.fault_table
            && t.is_blocked(self.source, self.target)
        {
            return Err(rpc_err(NetworkError::Disconnected));
        }
```

Note: `rpc_err` is generic over the error type `E`; it infers correctly at each call site (`install_snapshot` uses `InstallSnapshotError`, the others the default). `NetworkError` is already imported via `use super::{NetworkError, codec};`.

- [ ] **Step 3: `QuicRaftNetworkFactory` — cfg fields + setter + pass-through (`factory.rs`).** Add cfg fields to the struct (after `app_id`):

```rust
    app_id: String,
    #[cfg(feature = "fault-injection")]
    source: NodeId,
    #[cfg(feature = "fault-injection")]
    fault_table: Option<Arc<super::fault::FaultTable>>,
}
```

In `QuicRaftNetworkFactory::new`, init them in the returned `Self { .. }` (after `app_id,`):

```rust
            app_id,
            #[cfg(feature = "fault-injection")]
            source: 0,
            #[cfg(feature = "fault-injection")]
            fault_table: None,
        }
```

Add a cfg setter (inside `impl QuicRaftNetworkFactory`, after `new_with_default_endpoint`):

```rust
    #[cfg(feature = "fault-injection")]
    pub fn set_fault_injection(
        &mut self,
        source: NodeId,
        fault_table: Option<Arc<super::fault::FaultTable>>,
    ) {
        self.source = source;
        self.fault_table = fault_table;
    }
```

In `new_client`, after constructing `QuicRaftNetwork::new(...)` but before `.into_v2()`, rebind under cfg. Change the body to:

```rust
    async fn new_client(&mut self, target: NodeId, node: &NodeAddr) -> Self::Network {
        let net = QuicRaftNetwork::new(
            target,
            node.raft_addr,
            self.endpoint.clone(),
            self.client_cfg.clone(),
            self.pool.clone(),
            self.app_id.clone(),
        );
        #[cfg(feature = "fault-injection")]
        let net = net.with_fault(self.source, self.fault_table.clone());
        net.into_v2()
    }
```

(`Arc` is already imported in both files.)

- [ ] **Step 4: `NodeBuilder` — cfg field + setter (`builder.rs`).** Change the struct + `new`:

```rust
pub struct NodeBuilder<S: StateMachine> {
    config: NodeConfig,
    state_machine: S,
    #[cfg(feature = "fault-injection")]
    fault_table: Option<std::sync::Arc<crate::network::fault::FaultTable>>,
}

impl<S: StateMachine> NodeBuilder<S> {
    pub fn new(config: NodeConfig, state_machine: S) -> Self {
        Self {
            config,
            state_machine,
            #[cfg(feature = "fault-injection")]
            fault_table: None,
        }
    }

    /// Test-only: attach a shared network fault table (partition injection).
    #[cfg(feature = "fault-injection")]
    pub fn with_fault_table(
        mut self,
        fault_table: std::sync::Arc<crate::network::fault::FaultTable>,
    ) -> Self {
        self.fault_table = Some(fault_table);
        self
    }
```

- [ ] **Step 5: Thread the table from `start` into `finish` (`builder.rs`).** `finish` is called in both match arms of `start`. Add a cfg-gated trailing parameter to `finish`'s signature:

```rust
async fn finish<A, S>(
    config: NodeConfig,
    log_storage: JournalLogStorage,
    handles_for_node: LogStorageHandles,
    adapter: A,
    handle_sm: SmAdapter<S>,
    reconcile_request: Option<Arc<tokio::sync::Notify>>,
    output_chan_rx: Option<tokio::sync::mpsc::Receiver<(u64, bytes::Bytes)>>,
    output_progress: Option<ultima_journal::StableValue<u64>>,
    raft_handle_kind: RaftHandle,
    #[cfg(feature = "fault-injection")] fault_table: Option<
        std::sync::Arc<crate::network::fault::FaultTable>,
    >,
) -> Result<NodeHandle<S>, ClusterError>
```

(Match the EXACT existing parameter names/types of `finish` — read the current signature first; only the final cfg-gated param is new. The three `None`s in the `start` call sites correspond to `reconcile_request`/`output_chan_rx`/`output_progress` — keep them as-is.)

In BOTH `finish` call sites inside `start` (the `Embedded` and `Shmem` arms), append the cfg-gated argument as the last argument:

```rust
                    RaftHandle::Embedded,
                    #[cfg(feature = "fault-injection")]
                    self.fault_table.clone(),
                )
                .await
```

and for the Shmem arm likewise append after its final `RaftHandle::...` argument:

```rust
                    #[cfg(feature = "fault-injection")]
                    self.fault_table.clone(),
                )
                .await
```

- [ ] **Step 6: Wire the table into the factory inside `finish` (`builder.rs`).** Locate `let network = QuicRaftNetworkFactory::new(client_endpoint, client_tls_cfg, config.app_id.clone());` and replace with:

```rust
    #[allow(unused_mut)]
    let mut network =
        QuicRaftNetworkFactory::new(client_endpoint, client_tls_cfg, config.app_id.clone());
    #[cfg(feature = "fault-injection")]
    network.set_fault_injection(config.node_id, fault_table);
```

(`#[allow(unused_mut)]` is required because without the feature `network` is never mutated.)

- [ ] **Step 7: Build both ways + clippy.**

Run: `cargo clippy -p uc_node --all-targets -- -D warnings`
Expected: clean (feature off — fault code compiled out).

Run: `cargo clippy -p uc_node --all-targets --features fault-injection -- -D warnings`
Expected: clean (feature on).

- [ ] **Step 8: Regression gate — existing multi-node tests still pass WITH the feature** (no partition set ⇒ identical behavior; proves the send hook is inert when the table is empty/None):

Run: `cargo test -p uc_node --features fault-injection --test m2_multi_node -- --test-threads=1`
Expected: same result as without the feature (all pass).

- [ ] **Step 9: Commit.**

```bash
git add uc_node/src/network/instance.rs uc_node/src/network/factory.rs uc_node/src/runtime/builder.rs
git commit -m "feat(net): thread FaultTable to the QUIC send chokepoint via NodeBuilder"
```

---

## Task 3: `LinCluster` partition fault methods

**Files:**
- Modify: `uc_node/tests/lincheck/cluster.rs`

The harness file is compiled both with and without the feature (the existing capstone `lin_register.rs` builds it without). So all fault-table code here is cfg-gated.

- [ ] **Step 1: Imports + the cfg field on `LinCluster`.** Near the top of `cluster.rs`, add a cfg import:

```rust
#[cfg(feature = "fault-injection")]
use uc_node::network::fault::FaultTable;
```

Add a cfg field to the `LinCluster` struct (alongside `nodes` / `_serial`):

```rust
    #[cfg(feature = "fault-injection")]
    fault_table: std::sync::Arc<FaultTable>,
```

- [ ] **Step 2: Create + share the table in `start_3`, attach to each builder.** In `start_3`, before the node-spawn loop, create the table under cfg:

```rust
        #[cfg(feature = "fault-injection")]
        let fault_table = std::sync::Arc::new(FaultTable::new());
```

Inside the spawn loop, change the `NodeBuilder` construction so the builder gets the table (rebind pattern — compiles both ways):

```rust
            let cfg = node_config(id, &instance, &data, *addr, peers.clone());
            let builder = NodeBuilder::new(cfg, RegisterSm::default());
            #[cfg(feature = "fault-injection")]
            let builder = builder.with_fault_table(fault_table.clone());
            let task = tokio::spawn(async move { builder.start().await });
```

When constructing the final `LinCluster { .. }` value, add the cfg field:

```rust
        let cluster = LinCluster {
            nodes: tokio::sync::Mutex::new(nodes),
            _serial: serial,
            #[cfg(feature = "fault-injection")]
            fault_table,
        };
```

- [ ] **Step 3: Attach the table on the restart path too (`kill_and_restart_leader`).** So a restarted node still honors partitions (the capstone mixes kills with partitions). Find the respawn `NodeBuilder::new(cfg, RegisterSm::default())` inside `kill_and_restart_leader` and apply the same rebind:

```rust
        let cfg = node_config(id, &instance, &data, addr, peers);
        let builder = NodeBuilder::new(cfg, RegisterSm::default());
        #[cfg(feature = "fault-injection")]
        let builder = builder.with_fault_table(self.fault_table.clone());
        let cnc_instance = instance.clone();
        let node_task = tokio::spawn(async move { builder.start().await });
```

- [ ] **Step 4: Add a helper to read from a SPECIFIC node + find a follower.** Add these cfg-gated methods to `impl LinCluster` (near `read`/`leader_id`). They let the partition tests probe the isolated side and choose who to isolate:

```rust
    /// Linearizable read addressed to a specific node's client (not leader-routed).
    /// Used to probe a partitioned-away node — it must NOT return a stale `Ok`.
    #[cfg(feature = "fault-injection")]
    pub async fn read_from(&self, node_id: NodeId) -> ReadOutcome {
        use uc_client::ClientError as CE;
        let Some(client) = self.client_for(node_id).await else {
            return ReadOutcome::Indeterminate;
        };
        match client.query_linearizable::<(), Option<u64>>(&()).await {
            Ok(v) => ReadOutcome::Ok(v),
            Err(CE::NotLeader { .. }) | Err(CE::BackpressureFull) => ReadOutcome::Indeterminate,
            Err(CE::Timeout(_))
            | Err(CE::ResponseOverwritten)
            | Err(CE::NodeStalled)
            | Err(CE::ServiceStalled) => ReadOutcome::Indeterminate,
            Err(other) => ReadOutcome::Fatal(format!("{other:?}")),
        }
    }

    /// A current follower id (any live node that isn't the leader), if known.
    #[cfg(feature = "fault-injection")]
    pub async fn a_follower_id(&self) -> Option<NodeId> {
        let lid = self.leader_id().await?;
        let ids: Vec<NodeId> = self.nodes.lock().await.iter().map(|n| n.id).collect();
        ids.into_iter().find(|&id| id != lid)
    }

    /// All live node ids.
    #[cfg(feature = "fault-injection")]
    pub async fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.lock().await.iter().map(|n| n.id).collect()
    }
```

- [ ] **Step 5: Add the partition fault methods.** Add to `impl LinCluster` (cfg-gated):

```rust
    /// Isolate one follower from the other two (minority partition). The
    /// remaining two keep quorum; the isolated node falls behind.
    #[cfg(feature = "fault-injection")]
    pub async fn partition_minority(&self) -> Option<NodeId> {
        let all = self.node_ids().await;
        let follower = self.a_follower_id().await?;
        self.fault_table.set_partition(&[
            vec![follower],
            all.iter().copied().filter(|&n| n != follower).collect(),
        ]);
        Some(follower)
    }

    /// Isolate the current leader into the minority; the other two must elect a
    /// new leader. Returns the (now isolated) old leader id.
    #[cfg(feature = "fault-injection")]
    pub async fn partition_leader(&self) -> Option<NodeId> {
        let all = self.node_ids().await;
        let lid = self.leader_id().await?;
        self.fault_table.set_partition(&[
            vec![lid],
            all.iter().copied().filter(|&n| n != lid).collect(),
        ]);
        Some(lid)
    }

    /// Three-way split — no side has a majority (total quorum loss).
    #[cfg(feature = "fault-injection")]
    pub async fn partition_quorum_loss(&self) {
        let groups: Vec<Vec<NodeId>> = self.node_ids().await.into_iter().map(|n| vec![n]).collect();
        self.fault_table.set_partition(&groups);
    }

    /// Heal all partitions.
    #[cfg(feature = "fault-injection")]
    pub async fn heal(&self) {
        self.fault_table.heal();
    }

    /// Count nodes whose client reports the SAME committed `last_applied` — used
    /// to confirm the isolated node catches up after heal.
    #[cfg(feature = "fault-injection")]
    pub async fn last_applied_of(&self, node_id: NodeId) -> Option<u64> {
        let client = self.client_for(node_id).await?;
        client.node_status().map(|s| s.last_applied)
    }
```

NOTE on `last_applied_of`: confirm the client status accessor before using it — search `uc_client` for an existing method exposing `cnc` `NodeStatus.last_applied` (e.g. `node_status()` / `status()`). If none is public, DROP `last_applied_of` and instead assert catch-up in Task 4 by reading a known-written value back from the healed node via `read_from`. Do not invent an API.

- [ ] **Step 6: Compile the harness both ways (it has no standalone test binary; compile via the capstone + a throwaway):**

Run: `cargo test -p uc_node --features fault-injection --test lin_register --no-run`
Expected: compiles (harness builds with the feature).

Run: `cargo test -p uc_node --test lin_register --no-run`
Expected: compiles (harness builds without the feature — cfg code excluded).

- [ ] **Step 7: Commit.**

```bash
git add uc_node/tests/lincheck/cluster.rs
git commit -m "test(lincheck): partition/heal fault methods + per-node read probe on LinCluster"
```

---

## Task 4: Targeted scenario tests (`lin_partition.rs`)

**Files:**
- Create: `uc_node/tests/lin_partition.rs`

All three tests run on 3 nodes, single-threaded. The WGL checker over the full recorded history is the safety oracle (it catches split-brain / stale reads as `Violation`); each test adds a scenario-specific liveness/behavior assertion. Partitions are held ≥ `election_timeout_max` + margin (default tuning is 2000 ms ⇒ hold ~3.5 s).

- [ ] **Step 1: Write the file** with the shared scaffolding + three tests:

```rust
//! Targeted network-partition linearizability tests (cargo feature
//! `fault-injection`). Each isolates part of a 3-node cluster, drives ops, then
//! heals, and asserts WGL-linearizability plus a scenario-specific property:
//!   - minority partition: majority keeps committing; isolated node serves no
//!     stale read; catches up on heal.
//!   - leader isolation: the majority elects a NEW leader; old leader can't commit.
//!   - quorum loss: writes/reads fail cleanly (never a false Ok); recover on heal.
//!
//! Run: cargo test -p uc_node --features fault-injection --test lin_partition -- --test-threads=1
#![cfg(feature = "fault-injection")]

#[path = "lincheck/mod.rs"]
mod lincheck;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use lincheck::cluster::{LinCluster, ReadOutcome, SubmitOutcome};
use uc_lincheck::checker::{Verdict, check_register};
use uc_lincheck::history::{History, Outcome};
use uc_lincheck::model::{Op, RegResp};
use uc_lincheck::register::{Cmd, CmdResp};

/// Hold a partition long enough to guarantee an election under default tuning
/// (election_timeout_max = 2000 ms).
const PARTITION_HOLD: Duration = Duration::from_millis(3500);

/// Leader-routed worker: same op mix as the capstone, recording into `history`.
async fn worker(
    id: u32,
    cluster: Arc<LinCluster>,
    history: Arc<History>,
    stop: Arc<AtomicBool>,
    last_seen: Arc<AtomicU64>,
    mut rng: StdRng,
) {
    while !stop.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(15)).await;
        match rng.random_range(0..3u8) {
            0 => {
                let v = rng.random_range(1..1000u64);
                let inv = history.invoke();
                let outcome = match cluster.submit_cmd(&Cmd::Write(v)).await {
                    SubmitOutcome::Ok(_) => Outcome::Ok(RegResp::Ack),
                    SubmitOutcome::Indeterminate => Outcome::Indeterminate,
                    SubmitOutcome::Fatal(e) => panic!("fatal submit: {e}"),
                };
                history.record(id, Op::Write(v), inv, outcome);
            }
            1 => {
                let inv = history.invoke();
                let outcome = match cluster.read().await {
                    ReadOutcome::Ok(v) => {
                        if let Some(x) = v {
                            last_seen.store(x, Ordering::Relaxed);
                        }
                        Outcome::Ok(RegResp::Value(v))
                    }
                    ReadOutcome::Indeterminate => Outcome::Indeterminate,
                    ReadOutcome::Fatal(e) => panic!("fatal read: {e}"),
                };
                history.record(id, Op::Read, inv, outcome);
            }
            _ => {
                let old = if rng.random_bool(0.7) {
                    last_seen.load(Ordering::Relaxed)
                } else {
                    rng.random_range(1..1000u64)
                };
                let new = rng.random_range(1..1000u64);
                let inv = history.invoke();
                let outcome = match cluster.submit_cmd(&Cmd::Cas { old, new }).await {
                    SubmitOutcome::Ok(CmdResp::CasResult(b)) => {
                        if b {
                            last_seen.store(new, Ordering::Relaxed);
                        }
                        Outcome::Ok(RegResp::CasOk(b))
                    }
                    SubmitOutcome::Ok(other) => panic!("unexpected cas resp: {other:?}"),
                    SubmitOutcome::Indeterminate => Outcome::Indeterminate,
                    SubmitOutcome::Fatal(e) => panic!("fatal cas: {e}"),
                };
                history.record(id, Op::Cas { old, new }, inv, outcome);
            }
        }
    }
}

/// Spawn 3 leader-routed workers; return their join handles + shared state.
fn spawn_workers(
    cluster: &Arc<LinCluster>,
    history: &Arc<History>,
    stop: &Arc<AtomicBool>,
    last_seen: &Arc<AtomicU64>,
    seed: u64,
) -> Vec<tokio::task::JoinHandle<()>> {
    (0..3u32)
        .map(|w| {
            let rng = StdRng::seed_from_u64(seed ^ (w as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
            tokio::spawn(worker(
                w,
                cluster.clone(),
                history.clone(),
                stop.clone(),
                last_seen.clone(),
                rng,
            ))
        })
        .collect()
}

fn assert_linearizable(entries: &[uc_lincheck::history::Entry], seed: u64, label: &str) {
    let ok = History::ok_count(entries);
    eprintln!("[lin_partition::{label}] seed={seed} ops={} ok={ok}", entries.len());
    match check_register(entries) {
        Verdict::Linearizable => {}
        Verdict::Inconclusive => {
            eprintln!("[lin_partition::{label}] seed={seed}: Inconclusive (checker budget)");
        }
        Verdict::Violation => panic!("[{label}] NOT linearizable (seed={seed})"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn minority_partition_and_heal() {
    let seed = 7u64;
    let cluster = Arc::new(LinCluster::start_3().await);
    let history = Arc::new(History::default());
    let stop = Arc::new(AtomicBool::new(false));
    let last_seen = Arc::new(AtomicU64::new(0));
    let handles = spawn_workers(&cluster, &history, &stop, &last_seen, seed);

    // Warm up, then snapshot Ok count.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let before = History::ok_count(&history.snapshot());

    // Isolate a follower; the majority (leader + 1) must keep committing.
    let isolated = cluster.partition_minority().await.expect("a follower");
    tokio::time::sleep(PARTITION_HOLD).await;
    let after = History::ok_count(&history.snapshot());
    assert!(
        after > before,
        "majority did not progress during minority partition ({before} -> {after})"
    );

    // The isolated node must NOT serve a stale linearizable read (it's a
    // follower / can't confirm quorum) — record outcomes; the WGL check is the
    // safety oracle. Probe a few times.
    for _ in 0..5 {
        let inv = history.invoke();
        let outcome = match cluster.read_from(isolated).await {
            ReadOutcome::Ok(v) => Outcome::Ok(RegResp::Value(v)),
            ReadOutcome::Indeterminate => Outcome::Indeterminate,
            ReadOutcome::Fatal(e) => panic!("fatal isolated read: {e}"),
        };
        history.record(100, Op::Read, inv, outcome);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    cluster.heal().await;
    cluster.wait_for_stable_leader(Duration::from_secs(15)).await;
    tokio::time::sleep(Duration::from_millis(800)).await; // let the isolated node catch up + serve

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }
    let cluster = Arc::try_unwrap(cluster).ok().expect("sole owner");
    cluster.shutdown().await;

    let entries = Arc::try_unwrap(history).ok().expect("sole owner").into_entries();
    assert!(History::ok_count(&entries) >= 30, "too few Ok ops; run is vacuous");
    assert_linearizable(&entries, seed, "minority");
}

#[tokio::test(flavor = "multi_thread")]
async fn leader_isolation_elects_new_leader() {
    let seed = 42u64;
    let cluster = Arc::new(LinCluster::start_3().await);
    let history = Arc::new(History::default());
    let stop = Arc::new(AtomicBool::new(false));
    let last_seen = Arc::new(AtomicU64::new(0));
    let handles = spawn_workers(&cluster, &history, &stop, &last_seen, seed);

    tokio::time::sleep(Duration::from_millis(800)).await;
    let old_leader = cluster.leader_id().await.expect("leader");
    let before = History::ok_count(&history.snapshot());

    // Isolate the leader; the other two must elect a new one.
    let isolated = cluster.partition_leader().await.expect("leader");
    assert_eq!(isolated, old_leader);
    tokio::time::sleep(PARTITION_HOLD).await;

    // A new leader must have emerged among the majority (different id), and the
    // cluster must resume committing.
    let new_leader = cluster.leader_id().await.expect("new leader on majority");
    assert_ne!(new_leader, old_leader, "majority failed to elect a NEW leader");
    let after = History::ok_count(&history.snapshot());
    assert!(after > before, "no progress after re-election ({before} -> {after})");

    cluster.heal().await;
    cluster.wait_for_stable_leader(Duration::from_secs(15)).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }
    let cluster = Arc::try_unwrap(cluster).ok().expect("sole owner");
    cluster.shutdown().await;

    let entries = Arc::try_unwrap(history).ok().expect("sole owner").into_entries();
    assert!(History::ok_count(&entries) >= 30, "too few Ok ops; run is vacuous");
    assert_linearizable(&entries, seed, "leader-isolation");
}

#[tokio::test(flavor = "multi_thread")]
async fn total_quorum_loss_fails_clean_then_recovers() {
    let seed = 88_888u64;
    let cluster = Arc::new(LinCluster::start_3().await);
    let history = Arc::new(History::default());
    let stop = Arc::new(AtomicBool::new(false));
    let last_seen = Arc::new(AtomicU64::new(0));
    let handles = spawn_workers(&cluster, &history, &stop, &last_seen, seed);

    tokio::time::sleep(Duration::from_millis(800)).await;

    // Three-way split: nobody has quorum.
    cluster.partition_quorum_loss().await;
    // Let any in-flight ops drain, then measure the no-quorum window.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let lo = History::ok_count(&history.snapshot());
    tokio::time::sleep(PARTITION_HOLD).await;
    let hi = History::ok_count(&history.snapshot());
    assert_eq!(
        lo, hi,
        "ops committed Ok during total quorum loss ({lo} -> {hi}) — split-brain / false ack"
    );

    // Heal: the cluster must re-form and resume committing.
    cluster.heal().await;
    cluster.wait_for_stable_leader(Duration::from_secs(15)).await;
    let recovered_from = History::ok_count(&history.snapshot());
    tokio::time::sleep(Duration::from_secs(2)).await;
    let recovered_to = History::ok_count(&history.snapshot());
    assert!(
        recovered_to > recovered_from,
        "cluster did not resume after heal ({recovered_from} -> {recovered_to})"
    );

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }
    let cluster = Arc::try_unwrap(cluster).ok().expect("sole owner");
    cluster.shutdown().await;

    let entries = Arc::try_unwrap(history).ok().expect("sole owner").into_entries();
    assert!(History::ok_count(&entries) >= 30, "too few Ok ops; run is vacuous");
    assert_linearizable(&entries, seed, "quorum-loss");
}
```

- [ ] **Step 2: Verify `History::snapshot`, `History::ok_count`, `Entry`, and the client status accessor exist.** Read `uc-lincheck/src/history.rs` to confirm `snapshot() -> Vec<Entry>`, `ok_count(&[Entry]) -> usize`, and that `Entry` is public (the capstone uses all of these). If `read_from`/`last_applied_of` in Task 3 referenced a client method that doesn't exist, fix per Task 3 Step 5's note before this compiles.

- [ ] **Step 3: Run each scenario test.**

Run: `cargo test -p uc_node --features fault-injection --test lin_partition -- --test-threads=1 --nocapture`
Expected: 3 tests pass; each prints its `[lin_partition::*]` line and `Linearizable` (or `Inconclusive`); none panics with a Violation. If a test flakes on timing (no election within the window), increase `PARTITION_HOLD`. If a real `Violation` appears, STOP — that's a genuine consensus bug; use systematic-debugging, do not weaken the test.

- [ ] **Step 4: Clippy.**

Run: `cargo clippy -p uc_node --all-targets --features fault-injection -- -D warnings`
Expected: clean.

- [ ] **Step 5: Commit.**

```bash
git add uc_node/tests/lin_partition.rs
git commit -m "test(lincheck): partition scenario tests — minority / leader-isolation / quorum-loss"
```

---

## Task 5: Partition in the seeded capstone

**Files:**
- Modify: `uc_node/tests/lin_register.rs`

Add partition/heal as a third fault kind in the capstone scheduler, only under the feature. The non-feature build keeps running the existing two-fault capstone unchanged.

- [ ] **Step 1: Extend the fault scheduler.** Find the scheduler loop:

```rust
        if fault_rng.random_bool(0.5) {
            cluster.kill_and_restart_leader().await;
        } else {
            cluster.crash_and_restart_leader_service().await;
        }
        faults += 1;
```

Replace with a feature-gated three-way choice (kill / crash / partition+heal). Under the feature, partitions are one of three random fault kinds; the partition kind randomly picks minority / leader / quorum-loss, holds, then heals and waits for a stable leader:

```rust
        #[cfg(feature = "fault-injection")]
        {
            match fault_rng.random_range(0..3u8) {
                0 => cluster.kill_and_restart_leader().await,
                1 => cluster.crash_and_restart_leader_service().await,
                _ => {
                    match fault_rng.random_range(0..3u8) {
                        0 => {
                            cluster.partition_minority().await;
                        }
                        1 => {
                            cluster.partition_leader().await;
                        }
                        _ => cluster.partition_quorum_loss().await,
                    }
                    // Hold past election_timeout_max (default 2000 ms), then heal.
                    tokio::time::sleep(std::time::Duration::from_millis(3500)).await;
                    cluster.heal().await;
                    cluster.wait_for_stable_leader(std::time::Duration::from_secs(15)).await;
                }
            }
        }
        #[cfg(not(feature = "fault-injection"))]
        {
            if fault_rng.random_bool(0.5) {
                cluster.kill_and_restart_leader().await;
            } else {
                cluster.crash_and_restart_leader_service().await;
            }
        }
        faults += 1;
```

- [ ] **Step 2: Run the capstone WITHOUT the feature (unchanged behavior, regression):**

Run: `cargo test -p uc_node --test lin_register -- --test-threads=1`
Expected: passes as before (kill + crash faults only).

- [ ] **Step 3: Run the capstone WITH partitions across seeds.** The capstone reads the seed internally (it has a default; if it accepts `LIN_SEED`, drive it — otherwise run it as-is which exercises its built-in seed list):

Run: `cargo test -p uc_node --features fault-injection --test lin_register -- --test-threads=1 --nocapture`
Expected: `Linearizable`, liveness gate satisfied (the partition holds are longer than kill/crash recoveries, so confirm `target_ops` is still reached within the test budget; if it times out, the partition branch may need a smaller hold or the capstone's `target_ops` lowered — adjust and note it). If a `Violation` appears, STOP and debug (real bug).

- [ ] **Step 4: Commit.**

```bash
git add uc_node/tests/lin_register.rs
git commit -m "test(lincheck): add partition/heal to the seeded capstone fault scheduler (feature-gated)"
```

---

## Task 6: Docs, final review, merge

**Files:**
- Create: `docs/tasks/task15_partition_fault_injection.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Write `docs/tasks/task15_partition_fault_injection.md`** — the canonical record. Fold in the design rationale from the spec so it stands alone: the problem (network partitions / quorum loss were untested), the mechanism (FaultTable at the `instance.rs` send chokepoint, `NetworkError::Disconnected` → openraft unreachable-peer; behind the `fault-injection` feature, threaded via `NodeBuilder` not `NodeConfig` and why), the harness fault methods, the three scenario tests + capstone integration, the correctness rationale (quorum ⇒ no split-brain; ReadIndex barrier ⇒ no stale partitioned read; clean-fail ⇒ no phantom commit), and the run command. Link to task14 (the ReadIndex barrier it relies on). Keep the superpowers spec/plan in place (per CLAUDE.md — do not delete).

- [ ] **Step 2: Update `CLAUDE.md`.** In the Build & Test Commands block, add under the existing `cargo test` lines:

```bash
cargo test -p uc_node --features fault-injection -- --test-threads=1   # network-partition / quorum-loss linearizability
```

In the "Apply pipeline" / testing narrative or the reconstruction paragraph, add one sentence noting that partition/quorum-loss correctness is covered by `uc_node/tests/lin_partition.rs` + the feature-gated capstone fault, via an in-process QUIC fault layer (`uc_node/src/network/fault.rs`).

- [ ] **Step 3: Commit docs.**

```bash
git add docs/tasks/task15_partition_fault_injection.md CLAUDE.md
git commit -m "docs(task15): partition / quorum-loss fault injection — canonical record + CLAUDE.md"
```

- [ ] **Step 4: Final whole-feature review.** Dispatch a reviewer over `git diff main...HEAD`: verify (a) the feature is truly compiled out without `fault-injection` (no production behavior change; `cargo clippy -p uc_node --all-targets -- -D warnings` clean), (b) the send hook is correct and inert when the table is empty, (c) the tests can actually catch a violation (not vacuous — liveness gates present), (d) clippy clean WITH the feature, (e) the existing default `cargo test -p uc_node` is unchanged. Address any Critical/Important findings.

- [ ] **Step 5: Full verification before merge.**

Run: `cargo clippy -p uc_node --all-targets -- -D warnings`
Run: `cargo clippy -p uc_node --all-targets --features fault-injection -- -D warnings`
Run: `cargo test -p uc_node -- --test-threads=1` (default path unchanged)
Run: `cargo test -p uc_node --features fault-injection --test lin_partition -- --test-threads=1`
Expected: all clean / green.

- [ ] **Step 6: Finish the branch.** Use `superpowers:finishing-a-development-branch` to merge `feat/partition-fault-injection` to `main` locally (the established per-feature pattern). Do not push unless the user asks.

---

## Self-review notes (author)

- **Spec coverage:** symmetric drop (Task 1 `FaultTable`), send chokepoint hook (Task 2), feature gate / zero production surface (Tasks 1–2, gates in Steps), `FaultTable` API `set_partition`/`isolate`/`heal` (Task 1), 3-node harness wiring (Task 3), all three targeted scenarios (Task 4), capstone integration (Task 5), task15 doc (Task 6). Correctness rationale (no split-brain / no stale read / clean fail) is asserted structurally: WGL check + per-scenario liveness/behavior assertions.
- **`NodeConfig` untouched on purpose:** adding a cfg field there would force every `m2/m3/m4` `NodeConfig` literal to add it under the feature. Threading via `NodeBuilder` + `finish` avoids that. This is the single most important structural decision.
- **Type consistency:** `FaultTable::{new,is_blocked,set_partition,isolate,heal}` used identically in Tasks 1/3; `with_fault` (network) / `set_fault_injection` (factory) / `with_fault_table` (NodeBuilder) are distinct, intentional names for the three layers. `ReadOutcome`/`SubmitOutcome`/`Outcome`/`Op`/`RegResp`/`Cmd`/`CmdResp` match the existing capstone exactly.
- **Open verification points flagged inline (do not invent APIs):** the client `last_applied`/status accessor in Task 3 Step 5 and `History::snapshot`/`ok_count`/`Entry` visibility in Task 4 Step 2 must be confirmed against the real source; fallbacks specified.
- **Flake control:** `--test-threads=1`, partitions held past `election_timeout_max`, liveness gates guard against vacuous passes.
