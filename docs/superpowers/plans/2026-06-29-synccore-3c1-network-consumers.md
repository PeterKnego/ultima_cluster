# SyncCore 3c.1 — replication as per-peer busy-spin network consumers — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move openraft's replication off RaftCore's async tasks onto per-peer busy-spin disruptor consumers that drive `RaftNetworkV2` reactor-free, reproducing the exact ack contract — severing RaftCore's replication under the `sync-core` feature.

**Architecture:** Each follower gets a dedicated busy-spin OS thread (a "network consumer") fed by a per-peer disruptor **send ring**. `SyncCore::run_command`'s 8 currently-delegated task-spawning arms publish to these consumers instead of calling `self.core.run_command` (which spawns tokio `ReplicationCore`/heartbeat/vote tasks). The consumer is a **port of `ReplicationCore`** (`src/replication/mod.rs`) driven by the reactor-free `block_on` (hybrid reactor for quinn), emitting the same `Notification`s back via the existing `tx_notification` channel (the consensus loop already drains it). 3c.2 (separate plan) later moves those acks onto a disruptor input ring.

**Tech Stack:** Rust, openraft fork (`sync-core` feature), `disruptor` 4.3 (the ring pattern is established in `src/core/sync_durability.rs` — mirror it), tokio (hybrid reactor for quinn), the openraft 180-test suite as the correctness oracle.

## Global Constraints

- All work on the openraft fork `PeterKnego/openraft` branch `sync-core`, behind the `sync-core` Cargo feature. The default (RaftCore, feature-off) build path must remain behavior-identical (the 8 delegated arms keep working unchanged when the feature is off).
- Correctness oracle, green at every task: `cargo test -p tests --features sync-core` (**180** integration — replication/membership/snapshot/election are the real guards) + `cargo test -p openraft --features sync-core --lib` (**496**); default `cargo test -p openraft --lib` (**494**); `cargo clippy -p openraft --features sync-core -- -D warnings` and `-p openraft -- -D warnings` both clean. Crate at `/home/claude/ultima/openraft/openraft/`; cwd `/home/claude/ultima/openraft`.
- **The ack contract is law.** The per-peer consumer must reproduce, byte-for-byte in semantics, the `Notification`s `ReplicationCore`/`HeartbeatWorker`/`SnapshotTransmitter`/`spawn_parallel_vote_requests` emit. Reference + exact conditions are in the spec (`docs/superpowers/specs/2026-06-29-synccore-3c-disruptor-replication-design.md`, "Reproduced ack contract") and the source (`src/replication/mod.rs:323-456`, `src/core/heartbeat/worker.rs:152-206`, `src/replication/snapshot_transmitter.rs:107-261`, `src/core/raft_core.rs:1435-1520`). A wrong `inflight_id`, a dropped FIFO order, or a missing `matching.is_none()` guard corrupts quorum state.
- **Reactor-free I/O**: the consumer threads drive `RaftNetworkV2` via the reactor-free `block_on` from `sync_durability` (re-export/reuse it), entering the tokio runtime context (hybrid reactor) like the consensus thread does, so quinn finds a driver.
- **Acks via `tx_notification` in 3c.1** (the disruptor input ring is 3c.2). Consumers hold a clone of `tx_notification` and `send` `Notification`s, exactly as the tokio tasks do today.
- This is a PORT: read the reference subsystem completely before porting each piece. Do not invent new replication semantics.

---

## File structure

- Create `openraft/src/core/sync_network.rs` — the network-consumer module: `NetOp` (send-ring op enum), `NetEvent` (ring slot), `PeerConsumerHandle` (producer + join, mirrors `sync_durability::LogStoreHandle`), `spawn_peer(...)`, the per-peer `consumer_loop`, and the ported executor (`PeerExecutor`). One file, mirroring `sync_durability.rs`'s structure.
- Modify `openraft/src/core/mod.rs` — `#[cfg(feature="sync-core")] mod sync_network;` + re-exports.
- Modify `openraft/src/core/sync_core.rs` — `SyncCore` gains a `peers: PeerTable<C>` field; `run_command`'s 8 delegated arms publish to peer consumers / manage the peer table instead of delegating.
- Modify `openraft/src/core/sync_durability.rs` — make `block_on` reachable for `sync_network` (already `pub(crate)`); no behavior change.
- Test: unit tests in `sync_network.rs` (`#[cfg(test)]`) for the ring round-trip and the ack-contract; the 180-suite + UC lincheck/partition as integration guards.

---

## Task 1: Network-consumer scaffold (ring + per-peer thread + handle), inert

Establish the disruptor ring + busy-spin per-peer thread + lifecycle plumbing, mirroring `sync_durability`, with a **no-op executor**. Nothing routes to it yet (run_command still fully delegates), so behavior is unchanged and the suite stays green. This isolates the plumbing from the risky port.

**Files:**
- Create: `openraft/src/core/sync_network.rs`
- Modify: `openraft/src/core/mod.rs`
- Test: `openraft/src/core/sync_network.rs` `#[cfg(test)] mod tests`

**Interfaces:**
- Produces:
  - `pub(crate) enum NetOp<C: RaftTypeConfig>` with variants `Replicate { req: Replicate<C> }`, `Heartbeat { session_id: ReplicationSessionId<C>, committed: Option<LogIdOf<C>> }`, `Snapshot { inflight_id: InflightId }`, `Vote { req: VoteRequest<C>, kind: VoteKind }`, `TransferLeader { req: ... }` (exact payloads finalized in later tasks; Task 1 only needs the enum to exist with `Replicate` + a `#[allow(dead_code)]`).
  - `pub(crate) struct PeerConsumerHandle<C>` (mirrors `LogStoreHandle`: `producer: Option<SingleProducer<NetEvent<C>, SingleConsumerBarrier>>`, `join: Option<JoinHandle<()>>`; `publish(&mut self, op: NetOp<C>)`; `Drop` drops producer then joins).
  - `pub(crate) fn spawn_peer<C, NF>(target, network_client, tx_notify, reader_request_tx, config) -> PeerConsumerHandle<C>` — starts the busy-spin per-peer `consumer_loop`.
  - `pub(crate) type PeerTable<C> = std::collections::BTreeMap<C::NodeId, PeerConsumerHandle<C>>`.

- [ ] **Step 1: Write the failing ring round-trip test**

In `sync_network.rs` `#[cfg(test)] mod tests`, mirror `sync_durability`'s `consumer_services_writes_and_reader_requests` test shape: build the disruptor ring with a stub slot, publish 3 `NetOp::Replicate`-tagged values, assert a stub consumer receives all 3 in FIFO order and reports them back over an `mpsc`. Use the real `disruptor` API (`build_single_producer(64, factory, BusySpin).new_event_poller()`), and the real reactor-free `block_on` from `crate::core::sync_durability::block_on`. (This pins the ring + thread + FIFO wiring before any executor logic.)

- [ ] **Step 2: Run it — verify it fails (module doesn't exist)**

Run: `cargo test -p openraft --features sync-core --lib sync_network 2>&1 | tail -15`
Expected: FAIL — unresolved module `sync_network`.

- [ ] **Step 3: Implement the scaffold**

Write `sync_network.rs` mirroring `sync_durability.rs`: the `NetEvent<C>` slot (`Mutex<Option<NetOp<C>>>`, the move-only payload pattern — disruptor lends only `&event`), `NetOp` enum (Task 1: `Replicate` + `#[allow(dead_code)]` on unused variants), `PeerConsumerHandle` (producer + join, `publish`, `Drop`), `spawn_peer` starting a `std::thread` running `consumer_loop`, and `consumer_loop` that drains the ring via `EventPoller` with a **no-op `run_op`** (Task 1: take the op, drop it; `std::thread::yield_now()` when idle; exit on `Polling::Shutdown`). The thread enters the tokio runtime context (`rt_handle.enter()` — pass a `tokio::runtime::Handle` into `spawn_peer`, like the consensus thread). Add `#[cfg(feature="sync-core")] pub(crate) mod sync_network;` + re-exports to `core/mod.rs`. Keep everything `pub(crate)`.

- [ ] **Step 4: Run the round-trip test — verify it passes**

Run: `cargo test -p openraft --features sync-core --lib sync_network 2>&1 | tail -15`
Expected: PASS.

- [ ] **Step 5: Full gates (scaffold is inert → nothing changes)**

Run:
```bash
cargo test -p openraft --features sync-core --lib 2>&1 | grep "test result"
cargo test -p tests --features sync-core 2>&1 | grep -E "test result|FAILED"
cargo test -p openraft --lib 2>&1 | grep "test result"
cargo clippy -p openraft --features sync-core -- -D warnings 2>&1 | tail -3
```
Expected: lib 497/0 (496 + 1 new), integration 180/0, default lib 494/0, clippy clean. (Inert — `run_command` still delegates; no behavior change.)

- [ ] **Step 6: Commit**

```bash
cd /home/claude/ultima/openraft
git add openraft/src/core/sync_network.rs openraft/src/core/mod.rs
git commit -m "feat(sync-core): network-consumer scaffold (per-peer ring + busy-spin thread), inert

Mirrors sync_durability: disruptor send ring + per-peer busy-spin consumer thread +
PeerConsumerHandle, with a no-op executor. Nothing routes to it yet (run_command still
delegates); behavior unchanged, suite 180/497 green. Prepares the ReplicationCore port."
```

---

## Task 2: Port the append + heartbeat executor; flip Replicate/Heartbeat/Rebuild/Close

The core of 3c.1 and the riskiest task. Port `ReplicationCore`'s append+heartbeat send loop into `PeerExecutor` (run by `consumer_loop`'s `run_op`), and flip the four delegated arms (`RebuildReplicationStreams`, `CloseReplicationStreams`, `Replicate`, `BroadcastHeartbeat`) to manage/drive the peer table instead of delegating. Acks via `tx_notification`.

**Files:**
- Modify: `openraft/src/core/sync_network.rs` (the `PeerExecutor` port + `run_op`)
- Modify: `openraft/src/core/sync_core.rs` (`SyncCore.peers: PeerTable<C>` field; the 4 arms)
- Test: `sync_network.rs` ack-contract unit test + the 180-suite

**Interfaces:**
- Consumes: `PeerConsumerHandle`/`spawn_peer`/`NetOp` from Task 1; `block_on` from `sync_durability`; the reader-vend `log_reader_request_tx` side-channel (`raft_core.rs:207`).
- Produces: a `PeerExecutor<C, NF, LS>` that owns per-peer `matching`/inflight/payload state and drives RPCs; `SyncCore.peers`.

**Port reference (read completely before coding):** `src/replication/mod.rs` — `ReplicationCore::main` (199), `drain_events` (477), the stream build (264-278), `handle_response_stream` (323), `notify_progress` (427), `send_progress_error` (381), `notify_heartbeat_progress` (411); `inflight_append_queue.rs`; `event_watcher.rs`; `stream_state.rs`; heartbeat in `core/heartbeat/worker.rs:152-206`. The exact ack emits + conditions are enumerated in the spec's "Reproduced ack contract".

- [ ] **Step 1: Write the failing ack-contract unit test**

In `sync_network.rs` tests, drive a `PeerExecutor` with a **stub `RaftNetworkV2`** (returns a scripted `stream_append` result) and a stub gated reader, and assert the exact `Notification` sequence on a captured `tx_notify` for three scenarios: (a) successful match → `ReplicationProgress{Ok(Ok(Some(matching))), inflight_id=Some}` + `HeartbeatProgress`; (b) conflict → `ReplicationProgress{Ok(Err(conflict)), inflight_id}` (no `HeartbeatProgress` unless drain_acked timed); (c) RPC error with inflight → `ReplicationProgress{Err(_), inflight_id=Some}`. Assert NO `ReplicationProgress` when `matching.is_none()` on a heartbeat. This pins the contract.

- [ ] **Step 2: Run it — verify it fails**

Run: `cargo test -p openraft --features sync-core --lib sync_network::tests::ack_contract 2>&1 | tail -15`
Expected: FAIL (`PeerExecutor` not implemented).

- [ ] **Step 3: Port the append+heartbeat executor**

In `sync_network.rs`, implement `PeerExecutor` as a reactor-free port of `ReplicationCore`'s append+heartbeat path: own `remote_matched`, the `InflightAppendQueue` (port `inflight_append_queue.rs`), payload selection (log-id-range / `LogsSince` / zero-length heartbeat), entry reads via the `GatedLogReader` (request it from `log_reader_request_tx` at peer-spawn, like `spawn_replication_stream` at `raft_core.rs:1083`), the `network.append_entries`/`stream_append` call driven by `block_on`, `handle_response_stream` logic, backoff on RPC error. Emit the acks per the contract via `tx_notify`. `run_op` dispatches `NetOp::Replicate`/`NetOp::Heartbeat` into the executor. **Port method-by-method from the reference; do not redesign.**

- [ ] **Step 4: Flip the 4 arms in `sync_core.rs`**

Add `peers: PeerTable<C>` to `SyncCore` (init empty in `new`). In `run_command`, remove `RebuildReplicationStreams`/`CloseReplicationStreams`/`Replicate`/`BroadcastHeartbeat` from the `delegate` matches; handle them:
- `RebuildReplicationStreams{targets,...}` → for each target, `spawn_peer` (if absent) into `self.peers`; remove peers not in targets (drop their handle → joins). Port the heartbeat-worker spawn behavior into the same peer consumer.
- `CloseReplicationStreams` → clear `self.peers` (drops → joins).
- `Replicate{target, req}` → `self.peers.get_mut(&target).publish(NetOp::Replicate{req})`.
- `BroadcastHeartbeat{session_id,...}` → publish `NetOp::Heartbeat` to all peers.
(The other 4 arms — SendVote/SendPreVote/ReplicateSnapshot/BroadcastTransferLeader — stay delegated until Tasks 3-4.)

- [ ] **Step 5: Run ack-contract test + full suite**

Run:
```bash
cargo test -p openraft --features sync-core --lib sync_network 2>&1 | tail -5
cargo test -p tests --features sync-core 2>&1 | grep -E "test result|FAILED"
cargo test -p openraft --lib 2>&1 | grep "test result"
cargo clippy -p openraft --features sync-core -- -D warnings 2>&1 | tail -3
```
Expected: lib green, **integration 180/0** (replication/membership/heartbeat tests now exercise the ported executor — this is the real guard), default 494/0, clippy clean. If replication/membership tests fail, the ack contract or the readability gate is wrong — debug against the reference (systematic-debugging), do not patch the test.

- [ ] **Step 6: Commit**

```bash
cd /home/claude/ultima/openraft
git add openraft/src/core/sync_network.rs openraft/src/core/sync_core.rs
git commit -m "feat(sync-core): port append+heartbeat replication to per-peer network consumers

Replicate/BroadcastHeartbeat/RebuildReplicationStreams/CloseReplicationStreams now drive
per-peer busy-spin network consumers (ported ReplicationCore append+heartbeat path) instead
of delegating to RaftCore's tokio tasks. Acks reproduced via tx_notification. Suite 180/0."
```

---

## Task 3: Port snapshot streaming; flip ReplicateSnapshot

**Files:** Modify `sync_network.rs` (snapshot sub-state in `PeerExecutor`), `sync_core.rs` (the arm).
**Port reference:** `src/replication/snapshot_transmitter.rs` (whole file): `read_and_send_snapshot`, the `network.full_snapshot` call, cancel via `cancel_rx`, backoff, the `notify_progress(Ok(meta.last_log_id))` / `HeartbeatProgress` / `HigherVote` / `StorageError` emits (lines 107-261).

- [ ] **Step 1: Write the failing snapshot ack test** — stub `RaftNetworkV2::full_snapshot` returning success → assert `ReplicationProgress{Ok(Ok(meta.last_log_id)), inflight_id=Some}` + `HeartbeatProgress`; HigherVote → `HigherVote`; storage err → `StorageError`. Run, verify fail.
- [ ] **Step 2: Port the snapshot sub-state machine** into `PeerExecutor`: `NetOp::Snapshot{inflight_id}` starts a snapshot send (request a snapshot reader; `block_on(network.full_snapshot(...))`); the **cancel/replace** rule (risk 4) — a newer `Replicate`/`Snapshot` op cancels the in-progress snapshot; check the send ring + cancel between chunks so it doesn't stall. Reproduce the emits.
- [ ] **Step 3: Flip the arm** — remove `ReplicateSnapshot` from `delegate`; `self.peers.get_mut(&target).publish(NetOp::Snapshot{inflight_id})`.
- [ ] **Step 4: Run snapshot suite** — `cargo test -p tests --features sync-core 2>&1 | grep -E "snapshot|test result|FAILED"` + full integration 180/0 + lib + clippy. The `snapshot_streaming` / `snapshot_building` suites are the guard.
- [ ] **Step 5: Commit** — `feat(sync-core): port snapshot streaming to the network consumer (cancel/replace)`.

---

## Task 4: Port vote + transfer-leader; remove the delegation; lincheck

**Files:** Modify `sync_network.rs` (vote send), `sync_core.rs` (the 3 remaining arms + remove the now-empty `delegate` block), `raft_core.rs` (annotate/`#[cfg]` the now-dead `spawn_replication_stream`/`spawn_parallel_vote_requests`/heartbeat-spawn under sync-core, like prior phases annotated RaftCore twins).
**Port reference:** `spawn_parallel_vote_requests` (`raft_core.rs:1435-1520`) — per-voter `client.vote`/`pre_vote`, emit `VoteResponse`/`PreVoteResponse{target,resp,candidate_vote}` on reply, nothing on transport failure. `broadcast_transfer_leader` for `BroadcastTransferLeader`.

- [ ] **Step 1: Write the failing vote ack test** — stub `RaftNetworkV2::vote` → assert `Notification::VoteResponse{target,resp,candidate_vote}`; transport error → no notification. Run, verify fail.
- [ ] **Step 2: Port vote sending** — `NetOp::Vote{req, kind}` → `block_on(network.vote/pre_vote)` → emit `VoteResponse`/`PreVoteResponse`. For `SendVote`/`SendPreVote` (broadcast to all voters), publish a `Vote` op to each peer consumer (spawn transient peer entries for non-followers if needed, or a dedicated election fan-out — match the reference's per-voter behavior). `BroadcastTransferLeader` → publish a `TransferLeader` op to peers (`network.transfer_leader`).
- [ ] **Step 3: Remove delegation** — `run_command`'s `delegate` block is now empty (all 8 arms relocated); delete it and the `self.core.run_command(cmd).await` fallback; update the `_ => unreachable!()` accordingly. Under `#[cfg(feature="sync-core")]`, annotate the now-unused `RaftCore::spawn_replication_stream`/`spawn_parallel_vote_requests`/heartbeat-spawn as dead (`#[cfg_attr(feature="sync-core", allow(dead_code))]`) — do NOT delete (feature-off path uses them).
- [ ] **Step 4: Full gates + UC linearizability** —
  - `cargo test -p tests --features sync-core` 180/0; lib; default 494; clippy clean both.
  - In `/home/claude/ultima/ultima_cluster`: `cargo test -p uc_node --features sync-core --test lin_register 2>&1 | tail` (lincheck capstone) and `cargo test -p uc_node --features "sync-core fault-injection" --test lin_partition -- --test-threads=1 2>&1 | tail` (partition linearizability). These are the ultimate correctness guard that the reimplemented replication preserves linearizability under churn + faults. Expect linearizable / green.
- [ ] **Step 5: Commit** — `feat(sync-core): port vote+transfer-leader to consumers; remove RaftCore replication delegation`.

---

## Self-review notes

- **Spec coverage:** 3c.1's spec items map to tasks: per-peer consumers + send rings + lifecycle → Task 1+2; reimplemented append/heartbeat executor + ack contract → Task 2; readability via GatedLogReader (risk 2) → Task 2 Step 3; FIFO+inflight_id (risk 3) → Task 2 ack-contract test; snapshot cancel/replace (risk 4) → Task 3; reader-vend side-channel (risk 6) → Task 2 Step 3; acks via tx_notification (3c.1, not the ring) → all tasks; sever RaftCore delegation → Task 4. 3c.2 (input ring) is a SEPARATE plan — explicitly out of scope here.
- **Port, not placeholder:** Tasks 2-4's "port X" steps cite the exact reference (`src/replication/*`, file:line) + the ack contract (spec) + a concrete unit test pinning behavior + the 180-suite. For a faithful reimplementation of a tested subsystem, the reference source IS the spec; inlining ~500 lines of ported logic into the plan would be less accurate. The ack-contract unit tests + suite are the no-placeholder acceptance criteria.
- **Type consistency:** `NetOp`/`NetEvent`/`PeerConsumerHandle`/`PeerTable`/`PeerExecutor`/`spawn_peer` used consistently across tasks. `Replicate<C>`/`InflightId`/`ReplicationSessionId<C>`/`VoteRequest<C>` are the reference types (`src/replication/replicate.rs`, `replication_session_id.rs`).
- **Risk — parallelism:** per-peer thread (Task 1) preserves cross-follower parallelism; single-multiplexed is deferred to 3e (noted in spec).
- **Risk — the monster is Task 2;** if a fresh subagent can't hold the full append-path port + flip in one task, it may report DONE_WITH_CONCERNS / BLOCKED and the controller can split Step 3 (executor port) from Step 4 (the arm flip) into two commits within the task.
