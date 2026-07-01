# SyncCore 3c.2 — disruptor input ring (sync feed) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the consensus loop's tokio-mpsc draining of the HOT sync-produced notifications (per-peer network acks + durability io-done) with a single disruptor **MPSC input ring** pulled via `EventPoller`, and delete the `io_completion_forwarder` tokio task — so the hot ack/io-done path is a busy-spin ring instead of a futex-parking channel.

**Architecture:** One `build_multi_producer` disruptor ring carrying `Notification<C>` slots. Producers (busy-spin threads): each per-peer network consumer (`AckEmitter`) + the durability consumer's `IOFlushed` callback. Consumer: `SyncCore::runtime_loop` drains it via `EventPoller` each iteration, feeding `handle_notification` + `run_engine_commands` — exactly as it drains `rx_notification` today. The **async** producers stay on `rx_notification` (tokio mpsc): the `C::spawn` vote fan-out (`VoteResponse`/`PreVoteResponse`), the `Tick` worker, and the SM worker (`StateMachine`). Full single-ring unification (incl. those) is gated on 3e.

**Tech Stack:** Rust, openraft fork (`sync-core` feature), `disruptor` 4.3 (`build_multi_producer` / `MultiProducer` — `Clone`, min ring size 64), the openraft 180-test suite + UC lincheck/partition as guards.

## Global Constraints

- All work on the openraft fork `PeterKnego/openraft` branch `sync-core`, behind the `sync-core` Cargo feature. The default (RaftCore, feature-off) build path must remain behavior-identical. **BASE: `f510f38f`** (the 3c.1 tip).
- Correctness oracle, green at every task: `cargo test -p tests --features sync-core` (**180/0**, full default parallelism — `t10_save_committed`/`t90` may flake on a CPU-oversubscribed box; re-run, they are known environmental flakes on unrelated paths) + `cargo test -p openraft --features sync-core --lib` (currently 511 + new tests) + default `cargo test -p openraft --lib` (**494/0**) + `cargo clippy -p openraft --features sync-core -- -D warnings` and `-p openraft -- -D warnings` both clean. Crate at `/home/claude/ultima/openraft/openraft/`; cwd `/home/claude/ultima/openraft`.
- **Acks/io-done carry the SAME `Notification<C>` semantics** as today — only the transport changes (tokio mpsc → disruptor ring). The engine's `handle_notification` is unchanged; ordering per producer is preserved (each producer publishes in the same order it called `tx_notify.send` before).
- **Ring publish is a blocking spin when full** — producers are busy-spin threads so that's acceptable, but size the ring generously (**4096**) so a momentarily-behind consumer doesn't stall producers. Min is 64; do not go below.
- **Move-payload slot pattern** (disruptor lends only `&event`): the ring slot is `Mutex<Option<Notification<C>>>`, identical to `DurabilityEvent` in `sync_durability.rs`. Mirror it.
- This builds directly on 3c.1's `sync_network.rs` (`AckEmitter`) and `sync_durability.rs` (the durability consumer + its `IOFlushed` callback). Read those before editing.

---

## File structure

- Create `openraft/src/core/sync_input.rs` — the input-ring module: `InputEvent<C>` slot (`Mutex<Option<Notification<C>>>`), `InputProducer<C>` type alias (the `MultiProducer` handle), `InputPoller<C>` type alias (the `EventPoller`), and `build_input_ring<C>() -> (InputPoller<C>, InputProducer<C>)`. Mirrors the ring scaffolding in `sync_durability.rs`.
- Modify `openraft/src/core/mod.rs` — `#[cfg(feature="sync-core")] mod sync_input;` + re-exports.
- Modify `openraft/src/raft/mod.rs` — build the input ring in `Raft::new`; pass the poller into `SyncCore`; pass producer clones to the durability consumer and (Task 2) the peer-consumer spawns; **delete** `io_completion_forwarder` + the `tx_io_completed`/`rx_io_completed` watch channel.
- Modify `openraft/src/core/sync_core.rs` — `SyncCore` holds `input_poller: InputPoller<C>`; `runtime_loop` drains it (a `process_input_ring` method) alongside `process_notification`.
- Modify `openraft/src/core/sync_durability.rs` — the durability consumer + `IOFlushed` callback publish `Notification::LocalIO`/`StorageError` to an `InputProducer` clone instead of the watch.
- Modify `openraft/src/core/sync_network.rs` (Task 2) — `AckEmitter` holds an `InputProducer` instead of `tx_notify`; the 10 emit sites publish to the ring.
- Test: unit tests in `sync_input.rs` (multi-producer → poller round-trip); the 180-suite + UC lincheck/partition as integration guards.

---

## Task 1: Input ring infra + io-done path on the ring; drop io_completion_forwarder

Introduce the MPSC input ring, wire the `EventPoller` into the consensus loop, and route the **durability io-done** (`LocalIO`/`StorageError`) through it — deleting the `io_completion_forwarder` tokio task + its watch channel. Network acks still flow via `tx_notification` (unchanged) until Task 2. This is independently testable: a commit's `LocalIO` now reaches the engine via the ring, exercised by the full 180-suite (every commit waits on `LocalIO`).

**Files:**
- Create: `openraft/src/core/sync_input.rs`
- Modify: `openraft/src/core/mod.rs`, `openraft/src/raft/mod.rs`, `openraft/src/core/sync_core.rs`, `openraft/src/core/sync_durability.rs`
- Test: `sync_input.rs` `#[cfg(test)] mod tests`

**Interfaces:**
- Produces:
  - `pub(crate) struct InputEvent<C: RaftTypeConfig> { pub(crate) notify: std::sync::Mutex<Option<Notification<C>>> }`
  - `pub(crate) type InputProducer<C> = disruptor::MultiProducer<InputEvent<C>, disruptor::MultiProducerBarrier>`
  - `pub(crate) type InputPoller<C> = disruptor::EventPoller<InputEvent<C>, disruptor::MultiProducerBarrier>`
  - `pub(crate) fn build_input_ring<C: RaftTypeConfig>() -> (InputPoller<C>, InputProducer<C>)` — `build_multi_producer(4096, || InputEvent { notify: Mutex::new(None) }, BusySpin).new_event_poller()` then `builder.build()`.
  - `pub(crate) fn publish_notification<C>(producer: &mut InputProducer<C>, n: Notification<C>)` — `producer.publish(|slot| { *slot.notify.lock().unwrap() = Some(n); })`. (Helper so call sites don't repeat the closure; note `publish` takes the value by move into the closure.)
- Consumes (Task 1): durability consumer takes an `InputProducer<C>`; `SyncCore` takes an `InputPoller<C>`.

- [ ] **Step 1: Write the failing multi-producer round-trip test**

In `sync_input.rs` `#[cfg(test)] mod tests`, mirror `sync_durability`'s ring test but with TWO producers: `build_input_ring`, clone the producer, from producer A publish (e.g.) `Notification::Tick { i: 1 }` and `Tick { i: 2 }`, from clone B publish `Tick { i: 3 }`; drive the `EventPoller` on this thread and collect the drained `i`s; assert all three arrive (set-equality `{1,2,3}` — MPSC does not guarantee cross-producer order, only per-producer order). Use a `RaftTypeConfig` test impl already used by other lib tests (e.g. the one in `sync_durability`/`sync_network` tests).

- [ ] **Step 2: Run it — verify it fails**

Run: `cargo test -p openraft --features sync-core --lib sync_input 2>&1 | tail -15`
Expected: FAIL — unresolved module `sync_input`.

- [ ] **Step 3: Implement `sync_input.rs` + register the module**

Write `sync_input.rs` with the interfaces above (mirror `sync_durability.rs`'s `build_single_producer` setup but `build_multi_producer`; `use disruptor::{build_multi_producer, BusySpin, MultiProducer, MultiProducerBarrier, EventPoller, Polling};`). Add `#[cfg(feature = "sync-core")] pub(crate) mod sync_input;` + the re-exports to `core/mod.rs` (mirror `sync_durability`). Everything `pub(crate)`.

- [ ] **Step 4: Run the round-trip test — verify it passes**

Run: `cargo test -p openraft --features sync-core --lib sync_input 2>&1 | tail -15`
Expected: PASS.

- [ ] **Step 5: Build the ring in `Raft::new`, thread poller→SyncCore + producer→durability, delete the forwarder**

In `raft/mod.rs` `Raft::new` (around the existing `sync-core` spawn block + the watch channel at ~line 471 and the forwarder spawn at ~line 596):
- Call `let (input_poller, input_producer) = sync_input::build_input_ring::<C>();`.
- Pass `input_producer.clone()` to the durability-consumer spawn (the function that starts the `sync_durability` consumer) so the consumer + its `IOFlushed` callback can publish io-done.
- Pass `input_poller` into `SyncCore::new(...)` (add the param).
- **Delete** `io_completion_forwarder` (the whole `async fn` ~lines 359–408), its `C::spawn(io_completion_forwarder(...))` call (~line 596), the `tx_io_completed`/`rx_io_completed` `C::watch_channel` creation (~line 471), and the `tx_io_completed` field on `RaftCore` (and any `weak_tx_notify` downgrade used only by the forwarder). The durability consumer no longer needs the watch.

In `sync_durability.rs`: replace the `IOFlushed` callback's captured `tx_io_completed` with the `InputProducer<C>` clone — on flush, `sync_input::publish_notification(&mut producer, Notification::LocalIO { io_id })`; on the append failure path (~line 443), publish `Notification::StorageError { error }`. (The `IOFlushed` callback may fire from a storage flush thread — that's fine, `MultiProducer` is `Clone` and built for concurrent producers; give the callback its own clone.) Remove the now-unused `tx_io_completed` field from `DurabilityOp::Append` / the consumer.

In `sync_core.rs`: add `input_poller: InputPoller<C>` to `SyncCore`; in `runtime_loop` add a `process_input_ring(at_most)` method that drains the poller (`match poller.poll() { Ok(events) => for e in &mut events { if let Some(n) = e.notify.lock().unwrap().take() { self.core.handle_notification(n)?; self.run_engine_commands().await?; } }, Err(Polling::NoEvents) => {}, Err(Polling::Shutdown) => {} }`) and returns a processed count. Call it in the loop immediately before `process_notification` (line ~223), and include its processed count in the idle-yield check (`if raft_msg_processed == 0 && notify_processed == 0 && input_processed == 0 { yield_now() }`). `handle_notification`/`run_engine_commands` are the same ones `process_notification` uses — `process_input_ring` is `block_on`-wrapped at the call site exactly like `process_notification`.

- [ ] **Step 6: Build + full gates**

Run:
```bash
cargo build -p openraft --features sync-core
cargo test -p openraft --features sync-core --lib 2>&1 | grep "test result"
cargo test -p tests --features sync-core 2>&1 | grep -E "test result|FAILED"
cargo test -p openraft --lib 2>&1 | grep "test result"
cargo clippy -p openraft --features sync-core -- -D warnings 2>&1 | tail -3
cargo clippy -p openraft -- -D warnings 2>&1 | tail -3
```
Expected: lib green (511 + your new test), **integration 180/0** (every commit's `LocalIO` now flows through the ring — this is the real guard for Task 1), default 494/0, clippy clean both. If a commit hangs (engine never sees `LocalIO`), the durability→ring→loop path is broken — debug with systematic-debugging (trace the publish + the poller drain), do not weaken a test.

- [ ] **Step 7: Commit**

```bash
cd /home/claude/ultima/openraft
git add openraft/src/core/sync_input.rs openraft/src/core/mod.rs openraft/src/raft/mod.rs openraft/src/core/sync_core.rs openraft/src/core/sync_durability.rs
git commit -m "feat(sync-core): disruptor input ring + io-done on it; drop io_completion_forwarder (3c.2)

build_multi_producer input ring (Notification<C> slots); SyncCore drains it via EventPoller
each loop iteration. Durability consumer/IOFlushed callback publish LocalIO/StorageError
directly to the ring; the io_completion_forwarder task + tx_io_completed watch are deleted.
Network acks still via tx_notification (Task 2 moves them). Suite 180/0."
```

---

## Task 2: Move per-peer network acks onto the input ring

Swap `AckEmitter`'s `tx_notify` channel for an `InputProducer` and publish all 10 ack emit sites to the ring; pass a producer clone to each peer-consumer spawn. After this, the hot sync path (replication/heartbeat/snapshot acks + io-done) is fully on the disruptor ring; `rx_notification` carries only `Tick`, `StateMachine`, and the `C::spawn` vote responses.

**Files:**
- Modify: `openraft/src/core/sync_network.rs` (`AckEmitter` + the 10 emit sites), `openraft/src/core/sync_core.rs` (`spawn_peer_executor` passes a producer clone instead of `tx_notification.clone()`), `openraft/src/raft/mod.rs` (give the per-peer spawn path access to an `input_producer` clone).
- Test: a `sync_network` unit test asserting `AckEmitter` publishes to the ring; the 180-suite + UC lincheck/partition.

**Interfaces:**
- Consumes: `InputProducer<C>` + `publish_notification` from Task 1.
- Changes: `AckEmitter.tx_notify: MpscSenderOf<C, Notification<C>>` → `AckEmitter.producer: InputProducer<C>`.

- [ ] **Step 1: Write the failing AckEmitter→ring test**

In `sync_network.rs` tests, build an input ring (`sync_input::build_input_ring`), construct an `AckEmitter` with a producer clone, drive one emit (e.g. a `notify_heartbeat_progress` or a `notify_progress` success), drive the `EventPoller` on the test thread, and assert the expected `Notification` variant + fields arrive on the ring (mirror the existing ack-contract tests but assert via the poller instead of a captured mpsc). Run, verify it fails (AckEmitter still has `tx_notify`).

- [ ] **Step 2: Swap the field + the 10 emit sites**

In `sync_network.rs`: change `AckEmitter`'s `tx_notify: MpscSenderOf<C, Notification<C>>` field to `producer: InputProducer<C>`. Replace every emit (the 10 sites the map enumerates: `handle_response_stream` HigherVote; `send_progress_error`; `notify_heartbeat_progress`; `notify_progress`; `handle_heartbeat_result` HigherVote + Conflict; `send_heartbeat_progress`; `notify_snapshot_progress`; `on_snapshot_error` HigherVote + StorageError) — change `self.tx_notify.send(n).await.ok()` to `sync_input::publish_notification(&mut self.producer, n)`. These methods may now be sync (the publish is sync) — drop the `async`/`.await` where it becomes trivial, but if that ripples too far, keeping them `async` with a sync body is acceptable (don't over-refactor). Per-peer ordering is preserved (single consumer thread publishes in call order).

- [ ] **Step 3: Pass a producer clone at spawn**

In `sync_core.rs` `spawn_peer_executor` (~line 634): pass `self.core.input_producer.clone()` (you'll need to store an `InputProducer<C>` clone on `SyncCore` or `RaftCore` for spawning peers — add it in `raft/mod.rs` `Raft::new` from the same `input_producer`) instead of `self.core.tx_notification.clone()` into `PeerExecutor::new` → `AckEmitter`. The vote fan-out (`spawn_parallel_vote_requests`/`send_vote_request`) KEEPS `tx_notification.clone()` (async `C::spawn` path stays on tokio — do NOT change it).

- [ ] **Step 4: Build + full gates + UC lincheck/partition**

Run (openraft):
```bash
cargo build -p openraft --features sync-core
cargo test -p openraft --features sync-core --lib 2>&1 | grep "test result"
cargo test -p tests --features sync-core 2>&1 | grep -E "test result|FAILED"
cargo test -p openraft --lib 2>&1 | grep "test result"
cargo clippy -p openraft --features sync-core -- -D warnings 2>&1 | tail -3
cargo clippy -p openraft -- -D warnings 2>&1 | tail -3
```
Expected: lib green, **integration 180/0** (replication/membership/snapshot/election acks now flow through the ring — the real guard), default 494/0, clippy clean both.
Then UC (the ultimate guard) in `/home/claude/ultima/ultima_cluster`:
```bash
cargo test -p uc_node --features sync-core --test lin_register 2>&1 | tail -20
cargo test -p uc_node --features "sync-core fault-injection" --test lin_partition -- --test-threads=1 2>&1 | tail -20
```
Expected: lincheck Linearizable (3/3) + partition green (4/4) — confirms the ack-on-ring path preserves linearizability under churn + faults. If either fails, the ring transport dropped/reordered an ack — debug against the per-producer-order invariant, do not patch the test.

- [ ] **Step 5: Commit**

```bash
cd /home/claude/ultima/openraft
git add openraft/src/core/sync_network.rs openraft/src/core/sync_core.rs openraft/src/raft/mod.rs
git commit -m "feat(sync-core): move per-peer network acks onto the disruptor input ring (3c.2)

AckEmitter publishes ReplicationProgress/HeartbeatProgress/HigherVote/StorageError to the
input ring instead of tx_notification; vote responses (C::spawn fan-out), Tick, and SM stay
on tokio mpsc (async producers, 3e). Hot sync path now fully on the ring. Suite 180/0 + UC
lincheck 3/3 + partition 4/4."
```

---

## Self-review notes

- **Spec coverage** (3c spec §3c.2): disruptor input ring via EventPoller → Task 1 (infra + loop drain) + Task 2 (acks); io-done published directly by durability consumer + drop `io_completion_forwarder` → Task 1; network acks on the ring → Task 2; async producers (vote fan-out, Tick, SM) stay on `rx_notification` → explicitly preserved in Task 2 Step 3; `GetLinearizer`/read-barrier (risk 5) arrives via `rx_api`, untouched → no task needed. The `EventPoller` + balancer/budget integration → Task 1 Step 5.
- **Out of scope (3e, noted):** the async API/inbound-RPC producers staying on tokio mpsc (risk 1); single multiplexed network consumer; apply hop. Not in this plan.
- **No placeholders:** the ring infra, the publish helper, the loop-drain method, the durability publish, and the forwarder deletion are concrete; the 10 emit sites are enumerated from the touchpoint map with the exact mechanical swap; tests are the multi-producer round-trip + the AckEmitter→ring assertion + the 180-suite + UC lincheck/partition.
- **Type consistency:** `InputEvent<C>`/`InputProducer<C>`/`InputPoller<C>`/`build_input_ring`/`publish_notification` used consistently across both tasks; `Notification<C>` is the ring payload throughout.
- **Risk — producer-blocking-when-full:** ring sized 4096; producers are busy-spin threads; if a real stall appears under load it's a sizing/perf item for the measurement phase, not a correctness issue (publish blocks, doesn't drop).
- **Risk — ordering:** MPSC gives per-producer FIFO only; the engine tolerates cross-producer interleaving (it already did with N mpsc senders). Per-peer ack order is preserved because each peer consumer is a single thread publishing in call order. The Task 1 round-trip test asserts set-equality (not cross-producer order) to encode this.
- **After 3c.2:** measure (`commit_latency` + a denoised fleet once bench-infra ansible is hardened) — the hot path is now disruptor end-to-end on the sync side; this is the phase where the input-ring win should show.
