# Phase 3b.2 — durability consumer (log I/O off the consensus thread) — Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`).

**Goal:** Move `log_store` write I/O off the (currently tokio-driven) consensus loop onto a dedicated, **reactor-free** durability consumer thread fed by a **disruptor** ring — keeping openraft's 180-test suite green at every step.

**Architecture:** A **log-store-owner consumer** owns `log_store` (it must — `RaftLogStorage` is `&mut self`, single-owner) and runs a busy-spin loop that services **two inputs** each iteration: (1) the **disruptor write ring** — the 5 storage write Commands, driven to completion with the reactor-free `block_on` (proven in the 3b.2 spike); and (2) a **reader-request channel** — because the still-delegated replication path (`RaftCore::spawn_replication_stream` → `log_store.get_log_reader()`) needs read handles, and `log_store` now lives on the consumer. So the consumer is the *sole owner* of the log store, serving both writes and reader-vending; replication stays delegated to RaftCore (Phase 3c dissolves the reader-request seam when it moves replication into SyncCore). Per-op completion + after-work follows the IO-completion model mapped below.

> **Why two inputs (revised after Task 1's first attempt):** the original plan assumed RaftCore never touches `log_store` under the feature. It does — `get_log_reader` is reachable via the delegated `RebuildReplicationStreams` command. Rather than share `log_store` behind a lock (which would defeat the lock-free hot path), the consumer owns it outright and vends readers on request. The reader-request is rare (per replication-stream rebuild, on membership/leadership change), so a simple channel beside the hot write ring is the right asymmetry.

**Tech Stack:** Rust 2024, openraft fork `PeterKnego/openraft` branch `sync-core`, `disruptor` 4.3 (already added, behind `sync-core`); gate `cargo test -p tests --features sync-core`.

## Global Constraints

- All edits in `/home/claude/ultima/openraft`, branch `sync-core`.
- Default build (feature OFF) behavior-unchanged.
- **Gate after every task:** `cargo test -p tests --features sync-core` → **180/0**; both builds clean; clippy clean both feature states.
- The `block_on` reactor-free driver from `openraft/src/core/sync_durability_spike.rs` graduates into the durability consumer (the spike module is deleted in the final task).
- Preserve semantics exactly. The IO-completion path (`IOFlushed` → `tx_io_completed` watch → `io_completion_forwarder` → `tx_notification` → `handle_notification`) is openraft's and must keep working unchanged.

## The IO-completion model (design basis — from the source map)

Per-command after-work classification (`raft_core.rs` line refs):

| Command | storage call | after-work on the consensus loop | movable wholesale? |
|---|---|---|---|
| `AppendEntries` (2260) | `append(entries, IOFlushed)` | **none** — completion flows via the `IOFlushed` callback → forwarder → `Notification::LocalIO` → `handle_notification` | ✅ pure fire-and-forget |
| `SaveVote` (2299) | `save_vote(&vote)` | 2× `tx_notification.send(LocalIO / VoteResponse)` — **channel sends only** (cross-thread-safe) | ✅ (consumer can send them) |
| `PurgeLog` (2329) | `purge(upto)` | `client_responders.drain_upto` + `ForwardToLeader`; `engine.state.io_state_mut().update_purged` — **consensus-state mutation** | ❌ after-work stays on consensus loop |
| `TruncateLog` (2348) | `truncate_after(after)` | `client_responders.drain_from` + `ForwardToLeader` — **consensus-state mutation** | ❌ after-work stays on consensus loop |
| `SaveCommittedAndApply` (2376) | `save_committed(upto)` | `apply_progress_mut().submit`; `apply_to_state_machine` (responder drain + forward to `sm::Worker`) — **consensus-state mutation** | ❌ after-work stays on consensus loop |

**Implication:** the *storage call* of every op moves to the consumer (single owner), but the **consensus-state after-work of purge/truncate/save_committed must run on the consensus loop after the storage call completes**. So the consensus loop, for those ops, publishes the op + **awaits a completion signal** + runs the after-work. `AppendEntries` and `SaveVote` need no consensus after-work. This is the staging below.

---

## Task 1 (guided build): durability consumer with the uniform await-completion migration

> **Honesty note on this task's granularity.** Unlike 3b.1 (mechanical relocation of existing code), this task *builds new infrastructure* (the ring event type, the consumer thread, the `log_store` extraction, the completion-await wiring) whose exact code emerges as it's built and is **validated by the 180-test suite**, not transcribed from an existing source. It is therefore specified as a **guided build with a hard gate** — the design below is precise, but the implementer writes-and-validates rather than copying complete code. If a sub-piece turns out to need a design decision the spec didn't anticipate (esp. the `log_store` extraction mechanism), STOP and report it rather than guessing. Tasks 2+ become bite-sizable once this lands.

**Deliverable:** `log_store` lives on a single reactor-free consumer thread that serves **writes** (the 5 storage Commands, via the disruptor ring) **and vends readers** (`get_log_reader`, via a request channel, for the still-delegated replication path); semantics preserved (180/0). First cut for writes is **uniform**: every write op = consensus publishes `(op, completion oneshot)` → consumer `block_on(log_store.<op>)` → signals the oneshot → consensus awaits → runs the existing after-work unchanged (`Append` is the exception — fire-and-forget via its `IOFlushed` callback). Pipelining optimizations are Tasks 2–3.

**Files:**
- Create: `openraft/src/core/sync_durability.rs` — the durability consumer (ring event type, the consumer loop, the spawned-thread handle owning `log_store`, the graduated `block_on`).
- Modify: `openraft/src/core/sync_core.rs` — the 5 storage arms in `run_command` publish to the ring + await completion; SyncCore holds the durability handle; construction spawns the consumer with `log_store`.
- Modify: `openraft/src/core/raft_core.rs` — the `log_store` extraction (`Option<LS>` + take), the cfg-gated `unreachable!()` on the 5 dead write arms, the cfg-gated reader-requester field, and the cfg-branch in `spawn_replication_stream` to request a reader instead of `self.log_store.get_log_reader()`.
- Modify: `openraft/src/raft/mod.rs` — cfg-init the reader-requester and pass it into the `RaftCore` construction; wire the consumer's reader-request `Receiver` into SyncCore at construction.
- Delete (final task): `openraft/src/core/sync_durability_spike.rs`.

**Key design pieces (precise, but build-and-validate):**

1. **`log_store` extraction + the `get_log_reader` co-tenant (the resolved design).** The consumer owns `LS`; `SyncCore::new` extracts it from the wrapped `RaftCore` and spawns the consumer with it. Mechanism: change `RaftCore.log_store` to `Option<LS>` and `.take()` it in `SyncCore::new`. Feature-OFF behavior must stay identical (the field is always `Some` when the feature is off). Two viable ways — pick whichever stays cleanest; the controller has accepted both as in-scope:
   - **(preferred) cfg-gate the field type:** `#[cfg(feature="sync-core")] log_store: Option<LS>` / `#[cfg(not(feature="sync-core"))] log_store: LS`, so feature-OFF code is untouched; or
   - one `Option<LS>` field with feature-OFF call sites using `.as_mut().unwrap()` (behavior-identical, ~6 sites).

   Under the feature, `RaftCore` must then never read `self.log_store`:
   - The **5 write arms** in `RaftCore::run_command` (`raft_core.rs:2297/2306/2330/2349/2384`) are already dead under the feature (SyncCore owns them). cfg-gate them to `unreachable!("storage owned by SyncCore log-store consumer")` (or behind `#[cfg(not(feature="sync-core"))]`).
   - **`get_log_reader`** in `spawn_replication_stream` (`raft_core.rs:1058`) is **live** (reachable via the delegated `RebuildReplicationStreams`). Reroute it: give `RaftCore` a cfg-gated, cloneable **reader-requester** field (e.g. `std::sync::mpsc::Sender<OneshotSenderOf<C, LogReaderOf<C>>>`), set at construction (`raft/mod.rs`), and cfg-branch `spawn_replication_stream` to request a reader from the consumer instead of `self.log_store.get_log_reader()`. The consumer, on a reader request, `block_on(self.log_store.get_log_reader())` and returns it via the oneshot.

   All of the above is `#[cfg(feature="sync-core")]`-gated; the feature-OFF path is behavior-unchanged. (`StorageHelper`'s `&mut log_store` use is startup/tests only — before the consumer is spawned — and is not a runtime co-tenant; leave it on the feature-OFF path / the not-yet-extracted store at startup.)

2. **Ring event type** (in `sync_durability.rs`): a `DurabilityEvent { op: Mutex<Option<DurabilityOp>> }` (the `Mutex<Option<_>>` slot pattern the spike proved — disruptor lends only `&event` and requires `Send+Sync`). `DurabilityOp` is an enum over the 5 ops carrying their payloads + a completion signal:
   - `Append { entries, io_id, callback: IOFlushed<C> }` — no oneshot; completion is the `IOFlushed` callback (fired by `log_store.append`).
   - `SaveVote { vote, done }`, `Purge { upto, done }`, `Truncate { after, done }`, `SaveCommitted { upto, done }` — `done: OneshotSenderOf<C, Result<(), StorageError<C>>>` (use `C::oneshot()`).

3. **Consumer loop** (graduated from the spike): own `log_store`, the `EventPoller`, AND the reader-request `Receiver`. Each iteration: (a) **service reader requests** — `try_recv()` the reader-request channel; on a request `block_on(self.log_store.get_log_reader())` and send the reader back on its oneshot; (b) **poll the write ring** — per event `take()` the op under the slot lock, `match op` and `block_on(self.log_store.<call>)`; for the 4 oneshot ops send the result on `done`; for `Append` the `IOFlushed` callback handles completion (nothing extra). On `Polling::Shutdown` (write ring dropped) **and** the reader-request channel disconnected, exit. (The reader-request servicing is rare; keep it a cheap `try_recv` ahead of the busy-spin poll.)

4. **Producer side** (`sync_core.rs` storage arms): each arm keeps its existing **before-work** (the `io_accepted_tx.send_if_greater`, `log_progress_mut().submit`, `record_log_stage_now` — these stay on the consensus loop, ahead of publish), then `publish` the `DurabilityOp` to the ring; for the oneshot ops, `await` the receiver, then run the existing **after-work** unchanged (responder drains, `update_purged`, notifications, `apply_to_state_machine`). For `Append`, publish and return (fire-and-forget — its completion already flows via the callback/notification path).

- [ ] **Step 1:** Build `sync_durability.rs` — `DurabilityOp` enum, `DurabilityEvent` slot, the **reader-request** channel type, the consumer loop (services reader-requests + the write ring; graduate `block_on` here; remove it from the spike), and a `LogStoreHandle` (the write-ring producer + the reader-requester `Sender` + a shutdown/join). Unit-test the consumer in isolation first (mirror the spike's test shape — a fake write op AND a reader-request round-trip with a stub store) so the wiring is green before integration.
- [ ] **Step 2:** `log_store` extraction (design piece 1): `Option<LS>` (cfg-gated field preferred) + `.take()` in `SyncCore::new`; cfg the 5 dead write arms in `RaftCore::run_command` to `unreachable!()`. Build both feature states. If the extraction ripples beyond contained cfg edits in an unexpected way, STOP and report.
- [ ] **Step 3:** The `get_log_reader` reroute: add the cfg-gated reader-requester field to `RaftCore`, init it in `raft/mod.rs`, and cfg-branch `spawn_replication_stream` to request a reader from the consumer (await the oneshot) instead of `self.log_store.get_log_reader()`. Build both feature states.
- [ ] **Step 4:** Wire SyncCore construction — spawn the consumer with the extracted `log_store` + the reader-request `Receiver`; hold the `LogStoreHandle`; shut it down in SyncCore's shutdown path. (The requester `Sender` goes to `RaftCore` from Step 3; ensure the two halves are connected.)
- [ ] **Step 5:** Convert the 5 storage write arms in `run_command` to publish-to-ring (before-work stays on the consensus loop; `Append` fire-and-forget; the other 4 publish + `await` a local completion `rx` + existing after-work — per the report, no `self.core` borrow wall here).
- [ ] **Step 6:** Gate — `cargo build -p openraft --features sync-core` + `cargo build -p openraft` clean; `cargo clippy` both ways clean; **`cargo test -p tests --features sync-core` → 180/0**. Multi-node tests exercise BOTH the write ops AND the reader-vending (every replication-stream rebuild requests a reader); a wrong completion-ordering, a lost after-work, or a broken reader-vend shows up here.
- [ ] **Step 7:** Commit (`feat(sync-core): log-store consumer — writes + reader-vending on a reactor-free thread`).

---

## Task 2: `AppendEntries` pipelined (remove any residual await on the hot path)

If Task 1 left `Append` already fire-and-forget, confirm and harden it (no consensus-loop await on the append path; the `IOFlushed` callback fired from the consumer thread drives `tx_io_completed`). Verify the forwarder/notification path works with the callback fired off the consumer thread (tokio watch/mpsc are reactor-free and cross-thread-safe). Gate 180/0. *(Detailed once Task 1 reveals the exact append wiring.)*

## Task 3: `SaveVote` fully consumer-side

Move `SaveVote`'s two `tx_notification.send(LocalIO/VoteResponse)` onto the consumer (it has `tx_notification` + `self.id`), dropping the consensus-loop await for save-vote. `Purge`/`Truncate`/`SaveCommitted` keep the await (their after-work is consensus-state — correct to stay). Gate 180/0. *(Detailed after Task 1.)*

## Task 4: delete the spike module

Remove `openraft/src/core/sync_durability_spike.rs` + its `mod` line (its `block_on` now lives in `sync_durability.rs`). Gate 180/0; clippy clean both ways. Commit.

---

## Risks

- **`log_store` extraction + `get_log_reader` reroute** (Task 1 Steps 2–3) is the resolved-but-delicate part. The consumer owns the store; the still-delegated replication path requests readers from it. STOP-and-report if the extraction ripples beyond contained cfg edits.
- **Reader-vending latency/ordering** — `spawn_replication_stream` now awaits a reader round-trip through the consumer instead of a direct call. This is on the (rare) replication-rebuild path, not the hot write path; but a deadlock risk exists if the consumer is busy `block_on`-ing a slow write when a reader request arrives — the loop services reader-requests with a non-blocking `try_recv` each iteration *between* write-op processing, so a request is picked up promptly. The multi-node suite (constant stream rebuilds) is the guard.
- **Completion ordering** — purge/truncate/save_committed after-work must run *after* the consumer's storage call completes; the await enforces this. The suite's purge/snapshot/membership tests guard it.
- **Cross-thread `IOFlushed`** — the callback now fires on the consumer thread, writing `tx_io_completed` (a tokio watch, reactor-free, cross-thread-safe). Task 2 verifies.
- **Perf** — Task 1's uniform await re-serializes the rare ops (correctness-first); the consensus loop is still tokio-driven, so the win is partial until the loop goes synchronous (Phase 3d). 3b.2's value is structural: log I/O is reactor-free and off the consensus thread.

## Done-when

All 5 storage ops execute on the reactor-free durability consumer; suite 180/0; spike module gone; default build unchanged. The apply path (`StateMachine` command → `sm::Worker`) is already off-thread (openraft) and is addressed when the network consumers land (Phase 3c) or a dedicated apply-consumer step.
