# Phase 3b.2 — durability consumer (log I/O off the consensus thread) — Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`).

**Goal:** Move `log_store` write I/O off the (currently tokio-driven) consensus loop onto a dedicated, **reactor-free** durability consumer thread fed by a **disruptor** ring — keeping openraft's 180-test suite green at every step.

**Architecture:** The durability consumer **owns `log_store`** (it must — `RaftLogStorage` is `&mut self`, single-owner) and runs a busy-spin `EventPoller` loop, driving each storage call to completion with the reactor-free `block_on` (proven in the 3b.2 spike). All 5 storage Commands route through it. Per-op completion + after-work follows the IO-completion model mapped below.

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

**Deliverable:** `log_store` lives on a durability consumer thread; all 5 storage Commands route through a disruptor ring; semantics preserved (180/0). First cut is **uniform**: every op = consensus publishes `(op, completion oneshot)` → consumer `block_on(log_store.<op>)` → signals the oneshot → consensus awaits → runs the existing after-work unchanged. (Pipelining optimizations are Tasks 2–3.)

**Files:**
- Create: `openraft/src/core/sync_durability.rs` — the durability consumer (ring event type, the consumer loop, the spawned-thread handle owning `log_store`, the graduated `block_on`).
- Modify: `openraft/src/core/sync_core.rs` — the 5 storage arms in `run_command` publish to the ring + await completion; SyncCore holds the durability handle; construction spawns the consumer with `log_store`.
- Modify: `openraft/src/core/raft_core.rs` — **only** what's needed to extract `log_store` for the consumer (see the extraction sub-step).
- Delete (final task): `openraft/src/core/sync_durability_spike.rs`.

**Key design pieces (precise, but build-and-validate):**

1. **`log_store` extraction.** The durability consumer must own `LS`. `SyncCore` wraps `RaftCore { log_store: LS, .. }`. Resolve extraction with the least-invasive mechanism — recommended: add `#[cfg(feature = "sync-core")] pub(crate) fn into_log_store_and_rest(...)` or a `pub(crate) fn take_log_store(&mut self) -> LS` to `RaftCore` that moves `log_store` out, leaving the field in a state the feature-ON `RaftCore` never reads (it doesn't — under the feature, SyncCore owns all storage execution; `RaftCore::run_command`'s storage arms are unreachable). If `LS: Default`-free extraction needs the field to become `Option<LS>` under the feature, cfg-gate it minimally; if that ripples, STOP and report — the extraction mechanism is the one genuine design decision here and is worth getting right with the controller.

2. **Ring event type** (in `sync_durability.rs`): a `DurabilityEvent { op: Mutex<Option<DurabilityOp>> }` (the `Mutex<Option<_>>` slot pattern the spike proved — disruptor lends only `&event` and requires `Send+Sync`). `DurabilityOp` is an enum over the 5 ops carrying their payloads + a completion signal:
   - `Append { entries, io_id, callback: IOFlushed<C> }` — no oneshot; completion is the `IOFlushed` callback (fired by `log_store.append`).
   - `SaveVote { vote, done }`, `Purge { upto, done }`, `Truncate { after, done }`, `SaveCommitted { upto, done }` — `done: OneshotSenderOf<C, Result<(), StorageError<C>>>` (use `C::oneshot()`).

3. **Consumer loop** (graduated from the spike): own `log_store` + the `EventPoller`; busy-spin; per event `take()` the op under the slot lock; `match op` and `block_on(self.log_store.<call>)`; for the oneshot ops, send the result on `done`; for `Append`, the `IOFlushed` callback handles completion (nothing extra). On `Polling::Shutdown` (producer/ring dropped), exit.

4. **Producer side** (`sync_core.rs` storage arms): each arm keeps its existing **before-work** (the `io_accepted_tx.send_if_greater`, `log_progress_mut().submit`, `record_log_stage_now` — these stay on the consensus loop, ahead of publish), then `publish` the `DurabilityOp` to the ring; for the oneshot ops, `await` the receiver, then run the existing **after-work** unchanged (responder drains, `update_purged`, notifications, `apply_to_state_machine`). For `Append`, publish and return (fire-and-forget — its completion already flows via the callback/notification path).

- [ ] **Step 1:** Build `sync_durability.rs` — `DurabilityOp` enum, `DurabilityEvent` slot, the consumer loop (graduate `block_on` here; remove it from the spike), and a `DurabilityHandle` (the producer + a `join`/shutdown). Unit-test the consumer in isolation first (mirror the spike's test shape with a fake op) so the ring+driver wiring is green before integration.
- [ ] **Step 2:** Resolve `log_store` extraction (design piece 1). Build + confirm both feature states compile. If the mechanism ripples beyond a contained `cfg`, STOP and report.
- [ ] **Step 3:** Wire SyncCore construction — spawn the durability consumer with the extracted `log_store`; hold the `DurabilityHandle`; shut it down in SyncCore's shutdown path.
- [ ] **Step 4:** Convert the 5 storage arms in `run_command` to publish-to-ring (before-work stays; `Append` fire-and-forget; the other 4 publish + `await` completion + existing after-work).
- [ ] **Step 5:** Gate — `cargo build -p openraft --features sync-core` + `cargo build -p openraft` clean; `cargo clippy` both ways clean; **`cargo test -p tests --features sync-core` → 180/0**. The append/replication/membership/snapshot suites exercise every one of these ops; a wrong completion-ordering or a lost after-work shows up here.
- [ ] **Step 6:** Commit (`feat(sync-core): durability consumer — log I/O on a reactor-free thread`).

---

## Task 2: `AppendEntries` pipelined (remove any residual await on the hot path)

If Task 1 left `Append` already fire-and-forget, confirm and harden it (no consensus-loop await on the append path; the `IOFlushed` callback fired from the consumer thread drives `tx_io_completed`). Verify the forwarder/notification path works with the callback fired off the consumer thread (tokio watch/mpsc are reactor-free and cross-thread-safe). Gate 180/0. *(Detailed once Task 1 reveals the exact append wiring.)*

## Task 3: `SaveVote` fully consumer-side

Move `SaveVote`'s two `tx_notification.send(LocalIO/VoteResponse)` onto the consumer (it has `tx_notification` + `self.id`), dropping the consensus-loop await for save-vote. `Purge`/`Truncate`/`SaveCommitted` keep the await (their after-work is consensus-state — correct to stay). Gate 180/0. *(Detailed after Task 1.)*

## Task 4: delete the spike module

Remove `openraft/src/core/sync_durability_spike.rs` + its `mod` line (its `block_on` now lives in `sync_durability.rs`). Gate 180/0; clippy clean both ways. Commit.

---

## Risks

- **`log_store` extraction** (Task 1 Step 2) is the one genuine design decision — flagged to STOP-and-report if it ripples.
- **Completion ordering** — purge/truncate/save_committed after-work must run *after* the consumer's storage call completes; the await enforces this. The suite's purge/snapshot/membership tests guard it.
- **Cross-thread `IOFlushed`** — the callback now fires on the consumer thread, writing `tx_io_completed` (a tokio watch, reactor-free, cross-thread-safe). Task 2 verifies.
- **Perf** — Task 1's uniform await re-serializes the rare ops (correctness-first); the consensus loop is still tokio-driven, so the win is partial until the loop goes synchronous (Phase 3d). 3b.2's value is structural: log I/O is reactor-free and off the consensus thread.

## Done-when

All 5 storage ops execute on the reactor-free durability consumer; suite 180/0; spike module gone; default build unchanged. The apply path (`StateMachine` command → `sm::Worker`) is already off-thread (openraft) and is addressed when the network consumers land (Phase 3c) or a dedicated apply-consumer step.
