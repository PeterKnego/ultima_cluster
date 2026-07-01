# SyncCore async→synchronous transformation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transform `SyncCore` (in the openraft fork) from *"owns the event loop, delegates async execution to RaftCore"* into *"drives the Engine with synchronous, ring-dispatched command execution and isolated I/O consumers"* — keeping openraft's proven algorithm and validated at every step by openraft's own 180-test suite.

**Architecture:** openraft's `Engine` is a pure-synchronous state machine (feed events → drain `Command`s → execute). `RaftCore` is the async harness that executes those commands via async storage/network/sm traits. `SyncCore` already owns the event loop; this plan progressively moves *command execution* out of the delegated async path into a synchronous core that dispatches I/O commands to dedicated ring-fed consumer threads. The `Raft` public async API stays unchanged — which is what keeps openraft's test suite reusable as the executable spec.

**Tech Stack:** Rust (edition 2024), openraft fork `PeterKnego/openraft` branch `sync-core`, the `tests` crate as the golden suite (`cargo test -p tests --features sync-core`).

## Global Constraints

- All work happens in the **openraft fork** at `/home/claude/ultima/openraft`, branch `sync-core` (pushed to `fork` remote). NOT in `ultima_cluster`.
- The **default build (`RaftCore`, feature off) must stay byte-for-byte behavior-unchanged.** Every edit to `RaftCore` is additive/visibility-only or `#[cfg(feature = "sync-core")]`-gated.
- **The golden gate after every task:** `cargo test -p tests --features sync-core` → **180 passed / 0 failed**. Also confirm default green: `cargo test -p tests` unaffected.
- **No new behavior, no algorithm changes** in Phase 3a — pure relocation of execution. The Engine and `Command` semantics are openraft's, untouched.
- Clippy clean both ways: `cargo clippy -p openraft` and `cargo clippy -p openraft --features sync-core`.

---

## Phased roadmap (altitude)

Phase 3a is detailed below as executable tasks. Phases 3b–3e are **gated on two decisions** (next section) and get their own detailed plan once Task 1 lands and the decisions are made — their code depends on the chosen ring transport and I/O model, so detailing them now would be guesswork.

| Phase | What | Status |
|---|---|---|
| **3a** | SyncCore owns the **command-drain orchestration** (`run_engine_commands` + `run_progress_driven_command`, then `process_raft_msg`/`process_notification`) — all command flow routes through SyncCore, still calling `RaftCore::run_command`. | **Detailed below** |
| 3b | SyncCore owns `run_command` for the **pure-sync** commands (`Respond`, `ReplicateCommitted`, `UpdateIOProgress`, …); I/O commands stay delegated. | Follow-on plan |
| 3c | **Isolate durability** — `AppendEntries`/`SaveVote`/`SaveCommitted` publish to a durability consumer (ring); completion returns via the existing `IOFlushed`→notification path. | Follow-on plan (decision-gated) |
| 3d | **Isolate network/replication** — per-peer consumers (rings) call `RaftNetworkV2`; acks via notification. | Follow-on plan (decision-gated) |
| 3e | **Synchronous input + pin + measure** — replace the async `select!` with ring polling; pin the consensus core; busy-spin; re-run `busyspin-commit-bench` + a multi-node bench vs the floor decomposition. | Follow-on plan (decision-gated) |

## Decisions to lock before Phase 3b+

1. **Ring transport:** `disruptor-rs` vs hand-rolled SPSC/MPSC. (Leaning `disruptor-rs` per prior discussion: mature, `poll()`-based manual consumer bridges to our loop, ~32 ns/hop. Caveats: no closure signalling — needs a closed-flag; `publish` blocks on full ring.)
2. **I/O consumers' async model:** tokio reactor per consumer thread (enter a `Handle`, like the busy-spin runtime's hybrid) vs `block_on` per op vs io_uring. Affects 3c/3d.

These do not affect Phase 3a — proceed with it regardless.

---

## Task 1: SyncCore owns the command-drain orchestration

Move the command-drain loop (`run_engine_commands` + `run_progress_driven_command`) out of the delegated `RaftCore` path and into `SyncCore`, calling `RaftCore::run_command` per command. After this task, the top-level loop's command execution is orchestrated by SyncCore — the foothold for owning per-command execution later. `run_command` itself (the I/O executor) stays on `RaftCore` for now.

**Files:**
- Modify: `openraft/src/core/raft_core.rs` — bump `run_command` and `run_progress_driven_command` to `pub(crate)` (visibility only).
- Modify: `openraft/src/core/sync_core.rs` — add `run_engine_commands` + `run_progress_driven_command` methods; route the loop's three drain calls to them.
- Test: `tests/` golden suite (no new test; the 180-test suite is the spec).

**Interfaces:**
- Consumes (from `RaftCore`, all on `self.core`): `run_command(&mut self, Command<C,SM>) -> Result<Option<Command<C,SM>>, StorageError<C>>`; `send_satisfied_responds(&mut self)` (already `pub(crate)`); `engine.output.{iter_commands, sched_commands, pop_command, postpone_command, len}`; `engine.next_progress_driven_command()`; fields `engine`, `id`, `config`.
- Produces (on `SyncCore`): `async fn run_engine_commands(&mut self) -> Result<(), StorageError<C>>` and `async fn run_progress_driven_command(&mut self) -> Result<(), StorageError<C>>`, mirroring RaftCore's semantics.

- [ ] **Step 1: Bump `run_command` to `pub(crate)`**

In `openraft/src/core/raft_core.rs`, change the signature at the `run_command` definition (currently `async fn run_command`):

```rust
    pub(crate) async fn run_command(&mut self, cmd: Command<C, SM>) -> Result<Option<Command<C, SM>>, StorageError<C>> {
```

- [ ] **Step 2: Bump `run_progress_driven_command` to `pub(crate)`**

In the same file, at its definition (currently `async fn run_progress_driven_command`):

```rust
    pub(crate) async fn run_progress_driven_command(&mut self) -> Result<(), StorageError<C>> {
```

- [ ] **Step 3: Add the imports SyncCore needs**

In `openraft/src/core/sync_core.rs`, add to the `use` block:

```rust
use crate::engine::Command;
use crate::errors::StorageError;
```

(`Command` is referenced only in doc/type position; if unused after Step 4, drop it — keep `StorageError`.)

- [ ] **Step 4: Add `run_engine_commands` + `run_progress_driven_command` to `SyncCore`**

In `impl SyncCore`, add these methods. They mirror `RaftCore::run_engine_commands` / `run_progress_driven_command` exactly, with `self.X` → `self.core.X`:

```rust
    /// Mirrors `RaftCore::run_engine_commands`: drain and execute the Engine's
    /// emitted commands, delegating per-command execution to `RaftCore::run_command`.
    async fn run_engine_commands(&mut self) -> Result<(), StorageError<C>> {
        self.core.send_satisfied_responds();

        loop {
            self.core.engine.output.sched_commands(&self.core.config);

            let Some(cmd) = self.core.engine.output.pop_command() else {
                break;
            };

            let res = self.core.run_command(cmd).await?;

            let Some(cmd) = res else {
                continue;
            };

            // Command can't run yet; postpone it.
            if self.core.engine.output.postpone_command(cmd).is_ok() {
                continue;
            }
            break;
        }

        self.run_progress_driven_command().await?;
        Ok(())
    }

    /// Mirrors `RaftCore::run_progress_driven_command`.
    async fn run_progress_driven_command(&mut self) -> Result<(), StorageError<C>> {
        while let Some(cmd) = self.core.engine.next_progress_driven_command() {
            let res = self.core.run_command(cmd).await?;
            debug_assert!(res.is_none(), "progress driven command should always be executed");
        }
        Ok(())
    }
```

- [ ] **Step 5: Route SyncCore's loop drains to the new methods**

In `SyncCore::do_main` and `SyncCore::runtime_loop`, replace each `self.core.run_engine_commands().await?` with `self.run_engine_commands().await?`. There are three call sites: one in `do_main` (after `engine.startup()`), and two in `runtime_loop` (after the `select!`, and at the end of the loop body).

- [ ] **Step 6: Build both feature states**

Run:
```bash
cd /home/claude/ultima/openraft
cargo build -p openraft --features sync-core
cargo build -p openraft
```
Expected: both `Finished`, no errors. (If `Command` import is unused, remove it.)

- [ ] **Step 7: Run the golden suite through SyncCore**

Run:
```bash
cargo test -p tests --features sync-core
```
Expected: **180 passed; 0 failed** (across append_entries, client_api, elect, extensions, life_cycle, log_store, management, membership, metrics, replication, snapshot_building, snapshot_streaming, state_machine, public_api, custom_type_config).

- [ ] **Step 8: Confirm default (RaftCore) build still green**

Run:
```bash
cargo test -p tests --test elect --test client_api
```
Expected: green (RaftCore path unchanged). Also `cargo clippy -p openraft --features sync-core` clean.

- [ ] **Step 9: Commit**

```bash
git add openraft/src/core/raft_core.rs openraft/src/core/sync_core.rs
git commit -m "feat(sync-core): SyncCore owns the command-drain orchestration

run_engine_commands + run_progress_driven_command now live on SyncCore,
calling RaftCore::run_command per command (bumped to pub(crate)). All
top-level command flow routes through SyncCore; run_command (the I/O
executor) stays on RaftCore for now. Full suite green: 180/0."
```

---

## Phase 3a (cont.): own `process_raft_msg` + `process_notification`

> Roadmap altitude, detailed-at-execution: this is the next task after Task 1, built with the **identical mirror pattern** (read `RaftCore::process_raft_msg` / `process_notification`, reproduce on `SyncCore` with `self.X` → `self.core.X`). It is intentionally not written as fabricated steps — the executing agent mirrors the two source methods the same way Task 1 mirrors `run_engine_commands`.

Route the *handler-internal* command drains through SyncCore too, so **all** command execution flows through SyncCore's `run_engine_commands` (Task 1 only covered the loop-level drains; `process_raft_msg`/`process_notification` still call `RaftCore::run_engine_commands` internally).

**Files:**
- Modify: `openraft/src/core/raft_core.rs` — bump `handle_api_msg` is already `pub(crate)`; confirm `runtime_stats` field + `BatchRaftMsgReceiver::try_recv` are `pub(crate)` (they are). No new bumps expected beyond what Task 1 added.
- Modify: `openraft/src/core/sync_core.rs` — add `process_raft_msg` + `process_notification` mirroring RaftCore's (they call `self.core.handle_api_msg` / `self.core.handle_notification` + `self.run_engine_commands`), and call them from `runtime_loop` instead of `self.core.process_raft_msg`/`process_notification`.

**Interfaces:**
- Consumes: `self.core.handle_api_msg(RaftMsg<C>)`, `self.core.handle_notification(Notification<C>) -> Result<(),Fatal<C>>`, `self.core.rx_api.try_recv()`, `self.core.rx_notification.try_recv()`, `self.core.runtime_stats`, `self.core.engine.state.last_log_id()`.
- Produces: `SyncCore::process_raft_msg(at_most: u64) -> Result<u64, Fatal<C>>`, `SyncCore::process_notification(at_most: u64) -> Result<u64, Fatal<C>>`.

> **Detail deferred:** the exact bodies mirror `RaftCore::process_raft_msg` (raft_core.rs:1289) and `process_notification` (raft_core.rs:1338), substituting `self.X` → `self.core.X` and `self.run_engine_commands()` → SyncCore's. They are reproduced verbatim with that substitution when this task is executed (same pattern as Task 1 Step 4). Steps follow the identical shape: mirror → route the two `runtime_loop` call sites → build both → suite 180/0 → confirm default → commit.

After Task 2, the two `pub(crate)` bumps on `RaftCore::process_raft_msg`/`process_notification` (added when SyncCore first delegated to them) become unused under the feature; re-privatize them or leave with the feature-scoped `allow(dead_code)`, whichever keeps both builds warning-clean.

---

## After Phase 3a → the follow-on plan

With Phase 3a done, **all command flow is SyncCore's** and `run_command` is the single delegated async executor. That is the clean boundary from which Phase 3b+ proceeds — and the point at which the **two decisions** must be locked. The follow-on plan (`docs/superpowers/plans/<date>-synccore-ring-execution.md`) will detail, decision-informed:

- **3b:** move the pure-sync `run_command` arms into SyncCore (classify the 17 commands; `Respond`/`ReplicateCommitted`/`UpdateIOProgress` are synchronous and move first).
- **3c–3d:** isolate durability then network behind the chosen ring transport + I/O-consumer model.
- **3e:** synchronous input, pin, busy-spin, and **measure** against `docs/benchmarks/floor-decomposition-2026-06-25.md` using `busyspin-commit-bench` + a new multi-node bench.

Each step keeps the 180-test suite as the gate.
