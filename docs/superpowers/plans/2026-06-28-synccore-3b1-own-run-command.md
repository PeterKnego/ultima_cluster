# Phase 3b.1 — SyncCore owns `run_command` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Move the per-command execution (`run_command`) from `RaftCore` into `SyncCore` for the storage/apply/pure-sync commands, delegating the task-spawning (replication/snapshot/vote/heartbeat) commands to `RaftCore` unchanged — so SyncCore owns per-command dispatch, the precondition for ring-isolating I/O in 3b.2+.

**Architecture:** SyncCore (openraft fork, feature `sync-core`) already owns the event loop + command-drain orchestration; its `run_engine_commands`/`run_progress_driven_command` call `self.core.run_command`. This plan gives SyncCore its own `run_command`: a top match-by-ref delegates the 8 task-spawning commands to `RaftCore::run_command`; the 9 owned commands (storage/apply/pure-sync) are executed inline, mirroring RaftCore's arms with `self.X` → `self.core.X`. All helper methods/fields the owned arms touch are already `pub(crate)` — no visibility bumps.

**Tech Stack:** Rust 2024, openraft fork `PeterKnego/openraft` branch `sync-core`; golden gate `cargo test -p tests --features sync-core`.

## Global Constraints

- All edits in `/home/claude/ultima/openraft`, branch `sync-core`. Only `openraft/src/core/sync_core.rs` is modified by this plan (no `raft_core.rs` changes — all needed helpers are already `pub(crate)`).
- Default build (RaftCore, feature OFF) must stay behavior-unchanged — this plan touches only SyncCore (feature-ON path).
- **Pure relocation, no behavior/algorithm change.** Each owned arm is a faithful mirror of `RaftCore::run_command`'s arm (`raft_core.rs:2229-2506`), substituting `self.X` → `self.core.X`. The 8 task-spawning arms are delegated to `self.core.run_command` unchanged.
- **Golden gate after every task:** `cargo test -p tests --features sync-core` → **180 passed / 0 failed**. Also `cargo build -p openraft` (feature off) clean, and `cargo clippy -p openraft --features sync-core` + `cargo clippy -p openraft` clean.
- Refactor against the golden suite — no new unit test; the 180-test suite is the spec.
- Imports the arms require are mechanical and compiler-guided: they mirror `raft_core.rs`'s imports (`Command`, `Notification`, `Stage`, `ClientWriteError`, `ForwardToLeader`, `StorageIOResult` [the `sto_*` ext trait], `VoteResponse`, `IOId`, `IOFlushed`, plus the log-id/entry display+index ext traits). Add them as the compiler demands, the same way Phase 3a resolved `StorageError`/`RaftRuntime`.

## Command classification (the 17 arms)

| Owned inline (9) | Delegated to RaftCore (8, Phase 3c relocates) |
|---|---|
| `UpdateIOProgress`, `ReplicateCommitted`, `Respond` (pure-sync) | `SendVote`, `SendPreVote` |
| `AppendEntries`, `SaveVote`, `PurgeLog`, `TruncateLog` (storage) | `BroadcastHeartbeat`, `Replicate` |
| `SaveCommittedAndApply`, `StateMachine` (commit/apply) | `ReplicateSnapshot`, `BroadcastTransferLeader` |
| | `CloseReplicationStreams`, `RebuildReplicationStreams` |

---

## Task 1: `SyncCore::run_command` seam (delegate everything)

Add `SyncCore::run_command` that delegates the 8 task-spawning commands explicitly to `RaftCore::run_command` and (temporarily) delegates the owned commands too. Repoint SyncCore's drains to it. Establishes the dispatch seam with zero behavior change.

**Files:** Modify `openraft/src/core/sync_core.rs`.

**Interfaces:**
- Consumes: `self.core.run_command(Command<C,SM>) -> Result<Option<Command<C,SM>>, StorageError<C>>` (the `RaftRuntime` trait method, already in scope from Phase 3a).
- Produces: `SyncCore::run_command(&mut self, cmd: Command<C,SM>) -> Result<Option<Command<C,SM>>, StorageError<C>>`.

- [ ] **Step 1: Add the `Command` import**

In `openraft/src/core/sync_core.rs`, add to the `use` block (alphabetical-by-path, with the other `crate::engine`/`crate::errors` imports):

```rust
use crate::engine::Command;
```

- [ ] **Step 2: Add `SyncCore::run_command` (delegating)**

In `impl SyncCore`, add:

```rust
    /// Per-command execution. Phase 3b.1: the task-spawning/network commands are
    /// delegated to `RaftCore::run_command` (Phase 3c relocates them); the
    /// storage/apply/pure-sync commands are moved inline in later tasks. For now
    /// everything delegates — this task just establishes SyncCore as the dispatch
    /// point.
    async fn run_command(&mut self, cmd: Command<C, SM>) -> Result<Option<Command<C, SM>>, StorageError<C>> {
        // Task-spawning / network commands stay RaftCore's for now (Phase 3c).
        // Use `matches!` (a transient borrow) rather than `match &cmd { .. => return
        // self.core.run_command(cmd) }` — the latter moves `cmd` while the `&cmd`
        // scrutinee borrow is still live, which does not compile.
        let delegate = matches!(
            &cmd,
            Command::SendVote { .. }
                | Command::SendPreVote { .. }
                | Command::BroadcastHeartbeat { .. }
                | Command::Replicate { .. }
                | Command::ReplicateSnapshot { .. }
                | Command::BroadcastTransferLeader { .. }
                | Command::CloseReplicationStreams
                | Command::RebuildReplicationStreams { .. }
        );
        if delegate {
            return self.core.run_command(cmd).await;
        }
        // Owned commands (storage / apply / pure-sync) — delegated for now, moved
        // inline in Tasks 2-3.
        self.core.run_command(cmd).await
    }
```

- [ ] **Step 3: Repoint SyncCore's drains to `self.run_command`**

In `SyncCore::run_engine_commands` and `SyncCore::run_progress_driven_command`, change the two `self.core.run_command(cmd).await` calls to `self.run_command(cmd).await`.

- [ ] **Step 4: Build both feature states**

```bash
cd /home/claude/ultima/openraft
cargo build -p openraft --features sync-core
cargo build -p openraft
```
Expected: both `Finished`, no warnings.

- [ ] **Step 5: Golden gate + clippy**

```bash
cargo test -p tests --features sync-core
cargo clippy -p openraft --features sync-core && cargo clippy -p openraft
```
Expected: **180 passed; 0 failed**; clippy clean both ways.

- [ ] **Step 6: Commit**

```bash
git add openraft/src/core/sync_core.rs
git commit -m "feat(sync-core): SyncCore owns run_command dispatch (delegating seam)

run_engine_commands/run_progress_driven_command now call SyncCore::run_command,
which delegates all arms to RaftCore::run_command for now and explicitly routes
the 8 task-spawning commands (Phase 3c relocates them). No behavior change;
suite 180/0."
```

---

## Task 2: move the pure-sync + storage arms inline

Replace the temporary owned-delegation in `SyncCore::run_command` with the condition gate + stats + a `match` that executes the 3 pure-sync and 4 storage arms inline (mirrored from `raft_core.rs`), delegating the remaining owned arms (commit/apply, Task 3) via a `_` arm.

**Files:** Modify `openraft/src/core/sync_core.rs`.

**Interfaces:**
- Consumes (all already `pub(crate)` on `self.core`): fields `engine`, `runtime_stats`, `io_accepted_tx`, `tx_notification`, `tx_io_completed`, `committed_tx`, `log_store`, `client_responders`; methods `current_leader()`, `get_leader_node(..)`. Plus `cmd.condition()`, `cmd.name()`, `Condition::is_met(&io_state)`.

- [ ] **Step 1: Replace the owned-delegation with the condition gate + inline match**

In `SyncCore::run_command`, replace the trailing `self.core.run_command(cmd).await` (the "Owned commands … delegated for now" line) with the following. This mirrors `RaftCore::run_command` (`raft_core.rs:2232-2247` for the gate/stats, then the named arms) with `self.X` → `self.core.X`:

```rust
        // Owned commands: condition gate + stats, then execute inline.
        let condition = cmd.condition();
        if let Some(condition) = condition {
            if !condition.is_met(&self.core.engine.state.io_state) {
                tracing::debug!("{} not yet met, postpone cmd: {}", condition, cmd);
                return Ok(Some(cmd));
            }
        }
        self.core.runtime_stats.record_command(cmd.name());

        match cmd {
            Command::UpdateIOProgress { io_id, .. } => {
                self.core.io_accepted_tx.send_if_greater(io_id.clone());
                self.core.engine.state.log_progress_mut().submit(io_id.clone());
                let notify = Notification::LocalIO { io_id: io_id.clone() };
                self.core.tx_notification.send(notify).await.ok();
            }
            Command::ReplicateCommitted { committed } => {
                self.core.committed_tx.send_if_greater(committed);
            }
            Command::Respond { resp: send, .. } => {
                send.send();
            }
            Command::AppendEntries { committed_vote: vote, entries } => {
                let last_log_id = entries.last().unwrap().log_id();
                let last_log_index = last_log_id.index();
                let entry_count = entries.len() as u64;
                self.core.runtime_stats.append_batch.record(entry_count);
                if let Some(r) = &self.core.metrics_recorder {
                    r.record_append_batch(entry_count);
                }
                let io_id = IOId::new_log_io(vote, Some(last_log_id));
                let callback = IOFlushed::new(io_id.clone(), self.core.tx_io_completed.clone());
                self.core.io_accepted_tx.send_if_greater(io_id.clone());
                self.core.engine.state.log_progress_mut().submit(io_id.clone());
                self.core.runtime_stats.record_log_stage_now(Stage::Submitted, last_log_index + 1);
                self.core.log_store.append(entries, callback).await.sto_write_logs()?;
            }
            Command::SaveVote { vote } => {
                let io_id = IOId::new(&vote);
                self.core.io_accepted_tx.send_if_greater(io_id.clone());
                self.core.engine.state.log_progress_mut().submit(io_id.clone());
                self.core.log_store.save_vote(&vote).await.sto_write_vote()?;
                self.core.tx_notification
                    .send(Notification::LocalIO { io_id: IOId::new(&vote) })
                    .await
                    .ok();
                if let VoteStatus::Pending(non_committed) = vote.clone().into_vote_status() {
                    self.core.tx_notification
                        .send(Notification::VoteResponse {
                            target: self.core.id.clone(),
                            resp: VoteResponse::new(vote, None, true),
                            candidate_vote: non_committed,
                        })
                        .await
                        .ok();
                }
            }
            Command::PurgeLog { upto } => {
                self.core.log_store.purge(upto.clone()).await.sto_write_logs()?;
                let leader_id = self.core.current_leader();
                let leader_node = self.core.get_leader_node(leader_id.clone());
                for (log_index, tx) in self.core.client_responders.drain_upto(upto.index()) {
                    tx.on_complete(Err(ClientWriteError::ForwardToLeader(ForwardToLeader {
                        leader_id: leader_id.clone(),
                        leader_node: leader_node.clone(),
                    })));
                    tracing::debug!("sent ForwardToLeader for purged log_index: {}", log_index);
                }
                self.core.engine.state.io_state_mut().update_purged(Some(upto));
            }
            Command::TruncateLog { after } => {
                self.core.log_store.truncate_after(after.clone()).await.sto_write_logs()?;
                let leader_id = self.core.current_leader();
                let leader_node = self.core.get_leader_node(leader_id.clone());
                for (log_index, tx) in self.core.client_responders.drain_from(after.next_index()) {
                    tx.on_complete(Err(ClientWriteError::ForwardToLeader(ForwardToLeader {
                        leader_id: leader_id.clone(),
                        leader_node: leader_node.clone(),
                    })));
                    tracing::debug!("sent ForwardToLeader for log_index: {}", log_index);
                }
            }
            // Commit/apply arms move inline in Task 3; task-spawning already delegated above.
            _ => return self.core.run_command(cmd).await,
        }
        Ok(None)
```

(Note: the `let delegate = matches!(&cmd, …); if delegate { return … }` block from Task 1 stays above this, unchanged. This replacement swaps only the trailing `self.core.run_command(cmd).await` for the gate + match below.)

- [ ] **Step 2: Add the imports the arms need**

Add the imports the compiler flags — they mirror `raft_core.rs`'s: `Notification` (`crate::core::notification::Notification`), `Stage` (`crate::core::stage::Stage`), `ClientWriteError`, `ForwardToLeader`, `StorageIOResult` (`crate::errors::*`), `VoteResponse` (`crate::raft::VoteResponse`), `IOId` (`crate::raft_state::io_state::io_id::IOId`), `IOFlushed` (`crate::storage::IOFlushed`), and the vote/log-id/entry ext traits used by `.into_vote_status()`/`.log_id()`/`.index()`/`.next_index()` (grep `raft_core.rs` `use` lines for the exact paths). Keep imports alphabetical-by-path.

- [ ] **Step 3: Build both feature states** — `cargo build -p openraft --features sync-core` and `cargo build -p openraft`, both clean.

- [ ] **Step 4: Golden gate + clippy** — `cargo test -p tests --features sync-core` → **180/0**; `cargo clippy -p openraft --features sync-core` + `cargo clippy -p openraft` clean.

- [ ] **Step 5: Commit**

```bash
git add openraft/src/core/sync_core.rs
git commit -m "feat(sync-core): SyncCore executes pure-sync + storage commands inline

UpdateIOProgress/ReplicateCommitted/Respond + AppendEntries/SaveVote/PurgeLog/
TruncateLog now run in SyncCore (mirrored from RaftCore, self->self.core);
commit/apply + task-spawning still delegated. Suite 180/0."
```

---

## Task 3: move the commit/apply arms inline + close the match

Add the final two owned arms (`SaveCommittedAndApply`, `StateMachine`) inline and replace the `_ => return self.core.run_command(cmd).await` with `unreachable!()` (all owned arms now explicit; task-spawning already returned by the top by-ref block).

**Files:** Modify `openraft/src/core/sync_core.rs`.

**Interfaces:** Consumes (already `pub(crate)`): `self.core.engine.state` (`apply_progress_mut`, `get_log_id`), `self.core.log_store.save_committed`, `self.core.apply_to_state_machine(first, upto)`, `self.core.sm_handle.send(..)`, `self.core.runtime_stats.record_log_stage_now`.

- [ ] **Step 1: Add the two arms and close the match**

In `SyncCore::run_command`'s inline `match cmd`, replace the `_ => return self.core.run_command(cmd).await,` line with the two arms (mirrored from `raft_core.rs:2376-2499`) followed by an `unreachable!()` catch-all:

```rust
            Command::SaveCommittedAndApply { already_applied: already_committed, upto } => {
                self.core.runtime_stats.record_log_stage_now(Stage::Committed, upto.index() + 1);
                self.core.engine.state.apply_progress_mut().submit(upto.clone());
                self.core.log_store.save_committed(Some(upto.clone())).await.sto_write()?;
                let first = self.core.engine.state.get_log_id(already_committed.next_index()).unwrap();
                self.core.apply_to_state_machine(first, upto).await?;
            }
            Command::StateMachine { command } => {
                let io_id = command.get_log_progress();
                if let Some(io_id) = io_id {
                    self.core.engine.state.log_progress_mut().submit(io_id);
                }
                if let Some(log_id) = command.get_apply_progress() {
                    self.core.engine.state.apply_progress_mut().submit(log_id);
                }
                if let Some(log_id) = command.get_snapshot_progress() {
                    self.core.engine.state.snapshot_progress_mut().submit(log_id);
                }
                self.core.sm_handle
                    .send(command)
                    .await
                    .map_err(|_e| StorageError::write_state_machine(C::err_from_string("cannot send to sm::Worker")))?;
            }
            // All owned commands are explicit above; task-spawning commands returned
            // via the by-ref delegate block at the top of run_command.
            _ => unreachable!("task-spawning commands are delegated before this match"),
```

- [ ] **Step 2: Add any remaining imports** the two arms need (`StorageError::write_state_machine`, `C::err_from_string` — both already reachable; the `sm::Command` accessor methods are inherent). Compiler-guided.

- [ ] **Step 3: Build both feature states** — both clean.

- [ ] **Step 4: Golden gate + clippy** — `cargo test -p tests --features sync-core` → **180/0**; clippy clean both ways. (This is the milestone gate: SyncCore now owns all storage/apply/pure-sync command execution; only the 8 task-spawning commands and the two engine-driving handlers remain delegated.)

- [ ] **Step 5: Update the module doc + commit**

Refresh the `sync_core.rs` module doc "Status" block to note SyncCore now owns command *execution* for the storage/apply/pure-sync commands (delegating only the task-spawning commands + the engine-driving handlers). Then:

```bash
git add openraft/src/core/sync_core.rs
git commit -m "feat(sync-core): SyncCore owns commit/apply command execution

SaveCommittedAndApply + StateMachine now run in SyncCore; the owned-command
match is closed (unreachable catch-all). Only the 8 task-spawning commands
(Phase 3c) and handle_api_msg/handle_notification remain delegated. Suite 180/0."
```

---

## Done-when

SyncCore executes all 9 storage/apply/pure-sync commands inline; the 8 task-spawning commands delegate to `RaftCore::run_command`; the full openraft suite is green through `--features sync-core` (180/0); default build unchanged. This is the dispatch boundary Phase 3b.2 builds on (ring-isolating the durability + apply execution).
