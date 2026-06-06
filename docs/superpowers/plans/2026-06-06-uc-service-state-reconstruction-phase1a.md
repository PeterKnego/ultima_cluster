# Service-State Reconstruction — Phase 1a (cold-start catch-up) Implementation Plan

> **⚠️ SUPERSEDED (2026-06-06) — DO NOT IMPLEMENT.** Execution of this plan
> (Units 1-3 built + reviewed green) surfaced that its premise is a mirage:
> **openraft already reconstructs a fresh in-memory service on cold restart** when
> no snapshot exists (it re-applies `(durable_applied, committed]`, and durable
> `last_applied` only advances at snapshot cadence). So this plan's cold-start
> log-replay path is unreachable/inert (catch-up = `Nothing`), and its integration
> test would have passed via openraft, not via the new code. The real task12 gap is
> **mid-life reattach**, now Phase 1 in the spec. The handshake channel + catch-up
> driver this plan prototyped are sound and will be re-derived in the reattach
> context. Retained as the record of how the decomposition was corrected. See the
> spec's §2 callout and §3 for the revised phasing. The feature branch was reset to
> main; nothing from this plan is merged.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** At node startup, reconstruct a fresh/recovered service's in-memory state machine by replaying committed log entries `(service_last_applied, node_frontier]` from the journal before openraft begins live applies.

**Architecture:** Channel A — the service publishes its `last_applied` into the existing `ServiceStatus.last_applied` cnc atomic before flipping `READY`. The node reads it after `wait_for_service_ready`, then (in the builder, *before* `finish()` starts openraft) replays the committed gap straight to `service/apply.ring` via the adapter. Because this runs before openraft is live, there are no concurrent applies and **no gate is needed**. If the gap is below the purge boundary, Phase 1a returns an explicit error (Phase 2 adds snapshot-install).

**Tech Stack:** Rust, openraft 0.10, `ultima_journal`, shmem SPSC rings, tokio.

**Spec:** `docs/superpowers/specs/2026-06-06-uc-service-state-reconstruction-design.md` (§2, §3 Phase 1a, §4, §7).

---

## File structure

- **Modify** `uc_service/src/runtime/handshake.rs` — add `publish_service_last_applied()` helper (mirrors `set_service_state`).
- **Modify** `uc_service/src/runtime/service.rs` — call it in `run()` before flipping `READY`, reading the SM's `last_applied()`.
- **Create** `uc_node/src/runtime/reconstruct.rs` — the catch-up driver: pure `decide_catchup_source()` + `run_initial_catchup()`.
- **Modify** `uc_node/src/runtime/mod.rs` — `mod reconstruct;`.
- **Modify** `uc_node/src/raft/state_machine_shmem.rs` — add `node_frontier()` + `replay_committed()` on `ShmemAdaptedStateMachine`.
- **Modify** `uc_node/src/runtime/builder.rs` — read `ServiceStatus.last_applied`, run catch-up before `finish()`.
- **Create** `uc_node/tests/reconstruct_cold_start.rs` — integration: in-memory SM, cold-start, state reconstructed.

---

## Task 1: Service-side `publish_service_last_applied` helper

**Files:**
- Modify: `uc_service/src/runtime/handshake.rs`
- Test: `uc_service/src/runtime/handshake.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Write the failing test**

Add to the existing `mod tests` in `uc_service/src/runtime/handshake.rs`:

```rust
    #[test]
    fn publish_last_applied_round_trip() {
        let status = ServiceStatus {
            state: AtomicU32::new(service_state::HANDSHAKING),
            _pad_1: 0,
            last_applied: AtomicU64::new(0),
            last_output_ack: AtomicU64::new(0),
            heartbeat_seq: AtomicU64::new(0),
            heartbeat_at_ns: AtomicU64::new(0),
            service_pid: AtomicU32::new(0),
            _pad_2: [0; 20],
        };
        publish_service_last_applied(&status, 1234);
        assert_eq!(status.last_applied.load(Ordering::Acquire), 1234);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p uc_service runtime::handshake::tests::publish_last_applied_round_trip`
Expected: FAIL — `cannot find function publish_service_last_applied`.

- [ ] **Step 3: Add the helper**

In `uc_service/src/runtime/handshake.rs`, after `set_service_state`:

```rust
/// Publish the service's recovered `last_applied` into `ServiceStatus`.
/// MUST be called BEFORE `set_service_state(.., READY)` — the Release on the
/// state flag is what makes this value visible (Acquire) to the node. A
/// restarted service MUST overwrite this with its own value (0 for a fresh
/// in-memory SM) so the node never reads a stale-high value from a prior
/// incarnation.
pub fn publish_service_last_applied(status: &ServiceStatus, last_applied: u64) {
    status.last_applied.store(last_applied, Ordering::Release);
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p uc_service runtime::handshake::tests::publish_last_applied_round_trip`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add uc_service/src/runtime/handshake.rs
git commit -m "feat(uc_service): publish_service_last_applied cnc helper"
```

---

## Task 2: Service publishes `last_applied` before `READY`

**Files:**
- Modify: `uc_service/src/runtime/service.rs:192-197`

- [ ] **Step 1: Wire the publish into `run()`**

In `uc_service/src/runtime/service.rs`, find the block (around line 192-197):

```rust
        // Mark ourselves ready. The node side picks this up via Acquire on
        // ServiceStatus.state. The full `ServiceReady` frame publish
        // lands when the cnc-sub-region MPSC attach API exists.
        // SAFETY: cnc mmap owned by `Service` for the loop lifetime.
        let status = unsafe { &*service_status_ptr };
        set_service_state(status, service_state::READY);
```

Replace with:

```rust
        // Publish our recovered last_applied THEN flip to Ready. The node
        // reads last_applied on observing Ready (Acquire) and replays any
        // committed gap to reconstruct our state machine (channel A). For a
        // fresh in-memory SM this is 0; a self-persisting SM reports its
        // durable value. Order matters: last_applied store (Release) must
        // precede the state store (Release) the node Acquires on.
        // SAFETY: cnc mmap owned by `Service` for the loop lifetime.
        let status = unsafe { &*service_status_ptr };
        let recovered = sm_shared.read().await.last_applied().unwrap_or(0);
        super::handshake::publish_service_last_applied(status, recovered);
        set_service_state(status, service_state::READY);
```

Verify `use super::handshake::set_service_state;` (or equivalent) is already imported; `publish_service_last_applied` is in the same `super::handshake` module so the path above resolves. If `set_service_state` is imported directly, add `publish_service_last_applied` to that `use`.

- [ ] **Step 2: Build to verify it compiles**

Run: `cargo build -p uc_service`
Expected: builds clean.

- [ ] **Step 3: Commit**

```bash
git add uc_service/src/runtime/service.rs
git commit -m "feat(uc_service): publish SM last_applied before flipping READY"
```

---

## Task 3: Catch-up source decision (pure function)

**Files:**
- Create: `uc_node/src/runtime/reconstruct.rs`
- Modify: `uc_node/src/runtime/mod.rs`
- Test: `uc_node/src/runtime/reconstruct.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Register the module**

In `uc_node/src/runtime/mod.rs`, add alongside the other `mod` lines:

```rust
pub(crate) mod reconstruct;
```

- [ ] **Step 2: Write the failing test**

Create `uc_node/src/runtime/reconstruct.rs` with the type, a stub, and tests:

```rust
//! Service-state reconstruction (Phase 1a, cold-start).
//!
//! At node startup, after the service publishes its `last_applied` (channel A),
//! the node replays committed entries `(service_last_applied, node_frontier]`
//! to the service's apply ring BEFORE openraft starts driving live applies.
//! Runs once in the builder; no gate needed (no concurrent applies yet).

/// What the node must do to bring a freshly-attached service up to the node's
/// apply frontier.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CatchupSource {
    /// Service is already current; nothing to replay.
    Nothing,
    /// Replay committed entries with index in `(from, to]` from the journal.
    LogReplay { from: u64, to: u64 },
    /// The gap is below the purge boundary; needs snapshot-install (Phase 2).
    NeedsSnapshot { service_last_applied: u64, last_purged: u64 },
}

/// Decide the catch-up source. Pure; all inputs are indices.
/// `last_purged` is the highest purged log index (0 if nothing purged).
pub(crate) fn decide_catchup_source(
    service_last_applied: u64,
    node_frontier: u64,
    last_purged: u64,
) -> CatchupSource {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_when_service_is_current() {
        assert_eq!(decide_catchup_source(10, 10, 0), CatchupSource::Nothing);
    }

    #[test]
    fn nothing_when_service_ahead_or_empty_node() {
        // Service can't really be ahead, but the decision must not panic/replay.
        assert_eq!(decide_catchup_source(12, 10, 0), CatchupSource::Nothing);
        assert_eq!(decide_catchup_source(0, 0, 0), CatchupSource::Nothing);
    }

    #[test]
    fn log_replay_when_gap_above_purge() {
        assert_eq!(
            decide_catchup_source(3, 10, 0),
            CatchupSource::LogReplay { from: 3, to: 10 }
        );
        // Boundary: service exactly at last_purged is replayable (entries
        // strictly above last_purged are still present).
        assert_eq!(
            decide_catchup_source(5, 10, 5),
            CatchupSource::LogReplay { from: 5, to: 10 }
        );
    }

    #[test]
    fn needs_snapshot_when_below_purge() {
        assert_eq!(
            decide_catchup_source(2, 10, 5),
            CatchupSource::NeedsSnapshot { service_last_applied: 2, last_purged: 5 }
        );
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p uc_node --lib runtime::reconstruct::tests`
Expected: FAIL — `unimplemented!()` panics.

- [ ] **Step 4: Implement `decide_catchup_source`**

Replace the `unimplemented!()` body:

```rust
    if service_last_applied >= node_frontier {
        return CatchupSource::Nothing;
    }
    if service_last_applied < last_purged {
        return CatchupSource::NeedsSnapshot { service_last_applied, last_purged };
    }
    CatchupSource::LogReplay { from: service_last_applied, to: node_frontier }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p uc_node --lib runtime::reconstruct::tests`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add uc_node/src/runtime/reconstruct.rs uc_node/src/runtime/mod.rs
git commit -m "feat(uc_node): catch-up source decision (reconstruct, Phase 1a)"
```

---

## Task 4: Adapter `node_frontier()` + `replay_committed()`

**Files:**
- Modify: `uc_node/src/raft/state_machine_shmem.rs`

These expose the two operations the driver needs: read the node's in-memory apply frontier, and replay one committed entry to the service (publish to `apply.ring`, await the ack), reusing the existing private `publish_apply`/`await_apply_resp` helpers. They take the same `inner` lock the live apply path uses.

- [ ] **Step 1: Add the methods**

In `uc_node/src/raft/state_machine_shmem.rs`, extend the existing
`impl<S: StateMachine> ShmemAdaptedStateMachine<S>` block (the one containing
`signal_shutdown`, around line 81-88):

```rust
    /// The node's current in-memory apply frontier (highest applied log index),
    /// or `None` if nothing has been applied. Used by Phase-1a reconstruction to
    /// compute the replay gap before openraft starts.
    pub(crate) async fn node_frontier(&self) -> Option<u64> {
        self.inner.lock().await.last_applied.map(|l| l.index)
    }

    /// Replay one already-committed entry to the service: publish to apply.ring
    /// and await the ack. Does NOT advance `last_applied` (the entry is at or
    /// below the frontier) and does NOT touch the output channel (no re-fired
    /// outputs) or the client broadcast (catch-up bypasses client_write).
    /// Reuses the same `inner` lock + ring helpers as live `apply()`.
    pub(crate) async fn replay_committed(
        &self,
        log_id: RaftLogId,
        cmd_bytes: &[u8],
    ) -> Result<(), io::Error> {
        let shutdown = self.shutdown.clone();
        let g = self.inner.lock().await;
        let log_index = log_id.index;
        publish_apply(&g.apply_producer, log_index, cmd_bytes, log_id, &shutdown).await?;
        let _resp = await_apply_resp(
            &g.apply_resp_consumer,
            log_index,
            log_id,
            &shutdown,
            &g.apply_resp_bridge,
        )
        .await?;
        Ok(())
    }
```

- [ ] **Step 2: Build to verify it compiles**

Run: `cargo build -p uc_node`
Expected: builds clean. (`publish_apply`/`await_apply_resp` are private fns in this same module; `RaftLogId`, `io` are already imported.)

- [ ] **Step 3: Commit**

```bash
git add uc_node/src/raft/state_machine_shmem.rs
git commit -m "feat(uc_node): adapter node_frontier + replay_committed for catch-up"
```

---

## Task 5: `run_initial_catchup` driver

**Files:**
- Modify: `uc_node/src/runtime/reconstruct.rs`

Pulls the committed range from the journal via the `JournalLogStorage`
`RaftLogReader` impl, and replays each `Normal` entry through the adapter.
`Blank`/`Membership` entries were never published to the service originally, so
they are skipped (matching live `apply()` behavior).

- [ ] **Step 1: Add the driver function**

Append to `uc_node/src/runtime/reconstruct.rs` (above `#[cfg(test)] mod tests`):

```rust
use openraft::RaftLogReader;
use openraft::entry::RaftEntry;
use openraft::EntryPayload;

use crate::error::ClusterError;
use crate::raft::log_storage::JournalLogStorage;
use crate::raft::state_machine_shmem::ShmemAdaptedStateMachine;
use uc_service::StateMachine;

/// Bring a freshly-attached service up to the node's apply frontier by replaying
/// committed entries from the journal. Runs once at node startup, BEFORE openraft
/// begins live applies (so no gate is needed). Returns an error if the gap is
/// below the purge boundary (Phase 2 adds snapshot-install).
pub(crate) async fn run_initial_catchup<S: StateMachine>(
    adapter: &ShmemAdaptedStateMachine<S>,
    log_storage: &mut JournalLogStorage,
    service_last_applied: u64,
) -> Result<(), ClusterError> {
    let node_frontier = adapter.node_frontier().await.unwrap_or(0);
    let last_purged = log_storage
        .last_purged
        .load()
        .map_err(|e| ClusterError::Recovery(format!("reconstruct: read last_purged: {e}")))?
        .map(|l| l.index)
        .unwrap_or(0);

    match decide_catchup_source(service_last_applied, node_frontier, last_purged) {
        CatchupSource::Nothing => {
            tracing::info!(
                service_last_applied,
                node_frontier,
                "reconstruct: service already current; no catch-up"
            );
            Ok(())
        }
        CatchupSource::NeedsSnapshot { service_last_applied, last_purged } => {
            Err(ClusterError::Recovery(format!(
                "reconstruct: service at {service_last_applied} is below the purge \
                 boundary {last_purged}; snapshot-install reconstruction is Phase 2 \
                 and not yet implemented"
            )))
        }
        CatchupSource::LogReplay { from, to } => {
            tracing::info!(from, to, "reconstruct: replaying committed gap to service");
            // try_get_log_entries takes a [start, end) range; we want (from, to].
            let entries = log_storage
                .try_get_log_entries((from + 1)..(to + 1))
                .await
                .map_err(|e| ClusterError::Recovery(format!("reconstruct: read log: {e}")))?;
            let mut replayed = 0u64;
            for entry in entries {
                let log_id = entry.log_id();
                if let EntryPayload::Normal(cmd) = entry.payload {
                    adapter
                        .replay_committed(log_id, cmd.as_ref())
                        .await
                        .map_err(|e| {
                            ClusterError::Recovery(format!(
                                "reconstruct: replay log_index {}: {e}",
                                log_id.index
                            ))
                        })?;
                    replayed += 1;
                }
            }
            tracing::info!(from, to, replayed, "reconstruct: catch-up complete");
            Ok(())
        }
    }
}
```

NOTE on `entry.log_id()` / `entry.payload`: `try_get_log_entries` returns
`Vec<Entry<TypeConfig>>`. `Entry` exposes its payload as the public `payload`
field and its id via the `RaftEntry::log_id()` method (already used elsewhere in
the crate). If the local `Entry` alias differs, mirror the access pattern used in
`state_machine_shmem.rs::apply` (`entry.log_id` / `entry.payload`,
`EntryPayload::Normal(cmd_bytes)`, `cmd_bytes.as_ref()`).

- [ ] **Step 2: Build to verify it compiles**

Run: `cargo build -p uc_node`
Expected: builds clean. If `log_storage.last_purged` is not visible from this
module, add a `pub(crate) fn last_purged_index(&self) -> Result<u64, io::Error>`
accessor on `JournalLogStorage` (it already exposes `_testonly_last_purged`) and
call that instead.

- [ ] **Step 3: Run the existing decision tests still pass**

Run: `cargo test -p uc_node --lib runtime::reconstruct::tests`
Expected: PASS (still 4).

- [ ] **Step 4: Commit**

```bash
git add uc_node/src/runtime/reconstruct.rs
git commit -m "feat(uc_node): run_initial_catchup driver (log-replay, Phase 1a)"
```

---

## Task 6: Wire catch-up into the builder

**Files:**
- Modify: `uc_node/src/runtime/builder.rs` (around 128-163)

Insert catch-up between adapter construction (line ~159) and `finish()` (line
~163), reading the service's `last_applied` from the cnc `ServiceStatus`.
`log_storage` is still owned here (it is moved into `finish` at line ~165), so we
can borrow it `&mut`.

- [ ] **Step 1: Add the catch-up call**

In `uc_node/src/runtime/builder.rs`, immediately AFTER the `let adapter = ShmemAdaptedStateMachine::new(...)?;` block (ends ~line 159) and BEFORE
`let query_link = ...` (line ~160), insert:

```rust
                // Phase 1a: reconstruct the freshly-attached service from the
                // log before openraft starts. The service published its
                // last_applied into ServiceStatus.last_applied before flipping
                // Ready; we Acquire it here (the Ready state we already observed
                // established the happens-before edge).
                // SAFETY: same cnc mmap as service_status above; alive for the
                // builder's lifetime.
                let service_last_applied = {
                    let status = unsafe { &*service_status.0 };
                    status
                        .last_applied
                        .load(std::sync::atomic::Ordering::Acquire)
                };
                crate::runtime::reconstruct::run_initial_catchup(
                    &adapter,
                    &mut log_storage,
                    service_last_applied,
                )
                .await?;
```

Confirm `log_storage` is bound as `let mut log_storage` earlier (line ~49 binds
`let log_storage = JournalLogStorage::open_with_durability(...)`). Change that
binding to `let mut log_storage = ...` so the `&mut` borrow above type-checks.

- [ ] **Step 2: Build to verify it compiles**

Run: `cargo build -p uc_node`
Expected: builds clean. (`ServiceStatus` is `Sync`; `service_status.0` is the
`SendPtr` to it. `run_initial_catchup` returns `Result<(), ClusterError>` and the
builder body already uses `?` on `ClusterError`.)

- [ ] **Step 3: Run the full node lib + existing shmem tests (regression)**

Run: `cargo test -p uc_node --test m3_shmem_single_node`
Expected: PASS — existing single-node bring-up unaffected (a self-persisting or
already-current service reports its real `last_applied`, so catch-up is `Nothing`;
a fresh node has `node_frontier == 0`, also `Nothing`).

- [ ] **Step 4: Commit**

```bash
git add uc_node/src/runtime/builder.rs
git commit -m "feat(uc_node): run cold-start catch-up before openraft starts"
```

---

## Task 7: Integration test — cold-start reconstruction

**Files:**
- Create: `uc_node/tests/reconstruct_cold_start.rs`

Proves the contract: a purely in-memory SM, after a full node+service cold
restart against a persisted journal, has its state reconstructed by log-replay.

**Pattern:** mirror the node+service bring-up and config exactly from
`uc_node/tests/m3_shmem_single_node.rs` (same `NodeConfig`/`ServiceConfig` field
set, `log_durability: ultima_journal::Durability::Eventual`, same instance/data
dirs). Reuse the data + instance directories across the two runs (do NOT use a
fresh `TempDir` for the second run) so the journal persists. Use a `CounterSm`
whose `last_applied()` returns its highest applied index and `apply` increments a
counter; it is **non-persisting** (state lost on restart) to prove
reconstruction.

- [ ] **Step 1: Write the test**

```rust
//! Phase 1a: a non-persisting in-memory SM is reconstructed from the log on a
//! full node+service cold restart.

use std::sync::atomic::{AtomicU64, Ordering};

use uc_service::StateMachine;

/// Non-persisting in-memory counter. apply(n) adds n; last_applied tracks the
/// highest committed index applied. State is entirely in-memory: a restart
/// starts from zero, so surviving a restart proves the node reconstructed it.
#[derive(Default)]
struct CounterSm {
    sum: u64,
    last_applied: Option<u64>,
}

impl StateMachine for CounterSm {
    type Command = u64;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, log_index: u64, cmd: u64) -> u64 {
        self.sum += cmd;
        self.last_applied = Some(log_index);
        self.sum
    }
    fn query(&self, _q: ()) -> u64 {
        self.sum
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
    fn build_snapshot(&self, dst: &mut dyn std::io::Write) -> Result<u64, uc_service::SnapshotError> {
        // Phase 1a does not exercise snapshots; encode sum+last_applied so the
        // trait is satisfied. (Replaced by freeze/stream in Phase 2.)
        let li = self.last_applied.unwrap_or(0);
        dst.write_all(&self.sum.to_le_bytes()).map_err(uc_service::SnapshotError::from)?;
        dst.write_all(&li.to_le_bytes()).map_err(uc_service::SnapshotError::from)?;
        Ok(li)
    }
    fn install_snapshot(&mut self, src: &mut dyn std::io::Read) -> Result<u64, uc_service::SnapshotError> {
        let mut b = [0u8; 8];
        src.read_exact(&mut b).map_err(uc_service::SnapshotError::from)?;
        self.sum = u64::from_le_bytes(b);
        src.read_exact(&mut b).map_err(uc_service::SnapshotError::from)?;
        let li = u64::from_le_bytes(b);
        self.last_applied = (li != 0).then_some(li);
        Ok(li)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn in_memory_sm_reconstructed_on_cold_restart() {
    // --- arrange: stable instance + data dirs reused across both runs ---
    let base = tempfile::tempdir().unwrap();
    let instance_dir = base.path().join("instance");
    let data_dir = base.path().join("data");
    std::fs::create_dir_all(&instance_dir).unwrap();
    std::fs::create_dir_all(&data_dir).unwrap();

    // --- run 1: bring up node+service, submit commands, shut down ---
    // Mirror the node+service bring-up from m3_shmem_single_node.rs, using a
    // CounterSm and the dirs above. Submit three commands (1, 2, 3) via a
    // uc_client submit, awaiting each response so all three commit + apply.
    // Then shut the service and node down cleanly.
    //
    // Expected after run 1: counter sum == 6, node durable last_applied == 3
    // committed normal entries (plus any blank/membership entries openraft adds;
    // node_frontier is the highest committed index).
    submit_three_then_shutdown(&instance_dir, &data_dir).await;

    // --- run 2: cold restart with a FRESH CounterSm (state lost) ---
    // Bring the node+service back up against the SAME data_dir/instance_dir.
    // The fresh CounterSm reports last_applied == None (0) at attach; the node
    // must replay the committed gap so the reconstructed sum == 6.
    let handle = bring_up(&instance_dir, &data_dir).await;
    let sum = query_counter(&instance_dir).await;
    assert_eq!(sum, 6, "in-memory SM should be reconstructed from the log");
    handle.shutdown().await.unwrap();
}
```

Implement the helpers (`submit_three_then_shutdown`, `bring_up`, `query_counter`)
by copying the spawn/submit/query boilerplate from `m3_shmem_single_node.rs`
verbatim, substituting `CounterSm` and the stable dirs. Keep them in this test
file. The command codec is `u64` little-endian via the default bincode framing
the client/service already use for `Command`/`Query`.

- [ ] **Step 2: Run it to verify it fails first against the un-wired build**

(If implementing strictly TDD, stash Task 6's builder wiring, run, see the
assertion fail with `sum == 0`, then restore.) Otherwise run directly:

Run: `cargo test -p uc_node --test reconstruct_cold_start -- --test-threads=1`
Expected (with Tasks 1-6 in place): PASS — `sum == 6`.
Expected (without the catch-up wiring): FAIL — `sum == 0` (fresh SM, no replay).

- [ ] **Step 3: Commit**

```bash
git add uc_node/tests/reconstruct_cold_start.rs
git commit -m "test(uc_node): cold-start reconstruction of an in-memory SM"
```

---

## Task 8: Full verification + clippy

- [ ] **Step 1: Workspace build + clippy (project gate: zero warnings)**

Run: `cargo clippy --workspace -- -D warnings`
Expected: clean.

- [ ] **Step 2: Run the affected test suites**

Run:
```bash
cargo test -p uc_service runtime::handshake
cargo test -p uc_node --lib runtime::reconstruct
cargo test -p uc_node --test m3_shmem_single_node
cargo test -p uc_node --test reconstruct_cold_start -- --test-threads=1
```
Expected: all PASS.

- [ ] **Step 3: Commit any fmt fixups**

```bash
cargo fmt
git add -A && git commit -m "style: cargo fmt (reconstruct phase 1a)" || true
```

---

## Self-review notes (against the spec)

- **§4 channel A** → Tasks 1, 2 (service write) + Task 6 (node Acquire read). The
  overwrite-before-READY rule is enforced by Task 2's ordering.
- **§2 flow / log-replay** → Tasks 3 (decision), 4 (replay primitive), 5 (driver),
  6 (wiring). Catch-up runs before `finish()` (no gate) per the 1a simplification.
- **§7 error handling** → `NeedsSnapshot` returns a loud `ClusterError::Recovery`
  (Phase 2 placeholder is an *explicit error*, not a code placeholder); replay/read
  failures map to `ClusterError::Recovery` and fail node start without corrupting
  raft state.
- **Out of scope for 1a (deferred to 1b/Phase 2):** mid-life reattach, the gate,
  cursor reset, snapshot build/install, the `StateMachine` trait change, re-enabling
  the node-side cross-check, reverting `RegisterSm`. None are referenced by 1a tasks.
- **Type consistency:** `node_frontier`/`replay_committed` (Task 4) match their uses
  in Task 5; `decide_catchup_source`/`CatchupSource` (Task 3) match Task 5;
  `publish_service_last_applied` (Task 1) matches Task 2.
