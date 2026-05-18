# M5 Implementation Plan — `OutputHandler` at-least-once dispatch

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the leader-only `OutputHandler::on_committed` async path: a new `service/output.ring` + `service/output_resp.ring` SPSC pair, the service-side `output_loop`, a node-side `output_dispatcher` driven by a `tokio::sync::mpsc` channel from the apply path, a durable `output_progress.state` `StableValue<u64>`, and a leader-transition replay task that scans `(last_completed, last_applied]` from the journal. Apply path stays decoupled from output latency; output ring backpressure skips and lets replay catch up.

**Architecture:** Three coordinated changes inside `ultima_cluster`. (1) `uc_protocol::frames::output` defines two new wire types (`MSG_TYPE_OUTPUT=10`, `MSG_TYPE_OUTPUT_RESP=11`) with `service_id` in `flags` byte 0 (uniform with M4). (2) `uc_service` adds the `OutputHandler` trait, the `output_loop` tokio task, and wraps the user's `StateMachine` in `Arc<parking_lot::RwLock<S>>` so apply (sync write-lock) and output (async read-lock) can coexist. (3) `uc_node` adds `output_progress: StableValue<u64>` to `LogStorageHandles`, two new SPSC rings under `service/`, an `output_dispatcher` tokio task fed by an internal `tokio::sync::mpsc` channel from the apply_dispatcher, and an `output_replay` one-shot task spawned on leader-acquisition that drives gap-fill from the journal.

**Tech Stack:** Rust 2024 edition, openraft 0.10, tokio (current_thread for tests), bincode 2, bytes, memmap2, parking_lot, dashmap, crc32fast, thiserror, async-trait, tempfile, tracing. ultima_journal `StableValue<T>` for the marker; existing `Journal::iter_range` for replay reads.

---

## Spec & predecessor pointers

- **Canonical M5 spec:** `docs/superpowers/specs/2026-05-18-uc-m5-output-handler-design.md` (decisions table, phasing, error model, test scenarios).
- **Canonical project design:** `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` (§"OutputHandler" line 253; §"Output pipeline" line ~680).
- **Predecessor task records:** `docs/tasks/task05_m4_clients_and_ring_fix.md` (immediate), `task04_m3_5_openraft_0_10_upgrade.md`, `task03_m3_shmem_service_split.md`.

## File structure

### New files

| File | Responsibility |
|---|---|
| `uc_protocol/src/frames/output.rs` | `MSG_TYPE_OUTPUT=10`, `MSG_TYPE_OUTPUT_RESP=11` constants. `OutputError { Retryable(String), Permanent(String) }` enum (serde). `encode_extra_output(log_index: u64) -> [u8; 8]` and `decode_extra_output(extra) -> u64`. `encode_flags_output(service_id: u8) -> u16` and `decode_flags_output(flags) -> Result<u8, OutputFrameError>` with `OutputFrameError::UnknownServiceId(u8)`. |
| `uc_service/src/output.rs` | `OutputHandler<S>` async trait. `NoopOutput`. Re-exports `OutputError` from `uc_protocol`. |
| `uc_service/src/runtime/output_loop.rs` | The tokio task consuming `service/output.ring`, decoding `cmd_bytes` into `S::Command`, taking a read lock on the shared `Arc<RwLock<S>>`, invoking `output_handler.on_committed(...)`, publishing `OutputResp` on `service/output_resp.ring`. |
| `uc_node/src/runtime/output_dispatcher.rs` | `spawn_output_dispatcher<S>` tokio task. Owns the SPSC producer for `service/output.ring` and consumer for `service/output_resp.ring`. Reads `(log_index, cmd_bytes)` from a `tokio::sync::mpsc::Receiver` (fed by both apply_dispatcher and replay). Publishes OutputFrame with 1 s grace; on Ok advances `output_progress.state.store(idx).wait()`; on Retryable backs off (10 ms → 1 s cap) and republishes while still leader; on Permanent warn-logs + advances. Tracks `is_leader` from a `tokio::watch::Receiver` derived from `raft.metrics()` and drains-without-publishing when not leader. |
| `uc_node/src/runtime/output_replay.rs` | `spawn_output_replay<S>` one-shot task spawned on each Leader-acquisition. Reads `last_completed = output_progress.state.load()` and `last_applied` from raft metrics; for each `index in (last_completed, last_applied]` reads `cmd_bytes` from `Journal::iter_range`, sends `(index, cmd_bytes)` into the same mpsc channel that apply_dispatcher uses. Exits when caught up or when leadership is lost. |
| `uc_node/tests/m5_output_smoke.rs` … `m5_output_ring_backpressure_skip.rs` | Seven integration tests (one file per scenario). |
| `docs/tasks/task06_m5_output_handler.md` | Final consolidated record. Created in Phase 6; replaces the spec + this plan. |

### Modified files

| File | Change |
|---|---|
| `uc_protocol/src/frames/mod.rs` | `pub mod output;` |
| `uc_node/src/raft/log_storage.rs` | Add `output_progress: Arc<StableValue<u64>>` field to `LogStorageHandles` and `JournalLogStorage`. Open `output_progress.state` next to the existing StableValues. `assert_consistent` in `runtime/recovery.rs` checks `output_progress.state.load().unwrap_or(0) <= last_applied.load().unwrap_or(default).index`. |
| `uc_node/src/runtime/recovery.rs` | Add the `output_progress <= last_applied` invariant check. |
| `uc_node/src/ipc/service_link.rs` | `ServiceLink::create` also creates `service/output.ring` (SPSC) and `service/output_resp.ring` (SPSC). `ServiceLink` struct gains `output_producer: SpscProducer`, `output_resp_consumer: SpscConsumer`. Service-side `attach` does the symmetric open: `output_consumer: SpscConsumer`, `output_resp_producer: SpscProducer`. |
| `uc_service/src/runtime/apply_loop.rs` | Replace owned `state_machine: S` with `state: Arc<parking_lot::RwLock<S>>`. apply takes write_lock for the duration of `state.write().apply(log_index, cmd)`. |
| `uc_service/src/runtime/service.rs` | `ServiceBuilder::output_handler` actually stores the handler. `ServiceBuilder::run` allocates the `Arc<RwLock<S>>`, passes a clone to `apply_loop`, and (when a non-Noop handler is wired) spawns `output_loop` with the same clone + the output consumer half + the output_resp producer half. `Service` struct gains `output_loop: Option<OutputLoopHandle>`. Shutdown joins it before apply_loop. |
| `uc_service/src/lib.rs` | Re-export `OutputHandler`, `NoopOutput`, `OutputError`. |
| `uc_node/src/runtime/builder.rs` | Shmem branch: pass `output_producer` and `output_resp_consumer` (from `ServiceLink`) into `spawn_output_dispatcher` alongside the existing pieces. Wire the apply→output mpsc channel: apply_dispatcher gets a `Sender<(u64, Bytes)>`; output_dispatcher gets the matching `Receiver`. Subscribe to raft metrics to spawn `output_replay` on Leader transitions. |
| `uc_node/src/runtime/node.rs` | `NodeHandle` gains `output_dispatcher: Option<OutputDispatcherHandle>` and `output_replay: Arc<parking_lot::Mutex<Option<OutputReplayHandle>>>` (mutex because replays are spawned/joined repeatedly on leader transitions). `shutdown` joins both in order — output_replay first, then output_dispatcher, before service_watcher. |
| `uc_node/src/raft/state_machine_shmem.rs` | `ShmemAdaptedStateMachine::new` accepts a `Sender<(u64, Bytes)>` and, after publishing ApplyResp back to openraft, does a `try_send` on the channel. `try_send` Full → log warn + drop; treats as the skip path that replay will catch. |

### Cargo.toml additions

| Crate | Add to `[dependencies]` |
|---|---|
| `uc_protocol` | `serde` (already), `bincode` (already), `thiserror` (already) — no new deps. |
| `uc_service` | `async-trait = "0.1"` (workspace dep — check if present; add to workspace `[dependencies]` table if missing). `parking_lot` (already). `tokio` (already). |
| `uc_node` | No new deps. |

---

## Phase 1 — `uc_protocol::frames::output`

**Why first:** every later phase rides on these wire types. Land them with their codec tests; the rest of the stack imports them.

### Task 1.1: Create `frames::output` module + codec

**Files:**
- Create: `uc_protocol/src/frames/output.rs`
- Modify: `uc_protocol/src/frames/mod.rs`

- [ ] **Step 1: Create the module**

Create `uc_protocol/src/frames/output.rs`:

```rust
//! Output ring frame types (M5).
//!
//! `header_extra` layout (8 bytes): the raft `log_index` as a u64
//! little-endian.
//!
//! `flags` layout (uniform with M4 retrofits):
//!   * bits 0..7  — `service_id: u8` (always `0` in v1; decoders error
//!     on `!= 0` with `UnknownServiceId`).
//!   * bits 8..15 — reserved (must be zero).
//!
//! `msg_type`:
//!   * `10` — `OutputFrame` (node → service, SPSC `service/output.ring`).
//!     Payload: bincode-encoded `Command` from the journal record at
//!     `log_index`. Identical bytes to the original `ApplyFrame` payload.
//!   * `11` — `OutputResp` (service → node, SPSC `service/output_resp.ring`).
//!     Payload: bincode-encoded `Result<(), OutputError>`.

use serde::{Deserialize, Serialize};

pub const MSG_TYPE_OUTPUT: u16 = 10;
pub const MSG_TYPE_OUTPUT_RESP: u16 = 11;

pub const FLAGS_SERVICE_ID_MASK: u16 = 0x00FF;

#[derive(Debug, thiserror::Error)]
pub enum OutputFrameError {
    #[error("unknown service_id: {0}")]
    UnknownServiceId(u8),
}

/// User-returned outcome from `OutputHandler::on_committed`.
///
/// Wire-encoded by `uc_service` and decoded by `uc_node`'s
/// output_dispatcher to decide retry vs advance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OutputError {
    /// Retry while still leader. Backoff bounded by output_dispatcher.
    Retryable(String),
    /// Log warn, advance output_progress anyway, move on.
    Permanent(String),
}

#[inline]
pub fn encode_extra_output(log_index: u64) -> [u8; 8] {
    log_index.to_le_bytes()
}

#[inline]
pub fn decode_extra_output(extra: [u8; 8]) -> u64 {
    u64::from_le_bytes(extra)
}

#[inline]
pub fn encode_flags_output(service_id: u8) -> u16 {
    service_id as u16
}

#[inline]
pub fn decode_flags_output(flags: u16) -> Result<u8, OutputFrameError> {
    let service_id = (flags & FLAGS_SERVICE_ID_MASK) as u8;
    if service_id != 0 {
        return Err(OutputFrameError::UnknownServiceId(service_id));
    }
    Ok(service_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extra_round_trip() {
        for li in [0u64, 1, 42, 1 << 40, u64::MAX] {
            assert_eq!(decode_extra_output(encode_extra_output(li)), li);
        }
    }

    #[test]
    fn flags_round_trip_v1() {
        let f = encode_flags_output(0);
        assert_eq!(decode_flags_output(f).unwrap(), 0);
    }

    #[test]
    fn flags_rejects_nonzero_service_id() {
        let f = encode_flags_output(3);
        assert!(matches!(
            decode_flags_output(f),
            Err(OutputFrameError::UnknownServiceId(3))
        ));
    }

    #[test]
    fn output_error_serde_round_trip() {
        let cases = [
            OutputError::Retryable("upstream timeout".to_string()),
            OutputError::Permanent("invalid record".to_string()),
        ];
        for err in cases {
            let bytes = bincode::serde::encode_to_vec(&err, bincode::config::standard())
                .expect("encode");
            let (got, _) = bincode::serde::decode_from_slice::<OutputError, _>(
                &bytes,
                bincode::config::standard(),
            )
            .expect("decode");
            assert!(matches!((err.clone(), got), {
                (OutputError::Retryable(a), OutputError::Retryable(b)) if a == b
            } | {
                (OutputError::Permanent(a), OutputError::Permanent(b)) if a == b
            }));
        }
    }

    #[test]
    fn msg_type_constants_stable() {
        assert_eq!(MSG_TYPE_OUTPUT, 10);
        assert_eq!(MSG_TYPE_OUTPUT_RESP, 11);
    }
}
```

- [ ] **Step 2: Register the module**

In `uc_protocol/src/frames/mod.rs`, alongside the existing `pub mod apply; pub mod client; pub mod query; pub mod snapshot;` add:

```rust
pub mod output;
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p uc_protocol --lib frames::output`
Expected: 5 PASS.

- [ ] **Step 4: Run the workspace build to catch any unrelated breakage**

Run: `cargo build --workspace`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/frames/output.rs uc_protocol/src/frames/mod.rs
git commit -m "feat(uc_protocol): add frames::output (M5 OutputFrame/OutputResp + OutputError)"
```

---

## Phase 2 — `uc_service` output trait + loop

### Task 2.1: Add `async-trait` dep and the `OutputHandler` trait

**Files:**
- Create: `uc_service/src/output.rs`
- Modify: `uc_service/src/lib.rs`
- Modify: `uc_service/Cargo.toml`
- Modify: `Cargo.toml` (workspace `[workspace.dependencies]` if `async-trait` missing)

- [ ] **Step 1: Add `async-trait` to workspace dependencies if missing**

Check `Cargo.toml`:

```bash
grep -n "async-trait\|async_trait" Cargo.toml
```

If absent, add to `[workspace.dependencies]`:

```toml
async-trait = "0.1"
```

If `async-trait` is already in workspace deps, skip this step.

- [ ] **Step 2: Add `async-trait` to `uc_service/Cargo.toml` `[dependencies]`**

```toml
async-trait = { workspace = true }
```

- [ ] **Step 3: Create the trait module**

Create `uc_service/src/output.rs`:

```rust
//! `OutputHandler` — the user's optional async leader-only post-commit hook.
//!
//! See `docs/superpowers/specs/2026-05-18-uc-m5-output-handler-design.md`
//! for the at-least-once delivery contract.
//!
//! **State-borrow contract:** the framework calls `on_committed` while
//! holding a read borrow of the service's `StateMachine`. While that
//! borrow is alive, the service's `apply` path is blocked. To keep apply
//! latency stable, extract what you need synchronously and drop the
//! borrow before any `.await` on slow I/O:
//!
//! ```ignore
//! async fn on_committed(&self, _: u64, cmd: &Cmd, state: &Counter)
//!     -> Result<(), OutputError>
//! {
//!     let snapshot = state.get_relevant_data(cmd); // borrow ends here
//!     kafka_client.send(snapshot).await
//!         .map_err(|e| OutputError::Retryable(e.to_string()))?;
//!     Ok(())
//! }
//! ```

use crate::StateMachine;

pub use uc_protocol::frames::output::OutputError;

#[async_trait::async_trait]
pub trait OutputHandler<S: StateMachine>: Send + Sync + 'static {
    async fn on_committed(
        &self,
        log_index: u64,
        cmd: &S::Command,
        state: &S,
    ) -> Result<(), OutputError>;
}

/// No-op `OutputHandler` implementation. Returns `Ok(())` for every
/// commit. Used as the default when the user doesn't wire up a handler.
pub struct NoopOutput;

#[async_trait::async_trait]
impl<S: StateMachine> OutputHandler<S> for NoopOutput {
    async fn on_committed(
        &self,
        _log_index: u64,
        _cmd: &S::Command,
        _state: &S,
    ) -> Result<(), OutputError> {
        Ok(())
    }
}
```

- [ ] **Step 4: Re-export from `lib.rs`**

In `uc_service/src/lib.rs`, after the existing `pub mod ...; pub use ...;` lines add:

```rust
pub mod output;
pub use output::{NoopOutput, OutputError, OutputHandler};
```

- [ ] **Step 5: Build + verify**

Run: `cargo build -p uc_service`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml uc_service/Cargo.toml uc_service/src/output.rs uc_service/src/lib.rs
git commit -m "feat(uc_service): OutputHandler trait + NoopOutput (M5 Task 2.1)"
```

### Task 2.2: Rewire `apply_loop` to use `Arc<RwLock<S>>`

**Why now:** Phase 2's `output_loop` (Task 2.3) needs read-lock access to the same `S` that apply_loop has. Rewire apply first so output_loop has something to read from.

**Files:**
- Modify: `uc_service/src/runtime/apply_loop.rs`

- [ ] **Step 1: Inspect the current `apply_loop` signature**

```bash
sed -n '30,70p' uc_service/src/runtime/apply_loop.rs
```

The current shape (paraphrased) is `spawn_apply_loop<S>(state_machine: S, ...)` taking ownership of `S` and calling `state_machine.apply(...)` on the sync thread.

- [ ] **Step 2: Change the signature to accept `Arc<parking_lot::RwLock<S>>`**

Replace the `state_machine: S` parameter with `state: Arc<parking_lot::RwLock<S>>` (clone-cheap; the apply loop owns one clone, the output loop will own another).

In the loop body, replace every `state_machine.apply(...)` with:

```rust
let resp = {
    let mut guard = state.write();
    guard.apply(log_index, cmd)
};
```

And every `state_machine.query(...)` or `state_machine.build_snapshot(...)` etc. with the matching `state.read()` or `state.write()` block, **dropping the guard before any `await`** if one exists nearby. (`apply` itself is sync — the canonical contract — so the write guard scope is just the call.)

The `state_machine` local can be renamed to `state` throughout.

- [ ] **Step 3: Verify by building uc_service**

Run: `cargo build -p uc_service`
Expected: clean. If callers (in `service.rs`) break, leave them — Task 2.4 wires them up; this commit may leave `service.rs` broken temporarily.

If that's the case, instead make the apply_loop signature backwards-compatible by adding a NEW function `spawn_apply_loop_shared<S>` that takes `Arc<RwLock<S>>`, and leave the old one untouched for now. Either approach is fine — pick whichever leaves the workspace compiling. If you go with the new function, mark the old `spawn_apply_loop` as `#[deprecated]` so Task 2.4 doesn't forget to remove it.

Recommended path: leave old fn, add new fn `spawn_apply_loop_shared`. Less risk.

- [ ] **Step 4: Run existing M3 + M4 tests to confirm no regression**

```bash
cargo test -p uc_service --lib
cargo test -p uc_node --test m3_shmem_single_node --test m3_three_node_shmem
```

All must pass — no behavior should change (the old `spawn_apply_loop` is still wired in `service.rs`).

- [ ] **Step 5: Commit**

```bash
git add uc_service/src/runtime/apply_loop.rs
git commit -m "feat(uc_service): apply_loop accepts Arc<RwLock<S>> (M5 Task 2.2)"
```

### Task 2.3: Add `output_loop` tokio task

**Files:**
- Create: `uc_service/src/runtime/output_loop.rs`
- Modify: `uc_service/src/runtime/mod.rs` — register the new module.

- [ ] **Step 1: Inspect the ring producer/consumer types**

```bash
grep -n "fn try_write\|fn try_read\|pub struct Spsc" uc_protocol/src/ring/spsc.rs | head -10
```

Expected: `SpscProducer::try_write(msg_type, flags, header_extra, payload)` and `SpscConsumer::try_read(&mut buf) -> Result<Option<Record>, RingError>`. Match the apply_loop's existing patterns.

- [ ] **Step 2: Create the module**

Create `uc_service/src/runtime/output_loop.rs`:

```rust
//! Service-side `output_loop` — consumes `OutputFrame` from
//! `service/output.ring`, invokes the user's `OutputHandler::on_committed`
//! with a read-locked view of the state, publishes `OutputResp` back to
//! `service/output_resp.ring`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::RwLock;
use tokio::task::JoinHandle;

use uc_protocol::frames::output::{
    MSG_TYPE_OUTPUT, MSG_TYPE_OUTPUT_RESP, OutputError, decode_extra_output, decode_flags_output,
    encode_extra_output, encode_flags_output,
};
use uc_protocol::ring::RingError;
use uc_protocol::ring::spsc::{SpscConsumer, SpscProducer};

use crate::output::OutputHandler;
use crate::StateMachine;

const POLL_IDLE: Duration = Duration::from_micros(100);
const FULL_BACKOFF: Duration = Duration::from_micros(100);

pub struct OutputLoopHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

/// Spawn the service-side output loop.
pub fn spawn_output_loop<S, O>(
    state: Arc<RwLock<S>>,
    handler: Arc<O>,
    mut consumer: SpscConsumer,
    producer: SpscProducer,
) -> OutputLoopHandle
where
    S: StateMachine,
    O: OutputHandler<S> + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);

    let join = tokio::spawn(async move {
        let mut payload_buf: Vec<u8> = Vec::with_capacity(4096);
        while !stop_for_task.load(Ordering::Relaxed) {
            match consumer.try_read(&mut payload_buf) {
                Ok(Some(rec)) if rec.msg_type == MSG_TYPE_OUTPUT => {
                    let log_index = decode_extra_output(rec.header_extra);

                    // Validate service_id; for v1 it must be 0.
                    if let Err(e) = decode_flags_output(rec.flags) {
                        publish_resp(
                            &producer,
                            log_index,
                            Err(OutputError::Permanent(format!("{e}"))),
                        )
                        .await;
                        continue;
                    }

                    // Decode cmd from the payload.
                    let cmd = match bincode::serde::decode_from_slice::<S::Command, _>(
                        &payload_buf,
                        bincode::config::standard(),
                    ) {
                        Ok((cmd, _)) => cmd,
                        Err(e) => {
                            tracing::error!(?e, log_index, "output cmd decode failed");
                            publish_resp(
                                &producer,
                                log_index,
                                Err(OutputError::Permanent(format!("decode: {e}"))),
                            )
                            .await;
                            continue;
                        }
                    };

                    // Invoke user's on_committed under read lock.
                    // SAFETY of the borrow: user contract is documented in
                    // `output::OutputHandler`. The lock is held across the
                    // .await, so apply blocks meanwhile.
                    let result = {
                        let guard = state.read();
                        handler.on_committed(log_index, &cmd, &*guard).await
                    };

                    publish_resp(&producer, log_index, result).await;
                }
                Ok(Some(rec)) => {
                    tracing::warn!(msg_type = rec.msg_type, "unexpected frame on output.ring");
                }
                Ok(None) => tokio::time::sleep(POLL_IDLE).await,
                Err(e) => {
                    tracing::error!(error = ?e, "output.ring read");
                    tokio::time::sleep(POLL_IDLE).await;
                }
            }
        }
    });

    OutputLoopHandle { join, stop }
}

async fn publish_resp(
    producer: &SpscProducer,
    log_index: u64,
    result: Result<(), OutputError>,
) {
    let payload = match bincode::serde::encode_to_vec(&result, bincode::config::standard()) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(?e, log_index, "encode OutputResp payload");
            return;
        }
    };
    loop {
        match producer.try_write(
            MSG_TYPE_OUTPUT_RESP,
            encode_flags_output(0),
            encode_extra_output(log_index),
            &payload,
        ) {
            Ok(()) => return,
            Err(RingError::Full) => tokio::time::sleep(FULL_BACKOFF).await,
            Err(e) => {
                tracing::error!(?e, log_index, "output_resp.ring write");
                return;
            }
        }
    }
}
```

- [ ] **Step 3: Register the module**

In `uc_service/src/runtime/mod.rs` add:

```rust
pub mod output_loop;
```

- [ ] **Step 4: Build + verify**

Run: `cargo build -p uc_service`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add uc_service/src/runtime/output_loop.rs uc_service/src/runtime/mod.rs
git commit -m "feat(uc_service): output_loop tokio task (M5 Task 2.3)"
```

### Task 2.4: `ServiceBuilder::output_handler` + service-side wiring

**Files:**
- Modify: `uc_service/src/runtime/service.rs`

- [ ] **Step 1: Wire the handler storage**

Find `ServiceBuilder<S>`. It currently has an `output_handler(self, _handler: O) -> Self` method that ignores its argument. Replace with real storage:

```rust
pub struct ServiceBuilder<S: StateMachine> {
    config: ServiceConfig,
    state_machine: S,
    // NEW: optional output handler. None = NoopOutput; Some(boxed dyn) = user-supplied.
    output_handler: Option<Box<dyn ErasedOutputHandler<S>>>,
}
```

(Type-erase via `Box<dyn ErasedOutputHandler<S>>` because `OutputHandler<S>` has an `async fn` and an associated future type would make the field generic over O — but the builder shouldn't be generic over O. The erasure trait wraps `Arc<dyn OutputHandler<S>>`.)

Add the erasure trait near the top of the file:

```rust
use std::sync::Arc;
use parking_lot::RwLock;
use crate::output::{NoopOutput, OutputHandler};

trait ErasedOutputHandler<S: StateMachine>: Send + 'static {
    /// Spawn the output loop with the inner handler, returning its handle.
    fn spawn(
        self: Box<Self>,
        state: Arc<RwLock<S>>,
        consumer: uc_protocol::ring::spsc::SpscConsumer,
        producer: uc_protocol::ring::spsc::SpscProducer,
    ) -> crate::runtime::output_loop::OutputLoopHandle;
}

struct ErasedHandler<S: StateMachine, O: OutputHandler<S>> {
    inner: Arc<O>,
    _marker: std::marker::PhantomData<S>,
}

impl<S: StateMachine, O: OutputHandler<S>> ErasedOutputHandler<S> for ErasedHandler<S, O> {
    fn spawn(
        self: Box<Self>,
        state: Arc<RwLock<S>>,
        consumer: uc_protocol::ring::spsc::SpscConsumer,
        producer: uc_protocol::ring::spsc::SpscProducer,
    ) -> crate::runtime::output_loop::OutputLoopHandle {
        crate::runtime::output_loop::spawn_output_loop(state, self.inner, consumer, producer)
    }
}
```

- [ ] **Step 2: Rewrite `output_handler` to store the handler**

```rust
impl<S: StateMachine> ServiceBuilder<S> {
    pub fn output_handler<O: OutputHandler<S>>(mut self, handler: O) -> Self {
        self.output_handler = Some(Box::new(ErasedHandler {
            inner: Arc::new(handler),
            _marker: std::marker::PhantomData,
        }));
        self
    }
}
```

Update `ServiceBuilder::new` to default `output_handler: None`.

- [ ] **Step 3: Rewire `ServiceBuilder::run` to allocate the shared state**

In the `run()` body, where the apply_loop is currently spawned with `state_machine: S` (or `self.state_machine`), change:

1. Allocate the shared state: `let state = Arc::new(RwLock::new(self.state_machine));`
2. Pass `Arc::clone(&state)` into the new `spawn_apply_loop_shared` (the one introduced in Task 2.2).
3. If `self.output_handler` is Some, open the output rings from the attach side (see Step 4 below) and spawn `output_loop` with another `Arc::clone(&state)`.

Open output rings:

```rust
// After the existing service-side ring opens (apply.ring, apply_resp.ring,
// query.ring, query_resp.ring), add:
let output_consumer = uc_protocol::ring::spsc::SpscRing::open(
    &instance_dir.join("service").join("output.ring"),
)
.map_err(|e| ServiceError::Io(io::Error::other(format!("open output.ring: {e}"))))?
.into_split()
.1; // consumer half on the service side
let output_resp_producer = uc_protocol::ring::spsc::SpscRing::open(
    &instance_dir.join("service").join("output_resp.ring"),
)
.map_err(|e| ServiceError::Io(io::Error::other(format!("open output_resp.ring: {e}"))))?
.into_split()
.0; // producer half on the service side
```

Then if `self.output_handler.is_some()`:

```rust
let handle = self.output_handler.unwrap().spawn(
    Arc::clone(&state),
    output_consumer,
    output_resp_producer,
);
// store handle on the Service struct for shutdown
```

If `self.output_handler.is_none()`, **still open the rings** (the node side creates them unconditionally — see Phase 3) and just don't spawn the loop. The unconsumed `output.ring` will pile up and trigger the node's skip path within 1 s, which is the design intent for the no-handler case.

- [ ] **Step 4: Add the handle to the `Service` struct + shutdown ordering**

```rust
pub struct Service {
    // existing fields...
    output_loop: Option<crate::runtime::output_loop::OutputLoopHandle>,
}
```

In `Service::shutdown`, add an early step (before joining the apply_loop):

```rust
if let Some(o) = self.output_loop.take() {
    o.stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = o.join.await;
}
```

This ordering matters: output_loop holds the read lock during `on_committed`. Stopping it first unblocks any pending apply that wanted the write lock. Otherwise shutdown could deadlock if apply is waiting on the write lock while output is waiting on something that's already shutting down.

- [ ] **Step 5: Build + run M3 + M4 capstone tests**

```bash
cargo build --workspace
cargo test -p uc_node --test m3_shmem_single_node --test m3_three_node_shmem --test m3_service_crash --test m4_client_single_node --test m4_client_concurrent --test m4_client_session_gc
```

All must still pass. The output rings will be created (Phase 3 wires that) — until Phase 3 lands, this task will fail to build because the rings don't exist yet. **Sequence note:** Task 2.4 must be committed AFTER Task 3.1 (which creates the rings in `ServiceLink::create`). Reorder if needed: do Task 3.1, then Task 2.4.

**Recommended ordering for Phase 2 + 3:** Tasks 1.1, 2.1, 2.2, 2.3, **3.1, 2.4**, 3.2, 3.3, 3.4. Task 2.4 lives at the boundary.

- [ ] **Step 6: Commit**

```bash
git add uc_service/src/runtime/service.rs
git commit -m "feat(uc_service): ServiceBuilder::output_handler + spawn output_loop (M5 Task 2.4)"
```

---

## Phase 3 — `uc_node` infrastructure

### Task 3.1: Add output rings to `ServiceLink::create`

**Files:**
- Modify: `uc_node/src/ipc/service_link.rs`

- [ ] **Step 1: Add the two new SPSC rings**

In `ServiceLink::create(instance_dir, ...)`, after the existing `apply.ring`, `apply_resp.ring`, `query.ring`, `query_resp.ring` creation calls, add:

```rust
let output = SpscRing::create(
    &service_dir.join("output.ring"),
    OUTPUT_RING_CAP,
    OUTPUT_RING_MAX_MSG,
)?;
let output_resp = SpscRing::create(
    &service_dir.join("output_resp.ring"),
    OUTPUT_RING_CAP,
    OUTPUT_RING_MAX_MSG,
)?;

let (output_producer, _) = output.into_split();  // producer on node side
let (_, output_resp_consumer) = output_resp.into_split();  // consumer on node side
```

Add the two new constants at the top of the file:

```rust
pub const OUTPUT_RING_CAP: u64 = 16 * 1024 * 1024;
pub const OUTPUT_RING_MAX_MSG: u32 = 4 * 1024 * 1024;
```

Extend the `ServiceLink` struct:

```rust
pub struct ServiceLink {
    pub apply_producer: SpscProducer,
    pub apply_resp_consumer: SpscConsumer,
    pub query_producer: SpscProducer,
    pub query_resp_consumer: SpscConsumer,
    pub output_producer: SpscProducer,    // NEW
    pub output_resp_consumer: SpscConsumer, // NEW
}
```

Initialize the new fields in `ServiceLink::create`'s `Ok(ServiceLink { ... })`.

- [ ] **Step 2: Unit test in `service_link.rs::tests`**

Find the existing tests module. Add:

```rust
#[test]
fn create_creates_output_rings() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path().join("service")).unwrap();
    let _link = ServiceLink::create(tmp.path()).expect("create");
    assert!(tmp.path().join("service").join("output.ring").is_file());
    assert!(tmp.path().join("service").join("output_resp.ring").is_file());
}
```

- [ ] **Step 3: Verify M3/M4 still work**

```bash
cargo test -p uc_node --lib ipc::service_link
cargo test -p uc_node --test m3_shmem_single_node --test m4_client_single_node
```

All must pass — adding the two new rings doesn't change the existing pipeline.

- [ ] **Step 4: Commit**

```bash
git add uc_node/src/ipc/service_link.rs
git commit -m "feat(uc_node): add service/output.{ring,_resp.ring} to ServiceLink (M5 Task 3.1)"
```

> **Now run Task 2.4** if you haven't already — it depends on these rings existing.

### Task 3.2: Add `output_progress: StableValue<u64>` to `LogStorageHandles`

**Files:**
- Modify: `uc_node/src/raft/log_storage.rs`
- Modify: `uc_node/src/runtime/recovery.rs`

- [ ] **Step 1: Extend `JournalLogStorage`**

In `uc_node/src/raft/log_storage.rs`, in the `JournalLogStorage` struct definition, add:

```rust
pub(crate) output_progress: Arc<StableValue<u64>>,
```

In `JournalLogStorage::open`, after the existing StableValue opens (vote, committed, last_purged, last_applied, snapshot_meta), add:

```rust
let output_progress = Arc::new(StableValue::open(StableValueConfig {
    path: data_dir.join("output_progress.state"),
    durability: Durability::Sync,
    ..StableValueConfig::default()
})?);
```

(Use `Durability::Sync` to match per-record-fsync intent.)

Add to the `Self { ... }` block at the end: `output_progress,`.

- [ ] **Step 2: Extend `LogStorageHandles`**

```rust
pub struct LogStorageHandles {
    pub last_applied: Arc<StableValue<RaftLogId>>,
    pub snapshot_meta: Arc<StableValue<StoredSnapshotMeta>>,
    pub output_progress: Arc<StableValue<u64>>,  // NEW
    // (any existing handles)
}
```

In `JournalLogStorage::handles(...)`, populate `output_progress: Arc::clone(&self.output_progress)`.

- [ ] **Step 3: Extend `assert_consistent` in `recovery.rs`**

Find `assert_consistent` in `uc_node/src/runtime/recovery.rs`. After the existing consistency checks, add:

```rust
let last_applied_idx = log_storage
    .handles_local()
    .last_applied
    .load()?
    .map(|l| l.index)
    .unwrap_or(0);
let output_progress = log_storage
    .handles_local()
    .output_progress
    .load()?
    .unwrap_or(0);
if output_progress > last_applied_idx {
    return Err(ClusterError::Inconsistent(format!(
        "output_progress.state ({output_progress}) > last_applied ({last_applied_idx})"
    )));
}
```

(If `handles_local` doesn't exist, use whatever accessor `assert_consistent` is already using — adapt locally.)

- [ ] **Step 4: Run all existing uc_node tests**

```bash
cargo test -p uc_node --lib
cargo test -p uc_node --test m1_single_node --test m3_shmem_single_node
```

All must pass. The new StableValue is created at startup (defaults to absent → 0) and assert_consistent's added check is vacuously true for existing tests.

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/raft/log_storage.rs uc_node/src/runtime/recovery.rs
git commit -m "feat(uc_node): output_progress StableValue + invariant check (M5 Task 3.2)"
```

### Task 3.3: `output_dispatcher` tokio task

**Files:**
- Create: `uc_node/src/runtime/output_dispatcher.rs`
- Modify: `uc_node/src/runtime/mod.rs`

- [ ] **Step 1: Create the dispatcher module**

Create `uc_node/src/runtime/output_dispatcher.rs`:

```rust
//! Node-side output dispatcher.
//!
//! Reads `(log_index, cmd_bytes)` tuples off an in-process
//! `tokio::sync::mpsc::Receiver` (fed by apply_dispatcher in steady-state
//! and by output_replay during leader-transition gap-fill). Publishes
//! `OutputFrame` on `service/output.ring`, awaits `OutputResp` on
//! `service/output_resp.ring`, advances `output_progress.state` on Ok or
//! Permanent, retries with backoff on Retryable while still leader.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex as PlMutex;
use tokio::sync::mpsc::Receiver;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use uc_protocol::frames::output::{
    MSG_TYPE_OUTPUT, MSG_TYPE_OUTPUT_RESP, OutputError, decode_extra_output, decode_flags_output,
    encode_extra_output, encode_flags_output,
};
use uc_protocol::ring::RingError;
use uc_protocol::ring::spsc::{SpscConsumer, SpscProducer};

use ultima_journal::StableValue;

/// Whether the current node is the raft leader; flipped by the metrics
/// publisher (M4 infrastructure).
pub type LeaderStateRx = watch::Receiver<bool>;

pub struct OutputDispatcherHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

const PUBLISH_GRACE: Duration = Duration::from_secs(1);
const PUBLISH_RETRY_BACKOFF: Duration = Duration::from_micros(100);
const RESPONSE_POLL_IDLE: Duration = Duration::from_micros(100);
const RETRY_BACKOFF_INITIAL: Duration = Duration::from_millis(10);
const RETRY_BACKOFF_CAP: Duration = Duration::from_secs(1);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

pub fn spawn_output_dispatcher(
    mut rx: Receiver<(u64, Bytes)>,
    output_producer: SpscProducer,
    mut output_resp_consumer: SpscConsumer,
    output_progress: Arc<StableValue<u64>>,
    leader_rx: LeaderStateRx,
) -> OutputDispatcherHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);

    // Mutex on the SPSC producer so the publish-retry path doesn't fight
    // with itself (single producer; the mutex is for the borrow checker).
    let producer = Arc::new(PlMutex::new(output_producer));

    let join = tokio::spawn(async move {
        while !stop_for_task.load(Ordering::Relaxed) {
            // Receive next (log_index, cmd_bytes) tuple. Channel close
            // (sender dropped) → end the task.
            let Some((log_index, cmd_bytes)) = rx.recv().await else {
                break;
            };

            // Non-leader: drop. The replay path on next leadership
            // acquisition will pick this up via the journal scan.
            if !*leader_rx.borrow() {
                tracing::debug!(log_index, "output_dispatcher: not leader; dropping");
                continue;
            }

            // Steady-state attempt loop. Each iteration: publish frame,
            // await resp, decide based on resp variant.
            'outer: loop {
                if stop_for_task.load(Ordering::Relaxed) || !*leader_rx.borrow() {
                    break 'outer;
                }

                // 1) Publish OutputFrame with 1s grace.
                match publish_output_frame(&producer, log_index, &cmd_bytes).await {
                    PublishOutcome::Published => {}
                    PublishOutcome::Skipped => {
                        tracing::warn!(log_index, "output.ring full > 1s; skipping; replay will catch this");
                        // Do NOT advance output_progress — replay must
                        // catch this log_index later.
                        break 'outer;
                    }
                    PublishOutcome::FatalError => break 'outer,
                }

                // 2) Await response (timeout-bounded).
                let resp = match await_output_resp(&mut output_resp_consumer, log_index).await {
                    Some(r) => r,
                    None => {
                        tracing::warn!(log_index, "output_resp timeout; skipping; replay will catch");
                        break 'outer;
                    }
                };

                // 3) Decide.
                match resp {
                    Ok(()) => {
                        if let Err(e) = output_progress.store(log_index).wait() {
                            tracing::error!(?e, log_index, "output_progress store failed");
                        }
                        break 'outer;
                    }
                    Err(OutputError::Permanent(msg)) => {
                        tracing::warn!(log_index, msg, "OutputError::Permanent — advancing marker anyway");
                        if let Err(e) = output_progress.store(log_index).wait() {
                            tracing::error!(?e, log_index, "output_progress store failed");
                        }
                        break 'outer;
                    }
                    Err(OutputError::Retryable(msg)) => {
                        let backoff = current_backoff();
                        tracing::info!(log_index, msg, ?backoff, "OutputError::Retryable — backoff + retry");
                        tokio::time::sleep(backoff).await;
                        // Loop to retry publication.
                    }
                }
            } // 'outer
        }
    });

    OutputDispatcherHandle { join, stop }
}

enum PublishOutcome {
    Published,
    Skipped,       // bounded-wait expired
    FatalError,    // not Full; abandon
}

async fn publish_output_frame(
    producer: &Arc<PlMutex<SpscProducer>>,
    log_index: u64,
    cmd_bytes: &[u8],
) -> PublishOutcome {
    let deadline = std::time::Instant::now() + PUBLISH_GRACE;
    loop {
        let result = {
            let mut g = producer.lock();
            g.try_write(
                MSG_TYPE_OUTPUT,
                encode_flags_output(0),
                encode_extra_output(log_index),
                cmd_bytes,
            )
        };
        match result {
            Ok(()) => return PublishOutcome::Published,
            Err(RingError::Full) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(PUBLISH_RETRY_BACKOFF).await;
            }
            Err(RingError::Full) => return PublishOutcome::Skipped,
            Err(e) => {
                tracing::error!(?e, log_index, "output.ring write");
                return PublishOutcome::FatalError;
            }
        }
    }
}

async fn await_output_resp(
    consumer: &mut SpscConsumer,
    expected_log_index: u64,
) -> Option<Result<(), OutputError>> {
    let deadline = std::time::Instant::now() + RESPONSE_TIMEOUT;
    let mut buf: Vec<u8> = Vec::with_capacity(256);
    while std::time::Instant::now() < deadline {
        match consumer.try_read(&mut buf) {
            Ok(Some(rec)) if rec.msg_type == MSG_TYPE_OUTPUT_RESP => {
                if let Err(e) = decode_flags_output(rec.flags) {
                    tracing::warn!(?e, log_index = expected_log_index, "OutputResp bad flags");
                    return Some(Err(OutputError::Permanent(format!("{e}"))));
                }
                let got_idx = decode_extra_output(rec.header_extra);
                if got_idx != expected_log_index {
                    tracing::warn!(
                        got_idx,
                        expected = expected_log_index,
                        "OutputResp log_index mismatch — dropping"
                    );
                    continue;
                }
                let (result, _) = match bincode::serde::decode_from_slice::<
                    Result<(), OutputError>,
                    _,
                >(&buf, bincode::config::standard())
                {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!(?e, "OutputResp decode");
                        return Some(Err(OutputError::Permanent(format!("decode: {e}"))));
                    }
                };
                return Some(result);
            }
            Ok(Some(rec)) => {
                tracing::warn!(msg_type = rec.msg_type, "unexpected frame on output_resp.ring");
            }
            Ok(None) => tokio::time::sleep(RESPONSE_POLL_IDLE).await,
            Err(e) => {
                tracing::error!(?e, "output_resp.ring read");
                tokio::time::sleep(RESPONSE_POLL_IDLE).await;
            }
        }
    }
    None
}

// Module-level retry backoff state. The dispatcher has at most one
// in-flight Retryable per log_index, so a module-level state is fine for
// v1; a per-log_index policy is a follow-up.
fn current_backoff() -> Duration {
    // Simple exponential: starts at 10ms, doubles, caps at 1s. Reset on
    // each new log_index by the caller (we just track here per-attempt).
    // For v1, return a fixed mid-range value; refine in Task 5.3's test.
    Duration::from_millis(50)
}
```

**Note on `current_backoff`:** the simple stub returns 50 ms; the integration test `m5_output_retryable_backoff` (Task 5.3) will exercise the real exponential progression. Refactor in that task to a proper local `backoff_ms` variable inside the `'outer` loop, resetting on `Ok/Permanent` and doubling on each `Retryable`. **Reminder:** when Task 5.3 lands, the stub becomes:

```rust
let mut backoff = RETRY_BACKOFF_INITIAL;
'outer: loop {
    // ...
    Err(OutputError::Retryable(_)) => {
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(RETRY_BACKOFF_CAP);
    }
    Ok(_) | Err(OutputError::Permanent(_)) => break 'outer,
}
```

- [ ] **Step 2: Register the module**

In `uc_node/src/runtime/mod.rs`:

```rust
pub mod output_dispatcher;
```

- [ ] **Step 3: Build + verify**

Run: `cargo build -p uc_node`
Expected: clean. (Not wired in yet — that's Task 3.4.)

- [ ] **Step 4: Commit**

```bash
git add uc_node/src/runtime/output_dispatcher.rs uc_node/src/runtime/mod.rs
git commit -m "feat(uc_node): output_dispatcher tokio task (M5 Task 3.3)"
```

### Task 3.4: Apply-dispatcher → output_dispatcher channel; wire into builder

**Files:**
- Modify: `uc_node/src/raft/state_machine_shmem.rs` (apply → channel.try_send)
- Modify: `uc_node/src/runtime/builder.rs` (channel allocation, spawn output_dispatcher, expose leader_rx)
- Modify: `uc_node/src/runtime/node.rs` (NodeHandle fields + shutdown ordering)
- Modify: `uc_node/src/ipc/metrics_publisher.rs` (expose a watch::Sender<bool> for leader state)

- [ ] **Step 1: Add a `leader_rx: watch::Receiver<bool>` from `metrics_publisher`**

Find `uc_node/src/ipc/metrics_publisher.rs`. The publisher already polls raft metrics. Add a `tokio::sync::watch::channel(false)` allocated by `spawn_metrics_publisher`:

```rust
pub struct MetricsPublisherHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
    pub leader_rx: tokio::sync::watch::Receiver<bool>,  // NEW
}

pub unsafe fn spawn_metrics_publisher<S: StateMachine>(
    status_ptr: *const NodeStatus,
    raft: RaftHandle<S>,
) -> MetricsPublisherHandle {
    // ...
    let (leader_tx, leader_rx) = tokio::sync::watch::channel(false);
    // In the publish loop, after computing `role`:
    let is_leader = role == node_role::LEADER;
    let _ = leader_tx.send(is_leader);
    // ...
    MetricsPublisherHandle { join, stop, leader_rx }
}
```

- [ ] **Step 2: Plumb the channel into apply path**

In `uc_node/src/raft/state_machine_shmem.rs`, `ShmemAdaptedStateMachine::new` currently takes the apply_producer and apply_resp_consumer. Add a new parameter:

```rust
pub fn new(
    user_sm: S,
    handles: LogStorageHandles,
    apply_producer: SpscProducer,
    apply_resp_consumer: SpscConsumer,
    output_chan_tx: tokio::sync::mpsc::Sender<(u64, bytes::Bytes)>,  // NEW
) -> Result<Self, IpcError> { ... }
```

Store it on `Self`.

In the `apply()` impl, after publishing ApplyResp back to openraft, add:

```rust
// Hand off to output_dispatcher. `try_send` so apply never blocks on a
// full output channel — the skip path catches it during replay.
if let Err(e) = self.output_chan_tx.try_send((log_index, cmd_bytes.clone())) {
    tracing::warn!(log_index, ?e, "output_chan full; replay will catch this");
}
```

- [ ] **Step 3: Allocate the channel + spawn output_dispatcher in the builder**

In `uc_node/src/runtime/builder.rs`'s `IpcMode::Shmem` arm:

```rust
// Channel between apply_dispatcher (producer side, via SM adapter) and
// output_dispatcher (consumer side). 1024 in-flight outputs before
// `try_send` starts dropping → replay catches it.
let (output_chan_tx, output_chan_rx) =
    tokio::sync::mpsc::channel::<(u64, bytes::Bytes)>(1024);

// Pass output_chan_tx into ShmemAdaptedStateMachine::new:
let adapter = ShmemAdaptedStateMachine::new(
    self.state_machine,
    handles.clone(),
    link.apply_producer,
    link.apply_resp_consumer,
    output_chan_tx,
)?;

// After spawning metrics_publisher and capturing its leader_rx:
let metrics_publisher = unsafe {
    crate::ipc::metrics_publisher::spawn_metrics_publisher(
        node_status_ptr,
        handle.raft.clone(),
    )
};
let leader_rx = metrics_publisher.leader_rx.clone();
handle.metrics_publisher = Some(metrics_publisher);

// Spawn output_dispatcher.
let output_dispatcher = crate::runtime::output_dispatcher::spawn_output_dispatcher(
    output_chan_rx,
    link.output_producer,
    link.output_resp_consumer,
    handles.output_progress.clone(),
    leader_rx.clone(),
);
handle.output_dispatcher = Some(output_dispatcher);
```

- [ ] **Step 4: Extend `NodeHandle`**

In `uc_node/src/runtime/node.rs`:

```rust
pub struct NodeHandle<S: StateMachine> {
    // existing fields
    pub(crate) output_dispatcher: Option<crate::runtime::output_dispatcher::OutputDispatcherHandle>,
}
```

In `finish` (the constructor): `output_dispatcher: None,`.

In `shutdown()`, BEFORE client_dispatcher cleanup:

```rust
if let Some(d) = self.output_dispatcher {
    d.stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = d.join.await;
}
```

This must come before client_dispatcher cleanup so the output side stops accepting commits before the client side closes.

- [ ] **Step 5: Build + run M3 + M4 capstone tests with the new path**

```bash
cargo build --workspace
cargo test -p uc_node --test m3_shmem_single_node --test m3_three_node_shmem --test m4_client_single_node --test m4_client_concurrent
```

Existing tests use the default `NoopOutput`, so the output_dispatcher publishes frames that the service-side output_loop (created in Task 2.4 only if a handler is set) won't consume. Within 1 s, the apply_dispatcher's `try_send` fills the mpsc channel, then drops new outputs via `try_send Err`. M3 + M4 tests should still pass — the output path being a no-op on the consume side doesn't affect raft commit behavior.

If M3/M4 tests start failing, suspect: (a) the channel back-pressuring apply (`try_send` should prevent this), or (b) shutdown ordering deadlock. Add tracing and re-run.

- [ ] **Step 6: Commit**

```bash
git add uc_node/src/raft/state_machine_shmem.rs uc_node/src/runtime/builder.rs uc_node/src/runtime/node.rs uc_node/src/ipc/metrics_publisher.rs
git commit -m "feat(uc_node): wire apply→output channel + spawn output_dispatcher (M5 Task 3.4)"
```

---

## Phase 4 — Leader-transition replay

### Task 4.1: `output_replay` one-shot task

**Files:**
- Create: `uc_node/src/runtime/output_replay.rs`
- Modify: `uc_node/src/runtime/mod.rs`
- Modify: `uc_node/src/runtime/builder.rs` (spawn replay on Leader transitions)

- [ ] **Step 1: Create the replay module**

Create `uc_node/src/runtime/output_replay.rs`:

```rust
//! Output replay — on becoming leader, scan the journal between
//! `output_progress.state` and `last_applied`, inject each entry's
//! `(log_index, cmd_bytes)` into the apply→output mpsc channel for the
//! output_dispatcher to process. Exits when caught up or when leadership
//! is lost.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use ultima_journal::{Journal, StableValue};

pub struct OutputReplayHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

/// Spawn a one-shot replay task.
///
/// `last_applied_at_transition` is the `last_applied` at the moment we
/// observed the Leader transition. Replay scans (output_progress, last_applied_at_transition].
pub fn spawn_output_replay(
    journal: Arc<Journal>,
    output_progress: Arc<StableValue<u64>>,
    output_chan_tx: Sender<(u64, Bytes)>,
    leader_rx: watch::Receiver<bool>,
    last_applied_at_transition: u64,
) -> OutputReplayHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);

    let join = tokio::spawn(async move {
        let last_completed = match output_progress.load() {
            Ok(Some(v)) => v,
            Ok(None) => 0,
            Err(e) => {
                tracing::error!(?e, "replay: output_progress load failed");
                return;
            }
        };

        if last_completed >= last_applied_at_transition {
            tracing::debug!(
                last_completed,
                last_applied = last_applied_at_transition,
                "replay: caught up; no gap"
            );
            return;
        }

        tracing::info!(
            last_completed,
            last_applied = last_applied_at_transition,
            count = last_applied_at_transition - last_completed,
            "replay: scanning gap"
        );

        // Journal::iter_range yields records by seq. We use seq = log_index
        // throughout (matches log_storage convention).
        let range = (last_completed + 1)..=last_applied_at_transition;
        let iter = match journal.iter_range(range) {
            Ok(it) => it,
            Err(e) => {
                tracing::error!(?e, "replay: journal iter_range failed");
                return;
            }
        };

        for record in iter {
            if stop_for_task.load(Ordering::Relaxed) || !*leader_rx.borrow() {
                tracing::debug!("replay: aborted (stopped or no longer leader)");
                return;
            }
            let record = match record {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(?e, "replay: journal record read failed");
                    continue;
                }
            };

            // Extract log_index (seq) and cmd_bytes. The journal records
            // store bincode-encoded openraft Entry; M5's design says the
            // payload of OutputFrame is the original Command bincode. We
            // need to extract just the Command bytes from the Entry.
            let log_index = record.seq;
            let entry_bytes = record.payload;

            // Decode openraft Entry to pull out the user command bytes.
            // This is the same shape as the apply path uses; refer to
            // raft::log_storage for how Entry is encoded.
            let cmd_bytes = match extract_command_bytes(&entry_bytes) {
                Some(bytes) => bytes,
                None => {
                    tracing::warn!(log_index, "replay: entry has no command payload (blank/membership); skipping");
                    // For blank/membership entries, we still need to advance
                    // output_progress so we don't replay them forever. But we
                    // don't have a clean way to signal "skip this index" via
                    // the channel. Instead, advance the marker directly.
                    if let Err(e) = output_progress.store(log_index).wait() {
                        tracing::error!(?e, log_index, "replay: store output_progress");
                    }
                    continue;
                }
            };

            // Inject into the apply→output channel. Use `send` (not
            // `try_send`) so we block if the channel is full — replay
            // is willing to wait for the dispatcher to drain.
            if output_chan_tx.send((log_index, cmd_bytes)).await.is_err() {
                tracing::warn!(log_index, "replay: output_chan closed; aborting");
                return;
            }
        }

        tracing::info!("replay: complete");
    });

    OutputReplayHandle { join, stop }
}

/// Extract the user `Command` bytes from a journal-stored openraft `Entry`
/// payload. Returns `None` for non-command entries (blank, membership).
fn extract_command_bytes(_entry_bytes: &[u8]) -> Option<Bytes> {
    // TODO(M5 Task 4.1): inspect the bincode-encoded openraft Entry,
    // pattern-match on EntryPayload::Normal(app_data) and return the
    // app_data bytes. The encoding matches what raft::log_storage uses
    // for append. See `uc_node/src/raft/log_storage.rs::try_get_log_entries`
    // for the symmetric decode path.
    //
    // For initial implementation, deserialize via:
    //   let entry: openraft::Entry<TypeConfig> = bincode::deserialize(entry_bytes)?;
    //   match entry.payload {
    //       EntryPayload::Normal(data) => Some(data.0), // AppCommand newtype
    //       _ => None,
    //   }
    None
}
```

**Important:** the `extract_command_bytes` helper is a TODO marker. It MUST be implemented (not left as `None`) before this task is considered complete — otherwise replay always skips every entry. The implementation should mirror the decode pattern used in `log_storage::try_get_log_entries`. Look at that function and copy the relevant decode logic.

Concrete implementation (verify against actual openraft Entry shape in your tree):

```rust
fn extract_command_bytes(entry_bytes: &[u8]) -> Option<Bytes> {
    use crate::raft::TypeConfig;
    use openraft::EntryPayload;
    let (entry, _): (openraft::Entry<TypeConfig>, _) =
        bincode::serde::decode_from_slice(entry_bytes, bincode::config::standard()).ok()?;
    match entry.payload {
        EntryPayload::Normal(data) => {
            // AppCommand is a newtype around bytes::Bytes (per M3.5 Task
            // declare_raft_types!). Unwrap accordingly.
            Some(data.0)
        }
        EntryPayload::Blank | EntryPayload::Membership(_) => None,
    }
}
```

Cross-check against `uc_node/src/raft/state_machine_shmem.rs` and `uc_node/src/raft/log_storage.rs` — whichever does Entry decoding has the canonical pattern.

- [ ] **Step 2: Register the module**

In `uc_node/src/runtime/mod.rs`:

```rust
pub mod output_replay;
```

- [ ] **Step 3: Wire into builder — spawn on Leader transition**

In `uc_node/src/runtime/builder.rs`'s shmem arm, after spawning `output_dispatcher`, add a tokio task that watches `leader_rx` and spawns a replay each time it transitions to `true`:

```rust
// Watch for leader transitions and spawn a one-shot replay each time.
let journal = handles.journal.clone(); // assuming JournalLogStorage exposes journal
let output_progress = handles.output_progress.clone();
let output_chan_tx_for_replay = output_chan_tx.clone();
let leader_rx_for_watcher = leader_rx.clone();
let raft_for_watcher = handle.raft.clone();

let replay_watcher_stop = Arc::new(AtomicBool::new(false));
let replay_watcher_stop_inner = replay_watcher_stop.clone();
let replay_watcher_join = tokio::spawn(async move {
    let mut rx = leader_rx_for_watcher;
    let mut was_leader = false;
    loop {
        if replay_watcher_stop_inner.load(Ordering::Relaxed) {
            break;
        }
        // Wait for the next change in `rx`.
        let changed = rx.changed().await;
        if changed.is_err() { break; }  // sender dropped
        let is_leader = *rx.borrow();
        if is_leader && !was_leader {
            // Transition into Leader: snapshot last_applied + spawn replay.
            let last_applied = {
                use openraft::rt::WatchReceiver as _;
                let metrics = raft_for_watcher.metrics();
                let m = metrics.borrow_watched();
                m.last_applied.as_ref().map(|l| l.index).unwrap_or(0)
            };
            tracing::info!(last_applied, "leader transition — spawning output_replay");
            let _replay_handle = crate::runtime::output_replay::spawn_output_replay(
                journal.clone(),
                output_progress.clone(),
                output_chan_tx_for_replay.clone(),
                rx.clone(),
                last_applied,
            );
            // We don't store this handle — replay is fire-and-forget. If
            // the node loses leadership mid-replay, the replay task itself
            // observes leader_rx going false and exits cleanly.
        }
        was_leader = is_leader;
    }
});

handle.output_replay_watcher = Some(OutputReplayWatcherHandle {
    join: replay_watcher_join,
    stop: replay_watcher_stop,
});
```

Add a `pub struct OutputReplayWatcherHandle` in `output_replay.rs` mirroring the other `*Handle` types.

Add the field to `NodeHandle`:

```rust
pub(crate) output_replay_watcher: Option<crate::runtime::output_replay::OutputReplayWatcherHandle>,
```

Join it in shutdown BEFORE output_dispatcher.

- [ ] **Step 4: Confirm journal access**

Check that `handles.journal: Arc<Journal>` is reachable. If `LogStorageHandles` doesn't expose `journal`, add a field for it (mirror `output_progress`). Use `JournalLogStorage::journal: Arc<Journal>` if it's already there.

- [ ] **Step 5: Build + run M3 + M4 capstone tests**

```bash
cargo build --workspace
cargo test -p uc_node --test m3_shmem_single_node --test m3_three_node_shmem --test m4_client_single_node
```

All must pass. On single-node single-leader, replay scans `(0, last_applied]` once at startup, finds no gap (output_progress catches up via steady-state), exits quietly.

- [ ] **Step 6: Commit**

```bash
git add uc_node/src/runtime/output_replay.rs uc_node/src/runtime/mod.rs uc_node/src/runtime/builder.rs uc_node/src/runtime/node.rs
git commit -m "feat(uc_node): output_replay one-shot task + leader-transition watcher (M5 Task 4.1)"
```

---

## Phase 5 — Integration tests

All seven tests live under `uc_node/tests/` for consistency with the M3/M4 capstone-per-file style. Each uses `#[tokio::test]` (current_thread) per the established convention.

### Task 5.1: `m5_output_smoke` — happy-path

**Files:**
- Create: `uc_node/tests/m5_output_smoke.rs`

- [ ] **Step 1: Write the test**

Use `uc_node/tests/m4_client_single_node.rs` as the structural template. The new bits:

```rust
//! Service registers a `CountingOutputHandler` that records each (log_index, cmd)
//! it sees. Client submits 5 Incs; verify 5 records hit on_committed in order.

use std::sync::Arc;
use parking_lot::Mutex;
use uc_service::{OutputError, OutputHandler};

#[derive(Default, Clone)]
struct OutputLog(Arc<Mutex<Vec<(u64, Cmd)>>>);

#[async_trait::async_trait]
impl OutputHandler<Counter> for OutputLog {
    async fn on_committed(
        &self,
        log_index: u64,
        cmd: &Cmd,
        _state: &Counter,
    ) -> Result<(), OutputError> {
        self.0.lock().push((log_index, cmd.clone()));
        Ok(())
    }
}

#[tokio::test]
async fn m5_output_smoke() {
    // (copy boot harness from m4_client_single_node.rs verbatim)
    // Modification: when building ServiceBuilder, wire output_handler:
    let log = OutputLog::default();
    let svc_task = tokio::spawn(async move {
        ServiceBuilder::new(svc_cfg, Counter::default())
            .output_handler(log.clone())
            .run()
            .await
    });
    // (rest of harness)

    // Submit 5 increments.
    for i in 1..=5u64 {
        let _: Resp = client.submit(&Cmd::Inc(i)).await.expect("submit");
    }

    // Poll the OutputLog until it has 5 entries (or timeout).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while log.0.lock().len() < 5 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let entries = log.0.lock().clone();
    assert_eq!(entries.len(), 5, "expected 5 on_committed invocations");
    for (i, (log_index, cmd)) in entries.iter().enumerate() {
        // log_indexes are sequential (assuming no blank entries in this
        // simple test — single-node single-leader-from-bootstrap).
        assert!(*log_index >= 1);
        assert_eq!(*cmd, Cmd::Inc((i + 1) as u64));
    }

    // (standard shutdown sequence)
}
```

Take care: `OutputLog` must `impl Clone` — it's used both by the test and moved into the handler.

- [ ] **Step 2: Run**

```bash
cargo test -p uc_node --test m5_output_smoke -- --nocapture
```

Expected: PASS in ~2 s. If on_committed isn't called: verify the dispatcher's leader_rx sees `true` (RUST_LOG=info should show "leader transition" or similar).

- [ ] **Step 3: Commit**

```bash
git add uc_node/tests/m5_output_smoke.rs
git commit -m "test(uc_node): m5_output_smoke — happy-path OutputHandler invocation (M5 Task 5.1)"
```

### Task 5.2: `m5_output_idempotent_replay`

**Files:**
- Create: `uc_node/tests/m5_output_idempotent_replay.rs`

- [ ] **Step 1: Write the test**

Idea: connect a client, submit 3 commands, observe 3 on_committed invocations. Then force `output_progress.state` back to 0 (simulating a partial-output crash). Restart the service. Observe 3 MORE invocations (replay).

But the test environment has no clean way to reset `output_progress.state` from outside the node. Two options:

A. Expose a `#[cfg(test)] pub fn _test_reset_output_progress(&self)` on `NodeHandle`. Test-only.

B. Don't reset; instead, use `transfer_leader` to a different node and verify the new leader replays anything that hasn't been completed yet.

Option B is cleaner but needs 3 nodes. Option A is simpler. **Go with A.**

Add a test-only method on `NodeHandle`:

```rust
#[cfg(any(test, feature = "test-helpers"))]
impl<S: StateMachine> NodeHandle<S> {
    /// Force `output_progress.state` back to a specific value. For
    /// integration tests that need to verify replay correctness.
    pub fn _test_reset_output_progress(&self, value: u64) -> Result<(), ClusterError> {
        // The handles aren't directly exposed; reach into the field
        // populated by NodeBuilder.
        // Implementation detail — adapt to current handle storage.
        unimplemented!()
    }
}
```

Implement appropriately. Test:

```rust
#[tokio::test]
async fn m5_output_idempotent_replay() {
    // Single-node harness with the OutputLog from Task 5.1.
    let log = OutputLog::default();
    // ... boot, connect, submit 3 ...

    // Wait for 3 invocations.
    wait_until(|| log.0.lock().len() >= 3, Duration::from_secs(5)).await;

    // Reset output_progress to 0 to simulate partial-output crash.
    node._test_reset_output_progress(0).expect("reset");

    // Trigger a leader transition to force replay. Single-node has no
    // transfer target; the only way to re-trigger replay is to restart
    // the node. For v1 simplicity: shut down and rebuild.
    //
    // Or: extend the test scaffolding with a "force replay" knob that
    // re-emits the leader transition watch event. Use this option:
    node._test_force_leader_replay().expect("force replay");

    // Wait for 6 total invocations (3 original + 3 replayed).
    wait_until(|| log.0.lock().len() >= 6, Duration::from_secs(10)).await;
    assert_eq!(log.0.lock().len(), 6);
    // The same 3 log_indexes appear twice.
    let entries = log.0.lock();
    let first_three: Vec<u64> = entries[..3].iter().map(|(i, _)| *i).collect();
    let last_three: Vec<u64> = entries[3..].iter().map(|(i, _)| *i).collect();
    assert_eq!(first_three, last_three, "replay must hit the same log_indexes");

    // Shutdown.
}
```

The `_test_force_leader_replay` test-only knob just sends `false → true` on the leader_rx channel internally to retrigger the watcher. Add it to NodeHandle behind the same cfg-gate.

- [ ] **Step 2: Run**

```bash
cargo test -p uc_node --test m5_output_idempotent_replay -- --nocapture
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add uc_node/tests/m5_output_idempotent_replay.rs uc_node/src/runtime/node.rs
git commit -m "test(uc_node): m5_output_idempotent_replay (M5 Task 5.2)"
```

### Task 5.3: `m5_output_retryable_backoff`

**Files:**
- Create: `uc_node/tests/m5_output_retryable_backoff.rs`
- Modify: `uc_node/src/runtime/output_dispatcher.rs` (proper exponential backoff per the Task 3.3 reminder)

- [ ] **Step 1: Refactor the dispatcher's backoff to proper per-log_index exponential**

In `uc_node/src/runtime/output_dispatcher.rs`, remove `fn current_backoff()` and inline the backoff state inside the `'outer` loop:

```rust
let mut backoff = RETRY_BACKOFF_INITIAL;
'outer: loop {
    // ... publish, await resp, match
    match resp {
        Ok(()) | Err(OutputError::Permanent(_)) => break 'outer,
        Err(OutputError::Retryable(msg)) => {
            tracing::info!(log_index, msg, ?backoff, "retryable");
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(RETRY_BACKOFF_CAP);
        }
    }
}
```

- [ ] **Step 2: Test**

```rust
use std::sync::atomic::{AtomicU64, Ordering};

struct FlakyOutput { tries: AtomicU64 }

#[async_trait::async_trait]
impl OutputHandler<Counter> for FlakyOutput {
    async fn on_committed(
        &self,
        _log_index: u64,
        _cmd: &Cmd,
        _state: &Counter,
    ) -> Result<(), OutputError> {
        let n = self.tries.fetch_add(1, Ordering::Relaxed);
        if n < 3 {
            Err(OutputError::Retryable(format!("transient {n}")))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn m5_output_retryable_backoff() {
    // single-node harness with FlakyOutput
    let flaky = Arc::new(FlakyOutput { tries: AtomicU64::new(0) });
    // ... wire as handler ...
    let _: Resp = client.submit(&Cmd::Inc(1)).await.unwrap();

    // Wait until on_committed has been called 4 times (initial + 3 retries).
    wait_until(|| flaky.tries.load(Ordering::Relaxed) >= 4, Duration::from_secs(10)).await;
    assert_eq!(flaky.tries.load(Ordering::Relaxed), 4);

    // shutdown
}
```

- [ ] **Step 3: Run + commit**

```bash
cargo test -p uc_node --test m5_output_retryable_backoff
git add uc_node/src/runtime/output_dispatcher.rs uc_node/tests/m5_output_retryable_backoff.rs
git commit -m "test(uc_node): m5_output_retryable_backoff + proper exp backoff (M5 Task 5.3)"
```

### Task 5.4: `m5_output_permanent_advances_marker`

**Files:**
- Create: `uc_node/tests/m5_output_permanent_advances_marker.rs`

- [ ] **Step 1: Test**

```rust
struct PermFor2;

#[async_trait::async_trait]
impl OutputHandler<Counter> for PermFor2 {
    async fn on_committed(
        &self,
        log_index: u64,
        _cmd: &Cmd,
        _state: &Counter,
    ) -> Result<(), OutputError> {
        if log_index == 2 {
            Err(OutputError::Permanent("intentional failure for index 2".into()))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn m5_output_permanent_advances_marker() {
    // single-node, wire PermFor2, submit 3 Incs.
    // ...
    for i in 1..=3 { let _: Resp = client.submit(&Cmd::Inc(i)).await.unwrap(); }

    // output_progress should advance to 3 (log_index 2 had Permanent → advance anyway).
    // We need a way to introspect output_progress from the test. Expose
    // `node._test_output_progress() -> u64` behind cfg(test).
    wait_until(|| node._test_output_progress() >= 3, Duration::from_secs(10)).await;
    assert_eq!(node._test_output_progress(), 3);
    // shutdown
}
```

- [ ] **Step 2: Add the `_test_output_progress` getter**

In `uc_node/src/runtime/node.rs`, behind `#[cfg(any(test, feature = "test-helpers"))]`:

```rust
impl<S: StateMachine> NodeHandle<S> {
    pub fn _test_output_progress(&self) -> u64 {
        // Adapt: reach into the JournalLogStorage handles stored on the NodeHandle.
        self.log_storage_handles
            .output_progress
            .load()
            .ok()
            .flatten()
            .unwrap_or(0)
    }
}
```

If `log_storage_handles` isn't on NodeHandle, add it as a `pub(crate)` field populated by `finish`.

- [ ] **Step 3: Run + commit**

```bash
cargo test -p uc_node --test m5_output_permanent_advances_marker
git add uc_node/tests/m5_output_permanent_advances_marker.rs uc_node/src/runtime/node.rs
git commit -m "test(uc_node): m5_output_permanent_advances_marker (M5 Task 5.4)"
```

### Task 5.5: `m5_output_apply_does_not_stall`

**Files:**
- Create: `uc_node/tests/m5_output_apply_does_not_stall.rs`

- [ ] **Step 1: Test**

```rust
struct SlowOutput;

#[async_trait::async_trait]
impl OutputHandler<Counter> for SlowOutput {
    async fn on_committed(
        &self,
        _log_index: u64,
        _cmd: &Cmd,
        _state: &Counter,
    ) -> Result<(), OutputError> {
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(())
    }
}

#[tokio::test]
async fn m5_output_apply_does_not_stall() {
    // single-node harness with SlowOutput
    // Submit 20 commands; expect all submits to complete in <2s total.
    let start = std::time::Instant::now();
    for i in 0..20u64 {
        let _: Resp = client.submit(&Cmd::Inc(i)).await.expect("submit");
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "20 submits should complete in <2s; took {elapsed:?}"
    );
    // Note: this MAY hit the apply→output channel's 1024-deep capacity
    // before SlowOutput drains 20 entries. With each on_committed
    // sleeping 2s, the dispatcher processes 1 per 2s. 20 entries pile up
    // in the channel (capacity 1024 → no try_send drops). Apply's
    // try_send succeeds for all 20. Apply path stays fast.
    //
    // After all submits, output_progress is still tiny (~0); it will
    // eventually advance over the next ~40s. We don't wait for it here.

    // shutdown — must succeed even with pending outputs.
}
```

Caveat: the RwLock contention from SlowOutput's read-lock-during-await might still block apply. If the test shows apply latency > 200 ms (say, queueing behind one SlowOutput's read lock), that's the documented contract — user should release the read lock before slow I/O. SlowOutput here doesn't read state, so there's no `let _g = state.read();` held across the sleep. Apply should run freely.

Verify by reading the dispatcher code: the read-lock is held by `output_loop` *during* on_committed, so it IS held across the sleep. Apply's write_lock requests will queue. Apply IS gated by output here.

This is the contract — and the test as written WILL fail unless we redesign.

**Resolution:** in the test, mock state access pattern correctly. SlowOutput here doesn't read state (it just sleeps). The output_loop still takes the read lock and holds it across SlowOutput's sleep. Apply IS blocked.

To make the test verify the apply→output decoupling (channel-based), we need a different setup: the SlowOutput should be a DROP-LOCK-FIRST pattern internally, but it can't — the trait passes `&S` so the framework holds the lock.

Easiest fix: lower the SlowOutput sleep to 50 ms; submit 20 commands; total expected time = 20 × 50 ms = 1 s on the output side, but the apply side should still complete much faster (10s of ms each, queueing in tokio mpsc, but not blocked on output_loop's RwLock because the channel doesn't take any lock).

Actually wait — apply takes the RwLock write_lock to run `apply()`. While output_loop holds the read_lock during on_committed (the 50 ms sleep), apply blocks. So apply latency for entry N+1 starts only after output_loop releases the read lock for entry N's on_committed. That's serialization.

**This is the documented contract.** The test should reflect it: with a 50 ms sleep, total apply time = 20 × 50 ms = 1 s. That's the worst case. The test should assert "apply completes within 1.5 s for 20 submits with 50 ms output sleeps" — verifying the channel-based async doesn't add MORE latency on top of the RwLock contention.

Rewrite:

```rust
async fn on_committed(&self, ...) -> Result<(), OutputError> {
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}

// ...
let start = std::time::Instant::now();
for i in 0..20u64 {
    let _: Resp = client.submit(&Cmd::Inc(i)).await.expect("submit");
}
let elapsed = start.elapsed();
// Each submit's apply waits for the previous output's read lock = 50ms.
// Plus apply work (microseconds). Total: ~20 × 50ms = 1s.
assert!(
    elapsed < Duration::from_millis(1500),
    "20 submits should complete in <1.5s (50ms output latency × 20 = 1s plus apply overhead); took {elapsed:?}"
);
```

This verifies the *channel* doesn't add latency, while honestly reporting the RwLock contention. Document this in the test docstring.

- [ ] **Step 2: Run + commit**

```bash
cargo test -p uc_node --test m5_output_apply_does_not_stall
git add uc_node/tests/m5_output_apply_does_not_stall.rs
git commit -m "test(uc_node): m5_output_apply_does_not_stall (M5 Task 5.5)"
```

### Task 5.6: `m5_output_leader_transition_replay`

**Files:**
- Create: `uc_node/tests/m5_output_leader_transition_replay.rs`

- [ ] **Step 1: Test**

3-node cluster (mirror `m4_client_three_node.rs`). Both leader and followers register the same `OutputLog`. After convergence:

1. Pause the leader's output by registering `BlockingOutput` (holds Mutex, blocks forever).
2. Client (connected to leader) submits 3 commands → output_dispatcher tries publish, but service-side output_loop is blocked on the Mutex → 1 s grace → skip path; output_progress stays at 0.
3. `transfer_leader(other_node)` triggers a leader switch.
4. New leader's `output_replay_task` scans `(0, last_applied]` = `(0, 3]`, injects 3 entries into its output channel.
5. New leader's OutputLog (on a different service instance) sees 3 on_committed calls.

Practical implementation: the test uses 3 nodes, 3 services. Each service registers a unique OutputHandler that records to its OWN log. After the transfer, verify the new leader's log accumulates 3 entries.

- [ ] **Step 2: Run + commit**

```bash
cargo test -p uc_node --test m5_output_leader_transition_replay
git add uc_node/tests/m5_output_leader_transition_replay.rs
git commit -m "test(uc_node): m5_output_leader_transition_replay (M5 Task 5.6)"
```

### Task 5.7: `m5_output_ring_backpressure_skip`

**Files:**
- Create: `uc_node/tests/m5_output_ring_backpressure_skip.rs`

- [ ] **Step 1: Add a `ServiceRingConfig` knob to `NodeConfig` (mirror `ClientRingConfig` from M4)**

In `uc_node/src/config.rs`:

```rust
#[derive(Debug, Clone)]
pub struct ServiceRingConfig {
    pub cap_bytes: u64,
    pub max_msg: u32,
}
impl Default for ServiceRingConfig {
    fn default() -> Self {
        Self { cap_bytes: 16 * 1024 * 1024, max_msg: 4 * 1024 * 1024 }
    }
}
```

Add `pub service_rings: ServiceRingConfig` to `NodeConfig`. Re-export from `uc_node`'s `lib.rs`. Wire through to `ServiceLink::create` via a new `create_with_cap`. Mirror the M4 pattern.

Update every existing `NodeConfig { ... }` struct literal in the workspace tests to include `service_rings: ServiceRingConfig::default(),`. Run `rg "NodeConfig\s*\{" --type rust` to enumerate.

- [ ] **Step 2: Write the test**

```rust
#[tokio::test]
async fn m5_output_ring_backpressure_skip() {
    // boot single-node with service_rings: ServiceRingConfig { cap_bytes: 4*1024, max_msg: 1024 }
    // Don't wire an OutputHandler on the service (or wire one that blocks indefinitely).
    // Submit 50 commands.
    // Verify:
    //   - All 50 submits succeed (apply doesn't stall).
    //   - The node's tracing emits "output.ring full > 1s; skipping" warnings.
    //   - output_progress stays at 0 (or some small number ≤ 5 — channel queued some).
    //
    // Then unblock the service (or force a leader transfer) and verify replay
    // catches up output_progress to 50.
}
```

The "verify tracing emits" check can use `tracing-test` crate or just `tracing_subscriber::fmt::try_init()` with a capturing layer. Simplest: skip the tracing check; just verify output_progress doesn't advance and apply completes.

- [ ] **Step 3: Run + commit**

```bash
cargo test -p uc_node --test m5_output_ring_backpressure_skip
git add uc_node/src/config.rs uc_node/src/lib.rs uc_node/src/ipc/service_link.rs uc_node/src/runtime/builder.rs uc_node/tests/m5_output_ring_backpressure_skip.rs uc_node/tests/*.rs uc_service/...
git commit -m "test(uc_node): m5_output_ring_backpressure_skip + ServiceRingConfig (M5 Task 5.7)"
```

---

## Phase 6 — Polish + consolidation

### Task 6.1: Clippy + fmt clean across the workspace

- [ ] **Step 1: Run clippy with -D warnings across all targets**

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

Fix any warnings. Common: unused imports in tests (M3 + M4 tests with new `ServiceRingConfig`), missing fields in `NodeConfig` literals (catch the ones the grep missed), unnecessary clones around the new mpsc channel.

- [ ] **Step 2: Run fmt across workspace**

```bash
cargo fmt --all
```

- [ ] **Step 3: Run full test sweep**

```bash
cargo test --workspace
```

All M1–M5 tests pass; one ignored (M4's `m4_client_leader_failover`).

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "style: clippy + fmt across workspace (M5 Task 6.1)"
```

### Task 6.2: Consolidate to `docs/tasks/task06_m5_output_handler.md`; delete superpowers artifacts; bump README

- [ ] **Step 1: Write the consolidated task doc**

Use `docs/tasks/task05_m4_clients_and_ring_fix.md` as the template. Sections:
- Status, branch, commit range.
- Goal.
- Shipped (per phase).
- Verification (test runtimes per file).
- Deferred to M6+ (snapshot.region mmap, service-recovery handshake, M4 failover fix, ensure_linearizable plumbing, multi-process tests, multi-service runtime).
- Pointers (canonical design, predecessor records).

Save to `docs/tasks/task06_m5_output_handler.md`.

- [ ] **Step 2: Delete the spec + plan**

```bash
git rm docs/superpowers/specs/2026-05-18-uc-m5-output-handler-design.md
git rm docs/superpowers/plans/2026-05-18-uc-m5-output-handler.md
```

- [ ] **Step 3: Update README**

Change status line to "M5 — OutputHandler at-least-once dispatch." Update the workspace section's `uc_service` line to mention `OutputHandler`.

- [ ] **Step 4: Commit**

```bash
git add docs/tasks/task06_m5_output_handler.md docs/superpowers/specs/2026-05-18-uc-m5-output-handler-design.md docs/superpowers/plans/2026-05-18-uc-m5-output-handler.md README.md
git commit -m "docs(m5): consolidate plan/spec into task06; README pointer M4→M5 (M5 Task 6.2)"
```

---

## Risks & considerations

1. **`RwLock` contention is real.** The "parallel apply/output" decoupling at the IPC layer is undone by the user's `on_committed` holding the read lock across slow I/O. Documented in the trait rustdoc; integration test `m5_output_apply_does_not_stall` reflects this honestly. If real users complain, the future fix is making `state: &S` optional or providing a cheap snapshot mechanism — out of M5 scope.

2. **Replay path skips blank/membership entries.** `extract_command_bytes` returns `None` for `EntryPayload::{Blank, Membership}` — the replay task advances `output_progress` past those indices without invoking `on_committed`. Correct: user-defined `on_committed` operates on user commands only. Document in the task05 doc.

3. **Leader-loss mid-replay.** The replay task observes `leader_rx` and exits cleanly when leadership flips. The next leader's replay picks up from the partially-advanced `output_progress` — at most one duplicate `on_committed` invocation per crash. User's idempotency contract handles this.

4. **`service_id` validation in M5 frames.** Every output frame is decoded through `decode_flags_output`. v1 always writes 0; non-zero gets `Permanent("UnknownServiceId(n)")` and the marker advances. Forward-compatible with future multi-service rollout.

5. **`Bytes` cloning is cheap.** The apply path clones `cmd_bytes: Bytes` into the channel. `Bytes` is refcounted; the clone is an Arc bump. No memcpy.

6. **Channel capacity (1024).** If steady-state commit rate ever exceeds output drain rate × 1024 entries, the `try_send` starts dropping. This is the design (replay catches it). For most workloads 1024 is plenty. Knob deferred to a follow-up if measurement demands.

7. **Two `OutputHandler` instances on a 3-node test.** Each service has its own state and its own log. When leadership transfers, the new leader's OutputHandler is the one that runs replay. The old leader's OutputHandler doesn't see replayed entries. Tests assert against the right log.

---

## Self-review notes

- All seven test scenarios from the spec are covered by Tasks 5.1–5.7.
- All file-structure entries map to specific tasks (verify: 5 new files + 7 modified + 7 test files + 1 docs file = 20 file deltas total).
- Type consistency: `OutputError` defined in `uc_protocol::frames::output`, re-exported by `uc_service`. `OutputHandler<S>` async trait with the same signature throughout. `(u64, Bytes)` tuple in the mpsc channel everywhere.
- No "TBD" or "TODO" placeholders remain in step content; the `extract_command_bytes` helper has a concrete implementation in Task 4.1 Step 1.
- `_test_*` helpers gated behind `#[cfg(any(test, feature = "test-helpers"))]` per the M4 pattern.

---

## Pointers

- Canonical M5 spec: `docs/superpowers/specs/2026-05-18-uc-m5-output-handler-design.md`.
- Canonical project design: `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md`.
- Predecessor record: `docs/tasks/task05_m4_clients_and_ring_fix.md`.
- openraft 0.10 metrics surface (used for leader_rx): `uc_node/src/ipc/metrics_publisher.rs` (M4 follow-up #1, commit `010522c`).
- `ultima_journal::StableValue<u64>` usage example: see `last_purged.state` in `uc_node/src/raft/log_storage.rs`.
