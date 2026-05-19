# Task 06 — M5: `OutputHandler` at-least-once dispatch

**Status:** Complete.
**Branch:** `main`, commits `dfb9fcb..723468b` (24 commits).
**Workspace:** `ultima_cluster/`.

## Goal

Ship the leader-only `OutputHandler::on_committed` async path with at-least-once durability:

1. **Steady-state pipeline:** after each successful apply on the leader, the node publishes `OutputFrame{log_index, cmd_bytes}` onto `service/output.ring`; the service's `output_loop` decodes, invokes `on_committed`, and publishes `OutputResp` back; the node advances a durable `output_progress.state` (`StableValue<u64>`).
2. **Leader-transition replay:** on becoming leader, scan `(last_completed, last_applied]` from the journal, re-publish each, drain responses, advance the marker; then transition into steady-state for new commits. `log_index` is the natural idempotency key — users' responsibility to make `on_committed` idempotent.
3. **Output-ring backpressure:** when `output.ring` fills for >1 s, the node skips publication (log warn). Skipped indices are caught on the next replay sweep — `output_progress` only advances after a confirmed `Ok` or `Permanent`. Apply path never stalls on slow `on_committed`.

After M5, users can ship side-effecting business logic (Kafka writes, webhooks, external DB updates) in `on_committed` with at-least-once delivery and idempotency-by-`log_index`. All M1–M4 capstone tests continue to pass unchanged.

## Shipped

### Phase 1 — wire types (commit `66708b7`)

- **`uc_protocol::frames::output`** module with `MSG_TYPE_OUTPUT = 10`, `MSG_TYPE_OUTPUT_RESP = 11`, `FLAGS_SERVICE_ID_MASK = 0x00FF`, `OutputFrameError::UnknownServiceId(u8)`, and the `OutputError { Retryable(String), Permanent(String) }` wire type (serde + thiserror).
- Helpers: `encode_extra_output(u64) -> [u8;8]`, `decode_extra_output`, `encode_flags_output(u8) -> u16`, `decode_flags_output`. 5 unit tests covering codec round-trips, v1 `service_id = 0` contract, `service_id != 0` rejection, `OutputError` serde, and `msg_type` constant stability.

### Phase 2 — service-side trait + loop (commits `1a75405`, `d87cabd`, `1a652d0`, `2aa2b63`)

- **`uc_service::OutputHandler<S>`** async trait (using `async-trait`) with `on_committed(&self, log_index, cmd: &S::Command, state: &S) -> Result<(), OutputError>`. `NoopOutput` default.
- **OutputError unification** — collapsed pre-M5's `uc_service::error::OutputError` (non-serializable) and the new wire type into a single `uc_protocol::frames::output::OutputError` that derives both `serde::{Serialize, Deserialize}` and `thiserror::Error`. Preserves the `is_retryable()` helper.
- **`apply_loop` rewire** — `Arc<parking_lot::Mutex<S>>` → `Arc<tokio::sync::RwLock<S>>` (apply uses `blocking_write()`; output_loop and query_loop use `read().await`). `tokio::sync::RwLock`'s read guard is `Send`, so output_loop can hold it across the `on_committed` `.await`. `query_loop` benefits: query is `&self`, now uses the shared read lock instead of an exclusive mutex.
- **`output_loop`** tokio task in `uc_service::runtime::output_loop`: consumes `OutputFrame` on `service/output.ring`, validates `service_id`, decodes `cmd`, takes a read lock on the shared `Arc<RwLock<S>>`, invokes `on_committed`, publishes `OutputResp` carrying the user's `Result<(), OutputError>`. Type-erased through `ErasedOutputHandler<S>` so `ServiceBuilder<S>` doesn't need a second generic param for the handler.
- **`ServiceBuilder::output_handler<O: OutputHandler<S>>(handler)`** now actually stores the handler. `run()` spawns `output_loop` only when wired (Noop omits it). `Service::shutdown` stops `output_loop` **before** `apply_loop` so the read lock releases before apply's write lock is requested.

**State-borrow contract** (documented in `uc_service::OutputHandler` rustdoc): the framework holds a read lock on `S` for the duration of `on_committed`. While held, `apply` is blocked. Users must extract data synchronously from `state` and drop the borrow before any slow `.await`:

```rust
async fn on_committed(&self, _: u64, cmd: &Cmd, state: &Counter) -> Result<(), OutputError> {
    let snapshot = state.get_relevant_data(cmd); // borrow ends here
    kafka_client.send(snapshot).await
        .map_err(|e| OutputError::Retryable(e.to_string()))?;
    Ok(())
}
```

### Phase 3 — node-side infrastructure (commits `9f9c6e9`, `436a68c`, `76e51e1`, `b76aa26`, `b81384b`)

- **Two new SPSC rings** on `ServiceLink`: `service/output.ring` (node → service) and `service/output_resp.ring` (service → node). `ServiceLink::create` extended; new `create_with_output_cap(...)` lets callers configure the cap.
- **`output_progress: Arc<StableValue<u64>>`** added to `JournalLogStorage` + `LogStorageHandles`. `Durability::Consistent` per-record. `runtime::recovery::assert_consistent` extended with `output_progress <= last_applied` invariant check.
- **`output_dispatcher`** tokio task in `uc_node::runtime::output_dispatcher`. Receives `(u64, Bytes)` from a `tokio::sync::mpsc::Receiver`; publishes `OutputFrame` on `service/output.ring` with a 1 s grace, then skips on persistent Full; awaits `OutputResp`; on `Ok`/`Permanent` advances `output_progress` durably (`store(idx).wait()`); on `Retryable` does exponential backoff (10 ms → 1 s cap) and retries while still leader. Aborts cleanly on leadership loss or shutdown (`stop` flag honored in `await_output_resp` to prevent the 30 s timeout from gating shutdown).
- **Apply→output mpsc channel** (cap 1024) plumbed through `ShmemAdaptedStateMachine::new` so the node-side apply path `try_send`s `(log_index, cmd_bytes.clone())` after `await_apply_resp`. `try_send` failure logs a warn and continues — replay catches the gap.
- **`metrics_publisher`** now exposes `leader_rx: watch::Receiver<bool>` and `leader_tx: Arc<watch::Sender<bool>>` for the output dispatcher's leader gating and the test-only `_test_set_leader_state` helper.
- **`NodeHandle::shutdown`** ordering: stop `output_replay_watcher` → `output_dispatcher` → existing client/service cleanup → `raft.shutdown` → explicit `drop(sm)` to close the channel sender so `output_dispatcher` exits `rx.recv()` cleanly.

### Phase 4 — leader-transition replay (commit `9899d0f`)

- **`output_replay`** module in `uc_node::runtime::output_replay`:
  - `spawn_output_replay_watcher(journal, output_progress, output_chan_tx, leader_rx, last_applied_provider)` runs a tokio task subscribed to `leader_rx`. Each `false → true` transition snapshots `last_applied` (via the provider closure that reads raft metrics) and fires `spawn_one_shot_replay`.
  - The one-shot scans `journal.iter_range((last_completed + 1)..=last_applied_at_transition)`, decodes each entry via `bincode::serde::decode_from_slice::<<TypeConfig as RaftTypeConfig>::Entry, _>`, pattern-matches on `EntryPayload`:
    - `Normal(AppCommand(bytes))` → `output_chan_tx.send((seq, bytes)).await` (blocking, so replay backpressures naturally against the dispatcher).
    - `Blank | Membership(_)` → advance `output_progress` past the index without dispatch.
  - Exits when caught up or when `leader_rx.borrow()` flips to `false` mid-scan.
- Wired into the builder shmem arm after `output_dispatcher` spawn.

### Phase 5 — integration tests (commits `1f974aa`, `93497df..6589dcf`, `6fb67de..e39f8a4`, `f163bbb..9a1be05`)

Seven tests under `uc_node/tests/m5_output_*.rs` plus three test-only helpers on `NodeHandle` (`93497df`, `6fb67de`):

| Test | Validates | Runtime |
|---|---|---|
| `m5_output_smoke` | 1 node + 1 service + 1 client. CountingOutputHandler records each commit. 5 `Inc`s → 5 invocations, monotonic log_index. | ~2.2 s |
| `m5_output_idempotent_replay` | Submit 3, reset `output_progress` to 0 via `_test_reset_output_progress`, force a `false→true` leader transition via `_test_set_leader_state`, verify 6 total invocations with the same log_indexes (idempotency contract). | ~2.2 s |
| `m5_output_retryable_backoff` | FlakyOutput returns `Retryable` for 3 attempts then `Ok`. Submit one command, verify exactly 4 invocations (initial + 3 retries). | ~2.2 s |
| `m5_output_permanent_advances_marker` | `Permanent` at `log_index = 2` advances `output_progress` past 2 anyway. Submit 3, verify `_test_output_progress() >= 3`. | ~2.2 s |
| `m5_output_apply_does_not_stall` | SlowOutput sleeps 50 ms per commit. 20 submits complete in <3 s (~1 s lock-induced floor, no extra channel backpressure). | ~2.2 s |
| `m5_output_leader_transition_replay` | 3-node cluster. Submit 3 to leader. `_test_transfer_leader(other_node)`. New leader's OutputLog (different service instance) sees 3 replayed invocations via the leader-transition watcher. | ~10.5 s |
| `m5_output_ring_backpressure_skip` | 4 KiB `output.ring` + no service-side `output_handler` wired. 50 submits all succeed in <5 s; `output_progress` stays small (< 10); shutdown clean. | ~4.2 s |

**Test-only helpers on `NodeHandle`** (behind `#[cfg(any(test, feature = "test-helpers"))]`):
- `_test_output_progress() -> u64` — read the marker.
- `_test_reset_output_progress(u64)` — force the marker (durable).
- `_test_set_leader_state(bool) -> bool` — emit a single value on `leader_tx` (Yields between sends in the test, because watch coalesces).
- `_test_transfer_leader(target)` — public wrapper over `RaftHandle::transfer_leader`.

### Phase 6 — polish (commit `723468b`)

- `cargo fmt --all` clean across 9 changed files.
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
- `cargo test --workspace` — all M1–M5 tests pass; one ignored (`m4_client_leader_failover` — pre-existing M4 follow-up unrelated to M5).

## Wire-up summary (final)

```
[node — apply_dispatcher / SM adapter]
    on raft.apply(entry):
       ApplyFrame    → service/apply.ring       (existing)
       await ApplyResp                            (existing)
       try_send (log_index, cmd_bytes)
            → output_chan (tokio mpsc, cap 1024)  (NEW)
       return Response                            (existing)

[node — output_dispatcher tokio task] (NEW)
    while leader:
       recv (log_index, cmd_bytes) from output_chan
       OutputFrame → service/output.ring          (1 s grace; Skip on persistent Full)
       await OutputResp from service/output_resp.ring  (stop-flag aware)
       Ok | Permanent  → output_progress.store(idx).wait()
       Retryable       → exp backoff 10ms→1s, retry while still leader

[node — output_replay_watcher tokio task] (NEW)
    on leader_rx `false → true`:
       snapshot last_applied
       spawn one-shot replay:
            iter_range (last_completed + 1 ..= last_applied):
                Normal     → output_chan_tx.send((seq, cmd_bytes)).await
                Blank/Memb → advance output_progress past seq

[service — output_loop tokio task] (NEW; only when handler is wired)
    consume service/output.ring:
       state.read().await:                       (Arc<tokio::sync::RwLock<S>>)
          handler.on_committed(log_index, &cmd, &state).await
       publish OutputResp on service/output_resp.ring

[service — apply_loop] (unchanged shape; lock type swapped)
    state.blocking_write() → user_sm.apply(idx, cmd)
```

## Verification

All commands green at M5 close:

```bash
cargo build --workspace                                    # clean
cargo test  --workspace                                    # all M1/M2/M3/M3.5/M4/M5 pass; 1 ignored (M4 failover)
cargo clippy --workspace --all-targets -- -D warnings      # zero warnings
cargo fmt --check                                          # clean
```

Per-test runtimes (loopback single-machine):

```
m5_output_smoke                       ~ 2.2 s
m5_output_idempotent_replay           ~ 2.2 s
m5_output_retryable_backoff           ~ 2.2 s
m5_output_permanent_advances_marker   ~ 2.2 s
m5_output_apply_does_not_stall        ~ 2.2 s
m5_output_leader_transition_replay    ~ 10.5 s
m5_output_ring_backpressure_skip      ~ 4.2 s
```

## Deferred to M6+

- **`snapshot.region` mmap + openraft V2 `generic-snapshot-data`.** Snapshots still ride through the existing apply/journal path; the dedicated mmap region awaits the openraft cutover.
- **Service-recovery handshake** (cnc-sub-mmap MPSC attach for `ServiceReady{last_applied}` frames). M3.5 follow-up #1.
- **M4 failover test fix** (auto-remove unreachable voters after N failed AppendEntries). M4 follow-up; `m4_client_leader_failover` remains `#[ignore]`.
- **`raft.ensure_linearizable()` plumbing** on the query path. Two-line follow-up; defer until a test demands stricter semantics.
- **Multi-process integration tests.** All M5 tests run in-process tokio tasks; the protocol works identically across process boundaries; cross-process harness is M6+.
- **Multi-service runtime.** Protocol shape (`service_id` in `flags`, services-table in `cnc.dat`) was reserved in M4. The runtime — per-service dispatchers, per-service `StableValue`s, snapshot demux — is the multi-month feature tracked separately.
- **Client SDK auto-routing across hosts** — v1 surfaces `NotLeader { hint }` and lets callers decide. A `MultiHostClient` helper could come later if real callers ask for it.

## Pointers

- Canonical project design: `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` (§"OutputHandler" line 253, §"Output pipeline" line ~680).
- Predecessor record: `docs/tasks/task05_m4_clients_and_ring_fix.md`.
- M4 follow-up #1 (metrics publisher, foundation for `leader_rx`): `uc_node/src/ipc/metrics_publisher.rs` (commit `010522c` from M4).
- `ultima_journal::StableValue<u64>` usage example: `last_purged.state` in `uc_node/src/raft/log_storage.rs`.
