# M5 design — `OutputHandler` at-least-once dispatch

**Status:** design (brainstormed 2026-05-18, awaiting plan).
**Predecessors:** M4 (`docs/tasks/task05_m4_clients_and_ring_fix.md`) — `uc_client` SDK + ring wrap-fix; M3.5 (`task04`) — openraft 0.10 + `transfer_leader`; M3 (`task03`) — shmem IPC + `uc_service` process split.
**Workspace:** `ultima_cluster/`.

## Goal

Ship the leader-only `OutputHandler::on_committed` async path with at-least-once durability:

1. **Steady-state pipeline:** after each successful apply on the leader, the node publishes `OutputFrame{log_index, cmd_bytes}` onto a new `service/output.ring`; the service's `output_loop` decodes, invokes `on_committed`, and publishes `OutputResp` back; the node advances a durable `output_progress.state` marker (`StableValue<u64>`).
2. **Leader-transition replay:** on becoming leader, scan `(last_completed, last_applied]` from the journal, re-publish each, drain responses, advance the marker; *then* transition into steady-state for new commits. `log_index` is the natural idempotency key — users' responsibility to make `on_committed` idempotent.
3. **Output ring backpressure:** when `output.ring` fills past a 1 s grace period, the node *skips publication* for that index (log warn). The skipped indices are caught on the next replay sweep — output_progress only advances after a confirmed `Ok`. Apply path never stalls on slow `on_committed`.

After M5, users can ship side-effecting business logic (Kafka writes, webhooks, external DB updates) in `on_committed` with at-least-once delivery and idempotency-by-log-index. All M1–M4 capstone tests continue to pass unchanged.

## Out of scope

- **Multi-service runtime.** Protocol shape was already reserved in M4 (`service_id` in `flags` byte 0, services-table in cnc.dat). M5 still targets one `uc_service` per cluster; the shape just round-trips.
- **`snapshot.region` mmap + openraft V2 `generic-snapshot-data` cutover.** Tracked separately; M6+.
- **Service-recovery handshake** (cnc-sub-mmap MPSC attach so the node consumes `ServiceReady{last_applied}` from a re-attaching service). M3.5 follow-up #1. Defer.
- **M4 failover test fix** (auto-remove unreachable voters). M4 follow-up; orthogonal.
- **`raft.ensure_linearizable()` on the query path.** Two-line plumb-through follow-up; defer.
- **Multi-process integration tests.** All M5 tests run in-process tokio tasks; the protocol works identically across process boundaries.

## Top-level decisions

| Decision | Choice | Why |
|---|---|---|
| Scope | **Steady-state + leader-transition replay** | Replay is what makes at-least-once *credible*. Without it, in-flight outputs at the moment of a leader flip get silently dropped. |
| `cmd: &S::Command` plumbing | **Node ships `cmd_bytes` in `OutputFrame`** | One uniform code path for steady-state and replay (replay reads `cmd_bytes` straight from the journal). Service decodes once per output. Bandwidth cost is real only for huge commands; for typical KV writes it's noise. `Bytes` is refcounted so intra-process re-publish is cheap. |
| State sharing | **`Arc<RwLock<S>>` between apply_loop and output_loop** | Matches canonical trait `on_committed(&self, log_index, cmd, state: &S)`. apply_loop takes write lock; output_loop takes read lock for the duration of `on_committed`. Documented contract: `on_committed` must extract what it needs and drop the borrow before slow I/O. (`parking_lot::RwLock` — sync; output_loop's tokio task blocks briefly while holding read lock.) |
| Apply→output ordering | **Parallel** | apply_dispatcher publishes `ApplyResp` back to openraft immediately; the output path runs asynchronously off a node-internal `tokio::sync::mpsc` channel. `client_write` returns when the entry commits, *not* when its output completes. Raft commit latency never depends on user I/O speed. |
| Output ring backpressure | **Bounded wait then skip; replay catches it** | apply_dispatcher tries to publish `OutputFrame` with a 1 s grace; on persistent `Full`, logs warn and skips. `output_progress.state` never advances past the gap. Next leader transition (or this leader's idle-sweep) replays via the journal-scan path. Net: at-least-once preserved; raft commits never stall. |
| Output progress durability | **Per-record `.wait()`** | After every successful `on_committed`, output_dispatcher does `output_progress.state.store(log_index).wait()` before processing the next index. `StableValue`'s rotating two-slot atomic write is microseconds. Crash window: at most one redundant `on_committed` invocation (already idempotent contract). |
| `OutputResp` payload encoding | **bincode `Result<(), OutputError>`** where `OutputError ∈ { Retryable(String), Permanent(String) }` | Mirrors the canonical trait error model exactly. Wire-format trivial. |
| Retry semantics for `Retryable` | **Exponential backoff (10 ms → 1 s cap), forever-while-leader** | On loss of leadership, the retry loop exits without advancing `output_progress.state`; the new leader picks up via replay. No max-retry knob in v1 — `Retryable` means the user has confidence it's transient; `Permanent` is for "give up." |

## Phase 1 — protocol additions (`uc_protocol`)

### New frame types in `uc_protocol::frames::output`

| `msg_type` | Frame | Ring | Direction |
|---|---|---|---|
| 10 | `OutputFrame { log_index → header_extra; service_id → flags low byte; cmd_bytes → payload }` | `service/output.ring` (SPSC) | node → service |
| 11 | `OutputResp { log_index → header_extra; service_id → flags low byte; bincode<Result<(),OutputError>> → payload }` | `service/output_resp.ring` (SPSC) | service → node |

Helpers: `encode_extra_output(log_index: u64) -> [u8; 8]` (LE), matching `decode_*`. `encode_flags_output(service_id: u8) -> u16` + `decode_flags_output(flags) -> Result<u8, OutputFrameError>` with `OutputFrameError::UnknownServiceId(u8)` — mirror of the apply/query retrofits in M4 Task 2.3.

### `output_progress.state` StableValue

Already named in `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md:477`. Implement as `ultima_journal::StableValue<u64>` next to the existing `vote.state`, `committed.state`, `last_purged.state`, `membership.state` files under `<data_dir>/state/`. Stored value is the `log_index` of the last completed (`Ok`) `on_committed`. Default = 0 (no outputs completed yet; replay scans from 1).

### `OutputError` in `uc_protocol`

```rust
// uc_protocol/src/frames/output.rs
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum OutputError {
    /// Retry while still leader. Backoff bounded by output_dispatcher.
    Retryable(String),
    /// Log warn, advance output_progress anyway, move on.
    Permanent(String),
}
```

Re-exported by `uc_service` for users.

## Phase 2 — service-side `output_loop` (`uc_service`)

### `OutputHandler` trait

```rust
// uc_service/src/output.rs
#[async_trait::async_trait]
pub trait OutputHandler<S: StateMachine>: Send + Sync + 'static {
    async fn on_committed(
        &self,
        log_index: u64,
        cmd: &S::Command,
        state: &S,
    ) -> Result<(), OutputError>;
}

pub struct NoopOutput;
#[async_trait::async_trait]
impl<S: StateMachine> OutputHandler<S> for NoopOutput {
    async fn on_committed(&self, _: u64, _: &S::Command, _: &S) -> Result<(), OutputError> { Ok(()) }
}
```

`async_trait` is already a workspace dep (used elsewhere); confirm and add if missing.

### Shared state — `Arc<RwLock<S>>`

`uc_service::ServiceBuilder<S>` already owns the user's `S`. M5 wraps it in `Arc<parking_lot::RwLock<S>>` shared between:
- `apply_loop` (sync `std::thread`): takes `write()` for the duration of `apply(log_index, cmd)`. Releases on return. Microseconds in the typical case (deterministic, no I/O — the canonical apply contract).
- `output_loop` (tokio task): takes `read()` while invoking `on_committed`. Held across the user's `.await` points.

**Contract (documented in the `OutputHandler` rustdoc):**

> `on_committed` holds a read borrow of the service's state for its entire duration. While held, the service's `apply` path is blocked. To keep apply latency stable, extract what you need from `state` synchronously and drop the borrow before any `.await` on slow I/O. Pattern:
>
> ```rust
> let snapshot = {
>     let payload = state.get_relevant_data(cmd);
>     payload  // borrow ends here
> };
> kafka_client.send(snapshot).await?;
> ```

### `output_loop` task

```text
loop {
    consume one OutputFrame from service/output.ring
    decode log_index, service_id, cmd_bytes
    if service_id != 0: publish OutputResp{Err(Permanent("unknown service_id"))}; continue
    cmd = bincode::decode(cmd_bytes)
    let result = {
        let state_guard = state.read();
        output_handler.on_committed(log_index, &cmd, &*state_guard).await
    };
    publish OutputResp{log_index, bincode(result)} on service/output_resp.ring
}
```

No retry on the service side — retry is the node's responsibility (gated on leadership).

### Public API surface on `ServiceBuilder`

```rust
impl<S: StateMachine> ServiceBuilder<S> {
    pub fn output_handler<O: OutputHandler<S>>(self, h: O) -> Self;
}
```

Default: `NoopOutput`. If absent, no `output_loop` is spawned and the service-side `service/output.ring` consumer is never drained — but the node's perspective is unchanged because no OutputFrames are published either (see Phase 3: the node only publishes if the user opted-in node-side, mirrored via cnc handshake or config knob).

**Node-side awareness of the service's OutputHandler:** node always publishes. If the service didn't wire up an `output_loop` (`NoopOutput` or omitted entirely), frames pile up on `service/output.ring` until the apply_dispatcher's 1 s grace expires, then they're skipped per the backpressure policy. Effectively a no-op for users that don't care about output. If measurement later shows the skip-overhead matters, a `has_output_handler` flag in `ServiceStatus` cnc can be added in a follow-up — but the cost is at most one ring write per commit that gets paid back as a dropped frame within ~1 s.

Note: the service spawns `output_loop` only when a non-`NoopOutput` handler is wired in. With `NoopOutput`, `service/output.ring` is created (the node opens its producer half) but no consumer is attached service-side. The skip behavior kicks in within seconds.

## Phase 3 — node-side steady-state output (`uc_node`)

### `output_progress: StableValue<u64>` on the node

Add to `JournalLogStorage` / `log_storage::handles` alongside the existing StableValues. Per-record `.wait()` on advance. Recovery: read at node start; default 0 if absent. `assert_consistent` (`runtime/recovery.rs`) extended to verify `output_progress.state <= last_applied`.

### Apply→output pipeline

```text
[apply_dispatcher — existing]
    on raft.apply(entry):
        publish ApplyFrame{log_index, cmd_bytes}
        await ApplyResp
        return Response to openraft
        ALSO: send (log_index, cmd_bytes) on tokio::sync::mpsc::Sender<(u64, Bytes)>
              → output_dispatcher

[output_dispatcher — NEW tokio task]
    recv (log_index, cmd_bytes) from the channel:
        if !is_leader(): drop (replay handles non-leader-side)
        publish OutputFrame{log_index, cmd_bytes} on service/output.ring
            on Full: bounded 1s grace, then skip + warn-log
        await OutputResp from service/output_resp.ring (with timeout)
        match payload:
            Ok(()):              output_progress.state.store(log_index).wait()
            Err(Retryable(msg)): exponential backoff (10ms→1s cap), republish,
                                 abort if no-longer-leader
            Err(Permanent(msg)): warn-log; output_progress.state.store(log_index).wait()
        next
```

The channel between apply_dispatcher and output_dispatcher is bounded (`tokio::sync::mpsc::channel(N)` for some N, default 1024). When full: apply_dispatcher uses `try_send` — same skip-then-replay behavior. apply_dispatcher itself never blocks on output.

### Leadership transitions

A `tokio::watch::Receiver` derived from `raft.metrics()` lets output_dispatcher learn about role changes. On `state != Leader`, drain the channel and idle. On `state → Leader`, spawn replay (Phase 4); replay drives the channel itself until it catches up to `last_applied`, then steady-state takes over. The watch channel + the apply-output mpsc together fully control which mode output_dispatcher is in.

## Phase 4 — leader-transition replay

When the node transitions to Leader, the metrics-publisher fires (already exists from M4 follow-up #1). A new `output_replay_task`:

1. Reads `last_completed = output_progress.state.load()`.
2. Reads `last_applied` from raft metrics at moment-of-transition.
3. For `index in (last_completed, last_applied]`:
   - Read `cmd_bytes` from journal (`JournalLogStorage::read_record(index)`).
   - Publish `OutputFrame{log_index: index, cmd_bytes}` on `service/output.ring`.
   - Await `OutputResp`, advance `output_progress.state` per the steady-state policy.
4. Once caught up, signal output_dispatcher to enter steady-state mode (the mpsc channel from apply_dispatcher takes over for log indices `> last_applied_at_transition`).

Replay runs as a one-shot task per leadership-acquisition. If the node loses leadership mid-replay, the task aborts; the next leader's replay picks up from wherever `output_progress.state` got to (per-record durability means at most one duplicate).

**Edge case:** at moment of leadership acquisition, `last_applied` may still be advancing as the node catches up post-election. Replay reads `last_applied` once at the start. New commits (`> last_applied_at_transition`) flow through the apply→output channel naturally — there's no race because both replay and steady-state advance the same marker monotonically and the channel is processed in arrival order.

## Phase 5 — integration tests (`uc_node/tests/m5_output_*`)

All in-process tokio tasks, matching the M3/M4 capstone style.

1. **`m5_output_smoke`** — 1 node + 1 service + 1 client. Service registers a counting OutputHandler that records every (log_index, cmd) it sees. Client submits 5 `Inc`s. Verify all 5 hit on_committed with monotonic log_index. Final `output_progress.state` == 5. ~2 s.

2. **`m5_output_idempotent_replay`** — 1 node + 1 service. Submit 3 commands. Force `output_progress.state.store(0)` (simulate output progress reset). Restart the service (drop + re-create with the same OutputHandler instance). Verify on_committed runs 3 more times — the user's recorder sees 6 entries (3 original + 3 replayed) with the same log_indexes. Validates the idempotency contract. ~3 s.

3. **`m5_output_retryable_backoff`** — OutputHandler returns `Retryable` for the first 3 calls per log_index, then `Ok`. Submit one command. Verify on_committed is invoked 4× for that log_index (initial + 3 retries), output_progress.state advances exactly once at the end. ~2 s.

4. **`m5_output_permanent_advances_marker`** — OutputHandler returns `Permanent` for log_index 2 (only). Submit 3 commands. Verify output_progress.state == 3 at the end (Permanent advanced past index 2). Warn-log present for index 2. ~2 s.

5. **`m5_output_apply_does_not_stall`** — OutputHandler sleeps 2 s on every on_committed (simulates slow Kafka). Submit 20 commands. Verify all 20 client_write calls return within 1 s total (apply path uncoupled from output). output_progress.state lags last_applied but no client_write blocks. ~3 s.

6. **`m5_output_leader_transition_replay`** — 3-node cluster. Force leadership transfer (via `raft.transfer_leader`) after the leader's output_progress falls behind last_applied by some entries. Verify the new leader replays the gap on its OutputHandler. ~10 s.

7. **`m5_output_ring_backpressure_skip`** — Configure tiny `output.ring` (4 KiB). Block the service's output_loop. Submit 100 commands. Verify apply_dispatcher's skip-and-warn fires after the grace period; output_progress lags; commits keep flowing. Unblock output_loop and trigger a manual leader replay (transfer + re-acquire) to verify the gaps catch up. ~5 s.

## Wire-up summary

```
[node — apply_dispatcher]
    raft.apply(entry):
       ApplyFrame → service/apply.ring             (existing)
       await ApplyResp                              (existing)
       (log_index, cmd_bytes) → output_chan        (NEW, tokio mpsc)
       return Response                              (existing)

[node — output_dispatcher tokio task] (NEW)
    recv from output_chan OR replay_chan:
       OutputFrame → service/output.ring           (NEW ring)
       await OutputResp                             (NEW ring)
       output_progress.state.store(idx).wait()     (NEW StableValue)

[node — output_replay_task] (NEW, spawned on becoming leader)
    scan journal (last_completed, last_applied]:
       inject (log_index, cmd_bytes) → replay_chan

[service — output_loop tokio task] (NEW)
    consume service/output.ring:
       on_committed(log_index, &cmd, &state).await
       publish OutputResp                          (NEW)

[service — apply_loop, existing]
    write-lock state, apply, release, publish ApplyResp.

[service — state] (NEW Arc<parking_lot::RwLock<S>>)
    apply: write_lock for ~apply duration
    output: read_lock for ~on_committed duration
```

## File structure

### New files

| File | Responsibility |
|---|---|
| `uc_protocol/src/frames/output.rs` | `MSG_TYPE_OUTPUT`, `MSG_TYPE_OUTPUT_RESP`, `OutputError`, `encode/decode_extra_output`, `encode/decode_flags_output`. |
| `uc_service/src/output.rs` | `OutputHandler` trait, `NoopOutput`, `OutputError` re-export. |
| `uc_service/src/runtime/output_loop.rs` | The tokio task consuming `service/output.ring` and publishing `OutputResp`. |
| (no new file) | The two new SPSC rings (`service/output.ring`, `service/output_resp.ring`) are added to the existing `uc_node/src/ipc/service_link.rs::ServiceLink::create` and to the service side's symmetric attach. No new `output_link.rs` module — `ServiceLink` already owns the apply/query rings and the new pair fits the same lifecycle. |
| `uc_node/src/runtime/output_dispatcher.rs` | The tokio task processing `output_chan` and `replay_chan`; manages backoff, leadership-aware skip, marker advance. |
| `uc_node/src/runtime/output_replay.rs` | One-shot task spawned on leader-acquisition; scans journal, feeds `replay_chan`. |
| `uc_node/src/raft/output_progress.rs` (or extended `log_storage`) | `StableValue<u64>` for output_progress.state; recovery integration. |
| `uc_node/tests/m5_output_*.rs` | Seven integration tests. |
| `docs/tasks/task06_m5_output_handler.md` | Final consolidated record. |

### Modified files

| File | Change |
|---|---|
| `uc_protocol/src/frames/mod.rs` | `pub mod output;` |
| `uc_service/src/runtime/apply_loop.rs` | Wrap `S` in `Arc<RwLock<S>>`. apply takes write_lock. |
| `uc_service/src/runtime/service.rs` | `ServiceBuilder::output_handler` method; spawn `output_loop` if not `NoopOutput`. Wire the shared `Arc<RwLock<S>>`. |
| `uc_service/src/lib.rs` | Re-export `OutputHandler`, `NoopOutput`, `OutputError`. |
| `uc_node/src/raft/log_storage.rs` | Add `output_progress: StableValue<u64>` to handles. |
| `uc_node/src/runtime/builder.rs` | Open `output_progress`; create the new output rings on the shmem side; spawn `output_dispatcher` and (on Leader transitions) `output_replay`. |
| `uc_node/src/runtime/node.rs` | `NodeHandle` gains `output_dispatcher: Option<…Handle>`. `shutdown` joins it. |
| `uc_node/src/runtime/recovery.rs` | `assert_consistent` checks `output_progress.state <= last_applied`. |
| `uc_node/src/ipc/service_link.rs` | Create the two new ring files (`output.ring`, `output_resp.ring`) under `service/`. |

## Error model additions

`uc_protocol::frames::output::OutputFrameError::UnknownServiceId(u8)` — uniform with M4's apply/query retrofits.

`uc_service::OutputError::{Retryable, Permanent}` — already defined in the design spec.

`uc_node` internal error in the output_dispatcher (e.g., `output_resp.ring` decode failure, journal read failure during replay) gets `tracing::warn!`'d and the dispatcher continues. No surface to client errors — output is leader-only and asynchronous.

## Testing strategy

- **Unit tests in `uc_protocol::frames::output`** — round-trip the wire codec.
- **Unit tests in `uc_node::runtime::output_dispatcher`** — backoff cadence, leader-transition abort, marker advance on Ok/Retryable→Ok/Permanent.
- **Integration tests** (Phase 5) — seven `m5_output_*.rs` files under `uc_node/tests/`.

All seven tests use `#[tokio::test]` (current_thread) per the M3 convention recorded in `feedback_m3_test_runtime_flavor`.

## Implementation phasing

| Phase | Scope | Commits (est.) |
|---|---|---|
| 1 | `uc_protocol::frames::output` + helpers + tests. `output_progress.state` StableValue plumbing. | 2 |
| 2 | `uc_service::OutputHandler` trait + `output_loop` + Arc<RwLock<S>> rewire in service. | 3 |
| 3 | `uc_node` output_dispatcher + apply→output channel + StableValue advance. Steady-state pipeline working with retries. | 3-4 |
| 4 | `output_replay_task` + leadership-transition wiring. | 2 |
| 5 | Seven integration tests. | 5-7 |
| 6 | Polish: clippy/fmt; consolidate plan into `docs/tasks/task06_m5_output_handler.md`; delete plan + spec; README pointer M4 → M5. | 2 |

Total: ~17-20 commits.

## Pointers

- Canonical design: `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` (§"OutputHandler" line 253; §"Output pipeline" line ~680).
- M4 record (immediate predecessor): `docs/tasks/task05_m4_clients_and_ring_fix.md`.
- `ultima_journal::StableValue` usage pattern: `uc_node/src/raft/log_storage.rs` (vote, committed, last_purged, membership all follow the same shape).
- openraft 0.10 metrics surface (already plumbed via M4's `metrics_publisher`): `uc_node/src/ipc/metrics_publisher.rs`.
