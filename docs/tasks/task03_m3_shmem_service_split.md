# Task 03 — M3: Shmem Ring Buffers + Service Process Split

**Status:** Complete. Phase 1 = `uc_protocol` primitives (Tasks 1-7 + tail-wrap OOB fix). Phase 2 = `uc_service` runtime, `uc_node` IPC wiring, integration tests (Tasks 8-22).
**Branch:** `main`, commits `01b53fa..bd4ca23` (24 commits, +7,029 / -168 lines).
**Workspace:** `ultima_cluster/`.

## Goal

Split the user's state machine into a separate **`uc_service`** process, with shared-memory IPC between `uc_node` (Raft engine) and `uc_service` (deterministic apply / query / output). Same-host clients also reach `uc_node` over shmem via the future **`uc_client`** crate (full client wiring lands in M4).

```
[client process]     ──shmem──▶  [uc_node]  ◀──QUIC──▶  [uc_node on peer host]
                                     ▲
                                     │ shmem
                                     ▼
                                [uc_service]
```

After M3, a `uc_node` instance with `IpcMode::Shmem` plus a `uc_service::ServiceBuilder::run` form a single replicated state-machine instance. M2's "embedded SM in `uc_node`" path becomes the special-case `IpcMode::Embedded`. The shmem protocol is identical whether the service runs as a tokio task in the same process (M3 tests) or a separate OS process (M3.x).

---

## Phase 1 — wire primitives in `uc_protocol`

Land the on-the-wire primitives that the rest of M3 builds on: lock-free ring buffers, the `cnc.dat` control file, per-RPC frame types, and the heartbeat/handshake helpers. Everything lives in the `no_std`-leaning `uc_protocol` crate — language-gate so a future Python/Java/JS SDK can implement the same wire format without depending on `uc_node` or `uc_service`.

**Shipped:**

- Workspace deps for ring/cnc layer (`memmap2`, `parking_lot`, `page_size`, `crc32fast`, `tempfile` dev-dep).
- `uc_protocol::ring::common` — `RingHeader` (192 bytes, cache-padded), per-record `FrameHeader`, length-last record framing, `write_record_at` / `try_read_record_at` / `write_padding_marker_at`, `RingError`.
- `uc_protocol::ring::spsc` — single-producer single-consumer ring (Task 2).
- `uc_protocol::ring::mpsc` — multi-producer single-consumer ring with CAS-claim (Task 3).
- `uc_protocol::ring::broadcast` — single-producer many-consumer ring (Task 4).
- `uc_protocol::cnc` — `cnc.dat` layout, `CncHeader` (256-byte header with CRC), `NodeStatus` / `ServiceStatus` (64-byte cache-aligned status blocks), `sub::*` sub-buffer index constants, `init_cnc` / `validate_cnc` (Task 5).
- `uc_protocol::frames::{apply, query, snapshot}` — `msg_type` discriminants + `header_extra` codecs for the apply ring, query/response, and snapshot build/install handshake (Task 6).
- `uc_protocol::handshake` — `ServiceReady` / `RoleChanged` frame helpers (Task 7).
- `uc_protocol::liveness` — `tick_node` / `tick_service`, `HeartbeatWatcher` for monotonic-seq + wall-time stall detection (Task 7).
- Tail-wrap padding-marker OOB fix (see "Padding alignment" below).

### `uc_protocol` posture

`uc_protocol` was M1-only `no_std`-friendly. M3 relaxes that for the new modules (`ring`, `cnc`, `frames`, `liveness`, `handshake`) which need `memmap2` and `std::sync::atomic`. The three pure-data modules — `version.rs`, `magic.rs`, `error_codes.rs` — stay `core`-only-compatible. Multi-language SDKs reimplement the wire format directly; they don't link `uc_protocol`.

### Ring buffer record layout

```
length_inclusive_header  u32   total record size (header + payload + crc)
msg_type                 u16
flags                    u16
header_extra             [u8; 8]   per-msg-type metadata
payload                  variable
crc32                    u32   over (msg_type..end-of-payload)
```

`FRAME_HEADER_LEN = 16`, `FRAME_TRAILER_LEN = 4`. Records are length-prefixed with the length **written last** (Release fence + non-atomic store on `*mut u8`). Consumers Acquire-load `producer_position` and treat `length == 0` as "claimed but not yet committed."

**Padding alignment.** All position advancements (`producer_position`, `consumer_position`, broadcast `head`) round up to `RECORD_ALIGN = 8`. The on-wire length field still stores the unaligned record size — `align_record_size()` is applied only when bumping positions. Two properties fall out:

1. `producer_position & (capacity - 1)` is always a multiple of `RECORD_ALIGN`, so `bytes_to_tail = capacity - slot_offset` is also a multiple of `RECORD_ALIGN` and ≥ 8 whenever a wrap is needed.
2. The padding marker's 6-byte write (4-byte length + 2-byte msg_type = `PADDING_MSG_TYPE = 0xffff`) fits unconditionally inside that 8-byte minimum.

Without this, tiny payloads (e.g. 1-byte payload → 21-byte record) on a small capacity could leave `bytes_to_tail < 6` and `write_padding_marker_at` would scribble past the slot region. The fix is structural — no special-case at the OOB callsite — and the SPSC regression test `tiny_payload_tail_wrap_no_oob` drives 200 wraps against a 64-byte ring to lock it in. `init_ring_header` additionally rejects `capacity_bytes < RECORD_ALIGN`.

### Three ring shapes

| Ring | Producers | Consumers | Producer pos update | Consumer pos update | Use case |
|---|---|---|---|---|---|
| `SpscRing` | 1 (`&mut self`) | 1 (`&mut self`) | Release store after record bytes | Release after consume | service↔node apply / apply_resp / query / query_resp |
| `MpscRing` | N (`MpscProducer: Clone`, `&self`) | 1 (`&mut self`) | AcqRel CAS *before* record bytes | Release after consume | clients→node submit ring (M4), cnc control rings |
| `BroadcastRing` | 1 (`&mut self`) | N (each holds own `head: u64`) | Release after record bytes | n/a (in-memory `head`) | node→clients response broadcast (M4) |

All three share the same `RingHeader` + per-record framing, differ only in the producer/consumer half. The producer/consumer split is enforced structurally (separate types, `&mut self` for single-writer/single-reader contracts).

### Known limitation — MPSC/Broadcast post-wrap torn-record race

For MPSC and Broadcast, the producer advances `producer_position` **before** writing the record. The `length == 0 → not yet committed` check works on the **first generation** (mmap is zero-initialized), but **not after wrap-around** — stale length bytes from the previous generation at the same offset can spoof a commit. Documented prominently in `ring::mpsc` and `ring::broadcast` module headers. Fix tracked for M4:
- (a) a separate "published-up-to" position that producers advance in claim order (LMAX-Disruptor-style), or
- (b) per-slot generation counters.

In M3 this is acceptable because MPSC is only used for the cnc control rings (handshake + role-change — tiny traffic, no wrap expected) and Broadcast is not yet wired. **MPSC/Broadcast must not be used for high-traffic rings until M4 lands the fix.**

### `cnc.dat` layout

```
offset   contents                                                  size
──────   ─────────────────────────────────────────────────────     ────
   0     CncHeader (magic, protocol_version, app_id, instance_id,
         node_id, sub_buffer_offsets[8], sub_buffer_sizes[8], CRC)  256
 256     NodeStatus (heartbeat_seq, heartbeat_at_ns, role,
         current_term, last_applied, leader_node_id, …)              64
 320     ServiceStatus (heartbeat_seq, heartbeat_at_ns, state,
         last_applied, last_handshake_at_ns, …)                      64
 384     control_to_service ring (MPSC: RingHeader + 16 KiB slots)
   …     control_to_node    ring (MPSC: RingHeader + 16 KiB slots)
```

`sub::*` indices are stable across protocol-version-minor bumps; additions go to higher indices. Indices 4-7 are reserved for M4/M5 (`CONTROL_TO_CLIENTS`, `COUNTERS_METADATA`, `COUNTERS_VALUES`, `ERROR_LOG`).

`CncHeader::header_crc32` (offset 248) protects the whole header except the CRC field itself. `validate_cnc` rejects bad magic / bad CRC. Per-IPC-entry `(app_id, instance_id, protocol_version)` checks live at the attach paths in `uc_node::ipc::Instance::create` and `uc_service::runtime::attach::attach` (Phase 2).

### Frame types

| `msg_type` | Frame | Ring | Direction |
|---|---|---|---|
| `1` | `ApplyFrame { log_index → header_extra, cmd → payload }` | `service/apply.ring` (SPSC) | node → service |
| `2` | `ApplyRespFrame { log_index → header_extra, resp → payload }` | `service/apply_resp.ring` (SPSC) | service → node |
| `3` | `QueryFrame { kind ∈ {Linearizable, Snapshot}, request_id, query → payload }` | `service/query.ring` (SPSC, MPSC-shaped in M4) | clients → service via node |
| `4` | `QueryRespFrame { request_id, response → payload }` | `service/query_resp.ring` (SPSC) | service → node |
| `100` | `BuildSnapshot { /* empty header_extra */ }` | cnc control rings | node → service |
| `101` | `SnapshotBuilt { log_index → header_extra }` | cnc control rings | service → node |
| `200` | `ServiceReady { last_applied → header_extra }` | `control_to_node` (MPSC) | service → node |
| `201` | `RoleChanged { role → header_extra }` | `control_to_service` (MPSC) | node → service |

All `header_extra` fields are little-endian — pinned by a unit test (`snapshot_built_is_little_endian`) to lock the contract for cross-language SDKs.

### Liveness

Each side increments its own `heartbeat_seq` (relaxed `fetch_add`) and bumps `heartbeat_at_ns` on a fixed tick (100 ms). `HeartbeatWatcher` records the last observed `seq` + wall-time. Both signals are useful:
- `seq` change → peer is alive and ticking;
- wall-time → guards against the watcher itself waking from a long pause and falsely declaring death.

Liveness ops are all `Relaxed` because the seq doesn't protect any data — it's a free-running counter, and the `HeartbeatWatcher` only needs eventual consistency.

---

## Phase 2 — `uc_service` runtime + `uc_node` IPC wiring

Phase 1 was contentless wire spec. Phase 2 connects the spec to the actual node and service runtimes, end-to-end through openraft.

**Shipped:**

- **Task 8: `uc_service::ultima_db` adapter.** `StoreStateMachine<C, R, Q, QR>` implements `StateMachine` against an `ultima_db::Store`. `apply` opens `WriteTx` pinned to `log_index` and commits; `query` opens a `ReadTx` over the latest version; `build_snapshot` / `install_snapshot` delegate to `ultima_db::snapshot_stream`. Errors in `apply` panic (SMR contract: no retryable apply).
- **Task 9: `uc_service` runtime skeleton.** `ServiceBuilder<S>` + `ServiceConfig { instance_dir, app_id, data_dir, ... }`. Holds the user SM until `run()`.
- **Task 10: service-side attach + handshake.** `runtime::attach` opens cnc.dat, validates the `(app_id, instance_id, protocol_version)` trio, opens the four service-side SPSC ring files in producer/consumer roles, returns an `Attached` bundle. `runtime::handshake` provides `set_service_state` + future `ServiceReady` frame publish (the MPSC cnc-sub-mmap attach path is M4, so for now we transition `ServiceStatus::state = READY` directly).
- **Task 11: service loops + `Service` handle.** Three loops spawned by `ServiceBuilder::run`:
    - **apply** — `std::thread` (not tokio): drains `apply.ring`, takes the user-SM `parking_lot::Mutex`, calls `sm.apply(log_index, cmd)`, publishes `ApplyRespFrame` on `apply_resp.ring`. Sync thread because `apply` is sync.
    - **query** — tokio task: same shape as apply but for `query.ring` / `query_resp.ring`. Sync read against the latest version; lock is never held across an await.
    - **liveness** — tokio task: ticks `ServiceStatus::heartbeat_seq` every 100 ms.
    Returns a `Service` handle that owns the cnc mmap; `Service::shutdown` joins all three.
- **Task 12: `uc_node::ipc` module + instance directory + cnc.dat owner.** `Instance::create(instance_dir, app_id, node_id)` acquires `instance.lock` (exclusive flock — one node per instance directory), writes a fresh `cnc.dat` with a random `instance_id`, creates the `service/` subdir for the SPSC ring files.
- **Task 13: `ipc::service_link` — apply / query SPSC ring creation.** Creates `service/{apply,apply_resp,query,query_resp}.ring`. 16 MiB capacity, 4 MiB max frame for query/query_resp (linearizable reads can be large); 4 MiB / 1 MiB for apply / apply_resp. Returns a struct with the producer and consumer halves matched to the node's direction on each ring.
- **Task 14: node-side heartbeat + service-ready watcher.** `ipc::liveness::spawn_liveness` (mirror of the service side) and `ipc::handshake::wait_for_service_ready` (polls `ServiceStatus::state` until `READY`, with timeout).
- **Task 15: shmem-mode `AdaptedStateMachine`.** `ShmemAdaptedStateMachine<S>` satisfies `openraft::storage::RaftStateMachine<TypeConfig>` but its `apply()` bincode-encodes openraft's `Entry`, publishes onto `service/apply.ring`, awaits the matching response from `apply_resp.ring`. Snapshot build/install still run in-process against the (degenerate) node-side `S` — the snapshot-via-shmem region is M5.
- **Task 16: `NodeConfig::ipc_mode` + dispatch.** `IpcMode::Embedded` (M1/M2 path) vs `IpcMode::Shmem { instance_dir }`. `NodeBuilder::start` branches on the enum, constructs either `AdaptedStateMachine` or `ShmemAdaptedStateMachine`, then funnels into a shared `finish()` helper for the openraft+QUIC plumbing.
- **Task 17: first end-to-end shmem test.** `m3_shmem_single_node.rs::shmem_single_node_submit_apply` — single-node shmem instance, two `Inc` submits round-trip through the apply ring.
- **Task 18: `NodeHandle::submit_query` + node-side query link.** `ShmemQueryLink` wraps the `query.ring` producer + `query_resp.ring` consumer with a `tokio::sync::Mutex` (serializes concurrent callers; SPSC needs one writer). Allocates a `request_id` per submit and round-trips it as a corruption sanity check. `submit_query` works in both modes — embedded mode just calls `state_machine.query(q)` under the apply mutex. `m3_shmem_single_node.rs::shmem_single_node_query_roundtrip` exercises empty-state, post-apply, and concurrent-query cases.
- **Task 19: 3-node shmem cluster test.** `m3_three_node_shmem.rs::three_node_shmem_cluster` — three nodes, each with its own service, all in one tokio runtime, replicating Inc(1..=5) and reading back via `submit_query` on every node.
- **Task 20: service-liveness watcher + crash test.** `ipc::service_watcher::spawn_service_watcher` polls service heartbeat via `HeartbeatWatcher` with a 2 s default timeout. On stall: flips a public `stalled: AtomicBool` and, if this node is leader, calls `raft.shutdown()` (the M3 substitute for `Raft::trigger_leader_transfer` — see "openraft 0.9 limitations" below). `NodeHandle::service_stalled()` exposes the flag. `m3_service_crash.rs` brings up 3 nodes, kills the leader's service, verifies the watcher fires and the surviving voters re-elect.
- **Task 21: ultima_db adapter end-to-end via shmem.** `m3_ultima_db_adapter.rs::ultima_db_adapter_end_to_end` swaps the hand-rolled Counter SM for `StoreStateMachine` wrapping a `Store` with a `"counter"` table; verifies apply + query both work through the full pipeline.
- **Task 22: clippy / fmt / README polish.** `cargo fmt` across the workspace; README updated to reflect M3-complete status.

### Apply pipeline (steady state)

1. Client (or test harness) calls `NodeHandle::submit(cmd)`.
2. Bincode-encode `cmd` → `Bytes`; `raft.client_write(bytes)`.
3. openraft replicates, commits, and calls `ShmemAdaptedStateMachine::apply(entry)`.
4. We bincode-encode the openraft `Entry` payload as the apply ring's record payload (`log_index` rides in `header_extra`), publish on `service/apply.ring`.
5. Service's apply_loop std::thread consumes, takes the user-SM mutex, calls `sm.apply(log_index, cmd)`, bincode-encodes the response, publishes `ApplyRespFrame` on `service/apply_resp.ring`.
6. Node's apply (still inside `ShmemAdaptedStateMachine::apply`) reads `apply_resp.ring`, matches the `log_index`, returns the response `Bytes` back to openraft.
7. openraft returns from `client_write`; `submit` decodes the response.

Concurrency: only one apply is in flight at a time (openraft serializes its apply calls; we hold `inner` mutex for the duration of each apply's publish-then-await). The `apply_loop` runs as a std::thread because `sm.apply` is sync and we don't want to pin a tokio worker.

### Query pipeline

`submit_query` allocates a `request_id`, publishes a `QueryFrame` on `service/query.ring`, awaits the matching `QueryRespFrame` on `service/query_resp.ring`. A single `tokio::sync::Mutex` inside `ShmemQueryLink` serializes concurrent callers — single in-flight query at a time, so response-routing is trivial. The service's query_loop runs as a tokio task (not std::thread): queries are sync but cheap, and tokio's cooperative scheduling lets the same worker handle the liveness ticker.

`QueryKind::Snapshot` is the only kind M3 emits; `Linearizable` is reserved for the M4+ round-trip-through-raft path.

### Liveness + leader-transfer

- Node-side `spawn_liveness` ticks `NodeStatus::heartbeat_seq` every 100 ms.
- Service-side `spawn_liveness` ticks `ServiceStatus::heartbeat_seq` every 100 ms.
- Node-side `spawn_service_watcher` polls service status every 100 ms; declares stall when seq hasn't advanced within 2 s (configurable via `DEFAULT_LIVENESS_TIMEOUT`).
- On stall + this node is leader: `raft.shutdown()` to surrender leadership. Cluster's remaining voters re-elect via openraft's election timer.

### `NodeHandle::shutdown` ordering

1. `raft.shutdown()` — stops outbound RPCs (idempotent — the service watcher may already have called it).
2. `server.shutdown()` — closes the QUIC server endpoint, awaits the accept task.
3. `service_watcher.stop` + join — shmem mode only; joins before the cnc mmap drops because the task holds a `&'static ServiceStatus` into it.
4. `node_liveness.stop` + join — same reason.
5. `_instance` drops → cnc mmap unmaps, `instance.lock` releases.

### openraft 0.9 limitations

openraft 0.9.24 doesn't expose `Raft::trigger_leader_transfer` (added in 0.10+). M3 substitutes `raft.shutdown()` on a stalled leader: destructive (the freshly-stalled node stays out for the rest of the process lifetime) but achieves the stated outcome — cluster continues, new leader elected, fresh submits succeed. Tracked for M4 alongside an openraft upgrade.

### Service-crash → recovery cross-check

Phase 2 implements the watcher's *detection* and *leadership-surrender* halves. The *recovery* half (service restarts, opens cnc, handshakes with `last_applied`, node feeds backfill from `last_applied + 1`) is M4 work — needs the cnc-sub-mmap MPSC attach API to consume `ServiceReady` frames. The Task 15 module header documents the resulting M3 simplification: the node-side `S` is degenerate (used only by snapshot trait surface; no last_applied cross-check at start).

---

## Files added / changed

```
Phase 1:
  Cargo.toml                              # +6  workspace deps
  uc_protocol/Cargo.toml                  # +11 mmap2/parking_lot/page_size/crc32fast/tempfile
  uc_protocol/src/lib.rs                  # module wiring
  uc_protocol/src/ring/{mod,common,spsc,mpsc,broadcast}.rs  # +1,450
  uc_protocol/src/cnc.rs                  # +352
  uc_protocol/src/frames/{mod,apply,query,snapshot}.rs      # +151 + tests
  uc_protocol/src/handshake.rs            # +88
  uc_protocol/src/liveness.rs             # +143

Phase 2 — uc_service:
  uc_service/Cargo.toml                            # +features ultima_db
  uc_service/src/lib.rs                            # runtime + ultima_db modules
  uc_service/src/ultima_db/{mod,builder,store_state_machine}.rs   # adapter
  uc_service/src/runtime/{mod,service,attach,handshake,apply_loop,query_loop,liveness}.rs

Phase 2 — uc_node:
  uc_node/src/config.rs                            # +IpcMode enum
  uc_node/src/ipc/{mod,instance,service_link,liveness,handshake,query_link,service_watcher}.rs
  uc_node/src/raft/state_machine_shmem.rs          # ShmemAdaptedStateMachine
  uc_node/src/runtime/{builder,node}.rs            # Shmem dispatch + submit_query/service_stalled
  uc_node/Cargo.toml                               # +ultima-db dev-dep

Phase 2 — tests:
  uc_node/tests/m3_shmem_single_node.rs            # submit_apply + query roundtrip
  uc_node/tests/m3_three_node_shmem.rs             # 3-node replication via shmem
  uc_node/tests/m3_service_crash.rs                # service-crash leadership surrender
  uc_node/tests/m3_ultima_db_adapter.rs            # StoreStateMachine via shmem
```

Totals: 53 files changed, +7,029 / -168 lines across 24 commits.

## Tests

| Module | Tests | Notes |
|---|---|---|
| `uc_protocol::*` | 38 | rings, cnc, frames, handshake, liveness |
| `uc_node::*` (unit) | 14 | recovery, drift detection, frame round-trip, log storage, ipc::* |
| `uc_node::tests::m1_single_node` | 2 | M1 capstone |
| `uc_node::tests::m2_multi_node` | 5 | M2 capstone (3-node QUIC) |
| `uc_node::tests::m3_shmem_single_node` | 2 | submit + query roundtrip |
| `uc_node::tests::m3_three_node_shmem` | 1 | 3-node shmem replication |
| `uc_node::tests::m3_service_crash` | 1 | service-crash → leadership surrender |
| `uc_node::tests::m3_ultima_db_adapter` | 1 | StoreStateMachine end-to-end |
| `uc_node::tests::shmem_state_machine` | 2 | M3 unit |
| `uc_service::*` | 12 | runtime + ultima_db unit |

Total: **~91 tests** workspace-wide, all green at M3 close (commit `bd4ca23`). M3 capstone tests collectively cover the five scenarios from the plan's verification checklist.

## Notable design decisions

- **`RECORD_ALIGN = 8` chosen as the minimum that fits the 6-byte padding marker.** Could have been larger (e.g. 16 for cache-line-quarter alignment) but 8 minimizes wasted space for small records.
- **Length field stores unaligned size; advance uses aligned size.** Keeps the wire format unchanged from a cross-SDK perspective (length still equals `FRAME_HEADER_LEN + payload + FRAME_TRAILER_LEN`); only the position arithmetic carries the alignment.
- **MPSC/Broadcast wrap race is shipped as a documented limitation** rather than blocked on the fix, because M3's cnc control rings stay within first generation and Broadcast is unused in M3. Fix scheduled for M4 alongside `clients/submit.ring`.
- **`unsafe impl Send for FooInner` on each ring's Inner** — the mmap is owned, atomics provide the synchronization, and the SAFETY comments name the invariants.
- **Service `apply_loop` is a `std::thread`, `query_loop` is a tokio task.** `apply` is sync and we don't want to pin a tokio worker; `query` is sync but cheap and cooperates well with the liveness ticker on the same runtime.
- **Single in-flight query at a time, gated by `tokio::sync::Mutex`.** Keeps response routing trivial (the service publishes in publish order) at the cost of query throughput. M4 can revisit if profiling shows it matters.
- **`raft.shutdown()` as the M3 substitute for `Raft::trigger_leader_transfer`.** openraft 0.9.24 lacks the proper API; shutdown is the simplest available primitive that achieves the test's stated outcome.
- **In-process tests use `#[tokio::test]` (current_thread), not `multi_thread`.** Running two `multi_thread` tokio runtimes in the same test binary deadlocked the second runtime (observed when adding the query-roundtrip test in Task 18). `apply_loop`'s own `std::thread` keeps the apply path off tokio anyway, so current_thread is sufficient.
- **`SendPtr` newtype in `builder.rs`.** Raw `*const ServiceStatus` isn't `Send`; the `service_status_ptr` needs to survive the `wait_for_service_ready` + `finish().await` chain before reaching `spawn_service_watcher`. A small unsafe-`Send` wrapper carries it across — the mmap-pinning invariant is upheld by `Instance` outliving every consumer.

## Follow-ups tracked for M4+

- **MPSC/Broadcast post-wrap fix** (M4): published-up-to position or per-slot generation counters. Tracked in `ring::mpsc` and `ring::broadcast` module headers.
- **`Raft::trigger_leader_transfer` via openraft 0.10 upgrade.** Replaces the M3 `raft.shutdown()` substitute in `service_watcher`.
- **Service-recovery handshake.** Needs the cnc-sub-mmap MPSC attach API so the node can consume `ServiceReady{last_applied}` frames after a restart. After that lands, the node-side last_applied cross-check (currently skipped in `ShmemAdaptedStateMachine::new` with a warn-log) can run.
- **Snapshot via `snapshot.region` mmap** (M5). Current path keeps the M2 `Cursor<Vec<u8>>` storage.
- **`clients/*.ring` + `uc_client` real impl** (M4): MPSC for client submit, Broadcast for responses, session files for client identity.
- **`output.ring` + at-least-once OutputHandler dispatch** (M5).
- **`CncHeader::app_id_str`** returns `""` on bad UTF-8, which weakens app_id matching to "two empty-strings match." Tighten to `Option<&str>` or compare raw bytes.
- **`HeartbeatWatcher` monotonicity** — `seq != self.last_seq` currently accepts a regression as "alive." Tighten to `seq > self.last_seq` and surface a typed `SeqRewound`.
- **`BroadcastRing::producer(&self)`** can be called repeatedly, breaking the single-producer invariant structurally. Change to `into_producer(self)` or `Option`-take the handle before M4 wires high-traffic broadcast traffic.
- **Multi-process subprocess tests** (M3.x): the protocol works identically with `uc_service` as a separate OS process; in-process tokio-task tests prove the wire format and dispatch.

## Build / test / lint

```bash
cargo build --workspace
cargo test  --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

All green at M3 close (commit `bd4ca23`).

## Pointers

- Canonical design: `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` (Sections 4-10).
- M1 record: `docs/tasks/task01_m1_embedded_single_node.md`.
- M2 record: `docs/tasks/task02_m2_multi_node_quic.md`.
- Dependency primitives: `../ultima_journal/` (segmented append log + `StableValue`), `../ultima_db/` (MVCC store + `snapshot_stream`), `openraft 0.9.24` (Raft consensus), `quinn 0.11` (QUIC), `memmap2 0.9`, `crc32fast 1`, `parking_lot 0.12`.
