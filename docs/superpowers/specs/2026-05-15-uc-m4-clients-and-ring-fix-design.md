# M4 design — MPSC/Broadcast wrap-fix + `uc_client` end-to-end

**Status:** design (brainstormed 2026-05-15, awaiting plan).
**Predecessors:** M3.5 (`docs/tasks/task04_m3_5_openraft_0_10_upgrade.md`) — openraft 0.10 upgrade + `transfer_leader` cutover, which builds on M3 (`docs/tasks/task03_m3_shmem_service_split.md`) — shmem IPC + `uc_service` process split.
**Workspace:** `ultima_cluster/`.

## Goal

Two coupled deliverables:

1. **Fix the MPSC/Broadcast post-wrap torn-record race** documented as a known M3 limitation in `uc_protocol::ring::{mpsc, broadcast}`. The fix is a prerequisite for (2) because `clients/*` rides on MPSC and Broadcast under wrap-prone traffic.
2. **Land `uc_client` end-to-end** — `clients/{submit,query}.ring` (MPSC), `clients/response.broadcast` (Broadcast), `clients/sessions.dir/` for per-client liveness, the node-side dispatchers, and the public `Client::{connect, submit, query_linearizable, query_snapshot, shutdown}` API.

After M4, an out-of-process `uc_client` binary on the same host as a `uc_node` + `uc_service` pair can submit commands and queries end-to-end across the shmem boundary. The `M3` service-split tests continue to pass unchanged.

```
[client process]      ──shmem──▶  [uc_node]  ◀──QUIC──▶  [uc_node on peer host]
                                      ▲
                                      │ shmem
                                      ▼
                                 [uc_service]
```

## Out of scope for M4 (carried forward)

- **openraft 0.10 upgrade + real `Raft::trigger_leader_transfer`.** Shipped in M3.5 (`docs/tasks/task04_m3_5_openraft_0_10_upgrade.md`).
- **Service-recovery handshake** (cnc-sub-mmap MPSC attach so the node can consume `ServiceReady{last_applied}` frames). Tracked for M5.
- **`output.ring` + at-least-once OutputHandler dispatch.** M5.
- **`snapshot.region` mmap.** M5.
- **Multi-process (separate-OS-process) client tests.** M4 tests run all roles as tokio tasks in one process; the protocol works identically across process boundaries.
- **Client SDK that auto-routes across multiple nodes** (option C from brainstorming). v1 surfaces `NotLeader { hint }` and lets callers decide; a `TestClient` helper that owns several handles can come later if helpful.

## Top-level decisions

| Decision | Choice | Why |
|---|---|---|
| MPSC/Broadcast wrap-race fix | **Published-up-to / committed-head position** (LMAX-Disruptor pattern; option A from brainstorming) | No per-record wire-format change; producers stay lock-free with a bounded spin on slower peers; ~1 atomic added to the ring header. The Aeron alternative (partition rotation + cleaner; option E) goes in "future options" — heavier (3× memory + janitor task) than our contention pattern justifies. |
| Client identity | **`client_id: u32` from a `next_client_id: AtomicU64` slot in `cnc.dat`** (option A′; Aeron-style allocator) | One global counter, `fetch_add` at connect, no coordination beyond an atomic on an already-mapped page. Correlation `(client_id: u32, local_seq: u32)` packs into the existing 8-byte `header_extra` exactly — no payload-prefix steal, no wire-format growth. |
| Leader routing in `Client` | **Surface `NotLeader { hint: Option<NodeId> }`; no auto-retry, no multi-handle SDK** (option A) | Matches the existing `ClientError` enum in §11 of the canonical design. Smallest SDK surface. Callers running multi-host typically don't have shared local access to all instance_dirs anyway. |
| Response routing | **Broadcast filter by `client_id` in `header_extra`** | One reader task per Client, a `DashMap<local_seq, oneshot::Sender>` for in-flight submits, frames where `client_id != self.client_id` are discarded. |

## Phase 1 — `uc_protocol::ring` wrap-fix

### MPSC

Today (broken): producer atomically CAS-advances `producer_position` *before* writing the record. After wrap, stale length bytes at the same offset can spoof a "committed" record to a consumer reading `length != 0`.

After (option A): split producer position from publication position.

```
RingHeader (new fields, u64 each, cache-line padded):
    claim_position        // producers CAS-advance this to claim a slot
    publish_position      // producer advances this only after the record's bytes are visible
    consumer_position     // unchanged: reader advances as records are consumed
```

Producer flow:
1. `claim_position.fetch_add(aligned_size)` → `my_slot_start` (LMAX-style claim).
2. Write the record bytes at `my_slot_start` (header + payload + crc).
3. Spin until `publish_position == my_slot_start`, then `publish_position.store(my_slot_start + aligned_size, Release)`. This serializes publication ordering even though claims happen in parallel.

Consumer flow: read up to `publish_position` (instead of `producer_position`). Length-last-release semantics inside a record stay as today — but the wrap race vanishes because a slot is *never* read past `publish_position`, and `publish_position` only advances after the producer's writes are visible.

The spin in (3) is bounded by the slowest in-flight peer's write time. For our payloads (<4 MiB, bincode-encoded `Command`s) that's microseconds.

### Broadcast

Single producer, but the same wrap-race shape: consumer at stale offset sees old length. Fix is symmetric:

```
RingHeader:
    write_head         // producer-only; advances atomically per record
    committed_head     // producer advances only after the record's bytes are visible
```

Producer flow: bump `write_head` to claim space, write bytes, then `committed_head.store(write_head, Release)`. Consumers read up to `committed_head`. Single-producer makes (3)'s "wait for prior claims" trivial — there are no prior claims.

The existing `OverwrittenByProducer` signal (slow-consumer detection) stays unchanged — it's about lap-lapping, not about torn records.

### Regression tests

- `mpsc::tests::wrap_under_many_producers_no_torn_read` — 8 producers × 200 records on a tiny ring (~64 records' worth of capacity) → no torn reads, total count matches.
- `broadcast::tests::wrap_no_torn_read` — single producer + 2 consumers, wrap several times → consumers see only fully-committed records.
- `broadcast::tests::slow_consumer_still_detects_overwrite` — verify the existing `OverwrittenByProducer` path didn't regress.

### Future option (recorded, not built)

**Aeron-style partition rotation + cleaner (option E).** Three back-to-back partitions per logical ring; producer rotates on fill; a node-side janitor zeros stale partitions ahead of consumers. Slots are guaranteed zero before reuse, so the wrap race is *eliminated* rather than mitigated. Costs 3× memory in `/dev/shm` per ring and an additional long-running task per ring; the production-proven alternative when contention or throughput justifies it. We can swap to E inside `uc_protocol::ring` without changing any callers if profiling ever motivates it.

## Phase 2 — `cnc.dat` extensions + new wire frames

### `cnc.dat`

Add one sub-buffer index: `sub::NEXT_CLIENT_ID` → 16-byte sub-region (8 bytes `AtomicU64` + 8 bytes `_pad`).

```
[9: next_client_id]   AtomicU64 + pad   (16 bytes; clients fetch_add to allocate identity)
```

`init_cnc` initializes the counter to 1 (so `client_id = 0` can mean "not allocated yet" in any future use). All other cnc layout stays identical to M3.

This is the first cnc.dat region clients *write* (only via `fetch_add`). All other client-side cnc accesses remain read-only (status fields).

### New frame types in `uc_protocol::frames::client`

| `msg_type` | Frame | Ring | Direction |
|---|---|---|---|
| 5 | `SubmitFrame { client_id, local_seq → header_extra; cmd → payload }` | `clients/submit.ring` (MPSC) | clients → node |
| 6 | `SubmitResponse { client_id, local_seq → header_extra; resp → payload }` | `clients/response.broadcast` | node → clients |
| 7 | `ClientQueryFrame { client_id, local_seq → header_extra; kind in flags; query → payload }` | `clients/query.ring` (MPSC) | clients → node |
| 8 | `ClientQueryResp { client_id, local_seq → header_extra; resp → payload }` | `clients/response.broadcast` | node → clients |
| 9 | `NotLeaderResp { client_id, local_seq → header_extra; leader_hint → payload (Option<NodeId>) }` | `clients/response.broadcast` | node → clients |

`QueryKind` (Linearizable / Snapshot) rides in the 16-bit `flags` field (1 bit used; rest reserved). `header_extra` encoding helper: `encode_extra_client(client_id: u32, local_seq: u32) -> [u8; 8]` (LE).

**Note on `header_extra` convention asymmetry** vs the M3 `frames::query` module: the service-side query frame packs `(request_id: u32, kind: u8, _pad: 3)` in `header_extra` because the service path has no `client_id` to carry. Client frames need both identity *and* sequence in the 8 bytes, so `kind` moves to `flags`. Different shapes, same 8-byte budget, isolated by module.

## Phase 3 — node-side wiring (`uc_node`)

### `ipc::client_link` (mirror of `ipc::service_link`)

Creates the three client-facing ring files:

```
{instance_dir}/clients/submit.ring          # MPSC, ~16 MiB, max_frame 4 MiB
{instance_dir}/clients/query.ring           # MPSC, ~16 MiB, max_frame 4 MiB
{instance_dir}/clients/response.broadcast   # Broadcast, ~16 MiB, max_frame 4 MiB
```

Also creates the `clients/sessions.dir/` directory at startup. Returns a struct with the MPSC *consumer* halves (node side) for submit and query, and the Broadcast *producer* half (node side) for response.

### Dispatcher tasks

**`client_dispatcher`** (tokio task on the node):
- Loop: read next frame from `clients/submit.ring` (MPSC consumer half).
- If `raft.current_leader().await != Some(self.node_id)`: synthesize `NotLeaderResp { hint = raft.current_leader().await }`, publish to `response.broadcast` with the submitter's `header_extra`. Continue.
- Else: bincode-decode is unnecessary — payload is already an opaque `Bytes` to `raft.client_write`. Await the response. On success: publish `SubmitResponse{client_id, local_seq, payload}` to `response.broadcast`.

**`client_query_dispatcher`** (tokio task):
- Loop: read next frame from `clients/query.ring`.
- Decode `QueryKind` from `flags`.
- If `Linearizable`: leader-only — `NotLeaderResp` if not leader; else `raft.ensure_linearizable().await`; then forward through the existing `ShmemQueryLink` to `service/query.ring`; relay response.
- If `Snapshot`: forward directly through `ShmemQueryLink`; relay response.
- Publish `ClientQueryResp` to `response.broadcast`.

Both dispatchers share the response Broadcast producer via a `parking_lot::Mutex` — Broadcast is single-producer-by-design, and the mutex enforces that at the type level. Lock is held briefly across one record write; no awaits.

**`session_gc`** (tokio task, 2 s tick):
- Read `clients/sessions.dir/` directory.
- For each `{client_id}.session` filename: mmap (8 KiB max), read `heartbeat_seq` + `heartbeat_at_ns`, run a per-session `HeartbeatWatcher`.
- Stale = no `heartbeat_seq` advance for ≥ 5 s.
- On stale: unlink the file. (No further node-side state to clean — in-flight broadcasts for that client_id are no-ops; nobody reads them.)

### `NodeHandle` integration

- `IpcMode::Shmem` always creates the client rings + sessions dir. No new config knob in v1 (a `ClientConfig::Off` variant could come later if it's a real cost.)
- A new `client_link: Option<ClientLink>` field on `NodeHandle` owns the three ring mmaps (mirror of M3's `ServiceLink`). The three dispatcher tasks hold ring producer/consumer handles whose backing mmaps live in `ClientLink`; `NodeHandle::shutdown` joins all three dispatcher tasks *before* dropping `client_link`, same lifetime discipline as M3's `service_watcher` join-before-`_instance`-drop.

## Phase 4 — `uc_client` SDK

### Public API

```rust
pub struct Client { /* ... */ }

impl Client {
    pub async fn connect(instance_dir: &Path, app_id: &str) -> Result<Self, ClientError>;

    pub async fn submit<C, R>(&self, cmd: &C) -> Result<R, ClientError>
        where C: serde::Serialize, R: serde::de::DeserializeOwned;

    pub async fn query_linearizable<Q, QR>(&self, q: &Q) -> Result<QR, ClientError>
        where Q: serde::Serialize, QR: serde::de::DeserializeOwned;

    pub async fn query_snapshot<Q, QR>(&self, q: &Q) -> Result<QR, ClientError>
        where Q: serde::Serialize, QR: serde::de::DeserializeOwned;

    pub fn current_leader(&self) -> Option<NodeId>;   // direct cnc load
    pub fn last_applied(&self) -> u64;                // direct cnc load
    pub fn instance_id(&self) -> u128;
    pub fn client_id(&self) -> u32;

    pub async fn shutdown(self);
}
```

### Connect flow

1. Open + validate `cnc.dat` — magic, header CRC, `(app_id, protocol_version)` trio, cache `instance_id`.
2. `fetch_add(1)` on the cnc `next_client_id` slot; truncate to `u32` → `self.client_id`. The underlying counter is `u64`; truncation to `u32` accepts 4 B connects between node restarts as outside v1's threat model. (A node restart resets the counter; the cluster's `instance_id` flip is the existing signal callers use to invalidate cached client identity.)
3. Create + mmap `clients/sessions.dir/{client_id}.session` (64 bytes: `heartbeat_seq: AtomicU64`, `heartbeat_at_ns: AtomicU64`, `client_id_check: u32`, padding). Write `client_id_check`.
4. Open `clients/submit.ring` as an MPSC producer; `clients/query.ring` as an MPSC producer; `clients/response.broadcast` as a Broadcast consumer (fresh `head` = current `committed_head`).
5. Spawn the **session ticker** (tokio task, 100 ms): advances `heartbeat_seq` + `heartbeat_at_ns`. Joined on `shutdown`.
6. Spawn the **broadcast reader** (tokio task): consumes `clients/response.broadcast`, decodes `(client_id, local_seq)` from `header_extra`, discards frames whose `client_id` ≠ ours; for matching frames, looks up the `local_seq` in a `DashMap<u32, oneshot::Sender<Bytes>>`, sends payload, drops the entry.
7. Spawn **node-stall watchers** (two tokio tasks running `HeartbeatWatcher` against `NodeStatus` / `ServiceStatus`, polling every 100 ms with a 2 s liveness timeout — symmetric with the node-side `service_watcher` from M3). Each sets a `node_stalled: AtomicBool` / `service_stalled: AtomicBool` on the Client. Submits / queries select on the oneshot Receiver, the configured request timeout, and a 100 ms interval poll on the stall flags; whichever fires first wins.

### `submit` flow

1. bincode-encode `cmd`.
2. `local_seq = self.next_local_seq.fetch_add(1) as u32`.
3. `let (tx, rx) = oneshot::channel(); self.in_flight.insert(local_seq, tx);`
4. MPSC publish on `clients/submit.ring` with `header_extra = encode_extra_client(self.client_id, local_seq)`, msg_type=5, payload=encoded cmd. On `RingError::Full` → wait grace period (configurable, default 1 s), then `ClientError::BackpressureFull`.
5. `select!` between `rx.await`, the configured submit timeout, and stall-flag polling. The broadcast reader resolves `rx` with the response payload.
6. Distinguish `msg_type` (5 = SubmitResponse → decode payload as `R`; 9 = NotLeaderResp → decode hint, return `ClientError::NotLeader { hint }`; anything else → `ClientError::Decode("unexpected msg_type")`).

### `query_*` flow

Identical shape to `submit`, but on `clients/query.ring`, with `kind` set in `flags`. `query_linearizable` and `query_snapshot` are two thin wrappers over a shared private `submit_query(kind)`.

### `shutdown` flow

1. Set a `stop: AtomicBool`.
2. Join session ticker (which stops on `stop`).
3. Join broadcast reader.
4. Join stall watchers.
5. Unlink `clients/sessions.dir/{client_id}.session`.
6. Drop the cnc mmap and ring handles.

A dropped (not-shut-down) `Client` leaks its background tasks until process exit; the session file is unlinked by the next node-side `session_gc` sweep. We do not add a panicking `Drop` impl.

## Error model

```rust
pub enum ClientError {
    // Connect-time
    NotConnected,
    AppIdMismatch { expected: String, actual: String },
    ProtocolMismatch { local: u32, node: u32 },
    InstanceRestart { previous: u128, current: u128 },
    SessionCreate(io::Error),

    // Steady-state
    NotLeader { hint: Option<NodeId> },
    NodeStalled,
    ServiceStalled,
    Timeout(Duration),
    ResponseOverwritten,
    BackpressureFull,
    Submission(String),
    Decode(String),
    ShutDown,
}
```

- `ResponseOverwritten` is its own variant (not folded into `Decode`) so callers can distinguish "response lost in broadcast lap" (retry idempotent) from "response decode failed" (bug or protocol drift). Both are retry-safe for idempotent submits.
- `BackpressureFull` (submit ring full beyond grace period) is distinct from `Timeout` (we got our claim but response never arrived — node or service stalled).
- `NodeStalled` / `ServiceStalled` are produced by the in-Client stall watchers; in-flight `submit` / `query` futures resolve with the stall variant when watchers trip.
- Application errors from `raft.client_write` (e.g., raft fatal) bincode into the response payload and surface as `ClientError::Submission(_)`.

## Testing strategy

**Unit tests (in-crate):**
- `uc_protocol::ring::mpsc::tests::wrap_under_many_producers_no_torn_read` — regression for option A's publish_position discipline.
- `uc_protocol::ring::broadcast::tests::wrap_no_torn_read` — regression for committed_head discipline.
- `uc_protocol::ring::broadcast::tests::slow_consumer_still_detects_overwrite` — `OverwrittenByProducer` didn't regress.
- `uc_protocol::cnc::tests::next_client_id_fetch_add_concurrent` — N threads `fetch_add` on the new slot, expect distinct IDs and the right count.
- `uc_protocol::frames::client::tests::header_extra_round_trip` — `encode_extra_client` / `decode_extra_client` round-trip.
- `uc_client::handshake::tests::*` — drive a fake cnc.dat with bad magic, bad app_id, mismatched protocol, and verify each `ClientError` variant.

**Integration tests** (`uc_node/tests/m4_*`, matching the M3 capstone-per-scenario style):

1. **`m4_client_single_node`** — 1 node + 1 service + 1 client (all in-process tokio tasks). Client connects, submits 5 `Inc`s, queries via both kinds, shuts down. Smoke test for the full pipeline.
2. **`m4_client_three_node`** — 3 nodes + 3 services + 3 clients (one per node). Leader's client submits; followers' clients query (snapshot) and confirm convergence; each follower's client tries `submit` and receives `NotLeader { hint = leader_id }`.
3. **`m4_client_concurrent`** — 1 node + 1 service + 4 clients submitting in parallel from `tokio::join!`. Exercises MPSC contention on `submit.ring` and per-client response routing on the shared broadcast.
4. **`m4_client_wrap`** — small ring capacities (forces wrap within the test budget). Multiple clients submit enough to lap the rings several times. Validates the option-A wrap-fix end-to-end at the application layer (no torn responses, no dropped submits).
5. **`m4_client_leader_failover`** — 3 nodes + 3 clients. Shut down the leader node. Cluster re-elects. Old-leader's client surfaces `NodeStalled`; one of the new-leader-side clients submits and succeeds.
6. **`m4_client_session_gc`** — 1 node + 1 client; drop the `Client` without calling shutdown. After the staleness window, verify the node's `session_gc` unlinks the file.
7. **`m4_client_response_overwritten`** — 1 node + 1 slow client (broadcast reader paused). Submit enough that the broadcast laps. Resume the reader; verify it surfaces `ResponseOverwritten` for the submits whose responses were overwritten, and successful responses for the rest.

All M3 capstone tests stay unchanged and pass.

**Test runtime flavor.** M3 established that integration tests with both node and service halves run on `#[tokio::test]` (current_thread) — running multiple `multi_thread` tokio runtimes in one binary deadlocked the second one (recorded in `feedback_m3_test_runtime_flavor` memory + task03 doc). M4 tests carry the convention forward: every `m4_client_*` test uses default `#[tokio::test]`, and the `apply_loop`'s own `std::thread` keeps the apply path off the tokio runtime.

## Implementation phasing

| Phase | Scope | Commits |
|---|---|---|
| 1 | `uc_protocol::ring::{mpsc, broadcast}` published-up-to / committed-head fix + regression tests. | 2-3 |
| 2 | `uc_protocol::cnc` next_client_id slot + sub::NEXT_CLIENT_ID index. `uc_protocol::frames::client` module with the five new frame types + codec helpers. | 1-2 |
| 3 | `uc_node::ipc::client_link` (rings + sessions.dir creation). `client_dispatcher`, `client_query_dispatcher`, `session_gc` tasks. NodeHandle/builder/shutdown wiring. | 3-4 |
| 4 | `uc_client::Client` SDK: handshake, submit, query_linearizable, query_snapshot, broadcast reader, session ticker, stall watchers, shutdown. New `uc_client/Cargo.toml` deps: `tokio`, `bincode`, `bytes`, `serde`, `parking_lot`, `dashmap` (in-flight oneshot map), `memmap2`, `thiserror`, `tracing`. | 3-4 |
| 5 | Integration tests `m4_client_*` (seven scenarios). | 4-5 |
| 6 | Polish: clippy/fmt; consolidate plan into `docs/tasks/task04_m4_clients.md`; delete plan. README pointer M3 → M4. | 2 |

Total: ~15-20 commits.

## Pointers

- Canonical design: `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` (§4 cnc/rings, §5 public APIs, §8 pipelines, §11 errors).
- M3 record: `docs/tasks/task03_m3_shmem_service_split.md`.
- Aeron pattern references (read during brainstorming, kept for the future-option E note): `aeron/aeron-client/src/main/java/io/aeron/ConcurrentPublication.java` (CAS-claim + length-last-release), `aeron/aeron-client/src/main/java/io/aeron/logbuffer/LogBufferDescriptor.java:48` (PARTITION_COUNT = 3), `aeron/aeron-driver/src/main/java/io/aeron/driver/IpcPublication.java:646` (cleaner zeroing stale terms).
- openraft 0.9.24 (still pinned; 0.10 upgrade tracked for M5).
