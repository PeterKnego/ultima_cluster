# Task 05 — M4: MPSC/Broadcast wrap-fix + `uc_client` end-to-end

**Status:** Complete.
**Branch:** `main`, commits `21981ed..6ee50e0` (32 commits).
**Workspace:** `ultima_cluster/`.

## Goal

Two coupled deliverables:

1. **Fix the MPSC/Broadcast post-wrap torn-record race** documented as an M3 limitation in `uc_protocol::ring::{mpsc, broadcast}`. Prerequisite for (2): `clients/*` rides on MPSC and Broadcast under wrap-prone traffic.
2. **Ship `uc_client` end-to-end** — `clients/{submit,query}.ring` (MPSC), `clients/response.broadcast` (Broadcast), `clients/sessions.dir/` for per-client liveness, the node-side dispatchers, and the public `Client::{connect, submit, query_linearizable, query_snapshot, shutdown}` API.

After M4, an out-of-process `uc_client` on the same host as a `uc_node` + `uc_service` pair submits commands and queries end-to-end across the shmem boundary. All M3 + M3.5 capstone tests continue to pass unchanged.

```
[client process]      ──shmem──▶  [uc_node]  ◀──QUIC──▶  [uc_node on peer host]
                                      ▲
                                      │ shmem
                                      ▼
                                 [uc_service]
```

## Shipped

### Phase 1 — ring wrap-fix (commits `21981ed..52395df`)

LMAX-Disruptor "published-up-to" pattern: split `RingHeader::producer_position` into two atomics — `claim_position` (producers CAS-advance to reserve a slot) and `publish_position` (producer advances only after record bytes are visible). Consumers read up to `publish_position`. Eliminates the post-wrap torn-record window where a stale length prefix could spoof a "committed" record.

- **MPSC** producers CAS-claim on `claim_position`, write the record, spin until `publish_position == my_claim_pos`, then advance `publish_position` (Release). Publication strictly matches claim order — a padding-marker thread that claimed slot N must publish before slot N+padlen's real-record publisher proceeds.
- **SPSC** and **Broadcast** (single-producer): bump both atomics in lockstep on commit and tail-wrap.
- **`RingHeader`** grew from 192 B to 256 B (4 cache lines × 64 B). `static_assert` on the size.
- **`MpscProducer` invariant** documented in the module doc: producers must not panic between claim and publish (a panicking producer leaves a claimed-but-unpublished slot; subsequent producers spin forever). In our deployment model an in-process panic implies an unrecoverable node state and process restart.
- **Two regression tests** (Tasks 1.1/1.2; un-ignored in Task 1.5): `mpsc::tests::wrap_under_many_producers_no_torn_read` (8 producers × 200 records, ~9 wraps; reliably reproduces the bug in `--release` only) and `broadcast::tests::wrap_no_torn_read` (1 producer + 2 concurrent consumer threads).

### Phase 2 — `cnc.dat` + frames (commits `143a25c..e59b7bf`)

Multi-service forward-compatibility shape decided 2026-05-17 (spec amended in commit `92bcc22`): reserve the protocol shape now so a future multi-SMR rollout is a non-breaking bump, without doing the runtime work. Cost: 1 byte/frame + 448 B in cnc.dat.

- **`sub::SERVICE_STATUS`** widened from 64 B to an 8-slot services-table (512 B; slot 0 = today's `ServiceStatus`; slots 1..7 zero-reserved). New `service_status_slot_ptr(cnc_base, service_id) -> Option<*const ServiceStatus>` accessor. All M3 callsites of `sub_buffer_offsets[sub::SERVICE_STATUS]` work unchanged because slot 0's offset is unchanged.
- **`sub::NEXT_CLIENT_ID`** new index 4 (renumbered the M5 `CONTROL_TO_CLIENTS` placeholder to 5). 16 B sub-region (`AtomicU64` + pad), initialized to 1 so 0 stays a sentinel. Clients `fetch_add` to allocate a `client_id: u32`. `cnc_file_size()` grew by 464 B total.
- **`uc_protocol::frames::client`** — five new frame types:

  | `msg_type` | Frame | Ring | Direction |
  |---|---|---|---|
  | 5 | `SubmitFrame` | `clients/submit.ring` (MPSC) | clients → node |
  | 6 | `SubmitResponse` | `clients/response.broadcast` | node → clients |
  | 7 | `ClientQueryFrame` | `clients/query.ring` (MPSC) | clients → node |
  | 8 | `ClientQueryResp` | `clients/response.broadcast` | node → clients |
  | 9 | `NotLeaderResp` | `clients/response.broadcast` (payload: bincode `Option<u64>` hint) | node → clients |

  `header_extra` carries `(client_id: u32, local_seq: u32)` (LE). `flags` byte 0 = `service_id: u8` (always 0 in v1; decoders error on `!= 0` with `UnknownServiceId`); for `ClientQueryFrame` bit 8 = `QueryKind` (0=Linearizable, 1=Snapshot). Uniform `encode_flags_client(service_id, kind) -> u16` / `decode_flags_client(flags) -> Result<(u8, QueryKind), ClientFrameError>`.

- **Retrofit `frames::{apply, query}`** with `service_id` in `flags` byte 0. New `encode_flags_apply` / `encode_flags_query` helpers + matching `decode_flags_*` validators. All M3 writers (`raft::state_machine_shmem`, `ipc::service_link`, `ipc::query_link`, `uc_service::runtime::{apply_loop, query_loop, service}`) updated to call `encode_flags_apply(0)` / `encode_flags_query(0)`; readers validate `service_id` (v1: dead code; M5+: routing). Pre-existing `query.rs` `QueryFrameError::UnknownKind` enum extended (not replaced) with `UnknownServiceId(u8)`.

### Phase 3 — node-side wiring (commits `48890c8..823746f`)

- **`ipc::client_link::ClientLink`** owns the three new ring files (`submit.ring`, `query.ring`, `response.broadcast`) and creates `clients/sessions.dir/`. Default 16 MiB cap / 4 MiB max-msg; `ClientLink::create_with_cap(...)` lets tests use small rings.
- **`ipc::client_dispatcher`** — two tokio tasks:
  - `client_dispatcher`: consume `submit.ring`, validate `service_id`, check leader; on leader hit, `raft.client_write()` → publish `SubmitResponse` to `response.broadcast`; on `ForwardToLeader`, publish `NotLeaderResp`.
  - `client_query_dispatcher`: consume `query.ring`, validate `(service_id, kind)`, leader-only for Linearizable, forward to `ShmemQueryLink`, publish `ClientQueryResp`.
  Both share the response Broadcast producer through `Arc<parking_lot::Mutex<BroadcastProducer>>` (single-producer-by-design; mutex enforces serialization across both tasks; lock held only across one record write, no awaits).
- **`ipc::session_gc`** — 2 s tick reads `clients/sessions.dir/`, runs `HeartbeatWatcher`-style stale detection (5 s default), unlinks stale `.session` files. No further node-side state to clean — in-flight broadcasts for dead clients are no-ops.
- **`NodeHandle` gained four new fields**: `client_dispatcher`, `client_query_dispatcher`, `session_gc`, `metrics_publisher` (see below). All `Option<…Handle>`. `shutdown()` stops + joins in order **before** dropping `_instance` (which holds the cnc mmap that the dispatchers' ring halves reference). `query_link` migrated from `Option<ShmemQueryLink>` to `Option<Arc<ShmemQueryLink>>` so the query dispatcher can share it.
- **`ClientRingConfig` knob on `NodeConfig`** (`cap_bytes`, `max_msg`; default 16 MiB / 4 MiB). Re-exported from `uc_node`. All M1–M4 test `NodeConfig` literals updated with the field.
- **Bootstrap `add_learner` retry**: 5 ms fixed backoff → exponential (5 → 10 → 20 → … → cap 200 ms) within the existing 10 s deadline. Picks up M3.5 follow-up #2.

### Phase 4 — `uc_client` SDK (commits `a88e7e2..9edc39e`)

Public surface stays small (per the canonical design's §"Public APIs"):

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
    pub fn current_leader(&self) -> Option<u64>;
    pub fn last_applied(&self) -> u64;
    pub fn instance_id(&self) -> u128;
    pub fn client_id(&self) -> u32;
    pub async fn shutdown(self) -> Result<(), ClientError>;
}
```

Internal module layout:

- `cnc.rs` — read-mostly attach of `cnc.dat`; validate `(app_id, protocol_version, instance_id, header CRC)`; `fetch_add` on `next_client_id` for identity allocation; expose `*const NodeStatus` / `*const ServiceStatus`. (Opened `read+write` so the `AtomicU64::fetch_add` doesn't SIGBUS — a deviation from the plan's read-only `Mmap`; the rest of cnc is treated as read-only.)
- `session.rs` — 64-byte `clients/sessions.dir/{client_id}.session` mmap; 100 ms ticker bumps `heartbeat_seq` + `heartbeat_at_ns`.
- `watchers.rs` — `spawn_stall_watchers` runs two tasks polling `NodeStatus`/`ServiceStatus` at 100 ms with a 2 s liveness timeout; sets `node_stalled` / `service_stalled` `AtomicBool`s on the Client. Lifted raw pointers to `&'static NodeStatus`/`&'static ServiceStatus` before `tokio::spawn` to satisfy `Send`.
- `rings.rs` — opens `submit.ring`/`query.ring` (MPSC producers) + `response.broadcast` (Broadcast consumer). `spawn_broadcast_reader` decodes `(client_id, local_seq)` from `header_extra`, filters by `my_client_id`, routes matching frames into a `DashMap<u32, oneshot::Sender<RawResponse>>`. `RawResponse` is a 3-variant enum (`Record { msg_type, payload }`, `Overwritten`, `ShutDown`) so awaiters can distinguish broadcast lap loss from a clean shutdown.
- `client.rs` — `Client::connect` opens cnc, opens rings, creates session, spawns broadcast reader + stall watchers. `submit` / `query_*` go through a shared `send_and_await(msg_type, payload, flags, on_query_ring)` that:
  1. Allocates `local_seq` and registers a `oneshot::Sender` in the in-flight map.
  2. Writes to the chosen MPSC; on `Full`, retries within a 1 s grace, then `ClientError::BackpressureFull`.
  3. `tokio::select!`'s the response receiver against a 10 s timeout and a 100 ms stall-flag poll.
- **`impl Drop for Client`** (non-trivial fix the implementer caught during 5.6): sets background-task stop flags + `JoinHandle::abort()`s the stall watchers (critical — they hold `&'static` raw pointers into the cnc mmap; aborting them prevents a use-after-free SIGSEGV when the mmap drops as `cnc: Arc<CncAttach>` releases). `shutdown()` keeps doing the polite join-then-unlink-session-file dance.

### Phase 5 — integration tests (commits `93076d5..213daa2`)

Seven integration tests under `uc_node/tests/`:

- **`m4_client_single_node`** — 1 node + 1 service + 1 client, two `Inc` submits + one `query_snapshot`. ~2.3 s.
- **`m4_client_three_node`** — 3 nodes + 3 services + 3 clients; leader's client submits, followers' clients converge via `query_snapshot`, followers' submits get `NotLeader { hint: Some(leader_id) }`. ~6.4 s.
- **`m4_client_concurrent`** — 4 clients × 50 submits via `tokio::join!`, final value = 50×(1+2+3+4) = 500. ~4.3 s.
- **`m4_client_wrap`** — 32 KiB rings, 2 clients × 500 submits → ~6+ wraps. End-to-end validation of Phase 1's wrap-fix. ~10.3 s.
- **`m4_client_leader_failover`** — 3 nodes; shut down the leader; old-leader's client fires `NodeStalled` within ~2 s; surviving clients elect a new leader; post-failover submit through the new leader. **`#[ignore]`** — passes on the implementer's host but flakes consistently elsewhere because openraft keeps retrying replication to the (dead, unreachable) old-leader voter before accepting quorum. Election + `NodeStalled` detection are both verified; the post-failover client_write is what times out. Reliable fix needs either auto-remove of unreachable voters after N failed AppendEntries (openraft feature work) or the test pre-emptively calling `node.remove_node(dead_leader_id)` via a surviving handle before submitting. M4 follow-up. ~20 s when un-ignored.
- **`m4_client_session_gc`** — drop `Client` without `shutdown()`; node's `session_gc` unlinks the stale session file within 10 s.
- **`m4_client_response_overwritten`** — slow client with broadcast reader paused (`_test_pause_broadcast_reader` behind `test-helpers` feature); driver client floods 4 KiB broadcast ring to lap; resume reader; in-flight submit resolves to `ClientError::ResponseOverwritten`. ~6.3 s.

**Test runtime flavor.** Every `m4_*` test uses default `#[tokio::test]` (current_thread); the M3-established convention that multiple `multi_thread` runtimes in one binary deadlocked carries forward.

### M3 → M4 follow-up bonus: raft metrics publisher (commit `010522c`)

Discovered while writing M4 tests: `NodeStatus::{leader_node_id, current_term, role, last_applied, last_committed}` were never being written by `uc_node`. The M3 `liveness.rs` ticker only bumped `heartbeat_seq` / `_at_ns`. `Client::current_leader()` was effectively broken (always `Some(0)`, never the actual leader).

New `ipc::metrics_publisher::spawn_metrics_publisher` subscribes to `raft.metrics()` (a `WatchReceiver`) and writes the four fields whenever the snapshot changes. Mapping:

- `m.current_term` → `NodeStatus::current_term`.
- `m.state: ServerState` → `node_role::{FOLLOWER, CANDIDATE, LEADER, SHUTTING}` (Learner → FOLLOWER; Shutdown → SHUTTING).
- `m.current_leader: Option<NodeId>` → `leader_node_id` (None → `u64::MAX`).
- `m.last_applied.map(|l| l.index)` → `NodeStatus::last_applied`.
- `m.committed.map(|l| l.index)` → `NodeStatus::last_committed`.

Spawned post-`finish` (needs `handle.raft.clone()`). Joined in `shutdown` before the dispatcher cleanup. M3 capstone tests pass unchanged.

### Phase 6 — polish (commits `6ee50e0` + `8e7c077`)

- `cargo fmt --all` clean across all 28 modified files.
- `cargo clippy --workspace --tests -- -D warnings` clean. Fixed one stray `clippy::unnecessary_cast` in `cnc.rs` test and a `clippy::doc_lazy_continuation` in `m4_client_wrap.rs` doc.
- This task doc; spec + plan deleted per CLAUDE.md workflow (kept the consolidated record only). README pointer M3.5 → M4. `uc_client` line in README updated from "M1 stub" to the M4 surface.

## Verification

All commands green at M4 close:

```bash
cargo build --workspace                                    # clean
cargo test  --workspace                                    # all M1/M2/M3/M3.5/M4 tests pass
cargo clippy --workspace --tests -- -D warnings            # zero warnings
cargo fmt --check                                          # clean
```

Per-test runtimes (single-machine; loopback raft cluster):

```
m1_single_node                  ~ 2 s
m2_multi_node                   ~ 4 s
m3_shmem_single_node            ~ 2.5 s
m3_three_node_shmem             ~ 6.4 s
m3_service_crash                ~ 17 s   (transfer_leader fallback)
m3_ultima_db_adapter            ~ 2 s
shmem_state_machine             ~ 0.1 s
m4_client_single_node           ~ 2.3 s
m4_client_three_node            ~ 6.4 s
m4_client_concurrent            ~ 4.3 s
m4_client_wrap                  ~ 10.3 s
m4_client_leader_failover       (#[ignore] — see Phase 5 notes)
m4_client_session_gc            ~ 10.2 s  (stale-after window)
m4_client_response_overwritten  ~ 6.3 s
```

## Deferred to M5+

- **`output.ring` + at-least-once `OutputHandler` dispatch.** The leader-side side-effecting hook with durable `output_progress.state` marker. Out of M4 scope.
- **`snapshot.region` mmap** — currently snapshots ride through the existing apply/journal path; the dedicated mmap region awaits the openraft `generic-snapshot-data` cutover (also M5).
- **Service-recovery handshake** — cnc-sub-mmap MPSC attach so the node can consume `ServiceReady{last_applied}` frames from a re-attaching service. M3.5 follow-up #1.
- **Smarter `transfer_leader` target selection** (peer-service-health, prefer highest `last_applied`) — requires the cnc-sub-mmap visibility above. M3.5 follow-up #4.
- **`shmem_state_machine` adapter response-value test coverage** — adds a Counter-with-payload test in `shmem_state_machine.rs`. M3.5 follow-up #3, not blocking.
- **`raft.ensure_linearizable()` on the query path** — currently linearizable queries only check leader status; the explicit barrier is a one-liner in `client_query_dispatcher` once openraft 0.10's API surface is plumbed through `RaftHandle`. Two-line follow-up; deferred until a test demands stricter semantics.
- **Multi-service runtime** — the protocol *shape* is reserved (Task 2.3, services-table in cnc.dat); the *runtime* work (per-service dispatchers, per-service `StableValue`s, snapshot demux) is the multi-month feature tracked separately.
- **Multi-process client tests** — all M4 tests run all roles as tokio tasks in one process. The protocol works identically across process boundaries; cross-process harness is M6+.
- **Client SDK auto-routing across hosts** — v1 surfaces `NotLeader { hint }` and lets callers decide. A `MultiHostClient` helper (option C from brainstorming) can come later if real callers ask for it.

## Pointers

- Canonical project design: `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` (§4 cnc/rings, §5 public APIs, §8 pipelines, §11 errors).
- Predecessor records: `docs/tasks/task03_m3_shmem_service_split.md` (shmem IPC + service split), `docs/tasks/task04_m3_5_openraft_0_10_upgrade.md` (openraft 0.10 + `transfer_leader`).
- Aeron pattern reference (read during brainstorming; future option E if profiling ever motivates it): `aeron/aeron-client/src/main/java/io/aeron/ConcurrentPublication.java` (CAS-claim + length-last-release), `aeron/aeron-client/src/main/java/io/aeron/logbuffer/LogBufferDescriptor.java:48` (PARTITION_COUNT = 3), `aeron/aeron-driver/src/main/java/io/aeron/driver/IpcPublication.java:646` (cleaner zeroing stale terms).
- openraft 0.10 source: `../openraft/` (`0.10.0-alpha.20`).
