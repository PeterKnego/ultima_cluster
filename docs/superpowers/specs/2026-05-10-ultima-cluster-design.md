# ultima_cluster — Design Spec

**Status:** Draft (brainstorming complete, awaiting user review)
**Date:** 2026-05-10
**Workspace:** `ultima_cluster/` (peer of `ultima_db/`)
**Dependencies:** `ultima_journal` (task26), `ultima_db` (with `snapshot_stream`, task27), `openraft`, `quinn`

---

## 1. Goal

Build a reusable State Machine Replication application server on top of `openraft`. ultima_cluster ("UC") provides Raft consensus, log storage, snapshot transport, network plumbing, and process supervision; user applications provide deterministic business logic via a typed Rust SDK and run as separate processes that communicate with UC over shared memory.

The design follows industry best practice for low-latency clustered systems:

- A cluster engine process owns network and consensus.
- A service process owns business logic.
- Client processes own input handling.
- All same-host inter-process traffic is shared-memory IPC, not sockets.
- Cross-host traffic is the engine's responsibility, not the application's.

This split delivers (a) GC/scheduling isolation between the consensus engine and user code, (b) multi-language support for both service-side and client-side code via a stable shmem wire spec, (c) zero-copy hot paths between collaborating local processes.

---

## 2. SMR architecture (the user's mental model)

The system enforces the canonical SMR contract `(state, command) → (state', response)`:

1. **Input-logic** — user code in a separate client process. Receives external requests (HTTP, gRPC, MQTT, custom — entirely the user's choice), translates them into Commands, calls `Client::submit(cmd)` over shmem.
2. **State-manipulation-logic** — user's `StateMachine::apply(log_index, cmd) -> Response`, sync, deterministic, no I/O. Runs in `uc_service`.
3. **Output-logic** — user's `OutputHandler::on_committed(log_index, cmd, &state) -> Result<()>`, async, leader-only, retryable. Runs in `uc_service`.

UC owns the plumbing between phases (1) → (2) → (3): ordering, durability, replication, leader election, snapshot transport, output progress tracking.

---

## 3. Process model and workspace layout

### Three process roles, four crates

```
ultima_cluster/                 (workspace root)
├── uc_protocol/                # canonical wire spec; no_std-friendly
├── uc_node/                    # cluster engine (binary + lib)
├── uc_service/                 # service-side SDK (lib only)
├── uc_client/                  # local input-client SDK (lib only)
└── examples/                   # kv_node config + kv_service binary + kv_client binary
```

**`uc_protocol`** is the canonical artifact. It defines:
- The discovery directory layout.
- `cnc.dat` binary layout (header, sub-buffers, atomics positions).
- Ring buffer types (SPSC, MPSC, Broadcast) — implementations live here, not in `uc_node`.
- Per-message frame layouts for every ring (Apply, Query, Output, Submit, etc.).
- Liveness mechanics (heartbeat counters, timeout constants).
- Protocol version constants and compatibility checks.
- Stable error codes that cross the IPC boundary.

It is `no_std`-friendly — `core` only, no `tokio`, no `std::io`. This is the gate that enables non-Rust language bindings: any language that can `mmap` and respect the binary layout participates without dragging in tokio or openraft transitively.

**`uc_node`** is the cluster engine. Ships as both a library (`NodeBuilder`/`NodeHandle`) and a binary (`ultima-cluster-node`). Owns:
- Raft (via `openraft`).
- Log storage (via `ultima_journal` + raft-`StableValue`s).
- Inter-node QUIC network.
- The discovery directory and `cnc.dat`.
- Apply, output, query, client dispatchers (the goroutines that pump rings).
- Output-progress durability (`StableValue<u64>` next to vote/committed).

**`uc_service`** is the service-side SDK. User implements `StateMachine` (sync apply + sync query + snapshot in/out) and optionally `OutputHandler` (async, leader-only). The crate provides ring-buffer plumbing, the apply thread, the output tokio task, snapshot region marshaling. Provides `StoreStateMachine<…>` as a convenience adapter over `ultima_db::Store` with auto-pinning of `log_index → ultima_db version`.

**`uc_client`** is the local input-client SDK. Small dep set (no openraft, no quinn, no openraft type machinery). Provides `Client::submit`, `Client::query_linearizable`, `Client::query_snapshot`, plus low-cost direct reads of `cnc.dat` (`current_leader()`, `last_applied()`).

### Embedding mode (deferred to v1.x)

`uc_node` exports a library-mode `NodeBuilder` that allows running everything in a single process — apply happens via a direct trait call, no shmem rings, no separate service process. Same protocol shape, same semantics. Used for tests today; available to users who want a non-split deployment in v1.x. v1 priority is the separated mode.

---

## 4. Discovery directory and shared-memory layout

The shmem layer is the architectural centerpiece, following industry best practice for shared-memory IPC: a fixed-layout control file plus separate per-stream data buffer files, all memory-mapped, all coordinated through aligned atomics.

### Discovery directory

```
{instance_dir}/                                 # default: /dev/shm/ultima-{user}-{instance_name}
├── instance.lock                               # advisory flock; one node per instance
├── instance.toml                               # protocol_version, app_id, node_id, created_at, page_size
├── cnc.dat                                     # control plane
├── service/                                    # service ↔ node rings (1:1; v2 multi-tenant: service/{app_id}/…)
│   ├── apply.ring                              # node → service       SPSC
│   ├── apply_resp.ring                         # service → node       SPSC
│   ├── query.ring                              # node → service       SPSC (lin/snap multiplexed via header flag)
│   ├── query_resp.ring                         # service → node       SPSC
│   ├── output.ring                             # node → service       SPSC (leader-only producer)
│   ├── output_resp.ring                        # service → node       SPSC
│   └── snapshot.region                         # mmap'd; lifecycle managed
└── clients/
    ├── submit.ring                             # clients → node       MPSC
    ├── query.ring                              # clients → node       MPSC
    ├── response.broadcast                      # node → all clients   broadcast
    └── sessions.dir/                           # per-client session files (keepalive)
        └── {pid}-{nonce}.session
```

**Default `{instance_dir}`:**
- Linux: `/dev/shm/ultima-{user}-{instance_name}` (tmpfs).
- macOS (dev only): `/tmp/ultima-{user}-{instance_name}`.
- Override via node config; service and client must agree.

**`instance.lock`** — held exclusive by the running `uc_node`. Service and clients try shared lock as liveness probe. Disappearance = node gone.

### `cnc.dat` layout

Header is fixed-offset, atomically read; sub-buffer offsets/sizes in header point to per-region structures.

```
[file header — 256 bytes]
    magic              8 bytes   b"ULTCNC\0\0"
    protocol_version   u32       (semver-encoded)
    page_size          u32
    cnc_size_bytes     u64
    instance_id        u128      (random per node lifetime; clients use to detect node restart)
    app_id             64 bytes  (utf-8, null-padded)
    node_id            u64
    created_at_unix_ns u64
    sub_buffer_offsets [u64; 8]
    sub_buffer_sizes   [u64; 8]
    header_crc32       u32
    pad

[1: node_status]                  64 bytes; written by node only
    role               u32        (Initializing | Follower | Candidate | Leader | ShuttingDown)
    current_term       u64
    leader_node_id     u64        (u64::MAX = unknown)
    last_applied       u64
    last_committed     u64
    heartbeat_seq      u64        (incremented every 100 ms)
    heartbeat_at_ns    u64
    pad

[2: service_status]               64 bytes; written by service only
    state              u32        (Disconnected | Handshaking | Ready | Snapshotting | Stalled)
    last_applied       u64
    last_output_ack    u64
    heartbeat_seq      u64
    heartbeat_at_ns    u64
    service_pid        u32
    pad

[3: control_to_service]           ManyToOne ring (small) — RoleChanged, BootstrapRequested, BuildSnapshot, …
[4: control_to_node]              ManyToOne ring (small) — ServiceReady, SnapshotBuilt, OutputProgressMarker, …
[5: control_to_clients]           Broadcast (small) — LeaderChanged, NodeShuttingDown, AppIdMismatch
[6: counters_metadata]            label + type for each counter
[7: counters_values]              live counter array (observability scrape target)
[8: error_log]                    bounded ring-overwrite error log
```

### Ring buffer primitives (`uc_protocol::ring`)

**SPSC** — single producer, single consumer. Used for service↔node. Two cache-padded atomics (`producer_position`, `consumer_position`); fastest of the three. Writer claims slots via `fetch_add` on producer_position; reader uses relaxed loads.

**MPSC** — many producers, single consumer. Used for `clients/submit.ring` and `clients/query.ring`. Producers claim slots via CAS on producer_position; the single consumer reads with relaxed loads. Standard many-to-one lock-free ring buffer pattern.

**Broadcast** — single producer, many consumers, no backpressure (slow consumers may detect overwrites and report `OverwrittenByProducer`). Used for `clients/response.broadcast` and `cnc[5]`. Standard one-to-many broadcast buffer pattern.

**Per-ring header (128 bytes):**
```
magic              8 bytes
capacity_bytes     u64
max_msg_size       u32
msg_kind_filter    u32     (allow-list of msg kinds; mismatch rejected)
producer_position  u64     (cache-line padded)
consumer_position  u64     (cache-line padded)
pad
```

**Per-record framing:**
```
length_inclusive_header  u32     (atomic write — record visible only after this commits)
msg_type                 u16
flags                    u16
header_extra             8 bytes (per-msg-type; e.g. log_index for ApplyFrame)
payload                  variable
```

The `length_inclusive_header` atomic-after-write trick gives lock-free torn-record protection: reader sees length=0 → record not yet committed → spin/yield.

### Snapshot region

`service/snapshot.region` is a single mmap'd file used unidirectionally per snapshot operation:

- **Build** (service→node): node writes `BuildSnapshot{snapshot_id}` to `cnc[3]`; service streams snapshot bytes into the region as chunks with atomic-commit headers; service signals `SnapshotBuilt` via `cnc[4]`.
- **Install** (node→service): node spools incoming chunks from a peer's QUIC stream into the region; signals `InstallSnapshotReady` via `cnc[3]`; service installs and signals `RecoverFromSnapshot` via `cnc[4]`.

Resized via `ftruncate` if a snapshot exceeds current capacity; both processes detect via the header's size field and re-mmap. Cost paid per snapshot, not per record.

For `StoreStateMachine`, snapshot bytes flowing through this region are the same wire format as task27's `Store::snapshot_stream` — no translation.

### Liveness mechanism

Two heartbeats in `cnc.dat`:
- Node: increments `node_status.heartbeat_seq` every 100 ms.
- Service: increments `service_status.heartbeat_seq` every 100 ms.

Stall detection: if `heartbeat_seq` doesn't change for `liveness_timeout` (default 5 s, configurable per role), the watcher considers the peer dead. Cheap (memory loads only, no syscalls).

Per-client liveness uses session files under `clients/sessions.dir/`. Each client increments a counter inside its session file; node GC's stale session files. Closed sessions release any pending submit slots.

### App-id / instance-id / protocol-version handshake

All entries into the system check three fields from the cnc header:
- `app_id` must match the caller's expected app. Mismatch → refuse.
- `instance_id` cached by the caller on first attach. Different on reattach → node has restarted; reset session.
- `protocol_version` must be compatible (semver minor-or-equal).

---

## 5. Public API surfaces

### `uc_service::StateMachine` (the user implements)

```rust
pub trait StateMachine: Send + Sync + 'static {
    type Command:       Serialize + DeserializeOwned + Send + Sync + 'static;
    type Response:      Serialize + DeserializeOwned + Send + 'static;
    type Query:         Serialize + DeserializeOwned + Send + Sync + 'static;
    type QueryResponse: Serialize + DeserializeOwned + Send + 'static;

    /// MUST be deterministic. log_index doubles as ultima_db version when using StoreStateMachine.
    fn apply(&mut self, log_index: u64, cmd: Self::Command) -> Self::Response;

    /// Read-only against current applied state.
    fn query(&self, q: Self::Query) -> Self::QueryResponse;

    /// Highest log_index for which apply() completed AND is durable.
    /// StoreStateMachine derives this from Store::latest_version automatically.
    fn last_applied(&self) -> Option<u64>;

    fn build_snapshot(&self, dst: &mut dyn Write) -> Result<(), SnapshotError>;
    fn install_snapshot(&mut self, src: &mut dyn Read) -> Result<(), SnapshotError>;
}
```

### `uc_service::OutputHandler` (optional, leader-only)

```rust
#[async_trait]
pub trait OutputHandler<S: StateMachine>: Send + Sync + 'static {
    async fn on_committed(
        &self,
        log_index: u64,
        cmd: &S::Command,
        state: &S,
    ) -> Result<(), OutputError>;
}

pub enum OutputError {
    Retryable(String),   // retry while still leader
    Permanent(String),   // log warn, advance progress marker anyway
}

pub struct NoopOutput;   // OutputHandler<S> for any S
```

Delivery is at-least-once with a durable progress marker (`output_progress.state` on node side). On leader transition, the new leader scans `(last_completed, last_applied]` and re-runs `on_committed` for each — `log_index` is the natural idempotency key; user's responsibility to make `on_committed` idempotent.

### `uc_service::StoreStateMachine` (the ultima_db convenience adapter)

```rust
pub struct StoreStateMachine<C, R, Q, QR, FA, FQ> { /* … */ }

impl<C, R, Q, QR, FA, FQ> StoreStateMachine<C, R, Q, QR, FA, FQ>
where
    FA: Fn(&mut WriteTx<'_>, C) -> R + Send + Sync + 'static,
    FQ: Fn(&ReadTx<'_>, Q) -> QR + Send + Sync + 'static,
{
    pub fn new(store: Store, apply_fn: FA, query_fn: FQ) -> Self;
}
// + impl StateMachine — pins ultima_db version to log_index, wires snapshot_stream end-to-end.
```

### `uc_service::ServiceBuilder` (entry point)

```rust
pub struct ServiceBuilder<S: StateMachine> { /* … */ }

impl<S: StateMachine> ServiceBuilder<S> {
    pub fn new(config: ServiceConfig, state_machine: S) -> Self;
    pub fn output_handler<O: OutputHandler<S>>(self, h: O) -> Self;
    pub async fn run(self) -> Result<(), ServiceError>;       // blocks until shutdown
}

pub struct ServiceConfig {
    pub instance_dir: PathBuf,                   // must match node's
    pub app_id: String,
    pub data_dir: PathBuf,                       // service-side state (ultima_db checkpoints)
    pub liveness_timeout: Duration,              // default 5s
    pub apply_ring_capacity_bytes: usize,        // default 64 MiB
    // … other tuning
}
```

### `uc_client::Client`

```rust
pub struct Client { /* … */ }

impl Client {
    pub async fn connect(instance_dir: &Path, app_id: &str) -> Result<Self, ClientError>;

    pub async fn submit<C, R>(&self, cmd: &C) -> Result<R, ClientError>
        where C: Serialize, R: DeserializeOwned;

    pub async fn query_linearizable<Q, QR>(&self, q: &Q) -> Result<QR, ClientError>
        where Q: Serialize, QR: DeserializeOwned;

    pub async fn query_snapshot<Q, QR>(&self, q: &Q) -> Result<QR, ClientError>
        where Q: Serialize, QR: DeserializeOwned;

    pub fn current_leader(&self) -> Option<NodeId>;     // direct cnc.dat load
    pub fn last_applied(&self) -> u64;                  // direct cnc.dat load
    pub async fn shutdown(self);
}

pub enum ClientError {
    NotConnected, AppIdMismatch, ProtocolMismatch,
    NodeStalled, ServiceStalled, NotLeader { hint: Option<NodeId> },
    Submission(String), Decode(String), Timeout(Duration),
}
```

### `uc_node::NodeBuilder`

```rust
pub struct NodeBuilder { /* … */ }

impl NodeBuilder {
    pub fn new(config: NodeConfig) -> Self;
    pub async fn start(self) -> Result<NodeHandle, ClusterError>;
}

pub struct NodeConfig {
    pub node_id: NodeId,
    pub instance_dir: PathBuf,
    pub data_dir: PathBuf,                       // node-side: journal/, db/, *.state
    pub raft_listen_addr: SocketAddr,            // QUIC bind addr
    pub app_id: String,
    pub bootstrap: BootstrapConfig,              // Resume | SingleNode | Peers([…])
    pub raft: RaftTuning,
    pub liveness_timeout: Duration,
    pub metrics_listen_addr: Option<SocketAddr>,
    pub tls: TlsConfig,
}

pub struct NodeHandle { /* … */ }
impl NodeHandle {
    pub async fn add_learner(&self, node_id: NodeId, raft_addr: SocketAddr) -> Result<()>;
    pub async fn change_membership(&self, voters: BTreeSet<NodeId>) -> Result<()>;
    pub async fn remove_node(&self, node_id: NodeId) -> Result<()>;
    pub async fn current_leader(&self) -> Option<NodeId>;
    pub async fn manual_checkpoint(&self) -> Result<()>;
    pub async fn shutdown(self) -> Result<()>;
}

pub enum BootstrapConfig {
    Resume,                                      // default for re-running nodes
    SingleNode,                                  // dev/test
    Peers { peers: Vec<PeerSeed> },              // initial cluster members
}

pub struct PeerSeed {
    pub node_id: NodeId,
    pub raft_addr: SocketAddr,
}

pub enum TlsConfig {
    SelfSigned,                                  // generate at first start; default v1
    Files { cert: PathBuf, key: PathBuf, peer_ca: Option<PathBuf> },
}
```

---

## 6. Storage adapters (the openraft seam)

### Per-node disk layout (node-side)

```
{node.data_dir}/
├── journal/                       # raft log (ultima_journal)
│   └── seg-{base_seq:020}.log
├── vote.state                     # StableValue<openraft::Vote<NodeId>>
├── committed.state                # StableValue<openraft::LogId<NodeId>>
├── output_progress.state          # StableValue<u64> (last log_index for which on_committed completed)
├── membership.state               # StableValue<StoredMembership<NodeId>>
├── last_purged.state              # StableValue<openraft::LogId<NodeId>>
└── tls.crt / tls.key              # if TlsConfig::SelfSigned
```

Service has its own `data_dir` (separate from node's) for its own `ultima_db` checkpoints. The node holds **no** application state — its disk owns only Raft state. When a peer requests a snapshot, the node asks the service to build one and ships the bytes; nothing is cached locally. Two persistent stores in total: node owns Raft durability, service owns app state durability — both keyed by `log_index` for cross-process consistency.

### `RaftLogStorage` over `ultima_journal` (server feature)

```rust
pub struct JournalLogStorage {
    journal: Journal,
    vote: StableValue<Vote<NodeId>>,
    committed: StableValue<LogId<NodeId>>,
    last_purged: StableValue<LogId<NodeId>>,
}
```

Mapping (per [openraft docs §2.3](https://deepwiki.com/databendlabs/openraft/2.3-implementing-storage-traits)):

| openraft API | implementation |
|---|---|
| `save_vote(&Vote)` / `read_vote()` | `vote.store(v).wait()` / `vote.load()` |
| `save_committed(LogId)` / `read_committed()` | `committed.store(l).wait()` / `committed.load()` |
| `append(entries, callback)` | `Journal::append(seq=index, meta=term.0, payload=bincode(entry))` per entry; chain `Notifier::on_complete(callback)` for `IOFlushed` |
| `truncate(LogId)` | `Journal::truncate_after(log_id.index - 1)` |
| `purge(LogId)` | `Journal::purge_before(log_id.index + 1)` then `last_purged.store(log_id)` |
| `get_log_state()` | `(first_seq, last_seq)` from journal; first/last term from `meta` |
| `try_get_log_entries(range)` | `Journal::iter_range(...)`, decode `(seq, meta, payload)` to `Entry` (term comes from `meta` — no payload decode needed) |

The journal's `meta: u64` slot carrying entry term enables single-header-read responses to `get_log_state` and `get_key_log_ids` — exactly the use case task26 was designed for.

### `RaftStateMachine` adapter (bridges to apply ring)

```rust
pub struct AdaptedStateMachine {
    apply_dispatcher: ApplyDispatcherHandle,    // wraps service/apply.ring producer
    apply_resp_consumer: ApplyRespConsumer,     // wraps service/apply_resp.ring consumer
    last_applied: AtomicU64,                    // mirrored from cnc.dat::node_status
    last_membership: StoredMembership<NodeId>,
    snapshot_meta: SnapshotMeta<NodeId>,
}
```

Mapping:

| openraft API | implementation |
|---|---|
| `applied_state()` | `(last_applied, last_membership.clone())` |
| `apply(entries)` | for each entry: SPSC publish `ApplyFrame{log_index, payload_bytes}` → `service/apply.ring`; await SPSC consume from `service/apply_resp.ring` matching log_index. Membership entries also update `last_membership`. |
| `get_snapshot_builder()` | issues `BuildSnapshot{snapshot_id}` over cnc; wraps reader over `service/snapshot.region` |
| `begin_receiving_snapshot()` | clears `service/snapshot.region`; returns a writer handle |
| `install_snapshot(meta, snapshot)` | finalizes region; sends `InstallSnapshotReady` over cnc; awaits `RecoverFromSnapshot`; updates `last_applied`/`last_membership` |

### TypeConfig

```rust
openraft::declare_raft_types!(
    pub TypeConfig:
        D = AppCommand,                          // Bytes (refcounted; flows from journal through openraft to apply ring)
        R = AppResponse,                         // Bytes
        NodeId = u64,
        Node = NodeAddr { raft_addr: SocketAddr },
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = SnapshotRegionReader,
        AsyncRuntime = openraft::TokioRuntime,
);
```

`AppCommand = Bytes` is the zero-copy gate: a journal record's payload is wrapped with `Bytes::from(payload)` and flows through openraft without copies until the service's apply_loop bincode-decodes it from the apply ring's mmap'd memory.

### Snapshot trigger policy

Default: `openraft::SnapshotPolicy::LogsSinceLast(10_000)`. Configurable via `RaftTuning`. After a snapshot completes, openraft purges the log via `RaftLogStorage::purge` → `Journal::purge_before` (segment-aligned). For `StoreStateMachine`, the service additionally calls `Store::checkpoint()` post-snapshot so on-disk service state matches the just-snapshotted state.

---

## 7. Network layer (inter-node, QUIC)

### Connection model

- **One QUIC connection per peer-pair.** Long-lived. Reconnect on transport failure.
- **Multiple bidirectional streams per connection,** one per RPC class:
    - heartbeat
    - append-entries
    - vote
    - install-snapshot
- This is the no-head-of-line-blocking win: a stalled InstallSnapshot (slow follower) doesn't block heartbeats.

### Wire framing on top of QUIC streams

```
[per-message frame on a stream]
    msg_type        u8     (AppendEntriesReq | …Resp | VoteReq | …Resp | InstallSnapshotReq | …Resp | Handshake | HandshakeAck)
    flags           u8     (bit 0: is_response)
    request_id      u64
    body_len        u32
    body            (variable)
    body_crc32      u32
```

Handshake frame (sent first on every fresh connection): `app_id` + `node_id` of caller + protocol version. Mismatched values → drop.

### `AppendEntries` zero-copy on QUIC

`quinn::SendStream::write_chunks(&[Bytes])` does scatter-gather sends without internal copies. Leader reads journal records into `Bytes`, constructs per-entry meta as another `Bytes`, calls `write_chunks(&[header, meta_1, body_1, meta_2, body_2, …])`. CRC computed incrementally over each chunk before the trailing `frame_crc32`.

`AppendEntries` body shape:
```
[bincode header]         vote, prev_log_id, leader_commit, leader_id
entry_count              u32
[per entry, repeated]
    [bincode entry_meta] log_id (term, index), entry_kind tag
    body_len             u32
    body                 raw bytes (Bytes view into journal-read buffer)
```

### `InstallSnapshot` body — streaming

```
[per chunk]
    [bincode rpc_meta]  vote, snapshot_meta, offset, done
    chunk_len           u32
    chunk               raw bytes (from snapshot.region)
```

Receiver writes chunks into its own `service/snapshot.region`. On `done=true`, signals service via `InstallSnapshotReady`. On QUIC stream reset mid-stream → discard region content; openraft retries.

### TLS

`TlsConfig::SelfSigned` (v1 default): node generates self-signed cert at first start, stores `tls.crt`/`tls.key` in `data_dir`. Peers accept any cert with the matching `app_id` SAN — encryption-on-the-wire without a real PKI. Production deploys use `TlsConfig::Files`.

No insecure/cleartext mode is offered — `quinn` doesn't support it cleanly and the cost of TLS-over-QUIC is bounded.

### `RaftNetwork` impl

```rust
pub struct QuicRaftNetwork {
    endpoint: quinn::Endpoint,
    pool: Arc<Mutex<HashMap<NodeId, PeerLink>>>,
    app_id: String,
}
```

`PeerLink` owns the QUIC connection + lazily-opened bidirectional stream per RPC class + per-stream request_id allocator + response routing.

---

## 8. Pipelines (where every byte goes)

### Submit (write) — full path

```
[client process]
    Client::submit(cmd) → bincode encode → Bytes
        → MPSC claim slot in clients/submit.ring
        → write SubmitFrame{correlation_id, app_id_check, payload}
        → commit slot (atomic length write)
        → await on response.broadcast filtered by correlation_id

[uc_node process — client_dispatcher task]
    poll clients/submit.ring (single consumer)
        verify app_id_check
        if !is_leader: write NotLeader{leader_hint} → response.broadcast → done
    openraft.client_write(payload)                    // pushes into log
    on Future ready: response_bytes from apply
    write SubmitResponse{correlation_id, payload} → response.broadcast

[uc_node process — RaftStateMachine.apply]
    for each committed entry:
        SPSC publish ApplyFrame{log_index, payload} → service/apply.ring
        await SPSC consume from service/apply_resp.ring expecting log_index
    return responses to openraft → unblocks client_write Future

[uc_service process — apply_loop, sync thread]
    SPSC consume from service/apply.ring
    bincode decode payload → S::Command
    resp = state_machine.apply(log_index, cmd)        // SYNC, deterministic
    bincode encode resp
    SPSC publish ApplyRespFrame{log_index, response} → service/apply_resp.ring

[client process]
    broadcast subscription delivers SubmitResponse
        bincode decode payload → R, return to caller
```

**Honest copy count.** No copy crosses a process boundary — every cross-process hand-off is a shared-memory ring slot that producer and consumer view as the same physical pages. Within a process, a typical submit→response round-trip incurs ~4 in-process memcpy's: client-userland → submit.ring slot, journal record → apply.ring slot, state machine response → apply_resp.ring slot, apply_resp.ring → response.broadcast slot. Each is a single bounded write into mmap'd memory. Disk hops (journal append, journal read on apply) are unavoidable for any persistent log. The result is a no-syscall, bounded-memcpy hot path on the steady state.

### Query — linearizable

```
Client::query_linearizable(q)
    [client]   write QueryFrame{correlation_id, kind=Linearizable, payload} → clients/query.ring
    [node]     consume; if !is_leader: NotLeader → done
               openraft.ensure_linearizable().await
               SPSC publish QueryFrame{request_id, kind=Lin} → service/query.ring
               consume service/query_resp.ring → broadcast to client
    [service]  query_loop: read service/query.ring → state_machine.query(q) → publish resp
```

### Query — snapshot

Skip `ensure_linearizable`, skip leader-only check. Otherwise identical. Routed by `kind` flag in the QueryFrame.

### Output (leader-only, at-least-once)

```
[node — output_dispatcher, gated on is_leader]
    on becoming leader:
        last_completed = output_progress.state.load() or 0
        for index in last_completed+1 ..= last_applied (read from journal):
            publish OutputFrame{log_index, cmd_bytes} → service/output.ring
            consume service/output_resp.ring expecting log_index
            on Ok:        durable: output_progress.state.store(index).wait()
            on Retryable: backoff + retry while still leader
            on Permanent: warn-log, advance anyway
    on apply (steady-state leader): same loop, primed by apply_dispatcher

[service — output_loop, tokio task]
    consume service/output.ring
    output_handler.on_committed(log_index, &cmd, &state).await
    publish OutputResp{log_index, result} → service/output_resp.ring
```

The progress marker stays node-side (`output_progress.state`). Service is stateless about output progress — it just runs whatever the node sends. Clean leader transitions.

### Backpressure

| Ring | Full behavior |
|---|---|
| service/apply | apply_dispatcher awaits ring space → openraft.apply back-pressures naturally |
| service/apply_resp | service apply_loop blocks on publish; node's last_applied freezes; followers continue replicating; resumes when service drains |
| clients/submit | client awaits free slot with timeout → `ClientError::Timeout` on exhaustion |
| clients/response.broadcast | broadcast doesn't backpressure; slow client may detect overwrite → reports `ClientError::Decode("response overwritten")`; for committed submits, idempotent retry recovers |

---

## 9. Snapshot transfer

### Build (service → node, then node → cluster)

```
[node]    write BuildSnapshot{snapshot_id} → cnc[3] control_to_service
[service] state_machine.build_snapshot(&mut writer over service/snapshot.region)
          chunks written with atomic-commit headers
          on done: write SnapshotBuilt{snapshot_id, total_bytes, crc32} → cnc[4] control_to_node
[node]    read region; ship across cluster via QUIC install-snapshot stream
```

### Receive (peer → node → service)

```
[node]    receive chunks on QUIC install-snapshot stream → spool into service/snapshot.region
          on done: write InstallSnapshotReady{snapshot_id} → cnc[3]
[service] streaming reader over snapshot.region
          state_machine.install_snapshot(&mut reader)
          for ultima_db: store.install_snapshot_stream(reader, opts)
          on done: write RecoverFromSnapshot{last_applied} → cnc[4]
[node]    resume apply from last_applied + 1
```

The region is grown via `ftruncate`+remap if a snapshot exceeds current size. Both sides re-mmap on detected resize.

For ultima_db state machines, the bytes flowing through `snapshot.region` are exactly the wire format defined in task27.

---

## 10. Bootstrap and recovery

### Process startup ordering

**Node startup:**
1. Acquire `instance.lock` (exclusive flock). If held → `Err(InstanceAlreadyRunning)`.
2. Open/create `instance.toml`. Verify `node_id` and `app_id` match config (if existing), else write fresh.
3. Open `ultima_journal`, `StableValue`s (vote, committed, output_progress, last_purged, membership).
4. Reconcile journal vs. committed (replay any committed-but-not-yet-applied entries through the apply ring once service is up).
5. Create `cnc.dat`, write headers, allocate ring sub-buffers, generate fresh `instance_id` u128.
6. Create `service/`, `clients/` subdirs and ring files.
7. Open QUIC endpoint on `raft_listen_addr`.
8. Start runtime tasks (raft engine, dispatchers).
9. Begin writing heartbeats to `cnc.dat::node_status`.
10. Wait for service handshake (deadline = `bootstrap_service_timeout`, default 60 s). Refuse to take leader role until service is `Ready` (announce `state=Initializing`).
11. After service handshake completes: bootstrap per `BootstrapConfig` (Resume | SingleNode | Peers).

**Service startup:**
1. Open `instance.toml` — verify `protocol_version`, `app_id`, `node_id`. Mismatch → fail.
2. Try shared lock on `instance.lock`. Fail = no node running → wait + retry (or exit with `NodeNotRunning`).
3. Open `cnc.dat`, validate `instance_id`. Cache for session.
4. Mmap service rings. Validate ring headers.
5. Open service `data_dir` — load `ultima_db Store` (latest checkpoint). Determine `last_applied` from `state_machine.last_applied()`.
6. Write `service_status.last_applied`. Announce `state=Handshaking`.
7. Send `ServiceReady{last_applied}` via cnc control_to_node ring.
8. Wait for `RoleChanged` or first `ApplyFrame`.
9. Begin heartbeat loop.
10. Spawn apply_loop, query_loop, output_loop.

**Client startup:**
1. Open `cnc.dat` (read-only mmap). Validate `app_id`, `protocol_version`, `instance_id`.
2. Open client rings, validate.
3. Register a session file under `clients/sessions.dir/{pid}-{nonce}.session`.
4. Begin heartbeat into the session file.
5. Ready for submit/query.

### Service-vs-node `last_applied` reconciliation

On handshake, service sends its `last_applied`. Node:
- `service.last_applied < node.last_applied`: feed `(service.last_applied, node.last_applied]` through the apply ring to catch up. Responses discarded.
- `service.last_applied > node.last_applied`: state corruption — refuse handshake, log critical, operator intervenes.
- `service.last_applied < node.last_purged`: replay impossible — trigger snapshot install (from this node's local snapshot cache, or from a peer if needed).
- Equal: business as usual.

### Crash matrix

| Who crashes | Effect | Recovery |
|---|---|---|
| Service | Node sees missed heartbeats → marks Disconnected → if leader, voluntary leadership transfer; pause apply ring producer | Service restarts, opens cnc, handshakes with `last_applied`; node feeds backfill |
| Node | Service sees missed heartbeats → marks node Disconnected. Service idles. Clients see node stalled. | Node restarts, recreates `cnc.dat` with fresh `instance_id`. Service detects new `instance_id`, re-handshakes. Clients detect new `instance_id`, re-register sessions. |
| Both | As above | Whichever comes back first waits for the other |
| Client | Session file orphaned. Node GCs after timeout. | Client restarts, re-registers fresh session |

### Service-crash leader transfer (decision D)

When the leader's service dies, the node:
1. Detects via missed heartbeats.
2. Stops feeding apply ring (back-pressures openraft.apply naturally).
3. Calls `Raft::trigger_leader_transfer` to transfer to a node whose service is alive (preferring nodes the failure detector reports as healthy).
4. Continues participating as a follower; journal continues replicating from the new leader.
5. When the local service recovers, the node feeds backfill from `last_applied + 1` and re-joins normal apply.

---

## 11. Errors and observability

### Error taxonomy

Three error types, one per public crate. Cross-crate transport via stable `(error_code: u16, message: String)` codes defined in `uc_protocol::error_codes`.

```rust
// uc_node::error
pub enum ClusterError {
    Config(String), Recovery(...), Journal(...), StableValue(...),
    Db(...), Snapshot(...), Raft(String),    // openraft errors stringified — we don't expose the enum surface
    Network(...), NotLeader { leader_id, addr },
    Output(...), Ipc(IpcError), ServiceStalled,
    ShutDown, Io(...),
}

// uc_client::error
pub enum ClientError {
    NotConnected, AppIdMismatch, ProtocolMismatch,
    NodeStalled, ServiceStalled, NotLeader { hint },
    Submission(String), Decode(String), Timeout(Duration),
}

// uc_service::error
pub enum ServiceError { /* shmem attach, handshake, protocol mismatch + propagated OutputError/SnapshotError */ }
```

`OutputError::Permanent` advances the progress marker anyway (deliberate "give up, move on" escape hatch). `OutputError::Retryable` retries while still leader.

### Tracing

Spans gain a `process` attribute (`uc_node` | `uc_service` | `uc_client`). Cross-process correlation by `correlation_id` (client requests) and `log_index` (apply/output).

Span hierarchy:
```
cluster                                  (root, attrs: node_id, app_id, process)
├── apply_dispatcher (node)
│   └── apply{ index, kind }
├── apply_loop (service)
│   └── apply{ index }
├── output_dispatcher (node, leader only)
│   └── output{ index, attempt }
├── client_dispatcher (node)
│   └── submit{ correlation_id }
├── ipc_handshake
├── raft_rpc{ kind, peer, request_id }
└── snapshot{ id, role=build|install }
```

### Metrics inventory

```
# Apply
counter   uc_apply_entries_total{kind="normal|membership|blank"}
histogram uc_apply_duration_seconds                                 # service-side
gauge     uc_last_applied_index{process="node|service"}

# Raft state
gauge     uc_role{role="leader|follower|learner"}                   # 0/1
gauge     uc_current_term
gauge     uc_last_committed_index
gauge     uc_last_purged_index
gauge     uc_membership_size{kind="voter|learner"}

# Journal
counter   uc_journal_appends_total
histogram uc_journal_append_batch_size
histogram uc_journal_fsync_duration_seconds
gauge     uc_journal_segment_count
gauge     uc_journal_size_bytes

# Output
counter   uc_output_attempts_total{outcome="ok|retryable|permanent"}
histogram uc_output_duration_seconds
gauge     uc_last_output_completed_index
gauge     uc_output_lag_entries

# Inter-node QUIC
counter   uc_quic_rpc_total{kind="append|vote|install_snapshot", outcome}
histogram uc_quic_rpc_duration_seconds{kind}
counter   uc_quic_bytes_sent_total
counter   uc_quic_bytes_received_total
gauge     uc_quic_connections{peer}

# IPC
gauge     uc_ipc_ring_fill_pct{ring="apply|output|query|submit|response"}
counter   uc_ipc_handshake_attempts_total{outcome="ok|app_mismatch|protocol_mismatch|timeout"}
gauge     uc_service_state{state}
gauge     uc_node_heartbeat_age_ms{process="service|client"}        # client view of node freshness

# Snapshot
counter   uc_snapshot_built_total
histogram uc_snapshot_build_duration_seconds
counter   uc_snapshot_installed_total
histogram uc_snapshot_install_bytes
```

Prometheus HTTP exporter is on `uc_node` (`metrics_listen_addr`). Service exposes its own exporter on a different port. Different processes, different scrape targets.

### Diagnostic admin operations

`uc_node` exposes admin operations via `NodeHandle` (library API). For ops use, an admin CLI tool can either link `uc_node` directly or — as a v2 follow-up — connect to a running node via a dedicated admin shmem ring.

---

## 12. Testing strategy

### Two tiers

1. **In-process integration tests** — embedded mode (`NodeBuilder` with direct in-process `StateMachine`). Tests crate behavior, raft semantics, recovery, snapshot install. Fast, default `cargo test`.

2. **Multi-process integration tests** — under `tests/multi_process/`, gated behind `--features multi-process-tests`. Spawn `uc_node` + `uc_service` binaries as subprocesses on a tempdir instance. Validates handshake, liveness, crash semantics, real cnc layout.

### `MultiProcessCluster` harness

```rust
pub struct MultiProcessCluster {
    nodes: Vec<NodeProc>,
    services: Vec<ServiceProc>,
    instance_dirs: Vec<TempDir>,
}
impl MultiProcessCluster {
    pub async fn spawn_n(n: usize, app_id: &str) -> Self;
    pub fn kill_service(&mut self, node_idx: usize);
    pub fn kill_node(&mut self, node_idx: usize);
    pub async fn restart_service(&mut self, node_idx: usize);
    pub async fn restart_node(&mut self, node_idx: usize);
    pub async fn submit_via_local_client(&self, node_idx: usize, cmd: …) -> …;
}
```

### Test inventory

In-process (`tests/`):
- `single_node.rs` — bootstrap_single_node → submit → query → restart → state preserved
- `three_node_cluster.rs` — election, replication, divergence assert (every node's apply log identical)
- `leader_failover.rs` — kill leader, retry succeeds via NotLeader hint
- `snapshot_install.rs` — log growth on 2 nodes, add learner, transfer via region
- `output_at_least_once.rs` — output crash, recovery replay, idempotency
- `output_leader_failover.rs` — leader crashes mid-output; new leader picks up
- `membership_changes.rs` — add/remove/change persisted across restart
- `recovery_torn_apply.rs` — crash mid-apply; restart replays correctly
- `recovery_truncate_after_vote.rs` — conflicting log; truncate sentinel survives
- `network_zero_copy.rs` — assert AppendEntries body bytes contain journal payload bytes verbatim
- `client_retry.rs` — client retries on `NotLeader`, eventually succeeds
- `large_command.rs` — 16 MiB command survives full pipeline
- `concurrent_writes.rs` — many concurrent `submit` calls serialize through leader

Multi-process (`tests/multi_process/`):
- `handshake.rs` — service connects, handshakes, applies one entry, dies, reconnects
- `leader_transfer_on_service_death.rs` — confirms decision D
- `snapshot_install_real.rs` — real snapshot region transfer
- `concurrent_clients.rs` — many local clients submit via MPSC ring
- `node_restart_session_invalidation.rs` — clients detect new instance_id, re-register
- `instance_lock_exclusive.rs` — second node fails to start with InstanceAlreadyRunning

### Property-based / chaos / soak

Deferred. `proptest`-driven random sequences over `(submit | kill | restart | partition)` with linearizability checks live behind a `proptest` feature. Not v1 scope; tracked as a follow-up.

### What we deliberately don't test in v1

- Cross-host real-network failure (real packet loss via `tc`/`iptables`). Logical-level mocks only.
- Performance regression suite with absolute thresholds. Benches under `benches/` exist for relative comparison during development.
- Large-scale soak (multi-hour, hundreds of GB).

---

## 13. Decisions log

The brainstorming choices that locked this design:

1. **Single spec covering all subsystems** rather than API-first then storage/network.
2. **Hybrid typing** (option C): typed `Command`/`Response`/`Query`/`QueryResponse` in user code; bytes on the wire; framework handles bincode (de)serialization.
3. **Inter-node transport: QUIC via `quinn`** (option B); custom UDP+NAK is v2 work. TLS by default, self-signed mode for v1 dev/test ergonomics.
4. **Two traits** (option A): `StateMachine` (sync apply, sync query, snapshot) + `OutputHandler` (async, leader-only).
5. **Client request path: NotLeaderHint + client retry** (option A).
6. **Output delivery: at-least-once with durable progress marker** (option A); idempotency is the user's responsibility.
7. **Coupling to ultima_db: default-but-pluggable** (option B); `StoreStateMachine` adapter for the happy path.
8. **Bootstrap: programmatic primary + static peers seed** (option C).
9. **Reads via `Query` type, not closures** (post-pivot revision); same-process closure shortcuts dropped because of the IPC boundary.
10. **Process boundary scope: B (service split)** — `uc_node` and `uc_service` are separate processes; clients are also separate processes.
11. **Client transport: D (shmem-only, no remote clients)** — remote integration is the user's input-client process's job.
12. **Service-crash behavior: D** — keep replicating; voluntary leader transfer if leader.
13. **Multi-tenancy: C (1:1 in v1, layout supports v2 multi-tenant)** — directory paths use `{app_id}` from day one.

---

## 14. Open questions and follow-ups

- **Embedded mode in v1?** Currently: lib API exists; production priority is the separated mode. Tests use embedded mode internally. v1 ships both but the documentation foregrounds the separated mode.
- **Admin protocol for ops tooling.** v1: library-only via `NodeHandle`. v2: dedicated admin shmem ring + small CLI binary.
- **Multi-tenant v2.** Layout already supports it. Cost is mostly relaxing the "one service per node" check + per-app ring directories + per-app raft instances.
- **`ultima_transport` (raw UDP with NAK + flow control).** v2 subproject for sub-microsecond inter-node latency. Cluster's `RaftNetwork` abstraction will allow swap-in.
- **Property-based / linearizability testing.** Not in v1; tracked.
- **Cross-language SDKs.** `uc_protocol` is `no_std`-friendly to enable this. C bindings would be the first port; tracked separately.

---

## 15. Glossary

- **uc_node** — the cluster engine process. Owns Raft, log, network, shmem control plane.
- **uc_service** — the user's business-logic process. Implements `StateMachine`, `OutputHandler`. Connects to a node via shmem.
- **uc_client** — a user's input-client process. Submits commands and queries via shmem.
- **`uc_protocol`** — the wire spec crate. `no_std`-friendly; defines all binary layouts.
- **Instance directory** — the shmem-backed directory (typically `/dev/shm/...`) all participants share.
- **`cnc.dat`** — control plane file. Headers, status blocks, control rings, counters, error log.
- **`app_id`** — user-defined string identifying the application. Verified at every IPC boundary.
- **`instance_id`** — random u128 generated per node lifetime. Detect node restart.
- **SPSC / MPSC / Broadcast** — three ring buffer kinds in `uc_protocol::ring`.
- **Apply ring** — `service/apply.ring`; node→service entries to apply, in order.
- **Output ring** — `service/output.ring`; leader-only post-commit notifications.
- **Snapshot region** — `service/snapshot.region`; mmap'd file for snapshot byte transport.
- **Embedded mode** — `uc_node` running with an in-process `StateMachine` (no shmem rings, no separate service process).

---
