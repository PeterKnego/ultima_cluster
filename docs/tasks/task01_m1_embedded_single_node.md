# Task 01 — M1: Embedded Single-Node Skeleton

**Status:** Shipped 2026-05-10 (merge commit on `main`).
**Branch:** `feat/m1-embedded-single-node` (merged) — 20 commits.
**Workspace:** `ultima_cluster/` (peer of `ultima_db/`).

## Goal

Establish the workspace foundation and prove the openraft + `ultima_journal` seam end-to-end via a single-node SMR cluster that survives a restart with state preserved.

This is the first milestone of the broader ultima_cluster build sequence (M1-M5; canonical design at `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md`). M2 adds QUIC inter-node + multi-node bootstrap; M3 adds the shmem ring buffers + service-process split; M4 adds the local-shmem client; M5 adds output handler + production polish.

## Scope

**In M1:**
- Four-crate workspace (`uc_protocol`, `uc_service`, `uc_node`, `uc_client`).
- `uc_protocol` minimal stub (versions, error codes, magic constants; `no_std`-friendly).
- `uc_service::StateMachine` + `OutputHandler<S>` traits with full snapshot contract.
- `uc_node::NodeBuilder<S>` + `NodeHandle<S>` — embedded mode (in-process `StateMachine`; no shmem).
- `RaftLogStorage` impl over `ultima_journal::Journal` + three `StableValue`s (vote, committed, last_purged).
- `RaftStateMachine` impl that calls user's trait directly under a tokio Mutex.
- `BootstrapConfig::SingleNode` (initialize-or-resume idempotent) and `BootstrapConfig::Resume`.
- `NodeHandle::query_snapshot(closure)` for embedded reads.
- Capstone integration test: bootstrap → submit → query → restart → state preserved.

**Deferred to M2-M5:**
- QUIC inter-node network (M2). M1 ships a `NoopNetwork` placeholder whose RPC methods all `unreachable!()`.
- `BootstrapConfig::Peers` (M2). M1 returns `ClusterError::Config` if used.
- `query_linearizable` (M2 — needs openraft `ensure_linearizable()`).
- `uc_protocol` ring buffers + `cnc.dat` layout + service-process split (M3).
- `uc_client` real `Client` API (M4).
- `OutputHandler` wiring + at-least-once dispatcher (M5).
- `service/snapshot.region` mmap'd snapshot transport (M5). M1 uses in-memory `Cursor<Vec<u8>>`.
- Prometheus exporter (M5).

## Architecture

### Process model (M1)

Single process, single thread of `apply` execution, no shmem boundary. The `NodeHandle<S>` is generic over the user's `S: StateMachine`; the user instantiates it with their state machine via `NodeBuilder::new(config, sm).start().await`.

When M3 introduces the shmem split, a non-generic `NodeBuilder` will run alongside this generic one — the embedded path stays for testing and for users who want everything in-process.

### Crate map

```
ultima_cluster/
├── uc_protocol/                no_std-friendly wire spec
│   └── src/
│       ├── version.rs          ProtocolVersion (packed u32) + CURRENT + MIN_COMPATIBLE
│       ├── error_codes.rs      ErrorCode (#[repr(u16)], 13 variants)
│       └── magic.rs            CNC_MAGIC, RING_MAGIC, FRAME_MAGIC
├── uc_service/                 service-side SDK (trait surface only in M1)
│   └── src/
│       ├── state_machine.rs    StateMachine trait + invariants doc
│       ├── output_handler.rs   OutputHandler<S> + NoopOutput
│       └── error.rs            SnapshotError, OutputError (Retryable | Permanent)
├── uc_node/                    cluster engine (binary + lib)
│   └── src/
│       ├── config.rs           NodeConfig, BootstrapConfig (Resume | SingleNode | Peers), RaftTuning
│       ├── error.rs            ClusterError (with bincode 2 From impls)
│       ├── raft/
│       │   ├── mod.rs          TypeConfig (D=R=Bytes, NodeId=u64, Node=NodeAddr) + declare_raft_types!
│       │   ├── log_storage.rs  JournalLogStorage : RaftLogStorage + RaftLogReader (+ test_helpers::NoopNetwork)
│       │   └── state_machine.rs AdaptedStateMachine<S> : RaftStateMachine + RaftSnapshotBuilder
│       └── runtime/
│           ├── builder.rs      NodeBuilder<S>::{new, start}
│           ├── node.rs         NodeHandle<S>::{node_id, current_leader, submit, query_snapshot, shutdown}
│           └── recovery.rs     assert_consistent (last_seq >= last_purged.index)
└── uc_client/                  M1 stub (ClientError only); real API in M4
```

## Storage adapter mapping (`RaftLogStorage`)

`JournalLogStorage` wraps `ultima_journal::Journal` (raft log entries) plus three `StableValue`s. All fields are `Arc`-wrapped so `get_log_reader()` can return a clone. Mapping:

| openraft API | implementation |
|---|---|
| `save_vote(&Vote)` / `read_vote()` | `vote.store(v).wait()?` / `vote.load()?` |
| `save_committed(Option<LogId>)` / `read_committed()` | `committed.store(&id).wait()?` (Some) or `committed.clear().wait()?` (None) / `committed.load()?` |
| `append(entries, callback)` | per entry: `Journal::append(seq=index, meta=term, payload=bincode(entry))` under `append_lock`; chain final `Notifier::on_complete` into `LogFlushed::log_io_completed` |
| `truncate(LogId)` | `Journal::truncate_after(log_id.index.saturating_sub(1)).wait()?` |
| `purge(LogId)` | `Journal::purge_before(log_id.index)?` then `last_purged.store(&log_id).wait()?` |
| `get_log_state()` | `last_log_id` derived from `Journal::last_seq()` + per-record term in `meta`; `last_purged_log_id` from StableValue |
| `try_get_log_entries(range)` | `Journal::iter_range(range)`, bincode-decode each `(seq, meta, payload)` |

The journal's `meta: u64` slot carries the entry's term — `get_log_state` is a single header read, no full bincode decode. This is exactly the use case `task26_journal.md` was designed for.

The `append_lock: Arc<Mutex<()>>` satisfies the journal's caller-coordination caveat (no two threads ever submit the same seq); openraft already serializes appends, so contention is theoretical.

## State machine adapter (`RaftStateMachine`)

`AdaptedStateMachine<S>` wraps `Arc<tokio::sync::Mutex<Inner<S>>>` so the openraft engine and `NodeHandle` can both reach the same state via `Arc` clones. M3 will replace the direct `g.sm.apply(log_index, cmd)` call with a publish to a shmem ring; the surrounding decode/encode and lock-management stays the same.

| openraft API | implementation |
|---|---|
| `applied_state()` | `(last_applied, last_membership)` from Inner |
| `apply<I>(entries)` | per entry: bincode-decode `S::Command`, call `sm.apply(log_index, cmd)` (sync, deterministic), bincode-encode `S::Response`. Membership entries update `last_membership`. Blank entries push empty `Bytes`. |
| `get_snapshot_builder()` | returns `AdaptedSnapshotBuilder` sharing the Arc |
| `begin_receiving_snapshot()` | `Box::new(Cursor::new(Vec::new()))` |
| `install_snapshot(meta, snapshot)` | `sm.install_snapshot(&mut Cursor)`, sanity-check returned u64 vs `meta.last_log_id.index` via `debug_assert_eq!`, update Inner |
| `get_current_snapshot()` | clone of `current_snapshot` if any |
| `RaftSnapshotBuilder::build_snapshot()` | `sm.build_snapshot(&mut Vec<u8>)`, sanity-check returned u64 vs `last_applied.index`, package into `SnapshotMeta` + `Snapshot` |

The `debug_assert_eq!` on the user-returned `u64` honors the `StateMachine` trait's documented contract that `build_snapshot`/`install_snapshot` return the log_index represented (resolves the build-vs-apply race). With M1's Mutex serializing apply with build/install, the assertion is structurally trivial; M3's ring buffer weakens this and the asserts will need re-evaluation.

## Public API

```rust
// uc_service
pub trait StateMachine: Send + Sync + 'static {
    type Command:       Serialize + DeserializeOwned + Send + Sync + 'static;
    type Response:      Serialize + DeserializeOwned + Send + 'static;
    type Query:         Serialize + DeserializeOwned + Send + Sync + 'static;
    type QueryResponse: Serialize + DeserializeOwned + Send + 'static;

    fn apply(&mut self, log_index: u64, cmd: Self::Command) -> Self::Response;   // sync, deterministic
    fn query(&self, q: Self::Query) -> Self::QueryResponse;
    fn last_applied(&self) -> Option<u64>;
    fn build_snapshot(&self, dst: &mut dyn Write) -> Result<u64, SnapshotError>;     // returns log_index represented
    fn install_snapshot(&mut self, src: &mut dyn Read) -> Result<u64, SnapshotError>; // returns new last_applied
}

// uc_node
pub struct NodeBuilder<S: StateMachine> { /* … */ }
impl<S: StateMachine> NodeBuilder<S> {
    pub fn new(config: NodeConfig, state_machine: S) -> Self;
    pub async fn start(self) -> Result<NodeHandle<S>, ClusterError>;
}

pub struct NodeHandle<S: StateMachine> { /* … */ }
impl<S: StateMachine> NodeHandle<S> {
    pub fn node_id(&self) -> NodeId;
    pub async fn current_leader(&self) -> Option<NodeId>;
    pub async fn submit(&self, cmd: S::Command) -> Result<S::Response, ClusterError>;
    pub async fn query_snapshot<F, R>(&self, f: F) -> R
        where F: FnOnce(&S) -> R + Send, R: Send;
    pub async fn shutdown(self) -> Result<(), ClusterError>;
}
```

## Tests

11 tests in workspace:
- `uc_protocol`: 4 unit tests on `ProtocolVersion` (pack round-trip, compatibility cases).
- `uc_node` integration (`tests/log_storage_open.rs`): 5 — `reopen_observes_empty_state`, `save_and_read_vote_round_trip`, `save_and_read_committed_round_trip`, `append_then_read_round_trip`, `purge_retains_higher_indices`.
- `uc_node` integration (`tests/m1_single_node.rs`): 2 — `submit_query_works`, `state_survives_restart`.

Test infrastructure:
- `test-helpers` Cargo feature on `uc_node` exposes `_testonly_*` accessors on `JournalLogStorage`. Auto-activated for tests via dev-dependencies self-reference (`uc_node = { path = ".", features = ["test-helpers"] }`).
- `RaftLogStorageExt::blocking_append` is openraft's canonical test path used in the append round-trip test (because `LogFlushed::new` is `pub(crate)`).

## openraft 0.9.24 idiosyncrasies discovered

The `openraft = { version = "0.9", features = ["serde", "storage-v2"] }` workspace dep. The `storage-v2` feature is required for the `Sealed` supertrait pattern that gates `RaftLogStorage`/`RaftStateMachine` impls.

| Plan said | Reality (openraft 0.9.24) |
|---|---|
| `IOFlushed` callback | `LogFlushed` |
| `LogFlushed::log_io_completed` returns `Result<(), StorageIOError<NodeId>>` | Returns `Result<(), io::Error>` |
| `LogFlushed::new(...)` is constructible | It's `pub(crate)`; tests use `RaftLogStorageExt::blocking_append` |
| `StorageError::write` constructor | Doesn't exist; use `StorageIOError::{write_vote/read_vote/write_logs/read_logs/apply/write_snapshot/read_snapshot}.into()` |
| `#[async_trait]` on impls | Native `async fn` (openraft uses `#[add_async_trait]` which expands to native) |
| `RaftLogStorage::save_committed` takes `&LogId` | Takes `Option<LogId>` (with `clear()` for None) |
| `Vote::new(term, node_id)` | Confirmed (advanced leader-id mode, default features) |
| `LogId<u64>` clone | `Copy`; `.clone()` is unnecessary |
| `Cursor<Vec<u8>>` as `SnapshotData` | Works as-is — tokio implements `AsyncRead`/`AsyncWrite`/`AsyncSeek` for `std::io::Cursor<Vec<u8>>` directly |
| `bytes::Bytes` Serde | Requires `features = ["serde"]` on the `bytes` workspace dep |
| `NodeAddr: Default` | Required by `NodeEssential: Default`; manual impl returns `0.0.0.0:0` (unbindable sentinel) |
| `RaftError<NodeId, ClientWriteError<NodeId, C::Node>>` | Plan assumed `ClientWriteError<NodeId>` directly |
| `Raft::shutdown(&self)` | Takes `&self`, not `self`; returns `Result<(), JoinError>` |

These are documented inline in the source files as comments where the calls happen.

## Bugs caught during the two-stage subagent review

Both would have shipped without review:

1. **Task 10 — `purge` off-by-one** (`uc_node/src/raft/log_storage.rs`). Original `Journal::purge_before(log_id.index + 1)` retains records with seq > log_id.index + 1, dropping record at log_id.index + 1 — which raft expects to retain. Correct call is `purge_before(log_id.index)`. Fixed; regression test `purge_retains_higher_indices` added. Latent under M1 because `purge_before` is segment-aligned and M1 uses 64 MiB segments.

2. **Task 11 — silent discard of user's snapshot u64** (`uc_node/src/raft/state_machine.rs`). The trait's `build_snapshot`/`install_snapshot` return `Result<u64, SnapshotError>` specifically to "resolve the build-vs-apply race" (per trait doc). Adapter discarded the value. Fixed with `debug_assert_eq!` checks; M3's apply ring will need to re-evaluate when the Mutex stops serializing apply with snapshot ops.

Plus Task 8's missing `dev-dependencies` self-reference for `test-helpers` (broke `cargo test --workspace`).

## Notable design decisions

- **`NodeBuilder<S>` is generic** — M3 will introduce a non-generic shmem-fronted variant; the generic embedded path stays for tests and embedded users.
- **`AppCommand = AppResponse = bytes::Bytes`** — refcounted, zero-copy through openraft and into the apply path. The bincode-decode happens once inside the adapter; the `Bytes` itself is never re-encoded.
- **`std::sync::Mutex` for `append_lock`, `tokio::sync::Mutex` for `Inner<S>`** — append's body is fully synchronous (no `.await` inside the guard); the state machine adapter holds the lock across the user's sync apply call but the `lock().await` is itself an await point. Each Mutex is correct for its body shape.
- **Ed25519-style "sentinel that fails loud":** `NodeAddr::default()` returns `0.0.0.0:0` (unbindable). If a placeholder ever leaks into a real connect attempt, it errors immediately — no silent-routing-to-wrong-node failure mode.
- **`recovery::assert_consistent`** runs before `Raft::new`. Catches `last_seq < last_purged.index` (data dir corruption). Fast-fail with a meaningful `ClusterError::Recovery` message.

## Forward-compat hazards for M2+

- **`NodeAddr` serde format break:** the struct currently has only `raft_addr`. M2 likely adds `client_addr`. Adding a serde struct field with bincode is a wire-format break — persisted membership at M1 contains a single node with placeholder addr, so M2 needs an explicit migration step (versioned wrapper, `#[serde(default)]`, or one-shot data-dir wipe).
- **Single `instance.lock`** doesn't exist yet (M3 work). Two `uc_node` processes against the same data dir will conflict on the journal segment files and fail loudly, but the failure mode is messier than necessary.
- **`AdaptedStateMachine` lock contention:** the apply Mutex is held across the entire batch. M3 (apply ring) makes this irrelevant; until then, large batches can starve `query_snapshot`.

## Build/test/lint commands

```bash
cargo build --workspace
cargo test --workspace                                       # 11 tests
cargo clippy --workspace --all-targets -- -D warnings        # zero warnings
cargo doc --workspace --no-deps                              # builds; 5 rustdoc warnings on unbacktick'd <S> generics
```

## Pointers

- Canonical design: `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` (covers M1-M5).
- Upstream contracts: `../ultima_db/docs/tasks/task26_journal.md` (`ultima_journal`), `../ultima_db/docs/tasks/task27_snapshot_stream.md` (`ultima_db` snapshot wire format — used in M3+).
- openraft 0.9.24 source (cargo registry cache) for storage trait shapes.
