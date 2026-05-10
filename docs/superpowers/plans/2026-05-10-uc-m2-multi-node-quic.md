# M2: Multi-Node + QUIC Inter-Node Transport — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace M1's `NoopNetwork` placeholder with a real `RaftNetwork` impl over QUIC (`quinn`), wire `BootstrapConfig::Peers` + membership-change APIs on `NodeHandle`, and prove the result with a 3-node cluster test suite (election, replication, leader failover, snapshot install on new follower).

**Architecture:** One persistent QUIC connection per peer-pair, multiple bidirectional streams per connection (one per RPC class — heartbeat / append-entries / vote / install-snapshot). TLS self-signed by default. Wire framing on top of QUIC streams uses length-prefixed message frames. `AppendEntries` body uses `quinn::SendStream::write_chunks(&[Bytes])` for scatter-gather (zero-copy of the journal record bytes carrying the user's `Bytes` payload through openraft).

**Tech Stack:** Rust 2024 edition, `openraft` 0.9.24 (already on `storage-v2`), `quinn` 0.11.x, `rustls` 0.23.x, `rcgen` 0.13.x for self-signed certs, existing `ultima_journal` + `bytes` + `tokio`.

**Reference:** Canonical design at `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` Section 7 ("Network layer (inter-node, QUIC)"). M1 task record at `docs/tasks/task01_m1_embedded_single_node.md` for the existing crate layout, openraft 0.9.24 quirks already discovered, and the three M2-task-0 follow-up items from the M1 independent review.

**Out of scope for M2 (deferred to M3-M5):**
- `uc_protocol` ring buffers + `cnc.dat` layout (M3).
- `uc_service` shmem split (M3).
- `uc_client` real implementation (M4).
- `OutputHandler` wiring + at-least-once dispatcher (M5).
- `service/snapshot.region` mmap'd transport (M5; M2 still streams snapshots through openraft's `Cursor<Vec<u8>>`).
- Prometheus exporter (M5).
- Multi-process tests (M3).

---

## File Structure

```
ultima_cluster/
├── Cargo.toml                          # add quinn, rustls, rcgen, rustls-pemfile workspace deps
├── uc_node/
│   ├── Cargo.toml                      # add the workspace deps
│   └── src/
│       ├── config.rs                   # MODIFY: add TlsConfig enum + field on NodeConfig
│       ├── error.rs                    # MODIFY: add Network(NetworkError) variant
│       ├── raft/
│       │   ├── log_storage.rs          # MODIFY: remove test_helpers::NoopNetwork (moved out)
│       │   ├── state_machine.rs        # MODIFY (Task 2): durable install_snapshot
│       │   └── mod.rs                  # potentially MODIFY (Task 1, Task 3)
│       ├── network/                    # NEW MODULE
│       │   ├── mod.rs                  # public re-exports + NetworkError
│       │   ├── tls.rs                  # self-signed cert generation + rustls configs
│       │   ├── frame.rs                # wire framing: msg_type, request_id, body_len, body, crc32
│       │   ├── codec.rs                # encode/decode for AppendEntriesReq/Resp, VoteReq/Resp, InstallSnapshotReq/Resp
│       │   ├── server.rs               # QUIC listener; accepts inbound RPC streams
│       │   ├── client.rs               # per-peer QUIC connection wrapper; request/response correlation
│       │   ├── factory.rs              # QuicRaftNetworkFactory : RaftNetworkFactory; pool of peer connections
│       │   └── instance.rs             # QuicRaftNetwork : RaftNetwork; sends RPCs on a peer connection
│       └── runtime/
│           ├── builder.rs              # MODIFY: instantiate QuicRaftNetwork instead of NoopNetwork; spawn server task
│           └── node.rs                 # MODIFY: add_learner, change_membership, remove_node methods
└── uc_node/tests/
    ├── log_storage_open.rs             # unchanged
    ├── m1_single_node.rs               # unchanged
    ├── frame_roundtrip.rs              # NEW: wire framing encode/decode tests
    └── m2_multi_node.rs                # NEW: 3-node cluster integration tests
```

Decisions locked here:
- The `network/` module is a sibling of `raft/` and `runtime/` under `uc_node/src/`, not its own crate. M2 does not split into more crates.
- Frame layout uses a single binary format for all RPC kinds (per spec §7 wire framing block); per-kind body codecs are in `codec.rs`.
- One bidirectional QUIC stream per RPC class per peer connection (4 streams total: heartbeat/append/vote/install-snapshot). Stream reuse — not stream-per-RPC — to keep latency low.
- `NoopNetwork` lives only in M1 and is **removed** in this milestone (no more single-node-only tests requiring it; embedded mode is exercised via M1's existing tests which don't traverse the network).
- TLS is `SelfSigned` by default (per spec). `TlsConfig::Files` and `TlsConfig::Insecure` are out of scope for M2 (Files arrives in M5 production polish; Insecure is never offered).

---

## Task 1: Verify `get_log_state` synthetic node_id is safe

The M1 independent review flagged that `get_log_state` synthesizes `CommittedLeaderId::new(term, 0)` with `node_id=0` as a placeholder, and worried this could break vote comparisons under M2 multi-node. The Task 10 review counter-claimed this is safe because openraft 0.9.24's default `leader_id_std` mode discards `node_id` in `CommittedLeaderId` (the constructor does `let _ = node_id;` and the struct holds only `term`).

Resolve definitively before any multi-node code lands.

**Files:**
- Read: `~/.cargo/registry/src/*/openraft-0.9.24/src/vote/leader_id/leader_id_std.rs`
- Read: `~/.cargo/registry/src/*/openraft-0.9.24/src/vote/committed_leader_id.rs` (or wherever `CommittedLeaderId` is defined)
- Document findings in code comments in `uc_node/src/raft/log_storage.rs`

- [ ] **Step 1: Locate the openraft source**

Run: `find ~/.cargo/registry/src -name 'leader_id_std.rs' -path '*openraft-0.9.24*' 2>/dev/null`
Expected: prints the file path. If not found, run `cargo doc -p openraft --open` to fetch the crate, then retry.

- [ ] **Step 2: Read `leader_id_std.rs` and `committed_leader_id.rs`**

Confirm:
- `CommittedLeaderId<NID>` in `leader_id_std` mode is `{ term: u64, p: PhantomData<NID> }` (no actual NID storage).
- `CommittedLeaderId::new(term: u64, node_id: NID)` discards `node_id` via `let _ = node_id;` or equivalent.
- `PartialOrd`/`Ord` on `CommittedLeaderId` compares only `term`.

If the source matches this description, the synthetic `node_id=0` is **provably benign** and the M1 implementation is correct.

If the source DIFFERS (e.g., `CommittedLeaderId` actually carries `NID`, or comparisons use both), this is a **real bug** and the fix is to recover the real `node_id` by bincode-decoding the last entry payload in `get_log_state` and reading the `LogId::leader_id::node_id` field.

- [ ] **Step 3: Document the finding in `uc_node/src/raft/log_storage.rs`**

In the `get_log_state` body (currently around line 189), update the comment block that explains the `node_id=0` placeholder. Replace the existing comment (whatever it currently says) with:

```rust
// CommittedLeaderId::new(term, node_id=0) is safe here because openraft 0.9.24's
// default leader_id mode (leader_id_std, no `single-term-leader` feature) defines
// CommittedLeaderId<NID> = { term, PhantomData<NID> } with ordering by `term` only.
// Verified at openraft-0.9.24/src/vote/leader_id/leader_id_std.rs (see Task 1 of
// M2 plan for the audit trail). If openraft ever switches the default to
// `single-term-leader = false` storing real node_id, this synthesis must be
// replaced with a bincode-decode of the last entry's LogId.leader_id.
```

(Adjust wording to match the actual file path you confirmed in Step 2.)

If Step 2 found the synthesis IS unsafe, instead implement the fix (recover real node_id via bincode-decode of last entry) and replace the comment with documentation of the corrected approach.

- [ ] **Step 4: Build, clippy, test**

Run from `/Users/peter/Projects/ultima/ultima_cluster/`:
```bash
cargo build --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

All should be clean. 11 tests should still pass.

- [ ] **Step 5: Commit**

If the comment-only change is sufficient:
```bash
git add uc_node/src/raft/log_storage.rs
git commit -m "docs(uc_node): document get_log_state node_id=0 safety (M1 review item)"
```

If a real fix was needed:
```bash
git add uc_node/src/raft/log_storage.rs
git commit -m "fix(uc_node): recover real node_id in get_log_state (M1 review blocker)"
```

---

## Task 2: Durable `install_snapshot`

The M1 independent review flagged that `AdaptedStateMachine::install_snapshot` updates `last_applied`, `last_membership`, and `current_snapshot` only in memory. When M2 follower receives a snapshot then crashes, recovery comes up with `last_applied = None` and openraft tries to replay from index 1 — but the journal was purged past that point, breaking recovery.

Fix: persist `last_applied`, `last_membership`, and the snapshot bytes durably before `install_snapshot` returns Ok.

**Files:**
- Modify: `uc_node/src/raft/log_storage.rs` — add two new StableValues (`last_applied`, `current_snapshot_meta`) and a snapshot bytes file.
- Modify: `uc_node/src/raft/state_machine.rs` — accept the StableValues, persist on install_snapshot.
- Modify: `uc_node/src/runtime/builder.rs` — wire the new StableValues into the adapter.

- [ ] **Step 1: Add fields to `JournalLogStorage`**

In `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/raft/log_storage.rs`, add two new `StableValue` fields:

```rust
pub(crate) last_applied: Arc<StableValue<LogId<NodeId>>>,
pub(crate) snapshot_meta: Arc<StableValue<StoredSnapshotMeta>>,
```

Where `StoredSnapshotMeta` is a new type defined at the top of the file:

```rust
use openraft::StoredMembership;
use super::NodeAddr;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredSnapshotMeta {
    pub last_log_id: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, NodeAddr>,
    /// Path to the snapshot bytes file, relative to data_dir.
    pub bytes_filename: String,
}
```

Open both in `JournalLogStorage::open()` alongside the existing three:

```rust
let last_applied = Arc::new(StableValue::open(StableValueConfig {
    path: data_dir.join("last_applied.state"),
    durability: Durability::Consistent,
    max_payload_bytes: 4096 - 17,
})?);

let snapshot_meta = Arc::new(StableValue::open(StableValueConfig {
    path: data_dir.join("snapshot_meta.state"),
    durability: Durability::Consistent,
    max_payload_bytes: 4096 - 17,
})?);
```

And add to the struct initializer.

- [ ] **Step 2: Add test-only accessors for the two new StableValues**

Mirror the existing `_testonly_vote` / `_testonly_committed` / `_testonly_last_purged` accessors in the same impl block:

```rust
#[cfg(any(test, feature = "test-helpers"))]
pub fn _testonly_last_applied(&self) -> &StableValue<LogId<NodeId>> { &self.last_applied }
#[cfg(any(test, feature = "test-helpers"))]
pub fn _testonly_snapshot_meta(&self) -> &StableValue<StoredSnapshotMeta> { &self.snapshot_meta }
```

- [ ] **Step 3: Build to confirm the storage layer compiles**

Run: `cargo build -p uc_node`
Expected: clean. New StableValues are present but unused yet.

- [ ] **Step 4: Wire the StableValues into `AdaptedStateMachine`**

In `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/raft/state_machine.rs`, add fields to `Inner<S>`:

```rust
pub(crate) last_applied_sv: Arc<StableValue<LogId<NodeId>>>,
pub(crate) snapshot_meta_sv: Arc<StableValue<StoredSnapshotMeta>>,
pub(crate) snapshot_bytes_dir: PathBuf,
```

(Add `use std::path::PathBuf;` and `use crate::raft::log_storage::StoredSnapshotMeta;` and the StableValue/PathBuf imports.)

Update `AdaptedStateMachine::new` to take a `LogStorageHandles` struct carrying these (defined in `log_storage.rs`):

```rust
// In log_storage.rs:
pub struct LogStorageHandles {
    pub last_applied: Arc<StableValue<LogId<NodeId>>>,
    pub snapshot_meta: Arc<StableValue<StoredSnapshotMeta>>,
    pub data_dir: PathBuf,
}
```

And `JournalLogStorage` gets a method:
```rust
pub fn handles(&self, data_dir: PathBuf) -> LogStorageHandles {
    LogStorageHandles {
        last_applied: self.last_applied.clone(),
        snapshot_meta: self.snapshot_meta.clone(),
        data_dir,
    }
}
```

Then change `AdaptedStateMachine::new(sm)` to `AdaptedStateMachine::new(sm, handles: LogStorageHandles)`. Update `Inner::new` accordingly. The two existing M1 callers (`builder.rs` and the embedded `query_snapshot` shortcut) will need updating in Task 2 Step 6.

- [ ] **Step 5: Persist on install_snapshot**

In `install_snapshot`'s body (around `state_machine.rs:155`), after the user's `g.sm.install_snapshot(&mut cursor)?` returns Ok, persist:

```rust
// Write snapshot bytes to a uniquely-named file in data_dir.
let bytes_filename = format!("snapshot_{}.bin",
    meta.last_log_id.map(|l| l.index).unwrap_or(0));
let bytes_path = g.snapshot_bytes_dir.join(&bytes_filename);
std::fs::write(&bytes_path, &bytes)
    .map_err(|e| StorageIOError::<NodeId>::read_snapshot(Some(meta.signature()), &e))?;
// fsync the file for durability (write returns before fsync).
let f = std::fs::File::open(&bytes_path)
    .map_err(|e| StorageIOError::<NodeId>::read_snapshot(Some(meta.signature()), &e))?;
f.sync_all()
    .map_err(|e| StorageIOError::<NodeId>::read_snapshot(Some(meta.signature()), &e))?;
drop(f);

// Persist last_applied (if present).
if let Some(lid) = meta.last_log_id {
    g.last_applied_sv.store(&lid)
        .map_err(|e| StorageIOError::<NodeId>::read_snapshot(Some(meta.signature()),
            &std::io::Error::other(e.to_string())))?
        .wait()
        .map_err(|e| StorageIOError::<NodeId>::read_snapshot(Some(meta.signature()),
            &std::io::Error::other(e.to_string())))?;
}

// Persist snapshot meta (atomic pointer to the bytes file).
let stored_meta = StoredSnapshotMeta {
    last_log_id: meta.last_log_id,
    last_membership: meta.last_membership.clone(),
    bytes_filename: bytes_filename.clone(),
};
g.snapshot_meta_sv.store(&stored_meta)
    .map_err(|e| StorageIOError::<NodeId>::read_snapshot(Some(meta.signature()),
        &std::io::Error::other(e.to_string())))?
    .wait()
    .map_err(|e| StorageIOError::<NodeId>::read_snapshot(Some(meta.signature()),
        &std::io::Error::other(e.to_string())))?;

// Now the in-memory state is durable; update Inner.
g.last_applied = meta.last_log_id;
g.last_membership = meta.last_membership.clone();
g.current_snapshot = Some(StoredSnapshot { meta: meta.clone(), data: bytes });
Ok(())
```

- [ ] **Step 6: Update startup recovery to load durable state**

In `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/raft/state_machine.rs::AdaptedStateMachine::new`, after creating `Inner`, populate it from the StableValues:

```rust
// Recover persisted state on startup.
let loaded_last_applied = handles.last_applied.load().ok().flatten();
let loaded_snapshot_meta = handles.snapshot_meta.load().ok().flatten();

let (last_membership, current_snapshot) = match loaded_snapshot_meta {
    Some(meta) => {
        // Reload the bytes file.
        let bytes_path = handles.data_dir.join(&meta.bytes_filename);
        let bytes = std::fs::read(&bytes_path).unwrap_or_default();
        let openraft_meta = openraft::SnapshotMeta {
            last_log_id: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            snapshot_id: format!("snap-{}",
                meta.last_log_id.map(|l| l.index).unwrap_or(0)),
        };
        (meta.last_membership,
         Some(StoredSnapshot { meta: openraft_meta, data: bytes }))
    }
    None => (StoredMembership::default(), None),
};

// If snapshot exists, also install it into the user's state machine.
let mut sm = sm;
if let Some(ref snap) = current_snapshot {
    let mut cursor = std::io::Cursor::new(snap.data.clone());
    let _ = sm.install_snapshot(&mut cursor);  // ignore errors; openraft will resync
}

Self {
    inner: Arc::new(Mutex::new(Inner {
        sm,
        last_applied: loaded_last_applied,
        last_membership,
        current_snapshot,
        last_applied_sv: handles.last_applied,
        snapshot_meta_sv: handles.snapshot_meta,
        snapshot_bytes_dir: handles.data_dir,
    })),
}
```

(Replace the current `Inner::new` shape accordingly.)

- [ ] **Step 7: Update the builder to pass handles**

In `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/runtime/builder.rs`, in `start()`:

```rust
let log_storage = JournalLogStorage::open(&self.config.data_dir)?;
crate::runtime::recovery::assert_consistent(&log_storage)?;

let handles = log_storage.handles(self.config.data_dir.clone());
let sm_adapter = AdaptedStateMachine::new(self.state_machine, handles);
```

- [ ] **Step 8: Build, clippy, test**

```bash
cargo build --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

All 11 existing tests should still pass.

- [ ] **Step 9: Add a snapshot-survives-restart test**

Append to `/Users/peter/Projects/ultima/ultima_cluster/uc_node/tests/log_storage_open.rs`:

```rust
use uc_node::raft::log_storage::StoredSnapshotMeta;

#[tokio::test]
async fn snapshot_meta_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let storage = JournalLogStorage::open(dir.path()).expect("open");

    // Initially None.
    assert!(storage._testonly_snapshot_meta().load().expect("load").is_none());

    // Store a snapshot meta.
    let meta = StoredSnapshotMeta {
        last_log_id: Some(openraft::LogId::new(
            openraft::CommittedLeaderId::new(5, 0), 100)),
        last_membership: openraft::StoredMembership::default(),
        bytes_filename: "snapshot_100.bin".into(),
    };
    storage._testonly_snapshot_meta().store(&meta).expect("store")
        .wait().expect("wait");

    // Reopen and verify.
    drop(storage);
    let storage = JournalLogStorage::open(dir.path()).expect("reopen");
    let loaded = storage._testonly_snapshot_meta().load().expect("load after reopen");
    assert!(loaded.is_some());
    let loaded = loaded.unwrap();
    assert_eq!(loaded.bytes_filename, "snapshot_100.bin");
    assert_eq!(loaded.last_log_id.map(|l| l.index), Some(100));
}
```

Run: `cargo test -p uc_node --test log_storage_open`
Expected: 6 passes (the existing 5 + the new one).

- [ ] **Step 10: Commit**

```bash
git add uc_node/src/raft/log_storage.rs uc_node/src/raft/state_machine.rs uc_node/src/runtime/builder.rs uc_node/tests/log_storage_open.rs
git commit -m "fix(uc_node): durable install_snapshot (StableValue + bytes file)"
```

---

## Task 3: Resolve canonical `last_applied`

The M1 review flagged that `StateMachine` trait declares `fn last_applied(&self) -> Option<u64>` but the adapter never queries it — the adapter tracks its own `last_applied` and divergence is silent. M3's `ultima_db::Store` adapter will make this load-bearing.

Decide canonical-vs-adapter, document the choice, and either remove or cross-check.

**Files:**
- Modify: `uc_service/src/state_machine.rs` — update trait doc
- Modify: `uc_node/src/raft/state_machine.rs` — add startup cross-check

**Decision:** keep the trait method (it's the user's source of truth) and have the adapter **cross-check** the user's value against persisted `last_applied` at startup. Surface mismatches as `ClusterError::Recovery`.

- [ ] **Step 1: Update the trait doc on `last_applied`**

In `/Users/peter/Projects/ultima/ultima_cluster/uc_service/src/state_machine.rs`, replace the doc comment on `fn last_applied`:

```rust
    /// Returns the highest log_index the user's state machine has DURABLY applied.
    /// MUST agree with the framework's persisted last_applied at startup.
    ///
    /// The framework cross-checks this method at startup against the durable
    /// `last_applied.state`. Disagreement = data corruption, surfaced as
    /// `ClusterError::Recovery`.
    fn last_applied(&self) -> Option<u64>;
```

- [ ] **Step 2: Add the cross-check in `AdaptedStateMachine::new`**

In `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/raft/state_machine.rs::AdaptedStateMachine::new`, after loading `loaded_last_applied` from the StableValue (per Task 2 Step 6), call `sm.last_applied()` and compare:

```rust
let user_la = sm.last_applied();
let framework_la = loaded_last_applied.map(|l| l.index);

match (user_la, framework_la) {
    (Some(u), Some(f)) if u != f => {
        return Err(/* see Step 3 for the error path */);
    }
    (None, Some(_)) => {
        // User says fresh state but framework has persisted history.
        // This is OK only if the user just installed a snapshot they couldn't
        // re-derive last_applied from (allowed). Log warn but don't fail.
        tracing::warn!(framework_last_applied = ?framework_la,
            "framework has persisted last_applied but user reports None — \
             likely after install_snapshot; assuming framework is authoritative");
    }
    (Some(u), None) => {
        // User says they're caught up but framework has no record.
        // Surface as recovery error.
        return Err(/* see Step 3 */);
    }
    _ => {} // both None or both Some with same value — fine
}
```

Issue: `AdaptedStateMachine::new` currently doesn't return Result. Either change the signature to return `Result<Self, ClusterError>` (and update the builder to `?` the call), OR put the check elsewhere. The simpler option: change `AdaptedStateMachine::new` to return `Result<Self, ClusterError>`.

- [ ] **Step 3: Add `ClusterError::DriftDetected` variant**

In `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/error.rs`, add to `ClusterError`:

```rust
#[error("state drift: user last_applied={user:?} but framework last_applied={framework:?}")]
DriftDetected { user: Option<u64>, framework: Option<u64> },
```

Use this in `AdaptedStateMachine::new` cross-check.

- [ ] **Step 4: Update `AdaptedStateMachine::new` signature**

Change:
```rust
pub fn new(sm: S, handles: LogStorageHandles) -> Self { … }
```

To:
```rust
pub fn new(sm: S, handles: LogStorageHandles) -> Result<Self, crate::ClusterError> { … }
```

- [ ] **Step 5: Update the builder to handle the Result**

In `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/runtime/builder.rs`:

```rust
let sm_adapter = AdaptedStateMachine::new(self.state_machine, handles)?;
```

- [ ] **Step 6: Build, clippy, test**

```bash
cargo build --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

12 tests pass (11 + new snapshot_meta_survives_reopen from Task 2).

- [ ] **Step 7: Commit**

```bash
git add uc_service/src/state_machine.rs uc_node/src/error.rs uc_node/src/raft/state_machine.rs uc_node/src/runtime/builder.rs
git commit -m "feat(uc_node): cross-check user vs framework last_applied at startup"
```

---

## Task 4: TLS infrastructure (self-signed cert generation)

Add the workspace deps for QUIC + TLS, implement `TlsConfig::SelfSigned` certificate generation via `rcgen`, and persist generated certs to the data dir.

**Files:**
- Modify: `Cargo.toml` (workspace root) — add `quinn`, `rustls`, `rcgen`, `rustls-pemfile` workspace deps.
- Modify: `uc_node/Cargo.toml` — pull in the deps.
- Create: `uc_node/src/network/mod.rs` (module skeleton)
- Create: `uc_node/src/network/tls.rs`

- [ ] **Step 1: Add workspace deps**

In `/Users/peter/Projects/ultima/ultima_cluster/Cargo.toml`, append to `[workspace.dependencies]`:

```toml
# QUIC + TLS (M2)
quinn = { version = "0.11", default-features = false, features = ["runtime-tokio", "rustls-ring"] }
rustls = { version = "0.23", default-features = false, features = ["ring", "tls12", "std"] }
rcgen = { version = "0.13", default-features = false, features = ["pem", "ring"] }
rustls-pemfile = "2"
```

(Pin to a known-good version. quinn 0.11.x is current as of writing; verify the latest patch with `cargo search quinn` if needed.)

- [ ] **Step 2: Add to `uc_node/Cargo.toml`**

In `/Users/peter/Projects/ultima/ultima_cluster/uc_node/Cargo.toml` `[dependencies]`:

```toml
quinn = { workspace = true }
rustls = { workspace = true }
rcgen = { workspace = true }
rustls-pemfile = { workspace = true }
```

- [ ] **Step 3: Create the network module skeleton**

Create `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/network/mod.rs`:

```rust
//! QUIC inter-node transport for ultima_cluster.
//!
//! Replaces M1's `NoopNetwork` placeholder. One persistent QUIC connection
//! per peer-pair, multiple bidirectional streams per connection (one per
//! RPC class). TLS self-signed by default.

pub mod client;
pub mod codec;
pub mod factory;
pub mod frame;
pub mod instance;
pub mod server;
pub mod tls;

pub use factory::QuicRaftNetworkFactory;
pub use instance::QuicRaftNetwork;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum NetworkError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("tls: {0}")]
    Tls(String),
    #[error("quic connect: {0}")]
    Connect(String),
    #[error("quic stream: {0}")]
    Stream(String),
    #[error("rpc timeout")]
    Timeout,
    #[error("decode: {0}")]
    Decode(String),
    #[error("disconnected")]
    Disconnected,
    #[error("certificate: {0}")]
    Cert(String),
}
```

- [ ] **Step 4: Create the TLS module**

Create `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/network/tls.rs`:

```rust
//! Self-signed cert generation and rustls client/server configs.
//!
//! M2 only supports `TlsConfig::SelfSigned`: generates a fresh cert at first
//! start, writes `tls.crt` + `tls.key` to data_dir, accepts any peer cert
//! that lists the configured `app_id` as a SAN.

use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::{ClientConfig, ServerConfig};

use super::NetworkError;

/// Generates a fresh self-signed cert + key. Returns (cert_pem, key_pem).
/// The cert's CN is "ultima_cluster" and SAN includes the app_id.
pub fn generate_self_signed(app_id: &str)
    -> Result<(String, String), NetworkError>
{
    let mut params = rcgen::CertificateParams::new(vec![
        app_id.to_string(),
        "ultima_cluster".to_string(),
        "localhost".to_string(),
    ]).map_err(|e| NetworkError::Cert(format!("params: {e}")))?;
    params.distinguished_name.push(rcgen::DnType::CommonName, "ultima_cluster");

    let key_pair = rcgen::KeyPair::generate()
        .map_err(|e| NetworkError::Cert(format!("keygen: {e}")))?;
    let cert = params.self_signed(&key_pair)
        .map_err(|e| NetworkError::Cert(format!("sign: {e}")))?;

    Ok((cert.pem(), key_pair.serialize_pem()))
}

/// Open or initialize cert/key files under data_dir.
/// Returns the DER-decoded cert and key.
pub fn load_or_init(data_dir: &Path, app_id: &str)
    -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>), NetworkError>
{
    let cert_path = data_dir.join("tls.crt");
    let key_path = data_dir.join("tls.key");

    if !cert_path.exists() || !key_path.exists() {
        let (cert_pem, key_pem) = generate_self_signed(app_id)?;
        std::fs::write(&cert_path, &cert_pem)?;
        std::fs::write(&key_path, &key_pem)?;
    }

    let cert_pem = std::fs::read(&cert_path)?;
    let key_pem = std::fs::read(&key_path)?;

    let mut cert_reader = cert_pem.as_slice();
    let cert = rustls_pemfile::certs(&mut cert_reader)
        .next()
        .ok_or_else(|| NetworkError::Cert("no cert in tls.crt".into()))?
        .map_err(|e| NetworkError::Cert(format!("parse cert: {e}")))?;

    let mut key_reader = key_pem.as_slice();
    let key = rustls_pemfile::pkcs8_private_keys(&mut key_reader)
        .next()
        .ok_or_else(|| NetworkError::Cert("no pkcs8 key in tls.key".into()))?
        .map_err(|e| NetworkError::Cert(format!("parse key: {e}")))?;

    Ok((cert, PrivateKeyDer::Pkcs8(key)))
}

/// Build a rustls ServerConfig that accepts client certs from any peer
/// presenting our self-signed cert (or no client cert in M2 for simplicity).
pub fn build_server_config(
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
) -> Result<Arc<ServerConfig>, NetworkError> {
    let cfg = ServerConfig::builder()
        .with_no_client_auth()       // M2: trust the QUIC handshake's encryption only
        .with_single_cert(vec![cert], key)
        .map_err(|e| NetworkError::Tls(format!("server config: {e}")))?;
    Ok(Arc::new(cfg))
}

/// Build a rustls ClientConfig that accepts any cert (since we're using
/// self-signed certs that peers won't have in their trust store).
/// M5 production polish replaces this with a real CA path.
pub fn build_client_config() -> Result<Arc<ClientConfig>, NetworkError> {
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::ServerName;
    use rustls::DigitallySignedStruct;
    use rustls::SignatureScheme;

    #[derive(Debug)]
    struct AcceptAnything;
    impl ServerCertVerifier for AcceptAnything {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: rustls::pki_types::UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }
        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }
        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }
        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            vec![
                SignatureScheme::RSA_PKCS1_SHA256,
                SignatureScheme::RSA_PKCS1_SHA384,
                SignatureScheme::RSA_PKCS1_SHA512,
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::ECDSA_NISTP384_SHA384,
                SignatureScheme::ECDSA_NISTP521_SHA512,
                SignatureScheme::ED25519,
                SignatureScheme::RSA_PSS_SHA256,
                SignatureScheme::RSA_PSS_SHA384,
                SignatureScheme::RSA_PSS_SHA512,
            ]
        }
    }

    let cfg = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnything))
        .with_no_client_auth();
    Ok(Arc::new(cfg))
}
```

(Adjust to rustls 0.23 actual API — the trait shapes shown above are from rustls 0.23 docs but may need minor adjustment.)

- [ ] **Step 5: Stub the other network module files**

Create `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/network/frame.rs`:
```rust
//! Filled in by Task 5.
```

Create `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/network/codec.rs`:
```rust
//! Filled in by Task 6.
```

Create `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/network/server.rs`:
```rust
//! Filled in by Task 8.
```

Create `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/network/client.rs`:
```rust
//! Filled in by Task 9.
```

Create `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/network/factory.rs`:
```rust
//! Filled in by Task 10.
```

Create `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/network/instance.rs`:
```rust
//! Filled in by Task 11.
```

- [ ] **Step 6: Add `TlsConfig` to `NodeConfig`**

In `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/config.rs`, add:

```rust
#[derive(Debug, Clone)]
pub enum TlsConfig {
    /// Generate a self-signed cert at first start; persist to data_dir/tls.{crt,key}.
    SelfSigned,
}

impl Default for TlsConfig {
    fn default() -> Self { Self::SelfSigned }
}
```

And add to `NodeConfig`:
```rust
pub tls: TlsConfig,
```

(Add a doc comment: "TLS configuration for inter-node QUIC. M2 supports SelfSigned; Files arrives in M5.")

- [ ] **Step 7: Add `Network` variant to `ClusterError`**

In `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/error.rs`, add:

```rust
#[error("network: {0}")]
Network(#[from] crate::network::NetworkError),
```

(Place it near the other infrastructure errors.)

- [ ] **Step 8: Enable `pub mod network` in `uc_node/src/lib.rs`**

Add `pub mod network;` alongside the existing modules.

- [ ] **Step 9: Update test `NodeConfig` constructions**

Existing tests at `uc_node/tests/m1_single_node.rs` construct `NodeConfig` with all fields. Update the `cfg()` helper to include `tls: TlsConfig::default()`.

In the same file's `cfg` helper:
```rust
fn cfg(data_dir: PathBuf, bootstrap: BootstrapConfig) -> NodeConfig {
    NodeConfig {
        node_id: 1,
        data_dir,
        raft_listen_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        app_id: "counter-test".into(),
        bootstrap,
        raft: RaftTuning::default(),
        tls: TlsConfig::default(),     // NEW
    }
}
```

Add `use uc_node::TlsConfig;` to the imports.

- [ ] **Step 10: Re-export `TlsConfig` from `uc_node::lib`**

In `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/lib.rs`:
```rust
pub use config::{BootstrapConfig, NodeConfig, NodeId, PeerSeed, RaftTuning, TlsConfig};
```

- [ ] **Step 11: Build, clippy, test**

```bash
cargo build --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

All tests still pass (12 from Task 2). TLS module is unused yet but compiles.

- [ ] **Step 12: Add a unit test for cert generation**

Append to `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/network/tls.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn generate_self_signed_succeeds() {
        let (cert, key) = generate_self_signed("test-app").expect("gen");
        assert!(cert.contains("BEGIN CERTIFICATE"));
        assert!(key.contains("BEGIN PRIVATE KEY"));
    }

    #[test]
    fn load_or_init_creates_files_first_time() {
        let dir = TempDir::new().unwrap();
        let (_cert, _key) = load_or_init(dir.path(), "test-app").expect("init");
        assert!(dir.path().join("tls.crt").exists());
        assert!(dir.path().join("tls.key").exists());
    }

    #[test]
    fn load_or_init_idempotent_on_second_call() {
        let dir = TempDir::new().unwrap();
        let (cert1_der, _key1_der) = load_or_init(dir.path(), "test-app").expect("init 1");
        let cert1_bytes = std::fs::read(dir.path().join("tls.crt")).unwrap();

        let (cert2_der, _key2_der) = load_or_init(dir.path(), "test-app").expect("init 2");
        let cert2_bytes = std::fs::read(dir.path().join("tls.crt")).unwrap();

        // Second call returns the same cert (didn't regenerate).
        assert_eq!(cert1_bytes, cert2_bytes);
        assert_eq!(cert1_der.as_ref(), cert2_der.as_ref());
    }
}
```

Run: `cargo test -p uc_node tls::tests`
Expected: 3 passed.

- [ ] **Step 13: Commit**

```bash
git add Cargo.toml uc_node/
git commit -m "feat(uc_node): TLS infrastructure for QUIC (self-signed cert generation)"
```

---

## Task 5: Wire framing

Implement the message frame layout (per spec §7 wire framing block) used on every QUIC stream.

**Files:**
- Modify: `uc_node/src/network/frame.rs`
- Create: `uc_node/tests/frame_roundtrip.rs`

- [ ] **Step 1: Implement frame layout**

Replace `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/network/frame.rs`:

```rust
//! Wire framing on top of QUIC streams.
//!
//! Each stream carries a sequence of length-prefixed frames. The frame header
//! is fixed-size; the body is variable. CRC32 covers the body.
//!
//! Frame layout:
//!
//! ```text
//!     msg_type        u8     (MessageType enum)
//!     flags           u8     (bit 0: is_response)
//!     request_id      u64    (correlator for multiplexed in-flight requests)
//!     body_len        u32    (length of body in bytes)
//!     body            (variable)
//!     body_crc32      u32    (CRC over body)
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::NetworkError;

#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MessageType {
    AppendEntriesReq    = 1,
    AppendEntriesResp   = 2,
    VoteReq             = 3,
    VoteResp            = 4,
    InstallSnapshotReq  = 5,
    InstallSnapshotResp = 6,
    Handshake           = 10,
    HandshakeAck        = 11,
}

impl MessageType {
    pub const fn from_u8(v: u8) -> Result<Self, ()> {
        match v {
            1 => Ok(Self::AppendEntriesReq),
            2 => Ok(Self::AppendEntriesResp),
            3 => Ok(Self::VoteReq),
            4 => Ok(Self::VoteResp),
            5 => Ok(Self::InstallSnapshotReq),
            6 => Ok(Self::InstallSnapshotResp),
            10 => Ok(Self::Handshake),
            11 => Ok(Self::HandshakeAck),
            _ => Err(()),
        }
    }
}

const HEADER_LEN: usize = 1 + 1 + 8 + 4;     // 14 bytes
const TRAILER_LEN: usize = 4;                 // body_crc32

pub struct Frame {
    pub msg_type: MessageType,
    pub flags: u8,
    pub request_id: u64,
    pub body: Bytes,
}

impl Frame {
    pub fn new_request(msg_type: MessageType, request_id: u64, body: Bytes) -> Self {
        Self { msg_type, flags: 0, request_id, body }
    }
    pub fn new_response(msg_type: MessageType, request_id: u64, body: Bytes) -> Self {
        Self { msg_type, flags: 1, request_id, body }
    }
    pub fn is_response(&self) -> bool { self.flags & 1 != 0 }

    /// Encode the frame as a `BytesMut`. Includes header + body + CRC32 trailer.
    pub fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(HEADER_LEN + self.body.len() + TRAILER_LEN);
        buf.put_u8(self.msg_type as u8);
        buf.put_u8(self.flags);
        buf.put_u64(self.request_id);
        buf.put_u32(self.body.len() as u32);
        buf.put_slice(&self.body);
        let crc = crc32fast::hash(&self.body);
        buf.put_u32(crc);
        buf
    }

    /// Decode a frame from a buffer that has at least `HEADER_LEN` bytes;
    /// returns the frame and the number of bytes consumed.
    /// Returns Err if there isn't enough data yet.
    pub fn decode(buf: &mut Bytes) -> Result<Frame, NetworkError> {
        if buf.len() < HEADER_LEN {
            return Err(NetworkError::Decode(format!(
                "need {HEADER_LEN} bytes for header, have {}", buf.len())));
        }
        let msg_type_byte = buf.get_u8();
        let msg_type = MessageType::from_u8(msg_type_byte)
            .map_err(|_| NetworkError::Decode(format!("unknown msg_type {msg_type_byte}")))?;
        let flags = buf.get_u8();
        let request_id = buf.get_u64();
        let body_len = buf.get_u32() as usize;
        if buf.len() < body_len + TRAILER_LEN {
            return Err(NetworkError::Decode(format!(
                "need {body_len}+{TRAILER_LEN} body bytes, have {}", buf.len())));
        }
        let body = buf.copy_to_bytes(body_len);
        let crc_actual = buf.get_u32();
        let crc_expected = crc32fast::hash(&body);
        if crc_actual != crc_expected {
            return Err(NetworkError::Decode(format!(
                "crc mismatch: expected {crc_expected}, got {crc_actual}")));
        }
        Ok(Frame { msg_type, flags, request_id, body })
    }

    /// Read a frame from an `AsyncRead` source (e.g., `quinn::RecvStream`).
    pub async fn read_async<R>(reader: &mut R) -> Result<Frame, NetworkError>
    where R: tokio::io::AsyncRead + Unpin
    {
        use tokio::io::AsyncReadExt;
        let mut header = [0u8; HEADER_LEN];
        reader.read_exact(&mut header).await?;
        let mut header_buf = Bytes::copy_from_slice(&header);
        let msg_type_byte = header_buf.get_u8();
        let msg_type = MessageType::from_u8(msg_type_byte)
            .map_err(|_| NetworkError::Decode(format!("unknown msg_type {msg_type_byte}")))?;
        let flags = header_buf.get_u8();
        let request_id = header_buf.get_u64();
        let body_len = header_buf.get_u32() as usize;

        let mut body_vec = vec![0u8; body_len];
        reader.read_exact(&mut body_vec).await?;
        let mut crc_buf = [0u8; TRAILER_LEN];
        reader.read_exact(&mut crc_buf).await?;
        let crc_actual = u32::from_be_bytes(crc_buf);
        let crc_expected = crc32fast::hash(&body_vec);
        if crc_actual != crc_expected {
            return Err(NetworkError::Decode(format!(
                "crc mismatch: expected {crc_expected}, got {crc_actual}")));
        }
        Ok(Frame { msg_type, flags, request_id, body: Bytes::from(body_vec) })
    }
}
```

Add `crc32fast` to `uc_node/Cargo.toml` (already used by `ultima_journal` so it's in the lockfile; just declare the dep):

```toml
crc32fast = "1"
```

- [ ] **Step 2: Create the roundtrip test**

Create `/Users/peter/Projects/ultima/ultima_cluster/uc_node/tests/frame_roundtrip.rs`:

```rust
use bytes::Bytes;
use uc_node::network::frame::{Frame, MessageType};

#[test]
fn encode_decode_empty_body() {
    let frame = Frame::new_request(MessageType::VoteReq, 42, Bytes::new());
    let encoded = frame.encode();
    let mut bytes = encoded.freeze();
    let decoded = Frame::decode(&mut bytes).expect("decode");
    assert_eq!(decoded.msg_type, MessageType::VoteReq);
    assert_eq!(decoded.flags, 0);
    assert_eq!(decoded.request_id, 42);
    assert_eq!(decoded.body.len(), 0);
}

#[test]
fn encode_decode_with_body() {
    let body = Bytes::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
    let frame = Frame::new_response(MessageType::AppendEntriesResp, 0xdeadbeef, body.clone());
    let encoded = frame.encode();
    let mut bytes = encoded.freeze();
    let decoded = Frame::decode(&mut bytes).expect("decode");
    assert_eq!(decoded.msg_type, MessageType::AppendEntriesResp);
    assert_eq!(decoded.flags, 1);
    assert!(decoded.is_response());
    assert_eq!(decoded.request_id, 0xdeadbeef);
    assert_eq!(decoded.body, body);
}

#[test]
fn corrupted_crc_rejected() {
    let frame = Frame::new_request(MessageType::VoteReq, 1, Bytes::from(vec![42]));
    let mut encoded = frame.encode();
    // Flip a bit in the body.
    encoded[14] ^= 0xff;
    let mut bytes = encoded.freeze();
    let result = Frame::decode(&mut bytes);
    assert!(result.is_err(), "expected crc mismatch error");
}

#[test]
fn unknown_msg_type_rejected() {
    let mut encoded = Frame::new_request(MessageType::VoteReq, 1, Bytes::new()).encode();
    encoded[0] = 99;                            // unknown msg type
    let mut bytes = encoded.freeze();
    let result = Frame::decode(&mut bytes);
    assert!(result.is_err());
}

#[tokio::test]
async fn read_async_round_trip() {
    let body = Bytes::from(vec![10, 20, 30]);
    let frame = Frame::new_request(MessageType::InstallSnapshotReq, 7, body.clone());
    let encoded = frame.encode().freeze();
    let mut reader = std::io::Cursor::new(encoded.to_vec());
    let decoded = Frame::read_async(&mut reader).await.expect("read_async");
    assert_eq!(decoded.msg_type, MessageType::InstallSnapshotReq);
    assert_eq!(decoded.request_id, 7);
    assert_eq!(decoded.body, body);
}
```

- [ ] **Step 3: Make `Frame` module public**

In `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/network/mod.rs`, the existing `pub mod frame;` already exposes it. The test imports `uc_node::network::frame::Frame` so the `network` module must also be public from `lib.rs` (already added in Task 4 Step 8). Confirm by running:

```bash
cargo test -p uc_node --test frame_roundtrip
```

Expected: 5 passed.

- [ ] **Step 4: Commit**

```bash
git add uc_node/Cargo.toml uc_node/src/network/frame.rs uc_node/tests/frame_roundtrip.rs
git commit -m "feat(uc_node): wire framing for QUIC streams (Frame + roundtrip tests)"
```

---

## Task 6: RPC body codecs

Implement encode/decode for the 6 RPC body kinds: `AppendEntriesReq`/`Resp`, `VoteReq`/`Resp`, `InstallSnapshotReq`/`Resp`. Bodies are bincode-encoded openraft request/response types — except `AppendEntries` body which uses a manual scatter-gather encoding to enable zero-copy of entry payloads.

**Files:**
- Modify: `uc_node/src/network/codec.rs`

- [ ] **Step 1: Replace `codec.rs`**

```rust
//! RPC body encode/decode for the message types defined in `frame.rs`.
//!
//! Most bodies are bincode-encoded openraft request/response types. The
//! exception is `AppendEntriesReq` body, which uses a custom encoding to
//! enable scatter-gather zero-copy of entry payloads via
//! `quinn::SendStream::write_chunks(&[Bytes])`.

use bytes::Bytes;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use super::NetworkError;
use crate::raft::TypeConfig;

/// Bincode encode (for non-AppendEntries bodies).
pub fn encode<T: serde::Serialize>(value: &T) -> Result<Bytes, NetworkError> {
    let bytes = bincode::serde::encode_to_vec(value, bincode::config::standard())
        .map_err(|e| NetworkError::Decode(format!("encode: {e}")))?;
    Ok(Bytes::from(bytes))
}

/// Bincode decode (for non-AppendEntries bodies).
pub fn decode<T: serde::de::DeserializeOwned>(bytes: &Bytes) -> Result<T, NetworkError> {
    let (val, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map_err(|e| NetworkError::Decode(format!("decode: {e}")))?;
    Ok(val)
}

// Convenience wrappers per RPC kind.

pub fn encode_vote_req(req: &VoteRequest<u64>) -> Result<Bytes, NetworkError> { encode(req) }
pub fn decode_vote_req(b: &Bytes) -> Result<VoteRequest<u64>, NetworkError> { decode(b) }

pub fn encode_vote_resp(resp: &VoteResponse<u64>) -> Result<Bytes, NetworkError> { encode(resp) }
pub fn decode_vote_resp(b: &Bytes) -> Result<VoteResponse<u64>, NetworkError> { decode(b) }

pub fn encode_append_entries_req(req: &AppendEntriesRequest<TypeConfig>)
    -> Result<Bytes, NetworkError> {
    // M2 simple path: bincode-encode the whole request. The zero-copy
    // scatter-gather optimization (manual encoding + write_chunks) is a
    // perf optimization deferred to a follow-up; correctness first.
    encode(req)
}
pub fn decode_append_entries_req(b: &Bytes)
    -> Result<AppendEntriesRequest<TypeConfig>, NetworkError> {
    decode(b)
}

pub fn encode_append_entries_resp(resp: &AppendEntriesResponse<u64>)
    -> Result<Bytes, NetworkError> { encode(resp) }
pub fn decode_append_entries_resp(b: &Bytes)
    -> Result<AppendEntriesResponse<u64>, NetworkError> { decode(b) }

pub fn encode_install_snapshot_req(req: &InstallSnapshotRequest<TypeConfig>)
    -> Result<Bytes, NetworkError> { encode(req) }
pub fn decode_install_snapshot_req(b: &Bytes)
    -> Result<InstallSnapshotRequest<TypeConfig>, NetworkError> { decode(b) }

pub fn encode_install_snapshot_resp(resp: &InstallSnapshotResponse<u64>)
    -> Result<Bytes, NetworkError> { encode(resp) }
pub fn decode_install_snapshot_resp(b: &Bytes)
    -> Result<InstallSnapshotResponse<u64>, NetworkError> { decode(b) }
```

(Note: this is the **simple correctness-first** encoding. The scatter-gather optimization described in the spec is a follow-up perf task. Get correctness working first; bench/optimize after M2 tests pass.)

- [ ] **Step 2: Build and test**

```bash
cargo build -p uc_node
cargo clippy -p uc_node --all-targets -- -D warnings
```

Expected: clean.

- [ ] **Step 3: Add a codec roundtrip test**

Append to `/Users/peter/Projects/ultima/ultima_cluster/uc_node/tests/frame_roundtrip.rs`:

```rust
use openraft::Vote;
use openraft::raft::VoteRequest;
use uc_node::network::codec;

#[test]
fn vote_req_roundtrip() {
    let req: VoteRequest<u64> = VoteRequest::new(
        Vote::new(7, 3),
        Some(openraft::LogId::new(openraft::CommittedLeaderId::new(5, 0), 42)),
    );
    let bytes = codec::encode_vote_req(&req).expect("encode");
    let decoded = codec::decode_vote_req(&bytes).expect("decode");
    assert_eq!(decoded.vote, req.vote);
    assert_eq!(decoded.last_log_id, req.last_log_id);
}
```

(If `VoteRequest::new` doesn't exist in 0.9.24, use struct-literal construction matching the actual fields. Check openraft 0.9.24 source.)

Run: `cargo test -p uc_node --test frame_roundtrip vote_req_roundtrip`
Expected: passes.

- [ ] **Step 4: Commit**

```bash
git add uc_node/src/network/codec.rs uc_node/tests/frame_roundtrip.rs
git commit -m "feat(uc_node): RPC body codecs (bincode for openraft types)"
```

---

## Task 7: QUIC server (listener)

Bring up a QUIC endpoint that accepts inbound connections, dispatches inbound RPC frames to the local openraft `Raft<TypeConfig>` instance, and writes response frames back.

**Files:**
- Modify: `uc_node/src/network/server.rs`

The server's API needs to be: given a `Raft<TypeConfig>` handle and a TLS config + listen addr, spawn a background tokio task that listens and dispatches. Returns a handle that can be used to shut it down.

- [ ] **Step 1: Replace `server.rs`**

```rust
//! QUIC listener that accepts inbound RPCs and dispatches them to the local
//! `Raft<TypeConfig>` instance.

use std::net::SocketAddr;
use std::sync::Arc;

use openraft::Raft;
use quinn::{Endpoint, ServerConfig as QuicServerConfig};
use tokio::task::JoinHandle;

use super::frame::{Frame, MessageType};
use super::{codec, NetworkError};
use crate::raft::TypeConfig;

pub struct ServerHandle {
    endpoint: Endpoint,
    accept_task: JoinHandle<()>,
}

impl ServerHandle {
    pub async fn shutdown(self) {
        self.endpoint.close(0u32.into(), b"shutdown");
        let _ = self.accept_task.await;
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.endpoint.local_addr()
    }
}

pub fn spawn_server(
    listen_addr: SocketAddr,
    rustls_server_cfg: Arc<rustls::ServerConfig>,
    raft: Raft<TypeConfig>,
) -> Result<ServerHandle, NetworkError> {
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(rustls_server_cfg.as_ref().clone())
        .map_err(|e| NetworkError::Tls(format!("quic server cfg: {e}")))?;
    let server_cfg = QuicServerConfig::with_crypto(Arc::new(crypto));
    let endpoint = Endpoint::server(server_cfg, listen_addr)
        .map_err(|e| NetworkError::Connect(format!("endpoint: {e}")))?;

    let raft_for_accept = raft.clone();
    let accept_task = tokio::spawn(async move {
        while let Some(conn) = endpoint.accept().await {
            let raft = raft_for_accept.clone();
            tokio::spawn(async move {
                match conn.await {
                    Ok(conn) => handle_connection(conn, raft).await,
                    Err(e) => tracing::warn!(error = %e, "quic accept failed"),
                }
            });
        }
    });

    Ok(ServerHandle { endpoint, accept_task })
}

async fn handle_connection(conn: quinn::Connection, raft: Raft<TypeConfig>) {
    loop {
        match conn.accept_bi().await {
            Ok((mut send, mut recv)) => {
                let raft = raft.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_stream(&mut send, &mut recv, raft).await {
                        tracing::warn!(error = ?e, "quic stream handler failed");
                    }
                });
            }
            Err(quinn::ConnectionError::ApplicationClosed(_))
            | Err(quinn::ConnectionError::ConnectionClosed(_))
            | Err(quinn::ConnectionError::LocallyClosed) => break,
            Err(e) => {
                tracing::warn!(error = ?e, "quic accept_bi failed");
                break;
            }
        }
    }
}

async fn handle_stream(
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    raft: Raft<TypeConfig>,
) -> Result<(), NetworkError> {
    loop {
        let request = match Frame::read_async(recv).await {
            Ok(f) => f,
            Err(NetworkError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        };

        let response = dispatch(request, &raft).await?;
        let encoded = response.encode().freeze();
        use tokio::io::AsyncWriteExt;
        send.write_all(&encoded).await
            .map_err(|e| NetworkError::Stream(format!("write: {e}")))?;
    }
    Ok(())
}

async fn dispatch(req: Frame, raft: &Raft<TypeConfig>) -> Result<Frame, NetworkError> {
    let request_id = req.request_id;
    match req.msg_type {
        MessageType::AppendEntriesReq => {
            let decoded = codec::decode_append_entries_req(&req.body)?;
            let resp = raft.append_entries(decoded).await
                .map_err(|e| NetworkError::Stream(format!("append_entries: {e}")))?;
            let body = codec::encode_append_entries_resp(&resp)?;
            Ok(Frame::new_response(MessageType::AppendEntriesResp, request_id, body))
        }
        MessageType::VoteReq => {
            let decoded = codec::decode_vote_req(&req.body)?;
            let resp = raft.vote(decoded).await
                .map_err(|e| NetworkError::Stream(format!("vote: {e}")))?;
            let body = codec::encode_vote_resp(&resp)?;
            Ok(Frame::new_response(MessageType::VoteResp, request_id, body))
        }
        MessageType::InstallSnapshotReq => {
            let decoded = codec::decode_install_snapshot_req(&req.body)?;
            let resp = raft.install_snapshot(decoded).await
                .map_err(|e| NetworkError::Stream(format!("install_snapshot: {e}")))?;
            let body = codec::encode_install_snapshot_resp(&resp)?;
            Ok(Frame::new_response(MessageType::InstallSnapshotResp, request_id, body))
        }
        other => Err(NetworkError::Decode(format!("server got non-request msg_type {other:?}"))),
    }
}
```

(`Raft::vote(req)` / `Raft::append_entries(req)` / `Raft::install_snapshot(req)` are openraft's incoming-RPC handlers. Verify the exact method names against `~/.cargo/registry/.../openraft-0.9.24/src/raft/mod.rs` — they may be `Raft::handle_append_entries` or similar. Adapt as needed.)

- [ ] **Step 2: Build**

```bash
cargo build -p uc_node
cargo clippy -p uc_node --all-targets -- -D warnings
```

Expected: clean. The server module is unused yet.

- [ ] **Step 3: Commit**

```bash
git add uc_node/src/network/server.rs
git commit -m "feat(uc_node): QUIC server (listener + per-stream RPC dispatch)"
```

---

## Task 8: QUIC client (per-peer connection)

Manage a single QUIC connection to a peer + a multiplexed request/response correlation map. Send a frame, await the response.

**Files:**
- Modify: `uc_node/src/network/client.rs`

- [ ] **Step 1: Replace `client.rs`**

```rust
//! Per-peer QUIC connection wrapper with multiplexed request/response correlation.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use quinn::{ClientConfig as QuicClientConfig, Endpoint, RecvStream, SendStream};
use tokio::sync::{oneshot, Mutex};

use super::frame::{Frame, MessageType};
use super::NetworkError;

pub struct PeerConn {
    inner: Arc<PeerConnInner>,
}

struct PeerConnInner {
    send: Mutex<SendStream>,
    correlator: Mutex<HashMap<u64, oneshot::Sender<Frame>>>,
    request_id: AtomicU64,
}

impl PeerConn {
    pub async fn connect(
        rustls_client_cfg: Arc<rustls::ClientConfig>,
        peer_addr: SocketAddr,
        server_name: &str,
    ) -> Result<Self, NetworkError> {
        let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(
            rustls_client_cfg.as_ref().clone())
            .map_err(|e| NetworkError::Tls(format!("quic client cfg: {e}")))?;
        let mut client_cfg = QuicClientConfig::new(Arc::new(crypto));
        let mut transport = quinn::TransportConfig::default();
        transport.max_idle_timeout(Some(std::time::Duration::from_secs(30).try_into().unwrap()));
        client_cfg.transport_config(Arc::new(transport));

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())
            .map_err(|e| NetworkError::Connect(format!("client endpoint: {e}")))?;
        endpoint.set_default_client_config(client_cfg);

        let conn = endpoint.connect(peer_addr, server_name)
            .map_err(|e| NetworkError::Connect(format!("connect: {e}")))?
            .await
            .map_err(|e| NetworkError::Connect(format!("handshake: {e}")))?;

        let (send, mut recv) = conn.open_bi().await
            .map_err(|e| NetworkError::Stream(format!("open_bi: {e}")))?;

        let inner = Arc::new(PeerConnInner {
            send: Mutex::new(send),
            correlator: Mutex::new(HashMap::new()),
            request_id: AtomicU64::new(1),
        });

        // Spawn the read task.
        let inner_for_read = inner.clone();
        tokio::spawn(async move {
            loop {
                match Frame::read_async(&mut recv).await {
                    Ok(frame) => {
                        let mut correlator = inner_for_read.correlator.lock().await;
                        if let Some(tx) = correlator.remove(&frame.request_id) {
                            let _ = tx.send(frame);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = ?e, "peer read failed; closing");
                        break;
                    }
                }
            }
        });

        Ok(Self { inner })
    }

    pub async fn request(
        &self,
        msg_type: MessageType,
        body: bytes::Bytes,
        response_type: MessageType,
    ) -> Result<bytes::Bytes, NetworkError> {
        let request_id = self.inner.request_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();

        {
            let mut correlator = self.inner.correlator.lock().await;
            correlator.insert(request_id, tx);
        }

        let frame = Frame::new_request(msg_type, request_id, body);
        let encoded = frame.encode().freeze();

        {
            use tokio::io::AsyncWriteExt;
            let mut send = self.inner.send.lock().await;
            send.write_all(&encoded).await
                .map_err(|e| NetworkError::Stream(format!("write: {e}")))?;
        }

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            rx,
        ).await
            .map_err(|_| NetworkError::Timeout)?
            .map_err(|_| NetworkError::Disconnected)?;

        if response.msg_type != response_type {
            return Err(NetworkError::Decode(format!(
                "expected {response_type:?} got {:?}", response.msg_type)));
        }
        Ok(response.body)
    }
}

impl Clone for PeerConn {
    fn clone(&self) -> Self { Self { inner: self.inner.clone() } }
}
```

(Verify quinn 0.11 API names — `open_bi`/`accept_bi`/`set_default_client_config` — they're stable across recent versions but adjust if 0.11 differs.)

- [ ] **Step 2: Build**

```bash
cargo build -p uc_node
cargo clippy -p uc_node --all-targets -- -D warnings
```

Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add uc_node/src/network/client.rs
git commit -m "feat(uc_node): QUIC peer connection (multiplexed request/response)"
```

---

## Task 9: `QuicRaftNetworkFactory` + `QuicRaftNetwork`

Implement the openraft `RaftNetworkFactory` and `RaftNetwork` traits using the `PeerConn` from Task 8.

**Files:**
- Modify: `uc_node/src/network/factory.rs`
- Modify: `uc_node/src/network/instance.rs`

**Approach:** Don't connect in `new_client` (which can't fail per openraft's API). Instead, `QuicRaftNetwork` stores the connect parameters + a shared cache of established peer connections. Each RPC call gets-or-connects via the cache. Connection failures surface as `RPCError::Network`, which openraft retries with backoff.

- [ ] **Step 1: Implement `QuicRaftNetworkFactory`**

Replace `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/network/factory.rs`:

```rust
//! `RaftNetworkFactory` impl over QUIC.

use std::collections::HashMap;
use std::sync::Arc;

use openraft::network::RaftNetworkFactory;
use rustls::ClientConfig;
use tokio::sync::Mutex;

use super::client::PeerConn;
use super::instance::QuicRaftNetwork;
use crate::raft::{NodeAddr, NodeId, TypeConfig};

/// Shared map of established peer connections, keyed by NodeId.
pub type PeerPool = Arc<Mutex<HashMap<NodeId, PeerConn>>>;

pub struct QuicRaftNetworkFactory {
    client_cfg: Arc<ClientConfig>,
    pool: PeerPool,
    app_id: String,
}

impl QuicRaftNetworkFactory {
    pub fn new(client_cfg: Arc<ClientConfig>, app_id: String) -> Self {
        Self {
            client_cfg,
            pool: Arc::new(Mutex::new(HashMap::new())),
            app_id,
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for QuicRaftNetworkFactory {
    type Network = QuicRaftNetwork;

    async fn new_client(&mut self, target: NodeId, node: &NodeAddr) -> Self::Network {
        // Do NOT connect here. RaftNetworkFactory::new_client can't return Result,
        // and a failed connection at this point would force us into a panic-or-Box-Err
        // shape. Instead, defer connection to first request — RaftNetwork::* methods
        // CAN return Err, and openraft retries.
        QuicRaftNetwork {
            target,
            peer_addr: node.raft_addr,
            client_cfg: self.client_cfg.clone(),
            pool: self.pool.clone(),
            app_id: self.app_id.clone(),
        }
    }
}
```

- [ ] **Step 2: Implement `QuicRaftNetwork`**

Replace `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/network/instance.rs`:

```rust
//! `RaftNetwork` impl using a lazily-connected `PeerConn` from a shared pool.

use std::net::SocketAddr;
use std::sync::Arc;

use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use rustls::ClientConfig;

use super::client::PeerConn;
use super::factory::PeerPool;
use super::frame::MessageType;
use super::{codec, NetworkError};
use crate::raft::{NodeAddr, NodeId, TypeConfig};

pub struct QuicRaftNetwork {
    pub(crate) target: NodeId,
    pub(crate) peer_addr: SocketAddr,
    pub(crate) client_cfg: Arc<ClientConfig>,
    pub(crate) pool: PeerPool,
    pub(crate) app_id: String,
}

impl QuicRaftNetwork {
    /// Get the cached connection or establish a new one.
    /// Removes stale entries on connection failure.
    async fn get_or_connect(&self) -> Result<PeerConn, NetworkError> {
        // Fast path: cached.
        {
            let pool = self.pool.lock().await;
            if let Some(conn) = pool.get(&self.target) {
                return Ok(conn.clone());
            }
        }
        // Slow path: connect.
        let conn = PeerConn::connect(self.client_cfg.clone(), self.peer_addr, &self.app_id).await?;
        let mut pool = self.pool.lock().await;
        pool.insert(self.target, conn.clone());
        Ok(conn)
    }

    /// Drop a stale connection from the pool. Called after a request fails.
    async fn evict(&self) {
        let mut pool = self.pool.lock().await;
        pool.remove(&self.target);
    }
}

fn rpc_err<E>(e: NetworkError) -> RPCError<NodeId, NodeAddr, RaftError<NodeId, E>>
where E: std::error::Error
{
    RPCError::Network(openraft::error::NetworkError::new(&e))
}

impl RaftNetwork<TypeConfig> for QuicRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, NodeAddr, RaftError<NodeId>>> {
        let body = codec::encode_append_entries_req(&rpc).map_err(rpc_err)?;
        let conn = self.get_or_connect().await.map_err(rpc_err)?;
        match conn.request(MessageType::AppendEntriesReq, body, MessageType::AppendEntriesResp).await {
            Ok(resp_body) => codec::decode_append_entries_resp(&resp_body).map_err(rpc_err),
            Err(e) => {
                self.evict().await;
                Err(rpc_err(e))
            }
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, NodeAddr, RaftError<NodeId, InstallSnapshotError>>> {
        let body = codec::encode_install_snapshot_req(&rpc).map_err(rpc_err)?;
        let conn = self.get_or_connect().await.map_err(rpc_err)?;
        match conn.request(MessageType::InstallSnapshotReq, body, MessageType::InstallSnapshotResp).await {
            Ok(resp_body) => codec::decode_install_snapshot_resp(&resp_body).map_err(rpc_err),
            Err(e) => {
                self.evict().await;
                Err(rpc_err(e))
            }
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, NodeAddr, RaftError<NodeId>>> {
        let body = codec::encode_vote_req(&rpc).map_err(rpc_err)?;
        let conn = self.get_or_connect().await.map_err(rpc_err)?;
        match conn.request(MessageType::VoteReq, body, MessageType::VoteResp).await {
            Ok(resp_body) => codec::decode_vote_resp(&resp_body).map_err(rpc_err),
            Err(e) => {
                self.evict().await;
                Err(rpc_err(e))
            }
        }
    }
}
```

- [ ] **Step 3: Build, clippy**

```bash
cargo build -p uc_node
cargo clippy -p uc_node --all-targets -- -D warnings
```

Expected: clean. (If the `RaftError` generic type or `RPCError::Network` path has changed in openraft 0.9.24, adapt — but per Task 12 of M1 these are known names.)

- [ ] **Step 4: Commit**

```bash
git add uc_node/src/network/factory.rs uc_node/src/network/instance.rs
git commit -m "feat(uc_node): QuicRaftNetworkFactory + QuicRaftNetwork (RaftNetwork over QUIC)"
```

---

## Task 10: Wire QUIC into `NodeBuilder`; remove `NoopNetwork`

Update `NodeBuilder::start` to spawn a QUIC server + create a `QuicRaftNetworkFactory` instead of using `NoopNetwork`. Delete the now-unused `test_helpers::NoopNetwork` from `log_storage.rs`.

**Files:**
- Modify: `uc_node/src/runtime/builder.rs`
- Modify: `uc_node/src/runtime/node.rs` (add server handle for shutdown)
- Modify: `uc_node/src/raft/log_storage.rs` (remove NoopNetwork)

- [ ] **Step 1: Update `NodeHandle<S>` to own a `ServerHandle`**

In `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/runtime/node.rs`:

```rust
use crate::network::server::ServerHandle;

pub struct NodeHandle<S: StateMachine> {
    pub(crate) raft: Raft<TypeConfig>,
    pub(crate) config: NodeConfig,
    pub(crate) sm: AdaptedStateMachine<S>,
    pub(crate) server: ServerHandle,
}
```

Update `shutdown` to also shutdown the server:

```rust
pub async fn shutdown(self) -> Result<(), ClusterError> {
    self.raft.shutdown().await
        .map_err(|e| ClusterError::Raft(e.to_string()))?;
    self.server.shutdown().await;
    Ok(())
}
```

- [ ] **Step 2: Update `NodeBuilder::start` to wire QUIC**

In `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/runtime/builder.rs`, replace the network construction:

```rust
// (after log_storage / state machine setup, before Raft::new)

// TLS setup.
let (cert_der, key_der) = crate::network::tls::load_or_init(
    &self.config.data_dir, &self.config.app_id)?;
let server_cfg = crate::network::tls::build_server_config(
    cert_der.clone(), key_der.clone_key())?;
let client_cfg = crate::network::tls::build_client_config()?;

// Network factory.
let network = crate::network::QuicRaftNetworkFactory::new(
    client_cfg, self.config.app_id.clone());

let raft = openraft::Raft::new(
    self.config.node_id,
    raft_config,
    network,
    log_storage,
    sm_adapter,
).await
    .map_err(|e| ClusterError::Raft(e.to_string()))?;

// Spawn the server task.
let server = crate::network::server::spawn_server(
    self.config.raft_listen_addr,
    server_cfg,
    raft.clone(),
)?;
```

Then in the `Ok(NodeHandle { … })` return, add `server`.

Note: `PrivateKeyDer` may not have a `clone_key()` method in rustls 0.23 — if not, reload from the file instead, or extract the inner bytes and recreate.

- [ ] **Step 3: Remove `NoopNetwork` from log_storage.rs**

In `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/raft/log_storage.rs`, delete the entire `pub mod test_helpers { … }` block (added in Task 12 of M1).

- [ ] **Step 4: Build, clippy**

```bash
cargo build --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: clean.

- [ ] **Step 5: Run existing M1 tests**

```bash
cargo test --workspace
```

Expected: M1's `submit_query_works` and `state_survives_restart` should still pass, but now they're using real QUIC over loopback (single-node, so no network actually flows). 12+ tests pass.

If any test fails, the QUIC infrastructure has a bug. Common issues: rustls config mismatch, TLS handshake error, port binding conflict. Fix before proceeding.

- [ ] **Step 6: Commit**

```bash
git add uc_node/src/runtime/builder.rs uc_node/src/runtime/node.rs uc_node/src/raft/log_storage.rs
git commit -m "feat(uc_node): wire QUIC into NodeBuilder; remove NoopNetwork"
```

---

## Task 11: `BootstrapConfig::Peers` + membership-change methods

Wire `BootstrapConfig::Peers` to call `bootstrap_single_node` (on the lowest-id node) then `add_learner` + `change_membership` for the other peers. Add `add_learner`, `change_membership`, `remove_node` methods on `NodeHandle<S>`.

**Files:**
- Modify: `uc_node/src/runtime/builder.rs`
- Modify: `uc_node/src/runtime/node.rs`

- [ ] **Step 1: Add membership-change methods on `NodeHandle`**

In `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/runtime/node.rs`:

```rust
use std::collections::BTreeSet;
use std::net::SocketAddr;
use crate::raft::NodeAddr;

impl<S: StateMachine> NodeHandle<S> {
    // ... existing methods ...

    pub async fn add_learner(&self, node_id: NodeId, raft_addr: SocketAddr)
        -> Result<(), ClusterError>
    {
        let node = NodeAddr { raft_addr, client_addr: None };
        self.raft.add_learner(node_id, node, true).await
            .map_err(|e| ClusterError::Raft(e.to_string()))?;
        Ok(())
    }

    pub async fn change_membership(&self, voters: BTreeSet<NodeId>)
        -> Result<(), ClusterError>
    {
        self.raft.change_membership(voters, false).await
            .map_err(|e| ClusterError::Raft(e.to_string()))?;
        Ok(())
    }

    pub async fn remove_node(&self, node_id: NodeId) -> Result<(), ClusterError> {
        // openraft 0.9 path: change_membership to exclude the node, then it's removed.
        let metrics = self.raft.metrics().borrow().clone();
        let current = metrics.membership_config.membership().voter_ids().collect::<BTreeSet<_>>();
        let mut next = current.clone();
        next.remove(&node_id);
        self.raft.change_membership(next, false).await
            .map_err(|e| ClusterError::Raft(e.to_string()))?;
        Ok(())
    }
}
```

(Verify the exact `Raft::add_learner` / `change_membership` signatures in openraft 0.9.24 — they may use different boolean flags or take different argument shapes. Adapt as needed.)

- [ ] **Step 2: Implement `BootstrapConfig::Peers`**

In `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/runtime/builder.rs`, replace the `BootstrapConfig::Peers { … }` arm:

```rust
BootstrapConfig::Peers { peers } => {
    let min_id = peers.iter().map(|p| p.node_id).min()
        .ok_or_else(|| ClusterError::Config("Peers list is empty".into()))?;
    let self_id = self.config.node_id;

    if self_id == min_id {
        // I am the bootstrapper.
        let mut members: BTreeMap<u64, NodeAddr> = BTreeMap::new();
        members.insert(self_id, NodeAddr {
            raft_addr: self.config.raft_listen_addr,
            client_addr: None,
        });
        match raft.initialize(members).await {
            Ok(()) => {}
            Err(openraft::error::RaftError::APIError(
                openraft::error::InitializeError::NotAllowed(_))) => {
                // Already initialized on a prior run.
            }
            Err(e) => return Err(ClusterError::Raft(e.to_string())),
        }

        // Add the other peers as learners, then promote.
        for peer in peers.iter().filter(|p| p.node_id != self_id) {
            let node = NodeAddr {
                raft_addr: peer.raft_addr,
                client_addr: None,
            };
            // add_learner with blocking=true so it returns once log catches up.
            // For new peers with empty state, this triggers a snapshot install.
            if let Err(e) = raft.add_learner(peer.node_id, node, true).await {
                tracing::warn!(node_id = peer.node_id, error = ?e,
                    "add_learner failed; will need manual retry");
                // Don't fail — the bootstrap node ships even if some peers
                // weren't immediately reachable.
            }
        }
        let voters: BTreeSet<u64> = peers.iter().map(|p| p.node_id).collect();
        if let Err(e) = raft.change_membership(voters, false).await {
            tracing::warn!(error = ?e, "change_membership failed; manual recovery needed");
        }
    } else {
        // Not the bootstrapper — idle until added as learner.
        tracing::info!(self_id, min_id, "waiting for bootstrap node to add me");
    }
}
```

- [ ] **Step 3: Build, clippy**

```bash
cargo build --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add uc_node/src/runtime/builder.rs uc_node/src/runtime/node.rs
git commit -m "feat(uc_node): BootstrapConfig::Peers + add_learner/change_membership/remove_node"
```

---

## Task 12: Three-node cluster test harness

Set up the test harness that brings up 3 nodes in one process (loopback only) and runs M2 multi-node tests against it.

**Files:**
- Create: `uc_node/tests/m2_multi_node.rs`

- [ ] **Step 1: Create the harness file**

```rust
//! M2 multi-node integration tests.
//!
//! Each test brings up 3 nodes on different loopback ports. Each node has
//! its own tempdir, its own QUIC endpoint, and they discover each other
//! via BootstrapConfig::Peers.

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::sync::Mutex;

use uc_node::{
    BootstrapConfig, NodeBuilder, NodeConfig, NodeHandle, NodeId, PeerSeed,
    RaftTuning, TlsConfig,
};
use uc_service::{SnapshotError, StateMachine};

// Shared counter state machine for tests.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum Cmd { Inc(u64) }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Resp { value: u64 }

#[derive(Default)]
struct Counter {
    value: u64,
    last_applied: Option<u64>,
}

impl StateMachine for Counter {
    type Command = Cmd;
    type Response = Resp;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, idx: u64, c: Cmd) -> Resp {
        match c { Cmd::Inc(n) => self.value += n }
        self.last_applied = Some(idx);
        Resp { value: self.value }
    }
    fn query(&self, _: ()) -> u64 { self.value }
    fn last_applied(&self) -> Option<u64> { self.last_applied }

    fn build_snapshot(&self, dst: &mut dyn Write) -> Result<u64, SnapshotError> {
        let bytes = bincode::serde::encode_to_vec(
            &(self.value, self.last_applied), bincode::config::standard())
            .map_err(|e| SnapshotError::Codec(e.to_string()))?;
        dst.write_all(&bytes)?;
        Ok(self.last_applied.unwrap_or(0))
    }
    fn install_snapshot(&mut self, src: &mut dyn Read) -> Result<u64, SnapshotError> {
        let mut buf = Vec::new();
        src.read_to_end(&mut buf)?;
        let ((v, la), _) = bincode::serde::decode_from_slice::<(u64, Option<u64>), _>(
            &buf, bincode::config::standard())
            .map_err(|e| SnapshotError::Codec(e.to_string()))?;
        self.value = v;
        self.last_applied = la;
        Ok(la.unwrap_or(0))
    }
}

// Harness.

pub struct TestNode {
    pub node_id: NodeId,
    pub handle: Option<NodeHandle<Counter>>,
    pub data_dir: Arc<TempDir>,
    pub addr: SocketAddr,
}

fn node_config(
    node_id: NodeId,
    data_dir: &TempDir,
    listen_addr: SocketAddr,
    peers: Vec<PeerSeed>,
) -> NodeConfig {
    NodeConfig {
        node_id,
        data_dir: data_dir.path().to_owned(),
        raft_listen_addr: listen_addr,
        app_id: "m2-test".into(),
        bootstrap: BootstrapConfig::Peers { peers },
        raft: RaftTuning::default(),
        tls: TlsConfig::default(),
    }
}

pub async fn spawn_3_node_cluster() -> Vec<TestNode> {
    // Allocate three loopback addrs.
    let addrs: Vec<SocketAddr> = (1..=3)
        .map(|_| {
            let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let a = s.local_addr().unwrap();
            drop(s);
            a
        })
        .collect();

    let peer_seeds: Vec<PeerSeed> = (1..=3u64).zip(addrs.iter()).map(|(id, a)| {
        PeerSeed { node_id: id, raft_addr: *a }
    }).collect();

    // Bring up all 3 nodes concurrently.
    let mut nodes = Vec::new();
    for (i, addr) in addrs.iter().enumerate() {
        let node_id = (i as u64) + 1;
        let dir = Arc::new(TempDir::new().unwrap());
        let cfg = node_config(node_id, &dir, *addr, peer_seeds.clone());
        let handle = NodeBuilder::new(cfg, Counter::default())
            .start().await
            .expect(&format!("node {node_id} start"));
        nodes.push(TestNode {
            node_id,
            handle: Some(handle),
            data_dir: dir,
            addr: *addr,
        });
    }
    nodes
}

pub async fn wait_for_leader(nodes: &[TestNode], timeout: Duration) -> NodeId {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        for n in nodes {
            if let Some(h) = &n.handle {
                if let Some(leader) = h.current_leader().await {
                    return leader;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no leader within {timeout:?}");
}

pub async fn shutdown_all(mut nodes: Vec<TestNode>) {
    for n in nodes.iter_mut() {
        if let Some(handle) = n.handle.take() {
            let _ = handle.shutdown().await;
        }
    }
}

// Tests.

#[tokio::test]
async fn three_node_cluster_elects_leader() {
    let nodes = spawn_3_node_cluster().await;
    let leader = wait_for_leader(&nodes, Duration::from_secs(10)).await;
    assert!(leader >= 1 && leader <= 3);
    shutdown_all(nodes).await;
}
```

- [ ] **Step 2: Run the test**

```bash
cargo test -p uc_node --test m2_multi_node three_node_cluster_elects_leader
```

Expected: passes. Election within ~2-3 seconds.

If this fails, the QUIC infrastructure is broken. Common issues:
- Address binding conflicts.
- TLS handshake failing between peers (verify the AcceptAnything verifier from Task 4 works correctly).
- openraft can't reach `add_learner`/`change_membership` because the QUIC connection isn't established yet.

Add timing slack (`tokio::time::sleep` between start and `add_learner`) if needed.

- [ ] **Step 3: Commit**

```bash
git add uc_node/tests/m2_multi_node.rs
git commit -m "test(uc_node): 3-node cluster harness + election test"
```

---

## Task 13: Replication test (all nodes apply same log)

Submit commands and verify all 3 nodes have applied them.

**Files:**
- Modify: `uc_node/tests/m2_multi_node.rs`

- [ ] **Step 1: Add the replication test**

Append to `m2_multi_node.rs`:

```rust
#[tokio::test]
async fn three_node_replication() {
    let nodes = spawn_3_node_cluster().await;
    let leader_id = wait_for_leader(&nodes, Duration::from_secs(10)).await;
    let leader = nodes.iter().find(|n| n.node_id == leader_id).unwrap();
    let leader_handle = leader.handle.as_ref().unwrap();

    // Submit 5 increments via the leader.
    for i in 1..=5u64 {
        let resp = leader_handle.submit(Cmd::Inc(i)).await.expect("submit");
        // Cumulative: 1, 3, 6, 10, 15.
        let expected: u64 = (1..=i).sum();
        assert_eq!(resp.value, expected, "leader submit {i}: expected sum {expected}");
    }

    // Give followers time to apply.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Every node's query_snapshot should return 15.
    for n in &nodes {
        if let Some(h) = &n.handle {
            let v = h.query_snapshot(|c: &Counter| c.value).await;
            assert_eq!(v, 15, "node {} value", n.node_id);
        }
    }
    shutdown_all(nodes).await;
}
```

- [ ] **Step 2: Run the test**

```bash
cargo test -p uc_node --test m2_multi_node three_node_replication
```

Expected: passes. All 3 nodes converge on value=15.

- [ ] **Step 3: Commit**

```bash
git add uc_node/tests/m2_multi_node.rs
git commit -m "test(uc_node): three_node_replication — all nodes apply same log"
```

---

## Task 14: Leader failover test

Kill the leader, verify a new leader is elected, verify the cluster keeps working.

**Files:**
- Modify: `uc_node/tests/m2_multi_node.rs`

- [ ] **Step 1: Add a `wait_for_leader_among` helper to the harness**

In `uc_node/tests/m2_multi_node.rs`, after the existing `wait_for_leader` function, add:

```rust
async fn wait_for_leader_among(handles: &[&NodeHandle<Counter>], timeout: Duration) -> NodeId {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        for h in handles {
            if let Some(l) = h.current_leader().await {
                return l;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no leader among surviving nodes within {timeout:?}");
}
```

- [ ] **Step 2: Add the failover test**

Append:

```rust
#[tokio::test]
async fn leader_failover() {
    let mut nodes = spawn_3_node_cluster().await;
    let initial_leader_id = wait_for_leader(&nodes, Duration::from_secs(10)).await;

    // Submit one command before failover.
    {
        let leader = nodes.iter().find(|n| n.node_id == initial_leader_id).unwrap();
        leader.handle.as_ref().unwrap()
            .submit(Cmd::Inc(100)).await.expect("submit pre-failover");
    }

    // Kill the leader.
    {
        let leader_idx = nodes.iter().position(|n| n.node_id == initial_leader_id).unwrap();
        let leader = nodes[leader_idx].handle.take().unwrap();
        leader.shutdown().await.expect("shutdown leader");
    }

    // Wait for a new leader among the surviving nodes.
    let surviving: Vec<&NodeHandle<Counter>> = nodes.iter()
        .filter_map(|n| n.handle.as_ref()).collect();
    let new_leader_id = wait_for_leader_among(&surviving, Duration::from_secs(15)).await;
    assert_ne!(new_leader_id, initial_leader_id, "must elect a new leader");

    // Submit through the new leader.
    let new_leader = nodes.iter().find(|n| n.node_id == new_leader_id).unwrap();
    let resp = new_leader.handle.as_ref().unwrap()
        .submit(Cmd::Inc(50)).await.expect("submit post-failover");
    assert_eq!(resp.value, 150);

    shutdown_all(nodes).await;
}
```

- [ ] **Step 2: Run the test**

```bash
cargo test -p uc_node --test m2_multi_node leader_failover
```

Expected: passes.

- [ ] **Step 3: Commit**

```bash
git add uc_node/tests/m2_multi_node.rs
git commit -m "test(uc_node): leader_failover — kill leader, new one elected"
```

---

## Task 15: Snapshot install on new follower

Build a cluster where 2 nodes have lots of log, then add a 3rd node — verify the new node catches up via snapshot install.

**Files:**
- Modify: `uc_node/tests/m2_multi_node.rs`

- [ ] **Step 1: Extend `RaftTuning` to expose the snapshot trigger threshold**

M1's `RaftTuning` doesn't expose openraft's `snapshot_policy: SnapshotPolicy::LogsSinceLast(N)`. The default 5000 is too high for tests. Add a field:

In `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/config.rs`, add to `RaftTuning`:

```rust
pub struct RaftTuning {
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub max_in_snapshot_log_to_keep: u64,
    /// Trigger snapshot every N applied log entries. openraft default is 5000.
    pub snapshot_policy_logs_since_last: u64,
}

impl Default for RaftTuning {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 250,
            election_timeout_min_ms: 1000,
            election_timeout_max_ms: 2000,
            max_in_snapshot_log_to_keep: 1000,
            snapshot_policy_logs_since_last: 5000,
        }
    }
}
```

Then in `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/runtime/builder.rs`, where the openraft `Config` is built, add:

```rust
let raft_config_openraft = RaftConfigOpenraft {
    heartbeat_interval: self.config.raft.heartbeat_interval_ms,
    election_timeout_min: self.config.raft.election_timeout_min_ms,
    election_timeout_max: self.config.raft.election_timeout_max_ms,
    max_in_snapshot_log_to_keep: self.config.raft.max_in_snapshot_log_to_keep,
    snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(
        self.config.raft.snapshot_policy_logs_since_last),
    cluster_name: self.config.app_id.clone(),
    ..Default::default()
};
```

Run `cargo build -p uc_node` to verify clean.

- [ ] **Step 2: Add the snapshot test**

This test now exercises snapshot triggering via the new tuning field.

```rust
async fn spawn_2_node_cluster_tight_snapshot() -> Vec<TestNode> {
    // ... similar to spawn_3_node_cluster but only 2 nodes, and override
    // RaftTuning so max_in_snapshot_log_to_keep is small.
}

#[tokio::test]
async fn snapshot_install_on_new_follower() {
    let mut nodes = spawn_2_node_cluster_tight_snapshot().await;
    let leader_id = wait_for_leader(&nodes, Duration::from_secs(10)).await;
    let leader = nodes.iter().find(|n| n.node_id == leader_id).unwrap();

    // Submit lots of commands to force a snapshot.
    for i in 1..=200u64 {
        leader.handle.as_ref().unwrap()
            .submit(Cmd::Inc(1)).await.expect("submit");
    }
    let total: u64 = 200;

    // Wait for the snapshot to land.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Add a 3rd node as a learner.
    let new_addr = {
        let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let a = s.local_addr().unwrap();
        drop(s);
        a
    };
    let new_dir = Arc::new(TempDir::new().unwrap());
    // The new node's bootstrap is Resume (no existing state, but no Peers either —
    // it'll be added externally).
    let new_cfg = NodeConfig {
        node_id: 3,
        data_dir: new_dir.path().to_owned(),
        raft_listen_addr: new_addr,
        app_id: "m2-test".into(),
        bootstrap: BootstrapConfig::Resume,    // wait to be added
        raft: tight_raft_tuning(),
        tls: TlsConfig::default(),
    };
    let new_handle = NodeBuilder::new(new_cfg, Counter::default())
        .start().await.expect("new node start");

    // Leader adds it as a learner.
    leader.handle.as_ref().unwrap()
        .add_learner(3, new_addr).await.expect("add_learner");

    // Wait for the new node to catch up.
    let mut caught_up = false;
    for _ in 0..50 {
        let v = new_handle.query_snapshot(|c: &Counter| c.value).await;
        if v == total {
            caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(caught_up, "new node never caught up");

    new_handle.shutdown().await.expect("shutdown");
    shutdown_all(nodes).await;
}

fn tight_raft_tuning() -> RaftTuning {
    RaftTuning {
        heartbeat_interval_ms: 100,
        election_timeout_min_ms: 500,
        election_timeout_max_ms: 1000,
        max_in_snapshot_log_to_keep: 50,
        snapshot_policy_logs_since_last: 50,    // trigger snapshot every 50 applied entries
    }
}
```

(The `spawn_2_node_cluster_tight_snapshot` helper needs to be implemented; it's similar to `spawn_3_node_cluster` but with `tight_raft_tuning()` and 2 nodes.)

To trigger snapshots at lower thresholds, `RaftTuning` may also need a `snapshot_policy_logs_since_last` field — check `uc_node/src/config.rs::RaftTuning` for what's currently exposed and the openraft `Config::snapshot_policy` field. Add a tuning field if needed.

- [ ] **Step 2: Run the test**

```bash
cargo test -p uc_node --test m2_multi_node snapshot_install_on_new_follower
```

Expected: passes within ~30 seconds (snapshot install + catch-up is slow).

If the new node times out, debug by checking:
- Is the snapshot actually being built? Trace `RaftSnapshotBuilder::build_snapshot`.
- Is the network shipping the InstallSnapshotReq frames? Trace `instance.rs::install_snapshot`.
- Does the new node's `AdaptedStateMachine::install_snapshot` get called?

- [ ] **Step 3: Commit**

```bash
git add uc_node/tests/m2_multi_node.rs uc_node/src/config.rs
git commit -m "test(uc_node): snapshot_install_on_new_follower — catch up via snapshot"
```

---

## Task 16: Membership change test

`change_membership` to add/remove a voter dynamically.

**Files:**
- Modify: `uc_node/tests/m2_multi_node.rs`

- [ ] **Step 1: Add the test**

```rust
#[tokio::test]
async fn membership_change_remove_node() {
    let mut nodes = spawn_3_node_cluster().await;
    let leader_id = wait_for_leader(&nodes, Duration::from_secs(10)).await;
    let leader = nodes.iter().find(|n| n.node_id == leader_id).unwrap();

    // Submit one command with 3 voters.
    leader.handle.as_ref().unwrap()
        .submit(Cmd::Inc(10)).await.expect("submit");

    // Pick a node that is not the leader to remove.
    let victim = (1..=3u64).find(|i| *i != leader_id).unwrap();

    // Remove the victim from voters.
    leader.handle.as_ref().unwrap()
        .remove_node(victim).await.expect("remove_node");

    // Submit another command — should succeed with only 2 voters.
    let resp = leader.handle.as_ref().unwrap()
        .submit(Cmd::Inc(5)).await.expect("submit post-removal");
    assert_eq!(resp.value, 15);

    shutdown_all(nodes).await;
}
```

- [ ] **Step 2: Run the test**

```bash
cargo test -p uc_node --test m2_multi_node membership_change_remove_node
```

Expected: passes.

- [ ] **Step 3: Commit**

```bash
git add uc_node/tests/m2_multi_node.rs
git commit -m "test(uc_node): membership_change_remove_node"
```

---

## Task 17: Polish — clippy, fmt, README

**Files:**
- Modify: `README.md`
- Any source files `cargo fmt` reformats.

- [ ] **Step 1: Verify clippy**

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

Fix any warnings inline.

- [ ] **Step 2: Run rustfmt**

```bash
cargo fmt --all
git status
```

If any files changed, commit:

```bash
git add -u
git commit -m "style: cargo fmt across workspace"
```

- [ ] **Step 3: Update README**

Replace `/Users/peter/Projects/ultima/ultima_cluster/README.md`:

```markdown
# ultima_cluster

SMR cluster implementation on top of openraft.

**Status:** M2 — multi-node + QUIC inter-node transport complete. See
`docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` for the
canonical design and `docs/tasks/` for per-milestone records.

## Workspace

- `uc_protocol` — wire spec (`no_std`-friendly).
- `uc_service` — service-side SDK (`StateMachine`, `OutputHandler` traits).
- `uc_node` — cluster engine (Raft, log storage, QUIC inter-node network).
- `uc_client` — local-shmem client SDK (M1 stub; full impl in M4).

## Build & test

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

See `CLAUDE.md` for orientation.
```

- [ ] **Step 4: Commit README**

```bash
git add README.md
git commit -m "chore(m2): clippy/fmt clean + README pointer"
```

- [ ] **Step 5: Final verification**

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
```

Expected:
- Build clean.
- ~16-18 tests pass (12 M1 + ~6 M2 + 5 frame_roundtrip + 3 tls = 25+).
- Clippy clean.
- Doc builds.

---

## Verification checklist

After all tasks complete:

- [ ] `cargo build --workspace` — clean.
- [ ] `cargo test --workspace` — all tests pass (M1's 12 + M2's new ones).
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` — zero warnings.
- [ ] `cargo doc --workspace --no-deps` — docs build (warnings OK).
- [ ] M2 capstone tests pass:
  - `three_node_cluster_elects_leader`
  - `three_node_replication`
  - `leader_failover`
  - `snapshot_install_on_new_follower`
  - `membership_change_remove_node`

If all green, M2 is done. Consolidate into `docs/tasks/task02_m2_multi_node_quic.md` per the project's feature-dev workflow, delete this plan, merge to main.

---

## Self-review notes

**Spec coverage:** This plan covers spec Section 7 (Network layer / QUIC) for inter-node communication. Spec Section 5 surface (NodeHandle add_learner/change_membership/remove_node) is wired in Task 11. Sections 3-6, 8-15 either already landed in M1 or are deferred to M3-M5.

**Type consistency:** `NodeId = u64`, `NodeAddr { raft_addr, client_addr: Option<SocketAddr> }`, `TypeConfig` reused from M1. The `client_addr: None` placeholder is consistent across all `NodeAddr` constructions.

**openraft 0.9.24 unknowns:**
- `Raft::add_learner(node_id, node, blocking)` — verify the third arg's meaning. openraft 0.9 typically uses `true` for "block until log catches up".
- `Raft::change_membership(voters, retain)` — second arg semantics may differ; check.
- `RaftError<NodeId, ChangeMembershipError<NodeId>>` shape for change_membership errors.
- `Raft::install_snapshot(req)` and `Raft::append_entries(req)` and `Raft::vote(req)` server-side dispatch — verify these are the actual public method names that take incoming RPCs.
- `quinn 0.11` `Endpoint`/`Connection`/`SendStream`/`RecvStream` API stability — verify against the actual crate version.

**Known M2 simplifications:**
- AppendEntries body encoding uses bincode (simple, correct). The scatter-gather `write_chunks(&[Bytes])` zero-copy optimization is deferred — implement after M2 tests pass if profiling justifies it.
- `QuicRaftNetwork` lazy-connects on first RPC (per Task 9 design). A connection that breaks mid-stream evicts the cache entry and openraft retries on its own backoff. M2 does NOT implement a connection health-checker or explicit reconnect-with-backoff — relies on openraft to retry.
- TLS uses an AcceptAnything verifier. M5 production polish replaces with real CA validation.

**Forward consequences for M3+:**
- M3 will introduce the shmem service split. The `network/` module developed here is independent of that change — it sits between openraft and the wire.
- The non-generic shmem-fronted `NodeBuilder` arrives in M3 alongside `pub mod runtime::ipc`. M2's generic `NodeBuilder<S>` stays.
