# M1: Embedded Single-Node Skeleton — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the workspace foundation and a working single-node ultima_cluster that runs `openraft` over `ultima_journal`, applies user commands via an embedded `StateMachine` trait (no shmem yet), and survives a restart with state preserved.

**Architecture:** Four-crate workspace (`uc_protocol`, `uc_service`, `uc_node`, `uc_client`); for M1 only `uc_protocol` (stub), `uc_service` (traits), and `uc_node` (embedded engine) carry real code. `uc_node` is generic over `S: StateMachine` in M1 — the non-generic shmem-fronted NodeBuilder lands in M3. Apply happens via direct trait call from inside the openraft state-machine adapter.

**Tech Stack:** Rust 2024 edition, `openraft` 0.9, `ultima_journal` (workspace sibling), `tokio`, `bincode 2`, `bytes`, `serde`, `thiserror`, `async-trait`, `tracing`. No `quinn`, no shmem, no `ultima_db` in M1.

**Reference:** `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` (the canonical design). Where this plan says "per spec §6", consult that section.

**Out of scope for M1:** QUIC inter-node network (M2), `uc_protocol` ring buffers (M3), service-process split (M3), `uc_client` real implementation (M4), output handler (M5), Prometheus exporter (M5).

---

## File Structure

```
ultima_cluster/
├── Cargo.toml                          # [workspace]
├── rust-toolchain.toml                 # pinned channel
├── uc_protocol/
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs                      # re-exports
│       ├── version.rs                  # ProtocolVersion, MIN_COMPATIBLE
│       ├── error_codes.rs              # stable u16 codes
│       └── magic.rs                    # CNC_MAGIC, RING_MAGIC, FRAME_MAGIC (constants only in M1)
├── uc_service/
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── state_machine.rs            # StateMachine trait
│       ├── output_handler.rs           # OutputHandler trait + NoopOutput
│       └── error.rs                    # SnapshotError, OutputError
├── uc_node/
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs                      # NodeBuilder + NodeHandle re-exports
│       ├── config.rs                   # NodeConfig, BootstrapConfig, RaftTuning, TlsConfig
│       ├── error.rs                    # ClusterError
│       ├── raft/
│       │   ├── mod.rs                  # TypeConfig, NodeId, NodeAddr, AppCommand, AppResponse
│       │   ├── log_storage.rs          # JournalLogStorage : RaftLogStorage
│       │   └── state_machine.rs        # AdaptedStateMachine<S> : RaftStateMachine
│       └── runtime/
│           ├── mod.rs
│           ├── builder.rs              # NodeBuilder<S>
│           ├── node.rs                 # NodeHandle<S>
│           └── recovery.rs             # log replay through embedded apply
├── uc_client/
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs                      # M1 stub — exposes ClientError only
│       └── error.rs
└── tests/
    └── m1_single_node.rs               # workspace-root integration test
```

Decisions locked here:
- For M1, `NodeBuilder<S>` is generic over `S: StateMachine`. M3 will introduce a non-generic shmem-fronted path; the generic embedded path stays.
- `uc_protocol` is `no_std`-friendly even in M1 (we don't add `std::*` imports). This reserves the option without adding cost.
- `uc_client` exists as an empty crate so the workspace shape matches the spec from day one. Real implementation arrives in M4.

---

## Task 1: Workspace skeleton

**Files:**
- Create: `Cargo.toml` (workspace root — overwrites the existing single-crate one)
- Create: `rust-toolchain.toml`
- Modify: `.gitignore` (add `target/`, `Cargo.lock` only at workspace root tracked, etc.)

- [ ] **Step 1: Replace root `Cargo.toml`**

```toml
# Cargo.toml
[workspace]
resolver = "2"
members = ["uc_protocol", "uc_service", "uc_node", "uc_client"]

[workspace.package]
edition = "2024"
version = "0.1.0"
license = "Apache-2.0"
authors = ["Peter Knego"]

[workspace.dependencies]
# core
serde = { version = "1", features = ["derive"] }
bincode = { version = "2", features = ["serde"] }
bytes = "1"
thiserror = "2"
tracing = "0.1"

# async
tokio = { version = "1", features = ["macros", "rt-multi-thread", "sync", "signal", "time", "fs", "io-util"] }
async-trait = "0.1"
futures = "0.3"

# raft + storage
openraft = { version = "0.9", features = ["serde"] }
ultima-journal = { path = "../ultima_db/ultima_journal" }
# ultima_db deliberately omitted in M1; arrives in M3 as a uc_service feature

# dev
tempfile = "3"
anyhow = "1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

[profile.release]
lto = "thin"
codegen-units = 1
debug = 1
```

- [ ] **Step 2: Create `rust-toolchain.toml`**

```toml
# rust-toolchain.toml
[toolchain]
channel = "stable"
components = ["rustfmt", "clippy"]
```

- [ ] **Step 3: Update `.gitignore`**

Append to existing `.gitignore`:
```
/target/
**/*.rs.bk
```

- [ ] **Step 4: Verify workspace parses**

Run: `cargo metadata --no-deps -q | head -c 0`
Expected: exit 0 (no error). The four member directories don't exist yet, so `cargo metadata` will fail — that's fine; we'll create them next. Actually run only after Task 2 finishes.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml rust-toolchain.toml .gitignore
git rm src/main.rs
rmdir src
git commit -m "chore(m1): workspace skeleton"
```

---

## Task 2: `uc_protocol` crate skeleton

**Files:**
- Create: `uc_protocol/Cargo.toml`
- Create: `uc_protocol/src/lib.rs`
- Create: `uc_protocol/src/version.rs`
- Create: `uc_protocol/src/error_codes.rs`
- Create: `uc_protocol/src/magic.rs`

- [ ] **Step 1: Create `uc_protocol/Cargo.toml`**

```toml
[package]
name = "uc_protocol"
edition.workspace = true
version.workspace = true
license.workspace = true
authors.workspace = true
description = "ultima_cluster wire protocol — shared-memory layouts, frames, versions"

[dependencies]
# no_std-friendly; nothing pulled in for M1 stub
```

- [ ] **Step 2: Create `uc_protocol/src/lib.rs`**

```rust
//! Canonical wire spec for ultima_cluster shared-memory IPC.
//!
//! M1 only ships protocol-version constants, magic bytes, and stable error codes.
//! Ring buffer types and frame layouts arrive in M3.

#![cfg_attr(not(test), no_std)]

pub mod error_codes;
pub mod magic;
pub mod version;

pub use error_codes::ErrorCode;
pub use version::{ProtocolVersion, MIN_COMPATIBLE};
```

- [ ] **Step 3: Create `uc_protocol/src/version.rs`**

```rust
/// Encoded as packed u32: (major:u8 << 24) | (minor:u8 << 16) | (patch:u16).
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct ProtocolVersion(pub u32);

impl ProtocolVersion {
    pub const fn new(major: u8, minor: u8, patch: u16) -> Self {
        Self(((major as u32) << 24) | ((minor as u32) << 16) | (patch as u32))
    }
    pub const fn major(self) -> u8 { (self.0 >> 24) as u8 }
    pub const fn minor(self) -> u8 { ((self.0 >> 16) & 0xFF) as u8 }
    pub const fn patch(self) -> u16 { (self.0 & 0xFFFF) as u16 }

    /// Compatible if same major and `other.minor <= self.minor`.
    pub const fn compatible_with(self, other: ProtocolVersion) -> bool {
        self.major() == other.major() && other.minor() <= self.minor()
    }
}

pub const CURRENT: ProtocolVersion = ProtocolVersion::new(0, 1, 0);
pub const MIN_COMPATIBLE: ProtocolVersion = ProtocolVersion::new(0, 1, 0);
```

- [ ] **Step 4: Create `uc_protocol/src/error_codes.rs`**

```rust
/// Stable u16 error codes for cross-crate / cross-process error transport.
/// Code values MUST NOT be reused; deprecate by name only.
#[repr(u16)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ErrorCode {
    Unknown            = 0,
    AppIdMismatch      = 1,
    ProtocolMismatch   = 2,
    InstanceIdChanged  = 3,
    NotLeader          = 10,
    Stalled            = 11,
    ApplyFailed        = 20,
    QueryFailed        = 21,
    SnapshotFailed     = 30,
    OutputRetryable    = 40,
    OutputPermanent    = 41,
    BadFrame           = 50,
    Timeout            = 60,
}

impl ErrorCode {
    pub const fn as_u16(self) -> u16 { self as u16 }

    pub const fn from_u16(v: u16) -> Self {
        match v {
            1 => Self::AppIdMismatch,
            2 => Self::ProtocolMismatch,
            3 => Self::InstanceIdChanged,
            10 => Self::NotLeader,
            11 => Self::Stalled,
            20 => Self::ApplyFailed,
            21 => Self::QueryFailed,
            30 => Self::SnapshotFailed,
            40 => Self::OutputRetryable,
            41 => Self::OutputPermanent,
            50 => Self::BadFrame,
            60 => Self::Timeout,
            _ => Self::Unknown,
        }
    }
}
```

- [ ] **Step 5: Create `uc_protocol/src/magic.rs`**

```rust
/// Magic byte sequences. Used to detect catastrophically wrong files.
pub const CNC_MAGIC:   [u8; 8] = *b"ULTCNC\0\0";
pub const RING_MAGIC:  [u8; 8] = *b"ULTRNG\0\0";
pub const FRAME_MAGIC: [u8; 4] = *b"ULTC";
```

- [ ] **Step 6: Build the crate**

Run: `cargo build -p uc_protocol`
Expected: compiles cleanly.

- [ ] **Step 7: Add and run unit tests**

Append to `uc_protocol/src/version.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_round_trip() {
        let v = ProtocolVersion::new(1, 2, 3);
        assert_eq!(v.major(), 1);
        assert_eq!(v.minor(), 2);
        assert_eq!(v.patch(), 3);
    }

    #[test]
    fn compat_same_major_lower_minor_ok() {
        let a = ProtocolVersion::new(1, 5, 0);
        let b = ProtocolVersion::new(1, 3, 0);
        assert!(a.compatible_with(b));
    }

    #[test]
    fn compat_higher_minor_in_other_rejected() {
        let a = ProtocolVersion::new(1, 3, 0);
        let b = ProtocolVersion::new(1, 5, 0);
        assert!(!a.compatible_with(b));
    }

    #[test]
    fn compat_different_major_rejected() {
        let a = ProtocolVersion::new(1, 0, 0);
        let b = ProtocolVersion::new(2, 0, 0);
        assert!(!a.compatible_with(b));
    }
}
```

Run: `cargo test -p uc_protocol`
Expected: 4 passed.

- [ ] **Step 8: Commit**

```bash
git add uc_protocol/
git commit -m "feat(uc_protocol): version + error codes + magic constants"
```

---

## Task 3: `uc_service` traits

**Files:**
- Create: `uc_service/Cargo.toml`
- Create: `uc_service/src/lib.rs`
- Create: `uc_service/src/error.rs`
- Create: `uc_service/src/state_machine.rs`
- Create: `uc_service/src/output_handler.rs`

- [ ] **Step 1: Create `uc_service/Cargo.toml`**

```toml
[package]
name = "uc_service"
edition.workspace = true
version.workspace = true
license.workspace = true
authors.workspace = true
description = "ultima_cluster service-side SDK — StateMachine + OutputHandler traits"

[dependencies]
serde = { workspace = true }
async-trait = { workspace = true }
thiserror = { workspace = true }
uc_protocol = { path = "../uc_protocol" }
```

- [ ] **Step 2: Create `uc_service/src/error.rs`**

```rust
use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("snapshot io: {0}")]
    Io(#[from] io::Error),
    #[error("snapshot codec: {0}")]
    Codec(String),
}

#[derive(Debug, Error)]
pub enum OutputError {
    #[error("retryable: {0}")]
    Retryable(String),
    #[error("permanent: {0}")]
    Permanent(String),
}

impl OutputError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, OutputError::Retryable(_))
    }
}
```

- [ ] **Step 3: Create `uc_service/src/state_machine.rs`**

```rust
use serde::{Serialize, de::DeserializeOwned};
use std::io::{Read, Write};

use crate::error::SnapshotError;

/// Deterministic state machine. apply() runs serially on every node; query()
/// runs on the leader (linearizable) or any node (snapshot).
///
/// Invariants the framework relies on:
///   * apply MUST be deterministic (no clocks, no rand, no I/O).
///   * apply MUST be sync — you cannot await across the call.
///   * last_applied() MUST reflect the highest log_index for which apply()
///     completed AND the result is durable.
///   * build_snapshot returns the log_index its bytes represent (resolves
///     the build-vs-apply race).
///   * install_snapshot returns the new last_applied after a successful install.
pub trait StateMachine: Send + Sync + 'static {
    type Command:       Serialize + DeserializeOwned + Send + Sync + 'static;
    type Response:      Serialize + DeserializeOwned + Send + 'static;
    type Query:         Serialize + DeserializeOwned + Send + Sync + 'static;
    type QueryResponse: Serialize + DeserializeOwned + Send + 'static;

    fn apply(&mut self, log_index: u64, cmd: Self::Command) -> Self::Response;

    fn query(&self, q: Self::Query) -> Self::QueryResponse;

    fn last_applied(&self) -> Option<u64>;

    fn build_snapshot(&self, dst: &mut dyn Write) -> Result<u64, SnapshotError>;

    fn install_snapshot(&mut self, src: &mut dyn Read) -> Result<u64, SnapshotError>;
}
```

- [ ] **Step 4: Create `uc_service/src/output_handler.rs`**

```rust
use async_trait::async_trait;

use crate::error::OutputError;
use crate::state_machine::StateMachine;

/// Leader-only post-commit hook. At-least-once delivery via durable progress
/// marker on the node side; user MUST make on_committed idempotent.
/// log_index is the natural idempotency key.
#[async_trait]
pub trait OutputHandler<S: StateMachine>: Send + Sync + 'static {
    async fn on_committed(
        &self,
        log_index: u64,
        cmd: &S::Command,
        state: &S,
    ) -> Result<(), OutputError>;
}

pub struct NoopOutput;

#[async_trait]
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

- [ ] **Step 5: Create `uc_service/src/lib.rs`**

```rust
//! Service-side SDK for ultima_cluster.
//!
//! M1 ships the trait surface only. ServiceBuilder + shmem runtime + ultima_db
//! adapter arrive in M3.

pub mod error;
pub mod output_handler;
pub mod state_machine;

pub use error::{OutputError, SnapshotError};
pub use output_handler::{NoopOutput, OutputHandler};
pub use state_machine::StateMachine;
```

- [ ] **Step 6: Build and clippy**

Run: `cargo build -p uc_service && cargo clippy -p uc_service -- -D warnings`
Expected: clean build.

- [ ] **Step 7: Commit**

```bash
git add uc_service/
git commit -m "feat(uc_service): StateMachine + OutputHandler traits + NoopOutput"
```

---

## Task 4: `uc_client` stub crate

**Files:**
- Create: `uc_client/Cargo.toml`
- Create: `uc_client/src/lib.rs`
- Create: `uc_client/src/error.rs`

- [ ] **Step 1: Create `uc_client/Cargo.toml`**

```toml
[package]
name = "uc_client"
edition.workspace = true
version.workspace = true
license.workspace = true
authors.workspace = true
description = "ultima_cluster local-shmem client SDK (M1 stub)"

[dependencies]
thiserror = { workspace = true }
uc_protocol = { path = "../uc_protocol" }
```

- [ ] **Step 2: Create `uc_client/src/error.rs`**

```rust
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("not connected")]
    NotConnected,
    #[error("app_id mismatch")]
    AppIdMismatch,
    #[error("protocol mismatch")]
    ProtocolMismatch,
    #[error("node stalled")]
    NodeStalled,
    #[error("service stalled")]
    ServiceStalled,
    #[error("not leader; hint: {hint:?}")]
    NotLeader { hint: Option<u64> },
    #[error("submission: {0}")]
    Submission(String),
    #[error("decode: {0}")]
    Decode(String),
    #[error("timeout after {0:?}")]
    Timeout(Duration),
}
```

- [ ] **Step 3: Create `uc_client/src/lib.rs`**

```rust
//! Local-shmem client SDK for ultima_cluster.
//!
//! M1 ships the error type only. The Client struct + submit/query API arrive in M4.

pub mod error;
pub use error::ClientError;
```

- [ ] **Step 4: Build**

Run: `cargo build -p uc_client`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add uc_client/
git commit -m "feat(uc_client): M1 stub crate (ClientError only)"
```

---

## Task 5: `uc_node` crate skeleton + config + error

**Files:**
- Create: `uc_node/Cargo.toml`
- Create: `uc_node/src/lib.rs`
- Create: `uc_node/src/config.rs`
- Create: `uc_node/src/error.rs`

- [ ] **Step 1: Create `uc_node/Cargo.toml`**

```toml
[package]
name = "uc_node"
edition.workspace = true
version.workspace = true
license.workspace = true
authors.workspace = true
description = "ultima_cluster engine — Raft, journal, network (M1: embedded only)"

[dependencies]
serde = { workspace = true }
bincode = { workspace = true }
bytes = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }
tokio = { workspace = true }
async-trait = { workspace = true }
futures = { workspace = true }
openraft = { workspace = true }
ultima-journal = { workspace = true }
uc_protocol = { path = "../uc_protocol" }
uc_service = { path = "../uc_service" }

[dev-dependencies]
tempfile = { workspace = true }
anyhow = { workspace = true }
tracing-subscriber = { workspace = true }
```

- [ ] **Step 2: Create `uc_node/src/error.rs`**

```rust
use std::io;
use thiserror::Error;

use uc_service::{OutputError, SnapshotError};

#[derive(Debug, Error)]
pub enum ClusterError {
    #[error("config: {0}")]
    Config(String),
    #[error("recovery: {0}")]
    Recovery(String),
    #[error("journal: {0}")]
    Journal(#[from] ultima_journal::JournalError),
    #[error("stable value: {0}")]
    StableValue(#[from] ultima_journal::StableValueError),
    #[error("snapshot: {0}")]
    Snapshot(#[from] SnapshotError),
    #[error("raft: {0}")]
    Raft(String),
    #[error("not leader; current leader: {leader_id:?}")]
    NotLeader { leader_id: Option<u64> },
    #[error("output: {0}")]
    Output(#[from] OutputError),
    #[error("shut down")]
    ShutDown,
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("bincode: {0}")]
    Bincode(String),
}

impl From<bincode::error::EncodeError> for ClusterError {
    fn from(e: bincode::error::EncodeError) -> Self { Self::Bincode(e.to_string()) }
}
impl From<bincode::error::DecodeError> for ClusterError {
    fn from(e: bincode::error::DecodeError) -> Self { Self::Bincode(e.to_string()) }
}
```

- [ ] **Step 3: Create `uc_node/src/config.rs`**

```rust
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

pub type NodeId = u64;

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: NodeId,
    pub data_dir: PathBuf,
    pub raft_listen_addr: SocketAddr,        // unused in M1 (no QUIC yet); reserved
    pub app_id: String,
    pub bootstrap: BootstrapConfig,
    pub raft: RaftTuning,
}

#[derive(Debug, Clone)]
pub enum BootstrapConfig {
    Resume,
    SingleNode,
    Peers { peers: Vec<PeerSeed> },          // unused in M1
}

#[derive(Debug, Clone)]
pub struct PeerSeed {
    pub node_id: NodeId,
    pub raft_addr: SocketAddr,
}

#[derive(Debug, Clone)]
pub struct RaftTuning {
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub max_in_snapshot_log_to_keep: u64,
}

impl Default for RaftTuning {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 250,
            election_timeout_min_ms: 1000,
            election_timeout_max_ms: 2000,
            max_in_snapshot_log_to_keep: 1000,
        }
    }
}

impl NodeConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.app_id.is_empty() { return Err("app_id must not be empty".into()); }
        if self.app_id.len() > 64 { return Err("app_id must be <= 64 bytes".into()); }
        if self.raft.election_timeout_min_ms >= self.raft.election_timeout_max_ms {
            return Err("election_timeout_min_ms must be < max".into());
        }
        Ok(())
    }
}

#[allow(dead_code)]            // Duration import will be used by Tuning fields in later milestones
const _: Option<Duration> = None;
```

- [ ] **Step 4: Create `uc_node/src/lib.rs`**

```rust
//! ultima_cluster engine. M1 = embedded single-node.

pub mod config;
pub mod error;
pub mod raft;
pub mod runtime;

pub use config::{BootstrapConfig, NodeConfig, NodeId, PeerSeed, RaftTuning};
pub use error::ClusterError;
pub use runtime::builder::NodeBuilder;
pub use runtime::node::NodeHandle;
```

(Files referenced under `raft/` and `runtime/` will be created in subsequent tasks. Comment out the `pub mod raft; pub mod runtime;` lines for now and re-enable when those tasks land.)

Replace the `pub mod raft; pub mod runtime;` lines with:

```rust
// pub mod raft;       // enabled in Task 6
// pub mod runtime;    // enabled in Task 12
```

And the re-exports at the bottom likewise commented:

```rust
// pub use runtime::builder::NodeBuilder;
// pub use runtime::node::NodeHandle;
```

- [ ] **Step 5: Build**

Run: `cargo build -p uc_node`
Expected: clean. `Duration` and other imports may produce dead-code warnings — those are addressed when later tasks enable the modules.

- [ ] **Step 6: Commit**

```bash
git add uc_node/
git commit -m "feat(uc_node): crate skeleton + NodeConfig + ClusterError"
```

---

## Task 6: openraft `TypeConfig`

**Files:**
- Create: `uc_node/src/raft/mod.rs`

- [ ] **Step 1: Create `uc_node/src/raft/mod.rs`**

```rust
//! openraft TypeConfig and supporting types for ultima_cluster.

use std::net::SocketAddr;
use serde::{Deserialize, Serialize};
use bytes::Bytes;

pub mod log_storage;       // Task 7
pub mod state_machine;     // Task 11

pub type NodeId = u64;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeAddr {
    pub raft_addr: SocketAddr,
}

/// User-Command bytes, refcounted for zero-copy flow through the apply pipeline.
pub type AppCommand = Bytes;

/// User-Response bytes, also refcounted.
pub type AppResponse = Bytes;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = AppCommand,
        R = AppResponse,
        NodeId = NodeId,
        Node = NodeAddr,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,    // M1: in-memory; M5 swaps to snapshot.region reader
        AsyncRuntime = openraft::TokioRuntime,
);
```

(The `pub mod log_storage; pub mod state_machine;` lines reference files we'll create next. If openraft 0.9 requires a different `declare_raft_types!` shape, follow its current docs — the declaration must produce a public `TypeConfig` aliasing the type names above.)

- [ ] **Step 2: Enable the module in `uc_node/src/lib.rs`**

Uncomment `pub mod raft;` in `uc_node/src/lib.rs`.

- [ ] **Step 3: Stub the inner files so the build passes**

Create `uc_node/src/raft/log_storage.rs`:
```rust
//! Filled in by Task 7.
```

Create `uc_node/src/raft/state_machine.rs`:
```rust
//! Filled in by Task 11.
```

- [ ] **Step 4: Build**

Run: `cargo build -p uc_node`
Expected: clean. Some unused-import warnings on `state_machine` etc. are acceptable until Task 7+11 land.

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/raft/
git commit -m "feat(uc_node): openraft TypeConfig + Bytes-based AppCommand/AppResponse"
```

---

## Task 7: `JournalLogStorage` shell

Map `RaftLogStorage` operations onto `ultima_journal::Journal` + `StableValue`s. This task creates the struct and `open()`; subsequent tasks (8–10) implement the trait methods.

**Files:**
- Modify: `uc_node/src/raft/log_storage.rs`

- [ ] **Step 1: Replace `uc_node/src/raft/log_storage.rs`**

```rust
//! Implements `openraft::storage::RaftLogStorage` over `ultima_journal`.
//!
//! Storage seam mapping (per spec §6 "RaftLogStorage over ultima_journal"):
//!   * vote / committed / last_purged → StableValue<…>
//!   * append → Journal::append (seq=index, meta=term.0, payload=bincode(entry))
//!   * truncate → Journal::truncate_after
//!   * purge → Journal::purge_before
//!   * get_log_state → first_seq/last_seq + meta lookups
//!   * try_get_log_entries → Journal::iter_range

use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

use openraft::{LogId, Vote};
use ultima_journal::{
    Durability, Journal, JournalConfig, StableValue, StableValueConfig,
};

use crate::ClusterError;
use super::{NodeId, TypeConfig};

const SEGMENT_SIZE_BYTES: u64 = 64 * 1024 * 1024;

pub struct JournalLogStorage {
    pub(crate) journal: Arc<Journal>,
    pub(crate) vote: Arc<StableValue<Vote<NodeId>>>,
    pub(crate) committed: Arc<StableValue<LogId<NodeId>>>,
    pub(crate) last_purged: Arc<StableValue<LogId<NodeId>>>,
    /// Serializes seq assignment per the journal's caller-coordination requirement.
    /// openraft already serializes appends, so this is a no-contention guarantee.
    pub(crate) append_lock: Arc<Mutex<()>>,
}

impl JournalLogStorage {
    pub fn open(data_dir: &Path) -> Result<Self, ClusterError> {
        std::fs::create_dir_all(data_dir.join("journal"))?;

        let journal = Arc::new(Journal::open(JournalConfig {
            dir: data_dir.join("journal"),
            segment_size_bytes: SEGMENT_SIZE_BYTES,
            durability: Durability::Consistent,
        })?);

        let vote = Arc::new(StableValue::open(StableValueConfig {
            path: data_dir.join("vote.state"),
            durability: Durability::Consistent,
            max_payload_bytes: 4096 - 17,
        })?);

        let committed = Arc::new(StableValue::open(StableValueConfig {
            path: data_dir.join("committed.state"),
            durability: Durability::Consistent,
            max_payload_bytes: 4096 - 17,
        })?);

        let last_purged = Arc::new(StableValue::open(StableValueConfig {
            path: data_dir.join("last_purged.state"),
            durability: Durability::Consistent,
            max_payload_bytes: 4096 - 17,
        })?);

        Ok(Self {
            journal,
            vote,
            committed,
            last_purged,
            append_lock: Arc::new(Mutex::new(())),
        })
    }
}
```

- [ ] **Step 2: Build**

Run: `cargo build -p uc_node`
Expected: clean. The struct is unused so far.

- [ ] **Step 3: Commit**

```bash
git add uc_node/src/raft/log_storage.rs
git commit -m "feat(uc_node): JournalLogStorage::open over ultima_journal + StableValues"
```

---

## Task 8: `JournalLogStorage::open` recovery test

Before implementing the trait methods, lock the recovery contract with a test.

**Files:**
- Create: `uc_node/tests/log_storage_open.rs`

- [ ] **Step 1: Create the test**

```rust
//! Verifies that JournalLogStorage::open is idempotent and survives restart
//! with empty state.

use tempfile::TempDir;
use uc_node::raft::log_storage::JournalLogStorage;

#[test]
fn open_creates_fresh_data_dir() {
    let dir = TempDir::new().unwrap();
    let storage = JournalLogStorage::open(dir.path()).expect("open");
    drop(storage);

    // Reopen — must succeed and observe the same empty state.
    let storage = JournalLogStorage::open(dir.path()).expect("reopen");
    assert!(storage.vote.load().expect("load vote").is_none());
    assert!(storage.committed.load().expect("load committed").is_none());
    assert!(storage.last_purged.load().expect("load last_purged").is_none());
}
```

`JournalLogStorage`'s fields are `pub(crate)` — to expose for tests, add `pub mod log_storage;` in `raft/mod.rs` (already present) and a re-export in `lib.rs`:

In `uc_node/src/lib.rs` make `pub mod raft;` already present; tests import via `uc_node::raft::log_storage::JournalLogStorage`. The fields are `pub(crate)` which is `pub` to integration tests since they live outside the crate. Change them to `pub(crate)` → `pub` only on `vote`, `committed`, `last_purged` for test visibility:

Actually integration tests cannot access `pub(crate)`. To keep encapsulation, expose accessors:

```rust
// In uc_node/src/raft/log_storage.rs, add:
impl JournalLogStorage {
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn _testonly_vote(&self) -> &StableValue<Vote<NodeId>> { &self.vote }
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn _testonly_committed(&self) -> &StableValue<LogId<NodeId>> { &self.committed }
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn _testonly_last_purged(&self) -> &StableValue<LogId<NodeId>> { &self.last_purged }
}
```

And in `uc_node/Cargo.toml` add:
```toml
[features]
test-helpers = []
```

Then update the test to use the accessors:

```rust
use tempfile::TempDir;
use uc_node::raft::log_storage::JournalLogStorage;

#[test]
fn open_creates_fresh_data_dir() {
    let dir = TempDir::new().unwrap();
    let storage = JournalLogStorage::open(dir.path()).expect("open");
    drop(storage);

    let storage = JournalLogStorage::open(dir.path()).expect("reopen");
    assert!(storage._testonly_vote().load().expect("load vote").is_none());
    assert!(storage._testonly_committed().load().expect("load committed").is_none());
    assert!(storage._testonly_last_purged().load().expect("load last_purged").is_none());
}
```

Run integration tests with: `cargo test -p uc_node --features test-helpers --test log_storage_open`

- [ ] **Step 2: Run the test, expect pass**

Run: `cargo test -p uc_node --features test-helpers --test log_storage_open`
Expected: 1 passed.

- [ ] **Step 3: Commit**

```bash
git add uc_node/tests/log_storage_open.rs uc_node/Cargo.toml uc_node/src/raft/log_storage.rs
git commit -m "test(uc_node): JournalLogStorage::open is idempotent"
```

---

## Task 9: Implement `RaftLogStorage` trait — vote / committed / last_purged

Implement the simple StableValue-backed methods first.

**Files:**
- Modify: `uc_node/src/raft/log_storage.rs`

- [ ] **Step 1: Add the impl block**

Per spec §6 mapping table. The exact openraft 0.9 trait method signatures may differ slightly — consult the openraft docs at https://deepwiki.com/databendlabs/openraft/2.3-implementing-storage-traits for current shape. The mapping below is canonical; the code body must match openraft 0.9's actual `RaftLogStorage` trait.

Append to `uc_node/src/raft/log_storage.rs`:

```rust
use openraft::storage::IOFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogStorage;
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::StorageError;
use std::ops::RangeBounds;

#[async_trait::async_trait]
impl RaftLogReader<TypeConfig> for JournalLogStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<TypeConfig>>, StorageError<NodeId>> {
        // Implemented in Task 10.
        let _ = range;
        unimplemented!("Task 10")
    }
}

#[async_trait::async_trait]
impl RaftLogStorage<TypeConfig> for JournalLogStorage {
    type LogReader = Self;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        Self {
            journal: self.journal.clone(),
            vote: self.vote.clone(),
            committed: self.committed.clone(),
            last_purged: self.last_purged.clone(),
            append_lock: self.append_lock.clone(),
        }
    }

    async fn save_vote(
        &mut self,
        vote: &openraft::Vote<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        self.vote
            .store(vote)
            .map_err(map_sv_err)?
            .wait()
            .map_err(map_journal_err)?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<NodeId>>, StorageError<NodeId>> {
        self.vote.load().map_err(map_sv_err)
    }

    async fn save_committed(
        &mut self,
        committed: Option<openraft::LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        match committed {
            Some(id) => {
                self.committed
                    .store(&id)
                    .map_err(map_sv_err)?
                    .wait()
                    .map_err(map_journal_err)?;
            }
            None => {
                self.committed.clear().map_err(map_sv_err)?
                    .wait()
                    .map_err(map_journal_err)?;
            }
        }
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<openraft::LogId<NodeId>>, StorageError<NodeId>> {
        self.committed.load().map_err(map_sv_err)
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        // Implemented in Task 10.
        unimplemented!("Task 10")
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<TypeConfig>)
        -> Result<(), StorageError<NodeId>>
    where I: IntoIterator<Item = openraft::Entry<TypeConfig>> + Send {
        // Implemented in Task 10.
        let _ = (entries, callback);
        unimplemented!("Task 10")
    }

    async fn truncate(&mut self, log_id: openraft::LogId<NodeId>)
        -> Result<(), StorageError<NodeId>> {
        // Implemented in Task 10.
        let _ = log_id;
        unimplemented!("Task 10")
    }

    async fn purge(&mut self, log_id: openraft::LogId<NodeId>)
        -> Result<(), StorageError<NodeId>> {
        // Implemented in Task 10.
        let _ = log_id;
        unimplemented!("Task 10")
    }
}

fn map_sv_err(e: ultima_journal::StableValueError) -> StorageError<NodeId> {
    StorageError::write(&e)
}

fn map_journal_err(e: ultima_journal::JournalError) -> StorageError<NodeId> {
    StorageError::write(&e)
}
```

(If openraft 0.9's `RaftLogStorage` requires `Clone` on `Self` instead of returning a clone-ish reader, adapt `get_log_reader`. The `Arc` fields make either approach work.)

- [ ] **Step 2: Build**

Run: `cargo build -p uc_node`
Expected: clean. Methods that call `unimplemented!` will panic if invoked but compile fine.

- [ ] **Step 3: Add unit test for save_vote round-trip**

Append to `uc_node/tests/log_storage_open.rs`:

```rust
use openraft::{LeaderId, Vote};

#[tokio::test]
async fn save_and_read_vote_round_trip() {
    let dir = TempDir::new().unwrap();
    let mut storage = JournalLogStorage::open(dir.path()).expect("open");

    let v = Vote::new(7, 3);
    storage.save_vote(&v).await.expect("save");

    let loaded = storage.read_vote().await.expect("read");
    assert_eq!(loaded, Some(v));

    drop(storage);
    let mut storage = JournalLogStorage::open(dir.path()).expect("reopen");
    let loaded = storage.read_vote().await.expect("read after reopen");
    assert_eq!(loaded, Some(v));
}
```

(Imports `RaftLogStorage` trait via `use openraft::storage::RaftLogStorage as _;` if needed for the method calls.)

Run: `cargo test -p uc_node --features test-helpers --test log_storage_open save_and_read_vote_round_trip`
Expected: pass.

- [ ] **Step 4: Commit**

```bash
git add uc_node/src/raft/log_storage.rs uc_node/tests/log_storage_open.rs
git commit -m "feat(uc_node): RaftLogStorage save_vote/read_vote/read_committed/get_log_reader"
```

---

## Task 10: Implement remaining `RaftLogStorage` methods — append / truncate / purge / log_state / try_get_log_entries

Replace the four `unimplemented!()` bodies. Each gets its own sub-step.

**Files:**
- Modify: `uc_node/src/raft/log_storage.rs`

- [ ] **Step 1: Implement `try_get_log_entries`**

Replace the `unimplemented!("Task 10")` body in the `RaftLogReader` impl with:

```rust
let bounds = (range.start_bound().cloned(), range.end_bound().cloned());
let mut entries = Vec::new();
let iter = self
    .journal
    .iter_range(bounds)
    .map_err(map_journal_err)?;
for record in iter {
    let (_seq, _meta, payload) = record.map_err(map_journal_err)?;
    let (entry, _) = bincode::serde::decode_from_slice::<openraft::Entry<TypeConfig>, _>(
        &payload, bincode::config::standard())
        .map_err(|e| StorageError::read_logs(&std::io::Error::other(e.to_string())))?;
    entries.push(entry);
}
Ok(entries)
```

- [ ] **Step 2: Implement `get_log_state`**

Replace the body with:

```rust
let last_seq = self.journal.last_seq();
let first_seq = self.journal.first_seq();

// Last log id from journal (term in meta).
let last_log_id = if let Some(seq) = last_seq {
    let rec = self.journal.read(seq).map_err(map_journal_err)?
        .ok_or_else(|| StorageError::read_logs(&std::io::Error::other("missing last record")))?;
    let (meta, _) = rec;
    Some(openraft::LogId::new(
        openraft::CommittedLeaderId::new(meta, 0 /* leader_id placeholder; see openraft 0.9 docs */),
        seq,
    ))
} else {
    None
};

// last_purged_log_id from StableValue.
let last_purged_log_id = self.last_purged.load().map_err(map_sv_err)?;

let _ = first_seq;    // exposed via openraft's get_log_state if needed
Ok(LogState { last_purged_log_id, last_log_id })
```

(The exact `LogId` construction depends on openraft 0.9's `LeaderId` shape — verify against the openraft docs. The principle is: term comes from journal `meta`, index is the seq.)

- [ ] **Step 3: Implement `append`**

Replace the body with:

```rust
let _guard = self.append_lock.lock().unwrap();

let mut last_notifier: Option<ultima_journal::Notifier> = None;

for entry in entries {
    let payload = bincode::serde::encode_to_vec(&entry, bincode::config::standard())
        .map_err(|e| StorageError::write_logs(&std::io::Error::other(e.to_string())))?;

    let term: u64 = entry.log_id.leader_id.term;     // openraft 0.9 leader-id.term accessor
    let seq: u64 = entry.log_id.index;

    let notifier = self.journal.append(seq, term, &payload).map_err(map_journal_err)?;
    last_notifier = Some(notifier);
}

if let Some(notifier) = last_notifier {
    // Chain IOFlushed → Notifier::on_complete (per spec §6 — bg fsync thread invokes
    // the callback inline, so openraft's IOFlushed::io_completed fires without thread hop).
    notifier.on_complete(move |result| {
        let openraft_result = result.map_err(|e| {
            openraft::StorageIOError::write_logs(&std::io::Error::other(e.to_string()))
        });
        callback.io_completed(openraft_result);
    });
} else {
    // No entries → completion is immediate.
    callback.io_completed(Ok(()));
}

Ok(())
```

(Adjust to openraft 0.9's exact `IOFlushed` API and the `LeaderId::term` accessor name. The principle is per spec §6 mapping table.)

- [ ] **Step 4: Implement `truncate`**

```rust
// Remove entries with index >= log_id.index (i.e., keep entries with index < log_id.index).
let keep_seq = log_id.index.saturating_sub(1);
self.journal.truncate_after(keep_seq).map_err(map_journal_err)?
    .wait().map_err(map_journal_err)?;
Ok(())
```

- [ ] **Step 5: Implement `purge`**

```rust
self.journal.purge_before(log_id.index + 1).map_err(map_journal_err)?;
self.last_purged.store(&log_id).map_err(map_sv_err)?
    .wait().map_err(map_journal_err)?;
Ok(())
```

- [ ] **Step 6: Build**

Run: `cargo build -p uc_node`
Expected: clean. There will be openraft API surface details that need polishing against the actual crate; iterate until it builds.

- [ ] **Step 7: Add a smoke test for append + read**

Append to `uc_node/tests/log_storage_open.rs`:

```rust
use openraft::{Entry, EntryPayload};

#[tokio::test]
async fn append_then_read_round_trip() {
    let dir = TempDir::new().unwrap();
    let mut storage = JournalLogStorage::open(dir.path()).expect("open");

    // Build 3 simple entries.
    let entries: Vec<Entry<uc_node::raft::TypeConfig>> = (1..=3u64).map(|i| {
        Entry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 0), i),
            payload: EntryPayload::Normal(bytes::Bytes::from(format!("cmd-{i}"))),
        }
    }).collect();

    let (cb, mut completed_rx) = openraft::storage::IOFlushed::test_callback();
    storage.append(entries, cb).await.expect("append");
    completed_rx.recv().await.expect("io completed");

    let read = storage.try_get_log_entries(1u64..=3u64).await.expect("read");
    assert_eq!(read.len(), 3);
}
```

(If openraft 0.9 doesn't expose `IOFlushed::test_callback`, use a oneshot channel + a manual `IOFlushed` constructor per the openraft test utilities.)

Run: `cargo test -p uc_node --features test-helpers --test log_storage_open append_then_read_round_trip`
Expected: pass.

- [ ] **Step 8: Commit**

```bash
git add uc_node/src/raft/log_storage.rs uc_node/tests/log_storage_open.rs
git commit -m "feat(uc_node): RaftLogStorage append/truncate/purge/get_log_state/try_get_log_entries"
```

---

## Task 11: `AdaptedStateMachine<S>` — embedded mode

Wrap user's `StateMachine` so it satisfies `openraft::storage::RaftStateMachine`. M1 calls the user trait directly; M3 will replace the call with a publish to the apply ring.

**Files:**
- Modify: `uc_node/src/raft/state_machine.rs`

- [ ] **Step 1: Replace `uc_node/src/raft/state_machine.rs`**

```rust
//! `RaftStateMachine` adapter.
//!
//! M1 (embedded mode): calls user's `StateMachine::apply` directly inside the
//! adapter, under a tokio Mutex serializing access. M3 will replace the direct
//! call with a publish to `service/apply.ring`.

use std::io::Cursor;
use std::sync::Arc;

use openraft::{
    storage::{RaftStateMachine, Snapshot, SnapshotMeta},
    EntryPayload, LogId, StoredMembership, StorageError,
};
use tokio::sync::Mutex;

use uc_service::StateMachine;

use super::{NodeAddr, NodeId, TypeConfig};

pub struct AdaptedStateMachine<S: StateMachine> {
    pub(crate) inner: Arc<Mutex<Inner<S>>>,
}

pub(crate) struct Inner<S: StateMachine> {
    pub(crate) sm: S,
    pub(crate) last_applied: Option<LogId<NodeId>>,
    pub(crate) last_membership: StoredMembership<NodeId, NodeAddr>,
    pub(crate) snapshot_idx: u64,
    pub(crate) current_snapshot: Option<StoredSnapshot>,
}

#[derive(Clone)]
pub(crate) struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, NodeAddr>,
    pub data: Vec<u8>,
}

impl<S: StateMachine> AdaptedStateMachine<S> {
    pub fn new(sm: S) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                sm,
                last_applied: None,
                last_membership: StoredMembership::default(),
                snapshot_idx: 0,
                current_snapshot: None,
            })),
        }
    }
}

#[async_trait::async_trait]
impl<S: StateMachine> RaftStateMachine<TypeConfig> for AdaptedStateMachine<S> {
    type SnapshotBuilder = SnapshotBuilder<S>;

    async fn applied_state(&mut self)
        -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, NodeAddr>), StorageError<NodeId>>
    {
        let g = self.inner.lock().await;
        Ok((g.last_applied.clone(), g.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<bytes::Bytes>, StorageError<NodeId>>
    where I: IntoIterator<Item = openraft::Entry<TypeConfig>> + Send,
    {
        let mut g = self.inner.lock().await;
        let mut responses = Vec::new();

        for entry in entries {
            let log_index = entry.log_id.index;
            g.last_applied = Some(entry.log_id.clone());

            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(bytes::Bytes::new());
                }
                EntryPayload::Normal(cmd_bytes) => {
                    // Decode user's typed command.
                    let (cmd, _) = bincode::serde::decode_from_slice::<S::Command, _>(
                        &cmd_bytes, bincode::config::standard())
                        .map_err(|e| StorageError::apply(entry.log_id.clone(),
                            &std::io::Error::other(e.to_string())))?;

                    // SYNC, deterministic apply call.
                    let resp = g.sm.apply(log_index, cmd);

                    // Encode user's typed response.
                    let resp_bytes = bincode::serde::encode_to_vec(&resp, bincode::config::standard())
                        .map_err(|e| StorageError::apply(entry.log_id.clone(),
                            &std::io::Error::other(e.to_string())))?;
                    responses.push(bytes::Bytes::from(resp_bytes));
                }
                EntryPayload::Membership(m) => {
                    g.last_membership = StoredMembership::new(Some(entry.log_id.clone()), m);
                    responses.push(bytes::Bytes::new());
                }
            }
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder { inner: self.inner.clone() }
    }

    async fn begin_receiving_snapshot(&mut self)
        -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>>
    {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, NodeAddr>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = snapshot.into_inner();
        let mut g = self.inner.lock().await;

        // Hand bytes to user's StateMachine.
        let mut cursor = Cursor::new(bytes.clone());
        let _new_last = g.sm.install_snapshot(&mut cursor)
            .map_err(|e| StorageError::apply(meta.last_log_id.clone().unwrap_or_default(),
                &std::io::Error::other(e.to_string())))?;

        g.last_applied = meta.last_log_id.clone();
        g.last_membership = meta.last_membership.clone();
        g.current_snapshot = Some(StoredSnapshot { meta: meta.clone(), data: bytes });
        Ok(())
    }

    async fn get_current_snapshot(&mut self)
        -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>>
    {
        let g = self.inner.lock().await;
        match &g.current_snapshot {
            Some(s) => Ok(Some(Snapshot {
                meta: s.meta.clone(),
                snapshot: Box::new(Cursor::new(s.data.clone())),
            })),
            None => Ok(None),
        }
    }
}

pub struct SnapshotBuilder<S: StateMachine> {
    inner: Arc<Mutex<Inner<S>>>,
}

#[async_trait::async_trait]
impl<S: StateMachine> openraft::storage::RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder<S> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let mut g = self.inner.lock().await;
        let last_applied = g.last_applied.clone();
        let last_membership = g.last_membership.clone();

        let mut buf: Vec<u8> = Vec::new();
        let _idx = g.sm.build_snapshot(&mut buf)
            .map_err(|e| StorageError::apply(last_applied.clone().unwrap_or_default(),
                &std::io::Error::other(e.to_string())))?;

        g.snapshot_idx += 1;
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id: format!("snap-{}", g.snapshot_idx),
        };
        let stored = StoredSnapshot { meta: meta.clone(), data: buf.clone() };
        g.current_snapshot = Some(stored);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(buf)),
        })
    }
}
```

(Polish exact openraft 0.9 trait method signatures and `StorageError::apply`/`StorageError::read_logs` constructor names against the actual crate.)

- [ ] **Step 2: Build**

Run: `cargo build -p uc_node`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add uc_node/src/raft/state_machine.rs
git commit -m "feat(uc_node): AdaptedStateMachine<S> embedded-mode RaftStateMachine impl"
```

---

## Task 12: `runtime` module skeleton + `NodeBuilder<S>`

**Files:**
- Create: `uc_node/src/runtime/mod.rs`
- Create: `uc_node/src/runtime/builder.rs`
- Create: `uc_node/src/runtime/node.rs`
- Create: `uc_node/src/runtime/recovery.rs`

- [ ] **Step 1: Create `uc_node/src/runtime/mod.rs`**

```rust
pub mod builder;
pub mod node;
pub mod recovery;
```

- [ ] **Step 2: Create `uc_node/src/runtime/recovery.rs`**

Stub for now; M1 only needs the journal/StableValue recovery which is automatic.

```rust
//! Startup reconciliation. Filled in by Task 14.
```

- [ ] **Step 3: Create `uc_node/src/runtime/node.rs`**

```rust
use std::sync::Arc;
use bytes::Bytes;
use openraft::Raft;

use uc_service::StateMachine;

use crate::ClusterError;
use crate::config::{NodeConfig, NodeId};
use crate::raft::TypeConfig;

/// Public handle returned by NodeBuilder::start().
/// M1: generic over S; M3 will introduce a non-generic shmem-fronted handle.
pub struct NodeHandle<S: StateMachine> {
    pub(crate) raft: Raft<TypeConfig>,
    pub(crate) config: NodeConfig,
    pub(crate) _phantom: std::marker::PhantomData<S>,
}

impl<S: StateMachine> NodeHandle<S> {
    pub fn node_id(&self) -> NodeId { self.config.node_id }

    pub async fn current_leader(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }

    /// Embedded-mode submit: bincode-encode the command, push through openraft,
    /// await the typed response.
    pub async fn submit(&self, cmd: S::Command) -> Result<S::Response, ClusterError> {
        let bytes = bincode::serde::encode_to_vec(&cmd, bincode::config::standard())?;
        let app_command: Bytes = Bytes::from(bytes);

        let result = self.raft.client_write(app_command).await
            .map_err(|e| match e {
                openraft::error::ClientWriteError::ForwardToLeader(f) => {
                    ClusterError::NotLeader { leader_id: f.leader_id }
                }
                other => ClusterError::Raft(other.to_string()),
            })?;

        let resp_bytes: Bytes = result.data;
        let (resp, _) = bincode::serde::decode_from_slice::<S::Response, _>(
            &resp_bytes, bincode::config::standard())?;
        Ok(resp)
    }

    /// Embedded-mode query: snapshot read against the local applied state.
    /// Linearizable reads via raft read-index land in M2.
    pub async fn query_snapshot<F, R>(&self, _f: F) -> Result<R, ClusterError>
    where F: FnOnce(&S) -> R + Send + 'static, R: Send + 'static {
        // Implemented in Task 13 (closure embedded shortcut).
        unimplemented!("Task 13")
    }

    pub async fn shutdown(self) -> Result<(), ClusterError> {
        self.raft.shutdown().await
            .map_err(|e| ClusterError::Raft(e.to_string()))?;
        Ok(())
    }
}

/// Internal helper to keep storage Arc'd alongside raft for recovery / tests.
#[allow(dead_code)]
pub(crate) struct StorageBundle {
    pub log: Arc<tokio::sync::Mutex<crate::raft::log_storage::JournalLogStorage>>,
}
```

- [ ] **Step 4: Create `uc_node/src/runtime/builder.rs`**

```rust
use std::sync::Arc;
use openraft::{Raft, Config as RaftConfigOpenraft};
use tokio::sync::Mutex;

use uc_service::StateMachine;

use crate::ClusterError;
use crate::config::{NodeConfig, BootstrapConfig};
use crate::raft::log_storage::JournalLogStorage;
use crate::raft::state_machine::AdaptedStateMachine;
use crate::raft::TypeConfig;
use super::node::NodeHandle;

/// Builds an embedded-mode ultima_cluster node.
/// Generic over S; non-generic shmem-fronted variant arrives in M3.
pub struct NodeBuilder<S: StateMachine> {
    config: NodeConfig,
    state_machine: S,
}

impl<S: StateMachine> NodeBuilder<S> {
    pub fn new(config: NodeConfig, state_machine: S) -> Self {
        Self { config, state_machine }
    }

    pub async fn start(self) -> Result<NodeHandle<S>, ClusterError> {
        self.config.validate().map_err(ClusterError::Config)?;

        // Open log storage.
        let log_storage = JournalLogStorage::open(&self.config.data_dir)?;

        // Adapter over user's state machine.
        let sm_adapter = AdaptedStateMachine::new(self.state_machine);

        // openraft config.
        let raft_config = Arc::new(RaftConfigOpenraft {
            heartbeat_interval: self.config.raft.heartbeat_interval_ms,
            election_timeout_min: self.config.raft.election_timeout_min_ms,
            election_timeout_max: self.config.raft.election_timeout_max_ms,
            max_in_snapshot_log_to_keep: self.config.raft.max_in_snapshot_log_to_keep,
            ..Default::default()
        }.validate().map_err(|e| ClusterError::Config(e.to_string()))?);

        // M1 has no real network — placeholder no-op factory.
        let network = crate::raft::log_storage::test_helpers::NoopNetwork; // see Task 12 step 5

        let raft = Raft::new(
            self.config.node_id,
            raft_config,
            network,
            log_storage,
            sm_adapter,
        ).await.map_err(|e| ClusterError::Raft(e.to_string()))?;

        // Apply bootstrap.
        match &self.config.bootstrap {
            BootstrapConfig::Resume => { /* no-op; raft picks up from durable state */ }
            BootstrapConfig::SingleNode => {
                let mut members = std::collections::BTreeMap::new();
                members.insert(self.config.node_id, crate::raft::NodeAddr {
                    raft_addr: self.config.raft_listen_addr,
                });
                raft.initialize(members).await
                    .map_err(|e| ClusterError::Raft(e.to_string()))?;
            }
            BootstrapConfig::Peers { .. } => {
                return Err(ClusterError::Config(
                    "BootstrapConfig::Peers requires QUIC network (M2)".into()));
            }
        }

        Ok(NodeHandle {
            raft,
            config: self.config,
            _phantom: std::marker::PhantomData,
        })
    }
}
```

- [ ] **Step 5: Add `NoopNetwork` for M1**

In `uc_node/src/raft/log_storage.rs`, add at the bottom:

```rust
pub mod test_helpers {
    //! M1-only no-op RaftNetwork. Replaced by QuicRaftNetwork in M2.
    use openraft::error::{InstallSnapshotError, RPCError, RaftError};
    use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse,
        InstallSnapshotRequest, InstallSnapshotResponse,
        VoteRequest, VoteResponse,
    };
    use crate::raft::{NodeAddr, NodeId, TypeConfig};

    pub struct NoopNetwork;

    impl RaftNetworkFactory<TypeConfig> for NoopNetwork {
        type Network = NoopNetworkInstance;
        async fn new_client(&mut self, _t: NodeId, _n: &NodeAddr) -> Self::Network {
            NoopNetworkInstance
        }
    }

    pub struct NoopNetworkInstance;

    #[async_trait::async_trait]
    impl RaftNetwork<TypeConfig> for NoopNetworkInstance {
        async fn append_entries(
            &mut self, _rpc: AppendEntriesRequest<TypeConfig>, _o: RPCOption,
        ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, NodeAddr, RaftError<NodeId>>> {
            unreachable!("M1 single-node — no network calls")
        }
        async fn install_snapshot(
            &mut self, _rpc: InstallSnapshotRequest<TypeConfig>, _o: RPCOption,
        ) -> Result<InstallSnapshotResponse<NodeId>,
            RPCError<NodeId, NodeAddr, RaftError<NodeId, InstallSnapshotError>>> {
            unreachable!("M1 single-node — no network calls")
        }
        async fn vote(
            &mut self, _rpc: VoteRequest<NodeId>, _o: RPCOption,
        ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, NodeAddr, RaftError<NodeId>>> {
            unreachable!("M1 single-node — no network calls")
        }
    }
}
```

(Adjust to openraft 0.9's exact signature shape — it changes between minor versions.)

- [ ] **Step 6: Enable runtime in `uc_node/src/lib.rs`**

Replace the commented module declarations:
```rust
pub mod raft;
pub mod runtime;

pub use runtime::builder::NodeBuilder;
pub use runtime::node::NodeHandle;
```

- [ ] **Step 7: Build**

Run: `cargo build -p uc_node`
Expected: clean (after openraft API surface adjustments).

- [ ] **Step 8: Commit**

```bash
git add uc_node/src/runtime/ uc_node/src/raft/log_storage.rs uc_node/src/lib.rs
git commit -m "feat(uc_node): NodeBuilder<S> + NodeHandle<S> + bootstrap_single_node"
```

---

## Task 13: Embedded `query_snapshot` (closure shortcut)

In M1, embedded callers can run a closure against the locked state directly. M3 will introduce the IPC-based query path; the closure shortcut stays for embedded mode.

**Files:**
- Modify: `uc_node/src/runtime/node.rs`
- Modify: `uc_node/src/raft/state_machine.rs`

- [ ] **Step 1: Expose state-locking accessor on `AdaptedStateMachine`**

Append to `uc_node/src/raft/state_machine.rs`:

```rust
impl<S: StateMachine> AdaptedStateMachine<S> {
    /// Run a closure against the locked applied state.
    /// Used only by embedded `NodeHandle::query_snapshot` (M1).
    pub async fn with_state<R, F>(&self, f: F) -> R
    where F: FnOnce(&S) -> R + Send {
        let g = self.inner.lock().await;
        f(&g.sm)
    }
}
```

- [ ] **Step 2: Stash an Arc<AdaptedStateMachine<S>> in `NodeHandle<S>`**

The current builder builds `sm_adapter` and passes ownership to `Raft::new`. Refactor to share via `Arc`:

In `uc_node/src/runtime/builder.rs`, change:
```rust
let sm_adapter = AdaptedStateMachine::new(self.state_machine);
// …
let raft = Raft::new(self.config.node_id, raft_config, network, log_storage, sm_adapter).await…;
```

to:
```rust
let sm_adapter = AdaptedStateMachine::new(self.state_machine);
let sm_adapter_handle = sm_adapter.clone();   // requires Clone — see step 3
// …
let raft = Raft::new(self.config.node_id, raft_config, network, log_storage, sm_adapter).await…;

// pass sm_adapter_handle into NodeHandle:
Ok(NodeHandle { raft, config: self.config, sm: sm_adapter_handle, _phantom: PhantomData })
```

- [ ] **Step 3: Add `Clone` to `AdaptedStateMachine`**

In `uc_node/src/raft/state_machine.rs`, the struct already holds `Arc<Mutex<Inner<S>>>`. Add a manual `Clone`:

```rust
impl<S: StateMachine> Clone for AdaptedStateMachine<S> {
    fn clone(&self) -> Self { Self { inner: self.inner.clone() } }
}
```

- [ ] **Step 4: Add the `sm` field to `NodeHandle`**

Update `uc_node/src/runtime/node.rs`:

```rust
pub struct NodeHandle<S: StateMachine> {
    pub(crate) raft: Raft<TypeConfig>,
    pub(crate) config: NodeConfig,
    pub(crate) sm: crate::raft::state_machine::AdaptedStateMachine<S>,
    pub(crate) _phantom: std::marker::PhantomData<S>,
}
```

And replace the `query_snapshot` body:

```rust
pub async fn query_snapshot<F, R>(&self, f: F) -> Result<R, ClusterError>
where F: FnOnce(&S) -> R + Send, R: Send + 'static
{
    Ok(self.sm.with_state(f).await)
}
```

- [ ] **Step 5: Build**

Run: `cargo build -p uc_node`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add uc_node/src/raft/state_machine.rs uc_node/src/runtime/
git commit -m "feat(uc_node): NodeHandle::query_snapshot closure shortcut (embedded mode)"
```

---

## Task 14: Recovery — replay log through embedded apply on startup

After `Raft::new`, openraft itself replays committed entries through the state machine's `apply()` method automatically. We don't need to reimplement that. But we do need to ensure that on a restart (BootstrapConfig::Resume), the journal's recovered state is consistent and apply catches up correctly.

**Files:**
- Modify: `uc_node/src/runtime/recovery.rs`

- [ ] **Step 1: Add a sanity-check helper**

```rust
//! Startup reconciliation helpers.
//!
//! openraft replays committed entries through `RaftStateMachine::apply` on its
//! own when restarted. This module just verifies the durable state is consistent
//! before handing off to openraft.

use crate::ClusterError;
use crate::raft::log_storage::JournalLogStorage;

pub fn assert_consistent(storage: &JournalLogStorage) -> Result<(), ClusterError> {
    let last_seq = storage.journal.last_seq();
    let last_purged = storage.last_purged.load()?;

    if let (Some(seq), Some(purged)) = (last_seq, last_purged.as_ref()) {
        if seq < purged.index {
            return Err(ClusterError::Recovery(format!(
                "journal last_seq={} is below last_purged.index={} — data dir corrupt",
                seq, purged.index
            )));
        }
    }
    Ok(())
}
```

- [ ] **Step 2: Call from builder before `Raft::new`**

In `uc_node/src/runtime/builder.rs`, after opening `log_storage`:

```rust
crate::runtime::recovery::assert_consistent(&log_storage)?;
```

- [ ] **Step 3: Build and check no warnings**

Run: `cargo build -p uc_node && cargo clippy -p uc_node -- -D warnings`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add uc_node/src/runtime/recovery.rs uc_node/src/runtime/builder.rs
git commit -m "feat(uc_node): startup consistency check on durable state"
```

---

## Task 15: M1 integration test — single-node KV cluster

The capstone. Stand up a single-node cluster with a counter `StateMachine`, submit, query, restart, verify state preserved.

**Files:**
- Create: `tests/m1_single_node.rs`

- [ ] **Step 1: Add the workspace-root test**

Update workspace `Cargo.toml` to include `dev-dependencies` for the test:

```toml
[dev-dependencies]
uc_node = { path = "uc_node" }
uc_service = { path = "uc_service" }
ultima-journal = { workspace = true }
tokio = { workspace = true }
tempfile = { workspace = true }
anyhow = { workspace = true }
serde = { workspace = true }
bincode = { workspace = true }
tracing-subscriber = { workspace = true }
```

(If the workspace root doesn't compile a `[lib]` or `[bin]`, integration tests at the workspace root require an empty `[lib]` target. Either move the test inside `uc_node/tests/` or create a tiny driver crate. The simpler path: place the test in `uc_node/tests/m1_single_node.rs`.)

- [ ] **Step 2: Create `uc_node/tests/m1_single_node.rs`**

```rust
//! M1 capstone: bootstrap_single_node → submit → query → restart → state preserved.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use uc_node::{BootstrapConfig, NodeBuilder, NodeConfig, RaftTuning};
use uc_service::{SnapshotError, StateMachine};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum CounterCmd {
    Increment(u64),
    Reset,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct CounterResponse { value: u64 }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct CounterQuery;

#[derive(Default)]
struct Counter {
    value: u64,
    last_applied: Option<u64>,
}

impl StateMachine for Counter {
    type Command = CounterCmd;
    type Response = CounterResponse;
    type Query = CounterQuery;
    type QueryResponse = u64;

    fn apply(&mut self, log_index: u64, cmd: Self::Command) -> Self::Response {
        match cmd {
            CounterCmd::Increment(n) => self.value += n,
            CounterCmd::Reset => self.value = 0,
        }
        self.last_applied = Some(log_index);
        CounterResponse { value: self.value }
    }

    fn query(&self, _: Self::Query) -> Self::QueryResponse { self.value }

    fn last_applied(&self) -> Option<u64> { self.last_applied }

    fn build_snapshot(&self, dst: &mut dyn Write) -> Result<u64, SnapshotError> {
        let bytes = bincode::serde::encode_to_vec(&(self.value, self.last_applied),
            bincode::config::standard())
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

fn cfg(data_dir: PathBuf) -> NodeConfig {
    NodeConfig {
        node_id: 1,
        data_dir,
        raft_listen_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        app_id: "counter-test".into(),
        bootstrap: BootstrapConfig::SingleNode,
        raft: RaftTuning::default(),
    }
}

#[tokio::test]
async fn submit_query_works() {
    let dir = TempDir::new().unwrap();
    let node = NodeBuilder::new(cfg(dir.path().to_owned()), Counter::default())
        .start().await.expect("start");

    // Wait until we are leader.
    for _ in 0..50 {
        if node.current_leader().await == Some(1) { break; }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(node.current_leader().await, Some(1));

    let r1 = node.submit(CounterCmd::Increment(5)).await.expect("submit");
    assert_eq!(r1.value, 5);
    let r2 = node.submit(CounterCmd::Increment(3)).await.expect("submit");
    assert_eq!(r2.value, 8);

    let v = node.query_snapshot(|c: &Counter| c.value).await.expect("query");
    assert_eq!(v, 8);

    node.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn state_survives_restart() {
    let dir = TempDir::new().unwrap();
    let cfg1 = NodeConfig {
        bootstrap: BootstrapConfig::SingleNode,
        ..cfg(dir.path().to_owned())
    };

    {
        let node = NodeBuilder::new(cfg1.clone(), Counter::default())
            .start().await.expect("start");
        for _ in 0..50 {
            if node.current_leader().await == Some(1) { break; }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        node.submit(CounterCmd::Increment(42)).await.expect("submit");
        node.submit(CounterCmd::Increment(1)).await.expect("submit");
        node.shutdown().await.expect("shutdown");
    }

    // Restart with Resume — same data_dir.
    let cfg2 = NodeConfig { bootstrap: BootstrapConfig::Resume, ..cfg1 };
    let node = NodeBuilder::new(cfg2, Counter::default())
        .start().await.expect("restart");
    for _ in 0..50 {
        if node.current_leader().await == Some(1) { break; }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let v = node.query_snapshot(|c: &Counter| c.value).await.expect("query");
    assert_eq!(v, 43, "state must survive restart");
    node.shutdown().await.expect("shutdown");
}
```

- [ ] **Step 3: Run the integration tests**

Run: `cargo test -p uc_node --test m1_single_node`
Expected: 2 passed.

If the openraft API differs from the assumed shape, iterate on the trait implementations in Tasks 9–11. Failures here indicate a real bug, not a planning issue.

- [ ] **Step 4: Commit**

```bash
git add uc_node/tests/m1_single_node.rs
git commit -m "test(uc_node): M1 capstone — single-node submit/query/restart"
```

---

## Task 16: Polish — clippy, fmt, README pointer

**Files:**
- Modify: `README.md`
- All crates

- [ ] **Step 1: Run clippy across the workspace**

Run: `cargo clippy --workspace --all-targets --features uc_node/test-helpers -- -D warnings`
Expected: zero warnings. Fix any that surface inline.

- [ ] **Step 2: Run rustfmt**

Run: `cargo fmt --all`
Then: `git diff --stat` to see what changed.

- [ ] **Step 3: Update root `README.md`**

```markdown
# ultima_cluster

SMR cluster implementation on top of openraft.

**Status:** M1 — embedded single-node skeleton complete. See
`docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` for the
canonical design and `docs/superpowers/plans/` for milestone plans.

## Workspace

- `uc_protocol` — wire spec (no_std-friendly).
- `uc_service` — service-side SDK (`StateMachine`, `OutputHandler` traits).
- `uc_node` — cluster engine (Raft, log storage, network).
- `uc_client` — local-shmem client SDK (M1 stub).

## Build & test

```bash
cargo build
cargo test
cargo clippy --workspace --all-targets -- -D warnings
```

See `CLAUDE.md` for orientation.
```

- [ ] **Step 4: Final commit**

```bash
git add -A
git commit -m "chore(m1): clippy/fmt clean + README pointer"
```

---

## Verification checklist

After all tasks complete, verify:

- [ ] `cargo build --workspace` — clean, no warnings.
- [ ] `cargo test --workspace` — all tests pass (the two integration tests + uc_protocol unit tests).
- [ ] `cargo clippy --workspace --all-targets --features uc_node/test-helpers -- -D warnings` — zero warnings.
- [ ] `cargo doc --workspace --no-deps` — docs build.
- [ ] M1 capstone tests both pass: `submit_query_works` and `state_survives_restart`.

If all green, M1 is done. Move on to writing the M2 plan (multi-node + QUIC).

---

## Self-review notes

**Spec coverage:** This plan covers spec sections 1–6 (process model, workspace, cnc.dat — stubbed only, public API — traits only, storage adapters) for the embedded single-node case. Sections 4 (cnc layout in detail), 7 (QUIC), 8 (pipelines via shmem), 9 (snapshot region), 10–11 (multi-process bootstrap, errors observability), 12 (multi-process testing) defer to M2–M5.

**Type consistency:** `NodeId = u64`, `NodeAddr { raft_addr }`, `AppCommand = bytes::Bytes`, `AppResponse = bytes::Bytes` — used consistently across `raft/`, `runtime/`, and the integration test.

**openraft API caveat:** The exact method signatures in Tasks 9–11 depend on openraft 0.9's current API; the executor will likely need to adjust some constructor names and trait bounds. The mapping (which UC primitive backs which openraft method) is canonical and comes from spec §6.

**Known gaps to address in subsequent milestones:**
- `BootstrapConfig::Peers` returns Err in M1; M2 implements it.
- `query_linearizable` not present in M1 NodeHandle; M2 adds it via openraft `ensure_linearizable()`.
- OutputHandler not wired in M1; M5 adds the output dispatcher and progress marker.
- `uc_service::ultima_db::StoreStateMachine` adapter is M3 work.
