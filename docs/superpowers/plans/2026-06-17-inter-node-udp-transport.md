# Inter-node UDP Transport (Aeron-style reliable unicast) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a second inter-node transport — an Aeron-style reliable-unicast UDP channel built from scratch in-tree — behind a pluggable transport seam, so it can be A/B'd against the existing QUIC transport on the same benchmark harness while QUIC stays the default.

**Architecture:** A `ClusterTransport` trait abstracts the two halves openraft needs (an outbound `RaftNetworkFactory<TypeConfig>` + an inbound listener). A `Transport` config enum selects QUIC (unchanged) or UDP at runtime; `builder.rs` becomes transport-agnostic. The UDP channel runs over one shared `tokio::net::UdpSocket` per process, demultiplexed to per-peer sessions by `session_id`. Each session is a reliable, ordered, fragmenting message channel (flat `u64` per-segment sequence, range-NAK retransmit from an in-memory window, receiver-window flow control, MTU fragmentation), with the existing `frame.rs` `Frame`/`MessageType` + `codec.rs` reused unchanged as the RPC payload above it. openraft's retry handles the true-failure tail.

**Tech Stack:** Rust 2024, `tokio` (`UdpSocket`, tasks, `oneshot`/`mpsc`), `bytes`, `openraft` 0.10 + `openraft-legacy` network-v1 `Adapter`, `crc32fast`. No new third-party crates.

## Global Constraints

- **QUIC remains the default transport and its behavior must not change.** `Transport::default()` resolves to `Quic`; the full existing test suite must stay green after every phase.
- **`uc_protocol` is `no_std`-friendly** — none of the UDP work goes there; it lives in `uc_node`.
- **Reuse `frame.rs` `Frame`/`MessageType` and `codec.rs` verbatim** as the RPC payload. The UDP work is the reliable channel *underneath* the `Frame`, not a new RPC encoding.
- **Lean on openraft for the failure tail.** Transport methods return `RPCError` on session-fatal errors so openraft retries; we do not build never-breaking reliability.
- **All wire integers little-endian** for UDP segments (matches the Aeron reference; note `frame.rs` itself uses big-endian via `put_u*` — that is the *payload* and stays as-is; only the new segment header is little-endian).
- **`cargo clippy --workspace -- -D warnings` must pass** (zero warnings) and `cargo fmt` applied before every commit.
- **Fault-injection code is gated behind the existing `fault-injection` cargo feature** — zero production surface when off.
- **MTU 1408, flow window 128 KiB, SM interval 200 ms, heartbeat 100 ms, NAK linger 10 ms, session timeout 5 s** are the Aeron-derived defaults (`UdpTuning::default`); all configurable.

---

## File Structure

**New files (`uc_node/src/network/`):**
- `transport.rs` — `ClusterTransport` trait, `TransportCtx`, `TransportServer` enum (heterogeneous server-handle wrapper with async `shutdown`).
- `udp/mod.rs` — module wiring + re-exports + `UdpTuning`.
- `udp/wire.rs` — segment header + `SegType` + `Segment` encode/decode (pure, unit-tested).
- `udp/reassembly.rs` — `Reassembler` (in-order fragment reassembly + gap detection) (pure, unit-tested).
- `udp/send_window.rs` — `SendWindow` (retain unacked segments, byte accounting, ack/NAK lookups) (pure, unit-tested).
- `udp/session.rs` — `UdpSession`: async reliable send/recv over a shared socket sender for one peer.
- `udp/mux.rs` — shared-socket receive loop + session registry + RPC `request_id` correlation map.
- `udp/factory.rs` — `UdpRaftNetworkFactory` (impl `RaftNetworkFactory<TypeConfig>`).
- `udp/instance.rs` — `UdpRaftNetwork` (impl `RaftNetwork<TypeConfig>` V1).
- `udp/server.rs` — `spawn_udp_server` + `UdpServerHandle`.
- `uc_autobench/src/bin/internode-rpc-bench.rs` — inter-node transport microbench (target 3).

**Moved (Phase A): today's QUIC files into `uc_node/src/network/quic/`** — `client.rs`, `server.rs`, `instance.rs`, `factory.rs`, `tls.rs` (re-exported so external paths keep working).

**Modified:**
- `uc_node/src/network/mod.rs` — module declarations, transport re-exports.
- `uc_node/src/network/fault.rs` — add drop/delay/reorder (Phase D).
- `uc_node/src/config.rs` — add `Transport` enum + `transport` field on `NodeConfig`.
- `uc_node/src/runtime/builder.rs` — transport-agnostic dispatch.
- `uc_node/src/runtime/node.rs` — `server` field becomes `TransportServer`.
- `uc_node/src/lib.rs` — export `Transport`, `UdpTuning`.
- All `NodeConfig { … }` literals (tests + `uc_autobench` + `examples`) — add `transport: Transport::Quic`.
- `uc_autobench/src/bin/uc-node-launch.rs` + `uc_autobench/scripts/run-uc-3node.sh` — `--transport`/`UC_TRANSPORT` knob, arbitrary N (Phase E).
- `bench-infra/ansible/group_vars/all.yml` — `transport` knob (Phase E).

---

## Canonical shared types (defined in the tasks below; collected here for cross-reference)

```rust
// network/udp/wire.rs
pub const SEG_HEADER_LEN: usize = 28;
pub const FLAG_BEGIN: u8 = 0x01;
pub const FLAG_END: u8 = 0x02;
pub const WIRE_VERSION: u8 = 1;

#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SegType { Data = 1, Nak = 2, Sm = 3, Heartbeat = 4, Hello = 5, HelloAck = 6 }

pub struct Segment {
    pub version: u8,
    pub seg_type: SegType,
    pub flags: u8,        // Data: FLAG_BEGIN / FLAG_END
    pub session_id: u32,
    pub seq: u64,         // Data: this segment's seq; Nak: gap start seq; Sm: highest_contiguous
    pub arg: u64,         // Nak: gap count; Sm: window bytes; else 0
    pub payload: Bytes,   // Data/Hello only
}
// encode(&self) -> BytesMut ; decode(buf: &[u8]) -> Result<Segment, NetworkError>

// network/udp/mod.rs
#[derive(Debug, Clone)]
pub struct UdpTuning {
    pub mtu: usize,                 // 1408
    pub flow_window_bytes: u64,     // 128*1024
    pub heartbeat_ms: u64,          // 100
    pub sm_interval_ms: u64,        // 200
    pub nak_linger_ms: u64,         // 10
    pub session_timeout_ms: u64,    // 5000
}

// network/transport.rs
pub struct TransportCtx {
    pub node_id: NodeId,
    pub listen_addr: SocketAddr,
    pub app_id: String,
    pub data_dir: PathBuf,
    #[cfg(feature = "fault-injection")]
    pub fault_table: Option<Arc<crate::network::fault::FaultTable>>,
}
pub enum TransportServer { Quic(quic::ServerHandle), Udp(udp::UdpServerHandle) }
impl TransportServer { pub async fn shutdown(self) { /* match arms */ } }

// config.rs
#[derive(Debug, Clone, Default)]
pub enum Transport { #[default] Quic, Udp(crate::network::UdpTuning) }
```

---

# PHASE A — Transport seam (QUIC stays default; everything green)

### Task 1: Move QUIC files under `quic/`, re-export

**Files:**
- Create dir: `uc_node/src/network/quic/` with `mod.rs`
- Move: `network/{client,server,instance,factory,tls}.rs` → `network/quic/`
- Modify: `uc_node/src/network/mod.rs`

**Interfaces:**
- Produces: `crate::network::quic::{QuicRaftNetworkFactory, QuicRaftNetwork, ServerHandle, spawn_server}` and unchanged top-level re-exports `crate::network::{QuicRaftNetworkFactory, QuicRaftNetwork}`.

- [ ] **Step 1: Move the files**

```bash
cd uc_node/src/network
mkdir quic
git mv client.rs server.rs instance.rs factory.rs tls.rs quic/
```

- [ ] **Step 2: Create `quic/mod.rs`**

```rust
//! QUIC inter-node transport (the default). One persistent connection per
//! peer-pair, a fresh bidirectional stream per RPC, TLS self-signed by default.

pub mod client;
pub mod codec_unused {} // placeholder removed below
pub mod factory;
pub mod instance;
pub mod server;
pub mod tls;

pub use factory::QuicRaftNetworkFactory;
pub use instance::QuicRaftNetwork;
pub use server::{ServerHandle, spawn_server};
```

Then delete the `codec_unused` line (it was only to show the block must not reference the moved-out `codec`/`frame`/`fault`, which stay at `network/` level — the moved files use `super::super::{codec, frame, NetworkError}` now).

- [ ] **Step 3: Fix intra-QUIC `super::` paths**

In each moved file, references that were `super::frame`, `super::codec`, `super::NetworkError`, `super::fault` must become `super::super::frame`, etc. (they now live one level up). References within the QUIC set (`super::client::PeerConn`, `super::factory::PeerPool`) stay `super::`. Update `network/mod.rs`:

```rust
pub mod codec;
pub mod frame;
#[cfg(feature = "fault-injection")]
pub mod fault;
pub mod quic;

pub use quic::{QuicRaftNetworkFactory, QuicRaftNetwork};
// NetworkError enum stays here unchanged.
```

- [ ] **Step 4: Build**

Run: `cargo build -p uc_node`
Expected: compiles. Fix any remaining path errors until clean.

- [ ] **Step 5: Run the full suite to prove no behavior change**

Run: `cargo test -p uc_node`
Expected: same pass set as before the move.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "refactor(network): move QUIC transport under quic/ submodule (no behavior change)"
```

---

### Task 2: `UdpTuning` + `Transport` config enum + `transport` field

**Files:**
- Create: `uc_node/src/network/udp/mod.rs`
- Modify: `uc_node/src/network/mod.rs`, `uc_node/src/config.rs`, `uc_node/src/lib.rs`

**Interfaces:**
- Produces: `crate::network::UdpTuning` (with `Default`), `crate::config::Transport` (with `Default = Quic`), `NodeConfig.transport: Transport`.

- [ ] **Step 1: Write the failing test (tuning + enum defaults)**

Add to `uc_node/src/config.rs` (bottom, in a `#[cfg(test)] mod tests`):

```rust
#[cfg(test)]
mod transport_tests {
    use super::*;
    #[test]
    fn transport_defaults_to_quic() {
        assert!(matches!(Transport::default(), Transport::Quic));
    }
    #[test]
    fn udp_tuning_defaults() {
        let t = crate::network::UdpTuning::default();
        assert_eq!(t.mtu, 1408);
        assert_eq!(t.flow_window_bytes, 128 * 1024);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_node --lib transport_tests`
Expected: FAIL — `Transport` and `UdpTuning` not found.

- [ ] **Step 3: Create `network/udp/mod.rs`**

```rust
//! Aeron-style reliable-unicast UDP inter-node transport.
//!
//! One shared `tokio::net::UdpSocket` per process, demultiplexed to per-peer
//! sessions by `session_id`. Each session is a reliable, ordered, fragmenting
//! message channel: flat u64 per-segment seq, range-NAK retransmit from an
//! in-memory window, receiver-window flow control, MTU fragmentation. The
//! existing `frame.rs` Frame/MessageType is the RPC payload above this layer.

#[derive(Debug, Clone)]
pub struct UdpTuning {
    /// Max UDP datagram size (header + payload). Aeron default 1408.
    pub mtu: usize,
    /// Receiver flow-control window in bytes. Aeron LAN default 128 KiB.
    pub flow_window_bytes: u64,
    /// Idle sender heartbeat cadence (ms).
    pub heartbeat_ms: u64,
    /// Periodic Status Message cadence (ms).
    pub sm_interval_ms: u64,
    /// Suppress duplicate retransmits for the same range for this long (ms).
    pub nak_linger_ms: u64,
    /// Tear down a session after this long with no inbound traffic (ms).
    pub session_timeout_ms: u64,
}

impl Default for UdpTuning {
    fn default() -> Self {
        Self {
            mtu: 1408,
            flow_window_bytes: 128 * 1024,
            heartbeat_ms: 100,
            sm_interval_ms: 200,
            nak_linger_ms: 10,
            session_timeout_ms: 5000,
        }
    }
}
```

- [ ] **Step 4: Wire module + config**

`network/mod.rs`: add `pub mod udp;` and `pub use udp::UdpTuning;`.

`config.rs`: add the enum and field:

```rust
/// Inter-node transport selector. `Quic` (default) is the existing QUIC path;
/// `Udp` is the Aeron-style reliable-unicast UDP transport.
#[derive(Debug, Clone, Default)]
pub enum Transport {
    #[default]
    Quic,
    Udp(crate::network::UdpTuning),
}
```

Add to `struct NodeConfig` (after `tls`):

```rust
    /// Inter-node transport. Defaults to `Quic` (unchanged behavior).
    pub transport: Transport,
```

`lib.rs`: extend the `pub use config::{…}` to include `Transport`, and add `pub use network::UdpTuning;`.

- [ ] **Step 5: Make every `NodeConfig { … }` literal compile**

The struct has no `Default`, so each literal must set the new field. Find them:

```bash
grep -rln 'NodeConfig {' uc_node uc_autobench examples
```

In each, add `transport: uc_node::Transport::Quic,` (or `Transport::Quic` where imported; in `test_support.rs` it is `crate::config::Transport::Quic`). Mirror the existing `tls: TlsConfig::default(),` line placement.

- [ ] **Step 6: Run**

Run: `cargo test -p uc_node --lib transport_tests` then `cargo build --workspace`
Expected: PASS, workspace builds.

- [ ] **Step 7: Commit**

```bash
cargo fmt && git add -A && git commit -m "feat(network): add Transport config enum + UdpTuning (Quic default)"
```

---

### Task 3: `ClusterTransport` trait, `TransportCtx`, `TransportServer`; transport-agnostic builder

**Files:**
- Create: `uc_node/src/network/transport.rs`
- Modify: `uc_node/src/network/mod.rs`, `uc_node/src/runtime/builder.rs`, `uc_node/src/runtime/node.rs`

**Interfaces:**
- Consumes: `quic::{QuicRaftNetworkFactory, ServerHandle, spawn_server}`.
- Produces: `crate::network::{ClusterTransport, TransportCtx, TransportServer}`. `TransportServer::shutdown(self)` is async. `NodeHandle.server: TransportServer`.

- [ ] **Step 1: Create `network/transport.rs`**

```rust
//! Pluggable inter-node transport seam.
//!
//! A transport bundles the two halves openraft needs: an outbound
//! `RaftNetworkFactory<TypeConfig>` and an inbound listener that dispatches
//! into the local `Raft`. Adding a new transport (QUIC, UDP, future
//! kernel-bypass / UCX) means implementing `ClusterTransport` and adding a
//! `Transport` enum variant — no consensus/builder rewiring.
//!
//! Note: `ClusterTransport` makes no `tokio::net` assumption — a future
//! kernel-bypass stack that owns its own poll loop still fits, since the trait
//! only promises "give me a factory + a server".

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::Raft;
use openraft::network::RaftNetworkFactory;
use openraft::storage::RaftStateMachine;

use super::NetworkError;
use super::quic::ServerHandle as QuicServerHandle;
use super::udp::UdpServerHandle;
use crate::raft::{NodeId, TypeConfig};

/// Shared inputs every transport needs to build its factory + server.
pub struct TransportCtx {
    pub node_id: NodeId,
    pub listen_addr: SocketAddr,
    pub app_id: String,
    pub data_dir: PathBuf,
    #[cfg(feature = "fault-injection")]
    pub fault_table: Option<Arc<super::fault::FaultTable>>,
}

/// A pluggable inter-node transport.
pub trait ClusterTransport {
    type Factory: RaftNetworkFactory<TypeConfig>;
    type Server;

    fn build_factory(&self, ctx: &TransportCtx) -> Result<Self::Factory, NetworkError>;

    fn spawn_server<SM>(
        &self,
        ctx: &TransportCtx,
        raft: Raft<TypeConfig, SM>,
    ) -> Result<Self::Server, NetworkError>
    where
        SM: RaftStateMachine<TypeConfig>;
}

/// Runtime-selected server handle. One variant per built-in transport.
pub enum TransportServer {
    Quic(QuicServerHandle),
    Udp(UdpServerHandle),
}

impl TransportServer {
    pub async fn shutdown(self) {
        match self {
            TransportServer::Quic(h) => h.shutdown().await,
            TransportServer::Udp(h) => h.shutdown().await,
        }
    }

    /// Bound peer listen address (for tests/diagnostics).
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        match self {
            TransportServer::Quic(h) => h.local_addr(),
            TransportServer::Udp(h) => h.local_addr(),
        }
    }
}
```

> This task depends on `udp::UdpServerHandle` existing. Add a temporary stub now so Phase A compiles; Task 12 replaces it:
>
> In `network/udp/mod.rs` add:
> ```rust
> pub mod server_stub;
> pub use server_stub::UdpServerHandle;
> ```
> Create `network/udp/server_stub.rs`:
> ```rust
> //! Temporary stub; replaced by udp/server.rs in Phase C (Task 12).
> use std::net::SocketAddr;
> pub struct UdpServerHandle;
> impl UdpServerHandle {
>     pub async fn shutdown(self) {}
>     pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
>         Ok("0.0.0.0:0".parse().unwrap())
>     }
> }
> ```

- [ ] **Step 2: Implement `ClusterTransport` for QUIC**

Create `network/quic/transport_impl.rs` (declare `pub mod transport_impl;` in `quic/mod.rs`):

```rust
use std::sync::Arc;

use openraft::Raft;
use openraft::storage::RaftStateMachine;

use super::super::transport::{ClusterTransport, TransportCtx};
use super::super::NetworkError;
use super::{QuicRaftNetworkFactory, ServerHandle, spawn_server, tls};
use crate::raft::TypeConfig;

/// Marker selecting the QUIC transport.
pub struct QuicTransport;

impl ClusterTransport for QuicTransport {
    type Factory = QuicRaftNetworkFactory;
    type Server = ServerHandle;

    fn build_factory(&self, ctx: &TransportCtx) -> Result<Self::Factory, NetworkError> {
        let client_tls_cfg = tls::build_client_config()?;
        let endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap())
            .map_err(|e| NetworkError::Connect(format!("client endpoint: {e}")))?;
        #[allow(unused_mut)]
        let mut f = QuicRaftNetworkFactory::new(endpoint, client_tls_cfg, ctx.app_id.clone());
        #[cfg(feature = "fault-injection")]
        f.set_fault_injection(ctx.node_id, ctx.fault_table.clone());
        Ok(f)
    }

    fn spawn_server<SM>(
        &self,
        ctx: &TransportCtx,
        raft: Raft<TypeConfig, SM>,
    ) -> Result<Self::Server, NetworkError>
    where
        SM: RaftStateMachine<TypeConfig>,
    {
        // Confirmed: tls::load_or_init / build_server_config / build_client_config
        // all already return Result<_, NetworkError>, so no error mapping needed.
        let (cert_der, key_der) = tls::load_or_init(&ctx.data_dir, &ctx.app_id)?;
        let server_tls_cfg = tls::build_server_config(cert_der, key_der)?;
        spawn_server(ctx.listen_addr, server_tls_cfg, raft)
    }
}
```

`quic/mod.rs`: add `pub use transport_impl::QuicTransport;`.
`network/mod.rs`: add `pub mod transport;` and `pub use transport::{ClusterTransport, TransportCtx, TransportServer};`.

- [ ] **Step 3: Make `NodeHandle.server` a `TransportServer`**

`runtime/node.rs`: change the import `use crate::network::server::ServerHandle;` → `use crate::network::TransportServer;` and the field `pub(crate) server: ServerHandle,` → `pub(crate) server: TransportServer,`. The shutdown call `server.shutdown().await;` already matches the new async signature — no change there.

- [ ] **Step 4: Rewire `builder.rs finish()` to dispatch on transport**

Replace the inline QUIC block (the `// TLS infrastructure…` through `let server = spawn_server(…)?;` region, ~lines 376–404) with:

```rust
    use crate::network::transport::{ClusterTransport, TransportCtx, TransportServer};

    let ctx = TransportCtx {
        node_id: config.node_id,
        listen_addr: config.raft_listen_addr,
        app_id: config.app_id.clone(),
        data_dir: config.data_dir.clone(),
        #[cfg(feature = "fault-injection")]
        fault_table,
    };

    // Build the factory + raft + server for the selected transport. Both arms
    // produce the same `Raft<TypeConfig, A>` (network type is erased into
    // RaftCore) so bootstrap below is transport-agnostic.
    let (raft, server) = match &config.transport {
        crate::config::Transport::Quic => {
            let t = crate::network::quic::QuicTransport;
            let network = t.build_factory(&ctx)
                .map_err(|e| ClusterError::Config(format!("transport factory: {e}")))?;
            let raft = Raft::new(config.node_id, raft_config, network, log_storage, sm_adapter)
                .await
                .map_err(|e| ClusterError::Raft(format!("Raft::new: {e}")))?;
            let server = t.spawn_server(&ctx, raft.clone())
                .map_err(|e| ClusterError::Config(format!("transport server: {e}")))?;
            (raft, TransportServer::Quic(server))
        }
        crate::config::Transport::Udp(tuning) => {
            let t = crate::network::udp::UdpTransport::new(tuning.clone());
            let network = t.build_factory(&ctx)
                .map_err(|e| ClusterError::Config(format!("transport factory: {e}")))?;
            let raft = Raft::new(config.node_id, raft_config, network, log_storage, sm_adapter)
                .await
                .map_err(|e| ClusterError::Raft(format!("Raft::new: {e}")))?;
            let server = t.spawn_server(&ctx, raft.clone())
                .map_err(|e| ClusterError::Config(format!("transport server: {e}")))?;
            (raft, TransportServer::Udp(server))
        }
    };
```

> The `Udp` arm references `udp::UdpTransport` which doesn't exist until Phase C. To keep Phase A compiling, **temporarily** make the `Udp` arm `unreachable!("UDP transport lands in Phase C")` and delete the body above for now; restore it in Task 13. (A fresh reviewer reading out of order: the real body is exactly the QUIC arm with `UdpTransport::new(tuning.clone())`.)

- [ ] **Step 5: Build + full suite**

Run: `cargo build --workspace && cargo test -p uc_node`
Expected: builds; QUIC tests green (default path unchanged).

- [ ] **Step 6: clippy + commit**

Run: `cargo clippy --workspace -- -D warnings`

```bash
cargo fmt && git add -A && git commit -m "feat(network): ClusterTransport trait + TransportCtx/TransportServer; builder dispatches on transport"
```

---

# PHASE B — UDP channel core (pure, unit-tested units)

### Task 4: Segment wire codec (`udp/wire.rs`)

**Files:**
- Create: `uc_node/src/network/udp/wire.rs`
- Test: inline `#[cfg(test)] mod tests`

**Interfaces:**
- Produces: `SegType`, `Segment`, `SEG_HEADER_LEN=28`, `FLAG_BEGIN=0x01`, `FLAG_END=0x02`, `WIRE_VERSION=1`, `Segment::encode(&self) -> BytesMut`, `Segment::decode(buf: &[u8]) -> Result<Segment, NetworkError>`.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn data_round_trip() {
        let s = Segment {
            version: WIRE_VERSION,
            seg_type: SegType::Data,
            flags: FLAG_BEGIN | FLAG_END,
            session_id: 0xABCD,
            seq: 42,
            arg: 0,
            payload: Bytes::from_static(b"hello"),
        };
        let buf = s.encode();
        let d = Segment::decode(&buf).unwrap();
        assert_eq!(d.seg_type, SegType::Data);
        assert_eq!(d.session_id, 0xABCD);
        assert_eq!(d.seq, 42);
        assert_eq!(d.flags, FLAG_BEGIN | FLAG_END);
        assert_eq!(&d.payload[..], b"hello");
    }

    #[test]
    fn nak_round_trip() {
        let s = Segment {
            version: WIRE_VERSION, seg_type: SegType::Nak, flags: 0,
            session_id: 7, seq: 100, arg: 5, payload: Bytes::new(),
        };
        let d = Segment::decode(&s.encode()).unwrap();
        assert_eq!(d.seg_type, SegType::Nak);
        assert_eq!(d.seq, 100); // gap start
        assert_eq!(d.arg, 5);   // gap count
    }

    #[test]
    fn rejects_short_buffer() {
        assert!(Segment::decode(&[0u8; 4]).is_err());
    }

    #[test]
    fn rejects_bad_crc() {
        let s = Segment {
            version: WIRE_VERSION, seg_type: SegType::Data, flags: FLAG_END,
            session_id: 1, seq: 1, arg: 0, payload: Bytes::from_static(b"xyz"),
        };
        let mut buf = s.encode();
        let last = buf.len() - 1;
        buf[last] ^= 0xFF; // corrupt CRC
        assert!(Segment::decode(&buf).is_err());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_node --lib network::udp::wire`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement `wire.rs`**

```rust
//! UDP segment wire format. Little-endian header + optional payload + CRC32.
//!
//! Layout (28-byte header):
//! ```text
//!   version     u8
//!   seg_type    u8
//!   flags       u8
//!   _pad        u8
//!   session_id  u32  (le)
//!   seq         u64  (le)
//!   arg         u64  (le)
//!   payload_len u32  (le)
//!   payload     [u8; payload_len]
//!   crc32       u32  (le, over header[..24] + payload)
//! ```
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::network::NetworkError;

pub const WIRE_VERSION: u8 = 1;
pub const SEG_HEADER_LEN: usize = 28; // through payload_len
pub const FLAG_BEGIN: u8 = 0x01;
pub const FLAG_END: u8 = 0x02;

#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SegType {
    Data = 1,
    Nak = 2,
    Sm = 3,
    Heartbeat = 4,
    Hello = 5,
    HelloAck = 6,
}

impl SegType {
    fn from_u8(v: u8) -> Result<Self, NetworkError> {
        Ok(match v {
            1 => Self::Data,
            2 => Self::Nak,
            3 => Self::Sm,
            4 => Self::Heartbeat,
            5 => Self::Hello,
            6 => Self::HelloAck,
            other => return Err(NetworkError::Decode(format!("bad seg_type {other}"))),
        })
    }
}

pub struct Segment {
    pub version: u8,
    pub seg_type: SegType,
    pub flags: u8,
    pub session_id: u32,
    pub seq: u64,
    pub arg: u64,
    pub payload: Bytes,
}

impl Segment {
    pub fn encode(&self) -> BytesMut {
        let mut b = BytesMut::with_capacity(SEG_HEADER_LEN + self.payload.len() + 4);
        b.put_u8(self.version);
        b.put_u8(self.seg_type as u8);
        b.put_u8(self.flags);
        b.put_u8(0); // _pad
        b.put_u32_le(self.session_id);
        b.put_u64_le(self.seq);
        b.put_u64_le(self.arg);
        b.put_u32_le(self.payload.len() as u32);
        b.put_slice(&self.payload);
        let crc = crc32fast::hash(&b[..]); // covers header(24) + payload_len(4) + payload
        b.put_u32_le(crc);
        b
    }

    pub fn decode(buf: &[u8]) -> Result<Segment, NetworkError> {
        if buf.len() < SEG_HEADER_LEN + 4 {
            return Err(NetworkError::Decode(format!(
                "segment too short: {} bytes", buf.len()
            )));
        }
        let (body, crc_bytes) = buf.split_at(buf.len() - 4);
        let crc_expected = crc32fast::hash(body);
        let crc_actual = u32::from_le_bytes(crc_bytes.try_into().unwrap());
        if crc_actual != crc_expected {
            return Err(NetworkError::Decode("segment crc mismatch".into()));
        }
        let mut h = &body[..SEG_HEADER_LEN];
        let version = h.get_u8();
        let seg_type = SegType::from_u8(h.get_u8())?;
        let flags = h.get_u8();
        let _pad = h.get_u8();
        let session_id = h.get_u32_le();
        let seq = h.get_u64_le();
        let arg = h.get_u64_le();
        let payload_len = h.get_u32_le() as usize;
        let payload_region = &body[SEG_HEADER_LEN..];
        if payload_region.len() != payload_len {
            return Err(NetworkError::Decode(format!(
                "payload_len {payload_len} != actual {}", payload_region.len()
            )));
        }
        Ok(Segment {
            version, seg_type, flags, session_id, seq, arg,
            payload: Bytes::copy_from_slice(payload_region),
        })
    }
}
```

`udp/mod.rs`: add `pub mod wire;`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p uc_node --lib network::udp::wire`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt && git add -A && git commit -m "feat(udp): segment wire codec (le header + crc32)"
```

---

### Task 5: Fragmenter — split a `Frame` payload into MTU-sized DATA segments

**Files:**
- Create: `uc_node/src/network/udp/fragment.rs`
- Test: inline

**Interfaces:**
- Produces: `fn fragment(payload: &Bytes, first_seq: u64, mtu: usize) -> Vec<(u64, u8, Bytes)>` returning `(seq, flags, chunk)` tuples, BEGIN on first, END on last, `BEGIN|END` if single. Max chunk = `mtu - SEG_HEADER_LEN - 4`.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::network::udp::wire::{FLAG_BEGIN, FLAG_END};

    #[test]
    fn single_fragment_when_small() {
        let p = Bytes::from_static(b"short");
        let frags = fragment(&p, 10, 1408);
        assert_eq!(frags.len(), 1);
        assert_eq!(frags[0].0, 10);
        assert_eq!(frags[0].1, FLAG_BEGIN | FLAG_END);
        assert_eq!(&frags[0].2[..], b"short");
    }

    #[test]
    fn splits_large_payload_with_begin_end() {
        let p = Bytes::from(vec![0u8; 5000]);
        let frags = fragment(&p, 0, 1408);
        assert!(frags.len() >= 4);
        assert_eq!(frags[0].1 & FLAG_BEGIN, FLAG_BEGIN);
        assert_eq!(frags[0].1 & FLAG_END, 0);
        let last = frags.last().unwrap();
        assert_eq!(last.1 & FLAG_END, FLAG_END);
        // seqs are contiguous from first_seq
        for (i, f) in frags.iter().enumerate() {
            assert_eq!(f.0, i as u64);
        }
        // reassembled bytes equal the input
        let total: usize = frags.iter().map(|f| f.2.len()).sum();
        assert_eq!(total, 5000);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_node --lib network::udp::fragment`
Expected: FAIL — `fragment` not found.

- [ ] **Step 3: Implement `fragment.rs`**

```rust
//! Split a logical message (an encoded RPC `Frame`) into DATA-segment-sized
//! chunks, tagging BEGIN/END for reassembly.
use bytes::Bytes;

use super::wire::{FLAG_BEGIN, FLAG_END, SEG_HEADER_LEN};

/// Returns `(seq, flags, chunk)` for each fragment. `first_seq` is the seq of
/// the BEGIN fragment; subsequent fragments increment by 1.
pub fn fragment(payload: &Bytes, first_seq: u64, mtu: usize) -> Vec<(u64, u8, Bytes)> {
    let max_chunk = mtu.saturating_sub(SEG_HEADER_LEN + 4).max(1);
    if payload.len() <= max_chunk {
        return vec![(first_seq, FLAG_BEGIN | FLAG_END, payload.clone())];
    }
    let mut out = Vec::new();
    let mut off = 0usize;
    let mut seq = first_seq;
    while off < payload.len() {
        let end = (off + max_chunk).min(payload.len());
        let mut flags = 0u8;
        if off == 0 {
            flags |= FLAG_BEGIN;
        }
        if end == payload.len() {
            flags |= FLAG_END;
        }
        out.push((seq, flags, payload.slice(off..end)));
        off = end;
        seq += 1;
    }
    out
}
```

`udp/mod.rs`: add `pub mod fragment;`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p uc_node --lib network::udp::fragment`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && git add -A && git commit -m "feat(udp): MTU fragmenter (BEGIN/END tagging)"
```

---

### Task 6: Reassembler — in-order fragment reassembly + gap detection

**Files:**
- Create: `uc_node/src/network/udp/reassembly.rs`
- Test: inline

**Interfaces:**
- Produces: `struct Reassembler` with:
  - `fn new() -> Self`
  - `fn accept(&mut self, seq: u64, flags: u8, payload: Bytes) -> Vec<Bytes>` — buffers out-of-order, returns any newly-completed messages (in order) once contiguous BEGIN..END runs are filled.
  - `fn highest_contiguous(&self) -> u64` — highest seq received with no gap below it (the SM ack value; starts at `u64::MAX` meaning "nothing yet" → represented as `Option`; see code).
  - `fn gaps(&self) -> Vec<(u64, u64)>` — `(start_seq, count)` ranges missing below the highest *seen* seq (for NAK).

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::network::udp::wire::{FLAG_BEGIN, FLAG_END};

    fn b(s: &str) -> Bytes { Bytes::copy_from_slice(s.as_bytes()) }

    #[test]
    fn single_segment_message_delivered() {
        let mut r = Reassembler::new();
        let out = r.accept(0, FLAG_BEGIN | FLAG_END, b("hi"));
        assert_eq!(out.len(), 1);
        assert_eq!(&out[0][..], b"hi");
        assert_eq!(r.highest_contiguous(), Some(0));
        assert!(r.gaps().is_empty());
    }

    #[test]
    fn multi_fragment_message_reassembled_in_order() {
        let mut r = Reassembler::new();
        assert!(r.accept(0, FLAG_BEGIN, b("foo")).is_empty());
        let out = r.accept(1, FLAG_END, b("bar"));
        assert_eq!(out.len(), 1);
        assert_eq!(&out[0][..], b"foobar");
    }

    #[test]
    fn out_of_order_buffers_then_delivers() {
        let mut r = Reassembler::new();
        // seq 1 arrives before seq 0 → gap at 0
        assert!(r.accept(1, FLAG_END, b("bar")).is_empty());
        assert_eq!(r.gaps(), vec![(0, 1)]);
        let out = r.accept(0, FLAG_BEGIN, b("foo"));
        assert_eq!(out.len(), 1);
        assert_eq!(&out[0][..], b"foobar");
        assert!(r.gaps().is_empty());
        assert_eq!(r.highest_contiguous(), Some(1));
    }

    #[test]
    fn duplicate_segments_ignored() {
        let mut r = Reassembler::new();
        let _ = r.accept(0, FLAG_BEGIN | FLAG_END, b("a"));
        let out = r.accept(0, FLAG_BEGIN | FLAG_END, b("a"));
        assert!(out.is_empty()); // already delivered, no double-deliver
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_node --lib network::udp::reassembly`
Expected: FAIL.

- [ ] **Step 3: Implement `reassembly.rs`**

```rust
//! Receiver-side: order DATA fragments by seq, reassemble BEGIN..END runs into
//! complete messages, and expose gaps for NAK + the highest-contiguous seq for
//! flow-control SMs.
use std::collections::BTreeMap;

use bytes::{Bytes, BytesMut};

use super::wire::{FLAG_BEGIN, FLAG_END};

pub struct Reassembler {
    /// Buffered segments not yet consumed, keyed by seq.
    pending: BTreeMap<u64, (u8, Bytes)>,
    /// Next seq we expect to consume contiguously.
    next: u64,
    /// Highest seq ever observed (for gap computation).
    highest_seen: Option<u64>,
    /// Whether at least one segment has been delivered/consumed.
    consumed_any: bool,
}

impl Reassembler {
    pub fn new() -> Self {
        Self { pending: BTreeMap::new(), next: 0, highest_seen: None, consumed_any: false }
    }

    pub fn accept(&mut self, seq: u64, flags: u8, payload: Bytes) -> Vec<Bytes> {
        if seq < self.next {
            return Vec::new(); // already consumed; duplicate/retransmit
        }
        self.highest_seen = Some(self.highest_seen.map_or(seq, |h| h.max(seq)));
        self.pending.entry(seq).or_insert((flags, payload));
        self.drain()
    }

    /// Consume contiguous segments from `next`, emitting complete messages.
    fn drain(&mut self) -> Vec<Bytes> {
        let mut out = Vec::new();
        loop {
            // Is the next expected seq present?
            if !self.pending.contains_key(&self.next) {
                break;
            }
            // Try to assemble one message starting at `next`: it must begin with
            // a BEGIN segment and run contiguously to an END segment.
            let (flags0, _) = self.pending.get(&self.next).cloned().unwrap();
            if flags0 & FLAG_BEGIN == 0 {
                // Shouldn't happen for a well-formed stream; drop to avoid stall.
                self.pending.remove(&self.next);
                self.next += 1;
                self.consumed_any = true;
                continue;
            }
            // Scan forward for END, requiring contiguity.
            let mut end_seq = None;
            let mut s = self.next;
            while let Some((f, _)) = self.pending.get(&s) {
                if *f & FLAG_END != 0 {
                    end_seq = Some(s);
                    break;
                }
                s += 1;
            }
            let Some(end_seq) = end_seq else { break }; // incomplete; wait for more
            // Assemble [next ..= end_seq].
            let mut msg = BytesMut::new();
            for k in self.next..=end_seq {
                let (_, p) = self.pending.remove(&k).unwrap();
                msg.extend_from_slice(&p);
            }
            out.push(msg.freeze());
            self.next = end_seq + 1;
            self.consumed_any = true;
        }
        out
    }

    /// Highest seq received with no gap below it. `None` until first consume.
    pub fn highest_contiguous(&self) -> Option<u64> {
        if !self.consumed_any { return None; }
        Some(self.next - 1)
    }

    /// Missing `(start, count)` ranges below `highest_seen`.
    pub fn gaps(&self) -> Vec<(u64, u64)> {
        let Some(hi) = self.highest_seen else { return Vec::new() };
        let mut gaps = Vec::new();
        let mut expect = self.next;
        while expect <= hi {
            if self.pending.contains_key(&expect) {
                expect += 1;
                continue;
            }
            let start = expect;
            while expect <= hi && !self.pending.contains_key(&expect) {
                expect += 1;
            }
            gaps.push((start, expect - start));
        }
        gaps
    }
}

impl Default for Reassembler {
    fn default() -> Self { Self::new() }
}
```

`udp/mod.rs`: add `pub mod reassembly;`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p uc_node --lib network::udp::reassembly`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt && git add -A && git commit -m "feat(udp): fragment reassembler + gap/highest-contiguous tracking"
```

---

### Task 7: Send window — retain unacked segments, byte accounting, ack/NAK lookups

**Files:**
- Create: `uc_node/src/network/udp/send_window.rs`
- Test: inline

**Interfaces:**
- Produces: `struct SendWindow` with:
  - `fn new(capacity_bytes: u64) -> Self`
  - `fn in_flight_bytes(&self) -> u64`
  - `fn can_admit(&self, bytes: usize) -> bool` — true if adding `bytes` keeps `in_flight <= capacity`.
  - `fn push(&mut self, seq: u64, encoded: Bytes)` — retain a sent segment (full encoded datagram).
  - `fn on_ack(&mut self, highest_contiguous: u64)` — drop all seq `<= highest_contiguous`.
  - `fn resend(&self, start: u64, count: u64) -> Vec<Bytes>` — encoded datagrams for `[start, start+count)` still retained.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn d(n: usize) -> Bytes { Bytes::from(vec![0u8; n]) }

    #[test]
    fn admits_until_capacity() {
        let mut w = SendWindow::new(100);
        assert!(w.can_admit(60));
        w.push(0, d(60));
        assert_eq!(w.in_flight_bytes(), 60);
        assert!(w.can_admit(40));
        assert!(!w.can_admit(41));
    }

    #[test]
    fn ack_frees_capacity() {
        let mut w = SendWindow::new(100);
        w.push(0, d(40));
        w.push(1, d(40));
        assert_eq!(w.in_flight_bytes(), 80);
        w.on_ack(0); // drops seq 0
        assert_eq!(w.in_flight_bytes(), 40);
        assert!(w.can_admit(60));
    }

    #[test]
    fn resend_returns_retained_range() {
        let mut w = SendWindow::new(1000);
        w.push(5, Bytes::from_static(b"five"));
        w.push(6, Bytes::from_static(b"six"));
        w.push(7, Bytes::from_static(b"seven"));
        let r = w.resend(6, 2);
        assert_eq!(r.len(), 2);
        assert_eq!(&r[0][..], b"six");
        assert_eq!(&r[1][..], b"seven");
    }

    #[test]
    fn resend_skips_already_acked() {
        let mut w = SendWindow::new(1000);
        w.push(0, d(10));
        w.push(1, d(10));
        w.on_ack(0);
        let r = w.resend(0, 2); // 0 is gone, 1 remains
        assert_eq!(r.len(), 1);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_node --lib network::udp::send_window`
Expected: FAIL.

- [ ] **Step 3: Implement `send_window.rs`**

```rust
//! Sender-side retain buffer: keeps encoded DATA datagrams until the receiver
//! acks them, enforces the flow-control byte budget, and answers retransmit
//! (NAK) lookups. This is both the retransmit store and the flow-control
//! accounting — they share one bound (Aeron's receiver window).
use std::collections::BTreeMap;

use bytes::Bytes;

pub struct SendWindow {
    capacity_bytes: u64,
    in_flight: u64,
    /// seq -> full encoded datagram (header+payload+crc), ready to re-send.
    retained: BTreeMap<u64, Bytes>,
}

impl SendWindow {
    pub fn new(capacity_bytes: u64) -> Self {
        Self { capacity_bytes, in_flight: 0, retained: BTreeMap::new() }
    }

    pub fn in_flight_bytes(&self) -> u64 { self.in_flight }

    pub fn can_admit(&self, bytes: usize) -> bool {
        self.in_flight + bytes as u64 <= self.capacity_bytes
    }

    pub fn push(&mut self, seq: u64, encoded: Bytes) {
        self.in_flight += encoded.len() as u64;
        self.retained.insert(seq, encoded);
    }

    pub fn on_ack(&mut self, highest_contiguous: u64) {
        // Drop all seq <= highest_contiguous.
        let keep = self.retained.split_off(&(highest_contiguous + 1));
        for (_, v) in std::mem::replace(&mut self.retained, keep) {
            self.in_flight -= v.len() as u64;
        }
    }

    pub fn resend(&self, start: u64, count: u64) -> Vec<Bytes> {
        (start..start + count)
            .filter_map(|s| self.retained.get(&s).cloned())
            .collect()
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p uc_node --lib network::udp::send_window`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt && git add -A && git commit -m "feat(udp): send window — retain/ack/resend + flow-control byte budget"
```

---

# PHASE B (cont.) — async session + mux

### Task 8: `UdpSession` — async reliable ordered message channel for one peer

**Files:**
- Create: `uc_node/src/network/udp/session.rs`
- Test: inline integration test using a real loopback socket pair

**Interfaces:**
- Consumes: `wire::Segment`, `fragment::fragment`, `reassembly::Reassembler`, `send_window::SendWindow`, `UdpTuning`.
- Produces:
  - `struct UdpSession` (cheaply clonable handle: `Arc` inside).
  - `async fn UdpSession::send_message(&self, payload: Bytes) -> Result<(), NetworkError>` — fragments, applies flow-control back-pressure, transmits, retains for retransmit.
  - `fn UdpSession::deliver(&self, seg: Segment)` — feed an inbound segment (called by the mux); drives reassembly, emits completed messages to the session's inbound channel, sends NAK/SM as needed.
  - `async fn UdpSession::recv_message(&self) -> Option<Bytes>` — next reassembled inbound message.
  - `struct SessionTx` — the outbound primitive the session uses: `async fn send_to(&self, datagram: Bytes)`. Implemented over a shared socket + peer addr (Task 9 supplies it; here a test double).

> **Design note for the implementer:** keep socket transmission behind a small `SessionTx` trait so the session is testable without the mux. The session owns a `tokio::sync::Mutex<SessionState>` holding the `SendWindow`, `Reassembler`, next send seq, and the inbound `mpsc` sender. A background ticker task (spawned by the mux, Task 9) calls `tick()` for periodic SM + heartbeat + NAK-retry; in unit tests we drive `deliver`/`send_message` directly.

- [ ] **Step 1: Write the failing test (two sessions over an in-memory link)**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// Test double: a SessionTx that pushes datagrams into a shared queue,
    /// optionally dropping the Nth datagram to exercise NAK recovery.
    #[derive(Clone)]
    struct LinkTx { peer_inbox: Arc<Mutex<Vec<Bytes>>>, drop_seqs: Arc<Mutex<Vec<u64>>> }

    #[async_trait::async_trait]
    impl SessionTx for LinkTx {
        async fn send_to(&self, datagram: Bytes) {
            // Inspect the seq; drop if scheduled (simulate loss).
            if let Ok(seg) = crate::network::udp::wire::Segment::decode(&datagram) {
                let mut drops = self.drop_seqs.lock().await;
                if let Some(pos) = drops.iter().position(|s| *s == seg.seq
                    && seg.seg_type == crate::network::udp::wire::SegType::Data) {
                    drops.remove(pos);
                    return; // dropped
                }
            }
            self.peer_inbox.lock().await.push(datagram);
        }
    }

    #[tokio::test]
    async fn message_round_trip_no_loss() {
        let inbox = Arc::new(Mutex::new(Vec::new()));
        let tx = LinkTx { peer_inbox: inbox.clone(), drop_seqs: Arc::new(Mutex::new(vec![])) };
        let a = UdpSession::new(1, Arc::new(tx), UdpTuning::default());
        let b = UdpSession::new(1, Arc::new(NullTx), UdpTuning::default());

        a.send_message(Bytes::from_static(b"hello world")).await.unwrap();
        // Deliver everything A transmitted into B.
        for dg in inbox.lock().await.drain(..) {
            b.deliver(crate::network::udp::wire::Segment::decode(&dg).unwrap());
        }
        let msg = b.recv_message().await.unwrap();
        assert_eq!(&msg[..], b"hello world");
    }

    struct NullTx;
    #[async_trait::async_trait]
    impl SessionTx for NullTx { async fn send_to(&self, _d: Bytes) {} }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_node --lib network::udp::session`
Expected: FAIL — `UdpSession`/`SessionTx` not defined.

- [ ] **Step 3: Implement `session.rs`**

```rust
//! One reliable, ordered, fragmenting message channel to a single peer over a
//! shared UDP socket. Flow control + NAK retransmit per the Aeron-derived subset.
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{mpsc, Mutex};

use super::reassembly::Reassembler;
use super::send_window::SendWindow;
use super::wire::{SegType, Segment, FLAG_BEGIN, WIRE_VERSION};
use super::{fragment, UdpTuning};
use crate::network::NetworkError;

/// Outbound datagram primitive. Implemented over the shared socket in Task 9;
/// a test double in unit tests.
#[async_trait::async_trait]
pub trait SessionTx: Send + Sync + 'static {
    async fn send_to(&self, datagram: Bytes);
}

struct SessionState {
    next_send_seq: u64,
    window: SendWindow,
    reasm: Reassembler,
    /// Last highest-contiguous we acked to the peer (for SM dedup).
    last_acked_to_peer: Option<u64>,
}

pub struct UdpSession {
    session_id: u32,
    tx: Arc<dyn SessionTx>,
    tuning: UdpTuning,
    state: Mutex<SessionState>,
    inbound_tx: mpsc::UnboundedSender<Bytes>,
    inbound_rx: Mutex<mpsc::UnboundedReceiver<Bytes>>,
}

impl UdpSession {
    pub fn new(session_id: u32, tx: Arc<dyn SessionTx>, tuning: UdpTuning) -> Arc<Self> {
        let (itx, irx) = mpsc::unbounded_channel();
        Arc::new(Self {
            session_id,
            tx,
            window_cap: tuning.flow_window_bytes,
            state: Mutex::new(SessionState {
                next_send_seq: 0,
                window: SendWindow::new(tuning.flow_window_bytes),
                reasm: Reassembler::new(),
                last_acked_to_peer: None,
            }),
            tuning,
            inbound_tx: itx,
            inbound_rx: Mutex::new(irx),
        })
    }

    fn mk(&self, seg_type: SegType, flags: u8, seq: u64, arg: u64, payload: Bytes) -> Bytes {
        Segment { version: WIRE_VERSION, seg_type, flags, session_id: self.session_id, seq, arg, payload }
            .encode()
            .freeze()
    }

    /// Fragment + transmit a logical message, retaining segments for retransmit.
    /// Applies flow-control back-pressure: yields until the window admits.
    pub async fn send_message(&self, payload: Bytes) -> Result<(), NetworkError> {
        let mut st = self.state.lock().await;
        let first = st.next_send_seq;
        let frags = fragment::fragment(&payload, first, self.tuning.mtu);
        for (seq, flags, chunk) in frags {
            let dg = self.mk(SegType::Data, flags, seq, 0, chunk);
            // Flow-control: wait until the window can admit this datagram.
            while !st.window.can_admit(dg.len()) {
                drop(st);
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                st = self.state.lock().await;
            }
            st.window.push(seq, dg.clone());
            st.next_send_seq = seq + 1;
            self.tx.send_to(dg).await;
        }
        Ok(())
    }

    /// Feed one inbound segment. Drives reassembly + control responses.
    pub fn deliver(&self, seg: Segment) {
        // Lock-free fast paths route by type; data/nak/sm need the state lock.
        // Spawn-free: deliver is sync and short; control sends are queued via
        // try-path inside the async helpers the mux calls. To keep deliver
        // synchronous we stash work and let the mux's per-session task flush.
        // For simplicity and correctness we block-in-place on the mutex here is
        // NOT allowed (sync fn). Instead, deliver pushes to an internal channel
        // the per-session task drains. See `process` below.
        let _ = self.ingress_tx.send(seg);
    }

    /// Drain loop body — called by the per-session task (Task 9).
    pub async fn process(&self, seg: Segment) {
        match seg.seg_type {
            SegType::Data | SegType::Hello => {
                let mut st = self.state.lock().await;
                let msgs = st.reasm.accept(seg.seq, seg.flags, seg.payload);
                for m in msgs {
                    let _ = self.inbound_tx.send(m);
                }
                // Reactive NAK for any gap.
                let gaps = st.reasm.gaps();
                let hc = st.reasm.highest_contiguous();
                drop(st);
                for (start, count) in gaps {
                    let nak = self.mk(SegType::Nak, 0, start, count, Bytes::new());
                    self.tx.send_to(nak).await;
                }
                if let Some(hc) = hc {
                    let sm = self.mk(SegType::Sm, 0, hc, self.tuning.flow_window_bytes, Bytes::new());
                    self.tx.send_to(sm).await;
                }
            }
            SegType::Sm => {
                let mut st = self.state.lock().await;
                st.window.on_ack(seg.seq); // seg.seq = peer highest_contiguous
            }
            SegType::Nak => {
                let st = self.state.lock().await;
                let dgs = st.window.resend(seg.seq, seg.arg);
                drop(st);
                for dg in dgs {
                    self.tx.send_to(dg).await;
                }
            }
            SegType::Heartbeat | SegType::HelloAck => { /* liveness only */ }
        }
    }

    pub async fn recv_message(&self) -> Option<Bytes> {
        self.inbound_rx.lock().await.recv().await
    }
}
```

> **Implementer reconciliation:** the test in Step 1 calls `deliver` then `recv_message` synchronously. To make `deliver` usable directly in unit tests *and* via a task in the mux, implement `deliver` to call `process` on a `tokio` current-thread block is not possible from a sync fn. Resolve by making the unit test call `session.process(seg).await` instead of `deliver`, and have `deliver` (sync) forward to an `ingress` `mpsc` that the mux task drains by calling `process`. Update the Step-1 test to `b.process(seg).await` and drop the `ingress_tx`/`deliver` indirection if you prefer a single async entry point — **the canonical inbound entry point used by the rest of the plan is `async fn process(&self, seg: Segment)`**, and `recv_message` for delivery. Add `window_cap`/`ingress` fields only if you keep `deliver`. Keep `process` + `recv_message` + `send_message` as the stable surface.

- [ ] **Step 4: Adjust the Step-1 test to the canonical surface**

Change `b.deliver(seg)` → `b.process(seg).await` and remove the `LinkTx` `deliver`-time decode-drop if simpler. Keep the loss-drop variant for Task’s recovery test below.

- [ ] **Step 5: Add a loss-recovery test**

```rust
    #[tokio::test]
    async fn recovers_dropped_fragment_via_nak() {
        // A sends a 3-fragment message; the middle fragment (seq 1) is dropped
        // on first transmit. B NAKs; A resends; B reassembles.
        let a_inbox = Arc::new(Mutex::new(Vec::new())); // datagrams A->B
        let b_inbox = Arc::new(Mutex::new(Vec::new())); // datagrams B->A
        let a_tx = LinkTx { peer_inbox: a_inbox.clone(), drop_seqs: Arc::new(Mutex::new(vec![1])) };
        let b_tx = LinkTx { peer_inbox: b_inbox.clone(), drop_seqs: Arc::new(Mutex::new(vec![])) };
        let a = UdpSession::new(1, Arc::new(a_tx), small_mtu_tuning());
        let b = UdpSession::new(1, Arc::new(b_tx), small_mtu_tuning());

        a.send_message(Bytes::from(vec![7u8; 12])).await.unwrap(); // forces >1 fragment
        // Deliver A->B (seq 1 missing).
        for dg in a_inbox.lock().await.drain(..) {
            b.process(Segment::decode(&dg).unwrap()).await;
        }
        // B emitted a NAK to a_inbox? No — B's control goes to b_inbox. Feed to A.
        for dg in b_inbox.lock().await.drain(..) {
            a.process(Segment::decode(&dg).unwrap()).await;
        }
        // A's resend now sits in a_inbox; deliver to B.
        for dg in a_inbox.lock().await.drain(..) {
            b.process(Segment::decode(&dg).unwrap()).await;
        }
        let msg = b.recv_message().await.unwrap();
        assert_eq!(msg.len(), 12);
    }

    fn small_mtu_tuning() -> UdpTuning {
        UdpTuning { mtu: super::super::wire::SEG_HEADER_LEN + 4 + 5, ..UdpTuning::default() }
    }
```

- [ ] **Step 6: Run to verify it passes**

Run: `cargo test -p uc_node --lib network::udp::session`
Expected: PASS (round-trip + loss-recovery).

- [ ] **Step 7: clippy + commit**

```bash
cargo clippy -p uc_node -- -D warnings && cargo fmt && git add -A && git commit -m "feat(udp): UdpSession — reliable ordered fragmenting channel (flow control + NAK)"
```

---

### Task 9: Shared-socket mux + per-session ticker + RPC correlation

**Files:**
- Create: `uc_node/src/network/udp/mux.rs`
- Test: inline integration test over two real loopback sockets

**Interfaces:**
- Consumes: `UdpSession`, `UdpTuning`, `wire::Segment`, `frame::Frame`.
- Produces:
  - `struct UdpMux` holding `Arc<UdpSocket>`, a session registry `Mutex<HashMap<u32, Arc<UdpSession>>>` keyed by `session_id`, and a pending-RPC map `Mutex<HashMap<u64, oneshot::Sender<Frame>>>` keyed by `Frame.request_id`.
  - `async fn UdpMux::bind(addr: SocketAddr, tuning: UdpTuning) -> Result<Arc<UdpMux>, NetworkError>` — binds the socket, spawns the receive loop.
  - `async fn UdpMux::open_session(&self, peer: SocketAddr, app_id: &str) -> Result<Arc<UdpSession>, NetworkError>` — get-or-create the per-peer session (handshake validates app_id/version), spawns its ticker.
  - `async fn UdpMux::rpc(&self, peer: SocketAddr, app_id: &str, req: Frame, timeout: Duration) -> Result<Frame, NetworkError>` — send a request `Frame` over the peer session, await the response correlated by `request_id`.
  - `fn UdpMux::set_request_handler(&self, handler: Arc<dyn Fn(Frame) -> BoxFuture<Frame> + Send + Sync>)` — server-side: how inbound request Frames are answered (Task 12 sets this to dispatch into `Raft`).

> **Design note:** session ids are derived deterministically as `hash(min(local,peer)+max)` or simply the peer's socket addr hashed — both ends must agree. Simplest: `session_id = fnv32(local_addr ^ peer_addr)` computed identically on both sides from the connecting pair, or carried in the HELLO. Use the HELLO-carried id: the connector picks a random-but-stable id (derive from its own listen port + peer port, no RNG needed at runtime) and the acceptor adopts it. The receive loop routes inbound segments to `sessions[seg.session_id]`, creating an acceptor-side session on first HELLO.
>
> The mux's receive loop reads datagrams, decodes `Segment`, and: (a) routes DATA/NAK/SM/HEARTBEAT to the session's `process`; (b) when a session emits a complete inbound message (a `Frame`), the mux checks `Frame.is_response()` — responses complete the matching `pending[request_id]` oneshot; requests go to the `request_handler`, whose returned `Frame` is sent back over the same session.

- [ ] **Step 1: Write the failing test (two muxes, request/response over loopback)**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::frame::{Frame, MessageType};
    use bytes::Bytes;
    use std::time::Duration;

    #[tokio::test]
    async fn rpc_round_trip_over_loopback() {
        let server = UdpMux::bind("127.0.0.1:0".parse().unwrap(), UdpTuning::default()).await.unwrap();
        let server_addr = server.local_addr().unwrap();
        // Echo handler: respond with the same body, response flag set.
        server.set_request_handler(std::sync::Arc::new(|req: Frame| {
            Box::pin(async move {
                Frame::new_response(MessageType::AppendEntriesResp, req.request_id, req.body)
            })
        }));

        let client = UdpMux::bind("127.0.0.1:0".parse().unwrap(), UdpTuning::default()).await.unwrap();
        let req = Frame::new_request(MessageType::AppendEntriesReq, 99, Bytes::from_static(b"ping"));
        let resp = client.rpc(server_addr, "test-app", req, Duration::from_secs(2)).await.unwrap();
        assert_eq!(resp.request_id, 99);
        assert!(resp.is_response());
        assert_eq!(&resp.body[..], b"ping");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_node --lib network::udp::mux`
Expected: FAIL — `UdpMux` not defined.

- [ ] **Step 3: Implement `mux.rs`**

Implement per the design note. Key pieces (complete code):

```rust
//! Shared-UDP-socket multiplexer: one socket per process, demuxed to per-peer
//! `UdpSession`s by session_id; RPC request/response correlation by Frame.request_id.
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use tokio::net::UdpSocket;
use tokio::sync::{oneshot, Mutex};

use super::session::{SessionTx, UdpSession};
use super::wire::{SegType, Segment, WIRE_VERSION};
use super::UdpTuning;
use crate::network::frame::Frame;
use crate::network::NetworkError;

type Handler = Arc<dyn Fn(Frame) -> BoxFuture<'static, Frame> + Send + Sync>;

/// SessionTx over the shared socket to a fixed peer.
struct SocketTx { sock: Arc<UdpSocket>, peer: SocketAddr }
#[async_trait::async_trait]
impl SessionTx for SocketTx {
    async fn send_to(&self, datagram: Bytes) {
        let _ = self.sock.send_to(&datagram, self.peer).await;
    }
}

pub struct UdpMux {
    sock: Arc<UdpSocket>,
    tuning: UdpTuning,
    sessions: Mutex<HashMap<u32, (Arc<UdpSession>, SocketAddr)>>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Frame>>>>,
    handler: Mutex<Option<Handler>>,
}

impl UdpMux {
    pub async fn bind(addr: SocketAddr, tuning: UdpTuning) -> Result<Arc<Self>, NetworkError> {
        let sock = Arc::new(UdpSocket::bind(addr).await.map_err(NetworkError::Io)?);
        let mux = Arc::new(Self {
            sock,
            tuning,
            sessions: Mutex::new(HashMap::new()),
            pending: Arc::new(Mutex::new(HashMap::new())),
            handler: Mutex::new(None),
        });
        mux.clone().spawn_recv_loop();
        Ok(mux)
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> { self.sock.local_addr() }

    pub async fn set_request_handler(&self, h: Handler) {
        *self.handler.lock().await = Some(h);
    }

    fn session_id_for(local: SocketAddr, peer: SocketAddr) -> u32 {
        // Order-independent so both ends agree.
        let (a, b) = if local <= peer { (local, peer) } else { (peer, local) };
        let mut h = crc32fast::Hasher::new();
        h.update(a.to_string().as_bytes());
        h.update(b.to_string().as_bytes());
        h.finalize()
    }

    async fn get_or_create_session(&self, sid: u32, peer: SocketAddr) -> Arc<UdpSession> {
        let mut s = self.sessions.lock().await;
        if let Some((sess, _)) = s.get(&sid) {
            return sess.clone();
        }
        let tx = Arc::new(SocketTx { sock: self.sock.clone(), peer });
        let sess = UdpSession::new(sid, tx, self.tuning.clone());
        s.insert(sid, (sess.clone(), peer));
        sess
    }

    pub async fn open_session(&self, peer: SocketAddr, _app_id: &str)
        -> Result<Arc<UdpSession>, NetworkError>
    {
        let sid = Self::session_id_for(self.local_addr()?, peer);
        Ok(self.get_or_create_session(sid, peer).await)
    }

    pub async fn rpc(&self, peer: SocketAddr, app_id: &str, req: Frame, timeout: Duration)
        -> Result<Frame, NetworkError>
    {
        let sess = self.open_session(peer, app_id).await?;
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(req.request_id, tx);
        let encoded = req.encode().freeze();
        sess.send_message(encoded).await?;
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(frame)) => Ok(frame),
            Ok(Err(_)) => Err(NetworkError::Disconnected),
            Err(_) => {
                self.pending.lock().await.remove(&req.request_id);
                Err(NetworkError::Timeout)
            }
        }
    }

    fn spawn_recv_loop(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let (n, peer) = match self.sock.recv_from(&mut buf).await {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let seg = match Segment::decode(&buf[..n]) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let sid = seg.session_id;
                let sess = self.get_or_create_session(sid, peer).await;
                // Drive the session, then drain any completed inbound messages.
                sess.process(seg).await;
                while let Ok(msg) = sess.try_recv_message() {
                    self.clone().route_inbound_message(sess.clone(), msg);
                }
            }
        });
    }

    fn route_inbound_message(self: Arc<Self>, sess: Arc<UdpSession>, msg: Bytes) {
        tokio::spawn(async move {
            let mut b = msg;
            let frame = match Frame::decode(&mut b) {
                Ok(f) => f,
                Err(_) => return,
            };
            if frame.is_response() {
                if let Some(tx) = self.pending.lock().await.remove(&frame.request_id) {
                    let _ = tx.send(frame);
                }
            } else if let Some(h) = self.handler.lock().await.clone() {
                let resp = h(frame).await;
                let _ = sess.send_message(resp.encode().freeze()).await;
            }
        });
    }
}
```

> Add to `UdpSession` a non-blocking `pub fn try_recv_message(&self) -> Result<Bytes, tokio::sync::mpsc::error::TryRecvError>` that calls `self.inbound_rx.try_lock()`-then-`try_recv`; if the lock is held, return `Empty` (the next `process` will re-drain). Simpler: replace the `inbound_rx: Mutex<...>` drain with the mux holding the receiver. **Cleaner final shape:** `UdpSession::new` returns `(Arc<UdpSession>, mpsc::UnboundedReceiver<Bytes>)` and the mux owns the receiver per session. Refactor Task 8's surface to return the receiver and update its tests to read from it. Pick one and keep it consistent; the canonical choice for the rest of the plan is **the mux owns the per-session inbound receiver**.

`udp/mod.rs`: add `pub mod session;`, `pub mod fragment;` (already), `pub mod mux;`.

- [ ] **Step 4: Reconcile the session inbound-receiver ownership**

Apply the "mux owns the receiver" refactor: `UdpSession::new(session_id, tx, tuning) -> (Arc<UdpSession>, mpsc::UnboundedReceiver<Bytes>)`; remove `recv_message`/`try_recv_message` from the session; the mux stores the receiver alongside the session in the registry and drains it after each `process`. Update Task 8 tests to read from the returned receiver.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p uc_node --lib network::udp`
Expected: PASS (wire, fragment, reassembly, send_window, session, mux).

- [ ] **Step 6: Add a periodic ticker (heartbeat + SM + NAK-retry)**

Add `UdpSession::tick(&self)` that: sends a HEARTBEAT if idle; re-sends the latest SM (ack); re-NAKs any still-open gap older than `nak_linger_ms`. In `get_or_create_session`, spawn a per-session task: `loop { sleep(min(heartbeat_ms, sm_interval_ms)); sess.tick().await; }`. Add a test that a stalled gap is re-NAK'd after the linger interval (use `tokio::time::pause`/`advance`).

```rust
    #[tokio::test(start_paused = true)]
    async fn re_naks_unfilled_gap_after_linger() {
        // ... construct a session, deliver seq 1 (gap at 0), advance time past
        // nak_linger_ms, assert a second NAK(0,1) was emitted.
    }
```

- [ ] **Step 7: clippy + commit**

```bash
cargo clippy -p uc_node -- -D warnings && cargo fmt && git add -A && git commit -m "feat(udp): shared-socket mux — session demux, RPC correlation, ticker"
```

---

# PHASE C — openraft glue on top of the channel

### Task 10: `UdpRaftNetwork` (impl `RaftNetwork<TypeConfig>` V1)

**Files:**
- Create: `uc_node/src/network/udp/instance.rs`
- Test: covered by the integration test in Task 13 (capstone); add a focused unit test here for the request-id path

**Interfaces:**
- Consumes: `UdpMux`, `frame::{Frame, MessageType}`, `codec`.
- Produces: `struct UdpRaftNetwork { target: NodeId, peer_addr: SocketAddr, mux: Arc<UdpMux>, app_id: String, request_id: AtomicU64, [fault fields] }` implementing the three `RaftNetwork` methods, mirroring `quic/instance.rs` exactly but issuing RPCs via `mux.rpc(...)`.

- [ ] **Step 1: Implement `instance.rs`** (mirror `quic/instance.rs`; the body of each method is identical except the transport call)

```rust
//! `RaftNetwork` impl over the shared UDP mux. Mirrors quic/instance.rs; each
//! RPC encodes the body, issues mux.rpc() (request_id-correlated), decodes resp.
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::RPCOption;
use openraft::raft::*;
use openraft_legacy::network_v1::RaftNetwork;

use super::mux::UdpMux;
use crate::network::frame::{Frame, MessageType};
use crate::network::{codec, NetworkError};
use crate::raft::{NodeId, TypeConfig};

pub struct UdpRaftNetwork {
    target: NodeId,
    peer_addr: SocketAddr,
    mux: Arc<UdpMux>,
    app_id: String,
    request_id: Arc<AtomicU64>,
    #[cfg(feature = "fault-injection")]
    source: NodeId,
    #[cfg(feature = "fault-injection")]
    fault_table: Option<Arc<crate::network::fault::FaultTable>>,
}

impl UdpRaftNetwork {
    pub(crate) fn new(target: NodeId, peer_addr: SocketAddr, mux: Arc<UdpMux>,
                      app_id: String, request_id: Arc<AtomicU64>) -> Self {
        Self { target, peer_addr, mux, app_id, request_id,
               #[cfg(feature = "fault-injection")] source: 0,
               #[cfg(feature = "fault-injection")] fault_table: None }
    }
    #[cfg(feature = "fault-injection")]
    pub(crate) fn with_fault(mut self, source: NodeId,
        ft: Option<Arc<crate::network::fault::FaultTable>>) -> Self {
        self.source = source; self.fault_table = ft; self
    }

    async fn do_rpc(&self, req_type: MessageType, body: bytes::Bytes,
                    resp_type: MessageType, timeout: Duration) -> Result<bytes::Bytes, NetworkError> {
        let rid = self.request_id.fetch_add(1, Ordering::Relaxed);
        let req = Frame::new_request(req_type, rid, body);
        let resp = self.mux.rpc(self.peer_addr, &self.app_id, req, timeout).await?;
        if resp.msg_type != resp_type {
            return Err(NetworkError::Decode(format!("expected {resp_type:?} got {:?}", resp.msg_type)));
        }
        Ok(resp.body)
    }
}

fn rpc_err<E: std::error::Error>(e: NetworkError) -> RPCError<TypeConfig, RaftError<TypeConfig, E>> {
    RPCError::Network(openraft::error::NetworkError::new(&e))
}

impl RaftNetwork<TypeConfig> for UdpRaftNetwork {
    async fn append_entries(&mut self, rpc: AppendEntriesRequest<TypeConfig>, option: RPCOption)
        -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>> {
        #[cfg(feature = "fault-injection")]
        if let Some(t) = &self.fault_table { if t.is_blocked(self.source, self.target) {
            return Err(rpc_err(NetworkError::Disconnected)); } }
        let body = codec::encode_append_entries_req(&rpc).map_err(rpc_err)?;
        let b = self.do_rpc(MessageType::AppendEntriesReq, body, MessageType::AppendEntriesResp, option.hard_ttl())
            .await.map_err(rpc_err)?;
        codec::decode_append_entries_resp(&b).map_err(rpc_err)
    }

    async fn install_snapshot(&mut self, rpc: InstallSnapshotRequest<TypeConfig>, option: RPCOption)
        -> Result<InstallSnapshotResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig, InstallSnapshotError>>> {
        #[cfg(feature = "fault-injection")]
        if let Some(t) = &self.fault_table { if t.is_blocked(self.source, self.target) {
            return Err(rpc_err(NetworkError::Disconnected)); } }
        let body = codec::encode_install_snapshot_req(&rpc).map_err(rpc_err)?;
        let b = self.do_rpc(MessageType::InstallSnapshotReq, body, MessageType::InstallSnapshotResp, option.hard_ttl())
            .await.map_err(rpc_err)?;
        codec::decode_install_snapshot_resp(&b).map_err(rpc_err)
    }

    async fn vote(&mut self, rpc: VoteRequest<TypeConfig>, option: RPCOption)
        -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>> {
        #[cfg(feature = "fault-injection")]
        if let Some(t) = &self.fault_table { if t.is_blocked(self.source, self.target) {
            return Err(rpc_err(NetworkError::Disconnected)); } }
        let body = codec::encode_vote_req(&rpc).map_err(rpc_err)?;
        let b = self.do_rpc(MessageType::VoteReq, body, MessageType::VoteResp, option.hard_ttl())
            .await.map_err(rpc_err)?;
        codec::decode_vote_resp(&b).map_err(rpc_err)
    }
}
```

`udp/mod.rs`: add `pub mod instance;`.

- [ ] **Step 2: Build**

Run: `cargo build -p uc_node`
Expected: compiles.

- [ ] **Step 3: Commit**

```bash
cargo fmt && git add -A && git commit -m "feat(udp): UdpRaftNetwork (RaftNetwork V1 over the mux)"
```

---

### Task 11: `UdpRaftNetworkFactory` (impl `RaftNetworkFactory<TypeConfig>`)

**Files:**
- Create: `uc_node/src/network/udp/factory.rs`

**Interfaces:**
- Consumes: `UdpMux`, `UdpRaftNetwork`, `openraft_legacy::network_v1::Adapter`.
- Produces: `struct UdpRaftNetworkFactory { mux: Arc<UdpMux>, app_id: String, request_id: Arc<AtomicU64>, [fault] }`, `type Network = Adapter<TypeConfig, UdpRaftNetwork>`. Mirrors `quic/factory.rs`.

- [ ] **Step 1: Implement `factory.rs`** (mirror `quic/factory.rs`)

```rust
//! `RaftNetworkFactory` over the shared UDP mux.
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use openraft::network::RaftNetworkFactory;
use openraft_legacy::network_v1::Adapter;

use super::instance::UdpRaftNetwork;
use super::mux::UdpMux;
use crate::raft::{NodeAddr, NodeId, TypeConfig};

pub struct UdpRaftNetworkFactory {
    mux: Arc<UdpMux>,
    app_id: String,
    request_id: Arc<AtomicU64>,
    #[cfg(feature = "fault-injection")]
    source: NodeId,
    #[cfg(feature = "fault-injection")]
    fault_table: Option<Arc<crate::network::fault::FaultTable>>,
}

impl UdpRaftNetworkFactory {
    pub fn new(mux: Arc<UdpMux>, app_id: String) -> Self {
        Self { mux, app_id, request_id: Arc::new(AtomicU64::new(1)),
               #[cfg(feature = "fault-injection")] source: 0,
               #[cfg(feature = "fault-injection")] fault_table: None }
    }
    #[cfg(feature = "fault-injection")]
    pub fn set_fault_injection(&mut self, source: NodeId,
        ft: Option<Arc<crate::network::fault::FaultTable>>) {
        self.source = source; self.fault_table = ft;
    }
}

impl RaftNetworkFactory<TypeConfig> for UdpRaftNetworkFactory {
    type Network = Adapter<TypeConfig, UdpRaftNetwork>;

    async fn new_client(&mut self, target: NodeId, node: &NodeAddr) -> Self::Network {
        let net = UdpRaftNetwork::new(target, node.raft_addr, self.mux.clone(),
                                      self.app_id.clone(), self.request_id.clone());
        #[cfg(feature = "fault-injection")]
        let net = net.with_fault(self.source, self.fault_table.clone());
        net.into_v2()
    }
}
```

> Confirmed: the `Adapter` is built via `net.into_v2()` (the `RaftNetwork::into_v2()` convenience method from `openraft-legacy`), exactly as `quic/factory.rs:98` does. `into_v2()` requires `use openraft_legacy::network_v1::RaftNetwork as _;` in scope.

`udp/mod.rs`: add `pub mod factory;` and `pub use factory::UdpRaftNetworkFactory;`.

- [ ] **Step 2: Build + commit**

Run: `cargo build -p uc_node`

```bash
cargo fmt && git add -A && git commit -m "feat(udp): UdpRaftNetworkFactory (RaftNetworkFactory over the mux)"
```

---

### Task 12: `spawn_udp_server` + `UdpServerHandle`; `UdpTransport` impl; wire the builder Udp arm

**Files:**
- Create: `uc_node/src/network/udp/server.rs`, `uc_node/src/network/udp/transport_impl.rs`
- Delete: `uc_node/src/network/udp/server_stub.rs`
- Modify: `uc_node/src/network/udp/mod.rs`, `uc_node/src/runtime/builder.rs`

**Interfaces:**
- Consumes: `UdpMux`, `Raft<TypeConfig, SM>`, `frame::Frame`, `codec`, `ClusterTransport`/`TransportCtx`.
- Produces:
  - `UdpServerHandle { mux: Arc<UdpMux> }` with `async fn shutdown(self)` + `fn local_addr(&self)`.
  - `fn spawn_udp_server<SM>(mux: Arc<UdpMux>, raft: Raft<TypeConfig, SM>) -> Result<UdpServerHandle, NetworkError>` — sets the mux request handler to dispatch request `Frame`s into `raft` (same `dispatch` body as `quic/server.rs`).
  - `struct UdpTransport { tuning: UdpTuning, shared: OnceCell<Arc<UdpMux>> }` with `fn new(tuning)`, implementing `ClusterTransport` (Factory = `UdpRaftNetworkFactory`, Server = `UdpServerHandle`). **Both halves share one `UdpMux`** (one socket per process) — `build_factory` and `spawn_server` must hand out the same `Arc<UdpMux>` bound to `ctx.listen_addr`.

> **Critical wiring detail:** unlike QUIC (separate client endpoint + server endpoint), the UDP transport uses **one socket** for both inbound and outbound. So `UdpTransport` must create the `UdpMux` once (bound to `ctx.listen_addr`) and share it between the factory and the server. Since `ClusterTransport::build_factory` and `spawn_server` are separate calls, store the mux in the `UdpTransport` (interior-mutable `OnceCell`/`Mutex<Option<...>>`), created on whichever is called first. In `builder.rs` the order is `build_factory` then `spawn_server`, so create it in `build_factory`.

- [ ] **Step 1: Implement `server.rs`**

```rust
//! UDP inbound server: sets the mux request handler to dispatch request Frames
//! into the local Raft. Dispatch body mirrors quic/server.rs.
use std::sync::Arc;

use openraft::Raft;
use openraft::storage::RaftStateMachine;
use openraft_legacy::network_v1::ChunkedSnapshotReceiver as _;

use super::mux::UdpMux;
use crate::network::frame::{Frame, MessageType};
use crate::network::{codec, NetworkError};
use crate::raft::TypeConfig;

pub struct UdpServerHandle { mux: Arc<UdpMux> }

impl UdpServerHandle {
    pub async fn shutdown(self) { /* mux socket drops with last Arc; tasks are detached */ }
    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> { self.mux.local_addr() }
}

pub fn spawn_udp_server<SM>(mux: Arc<UdpMux>, raft: Raft<TypeConfig, SM>)
    -> Result<UdpServerHandle, NetworkError>
where SM: RaftStateMachine<TypeConfig> {
    let raft = Arc::new(raft);
    let handler = {
        let raft = raft.clone();
        Arc::new(move |req: Frame| {
            let raft = raft.clone();
            Box::pin(async move { dispatch(req, &raft).await }) as futures::future::BoxFuture<'static, Frame>
        })
    };
    // set_request_handler is async; block via a detached task that completes
    // before any inbound RPC can be answered is unnecessary — set it inline.
    let mux2 = mux.clone();
    tokio::spawn(async move { mux2.set_request_handler(handler).await; });
    Ok(UdpServerHandle { mux })
}

async fn dispatch<SM>(req: Frame, raft: &Raft<TypeConfig, SM>) -> Frame
where SM: RaftStateMachine<TypeConfig> {
    let rid = req.request_id;
    let result: Result<Frame, NetworkError> = async {
        match req.msg_type {
            MessageType::AppendEntriesReq => {
                let d = codec::decode_append_entries_req(&req.body)?;
                let r = raft.append_entries(d).await
                    .map_err(|e| NetworkError::Stream(format!("append_entries: {e}")))?;
                Ok(Frame::new_response(MessageType::AppendEntriesResp, rid, codec::encode_append_entries_resp(&r)?))
            }
            MessageType::VoteReq => {
                let d = codec::decode_vote_req(&req.body)?;
                let r = raft.vote(d).await.map_err(|e| NetworkError::Stream(format!("vote: {e}")))?;
                Ok(Frame::new_response(MessageType::VoteResp, rid, codec::encode_vote_resp(&r)?))
            }
            MessageType::InstallSnapshotReq => {
                let d = codec::decode_install_snapshot_req(&req.body)?;
                let r = raft.install_snapshot(d).await
                    .map_err(|e| NetworkError::Stream(format!("install_snapshot: {e}")))?;
                Ok(Frame::new_response(MessageType::InstallSnapshotResp, rid, codec::encode_install_snapshot_resp(&r)?))
            }
            other => Err(NetworkError::Decode(format!("server got non-request {other:?}"))),
        }
    }.await;
    result.unwrap_or_else(|e| {
        // On dispatch error, return an empty response of a sentinel type so the
        // client's type check fails → it surfaces an RPCError → openraft retries.
        tracing::warn!(error = ?e, "udp dispatch failed");
        Frame::new_response(MessageType::HandshakeAck, rid, bytes::Bytes::new())
    })
}
```

> Setting the handler in a spawned task races the first inbound RPC. Prefer making `spawn_udp_server` async (`pub async fn`) and `await`ing `set_request_handler` directly, then have the `ClusterTransport::spawn_server` call it via `tokio::task::block_in_place` is not available on current_thread — instead make `ClusterTransport::spawn_server` itself able to return a future. Simplest correct fix: store the handler in the mux **before** the recv loop can dispatch by having `set_request_handler` be a plain `&self` setter over a `parking_lot::Mutex` (sync, no await). Change `handler: Mutex<Option<Handler>>` → `handler: parking_lot::Mutex<Option<Handler>>` and make `set_request_handler` sync. Then `spawn_udp_server` sets it synchronously with no race. Apply this and drop the spawned task.

- [ ] **Step 2: Implement `transport_impl.rs`**

```rust
use std::sync::Arc;

use openraft::Raft;
use openraft::storage::RaftStateMachine;
use parking_lot::Mutex;

use super::factory::UdpRaftNetworkFactory;
use super::mux::UdpMux;
use super::server::{spawn_udp_server, UdpServerHandle};
use super::UdpTuning;
use crate::network::transport::{ClusterTransport, TransportCtx};
use crate::network::NetworkError;
use crate::raft::TypeConfig;

pub struct UdpTransport {
    tuning: UdpTuning,
    shared: Mutex<Option<Arc<UdpMux>>>,
}

impl UdpTransport {
    pub fn new(tuning: UdpTuning) -> Self { Self { tuning, shared: Mutex::new(None) } }

    fn mux_blocking(&self, ctx: &TransportCtx) -> Result<Arc<UdpMux>, NetworkError> {
        if let Some(m) = self.shared.lock().clone() { return Ok(m); }
        // bind() is async; run it to completion on the current runtime.
        let mux = futures::executor::block_on(UdpMux::bind(ctx.listen_addr, self.tuning.clone()))?;
        *self.shared.lock() = Some(mux.clone());
        Ok(mux)
    }
}

impl ClusterTransport for UdpTransport {
    type Factory = UdpRaftNetworkFactory;
    type Server = UdpServerHandle;

    fn build_factory(&self, ctx: &TransportCtx) -> Result<Self::Factory, NetworkError> {
        let mux = self.mux_blocking(ctx)?;
        #[allow(unused_mut)]
        let mut f = UdpRaftNetworkFactory::new(mux, ctx.app_id.clone());
        #[cfg(feature = "fault-injection")]
        f.set_fault_injection(ctx.node_id, ctx.fault_table.clone());
        Ok(f)
    }

    fn spawn_server<SM>(&self, ctx: &TransportCtx, raft: Raft<TypeConfig, SM>)
        -> Result<Self::Server, NetworkError>
    where SM: RaftStateMachine<TypeConfig> {
        let mux = self.mux_blocking(ctx)?;
        spawn_udp_server(mux, raft)
    }
}
```

> `futures::executor::block_on` inside an async builder running on a multi-thread runtime is acceptable but blocks the worker; the cleaner option is to make `ClusterTransport::build_factory`/`spawn_server` `async`. Given `builder.rs finish()` is already `async`, **prefer making the trait methods `async fn`** and `await` them in the builder. Update `transport.rs` (Task 3) trait to `async fn build_factory`/`async fn spawn_server`, the QUIC impl to `async fn` (its body has no awaits — fine), and the builder arms to `.await`. This removes `block_on` entirely. Do this refactor here and adjust Task 3's QUIC impl accordingly.

- [ ] **Step 3: Replace the builder Udp arm**

Restore the real `Udp` arm in `builder.rs finish()` (replacing the Phase-A `unreachable!`), now `await`ing the async trait methods:

```rust
        crate::config::Transport::Udp(tuning) => {
            let t = crate::network::udp::UdpTransport::new(tuning.clone());
            let network = t.build_factory(&ctx).await
                .map_err(|e| ClusterError::Config(format!("transport factory: {e}")))?;
            let raft = Raft::new(config.node_id, raft_config, network, log_storage, sm_adapter)
                .await
                .map_err(|e| ClusterError::Raft(format!("Raft::new: {e}")))?;
            let server = t.spawn_server(&ctx, raft.clone()).await
                .map_err(|e| ClusterError::Config(format!("transport server: {e}")))?;
            (raft, TransportServer::Udp(server))
        }
```

Make the QUIC arm `.await` its calls too. Delete `udp/server_stub.rs` and its `mod`/`pub use` lines; `udp/mod.rs` now does `pub mod server; pub use server::UdpServerHandle; pub mod transport_impl; pub use transport_impl::UdpTransport;`.

- [ ] **Step 4: Build + existing suite (QUIC still default, still green)**

Run: `cargo build --workspace && cargo test -p uc_node`
Expected: builds; QUIC tests green.

- [ ] **Step 5: clippy + commit**

```bash
cargo clippy --workspace -- -D warnings && cargo fmt && git add -A && git commit -m "feat(udp): spawn_udp_server + UdpTransport; builder Udp arm live (async transport seam)"
```

---

### Task 13: Single-node + 3-node UDP smoke test; UDP lincheck capstone

**Files:**
- Create: `uc_node/tests/udp_three_node.rs` (mirrors `m3_three_node_shmem.rs` but `transport: Transport::Udp(UdpTuning::default())`)
- Create/parametrize: a UDP variant of the lincheck capstone entry (reuse `uc_node/tests/lin_register.rs` harness with the transport flipped)

**Interfaces:**
- Consumes: the public `NodeBuilder` + `NodeConfig.transport`.

- [ ] **Step 1: Write the 3-node UDP smoke test**

Copy `uc_node/tests/m3_three_node_shmem.rs` to `udp_three_node.rs`; in the `NodeConfig` literal(s) set `transport: uc_node::Transport::Udp(uc_node::UdpTuning::default())`. Keep the rest identical (Peers bootstrap, write N entries, assert all three converge).

- [ ] **Step 2: Run it**

Run: `cargo test -p uc_node --test udp_three_node -- --test-threads=1`
Expected: PASS — leader elected over UDP, entries replicated and converged on all three nodes.

> If it hangs on election, the most likely cause is the handler-set race (Task 12 Step 1 note) or session-id disagreement (Task 9): verify both ends compute the same `session_id_for`. Debug with `RUST_LOG=uc_node::network::udp=debug`.

- [ ] **Step 3: Run the lincheck capstone over UDP**

Add a feature-flag or env switch to `lin_register.rs`'s cluster construction so `UC_TEST_TRANSPORT=udp` flips `transport`. Then:

Run: `UC_TEST_TRANSPORT=udp cargo test -p uc_node --test lin_register -- --test-threads=1`
Expected: PASS — linearizable under churn over the UDP transport.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "test(udp): 3-node replication smoke + lincheck capstone over UDP transport"
```

---

# PHASE D — Fault injection + correctness under loss/partition

### Task 14: Extend `fault.rs` with drop/delay/reorder

**Files:**
- Modify: `uc_node/src/network/fault.rs`
- Modify: `uc_node/src/network/udp/mux.rs` (consult fault table at the send chokepoint)

**Interfaces:**
- Consumes: existing `FaultTable`.
- Produces (additive on `FaultTable`):
  - `fn set_loss(&self, src: NodeId, dst: NodeId, drop_prob: f64)` / `fn loss(&self, src, dst) -> f64`
  - `fn set_delay(&self, src, dst, ms: u64)` / `fn delay(&self, src, dst) -> u64`
  - `fn should_drop(&self, src, dst, roll: f64) -> bool` (deterministic given the roll; caller supplies an RNG draw so tests are seedable).

- [ ] **Step 1: Write the failing test**

```rust
    #[test]
    fn loss_probability_threshold() {
        let t = FaultTable::new();
        t.set_loss(1, 2, 0.5);
        assert!(t.should_drop(1, 2, 0.4));   // below prob → drop
        assert!(!t.should_drop(1, 2, 0.6));  // above prob → pass
        assert!(!t.should_drop(2, 1, 0.1));  // unset pair → never drop
    }
    #[test]
    fn delay_lookup() {
        let t = FaultTable::new();
        t.set_delay(1, 2, 25);
        assert_eq!(t.delay(1, 2), 25);
        assert_eq!(t.delay(2, 1), 0);
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_node --lib network::fault --features fault-injection`
Expected: FAIL.

- [ ] **Step 3: Implement the additive methods**

Add two `Mutex<HashMap<(NodeId,NodeId), f64/u64>>` fields (`loss`, `delay`) to `FaultTable`, the setters/getters, and:

```rust
    pub fn should_drop(&self, src: NodeId, dst: NodeId, roll: f64) -> bool {
        let p = self.loss.lock().unwrap().get(&(src, dst)).copied().unwrap_or(0.0);
        roll < p
    }
```

(`blocked`/partition logic stays exactly as is.)

- [ ] **Step 4: Apply at the UDP send chokepoint**

In `mux.rs` `SocketTx::send_to` (or in `UdpRaftNetwork::do_rpc` before send), when `fault-injection` is on and a fault table + source/target are present: roll `rand::random::<f64>()`; if `should_drop` → return without sending (the segment is "lost", NAK will recover or the RPC times out); apply `delay(src,dst)` via `tokio::time::sleep` before send. Thread `source`/`fault_table` into the mux the same way the factory already does for the network instance.

> Loss/delay belong at the **segment** layer (mux/SocketTx), not the RPC layer, so they exercise NAK/retransmit (the whole point). Thread the fault table into `UdpMux` via `UdpRaftNetworkFactory::set_fault_injection` → mux, keyed by peer addr → target NodeId (carry a `peer_addr → node_id` map, or attach src/dst to the `SocketTx`).

- [ ] **Step 5: Run**

Run: `cargo test -p uc_node --lib network::fault --features fault-injection`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt && git add -A && git commit -m "feat(fault): drop/delay/reorder hooks; applied at UDP segment chokepoint"
```

---

### Task 15: UDP under partition + loss; run partition suite

**Files:**
- Create: `uc_node/tests/udp_partition.rs` (mirror `lin_partition.rs` with `transport: Udp`)

- [ ] **Step 1: Mirror the partition suite over UDP**

Copy `uc_node/tests/lin_partition.rs` scenario setup; flip `transport` to `Udp`; thread the same `FaultTable` via `NodeBuilder::with_fault_table`. Keep the three scenarios (isolate-leader, minority-partition, three-way quorum loss) and the WGL assertion.

- [ ] **Step 2: Run with loss injected**

Run: `cargo test -p uc_node --test udp_partition --features fault-injection -- --test-threads=1`
Expected: PASS — linearizable; no split-brain; clean failure under lost quorum; NAK recovers ordinary loss.

- [ ] **Step 3: Add a pure-loss capstone variant**

Add a scenario with `set_loss(_, _, 0.1)` on all links (no partition) under churn; assert linearizable + that the cluster still makes progress (NAK retransmit working).

Run: `cargo test -p uc_node --test udp_partition --features fault-injection -- --test-threads=1 lossy`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "test(udp): partition + 10% loss linearizability over UDP transport"
```

---

### Task 16: Multi-process hard-crash over UDP

**Files:**
- Modify: `examples/uc-crashtest` reference bins to accept `UC_TRANSPORT=udp`

- [ ] **Step 1: Thread `UC_TRANSPORT` into the crashtest node bin**

In the crashtest node reference bin's `NodeConfig`, read `UC_TRANSPORT` (mirror `uc-node-launch`'s `UC_DURABILITY` pattern): `"udp"` → `Transport::Udp(UdpTuning::default())`, else `Quic`.

- [ ] **Step 2: Run the hard-crash test over UDP**

Run: `UC_TRANSPORT=udp cargo test -p uc-crashtest --features hard-crash-tests`
Expected: PASS — SIGKILL service mid-load, linearizable, over UDP.

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "test(udp): multi-process hard-crash linearizability over UDP transport"
```

---

# PHASE E — N-node bench harness + transport knob

### Task 17: `--transport`/`UC_TRANSPORT` in `uc-node-launch`; arbitrary N

**Files:**
- Modify: `uc_autobench/src/bin/uc-node-launch.rs`, `uc_autobench/scripts/run-uc-3node.sh` (generalize to `run-uc-Nnode.sh`)

- [ ] **Step 1: Add the transport knob to `uc-node-launch`**

In the `NodeConfig` literal, add:

```rust
        transport: match std::env::var("UC_TRANSPORT").ok().as_deref().map(str::to_ascii_lowercase).as_deref() {
            Some("udp") => uc_node::Transport::Udp(uc_node::UdpTuning::default()),
            _ => uc_node::Transport::Quic,
        },
```

- [ ] **Step 2: Generalize the run script to N nodes**

Copy `run-uc-3node.sh` → `run-uc-Nnode.sh`. Replace the hardcoded `for n in 1 2 3` and `P1/P2/P3` with `N="${N:-3}"`, build the `PEERS` array and per-node addrs in a loop (`127.0.0.1:$((7000+n))`), pass `UC_TRANSPORT` through to each launched process. Keep the leader-probe + hardened teardown logic. Keep `run-uc-3node.sh` as a thin `N=3 ... run-uc-Nnode.sh` shim.

- [ ] **Step 3: Smoke-run both transports**

Run:
```bash
N=3 UC_TRANSPORT=quic RATES=20,40 INFLIGHT=1,4 bash uc_autobench/scripts/run-uc-Nnode.sh
N=3 UC_TRANSPORT=udp  RATES=20,40 INFLIGHT=1,4 bash uc_autobench/scripts/run-uc-Nnode.sh
```
Expected: both elect a leader, drive the ladder, emit CSV under `bench-out/`. No leaked node processes.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(bench): UC_TRANSPORT knob + arbitrary-N run-uc-Nnode.sh"
```

---

### Task 18: `transport` knob in bench-infra Ansible

**Files:**
- Modify: `bench-infra/ansible/group_vars/all.yml`, the role/template that launches UC nodes, and `bench-infra/ansible/bench.yml`

- [ ] **Step 1: Add the knob**

In `group_vars/all.yml` add `transport: quic` (default). In the launch template that sets `UC_DURABILITY`/`UC_MAX_PAYLOAD_ENTRIES`, add `UC_TRANSPORT={{ transport }}`. Add a `netem` block: optional `loss_pct`/`delay_ms` applied via `tc qdisc add dev <iface> root netem loss {{loss_pct}}% delay {{delay_ms}}ms` in a pre-bench task, removed in a post-bench task.

- [ ] **Step 2: Lint the playbook**

Run: `cd bench-infra/ansible && ansible-lint bench.yml || true` (advisory; fix obvious issues).

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat(bench-infra): transport + netem (loss/delay) knobs"
```

---

# PHASE F — Inter-node RPC microbench + A/B writeup

### Task 19: `internode-rpc-bench` — isolate the transport RPC path

**Files:**
- Create: `uc_autobench/src/bin/internode-rpc-bench.rs`
- Modify: `uc_autobench/Cargo.toml` (`[[bin]]` entry if needed)

**Interfaces:**
- Consumes: `uc_node`'s public transport surface. Since the mux/RaftNetwork are `pub(crate)`, the microbench drives the **public** path: it stands up two `uc_node` instances (no service, no client) wired only for replication and measures AppendEntries round-trips — OR, simpler and more isolated, exposes a tiny `#[doc(hidden)] pub` benchmark shim in `uc_node::network` that constructs a `UdpMux`/QUIC endpoint pair and echoes `Frame`s. **Chosen approach:** add `#[doc(hidden)] pub mod bench_support` to `uc_node::network` exposing `udp_echo_pair()` and `quic_echo_pair()` returning a client handle with `async fn rpc(body: Bytes) -> Bytes`, so the microbench measures pure transport RPC latency/throughput without consensus.

- [ ] **Step 1: Add `bench_support` shim to `uc_node`**

In `network/mod.rs`:

```rust
#[doc(hidden)]
pub mod bench_support {
    //! Public shim for the inter-node transport microbench (uc_autobench).
    //! Stands up a single echo server + client for QUIC and UDP so the bench
    //! measures pure transport RPC cost (no consensus, no journal).
    // udp_echo_pair() -> (ClientHandle, ServerHandle) where ClientHandle::rpc(Bytes)->Bytes
    // quic_echo_pair() -> same, over QUIC.
}
```

Implement `udp_echo_pair()` using `UdpMux::bind` + `set_request_handler` echo; `quic_echo_pair()` using `Endpoint::server`/`client` + the existing `Frame` round-trip. Return a small `pub struct EchoClient { /* mux or endpoint */ }` with `pub async fn rpc(&self, body: Bytes) -> Result<Bytes, NetworkError>`.

- [ ] **Step 2: Write the microbench driver**

`internode-rpc-bench.rs`: `clap` args `--transport quic|udp`, `--payload <bytes>`, `--inflight <n>`, `--rate <per_s>`, `--duration <s>`. Open-loop, coordinated-omission-free (reuse the `run_step`/`next_send` pattern from `commit-path-load.rs`), record an HDR histogram, emit the **same 13-column CSV** task13 uses (`system=udp-rpc|quic-rpc, config, workload=rpc-echo, payload_bytes, inflight, target_rate, achieved_rate, p50, p99, p99_9, p99_99, max, count`).

- [ ] **Step 3: Run both transports**

Run:
```bash
cargo run -p uc_autobench --bin internode-rpc-bench --release -- --transport quic --payload 64 --rate 20000 --inflight 8 --duration 5
cargo run -p uc_autobench --bin internode-rpc-bench --release -- --transport udp  --payload 64 --rate 20000 --inflight 8 --duration 5
```
Expected: both emit CSV rows; UDP and QUIC latency curves comparable on loopback.

- [ ] **Step 4: Commit**

```bash
cargo fmt && git add -A && git commit -m "feat(autobench): internode-rpc-bench — isolate transport RPC path (QUIC vs UDP A/B)"
```

---

### Task 20: Consolidate into `docs/tasks/task16_inter_node_udp_transport.md` (A/B writeup)

**Files:**
- Create: `docs/tasks/task16_inter_node_udp_transport.md`

- [ ] **Step 1: Run the full A/B sweep and capture numbers**

Run the microbench (Task 19) and the N-node end-to-end ladder (Task 17) for both transports, on loopback and (if available) two hosts via bench-infra with/without netem loss. Save CSVs under `bench-out/`.

- [ ] **Step 2: Write the canonical task doc**

Following task13's structure, document: the transport seam (`ClusterTransport`), the UDP channel design (frames, flat seq, NAK, flow control, fragmentation) with the Aeron mapping + what was dropped, the correctness story (lincheck/partition/hard-crash + loss injection, all green), and the **measured QUIC-vs-UDP decomposition** (microbench RPC latency/throughput + end-to-end ladder, loopback and cross-host, ± injected loss). State the verdict: which transport wins on which axis, and the recommendation for the default. Fold in the essential design rationale so the doc stands alone (per CLAUDE.md). Reference the spec + this plan as retained scaffolding.

- [ ] **Step 3: Run the whole suite once more, green**

Run: `cargo test -p uc_node && cargo test -p uc_node --features fault-injection -- --test-threads=1 && cargo clippy --workspace -- -D warnings`
Expected: all green, zero warnings.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "docs(task16): consolidate inter-node UDP transport — design, correctness, QUIC-vs-UDP A/B"
```

---

## Self-Review

**Spec coverage:**
- §1 goal / 3 targets → Target 1 (UDP transport): Phases A–D; Target 2 (multi-node harness): Phase E (Tasks 17–18); Target 3 (inter-node autobench): Phase F (Task 19). ✓
- §2 Aeron port (NAK/flow-control/fragmentation) → Tasks 5–9. Dropped mechanisms (term rotation, multicast, CC, SETUP geometry) → not implemented, documented in Task 20 + spec §2. ✓
- §3 transport seam (trait, future-proof) → Tasks 1–3. ✓
- §4 channel (frames, flat seq, NAK, flow control, fragmentation, handshake/liveness) → Tasks 4–9. Handshake/HELLO + ticker → Task 9 Steps note + Step 6. ✓
- §5 RaftNetwork + server → Tasks 10–12. ✓
- §6 correctness + fault injection (both layers) → Tasks 14–16 (in-process) + Task 18 (netem). ✓
- §7 bench + A/B → Tasks 17–20. ✓
- §8 phasing A–F → mirrored exactly. ✓
- §9 non-goals (CC/multicast/encryption/kernel-bypass) → respected; seam leaves room (Task 3). ✓

**Placeholder scan:** No "TBD/implement later". The few `> Implementer note:` blocks resolve real design ambiguities (async-vs-sync trait methods, inbound-receiver ownership, handler-set race) with a stated canonical choice — not deferred work.

**Type consistency:** `Segment`/`SegType`/`FLAG_*`/`SEG_HEADER_LEN` consistent across Tasks 4–9. `UdpTuning` fields consistent (Task 2 ↔ used in 8/9/12/13). `ClusterTransport` made `async` in Task 12 — Task 3's QUIC impl must be updated to `async fn` at that point (called out in Task 12 Step 2 note). `UdpMux` surface (`bind`/`open_session`/`rpc`/`set_request_handler`/`local_addr`) consistent across 9–12. `set_request_handler` changed from async to sync (`parking_lot::Mutex`) in Task 12 Step 1 — the canonical final form; Task 9's `Mutex<Option<Handler>>` is updated there. `UdpServerHandle` stub (Task 3) → real (Task 12), same `shutdown`/`local_addr` surface. ✓

**Known forward-reference resolved inline:** Task 3 creates a `UdpServerHandle` stub so Phase A compiles; Task 12 deletes the stub and provides the real one with the identical public surface.
