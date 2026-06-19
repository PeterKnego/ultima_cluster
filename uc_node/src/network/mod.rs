//! Inter-node transport for ultima_cluster.
//!
//! The default transport is QUIC (`quic/`). One persistent connection per
//! peer-pair, multiple bidirectional streams per connection (one per RPC
//! class). TLS self-signed by default.

/// Public shim for the inter-node transport microbench (`uc_autobench`).
/// Exposes uniform echo-RPC pairs for QUIC and UDP so the bench can measure
/// pure transport RPC cost. Test/bench-only surface.
#[doc(hidden)]
pub mod bench_support;
pub mod codec;
#[cfg(feature = "fault-injection")]
pub mod fault;
pub mod frame;
pub mod pipelined;
pub mod quic;
pub mod transport;
pub mod udp;

// Top-level re-exports so existing call-sites keep working unchanged.
pub use quic::{QuicRaftNetwork, QuicRaftNetworkFactory};
pub use transport::{ClusterTransport, TransportCtx, TransportServer};
pub use pipelined::PipelinedNet;
pub use udp::UdpTuning;

use thiserror::Error;

/// Default look-ahead depth for the pipelined `RaftNetworkV2::stream_append`
/// override ([`PipelinedNet`]). The leader keeps up to this many
/// AppendEntries RPCs in flight to a single peer at once (ordered), rather
/// than the openraft default of strictly one-at-a-time. Bounded so a slow or
/// lossy peer cannot make the leader buffer unboundedly.
pub const PIPELINE_DEPTH: usize = 8;

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
