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

/// Parse a pipeline depth from an optional string (e.g. from env).
///
/// - `None`          → `PIPELINE_DEPTH` (default 8)
/// - `Some("0")`     → 1 (clamped; zero is not useful)
/// - `Some("1")`     → 1
/// - `Some("bad")`   → `PIPELINE_DEPTH` (fallback on parse error)
fn parse_pipeline_depth(s: Option<&str>) -> usize {
    match s {
        None => PIPELINE_DEPTH,
        Some(v) => v.parse::<usize>().unwrap_or(PIPELINE_DEPTH).max(1),
    }
}

/// Runtime-configurable pipeline depth.
///
/// Reads `UC_PIPELINE_DEPTH` from the environment; falls back to
/// [`PIPELINE_DEPTH`] on missing or non-numeric values. Always returns at
/// least 1.
pub(crate) fn pipeline_depth() -> usize {
    parse_pipeline_depth(std::env::var("UC_PIPELINE_DEPTH").ok().as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pipeline_depth_none_returns_default() {
        assert_eq!(parse_pipeline_depth(None), PIPELINE_DEPTH);
    }

    #[test]
    fn parse_pipeline_depth_one() {
        assert_eq!(parse_pipeline_depth(Some("1")), 1);
    }

    #[test]
    fn parse_pipeline_depth_zero_clamped_to_one() {
        assert_eq!(parse_pipeline_depth(Some("0")), 1);
    }

    #[test]
    fn parse_pipeline_depth_garbage_returns_default() {
        assert_eq!(parse_pipeline_depth(Some("garbage")), PIPELINE_DEPTH);
    }
}

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
