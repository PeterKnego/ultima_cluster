//! Inter-node transport for ultima_cluster.
//!
//! The default transport is QUIC (`quic/`). One persistent connection per
//! peer-pair, multiple bidirectional streams per connection (one per RPC
//! class). TLS self-signed by default.

pub mod codec;
#[cfg(feature = "fault-injection")]
pub mod fault;
pub mod frame;
pub mod quic;
pub mod udp;

// Top-level re-exports so existing call-sites keep working unchanged.
pub use quic::{QuicRaftNetwork, QuicRaftNetworkFactory};
pub use udp::UdpTuning;

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
