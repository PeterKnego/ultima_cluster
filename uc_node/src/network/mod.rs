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

// Re-exports added by Task 9:
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
