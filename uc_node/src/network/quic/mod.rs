//! QUIC inter-node transport (the default). One persistent connection per
//! peer-pair, a fresh bidirectional stream per RPC, TLS self-signed by default.

pub mod client;
pub mod factory;
pub mod instance;
pub mod server;
pub mod tls;
pub mod transport_impl;

pub use factory::QuicRaftNetworkFactory;
pub use instance::QuicRaftNetwork;
pub use server::{ServerHandle, spawn_server};
pub use transport_impl::QuicTransport;
