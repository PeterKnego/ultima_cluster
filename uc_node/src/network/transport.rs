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
#[cfg(feature = "fault-injection")]
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
///
/// The `async fn` methods are intentionally not `+ Send`-desugared: this trait
/// is internal to `uc_node` (only `QuicTransport`/`UdpTransport` impl it and
/// only the builder calls it on the runtime thread), so the auto-trait
/// flexibility the lint warns about is not needed here.
#[allow(async_fn_in_trait)]
pub trait ClusterTransport {
    type Factory: RaftNetworkFactory<TypeConfig>;
    type Server;

    async fn build_factory(&self, ctx: &TransportCtx) -> Result<Self::Factory, NetworkError>;

    async fn spawn_server<SM>(
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
