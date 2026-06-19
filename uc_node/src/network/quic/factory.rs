//! `RaftNetworkFactory` impl over QUIC.
//!
//! Holds a single shared client `Endpoint` (one UDP socket per process) and a
//! pool of established `PeerConn` instances keyed by `NodeId`. `new_client`
//! does NOT open a connection — `QuicRaftNetwork` lazily connects on first
//! request (the trait method can't return an error, so deferring is safer
//! than panic-or-Box-Err).

use std::collections::HashMap;
use std::sync::Arc;

use openraft::network::RaftNetworkFactory;
use quinn::Endpoint;
use rustls::ClientConfig;
use tokio::sync::Mutex;

use super::client::PeerConn;
use super::instance::QuicRaftNetwork;
use crate::network::{pipeline_depth, PipelinedNet};
use crate::raft::{NodeAddr, NodeId, TypeConfig};

/// Shared map of established peer connections, keyed by NodeId.
pub type PeerPool = Arc<Mutex<HashMap<NodeId, PeerConn>>>;

pub struct QuicRaftNetworkFactory {
    endpoint: Endpoint,
    client_cfg: Arc<ClientConfig>,
    pool: PeerPool,
    /// TLS SNI / `server_name` presented to peers. Treated as an opaque
    /// application identifier; must match the server cert's SAN.
    app_id: String,
    #[cfg(feature = "fault-injection")]
    source: NodeId,
    #[cfg(feature = "fault-injection")]
    fault_table: Option<Arc<super::super::fault::FaultTable>>,
}

impl QuicRaftNetworkFactory {
    /// Build a factory. Caller provides the shared client `Endpoint`
    /// (typically bound to `0.0.0.0:0`) and the rustls client config.
    pub fn new(endpoint: Endpoint, client_cfg: Arc<ClientConfig>, app_id: String) -> Self {
        Self {
            endpoint,
            client_cfg,
            pool: Arc::new(Mutex::new(HashMap::new())),
            app_id,
            #[cfg(feature = "fault-injection")]
            source: 0,
            #[cfg(feature = "fault-injection")]
            fault_table: None,
        }
    }

    /// Convenience constructor that binds a fresh client endpoint to 0.0.0.0:0.
    /// Primarily for tests / single-endpoint setups; production callers should
    /// pass a shared endpoint via [`Self::new`].
    pub fn new_with_default_endpoint(
        client_cfg: Arc<ClientConfig>,
        app_id: String,
    ) -> Result<Self, super::super::NetworkError> {
        let endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())
            .map_err(|e| super::super::NetworkError::Connect(format!("client endpoint: {e}")))?;
        Ok(Self::new(endpoint, client_cfg, app_id))
    }

    #[cfg(feature = "fault-injection")]
    pub fn set_fault_injection(
        &mut self,
        source: NodeId,
        fault_table: Option<Arc<super::super::fault::FaultTable>>,
    ) {
        self.source = source;
        self.fault_table = fault_table;
    }
}

impl RaftNetworkFactory<TypeConfig> for QuicRaftNetworkFactory {
    /// `PipelinedNet` wraps our V1 `QuicRaftNetwork`, satisfying the
    /// `RaftNetworkV2` bound that `RaftNetworkFactory::Network` requires in
    /// openraft 0.10 while overriding `stream_append` with a bounded in-order
    /// pipeline (see [`crate::network::PipelinedNet`]).
    type Network = PipelinedNet<QuicRaftNetwork>;

    async fn new_client(&mut self, target: NodeId, node: &NodeAddr) -> Self::Network {
        // Do NOT connect here. `RaftNetworkFactory::new_client` can't return a
        // Result; a failed connection at this point would force panic-or-Box-Err.
        // Defer connection to first request — `RaftNetwork::*` methods CAN
        // return Err, and openraft retries.
        let net = QuicRaftNetwork::new(
            target,
            node.raft_addr,
            self.endpoint.clone(),
            self.client_cfg.clone(),
            self.pool.clone(),
            self.app_id.clone(),
        );
        #[cfg(feature = "fault-injection")]
        let net = net.with_fault(self.source, self.fault_table.clone());
        PipelinedNet::new(net, pipeline_depth())
    }
}
