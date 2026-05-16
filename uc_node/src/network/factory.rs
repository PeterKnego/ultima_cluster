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
use openraft_legacy::network_v1::Adapter;
use openraft_legacy::network_v1::RaftNetwork as _;
use quinn::Endpoint;
use rustls::ClientConfig;
use tokio::sync::Mutex;

use super::client::PeerConn;
use super::instance::QuicRaftNetwork;
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
        }
    }

    /// Convenience constructor that binds a fresh client endpoint to 0.0.0.0:0.
    /// Primarily for tests / single-endpoint setups; production callers should
    /// pass a shared endpoint via [`Self::new`].
    pub fn new_with_default_endpoint(
        client_cfg: Arc<ClientConfig>,
        app_id: String,
    ) -> Result<Self, super::NetworkError> {
        let endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())
            .map_err(|e| super::NetworkError::Connect(format!("client endpoint: {e}")))?;
        Ok(Self::new(endpoint, client_cfg, app_id))
    }
}

impl RaftNetworkFactory<TypeConfig> for QuicRaftNetworkFactory {
    /// `Adapter` wraps our V1 `QuicRaftNetwork` to satisfy the `RaftNetworkV2`
    /// bound that `RaftNetworkFactory::Network` requires in openraft 0.10.
    type Network = Adapter<TypeConfig, QuicRaftNetwork>;

    async fn new_client(&mut self, target: NodeId, node: &NodeAddr) -> Self::Network {
        // Do NOT connect here. `RaftNetworkFactory::new_client` can't return a
        // Result; a failed connection at this point would force panic-or-Box-Err.
        // Defer connection to first request — `RaftNetwork::*` methods CAN
        // return Err, and openraft retries.
        QuicRaftNetwork::new(
            target,
            node.raft_addr,
            self.endpoint.clone(),
            self.client_cfg.clone(),
            self.pool.clone(),
            self.app_id.clone(),
        )
        .into_v2()
    }
}
