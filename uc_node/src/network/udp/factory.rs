//! `RaftNetworkFactory` over the shared UDP mux.
//!
//! Mints `UdpRaftNetwork` instances (Task 10) for each peer. The shared
//! `request_id` counter is created once here and cloned into every minted
//! client so that request IDs are globally unique across all peers — no
//! per-peer rollover confusion at the mux correlation layer.
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use openraft::network::RaftNetworkFactory;
use openraft_legacy::network_v1::Adapter;
use openraft_legacy::network_v1::RaftNetwork as _;

use super::instance::UdpRaftNetwork;
use super::mux::UdpMux;
use crate::raft::{NodeAddr, NodeId, TypeConfig};

pub struct UdpRaftNetworkFactory {
    mux: Arc<UdpMux>,
    app_id: String,
    /// Node-wide request-ID counter — shared across all minted clients so IDs
    /// are globally unique (no per-peer wrap-around collision at the mux).
    request_id: Arc<AtomicU64>,
    #[cfg(feature = "fault-injection")]
    source: NodeId,
    #[cfg(feature = "fault-injection")]
    fault_table: Option<Arc<crate::network::fault::FaultTable>>,
}

impl UdpRaftNetworkFactory {
    pub fn new(mux: Arc<UdpMux>, app_id: String) -> Self {
        Self {
            mux,
            app_id,
            request_id: Arc::new(AtomicU64::new(1)),
            #[cfg(feature = "fault-injection")]
            source: 0,
            #[cfg(feature = "fault-injection")]
            fault_table: None,
        }
    }

    #[cfg(feature = "fault-injection")]
    pub fn set_fault_injection(
        &mut self,
        source: NodeId,
        fault_table: Option<Arc<crate::network::fault::FaultTable>>,
    ) {
        self.source = source;
        self.fault_table = fault_table;
    }
}

impl RaftNetworkFactory<TypeConfig> for UdpRaftNetworkFactory {
    /// `Adapter` wraps our V1 `UdpRaftNetwork` to satisfy the `RaftNetworkV2`
    /// bound that `RaftNetworkFactory::Network` requires in openraft 0.10.
    type Network = Adapter<TypeConfig, UdpRaftNetwork>;

    async fn new_client(&mut self, target: NodeId, node: &NodeAddr) -> Self::Network {
        let net = UdpRaftNetwork::new(
            target,
            node.raft_addr,
            self.mux.clone(),
            self.app_id.clone(),
            self.request_id.clone(),
        );
        #[cfg(feature = "fault-injection")]
        let net = net.with_fault(self.source, self.fault_table.clone());
        net.into_v2()
    }
}
