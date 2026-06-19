//! `RaftNetworkFactory` over the shared UDP mux.
//!
//! Mints `UdpRaftNetwork` instances (Task 10) for each peer. The shared
//! `request_id` counter is created once here and cloned into every minted
//! client so that request IDs are globally unique across all peers — no
//! per-peer rollover confusion at the mux correlation layer.
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use openraft::network::RaftNetworkFactory;

use super::instance::UdpRaftNetwork;
use super::mux::UdpMux;
use crate::network::{pipeline_depth, PipelinedNet};
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
        self.fault_table = fault_table.clone();
        // Thread the fault config into the mux so the recv loop can drop/delay
        // inbound segments (exercising NAK/retransmit, not just whole-RPC fail).
        self.mux.set_fault_injection(source, fault_table);
    }
}

impl RaftNetworkFactory<TypeConfig> for UdpRaftNetworkFactory {
    /// `PipelinedNet` wraps our V1 `UdpRaftNetwork`, satisfying the
    /// `RaftNetworkV2` bound that `RaftNetworkFactory::Network` requires in
    /// openraft 0.10 while overriding `stream_append` with a bounded in-order
    /// pipeline (see [`crate::network::PipelinedNet`]).
    type Network = PipelinedNet<UdpRaftNetwork>;

    async fn new_client(&mut self, target: NodeId, node: &NodeAddr) -> Self::Network {
        let net = UdpRaftNetwork::new(
            target,
            node.raft_addr,
            self.mux.clone(),
            self.app_id.clone(),
            self.request_id.clone(),
        );
        // Register peer addr → src NodeId so the mux recv loop can key inbound
        // segments from this peer against the NodeId-keyed fault table.
        #[cfg(feature = "fault-injection")]
        self.mux.register_peer(node.raft_addr, target);
        #[cfg(feature = "fault-injection")]
        let net = net.with_fault(self.source, self.fault_table.clone());
        PipelinedNet::new(net, pipeline_depth())
    }
}
