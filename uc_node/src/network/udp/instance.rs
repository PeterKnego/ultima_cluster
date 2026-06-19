//! `RaftNetwork` impl over the shared UDP mux. Mirrors quic/instance.rs; each
//! RPC encodes the body, issues mux.rpc() (request_id-correlated), decodes resp.
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft_legacy::network_v1::RaftNetwork;

use super::mux::UdpMux;
use crate::network::frame::{Frame, MessageType};
use crate::network::{NetworkError, codec};
use crate::raft::{NodeId, TypeConfig};

#[derive(Clone)]
pub struct UdpRaftNetwork {
    // Only read inside the fault-injection is_blocked check; genuinely unused
    // under default features.
    #[cfg_attr(not(feature = "fault-injection"), allow(dead_code))]
    target: NodeId,
    peer_addr: SocketAddr,
    mux: Arc<UdpMux>,
    app_id: String,
    request_id: Arc<AtomicU64>,
    #[cfg(feature = "fault-injection")]
    source: NodeId,
    #[cfg(feature = "fault-injection")]
    fault_table: Option<Arc<crate::network::fault::FaultTable>>,
}

impl UdpRaftNetwork {
    pub(crate) fn new(
        target: NodeId,
        peer_addr: SocketAddr,
        mux: Arc<UdpMux>,
        app_id: String,
        request_id: Arc<AtomicU64>,
    ) -> Self {
        Self {
            target,
            peer_addr,
            mux,
            app_id,
            request_id,
            #[cfg(feature = "fault-injection")]
            source: 0,
            #[cfg(feature = "fault-injection")]
            fault_table: None,
        }
    }

    #[cfg(feature = "fault-injection")]
    pub(crate) fn with_fault(
        mut self,
        source: NodeId,
        ft: Option<Arc<crate::network::fault::FaultTable>>,
    ) -> Self {
        self.source = source;
        self.fault_table = ft;
        self
    }

    async fn do_rpc(
        &self,
        req_type: MessageType,
        body: bytes::Bytes,
        resp_type: MessageType,
        timeout: Duration,
    ) -> Result<bytes::Bytes, NetworkError> {
        let rid = self.request_id.fetch_add(1, Ordering::Relaxed);
        let req = Frame::new_request(req_type, rid, body);
        let resp = self
            .mux
            .rpc(self.peer_addr, &self.app_id, req, timeout)
            .await?;
        if resp.msg_type != resp_type {
            return Err(NetworkError::Decode(format!(
                "expected {resp_type:?} got {:?}",
                resp.msg_type
            )));
        }
        Ok(resp.body)
    }
}

/// Map a transport-level [`NetworkError`] to openraft's [`RPCError`].
fn rpc_err<E: std::error::Error>(
    e: NetworkError,
) -> RPCError<TypeConfig, RaftError<TypeConfig, E>> {
    RPCError::Network(openraft::error::NetworkError::new(&e))
}

#[cfg(test)]
impl UdpRaftNetwork {
    /// Returns the shared `Arc<UdpMux>` handle (for Clone-sharing assertions).
    pub(crate) fn mux_arc(&self) -> &Arc<UdpMux> {
        &self.mux
    }

    /// Returns the shared `Arc<AtomicU64>` request-id counter (for Clone-sharing assertions).
    pub(crate) fn request_id_arc(&self) -> &Arc<AtomicU64> {
        &self.request_id
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use super::UdpRaftNetwork;
    use crate::network::udp::UdpTuning;
    use crate::network::udp::mux::UdpMux;

    async fn make_udp_raft_network() -> UdpRaftNetwork {
        let mux = UdpMux::bind("127.0.0.1:0".parse().unwrap(), UdpTuning::default())
            .await
            .expect("UdpMux::bind");
        let peer_addr: SocketAddr = "127.0.0.1:19999".parse().unwrap();
        let request_id = Arc::new(AtomicU64::new(1));
        UdpRaftNetwork::new(1, peer_addr, mux, "test-app".to_string(), request_id)
    }

    #[tokio::test]
    async fn udp_raft_network_clone_shares_mux() {
        let net = make_udp_raft_network().await;
        let net2 = net.clone();
        assert!(
            Arc::ptr_eq(net.mux_arc(), net2.mux_arc()),
            "clone must share the same underlying UdpMux Arc"
        );
    }

    #[tokio::test]
    async fn udp_raft_network_clone_shares_request_id() {
        let net = make_udp_raft_network().await;
        let net2 = net.clone();
        assert!(
            Arc::ptr_eq(net.request_id_arc(), net2.request_id_arc()),
            "clone must share the same Arc<AtomicU64> request-id counter"
        );
    }
}

impl RaftNetwork<TypeConfig> for UdpRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>>
    {
        #[cfg(feature = "fault-injection")]
        if let Some(t) = &self.fault_table
            && t.is_blocked(self.source, self.target)
        {
            return Err(rpc_err(NetworkError::Disconnected));
        }
        let body = codec::encode_append_entries_req(&rpc).map_err(rpc_err)?;
        let b = self
            .do_rpc(
                MessageType::AppendEntriesReq,
                body,
                MessageType::AppendEntriesResp,
                option.hard_ttl(),
            )
            .await
            .map_err(rpc_err)?;
        codec::decode_append_entries_resp(&b).map_err(rpc_err)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<TypeConfig>,
        RPCError<TypeConfig, RaftError<TypeConfig, InstallSnapshotError>>,
    > {
        #[cfg(feature = "fault-injection")]
        if let Some(t) = &self.fault_table
            && t.is_blocked(self.source, self.target)
        {
            return Err(rpc_err(NetworkError::Disconnected));
        }
        let body = codec::encode_install_snapshot_req(&rpc).map_err(rpc_err)?;
        let b = self
            .do_rpc(
                MessageType::InstallSnapshotReq,
                body,
                MessageType::InstallSnapshotResp,
                option.hard_ttl(),
            )
            .await
            .map_err(rpc_err)?;
        codec::decode_install_snapshot_resp(&b).map_err(rpc_err)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>> {
        #[cfg(feature = "fault-injection")]
        if let Some(t) = &self.fault_table
            && t.is_blocked(self.source, self.target)
        {
            return Err(rpc_err(NetworkError::Disconnected));
        }
        let body = codec::encode_vote_req(&rpc).map_err(rpc_err)?;
        let b = self
            .do_rpc(
                MessageType::VoteReq,
                body,
                MessageType::VoteResp,
                option.hard_ttl(),
            )
            .await
            .map_err(rpc_err)?;
        codec::decode_vote_resp(&b).map_err(rpc_err)
    }
}
