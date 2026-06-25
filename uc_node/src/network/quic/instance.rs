//! `RaftNetwork` impl using a lazily-connected `PeerConn` from a shared pool.
//!
//! Each RPC method follows the same shape:
//!   1. Encode the request body.
//!   2. `get_or_connect()` — fetch the cached `PeerConn` or open a new one.
//!   3. Issue the request on a fresh bi-stream.
//!   4. Decode the response, OR on error: `evict()` and surface RPCError so
//!      openraft can retry; the next attempt lazy-connects again.

use std::net::SocketAddr;
use std::sync::Arc;

use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft_legacy::network_v1::RaftNetwork;
use quinn::Endpoint;
use rustls::ClientConfig;

use super::super::frame::MessageType;
use super::super::{NetworkError, codec};
use super::client::PeerConn;
use super::factory::PeerPool;
use crate::raft::{NodeId, TypeConfig};

#[derive(Clone)]
pub struct QuicRaftNetwork {
    target: NodeId,
    peer_addr: SocketAddr,
    endpoint: Endpoint,
    client_cfg: Arc<ClientConfig>,
    pool: PeerPool,
    app_id: String,
    #[cfg(feature = "fault-injection")]
    source: NodeId,
    #[cfg(feature = "fault-injection")]
    fault_table: Option<Arc<super::super::fault::FaultTable>>,
}

impl QuicRaftNetwork {
    pub(crate) fn new(
        target: NodeId,
        peer_addr: SocketAddr,
        endpoint: Endpoint,
        client_cfg: Arc<ClientConfig>,
        pool: PeerPool,
        app_id: String,
    ) -> Self {
        Self {
            target,
            peer_addr,
            endpoint,
            client_cfg,
            pool,
            app_id,
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
        fault_table: Option<Arc<super::super::fault::FaultTable>>,
    ) -> Self {
        self.source = source;
        self.fault_table = fault_table;
        self
    }

    /// Get the cached connection or establish a new one.
    async fn get_or_connect(&self) -> Result<PeerConn, NetworkError> {
        // Fast path: cached.
        {
            let pool = self.pool.lock().await;
            if let Some(conn) = pool.get(&self.target) {
                return Ok(conn.clone());
            }
        }
        // Slow path: connect using the shared endpoint, then cache.
        // Note: under contention two concurrent callers may both race to
        // connect; the second insert simply replaces the first. The losing
        // PeerConn is dropped (its quinn::Connection closes gracefully).
        let conn = PeerConn::connect(
            &self.endpoint,
            self.client_cfg.clone(),
            self.peer_addr,
            &self.app_id,
        )
        .await?;
        let mut pool = self.pool.lock().await;
        pool.insert(self.target, conn.clone());
        Ok(conn)
    }

    /// Drop a stale connection from the pool. Called after a request fails so
    /// the next attempt opens a fresh connection.
    ///
    /// Compare-and-evict: only remove the entry if it still points at the
    /// same underlying `PeerConnInner` as the failed connection. If another
    /// concurrent caller already evicted and reconnected, the pool now holds
    /// a fresh connection that we must not drop.
    async fn evict(&self, failed: &PeerConn) {
        let mut pool = self.pool.lock().await;
        if let Some(current) = pool.get(&self.target)
            && Arc::ptr_eq(&current.inner, &failed.inner)
        {
            pool.remove(&self.target);
        }
    }
}

/// Map a transport-level [`NetworkError`] to openraft's [`RPCError`].
fn rpc_err<E: std::error::Error>(
    e: NetworkError,
) -> RPCError<TypeConfig, RaftError<TypeConfig, E>> {
    RPCError::Network(openraft::error::NetworkError::new(&e))
}

#[cfg(test)]
impl QuicRaftNetwork {
    /// Returns the shared `PeerPool` Arc (for Clone-sharing assertions).
    pub(crate) fn pool_arc(&self) -> &super::factory::PeerPool {
        &self.pool
    }

}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use super::QuicRaftNetwork;
    use crate::network::quic::factory::PeerPool;

    fn make_quic_raft_network() -> QuicRaftNetwork {
        // Build a minimal client endpoint; no connections are opened in these tests.
        let endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap())
            .expect("client endpoint");
        let pool: PeerPool = Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
        let peer_addr: SocketAddr = "127.0.0.1:19998".parse().unwrap();
        // Use a placeholder rustls client config (no actual TLS handshake needed).
        let root_store = rustls::RootCertStore::empty();
        let tls_cfg = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth(),
        );
        QuicRaftNetwork::new(1, peer_addr, endpoint, tls_cfg, pool, "test-app".to_string())
    }

    #[tokio::test]
    async fn quic_raft_network_clone_shares_pool() {
        let net = make_quic_raft_network();
        let net2 = net.clone();
        assert!(
            Arc::ptr_eq(net.pool_arc(), net2.pool_arc()),
            "clone must share the same underlying PeerPool Arc"
        );
    }
}

impl RaftNetwork<TypeConfig> for QuicRaftNetwork {
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
        // Leader-observed replication-RPC timing (floor-decomposition probe): bracket
        // the whole RPC (encode + connect + send + wire×2 + follower processing +
        // decode) on a single clock. Exclude empty heartbeats so only real
        // replication round-trips are sampled. No-op without `uc-bench-probes`.
        let has_entries = !rpc.entries.is_empty();
        let rpc_start = std::time::Instant::now();
        let body = codec::encode_append_entries_req(&rpc).map_err(rpc_err)?;
        let timeout = option.hard_ttl();
        let conn = self.get_or_connect().await.map_err(rpc_err)?;
        match conn
            .request(
                MessageType::AppendEntriesReq,
                body,
                MessageType::AppendEntriesResp,
                timeout,
            )
            .await
        {
            Ok(resp_body) => {
                let resp = codec::decode_append_entries_resp(&resp_body).map_err(rpc_err)?;
                if has_entries {
                    uc_protocol::probes::record_repl_rpc_ns(rpc_start.elapsed().as_nanos() as u64);
                }
                Ok(resp)
            }
            Err(e) => {
                self.evict(&conn).await;
                Err(rpc_err(e))
            }
        }
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
        let timeout = option.hard_ttl();
        let conn = self.get_or_connect().await.map_err(rpc_err)?;
        match conn
            .request(
                MessageType::InstallSnapshotReq,
                body,
                MessageType::InstallSnapshotResp,
                timeout,
            )
            .await
        {
            Ok(resp_body) => codec::decode_install_snapshot_resp(&resp_body).map_err(rpc_err),
            Err(e) => {
                self.evict(&conn).await;
                Err(rpc_err(e))
            }
        }
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
        let timeout = option.hard_ttl();
        let conn = self.get_or_connect().await.map_err(rpc_err)?;
        match conn
            .request(MessageType::VoteReq, body, MessageType::VoteResp, timeout)
            .await
        {
            Ok(resp_body) => codec::decode_vote_resp(&resp_body).map_err(rpc_err),
            Err(e) => {
                self.evict(&conn).await;
                Err(rpc_err(e))
            }
        }
    }
}
