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
use openraft::network::{RPCOption, RaftNetwork};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use quinn::Endpoint;
use rustls::ClientConfig;

use super::client::PeerConn;
use super::factory::PeerPool;
use super::frame::MessageType;
use super::{codec, NetworkError};
use crate::raft::{NodeAddr, NodeId, TypeConfig};

pub struct QuicRaftNetwork {
    target: NodeId,
    peer_addr: SocketAddr,
    endpoint: Endpoint,
    client_cfg: Arc<ClientConfig>,
    pool: PeerPool,
    app_id: String,
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
        }
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
fn rpc_err<E>(e: NetworkError) -> RPCError<NodeId, NodeAddr, RaftError<NodeId, E>>
where
    E: std::error::Error,
{
    RPCError::Network(openraft::error::NetworkError::new(&e))
}

impl RaftNetwork<TypeConfig> for QuicRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, NodeAddr, RaftError<NodeId>>> {
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
            Ok(resp_body) => codec::decode_append_entries_resp(&resp_body).map_err(rpc_err),
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
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, NodeAddr, RaftError<NodeId, InstallSnapshotError>>,
    > {
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
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, NodeAddr, RaftError<NodeId>>> {
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
