//! QUIC listener that accepts inbound RPCs and dispatches them to the local
//! `Raft<TypeConfig>` instance.

use std::net::SocketAddr;
use std::sync::Arc;

use openraft::Raft;
use openraft::storage::RaftStateMachine;
// Extension trait — adds `install_snapshot` to `Raft<C, SM>`. Imported for its
// method, not its name.
use openraft_legacy::network_v1::ChunkedSnapshotReceiver as _;
use quinn::{Endpoint, ServerConfig as QuicServerConfig};
use tokio::task::JoinHandle;

use super::frame::{Frame, MessageType};
use super::{NetworkError, codec};
use crate::raft::TypeConfig;

pub struct ServerHandle {
    endpoint: Endpoint,
    accept_task: JoinHandle<()>,
}

impl ServerHandle {
    pub async fn shutdown(self) {
        self.endpoint.close(0u32.into(), b"shutdown");
        let _ = self.accept_task.await;
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.endpoint.local_addr()
    }
}

pub fn spawn_server<SM>(
    listen_addr: SocketAddr,
    rustls_server_cfg: Arc<rustls::ServerConfig>,
    raft: Raft<TypeConfig, SM>,
) -> Result<ServerHandle, NetworkError>
where
    SM: RaftStateMachine<TypeConfig>,
{
    let crypto =
        quinn::crypto::rustls::QuicServerConfig::try_from(rustls_server_cfg.as_ref().clone())
            .map_err(|e| NetworkError::Tls(format!("quic server cfg: {e}")))?;
    let server_cfg = QuicServerConfig::with_crypto(Arc::new(crypto));
    let endpoint = Endpoint::server(server_cfg, listen_addr)
        .map_err(|e| NetworkError::Connect(format!("endpoint: {e}")))?;

    let endpoint_clone = endpoint.clone();
    let accept_task = tokio::spawn(async move {
        while let Some(conn) = endpoint_clone.accept().await {
            let raft = raft.clone();
            tokio::spawn(async move {
                match conn.await {
                    Ok(conn) => handle_connection(conn, raft).await,
                    Err(e) => tracing::warn!(error = %e, "quic accept failed"),
                }
            });
        }
    });

    Ok(ServerHandle {
        endpoint,
        accept_task,
    })
}

async fn handle_connection<SM>(conn: quinn::Connection, raft: Raft<TypeConfig, SM>)
where
    SM: RaftStateMachine<TypeConfig>,
{
    loop {
        match conn.accept_bi().await {
            Ok((mut send, mut recv)) => {
                let raft = raft.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_stream(&mut send, &mut recv, raft).await {
                        tracing::warn!(error = ?e, "quic stream handler failed");
                    }
                });
            }
            Err(quinn::ConnectionError::ApplicationClosed(_))
            | Err(quinn::ConnectionError::ConnectionClosed(_))
            | Err(quinn::ConnectionError::LocallyClosed) => break,
            Err(e) => {
                tracing::warn!(error = ?e, "quic accept_bi failed");
                break;
            }
        }
    }
}

async fn handle_stream<SM>(
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    raft: Raft<TypeConfig, SM>,
) -> Result<(), NetworkError>
where
    SM: RaftStateMachine<TypeConfig>,
{
    loop {
        let request = match Frame::read_async(recv).await {
            Ok(f) => f,
            Err(NetworkError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        };

        let response = dispatch(request, &raft).await?;
        let encoded = response.encode().freeze();
        send.write_all(&encoded)
            .await
            .map_err(|e| NetworkError::Stream(format!("write: {e}")))?;
    }
    Ok(())
}

async fn dispatch<SM>(req: Frame, raft: &Raft<TypeConfig, SM>) -> Result<Frame, NetworkError>
where
    SM: RaftStateMachine<TypeConfig>,
{
    let request_id = req.request_id;
    match req.msg_type {
        MessageType::AppendEntriesReq => {
            let decoded = codec::decode_append_entries_req(&req.body)?;
            let resp = raft
                .append_entries(decoded)
                .await
                .map_err(|e| NetworkError::Stream(format!("append_entries: {e}")))?;
            let body = codec::encode_append_entries_resp(&resp)?;
            Ok(Frame::new_response(
                MessageType::AppendEntriesResp,
                request_id,
                body,
            ))
        }
        MessageType::VoteReq => {
            let decoded = codec::decode_vote_req(&req.body)?;
            let resp = raft
                .vote(decoded)
                .await
                .map_err(|e| NetworkError::Stream(format!("vote: {e}")))?;
            let body = codec::encode_vote_resp(&resp)?;
            Ok(Frame::new_response(MessageType::VoteResp, request_id, body))
        }
        MessageType::InstallSnapshotReq => {
            let decoded = codec::decode_install_snapshot_req(&req.body)?;
            let resp = raft
                .install_snapshot(decoded)
                .await
                .map_err(|e| NetworkError::Stream(format!("install_snapshot: {e}")))?;
            let body = codec::encode_install_snapshot_resp(&resp)?;
            Ok(Frame::new_response(
                MessageType::InstallSnapshotResp,
                request_id,
                body,
            ))
        }
        other => Err(NetworkError::Decode(format!(
            "server got non-request msg_type {other:?}"
        ))),
    }
}
