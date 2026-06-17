//! UDP inbound server: sets the mux request handler to dispatch request
//! `Frame`s into the local `Raft`. The dispatch body mirrors `quic/server.rs`.

use std::net::SocketAddr;
use std::sync::Arc;

use openraft::Raft;
use openraft::storage::RaftStateMachine;
// Extension trait — adds `install_snapshot` to `Raft<C, SM>`. Imported for its
// method, not its name.
use openraft_legacy::network_v1::ChunkedSnapshotReceiver as _;

use super::mux::UdpMux;
use crate::network::frame::{Frame, MessageType};
use crate::network::{NetworkError, codec};
use crate::raft::TypeConfig;

pub struct UdpServerHandle {
    mux: Arc<UdpMux>,
}

impl UdpServerHandle {
    pub async fn shutdown(self) {
        self.mux.shutdown();
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.mux.local_addr()
    }
}

/// Register the request handler that dispatches inbound RPC `Frame`s into the
/// local `Raft`. `set_request_handler` is sync (parking_lot), so there is no
/// set-race and no task spawn needed.
pub fn spawn_udp_server<SM>(
    mux: Arc<UdpMux>,
    raft: Raft<TypeConfig, SM>,
) -> Result<UdpServerHandle, NetworkError>
where
    SM: RaftStateMachine<TypeConfig>,
{
    let raft = Arc::new(raft);
    let handler = {
        let raft = raft.clone();
        Arc::new(move |req: Frame| {
            let raft = raft.clone();
            Box::pin(async move { dispatch(req, &raft).await })
                as futures::future::BoxFuture<'static, Frame>
        })
    };
    mux.set_request_handler(handler);
    Ok(UdpServerHandle { mux })
}

async fn dispatch<SM>(req: Frame, raft: &Raft<TypeConfig, SM>) -> Frame
where
    SM: RaftStateMachine<TypeConfig>,
{
    let request_id = req.request_id;
    let result: Result<Frame, NetworkError> = async {
        match req.msg_type {
            MessageType::AppendEntriesReq => {
                let decoded = codec::decode_append_entries_req(&req.body)?;
                let resp = raft
                    .append_entries(decoded)
                    .await
                    .map_err(|e| NetworkError::Stream(format!("append_entries: {e}")))?;
                Ok(Frame::new_response(
                    MessageType::AppendEntriesResp,
                    request_id,
                    codec::encode_append_entries_resp(&resp)?,
                ))
            }
            MessageType::VoteReq => {
                let decoded = codec::decode_vote_req(&req.body)?;
                let resp = raft
                    .vote(decoded)
                    .await
                    .map_err(|e| NetworkError::Stream(format!("vote: {e}")))?;
                Ok(Frame::new_response(
                    MessageType::VoteResp,
                    request_id,
                    codec::encode_vote_resp(&resp)?,
                ))
            }
            MessageType::InstallSnapshotReq => {
                let decoded = codec::decode_install_snapshot_req(&req.body)?;
                let resp = raft
                    .install_snapshot(decoded)
                    .await
                    .map_err(|e| NetworkError::Stream(format!("install_snapshot: {e}")))?;
                Ok(Frame::new_response(
                    MessageType::InstallSnapshotResp,
                    request_id,
                    codec::encode_install_snapshot_resp(&resp)?,
                ))
            }
            other => Err(NetworkError::Decode(format!(
                "server got non-request msg_type {other:?}"
            ))),
        }
    }
    .await;

    result.unwrap_or_else(|e| {
        // On dispatch error, return an empty response of a sentinel type so the
        // client's response-type check fails → it surfaces an RPCError →
        // openraft retries.
        tracing::warn!(error = ?e, "udp dispatch failed");
        Frame::new_response(MessageType::HandshakeAck, request_id, bytes::Bytes::new())
    })
}
