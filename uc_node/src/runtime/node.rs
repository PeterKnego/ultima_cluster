//! Public NodeHandle returned by NodeBuilder::start().
//!
//! M1 is generic over S; M3 will introduce a non-generic shmem-fronted handle
//! that can be embedded in a process boundary alongside a separate state-machine
//! worker. The shape (submit / current_leader / node_id / shutdown) stays the
//! same.

use bytes::Bytes;
use openraft::error::{ClientWriteError, RaftError};
use openraft::Raft;

use uc_service::StateMachine;

use crate::config::{NodeConfig, NodeId};
use crate::raft::state_machine::AdaptedStateMachine;
use crate::raft::TypeConfig;
use crate::ClusterError;

/// Public handle returned by [`NodeBuilder::start`](super::builder::NodeBuilder::start).
pub struct NodeHandle<S: StateMachine> {
    pub(crate) raft: Raft<TypeConfig>,
    pub(crate) config: NodeConfig,
    /// Cloned handle to the user state-machine adapter. The Raft engine owns
    /// another clone internally; both share the same `Arc<Mutex<Inner<S>>>`.
    /// Used by `query_snapshot` to reach the user SM directly without going
    /// through Raft.
    pub(crate) sm: AdaptedStateMachine<S>,
}

impl<S: StateMachine> NodeHandle<S> {
    pub fn node_id(&self) -> NodeId {
        self.config.node_id
    }

    pub async fn current_leader(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }

    /// Embedded-mode submit: bincode-encode the command, push it through
    /// openraft's `client_write`, await the typed response.
    ///
    /// On `ForwardToLeader` we surface [`ClusterError::NotLeader`] with the
    /// leader hint extracted from the openraft error. All other Raft / Fatal
    /// errors are stringified into [`ClusterError::Raft`].
    pub async fn submit(&self, cmd: S::Command) -> Result<S::Response, ClusterError> {
        let bytes = bincode::serde::encode_to_vec(&cmd, bincode::config::standard())?;
        let app_command: Bytes = Bytes::from(bytes);

        let result = self.raft.client_write(app_command).await.map_err(map_client_write_error)?;

        // result.data is the bincode-encoded S::Response (per TypeConfig: R = Bytes).
        let resp_bytes: Bytes = result.data;
        let (resp, _) =
            bincode::serde::decode_from_slice::<S::Response, _>(&resp_bytes, bincode::config::standard())?;
        Ok(resp)
    }

    /// Embedded-mode snapshot read: run a closure against the applied state.
    /// Holds the same Mutex that `apply` takes, so it sees a consistent
    /// view (no torn state across multiple reads inside the closure).
    /// Returns the closure's value.
    ///
    /// M1 only — M3 introduces typed Query types over a shmem ring.
    pub async fn query_snapshot<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&S) -> R + Send,
        R: Send,
    {
        self.sm.with_state(f).await
    }

    pub async fn shutdown(self) -> Result<(), ClusterError> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| ClusterError::Raft(format!("shutdown: {e}")))?;
        Ok(())
    }
}

/// Map openraft 0.9.24's `RaftError<NodeId, ClientWriteError<NodeId, NodeAddr>>`
/// into our `ClusterError`. ForwardToLeader yields `NotLeader { leader_id }`;
/// everything else stringifies into `Raft(_)`.
fn map_client_write_error(
    e: RaftError<NodeId, ClientWriteError<NodeId, crate::raft::NodeAddr>>,
) -> ClusterError {
    if let RaftError::APIError(ClientWriteError::ForwardToLeader(f)) = &e {
        return ClusterError::NotLeader { leader_id: f.leader_id };
    }
    ClusterError::Raft(e.to_string())
}
