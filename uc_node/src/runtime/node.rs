//! Public NodeHandle returned by NodeBuilder::start().
//!
//! M1 is generic over S; M3 will introduce a non-generic shmem-fronted handle
//! that can be embedded in a process boundary alongside a separate state-machine
//! worker. The shape (submit / current_leader / node_id / shutdown) stays the
//! same.

use std::collections::BTreeSet;
use std::net::SocketAddr;

use bytes::Bytes;
use openraft::Raft;
use openraft::error::{ClientWriteError, RaftError};

use uc_service::StateMachine;

use crate::ClusterError;
use crate::config::{NodeConfig, NodeId};
use crate::network::server::ServerHandle;
use crate::raft::TypeConfig;
use crate::raft::state_machine::AdaptedStateMachine;
use crate::raft::NodeAddr;

/// Public handle returned by [`NodeBuilder::start`](super::builder::NodeBuilder::start).
pub struct NodeHandle<S: StateMachine> {
    pub(crate) raft: Raft<TypeConfig>,
    pub(crate) config: NodeConfig,
    /// Cloned handle to the user state-machine adapter. The Raft engine owns
    /// another clone internally; both share the same `Arc<Mutex<Inner<S>>>`.
    /// Used by `query_snapshot` to reach the user SM directly without going
    /// through Raft.
    pub(crate) sm: AdaptedStateMachine<S>,
    /// QUIC server handle. Closes the inbound endpoint and awaits the accept
    /// task during [`shutdown`].
    pub(crate) server: ServerHandle,
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

        let result = self
            .raft
            .client_write(app_command)
            .await
            .map_err(map_client_write_error)?;

        // result.data is the bincode-encoded S::Response (per TypeConfig: R = Bytes).
        let resp_bytes: Bytes = result.data;
        let (resp, _) = bincode::serde::decode_from_slice::<S::Response, _>(
            &resp_bytes,
            bincode::config::standard(),
        )?;
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

    /// Add a learner (non-voting follower) to the cluster.
    /// Returns once the learner has caught up via log replication / snapshot
    /// install (openraft 0.9.24 `add_learner(id, node, blocking=true)`).
    pub async fn add_learner(
        &self,
        node_id: NodeId,
        raft_addr: SocketAddr,
    ) -> Result<(), ClusterError> {
        let node = NodeAddr {
            raft_addr,
            client_addr: None,
        };
        self.raft
            .add_learner(node_id, node, true)
            .await
            .map_err(|e| ClusterError::Raft(e.to_string()))?;
        Ok(())
    }

    /// Change the membership to the given set of voters.
    /// Uses openraft 0.9.24 `change_membership(members, retain=false)`: nodes
    /// not in `voters` are removed from the cluster (not retained as learners).
    pub async fn change_membership(
        &self,
        voters: BTreeSet<NodeId>,
    ) -> Result<(), ClusterError> {
        self.raft
            .change_membership(voters, false)
            .await
            .map_err(|e| ClusterError::Raft(e.to_string()))?;
        Ok(())
    }

    /// Remove a node from the cluster. Convenience wrapper over
    /// [`change_membership`]: snapshots the current voter set from openraft
    /// metrics, drops `node_id`, and applies the result.
    pub async fn remove_node(&self, node_id: NodeId) -> Result<(), ClusterError> {
        // `metrics()` returns a cloned `watch::Receiver`; `.borrow()` is sync.
        // `voter_ids()` on `StoredMembership` yields owned `NID` values.
        let current: BTreeSet<NodeId> = {
            let metrics = self.raft.metrics();
            let m = metrics.borrow();
            m.membership_config.voter_ids().collect()
        };
        let mut next = current;
        next.remove(&node_id);
        self.raft
            .change_membership(next, false)
            .await
            .map_err(|e| ClusterError::Raft(e.to_string()))?;
        Ok(())
    }

    pub async fn shutdown(self) -> Result<(), ClusterError> {
        // Shut down raft first so it stops issuing outbound RPCs.
        self.raft
            .shutdown()
            .await
            .map_err(|e| ClusterError::Raft(format!("shutdown: {e}")))?;
        // Then shut down the QUIC server (closes endpoint, awaits accept task).
        self.server.shutdown().await;
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
        return ClusterError::NotLeader {
            leader_id: f.leader_id,
        };
    }
    ClusterError::Raft(e.to_string())
}
