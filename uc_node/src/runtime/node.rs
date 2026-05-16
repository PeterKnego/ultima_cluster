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
use openraft::rt::WatchReceiver as _;

use uc_service::StateMachine;

use crate::ClusterError;
use crate::config::{NodeConfig, NodeId};
use crate::ipc::Instance;
use crate::ipc::liveness::LivenessHandle;
use crate::ipc::query_link::ShmemQueryLink;
use crate::ipc::service_watcher::ServiceWatcherHandle;
use crate::network::server::ServerHandle;
use crate::raft::NodeAddr;
use crate::raft::TypeConfig;
use crate::raft::state_machine::AdaptedStateMachine;
use crate::raft::state_machine_shmem::ShmemAdaptedStateMachine;

/// Which state-machine adapter the node is driving.
pub(crate) enum SmAdapter<S: StateMachine> {
    Embedded(AdaptedStateMachine<S>),
    /// Shmem-mode handle. The `ShmemAdaptedStateMachine` is held here for
    /// future query/snapshot routing through the service; M3 doesn't
    /// reach into it (apply traffic flows entirely through openraft's
    /// internal clone of the adapter).
    #[allow(dead_code)]
    Shmem(ShmemAdaptedStateMachine<S>),
}

/// Type-erased Raft handle over the two concrete SM adapters.
///
/// `openraft::Raft<C, SM>` is generic over `SM` (the state machine adapter);
/// embedded mode uses `AdaptedStateMachine<S>` and shmem mode uses
/// `ShmemAdaptedStateMachine<S>`.  We store either variant in this enum so
/// that `NodeHandle<S>` can remain a single public type.
pub(crate) enum RaftHandle<S: StateMachine> {
    Embedded(Raft<TypeConfig, AdaptedStateMachine<S>>),
    Shmem(Raft<TypeConfig, ShmemAdaptedStateMachine<S>>),
}

impl<S: StateMachine> Clone for RaftHandle<S> {
    fn clone(&self) -> Self {
        match self {
            Self::Embedded(r) => Self::Embedded(r.clone()),
            Self::Shmem(r) => Self::Shmem(r.clone()),
        }
    }
}

impl<S: StateMachine> RaftHandle<S> {
    pub(crate) async fn client_write(
        &self,
        app_command: crate::raft::AppCommand,
    ) -> Result<
        openraft::raft::ClientWriteResponse<TypeConfig>,
        RaftError<TypeConfig, ClientWriteError<TypeConfig>>,
    > {
        match self {
            Self::Embedded(r) => r.client_write(app_command).await,
            Self::Shmem(r) => r.client_write(app_command).await,
        }
    }

    pub(crate) async fn current_leader(&self) -> Option<NodeId> {
        match self {
            Self::Embedded(r) => r.current_leader().await,
            Self::Shmem(r) => r.current_leader().await,
        }
    }

    pub(crate) async fn add_learner(
        &self,
        id: NodeId,
        node: NodeAddr,
        blocking: bool,
    ) -> Result<
        openraft::raft::ClientWriteResponse<TypeConfig>,
        RaftError<TypeConfig, openraft::error::ClientWriteError<TypeConfig>>,
    > {
        match self {
            Self::Embedded(r) => r.add_learner(id, node, blocking).await,
            Self::Shmem(r) => r.add_learner(id, node, blocking).await,
        }
    }

    pub(crate) async fn change_membership(
        &self,
        members: std::collections::BTreeSet<NodeId>,
        retain: bool,
    ) -> Result<
        openraft::raft::ClientWriteResponse<TypeConfig>,
        RaftError<TypeConfig, openraft::error::ClientWriteError<TypeConfig>>,
    > {
        match self {
            Self::Embedded(r) => r.change_membership(members, retain).await,
            Self::Shmem(r) => r.change_membership(members, retain).await,
        }
    }

    pub(crate) async fn shutdown(
        &self,
    ) -> Result<(), openraft::type_config::alias::JoinErrorOf<TypeConfig>> {
        match self {
            Self::Embedded(r) => r.shutdown().await,
            Self::Shmem(r) => r.shutdown().await,
        }
    }

    pub(crate) fn metrics(
        &self,
    ) -> openraft::type_config::alias::WatchReceiverOf<TypeConfig, openraft::RaftMetrics<TypeConfig>> {
        match self {
            Self::Embedded(r) => r.metrics(),
            Self::Shmem(r) => r.metrics(),
        }
    }
}

/// Public handle returned by [`NodeBuilder::start`](super::builder::NodeBuilder::start).
pub struct NodeHandle<S: StateMachine> {
    pub(crate) raft: RaftHandle<S>,
    pub(crate) config: NodeConfig,
    /// Cloned handle to the user state-machine adapter. The Raft engine owns
    /// another clone internally. Used by [`Self::query_snapshot`] in
    /// embedded mode; in shmem mode the closure path is unavailable.
    pub(crate) sm: SmAdapter<S>,
    /// QUIC server handle. Closes the inbound endpoint and awaits the accept
    /// task during [`shutdown`].
    pub(crate) server: ServerHandle,
    /// Shmem-mode only: keeps the cnc.dat mapping + `instance.lock` alive.
    pub(crate) _instance: Option<Instance>,
    /// Shmem-mode only: node-side heartbeat ticker handle. Stop+joined on
    /// shutdown before the cnc mmap is dropped.
    pub(crate) node_liveness: Option<LivenessHandle>,
    /// Shmem-mode only: query.ring producer + query_resp.ring consumer,
    /// wrapped in the publish/await helper used by [`Self::submit_query`].
    pub(crate) query_link: Option<ShmemQueryLink>,
    /// Shmem-mode only: watches the service's heartbeat. Joined on
    /// shutdown before the cnc mmap drops.
    pub(crate) service_watcher: Option<ServiceWatcherHandle>,
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
        use crate::raft::AppCommand;
        let bytes = bincode::serde::encode_to_vec(&cmd, bincode::config::standard())?;
        let app_command: AppCommand = AppCommand::from(Bytes::from(bytes));

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
    ///
    /// **Embedded mode only.** In shmem mode the user SM lives in
    /// `uc_service::ServiceBuilder::run` and is not reachable via a
    /// closure across the IPC boundary — submit a typed `Query` through
    /// the query ring instead. Calling this in shmem mode panics.
    pub async fn query_snapshot<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&S) -> R + Send,
        R: Send,
    {
        match &self.sm {
            SmAdapter::Embedded(a) => a.with_state(f).await,
            SmAdapter::Shmem(_) => panic!(
                "query_snapshot is embedded-only; in shmem mode submit a typed Query \
                 through uc_service's query.ring path"
            ),
        }
    }

    /// Submit a typed `S::Query`. Works in both IPC modes:
    ///
    /// * **Embedded** — takes the same mutex `apply` does and calls
    ///   `state_machine.query(q)` in-process. Equivalent to
    ///   [`Self::query_snapshot`] but with the user's typed `Query` rather
    ///   than a closure.
    /// * **Shmem** — bincode-encodes `q`, publishes a [`QueryFrame`] on
    ///   `service/query.ring`, awaits the matching `QueryRespFrame` on
    ///   `service/query_resp.ring`, decodes the response payload.
    ///
    /// All M3 queries are routed as [`QueryKind::Snapshot`]. Linearizable
    /// reads (round-trip through raft) arrive in M4.
    ///
    /// [`QueryFrame`]: uc_protocol::frames::query
    /// [`QueryKind::Snapshot`]: uc_protocol::frames::query::QueryKind::Snapshot
    pub async fn submit_query(&self, q: S::Query) -> Result<S::QueryResponse, ClusterError> {
        match &self.sm {
            SmAdapter::Embedded(a) => Ok(a.with_state(|s| s.query(q)).await),
            SmAdapter::Shmem(_) => {
                use uc_protocol::frames::query::QueryKind;
                let link = self.query_link.as_ref().ok_or_else(|| {
                    ClusterError::Config(
                        "shmem-mode NodeHandle missing query_link (builder bug)".into(),
                    )
                })?;
                let payload = bincode::serde::encode_to_vec(&q, bincode::config::standard())?;
                let resp_bytes = link.submit(&payload, QueryKind::Snapshot).await?;
                let (resp, _) = bincode::serde::decode_from_slice::<S::QueryResponse, _>(
                    &resp_bytes,
                    bincode::config::standard(),
                )?;
                Ok(resp)
            }
        }
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
            .map_err(|e| ClusterError::Raft(format!("{:?}", e)))?;
        Ok(())
    }

    /// Change the membership to the given set of voters.
    /// Uses openraft 0.9.24 `change_membership(members, retain=false)`: nodes
    /// not in `voters` are removed from the cluster (not retained as learners).
    pub async fn change_membership(&self, voters: BTreeSet<NodeId>) -> Result<(), ClusterError> {
        self.raft
            .change_membership(voters, false)
            .await
            .map_err(|e| ClusterError::Raft(format!("{:?}", e)))?;
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
            let m = metrics.borrow_watched();
            m.membership_config.voter_ids().collect()
        };
        let mut next = current;
        next.remove(&node_id);
        self.raft
            .change_membership(next, false)
            .await
            .map_err(|e| ClusterError::Raft(format!("{:?}", e)))?;
        Ok(())
    }

    /// Shmem-mode only: `true` once the service-liveness watcher has
    /// detected a stall (heartbeat_seq did not advance within its
    /// configured timeout). Returns `false` in embedded mode and before
    /// the first stall observation.
    pub fn service_stalled(&self) -> bool {
        self.service_watcher
            .as_ref()
            .is_some_and(|w| w.stalled.load(std::sync::atomic::Ordering::Relaxed))
    }

    pub async fn shutdown(self) -> Result<(), ClusterError> {
        // Shut down raft first so it stops issuing outbound RPCs.
        // Idempotent on second call (e.g. if the service watcher already
        // shut raft down on a stalled-leader event).
        self.raft
            .shutdown()
            .await
            .map_err(|e| ClusterError::Raft(format!("shutdown: {:?}", e)))?;
        // Then the QUIC server (closes endpoint, awaits accept task).
        self.server.shutdown().await;
        // Shmem-mode only: stop+join both cnc-mmap-holding tasks before
        // `_instance` drops. Both hold `&'static` references into the
        // cnc mmap that lives in `_instance`.
        if let Some(w) = self.service_watcher {
            w.stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = w.join.await;
        }
        if let Some(lv) = self.node_liveness {
            lv.stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = lv.join.await;
        }
        Ok(())
    }
}

/// Map openraft 0.10's `RaftError<TypeConfig, ClientWriteError<TypeConfig>>`
/// into our `ClusterError`. ForwardToLeader yields `NotLeader { leader_id }`;
/// everything else stringifies into `Raft(_)`.
fn map_client_write_error(
    e: RaftError<crate::raft::TypeConfig, ClientWriteError<crate::raft::TypeConfig>>,
) -> ClusterError {
    if let RaftError::APIError(ClientWriteError::ForwardToLeader(f)) = &e {
        return ClusterError::NotLeader {
            leader_id: f.leader_id,
        };
    }
    ClusterError::Raft(format!("{:?}", e))
}
