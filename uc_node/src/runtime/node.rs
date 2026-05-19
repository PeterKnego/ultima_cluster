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
use crate::ipc::client_dispatcher::ClientDispatcherHandle;
use crate::ipc::liveness::LivenessHandle;
use crate::ipc::metrics_publisher::MetricsPublisherHandle;
use crate::ipc::query_link::ShmemQueryLink;
use crate::ipc::service_watcher::ServiceWatcherHandle;
use crate::ipc::session_gc::SessionGcHandle;
use crate::network::server::ServerHandle;
use crate::raft::NodeAddr;
use crate::raft::TypeConfig;
use crate::raft::log_storage::LogStorageHandles;
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

    pub(crate) async fn transfer_leader(
        &self,
        target: NodeId,
    ) -> Result<(), openraft::error::Fatal<TypeConfig>> {
        match self {
            Self::Embedded(r) => r.trigger().transfer_leader(target).await,
            Self::Shmem(r) => r.trigger().transfer_leader(target).await,
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
    ) -> openraft::type_config::alias::WatchReceiverOf<TypeConfig, openraft::RaftMetrics<TypeConfig>>
    {
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
    /// Durable log storage handles (output_progress, last_applied, snapshot_meta).
    /// Kept here for test-only helpers that inspect or mutate durable state.
    #[cfg_attr(not(any(test, feature = "test-helpers")), allow(dead_code))]
    pub(crate) log_storage_handles: LogStorageHandles,
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
    pub(crate) query_link: Option<std::sync::Arc<ShmemQueryLink>>,
    /// Shmem-mode only: watches the service's heartbeat. Joined on
    /// shutdown before the cnc mmap drops.
    pub(crate) service_watcher: Option<ServiceWatcherHandle>,
    /// Shmem-mode only: copies raft metrics into NodeStatus cnc fields.
    /// Joined on shutdown before the cnc mmap drops.
    pub(crate) metrics_publisher: Option<MetricsPublisherHandle>,
    /// Shmem-mode only: submit-ring dispatcher.
    pub(crate) client_dispatcher: Option<ClientDispatcherHandle>,
    /// Shmem-mode only: query-ring dispatcher.
    pub(crate) client_query_dispatcher: Option<ClientDispatcherHandle>,
    /// Shmem-mode only: session-file GC.
    pub(crate) session_gc: Option<SessionGcHandle>,
    /// Shmem-mode only: output ring dispatcher (M5). Stopped before client
    /// dispatchers so the output side drains before the client side closes.
    pub(crate) output_dispatcher: Option<crate::runtime::output_dispatcher::OutputDispatcherHandle>,
    /// Shmem-mode only: leader-transition watcher that spawns output_replay
    /// one-shots (M5 Task 4.1). Stopped before output_dispatcher so no new
    /// replay tasks are spawned while the dispatcher is draining.
    pub(crate) output_replay_watcher:
        Option<crate::runtime::output_replay::OutputReplayWatcherHandle>,
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
    /// install (`add_learner(id, node, blocking=true)`).
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
    /// Uses `change_membership(members, retain=false)`: nodes not in `voters`
    /// are removed from the cluster (not retained as learners).
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
        // Destructure so we can control drop order precisely.
        let NodeHandle {
            raft,
            config: _,
            log_storage_handles: _,
            sm,
            server,
            _instance,
            node_liveness,
            query_link: _,
            service_watcher,
            metrics_publisher,
            client_dispatcher,
            client_query_dispatcher,
            session_gc,
            output_dispatcher,
            output_replay_watcher,
        } = self;

        // Shut down raft first so it stops issuing outbound RPCs.
        // Idempotent on second call (e.g. if the service watcher already
        // shut raft down on a stalled-leader event).
        raft.shutdown()
            .await
            .map_err(|e| ClusterError::Raft(format!("shutdown: {:?}", e)))?;
        // Then the QUIC server (closes endpoint, awaits accept task).
        server.shutdown().await;
        // Shmem-mode only: stop+join all cnc-mmap-holding tasks before
        // `_instance` drops. These hold `&'static` references into the
        // cnc mmap that lives in `_instance`.
        if let Some(p) = metrics_publisher {
            p.stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = p.join.await;
        }
        if let Some(w) = service_watcher {
            w.stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = w.join.await;
        }
        // M5 Task 4.1: stop output_replay_watcher first so no new replay tasks
        // are spawned while we drain. The watcher holds a clone of
        // `output_chan_tx`; stopping it here releases that clone.
        if let Some(w) = output_replay_watcher {
            w.stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = w.join.await;
        }
        // M5: drop the SM adapter (closing `output_chan_tx`) BEFORE joining
        // the output_dispatcher. The dispatcher blocks in `rx.recv().await`;
        // dropping the last sender unblocks it so the join can complete.
        drop(sm);
        // M5: stop output_dispatcher before client dispatchers so the output
        // side drains before the client side closes.
        if let Some(d) = output_dispatcher {
            d.stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = d.join.await;
        }
        if let Some(d) = client_dispatcher {
            d.stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = d.join.await;
        }
        if let Some(d) = client_query_dispatcher {
            d.stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = d.join.await;
        }
        if let Some(g) = session_gc {
            g.stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = g.join.await;
        }
        if let Some(lv) = node_liveness {
            lv.stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = lv.join.await;
        }
        // _instance drops here after all tasks referencing the cnc mmap are joined.
        drop(_instance);
        Ok(())
    }
}

#[cfg(any(test, feature = "test-helpers"))]
impl<S: StateMachine> NodeHandle<S> {
    /// Test-only: read the current `output_progress.state`. Returns 0 if
    /// the StableValue is absent or load fails.
    pub fn _test_output_progress(&self) -> u64 {
        self.log_storage_handles
            .output_progress
            .load()
            .ok()
            .flatten()
            .unwrap_or(0)
    }

    /// Test-only: force `output_progress.state` to a specific value. Used
    /// by `m5_output_idempotent_replay` to simulate a partial-output crash.
    pub fn _test_reset_output_progress(&self, value: u64) {
        let _ = self
            .log_storage_handles
            .output_progress
            .store(&value)
            .map(|n| {
                // best-effort wait; ignore wait errors in tests
                let _ = n.wait();
            });
    }

    /// Test-only: trigger graceful raft `transfer_leader` to `target`.
    /// Returns the openraft `Fatal` error if the call fails; otherwise
    /// the local node initiates the transfer and the target eventually
    /// becomes leader on a new term.
    pub async fn _test_transfer_leader(
        &self,
        target: crate::raft::NodeId,
    ) -> Result<(), openraft::error::Fatal<crate::raft::TypeConfig>> {
        self.raft.transfer_leader(target).await
    }

    /// Test-only: set the leader watch channel to `value`. Combined with a
    /// `tokio::task::yield_now().await` between calls to `false` then `true`,
    /// this triggers the output_replay_watcher to spawn a replay sweep without
    /// an actual Raft leader flip.
    ///
    /// Returns false if there's no metrics_publisher (embedded mode has no
    /// leader watch channel).
    ///
    /// # Usage
    ///
    /// ```ignore
    /// node._test_set_leader_state(false);
    /// tokio::task::yield_now().await;   // let watcher see false
    /// node._test_set_leader_state(true);
    /// tokio::task::yield_now().await;   // let watcher see true and spawn replay
    /// ```
    pub fn _test_set_leader_state(&self, value: bool) -> bool {
        if let Some(mp) = self.metrics_publisher.as_ref() {
            mp.leader_tx.send_replace(value);
            true
        } else {
            false
        }
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
