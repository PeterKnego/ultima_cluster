//! NodeBuilder<S>: constructs the JournalLogStorage + AdaptedStateMachine<S>
//! pair, builds an `openraft::Raft<TypeConfig, SM>`, applies bootstrap, and
//! returns a NodeHandle.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use openraft::{Config as RaftConfigOpenraft, Raft};

use uc_service::StateMachine;

use super::node::{NodeHandle, RaftHandle, SmAdapter};
use crate::ClusterError;
use crate::config::{BootstrapConfig, IpcMode, NodeConfig};
use crate::ipc::handshake::wait_for_service_ready;
use crate::ipc::liveness::spawn_liveness;
use crate::ipc::query_link::ShmemQueryLink;
use crate::ipc::service_link::ServiceLink;
use crate::ipc::service_watcher::{DEFAULT_LIVENESS_TIMEOUT, spawn_service_watcher};
use crate::ipc::{HandshakeError, Instance};
use crate::network::QuicRaftNetworkFactory;
use crate::network::server::spawn_server;
use crate::network::tls;
use crate::raft::log_storage::JournalLogStorage;
use crate::raft::state_machine::AdaptedStateMachine;
use crate::raft::state_machine_shmem::ShmemAdaptedStateMachine;
use crate::raft::{NodeAddr, NodeId, TypeConfig};

/// Builds an embedded-mode ultima_cluster node.
/// Generic over S; non-generic shmem-fronted variant arrives in M3.
pub struct NodeBuilder<S: StateMachine> {
    config: NodeConfig,
    state_machine: S,
}

impl<S: StateMachine> NodeBuilder<S> {
    pub fn new(config: NodeConfig, state_machine: S) -> Self {
        Self {
            config,
            state_machine,
        }
    }

    pub async fn start(self) -> Result<NodeHandle<S>, ClusterError> {
        self.config.validate().map_err(ClusterError::Config)?;

        // Open log storage (journal + StableValues).
        let log_storage = JournalLogStorage::open(&self.config.data_dir)?;

        // Sanity-check durable state before handing off to openraft.
        crate::runtime::recovery::assert_consistent(&log_storage)?;

        let handles = log_storage.handles(self.config.data_dir.clone());

        // Branch on ipc_mode and call the parameterized `finish` helper with
        // whichever concrete SM adapter we built.
        match self.config.ipc_mode.clone() {
            IpcMode::Embedded => {
                let adapter = AdaptedStateMachine::new(self.state_machine, handles)?;
                let handle_sm = SmAdapter::Embedded(adapter.clone());
                finish(
                    self.config,
                    log_storage,
                    adapter,
                    handle_sm,
                    None,
                    None,
                    None,
                    RaftHandle::Embedded,
                )
                .await
            }
            IpcMode::Shmem { instance_dir } => {
                let instance =
                    Instance::create(&instance_dir, &self.config.app_id, self.config.node_id)?;
                let link = ServiceLink::create(&instance_dir)?;

                // Pointers into the cnc mmap for the heartbeat ticker + the
                // service-side handshake watcher. Lifetimes are upheld by
                // `instance` (moved into the NodeHandle) outliving both.
                let (node_status_ptr, service_status_ptr) = status_ptrs(&instance.cnc_mmap);
                // SendPtr lets us hold service_status_ptr across the awaits
                // below; raw `*const T` is not `Send` on its own. The mmap
                // backing the target outlives every consumer (cf. SAFETY
                // notes on each `unsafe` block below).
                let service_status = SendPtr(service_status_ptr);

                // SAFETY: `instance.cnc_mmap` is moved into the NodeHandle
                // below and stays alive until shutdown stops + joins this
                // ticker.
                let node_liveness = unsafe { spawn_liveness(node_status_ptr) };

                // Block until the service publishes state = Ready. 30s is
                // long enough for the test harness to spawn the service in
                // parallel; production callers can wrap with their own
                // outer timeout if needed.
                // SAFETY: see node_status_ptr SAFETY above; same mmap.
                unsafe {
                    wait_for_service_ready(service_status.0, std::time::Duration::from_secs(30))
                        .await
                }
                .map_err(map_handshake_err)?;

                let adapter = ShmemAdaptedStateMachine::new(
                    self.state_machine,
                    handles,
                    link.apply_producer,
                    link.apply_resp_consumer,
                )?;
                let query_link = ShmemQueryLink::new(link.query_producer, link.query_resp_consumer);
                let handle_sm = SmAdapter::Shmem(adapter.clone());
                let node_id_for_watcher = self.config.node_id;
                let mut handle = finish(
                    self.config,
                    log_storage,
                    adapter,
                    handle_sm,
                    Some(instance),
                    Some(node_liveness),
                    Some(query_link),
                    RaftHandle::Shmem,
                )
                .await?;

                // Service-liveness watcher: shuts raft down if this node
                // is leader when the service stalls. Spawned post-finish
                // because it needs a `Raft<TypeConfig>` clone.
                // SAFETY: see service_status_ptr SAFETY above. The mmap
                // lives in `handle._instance`, which `handle.shutdown()`
                // drops only after joining this watcher.
                let watcher = unsafe {
                    spawn_service_watcher(
                        service_status.0,
                        handle.raft.clone(),
                        node_id_for_watcher,
                        DEFAULT_LIVENESS_TIMEOUT,
                    )
                };
                handle.service_watcher = Some(watcher);
                Ok(handle)
            }
        }
    }
}

/// Common openraft + QUIC setup. Generic over whichever SM adapter the
/// caller built.
///
/// `wrap_raft` converts the concrete `Raft<TypeConfig, A>` (produced by
/// `Raft::new`) into the mode-erased `RaftHandle<S>` enum that
/// `NodeHandle<S>` stores. This is needed because the two IPC modes use
/// different SM adapter types, but the public `NodeHandle<S>` must be a
/// single type.
#[allow(clippy::too_many_arguments)]
async fn finish<A, S>(
    config: NodeConfig,
    log_storage: JournalLogStorage,
    sm_adapter: A,
    handle_sm: SmAdapter<S>,
    instance: Option<Instance>,
    node_liveness: Option<crate::ipc::liveness::LivenessHandle>,
    query_link: Option<ShmemQueryLink>,
    wrap_raft: impl FnOnce(Raft<TypeConfig, A>) -> RaftHandle<S>,
) -> Result<NodeHandle<S>, ClusterError>
where
    S: StateMachine,
    A: openraft::storage::RaftStateMachine<crate::raft::TypeConfig>,
{
    // openraft::Config — fields are u64 (millis), not Duration. Validate
    // via the inherent `Config::validate(self) -> Result<Config, ConfigError>`
    // method (consumes self, returns the validated config).
    let raft_config_unvalidated = RaftConfigOpenraft {
        cluster_name: config.app_id.clone(),
        heartbeat_interval: config.raft.heartbeat_interval_ms,
        election_timeout_min: config.raft.election_timeout_min_ms,
        election_timeout_max: config.raft.election_timeout_max_ms,
        max_in_snapshot_log_to_keep: config.raft.max_in_snapshot_log_to_keep,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(
            config.raft.snapshot_policy_logs_since_last,
        ),
        ..Default::default()
    };
    let validated = raft_config_unvalidated
        .validate()
        .map_err(|e| ClusterError::Config(format!("raft config: {e}")))?;
    let raft_config = Arc::new(validated);

    // TLS infrastructure: load-or-create the self-signed cert and build
    // rustls configs.
    let (cert_der, key_der) = tls::load_or_init(&config.data_dir, &config.app_id)?;
    let server_tls_cfg = tls::build_server_config(cert_der, key_der)?;
    let client_tls_cfg = tls::build_client_config()?;

    // Shared client QUIC endpoint (one UDP socket for all outbound peer
    // connections). Bind to 0.0.0.0:0 — kernel picks an ephemeral port.
    let client_endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap())?;

    let network =
        QuicRaftNetworkFactory::new(client_endpoint, client_tls_cfg, config.app_id.clone());

    let raft = Raft::new(
        config.node_id,
        raft_config,
        network,
        log_storage,
        sm_adapter,
    )
    .await
    .map_err(|e| ClusterError::Raft(format!("Raft::new: {e}")))?;

    // Spawn the QUIC server. Binds `raft_listen_addr` and starts accepting
    // peer connections that dispatch into `raft`.
    let server = spawn_server(config.raft_listen_addr, server_tls_cfg, raft.clone())?;

    // Apply bootstrap.
    match &config.bootstrap {
        BootstrapConfig::Resume => {
            // No-op; raft picks up state from the durable log + StableValues.
        }
        BootstrapConfig::SingleNode => {
            let mut members: BTreeMap<NodeId, NodeAddr> = BTreeMap::new();
            members.insert(
                config.node_id,
                NodeAddr {
                    raft_addr: config.raft_listen_addr,
                    client_addr: None,
                },
            );
            use openraft::error::{InitializeError, RaftError};
            match raft.initialize(members).await {
                Ok(()) => {}
                Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {
                    // Already initialized on a prior run — idempotent.
                }
                Err(e) => {
                    return Err(ClusterError::Raft(format!("initialize: {e}")));
                }
            }
        }
        BootstrapConfig::Peers { peers } => {
            let min_id = peers
                .iter()
                .map(|p| p.node_id)
                .min()
                .ok_or_else(|| ClusterError::Config("Peers list is empty".into()))?;
            let self_id = config.node_id;

            if self_id == min_id {
                let mut members: BTreeMap<NodeId, NodeAddr> = BTreeMap::new();
                members.insert(
                    self_id,
                    NodeAddr {
                        raft_addr: config.raft_listen_addr,
                        client_addr: None,
                    },
                );
                use openraft::error::{InitializeError, RaftError};
                match raft.initialize(members).await {
                    Ok(()) => {}
                    Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {
                        // Already initialized on a prior run — idempotent.
                    }
                    Err(e) => {
                        return Err(ClusterError::Raft(format!("initialize: {e}")));
                    }
                }

                let mut promotable: BTreeSet<NodeId> = BTreeSet::from([self_id]);
                for peer in peers.iter().filter(|p| p.node_id != self_id) {
                    let node = NodeAddr {
                        raft_addr: peer.raft_addr,
                        client_addr: None,
                    };
                    // openraft 0.10: `initialize()` returns after the init log
                    // is FLUSHED, not committed. `add_learner` called too soon
                    // races with the ongoing membership change and fails with
                    // `InProgress`. Retry with backoff until the init
                    // membership commits (typically < 10 ms in a single-voter
                    // cluster) or the overall deadline is reached.
                    use openraft::error::{
                        ChangeMembershipError, ClientWriteError, RaftError as OR,
                    };
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                    loop {
                        match raft.add_learner(peer.node_id, node.clone(), true).await {
                            Ok(_) => {
                                promotable.insert(peer.node_id);
                                break;
                            }
                            Err(OR::APIError(ClientWriteError::ChangeMembershipError(
                                ChangeMembershipError::InProgress(_),
                            ))) if std::time::Instant::now() < deadline => {
                                // Race: init membership not yet committed.
                                tracing::trace!(
                                    node_id = peer.node_id,
                                    "add_learner saw InProgress; retrying after 5ms"
                                );
                                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                                continue;
                            }
                            Err(OR::APIError(ClientWriteError::ChangeMembershipError(
                                ChangeMembershipError::InProgress(_),
                            ))) => {
                                tracing::warn!(
                                    node_id = peer.node_id,
                                    "add_learner timed out (InProgress past 10s deadline); \
                                     peer will not be promoted to voter"
                                );
                                break;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    node_id = peer.node_id,
                                    error = ?e,
                                    "add_learner failed; peer will not be promoted to voter"
                                );
                                break;
                            }
                        }
                    }
                }

                if let Err(e) = raft.change_membership(promotable, false).await {
                    tracing::warn!(
                        error = ?e,
                        "change_membership failed; cluster may remain as single voter"
                    );
                }
            } else {
                tracing::info!(self_id, min_id, "waiting for bootstrap node to add me");
            }
        }
    }

    let raft_handle = wrap_raft(raft);

    Ok(NodeHandle {
        raft: raft_handle,
        config,
        sm: handle_sm,
        server,
        _instance: instance,
        node_liveness,
        query_link,
        service_watcher: None,
    })
}

/// Compute `(*const NodeStatus, *const ServiceStatus)` from the cnc mmap
/// header's sub-buffer offset table.
fn status_ptrs(
    cnc_mmap: &memmap2::MmapMut,
) -> (
    *const uc_protocol::cnc::NodeStatus,
    *const uc_protocol::cnc::ServiceStatus,
) {
    use uc_protocol::cnc::{CncHeader, sub};
    let base = cnc_mmap.as_ptr();
    // SAFETY: cnc_mmap was just validated by `init_cnc`; the header layout
    // is fixed by `#[repr(C)]` and the sub_buffer_offsets entry was
    // populated to point at the status blocks.
    let header = unsafe { &*(base as *const CncHeader) };
    let node_off = header.sub_buffer_offsets[sub::NODE_STATUS] as usize;
    let service_off = header.sub_buffer_offsets[sub::SERVICE_STATUS] as usize;
    // SAFETY: offsets are within the mmap by construction in `init_cnc`.
    let node = unsafe { base.add(node_off) as *const uc_protocol::cnc::NodeStatus };
    let service = unsafe { base.add(service_off) as *const uc_protocol::cnc::ServiceStatus };
    (node, service)
}

fn map_handshake_err(e: HandshakeError) -> ClusterError {
    ClusterError::Config(format!("service handshake: {e}"))
}

/// `Send`-carrier for a `*const ServiceStatus` used across the
/// `start()` await chain. `ServiceStatus` is `Sync` (all-atomic fields),
/// so the pointer is logically safe to send between threads as long as
/// the underlying mmap is pinned — which the caller ensures by keeping
/// `Instance` alive until every consumer joins.
#[derive(Copy, Clone)]
struct SendPtr(*const uc_protocol::cnc::ServiceStatus);

// SAFETY: see SendPtr docs — invariant upheld at every consumer call site.
unsafe impl Send for SendPtr {}
