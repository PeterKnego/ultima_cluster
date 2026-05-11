//! NodeBuilder<S>: constructs the JournalLogStorage + AdaptedStateMachine<S>
//! pair, builds an `openraft::Raft<TypeConfig>`, applies bootstrap, and returns
//! a NodeHandle.
//!
//! openraft 0.9.24 deviations from the original spec are documented inline.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use openraft::{Config as RaftConfigOpenraft, Raft};

use uc_service::StateMachine;

use super::node::NodeHandle;
use crate::ClusterError;
use crate::config::{BootstrapConfig, NodeConfig};
use crate::network::server::spawn_server;
use crate::network::tls;
use crate::network::QuicRaftNetworkFactory;
use crate::raft::log_storage::JournalLogStorage;
use crate::raft::state_machine::AdaptedStateMachine;
use crate::raft::{NodeAddr, NodeId};

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

        // Adapter over user's state machine. Clone (Arc<Mutex<_>>-shared) so
        // NodeHandle can keep a handle for the M1.b query path.
        let handles = log_storage.handles(self.config.data_dir.clone());
        let sm_adapter = AdaptedStateMachine::new(self.state_machine, handles)?;
        let sm_adapter_handle = sm_adapter.clone();

        // openraft::Config — fields are u64 (millis), not Duration. Validate
        // via the inherent `Config::validate(self) -> Result<Config, ConfigError>`
        // method (consumes self, returns the validated config).
        let raft_config_unvalidated = RaftConfigOpenraft {
            cluster_name: self.config.app_id.clone(),
            heartbeat_interval: self.config.raft.heartbeat_interval_ms,
            election_timeout_min: self.config.raft.election_timeout_min_ms,
            election_timeout_max: self.config.raft.election_timeout_max_ms,
            max_in_snapshot_log_to_keep: self.config.raft.max_in_snapshot_log_to_keep,
            ..Default::default()
        };
        let validated = raft_config_unvalidated
            .validate()
            .map_err(|e| ClusterError::Config(format!("raft config: {e}")))?;
        let raft_config = Arc::new(validated);

        // TLS infrastructure: load-or-create the self-signed cert and build
        // rustls configs. `PrivateKeyDer` is not `Clone`, so we consume
        // `key_der` into the server config and reload from disk if we ever
        // need it again (we don't, here).
        let (cert_der, key_der) = tls::load_or_init(&self.config.data_dir, &self.config.app_id)?;
        let server_tls_cfg = tls::build_server_config(cert_der, key_der)?;
        let client_tls_cfg = tls::build_client_config()?;

        // Shared client QUIC endpoint (one UDP socket for all outbound peer
        // connections). Bind to 0.0.0.0:0 — kernel picks an ephemeral port.
        let client_endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap())?;

        // Real network factory replacing M1's NoopNetwork.
        let network = QuicRaftNetworkFactory::new(
            client_endpoint,
            client_tls_cfg,
            self.config.app_id.clone(),
        );

        let raft = Raft::new(
            self.config.node_id,
            raft_config,
            network,
            log_storage,
            sm_adapter,
        )
        .await
        .map_err(|e| ClusterError::Raft(format!("Raft::new: {e}")))?;

        // Spawn the QUIC server. Binds `raft_listen_addr` and starts accepting
        // peer connections that dispatch into `raft`.
        let server = spawn_server(self.config.raft_listen_addr, server_tls_cfg, raft.clone())?;

        // Apply bootstrap.
        match &self.config.bootstrap {
            BootstrapConfig::Resume => {
                // No-op; raft picks up state from the durable log + StableValues.
            }
            BootstrapConfig::SingleNode => {
                let mut members: BTreeMap<NodeId, NodeAddr> = BTreeMap::new();
                members.insert(
                    self.config.node_id,
                    NodeAddr {
                        raft_addr: self.config.raft_listen_addr,
                        client_addr: None,
                    },
                );
                // openraft's `initialize()` returns InitializeError::NotAllowed when
                // the node was already initialized on a prior run (per the doc on
                // `Raft::initialize`: "If `InitializeError::NotAllowed` is returned …
                // it is safe to ignore"). We swallow that variant explicitly so a
                // restart with BootstrapConfig::SingleNode is idempotent; everything
                // else propagates.
                use openraft::error::{InitializeError, RaftError};
                match raft.initialize(members).await {
                    Ok(()) => {}
                    Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {
                        // Already initialized on a prior run — fine.
                    }
                    Err(e) => {
                        return Err(ClusterError::Raft(format!("initialize: {e}")));
                    }
                }
            }
            BootstrapConfig::Peers { peers } => {
                // Split-brain-safe bootstrap: the node with the lowest id in
                // the peer list is the bootstrapper; everyone else waits to be
                // added as a learner.
                let min_id = peers
                    .iter()
                    .map(|p| p.node_id)
                    .min()
                    .ok_or_else(|| ClusterError::Config("Peers list is empty".into()))?;
                let self_id = self.config.node_id;

                if self_id == min_id {
                    // I am the bootstrapper.
                    let mut members: BTreeMap<NodeId, NodeAddr> = BTreeMap::new();
                    members.insert(
                        self_id,
                        NodeAddr {
                            raft_addr: self.config.raft_listen_addr,
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

                    // Add the other peers as learners (blocking until they
                    // catch up). A transient peer failure here is warn-logged,
                    // not fatal: a partial cluster is recoverable via the
                    // membership-change methods on NodeHandle.
                    for peer in peers.iter().filter(|p| p.node_id != self_id) {
                        let node = NodeAddr {
                            raft_addr: peer.raft_addr,
                            client_addr: None,
                        };
                        if let Err(e) = raft.add_learner(peer.node_id, node, true).await {
                            tracing::warn!(
                                node_id = peer.node_id,
                                error = ?e,
                                "add_learner failed; peer may need to be added manually after startup"
                            );
                        }
                    }

                    // Promote all peers to voters via change_membership.
                    let voters: BTreeSet<NodeId> = peers.iter().map(|p| p.node_id).collect();
                    if let Err(e) = raft.change_membership(voters, false).await {
                        tracing::warn!(
                            error = ?e,
                            "change_membership failed; manual recovery may be needed"
                        );
                    }
                } else {
                    // Not the bootstrapper — idle until added as learner by
                    // the min-id node.
                    tracing::info!(self_id, min_id, "waiting for bootstrap node to add me");
                }
            }
        }

        Ok(NodeHandle {
            raft,
            config: self.config,
            sm: sm_adapter_handle,
            server,
        })
    }
}
