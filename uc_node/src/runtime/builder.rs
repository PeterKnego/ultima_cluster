//! NodeBuilder<S>: constructs the JournalLogStorage + AdaptedStateMachine<S>
//! pair, builds an `openraft::Raft<TypeConfig>`, applies bootstrap, and returns
//! a NodeHandle.
//!
//! openraft 0.9.24 deviations from the original spec are documented inline.

use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::{Config as RaftConfigOpenraft, Raft};

use uc_service::StateMachine;

use super::node::NodeHandle;
use crate::ClusterError;
use crate::config::{BootstrapConfig, NodeConfig};
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

        // M1: no real network. NoopNetwork's RaftNetwork methods all
        // unreachable!() — single-node mode never sends RPCs.
        let network = crate::raft::log_storage::test_helpers::NoopNetwork;

        let raft = Raft::new(
            self.config.node_id,
            raft_config,
            network,
            log_storage,
            sm_adapter,
        )
        .await
        .map_err(|e| ClusterError::Raft(format!("Raft::new: {e}")))?;

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
            BootstrapConfig::Peers { .. } => {
                return Err(ClusterError::Config(
                    "BootstrapConfig::Peers requires QUIC network (M2)".into(),
                ));
            }
        }

        Ok(NodeHandle {
            raft,
            config: self.config,
            sm: sm_adapter_handle,
        })
    }
}
