use std::net::SocketAddr;
use std::path::PathBuf;

pub type NodeId = u64;

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: NodeId,
    pub data_dir: PathBuf,
    pub raft_listen_addr: SocketAddr, // unused in M1 (no QUIC yet); reserved
    pub app_id: String,
    pub bootstrap: BootstrapConfig,
    pub raft: RaftTuning,
    /// TLS configuration for inter-node QUIC. M2 supports `SelfSigned`;
    /// `Files` (operator-provided certs) arrives in M5.
    pub tls: TlsConfig,
    /// In-process apply vs shmem-fronted apply. Default `Embedded` matches
    /// M1/M2 behavior; `Shmem` activates the M3 service-process split.
    pub ipc_mode: IpcMode,
}

/// Selects how `apply()` is dispatched.
///
/// * [`IpcMode::Embedded`] (default) — the user's [`uc_service::StateMachine`]
///   runs in-process; `AdaptedStateMachine` calls `apply()` directly under a
///   tokio mutex. This is the M1/M2 path.
/// * [`IpcMode::Shmem`] — `apply()` publishes onto `<instance_dir>/service/apply.ring`
///   and awaits the response on `apply_resp.ring`. The user's state machine
///   runs in `uc_service::ServiceBuilder::run` (typically a separate tokio
///   task or process). The node-side state machine handed to `NodeBuilder`
///   is degenerate in this mode — used only for snapshot codec.
#[derive(Debug, Clone, Default)]
pub enum IpcMode {
    #[default]
    Embedded,
    Shmem {
        instance_dir: PathBuf,
    },
}

/// TLS configuration for inter-node QUIC.
///
/// M2 supports `SelfSigned`. `Files` (operator-provided certs) and
/// `Insecure` (no TLS) arrive in M5 production polish.
#[derive(Debug, Clone, Default)]
pub enum TlsConfig {
    /// Generate a self-signed cert at first start; persist to `data_dir/tls.{crt,key}`.
    #[default]
    SelfSigned,
}

#[derive(Debug, Clone)]
pub enum BootstrapConfig {
    Resume,
    SingleNode,
    Peers { peers: Vec<PeerSeed> }, // unused in M1
}

#[derive(Debug, Clone)]
pub struct PeerSeed {
    pub node_id: NodeId,
    pub raft_addr: SocketAddr,
}

#[derive(Debug, Clone)]
pub struct RaftTuning {
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub max_in_snapshot_log_to_keep: u64,
    /// Trigger snapshot every N applied log entries. openraft default is 5000.
    pub snapshot_policy_logs_since_last: u64,
}

impl Default for RaftTuning {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 250,
            election_timeout_min_ms: 1000,
            election_timeout_max_ms: 2000,
            max_in_snapshot_log_to_keep: 1000,
            snapshot_policy_logs_since_last: 5000,
        }
    }
}

impl NodeConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.app_id.is_empty() {
            return Err("app_id must not be empty".into());
        }
        if self.app_id.len() > 64 {
            return Err("app_id must be <= 64 bytes".into());
        }
        if self.raft.election_timeout_min_ms >= self.raft.election_timeout_max_ms {
            return Err("election_timeout_min_ms must be < max".into());
        }
        Ok(())
    }
}
