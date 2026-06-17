use std::net::SocketAddr;
use std::path::PathBuf;

pub type NodeId = u64;

/// Ring-buffer sizing for the client-facing rings
/// (`submit.ring`, `query.ring`, `response.broadcast`).
///
/// Production defaults: 16 MiB cap, 4 MiB max single frame.
/// Tests that want to exercise wrap behaviour can reduce `cap_bytes`
/// to e.g. 32 KiB.
#[derive(Debug, Clone)]
pub struct ClientRingConfig {
    /// Capacity of each ring file in bytes.
    pub cap_bytes: u64,
    /// Maximum single-message frame size in bytes.
    pub max_msg: u32,
}

impl Default for ClientRingConfig {
    fn default() -> Self {
        Self {
            cap_bytes: 16 * 1024 * 1024,
            max_msg: 4 * 1024 * 1024,
        }
    }
}

/// M5: knob for the service-side output ring caps. Default sized for
/// production; tests can shrink it to force the dispatcher's skip path.
#[derive(Debug, Clone)]
pub struct ServiceRingConfig {
    /// Capacity (slot-region bytes) for `service/output.ring` and
    /// `service/output_resp.ring`. Power of 2 ≥ RECORD_ALIGN.
    pub output_cap_bytes: u64,
    /// Max single message size on the output rings.
    pub output_max_msg: u32,
    /// Max apply entries published before awaiting responses (apply pipeline depth).
    /// Bounds in-flight apply frames so the apply/apply_resp rings never overflow.
    /// Must be <= the apply ring's frame capacity. Default 256.
    pub apply_pipeline_depth: usize,
}

impl Default for ServiceRingConfig {
    fn default() -> Self {
        Self {
            output_cap_bytes: 16 * 1024 * 1024,
            output_max_msg: 4 * 1024 * 1024,
            apply_pipeline_depth: 256,
        }
    }
}

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
    /// Sizing for the three client-facing ring files. Defaults to 16 MiB /
    /// 4 MiB. Override in tests to force wrap-path coverage.
    pub client_rings: ClientRingConfig,
    /// Sizing for the service-side output ring files. Defaults to 16 MiB /
    /// 4 MiB. Override in tests to force the dispatcher's skip path.
    pub service_rings: ServiceRingConfig,
    /// Durability for the Raft log journal. `Eventual` (recommended/default) acks
    /// an append at the page-cache write, with fsync off the commit critical path
    /// — durability via quorum replication; survives process crash, **not**
    /// simultaneous quorum power loss (Aeron `fileSyncLevel=0`). `Consistent`
    /// fsyncs before ack (power-loss safe; Aeron `fileSyncLevel>=1`). Aeron sync
    /// levels 1 and 2 both map to `Consistent` (the journal fsyncs data+metadata).
    pub log_durability: ultima_journal::Durability,
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
    /// Max entries packed into one AppendEntries replication RPC. openraft
    /// default is 300; raising it amortizes the quorum round-trip + per-batch
    /// follower fsync over more entries (the 3-node throughput lever).
    pub max_payload_entries: u64,
}

impl Default for RaftTuning {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 250,
            election_timeout_min_ms: 1000,
            election_timeout_max_ms: 2000,
            max_in_snapshot_log_to_keep: 1000,
            snapshot_policy_logs_since_last: 5000,
            max_payload_entries: 300,
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
