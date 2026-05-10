use std::net::SocketAddr;
use std::path::PathBuf;

pub type NodeId = u64;

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: NodeId,
    pub data_dir: PathBuf,
    pub raft_listen_addr: SocketAddr,        // unused in M1 (no QUIC yet); reserved
    pub app_id: String,
    pub bootstrap: BootstrapConfig,
    pub raft: RaftTuning,
}

#[derive(Debug, Clone)]
pub enum BootstrapConfig {
    Resume,
    SingleNode,
    Peers { peers: Vec<PeerSeed> },          // unused in M1
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
}

impl Default for RaftTuning {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 250,
            election_timeout_min_ms: 1000,
            election_timeout_max_ms: 2000,
            max_in_snapshot_log_to_keep: 1000,
        }
    }
}

impl NodeConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.app_id.is_empty() { return Err("app_id must not be empty".into()); }
        if self.app_id.len() > 64 { return Err("app_id must be <= 64 bytes".into()); }
        if self.raft.election_timeout_min_ms >= self.raft.election_timeout_max_ms {
            return Err("election_timeout_min_ms must be < max".into());
        }
        Ok(())
    }
}
