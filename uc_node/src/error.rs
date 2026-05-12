use std::io;
use thiserror::Error;

use uc_service::{OutputError, SnapshotError};

#[derive(Debug, Error)]
pub enum ClusterError {
    #[error("config: {0}")]
    Config(String),
    #[error("recovery: {0}")]
    Recovery(String),
    #[error("state drift: user last_applied={user:?} but framework last_applied={framework:?}")]
    DriftDetected {
        user: Option<u64>,
        framework: Option<u64>,
    },
    #[error("journal: {0}")]
    Journal(#[from] ultima_journal::JournalError),
    #[error("stable value: {0}")]
    StableValue(#[from] ultima_journal::StableValueError),
    #[error("snapshot: {0}")]
    Snapshot(#[from] SnapshotError),
    #[error("raft: {0}")]
    Raft(String),
    #[error("not leader; current leader: {leader_id:?}")]
    NotLeader { leader_id: Option<u64> },
    #[error("output: {0}")]
    Output(#[from] OutputError),
    #[error("shut down")]
    ShutDown,
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("network: {0}")]
    Network(#[from] crate::network::NetworkError),
    #[error("ipc: {0}")]
    Ipc(#[from] crate::ipc::IpcError),
    #[error("bincode: {0}")]
    Bincode(String),
}

impl From<bincode::error::EncodeError> for ClusterError {
    fn from(e: bincode::error::EncodeError) -> Self {
        Self::Bincode(e.to_string())
    }
}
impl From<bincode::error::DecodeError> for ClusterError {
    fn from(e: bincode::error::DecodeError) -> Self {
        Self::Bincode(e.to_string())
    }
}
