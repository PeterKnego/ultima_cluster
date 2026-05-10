use std::io;
use thiserror::Error;

use uc_service::{OutputError, SnapshotError};

#[derive(Debug, Error)]
pub enum ClusterError {
    #[error("config: {0}")]
    Config(String),
    #[error("recovery: {0}")]
    Recovery(String),
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
