use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("snapshot io: {0}")]
    Io(#[from] io::Error),
    #[error("snapshot codec: {0}")]
    Codec(String),
}

#[derive(Debug, Error)]
pub enum OutputError {
    #[error("retryable: {0}")]
    Retryable(String),
    #[error("permanent: {0}")]
    Permanent(String),
}

impl OutputError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, OutputError::Retryable(_))
    }
}
