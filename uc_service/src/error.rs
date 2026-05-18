use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("snapshot io: {0}")]
    Io(#[from] io::Error),
    #[error("snapshot codec: {0}")]
    Codec(String),
}
