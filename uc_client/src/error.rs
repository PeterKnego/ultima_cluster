use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("not connected")]
    NotConnected,
    #[error("app_id mismatch")]
    AppIdMismatch,
    #[error("protocol mismatch")]
    ProtocolMismatch,
    #[error("node stalled")]
    NodeStalled,
    #[error("service stalled")]
    ServiceStalled,
    #[error("not leader; hint: {hint:?}")]
    NotLeader { hint: Option<u64> },
    #[error("submission: {0}")]
    Submission(String),
    #[error("decode: {0}")]
    Decode(String),
    #[error("timeout after {0:?}")]
    Timeout(Duration),
}
