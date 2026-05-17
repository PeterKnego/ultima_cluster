use std::io;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    // ── Connect-time ────────────────────────────────────────────────────
    #[error("not connected")]
    NotConnected,
    #[error("app_id mismatch: expected {expected:?}, got {actual:?}")]
    AppIdMismatch { expected: String, actual: String },
    #[error("protocol version mismatch: local={local}, node={node}")]
    ProtocolMismatch { local: u32, node: u32 },
    #[error("node restarted since previous connect: previous={previous:x}, current={current:x}")]
    InstanceRestart { previous: u128, current: u128 },
    #[error("session file create/mmap: {0}")]
    SessionCreate(#[from] io::Error),

    // ── Steady-state ────────────────────────────────────────────────────
    #[error("not leader; hint: {hint:?}")]
    NotLeader { hint: Option<u64> },
    #[error("node stalled")]
    NodeStalled,
    #[error("service stalled")]
    ServiceStalled,
    #[error("timeout after {0:?}")]
    Timeout(Duration),
    #[error("response was overwritten by broadcast lap")]
    ResponseOverwritten,
    #[error("submit ring full past grace period")]
    BackpressureFull,
    #[error("submit: {0}")]
    Submission(String),
    #[error("decode: {0}")]
    Decode(String),
    #[error("client shut down")]
    ShutDown,
}

impl From<bincode::error::EncodeError> for ClientError {
    fn from(e: bincode::error::EncodeError) -> Self {
        Self::Decode(format!("encode: {e}"))
    }
}
impl From<bincode::error::DecodeError> for ClientError {
    fn from(e: bincode::error::DecodeError) -> Self {
        Self::Decode(e.to_string())
    }
}
