use async_trait::async_trait;

pub use uc_protocol::frames::output::OutputError;
use crate::state_machine::StateMachine;

/// Leader-only post-commit hook. At-least-once delivery via durable progress
/// marker on the node side; user MUST make on_committed idempotent.
/// log_index is the natural idempotency key.
#[async_trait]
pub trait OutputHandler<S: StateMachine>: Send + Sync + 'static {
    async fn on_committed(
        &self,
        log_index: u64,
        cmd: &S::Command,
        state: &S,
    ) -> Result<(), OutputError>;
}

pub struct NoopOutput;

#[async_trait]
impl<S: StateMachine> OutputHandler<S> for NoopOutput {
    async fn on_committed(
        &self,
        _log_index: u64,
        _cmd: &S::Command,
        _state: &S,
    ) -> Result<(), OutputError> {
        Ok(())
    }
}
