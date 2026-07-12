// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The service-side SDK traits (M5 spec §7). The user implements
//! [`StateMachine`] (and optionally [`OutputHandler`]); the framework drives
//! them off the committed log.
//!
//! v2 differences from the v1 `uc_service` traits: apply is keyed by an
//! absolute byte **`position`** (the v2 log_index analog and the idempotency
//! key), not a log index; there are no snapshot methods (M5 reconstruction
//! replays the log instead — Task 9).

/// The user's deterministic business logic.
///
/// **`apply` is sync, deterministic, no I/O, no clock, no randomness.** The
/// signature enforces it: a `&mut self` transition with no `async`, no context
/// handle. This is non-negotiable for state-machine-replication correctness —
/// every replica must reach the same state from the same committed log.
pub trait StateMachine: Send + 'static {
    type Command: serde::Serialize + serde::de::DeserializeOwned + Send + 'static;
    type Response: serde::Serialize + serde::de::DeserializeOwned + Send + 'static;
    type Query: serde::Serialize + serde::de::DeserializeOwned + Send + 'static;
    type QueryResponse: serde::Serialize + serde::de::DeserializeOwned + Send + 'static;

    /// Apply one committed command. `position` is the frame's absolute byte
    /// position (the v2 log_index analog and the natural idempotency key).
    fn apply(&mut self, position: u64, cmd: Self::Command) -> Self::Response;

    /// Answer a read. Same method whether the framework routes it linearizable
    /// or snapshot (Task 11) — the IPC boundary carries typed queries, not
    /// closures.
    fn query(&self, q: Self::Query) -> Self::QueryResponse;

    /// Position of the last applied frame; `None` = fresh (nothing applied).
    /// Under-reporting is safe (the apply loop's idempotent-skip re-applies
    /// nothing already seen); over-reporting above the journal frontier is
    /// refused at attach ([`ServiceError::Drift`](crate::ServiceError::Drift)).
    fn last_applied(&self) -> Option<u64>;
}

/// Optional leader-only, at-least-once side-effect handler (Task 12 wires the
/// output agent; defined here so the builder's default type parameter exists).
#[allow(async_fn_in_trait)]
pub trait OutputHandler<S: StateMachine>: Send + 'static {
    /// Run after a command commits, on the leader only. `Retryable` is retried
    /// while still leader; `Permanent` advances the progress marker anyway.
    async fn on_committed(
        &self,
        position: u64,
        cmd: &S::Command,
        state: &S,
    ) -> Result<(), OutputError>;
}

/// The default no-op output handler (no side effects).
pub struct NoopOutput;

impl<S: StateMachine> OutputHandler<S> for NoopOutput {
    async fn on_committed(
        &self,
        _position: u64,
        _cmd: &S::Command,
        _state: &S,
    ) -> Result<(), OutputError> {
        Ok(())
    }
}

/// Why an `on_committed` side effect did not complete.
#[derive(Debug, Clone, thiserror::Error)]
pub enum OutputError {
    #[error("retryable: {0}")]
    Retryable(String),
    #[error("permanent: {0}")]
    Permanent(String),
}
