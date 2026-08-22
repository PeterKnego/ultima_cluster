// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The service-side SDK traits (M5 spec §7, M12a spec §3.1). The user
//! implements [`StateMachine`] (typed) or [`RawStateMachine`] (bytes-in/
//! bytes-out, the raw tier) — exactly one of the two — and optionally
//! [`OutputHandler`] / [`RawOutputHandler`]; the framework drives them off
//! the committed log.
//!
//! v2 differences from the v1 `uc_service` traits: apply is keyed by an
//! absolute byte **`position`** (the v2 log_index analog and the idempotency
//! key), not a log index; there are no snapshot methods (M5 reconstruction
//! replays the log instead — Task 9).
//!
//! M6 adds one OPTIONAL capability trait, [`SnapshotStateMachine`], for SMs that
//! can serialize/restore their full state (what gates log purge). Existing SMs
//! that don't implement it are untouched.

use crate::config::SnapshotError;

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

/// The core state-machine contract: bytes in, bytes out. The framework hands
/// `apply` the committed frame payload exactly as it sits in the log buffer
/// and reuses `out` across calls — no decode, no allocation in steady state.
/// Implement this directly for SBE / flatbuffers / hand-laid frames; or
/// implement [`StateMachine`] (typed, serde + bincode) and get this for free
/// via the blanket impl below. A type implements ONE of the two.
pub trait RawStateMachine: Send + 'static {
    /// Apply the committed command at `position` (the absolute log byte
    /// offset, the idempotency key). Write the response bytes into `out`
    /// (cleared by the caller). Deterministic, sync, no I/O.
    fn apply(&mut self, position: u64, cmd: &[u8], out: &mut Vec<u8>);
    /// Answer a read. `out` is cleared by the caller.
    fn query(&self, q: &[u8], out: &mut Vec<u8>);
    /// Highest position applied so far (`None` before the first).
    fn last_applied(&self) -> Option<u64>;
}

/// Every typed state machine is a raw one: decode with bincode-standard,
/// apply, encode the response with bincode-standard — exactly the codec the
/// framework used through v2.5.0, so the wire is byte-identical.
impl<S: StateMachine> RawStateMachine for S {
    #[inline]
    fn apply(&mut self, position: u64, cmd: &[u8], out: &mut Vec<u8>) {
        let (cmd, _) = bincode::serde::decode_from_slice::<S::Command, _>(cmd, bincode::config::standard())
            .expect("corrupt committed frame (fail-stop)");
        let resp = StateMachine::apply(self, position, cmd);
        bincode::serde::encode_into_std_write(&resp, out, bincode::config::standard())
            .expect("response bincode-encode (fail-stop)");
    }
    #[inline]
    fn query(&self, q: &[u8], out: &mut Vec<u8>) {
        let (q, _) = bincode::serde::decode_from_slice::<S::Query, _>(q, bincode::config::standard())
            .expect("corrupt query frame (fail-stop)");
        let qr = StateMachine::query(self, q);
        bincode::serde::encode_into_std_write(&qr, out, bincode::config::standard())
            .expect("query-response bincode-encode (fail-stop)");
    }
    #[inline]
    fn last_applied(&self) -> Option<u64> { StateMachine::last_applied(self) }
}

/// Optional capability: state machines that can serialize their full state and
/// restore it wholesale. This is what lets the framework **purge** the log — a
/// deployment whose SM does not implement it never purges (M6; documented), and
/// below-floor reconstruction (Task 5) installs a snapshot instead of replaying
/// from the archive.
///
/// It is a *separate* trait from [`RawStateMachine`] on purpose: v2's base trait
/// carries no snapshot methods (M5 reconstruction replayed the log), so existing
/// SMs are untouched — a snapshot-capable SM opts in by also implementing this.
/// The supertrait is the CORE contract ([`RawStateMachine`]), so a raw-tier SM
/// can be snapshot-capable too; a typed [`StateMachine`] satisfies it through
/// the blanket impl above, so existing implementations are unchanged.
///
/// ## Position-as-version
///
/// The `u64`s here are absolute byte **positions** in the log (the v2 log-index
/// analog), NOT dense 1,2,3 indexes: they are sparse, strictly-increasing
/// artifact tags. `freeze` pins the current position; `install_snapshot` is told
/// the position `S` the artifact was tagged with and must land the restored
/// state exactly there.
pub trait SnapshotStateMachine: RawStateMachine {
    /// An opaque, consistent handle to the frozen state. `Send + 'static` so it
    /// can cross to the off-thread streaming step.
    type SnapshotHandle: Send + 'static;

    /// O(1) consistent pin of the current state; returns `(handle, position)`
    /// where `position == self.last_applied().unwrap_or(0)` at pin time. Called
    /// on the APPLY thread with the SM lock held, so the position cannot move
    /// underneath the pin.
    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), SnapshotError>;

    /// Stream the pinned state to `dst`. Runs OFF the apply thread with NO SM
    /// lock held (the v1 rule) — the handle carries a consistent, immutable view
    /// so concurrent applies cannot corrupt the stream.
    fn stream_snapshot(
        handle: Self::SnapshotHandle,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), SnapshotError>;

    /// Replace the state wholesale from a stream produced by
    /// [`stream_snapshot`](Self::stream_snapshot), landing it at `position`
    /// (the artifact tag `S` — the caller supplies it; the bare byte stream does
    /// not carry it). Returns the post-install position, which MUST equal
    /// `position`. Runs on the apply thread with the SM lock held.
    ///
    /// Deviation from the M6 brief's literal trait block: the brief sketched a
    /// no-argument `install_snapshot(&mut self, src)` that recovered `S` from a
    /// "stream trailer". No such trailer exists in the ULTSNAP wire format, so
    /// the honest shape passes `position` explicitly — the caller already knows
    /// `S` (it is the snapshot artifact's tag).
    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, SnapshotError>;
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

/// Raw-tier output handler: sees the committed command bytes. The typed
/// [`OutputHandler`] is adapted onto this by [`TypedOutput`].
#[allow(async_fn_in_trait)]
pub trait RawOutputHandler<S: RawStateMachine>: Send + 'static {
    async fn on_committed(&self, position: u64, cmd: &[u8], state: &S) -> Result<(), OutputError>;
}

impl<S: RawStateMachine> RawOutputHandler<S> for NoopOutput {
    async fn on_committed(&self, _position: u64, _cmd: &[u8], _state: &S) -> Result<(), OutputError> { Ok(()) }
}

/// Adapts a typed [`OutputHandler`] to the raw tier (one bincode decode per
/// committed command, as the output agent did through v2.5.0).
pub struct TypedOutput<O>(pub O);

impl<S: StateMachine, O: OutputHandler<S>> RawOutputHandler<S> for TypedOutput<O> {
    async fn on_committed(&self, position: u64, cmd: &[u8], state: &S) -> Result<(), OutputError> {
        let (cmd, _) = bincode::serde::decode_from_slice::<S::Command, _>(cmd, bincode::config::standard())
            .expect("corrupt committed frame (fail-stop)");
        self.0.on_committed(position, &cmd, state).await
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
