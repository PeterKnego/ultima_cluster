use serde::{Serialize, de::DeserializeOwned};
use std::io::{Read, Write};

use crate::error::SnapshotError;

/// Deterministic state machine. apply() runs serially on every node; query()
/// runs on the leader (linearizable) or any node (snapshot).
///
/// Invariants the framework relies on:
///   * apply MUST be deterministic (no clocks, no rand, no I/O).
///   * apply MUST be sync — you cannot await across the call.
///   * last_applied() MUST reflect the highest log_index for which apply()
///     completed AND the result is durable.
///   * freeze() returns a consistent point-in-time handle and the log_index it
///     represents (resolves the freeze-vs-apply race). freeze MUST be cheap
///     (O(1) MVCC version pin for a real store; a clone/serialize for trivial
///     SMs) — it is called under a brief lock.
///   * stream_snapshot() streams the frozen handle to `dst`; holds NO SM lock.
///   * install_snapshot returns the new last_applied after a successful install.
pub trait StateMachine: Send + Sync + 'static {
    type Command: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Response: Serialize + DeserializeOwned + Send + 'static;
    type Query: Serialize + DeserializeOwned + Send + Sync + 'static;
    type QueryResponse: Serialize + DeserializeOwned + Send + 'static;

    /// A cheap, consistent point-in-time capture of the SM at its current applied
    /// frontier. Returns (handle, snapshot_index). MUST be cheap (O(1) MVCC version
    /// pin for a real store; a clone/serialize for trivial SMs) — called under a
    /// brief lock. `Send` so the handle streams on a background thread.
    type SnapshotHandle: Send + 'static;

    fn apply(&mut self, log_index: u64, cmd: Self::Command) -> Self::Response;

    fn query(&self, q: Self::Query) -> Self::QueryResponse;

    /// Returns the highest log_index the user's state machine has DURABLY applied.
    /// MUST agree with the framework's persisted `last_applied` at startup.
    ///
    /// The framework cross-checks this method at startup against the durable
    /// `last_applied.state`. Disagreement is treated as data corruption and
    /// surfaced as `ClusterError::DriftDetected`. Allowed exceptions:
    ///   * User says `None` while framework has persisted history — treated as
    ///     "fresh state after install_snapshot, framework value is authoritative."
    ///     Logged at warn level; not an error.
    fn last_applied(&self) -> Option<u64>;

    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), SnapshotError>;

    /// Stream a frozen handle to `dst`. Consumes the handle; holds NO SM lock.
    fn stream_snapshot(handle: Self::SnapshotHandle, dst: &mut dyn Write)
        -> Result<(), SnapshotError>;

    fn install_snapshot(&mut self, src: &mut dyn Read) -> Result<u64, SnapshotError>;
}
