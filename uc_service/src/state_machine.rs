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
///   * build_snapshot returns the log_index its bytes represent (resolves
///     the build-vs-apply race).
///   * install_snapshot returns the new last_applied after a successful install.
pub trait StateMachine: Send + Sync + 'static {
    type Command: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Response: Serialize + DeserializeOwned + Send + 'static;
    type Query: Serialize + DeserializeOwned + Send + Sync + 'static;
    type QueryResponse: Serialize + DeserializeOwned + Send + 'static;

    fn apply(&mut self, log_index: u64, cmd: Self::Command) -> Self::Response;

    fn query(&self, q: Self::Query) -> Self::QueryResponse;

    fn last_applied(&self) -> Option<u64>;

    fn build_snapshot(&self, dst: &mut dyn Write) -> Result<u64, SnapshotError>;

    fn install_snapshot(&mut self, src: &mut dyn Read) -> Result<u64, SnapshotError>;
}
