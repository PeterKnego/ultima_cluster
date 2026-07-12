// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Reference [`StateMachine`] + [`SnapshotStateMachine`] adapter backed by an
//! [`ultima_db::Store`] (Cargo feature `ultima_db`).
//!
//! # position ↔ ultima_db version (the position-as-version invariant)
//!
//! In UC v2 the log-index analog is an absolute **byte position** in the log:
//! positions are *sparse*, *strictly-increasing* `u64`s (a frame at byte 96,
//! the next at 192, a later one at 4096 — gaps are the norm, not 1,2,3). This
//! adapter keeps the ultima_db version space in lockstep with that position
//! space:
//!
//! - Every [`apply`](StateMachine::apply) opens its write transaction pinned to
//!   the frame's `position` (`store.begin_write(Some(position))`), so after the
//!   commit `store.latest_version() == position`.
//! - [`last_applied`](StateMachine::last_applied) returns that version (with
//!   `0 ⇒ None` for a fresh store).
//! - [`install_snapshot`](SnapshotStateMachine::install_snapshot) lands the
//!   restored snapshot at exactly the artifact's tagged position `S`
//!   (`InstallOptions { commit_version: Some(S), .. }` — the sibling
//!   `ultima_db` change that honors this option).
//!
//! ultima_db accepts these gapped versions: `begin_write(Some(v))` for a `v`
//! far above `latest_version` is legal and simply advances `latest`/`next` to
//! `v`/`v+1` (see the `apply_pins_sparse_positions` test below). That is what
//! makes byte positions usable directly as the version key.
//!
//! # Snapshot path
//!
//! `freeze` / `stream_snapshot` / `install_snapshot` delegate to ultima_db's
//! `snapshot_stream` / `install_snapshot_stream` (wire format documented in
//! `ultima_db/docs/tasks/task27_snapshot_stream.md`), both requiring the
//! `persistence` feature on `ultima-db` (carried by the workspace dep). The
//! adapter assumes a **persisted** store (e.g. `Persistence::smr(dir)`):
//! `install_snapshot` runs `checkpoint()` after the install, which errors
//! under `Persistence::None`.
//!
//! # Errors in `apply`
//!
//! `apply` is sync, deterministic, and on the critical path; the SMR contract
//! has no "retryable apply". If `begin_write`/`commit` fails the store is
//! corrupt and the service must abort — we panic via `expect` rather than
//! swallow the error (the node detects the death via the heartbeat watcher).

use std::io::{Read, Write};

use serde::{Serialize, de::DeserializeOwned};
use ultima_db::{InstallOptions, OnExtra, ReadTx, SnapshotReader, Store, WriteTx};

use crate::config::SnapshotError;
use crate::traits::{SnapshotStateMachine, StateMachine};

/// Boxed closure invoked from [`StateMachine::apply`]. Sees the open `WriteTx`
/// pinned to the frame's byte `position`; commits automatically after it
/// returns.
pub type ApplyFn<C, R> = Box<dyn Fn(&mut WriteTx, C) -> R + Send>;

/// Boxed closure invoked from [`StateMachine::query`]. Sees a `ReadTx` over the
/// latest committed version.
pub type QueryFn<Q, QR> = Box<dyn Fn(&ReadTx, Q) -> QR + Send>;

/// Adapter turning an [`ultima_db::Store`] + user apply/query closures into a
/// snapshot-capable [`StateMachine`]. Construct via [`StoreStateMachine::builder`].
pub struct StoreStateMachine<C, R, Q, QR>
where
    C: Serialize + DeserializeOwned + Send + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    Q: Serialize + DeserializeOwned + Send + 'static,
    QR: Serialize + DeserializeOwned + Send + 'static,
{
    pub(crate) store: Store,
    pub(crate) apply_fn: ApplyFn<C, R>,
    pub(crate) query_fn: QueryFn<Q, QR>,
}

impl<C, R, Q, QR> StateMachine for StoreStateMachine<C, R, Q, QR>
where
    C: Serialize + DeserializeOwned + Send + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    Q: Serialize + DeserializeOwned + Send + 'static,
    QR: Serialize + DeserializeOwned + Send + 'static,
{
    type Command = C;
    type Response = R;
    type Query = Q;
    type QueryResponse = QR;

    fn apply(&mut self, position: u64, cmd: Self::Command) -> Self::Response {
        let mut tx = self
            .store
            .begin_write(Some(position))
            .expect("ultima_db begin_write");
        let resp = (self.apply_fn)(&mut tx, cmd);
        tx.commit().expect("ultima_db commit");
        resp
    }

    fn query(&self, q: Self::Query) -> Self::QueryResponse {
        let tx = self.store.begin_read(None).expect("ultima_db begin_read");
        (self.query_fn)(&tx, q)
    }

    fn last_applied(&self) -> Option<u64> {
        let v = self.store.latest_version();
        // A freshly-initialized store reports version 0 with nothing applied.
        // Treat 0 as "fresh" so the framework's None/Some(0) cross-check matches.
        if v == 0 { None } else { Some(v) }
    }
}

impl<C, R, Q, QR> SnapshotStateMachine for StoreStateMachine<C, R, Q, QR>
where
    C: Serialize + DeserializeOwned + Send + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    Q: Serialize + DeserializeOwned + Send + 'static,
    QR: Serialize + DeserializeOwned + Send + 'static,
{
    type SnapshotHandle = SnapshotReader;

    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), SnapshotError> {
        let v = self.store.latest_version();
        let reader = self
            .store
            .snapshot_stream(Some(v))
            .map_err(|e| SnapshotError::Codec(format!("snapshot_stream: {e}")))?;
        Ok((reader, v))
    }

    fn stream_snapshot(
        mut handle: Self::SnapshotHandle,
        dst: &mut dyn Write,
    ) -> Result<(), SnapshotError> {
        std::io::copy(&mut handle, dst)?;
        Ok(())
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn Read,
    ) -> Result<u64, SnapshotError> {
        // Pin the installed snapshot to the artifact's tagged position `S`, so
        // the version space stays in lockstep with the log position space.
        //
        // `OnExtra::Drop` = strict replace semantics: the trait contract is
        // "replace the state wholesale", so a destination table absent from
        // the incoming snapshot (a divergent prior life) must NOT survive the
        // install — exactly the Raft `InstallSnapshot` mode ultima_db
        // documents `Drop` for.
        self.store
            .install_snapshot_stream(
                src,
                InstallOptions {
                    on_extra_tables: OnExtra::Drop,
                    commit_version: Some(position),
                    ..InstallOptions::default()
                },
            )
            .map_err(|e| SnapshotError::Codec(format!("install_snapshot_stream: {e}")))?;
        self.store
            .checkpoint()
            .map_err(|e| SnapshotError::Codec(format!("checkpoint: {e}")))?;
        // Belt-and-suspenders: the sibling `commit_version` honoring must have
        // landed us at exactly `position`.
        let v = self.store.latest_version();
        if v != position {
            return Err(SnapshotError::Codec(format!(
                "install landed at version {v}, expected position {position}"
            )));
        }
        Ok(v)
    }
}

// --------------------------------------------------------------------- builder

impl<C, R, Q, QR> StoreStateMachine<C, R, Q, QR>
where
    C: Serialize + DeserializeOwned + Send + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    Q: Serialize + DeserializeOwned + Send + 'static,
    QR: Serialize + DeserializeOwned + Send + 'static,
{
    /// Start a fluent builder. The `store` must already be opened (with its
    /// tables registered) by the caller; the adapter does not own `StoreConfig`.
    pub fn builder(store: Store) -> StoreStateMachineBuilder<C, R, Q, QR> {
        StoreStateMachineBuilder { store, apply_fn: None, query_fn: None }
    }
}

/// Fluent builder for [`StoreStateMachine`].
pub struct StoreStateMachineBuilder<C, R, Q, QR>
where
    C: Serialize + DeserializeOwned + Send + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    Q: Serialize + DeserializeOwned + Send + 'static,
    QR: Serialize + DeserializeOwned + Send + 'static,
{
    store: Store,
    apply_fn: Option<ApplyFn<C, R>>,
    query_fn: Option<QueryFn<Q, QR>>,
}

impl<C, R, Q, QR> StoreStateMachineBuilder<C, R, Q, QR>
where
    C: Serialize + DeserializeOwned + Send + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    Q: Serialize + DeserializeOwned + Send + 'static,
    QR: Serialize + DeserializeOwned + Send + 'static,
{
    /// Closure invoked from [`StateMachine::apply`].
    pub fn apply_fn<F>(mut self, f: F) -> Self
    where
        F: Fn(&mut WriteTx, C) -> R + Send + 'static,
    {
        self.apply_fn = Some(Box::new(f));
        self
    }

    /// Closure invoked from [`StateMachine::query`].
    pub fn query_fn<F>(mut self, f: F) -> Self
    where
        F: Fn(&ReadTx, Q) -> QR + Send + 'static,
    {
        self.query_fn = Some(Box::new(f));
        self
    }

    pub fn build(self) -> Result<StoreStateMachine<C, R, Q, QR>, BuildError> {
        Ok(StoreStateMachine {
            store: self.store,
            apply_fn: self.apply_fn.ok_or(BuildError::MissingApplyFn)?,
            query_fn: self.query_fn.ok_or(BuildError::MissingQueryFn)?,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("missing apply_fn")]
    MissingApplyFn,
    #[error("missing query_fn")]
    MissingQueryFn,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_fns_rejected() {
        let a = StoreStateMachine::<u8, u8, u8, u8>::builder(Store::default())
            .query_fn(|_tx, q| q)
            .build();
        assert!(matches!(a, Err(BuildError::MissingApplyFn)));
        let b = StoreStateMachine::<u8, u8, u8, u8>::builder(Store::default())
            .apply_fn(|_tx, c| c)
            .build();
        assert!(matches!(b, Err(BuildError::MissingQueryFn)));
    }

    /// The position-as-version invariant: `begin_write(Some(v))` accepts sparse,
    /// gapped byte positions, and `last_applied()` tracks the latest of them.
    #[test]
    fn apply_pins_sparse_positions() {
        let mut sm = StoreStateMachine::<u32, u32, u32, u32>::builder(Store::default())
            .apply_fn(|_tx, cmd| cmd.wrapping_add(1))
            .query_fn(|_tx, q| q.wrapping_mul(2))
            .build()
            .expect("build");

        assert_eq!(sm.last_applied(), None, "fresh store");

        // Sparse positions with large gaps — not 1,2,3.
        assert_eq!(sm.apply(96, 41), 42);
        assert_eq!(sm.last_applied(), Some(96));
        assert_eq!(sm.apply(4096, 100), 101);
        assert_eq!(sm.last_applied(), Some(4096), "gapped positions accepted");

        // Reads see the latest committed version (no panic).
        assert_eq!(sm.query(5), 10);
    }
}
