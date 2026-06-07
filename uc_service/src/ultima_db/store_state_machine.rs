//! [`StateMachine`] implementation backed by [`ultima_db::Store`].
//!
//! # log_index ↔ ultima_db version
//!
//! Every `apply` opens a write transaction pinned to `log_index`
//! (`store.begin_write(Some(log_index))`), so the ultima_db version space and
//! the Raft log_index space stay in lockstep. After all committed writes for
//! log_index N succeed, `store.latest_version() == N`, and
//! [`StateMachine::last_applied`] returns `Some(N)`.
//!
//! This is the property that lets the framework recover deterministically:
//! after a crash, the framework loads the durable `last_applied` and the
//! user's `Store` advertises the same version, so the cross-check in
//! `AdaptedStateMachine::new` passes.
//!
//! # Snapshot path
//!
//! `freeze` / `stream_snapshot` / `install_snapshot` delegate to ultima_db's
//! `snapshot_stream` / `install_snapshot_stream`, which use the wire format
//! documented in `ultima_db/docs/tasks/task27_snapshot_stream.md`. Both
//! methods require the `persistence` Cargo feature on `ultima-db` (already
//! enabled via the workspace dep).
//!
//! # Errors in `apply`
//!
//! `apply` is sync, deterministic, and on the critical path; the SMR
//! contract has no "retryable apply" notion. If `begin_write` or `commit`
//! fails the store is corrupt and the service must abort — we panic via
//! `expect` rather than swallow the error. The framework will detect the
//! crash through the heartbeat watcher.

use std::io::{Read, Write};

use serde::{Serialize, de::DeserializeOwned};
use ultima_db::{ReadTx, SnapshotReader, Store, WriteTx};

use crate::error::SnapshotError;
use crate::state_machine::StateMachine;

/// Boxed closure invoked from `StateMachine::apply`.
pub type ApplyFn<C, R> = Box<dyn Fn(&mut WriteTx, C) -> R + Send + Sync>;

/// Boxed closure invoked from `StateMachine::query`.
pub type QueryFn<Q, QR> = Box<dyn Fn(&ReadTx, Q) -> QR + Send + Sync>;

/// Adapter that turns an [`ultima_db::Store`] + user-supplied apply/query
/// closures into a [`StateMachine`].
///
/// Construct via [`StoreStateMachine::builder`].
pub struct StoreStateMachine<C, R, Q, QR>
where
    C: Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    Q: Serialize + DeserializeOwned + Send + Sync + 'static,
    QR: Serialize + DeserializeOwned + Send + 'static,
{
    pub(crate) store: Store,
    pub(crate) apply_fn: ApplyFn<C, R>,
    pub(crate) query_fn: QueryFn<Q, QR>,
}

impl<C, R, Q, QR> StateMachine for StoreStateMachine<C, R, Q, QR>
where
    C: Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    Q: Serialize + DeserializeOwned + Send + Sync + 'static,
    QR: Serialize + DeserializeOwned + Send + 'static,
{
    type Command = C;
    type Response = R;
    type Query = Q;
    type QueryResponse = QR;
    type SnapshotHandle = SnapshotReader;

    fn apply(&mut self, log_index: u64, cmd: Self::Command) -> Self::Response {
        let mut tx = self
            .store
            .begin_write(Some(log_index))
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
        // ultima_db's freshly-initialized store reports version 0 even when
        // no writes have happened. Treat 0 as "fresh" so the framework's
        // None/Some(0) cross-check matches.
        if v == 0 { None } else { Some(v) }
    }

    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), SnapshotError> {
        let v = self.store.latest_version();
        let reader = self
            .store
            .snapshot_stream(Some(v))
            .map_err(|e| SnapshotError::Codec(format!("snapshot_stream: {e}")))?;
        Ok((reader, v))
    }

    fn stream_snapshot(mut handle: Self::SnapshotHandle, dst: &mut dyn Write)
        -> Result<(), SnapshotError> {
        std::io::copy(&mut handle, dst)?;
        Ok(())
    }

    fn install_snapshot(&mut self, src: &mut dyn Read) -> Result<u64, SnapshotError> {
        self.store
            .install_snapshot_stream(src, ultima_db::InstallOptions::default())
            .map_err(|e| SnapshotError::Codec(format!("install_snapshot_stream: {e}")))?;
        self.store
            .checkpoint()
            .map_err(|e| SnapshotError::Codec(format!("checkpoint: {e}")))?;
        Ok(self.store.latest_version())
    }
}
