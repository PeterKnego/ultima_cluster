//! `RaftStateMachine` adapter — bridges openraft's storage-v2 state-machine
//! contract to the user's [`uc_service::StateMachine`] trait.
//!
//! M1 (embedded mode): calls user's `StateMachine::apply` directly inside the
//! adapter, under a tokio Mutex serializing access. M3 will replace the direct
//! call with a publish to `service/apply.ring` (shared-memory ring), but the
//! adapter shape stays the same.
//!
//! Implementation notes (openraft 0.9.24):
//!   * Trait uses `#[add_async_trait]` (native async fn) — no `#[async_trait]`
//!     attribute on the impl.
//!   * `C::D` and `C::R` are both `bytes::Bytes` (see `super::TypeConfig`). So
//!     `EntryPayload::Normal(d)` carries the user's bincode-encoded `Command`,
//!     and we return bincode-encoded `Response` bytes.
//!   * `C::SnapshotData = std::io::Cursor<Vec<u8>>` — tokio implements its
//!     async-IO traits for `std::io::Cursor<Vec<u8>>`, so the bound is
//!     satisfied without a wrapper.
//!   * Errors from decode/encode/apply paths are wrapped via
//!     `StorageIOError::apply(log_id, &io_err)`. Snapshot build/install errors
//!     use `write_snapshot` / `read_snapshot` respectively (signature=None
//!     because we don't have a snapshot-id at the failure point).

use std::io::Cursor;
use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot, SnapshotMeta};
use openraft::{EntryPayload, LogId, StorageError, StorageIOError, StoredMembership};
use tokio::sync::Mutex;

use uc_service::StateMachine;

use super::{NodeAddr, NodeId, TypeConfig};

/// Adapter from openraft's `RaftStateMachine` to the user-supplied
/// `uc_service::StateMachine`.
pub struct AdaptedStateMachine<S: StateMachine> {
    pub(crate) inner: Arc<Mutex<Inner<S>>>,
}

pub(crate) struct Inner<S: StateMachine> {
    pub(crate) sm: S,
    pub(crate) last_applied: Option<LogId<NodeId>>,
    pub(crate) last_membership: StoredMembership<NodeId, NodeAddr>,
    pub(crate) snapshot_idx: u64,
    pub(crate) current_snapshot: Option<StoredSnapshot>,
}

#[derive(Clone)]
pub(crate) struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, NodeAddr>,
    pub data: Vec<u8>,
}

impl<S: StateMachine> AdaptedStateMachine<S> {
    pub fn new(sm: S) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                sm,
                last_applied: None,
                last_membership: StoredMembership::default(),
                snapshot_idx: 0,
                current_snapshot: None,
            })),
        }
    }
}

impl<S: StateMachine> Clone for AdaptedStateMachine<S> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<S: StateMachine> AdaptedStateMachine<S> {
    /// Run a closure against the locked applied state.
    /// Used by embedded `NodeHandle::query_snapshot` (M1).
    /// M3 will replace this with typed Query types over a shmem ring.
    pub async fn with_state<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&S) -> R + Send,
        R: Send,
    {
        let g = self.inner.lock().await;
        f(&g.sm)
    }
}

// ---------------------------------------------------------------------------
// RaftStateMachine impl
// ---------------------------------------------------------------------------

impl<S: StateMachine> RaftStateMachine<TypeConfig> for AdaptedStateMachine<S> {
    type SnapshotBuilder = AdaptedSnapshotBuilder<S>;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (Option<LogId<NodeId>>, StoredMembership<NodeId, NodeAddr>),
        StorageError<NodeId>,
    > {
        let g = self.inner.lock().await;
        Ok((g.last_applied, g.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<bytes::Bytes>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut g = self.inner.lock().await;
        let mut responses = Vec::new();

        for entry in entries {
            let log_id = entry.log_id;
            let log_index = log_id.index;
            g.last_applied = Some(log_id);

            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(bytes::Bytes::new());
                }
                EntryPayload::Normal(cmd_bytes) => {
                    let (cmd, _) = bincode::serde::decode_from_slice::<S::Command, _>(
                        cmd_bytes.as_ref(),
                        bincode::config::standard(),
                    )
                    .map_err(|e| {
                        let io_err = std::io::Error::other(e.to_string());
                        StorageIOError::<NodeId>::apply(log_id, &io_err)
                    })?;

                    let resp = g.sm.apply(log_index, cmd);

                    let resp_bytes = bincode::serde::encode_to_vec(
                        &resp,
                        bincode::config::standard(),
                    )
                    .map_err(|e| {
                        let io_err = std::io::Error::other(e.to_string());
                        StorageIOError::<NodeId>::apply(log_id, &io_err)
                    })?;
                    responses.push(bytes::Bytes::from(resp_bytes));
                }
                EntryPayload::Membership(m) => {
                    g.last_membership = StoredMembership::new(Some(log_id), m);
                    responses.push(bytes::Bytes::new());
                }
            }
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        AdaptedSnapshotBuilder { inner: self.inner.clone() }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, NodeAddr>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = snapshot.into_inner();
        let mut g = self.inner.lock().await;

        let mut cursor = Cursor::new(bytes.clone());
        let user_last_applied = g.sm.install_snapshot(&mut cursor).map_err(|e| {
            let io_err = std::io::Error::other(e.to_string());
            StorageIOError::<NodeId>::read_snapshot(Some(meta.signature()), &io_err)
        })?;

        // Sanity check: user's reported last_applied after install must match
        // the openraft-supplied meta.last_log_id.index. Mismatch = user's
        // install_snapshot is buggy or the wire bytes don't match the meta.
        let meta_index = meta.last_log_id.map(|l| l.index).unwrap_or(0);
        debug_assert_eq!(
            user_last_applied,
            meta_index,
            "user install_snapshot returned index {user_last_applied} but meta.last_log_id.index is {meta_index}",
        );

        g.last_applied = meta.last_log_id;
        g.last_membership = meta.last_membership.clone();
        g.current_snapshot = Some(StoredSnapshot { meta: meta.clone(), data: bytes });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let g = self.inner.lock().await;
        match &g.current_snapshot {
            Some(s) => Ok(Some(Snapshot {
                meta: s.meta.clone(),
                snapshot: Box::new(Cursor::new(s.data.clone())),
            })),
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// RaftSnapshotBuilder impl
// ---------------------------------------------------------------------------

pub struct AdaptedSnapshotBuilder<S: StateMachine> {
    inner: Arc<Mutex<Inner<S>>>,
}

impl<S: StateMachine> RaftSnapshotBuilder<TypeConfig> for AdaptedSnapshotBuilder<S> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let mut g = self.inner.lock().await;
        let last_applied = g.last_applied;
        let last_membership = g.last_membership.clone();

        let mut buf: Vec<u8> = Vec::new();
        let user_index = g.sm.build_snapshot(&mut buf).map_err(|e| {
            let io_err = std::io::Error::other(e.to_string());
            StorageIOError::<NodeId>::write_snapshot(None, &io_err)
        })?;

        // Sanity check: with the Mutex held across apply and build, the user's returned
        // index MUST equal our last_applied.index. M3 moves apply to a ring buffer and
        // this invariant will need to be re-evaluated.
        debug_assert_eq!(
            user_index,
            last_applied.map(|l| l.index).unwrap_or(0),
            "user build_snapshot returned index {user_index} but adapter has last_applied {:?}",
            last_applied
        );

        g.snapshot_idx += 1;
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id: format!("snap-{}", g.snapshot_idx),
        };
        let stored = StoredSnapshot { meta: meta.clone(), data: buf.clone() };
        g.current_snapshot = Some(stored);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(buf)),
        })
    }
}
