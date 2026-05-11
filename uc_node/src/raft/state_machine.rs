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
use std::path::PathBuf;
use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot, SnapshotMeta};
use openraft::{EntryPayload, LogId, StorageError, StorageIOError, StoredMembership};
use tokio::sync::Mutex;
use ultima_journal::StableValue;

use uc_service::StateMachine;

use super::log_storage::{LogStorageHandles, StoredSnapshotMeta};
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
    pub(crate) current_snapshot: Option<StoredSnapshot>,
    pub(crate) last_applied_sv: Arc<StableValue<LogId<NodeId>>>,
    pub(crate) snapshot_meta_sv: Arc<StableValue<StoredSnapshotMeta>>,
    pub(crate) snapshot_bytes_dir: PathBuf,
}

#[derive(Clone)]
pub(crate) struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, NodeAddr>,
    pub data: Vec<u8>,
}

impl<S: StateMachine> AdaptedStateMachine<S> {
    pub fn new(sm: S, handles: LogStorageHandles) -> Result<Self, crate::ClusterError> {
        // Recover persisted state on startup.
        let loaded_last_applied = handles.last_applied.load().ok().flatten();
        let loaded_snapshot_meta = handles.snapshot_meta.load().ok().flatten();

        let (last_membership, current_snapshot) = match loaded_snapshot_meta {
            Some(meta) => {
                let bytes_path = handles.data_dir.join(&meta.bytes_filename);
                // FAIL-HARD (I1 fix): if snapshot_meta points at a bytes file,
                // that file MUST be present and readable. Silently substituting
                // an empty Vec would let user SMs whose install_snapshot([])
                // returns Ok start in default state while framework claims the
                // snapshot index — silent corruption.
                let bytes = std::fs::read(&bytes_path).map_err(|e| {
                    crate::ClusterError::Recovery(format!(
                        "snapshot_meta points to {bytes_path:?} but read failed: {e}"
                    ))
                })?;
                let openraft_meta = SnapshotMeta {
                    last_log_id: meta.last_log_id,
                    last_membership: meta.last_membership.clone(),
                    snapshot_id: format!("snap-{}", meta.last_log_id.map(|l| l.index).unwrap_or(0)),
                };
                (
                    meta.last_membership,
                    Some(StoredSnapshot {
                        meta: openraft_meta,
                        data: bytes,
                    }),
                )
            }
            None => (StoredMembership::default(), None),
        };

        // If a snapshot exists, install it into the user's state machine so its
        // in-memory state matches the durable snapshot at startup.
        let mut sm = sm;
        if let Some(ref snap) = current_snapshot {
            let mut cursor = std::io::Cursor::new(snap.data.clone());
            // Errors here are NOT recoverable. The journal has been purged
            // below the snapshot index, so there's nothing for openraft to
            // "resync" from. If we silently swallowed the error, the
            // framework would report last_applied = Some(N) while the user's
            // sm sits at default — subsequent applies would corrupt state.
            // Surface as a hard ClusterError::Recovery so the operator sees
            // the cause and the node fails to start.
            sm.install_snapshot(&mut cursor).map_err(|e| {
                crate::ClusterError::Recovery(format!("snapshot replay at startup: {e}"))
            })?;
        }

        // Cross-check user-reported last_applied against framework's durable
        // last_applied. Disagreement = data corruption.
        let user_la = sm.last_applied();
        let framework_la = loaded_last_applied.map(|l| l.index);

        match (user_la, framework_la) {
            (Some(u), Some(f)) if u != f => {
                return Err(crate::ClusterError::DriftDetected {
                    user: Some(u),
                    framework: Some(f),
                });
            }
            (None, Some(_)) => {
                // I2 fix: only tolerate user=None / framework=Some when a
                // snapshot was actually loaded — the user's install_snapshot
                // may not have been able to re-derive last_applied from the
                // bytes alone, so framework's value is authoritative. Without
                // a loaded snapshot, framework claiming index N with the user
                // SM at default is silent corruption: surface as drift.
                if current_snapshot.is_some() {
                    tracing::warn!(
                        framework_last_applied = ?framework_la,
                        "user reports None last_applied after snapshot install; framework authoritative",
                    );
                } else {
                    return Err(crate::ClusterError::DriftDetected {
                        user: None,
                        framework: framework_la,
                    });
                }
            }
            (Some(u), None) => {
                // User says caught up but framework has no record.
                // This indicates the user has applied entries the framework doesn't
                // know about — surface as drift.
                return Err(crate::ClusterError::DriftDetected {
                    user: Some(u),
                    framework: None,
                });
            }
            _ => {} // both None or both Some-with-equal-value — fine
        }

        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                sm,
                last_applied: loaded_last_applied,
                last_membership,
                current_snapshot,
                last_applied_sv: handles.last_applied,
                snapshot_meta_sv: handles.snapshot_meta,
                snapshot_bytes_dir: handles.data_dir,
            })),
        })
    }
}

impl<S: StateMachine> Clone for AdaptedStateMachine<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
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
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, NodeAddr>), StorageError<NodeId>>
    {
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
            // NOTE (M2 Task 3): apply() updates the in-memory `g.last_applied`
            // only. `last_applied_sv` (the durable StableValue) is updated only
            // on install_snapshot. On restart, framework reads `last_applied_sv`
            // (which may be None or the snapshot index); user's sm.last_applied()
            // is the actual user-visible last_applied. Task 3's startup
            // cross-check reconciles these.
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

                    let resp_bytes =
                        bincode::serde::encode_to_vec(&resp, bincode::config::standard()).map_err(
                            |e| {
                                let io_err = std::io::Error::other(e.to_string());
                                StorageIOError::<NodeId>::apply(log_id, &io_err)
                            },
                        )?;
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
        AdaptedSnapshotBuilder {
            inner: self.inner.clone(),
        }
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

        // C1 fix: durable writes FIRST, then user-sm mutation, then adapter
        // metadata. If any durable write fails, the user's state machine and
        // adapter metadata are both unchanged — the caller sees Err and the
        // adapter's invariants are preserved. If the user-sm install fails
        // after the durable record is written, the next startup recovery
        // surfaces it as `ClusterError::Recovery` (see snapshot replay path
        // in `new`); the node fails to start — loud-but-safe.

        // 1. Write snapshot bytes to a uniquely-named file in data_dir.
        let bytes_filename = format!(
            "snapshot_{}.bin",
            meta.last_log_id.map(|l| l.index).unwrap_or(0)
        );
        // TODO(M5): when the new snapshot lands, the previous snapshot's bytes
        // file (if any) becomes orphaned. Implement cleanup when M5 hardens
        // snapshot lifecycle (e.g. via the snapshot.region mmap'd transport).
        let bytes_path = g.snapshot_bytes_dir.join(&bytes_filename);
        std::fs::write(&bytes_path, &bytes)
            .map_err(|e| StorageIOError::<NodeId>::read_snapshot(Some(meta.signature()), &e))?;
        let f = std::fs::File::open(&bytes_path)
            .map_err(|e| StorageIOError::<NodeId>::read_snapshot(Some(meta.signature()), &e))?;
        f.sync_all()
            .map_err(|e| StorageIOError::<NodeId>::read_snapshot(Some(meta.signature()), &e))?;
        drop(f);
        // M5 hardening: also fsync the parent directory to guarantee the
        // directory entry is durable, not just the file inode. For now we
        // accept the small crash window between f.sync_all() and
        // snapshot_meta_sv.store() — if a crash hits there, snapshot_meta_sv
        // is None on restart so the orphan bytes file is never read.

        // 2. Persist snapshot meta FIRST (atomic pointer to the bytes file).
        //    Ordering rationale (I1 fix): snapshot_meta is the "I have a snapshot
        //    at index N" pointer. By writing it before last_applied_sv we keep
        //    the on-disk state consistent across crash windows:
        //      * crash between bytes-write and meta-write: meta=None → restart
        //        sees no snapshot, no last_applied; user SM stays default.
        //      * crash between meta-write and last_applied-write: meta=Some(N)
        //        → restart re-installs the snapshot; user's install_snapshot
        //        returns N and that becomes the authoritative last_applied.
        //    The previous ordering allowed last_applied=Some(N) with meta=None,
        //    yielding silent corruption (framework at N, user SM at default).
        let stored_meta = StoredSnapshotMeta {
            last_log_id: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            bytes_filename: bytes_filename.clone(),
        };
        g.snapshot_meta_sv
            .store(&stored_meta)
            .map_err(|e| {
                StorageIOError::<NodeId>::read_snapshot(
                    Some(meta.signature()),
                    &std::io::Error::other(e.to_string()),
                )
            })?
            .wait()
            .map_err(|e| {
                StorageIOError::<NodeId>::read_snapshot(
                    Some(meta.signature()),
                    &std::io::Error::other(e.to_string()),
                )
            })?;

        // 3. Persist last_applied (if present).
        if let Some(lid) = meta.last_log_id {
            g.last_applied_sv
                .store(&lid)
                .map_err(|e| {
                    StorageIOError::<NodeId>::read_snapshot(
                        Some(meta.signature()),
                        &std::io::Error::other(e.to_string()),
                    )
                })?
                .wait()
                .map_err(|e| {
                    StorageIOError::<NodeId>::read_snapshot(
                        Some(meta.signature()),
                        &std::io::Error::other(e.to_string()),
                    )
                })?;
        }

        // 4. Now mutate the user's state machine. If this fails, the durable
        //    record points at a snapshot the user can't install — surface as
        //    a hard error. Subsequent restarts will surface this as
        //    `ClusterError::Recovery` from `AdaptedStateMachine::new`
        //    (snapshot replay path).
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
            user_last_applied, meta_index,
            "user install_snapshot returned index {user_last_applied} but meta.last_log_id.index is {meta_index}",
        );

        // 5. Update adapter metadata.
        g.last_applied = meta.last_log_id;
        g.last_membership = meta.last_membership.clone();
        g.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: bytes,
        });
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

        let snapshot_id_index = last_applied.map(|l| l.index).unwrap_or(0);
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id: format!("snap-{snapshot_id_index}"),
        };
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: buf.clone(),
        };
        g.current_snapshot = Some(stored);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(buf)),
        })
    }
}
