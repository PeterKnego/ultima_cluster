//! Shmem-mode `RaftStateMachine` adapter.
//!
//! Counterpart to [`super::state_machine::AdaptedStateMachine`]: same trait
//! impl, but `apply()` publishes each entry's payload onto the
//! `service/apply.ring` SPSC and awaits the matching response on
//! `apply_resp.ring`, instead of calling `sm.apply()` in-process. The
//! user's actual `StateMachine` lives on the *service* side (driven by
//! `uc_service::ServiceBuilder::run`).
//!
//! Implementation notes (openraft 0.10):
//!   * Trait uses `#[add_async_trait]` (native async fn) — no `#[async_trait]`
//!     attribute on the impl.
//!   * `apply()` takes `Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>>`
//!     instead of the 0.9 `IntoIterator<Item = Entry<TypeConfig>>`.
//!   * `C::SnapshotData = std::io::Cursor<Vec<u8>>` — no Box wrapping in 0.10.
//!   * Errors use `io::Error` throughout (StorageError removed from public trait surface).
//!
//! # M3 limitations (intentionally accepted; M5 fixes)
//!
//! The node-side `sm: S` is **degenerate**: its job is to satisfy openraft's
//! snapshot trait surface (`build_snapshot` / `install_snapshot`), not to
//! mirror the service-side state. Consequences:
//!
//! * The startup user-vs-framework `last_applied` cross-check
//!   (`AdaptedStateMachine::new`) cannot run here — the node-side `sm` has no
//!   real last_applied. Skipped, with a tracing warning. Real cross-check
//!   needs the service's last_applied via the cnc handshake (the ServiceReady
//!   frame), which requires the cnc-sub-mmap MPSC attach API.
//! * Snapshot build/install at runtime still call the node-side `sm`'s
//!   `build_snapshot` / `install_snapshot`. Until M5 routes them through the
//!   service via `snapshot.region`, the produced snapshots reflect the
//!   degenerate node-side state — i.e., empty / default. The in-process
//!   M3 tests don't exercise snapshot install, so this is unobserved in
//!   practice.

use std::io;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use openraft::EntryPayload;
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use parking_lot::Mutex as PlMutex;
use tokio::sync::Mutex as TokioMutex;
use uc_protocol::frames::apply::{
    MSG_TYPE_APPLY, MSG_TYPE_APPLY_RESP, decode_extra_apply, decode_flags_apply,
    encode_extra_apply, encode_flags_apply,
};
use uc_protocol::ring::RingError;
use uc_protocol::ring::spsc::{SpscConsumer, SpscProducer};
use uc_service::StateMachine;
use ultima_journal::StableValue;

use super::log_storage::{LogStorageHandles, StoredSnapshotMeta};
use super::state_machine::StoredSnapshot;
use super::{RaftLogId, RaftSnapshot, RaftSnapshotMeta, RaftStoredMembership, TypeConfig};

const FULL_BACKOFF: Duration = Duration::from_micros(100);
const EMPTY_BACKOFF: Duration = Duration::from_micros(100);

pub struct ShmemAdaptedStateMachine<S: StateMachine> {
    pub(crate) inner: Arc<TokioMutex<ShmemInner<S>>>,
}

pub(crate) struct ShmemInner<S: StateMachine> {
    /// Node-side user-SM. Degenerate in shmem mode (see module docs); used
    /// only by the snapshot trait methods.
    pub(crate) sm: S,
    pub(crate) last_applied: Option<RaftLogId>,
    pub(crate) last_membership: RaftStoredMembership,
    pub(crate) current_snapshot: Option<StoredSnapshot>,
    pub(crate) last_applied_sv: Arc<StableValue<RaftLogId>>,
    pub(crate) snapshot_meta_sv: Arc<StableValue<StoredSnapshotMeta>>,
    pub(crate) snapshot_bytes_dir: PathBuf,
    /// Wrapped in `parking_lot::Mutex` so we can take it across an `await`
    /// without holding a non-`Send` borrow. The tokio mutex on the outer
    /// struct already serializes `apply()` calls; the parking_lot lock here
    /// is essentially trivial but lets us keep the ring halves
    /// `Sync`-friendly inside `Arc`s if we ever need to.
    pub(crate) apply_producer: PlMutex<SpscProducer>,
    pub(crate) apply_resp_consumer: PlMutex<SpscConsumer>,
    /// M5: in-process channel to the output_dispatcher. Normal entries are
    /// forwarded here after apply_resp returns. `try_send` keeps apply from
    /// blocking on a full output channel — the replay sweep covers any gaps.
    pub(crate) output_chan_tx: tokio::sync::mpsc::Sender<(u64, Bytes)>,
}

impl<S: StateMachine> ShmemAdaptedStateMachine<S> {
    pub fn new(
        sm: S,
        handles: LogStorageHandles,
        apply_producer: SpscProducer,
        apply_resp_consumer: SpscConsumer,
        output_chan_tx: tokio::sync::mpsc::Sender<(u64, Bytes)>,
    ) -> Result<Self, crate::ClusterError> {
        // Recover the framework-durable values; skip the user-side
        // cross-check (see module docs).
        let loaded_last_applied = handles.last_applied.load().ok().flatten();
        let loaded_snapshot_meta = handles.snapshot_meta.load().ok().flatten();

        let (last_membership, current_snapshot) = match loaded_snapshot_meta {
            Some(meta) => {
                let bytes_path = handles.data_dir.join(&meta.bytes_filename);
                let bytes = std::fs::read(&bytes_path).map_err(|e| {
                    crate::ClusterError::Recovery(format!(
                        "snapshot_meta points to {bytes_path:?} but read failed: {e}"
                    ))
                })?;
                let openraft_meta = RaftSnapshotMeta {
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
            None => (RaftStoredMembership::default(), None),
        };

        // Best-effort: install the snapshot into the (degenerate) node-side
        // sm so its trait surface returns something sensible. Errors here
        // are still hard — a present-but-corrupt snapshot is a real problem.
        let mut sm = sm;
        if let Some(ref snap) = current_snapshot {
            let mut cursor = Cursor::new(snap.data.clone());
            sm.install_snapshot(&mut cursor).map_err(|e| {
                crate::ClusterError::Recovery(format!("shmem-mode snapshot replay at startup: {e}"))
            })?;
        }

        if loaded_last_applied.is_some() {
            tracing::warn!(
                framework_last_applied = ?loaded_last_applied,
                "shmem mode: skipping user/framework last_applied cross-check \
                 (deferred until cnc-sub-mmap MPSC attach lands)"
            );
        }

        Ok(Self {
            inner: Arc::new(TokioMutex::new(ShmemInner {
                sm,
                last_applied: loaded_last_applied,
                last_membership,
                current_snapshot,
                last_applied_sv: handles.last_applied,
                snapshot_meta_sv: handles.snapshot_meta,
                snapshot_bytes_dir: handles.data_dir,
                apply_producer: PlMutex::new(apply_producer),
                apply_resp_consumer: PlMutex::new(apply_resp_consumer),
                output_chan_tx,
            })),
        })
    }
}

impl<S: StateMachine> Clone for ShmemAdaptedStateMachine<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// RaftStateMachine impl
// ---------------------------------------------------------------------------

impl<S: StateMachine> RaftStateMachine<TypeConfig> for ShmemAdaptedStateMachine<S> {
    type SnapshotBuilder = ShmemSnapshotBuilder<S>;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<RaftLogId>, RaftStoredMembership), io::Error> {
        let g = self.inner.lock().await;
        Ok((g.last_applied, g.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures::Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + Send,
    {
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            let log_id = entry.log_id;
            let log_index = log_id.index;

            let mut g = self.inner.lock().await;
            g.last_applied = Some(log_id);

            let resp_bytes: Bytes = match entry.payload {
                EntryPayload::Blank => Bytes::new(),
                EntryPayload::Membership(m) => {
                    g.last_membership = RaftStoredMembership::new(Some(log_id), m);
                    Bytes::new()
                }
                EntryPayload::Normal(cmd_bytes) => {
                    // Normal app-data: publish to apply.ring, await response from apply_resp.ring.
                    publish_apply(&g.apply_producer, log_index, cmd_bytes.as_ref(), log_id).await?;
                    let resp = await_apply_resp(&g.apply_resp_consumer, log_index, log_id).await?;
                    // M5: hand off to output_dispatcher. try_send so apply never blocks
                    // on a full output channel — the skip path catches it during replay.
                    if let Err(e) = g
                        .output_chan_tx
                        .try_send((log_index, cmd_bytes.clone().into()))
                    {
                        tracing::warn!(log_index, ?e, "output_chan full; replay will catch this");
                    }
                    resp
                }
            };
            drop(g);

            if let Some(r) = responder {
                r.send(resp_bytes);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        ShmemSnapshotBuilder {
            inner: self.inner.clone(),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &RaftSnapshotMeta,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        // Same durable-writes-first ordering as `AdaptedStateMachine`. The
        // node-side `sm` is degenerate in shmem mode but we still run its
        // `install_snapshot` so its in-memory state matches whatever the
        // user's snapshot decoder produces (typically a no-op).
        let bytes = snapshot.into_inner();
        let mut g = self.inner.lock().await;

        let bytes_filename = format!(
            "snapshot_{}.bin",
            meta.last_log_id.map(|l| l.index).unwrap_or(0)
        );
        let bytes_path = g.snapshot_bytes_dir.join(&bytes_filename);
        std::fs::write(&bytes_path, &bytes).map_err(io::Error::other)?;
        let f = std::fs::File::open(&bytes_path).map_err(io::Error::other)?;
        f.sync_all().map_err(io::Error::other)?;
        drop(f);

        let stored_meta = StoredSnapshotMeta {
            last_log_id: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            bytes_filename: bytes_filename.clone(),
        };
        g.snapshot_meta_sv
            .store(&stored_meta)
            .map_err(io::Error::other)?
            .wait()
            .map_err(io::Error::other)?;

        if let Some(lid) = meta.last_log_id {
            g.last_applied_sv
                .store(&lid)
                .map_err(io::Error::other)?
                .wait()
                .map_err(io::Error::other)?;
        }

        let mut cursor = Cursor::new(bytes.clone());
        let _user_last_applied =
            g.sm.install_snapshot(&mut cursor)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        g.last_applied = meta.last_log_id;
        g.last_membership = meta.last_membership.clone();
        g.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: bytes,
        });
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<RaftSnapshot>, io::Error> {
        let g = self.inner.lock().await;
        match &g.current_snapshot {
            Some(s) => Ok(Some(Snapshot {
                meta: s.meta.clone(),
                snapshot: Cursor::new(s.data.clone()),
            })),
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// ring publish / await helpers
// ---------------------------------------------------------------------------

async fn publish_apply(
    producer: &PlMutex<SpscProducer>,
    log_index: u64,
    cmd_bytes: &[u8],
    log_id: RaftLogId,
) -> Result<(), io::Error> {
    let _ = log_id; // kept for parity with the 0.9 error-context site
    loop {
        let result = {
            let mut p = producer.lock();
            p.try_write(
                MSG_TYPE_APPLY,
                encode_flags_apply(0),
                encode_extra_apply(log_index),
                cmd_bytes,
            )
        };
        match result {
            Ok(()) => return Ok(()),
            Err(RingError::Full) => tokio::time::sleep(FULL_BACKOFF).await,
            Err(e) => {
                return Err(io::Error::other(format!(
                    "apply ring write at {log_index}: {e}"
                )));
            }
        }
    }
}

async fn await_apply_resp(
    consumer: &PlMutex<SpscConsumer>,
    expected_log_index: u64,
    log_id: RaftLogId,
) -> Result<Bytes, io::Error> {
    let _ = log_id; // kept for parity with the 0.9 error-context site
    let mut payload_buf: Vec<u8> = Vec::with_capacity(1024);
    loop {
        let read_result = {
            let mut c = consumer.lock();
            c.try_read(&mut payload_buf)
        };
        match read_result {
            Ok(Some(rec)) if rec.msg_type == MSG_TYPE_APPLY_RESP => {
                if let Err(e) = decode_flags_apply(rec.flags) {
                    tracing::warn!("dropping apply_resp frame with bad flags: {e}");
                    continue;
                }
                let li = decode_extra_apply(rec.header_extra);
                if li != expected_log_index {
                    // The service apply loop emits responses in publish order,
                    // so a mismatch here is unrecoverable corruption.
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "apply_resp log_index mismatch: got {li}, expected {expected_log_index}"
                        ),
                    ));
                }
                return Ok(Bytes::from(std::mem::take(&mut payload_buf)));
            }
            Ok(Some(rec)) => {
                tracing::warn!(
                    msg_type = rec.msg_type,
                    "unexpected frame on apply_resp ring"
                );
            }
            Ok(None) => tokio::time::sleep(EMPTY_BACKOFF).await,
            Err(e) => {
                return Err(io::Error::other(format!("apply_resp ring read: {e}")));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshot builder
// ---------------------------------------------------------------------------

pub struct ShmemSnapshotBuilder<S: StateMachine> {
    inner: Arc<TokioMutex<ShmemInner<S>>>,
}

impl<S: StateMachine> RaftSnapshotBuilder<TypeConfig> for ShmemSnapshotBuilder<S> {
    async fn build_snapshot(&mut self) -> Result<RaftSnapshot, io::Error> {
        let mut g = self.inner.lock().await;
        let last_applied = g.last_applied;
        let last_membership = g.last_membership.clone();

        // Degenerate (see module docs): the node-side sm is empty. Build
        // produces whatever the user's default `build_snapshot` returns.
        let mut buf: Vec<u8> = Vec::new();
        let _user_index =
            g.sm.build_snapshot(&mut buf)
                .map_err(|e| io::Error::other(e.to_string()))?;

        let snapshot_id_index = last_applied.map(|l| l.index).unwrap_or(0);
        let meta = RaftSnapshotMeta {
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
            snapshot: Cursor::new(buf),
        })
    }
}
