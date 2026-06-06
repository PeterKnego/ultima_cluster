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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::ipc::ring_bridge::NotifyBridge;
use uc_protocol::cnc::ServiceStatus;

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

/// `Send`/`Sync` wrapper for a `*const ServiceStatus` into the cnc mmap. The cnc
/// mmap is owned by the node `Instance`/handle and outlives the adapter; all
/// `ServiceStatus` fields are atomics (so `Sync`) and the node only reads them.
#[derive(Clone, Copy)]
pub struct ServiceStatusPtr(pub(crate) *const ServiceStatus);
// SAFETY: see doc — points into a Sync, longer-lived mmap; read-only.
unsafe impl Send for ServiceStatusPtr {}
unsafe impl Sync for ServiceStatusPtr {}

/// Current service epoch (0 if no pointer). SAFETY: live cnc mmap.
fn epoch_of(p: Option<ServiceStatusPtr>) -> u64 {
    match p {
        Some(ServiceStatusPtr(s)) => unsafe { (*s).service_epoch.load(Ordering::Acquire) },
        None => 0,
    }
}

/// The reattached service's reported last_applied (0 if no pointer). SAFETY: live cnc mmap.
fn service_last_of(p: Option<ServiceStatusPtr>) -> u64 {
    match p {
        Some(ServiceStatusPtr(s)) => unsafe { (*s).last_applied.load(Ordering::Acquire) },
        None => 0,
    }
}

const FULL_BACKOFF: Duration = Duration::from_micros(100);

pub struct ShmemAdaptedStateMachine<S: StateMachine> {
    pub(crate) inner: Arc<TokioMutex<ShmemInner<S>>>,
    /// Set by `node.shutdown()` (via [`Self::signal_shutdown`]) just before
    /// `raft.shutdown()`. `apply()` waits on the service apply/resp rings
    /// indefinitely so it can resume when a crashed service reconnects; that
    /// same indefinite wait would otherwise deadlock shutdown, because
    /// `raft.shutdown()` drains the openraft state-machine worker that is
    /// parked inside `apply()`. The ring-wait loops poll this flag and abort.
    ///
    /// Lives OUTSIDE `inner` on purpose: a wedged `apply()` holds the `inner`
    /// lock, so the flag must be settable without acquiring it. Shared across
    /// `Clone` (openraft's worker copy and the `NodeHandle` copy) via the `Arc`.
    pub(crate) shutdown: Arc<AtomicBool>,
}

impl<S: StateMachine> ShmemAdaptedStateMachine<S> {
    /// Signal any in-flight `apply()` to stop waiting on the service rings and
    /// return an error, so openraft's state-machine worker can finish and
    /// `raft.shutdown()` can complete even when the service has crashed.
    pub(crate) fn signal_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }
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
    /// Bridge that wakes `await_apply_resp` when the service publishes a
    /// response. The parker thread is stopped and joined when `ShmemInner`
    /// is dropped (via `NotifyBridge::Drop`).
    pub(crate) apply_resp_bridge: NotifyBridge,
    /// M5: in-process channel to the output_dispatcher. Normal entries are
    /// forwarded here after apply_resp returns. `try_send` keeps apply from
    /// blocking on a full output channel — the replay sweep covers any gaps.
    pub(crate) output_chan_tx: tokio::sync::mpsc::Sender<(u64, Bytes)>,
    /// Pointer to the cnc ServiceStatus (epoch + last_applied). None in tests.
    pub(crate) service_status_ptr: Option<ServiceStatusPtr>,
    /// Epoch last reconciled by reconstruction; a change means a service reattach.
    pub(crate) last_seen_epoch: u64,
    /// Journal handle for replaying committed entries during catch-up.
    pub(crate) journal: Arc<ultima_journal::Journal>,
    /// Purge boundary, for the below-purge -> NeedsSnapshot (Phase 2) decision.
    pub(crate) last_purged: Arc<StableValue<RaftLogId>>,
}

impl<S: StateMachine> ShmemAdaptedStateMachine<S> {
    // `new` is `pub` because the integration tests (external crates) construct it;
    // `ServiceStatusPtr` is `pub` for the same reason (it appears in this public
    // signature). The arg count grew with the reconstruction context
    // (journal/last_purged/status ptr) — these are cohesive constructor inputs,
    // so allow rather than bundle.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sm: S,
        handles: LogStorageHandles,
        apply_producer: SpscProducer,
        apply_resp_consumer: SpscConsumer,
        output_chan_tx: tokio::sync::mpsc::Sender<(u64, Bytes)>,
        journal: Arc<ultima_journal::Journal>,
        last_purged: Arc<StableValue<RaftLogId>>,
        service_status_ptr: Option<ServiceStatusPtr>,
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

        // Build the apply_resp bridge BEFORE moving the consumer into ShmemInner.
        let apply_resp_bridge =
            NotifyBridge::spawn(apply_resp_consumer.wait_handle(), "apply_resp");

        let last_seen_epoch = epoch_of(service_status_ptr);
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
                apply_resp_bridge,
                output_chan_tx,
                service_status_ptr,
                last_seen_epoch,
                journal,
                last_purged,
            })),
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }
}

impl<S: StateMachine> Clone for ShmemAdaptedStateMachine<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            shutdown: self.shutdown.clone(),
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
        // Shared with `node.shutdown()`; read lock-free in the ring-wait loops
        // so a service crash can't wedge shutdown.
        let shutdown = self.shutdown.clone();
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
                    // Copy the epoch context out before the field borrows below.
                    let ss_ptr = g.service_status_ptr;
                    let expected_epoch = g.last_seen_epoch;
                    let resp: Bytes = if epoch_of(ss_ptr) != expected_epoch {
                        // Service reattached since we last reconciled: catch up
                        // (replays incl. this entry) before publishing it live.
                        let (b, epoch) = drive_catchup(&g, &shutdown, log_id).await?;
                        g.last_seen_epoch = epoch;
                        b
                    } else {
                        publish_apply(
                            &g.apply_producer,
                            log_index,
                            cmd_bytes.as_ref(),
                            log_id,
                            &shutdown,
                        )
                        .await?;
                        match await_apply_resp(
                            &g.apply_resp_consumer,
                            log_index,
                            log_id,
                            &shutdown,
                            &g.apply_resp_bridge,
                            ss_ptr,
                            expected_epoch,
                        )
                        .await?
                        {
                            ApplyOutcome::Resp(b) => b,
                            ApplyOutcome::Reattach => {
                                let (b, epoch) = drive_catchup(&g, &shutdown, log_id).await?;
                                g.last_seen_epoch = epoch;
                                b
                            }
                        }
                    };
                    // M5: hand off to output_dispatcher exactly once for this entry.
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
    shutdown: &AtomicBool,
) -> Result<(), io::Error> {
    let _ = log_id; // kept for parity with the 0.9 error-context site
    loop {
        if shutdown.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "apply publish interrupted: node shutting down",
            ));
        }
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
            Ok(()) => {
                uc_protocol::probes::stamp_log(
                    log_index,
                    uc_protocol::probes::Checkpoint::ApplyEnqueue,
                );
                return Ok(());
            }
            Err(RingError::Full) => tokio::time::sleep(FULL_BACKOFF).await,
            Err(e) => {
                return Err(io::Error::other(format!(
                    "apply ring write at {log_index}: {e}"
                )));
            }
        }
    }
}

enum ApplyOutcome {
    Resp(Bytes),
    /// The service reattached (epoch changed) before this resp arrived.
    Reattach,
}

async fn await_apply_resp(
    consumer: &PlMutex<SpscConsumer>,
    expected_log_index: u64,
    log_id: RaftLogId,
    shutdown: &AtomicBool,
    bridge: &NotifyBridge,
    service_status_ptr: Option<ServiceStatusPtr>,
    expected_epoch: u64,
) -> Result<ApplyOutcome, io::Error> {
    let _ = log_id; // kept for parity with the 0.9 error-context site
    let mut payload_buf: Vec<u8> = Vec::with_capacity(1024);
    loop {
        if shutdown.load(Ordering::Acquire) {
            // Service crashed and we're shutting down: stop waiting for a
            // response that will never come so raft.shutdown() can proceed.
            // The entry is NOT durably applied (no last_applied advance), so
            // it is re-applied on restart once the service is back.
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "apply interrupted: node shutting down",
            ));
        }
        if epoch_of(service_status_ptr) != expected_epoch {
            return Ok(ApplyOutcome::Reattach);
        }
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
                uc_protocol::probes::stamp_log(
                    expected_log_index,
                    uc_protocol::probes::Checkpoint::RespDequeue,
                );
                return Ok(ApplyOutcome::Resp(Bytes::from(std::mem::take(
                    &mut payload_buf,
                ))));
            }
            Ok(Some(rec)) => {
                tracing::warn!(
                    msg_type = rec.msg_type,
                    "unexpected frame on apply_resp ring"
                );
            }
            Ok(None) => bridge.notified().await,
            Err(e) => {
                return Err(io::Error::other(format!("apply_resp ring read: {e}")));
            }
        }
    }
}

/// Replay committed entries to a reattached service. Called from `apply()` when a
/// reattach is observed (the parked entry `up_to` is, by the single-in-flight
/// invariant, the node's live frontier). Reuses the apply ring + resp ring under
/// the already-held inner lock (no concurrent apply). Returns `(resp_for_up_to,
/// reconciled_epoch)`. An outer loop restarts if the service reattaches AGAIN
/// mid-catch-up.
///
/// Safety: the re-applied response captured for `up_to` is what we hand back to
/// openraft's responder for the parked entry. This is safe because the node
/// broadcasts to the client only AFTER `await_apply_resp`/catch-up returns — a
/// resp the node never consumed (e.g. from the dead incarnation) was never
/// observed by any client, and re-applying `up_to` is idempotent by log_index.
///
/// Note: `iter_range` is eager — it buffers the whole `(from, up_to]` span into a
/// Vec. Fine for Phase 1; it becomes bounded once the Phase 2 snapshot cutover
/// lands (catch-up below the purge boundary installs a snapshot instead).
async fn drive_catchup<S: StateMachine>(
    g: &ShmemInner<S>,
    shutdown: &AtomicBool,
    up_to_log_id: RaftLogId,
) -> Result<(Bytes, u64), io::Error> {
    let up_to = up_to_log_id.index;
    debug_assert!(up_to >= 1, "up_to is a Normal entry index, always >= 1");
    let ss_ptr = g.service_status_ptr;
    let last_purged = g
        .last_purged
        .load()
        .ok()
        .flatten()
        .map(|l| l.index)
        .unwrap_or(0);

    loop {
        let epoch = epoch_of(ss_ptr);
        let service_last = service_last_of(ss_ptr);

        // Decide the replay span. The plan always includes up_to (the parked
        // entry must be applied + acked, idempotently — log_index is the key).
        // Fresh in-memory service => service_last == 0.
        let (from, to) = match crate::runtime::reconstruct::plan_replay(
            service_last,
            up_to,
            last_purged,
        ) {
            crate::runtime::reconstruct::ReplayPlan::NeedsSnapshot {
                service_last,
                last_purged,
            } => {
                return Err(io::Error::other(format!(
                    "reconstruct: service at {service_last} below purge boundary {last_purged}; \
                     snapshot-install is Phase 2"
                )));
            }
            crate::runtime::reconstruct::ReplayPlan::Replay { from, to } => (from, to),
        };

        // Drop any stale resps left by the dead incarnation (node owns the resp
        // ring's consumer_position).
        g.apply_resp_consumer.lock().discard_backlog();

        let iter = g
            .journal
            .iter_range((from + 1)..(to + 1))
            .map_err(|e| io::Error::other(format!("reconstruct: iter_range: {e}")))?;

        let mut last_resp = Bytes::new();
        let mut saw_up_to = false;
        let mut restart = false;
        for record in iter {
            let (seq, _meta, payload) =
                record.map_err(|e| io::Error::other(format!("reconstruct: journal read: {e}")))?;
            let (entry, _) = bincode::serde::decode_from_slice::<
                <TypeConfig as openraft::RaftTypeConfig>::Entry,
                _,
            >(&payload, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let log_id = entry.log_id;
            let cmd = match entry.payload {
                EntryPayload::Normal(cmd) => cmd,
                _ => continue, // Blank/Membership were never delivered to the service
            };
            publish_apply(&g.apply_producer, seq, cmd.as_ref(), log_id, shutdown).await?;
            match await_apply_resp(
                &g.apply_resp_consumer,
                seq,
                log_id,
                shutdown,
                &g.apply_resp_bridge,
                ss_ptr,
                epoch,
            )
            .await?
            {
                ApplyOutcome::Resp(b) => {
                    if seq == up_to {
                        last_resp = b;
                        saw_up_to = true;
                    }
                }
                ApplyOutcome::Reattach => {
                    restart = true;
                    break;
                }
            }
        }
        if restart {
            continue;
        }
        if !saw_up_to {
            return Err(io::Error::other(format!(
                "reconstruct: parked entry {up_to} missing from journal during catch-up \
                 (gap/corruption)"
            )));
        }
        return Ok((last_resp, epoch));
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
