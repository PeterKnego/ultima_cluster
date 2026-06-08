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
//! * The startup `last_applied` cross-check now runs here as an UPPER-BOUND
//!   check (Phase 3). `new()` reads the service's reported `last_applied` from
//!   the cnc `ServiceStatus.last_applied` atomic (via `service_last_of`) and
//!   refuses (`ClusterError::DriftDetected`) only if it exceeds the node's
//!   journal tail (`journal.last_seq()`) — a service can only have applied
//!   entries this node logged, so a higher index means corruption or a wrong
//!   incarnation. A service at-or-below the tail (including a fresh in-memory
//!   service at 0) is the normal reconstruction case and is allowed through.
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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::ipc::ring_bridge::NotifyBridge;
use uc_protocol::cnc::ServiceStatus;

use bytes::Bytes;
use futures::StreamExt;
use openraft::EntryPayload;
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use parking_lot::Mutex as PlMutex;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::Notify;
use uc_protocol::frames::apply::{
    MSG_TYPE_APPLY, MSG_TYPE_APPLY_RESP, decode_extra_apply, decode_flags_apply,
    encode_extra_apply, encode_flags_apply,
};
use uc_protocol::frames::snapshot::{
    MSG_TYPE_BUILD_SNAPSHOT, MSG_TYPE_INSTALL_SNAPSHOT, MSG_TYPE_SNAPSHOT_BUILT,
    MSG_TYPE_SNAPSHOT_INSTALLED, encode_extra_install_snapshot,
};
use uc_protocol::ring::RingError;
use uc_protocol::ring::spsc::{SpscConsumer, SpscProducer};
use uc_protocol::snapshot_region;
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
    /// Snapshot control channel — OUTSIDE `inner` so `build_snapshot` does the
    /// BUILD round-trip without holding the apply lock. See [`SnapshotChannel`].
    pub(crate) snapshot_op: Arc<TokioMutex<SnapshotChannel>>,
    /// Highest service `service_epoch` for which the service has been reconstructed
    /// to the node's applied frontier. Atomic + OUTSIDE `inner` so the read-gate
    /// ([`ReconcileGate`]) checks it lock-free; kept in lockstep with
    /// `ShmemInner::last_seen_epoch`. Updated by `apply()` (lazy) and by the
    /// reconcile driver via [`Self::ensure_reconciled`] (proactive).
    pub(crate) reconciled_epoch: Arc<AtomicU64>,
    /// Fired after the service catch-up frontier advances, to wake reads parked
    /// in [`ReconcileGate::wait_until_ready`].
    pub(crate) reconcile_done: Arc<Notify>,
    /// Highest all-entry log index the CURRENT service incarnation has caught up
    /// to (Normal entries applied+acked; Blank/Membership trivially passed). The
    /// read-gate waits until this reaches the node's committed index, so a node
    /// that is mid-replay after a restart (service still empty) doesn't serve a
    /// stale read. Reset implicitly per incarnation via the epoch check.
    pub(crate) service_caught_up_to: Arc<AtomicU64>,
    /// Cached cnc `ServiceStatus` pointer (also in `inner`); duplicated here so the
    /// read-gate reads the live service epoch without taking `inner`.
    pub(crate) service_status_ptr: Option<ServiceStatusPtr>,
}

/// Snapshot control channel — OUTSIDE `inner` so `build_snapshot` does the BUILD
/// round-trip without holding the apply lock. A dedicated `tokio::Mutex` serializes
/// its two users (build_snapshot, drive_catchup INSTALL) for SPSC single-producer
/// safety, decoupled from `inner`.
pub(crate) struct SnapshotChannel {
    pub(crate) producer: SpscProducer,
    pub(crate) resp_consumer: SpscConsumer,
    pub(crate) resp_bridge: NotifyBridge,
    pub(crate) region_path: std::path::PathBuf,
}

impl<S: StateMachine> ShmemAdaptedStateMachine<S> {
    /// Reconstruct the CURRENT service incarnation to the node's applied frontier
    /// if it has reattached since we last reconciled. Idempotent + cheap on the
    /// fast path. Called by the proactive reconcile driver (and safe to call
    /// concurrently with `apply()`'s lazy reconstruction — both serialize on
    /// `inner`, and this double-checks `reconciled_epoch` under the lock so the
    /// catch-up runs at most once per incarnation).
    ///
    /// Unlike `apply()`'s reconstruction, an error here is NOT node-fatal: the
    /// driver logs and retries, so a transient reconstruction failure degrades
    /// to "reads wait a bit longer" rather than crashing the node.
    pub(crate) async fn ensure_reconciled(&self) -> io::Result<()> {
        let mut g = self.inner.lock().await;
        let cur = epoch_of(g.service_status_ptr);
        let frontier_idx = g.last_applied.map(|l| l.index).unwrap_or(0);
        // Reconstruct if the service reattached (epoch changed) OR the service is
        // behind the node's applied frontier (a node restart whose log was purged
        // below committed leaves a fresh service missing the snapshot'd prefix even
        // with the SAME epoch). Otherwise nothing to do.
        let needs = cur != self.reconciled_epoch.load(Ordering::Acquire)
            || self.service_caught_up_to.load(Ordering::Acquire) < frontier_idx;
        if !needs {
            return Ok(());
        }
        let (reconciled, caught_up) = match g.last_applied {
            // Replay (service_last, frontier] into the fresh service. `drive_catchup`
            // borrows `&g` (interior-mutable rings) and returns the reconciled epoch;
            // the service is then caught up to `frontier`.
            Some(frontier) => {
                let (_resp, epoch) =
                    drive_catchup(&g, &self.snapshot_op, &self.shutdown, frontier).await?;
                (epoch, frontier.index)
            }
            // Nothing applied yet: the empty service already matches the node's
            // (empty) frontier — mark this incarnation reconciled at index 0.
            None => (cur, 0),
        };
        g.last_seen_epoch = reconciled;
        self.reconciled_epoch.store(reconciled, Ordering::Release);
        self.service_caught_up_to.store(caught_up, Ordering::Release);
        self.reconcile_done.notify_waiters();
        Ok(())
    }

    /// Build a lightweight [`ReconcileGate`] for the query read-path. It shares
    /// this adapter's reconcile signals but holds NO adapter clone (so it does not
    /// inflate the live-clone count — see the driver-redesign plan), and pokes
    /// `request` to wake the driver. The read catch-up target (`read_index`) is
    /// supplied per-read by `ensure_linearizable`, so the gate needs no committed
    /// pointer of its own.
    pub(crate) fn reconcile_gate(&self, request: Arc<Notify>) -> ReconcileGate {
        ReconcileGate {
            reconciled_epoch: self.reconciled_epoch.clone(),
            service_caught_up_to: self.service_caught_up_to.clone(),
            done: self.reconcile_done.clone(),
            request,
            service_status_ptr: self.service_status_ptr,
        }
    }
}

/// How long a parked read waits for the driver before re-checking / re-poking.
const GATE_BACKSTOP: Duration = Duration::from_millis(25);
/// Driver idle backstop: re-checks for a reattach even if a poke was missed.
const DRIVER_BACKSTOP: Duration = Duration::from_millis(50);

/// Lightweight read-gate handed to the query path. Holds only shared signals
/// (no adapter clone): `reconciled_epoch` (current incarnation reconstructed),
/// `service_caught_up_to` (the all-entry index the service has caught up to), a
/// `done` notifier woken when either advances, a `request` notifier that pokes the
/// reconcile driver, and the cnc `ServiceStatus` pointer (live service epoch).
#[derive(Clone)]
pub(crate) struct ReconcileGate {
    reconciled_epoch: Arc<AtomicU64>,
    service_caught_up_to: Arc<AtomicU64>,
    done: Arc<Notify>,
    request: Arc<Notify>,
    service_status_ptr: Option<ServiceStatusPtr>,
}

impl ReconcileGate {
    /// Block until it is safe to serve a read at `read_index` (the linearizable
    /// read point from `ensure_linearizable`). Two conditions, both required:
    ///
    /// 1. the CURRENT service incarnation has been reconstructed (`service epoch ==
    ///    reconciled_epoch`) — catches a mid-life service crash/reattach, where a
    ///    stale-high `service_caught_up_to` from before the crash must not count; and
    /// 2. the service has caught up to `read_index` — catches a node restart where
    ///    the service is empty until the committed log is replayed into it.
    ///
    /// Fast path returns immediately in steady state; otherwise pokes the driver
    /// and waits, with a backstop so a missed wakeup can't hang the read.
    pub(crate) async fn wait_until_caught_up(&self, read_index: u64) {
        loop {
            // Register interest BEFORE the checks so a `done` fired in between is
            // not lost.
            let done_fut = self.done.notified();
            let incarnation_ok =
                epoch_of(self.service_status_ptr) == self.reconciled_epoch.load(Ordering::Acquire);
            let caught_up = self.service_caught_up_to.load(Ordering::Acquire) >= read_index;
            if incarnation_ok && caught_up {
                return;
            }
            // Poke the driver to reconcile a reattached incarnation now (proactive),
            // then wait for progress — or re-check after the backstop.
            self.request.notify_one();
            tokio::select! {
                _ = done_fut => {}
                _ = tokio::time::sleep(GATE_BACKSTOP) => {}
            }
        }
    }
}

/// Handle for the proactive reconcile driver task.
pub(crate) struct ReconcileDriverHandle {
    pub(crate) join: tokio::task::JoinHandle<()>,
    pub(crate) stop: Arc<AtomicBool>,
    /// Poked to wake the driver promptly (also poked by the read-gate).
    pub(crate) request: Arc<Notify>,
}

/// Spawn the proactive reconcile driver. It owns the (repurposed) second adapter
/// clone — so the total live-clone count stays at two (openraft's + this) and the
/// extra-clone freeze is avoided. On each wake (a `request` poke or the idle
/// backstop) it reconstructs a reattached service to the node frontier, BEFORE
/// any read needs it (closing the lazy-only staleness window).
pub(crate) fn spawn_reconcile_driver<S: StateMachine>(
    adapter: ShmemAdaptedStateMachine<S>,
    request: Arc<Notify>,
    stop: Arc<AtomicBool>,
) -> ReconcileDriverHandle {
    let stop_for_task = stop.clone();
    let request_for_task = request.clone();
    let join = tokio::spawn(async move {
        while !stop_for_task.load(Ordering::Relaxed) {
            tokio::select! {
                _ = request_for_task.notified() => {}
                _ = tokio::time::sleep(DRIVER_BACKSTOP) => {}
            }
            if stop_for_task.load(Ordering::Relaxed) {
                break;
            }
            if let Err(e) = adapter.ensure_reconciled().await {
                tracing::warn!(error = %e, "reconcile driver: ensure_reconciled failed");
                tokio::time::sleep(DRIVER_BACKSTOP).await;
            }
        }
    });
    ReconcileDriverHandle { join, stop, request }
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
    // `new` is `pub` because the integration tests (external crates) construct it.
    // The arg count grew with the reconstruction + snapshot context
    // (journal/last_purged/status ptr/snapshot ring) — cohesive constructor inputs,
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
        snapshot_producer: SpscProducer,
        snapshot_resp_consumer: SpscConsumer,
        snapshot_region_path: std::path::PathBuf,
    ) -> Result<Self, crate::ClusterError> {
        // Recover the framework-durable values.
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

        // Phase 3 cross-check (upper-bound / Option A). By the time the node builds
        // the adapter, the builder has already awaited `wait_for_service_ready`, so
        // the service has published `ServiceStatus.last_applied`. The service can
        // only have applied entries this node logged, so a reported index ABOVE the
        // journal tail is impossible — corruption or a wrong incarnation. Refuse.
        // A service at-or-below the tail (incl. a fresh in-memory service at 0) is
        // the normal reconstruction case and is allowed through; reconstruction (the
        // apply/reattach path) brings it up to the node frontier.
        let service_last = service_last_of(service_status_ptr);
        let log_tail = journal.last_seq().unwrap_or(0);
        if let Err(d) = crate::runtime::reconstruct::service_not_ahead(service_last, log_tail) {
            tracing::error!(
                service_last,
                log_tail,
                "shmem startup cross-check: service reports last_applied above the \
                 journal tail; refusing as drift (corruption or wrong incarnation)"
            );
            return Err(crate::ClusterError::DriftDetected {
                user: Some(d.service_last),
                framework: Some(d.frontier),
            });
        }
        if loaded_last_applied.is_some() {
            tracing::debug!(
                framework_last_applied = ?loaded_last_applied,
                service_last,
                log_tail,
                "shmem startup cross-check passed (service at-or-below log tail)"
            );
        }

        // Build the apply_resp bridge BEFORE moving the consumer into ShmemInner.
        let apply_resp_bridge =
            NotifyBridge::spawn(apply_resp_consumer.wait_handle(), "apply_resp");
        // Build the snapshot_resp bridge BEFORE moving the consumer into ShmemInner.
        let snapshot_resp_bridge =
            NotifyBridge::spawn(snapshot_resp_consumer.wait_handle(), "snapshot_resp");

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
            reconciled_epoch: Arc::new(AtomicU64::new(last_seen_epoch)),
            reconcile_done: Arc::new(Notify::new()),
            service_caught_up_to: Arc::new(AtomicU64::new(0)),
            service_status_ptr,
            snapshot_op: Arc::new(TokioMutex::new(SnapshotChannel {
                producer: snapshot_producer,
                resp_consumer: snapshot_resp_consumer,
                resp_bridge: snapshot_resp_bridge,
                region_path: snapshot_region_path,
            })),
        })
    }
}

impl<S: StateMachine> Clone for ShmemAdaptedStateMachine<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            shutdown: self.shutdown.clone(),
            reconciled_epoch: self.reconciled_epoch.clone(),
            reconcile_done: self.reconcile_done.clone(),
            service_caught_up_to: self.service_caught_up_to.clone(),
            service_status_ptr: self.service_status_ptr,
            snapshot_op: self.snapshot_op.clone(),
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

            // Catch-up bookkeeping (see the post-match store): `filled_to_here` is
            // set by the Normal arm, which always brings the service to `log_index`
            // (publish when contiguous, or drive_catchup which fills any prefix gap).
            let prev_caught = self.service_caught_up_to.load(Ordering::Acquire);
            let mut filled_to_here = false;
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
                    // Reconstruct when the service reattached (epoch changed) OR when
                    // the node has not delivered up to `log_index - 1` to this service
                    // incarnation — a GAP. The gap arises on a node restart whose log
                    // was purged below committed: openraft replays only the post-
                    // snapshot tail, so the fresh service is missing the snapshot'd
                    // prefix `(service_caught_up_to, log_index-1]`. `drive_catchup`'s
                    // NeedsSnapshot branch installs the node snapshot into the service
                    // then tail-replays, filling the prefix. `service_caught_up_to` is
                    // node-local (no cnc-publish lag), so it never false-positives in
                    // steady state (it equals the previous entry's index).
                    let gap = self.service_caught_up_to.load(Ordering::Acquire) + 1 < log_index;
                    let resp: Bytes = if epoch_of(ss_ptr) != expected_epoch || gap {
                        // Service reattached or has a prefix gap: catch up (replays
                        // incl. this entry, installing a snapshot if below purge)
                        // before publishing it live.
                        let (b, epoch) =
                            drive_catchup(&g, &self.snapshot_op, &shutdown, log_id).await?;
                        g.last_seen_epoch = epoch;
                        self.reconciled_epoch.store(epoch, Ordering::Release);
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
                                let (b, epoch) =
                                    drive_catchup(&g, &self.snapshot_op, &shutdown, log_id).await?;
                                g.last_seen_epoch = epoch;
                                self.reconciled_epoch.store(epoch, Ordering::Release);
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
                    filled_to_here = true;
                    resp
                }
            };
            // Advance the service catch-up frontier ONLY when the service is actually
            // at `log_index` now: the Normal arm always brings it there (publish or
            // drive_catchup → `filled_to_here`); a Blank/Membership delivers nothing,
            // so it advances the frontier only when it was already contiguous
            // (`prev_caught == log_index - 1`). Otherwise an unfilled prefix gap must
            // be preserved — advancing here would let a Blank/Membership entry mask
            // the gap and a read be served against a service still missing its prefix.
            if filled_to_here || prev_caught + 1 == log_index {
                self.service_caught_up_to.store(log_index, Ordering::Release);
                self.reconcile_done.notify_waiters();
            }
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
            shutdown: self.shutdown.clone(),
            snapshot_op: self.snapshot_op.clone(),
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

// ---------------------------------------------------------------------------
// snapshot ring publish / await helpers (Phase 2a)
// ---------------------------------------------------------------------------

async fn publish_snapshot_cmd(
    producer: &mut SpscProducer,
    msg_type: u16,
    extra: [u8; 8],
    shutdown: &AtomicBool,
) -> Result<(), io::Error> {
    loop {
        if shutdown.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "snapshot cmd: shutting down",
            ));
        }
        let r = producer.try_write(msg_type, 0, extra, &[]);
        match r {
            Ok(()) => return Ok(()),
            Err(RingError::Full) => tokio::time::sleep(FULL_BACKOFF).await,
            Err(e) => return Err(io::Error::other(format!("snapshot cmd write: {e}"))),
        }
    }
}

/// Await a snapshot resp frame of `expected_msg_type`; returns its header_extra u64.
async fn await_snapshot_resp(
    consumer: &mut SpscConsumer,
    expected_msg_type: u16,
    shutdown: &AtomicBool,
    bridge: &NotifyBridge,
) -> Result<u64, io::Error> {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        if shutdown.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "snapshot resp: shutting down",
            ));
        }
        let read = consumer.try_read(&mut buf);
        match read {
            Ok(Some(rec)) if rec.msg_type == expected_msg_type => {
                return Ok(u64::from_le_bytes(rec.header_extra));
            }
            Ok(Some(rec)) => {
                tracing::warn!(
                    msg_type = rec.msg_type,
                    "unexpected frame on snapshot_resp ring"
                )
            }
            Ok(None) => bridge.notified().await,
            Err(e) => return Err(io::Error::other(format!("snapshot_resp read: {e}"))),
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
    snapshot_op: &Arc<TokioMutex<SnapshotChannel>>,
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

        // Phase 3 cross-check at reattach: the live node frontier (`up_to`) is the
        // upper bound. A reattached service reporting an index above it claims state
        // the node has not applied — refuse (drive_catchup errors are node-fatal,
        // which IS the intended refusal). At-or-below is replayed/reconstructed.
        if let Err(d) = crate::runtime::reconstruct::service_not_ahead(service_last, up_to) {
            return Err(io::Error::other(format!(
                "service reattach drift: service last_applied={} > node frontier={}",
                d.service_last, d.frontier
            )));
        }

        // Decide the replay span. The plan always includes up_to (the parked
        // entry must be applied + acked, idempotently — log_index is the key).
        // Fresh in-memory service => service_last == 0.
        let (from, to) =
            match crate::runtime::reconstruct::plan_replay(service_last, up_to, last_purged) {
                crate::runtime::reconstruct::ReplayPlan::NeedsSnapshot {
                    service_last,
                    last_purged,
                } => {
                    // Install the node's snapshot into the fresh service, then tail-replay.
                    let (snap_index, snap_bytes) = match &g.current_snapshot {
                        Some(s) => (
                            s.meta.last_log_id.map(|l| l.index).unwrap_or(0),
                            s.data.clone(),
                        ),
                        None => {
                            // Durable snapshot on disk (e.g. after a node restart).
                            let meta = g.snapshot_meta_sv.load().ok().flatten();
                            match meta {
                                Some(m) => {
                                    let path = g.snapshot_bytes_dir.join(&m.bytes_filename);
                                    let bytes = std::fs::read(&path).map_err(|e| {
                                        io::Error::other(format!(
                                            "reconstruct: read snapshot {path:?}: {e}"
                                        ))
                                    })?;
                                    (m.last_log_id.map(|l| l.index).unwrap_or(0), bytes)
                                }
                                None => {
                                    return Err(io::Error::other(format!(
                                        "reconstruct: service at {service_last} below purge \
                                     {last_purged} but node has no snapshot to install"
                                    )));
                                }
                            }
                        }
                    };
                    // Lock ordering: we already hold `inner` (caller's `g`); nest
                    // `snapshot_op` here. This is the allowed order — build_snapshot
                    // never holds snapshot_op while awaiting inner, so no cycle.
                    let installed = {
                        let mut ch = snapshot_op.lock().await;
                        // Split-borrow distinct fields so `&mut resp_consumer` and
                        // `&resp_bridge` can be passed together (disjoint borrows).
                        let SnapshotChannel {
                            producer,
                            resp_consumer,
                            resp_bridge,
                            region_path,
                        } = &mut *ch;
                        snapshot_region::write(region_path, snap_index, &snap_bytes)
                            .map_err(|e| io::Error::other(e.to_string()))?;
                        resp_consumer.discard_backlog();
                        publish_snapshot_cmd(
                            producer,
                            MSG_TYPE_INSTALL_SNAPSHOT,
                            encode_extra_install_snapshot(snap_index),
                            shutdown,
                        )
                        .await?;
                        await_snapshot_resp(
                            resp_consumer,
                            MSG_TYPE_SNAPSHOT_INSTALLED,
                            shutdown,
                            resp_bridge,
                        )
                        .await?
                    };
                    if installed != snap_index {
                        // The service's install_snapshot reported a different index than
                        // the snapshot we sent — a user-SM bug. We trust snap_index for
                        // the tail boundary regardless. (Mirrors the build-path warn.)
                        tracing::warn!(
                            snap_index,
                            reported = installed,
                            "service install_snapshot reported index != installed snapshot index"
                        );
                    }
                    // Service is now at snap_index; replay the tail (snap_index, up_to].
                    (snap_index, up_to)
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
    shutdown: Arc<AtomicBool>,
    snapshot_op: Arc<TokioMutex<SnapshotChannel>>,
}

impl<S: StateMachine> RaftSnapshotBuilder<TypeConfig> for ShmemSnapshotBuilder<S> {
    async fn build_snapshot(&mut self) -> Result<RaftSnapshot, io::Error> {
        // LOCK ORDERING (critical — the three scopes are kept separate on purpose):
        //   (1) take + drop `inner` (frontier/membership/epoch capture);
        //   (2) take + drop `snapshot_op` (the BUILD round-trip — NO inner held,
        //       so apply() runs concurrently);
        //   (3) take `inner` again (epoch race guard + persist).
        // build NEVER holds `snapshot_op` while awaiting `inner`. drive_catchup
        // (Task 4) holds `inner` then nests `snapshot_op` (the allowed order);
        // because build releases `snapshot_op` before re-taking `inner`, there is
        // no inner↔snapshot_op cycle.

        // (1) Brief inner lock: capture frontier + membership + epoch_before.
        let (frontier, last_membership, epoch_before, ss_ptr) = {
            let g = self.inner.lock().await;
            (
                g.last_applied,
                g.last_membership.clone(),
                epoch_of(g.service_status_ptr),
                g.service_status_ptr,
            )
        };

        // (2) snapshot_op round-trip — NO inner held → apply() runs concurrently.
        let (built_index, bytes) = {
            let mut ch = self.snapshot_op.lock().await;
            // Split-borrow distinct fields so `&mut resp_consumer` and
            // `&resp_bridge` can be passed together (disjoint borrows).
            let SnapshotChannel {
                producer,
                resp_consumer,
                resp_bridge,
                region_path,
            } = &mut *ch;
            // Defensive (parity with drive_catchup): drop any stale resp left by a
            // prior build that was aborted by shutdown after sending BUILD but before
            // consuming BUILT, so we never match a stale frame to this request.
            resp_consumer.discard_backlog();
            publish_snapshot_cmd(producer, MSG_TYPE_BUILD_SNAPSHOT, [0u8; 8], &self.shutdown)
                .await?;
            let bi = await_snapshot_resp(
                resp_consumer,
                MSG_TYPE_SNAPSHOT_BUILT,
                &self.shutdown,
                resp_bridge,
            )
            .await?;
            let (_i, b) =
                snapshot_region::read(region_path).map_err(|e| io::Error::other(e.to_string()))?;
            (bi, b)
        };

        // (3) Brief inner lock: EPOCH race guard + persist.
        let mut g = self.inner.lock().await;

        // RACE GUARD (data-loss): the service may have crashed and a FRESH,
        // not-yet-reconstructed service reattached while this BUILD was in flight.
        // With the round-trip now OFF the inner lock, apply()/drive_catchup may
        // legitimately advance the frontier during a build — so a frontier-vs-
        // built_index comparison would false-positive. We instead key the guard on
        // the service EPOCH: a change means a reattach (crash) happened across the
        // BUILD, and the bytes we just read may be from a fresh, degenerate service.
        //
        // A fresh service answers BUILD with empty bytes. If we persisted that with
        // meta.last_log_id = the node frontier, openraft would advance the snapshot
        // pointer and purge the log below the frontier; a later install of that
        // empty snapshot is silent durable data loss.
        //
        // We CANNOT signal "retry" by returning Err: in openraft 0.10.0-alpha.20 a
        // build_snapshot Err is wrapped into StorageError and surfaced via
        // `command_result.result?` in RaftCore::handle_notification, which converts
        // it to Fatal::StorageError and SHUTS THE NODE DOWN (verified in
        // src/core/sm/worker.rs + src/core/raft_core.rs + src/errors/fatal.rs).
        // It is exactly as fatal as an apply() error — NOT rescheduled.
        //
        // Instead we return the LAST GOOD snapshot unchanged (its real persisted
        // meta+bytes). openraft's SnapshotHandler::update_snapshot only advances /
        // purges when the new meta.last_log_id is STRICTLY GREATER than the current
        // snapshot pointer; returning the existing (or an empty/None) meta is not
        // greater, so it advances nothing and purges nothing. The node stays up,
        // the log stays intact, and the SnapshotPolicy re-triggers a build later —
        // by which point apply()/drive_catchup has reconstructed the reattached
        // service to the frontier and the retried build returns real bytes.
        let epoch_after = epoch_of(ss_ptr);
        if epoch_after != epoch_before {
            tracing::warn!(
                epoch_before,
                epoch_after,
                "snapshot build raced a reattach; returning last-good (no persist)"
            );
            // last-good (or empty) is non-advancing: openraft's update_snapshot only
            // advances/purges when last_log_id is STRICTLY GREATER than the current
            // pointer, so neither result advances the snapshot or purges the log.
            return Ok(last_good_or_empty(&g));
        }

        // The snapshot BYTES describe the service's freeze-time applied index
        // (`built_index`, reported in the BUILT frame). Because the BUILD round-trip
        // in scope 2 ran with `inner` FREE, apply() may have advanced the service
        // PAST the `frontier` we captured in scope 1 (or the service may lag behind
        // it). meta.last_log_id MUST describe the bytes, so we resolve the TRUE
        // LogId at `built_index` from the journal — the same source drive_catchup
        // replays from. Labeling with `frontier` instead mislabels the snapshot, so
        // the reconstruction tail-replay re-applies (or skips) the span between
        // built_index and frontier — the regression that produced reconstructed
        // sums like 43/49 instead of 31.
        let _ = frontier; // superseded by built_index for the snapshot label
        let last_log_id: Option<RaftLogId> = if built_index == 0 {
            None
        } else {
            match log_id_at(g.journal.as_ref(), built_index) {
                Ok(id) => Some(id),
                Err(e) => {
                    tracing::warn!(
                        built_index,
                        error = %e,
                        "snapshot: cannot resolve LogId for freeze index; \
                         returning last-good (no persist)"
                    );
                    return Ok(last_good_or_empty(&g));
                }
            }
        };
        let meta = RaftSnapshotMeta {
            last_log_id,
            // Membership is captured at `frontier`. v1 establishes membership once at
            // init and never reconfigures at runtime, so it is stable across the
            // build window (revisit if runtime membership changes land).
            last_membership: last_membership.clone(),
            // Derive from last_log_id (matches recovery's reconstruction in `new()`)
            // so the snapshot_id is stable across a restart.
            snapshot_id: format!("snap-{}", last_log_id.map(|l| l.index).unwrap_or(0)),
        };

        // REQUIRED: persist to disk so a node restart after purge can still
        // reconstruct (openraft purges the log right after snapshotting). Mirror
        // install_snapshot's persist block.
        let bytes_filename = format!("snapshot_{}.bin", last_log_id.map(|l| l.index).unwrap_or(0));
        let bytes_path = g.snapshot_bytes_dir.join(&bytes_filename);
        std::fs::write(&bytes_path, &bytes).map_err(io::Error::other)?;
        let f = std::fs::File::open(&bytes_path).map_err(io::Error::other)?;
        f.sync_all().map_err(io::Error::other)?;
        drop(f);
        let stored_meta = StoredSnapshotMeta {
            last_log_id,
            last_membership,
            bytes_filename,
        };
        g.snapshot_meta_sv
            .store(&stored_meta)
            .map_err(io::Error::other)?
            .wait()
            .map_err(io::Error::other)?;

        g.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: bytes.clone(),
        });
        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(bytes),
        })
    }
}

/// Resolve the full Raft [`RaftLogId`] (term + index) for committed log `index`
/// by reading that record from the journal. `build_snapshot` uses this to label a
/// snapshot with the index its bytes actually represent (the service's freeze-time
/// index), which the async build can move off the node's scope-1 frontier. The
/// journal seq IS the raft log index (see `drive_catchup`'s replay loop).
fn log_id_at(journal: &ultima_journal::Journal, index: u64) -> Result<RaftLogId, io::Error> {
    let mut iter = journal
        .iter_range(index..(index + 1))
        .map_err(|e| io::Error::other(format!("snapshot: iter_range({index}): {e}")))?;
    let record = iter.next().ok_or_else(|| {
        io::Error::other(format!(
            "snapshot: journal has no record at index {index} (purged or gap)"
        ))
    })?;
    let (_seq, _meta, payload) =
        record.map_err(|e| io::Error::other(format!("snapshot: journal read at {index}: {e}")))?;
    let (entry, _) = bincode::serde::decode_from_slice::<
        <TypeConfig as openraft::RaftTypeConfig>::Entry,
        _,
    >(&payload, bincode::config::standard())
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(entry.log_id)
}

/// The "non-advancing" snapshot returned when a build cannot safely persist (the
/// epoch raced a reattach, or the freeze index can't be resolved): the last-good
/// persisted snapshot unchanged, or an empty `last_log_id = None` snapshot if none
/// exists. openraft's `update_snapshot` only advances/purges when `last_log_id` is
/// STRICTLY GREATER than the current pointer, so neither result advances the
/// snapshot or purges the log.
fn last_good_or_empty<S: StateMachine>(g: &ShmemInner<S>) -> RaftSnapshot {
    if let Some(s) = &g.current_snapshot {
        return Snapshot {
            meta: s.meta.clone(),
            snapshot: Cursor::new(s.data.clone()),
        };
    }
    Snapshot {
        meta: RaftSnapshotMeta {
            last_log_id: None,
            last_membership: Default::default(),
            snapshot_id: "snap-0".to_string(),
        },
        snapshot: Cursor::new(Vec::new()),
    }
}
