//! Node-side watcher for the service's liveness heartbeat.
//!
//! Polls `ServiceStatus::heartbeat_seq` via [`HeartbeatWatcher`]; on a
//! detected stall (no advance within `timeout`) sets a public
//! `AtomicBool` and — if this node is the raft leader — calls
//! `raft.shutdown()`. The remaining voters re-elect, the cluster keeps
//! moving, and the freshly-stalled node stays out for the rest of this
//! process lifetime.
//!
//! # Why `raft.shutdown` instead of a proper leader transfer
//!
//! The design (see `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md`
//! §10) calls for `Raft::trigger_leader_transfer` here. That API only
//! exists in openraft 0.10+; we're on 0.9.24. The available primitives
//! that surrender leadership are:
//!
//!   * `raft.shutdown()` — terminates this node's raft entirely. Cluster
//!     re-elects from the remaining voters.
//!   * `raft.change_membership(set_without_self, retain=true)` — leader
//!     issues a config change removing itself; complex to drive from
//!     inside a watcher.
//!
//! We pick `raft.shutdown()` for M3 as the simplest primitive that
//! achieves the stated outcome (cluster continues, new leader elects).
//! M4 will swap in a real transfer when we upgrade.
//!
//! # Safety
//!
//! [`spawn_service_watcher`] captures the pointed-to `ServiceStatus` for
//! the task's lifetime. The caller must keep the cnc mmap alive until the
//! task is joined — in practice the node-side `Instance` lives in
//! [`crate::NodeHandle::_instance`] and is dropped only after
//! [`crate::NodeHandle::shutdown`] joins this task.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use openraft::Raft;
use tokio::task::JoinHandle;

use uc_protocol::cnc::ServiceStatus;
use uc_protocol::liveness::HeartbeatWatcher;

use crate::raft::{NodeId, TypeConfig};

const POLL_PERIOD: Duration = Duration::from_millis(100);

/// Default time without an advancing service `heartbeat_seq` after which
/// the watcher declares the service stalled. The service ticks every
/// 100 ms; 2 s leaves a 20× margin over scheduling jitter.
pub const DEFAULT_LIVENESS_TIMEOUT: Duration = Duration::from_millis(2000);

pub struct ServiceWatcherHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
    /// Observable flag — flipped to `true` on stall detection, back to
    /// `false` if the service heartbeat resumes (M3 only flips to `true`;
    /// resumption is a no-op until the M4 reconnect path exists).
    pub stalled: Arc<AtomicBool>,
}

/// Spawn the service-liveness watcher.
///
/// # Safety
///
/// `status_ptr` must point at a `ServiceStatus` that stays valid until the
/// returned task is joined.
pub unsafe fn spawn_service_watcher(
    status_ptr: *const ServiceStatus,
    raft: Raft<TypeConfig>,
    node_id: NodeId,
    timeout: Duration,
) -> ServiceWatcherHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let stalled = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);
    let stalled_for_task = Arc::clone(&stalled);

    // Lift to `&'static`: ServiceStatus is `Sync` (all-atomic), and the
    // caller pins the cnc mmap for the task's lifetime.
    // SAFETY: see function-level # Safety.
    let status: &'static ServiceStatus = unsafe { &*status_ptr };

    let join = tokio::spawn(async move {
        let initial_now_ns = now_ns();
        let initial_seq = status.heartbeat_seq.load(Ordering::Relaxed);
        let mut watcher = HeartbeatWatcher::new(initial_seq, initial_now_ns);
        let timeout_ns = timeout.as_nanos() as u64;

        while !stop_for_task.load(Ordering::Relaxed) {
            let alive = watcher.poll_service(status, now_ns(), timeout_ns);

            if !alive && !stalled_for_task.load(Ordering::Relaxed) {
                tracing::warn!(
                    node_id,
                    timeout_ms = timeout.as_millis() as u64,
                    "service heartbeat stalled"
                );
                stalled_for_task.store(true, Ordering::Relaxed);

                if raft.current_leader().await == Some(node_id) {
                    tracing::warn!(
                        node_id,
                        "this node was leader; calling raft.shutdown() \
                         (M3 substitute for Raft::trigger_leader_transfer)"
                    );
                    let _ = raft.shutdown().await;
                }
            } else if alive && stalled_for_task.load(Ordering::Relaxed) {
                tracing::info!(node_id, "service heartbeat resumed");
                stalled_for_task.store(false, Ordering::Relaxed);
            }

            tokio::time::sleep(POLL_PERIOD).await;
        }
    });

    ServiceWatcherHandle {
        join,
        stop,
        stalled,
    }
}

#[inline]
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}
