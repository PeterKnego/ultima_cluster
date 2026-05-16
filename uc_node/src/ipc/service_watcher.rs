//! Node-side watcher for the service's liveness heartbeat.
//!
//! Polls `ServiceStatus::heartbeat_seq` via [`HeartbeatWatcher`]; on a
//! detected stall (no advance within `timeout`) sets a public
//! `AtomicBool` and — if this node is the raft leader — calls
//! `raft.trigger().transfer_leader(target)`. Strict target selection:
//! pick any voter in the current membership other than self. If the
//! transfer doesn't take within 15 s, fall back to `raft.shutdown()`
//! so a doomed transfer doesn't pin leadership indefinitely.
//!
//! # Safety
//!
//! [`spawn_service_watcher`] captures the pointed-to `ServiceStatus`
//! for the task's lifetime. The caller must keep the cnc mmap alive
//! until the task is joined — in practice the node-side `Instance`
//! lives in [`crate::NodeHandle::_instance`] and is dropped only after
//! [`crate::NodeHandle::shutdown`] joins this task.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::task::JoinHandle;

use uc_protocol::cnc::ServiceStatus;
use uc_protocol::liveness::HeartbeatWatcher;
use uc_service::StateMachine;

use crate::raft::NodeId;
use crate::runtime::node::RaftHandle;

const POLL_PERIOD: Duration = Duration::from_millis(100);

/// Default time without an advancing service `heartbeat_seq` after
/// which the watcher declares the service stalled. The service ticks
/// every 100 ms; 2 s leaves a 20× margin over scheduling jitter.
pub const DEFAULT_LIVENESS_TIMEOUT: Duration = Duration::from_millis(2000);

/// How long the watcher waits for `transfer_leader` to take effect
/// before falling back to `raft.shutdown()`.
///
/// After `trigger().transfer_leader(target)` is called the local node
/// immediately stops being leader (lease disabled), but `current_leader()`
/// (which reads the metrics snapshot) still returns `Some(self)` until a
/// *new* leader is elected.  With the default `RaftTuning` election window
/// of 1-2 s the new election completes in at most ~4-5 s on a loaded host;
/// adding a generous margin puts us at 15 s.  Setting this too short is the
/// cause of the flaky `service_crash_on_leader_transfers_leadership` test:
/// if the election hasn't propagated yet at fallback time the check
/// `current_leader() == Some(self)` is still true, the fallback fires
/// `raft.shutdown()`, and the test sees `current_leader() == None`.
const TRANSFER_FALLBACK_TIMEOUT: Duration = Duration::from_secs(15);

pub struct ServiceWatcherHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
    /// Observable flag — flipped to `true` on stall detection, back to
    /// `false` if the service heartbeat resumes.
    pub stalled: Arc<AtomicBool>,
}

/// Pick a transfer target (strict). Returns the first voter in the
/// current membership that isn't `self_node_id`, or `None` if no peer
/// is in the voter set (single-node cluster).
fn pick_transfer_target<S: StateMachine>(
    raft: &RaftHandle<S>,
    self_node_id: NodeId,
) -> Option<NodeId> {
    use openraft::rt::WatchReceiver as _;
    let metrics = raft.metrics();
    let m = metrics.borrow_watched();
    m.membership_config
        .voter_ids()
        .find(|id| *id != self_node_id)
}

/// Spawn the service-liveness watcher.
///
/// # Safety
///
/// `status_ptr` must point at a `ServiceStatus` that stays valid until
/// the returned task is joined.
pub(crate) unsafe fn spawn_service_watcher<S: StateMachine>(
    status_ptr: *const ServiceStatus,
    raft: RaftHandle<S>,
    node_id: NodeId,
    timeout: Duration,
) -> ServiceWatcherHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let stalled = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);
    let stalled_for_task = Arc::clone(&stalled);

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
                    match pick_transfer_target(&raft, node_id) {
                        Some(target) => {
                            tracing::warn!(
                                node_id,
                                target,
                                "this node was leader; calling \
                                 raft.trigger().transfer_leader(target)"
                            );
                            let _ = raft.transfer_leader(target).await;
                            spawn_fallback_shutdown(raft.clone(), node_id);
                        }
                        None => {
                            tracing::warn!(
                                node_id,
                                "no peer voter to transfer to; calling raft.shutdown()"
                            );
                            let _ = raft.shutdown().await;
                        }
                    }
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

/// Spawn the 15 s fallback that calls `raft.shutdown()` if we are still
/// the raft leader. Fire-and-forget orphan; `NodeHandle::shutdown` does
/// not join it. The fallback's `raft.shutdown()` is idempotent: if the
/// transfer succeeded and the node already learned of the new leader,
/// `current_leader()` returns `Some(new_leader) != Some(node_id)` and
/// the body is skipped. If the transfer genuinely timed out,
/// `current_leader()` still returns `Some(node_id)` and we fall back
/// to shutdown.
fn spawn_fallback_shutdown<S: StateMachine>(raft: RaftHandle<S>, node_id: NodeId) {
    tokio::spawn(async move {
        tokio::time::sleep(TRANSFER_FALLBACK_TIMEOUT).await;
        if raft.current_leader().await == Some(node_id) {
            tracing::warn!(
                node_id,
                "transfer_leader did not take within {:?}; calling raft.shutdown() fallback",
                TRANSFER_FALLBACK_TIMEOUT
            );
            let _ = raft.shutdown().await;
        }
    });
}

#[inline]
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}
