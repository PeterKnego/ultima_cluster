//! Node-side heartbeat producer.
//!
//! Mirror of `uc_service::runtime::liveness`: ticks
//! [`NodeStatus::{heartbeat_seq, heartbeat_at_ns}`] every 100 ms (relaxed
//! atomics — the field is a free-running counter). The service side's
//! [`uc_protocol::liveness::HeartbeatWatcher`] reads these to detect a
//! stalled node.
//!
//! Shutdown: caller sets [`LivenessHandle::stop`] and `await`s the join.
//!
//! # Safety
//!
//! [`spawn_liveness`] captures the pointed-to `NodeStatus` for the task's
//! lifetime. The caller (the node-side runtime) must keep the cnc mmap
//! alive at least until the task's join completes.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::task::JoinHandle;
use uc_protocol::cnc::NodeStatus;
use uc_protocol::liveness::tick_node;

const HEARTBEAT_PERIOD: Duration = Duration::from_millis(100);

pub struct LivenessHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

/// Spawn the node-side liveness ticker.
///
/// # Safety
///
/// `status_ptr` must point at a `NodeStatus` that stays valid until the
/// returned task is joined.
pub unsafe fn spawn_liveness(status_ptr: *const NodeStatus) -> LivenessHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);

    // Lift to `&'static NodeStatus` so the future is `Send`. NodeStatus
    // is `Sync` (all-atomic fields + padding); the 'static is upheld by
    // the caller keeping the cnc mmap alive across the task's lifetime.
    // SAFETY: see function-level # Safety.
    let status: &'static NodeStatus = unsafe { &*status_ptr };

    let join = tokio::spawn(async move {
        while !stop_for_task.load(Ordering::Relaxed) {
            let now_ns = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            tick_node(status, now_ns);
            tokio::time::sleep(HEARTBEAT_PERIOD).await;
        }
    });
    LivenessHandle { join, stop }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, AtomicU64};

    fn fresh_node_status() -> Box<NodeStatus> {
        Box::new(NodeStatus {
            role: AtomicU32::new(0),
            current_term: AtomicU64::new(0),
            leader_node_id: AtomicU64::new(u64::MAX),
            last_applied: AtomicU64::new(0),
            last_committed: AtomicU64::new(0),
            heartbeat_seq: AtomicU64::new(0),
            heartbeat_at_ns: AtomicU64::new(0),
            _pad: [0; 8],
        })
    }

    #[tokio::test]
    async fn ticker_advances_heartbeat_seq() {
        let status = fresh_node_status();
        let ptr: *const NodeStatus = &*status;
        // SAFETY: `status` (heap-owned) outlives `handle` which we join
        // before dropping `status`.
        let handle = unsafe { spawn_liveness(ptr) };

        // Wait long enough for ≥ 2 ticks at the 100 ms period.
        tokio::time::sleep(Duration::from_millis(250)).await;
        let seq_after = status.heartbeat_seq.load(Ordering::Relaxed);
        assert!(
            seq_after >= 2,
            "expected heartbeat_seq ≥ 2 after 250 ms, got {seq_after}"
        );
        let at_ns = status.heartbeat_at_ns.load(Ordering::Relaxed);
        assert!(at_ns > 0, "heartbeat_at_ns should be non-zero");

        handle.stop.store(true, Ordering::Relaxed);
        let _ = handle.join.await;
    }
}
