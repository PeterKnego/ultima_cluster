//! Client-side liveness watchers for NodeStatus and ServiceStatus.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::task::JoinHandle;

use uc_protocol::cnc::{NodeStatus, ServiceStatus};
use uc_protocol::liveness::HeartbeatWatcher;

const POLL_PERIOD: Duration = Duration::from_millis(100);
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

pub struct StallWatchers {
    pub node_stalled: Arc<AtomicBool>,
    pub service_stalled: Arc<AtomicBool>,
    pub join_node: JoinHandle<()>,
    pub join_service: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

/// Spawn both stall watchers. `node_status_ptr` / `service_status_ptr`
/// must outlive the returned tasks (kept alive by the Client's CncAttach).
///
/// # Safety
///
/// Both pointers must reference valid initialized cnc.dat regions for
/// the lifetime of the spawned tasks.
pub unsafe fn spawn_stall_watchers(
    node_status_ptr: *const NodeStatus,
    service_status_ptr: *const ServiceStatus,
) -> StallWatchers {
    let stop = Arc::new(AtomicBool::new(false));
    let node_stalled = Arc::new(AtomicBool::new(false));
    let service_stalled = Arc::new(AtomicBool::new(false));

    // Convert raw pointers to 'static references before spawning. The
    // references are Send+Sync because NodeStatus/ServiceStatus are all-atomic.
    // SAFETY: caller guarantees the pointers remain valid for 'static (pinned
    // to the CncAttach mmap which lives for the duration of Client).
    let node_ref: &'static NodeStatus = unsafe { &*node_status_ptr };
    let svc_ref: &'static ServiceStatus = unsafe { &*service_status_ptr };

    let join_node = {
        let stalled = Arc::clone(&node_stalled);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let init_ns = now_ns();
            let mut w =
                HeartbeatWatcher::new(node_ref.heartbeat_seq.load(Ordering::Relaxed), init_ns);
            while !stop.load(Ordering::Relaxed) {
                let alive = w.poll_node(node_ref, now_ns(), DEFAULT_TIMEOUT.as_nanos() as u64);
                stalled.store(!alive, Ordering::Relaxed);
                tokio::time::sleep(POLL_PERIOD).await;
            }
        })
    };
    let join_service = {
        let stalled = Arc::clone(&service_stalled);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let init_ns = now_ns();
            let mut w =
                HeartbeatWatcher::new(svc_ref.heartbeat_seq.load(Ordering::Relaxed), init_ns);
            while !stop.load(Ordering::Relaxed) {
                let alive = w.poll_service(svc_ref, now_ns(), DEFAULT_TIMEOUT.as_nanos() as u64);
                stalled.store(!alive, Ordering::Relaxed);
                tokio::time::sleep(POLL_PERIOD).await;
            }
        })
    };

    StallWatchers {
        node_stalled,
        service_stalled,
        join_node,
        join_service,
        stop,
    }
}

#[inline]
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}
