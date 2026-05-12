//! Service-side heartbeat producer.
//!
//! Ticks [`ServiceStatus::heartbeat_seq`] + `heartbeat_at_ns` on a 100 ms
//! cadence (relaxed atomics — the field is a free-running counter, not a
//! synchronization point). The node side's `HeartbeatWatcher` reads these
//! to detect a stalled service.
//!
//! Shutdown: caller sets [`LivenessHandle::stop`] and `await`s the join.
//!
//! # Safety
//!
//! The task captures a raw `*const ServiceStatus` that points into the
//! `cnc.dat` mmap. The caller (currently
//! [`super::service::ServiceBuilder::run`]) is responsible for keeping
//! the cnc mmap alive at least until [`LivenessHandle::join`] completes.
//! The atomics on `ServiceStatus` provide the cross-thread / cross-process
//! synchronization.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::task::JoinHandle;
use uc_protocol::cnc::ServiceStatus;
use uc_protocol::liveness::tick_service;

const HEARTBEAT_PERIOD: Duration = Duration::from_millis(100);

pub struct LivenessHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

/// Spawn the liveness ticker.
///
/// # Safety
///
/// `status_ptr` must point at a `ServiceStatus` that stays valid until the
/// returned task is joined. The owner of the underlying mmap (the
/// [`super::service::Service`] handle) is responsible for this.
pub unsafe fn spawn_liveness(status_ptr: *const ServiceStatus) -> LivenessHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);

    // Lift the raw pointer to a `&'static ServiceStatus` so the future is
    // `Send` (raw pointers aren't `Send`; `&'static T where T: Sync` is).
    // ServiceStatus is `Sync` thanks to its atomic fields. The 'static
    // lifetime claim is upheld externally by the cnc mmap living inside
    // Service for the task's lifetime.
    // SAFETY: see function-level # Safety contract.
    let status: &'static ServiceStatus = unsafe { &*status_ptr };

    let join = tokio::spawn(async move {
        while !stop_for_task.load(Ordering::Relaxed) {
            let now_ns = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            tick_service(status, now_ns);
            tokio::time::sleep(HEARTBEAT_PERIOD).await;
        }
    });
    LivenessHandle { join, stop }
}
