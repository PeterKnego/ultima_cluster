//! Node-side handshake watcher: poll `ServiceStatus::state` until it
//! transitions to `Ready`, or time out.
//!
//! The full handshake protocol also involves consuming a `ServiceReady`
//! frame off the cnc `control_to_node` MPSC ring (which carries the
//! service's `last_applied` for back-fill cross-checks). Reading from a
//! cnc-resident MPSC ring needs the sub-mmap attach API; until that
//! lands, watching `ServiceStatus::state` is sufficient for the
//! in-process test harness — the service-side runtime sets the state
//! to `Ready` immediately after spawning its loops.

use std::future::Future;
use std::time::Duration;

use thiserror::Error;
use uc_protocol::cnc::{ServiceStatus, service_state};

const POLL_PERIOD: Duration = Duration::from_millis(50);

#[derive(Debug, Error)]
pub enum HandshakeError {
    #[error("timed out waiting for service handshake after {0:?}")]
    TimedOut(Duration),
}

/// Wait for the service to publish `state = Ready` on its `ServiceStatus`.
///
/// Returns an `impl Future + Send`. The sync entry point lifts the raw
/// pointer to a `&'static ServiceStatus` (sound because `ServiceStatus`
/// is `Sync` and the caller pins the cnc mmap for the future's lifetime,
/// see Safety); the future then only holds a `Send` reference across
/// awaits.
///
/// # Safety
///
/// `status_ptr` must point to a `ServiceStatus` that stays valid for the
/// entire wait. Typically the caller holds the cnc.dat mapping in the
/// node-side `Instance` handle.
pub unsafe fn wait_for_service_ready(
    status_ptr: *const ServiceStatus,
    timeout: Duration,
) -> impl Future<Output = Result<(), HandshakeError>> + Send {
    // SAFETY: see function-level # Safety.
    let status: &'static ServiceStatus = unsafe { &*status_ptr };

    async move {
        let start = std::time::Instant::now();
        loop {
            if status.state.load(std::sync::atomic::Ordering::Acquire) == service_state::READY {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                return Err(HandshakeError::TimedOut(timeout));
            }
            tokio::time::sleep(POLL_PERIOD).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

    fn fresh_service_status() -> Arc<ServiceStatus> {
        Arc::new(ServiceStatus {
            state: AtomicU32::new(service_state::HANDSHAKING),
            _pad_1: 0,
            last_applied: AtomicU64::new(0),
            last_output_ack: AtomicU64::new(0),
            heartbeat_seq: AtomicU64::new(0),
            heartbeat_at_ns: AtomicU64::new(0),
            service_pid: AtomicU32::new(0),
            _pad_2a: 0,
            service_epoch: AtomicU64::new(0),
            _pad_2: [0; 8],
        })
    }

    #[tokio::test]
    async fn returns_ok_when_state_flips_to_ready() {
        let status = fresh_service_status();
        let ptr: *const ServiceStatus = Arc::as_ptr(&status);

        // Flip to Ready after a short delay on a separate task.
        let status_for_flip = Arc::clone(&status);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            status_for_flip
                .state
                .store(service_state::READY, Ordering::Release);
        });

        // SAFETY: `status` (Arc) outlives this await.
        let r = unsafe { wait_for_service_ready(ptr, Duration::from_secs(2)).await };
        assert!(r.is_ok(), "expected Ok, got {r:?}");
    }

    #[tokio::test]
    async fn returns_timed_out_when_state_never_flips() {
        let status = fresh_service_status();
        let ptr: *const ServiceStatus = Arc::as_ptr(&status);

        // SAFETY: `status` (Arc) outlives this await.
        let r = unsafe { wait_for_service_ready(ptr, Duration::from_millis(150)).await };
        assert!(
            matches!(r, Err(HandshakeError::TimedOut(_))),
            "expected TimedOut, got {r:?}"
        );
    }

    #[tokio::test]
    async fn returns_immediately_if_already_ready() {
        let status = fresh_service_status();
        status.state.store(service_state::READY, Ordering::Release);
        let ptr: *const ServiceStatus = Arc::as_ptr(&status);

        let start = std::time::Instant::now();
        // SAFETY: see above.
        let r = unsafe { wait_for_service_ready(ptr, Duration::from_secs(2)).await };
        assert!(r.is_ok());
        // First iteration of the loop checks before sleeping, so we
        // should return well under one poll period.
        assert!(start.elapsed() < POLL_PERIOD);
    }
}
