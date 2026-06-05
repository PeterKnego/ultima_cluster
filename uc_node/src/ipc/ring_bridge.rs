//! Bridges a blocking ring futex-park to an async consumer.
//!
//! A `current_thread` tokio task cannot call the blocking `FUTEX_WAIT` without
//! stalling its runtime, so one dedicated OS thread parks on the ring's wakeup
//! word and fires a `tokio::sync::Notify` whenever the word changes (or every
//! `PARK_CEIL` as a backstop). The async consumer loops:
//! `match try_read { Some => .., None => bridge.notified().await }`.
//!
//! Lost-wakeup bound: if a publish lands in the gap between the consumer's
//! `try_read == None` and the parker's snapshot, the parker waits for the NEXT
//! change and the consumer is re-notified within `PARK_CEIL` (the backstop).
//! Correctness never depends on the wake; only sub-`PARK_CEIL` latency does.
//!
//! Shutdown is prompt: `shutdown()` keeps a cloned `RingWaitHandle` (`waker`) and
//! force-wakes the parker's `FUTEX_WAIT` so the join returns immediately rather
//! than blocking for the full `PARK_CEIL` — important because the bridge can be
//! dropped from an `async fn` on a current_thread runtime (e.g. node shutdown).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;
use uc_protocol::ring::{PARK_CEIL, RingWaitHandle};

pub struct NotifyBridge {
    notify: Arc<Notify>,
    stop: Arc<AtomicBool>,
    /// Clone of the parker's handle, retained so `shutdown()` can force-wake the
    /// parker's `FUTEX_WAIT` (the thread itself owns the other clone).
    waker: RingWaitHandle,
    join: Option<std::thread::JoinHandle<()>>,
}

impl NotifyBridge {
    /// Spawn the parker thread for `handle`. `name` is for diagnostics.
    pub fn spawn(handle: RingWaitHandle, name: &'static str) -> Self {
        let notify = Arc::new(Notify::new());
        let stop = Arc::new(AtomicBool::new(false));
        let waker = handle.clone();
        let n = notify.clone();
        let s = stop.clone();
        let join = std::thread::Builder::new()
            .name(format!("ring-park-{name}"))
            .spawn(move || {
                handle.arm();
                while !s.load(Ordering::Acquire) {
                    let seq = handle.current_seq();
                    handle.park(seq, PARK_CEIL);
                    n.notify_one();
                }
                handle.disarm();
            })
            .expect("spawn ring parker thread");
        Self {
            notify,
            stop,
            waker,
            join: Some(join),
        }
    }

    /// Await the next wakeup (or a stored permit if one is pending).
    pub async fn notified(&self) {
        self.notify.notified().await;
    }

    /// Stop the parker thread and join it promptly. Idempotent.
    pub fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Release);
        // Force-wake the parker out of FUTEX_WAIT so join() doesn't block up to
        // PARK_CEIL, then unblock any async awaiter.
        self.waker.wake();
        self.notify.notify_one();
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

impl Drop for NotifyBridge {
    fn drop(&mut self) {
        self.shutdown();
    }
}
