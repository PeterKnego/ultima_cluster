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
    /// `spin_budget`: `0` = park immediately (default); `u32::MAX` = busy-spin
    /// (never park, notify only on a real `current_seq` change); finite `N` =
    /// spin `N` times looking for a change, then park.
    pub fn spawn(handle: RingWaitHandle, name: &'static str, spin_budget: u32) -> Self {
        let notify = Arc::new(Notify::new());
        let stop = Arc::new(AtomicBool::new(false));
        let waker = handle.clone();
        let n = notify.clone();
        let s = stop.clone();
        let join = std::thread::Builder::new()
            .name(format!("ring-park-{name}"))
            .spawn(move || {
                handle.arm();
                let mut last = handle.current_seq();
                while !s.load(Ordering::Acquire) {
                    if spin_budget == u32::MAX {
                        // Busy: spin until the wakeup word changes or we stop;
                        // notify ONLY on a real change (never park, no syscall).
                        loop {
                            let now = handle.current_seq();
                            if now != last {
                                last = now;
                                n.notify_one();
                                break;
                            }
                            if s.load(Ordering::Acquire) {
                                break;
                            }
                            std::hint::spin_loop();
                        }
                    } else {
                        // Spin up to `spin_budget` looking for a change (0 = none),
                        // then park up to PARK_CEIL. Notify after either path —
                        // a spurious notify on timeout is tolerated (the consumer
                        // re-checks via try_read), matching the prior behavior.
                        let mut changed = false;
                        for _ in 0..spin_budget {
                            let now = handle.current_seq();
                            if now != last {
                                last = now;
                                changed = true;
                                break;
                            }
                            std::hint::spin_loop();
                        }
                        if !changed {
                            let seq = handle.current_seq();
                            handle.park(seq, PARK_CEIL);
                            last = handle.current_seq();
                        }
                        n.notify_one();
                    }
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

/// Parse `UC_NODE_BRIDGE_SPIN_BUDGET` into a parker spin budget. Pure (testable):
/// `None`/unparseable -> `0` (park immediately, today's behavior); `busy`/`max`
/// (case-insensitive) -> `u32::MAX` (pure busy-spin); `<N>` -> N (spin then park).
pub fn parse_bridge_spin_budget(v: Option<&str>) -> u32 {
    match v {
        Some(s) if s.trim().eq_ignore_ascii_case("busy") || s.trim().eq_ignore_ascii_case("max") => {
            u32::MAX
        }
        Some(s) => s.trim().parse::<u32>().unwrap_or(0),
        None => 0,
    }
}

/// Read the node bridge spin budget from the environment.
pub fn bridge_spin_budget() -> u32 {
    parse_bridge_spin_budget(std::env::var("UC_NODE_BRIDGE_SPIN_BUDGET").ok().as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc_protocol::ring::SpscRing;

    fn tmp_ring_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("uc-bridge-test-{}-{tag}.ring", std::process::id()))
    }

    #[test]
    fn parse_bridge_spin_budget_cases() {
        assert_eq!(parse_bridge_spin_budget(None), 0);
        assert_eq!(parse_bridge_spin_budget(Some("busy")), u32::MAX);
        assert_eq!(parse_bridge_spin_budget(Some("BUSY")), u32::MAX);
        assert_eq!(parse_bridge_spin_budget(Some("max")), u32::MAX);
        assert_eq!(parse_bridge_spin_budget(Some("MAX")), u32::MAX);
        assert_eq!(parse_bridge_spin_budget(Some(" 256 ")), 256);
        assert_eq!(parse_bridge_spin_budget(Some("garbage")), 0);
    }

    // Busy mode: the parker notifies on a publish and shuts down cleanly.
    #[tokio::test]
    async fn busy_bridge_notifies_on_publish_and_shuts_down() {
        let path = tmp_ring_path("busy");
        let ring = SpscRing::create(&path, 4096, 1024).expect("create");
        let (mut producer, consumer) = ring.into_split();
        let bridge = NotifyBridge::spawn(consumer.wait_handle(), "test-busy", u32::MAX);

        // Mirror the real consumer: the parker snapshots `current_seq` at startup
        // and notifies on a LATER change. Let it start watching, THEN publish, so
        // the publish is a real change the parker observes (a publish before the
        // snapshot would be one the consumer's own try_read handles, not notify).
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        producer.try_write(7, 0, [0; 8], b"hi").expect("write");
        let res = tokio::time::timeout(std::time::Duration::from_secs(1), bridge.notified()).await;
        assert!(res.is_ok(), "busy bridge did not notify on publish");

        drop(bridge); // shutdown joins the parker; test completing => no hang
        let _ = std::fs::remove_file(&path);
    }

    // Default (budget 0 = park): still notifies on a publish (via the park path).
    #[tokio::test]
    async fn park_bridge_notifies_on_publish() {
        let path = tmp_ring_path("park");
        let ring = SpscRing::create(&path, 4096, 1024).expect("create");
        let (mut producer, consumer) = ring.into_split();
        let bridge = NotifyBridge::spawn(consumer.wait_handle(), "test-park", 0);

        // Let the parker reach its park, then publish so the publish wakes it.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        producer.try_write(7, 0, [0; 8], b"hi").expect("write");
        let res = tokio::time::timeout(std::time::Duration::from_secs(1), bridge.notified()).await;
        assert!(res.is_ok(), "park bridge did not notify on publish");

        drop(bridge);
        let _ = std::fs::remove_file(&path);
    }
}
