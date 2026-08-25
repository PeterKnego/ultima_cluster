// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `WaitCell` — a seq-stamped park/wake pair.
//!
//! Ported in shape (not as a dependency) from `uc2_client`'s
//! `RingWaitHandle` usage: this crate's small dependency set is an advertised
//! property, so the ~40 lines are copied rather than pulled in.
//!
//! The contract is the one every "check, then park" loop needs: a waiter
//! reads [`WaitCell::seq`] BEFORE it re-checks its condition, and passes that
//! value to [`WaitCell::park`]. A [`WaitCell::signal`] that lands between the
//! check and the park bumps the seq, so the park returns immediately instead
//! of sleeping through the wake it was told about.
//!
//! Task 5 wired the last of it up: the writer thread parks on the outgoing
//! ring's cell, the poller on the completion queue's `ready` cell and the
//! reader on its `drained` cell, all through the seq-observed pattern above.
//! Nothing here is unused any more.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

pub(crate) struct WaitCell {
    seq: AtomicU64,
    waiters: AtomicU32,
    lock: Mutex<()>,
    cv: Condvar,
}

impl WaitCell {
    pub(crate) fn new() -> WaitCell {
        WaitCell {
            seq: AtomicU64::new(0),
            waiters: AtomicU32::new(0),
            lock: Mutex::new(()),
            cv: Condvar::new(),
        }
    }

    pub(crate) fn seq(&self) -> u64 {
        self.seq.load(Ordering::Acquire)
    }

    /// Publish a wake. `SeqCst` on both the bump and the `waiters` load is
    /// load-bearing: it is what makes "signaller saw no waiters" imply
    /// "waiter will see the new seq" (store-buffer / Dekker ordering).
    pub(crate) fn signal(&self) {
        self.seq.fetch_add(1, Ordering::SeqCst);
        if self.waiters.load(Ordering::SeqCst) != 0 {
            let _g = self.lock.lock().unwrap();
            self.cv.notify_all();
        }
    }

    /// Park until the seq moves past `observed`, or `timeout` elapses.
    pub(crate) fn park(&self, observed: u64, timeout: Duration) {
        self.waiters.fetch_add(1, Ordering::SeqCst);
        let g = self.lock.lock().unwrap();
        if self.seq.load(Ordering::SeqCst) == observed {
            let _ = self.cv.wait_timeout(g, timeout).unwrap();
        } else {
            drop(g);
        }
        self.waiters.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Instant;

    #[test]
    fn a_signal_between_the_check_and_the_park_is_not_missed() {
        let c = WaitCell::new();
        let observed = c.seq();
        c.signal(); // the wake the waiter must not sleep through
        let t = Instant::now();
        c.park(observed, Duration::from_secs(5));
        assert!(t.elapsed() < Duration::from_secs(1), "park slept through a signal");
    }

    #[test]
    fn a_park_without_a_signal_returns_at_its_timeout() {
        let c = WaitCell::new();
        let observed = c.seq();
        let t = Instant::now();
        c.park(observed, Duration::from_millis(50));
        assert!(t.elapsed() >= Duration::from_millis(40), "park returned far too early");
    }

    #[test]
    fn a_signal_from_another_thread_wakes_a_parked_waiter() {
        let c = Arc::new(WaitCell::new());
        let observed = c.seq();
        let c2 = Arc::clone(&c);
        let h = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            c2.signal();
        });
        let t = Instant::now();
        c.park(observed, Duration::from_secs(10));
        assert!(t.elapsed() < Duration::from_secs(5), "waiter was not woken by the signal");
        h.join().unwrap();
    }
}
