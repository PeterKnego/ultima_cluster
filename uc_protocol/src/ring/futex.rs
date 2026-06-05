//! Cross-process futex wait/wake over a 32-bit word living in shared memory.
//!
//! The word is the low 32 bits of a ring's `publish_position` (see
//! `RingHeader::wake_word`). We deliberately do NOT pass `FUTEX_PRIVATE_FLAG`:
//! the client, node, and service are separate processes sharing the ring mmap,
//! so the futex must be a shared (inter-process) futex.

use std::sync::atomic::AtomicU32;
use std::time::Duration;

/// Block until `*word != expected`, a wake arrives, or `timeout` elapses.
/// Returns regardless of which (the caller re-checks state in a loop). All of
/// EAGAIN (value already changed), ETIMEDOUT, and EINTR collapse to "return".
#[cfg(target_os = "linux")]
pub fn futex_wait(word: &AtomicU32, expected: u32, timeout: Duration) {
    let ts = libc::timespec {
        tv_sec: timeout.as_secs() as libc::time_t,
        tv_nsec: timeout.subsec_nanos() as libc::c_long,
    };
    // SAFETY: `word` points into a live mmap; FUTEX_WAIT reads it atomically.
    // No FUTEX_PRIVATE_FLAG -> inter-process futex on the shared mapping.
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            word as *const AtomicU32 as *const u32,
            libc::FUTEX_WAIT,
            expected,
            &ts as *const libc::timespec,
        );
    }
}

/// Wake up to `n` consumers parked on `word` (`i32::MAX` == all).
#[cfg(target_os = "linux")]
pub fn futex_wake(word: &AtomicU32, n: i32) {
    // SAFETY: same shared mapping; FUTEX_WAKE only reads the address as a key.
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            word as *const AtomicU32 as *const u32,
            libc::FUTEX_WAKE,
            n,
        );
    }
}

/// Non-Linux fallback: no kernel wait primitive — callers rely on the timeout
/// backstop (the `Poll` `ParkMode`). These should not be reached on Linux.
#[cfg(not(target_os = "linux"))]
pub fn futex_wait(_word: &AtomicU32, _expected: u32, timeout: Duration) {
    std::thread::sleep(timeout);
}
#[cfg(not(target_os = "linux"))]
pub fn futex_wake(_word: &AtomicU32, _n: i32) {}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    #[test]
    fn wake_unblocks_a_parked_waiter() {
        let word = Arc::new(AtomicU32::new(0));
        let w2 = word.clone();
        let start = Instant::now();
        let h = std::thread::spawn(move || {
            // park expecting 0; the waker will store 1 then wake.
            futex_wait(&w2, 0, Duration::from_secs(5));
        });
        std::thread::sleep(Duration::from_millis(50));
        word.store(1, Ordering::Release);
        futex_wake(&word, 1);
        h.join().unwrap();
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "wake should be prompt, not timeout"
        );
    }

    #[test]
    fn wait_returns_immediately_when_value_already_changed() {
        let word = AtomicU32::new(7);
        let start = Instant::now();
        futex_wait(&word, 0, Duration::from_secs(5)); // expected!=actual -> EAGAIN, immediate
        assert!(start.elapsed() < Duration::from_millis(500));
    }

    #[test]
    fn wait_times_out_with_no_waker() {
        let word = AtomicU32::new(0);
        let start = Instant::now();
        futex_wait(&word, 0, Duration::from_millis(150));
        assert!(start.elapsed() >= Duration::from_millis(100));
    }
}
