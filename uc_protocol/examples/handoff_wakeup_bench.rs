//! Microbench: per-hop cross-thread handoff cost — **futex park/wake** vs
//! **busy-spin** — for the Aeron-vs-UC threading investigation
//! (`docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md`, §5).
//!
//! Measures a ping-pong round trip between two threads over a shared word. The
//! futex arm uses the *same* `FUTEX_WAIT`/`FUTEX_WAKE` syscall UC's ring park
//! path uses (`uc_protocol/src/ring/futex.rs`); the busy-spin arm replaces the
//! park with `spin_loop()`. The delta is what a hop saves by never parking —
//! i.e. the per-wakeup cost the inter-process ring hops (#1/#5/#7/#10 in §2)
//! pay, and what an Aeron-style polling consumer avoids.
//!
//! Run: `cargo run -p uc_protocol --release --example handoff_wakeup_bench`
//! (release matters — spin loops and syscall overhead are meaningless in debug).
//!
//! No criterion / no extra deps: criterion is not vendored in this repo and the
//! sandbox has no network. Plain `Instant` timing over a fixed iteration count.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const ROUND_TRIPS: u32 = 100_000;

// FUTEX_WAIT/WAKE on a shared word — identical to uc_protocol/src/ring/futex.rs
// (no FUTEX_PRIVATE_FLAG; here the threads share the process but the syscall
// path measured is the same one the cross-process rings take).
#[cfg(target_os = "linux")]
fn futex_wait(word: &AtomicU32, expected: u32) {
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            word as *const AtomicU32 as *const u32,
            libc::FUTEX_WAIT,
            expected,
            std::ptr::null::<libc::timespec>(),
        );
    }
}
#[cfg(target_os = "linux")]
fn futex_wake(word: &AtomicU32) {
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            word as *const AtomicU32 as *const u32,
            libc::FUTEX_WAKE,
            1i32,
        );
    }
}
#[cfg(not(target_os = "linux"))]
fn futex_wait(_word: &AtomicU32, _expected: u32) {
    std::thread::yield_now();
}
#[cfg(not(target_os = "linux"))]
fn futex_wake(_word: &AtomicU32) {}

/// Ping-pong: `turn` alternates 0 (producer's turn) ↔ 1 (consumer's turn).
/// One round trip = producer hands to consumer and gets it back = 2 wakeups.
fn bench_futex() -> Duration {
    let turn = Arc::new(AtomicU32::new(0));
    let t2 = turn.clone();
    let consumer = thread::spawn(move || {
        loop {
            // park while it's not our turn; producer sets 1 = work, 2 = stop.
            while t2.load(Ordering::Acquire) == 0 {
                futex_wait(&t2, 0);
            }
            let stop = t2.load(Ordering::Acquire) == 2;
            t2.store(0, Ordering::Release);
            futex_wake(&t2);
            if stop {
                break;
            }
        }
    });

    let start = Instant::now();
    for _ in 0..ROUND_TRIPS {
        turn.store(1, Ordering::Release);
        futex_wake(&turn);
        while turn.load(Ordering::Acquire) != 0 {
            futex_wait(&turn, 1);
        }
    }
    let elapsed = start.elapsed();

    // stop the consumer
    turn.store(2, Ordering::Release);
    futex_wake(&turn);
    consumer.join().unwrap();
    elapsed
}

fn bench_busyspin() -> Duration {
    let turn = Arc::new(AtomicU32::new(0));
    let t2 = turn.clone();
    let consumer = thread::spawn(move || {
        loop {
            while t2.load(Ordering::Acquire) == 0 {
                std::hint::spin_loop();
            }
            let stop = t2.load(Ordering::Acquire) == 2;
            t2.store(0, Ordering::Release);
            if stop {
                break;
            }
        }
    });

    let start = Instant::now();
    for _ in 0..ROUND_TRIPS {
        turn.store(1, Ordering::Release);
        while turn.load(Ordering::Acquire) != 0 {
            std::hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();

    turn.store(2, Ordering::Release);
    consumer.join().unwrap();
    elapsed
}

fn report(name: &str, d: Duration) {
    let rt_ns = d.as_nanos() as f64 / ROUND_TRIPS as f64;
    println!(
        "{name:<24} {rt_ns:>9.0} ns/round-trip   {:>9.0} ns/wakeup   ({ROUND_TRIPS} round-trips)",
        rt_ns / 2.0
    );
}

fn main() {
    // warm up scheduler / caches
    let _ = bench_busyspin();
    let _ = bench_futex();

    let spin = bench_busyspin();
    let futex = bench_futex();
    println!("== handoff wakeup microbench (release) ==");
    report("busyspin_roundtrip", spin);
    report("futex_roundtrip", futex);
    let delta = (futex.as_nanos() as f64 - spin.as_nanos() as f64) / ROUND_TRIPS as f64 / 2.0;
    println!("\nfutex park/wake cost over busy-spin: ~{delta:.0} ns/wakeup");
}
