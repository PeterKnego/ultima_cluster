//! Direction microbench for O1 (`docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md`):
//! consume-wakeup latency of the SPSC apply consumer in **park** mode (default
//! SPIN_TRIES then FUTEX_WAIT) vs **busy** mode (`set_spin_budget(u32::MAX)`),
//! over a *real* `SpscRing` (Futex ParkMode on Linux — the actual code path).
//!
//! Ping-pong: main writes a token on `fwd`, a responder thread consumes it via
//! `read_or_park` (the mode under test) and writes an ack on `back`; main
//! busy-reads the ack (so only the responder's consume mechanism varies). One
//! round trip ~= one fwd publish->consume wakeup + one ack.
//!
//! Run: `cargo run -p uc_protocol --release --example apply_spin_consume_bench`

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use uc_protocol::ring::SpscRing;

const ROUND_TRIPS: u32 = 50_000;

fn tmp_path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("uc-spin-bench-{}-{tag}.ring", std::process::id()))
}

fn run(busy: bool) -> Duration {
    let fwd_path = tmp_path(if busy { "busy-fwd" } else { "park-fwd" });
    let back_path = tmp_path(if busy { "busy-back" } else { "park-back" });
    let fwd = SpscRing::create(&fwd_path, 4096, 1024).expect("fwd");
    let back = SpscRing::create(&back_path, 4096, 1024).expect("back");
    let (mut fwd_tx, mut fwd_rx) = fwd.into_split();
    let (mut back_tx, mut back_rx) = back.into_split();
    if busy {
        fwd_rx.set_spin_budget(u32::MAX);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let s2 = stop.clone();

    let responder = thread::Builder::new()
        .name("responder".into())
        .spawn(move || {
            let mut buf = Vec::new();
            while !s2.load(Ordering::Relaxed) {
                if let Ok(Some(_)) = fwd_rx.read_or_park(&mut buf) {
                    while back_tx.try_write(1, 0, [0; 8], b"a").is_err() {
                        std::hint::spin_loop();
                    }
                }
            }
        })
        .unwrap();

    let mut buf = Vec::new();
    // warm up
    for _ in 0..1000 {
        while fwd_tx.try_write(1, 0, [0; 8], b"a").is_err() { std::hint::spin_loop(); }
        loop {
            if let Ok(Some(_)) = back_rx.try_read(&mut buf) { break; }
            std::hint::spin_loop();
        }
    }

    let start = Instant::now();
    for _ in 0..ROUND_TRIPS {
        while fwd_tx.try_write(1, 0, [0; 8], b"a").is_err() { std::hint::spin_loop(); }
        loop {
            if let Ok(Some(_)) = back_rx.try_read(&mut buf) { break; }
            std::hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();

    stop.store(true, Ordering::Relaxed);
    // nudge the responder out of a park so it observes stop
    let _ = fwd_tx.try_write(1, 0, [0; 8], b"a");
    responder.join().unwrap();
    let _ = std::fs::remove_file(&fwd_path);
    let _ = std::fs::remove_file(&back_path);
    elapsed
}

fn report(name: &str, d: Duration) {
    let rt = d.as_nanos() as f64 / ROUND_TRIPS as f64;
    println!("{name:<22} {rt:>9.0} ns/round-trip   ({ROUND_TRIPS} round-trips)");
}

fn main() {
    println!("== apply-consumer consume latency: park vs busy (release) ==");
    let park = run(false);
    let busy = run(true);
    report("park (SPIN_TRIES)", park);
    report("busy (u32::MAX)", busy);
    let d = (park.as_nanos() as f64 - busy.as_nanos() as f64) / ROUND_TRIPS as f64;
    println!("\nbusy-mode saves ~{d:.0} ns/round-trip on the consume wakeup");
}
