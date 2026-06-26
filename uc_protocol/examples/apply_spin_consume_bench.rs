//! Direction microbench for O1 (docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md):
//! consume-wakeup latency of the SPSC apply consumer in **park** mode (SPIN_TRIES
//! then FUTEX_WAIT) vs **busy** mode (`set_spin_budget(u32::MAX)`), over a real
//! `SpscRing` (Futex ParkMode on Linux — the actual code path).
//!
//! Two regimes, because O1 only helps one:
//!   - saturated: hot producer; the consumer's 64-spin window usually catches the
//!     record, so it rarely parks and busy-spin barely helps.
//!   - spaced: a gap between messages forces the consumer to exhaust its spin
//!     budget and PARK, so park mode pays the ~us futex wakeup that busy-spin skips.
//!
//! Run: `cargo run -p uc_protocol --release --example apply_spin_consume_bench`

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use uc_protocol::ring::SpscRing;

const SATURATED_RTS: u32 = 50_000;
const SPACED_RTS: u32 = 5_000;
const SPACED_GAP: Duration = Duration::from_micros(200);

fn tmp_path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("uc-spin-bench-{}-{tag}.ring", std::process::id()))
}

/// Summed round-trip time (excluding inter-message gaps). `gap`: Some -> sleep
/// before each timed round-trip so the responder's consumer parks (low-rate
/// regime); None -> hot producer (saturated regime).
fn run(busy: bool, gap: Option<Duration>, round_trips: u32) -> Duration {
    let fwd_path = tmp_path("fwd");
    let back_path = tmp_path("back");
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
    for _ in 0..1000 {
        while fwd_tx.try_write(1, 0, [0; 8], b"a").is_err() {
            std::hint::spin_loop();
        }
        loop {
            if let Ok(Some(_)) = back_rx.try_read(&mut buf) {
                break;
            }
            std::hint::spin_loop();
        }
    }

    let mut total = Duration::ZERO;
    for _ in 0..round_trips {
        if let Some(g) = gap {
            thread::sleep(g);
        }
        let t0 = Instant::now();
        while fwd_tx.try_write(1, 0, [0; 8], b"a").is_err() {
            std::hint::spin_loop();
        }
        loop {
            if let Ok(Some(_)) = back_rx.try_read(&mut buf) {
                break;
            }
            std::hint::spin_loop();
        }
        total += t0.elapsed();
    }

    stop.store(true, Ordering::Relaxed);
    let _ = fwd_tx.try_write(1, 0, [0; 8], b"a");
    responder.join().unwrap();
    let _ = std::fs::remove_file(&fwd_path);
    let _ = std::fs::remove_file(&back_path);
    total
}

fn per_rt_ns(total: Duration, n: u32) -> f64 {
    total.as_nanos() as f64 / n as f64
}

fn main() {
    println!("== apply-consumer consume latency: park vs busy (release) ==");

    println!("\n-- saturated (hot producer; 64-spin window usually catches it, rarely parks) --");
    let sat_park = run(false, None, SATURATED_RTS);
    let sat_busy = run(true, None, SATURATED_RTS);
    println!("park  {:>9.0} ns/rt", per_rt_ns(sat_park, SATURATED_RTS));
    println!("busy  {:>9.0} ns/rt", per_rt_ns(sat_busy, SATURATED_RTS));
    println!(
        "delta {:>9.0} ns/rt (near-zero/noisy, sign varies: when hot the consumer rarely parks, so busy-spin saves no futex wakeup — and its 256-spin chunks can cost MORE than park's short spin)",
        per_rt_ns(sat_park, SATURATED_RTS) - per_rt_ns(sat_busy, SATURATED_RTS)
    );

    println!(
        "\n-- spaced (~{}us gap; consumer parks between messages) --",
        SPACED_GAP.as_micros()
    );
    let sp_park = run(false, Some(SPACED_GAP), SPACED_RTS);
    let sp_busy = run(true, Some(SPACED_GAP), SPACED_RTS);
    println!("park  {:>9.0} ns/rt", per_rt_ns(sp_park, SPACED_RTS));
    println!("busy  {:>9.0} ns/rt", per_rt_ns(sp_busy, SPACED_RTS));
    println!(
        "delta {:>9.0} ns/rt (the O1 win: busy skips the ~us futex wakeup the parked consumer pays)",
        per_rt_ns(sp_park, SPACED_RTS) - per_rt_ns(sp_busy, SPACED_RTS)
    );
}
