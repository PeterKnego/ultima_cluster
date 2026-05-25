//! shmem-microbench — the human-owned fitness function for the shmem ring
//! optimization task. Emits ONE flat JSON line of `{metric: f64}` on stdout
//! (consumed by the orchestrator via `BenchResult::from_json_line`). All
//! progress/diagnostics go to stderr; stdout carries exactly one line.
//!
//! Eight metrics (must match `tasks/shmem/task.toml` `[microbench] metrics`):
//!   spsc_p50_ns, spsc_p99_ns, spsc_throughput_msgs,
//!   mpsc_4p_p99_ns, mpsc_4p_throughput,
//!   broadcast_4sub_p99_ns, large_payload_p99_ns, wrap_throughput
//!
//! ## Latency method (single-thread round-trip)
//!
//! Measuring true cross-thread producer→consumer latency is dominated by
//! scheduler/wakeup noise and is not a stable optimization signal. Instead the
//! latency sub-benches run producer + consumer in ONE thread: write one record,
//! immediately read it back, time the write+read pair with a fresh `Instant`.
//! This isolates the ring's own enqueue/dequeue cost (header write, atomic
//! length publish, atomic length read, payload copy) which is exactly what a
//! ring rewrite changes — so relative improvements are faithfully reflected.
//!
//! For MPSC the round-trip uses one producer clone (the consumer still sees the
//! same per-record cost); for Broadcast the producer writes and all 4
//! subscribers read each record, timing the full fan-out pair. The 4-producer /
//! 4-subscriber THROUGHPUT figures use real multi-thread saturation.
//!
//! Each sub-bench: 3 warmup + 9 measured runs; we report the MEDIAN.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tempfile::TempDir;
use uc_protocol::ring::{BroadcastRing, MpscRing, RingError, SpscRing};

const WARMUP: usize = 3;
const MEASURED: usize = 9;

// on-wire framing: FrameHeader(16) + payload + crc(4)
const FRAME_OVERHEAD: usize = 20;

const THROUGHPUT_WINDOW: Duration = Duration::from_millis(250);

/// Round-trip pairs per latency run. Big enough for a stable p99, small enough
/// to keep total runtime well under budget.
const LAT_PAIRS: usize = 50_000;

fn max_msg_size_for(payload: usize) -> u32 {
    (payload + FRAME_OVERHEAD) as u32
}

fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    xs[xs.len() / 2]
}

fn percentile(mut xs: Vec<u64>, p: f64) -> f64 {
    xs.sort_unstable();
    let idx = (((xs.len() - 1) as f64) * p).round() as usize;
    xs[idx] as f64
}

/// Run a sub-bench `f` for WARMUP + MEASURED runs, returning the median of the
/// measured runs. `f` returns one f64 per run.
fn repeat_median(label: &str, mut f: impl FnMut() -> f64) -> f64 {
    for _ in 0..WARMUP {
        f();
    }
    let mut out = Vec::with_capacity(MEASURED);
    for _ in 0..MEASURED {
        out.push(f());
    }
    let m = median(out);
    eprintln!("  {label}: {m:.1}");
    m
}

// ===========================================================================
// Latency: single-thread round-trip (write one, read it back), time each pair.
// Returns (p50_ns, p99_ns) over LAT_PAIRS pairs.
// ===========================================================================

fn spsc_roundtrip(payload: usize) -> (u64, u64) {
    let dir = TempDir::new().unwrap();
    let ring = SpscRing::create(
        &dir.path().join("spsc_lat.ring"),
        1 << 16,
        max_msg_size_for(payload),
    )
    .unwrap();
    let (mut producer, mut consumer) = ring.into_split();
    let msg = vec![0xABu8; payload];
    let mut buf = Vec::with_capacity(payload);

    let mut lat = Vec::with_capacity(LAT_PAIRS);
    for _ in 0..LAT_PAIRS {
        let t = Instant::now();
        producer.try_write(1, 0, [0; 8], &msg).unwrap();
        loop {
            match consumer.try_read(&mut buf) {
                Ok(Some(_)) => break,
                Ok(None) => std::hint::spin_loop(),
                Err(e) => panic!("read error: {e}"),
            }
        }
        lat.push(t.elapsed().as_nanos() as u64);
    }
    (
        percentile(lat.clone(), 0.50) as u64,
        percentile(lat, 0.99) as u64,
    )
}

fn mpsc_roundtrip(payload: usize) -> u64 {
    let dir = TempDir::new().unwrap();
    let ring = MpscRing::create(
        &dir.path().join("mpsc_lat.ring"),
        1 << 16,
        max_msg_size_for(payload),
    )
    .unwrap();
    let (producer, mut consumer) = ring.into_split();
    let msg = vec![0xABu8; payload];
    let mut buf = Vec::with_capacity(payload);

    let mut lat = Vec::with_capacity(LAT_PAIRS);
    for _ in 0..LAT_PAIRS {
        let t = Instant::now();
        producer.try_write(1, 0, [0; 8], &msg).unwrap();
        loop {
            match consumer.try_read(&mut buf) {
                Ok(Some(_)) => break,
                Ok(None) => std::hint::spin_loop(),
                Err(e) => panic!("read error: {e}"),
            }
        }
        lat.push(t.elapsed().as_nanos() as u64);
    }
    percentile(lat, 0.99) as u64
}

fn broadcast_roundtrip(payload: usize, n_subs: usize) -> u64 {
    let dir = TempDir::new().unwrap();
    let ring = BroadcastRing::create(
        &dir.path().join("bcast_lat.ring"),
        1 << 16,
        max_msg_size_for(payload),
    )
    .unwrap();
    let mut producer = ring.producer();
    let mut subs: Vec<_> = (0..n_subs).map(|_| ring.subscribe()).collect();
    let msg = vec![0xABu8; payload];
    let mut buf = Vec::with_capacity(payload);

    // Round-trip = write one record, have every subscriber read it; time the
    // full fan-out. One record in flight at a time => no lapping.
    let mut lat = Vec::with_capacity(LAT_PAIRS);
    for _ in 0..LAT_PAIRS {
        let t = Instant::now();
        producer.write(1, 0, [0; 8], &msg).unwrap();
        for sub in subs.iter_mut() {
            loop {
                match sub.try_read(&mut buf) {
                    Ok(Some(_)) => break,
                    Ok(None) => std::hint::spin_loop(),
                    Err(e) => panic!("broadcast read error: {e}"),
                }
            }
        }
        lat.push(t.elapsed().as_nanos() as u64);
    }
    percentile(lat, 0.99) as u64
}

// ===========================================================================
// Throughput: real multi-thread saturation over a fixed wall-clock window.
// Returns msgs/sec consumed.
// ===========================================================================

fn spsc_throughput(payload: usize, capacity_bytes: u64) -> f64 {
    let dir = TempDir::new().unwrap();
    let ring = SpscRing::create(
        &dir.path().join("spsc_thr.ring"),
        capacity_bytes,
        max_msg_size_for(payload),
    )
    .unwrap();
    let (mut producer, mut consumer) = ring.into_split();
    let stop = Arc::new(AtomicBool::new(false));

    let stop_p = stop.clone();
    let prod = std::thread::spawn(move || {
        let msg = vec![0xABu8; payload];
        while !stop_p.load(Ordering::Relaxed) {
            match producer.try_write(1, 0, [0; 8], &msg) {
                Ok(()) | Err(RingError::Full) => {}
                Err(e) => panic!("write error: {e}"),
            }
        }
    });

    let stop_c = stop.clone();
    let cons = std::thread::spawn(move || {
        let mut buf = Vec::with_capacity(payload);
        let mut got = 0u64;
        loop {
            match consumer.try_read(&mut buf) {
                Ok(Some(_)) => got += 1,
                Ok(None) => {
                    if stop_c.load(Ordering::Relaxed) {
                        // drain remaining, then stop
                        match consumer.try_read(&mut buf) {
                            Ok(Some(_)) => got += 1,
                            _ => break,
                        }
                    }
                }
                Err(e) => panic!("read error: {e}"),
            }
        }
        got
    });

    std::thread::sleep(THROUGHPUT_WINDOW);
    stop.store(true, Ordering::Relaxed);
    prod.join().unwrap();
    let got = cons.join().unwrap();
    (got as f64) / THROUGHPUT_WINDOW.as_secs_f64()
}

fn mpsc_throughput(payload: usize, n_prod: usize, capacity_bytes: u64) -> f64 {
    let dir = TempDir::new().unwrap();
    let ring = MpscRing::create(
        &dir.path().join("mpsc_thr.ring"),
        capacity_bytes,
        max_msg_size_for(payload),
    )
    .unwrap();
    let (producer, mut consumer) = ring.into_split();
    let stop = Arc::new(AtomicBool::new(false));

    let prods: Vec<_> = (0..n_prod)
        .map(|_| {
            let producer = producer.clone();
            let stop_p = stop.clone();
            std::thread::spawn(move || {
                let msg = vec![0xABu8; payload];
                while !stop_p.load(Ordering::Relaxed) {
                    match producer.try_write(1, 0, [0; 8], &msg) {
                        Ok(()) | Err(RingError::Full) => {}
                        Err(e) => panic!("write error: {e}"),
                    }
                }
            })
        })
        .collect();
    drop(producer);

    let stop_c = stop.clone();
    let cons = std::thread::spawn(move || {
        let mut buf = Vec::with_capacity(payload);
        let mut got = 0u64;
        loop {
            match consumer.try_read(&mut buf) {
                Ok(Some(_)) => got += 1,
                Ok(None) => {
                    if stop_c.load(Ordering::Relaxed) {
                        match consumer.try_read(&mut buf) {
                            Ok(Some(_)) => got += 1,
                            _ => break,
                        }
                    }
                }
                Err(e) => panic!("read error: {e}"),
            }
        }
        got
    });

    std::thread::sleep(THROUGHPUT_WINDOW);
    stop.store(true, Ordering::Relaxed);
    for p in prods {
        p.join().unwrap();
    }
    let got = cons.join().unwrap();
    (got as f64) / THROUGHPUT_WINDOW.as_secs_f64()
}

fn main() {
    let total = Instant::now();
    eprintln!("shmem-microbench: WARMUP={WARMUP} MEASURED={MEASURED}");

    // SPSC latency (p50 + p99) and saturated throughput, 64B.
    let spsc_p50 = repeat_median("spsc_p50_ns", || spsc_roundtrip(64).0 as f64);
    let spsc_p99 = repeat_median("spsc_p99_ns", || spsc_roundtrip(64).1 as f64);
    let spsc_throughput_msgs =
        repeat_median("spsc_throughput_msgs", || spsc_throughput(64, 1 << 20));

    // MPSC 4-producer p99 + saturated throughput, 64B.
    let mpsc_4p_p99_ns = repeat_median("mpsc_4p_p99_ns", || mpsc_roundtrip(64) as f64);
    let mpsc_4p_throughput =
        repeat_median("mpsc_4p_throughput", || mpsc_throughput(64, 4, 1 << 20));

    // Broadcast 1-producer→4-subscriber p99, 64B.
    let broadcast_4sub_p99_ns = repeat_median("broadcast_4sub_p99_ns", || {
        broadcast_roundtrip(64, 4) as f64
    });

    // SPSC p99 with a 4096B payload.
    let large_payload_p99_ns =
        repeat_median("large_payload_p99_ns", || spsc_roundtrip(4096).1 as f64);

    // SPSC saturated throughput on a tiny 4 KiB ring (frequent wraps), 64B.
    let wrap_throughput = repeat_median("wrap_throughput", || spsc_throughput(64, 4096));

    let out = serde_json::json!({
        "spsc_p50_ns": spsc_p50,
        "spsc_p99_ns": spsc_p99,
        "spsc_throughput_msgs": spsc_throughput_msgs,
        "mpsc_4p_p99_ns": mpsc_4p_p99_ns,
        "mpsc_4p_throughput": mpsc_4p_throughput,
        "broadcast_4sub_p99_ns": broadcast_4sub_p99_ns,
        "large_payload_p99_ns": large_payload_p99_ns,
        "wrap_throughput": wrap_throughput,
    });

    eprintln!(
        "shmem-microbench: done in {:.1}s",
        total.elapsed().as_secs_f64()
    );
    // The ONE stdout line the orchestrator parses.
    println!("{out}");
}
