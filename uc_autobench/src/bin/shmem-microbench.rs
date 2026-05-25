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
//! immediately read it back (see batched timing below). This isolates the
//! ring's own enqueue/dequeue cost (header write, atomic length publish, atomic
//! length read, payload copy) which is exactly what a ring rewrite changes — so
//! relative improvements are faithfully reflected.
//!
//! For MPSC the round-trip uses one producer clone (the consumer still sees the
//! same per-record cost); for Broadcast the producer writes and all 4
//! subscribers read each record, timing the full fan-out pair. The 4-producer /
//! 4-subscriber THROUGHPUT figures use real multi-thread saturation.
//!
//! ## Sub-tick resolution via batched-sample percentiles
//!
//! The host monotonic clock resolution is ~42ns, so timing a single sub-100ns
//! round-trip quantizes to one tick and `p50 == p99` (σ ≈ 0). That destroys the
//! optimization loop's ability to rank ring variants — `spsc_p99_ns` is its
//! PRIMARY (minimize) metric and it promotes only when a candidate beats the
//! best by > ~2σ.
//!
//! Fix: for each LATENCY sub-bench we collect `SAMPLES` data points; each sample
//! times a BATCH of `BATCH` round-trips with ONE `Instant::now()` before/after
//! and records the per-op latency as `elapsed_ns / BATCH`. With BATCH=1000 the
//! ~42ns tick is amortized 1000× → ~0.042ns resolution, and the SAMPLES
//! fractional data points still carry sample-to-sample tail variation (σ > 0).
//! We then report the p50/p99 percentile over those fractional per-op means.
//! Metric NAMES are unchanged so `tasks/shmem/task.toml` still matches; values
//! are now smooth, sub-tick, and `p50 != p99` in general.
//!
//! Warmup: 3 short batched passes before the measured pass. The SAMPLES-point
//! percentile is itself stable, so no extra outer 9× median loop is needed for
//! the latency metrics. Throughput metrics keep the warmup + median structure.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tempfile::TempDir;
use uc_protocol::ring::{BroadcastRing, MpscRing, RingError, SpscConsumer, SpscProducer, SpscRing};

const WARMUP: usize = 3;
const MEASURED: usize = 9;

// on-wire framing: FrameHeader(16) + payload + crc(4)
const FRAME_OVERHEAD: usize = 20;

const THROUGHPUT_WINDOW: Duration = Duration::from_millis(250);

/// Latency sampling: each SAMPLE times a BATCH of round-trips with one clock
/// read and records `elapsed_ns / BATCH` (a fractional per-op latency). The
/// BATCH amortizes the ~42ns host clock tick to sub-nanosecond resolution; the
/// SAMPLES fractional points still carry sample-to-sample tail variation so
/// p50/p99 are smooth and σ > 0. SAMPLES * BATCH round-trips per sub-bench;
/// with 4 latency sub-benches + 3 warmups each this stays well under budget.
const LAT_BATCH: usize = 1000;
const LAT_SAMPLES: usize = 600;

fn max_msg_size_for(payload: usize) -> u32 {
    (payload + FRAME_OVERHEAD) as u32
}

fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(f64::total_cmp);
    xs[xs.len() / 2]
}

/// Percentile over fractional per-batch-mean latency samples (ns). Uses
/// `f64::total_cmp` (clippy-clean; no `partial_cmp().unwrap()`).
fn percentile_f64(mut xs: Vec<f64>, p: f64) -> f64 {
    xs.sort_by(f64::total_cmp);
    let idx = (((xs.len() - 1) as f64) * p).round() as usize;
    xs[idx]
}

/// Run a throughput sub-bench `f` for WARMUP + MEASURED runs, returning the
/// median of the measured runs. `f` returns one f64 per run.
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

/// Run a LATENCY sub-bench: `sample` collects one batch-mean per-op latency (ns)
/// per call. Do WARMUP warmup passes (each = WARMUP_BATCHES batches) then a
/// measured pass of LAT_SAMPLES batches; report (p50, p99) over those fractional
/// batch means. Smooth, sub-tick, σ > 0.
fn repeat_latency(label: &str, mut sample: impl FnMut() -> f64) -> (f64, f64) {
    // Warmup: a handful of batches per warmup run is enough to prime caches /
    // branch predictor / TLB; no need to pay a full SAMPLES pass each time.
    const WARMUP_BATCHES: usize = 32;
    for _ in 0..WARMUP {
        for _ in 0..WARMUP_BATCHES {
            sample();
        }
    }
    let mut means = Vec::with_capacity(LAT_SAMPLES);
    for _ in 0..LAT_SAMPLES {
        means.push(sample());
    }
    let p50 = percentile_f64(means.clone(), 0.50);
    let p99 = percentile_f64(means, 0.99);
    eprintln!("  {label}: p50={p50:.3} p99={p99:.3}");
    (p50, p99)
}

// ===========================================================================
// Latency: single-thread round-trip (write one, read it back). Each *_sample
// fn times a BATCH of LAT_BATCH round-trips with one clock read and returns the
// per-op batch-mean latency in ns (fractional). Collected LAT_SAMPLES times.
// ===========================================================================

fn spsc_lat_sample(
    producer: &mut SpscProducer,
    consumer: &mut SpscConsumer,
    msg: &[u8],
    buf: &mut Vec<u8>,
) -> f64 {
    let t = Instant::now();
    for _ in 0..LAT_BATCH {
        producer.try_write(1, 0, [0; 8], msg).unwrap();
        loop {
            match consumer.try_read(buf) {
                Ok(Some(_)) => break,
                Ok(None) => std::hint::spin_loop(),
                Err(e) => panic!("read error: {e}"),
            }
        }
    }
    t.elapsed().as_nanos() as f64 / LAT_BATCH as f64
}

/// (p50_ns, p99_ns) over LAT_SAMPLES batch means for an SPSC round-trip.
fn spsc_roundtrip(label: &str, payload: usize) -> (f64, f64) {
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
    repeat_latency(label, || {
        spsc_lat_sample(&mut producer, &mut consumer, &msg, &mut buf)
    })
}

/// p99_ns over LAT_SAMPLES batch means for an MPSC (single producer clone) round-trip.
fn mpsc_roundtrip(label: &str, payload: usize) -> f64 {
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
    let (_p50, p99) = repeat_latency(label, || {
        let t = Instant::now();
        for _ in 0..LAT_BATCH {
            producer.try_write(1, 0, [0; 8], &msg).unwrap();
            loop {
                match consumer.try_read(&mut buf) {
                    Ok(Some(_)) => break,
                    Ok(None) => std::hint::spin_loop(),
                    Err(e) => panic!("read error: {e}"),
                }
            }
        }
        t.elapsed().as_nanos() as f64 / LAT_BATCH as f64
    });
    p99
}

/// p99_ns over LAT_SAMPLES batch means for a 1-producer→n_subs broadcast fan-out.
fn broadcast_roundtrip(label: &str, payload: usize, n_subs: usize) -> f64 {
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
    let (_p50, p99) = repeat_latency(label, || {
        let t = Instant::now();
        for _ in 0..LAT_BATCH {
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
        }
        t.elapsed().as_nanos() as f64 / LAT_BATCH as f64
    });
    p99
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

    // SPSC latency (p50 + p99 from the same batched-sample pass) and saturated
    // throughput, 64B.
    let (spsc_p50, spsc_p99) = spsc_roundtrip("spsc_lat", 64);
    let spsc_throughput_msgs =
        repeat_median("spsc_throughput_msgs", || spsc_throughput(64, 1 << 20));

    // MPSC 4-producer p99 + saturated throughput, 64B.
    let mpsc_4p_p99_ns = mpsc_roundtrip("mpsc_4p_p99_ns", 64);
    let mpsc_4p_throughput =
        repeat_median("mpsc_4p_throughput", || mpsc_throughput(64, 4, 1 << 20));

    // Broadcast 1-producer→4-subscriber p99, 64B.
    let broadcast_4sub_p99_ns = broadcast_roundtrip("broadcast_4sub_p99_ns", 64, 4);

    // SPSC p99 with a 4096B payload.
    let (_lp_p50, large_payload_p99_ns) = spsc_roundtrip("large_payload_p99_ns", 4096);

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
