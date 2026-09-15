//! B3's paired register arm (and the same driver against the KV): a
//! pipelined Put load through `uc_remote::RemoteClient` with the
//! `m12_gate` steady window — completions inside the warm-up are
//! discarded, the rate and latency percentiles come from the measure
//! window only. This is the BLOCKING client with a window of tickets in
//! flight, not the engine halves M13's bars measured; the number is a
//! paired comparison between two services through one driver, never a
//! platform figure.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use hdrhistogram::Histogram;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use uc_remote::{RemoteClient, RemoteConfig, Ticket};

use crate::adapter::{Adapter, KvOp};

#[derive(Clone, Debug)]
pub struct RateCfg {
    pub workers: u32,
    pub window: u32,
    pub warmup: Duration,
    pub measure: Duration,
    pub keys: u32,
    pub seed: u64,
}

pub struct RateReport {
    pub measured_ops: u64,
    pub measure_secs: f64,
    pub ops_per_s: f64,
    pub p50_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
    pub errors: u64,
    pub warmup_ops: u64,
}

pub fn run(
    adapter: Arc<dyn Adapter>,
    gateways: Vec<String>,
    app_id: String,
    cfg: &RateCfg,
) -> Result<RateReport> {
    let start = Instant::now();
    let measure_from = start + cfg.warmup;
    let measure_to = measure_from + cfg.measure;
    let stop = Arc::new(AtomicBool::new(false));
    let hist = Arc::new(Mutex::new(Histogram::<u64>::new_with_bounds(
        1, 60_000_000, 3,
    )?));
    let totals = Arc::new(Mutex::new((0u64, 0u64, 0u64))); // measured, warmup, errors
    let caps = adapter.caps();
    let keys: Vec<Vec<u8>> = (0..cfg.keys.max(1))
        .map(|i| format!("rate:{}:{i}", cfg.seed).into_bytes())
        .collect();
    let mut handles = Vec::new();
    for id in 0..cfg.workers {
        let adapter = adapter.clone();
        let gateways = gateways.clone();
        let app_id = app_id.clone();
        let stop = stop.clone();
        let hist = hist.clone();
        let totals = totals.clone();
        let keys = keys.clone();
        let window = cfg.window as usize;
        let seed = cfg.seed;
        handles.push(std::thread::spawn(move || -> Result<()> {
            let c = RemoteClient::connect(RemoteConfig {
                app_id,
                members: gateways,
                client_id: Some(rand::random::<u64>() | 1),
                max_inflight: cfg_window(window),
                request_timeout: Duration::from_secs(30),
                ..Default::default()
            })?;
            let mut rng = StdRng::seed_from_u64(seed ^ ((id as u64 + 1) << 32));
            let mut inflight: VecDeque<(Instant, Ticket)> = VecDeque::new();
            let mut local = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;
            let mut counter = 0u64;
            // (measured, warm, errors) tallies, threaded explicitly so the
            // submit loop below can also count errors.
            let mut tally = (0u64, 0u64, 0u64);
            let drain = |inflight: &mut VecDeque<(Instant, Ticket)>,
                         local: &mut Histogram<u64>,
                         tally: &mut (u64, u64, u64)| {
                if let Some((t0, t)) = inflight.pop_front() {
                    let ok = t.wait().is_ok();
                    let now = Instant::now();
                    if !ok {
                        tally.2 += 1;
                    } else if t0 >= measure_from && t0 < measure_to {
                        tally.0 += 1;
                        let _ = local.record((now - t0).as_micros().max(1) as u64);
                    } else {
                        tally.1 += 1;
                    }
                }
            };
            while !stop.load(Ordering::Relaxed) {
                let k = if caps.keys {
                    rng.random_range(0..keys.len())
                } else {
                    0
                };
                counter += 1;
                let cmd = adapter.encode(&keys[k], &KvOp::Put(((id as u64) << 40) | counter), 0);
                while inflight.len() >= window.saturating_sub(1).max(1) {
                    drain(&mut inflight, &mut local, &mut tally);
                }
                match c.submit(&cmd) {
                    Ok(t) => inflight.push_back((Instant::now(), t)),
                    Err(_) => {
                        tally.2 += 1;
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
            }
            while !inflight.is_empty() {
                drain(&mut inflight, &mut local, &mut tally);
            }
            c.shutdown();
            hist.lock().unwrap().add(&local)?;
            let mut t = totals.lock().unwrap();
            t.0 += tally.0;
            t.1 += tally.1;
            t.2 += tally.2;
            Ok(())
        }));
    }
    std::thread::sleep(cfg.warmup + cfg.measure);
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join()
            .map_err(|_| anyhow::anyhow!("rate worker panicked"))??;
    }
    let h = hist.lock().unwrap();
    let (measured, warm, errors) = *totals.lock().unwrap();
    let secs = cfg.measure.as_secs_f64();
    Ok(RateReport {
        measured_ops: measured,
        measure_secs: secs,
        ops_per_s: measured as f64 / secs,
        p50_us: h.value_at_quantile(0.5),
        p99_us: h.value_at_quantile(0.99),
        max_us: h.max(),
        errors,
        warmup_ops: warm,
    })
}

fn cfg_window(window: usize) -> u32 {
    (window as u32).max(1)
}
