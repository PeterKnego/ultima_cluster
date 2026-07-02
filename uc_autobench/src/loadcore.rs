//! loadcore — shared open-loop KV load-driver core for the fleet bench bins.
//!
//! `commit-path-load` (shmem `uc_client` path) and `uc-node-launch
//! --ipc-mode embedded` (in-process `NodeHandle` path) drive the SAME rate
//! ladder × in-flight sweep through the [`Submitter`] seam, so the two arms
//! of the embedded-vs-shmem A/B differ only in the submit path — never in
//! the workload, pacing, or measurement. `KvCmd`/`KvSm` live here for the
//! same reason: one definition, one wire shape, no drift between bins.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use uc_service::{SnapshotError, StateMachine};

/// KV command: write `val` at `key`. Serializable so it rides both
/// `Client::submit` and `NodeHandle::submit` with one wire shape.
#[derive(Serialize, Deserialize)]
pub enum KvCmd {
    Put { key: u64, val: Vec<u8> },
}

/// In-memory KV state machine. Default-able so it works with ClusterFixture
/// and NodeBuilder in either IPC mode.
#[derive(Default)]
pub struct KvSm {
    map: std::collections::HashMap<u64, Vec<u8>>,
    last_applied: Option<u64>,
}

impl StateMachine for KvSm {
    type Command = KvCmd;
    type Response = u64; // returns current map.len()
    type Query = u64; // key to read
    type QueryResponse = Option<Vec<u8>>;
    type SnapshotHandle = Vec<u8>;

    fn apply(&mut self, log_index: u64, cmd: KvCmd) -> u64 {
        match cmd {
            KvCmd::Put { key, val } => {
                self.map.insert(key, val);
            }
        }
        self.last_applied = Some(log_index);
        self.map.len() as u64
    }

    fn query(&self, key: u64) -> Option<Vec<u8>> {
        self.map.get(&key).cloned()
    }

    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }

    fn freeze(&self) -> Result<(Vec<u8>, u64), SnapshotError> {
        Ok((Vec::new(), self.last_applied.unwrap_or(0)))
    }

    fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn Write) -> Result<(), SnapshotError> {
        dst.write_all(&handle)?;
        Ok(())
    }

    fn install_snapshot(&mut self, _src: &mut dyn Read) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }
}

/// The submit-path seam between the two bench arms. `Err` = dropped request
/// (timeout / overload) — counted, never recorded in the histogram.
///
/// Boxed future (not RPITIT) so `run_step` can `tokio::spawn` it without
/// threading `Send` bounds through the trait; one small alloc per request is
/// noise against a ms-scale commit and identical in both arms.
pub trait Submitter: Clone + Send + Sync + 'static {
    fn submit(&self, cmd: KvCmd) -> BoxFuture<'static, anyhow::Result<()>>;
}

/// Shmem arm: submits through `uc_client` over cnc.dat + client rings.
#[derive(Clone)]
pub struct ClientSubmitter(pub Arc<uc_client::Client>);

impl Submitter for ClientSubmitter {
    fn submit(&self, cmd: KvCmd) -> BoxFuture<'static, anyhow::Result<()>> {
        let client = Arc::clone(&self.0);
        Box::pin(async move {
            client
                .submit::<_, u64>(&cmd)
                .await
                .map(|_| ())
                .map_err(|e| anyhow::anyhow!("{e}"))
        })
    }
}

/// Embedded arm: submits in-process through `NodeHandle::submit` (no shmem
/// surface exists in embedded mode). `timeout` mirrors uc_client's per-request
/// timeout so "dropped" means the same thing in both arms.
#[derive(Clone)]
pub struct NodeSubmitter {
    pub node: Arc<uc_node::NodeHandle<KvSm>>,
    pub timeout: Duration,
}

impl Submitter for NodeSubmitter {
    fn submit(&self, cmd: KvCmd) -> BoxFuture<'static, anyhow::Result<()>> {
        let node = Arc::clone(&self.node);
        let timeout = self.timeout;
        Box::pin(async move {
            match tokio::time::timeout(timeout, node.submit(cmd)).await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => Err(anyhow::anyhow!("{e}")),
                Err(_) => Err(anyhow::anyhow!("request timeout")),
            }
        })
    }
}

/// Per-request timeout for the embedded arm: same env knob + default as
/// uc_client's `request_timeout()` so the bench-infra
/// `uc_client_request_timeout_ms` var means the same thing in both arms.
pub fn request_timeout_from_env() -> Duration {
    std::env::var("UC_CLIENT_REQUEST_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(10))
}

pub struct StepRow {
    pub config: String,
    pub workload: String,
    pub payload_bytes: usize,
    pub inflight: usize,
    pub target_rate: f64,
    pub achieved_rate: f64,
    pub hist: Histogram<u64>,
}

impl StepRow {
    pub fn to_csv(&self) -> String {
        format!(
            "uc,{},{},{},{},{:.0},{:.1},{},{},{},{},{},{}",
            self.config,
            self.workload,
            self.payload_bytes,
            self.inflight,
            self.target_rate,
            self.achieved_rate,
            self.hist.value_at_quantile(0.50),
            self.hist.value_at_quantile(0.99),
            self.hist.value_at_quantile(0.999),
            self.hist.value_at_quantile(0.9999),
            self.hist.max(),
            self.hist.len(),
        )
    }
}

pub const CSV_HEADER: &str = "system,config,workload,payload_bytes,inflight,target_rate,achieved_rate,\
p50_ns,p99_ns,p99_9_ns,p99_99_ns,max_ns,count";

/// ns→µs: matches Aeron's default `outputTimeUnit=MICROSECONDS`, so both
/// systems' .hgrm files share the same value axis on the HdrHistogram plotter.
pub const HGRM_SCALE: f64 = 1000.0;
/// HdrHistogram default `percentileTicksPerHalfDistance` (Aeron uses the default).
const HGRM_TICKS: u32 = 5;

/// Write `hist` as an HdrHistogram percentile-distribution text file (.hgrm),
/// byte-compatible with Java HdrHistogram's
/// `outputPercentileDistribution(out, ticksPerHalfDistance=5, scalingRatio, csv=false)`
/// — the exact format Aeron's `LoadTestRig` prints. Values are divided by
/// `scale` (1000.0 = ns→µs). Drop the resulting files (both systems') onto
/// <https://hdrhistogram.github.io/HdrHistogram/plotFiles.html> to overlay them.
///
/// Note: the 3-decimal value precision below mirrors the 3 significant figures
/// the histogram is created with (`new_with_bounds(.., 3)`); keep them in sync.
pub fn write_hgrm(hist: &Histogram<u64>, path: &Path, scale: f64) -> std::io::Result<()> {
    use std::io::Write as _;
    let mut f = std::io::BufWriter::new(std::fs::File::create(path)?);

    // Header (Java prints "...\n\n" — a header row then a blank line).
    writeln!(
        f,
        "{:>12} {:>14} {:>10} {:>14}",
        "Value", "Percentile", "TotalCount", "1/(1-Percentile)"
    )?;
    writeln!(f)?;

    // One row per percentile tick. `count_since_last_iteration` summed in visit
    // order == Java's getTotalCountToThisValue (the iterator walks values up).
    let mut total: u64 = 0;
    for v in hist.iter_quantiles(HGRM_TICKS) {
        total += v.count_since_last_iteration();
        let value = v.value_iterated_to() as f64 / scale;
        let q = v.quantile_iterated_to();
        if q < 1.0 {
            // Java: "%12.3f %2.12f %10d %14.2f"
            writeln!(f, "{:12.3} {:2.12} {:10} {:14.2}", value, q, total, 1.0 / (1.0 - q))?;
        } else {
            // 100th percentile: 1/(1-p) is infinite, so the column is dropped.
            // Java: "%12.3f %2.12f %10d"
            writeln!(f, "{:12.3} {:2.12} {:10}", value, q, total)?;
        }
    }

    // Footer comment lines (parsed for Mean/Max by the plotter; '#'-prefixed).
    writeln!(
        f,
        "#[Mean    = {:12.3}, StdDeviation   = {:12.3}]",
        hist.mean() / scale,
        hist.stdev() / scale
    )?;
    writeln!(
        f,
        "#[Max     = {:12.3}, Total count    = {:12}]",
        hist.max() as f64 / scale,
        hist.len()
    )?;
    f.flush()
}

/// Accumulates the actual in-flight concurrency observed during a step, so we
/// can report achieved-vs-cap (the Phase-0 Little's-law check).
#[derive(Default)]
pub struct ConcurrencyGauge {
    sum: u64,
    samples: u64,
    max: usize,
}

pub struct ConcurrencyStat {
    pub mean: f64,
    pub max: usize,
}

impl ConcurrencyGauge {
    pub fn sample(&mut self, inflight: usize) {
        self.sum += inflight as u64;
        self.samples += 1;
        self.max = self.max.max(inflight);
    }
    pub fn finish(&self) -> ConcurrencyStat {
        ConcurrencyStat {
            mean: if self.samples == 0 {
                0.0
            } else {
                self.sum as f64 / self.samples as f64
            },
            max: self.max,
        }
    }
}

/// Run one ladder step: open-loop at `target_rate` msgs/s, at most `inflight`
/// concurrent submits, for `duration`. Records intended-send→response latency
/// (coordinated-omission-free: latency is measured from the request's INTENDED
/// send time, not its actual dispatch time). `payload_bytes` sets the KV value
/// size. Returns the populated histogram and the achieved rate
/// (completed / wall-seconds).
pub async fn run_step<Sub: Submitter>(
    sub: Sub,
    target_rate: f64,
    inflight: usize,
    duration: Duration,
    payload_bytes: usize,
) -> anyhow::Result<(Histogram<u64>, f64, ConcurrencyStat)> {
    // 1ns..600s range, 3 sig figs (matches Aeron-side hdr_init precision).
    let mut hist = Histogram::<u64>::new_with_bounds(1, 600_000_000_000, 3)?;
    let period = Duration::from_secs_f64(1.0 / target_rate);
    let start = Instant::now();
    let deadline = start + duration;

    // Concurrency cap: `inflight` permits. Each request is a spawned task (so the
    // multi-threaded runtime spreads them across cores); acquiring a permit blocks
    // (backpressure) when the cluster can't keep up, turning the open loop into a
    // closed loop at saturation. Results (Some(latency_ns) | None=dropped) return
    // via the channel so a straggler can't abort the step.
    let sem = Arc::new(tokio::sync::Semaphore::new(inflight));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Option<u64>>();
    let val = Arc::new(vec![0u8; payload_bytes]);
    let mut next_send = start;
    let mut seq: u64 = 0;
    let mut completed: u64 = 0;
    let mut dropped: u64 = 0;
    let mut gauge = ConcurrencyGauge::default();

    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        // Pace to the open-loop schedule.
        if now < next_send {
            tokio::time::sleep_until(tokio::time::Instant::from_std(next_send)).await;
        }
        // Acquire a permit (caps concurrency at `inflight`; blocks under saturation).
        let permit = Arc::clone(&sem)
            .acquire_owned()
            .await
            .expect("semaphore not closed");
        gauge.sample(inflight - sem.available_permits());
        let intended = next_send;
        next_send += period;
        let sq = seq;
        seq += 1;
        let sub = sub.clone();
        let val = Arc::clone(&val);
        let tx = tx.clone();
        tokio::spawn(async move {
            let cmd = KvCmd::Put {
                key: sq % 4096,
                val: (*val).clone(),
            };
            let outcome = match sub.submit(cmd).await {
                Ok(()) => Some(intended.elapsed().as_nanos() as u64),
                Err(_) => None, // straggler timeout/error — counted as dropped
            };
            let _ = tx.send(outcome);
            drop(permit);
        });

        // Non-blocking drain of any completed results to keep counters current.
        while let Ok(outcome) = rx.try_recv() {
            match outcome {
                Some(l) => {
                    hist.record(l.min(600_000_000_000))?;
                    completed += 1;
                }
                None => dropped += 1,
            }
        }
    }

    // Stop spawning; drop our sender so `rx` closes once all in-flight tasks finish,
    // then drain the tail.
    drop(tx);
    while let Some(outcome) = rx.recv().await {
        match outcome {
            Some(l) => {
                hist.record(l.min(600_000_000_000))?;
                completed += 1;
            }
            None => dropped += 1,
        }
    }

    let achieved = completed as f64 / start.elapsed().as_secs_f64();
    if dropped > 0 {
        eprintln!(
            "  [overload] target={:.0} inflight={} completed={} DROPPED={} ({:.1}% — raise UC_CLIENT_REQUEST_TIMEOUT_MS)",
            target_rate,
            inflight,
            completed,
            dropped,
            100.0 * dropped as f64 / (completed + dropped).max(1) as f64,
        );
    }
    Ok((hist, achieved, gauge.finish()))
}

pub struct SweepOpts {
    /// CSV config label (e.g. `3node_consistent_embedded`).
    pub config: String,
    pub rates: Vec<f64>,
    pub inflights: Vec<usize>,
    pub payload_bytes: usize,
    pub window_secs: f64,
    pub warmup_secs: f64,
    /// Output CSV path. Written incrementally (one flushed row per step).
    pub out: PathBuf,
    /// Directory for per-step .hgrm files. `None` to skip.
    pub hgrm_dir: Option<PathBuf>,
}

/// The full inflight × rate ladder: warmup (discarded) + measured window per
/// step, one CSV row per step, flushed as it goes.
pub async fn run_sweep<Sub: Submitter>(sub: Sub, opts: &SweepOpts) -> anyhow::Result<()> {
    let window = Duration::from_secs_f64(opts.window_secs);
    let warmup = Duration::from_secs_f64(opts.warmup_secs);

    if let Some(parent) = opts.out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut csv = std::fs::File::create(&opts.out)?;
    writeln!(csv, "{CSV_HEADER}")?;

    if let Some(dir) = &opts.hgrm_dir {
        std::fs::create_dir_all(dir)?;
    }

    eprintln!(
        "loadcore sweep: config={} rates={:?} inflight={:?} payload={}B",
        opts.config, opts.rates, opts.inflights, opts.payload_bytes
    );

    for &inflight in &opts.inflights {
        for &rate in &opts.rates {
            // Warmup (discarded).
            let _ = run_step(sub.clone(), rate, inflight, warmup, opts.payload_bytes).await?;
            // Measured.
            let (hist, achieved, conc) =
                run_step(sub.clone(), rate, inflight, window, opts.payload_bytes).await?;
            eprintln!(
                "  [gauge] inflight cap={inflight} actual mean={:.1} max={}",
                conc.mean, conc.max
            );
            let row = StepRow {
                config: opts.config.clone(),
                workload: "kv".into(),
                payload_bytes: opts.payload_bytes,
                inflight,
                target_rate: rate,
                achieved_rate: achieved,
                hist,
            };
            let line = row.to_csv();
            writeln!(csv, "{line}")?;
            csv.flush()?;
            eprintln!("  {line}");

            if let Some(dir) = &opts.hgrm_dir {
                let path = dir.join(format!(
                    "{}_{}_r{}_if{}.hgrm",
                    opts.config, row.workload, rate as u64, inflight
                ));
                write_hgrm(&row.hist, &path, HGRM_SCALE)?;
                eprintln!("  wrote {}", path.display());
            }
        }
    }

    Ok(())
}

pub fn parse_list<T: std::str::FromStr>(s: &str) -> Vec<T>
where
    T::Err: std::fmt::Debug,
{
    s.split(',').map(|x| x.trim().parse().unwrap()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrency_gauge_tracks_mean_and_max() {
        let mut g = ConcurrencyGauge::default();
        g.sample(0);
        g.sample(4);
        g.sample(8);
        g.sample(4);
        let s = g.finish();
        assert_eq!(s.max, 8);
        assert!((s.mean - 4.0).abs() < 1e-9); // (0+4+8+4)/4
    }

    #[test]
    fn empty_gauge_is_zero() {
        let s = ConcurrencyGauge::default().finish();
        assert_eq!(s.max, 0);
        assert_eq!(s.mean, 0.0);
    }

    #[test]
    fn hgrm_matches_hdrhistogram_text_format() {
        let mut h = Histogram::<u64>::new_with_bounds(1, 600_000_000_000, 3).unwrap();
        // Record in ns; exporter scales ÷1000 to µs.
        for &v in &[1_000u64, 2_000, 3_000, 50_000, 1_000_000] {
            h.record(v).unwrap();
        }
        let path = std::env::temp_dir().join(format!("uc_hgrm_{}.hgrm", std::process::id()));
        write_hgrm(&h, &path, HGRM_SCALE).unwrap();
        let s = std::fs::read_to_string(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        // Header columns (HdrHistogram / Aeron text format).
        assert!(s.contains("Value"));
        assert!(s.contains("Percentile"));
        assert!(s.contains("TotalCount"));
        assert!(s.contains("1/(1-Percentile)"));
        // Footer comment lines.
        assert!(s.contains("#[Mean"));
        assert!(s.contains("#[Max"));
        // 1000 ns scaled to µs -> "1.000" appears as the smallest value.
        assert!(s.contains("1.000"));
        // Final row reaches the 100th percentile (quantile 1.0).
        assert!(s.contains("1.000000000000"));
        // Total count reaches the number of recorded samples.
        assert!(s.lines().any(|l| l.split_whitespace().nth(2) == Some("5")));
    }

    #[test]
    fn kv_apply_inserts_and_counts() {
        let mut sm = KvSm::default();
        let n = sm.apply(
            1,
            KvCmd::Put {
                key: 7,
                val: vec![1, 2, 3],
            },
        );
        assert_eq!(n, 1);
        assert_eq!(sm.query(7), Some(vec![1, 2, 3]));
        assert_eq!(sm.last_applied(), Some(1));
    }

    #[test]
    fn csv_row_has_13_columns() {
        let mut hist = Histogram::<u64>::new(3).unwrap();
        for v in [100u64, 200, 300, 400, 500] {
            hist.record(v).unwrap();
        }
        let row = StepRow {
            config: "single_disk".into(),
            workload: "kv".into(),
            payload_bytes: 64,
            inflight: 8,
            target_rate: 1000.0,
            achieved_rate: 987.6,
            hist,
        };
        let csv = row.to_csv();
        assert_eq!(csv.split(',').count(), 13);
        assert!(csv.starts_with("uc,single_disk,kv,64,8,1000,987.6,"));
        assert_eq!(CSV_HEADER.split(',').count(), 13);
    }
}
