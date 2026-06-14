//! commit-path-load — open-loop load driver for UC's full single-node commit
//! path. Drives a rate ladder × in-flight-concurrency sweep against an
//! in-process `ClusterFixture`, recording submit→response latency in an HDR
//! histogram and writing one CSV row per ladder step.
//!
//! Runtime MUST be current_thread (memory feedback_m3_test_runtime_flavor):
//! a multi_thread runtime intermittently times out the shmem handshake.

use std::io::{Read, Write};
use std::time::{Duration, Instant};

use futures::stream::{FuturesUnordered, StreamExt};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use uc_service::{SnapshotError, StateMachine};

/// KV command: write `val` at `key`. Serializable so it rides Client::submit.
#[derive(Serialize, Deserialize)]
enum KvCmd {
    Put { key: u64, val: Vec<u8> },
}

/// In-memory KV state machine. Default-able so it works with ClusterFixture.
#[derive(Default)]
struct KvSm {
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

struct StepRow {
    config: String,
    workload: String,
    payload_bytes: usize,
    inflight: usize,
    target_rate: f64,
    achieved_rate: f64,
    hist: Histogram<u64>,
}

impl StepRow {
    fn to_csv(&self) -> String {
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

const CSV_HEADER: &str = "system,config,workload,payload_bytes,inflight,target_rate,achieved_rate,\
p50_ns,p99_ns,p99_9_ns,p99_99_ns,max_ns,count";

/// ns→µs: matches Aeron's default `outputTimeUnit=MICROSECONDS`, so both
/// systems' .hgrm files share the same value axis on the HdrHistogram plotter.
const HGRM_SCALE: f64 = 1000.0;
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
fn write_hgrm(hist: &Histogram<u64>, path: &std::path::Path, scale: f64) -> std::io::Result<()> {
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

/// Run one ladder step: open-loop at `target_rate` msgs/s, at most `inflight`
/// concurrent submits, for `duration`. Records intended-send→response latency
/// (coordinated-omission-free: latency is measured from the request's INTENDED
/// send time, not its actual dispatch time). `payload_bytes` sets the KV value
/// size. Returns the populated histogram and the achieved rate
/// (completed / wall-seconds).
/// Accumulates the actual in-flight concurrency observed during a step, so we
/// can report achieved-vs-cap (the Phase-0 Little's-law check).
#[derive(Default)]
struct ConcurrencyGauge {
    sum: u64,
    samples: u64,
    max: usize,
}

struct ConcurrencyStat {
    mean: f64,
    max: usize,
}

impl ConcurrencyGauge {
    fn sample(&mut self, inflight: usize) {
        self.sum += inflight as u64;
        self.samples += 1;
        self.max = self.max.max(inflight);
    }
    fn finish(&self) -> ConcurrencyStat {
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

async fn run_step(
    client: &uc_client::Client,
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

    let mut inflight_set = FuturesUnordered::new();
    let mut next_send = start;
    let mut seq: u64 = 0;
    let mut completed: u64 = 0;
    let val = vec![0u8; payload_bytes];
    let mut gauge = ConcurrencyGauge::default();

    loop {
        let now = Instant::now();
        if now >= deadline && inflight_set.is_empty() {
            break;
        }

        // Launch all sends whose intended time has arrived, up to the cap.
        while now >= next_send && inflight_set.len() < inflight && next_send < deadline {
            let intended = next_send;
            let cmd = KvCmd::Put {
                key: seq % 4096,
                val: val.clone(),
            };
            seq += 1;
            next_send += period;
            inflight_set.push(async move {
                let _r: u64 = client.submit(&cmd).await?;
                Ok::<_, anyhow::Error>(intended.elapsed().as_nanos() as u64)
            });
        }

        // Sample concurrency AFTER launching, so it reflects the true working
        // set we are about to await — not the post-drain count from the prior
        // iteration (which systematically under-reports by one).
        gauge.sample(inflight_set.len());

        // Drain whatever completed, or wait until the next scheduled send / a
        // completion, whichever is first.
        tokio::select! {
            Some(res) = inflight_set.next(), if !inflight_set.is_empty() => {
                hist.record(res?.min(600_000_000_000))?;
                completed += 1;
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(next_send)),
                if now < deadline && inflight_set.len() < inflight => {}
            else => { break; }
        }
    }

    let achieved = completed as f64 / start.elapsed().as_secs_f64();
    Ok((hist, achieved, gauge.finish()))
}

use clap::Parser;
use uc_node::test_support::ClusterFixture;

#[derive(Parser)]
#[command(about = "Open-loop commit-path load driver for UC (single-node)")]
struct Args {
    /// config label written into the CSV (e.g. single_tmpfs, single_disk)
    #[arg(long, default_value = "single_disk")]
    config: String,
    /// comma-separated target rates (msgs/s) — the rate ladder
    #[arg(long, default_value = "100,500,1000,2000,5000,10000,20000")]
    rates: String,
    /// in-flight concurrency values to sweep
    #[arg(long, default_value = "1,8,32,128")]
    inflight: String,
    /// KV value size in bytes
    #[arg(long, default_value_t = 64)]
    payload_bytes: usize,
    /// measurement window per step (seconds)
    #[arg(long, default_value_t = 5.0)]
    window_secs: f64,
    /// warmup window per step (seconds)
    #[arg(long, default_value_t = 2.0)]
    warmup_secs: f64,
    /// output CSV path
    #[arg(long, default_value = "bench-out/uc.csv")]
    out: String,
    /// directory for per-step .hgrm files (HdrHistogram text, µs-scaled, format-
    /// identical to Aeron's LoadTestRig output, for one-chart overlay). Omit to skip.
    #[arg(long)]
    hgrm_dir: Option<std::path::PathBuf>,
    /// Attach to a running cluster at this instance dir instead of spawning the
    /// in-process single-node fixture. For multi-process N-node (Phase 2) runs.
    #[arg(long)]
    connect: Option<std::path::PathBuf>,
    /// app_id of the running cluster (used with --connect)
    #[arg(long, default_value = "uc-bench-3node")]
    app_id: String,
}

fn parse_list<T: std::str::FromStr>(s: &str) -> Vec<T>
where
    T::Err: std::fmt::Debug,
{
    s.split(',').map(|x| x.trim().parse().unwrap()).collect()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();
    let args = Args::parse();
    let rates: Vec<f64> = parse_list(&args.rates);
    let inflights: Vec<usize> = parse_list(&args.inflight);
    let window = Duration::from_secs_f64(args.window_secs);
    let warmup = Duration::from_secs_f64(args.warmup_secs);

    if let Some(parent) = std::path::Path::new(&args.out).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut csv = std::fs::File::create(&args.out)?;
    writeln!(csv, "{CSV_HEADER}")?;

    if let Some(dir) = &args.hgrm_dir {
        std::fs::create_dir_all(dir)?;
    }

    eprintln!(
        "commit-path-load: config={} rates={:?} inflight={:?} payload={}B",
        args.config, rates, inflights, args.payload_bytes
    );

    // One client is enough for the open-loop driver (submit takes &self and we
    // keep many requests in flight). Either attach to a running cluster
    // (--connect, Phase 2 multi-process) or spawn the in-process single-node
    // fixture (Phase 1). Both bindings must outlive the run, so declare them
    // before the branch.
    let fixture: Option<ClusterFixture<KvSm>>;
    let owned_client;
    let client: &uc_client::Client = if let Some(dir) = &args.connect {
        eprintln!(
            "commit-path-load: attaching to running cluster at {} (app_id={})",
            dir.display(),
            args.app_id
        );
        owned_client = uc_client::Client::connect(dir, &args.app_id).await?;
        fixture = None;
        &owned_client
    } else {
        fixture = Some(ClusterFixture::<KvSm>::single_node(1).await?);
        fixture.as_ref().unwrap().client(0)
    };

    for &inflight in &inflights {
        for &rate in &rates {
            // Warmup (discarded).
            let _ = run_step(client, rate, inflight, warmup, args.payload_bytes).await?;
            // Measured.
            let (hist, achieved, conc) =
                run_step(client, rate, inflight, window, args.payload_bytes).await?;
            eprintln!(
                "  [gauge] inflight cap={inflight} actual mean={:.1} max={}",
                conc.mean, conc.max
            );
            let row = StepRow {
                config: args.config.clone(),
                workload: "kv".into(),
                payload_bytes: args.payload_bytes,
                inflight,
                target_rate: rate,
                achieved_rate: achieved,
                hist,
            };
            let line = row.to_csv();
            writeln!(csv, "{line}")?;
            csv.flush()?;
            eprintln!("  {line}");

            if let Some(dir) = &args.hgrm_dir {
                let path = dir.join(format!(
                    "{}_{}_r{}_if{}.hgrm",
                    args.config, row.workload, rate as u64, inflight
                ));
                write_hgrm(&row.hist, &path, HGRM_SCALE)?;
                eprintln!("  wrote {}", path.display());
            }
        }
    }

    // Ordered teardown only for the in-process fixture; a --connect client is
    // dropped (the external cluster keeps running).
    if let Some(fixture) = fixture {
        fixture.shutdown().await?;
    }

    eprintln!("commit-path-load: wrote {}", args.out);
    Ok(())
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
