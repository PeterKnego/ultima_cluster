//! Full-path latency attribution: drives the in-process fixture under bounded
//! concurrency, drains the probe sink, and writes per-stage percentiles to
//! attribution.csv. Build with `--features uc-bench-probes`.
//!
//! Storage axis: this binary does NOT relocate the journal itself. Control
//! tmpfs vs disk by exporting TMPDIR before running (the fixture's TempDir
//! honors it), and pass the matching --config label, e.g.:
//!   TMPDIR=/dev/shm cargo run -p uc_autobench --features uc-bench-probes \
//!     --bin attribution-bench --release -- --config single_tmpfs --inflight 8

use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;

use clap::Parser;
use futures::stream::{self, StreamExt};
use hdrhistogram::Histogram;
use uc_node::test_support::ClusterFixture;
use uc_service::{SnapshotError, StateMachine};

#[derive(Parser, Debug)]
#[command(name = "attribution-bench")]
struct Args {
    /// CSV `config` label (e.g. single_tmpfs, single_disk).
    #[arg(long, default_value = "single_tmpfs")]
    config: String,
    /// Concurrency depth (in-flight submits).
    #[arg(long, default_value_t = 8)]
    inflight: usize,
    /// Total requests to issue.
    #[arg(long, default_value_t = 5000)]
    count: usize,
    /// Payload size in bytes.
    #[arg(long, default_value_t = 64)]
    payload_bytes: usize,
    /// Output CSV path.
    #[arg(long, default_value = "bench-out/attribution.csv")]
    out: PathBuf,
    /// Warmup requests (not measured).
    #[arg(long, default_value_t = 500)]
    warmup: usize,
}

#[derive(Default)]
struct Echo {
    counter: u64,
    last_applied: Option<u64>,
}

impl StateMachine for Echo {
    type Command = Vec<u8>;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;
    type SnapshotHandle = Vec<u8>;

    fn apply(&mut self, log_index: u64, cmd: Vec<u8>) -> u64 {
        self.counter = self.counter.wrapping_add(cmd.len() as u64);
        self.last_applied = Some(log_index);
        self.counter
    }
    fn query(&self, _: ()) -> u64 {
        self.counter
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
    fn install_snapshot(&mut self, _: &mut dyn Read) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }
}

async fn drive(client: &uc_client::Client, n: usize, inflight: usize, payload_bytes: usize) {
    let payload = vec![0u8; payload_bytes];
    stream::iter(0..n)
        .map(|_| {
            let p = payload.clone();
            async move {
                let _r: u64 = client.submit(&p).await.expect("submit");
            }
        })
        .buffer_unordered(inflight)
        .for_each(|_| async {})
        .await;
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = Args::parse();
    let fixture = ClusterFixture::<Echo>::single_node(1)
        .await
        .expect("spawn single-node cluster");
    let client = fixture.client(0);

    // Warmup (prime caches; not measured).
    drive(client, args.warmup, args.inflight, args.payload_bytes).await;

    uc_protocol::probes::reset();
    drive(client, args.count, args.inflight, args.payload_bytes).await;
    let rows = uc_protocol::probes::drain_joined();

    // One histogram per stage name, preserving first-seen order.
    let mut order: Vec<&'static str> = Vec::new();
    let mut hists: std::collections::HashMap<&'static str, Histogram<u64>> =
        std::collections::HashMap::new();
    for row in &rows {
        for (name, delta) in uc_protocol::probes::stage_deltas(row) {
            let h = hists.entry(name).or_insert_with(|| {
                order.push(name);
                Histogram::<u64>::new(3).expect("hist")
            });
            h.record(delta).ok();
        }
    }

    if let Some(parent) = args.out.parent() {
        std::fs::create_dir_all(parent).expect("create out dir");
    }
    let mut f = File::create(&args.out).expect("create csv");
    writeln!(
        f,
        "config,workload,payload_bytes,inflight,stage,p50_ns,p99_ns,p99_9_ns,count"
    )
    .unwrap();
    for name in &order {
        let h = &hists[name];
        writeln!(
            f,
            "{},bytes,{},{},{},{},{},{},{}",
            args.config,
            args.payload_bytes,
            args.inflight,
            name,
            h.value_at_quantile(0.50),
            h.value_at_quantile(0.99),
            h.value_at_quantile(0.999),
            h.len(),
        )
        .unwrap();
    }

    // JSON summary on stdout for run-iter / quick inspection.
    let total = hists.get("total");
    let dominant = order
        .iter()
        .filter(|n| **n != "total")
        .max_by_key(|n| hists[**n].value_at_quantile(0.99))
        .copied()
        .unwrap_or("none");
    let summary = serde_json::json!({
        "n_requests": rows.len(),
        "total_p99_ns": total.map(|h| h.value_at_quantile(0.99)).unwrap_or(0),
        "dominant_stage": dominant,
        "dominant_stage_p99_ns": hists.get(dominant)
            .map(|h| h.value_at_quantile(0.99)).unwrap_or(0),
        "out": args.out.to_string_lossy(),
    });
    println!("{summary}");

    fixture.shutdown().await.expect("shutdown");
}
