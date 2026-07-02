//! commit-path-load — open-loop load driver for UC's full commit path over
//! the shmem client surface. Drives a rate ladder × in-flight-concurrency
//! sweep (shared core: `uc_autobench::loadcore`) against either an in-process
//! `ClusterFixture` (Phase 1, single-node) or a running cluster's instance
//! dir (`--connect`, Phase 2 multi-process), recording submit→response
//! latency in an HDR histogram and writing one CSV row per ladder step.
//!
//! For the embedded (no-shmem) arm, the same sweep runs in-process inside
//! `uc-node-launch --ipc-mode embedded` — see loadcore's `Submitter` seam.

use std::sync::Arc;

use clap::Parser;
use uc_autobench::loadcore::{parse_list, run_sweep, ClientSubmitter, KvSm, SweepOpts};
use uc_node::test_support::ClusterFixture;

#[derive(Parser)]
#[command(about = "Open-loop commit-path load driver for UC (shmem client path)")]
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

// Multi-threaded runtime: at high inflight the driver must spread the in-flight
// request futures + the client's response-reader task across cores. On the old
// single-threaded (`current_thread`) runtime one core saturated at inflight >= 512
// (each in-flight future also wakes a 100ms stall-check timer), starving the
// response path and stalling the sweep. run_step spawns each request as a task.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();
    let args = Args::parse();

    eprintln!(
        "commit-path-load: config={} rates={} inflight={} payload={}B",
        args.config, args.rates, args.inflight, args.payload_bytes
    );

    // One client is enough for the open-loop driver (submit takes &self and we
    // keep many requests in flight). Either attach to a running cluster
    // (--connect, Phase 2 multi-process) or spawn the in-process single-node
    // fixture (Phase 1). Both bindings must outlive the run, so declare them
    // before the branch.
    let fixture: Option<ClusterFixture<KvSm>>;
    let client: Arc<uc_client::Client> = if let Some(dir) = &args.connect {
        eprintln!(
            "commit-path-load: attaching to running cluster at {} (app_id={})",
            dir.display(),
            args.app_id
        );
        fixture = None;
        Arc::new(uc_client::Client::connect(dir, &args.app_id).await?)
    } else {
        let f = ClusterFixture::<KvSm>::single_node(1).await?;
        let c = Arc::new(uc_client::Client::connect(f.instance_path(), f.app_id()).await?);
        fixture = Some(f);
        c
    };

    let opts = SweepOpts {
        config: args.config,
        rates: parse_list(&args.rates),
        inflights: parse_list(&args.inflight),
        payload_bytes: args.payload_bytes,
        window_secs: args.window_secs,
        warmup_secs: args.warmup_secs,
        out: args.out.clone().into(),
        hgrm_dir: args.hgrm_dir,
    };
    run_sweep(ClientSubmitter(client), &opts).await?;

    // Ordered teardown only for the in-process fixture; a --connect client is
    // dropped (the external cluster keeps running).
    if let Some(fixture) = fixture {
        fixture.shutdown().await?;
    }

    eprintln!("commit-path-load: wrote {}", args.out);
    Ok(())
}
