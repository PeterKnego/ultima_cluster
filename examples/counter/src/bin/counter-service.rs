// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
//
//! Runs a `CounterSm` against an already-running `counter-node`.
//!
//! The service attaches to the node's shared memory and polls the committed log
//! in place — it is not sent entries, and nothing is copied across the process
//! boundary except the one payload copy at the apply call itself.
//!
//! Every node in the cluster runs one of these. They all apply the same
//! commands in the same order and therefore hold identical state.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::Parser;
use counter::CounterSm;
use uc2_service::{ServiceBuilder, ServiceConfig};

#[derive(Parser)]
#[command(about = "Runs the counter state machine against a local node")]
struct Args {
    /// The instance directory of the node to attach to.
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "counter")]
    app_id: String,
    /// How long to wait for the node's control page to appear.
    #[arg(long, default_value_t = 30)]
    wait_secs: u64,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // The node creates the control page on startup; tolerate being launched
    // first.
    let cnc = args.instance_dir.join("cnc2.dat");
    let deadline = Instant::now() + Duration::from_secs(args.wait_secs);
    while !cnc.exists() {
        anyhow::ensure!(
            Instant::now() < deadline,
            "no node at {} after {}s (is counter-node running?)",
            args.instance_dir.display(),
            args.wait_secs
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    let cfg = ServiceConfig::new(args.instance_dir.clone(), args.app_id);
    let _service = ServiceBuilder::new(cfg, CounterSm::default()).start()?;
    println!("service attached at {}", args.instance_dir.display());

    loop {
        std::thread::park();
    }
}
