// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Service-only reference binary for the v2 multi-process hard-crash test
//! (M5 Task 14, spec §8 L3). Waits for the node's `cnc2.dat` to appear (up
//! to 30s), attaches, runs the non-persisting `RegisterSm`, then parks until
//! killed (the test SIGKILLs it mid-apply).
//!
//! Sync, like the node bin — no tokio.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::Parser;
use uc2_service::{ServiceBuilder, ServiceConfig};
use uc_lincheck::register::RegisterSm;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "uc2-crashtest")]
    app_id: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let cnc = args.instance_dir.join("cnc2.dat");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cnc.exists() {
        anyhow::ensure!(Instant::now() < deadline, "timed out waiting for cnc2.dat");
        std::thread::sleep(Duration::from_millis(20));
    }

    let cfg = ServiceConfig::new(args.instance_dir, args.app_id);
    let _svc = ServiceBuilder::new(cfg, RegisterSm::default()).start()?;
    loop {
        std::thread::park();
    }
}
