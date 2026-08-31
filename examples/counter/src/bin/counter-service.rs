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
use uc_service::{ServiceBuilder, ServiceConfig};

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
    /// Which declared FSM slot this process is (see [services] ids).
    #[arg(long, default_value_t = 0)]
    service_id: u8,
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

    let cfg =
        ServiceConfig::new(args.instance_dir.clone(), args.app_id).service_id(args.service_id);
    let service = ServiceBuilder::new(cfg, CounterSm::default()).start()?;
    println!(
        "service {} attached at {}",
        args.service_id,
        args.instance_dir.display()
    );

    // The template every service binary should copy: a signal flag, a poll
    // loop that supervises the apply agent, and an explicit stop. A service
    // killed by SIGTERM's default disposition never calls `Service::stop`, so
    // it leaves the node's shared memory attached until the OS tears it down.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        signal_hook::flag::register(sig, std::sync::Arc::clone(&stop))?;
    }

    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
        // A fail-stopped apply thread must not look like a healthy service.
        // `is_alive` is false once the apply agent's work closure has panicked
        // (instance mismatch, log rewind) — exit non-zero so the supervisor
        // restarts us rather than leaving a zombie attached.
        if !service.is_alive() {
            eprintln!("counter-service: apply agent died; exiting for restart");
            return Err(anyhow::anyhow!("apply agent fail-stopped"));
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    println!("counter-service: signalled, stopping");
    service.stop();
    Ok(())
}
