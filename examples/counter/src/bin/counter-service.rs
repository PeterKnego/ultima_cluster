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
use uc_service::{ServiceBuilder, ServiceConfig, ServiceError, StateMachine};

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

    // The template every service binary should copy: a signal flag, a poll
    // loop that supervises the apply agent, and an explicit stop. A service
    // killed by SIGTERM's default disposition never calls `Service::stop`, so
    // it leaves the node's shared memory attached until the OS tears it down.
    // Register the flag FIRST — before any wait — so a SIGTERM that arrives
    // while the node is still booting also ends in a clean exit, not a kill.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        signal_hook::flag::register(sig, std::sync::Arc::clone(&stop))?;
    }
    let stopping = || stop.load(std::sync::atomic::Ordering::Relaxed);

    // The node creates the control page on startup; tolerate being launched
    // first.
    let cnc = args.instance_dir.join("cnc2.dat");
    let deadline = Instant::now() + Duration::from_secs(args.wait_secs);
    while !cnc.exists() {
        if stopping() {
            println!("counter-service: signalled before attach, exiting");
            return Ok(());
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "no node at {} after {}s (is counter-node running?)",
            args.instance_dir.display(),
            args.wait_secs
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    // The control page exists before the node has published its service
    // table, and `start` refuses that window by name (`NodeBooting`) rather
    // than attaching to a half-initialised node. Retry it within the same
    // deadline; any other refusal is final. Each `start` waits only briefly
    // (its own `boot_wait` defaults to 10 s), so this loop — which owns the
    // deadline — also notices a stop request promptly.
    let service = loop {
        if stopping() {
            println!("counter-service: signalled before attach, exiting");
            return Ok(());
        }
        let cfg = ServiceConfig::new(args.instance_dir.clone(), args.app_id.clone())
            .with_boot_wait(Duration::from_millis(200));
        match ServiceBuilder::new(cfg, CounterSm::default()).start() {
            Ok(service) => break service,
            Err(ServiceError::NodeBooting) => {
                anyhow::ensure!(
                    Instant::now() < deadline,
                    "node at {} still booting after {}s",
                    args.instance_dir.display(),
                    args.wait_secs
                );
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(e) => return Err(e.into()),
        }
    };
    println!(
        "service {:?} attached at {}",
        CounterSm::NAME,
        args.instance_dir.display()
    );

    while !stopping() {
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
