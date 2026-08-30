// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The `ultima_cluster` gateway edge daemon.
//!
//! Starts one [`Edge`] from a TOML config file and runs until signalled.
//! `uc2-gateway` is meant to run co-located with a `uc2-node` on the same
//! host (`packaging/systemd/uc2-gateway.service` binds it to that unit) and
//! attaches to the node's instance directory over shmem — see
//! `docs/superpowers/specs/2026-08-22-uc2-m12-adoptable-design.md` §4.3.
//!
//! Exit codes:
//! - `2` — the config file could not be read/parsed, or [`EdgeConfig::validate`]
//!   refused it by name. `packaging/systemd/uc2-gateway.service` sets
//!   `RestartPreventExitStatus=2`: a bad config is an operator problem, not
//!   something a restart loop fixes.
//! - `1` — [`Edge::start`] failed (e.g. the node's instance directory does not
//!   exist yet, or its listener could not bind). ALSO: once running, the main
//!   loop polls [`Edge::is_faulted`] every tick; when the underlying node's
//!   shmem instance restarts under it, the edge latches faulted and refuses
//!   new connections forever, so this binary exits 1 to let systemd's
//!   `Restart=on-failure` bring up a fresh gateway against the new node
//!   instance rather than serve a permanently faulted edge.
//! - `0` — clean stop on `SIGTERM`/`SIGINT`.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use clap::Parser;
use uc_gateway::{Edge, config_file};

#[derive(Parser)]
#[command(name = "uc2-gateway", about = "An ultima_cluster gateway edge")]
struct Args {
    /// Path to the gateway's TOML configuration file.
    #[arg(long)]
    config: PathBuf,
}

/// How often (in 100 ms ticks) the stats line prints to stderr — 100 * 100 ms
/// = 10 s, per the controller ruling.
const STATS_EVERY_N_TICKS: u64 = 100;

fn main() -> ExitCode {
    let args = Args::parse();

    let cfg = match config_file::load_from_path(&args.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("uc2-gateway: {e}");
            return ExitCode::from(2);
        }
    };

    for w in cfg.warnings() {
        eprintln!("uc2-gateway: warning: {w}");
    }

    let edge = match Edge::start(cfg) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("uc2-gateway: failed to start: {e}");
            return ExitCode::from(1);
        }
    };
    println!("uc2-gateway: listening on {}", edge.local_addr());

    let stop = Arc::new(AtomicBool::new(false));
    for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        if let Err(e) = signal_hook::flag::register(sig, Arc::clone(&stop)) {
            eprintln!("uc2-gateway: cannot install signal handler: {e}");
            edge.stop();
            return ExitCode::from(1);
        }
    }

    let mut tick: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        // A faulted edge is permanently done: the node's instance restarted
        // underneath it, so the attach it is running on is void. Exit rather
        // than idle forever, so the supervisor gets a fresh gateway up
        // against the new node instance.
        if edge.is_faulted() {
            eprintln!(
                "uc2-gateway: edge faulted: the node's instance restarted; exiting so the \
                 supervisor restarts the gateway"
            );
            return ExitCode::from(1);
        }

        tick += 1;
        if tick.is_multiple_of(STATS_EVERY_N_TICKS) {
            let s = edge.stats();
            eprintln!(
                "uc2-gateway: conns={} submits={} queries={} responses={} redirects={} \
                 retries={} unknown={} backpressure={} grant_changes={} leader_changes={} \
                 status={} refused_busy={}",
                s.connections,
                s.submits,
                s.queries,
                s.responses,
                s.redirects,
                s.retries,
                s.unknown,
                s.backpressure_events,
                s.grant_changes,
                s.leader_changes,
                s.status_frames,
                s.refused_busy,
            );
        }

        std::thread::sleep(Duration::from_millis(100));
    }

    println!("uc2-gateway: signalled, stopping");
    edge.stop();
    ExitCode::SUCCESS
}
