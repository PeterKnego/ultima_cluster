// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The `ultima_cluster` node daemon.
//!
//! Starts one node from a TOML config file and runs until signalled. On
//! `SIGTERM`/`SIGINT` it drains the archive to a bounded deadline and stops
//! the agents cleanly, so the restarted node rejoins from the journal instead
//! of paying reconstruction.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use clap::Parser;
use uc2_node::preflight::FsVerdict;
use uc2_node::{DrainOutcome, Node, config_file, preflight};

#[derive(Parser)]
#[command(name = "uc2-node", about = "An ultima_cluster node")]
struct Args {
    /// Path to the node's TOML configuration file.
    #[arg(long)]
    config: PathBuf,
    /// How long to let the archive drain before stopping anyway.
    #[arg(long, default_value = "5")]
    drain_timeout_secs: u64,
}

fn main() -> ExitCode {
    let args = Args::parse();

    let (cfg, opts) = match config_file::load_from_path(&args.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("uc2-node: {e}");
            return ExitCode::from(2);
        }
    };
    // A config section this version parses but does not act on must never be
    // silently swallowed — the operator wrote it expecting an effect. Same
    // never-silent discipline as the durability override below.
    if opts.reserved.any() {
        eprintln!(
            "uc2-node: NOTE: config section(s) [{}] are RESERVED for a future release \
             and have NO effect in this version; they were parsed and ignored.",
            opts.reserved.names().join("], [")
        );
    }
    match preflight::check(&cfg, &opts) {
        Ok(FsVerdict::Durable) => {}
        // The override suppresses the refusal, never the notice — and it is
        // announced on EVERY boot, not just the one where it was added. A
        // cluster running on a RAM-backed filesystem must never look quiet.
        Ok(FsVerdict::VolatileOverridden { fs }) => {
            eprintln!(
                "uc2-node: WARNING: starting with the durability check OVERRIDDEN. \
                 Every fsync may be a silent no-op, so this node can lose committed \
                 data on power loss. TEST/DEV ONLY. Detail: {fs}"
            );
        }
        Err(e) => {
            eprintln!("uc2-node: refusing to start: {e}");
            return ExitCode::from(2);
        }
    }

    let id = cfg.id;
    let bind = cfg.bind;
    let node = match Node::start(cfg) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("uc2-node: failed to start node {id}: {e}");
            return ExitCode::from(1);
        }
    };
    println!("uc2-node: node {id} listening on {bind}");

    let stop = Arc::new(AtomicBool::new(false));
    for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        if let Err(e) = signal_hook::flag::register(sig, Arc::clone(&stop)) {
            eprintln!("uc2-node: cannot install signal handler: {e}");
            node.stop();
            return ExitCode::from(1);
        }
    }

    let mut was_leader = None;
    while !stop.load(Ordering::Relaxed) {
        let is_leader = node.is_leader();
        if was_leader != Some(is_leader) {
            println!(
                "uc2-node: node {id} is now {} (term {})",
                if is_leader { "LEADER" } else { "follower" },
                node.current_term()
            );
            was_leader = Some(is_leader);
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    println!("uc2-node: signalled, draining");
    match node.stop_draining(Duration::from_secs(args.drain_timeout_secs)) {
        DrainOutcome::Drained => println!("uc2-node: drained, stopped cleanly"),
        DrainOutcome::DeadlineExpired { append, durable } => eprintln!(
            "uc2-node: drain deadline expired with {} bytes unrecorded \
             (append {append}, durable {durable}); stopped anyway — the restarted \
             node will re-fetch them",
            append.saturating_sub(durable)
        ),
    }
    ExitCode::SUCCESS
}
