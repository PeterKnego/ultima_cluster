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

/// `version` is not decoration: clap's derive only generates `--version` when
/// this flag is present, and it was missing here while `uc2-node` and `uc2ctl`
/// both had it — so `uc2-gateway --version` answered "unexpected argument"
/// right through the 2.10.0 release. `identifies_itself_by_version` below is
/// the regression guard.
#[derive(Parser)]
#[command(
    name = "uc2-gateway",
    version,
    about = "An ultima_cluster gateway edge"
)]
struct Args {
    /// Path to the gateway's TOML configuration file.
    #[arg(long)]
    config: PathBuf,
}

/// How often (in 100 ms ticks) the stats record is emitted — 100 * 100 ms
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
    let bind_str = edge.local_addr().to_string();
    uc_obs::obs_event!(Info, "gateway_listening", bind = bind_str.as_str());

    let stop = Arc::new(AtomicBool::new(false));
    for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        if let Err(e) = signal_hook::flag::register(sig, Arc::clone(&stop)) {
            let err = e.to_string();
            uc_obs::obs_event!(Error, "gateway_signal_handler_failed", err = err.as_str());
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
            uc_obs::obs_event!(
                Error,
                "gateway_edge_faulted",
                reason = "node instance restarted"
            );
            return ExitCode::from(1);
        }

        tick += 1;
        if tick.is_multiple_of(STATS_EVERY_N_TICKS) {
            let s = edge.stats();
            uc_obs::obs_event!(
                Info,
                "gateway_stats",
                conns = s.connections,
                submits = s.submits,
                queries = s.queries,
                responses = s.responses,
                redirects = s.redirects,
                retries = s.retries,
                unknown = s.unknown,
                backpressure = s.backpressure_events,
                grant_changes = s.grant_changes,
                leader_changes = s.leader_changes,
                status = s.status_frames,
                refused_busy = s.refused_busy,
            );
        }

        std::thread::sleep(Duration::from_millis(100));
    }

    uc_obs::obs_event!(Info, "gateway_stopped");
    edge.stop();
    ExitCode::SUCCESS
}

#[cfg(test)]
mod cli_tests {
    use super::Args;
    use clap::CommandFactory;

    /// The whole surface a `--version` flag needs: clap emits it iff
    /// `#[command(version)]` is set, and the string must be the crate's own
    /// version so it moves with the lockstep workspace bump.
    #[test]
    fn identifies_itself_by_version() {
        let cmd = Args::command();
        assert_eq!(
            cmd.get_version(),
            Some(env!("CARGO_PKG_VERSION")),
            "uc2-gateway must answer --version, like uc2-node and uc2ctl"
        );
    }
}
