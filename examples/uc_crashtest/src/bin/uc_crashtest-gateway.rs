// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Gateway-only reference binary for the M12a remote lincheck capstone
//! (`tests/remote_lin.rs`, spec §4.6 row 1).
//!
//! Deliberately a near-clone of the shipped `uc2-gateway` daemon
//! (`uc_gateway/src/bin/uc2-gateway.rs`) with one difference: it is
//! configured by **CLI flags**, not a TOML file. The capstone respawns edges
//! every few seconds against ephemeral ports chosen at test start, and
//! writing (and rewriting) three config files to do that would add a
//! filesystem dance the test does not otherwise need.
//!
//! The exit-code contract is the interesting part, and it is the real one:
//!
//! - `1` — [`Edge::start`] failed (the node's instance dir is mid-restart, or
//!   the listener could not bind), **or** the edge latched faulted because
//!   the node's shmem instance restarted underneath it. A faulted edge
//!   refuses every new connection forever, so the only correct thing to do
//!   is die and let a supervisor bring up a fresh one against the new
//!   instance. `packaging/systemd/uc2-gateway.service` is that supervisor in
//!   production; in the capstone it is the test's own respawn loop — and
//!   proving that loop is enough to keep clients served across a leader
//!   SIGKILL is part of what the capstone is for.
//! - `2` — the flags were refused by [`EdgeConfig::validate`].
//!
//! There is no signal handling: the test tears this process down with
//! SIGKILL (`common::Reap`), which needs no cooperation from it.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;
use uc_gateway::{Edge, EdgeConfig, Member};

#[derive(Parser)]
struct Args {
    /// The local node's instance directory (the edge attaches over shmem).
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "uc_crashtest")]
    app_id: String,
    /// TCP address to accept remote clients on.
    #[arg(long)]
    listen: SocketAddr,
    /// Comma-separated `id@addr` map of every member's GATEWAY address
    /// (not the node's UDP bind) — what `REDIRECT`/`LEADER_CHANGED` name.
    #[arg(long)]
    members: String,
    /// Turn the session envelope OFF (raw pass-through, at-least-once). Must
    /// match the service's `--sessioned`, inverted.
    #[arg(long, default_value_t = false)]
    no_envelope: bool,
}

/// The edge's per-request deadline. Much shorter than the shipped 10 s
/// default on purpose: while its node is dead but not yet restarted, the cnc
/// page is frozen with `CAN_SERVE` still set, so this edge accepts submits
/// into a ring nobody drains and can only answer them `UNKNOWN` a timeout
/// later. That timeout is how quickly a client pinned here gets to re-send
/// somewhere useful, so in a test that kills a node every few seconds it has
/// to be a fraction of the kill period, not twice it.
///
/// The same reasoning applies in production, where the supervisor closes the
/// window instead of a test loop: see `docs/how-to/run-a-gateway.md`, "When
/// the node underneath dies".
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
/// Also the edge→client liveness tick; well under `RemoteConfig::dead_after`.
const STATUS_INTERVAL: Duration = Duration::from_millis(100);

fn parse_members(s: &str) -> Vec<Member> {
    s.split(',')
        .map(|part| {
            let (id, addr) = part
                .split_once('@')
                .unwrap_or_else(|| panic!("bad --members entry {part:?}, expected id@addr"));
            let node_id: u32 =
                id.parse().unwrap_or_else(|e| panic!("bad member id {id:?}: {e}"));
            Member { node_id, gateway: addr.to_string() }
        })
        .collect()
}

fn main() -> ExitCode {
    let args = Args::parse();

    let cfg = EdgeConfig {
        instance_dir: args.instance_dir,
        app_id: args.app_id,
        listen: args.listen,
        members: parse_members(&args.members),
        session_envelope: !args.no_envelope,
        request_timeout: REQUEST_TIMEOUT,
        status_interval: STATUS_INTERVAL,
        ..EdgeConfig::defaults()
    };
    if let Err(e) = cfg.validate() {
        eprintln!("uc_crashtest-gateway: {e}");
        return ExitCode::from(2);
    }

    let edge = match Edge::start(cfg) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("uc_crashtest-gateway: failed to start: {e}");
            return ExitCode::from(1);
        }
    };
    println!("uc_crashtest-gateway: listening on {}", edge.local_addr());

    loop {
        if edge.is_faulted() {
            let s = edge.stats();
            eprintln!(
                "uc_crashtest-gateway: edge faulted (the node's instance restarted); exiting 1 \
                 so the supervisor restarts it. conns={} submits={} queries={} responses={} \
                 redirects={} unknown={}",
                s.connections, s.submits, s.queries, s.responses, s.redirects, s.unknown
            );
            return ExitCode::from(1);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}
