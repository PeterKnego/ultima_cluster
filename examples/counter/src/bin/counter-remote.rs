// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
//
//! The **remote twin of `counter-client`** — and the reference for how a
//! remote client talks to a gateway.
//!
//! `counter-client` attaches to one node's instance directory over shared
//! memory: it must run on a cluster host, and it must be pointed at the
//! leader. This binary does the same three things (add, reset, read) from
//! anywhere on the network, over `uc_remote`'s framed TCP protocol, against
//! whichever `uc2-gateway` answers first. The differences worth copying into
//! your own remote client are all in `run` below:
//!
//! 1. **Give it every gateway, not the leader's.** `--gateways` is the whole
//!    member list; the client dials them in order, follows the `REDIRECT` /
//!    `LEADER_CHANGED` the edge sends when it is not the leader, and re-sends
//!    across a failover on its own. There is no "point it at the leader" step
//!    here, and no `NotLeader` error to handle — that is the gateway's job.
//! 2. **The payload is opaque bytes.** The edge never deserialises a command;
//!    it moves bytes between this process and the state machine. So the
//!    encoding is the application's contract with *itself*: `counter-service`
//!    decodes `counter::Command` with bincode-standard, so this encodes with
//!    bincode-standard. `counter-client` does exactly the same thing — it just
//!    has `uc_client` do the calls for it.
//! 3. **One request, one resolution.** A ticket ends in the response or in a
//!    named error; `RETRY`, `REDIRECT` and connection loss never reach the
//!    caller. This binary waits with `Ticket::wait_timeout` rather than
//!    `Ticket::wait`, so `--timeout-secs` bounds the *process*, not just the
//!    request: a client that is stuck reconnecting-and-being-redirected (every
//!    edge answering "not me", e.g. a cluster with no leader) is still making
//!    progress from `RemoteClient`'s point of view, and a one-shot CLI must
//!    not wait for that forever.
//!
//! Because the counter has no dedup, this client sets
//! `resend_on_unknown: false`: an UNKNOWN outcome is reported, never retried.
//!
//! Note that the gateway used by the quickstart runs with
//! `[session] envelope = false`, because `counter-service` runs a plain
//! `CounterSm`. A service that wraps its state machine in
//! `uc_service::Sessioned` turns the envelope on and gets exactly-once
//! writes across a re-send — then `replayed=true` in this binary's output
//! means "your write had already been applied; it was not applied twice".
//!
//! Exit codes: `0` success, `1` the request failed, `2` bad arguments.

use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use counter::{Applied, Command as CounterCommand, Query, QueryResponse};
use uc_remote::{Consistency, RemoteClient, RemoteConfig, RemoteError};

#[derive(Parser)]
#[command(
    name = "counter-remote",
    about = "Drives the counter cluster through a uc2-gateway"
)]
struct Args {
    /// Every gateway's address, comma-separated: `host:port[,host:port…]`.
    /// List them all — the client dials in order and follows redirects, so
    /// this does not have to name the leader's.
    #[arg(long, value_delimiter = ',', required = true)]
    gateways: Vec<String>,
    /// Application identity. Must match the gateway's (and the node's).
    #[arg(long, default_value = "counter")]
    app_id: String,
    /// Budget for the request, across re-sends and reconnects. Approximate:
    /// it is applied twice — once to the connect retry loop, once to the wait
    /// — so the worst case is ~2x this plus the one-second floor the wait
    /// keeps, and `uc_remote` itself enforces `request_timeout` only within
    /// about a sweep interval plus a connect attempt.
    #[arg(long, default_value_t = 10)]
    timeout_secs: u64,
    #[command(subcommand)]
    cmd: Sub,
}

#[derive(Subcommand)]
enum Sub {
    /// Add `n` to the counter (negative subtracts).
    Add { n: i64 },
    /// Set the counter back to zero.
    Reset,
    /// Read the counter.
    Get {
        /// Go through the cluster's read barrier — the answer reflects every
        /// write acknowledged before this call. Without it the read is served
        /// from whichever replica's gateway answered, and may be stale.
        #[arg(long)]
        linearizable: bool,
    },
}

/// Bad arguments (exit 2) vs. a failed request (exit 1). Keeping them apart
/// matters for scripting: exit 2 says "you typed it wrong", and no amount of
/// retrying will change it.
enum Fail {
    Args(String),
    Run(String),
}

fn enc<T: serde::Serialize>(v: &T) -> Vec<u8> {
    bincode::serde::encode_to_vec(v, bincode::config::standard())
        .expect("encoding a counter command cannot fail")
}

fn dec<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T, Fail> {
    bincode::serde::decode_from_slice(b, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| {
            Fail::Run(format!(
                "cannot decode the response ({e}) — app_id mismatch?"
            ))
        })
}

/// Connect, retrying while the deadline holds: a cluster that is still
/// electing, or gateways that have not bound their listener yet, are a
/// *timing* condition, not a configuration error. A `Config` refusal is not
/// retried — it can never start working.
fn connect(args: &Args, deadline: Instant) -> Result<RemoteClient, Fail> {
    let cfg = RemoteConfig {
        app_id: args.app_id.clone(),
        members: args.gateways.clone(),
        request_timeout: Duration::from_secs(args.timeout_secs),
        // `CounterSm` is not wrapped in `uc_service::Sessioned`, so nothing
        // downstream can tell a re-send from a second command: re-sending an
        // `Add(5)` whose outcome is UNKNOWN would risk applying it twice. Say
        // "unknown" out loud instead — the honest answer for a state machine
        // with no dedup. A service that wraps its SM in `Sessioned` (and an
        // edge with the session envelope on) leaves this at its `true`
        // default, because there the re-send is answered "replayed".
        resend_on_unknown: false,
        ..Default::default()
    };
    loop {
        match RemoteClient::connect(cfg.clone()) {
            Ok(c) => return Ok(c),
            Err(RemoteError::Config(m)) => return Err(Fail::Args(m)),
            Err(e) => {
                if Instant::now() >= deadline {
                    return Err(Fail::Run(format!("cannot reach any gateway: {e}")));
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

/// What is left of the `--timeout-secs` budget, floored at a second so a
/// request that only just got a connection still gets a chance to resolve.
fn remaining(deadline: Instant) -> Duration {
    deadline
        .saturating_duration_since(Instant::now())
        .max(Duration::from_secs(1))
}

fn run(args: &Args) -> Result<(), Fail> {
    for g in &args.gateways {
        if g.trim().is_empty() || !g.contains(':') {
            return Err(Fail::Args(format!(
                "--gateways entry {g:?} is not a host:port address"
            )));
        }
    }
    if args.timeout_secs == 0 {
        return Err(Fail::Args(
            "--timeout-secs must be greater than zero".into(),
        ));
    }

    let deadline = Instant::now() + Duration::from_secs(args.timeout_secs);
    let client = connect(args, deadline)?;

    let result = match &args.cmd {
        Sub::Add { n } => submit(&client, &CounterCommand::Add(*n), deadline),
        Sub::Reset => submit(&client, &CounterCommand::Reset, deadline),
        Sub::Get { linearizable } => query(&client, *linearizable, deadline),
    };
    // Shut the client's reader thread down before returning either way — the
    // process is about to exit, but a reference client should still show the
    // explicit close rather than relying on it.
    client.shutdown();
    result
}

fn submit(client: &RemoteClient, cmd: &CounterCommand, deadline: Instant) -> Result<(), Fail> {
    let ticket = client
        .submit(&enc(cmd))
        .map_err(|e| Fail::Run(e.to_string()))?;
    let resp = ticket
        .wait_timeout(remaining(deadline))
        .map_err(|e| Fail::Run(e.to_string()))?;
    let applied: Applied = dec(&resp.bytes)?;
    println!(
        "value={} position={} replayed={}",
        applied.value, resp.position, resp.replayed
    );
    Ok(())
}

fn query(client: &RemoteClient, linearizable: bool, deadline: Instant) -> Result<(), Fail> {
    let consistency = if linearizable {
        Consistency::Linearizable
    } else {
        Consistency::Snapshot
    };
    let ticket = client
        .query(&enc(&Query::Value), consistency)
        .map_err(|e| Fail::Run(e.to_string()))?;
    let resp = ticket
        .wait_timeout(remaining(deadline))
        .map_err(|e| Fail::Run(e.to_string()))?;
    let answer: QueryResponse = dec(&resp.bytes)?;
    println!("value={}", answer.value);
    Ok(())
}

fn main() -> ExitCode {
    let args = Args::parse();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(Fail::Run(m)) => {
            eprintln!("counter-remote: {m}");
            ExitCode::from(1)
        }
        Err(Fail::Args(m)) => {
            eprintln!("counter-remote: {m}");
            ExitCode::from(2)
        }
    }
}
