// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
//
//! Talks to a running counter cluster over shared memory.
//!
//! A client attaches to *one* node's instance directory. Writes must go to the
//! leader; if you point this at a follower it will say so and name the leader,
//! rather than silently doing something wrong.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::Parser;
use counter::{Applied, Command, Query, QueryResponse};
use uc2_client::{Client, ClientError};

#[derive(Parser)]
#[command(about = "Submits commands and reads the counter")]
struct Args {
    /// The instance directory of the node to talk to.
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "counter")]
    app_id: String,
    /// Add this much, this many times.
    #[arg(long, default_value_t = 1)]
    add: i64,
    /// How many commands to submit.
    #[arg(long, default_value_t = 5)]
    count: u32,
    /// Reset the counter to zero instead of adding.
    #[arg(long)]
    reset: bool,
    /// Only read the current value; submit nothing.
    #[arg(long)]
    read_only: bool,
    /// Use a snapshot read instead of a linearizable one. Snapshot reads are
    /// answered from the local replica's own state, so they work on a follower
    /// — which is how you can see for yourself that replication happened.
    #[arg(long)]
    snapshot: bool,
    /// Wait up to this long for the node to become able to serve.
    #[arg(long, default_value_t = 30)]
    wait_secs: u64,
}

/// Writes are leader-only, and a freshly started cluster takes an election plus
/// one committed NewTerm frame before anyone can serve. Retry the transient
/// cases; report the rest.
fn submit_with_retry(client: &Client, cmd: &Command, deadline: Instant) -> anyhow::Result<Applied> {
    loop {
        match client.submit::<Command, Applied>(cmd) {
            Ok(applied) => return Ok(applied),
            Err(ClientError::NotLeader { hint }) if Instant::now() < deadline => {
                if let Some(id) = hint {
                    anyhow::bail!(
                        "this node is not the leader — node {id} is. \
                         Point --instance-dir at that node."
                    );
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(ClientError::Retry | ClientError::Timeout(_)) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => return Err(e.into()),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let client = Client::connect(&args.instance_dir, &args.app_id)?;
    let deadline = Instant::now() + Duration::from_secs(args.wait_secs);

    if !args.read_only {
        if args.reset {
            let applied = submit_with_retry(&client, &Command::Reset, deadline)?;
            println!("Reset -> value {} @ position {}", applied.value, applied.position);
        } else {
            for _ in 0..args.count {
                let applied = submit_with_retry(&client, &Command::Add(args.add), deadline)?;
                println!(
                    "Add({}) -> value {} @ position {}",
                    args.add, applied.value, applied.position
                );
            }
        }
    }

    if args.snapshot {
        let r: QueryResponse = client.query_snapshot(&Query::Value)?;
        println!("snapshot read -> {}", r.value);
    } else {
        match client.query_linearizable::<Query, QueryResponse>(&Query::Value) {
            Ok(r) => println!("linearizable read -> {}", r.value),
            Err(ClientError::NotLeader { hint }) => anyhow::bail!(
                "linearizable reads are leader-only and this node is a follower{}. \
                 Either point --instance-dir at the leader, or pass --snapshot to \
                 read this replica's own copy of the state.",
                hint.map(|id| format!(" (node {id} is the leader)")).unwrap_or_default()
            ),
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
