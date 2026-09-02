// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
//
//! One `ultima_cluster` node. Creates the instance directory's shared-memory
//! control page and log buffer, starts the four agent threads, and stays alive.
//!
//! Run one of these per cluster member, then attach a `counter-service` to each.
//! See `docs/QUICKSTART.md`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use counter::CounterSm;
use uc_consensus::election::NodeId;
use uc_net::fault::FaultConfig;
use uc_node::{CryptoConfig, Node, NodeConfig, PurgePolicy};
use uc_service::StateMachine;

#[derive(Parser)]
#[command(about = "A counter-example ultima_cluster node")]
struct Args {
    /// Where this node's log buffer, journal, and control page live.
    #[arg(long)]
    instance_dir: PathBuf,
    /// This node's id. Must be unique in the cluster and must appear in --members.
    #[arg(long)]
    id: NodeId,
    /// UDP address this node binds for peer traffic.
    #[arg(long)]
    bind: SocketAddr,
    /// Comma-separated `id@addr` list of every member, including this one.
    /// Omit for a single-node cluster.
    #[arg(long)]
    members: Option<String>,
    #[arg(long, default_value = "counter")]
    app_id: String,
}

fn parse_members(s: &str) -> anyhow::Result<Vec<(NodeId, SocketAddr)>> {
    s.split(',')
        .map(|part| {
            let (id, addr) = part
                .split_once('@')
                .ok_or_else(|| anyhow::anyhow!("bad --members entry {part:?}, expected id@addr"))?;
            Ok((id.trim().parse()?, addr.trim().parse()?))
        })
        .collect()
}

/// Give each node a distinct election-timeout stream. Without this every node
/// runs the identical randomized sequence, so they all time out at the same
/// instant, all stand for election, all split the vote, and the cluster
/// livelocks instead of electing anyone.
fn seed_for(id: NodeId) -> u64 {
    1 ^ (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let members = match &args.members {
        Some(s) => parse_members(s)?,
        None => vec![(args.id, args.bind)],
    };
    anyhow::ensure!(
        members.iter().any(|(id, _)| *id == args.id),
        "--members must include this node's own id ({})",
        args.id
    );

    let node = Node::start(NodeConfig {
        id: args.id,
        members,
        bind: args.bind,
        instance_dir: args.instance_dir,
        app_id: args.app_id,
        buffer_bytes: 1 << 22, // 4 MiB log ring
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: seed_for(args.id),
        faults: FaultConfig::default(),
        purge: PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::single(CounterSm::NAME),
    })?;

    println!("node {} listening on {}", args.id, args.bind);

    // Report role transitions, so a terminal running this shows the election
    // and any later failover.
    let mut was_leader = None;
    loop {
        let is_leader = node.is_leader();
        if was_leader != Some(is_leader) {
            println!(
                "node {} is now {} (term {})",
                args.id,
                if is_leader { "LEADER" } else { "follower" },
                node.current_term()
            );
            was_leader = Some(is_leader);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}
