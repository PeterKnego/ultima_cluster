// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Node-only reference binary for the v2 multi-process hard-crash test
//! (M5 Task 14, spec §8 L3). Creates the instance_dir's `cnc2.dat` +
//! shared-memory IPC rings, runs the v2 sync node (a single-node cluster by
//! default), then parks until killed (the test SIGKILLs it).
//!
//! Unlike the v1 `uc-crashtest-node` binary, this has no `#[tokio::main]` —
//! the v2 stack is entirely synchronous (polling agents on OS threads):
//! `Node::start` spawns its agent threads and returns immediately, so `main`
//! just has to stay alive for them.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use uc2_consensus::election::NodeId;
use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "uc2-crashtest")]
    app_id: String,
    #[arg(long, default_value = "127.0.0.1:0")]
    bind: SocketAddr,
    #[arg(long, default_value_t = 0)]
    id: NodeId,
    /// Comma-separated `id@addr` member list (every member INCLUDING self).
    /// Defaults to a single-node cluster of just `--id` at `--bind`.
    #[arg(long)]
    members: Option<String>,
}

fn parse_members(s: &str) -> Vec<(NodeId, SocketAddr)> {
    s.split(',')
        .map(|part| {
            let (id, addr) = part
                .split_once('@')
                .unwrap_or_else(|| panic!("bad --members entry {part:?}, expected id@addr"));
            let id: NodeId = id.parse().unwrap_or_else(|e| panic!("bad member id {id:?}: {e}"));
            let addr: SocketAddr =
                addr.parse().unwrap_or_else(|e| panic!("bad member addr {addr:?}: {e}"));
            (id, addr)
        })
        .collect()
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let members = match &args.members {
        Some(s) => parse_members(s),
        None => vec![(args.id, args.bind)],
    };

    let cfg = NodeConfig {
        id: args.id,
        members,
        bind: args.bind,
        instance_dir: args.instance_dir,
        app_id: args.app_id,
        buffer_bytes: 1 << 22, // 4 MiB
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: 1,
        faults: FaultConfig::default(),
        purge: uc2_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
    };

    let _node = Node::start(cfg)?;
    // Park forever: the node's agent threads keep running in the background;
    // this process is torn down by a real SIGKILL from the test harness.
    loop {
        std::thread::park();
    }
}
