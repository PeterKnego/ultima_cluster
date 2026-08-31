// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `hop_bench` — the M13 per-hop isolation bench for the remote request path.
//!
//! The end-to-end path is
//!
//! ```text
//! [RemoteClient] ─TCP─▶ [Edge] ─shmem Engine─▶ [node] ─▶ consensus ─▶ [service]
//!      hop 3             hop 2         hop 1              (≈1.4 M/s, known)
//! ```
//!
//! and this binary carries a **dummy sink and a minimal driver for every
//! hop**, each one process, so each hop can be measured alone and pairs of
//! hops can be composed:
//!
//! | role | what it is | isolates |
//! |---|---|---|
//! | `dummy-node` | a node-shaped process over a REAL instance dir (cnc page + rings): pops the ingress ring, publishes one position-keyed RESPONSE per record, discards the payload | hop 1 sink (an infinitely fast backend) |
//! | `engine-load` | N independent `uc_client::Engine`s driving the local instance dir | hop 1 driver |
//! | `edge` | the real `uc_gateway::Edge` (same config the M12 fleet edge uses) | hop 2 |
//! | `blaster` | a raw remote-protocol v1 client: HELLO, pre-encoded SUBMITs, credit tracking, RESPONSE parsing — none of `RemoteClient`'s state machine | hop 2 driver / TCP floor |
//! | `remote-load` | N real remote clients on the `RemoteEngine` halves | hop 3 driver |
//! | `dummy-edge` | a TCP server that answers HELLO_OK and one RESPONSE per SUBMIT immediately | hop 3 sink |
//! | `local` | dev-box smoke: spawns the roles above as subprocesses and runs the composition matrix | — |
//!
//! Every load role prints exactly one `RESULT {json}` line (`stats.rs`); the
//! fleet driver is `bench-infra/scripts/m13_hop_bench.py`.

mod blaster;
mod dummy_edge;
mod dummy_node;
mod engine_load;
mod local;
mod remote_load;
mod stats;

use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser)]
#[command(
    name = "hop_bench",
    about = "M13 per-hop isolation bench (see the module doc)"
)]
struct Cli {
    #[command(subcommand)]
    role: Role,
}

#[derive(clap::Subcommand)]
enum Role {
    /// Hop-1 sink: node-shaped process over a real instance dir. Parks until killed.
    DummyNode(dummy_node::Args),
    /// Hop-1 driver: N independent local `Engine`s over the instance dir.
    EngineLoad(engine_load::Args),
    /// The real `uc_gateway::Edge` over the instance dir. Parks until killed.
    Edge(EdgeArgs),
    /// Hop-2 driver / TCP floor: raw remote-protocol client, N connections.
    Blaster(blaster::Args),
    /// Hop-3 driver: N real remote clients on the `RemoteEngine` halves.
    RemoteLoad(remote_load::Args),
    /// Hop-3 sink: TCP server answering every SUBMIT immediately. Parks until killed.
    DummyEdge(dummy_edge::Args),
    /// Dev-box smoke: run the composition matrix with subprocesses.
    Local(local::Args),
}

#[derive(clap::Args)]
pub struct EdgeArgs {
    #[arg(long)]
    pub instance_dir: PathBuf,
    #[arg(long, default_value = "hop-bench")]
    pub app_id: String,
    #[arg(long)]
    pub listen: SocketAddr,
    /// Comma-separated `id@gateway_addr` map (every member's EDGE address).
    /// Defaults to `0@<listen>`, which is right for a dummy node with node id 0.
    #[arg(long)]
    pub members: Option<String>,
    /// Engine window (`max_inflight`).
    #[arg(long, default_value_t = 4096)]
    pub max_inflight: u32,
    /// Credits granted to every connection at HELLO_OK (`per_conn_inflight`).
    /// Must be at or under the edge's grant budget — `max_inflight` less its
    /// 1/8 headroom — or `Edge::start` refuses by name.
    #[arg(long, default_value_t = 1024)]
    pub per_conn_inflight: u32,
    #[arg(long, default_value_t = false)]
    pub envelope: bool,
}

fn run_edge(a: EdgeArgs) -> anyhow::Result<()> {
    use uc_gateway::{Edge, EdgeConfig, Member};
    let members_str = a.members.unwrap_or_else(|| format!("0@{}", a.listen));
    let members: Vec<Member> = members_str
        .split(',')
        .filter(|s| !s.trim().is_empty())
        .map(|s| {
            let (id, addr) = s.trim().split_once('@').expect("member is id@addr");
            Member {
                node_id: id.parse().expect("member id"),
                gateway: addr.to_string(),
            }
        })
        .collect();
    let edge = Edge::start(EdgeConfig {
        instance_dir: a.instance_dir,
        app_id: a.app_id,
        listen: a.listen,
        members,
        session_envelope: a.envelope,
        max_inflight: a.max_inflight,
        per_conn_inflight: a.per_conn_inflight,
        status_interval: Duration::from_millis(200),
        request_timeout: Duration::from_secs(30),
        ..EdgeConfig::defaults()
    })
    .map_err(|e| anyhow::anyhow!("edge start: {e}"))?;
    println!(
        "hop_bench edge up on {}; parking (killed externally)",
        edge.local_addr()
    );
    println!("READY");
    // One stats line per second, deltas, so a collapse rung shows WHAT the
    // edge is doing (backpressure events, retries, responses) not just that
    // it is busy.
    let mut last = edge.stats();
    loop {
        std::thread::sleep(Duration::from_secs(1));
        let s = edge.stats();
        println!(
            "edge: conns={} submits/s={} responses/s={} backpressure/s={} retries/s={} \
             unknown/s={} status/s={} grants/s={}",
            s.connections,
            s.submits - last.submits,
            s.responses - last.responses,
            s.backpressure_events - last.backpressure_events,
            s.retries - last.retries,
            s.unknown - last.unknown,
            s.status_frames - last.status_frames,
            s.grant_changes - last.grant_changes,
        );
        last = s;
    }
}

fn main() -> anyhow::Result<()> {
    match Cli::parse().role {
        Role::DummyNode(a) => dummy_node::run(a),
        Role::EngineLoad(a) => engine_load::run(a),
        Role::Edge(a) => run_edge(a),
        Role::Blaster(a) => blaster::run(a),
        Role::RemoteLoad(a) => remote_load::run(a),
        Role::DummyEdge(a) => dummy_edge::run(a),
        Role::Local(a) => local::run(a),
    }
}
