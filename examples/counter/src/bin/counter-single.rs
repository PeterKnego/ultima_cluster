// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
//
//! The whole cluster in one process: node, service, and client.
//!
//! ```text
//! cargo run -p counter --bin counter-single
//! ```
//!
//! A single-node cluster is a real cluster — same consensus code, same log,
//! same durability — it just elects itself immediately and commits as soon as
//! its own fsync lands. That makes it the shortest path to seeing the system
//! work. For a genuinely fault-tolerant cluster see `docs/QUICKSTART.md`.
//!
//! Running the service in the node's process is a configuration choice, not a
//! different architecture: coordination between them is counters in shared
//! memory either way.

use std::time::{Duration, Instant};

use counter::{Applied, Command, CounterSm, Query, QueryResponse};
use uc_client::Client;
use uc_net::fault::FaultConfig;
use uc_node::{CryptoConfig, Node, NodeConfig, PurgePolicy};
use uc_service::{ServiceBuilder, ServiceConfig};

const APP_ID: &str = "counter";

fn main() -> anyhow::Result<()> {
    // A throwaway instance directory. The log buffer, the journal, and the
    // shared-memory control page all live here; a real deployment points this
    // at durable storage and keeps it.
    let dir = tempfile::tempdir()?;
    let instance_dir = dir.path().to_path_buf();
    println!("instance dir: {}", instance_dir.display());

    // 1. Start the node. This spawns the four agent threads (consensus, sender,
    //    receiver, archive) and returns immediately.
    let node = Node::start(NodeConfig {
        id: 0,
        members: vec![(0, "127.0.0.1:0".parse()?)],
        bind: "127.0.0.1:0".parse()?,
        instance_dir: instance_dir.clone(),
        app_id: APP_ID.to_string(),
        buffer_bytes: 1 << 22, // 4 MiB log ring
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: 1,
        faults: FaultConfig::default(),
        purge: PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::default(),
    })?;

    // 2. Wait until this node has won its election and appended (and committed)
    //    the NewTerm frame that Raft §5.4.2 requires before a leader may serve.
    //    Even alone, it does not skip that step.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !node.can_serve() {
        anyhow::ensure!(
            Instant::now() < deadline,
            "node never became ready to serve"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    println!("node is leader and serving\n");

    // 3. Attach the state machine. The apply agent starts polling the committed
    //    log and calling CounterSm::apply.
    let _service = ServiceBuilder::new(
        ServiceConfig::new(instance_dir.clone(), APP_ID.to_string()),
        CounterSm::default(),
    )
    .start()?;

    // 4. Connect a client over shared memory and drive it.
    let client = Client::connect(&instance_dir, APP_ID)?;

    for n in [1, 2, 3, 10, -6] {
        let applied: Applied = client.submit(&Command::Add(n))?;
        println!(
            "Add({n:>3}) -> value {:>3}  @ log position {}",
            applied.value, applied.position
        );
    }

    // A linearizable read: the node confirms it is still leader with a quorum
    // round before answering, so this cannot return a stale value even if this
    // node were deposed a microsecond ago.
    let r: QueryResponse = client.query_linearizable(&Query::Value)?;
    println!("\nlinearizable read -> {}", r.value);

    // A snapshot read skips that barrier: cheaper, and may be slightly behind.
    let r: QueryResponse = client.query_snapshot(&Query::Value)?;
    println!("snapshot read     -> {}", r.value);

    let applied: Applied = client.submit(&Command::Reset)?;
    println!(
        "\nReset      -> value {:>3}  @ log position {}",
        applied.value, applied.position
    );

    println!("\nEverything above went through consensus and was fsync'd before it was acked.");
    Ok(())
}
