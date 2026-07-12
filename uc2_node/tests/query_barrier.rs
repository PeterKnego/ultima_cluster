// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 11 barrier capstone: the stale-leader-fails-read-confirmation theorem,
//! pinned live on a real 3-node loopback cluster (harness shaped after
//! `failover.rs`).
//!
//! A linearizable read against a healthy leader completes (the READ_PROBE
//! quorum confirms the read index, the service catches up, the answer returns).
//! Then the leader is partitioned from BOTH followers: its next linearizable
//! read can no longer collect a read-index quorum, so within the barrier
//! deadline it is answered `RETRY`/`NOT_LEADER` — NEVER a stale value. This is
//! the teeth of the no-stale-read guarantee, without waiting for the full
//! `lin_partition` port.

use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use uc2_client::{Client, ClientError};
use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};
use uc2_service::{ServiceBuilder, ServiceConfig, StateMachine};

const APP: &str = "q-barrier";

// ------------------------------------------------------------- state machine

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Cmd {
    Add(u64),
}

#[derive(Default)]
struct CountSm {
    total: u64,
    last_applied: Option<u64>,
}

impl StateMachine for CountSm {
    type Command = Cmd;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, position: u64, cmd: Cmd) -> u64 {
        let Cmd::Add(n) = cmd;
        self.total += n;
        self.last_applied = Some(position);
        self.total
    }
    fn query(&self, _q: ()) -> u64 {
        self.total
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

// ------------------------------------------------------------------ harness

fn make_config(
    id: u32,
    members: Vec<(u32, SocketAddr)>,
    instance_dir: PathBuf,
    seed: u64,
    addr: SocketAddr,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        bind: addr,
        instance_dir,
        app_id: APP.into(),
        buffer_bytes: 1 << 22, // 4 MiB: no wrap within the test
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed,
        faults: FaultConfig::default(),
        purge: uc2_node::PurgePolicy::Disabled,
        journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
    }
}

struct Cluster {
    _dir: tempfile::TempDir,
    dirs: Vec<PathBuf>,
    members: Vec<(u32, SocketAddr)>,
    nodes: Vec<Node>,
}

fn spawn_cluster(n: usize) -> Cluster {
    let dir = tempfile::Builder::new()
        .prefix("uc2-qbarrier-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    // Bind every socket first so the full member map is known before any agent.
    let socks: Vec<UdpSocket> =
        (0..n).map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind")).collect();
    let members: Vec<(u32, SocketAddr)> =
        socks.iter().enumerate().map(|(i, s)| (i as u32, s.local_addr().unwrap())).collect();
    let mut dirs = Vec::with_capacity(n);
    let mut nodes = Vec::with_capacity(n);
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = members[i].1;
        let instance_dir = dir.path().join(format!("n{i}"));
        dirs.push(instance_dir.clone());
        let seed = 0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let cfg = make_config(i as u32, members.clone(), instance_dir, seed, addr);
        nodes.push(Node::start_with_socket(cfg, sock).expect("start"));
    }
    Cluster { _dir: dir, dirs, members, nodes }
}

/// Wait for exactly one serving leader; assert no split-brain throughout.
fn await_single_leader(nodes: &[Node], secs: u64) -> usize {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let serving: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].can_serve()).collect();
        assert!(serving.len() <= 1, "split-brain: nodes {serving:?} all serve");
        if serving.len() == 1 {
            assert!(nodes[serving[0]].is_leader(), "serving node not flagged leader");
            return serving[0];
        }
        assert!(Instant::now() < deadline, "no single leader elected");
        std::thread::yield_now();
    }
}

/// Cut every link between nodes `a` and `b` (both send directions) via their
/// per-socket partition handles.
fn cut(nodes: &[Node], a: usize, b: usize, members: &[(u32, SocketAddr)]) {
    for h in nodes[a].partition_handles() {
        h.block(members[b].1);
    }
    for h in nodes[b].partition_handles() {
        h.block(members[a].1);
    }
}

fn drive_submits(client: &Client, n: u64) {
    for _ in 0..n {
        let _total: u64 = client.submit(&Cmd::Add(1)).expect("submit to serving leader");
    }
}

// --------------------------------------------------------------------- test

#[test]
fn stale_leader_fails_linearizable_read_confirmation() {
    let mut c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 30);
    let leader_dir = c.dirs[leader].clone();

    // A real service + client attached to the current leader.
    let svc = ServiceBuilder::new(ServiceConfig::new(&leader_dir, APP), CountSm::default())
        .start()
        .unwrap();
    let client = Client::connect(&leader_dir, APP).unwrap();

    // Five committed applies, then a HEALTHY linearizable read completes and
    // returns the running total — the READ_PROBE quorum confirmed the read
    // index across a live majority and the service answered.
    drive_submits(&client, 5);
    let healthy: u64 = client.query_linearizable(&()).unwrap();
    assert_eq!(healthy, 5, "a healthy leader's linearizable read must complete");

    // Partition the leader from BOTH followers.
    for f in (0..3).filter(|&i| i != leader) {
        cut(&c.nodes, leader, f, &c.members);
    }

    // The next linearizable read against the now-isolated leader can no longer
    // collect a read-index quorum. Within the barrier deadline it MUST fail
    // (RETRY, or NOT_LEADER if the leader has already stepped down) — never a
    // stale value. This is the stale-leader-fails-read-confirmation theorem.
    let started = Instant::now();
    let res: Result<u64, ClientError> = client.query_linearizable(&());
    let elapsed = started.elapsed();
    assert!(
        matches!(res, Err(ClientError::Retry) | Err(ClientError::NotLeader { .. })),
        "isolated leader answered a linearizable read (got {res:?}) — stale-read guarantee broken"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "read-confirmation failure took {elapsed:?} — well past the ~1s barrier deadline"
    );

    client.shutdown();
    svc.stop();
    for n in c.nodes.drain(..) {
        n.stop();
    }
}
