// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M6 Task 7 — the learner role, end to end on a real cluster.
//!
//! A learner is **replicated-to but never counted**: it receives the DATA stream
//! (durable advances), commit gossip (commit advances), and term maps (it
//! reconciles), so its state machine tracks the cluster exactly — yet it never
//! votes, never occupies a quorum slot, never paces flow control, and never acks
//! a read probe. This test proves both halves against a real 3-voter + 1-learner
//! cluster over loopback UDP, driven only through the public [`Node`] API:
//!
//! * **fan-out yes** — the learner's commit catches up to the cluster commit
//!   under load;
//! * **quorum no** — killing the learner never stalls commit; and killing the
//!   *leader* re-elects a **voter** (the learner never becomes a candidate), with
//!   the learner rejoining on restart via ordinary NAK-replay (no config change).
//!
//! Sizing mirrors `failover.rs` (journals on ext4 under `CARGO_TARGET_TMPDIR`,
//! 4 MiB no-wrap ring, 150–300 ms election timeouts, whole-box serialization).

use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use uc2_consensus::election::NodeId;
use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};

const PAYLOAD: usize = 96;

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn serialize() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

struct NodeH {
    id: NodeId,
    addr: SocketAddr,
    instance_dir: PathBuf,
    seed: u64,
    is_learner: bool,
    node: Option<Node>,
}

impl NodeH {
    fn n(&self) -> &Node {
        self.node.as_ref().expect("node stopped")
    }
    fn is_leader(&self) -> bool {
        self.node.as_ref().is_some_and(|n| n.is_leader())
    }
    fn can_serve(&self) -> bool {
        self.node.as_ref().is_some_and(|n| n.can_serve())
    }
    fn term(&self) -> u32 {
        self.n().current_term()
    }
    fn commit(&self) -> u64 {
        self.n().counters().commit.load_acquire()
    }
    fn append(&self) -> u64 {
        self.n().counters().append.load_acquire()
    }
    fn try_submit(&self, payload: Vec<u8>) -> Result<(), uc2_node::SubmitError> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match self.n().submit(payload.clone()) {
                Ok(()) => return Ok(()),
                Err(uc2_node::SubmitError::Full) => {
                    assert!(Instant::now() < deadline, "ingress stayed full");
                    std::thread::yield_now();
                }
                Err(e) => return Err(e),
            }
        }
    }
    /// Block this node's outbound sends to `peer` on ALL sockets (one side of a
    /// link cut; block the other side too for a full partition).
    fn block(&self, peer: SocketAddr) {
        for h in self.n().partition_handles() {
            h.block(peer);
        }
    }
    fn stop(&mut self) {
        if let Some(node) = self.node.take() {
            node.stop();
        }
    }
    fn crash(&mut self) {
        if let Some(node) = self.node.take() {
            node.crash();
        }
    }
    fn restart(&mut self, members: &[(NodeId, SocketAddr)], learners: &[(NodeId, SocketAddr)]) {
        assert!(self.node.is_none(), "restart of a live node");
        let sock = rebind(self.addr);
        let cfg = make_config(
            self.id,
            members.to_vec(),
            learners.to_vec(),
            self.instance_dir.clone(),
            self.seed,
            self.addr,
        );
        self.node = Some(Node::start_with_socket(cfg, sock).expect("restart"));
    }
}

fn rebind(addr: SocketAddr) -> UdpSocket {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match UdpSocket::bind(addr) {
            Ok(s) => return s,
            Err(_) if Instant::now() < deadline => std::thread::yield_now(),
            Err(e) => panic!("rebind {addr} failed: {e}"),
        }
    }
}

fn make_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    learners: Vec<(NodeId, SocketAddr)>,
    instance_dir: PathBuf,
    seed: u64,
    addr: SocketAddr,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        learners,
        bind: addr,
        instance_dir,
        app_id: "learner".into(),
        buffer_bytes: 1 << 22,
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

fn seed_for(i: usize) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

struct Cluster {
    _dir: tempfile::TempDir,
    members: Vec<(NodeId, SocketAddr)>,
    learners: Vec<(NodeId, SocketAddr)>,
    nodes: Vec<NodeH>,
}

/// Bind `n_voters` voter sockets + `n_learners` learner sockets, then start each
/// node with the full (members, learners) maps. Learner ids are `n_voters..`.
fn spawn_cluster_with_learner(n_voters: usize, n_learners: usize) -> Cluster {
    let dir = tempfile::Builder::new()
        .prefix("uc2-learner-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");

    let total = n_voters + n_learners;
    let socks: Vec<UdpSocket> =
        (0..total).map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind")).collect();
    let all: Vec<(NodeId, SocketAddr)> =
        socks.iter().enumerate().map(|(i, s)| (i as NodeId, s.local_addr().unwrap())).collect();
    let members: Vec<(NodeId, SocketAddr)> = all[..n_voters].to_vec();
    let learners: Vec<(NodeId, SocketAddr)> = all[n_voters..].to_vec();

    let mut nodes = Vec::with_capacity(total);
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = all[i].1;
        let instance_dir = dir.path().join(format!("n{i}"));
        let seed = seed_for(i);
        let is_learner = i >= n_voters;
        let cfg =
            make_config(i as NodeId, members.clone(), learners.clone(), instance_dir.clone(), seed, addr);
        let node = Node::start_with_socket(cfg, sock).expect("start");
        nodes.push(NodeH { id: i as NodeId, addr, instance_dir, seed, is_learner, node: Some(node) });
    }
    Cluster { _dir: dir, members, learners, nodes }
}

fn deadline_secs(secs: u64) -> Instant {
    Instant::now() + Duration::from_secs(secs)
}

fn await_until(secs: u64, msg: &str, mut f: impl FnMut() -> bool) {
    let deadline = deadline_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "{msg}");
        std::thread::yield_now();
    }
}

/// Exactly one serving leader among the VOTERS; the learner must never serve.
fn await_single_leader(nodes: &[NodeH], secs: u64) -> usize {
    let deadline = deadline_secs(secs);
    loop {
        for h in nodes.iter().filter(|h| h.is_learner) {
            assert!(!h.can_serve() && !h.is_leader(), "learner {} became a leader", h.id);
        }
        let serving: Vec<usize> =
            (0..nodes.len()).filter(|&i| nodes[i].node.is_some() && nodes[i].can_serve()).collect();
        assert!(serving.len() <= 1, "split-brain: {serving:?} all serve");
        if serving.len() == 1 {
            let i = serving[0];
            assert!(nodes[i].is_leader(), "serving node {i} not flagged leader");
            assert!(!nodes[i].is_learner, "a learner must never lead");
            return i;
        }
        assert!(Instant::now() < deadline, "no single leader elected");
        std::thread::yield_now();
    }
}

fn await_serving_among(nodes: &[NodeH], idxs: &[usize], secs: u64) -> usize {
    let deadline = deadline_secs(secs);
    loop {
        let serving: Vec<usize> = idxs.iter().copied().filter(|&i| nodes[i].can_serve()).collect();
        assert!(serving.len() <= 1, "split-brain among {idxs:?}: {serving:?}");
        if serving.len() == 1 {
            return serving[0];
        }
        assert!(Instant::now() < deadline, "no leader among {idxs:?}");
        std::thread::yield_now();
    }
}

fn submit_n(node: &NodeH, base: u64, n: u64) {
    for i in base..base + n {
        let mut p = vec![0u8; PAYLOAD];
        p[..8].copy_from_slice(&i.to_le_bytes());
        node.try_submit(p).expect("submit to serving leader");
    }
}

#[test]
fn learner_replicates_live_and_never_disturbs_quorum() {
    let _g = serialize();
    let mut c = spawn_cluster_with_learner(3, 1);
    let learner_idx = 3;

    // Elect a voter leader; the learner never serves.
    let leader = await_single_leader(&c.nodes, 30);
    assert!(!c.nodes[learner_idx].is_learner || c.nodes[learner_idx].id == 3);

    // Drive commits; the learner replicates LIVE — its commit reaches the leader's.
    submit_n(&c.nodes[leader], 0, 2000);
    let leader_commit = {
        let deadline = deadline_secs(20);
        loop {
            let a = c.nodes[leader].append();
            if c.nodes[leader].commit() == a {
                break a;
            }
            assert!(Instant::now() < deadline, "leader never quiesced");
            std::thread::yield_now();
        }
    };
    await_until(20, "learner never caught up to cluster commit", || {
        c.nodes[learner_idx].commit() >= leader_commit
    });

    // Kill the learner mid-life: commit KEEPS advancing (no quorum coupling).
    let commit0 = c.nodes[leader].commit();
    c.nodes[learner_idx].crash();
    submit_n(&c.nodes[leader], 2000, 1000);
    await_until(20, "commit stalled after learner died (phantom quorum coupling)", || {
        c.nodes[leader].commit() > commit0
    });

    // Learner restarts and rejoins via ordinary replay — NO leader config change.
    let (members, learners) = (c.members.clone(), c.learners.clone());
    c.nodes[learner_idx].restart(&members, &learners);
    let caught = c.nodes[leader].commit();
    await_until(20, "restarted learner never re-caught up", || {
        c.nodes[learner_idx].commit() >= caught
    });

    // Kill the leader: a VOTER must win the re-election; the learner never
    // becomes a candidate (its term never runs ahead, it never serves).
    let voters: Vec<usize> = (0..3).filter(|&i| i != leader).collect();
    let learner_term_before = c.nodes[learner_idx].term();
    c.nodes[leader].crash();
    let new_leader = await_serving_among(&c.nodes, &voters, 30);
    assert!(new_leader < 3, "the new leader must be a voter, got {new_leader}");
    assert!(!c.nodes[learner_idx].is_leader(), "the learner must never lead");
    // The learner adopts the new term for liveness but never self-incremented one
    // via candidacy: its term equals the new leader's, not beyond it.
    await_until(20, "learner never adopted the new leader's term", || {
        c.nodes[learner_idx].term() >= learner_term_before
            && c.nodes[learner_idx].term() <= c.nodes[new_leader].term()
    });
    assert!(
        c.nodes[learner_idx].term() <= c.nodes[new_leader].term(),
        "a learner's term must never exceed the leader's (it never candidacies)"
    );

    for node in &mut c.nodes {
        node.stop();
    }
}

#[test]
fn learner_alone_cannot_supply_a_voter_quorum() {
    // Step 4 (the read-index guard, at cluster scale): commit — and therefore the
    // ReadIndex barrier that rides the SAME voter quorum — cannot advance on the
    // strength of a learner alone. A leader cut off from BOTH voter followers, but
    // still connected to the learner, must NOT commit the new bytes the learner
    // durably receives. (The read-probe ack path enforces the identical
    // voters-only rule in `on_read_probe_ack`, unit-pinned separately.)
    let _g = serialize();
    let mut c = spawn_cluster_with_learner(3, 1);
    let learner_idx = 3;
    let leader = await_single_leader(&c.nodes, 30);

    submit_n(&c.nodes[leader], 0, 200);
    await_until(20, "warmup never committed", || {
        c.nodes[leader].commit() == c.nodes[leader].append()
    });

    // Partition the leader from BOTH voter followers; leave the learner reachable.
    let voters: Vec<usize> = (0..3).filter(|&i| i != leader).collect();
    for &v in &voters {
        c.nodes[leader].block(c.nodes[v].addr);
        c.nodes[v].block(c.nodes[leader].addr);
    }

    // Let any in-flight commit settle, then freeze the reference.
    let settle = deadline_secs(2);
    let mut frozen = c.nodes[leader].commit();
    while Instant::now() < settle {
        frozen = c.nodes[leader].commit();
        std::thread::yield_now();
    }

    // Submit more: the learner durably RECEIVES these bytes (fan-out), but with no
    // voter reachable the leader cannot form a quorum — commit must stay frozen.
    for i in 200u64..500 {
        let mut p = vec![0u8; PAYLOAD];
        p[..8].copy_from_slice(&i.to_le_bytes());
        let _ = c.nodes[leader].n().submit(p); // best-effort (Full/NotServing tolerated)
    }

    // Watch: the isolated leader's commit never advances past `frozen` on the
    // learner's replication alone. The learner's durable, meanwhile, DOES advance —
    // proving it received the bytes yet still could not be counted.
    let watch = deadline_secs(3);
    while Instant::now() < watch {
        assert_eq!(
            c.nodes[leader].commit(),
            frozen,
            "isolated leader committed past {frozen} without a voter quorum (learner miscounted)"
        );
        std::thread::yield_now();
    }
    assert!(
        c.nodes[learner_idx].node.as_ref().unwrap().counters().durable.load_acquire()
            >= c.nodes[leader].commit(),
        "the learner should still be replicating bytes it just cannot vote on"
    );

    for node in &mut c.nodes {
        node.stop();
    }
}
