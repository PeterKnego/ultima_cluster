// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M4 gate: leader-failover measurement (spec §9 GO/NO-GO — sim + partition
//! lincheck green AND sub-second failover with zero committed loss).
//!
//! ```text
//! cargo run -p uc2_node --release --example m4_gate -- <journal_root> [iterations=10]
//! ```
//!
//! Boots a real 3-node cluster over loopback UDP — three [`Node`]s, each with
//! its four polling agents (consensus / sender / receiver / archive) — and
//! loops `iterations` times. Each iteration:
//!
//!   1. await a single serving leader,
//!   2. drive ~2 s of admission-paced load (append − commit ≤ 1 MiB, 64 B
//!      payloads) so the leader is killed with real committed history behind it
//!      AND an uncommitted inflight tail ahead of its commit,
//!   3. record the survivors' committed floor,
//!   4. `crash()` the leader and time (a) kill → a survivor's `can_serve`, and
//!      (b) kill → the first commit-advance past the recorded floor (the new
//!      term's NewTerm frame reaching quorum),
//!   5. assert zero committed loss — every survivor's commit stayed ≥ the
//!      recorded floor,
//!   6. restart the killed node from its SAME dirs on its SAME port; it rejoins
//!      as a follower in the current term (truncating any divergent inflight
//!      tail), and the next iteration proceeds on a healthy 3-node cluster.
//!
//! Summary: failover p50/p90/max for both metrics, truncations observed, terms
//! consumed. Verdict: `PASS` iff every iteration served in < 1 s AND no
//! committed byte was ever lost; exit 1 on FAIL.
//!
//! Journals live on a REAL filesystem: `<journal_root>/n{0,1,2}` — the dev
//! sandbox uses `/home/claude/...`, NEVER `/tmp` (RAM-backed tmpfs). The
//! archive preallocates 64 MiB segments per node.
//!
//! Loopback is REPRESENTATIVE for this gate: failover time is dominated by the
//! 150–300 ms randomized election timeouts, not by the wire (loopback RTT is
//! sub-µs; a real LAN adds only a few hundred µs). The absolute detection
//! timing gets its confirmation on the 3×c6id fleet later.

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use uc2_consensus::election::NodeId;
use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig, SubmitError};

/// Payload every submit carries (64 B → a 96 B on-wire frame after the 32 B
/// header, already 32 B-aligned).
const PAYLOAD: usize = 64;
/// Admission window: append is paced so append − commit ≤ this many bytes.
const ADMISSION_BUDGET: u64 = 1 << 20; // 1 MiB
/// Ring capacity — 256 MiB so a 2 s load burst never wraps within an iteration.
const BUFFER_BYTES: usize = 256 << 20;
/// The sub-second serving budget that defines a PASS.
const SERVE_BUDGET: Duration = Duration::from_secs(1);

// ------------------------------------------------------------- node handle

/// One cluster member: identity/address/dirs plus the live [`Node`] (taken out
/// of the `Option` on crash so the handle survives to be restarted).
struct NodeH {
    id: NodeId,
    addr: SocketAddr,
    instance_dir: PathBuf,
    seed: u64,
    node: Option<Node>,
}

impl NodeH {
    fn n(&self) -> &Node {
        self.node.as_ref().expect("node stopped")
    }
    fn live(&self) -> bool {
        self.node.is_some()
    }
    fn is_leader(&self) -> bool {
        self.n().is_leader()
    }
    fn can_serve(&self) -> bool {
        self.n().can_serve()
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
    fn truncations(&self) -> u64 {
        self.n().truncations()
    }

    fn crash(&mut self) {
        if let Some(node) = self.node.take() {
            node.crash();
        }
    }
    fn stop(&mut self) {
        if let Some(node) = self.node.take() {
            node.stop();
        }
    }

    /// Restart from the SAME dirs on the SAME port (static membership). Recovers
    /// durable state, so it rejoins in the current term and truncates any
    /// divergent inflight tail it had appended before the crash.
    fn restart(&mut self, members: &[(NodeId, SocketAddr)]) {
        assert!(self.node.is_none(), "restart of a live node");
        let sock = rebind(self.addr);
        let cfg = make_config(self.id, members.to_vec(), self.instance_dir.clone(), self.seed, self.addr);
        self.node = Some(Node::start_with_socket(cfg, sock).expect("restart"));
    }
}

/// Bind a fresh UDP socket on a specific loopback address, retrying briefly (the
/// old socket was just dropped; rebinding loopback UDP succeeds ~immediately).
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
    instance_dir: PathBuf,
    seed: u64,
    addr: SocketAddr,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        bind: addr,
        instance_dir,
        app_id: "m4_gate".into(),
        buffer_bytes: BUFFER_BYTES,
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

/// A distinct, index-derived seed so each node's randomized election timeout
/// differs — a clean boot then elects exactly one leader.
fn seed_for(i: usize) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

// ------------------------------------------------------------- the cluster

struct Cluster {
    members: Vec<(NodeId, SocketAddr)>,
    nodes: Vec<NodeH>,
}

/// Bind every node's socket FIRST (so the full member map is known before any
/// agent runs), then start each node on its pre-bound socket.
fn spawn_cluster(root: &Path, n: usize) -> Cluster {
    let socks: Vec<UdpSocket> =
        (0..n).map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind")).collect();
    let members: Vec<(NodeId, SocketAddr)> =
        socks.iter().enumerate().map(|(i, s)| (i as NodeId, s.local_addr().unwrap())).collect();

    let mut nodes = Vec::with_capacity(n);
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = members[i].1;
        let instance_dir = root.join(format!("n{i}"));
        let seed = seed_for(i);
        let cfg = make_config(i as NodeId, members.clone(), instance_dir.clone(), seed, addr);
        let node = Node::start_with_socket(cfg, sock).expect("start");
        nodes.push(NodeH { id: i as NodeId, addr, instance_dir, seed, node: Some(node) });
    }
    Cluster { members, nodes }
}

// ------------------------------------------------------------- wait helpers

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

/// Wait for EXACTLY one serving leader across the live cluster and return its
/// index, asserting no split-brain (never two serving leaders) throughout.
fn await_single_leader(nodes: &[NodeH], secs: u64) -> usize {
    let deadline = deadline_secs(secs);
    loop {
        let serving: Vec<usize> =
            (0..nodes.len()).filter(|&i| nodes[i].live() && nodes[i].can_serve()).collect();
        assert!(serving.len() <= 1, "split-brain: nodes {serving:?} all serve");
        if serving.len() == 1 {
            let i = serving[0];
            assert!(nodes[i].is_leader(), "serving node {i} is not flagged leader");
            return i;
        }
        assert!(Instant::now() < deadline, "no single leader elected");
        std::thread::yield_now();
    }
}

/// Wait until exactly one of `idxs` is serving and return its index (used with
/// the killed leader excluded).
fn await_serving_among(nodes: &[NodeH], idxs: &[usize], secs: u64) -> usize {
    let deadline = deadline_secs(secs);
    loop {
        let serving: Vec<usize> = idxs.iter().copied().filter(|&i| nodes[i].can_serve()).collect();
        assert!(serving.len() <= 1, "split-brain among {idxs:?}: {serving:?} serve");
        if serving.len() == 1 {
            return serving[0];
        }
        assert!(Instant::now() < deadline, "no leader among {idxs:?}");
        std::thread::yield_now();
    }
}

/// Drive `secs` of admission-paced load through `node`: submit 64 B payloads,
/// pacing so append − commit ≤ [`ADMISSION_BUDGET`]. Stops early if the node
/// stops serving (it should not — we only load the current leader). This leaves
/// the leader with real committed history AND an uncommitted inflight tail.
fn drive_load(node: &NodeH, secs: u64) {
    let clock = Instant::now();
    let mut seq = 0u64;
    while clock.elapsed().as_secs() < secs {
        // Admission window vs commit (leader-local counters).
        loop {
            let c = node.n().counters();
            let inflight = c.append.load_acquire().saturating_sub(c.commit.load_acquire());
            if inflight <= ADMISSION_BUDGET {
                break;
            }
            std::thread::yield_now();
        }
        let mut p = vec![0u8; PAYLOAD];
        p[..8].copy_from_slice(&seq.to_le_bytes());
        match node.n().submit(p) {
            Ok(()) => seq += 1,
            Err(SubmitError::Full) => std::thread::yield_now(),
            Err(SubmitError::NotServing) => break,
        }
    }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn ms(ns: u64) -> f64 {
    ns as f64 / 1e6
}

// ------------------------------------------------------------------- main

fn main() {
    let mut args = std::env::args().skip(1);
    let root: PathBuf = args
        .next()
        .expect("usage: m4_gate <journal_root> [iterations=10]")
        .into();
    let iterations: u32 = args.next().map(|s| s.parse().expect("iterations")).unwrap_or(10);
    assert!(
        !root.starts_with("/tmp"),
        "journal_root must be on a real filesystem (never /tmp — RAM tmpfs)"
    );
    std::fs::create_dir_all(&root).expect("create journal_root");

    println!("== uc2 M4 gate (local loopback, 3 nodes) ==");
    println!(
        "{iterations} iterations; {PAYLOAD} B payloads; admission {} MiB; \
         election timeout 150-300 ms; journals under {}",
        ADMISSION_BUDGET >> 20,
        root.display()
    );

    let mut c = spawn_cluster(&root, 3);
    let members = c.members.clone();

    let mut serve_ns: Vec<u64> = Vec::with_capacity(iterations as usize);
    let mut advance_ns: Vec<u64> = Vec::with_capacity(iterations as usize);
    let mut zero_loss = true;
    let mut worst_serve = Duration::ZERO;
    let first_term;

    {
        let l0 = await_single_leader(&c.nodes, 30);
        first_term = c.nodes[l0].term();
    }
    let mut last_term = first_term;

    for it in 0..iterations {
        let leader = await_single_leader(&c.nodes, 30);
        let old_term = c.nodes[leader].term();

        // ~2 s of admission-paced load so the kill lands on a live, committing
        // leader with an inflight tail beyond its commit.
        drive_load(&c.nodes[leader], 2);

        let survivors: Vec<usize> = (0..3).filter(|&i| i != leader).collect();
        // Committed floor a survivor observed just before the kill — nothing at
        // or below this may ever be lost.
        let floor = c.nodes[survivors[0]].commit().min(c.nodes[survivors[1]].commit());
        let leader_append = c.nodes[leader].append();
        let leader_commit = c.nodes[leader].commit();

        // Kill and time the failover from one instant.
        let t0 = Instant::now();
        c.nodes[leader].crash();

        // (a) kill -> a survivor can serve.
        let new_leader = await_serving_among(&c.nodes, &survivors, 30);
        let serve = t0.elapsed();

        // (b) kill -> first commit-advance past the recorded floor (the new
        // term's NewTerm frame reaching quorum).
        await_until(30, "no commit advanced past the recorded floor", || {
            survivors.iter().any(|&i| c.nodes[i].commit() > floor)
        });
        let advance = t0.elapsed();
        let new_term = c.nodes[new_leader].term();

        // Zero committed loss: every survivor still holds the pre-kill floor.
        let mut lost = false;
        for &i in &survivors {
            if c.nodes[i].commit() < floor {
                lost = true;
            }
        }
        zero_loss &= !lost;
        if serve > worst_serve {
            worst_serve = serve;
        }
        serve_ns.push(serve.as_nanos() as u64);
        advance_ns.push(advance.as_nanos() as u64);

        println!(
            "iter {it:>2}: leader n{leader} (t{old_term}, commit {leader_commit}, \
             inflight {} B) -> n{new_leader} (t{new_term})  serve {:.1} ms  advance {:.1} ms  \
             floor {floor}  {}",
            leader_append.saturating_sub(leader_commit),
            serve.as_secs_f64() * 1e3,
            advance.as_secs_f64() * 1e3,
            if lost { "LOST-COMMITTED-DATA" } else { "no-loss" },
        );

        last_term = new_term;

        // Restart the killed node from its dirs; it rejoins as a follower in the
        // current term (truncating any divergent inflight tail).
        c.nodes[leader].restart(&members);
        await_until(30, "restarted node did not adopt the current term", || {
            c.nodes[leader].live() && c.nodes[leader].term() >= new_term
        });
    }

    let total_truncations: u64 = (0..3).map(|i| c.nodes[i].truncations()).sum();
    let terms_consumed = last_term.saturating_sub(first_term);

    for node in &mut c.nodes {
        node.stop();
    }

    serve_ns.sort_unstable();
    advance_ns.sort_unstable();
    println!("== uc2 M4 gate ==");
    println!(
        "kill->serve          p50 {:.1} ms  p90 {:.1} ms  max {:.1} ms",
        ms(percentile(&serve_ns, 0.50)),
        ms(percentile(&serve_ns, 0.90)),
        ms(*serve_ns.last().unwrap()),
    );
    println!(
        "kill->commit-advance p50 {:.1} ms  p90 {:.1} ms  max {:.1} ms",
        ms(percentile(&advance_ns, 0.50)),
        ms(percentile(&advance_ns, 0.90)),
        ms(*advance_ns.last().unwrap()),
    );
    println!("truncations observed {total_truncations}  (deposed leaders cut divergent tails)");
    println!("terms consumed       {terms_consumed}  (t{first_term} -> t{last_term})");
    println!("committed loss       {}", if zero_loss { "none" } else { "DETECTED" });

    let pass = zero_loss && worst_serve < SERVE_BUDGET;
    println!(
        "GATE (sub-second failover, no committed loss): {}",
        if pass { "PASS" } else { "FAIL" }
    );
    if !pass {
        std::process::exit(1);
    }
}
