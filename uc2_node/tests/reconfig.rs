// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M7 Task 7 — the admin path reachable end to end on a real cluster: writing
//! an admin request directly into a node's cnc page (the `uc2ctl` codepath
//! minus the bin binary itself) drives `propose_config` -> `append_config_frame`
//! -> `Action::ConfigAdopted` on the leader, and forwards through kind 16/17
//! when the request lands on a follower's cnc page instead.
//!
//! Sizing mirrors `failover.rs`/`learner.rs` (journals on ext4 under
//! `CARGO_TARGET_TMPDIR`, 4 MiB no-wrap ring, 150-300 ms election timeouts,
//! whole-box serialization).

use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use uc2_consensus::election::NodeId;
use uc2_log::cnc::{AdminReq, AdminResp, CncPage};
use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};
use uc_protocol::v2::cnc::{CNC_MAX_PEER_SLOTS, CNC_PEER_ROLE_LEARNER};

const APP: &str = "reconfig";

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn serialize() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

struct NodeH {
    id: NodeId,
    instance_dir: PathBuf,
    node: Node,
}

fn make_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    addr: SocketAddr,
    instance_dir: PathBuf,
    seed: u64,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        learners: Vec::new(),
        bind: addr,
        instance_dir,
        app_id: APP.into(),
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
    nodes: Vec<NodeH>,
}

/// Bind every voter's socket first (so the full member map is known up front),
/// then start each node. No learners at boot — this test's whole point is
/// adding one live.
fn spawn_cluster(n: usize) -> Cluster {
    let dir = tempfile::Builder::new()
        .prefix("uc2-reconfig-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");

    let socks: Vec<UdpSocket> =
        (0..n).map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind")).collect();
    let members: Vec<(NodeId, SocketAddr)> =
        socks.iter().enumerate().map(|(i, s)| (i as NodeId, s.local_addr().unwrap())).collect();

    let mut nodes = Vec::with_capacity(n);
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = members[i].1;
        let instance_dir = dir.path().join(format!("n{i}"));
        let cfg = make_config(i as NodeId, members.clone(), addr, instance_dir.clone(), seed_for(i));
        let node = Node::start_with_socket(cfg, sock).expect("start");
        nodes.push(NodeH { id: i as NodeId, instance_dir, node });
    }
    Cluster { _dir: dir, nodes }
}

fn deadline_secs(secs: u64) -> Instant {
    Instant::now() + Duration::from_secs(secs)
}

fn await_single_leader(nodes: &[NodeH], secs: u64) -> usize {
    let deadline = deadline_secs(secs);
    loop {
        let serving: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].node.can_serve()).collect();
        assert!(serving.len() <= 1, "split-brain: {serving:?} all serve");
        if serving.len() == 1 {
            let i = serving[0];
            assert!(nodes[i].node.is_leader(), "serving node {i} not flagged leader");
            return i;
        }
        assert!(Instant::now() < deadline, "no single leader elected");
        std::thread::yield_now();
    }
}

/// Open a node's cnc page directly by its instance dir — exactly the
/// `uc2ctl` attach path (`CncPage::open_file` + app_id check), reached here
/// without the bin.
fn open_cnc(instance_dir: &std::path::Path) -> std::sync::Arc<CncPage> {
    CncPage::open_file(&instance_dir.join("cnc2.dat"), APP).expect("open cnc")
}

/// The `uc2ctl` mutating-command flow, minus the bin: read the admin band's
/// current seq, write a fresh request (`seq = old_seq + 1`, a random nonce,
/// the given op/id/addr fields), poll the response line for the echoed seq.
fn admin_request(cnc: &CncPage, op: u32, id: u32, ip: u32, port: u16) -> AdminResp {
    let old_seq = cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0);
    let seq = old_seq + 1;
    let nonce = rand::random::<u64>();
    cnc.write_admin_req(&AdminReq { seq, nonce, op, id, ip, port });
    let deadline = deadline_secs(15);
    loop {
        if let Some(resp) = cnc.read_admin_resp(seq) {
            return resp;
        }
        assert!(Instant::now() < deadline, "admin response timed out for seq {seq}");
        std::thread::yield_now();
    }
}

fn addr_to_wire(addr: SocketAddr) -> (u32, u16) {
    match addr {
        SocketAddr::V4(a) => (u32::from(*a.ip()), a.port()),
        SocketAddr::V6(_) => panic!("this harness only binds IPv4 loopback"),
    }
}

/// Assert every node's cnc-mirrored config version reaches `version` within
/// `secs`, and that `learner_id`'s peer slot shows up with `role=learner` on
/// every node (voters carry every OTHER member's slot, never their own).
fn await_config_converged(nodes: &[NodeH], version: u64, learner_id: NodeId, secs: u64) {
    let deadline = deadline_secs(secs);
    loop {
        if nodes.iter().all(|h| h.node.config_version() >= version) {
            break;
        }
        assert!(Instant::now() < deadline, "config version {version} never converged cluster-wide");
        std::thread::yield_now();
    }
    for h in nodes {
        let cnc = open_cnc(&h.instance_dir);
        let mut found = false;
        for i in 0..CNC_MAX_PEER_SLOTS {
            let raw = cnc.peer_slot(i).id_and_role.load_acquire();
            if raw == 0 {
                continue;
            }
            let id = (raw >> 8) as u32;
            let role = (raw & 0xff) as u8;
            if id == learner_id {
                assert_eq!(
                    role, CNC_PEER_ROLE_LEARNER,
                    "node {}: learner {learner_id}'s peer slot has role byte {role}, want LEARNER",
                    h.id
                );
                found = true;
            }
        }
        assert!(found, "node {}: no peer slot published for learner {learner_id}", h.id);
    }
}

#[test]
fn add_learner_via_leader_cnc_is_accepted_and_converges() {
    let _g = serialize();
    let c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 20);

    let learner_id: NodeId = 10;
    let learner_addr: SocketAddr = "127.0.0.1:59010".parse().unwrap();
    let (ip, port) = addr_to_wire(learner_addr);

    let cnc = open_cnc(&c.nodes[leader].instance_dir);
    let resp = admin_request(&cnc, 1 /* AddLearner */, learner_id, ip, port);
    assert_eq!(resp.status, 0, "add-learner via the leader's own cnc was refused: reason {}", resp.reason);
    assert_eq!(resp.version, 1);

    await_config_converged(&c.nodes, 1, learner_id, 20);

    for h in c.nodes {
        h.node.stop();
    }
}

#[test]
fn add_learner_via_follower_cnc_is_forwarded_and_converges() {
    let _g = serialize();
    let c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 20);
    let follower = (0..c.nodes.len()).find(|&i| i != leader).expect("a follower exists");

    let learner_id: NodeId = 11;
    let learner_addr: SocketAddr = "127.0.0.1:59011".parse().unwrap();
    let (ip, port) = addr_to_wire(learner_addr);

    // Written into the FOLLOWER's cnc admin slot: the follower's own do_work
    // step 11 forwards it to the leader hint as a kind-16 ConfigProposal, and
    // the eventual kind-17 reply is written back to THIS SAME response line —
    // so polling the follower's cnc page (not the leader's) is the right
    // "uc2ctl talked to the wrong node but it still worked" assertion.
    let cnc = open_cnc(&c.nodes[follower].instance_dir);
    let resp = admin_request(&cnc, 1 /* AddLearner */, learner_id, ip, port);
    assert_eq!(
        resp.status, 0,
        "add-learner forwarded via a follower's cnc was refused: reason {}",
        resp.reason
    );
    assert_eq!(resp.version, 1);

    await_config_converged(&c.nodes, 1, learner_id, 20);

    for h in c.nodes {
        h.node.stop();
    }
}
