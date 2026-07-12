// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Single-node composition smoke test (plan §Task 8): one node elects itself,
//! opens its term, commits the NewTerm frame, serves, and drives commit forward
//! through `submit`. Pins the whole action-execution loop with no peers.
//!
//! M5 Task 5 adds `instance_dir_lock_and_cnc_publication`: the node takes an
//! exclusive `instance.lock` (a second node on the same dir is refused), creates
//! the cnc v2 page + the shared-memory ring files, and publishes its
//! term/flags/commit onto the page for cross-process attachers.

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};

/// A single-member config over an ephemeral loopback port. The member addr is a
/// placeholder — a one-node cluster elects itself with no peer sends, so the
/// address is never used, and the flock refusal (the second-start case) fires
/// before any election.
fn config_for(dir: &Path) -> NodeConfig {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        bind,
        instance_dir: dir.to_path_buf(),
        app_id: "smoke".into(),
        buffer_bytes: 1 << 20,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: FaultConfig::default(),
    }
}

fn start_single_node(dir: &Path) -> Node {
    Node::start(config_for(dir)).unwrap()
}

fn wait_until(mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "condition never held");
        std::thread::yield_now();
    }
}

#[test]
fn instance_dir_lock_and_cnc_publication() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_single_node(dir.path());

    // Exclusive instance lock: a second node on the SAME dir must refuse. The
    // ephemeral rebind may also collide (AddrInUse) — both are acceptable
    // refusals; the flock is the load-bearing one.
    match Node::start(config_for(dir.path())) {
        Ok(_) => panic!("second node on the same instance dir must be refused"),
        Err(e) => assert!(
            e.kind() == std::io::ErrorKind::AddrInUse || format!("{e}").contains("AlreadyRunning"),
            "unexpected refusal error: {e} (kind {:?})",
            e.kind()
        ),
    }

    wait_until(|| node.can_serve());

    // Attach to the published cnc page the way the service/clients will
    // (cross-process, by path + app_id) and read the node's live status.
    let cnc = uc2_log::cnc::CncPage::open_file(&dir.path().join("cnc2.dat"), "smoke").unwrap();
    assert_eq!(
        cnc.status().flags.load_acquire() & uc_protocol::v2::cnc::NODE_FLAG_CAN_SERVE,
        uc_protocol::v2::cnc::NODE_FLAG_CAN_SERVE
    );
    assert_eq!(
        cnc.status().flags.load_acquire() & uc_protocol::v2::cnc::NODE_FLAG_LEADER,
        uc_protocol::v2::cnc::NODE_FLAG_LEADER
    );
    assert_eq!(cnc.status().term.load_acquire(), 1);
    // can_serve implies the 32-byte NewTerm frame committed (§5.4.2).
    assert!(cnc.counters().commit.load_acquire() >= 32);

    // The shared-memory ring files exist (created fresh at boot).
    assert!(dir.path().join("ingress.ring").exists());
    assert!(dir.path().join("query.ring").exists());
    assert!(dir.path().join("svc_query.ring").exists());
    assert!(dir.path().join("egress_service.broadcast").exists());
    assert!(dir.path().join("egress_node.broadcast").exists());

    node.stop();
}

#[test]
fn single_node_cluster_elects_itself_and_serves() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_single_node(dir.path());

    let deadline = Instant::now() + Duration::from_secs(10);
    while !node.can_serve() {
        assert!(Instant::now() < deadline, "single node never elected itself");
        std::thread::yield_now();
    }
    assert!(node.is_leader());
    assert_eq!(node.current_term(), 1);
    // Raft §5.4.2 pin (one-sided): can_serve implies the NewTerm frame is
    // COMMITTED — commit must already cover the 32-byte frame's END. Pre-fix
    // (NewTermAppended fed the frame START) can_serve flipped at commit >= 0,
    // i.e. instantly, typically observed here before the archive's fsync.
    assert!(
        node.counters().commit.load_acquire() >= 32,
        "serving before the NewTerm frame committed (§5.4.2 gate defeated)"
    );

    for i in 0..100u64 {
        node.submit(vec![i as u8; 64]).unwrap();
    }

    let end = {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let c = node.counters();
            let (a, k) = (c.append.load_acquire(), c.commit.load_acquire());
            if k == a && k > 32 {
                break k;
            }
            assert!(Instant::now() < deadline, "commit never caught append");
            std::thread::yield_now();
        }
    };
    assert!(end > 32); // NewTerm frame (32 B) + data
    node.stop();

    // Obligation 1 pin (persist ordering): the self-vote landed durably — a
    // prerequisite for BecomeLeader, whose term-map store runs strictly AFTER
    // the self-vote's durable store, which is the prerequisite for serving. If
    // the vote had not been persisted first, neither the term map nor the
    // committed NewTerm frame (both observed above) could exist.
    let vote = uc2_log::state::NodeState::open(&dir.path().join("state")).unwrap().vote();
    assert_eq!(vote, Some(uc2_log::state::VoteRecord { term: 1, voted_for: 0 }));
}
