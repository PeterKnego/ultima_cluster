// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Single-node composition smoke test (plan §Task 8): one node elects itself,
//! opens its term, commits the NewTerm frame, serves, and drives commit forward
//! through `submit`. Pins the whole action-execution loop with no peers.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig, NodeDirs};

#[test]
fn single_node_cluster_elects_itself_and_serves() {
    let dir = tempfile::tempdir().unwrap();
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    // bind first to learn the addr, then start
    let sock = std::net::UdpSocket::bind(bind).unwrap();
    let addr = sock.local_addr().unwrap();
    drop(sock); // Node::start rebinds; races are a harness non-issue locally
    let node = Node::start(NodeConfig {
        id: 0,
        members: vec![(0, addr)],
        bind: addr,
        dirs: NodeDirs { journal: dir.path().join("j"), state: dir.path().join("s") },
        buffer_bytes: 1 << 20,
        max_payload: 256,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: FaultConfig::default(),
    })
    .unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    while !node.can_serve() {
        assert!(Instant::now() < deadline, "single node never elected itself");
        std::thread::yield_now();
    }
    assert!(node.is_leader());
    assert_eq!(node.current_term(), 1);

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
    let vote = uc2_log::state::NodeState::open(&dir.path().join("s")).unwrap().vote();
    assert_eq!(vote, Some(uc2_log::state::VoteRecord { term: 1, voted_for: 0 }));
}
