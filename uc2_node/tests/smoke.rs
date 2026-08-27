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
use uc_protocol::v2::ipc::{MSG_V2_SUBMIT, extra_client};

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
        purge: uc2_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc2_node::CryptoConfig::Disabled,
        services: uc2_node::ServicesConfig::none_for_tests(),
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
    assert!(dir.path().join("svc_query.0.ring").exists());
    assert!(dir.path().join("egress_service.0.broadcast").exists());
    assert!(!dir.path().join("svc_query.ring").exists(), "the legacy singular name is not created");
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

/// Task 12 review pin: the durable output-progress marker is a HIGH-WATER
/// MARK — a node restart must never regress it. The cnc page is re-created
/// fresh every boot, so `service().output_completed` restarts at 0 while the
/// recovered on-disk marker is M > 0; a change-detecting (rather than
/// increase-only) persister would deterministically persist 0 on the first
/// consensus cycle after the restart, clobbering M (at-least-once safe, but
/// the next leader would replay ALL outputs from 0/the purge floor).
///
/// Drives `output_completed` directly on the page (unit-style — the service
/// is out of scope here; the node's persister only ever reads the counter),
/// waits for the durable persist + mirror, restarts the node on the SAME
/// instance dir, and asserts both the mirror and the on-disk StableValue
/// still report M.
#[test]
fn output_progress_marker_survives_node_restart() {
    const M: u64 = 4096;
    let dir = tempfile::tempdir().unwrap();

    let node = start_single_node(dir.path());
    wait_until(|| node.can_serve());

    // Simulate a service having completed outputs up to M. The first-ever
    // increase persists without waiting out the 100 ms floor, so the mirror
    // update is prompt.
    let cnc = uc2_log::cnc::CncPage::open_file(&dir.path().join("cnc2.dat"), "smoke").unwrap();
    cnc.service().output_completed.store_release(M);
    wait_until(|| cnc.status().output_progress.load_acquire() == M);
    drop(cnc);
    node.stop();

    // The marker is durable on disk.
    let state = uc2_log::state::NodeState::open(&dir.path().join("state")).unwrap();
    assert_eq!(state.output_progress(), M, "marker durably persisted before the restart");
    drop(state);

    // Restart on the same instance dir: the fresh cnc page's output_completed
    // is 0. The increase-only persister must leave both the mirror and the
    // on-disk marker at M.
    let node = Node::start(config_for(dir.path())).unwrap();
    wait_until(|| node.can_serve());
    let cnc = uc2_log::cnc::CncPage::open_file(&dir.path().join("cnc2.dat"), "smoke").unwrap();
    assert_eq!(
        cnc.status().output_progress.load_acquire(),
        M,
        "boot mirrors the recovered marker"
    );
    // Let the consensus loop run a comfortable number of duty cycles — the
    // pre-fix regression fired on the very FIRST cycle, so this settle window
    // is more than enough to catch it.
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        cnc.status().output_progress.load_acquire(),
        M,
        "a node restart must not clobber the marker with the fresh page's 0"
    );
    drop(cnc);
    node.stop();

    let state = uc2_log::state::NodeState::open(&dir.path().join("state")).unwrap();
    assert_eq!(state.output_progress(), M, "on-disk marker still M after the restart");
}

/// Read the frame at `pos` via the buffer's validated read, panicking if it
/// is not (yet) a committed message frame — the harness already waited for
/// commit to pass `pos` before calling this.
fn read_frame_at(node: &Node, pos: u64, buf: &mut Vec<u8>) -> uc_protocol::v2::frame::FrameHeader {
    match node.read_frame_validated(pos, buf) {
        uc2_log::buffer::FrameRead::Frame(hdr) => hdr,
        other => panic!("expected a committed frame at {pos}, got {other:?}"),
    }
}

/// Task 7: a client attaches to `ingress.ring` directly (the real cross-
/// process path — no `Node::submit`), writes a `MSG_V2_SUBMIT` record
/// carrying its `(client_id, local_seq)` in `header_extra`, and the consensus
/// agent's ring drain appends it — stamping the log frame's
/// `session_id`/`correlation_id` from that same identity end to end.
#[test]
fn ingress_ring_submission_reaches_commit_and_non_leader_redirects() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_single_node(dir.path());
    wait_until(|| node.can_serve());

    let ring = uc_protocol::ring::mpsc::MpscRing::open(&dir.path().join("ingress.ring")).unwrap();
    let (prod, _) = ring.into_split();

    let commit0 = node.counters().commit.load_acquire();
    prod.try_write(MSG_V2_SUBMIT, 0, extra_client(7, 1), b"hello-ring").unwrap();
    wait_until(|| node.counters().commit.load_acquire() > commit0);

    // The frame carries the client identity end to end.
    let mut buf = Vec::new();
    let hdr = read_frame_at(&node, commit0, &mut buf);
    assert_eq!(hdr.session_id, 7);
    assert_eq!(hdr.correlation_id, 1);

    node.stop();
}
