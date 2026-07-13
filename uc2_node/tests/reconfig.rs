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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use uc2_consensus::election::NodeId;
use uc2_log::cnc::{AdminReq, AdminResp, CncPage};
use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};
use uc_protocol::v2::cnc::{
    CNC_MAX_PEER_SLOTS, CNC_PEER_ROLE_LEARNER, NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER,
};
use uc_protocol::v2::datagram::{
    DATAGRAM_HEADER_LEN, DGRAM_KIND_APPEND_POSITION, DGRAM_KIND_REQUEST_VOTE, DatagramHeader,
    RequestVoteBody, write_datagram_header, write_request_vote_body,
};

const APP: &str = "reconfig";

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn serialize() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

struct NodeH {
    id: NodeId,
    addr: SocketAddr,
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
    dir_path: PathBuf,
    members: Vec<(NodeId, SocketAddr)>,
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
    let dir_path = dir.path().to_path_buf();

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
        nodes.push(NodeH { id: i as NodeId, addr, instance_dir, node });
    }
    Cluster { _dir: dir, dir_path, members, nodes }
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
/// `secs`, and that `member_id`'s peer slot shows up with role `want_role` on
/// every OTHER node (voters carry every OTHER member's slot, never their own —
/// so `member_id` itself is skipped when it is present in `nodes`).
fn await_config_converged(nodes: &[NodeH], version: u64, member_id: NodeId, want_role: u8, secs: u64) {
    let deadline = deadline_secs(secs);
    loop {
        if nodes.iter().all(|h| h.node.config_version() >= version) {
            break;
        }
        assert!(Instant::now() < deadline, "config version {version} never converged cluster-wide");
        std::thread::yield_now();
    }
    for h in nodes.iter().filter(|h| h.id != member_id) {
        let cnc = open_cnc(&h.instance_dir);
        let mut found = false;
        for i in 0..CNC_MAX_PEER_SLOTS {
            let raw = cnc.peer_slot(i).id_and_role.load_acquire();
            if raw == 0 {
                continue;
            }
            let id = (raw >> 8) as u32;
            let role = (raw & 0xff) as u8;
            if id == member_id {
                assert_eq!(
                    role, want_role,
                    "node {}: member {member_id}'s peer slot has role byte {role}, want {want_role}",
                    h.id
                );
                found = true;
            }
        }
        assert!(found, "node {}: no peer slot published for member {member_id}", h.id);
    }
}

/// Retry an admin op until accepted, tolerating the transient refusals a live
/// cluster legitimately produces — `status=2` ("retry": no leader hint known
/// yet, or the append ring was momentarily full) and the `NotServing` /
/// `ChangePending` / `NotCaughtUp` structural-but-transient refusals (reason
/// codes 2/3/10 — see `uc2_consensus::config::ClusterConfig::reason_code`).
/// Any OTHER refusal panics: this harness's ops are all legal, so a different
/// reason is a real bug, not something to paper over with a retry.
fn admin_request_ok(cnc: &CncPage, op: u32, id: u32, ip: u32, port: u16, secs: u64) -> AdminResp {
    let deadline = deadline_secs(secs);
    loop {
        let resp = admin_request(cnc, op, id, ip, port);
        match resp.status {
            0 => return resp,
            2 => {}
            1 if matches!(resp.reason, 2 | 3 | 10) => {}
            _ => panic!(
                "admin op {op} on id {id} refused: status={} reason={}",
                resp.status, resp.reason
            ),
        }
        assert!(
            Instant::now() < deadline,
            "admin op {op} on id {id} never accepted (last status={} reason={})",
            resp.status,
            resp.reason
        );
        std::thread::sleep(Duration::from_millis(20));
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

    await_config_converged(&c.nodes, 1, learner_id, CNC_PEER_ROLE_LEARNER, 20);

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

    await_config_converged(&c.nodes, 1, learner_id, CNC_PEER_ROLE_LEARNER, 20);

    for h in c.nodes {
        h.node.stop();
    }
}

// ------------------------------------------------------------ M7 Task 8

/// M7 Task 8 — leader self-removal end to end on a real cluster: a serving
/// leader removes ITSELF via its own admin cnc slot while a write-load thread
/// keeps submitting. The change is accepted; the old leader's cnc drops
/// LEADER + CAN_SERVE; a NEW leader (one of the two survivors) emerges and
/// serves; the cluster's committed high-water never regresses across the
/// handoff; the whole gap is bounded (well under 5s at this local-process
/// election-timeout scale).
#[test]
fn leader_self_removal_hands_off() {
    let _g = serialize();
    let c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 20);
    let leader_id = c.nodes[leader].id;

    let stop = AtomicBool::new(false);
    let high_water = AtomicU64::new(0);
    let regressed = AtomicBool::new(false);

    // `thread::scope` joins every spawned thread before returning OR
    // propagating a panic from the closure below — so if the write-load
    // thread's `while !stop.load() {..}` loop never sees `stop` flip, an
    // assertion failure anywhere below would hang forever joining it instead
    // of failing cleanly. This guard sets `stop` on unwind too (Drop runs
    // during the closure's own stack unwind, strictly before `scope`'s join).
    struct StopOnDrop<'a>(&'a AtomicBool);
    impl Drop for StopOnDrop<'_> {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    std::thread::scope(|scope| {
        let _stop_guard = StopOnDrop(&stop);

        // Write-load thread: submit continuously to whichever node currently
        // reports itself a serving leader, and separately track the cluster's
        // committed high-water (max over every node's own commit counter,
        // which is individually monotonic) to catch any handoff-induced
        // regression.
        scope.spawn(|| {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if let Some(h) = c.nodes.iter().find(|h| h.node.can_serve()) {
                    let mut p = vec![0u8; 64];
                    p[..8].copy_from_slice(&i.to_le_bytes());
                    let _ = h.node.submit(p);
                    i += 1;
                }
                let cur =
                    c.nodes.iter().map(|h| h.node.counters().commit.load_acquire()).max().unwrap_or(0);
                let prev = high_water.fetch_max(cur, Ordering::Relaxed);
                if cur < prev {
                    regressed.store(true, Ordering::Relaxed);
                }
                std::thread::yield_now();
            }
        });

        let start = Instant::now();

        // Remove the leader via ITS OWN admin cnc slot.
        let cnc = open_cnc(&c.nodes[leader].instance_dir);
        let resp = admin_request(&cnc, 5 /* RemoveVoter */, leader_id, 0, 0);
        assert_eq!(resp.status, 0, "leader self-removal refused: reason {}", resp.reason);
        assert_eq!(resp.version, 1);

        // The old leader's cnc drops LEADER + CAN_SERVE once its own removal
        // commits (Task 8's step-down, not the (already-true) adoption-time
        // follower halt — this IS the leader path).
        let deadline = deadline_secs(5);
        loop {
            let flags = cnc.status().flags.load_acquire();
            if flags & (NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE) == 0 {
                break;
            }
            assert!(Instant::now() < deadline, "old leader never dropped LEADER/CAN_SERVE");
            std::thread::yield_now();
        }
        assert!(!c.nodes[leader].node.is_leader());
        assert!(!c.nodes[leader].node.can_serve());

        // A NEW leader — one of the two survivors — emerges and serves.
        let survivors: Vec<usize> = (0..c.nodes.len()).filter(|&i| i != leader).collect();
        let new_leader = {
            let deadline = deadline_secs(5);
            loop {
                let serving: Vec<usize> =
                    survivors.iter().copied().filter(|&i| c.nodes[i].node.can_serve()).collect();
                assert!(serving.len() <= 1, "split-brain among survivors: {serving:?}");
                if serving.len() == 1 {
                    break serving[0];
                }
                assert!(Instant::now() < deadline, "no new leader emerged among the survivors");
                std::thread::yield_now();
            }
        };
        assert!(c.nodes[new_leader].node.is_leader());
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "self-removal-to-new-leader gap exceeded 5s: {:?}",
            start.elapsed()
        );

        // The new leader keeps committing.
        let before = c.nodes[new_leader].node.counters().commit.load_acquire();
        let deadline = deadline_secs(10);
        loop {
            if c.nodes[new_leader].node.counters().commit.load_acquire() > before {
                break;
            }
            assert!(Instant::now() < deadline, "the new leader never advanced commit");
            std::thread::yield_now();
        }

        stop.store(true, Ordering::Relaxed);
    });

    assert!(!regressed.load(Ordering::Relaxed), "committed high-water regressed across the handoff");

    for h in c.nodes {
        h.node.stop();
    }
}

/// M7 Task 8 — a removed (live) follower fail-stops (heartbeat freezes; it
/// never regains LEADER/CAN_SERVE); the survivors' `current_term` then stays
/// stable for 2s even under forged high-term RequestVote/Report datagrams
/// sent from a raw socket rebound to the removed node's now-freed address
/// ("its dead identity") — the removed member no longer maps to any known id
/// in the survivors' peer tables (or the SM's own membership check would drop
/// it too), so a zombie impersonating that address cannot disrupt.
#[test]
fn removed_follower_halts_and_zombie_cannot_disrupt() {
    let _g = serialize();
    let mut c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 20);
    let target = (0..c.nodes.len()).find(|&i| i != leader).expect("a follower exists");
    let target_id = c.nodes[target].id;
    let target_addr = c.nodes[target].addr;

    let leader_cnc = open_cnc(&c.nodes[leader].instance_dir);
    let resp = admin_request(&leader_cnc, 5 /* RemoveVoter */, target_id, 0, 0);
    assert_eq!(resp.status, 0, "follower removal refused: reason {}", resp.reason);
    assert_eq!(resp.version, 1);

    // It adopts its own removal (config_version reaches 1) — the same event
    // that, for a non-leader, emits `Action::HaltRemoved` synchronously.
    let deadline = deadline_secs(20);
    while c.nodes[target].node.config_version() < 1 {
        assert!(Instant::now() < deadline, "removed follower never adopted its own removal");
        std::thread::yield_now();
    }

    // Fail-stop proof: the removed node's cnc heartbeat FREEZES (do_work's
    // entry check short-circuits every subsequent cycle, so publish_status
    // never runs again) — unlike a live idle follower, whose heartbeat keeps
    // advancing every duty cycle. Contrast against a SURVIVING follower's
    // heartbeat, which must keep moving, to rule out a stalled test process.
    let target_cnc = open_cnc(&c.nodes[target].instance_dir);
    let survivor = (0..c.nodes.len()).find(|&i| i != leader && i != target).unwrap();
    let survivor_cnc = open_cnc(&c.nodes[survivor].instance_dir);
    let hb0 = target_cnc.status().node_heartbeat_ns.load_acquire();
    let sv0 = survivor_cnc.status().node_heartbeat_ns.load_acquire();
    let settle = deadline_secs(2);
    while Instant::now() < settle
        && (target_cnc.status().node_heartbeat_ns.load_acquire() == hb0
            || survivor_cnc.status().node_heartbeat_ns.load_acquire() == sv0)
    {
        std::thread::yield_now();
    }
    let hb1 = target_cnc.status().node_heartbeat_ns.load_acquire();
    assert!(
        survivor_cnc.status().node_heartbeat_ns.load_acquire() > sv0,
        "sanity: a live survivor's heartbeat must keep advancing"
    );
    let frozen_hb = hb1;
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        target_cnc.status().node_heartbeat_ns.load_acquire(),
        frozen_hb,
        "removed follower's heartbeat must be frozen (fail-stopped), not still ticking"
    );
    let flags = target_cnc.status().flags.load_acquire();
    assert_eq!(
        flags & (NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE),
        0,
        "a halted node must never re-claim LEADER/CAN_SERVE"
    );

    // Free the removed node's address (a fail-stopped process is, in
    // practice, eventually powered off) and rebind a raw socket to its exact
    // "dead identity" — then forge disruptive datagrams from it at the
    // survivors: a RequestVote soliciting a huge new term, and an
    // AppendPosition report claiming a huge durable at a huge term.
    // `Node::stop` takes `self` by value, so the owning `NodeH` is removed
    // from the vec first (id-based lookups below never depend on vec
    // position, so the shift this causes is harmless).
    c.nodes.remove(target).node.stop();

    let zombie = {
        let deadline = deadline_secs(5);
        loop {
            match UdpSocket::bind(target_addr) {
                Ok(s) => break s,
                Err(_) if Instant::now() < deadline => std::thread::yield_now(),
                Err(e) => panic!("could not rebind the freed address {target_addr}: {e}"),
            }
        }
    };

    let survivor_ids: Vec<NodeId> = c.nodes.iter().map(|h| h.id).collect();
    let terms_before: Vec<u32> = survivor_ids
        .iter()
        .map(|&id| c.nodes.iter().find(|h| h.id == id).unwrap().node.current_term())
        .collect();
    let survivor_addrs: Vec<SocketAddr> = c.nodes.iter().map(|h| h.addr).collect();

    let huge_term = terms_before.iter().copied().max().unwrap_or(0) + 10_000;
    let mut rvb = vec![0u8; DATAGRAM_HEADER_LEN + uc_protocol::v2::datagram::REQUEST_VOTE_BODY_LEN];
    write_datagram_header(
        &mut rvb,
        &DatagramHeader {
            position: 0,
            leadership_term_id: huge_term,
            kind: DGRAM_KIND_REQUEST_VOTE,
            flags: 0,
        },
    );
    write_request_vote_body(
        &mut rvb[DATAGRAM_HEADER_LEN..],
        &RequestVoteBody { new_term: huge_term, last_term: huge_term, last_durable: u64::MAX },
    );
    let mut report = vec![0u8; DATAGRAM_HEADER_LEN];
    write_datagram_header(
        &mut report,
        &DatagramHeader {
            position: u64::MAX / 2,
            leadership_term_id: huge_term,
            kind: DGRAM_KIND_APPEND_POSITION,
            flags: 0,
        },
    );

    let watch = deadline_secs(2);
    while Instant::now() < watch {
        for &addr in &survivor_addrs {
            let _ = zombie.send_to(&rvb, addr);
            let _ = zombie.send_to(&report, addr);
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    for (i, &id) in survivor_ids.iter().enumerate() {
        assert_eq!(
            c.nodes.iter().find(|h| h.id == id).unwrap().node.current_term(),
            terms_before[i],
            "survivor id {id}'s term must stay stable under the zombie's forged datagrams"
        );
    }

    drop(zombie);
    for h in c.nodes {
        h.node.stop();
    }
}

/// M7 Task 8 — a joining node boots from a STALE seed (a members list that
/// predates its own admission): 3-voter cluster, add-learner id 5 via admin,
/// THEN boot node 5 with a seed listing only the ORIGINAL 3 voters. It must
/// still adopt v1 from the replicated CONFIG frame in the byte stream (the
/// voters already fan out to it once the frame committed, regardless of what
/// node 5's own local seed says) and appear in every node's peer band. Then
/// promoting it (after catch-up) yields a genuine 4-voter quorum: crashing
/// one OTHER voter still lets the cluster commit new writes.
#[test]
fn joining_node_boots_from_stale_seed() {
    let _g = serialize();
    let mut c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 20);

    // Pre-bind node 5's socket so its real address is known up front (the
    // admin op needs it), even though node 5 itself does not start yet.
    let sock5 = UdpSocket::bind("127.0.0.1:0").expect("bind node 5's socket");
    let addr5 = sock5.local_addr().unwrap();
    let (ip5, port5) = addr_to_wire(addr5);

    let leader_cnc = open_cnc(&c.nodes[leader].instance_dir);
    let resp = admin_request_ok(&leader_cnc, 1 /* AddLearner */, 5, ip5, port5, 20);
    assert_eq!(resp.version, 1);

    // Boot node 5 with a STALE seed: only the ORIGINAL 3 voters — no
    // knowledge whatsoever of its own learner-hood (a fresh instance dir, so
    // this seed is authoritative at boot per `NodeConfig::members`' doc).
    let instance_dir5 = c.dir_path.join("n5");
    let cfg5 = make_config(5, c.members.clone(), addr5, instance_dir5.clone(), seed_for(5));
    let node5 = Node::start_with_socket(cfg5, sock5).expect("start joining node from a stale seed");

    // It adopts v1 from the replicated stream, not from its own (stale) seed.
    let deadline = deadline_secs(40);
    while node5.config_version() < 1 {
        assert!(Instant::now() < deadline, "node 5 never adopted v1 from the stream");
        std::thread::yield_now();
    }

    // It appears in every (other) node's peer band as a LEARNER.
    let node5_h = NodeH { id: 5, addr: addr5, instance_dir: instance_dir5.clone(), node: node5 };
    for h in c.nodes.iter().chain(std::iter::once(&node5_h)) {
        assert!(h.node.config_version() >= 1, "node {} lagging on config version", h.id);
        if h.id == 5 {
            continue; // a node never lists its own slot
        }
        let cnc = open_cnc(&h.instance_dir);
        let mut found = false;
        for i in 0..CNC_MAX_PEER_SLOTS {
            let raw = cnc.peer_slot(i).id_and_role.load_acquire();
            if raw == 0 {
                continue;
            }
            let id = (raw >> 8) as u32;
            let role = (raw & 0xff) as u8;
            if id == 5 {
                assert_eq!(role, CNC_PEER_ROLE_LEARNER, "node {}: id 5's peer slot role", h.id);
                found = true;
            }
        }
        assert!(found, "node {}: no peer slot published for learner 5", h.id);
    }

    // Drive some real writes so node 5 has something to catch up on, then
    // promote it (retrying through `NotCaughtUp` while it closes the gap).
    for i in 0u64..500 {
        let mut p = vec![0u8; 64];
        p[..8].copy_from_slice(&i.to_le_bytes());
        let deadline = deadline_secs(20);
        loop {
            match c.nodes[leader].node.submit(p.clone()) {
                Ok(()) => break,
                Err(_) => {
                    assert!(Instant::now() < deadline, "submit stayed refused");
                    std::thread::yield_now();
                }
            }
        }
    }
    let resp = admin_request_ok(&leader_cnc, 2 /* PromoteLearner */, 5, 0, 0, 30);
    assert_eq!(resp.version, 2);

    let deadline = deadline_secs(20);
    while node5_h.node.config_version() < 2 {
        assert!(Instant::now() < deadline, "node 5 never converged on its own promotion");
        std::thread::yield_now();
    }

    // 4-voter quorum: crash ONE OTHER voter; the cluster still commits.
    // `Node::crash` takes `self` by value, so the owning `NodeH` is removed
    // from the vec first (nothing downstream depends on vec position — the
    // crashed node is simply no longer in `c.nodes` at all).
    let other_voter_idx = (0..c.nodes.len()).find(|&i| i != leader).unwrap();
    c.nodes.remove(other_voter_idx).node.crash();

    let all_live = || c.nodes.iter().chain(std::iter::once(&node5_h));
    let before = {
        let deadline = deadline_secs(20);
        loop {
            if let Some(h) = all_live().find(|h| h.node.can_serve()) {
                break h.node.counters().commit.load_acquire();
            }
            assert!(Instant::now() < deadline, "no leader among the remaining 3 live voters");
            std::thread::yield_now();
        }
    };
    for i in 500u64..1000 {
        let mut p = vec![0u8; 64];
        p[..8].copy_from_slice(&i.to_le_bytes());
        if let Some(h) = all_live().find(|h| h.node.can_serve()) {
            let _ = h.node.submit(p);
        }
    }
    let deadline = deadline_secs(20);
    loop {
        let cur = all_live().map(|h| h.node.counters().commit.load_acquire()).max().unwrap_or(0);
        if cur > before {
            break;
        }
        assert!(Instant::now() < deadline, "the 4-voter (minus 1 crashed) cluster never committed more");
        std::thread::yield_now();
    }

    node5_h.node.stop();
    for h in c.nodes {
        h.node.stop();
    }
}
