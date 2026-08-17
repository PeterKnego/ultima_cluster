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
    CNC_MAX_PEER_SLOTS, CNC_PEER_ROLE_LEARNER, CNC_PEER_ROLE_VOTER, NODE_FLAG_CAN_SERVE,
    NODE_FLAG_LEADER,
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
        crypto: uc2_node::CryptoConfig::Disabled,
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
        assert!(
            Instant::now() < deadline,
            "config version {version} never converged cluster-wide: {:?}",
            nodes
                .iter()
                .map(|h| {
                    let c = h.node.counters();
                    (
                        h.id,
                        h.node.config_version(),
                        c.append.load_acquire(),
                        c.durable.load_acquire(),
                        c.commit.load_acquire(),
                        h.node.is_leader(),
                        h.node.can_serve(),
                    )
                })
                .collect::<Vec<_>>()
        );
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

        // The removed leader's own last published commit BEFORE the handoff —
        // captured right before the removal is even requested, so it is
        // genuinely a pre-handoff value. The strengthened assertion below
        // proves the new leader's post-handoff commit strictly exceeds this,
        // i.e. it actually COMMITS NEW entries rather than merely inheriting
        // (or freezing at) whatever counter value the old leader left behind.
        let old_commit = c.nodes[leader].node.counters().commit.load_acquire();

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

        // Strict commit-monotonicity across the handoff: the new leader's
        // commit must be STRICTLY greater than the removed leader's own last
        // published commit from before the handoff even started. This is the
        // discriminating strengthening over the (still-kept) high-water
        // non-regression check below — it fails a new leader that merely
        // inherits `old_commit` and then freezes, since the wait loop above
        // only proves forward motion from an arbitrary POST-handoff baseline.
        let new_commit = c.nodes[new_leader].node.counters().commit.load_acquire();
        assert!(
            new_commit > old_commit,
            "new leader's commit ({new_commit}) did not strictly exceed the removed \
             leader's pre-handoff commit ({old_commit}) — it must commit NEW entries, \
             not merely inherit the counter"
        );

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
            key_epoch: 0,
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
            key_epoch: 0,
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

// ------------------------------------------------------------ M7 Task 9
//
// The remaining spec §9.3 integration scenarios: a full box-replacement
// recipe, a 3->5->3 resize under continuous traffic, an exhaustive refusal
// matrix (exact reason codes), a truncation/revert proof combining a
// pending config with a real network partition, and a crash-mid-pending
// recovery. These reuse T7/T8's admin-slot harness (`admin_request`,
// `admin_request_ok`, `await_config_converged`, `open_cnc`, ...) verbatim
// and add a handful of small, purely-additive test-support helpers below
// (partition control mirroring `failover.rs`'s `NodeH::block/heal`, a
// small-admission cluster variant to make `NotCaughtUp` reachable without
// thousands of baseline writes, and a "config settled" poll to avoid
// racing `ChangePending` against later, unrelated refusal probes).

/// Cut node `a`'s outbound sends to `peer` (one side of a link cut — the
/// caller cuts the other direction too via [`partition`]). Mirrors
/// `failover.rs`'s `NodeH::block`.
fn block_link(a: &NodeH, peer: SocketAddr) {
    for h in a.node.partition_handles() {
        h.block(peer);
    }
}

/// Heal every partition on `a`'s sockets. Mirrors `failover.rs`'s `NodeH::heal`.
fn heal_link(a: &NodeH) {
    for h in a.node.partition_handles() {
        h.clear();
    }
}

/// Cut every link between `a` and `b` (both send directions) — mirrors
/// `failover.rs`'s free `partition` fn.
fn partition(a: &NodeH, b: &NodeH) {
    block_link(a, b.addr);
    block_link(b, a.addr);
}

/// Wait until exactly one of `idxs` is serving and return its index (used
/// when some members are excluded from the check — e.g. an isolated
/// minority leader that still privately believes it serves). Mirrors
/// `failover.rs`'s `await_serving_among`.
fn await_serving_among(nodes: &[NodeH], idxs: &[usize], secs: u64) -> usize {
    let deadline = deadline_secs(secs);
    loop {
        let serving: Vec<usize> = idxs.iter().copied().filter(|&i| nodes[i].node.can_serve()).collect();
        assert!(serving.len() <= 1, "split-brain among {idxs:?}: {serving:?} serve");
        if serving.len() == 1 {
            return serving[0];
        }
        assert!(Instant::now() < deadline, "no leader among {idxs:?}");
        std::thread::yield_now();
    }
}

/// Wait for a STABLE single serving leader across the WHOLE cluster,
/// tolerating (rather than asserting away) a transient overlap right after
/// healing a partition that isolated a believing leader: at the instant
/// `heal()` merely stops dropping datagrams, the old leader's own state is
/// unchanged (it has not yet received or sent anything revealing the
/// higher term), so it and the freshly-elected majority leader can BOTH
/// legitimately read `can_serve() == true` for the first few poll
/// iterations. `await_single_leader`'s strict split-brain assert is the
/// right tool everywhere else in this suite (proving no overlap ever
/// happens under normal operation); here we deliberately relax it because
/// the overlap is the expected, momentary shape of "a heal in flight", not
/// a bug — we just need a settled leader to keep driving the test.
fn find_stable_leader(nodes: &[NodeH], secs: u64) -> usize {
    let deadline = deadline_secs(secs);
    loop {
        let serving: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].node.can_serve()).collect();
        if serving.len() == 1 {
            let i = serving[0];
            std::thread::sleep(Duration::from_millis(20));
            let still: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].node.can_serve()).collect();
            if still == vec![i] {
                return i;
            }
        }
        assert!(Instant::now() < deadline, "no stable single leader emerged");
        std::thread::yield_now();
    }
}

/// As [`spawn_cluster`] but with an explicit `admission_bytes` (the M7
/// `NotCaughtUp` catch-up-slack window) — lets a test reach a real
/// `NotCaughtUp` refusal with a modest amount of traffic rather than
/// needing to exceed the default 256 KiB slack.
fn spawn_cluster_admission(n: usize, admission_bytes: u64) -> Cluster {
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
        let mut cfg =
            make_config(i as NodeId, members.clone(), addr, instance_dir.clone(), seed_for(i));
        cfg.admission_bytes = admission_bytes;
        let node = Node::start_with_socket(cfg, sock).expect("start");
        nodes.push(NodeH { id: i as NodeId, addr, instance_dir, node });
    }
    Cluster { _dir: dir, dir_path, members, nodes }
}

/// Wait until `cnc`'s mirrored `config_pending` reads stable (0). Used
/// between successive "real" (expected-to-succeed) admin ops so a later,
/// unrelated refusal probe cannot spuriously observe `ChangePending`
/// instead of the structural reason it is actually testing for — the
/// pending gate is checked BEFORE every structural precondition in
/// `ElectionSm::propose_config`, so an unsettled prior change would
/// silently swap in reason 3 for whatever exact reason a probe expects.
fn await_config_settled(cnc: &CncPage, secs: u64) {
    let deadline = deadline_secs(secs);
    while cnc.config_pending() != 0 {
        assert!(Instant::now() < deadline, "config change never settled (stayed pending)");
        std::thread::yield_now();
    }
}

/// Submit `n` distinct 64 B payloads through `node`, retrying while the
/// bounded ingress reports `Full`.
fn submit_batch(node: &Node, n: u64) {
    for i in 0..n {
        let mut p = vec![0u8; 64];
        p[..8].copy_from_slice(&i.to_le_bytes());
        let deadline = deadline_secs(20);
        loop {
            match node.submit(p.clone()) {
                Ok(()) => break,
                Err(_) => {
                    assert!(Instant::now() < deadline, "submit stayed refused");
                    std::thread::yield_now();
                }
            }
        }
    }
}

/// Max over every live node's own commit counter (each individually
/// monotonic) — a cluster-wide high-water mark for "commit advanced".
fn commit_high_water(nodes: &[NodeH]) -> u64 {
    nodes.iter().map(|h| h.node.counters().commit.load_acquire()).max().unwrap_or(0)
}

/// Wait (bounded) for the cluster-wide commit high-water to advance past
/// `before` — `submit_batch` only enqueues (it returns as soon as the ring
/// accepts the payload), so the actual quorum-commit of those bytes lags
/// behind it and must be awaited, not read back immediately.
fn await_commit_advanced(nodes: &[NodeH], before: u64, msg: &str) {
    let deadline = deadline_secs(20);
    while commit_high_water(nodes) <= before {
        assert!(Instant::now() < deadline, "{msg}");
        std::thread::yield_now();
    }
}

/// Add `id` as a learner via `leader_cnc` (asserting the resulting version),
/// then boot a REAL node for it on a freshly bound socket and push its
/// handle into `c.nodes` — the "replace/resize a box" pattern shared by
/// `full_replace_a_box_recipe` and `resize_3_to_5_to_3`: a real process that
/// can genuinely receive replicated data and report its own catch-up
/// progress, not a bare structural entry.
fn add_learner_and_boot(c: &mut Cluster, leader_cnc: &CncPage, id: NodeId, expect_version: u64) {
    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind");
    let addr = sock.local_addr().unwrap();
    let (ip, port) = addr_to_wire(addr);
    let resp = admin_request_ok(leader_cnc, 1 /* AddLearner */, id, ip, port, 20);
    assert_eq!(resp.version, expect_version, "add-learner {id} landed at an unexpected version");
    let instance_dir = c.dir_path.join(format!("n{id}"));
    let cfg = make_config(id, c.members.clone(), addr, instance_dir.clone(), seed_for(id as usize));
    let node = Node::start_with_socket(cfg, sock).expect("start extra node");
    c.nodes.push(NodeH { id, addr, instance_dir, node });
}

/// The set of ids `cnc`'s own peer band currently publishes with VOTER role
/// (i.e. every OTHER member this node currently considers a voter).
fn voter_ids_via_cnc(cnc: &CncPage) -> Vec<NodeId> {
    let mut ids = Vec::new();
    for i in 0..CNC_MAX_PEER_SLOTS {
        let raw = cnc.peer_slot(i).id_and_role.load_acquire();
        if raw == 0 {
            continue;
        }
        let id = (raw >> 8) as u32;
        let role = (raw & 0xff) as u8;
        if role == CNC_PEER_ROLE_VOTER {
            ids.push(id);
        }
    }
    ids
}

/// Non-panicking predicate mirroring [`assert_peer_band_clean`] below, for
/// use in a settle-poll loop ahead of the hard assertion (`publish_peer_band`
/// runs inline with config adoption, but a reader can still catch a beat
/// where a peer's cnc page reflects an in-flight transition).
fn peer_band_is_clean(cnc: &CncPage, expected_ids: &[NodeId]) -> bool {
    let mut seen: Vec<NodeId> = Vec::new();
    for i in 0..CNC_MAX_PEER_SLOTS {
        let raw = cnc.peer_slot(i).id_and_role.load_acquire();
        if raw == 0 {
            continue;
        }
        let id = (raw >> 8) as u32;
        if !expected_ids.contains(&id) || seen.contains(&id) {
            return false;
        }
        seen.push(id);
    }
    true
}

/// T11 review (peer-band ghost-slot regression): scan EVERY
/// `CNC_MAX_PEER_SLOTS` slot of `cnc`'s observability band — not just the
/// slots the current membership happens to occupy — and assert the whole
/// band is clean: no id appears twice, and no id outside `expected_ids`
/// appears at all. This is the discriminating check for the
/// `publish_peer_band` fix (`uc2_node/src/node.rs`): a rebuild that SHRINKS
/// the band used to only ever rewrite `0..peer_band.len()`, leaving a stale
/// `id_and_role` from the previous, longer band lingering in a trailing slot
/// forever — producing a ghost duplicate entry for a still-live id at its
/// old index, alongside its real, freshly-rewritten slot at its new index.
/// `pack_id_and_role(peer_id, role)` is `(peer_id << 8) | role`, and no
/// populated slot is ever written with role byte 0 (`CNC_PEER_ROLE_VOTER` =
/// 1, `CNC_PEER_ROLE_LEARNER` = 2), so `raw == 0` unambiguously means
/// "empty" — even for id 0, which IS a real member id in this suite (a
/// populated slot for id 0 packs to `(0 << 8) | role` = 1 or 2, never 0).
fn assert_peer_band_clean(cnc: &CncPage, expected_ids: &[NodeId]) {
    let mut seen: Vec<NodeId> = Vec::new();
    for i in 0..CNC_MAX_PEER_SLOTS {
        let raw = cnc.peer_slot(i).id_and_role.load_acquire();
        if raw == 0 {
            continue;
        }
        let id = (raw >> 8) as u32;
        let role = (raw & 0xff) as u8;
        assert!(
            expected_ids.contains(&id),
            "slot {i}: ghost id {id} (role byte {role}) present, outside expected set {expected_ids:?}"
        );
        assert!(
            !seen.contains(&id),
            "slot {i}: id {id} duplicated in the peer band (a stale ghost slot from a prior, wider band) — role byte {role}, already seen at an earlier slot"
        );
        seen.push(id);
    }
}

/// Rebind a fresh UDP socket on a specific loopback address, retrying
/// briefly (mirrors `failover.rs`'s `rebind`).
fn rebind(addr: SocketAddr) -> UdpSocket {
    let deadline = deadline_secs(5);
    loop {
        match UdpSocket::bind(addr) {
            Ok(s) => return s,
            Err(_) if Instant::now() < deadline => std::thread::yield_now(),
            Err(e) => panic!("rebind {addr} failed: {e}"),
        }
    }
}

/// "Replace a box" recipe end to end: add a fresh node as a learner, prove
/// its catch-up via an EXPLICIT `PeerSlot::reported_durable` poll (not a
/// blind promote-retry), promote it to voter, crash one of the ORIGINAL
/// voters, remove that crashed voter — landing on a 3-voter set that is the
/// original set minus the crashed one plus the new node. Every intermediate
/// `config_version` (1, 2, 3) is directly observed along the way.
#[test]
fn full_replace_a_box_recipe() {
    let _g = serialize();
    let mut c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 20);
    let leader_id = c.nodes[leader].id;
    let leader_cnc = open_cnc(&c.nodes[leader].instance_dir);

    let new_id: NodeId = 50;
    add_learner_and_boot(&mut c, &leader_cnc, new_id, 1);
    await_config_converged(&c.nodes, 1, new_id, CNC_PEER_ROLE_LEARNER, 20);

    // Drive real writes so the new learner has a real gap to close.
    submit_batch(&c.nodes[leader].node, 800);

    // EXPLICIT PeerSlot poll: wait for the new learner's reported_durable
    // (as the LEADER sees it) to reach the leader's own commit — this is
    // the discriminating "poll catch-up" step the recipe calls for, as
    // opposed to just retrying PromoteLearner blindly until NotCaughtUp
    // clears.
    let target_commit = c.nodes[leader].node.counters().commit.load_acquire();
    let deadline = deadline_secs(30);
    loop {
        let mut caught_up = false;
        for i in 0..CNC_MAX_PEER_SLOTS {
            let raw = leader_cnc.peer_slot(i).id_and_role.load_acquire();
            if raw == 0 {
                continue;
            }
            let id = (raw >> 8) as u32;
            if id == new_id
                && leader_cnc.peer_slot(i).reported_durable.load_acquire() >= target_commit
            {
                caught_up = true;
            }
        }
        if caught_up {
            break;
        }
        assert!(Instant::now() < deadline, "new node's PeerSlot never reported catch-up");
        std::thread::yield_now();
    }

    // Promote — the poll above already proved it is caught up, so this must
    // succeed on the first try (no NotCaughtUp-tolerant retry needed).
    let resp = admin_request_ok(&leader_cnc, 2 /* PromoteLearner */, new_id, 0, 0, 20);
    assert_eq!(resp.version, 2);
    await_config_converged(&c.nodes, 2, new_id, CNC_PEER_ROLE_VOTER, 20);

    // Now 4 voters: the original 3 plus the new one. Crash one of the
    // ORIGINAL voters (never the current leader, to keep this a plain
    // dead-follower removal rather than self-removal — already covered by
    // `leader_self_removal_hands_off`).
    let crash_id = (0..3u32).find(|&id| id != leader_id).unwrap();
    let crash_idx = c.nodes.iter().position(|h| h.id == crash_id).unwrap();
    c.nodes.remove(crash_idx).node.crash();

    let resp = admin_request_ok(&leader_cnc, 5 /* RemoveVoter */, crash_id, 0, 0, 20);
    assert_eq!(resp.version, 3);

    let deadline = deadline_secs(20);
    while c.nodes.iter().any(|h| h.node.config_version() < 3) {
        assert!(Instant::now() < deadline, "config version 3 never converged after the replace");
        std::thread::yield_now();
    }
    // The crashed/removed original voter must never surface in a survivor's
    // peer band again.
    for h in &c.nodes {
        let cnc = open_cnc(&h.instance_dir);
        for i in 0..CNC_MAX_PEER_SLOTS {
            let raw = cnc.peer_slot(i).id_and_role.load_acquire();
            if raw == 0 {
                continue;
            }
            let id = (raw >> 8) as u32;
            assert_ne!(id, crash_id, "node {}: removed voter {crash_id} still published", h.id);
        }
    }
    // The new node is now a fully-fledged voter cluster-wide, at v3.
    await_config_converged(&c.nodes, 3, new_id, CNC_PEER_ROLE_VOTER, 20);

    // T11 review: the shrink from 4 members back to 3 (the `RemoveVoter`
    // above) must not leave a ghost/duplicate slot anywhere in the band —
    // scan ALL 8 slots, not just the ones `voter_ids_via_cnc`-style checks
    // look at. Final voter set: the original 3 minus the crashed one, plus
    // the new node; every survivor's own band excludes itself.
    let final_voters: Vec<NodeId> =
        [0u32, 1, 2].into_iter().filter(|&id| id != crash_id).chain(std::iter::once(new_id)).collect();
    for h in &c.nodes {
        let cnc = open_cnc(&h.instance_dir);
        let expected: Vec<NodeId> = final_voters.iter().copied().filter(|&id| id != h.id).collect();
        assert_peer_band_clean(&cnc, &expected);
    }

    for h in c.nodes {
        h.node.stop();
    }
}

/// Grow 3 -> 5 (two add+promote pairs, versions 1-4) then shrink 5 -> 3 (two
/// demote+remove-learner pairs, versions 5-8), submitting real traffic
/// around every step and asserting commit strictly advances throughout. The
/// final voter set must be exactly the original 3.
///
/// Was IGNORED; un-ignored by the M7 Task 9 fix (see `docs/tasks` / the
/// task-9 report for the full writeup).
///
/// Root cause (FIXED): `ElectionSm::adopt_config` (`uc2_consensus/src/election.rs`)
/// used to set the permanent one-way latch `self_removed = true` whenever
/// the config it JUST adopted excluded `self.id` — with no way to tell "a
/// real removal" apart from "this is a HISTORICAL config, from before I
/// joined, that I am replaying during catch-up, and a LATER config in the
/// very same catch-up run legitimately re-includes me". The second learner
/// added here (id 61) sequentially and correctly adopts v1 (voters
/// [0,1,2], learner [60] — 61 legitimately absent, it hasn't joined yet),
/// v2 (60 promoted — still legitimately absent), then v3 (61 finally
/// present as learner) — the old absence-based predicate would have latched
/// `self_removed` on v1/v2 already, and `halt_if_removed_follower` (called
/// from unrelated tick/event paths) would then fire `Action::HaltRemoved`
/// and permanently park node 61. Fixed by switching the predicate to
/// tombstone-based (`config.tombstones.contains(&self.id)`): absent-and-not-
/// tombstoned now correctly means "not yet admitted", never "removed" —
/// ids are fresh-forever (`ClusterConfig::apply`: an id enters only via
/// `AddLearner`, blocked by the tombstone check for a tombstoned id; an id
/// leaves only via `Remove*`, which ALWAYS pushes a tombstone).
#[test]
fn resize_3_to_5_to_3() {
    let _g = serialize();
    let mut c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 20);
    let leader_cnc = open_cnc(&c.nodes[leader].instance_dir);

    // ---- grow 3 -> 5: two add+promote pairs (v1-v4) ----
    add_learner_and_boot(&mut c, &leader_cnc, 60, 1);
    let before = commit_high_water(&c.nodes);
    submit_batch(&c.nodes[leader].node, 300);
    await_commit_advanced(&c.nodes, before, "commit must advance across add-learner 60");
    await_config_converged(&c.nodes, 1, 60, CNC_PEER_ROLE_LEARNER, 20);

    let resp = admin_request_ok(&leader_cnc, 2 /* PromoteLearner */, 60, 0, 0, 30);
    assert_eq!(resp.version, 2);
    let before = commit_high_water(&c.nodes);
    submit_batch(&c.nodes[leader].node, 300);
    await_commit_advanced(&c.nodes, before, "commit must advance across promote 60");
    await_config_converged(&c.nodes, 2, 60, CNC_PEER_ROLE_VOTER, 20);

    add_learner_and_boot(&mut c, &leader_cnc, 61, 3);
    let before = commit_high_water(&c.nodes);
    submit_batch(&c.nodes[leader].node, 300);
    await_commit_advanced(&c.nodes, before, "commit must advance across add-learner 61");
    await_config_converged(&c.nodes, 3, 61, CNC_PEER_ROLE_LEARNER, 90);

    let resp = admin_request_ok(&leader_cnc, 2, 61, 0, 0, 30);
    assert_eq!(resp.version, 4);
    let before = commit_high_water(&c.nodes);
    submit_batch(&c.nodes[leader].node, 300);
    await_commit_advanced(&c.nodes, before, "commit must advance across promote 61");
    await_config_converged(&c.nodes, 4, 61, CNC_PEER_ROLE_VOTER, 90);

    // ---- shrink 5 -> 3: two demote+remove-learner pairs (v5-v8) ----
    let resp = admin_request_ok(&leader_cnc, 3 /* DemoteVoter */, 60, 0, 0, 20);
    assert_eq!(resp.version, 5);
    let before = commit_high_water(&c.nodes);
    submit_batch(&c.nodes[leader].node, 200);
    await_commit_advanced(&c.nodes, before, "commit must advance across demote 60");

    let resp = admin_request_ok(&leader_cnc, 4 /* RemoveLearner */, 60, 0, 0, 20);
    assert_eq!(resp.version, 6);
    let before = commit_high_water(&c.nodes);
    submit_batch(&c.nodes[leader].node, 200);
    await_commit_advanced(&c.nodes, before, "commit must advance across remove-learner 60");

    let resp = admin_request_ok(&leader_cnc, 3 /* DemoteVoter */, 61, 0, 0, 20);
    assert_eq!(resp.version, 7);
    let before = commit_high_water(&c.nodes);
    submit_batch(&c.nodes[leader].node, 200);
    await_commit_advanced(&c.nodes, before, "commit must advance across demote 61");

    let resp = admin_request_ok(&leader_cnc, 4 /* RemoveLearner */, 61, 0, 0, 20);
    assert_eq!(resp.version, 8);
    let before = commit_high_water(&c.nodes);
    submit_batch(&c.nodes[leader].node, 200);
    await_commit_advanced(&c.nodes, before, "commit must advance across remove-learner 61");

    // ---- final: the original 3 converge on v8; the two added-then-removed
    // nodes (60, 61) have permanently fail-stopped on adopting their own
    // removal (60 at v6, 61 at v8) and must be excluded from an "every
    // c.nodes entry" convergence/voter-set loop — folding them in would
    // either spin forever (60 is frozen below v8) or read a stale,
    // no-longer-current peer band (both).
    let removed_60 = c.nodes.iter().find(|h| h.id == 60).unwrap();
    let removed_61 = c.nodes.iter().find(|h| h.id == 61).unwrap();

    // A removed node adopting its OWN removal is BEST EFFORT, not a guarantee,
    // and this is the contract — not a concession to flakiness. The removal
    // frame reaches the removed node only if it arrives before the leader stops
    // replicating to it, which the leader does as soon as the removal commits;
    // continuing to ship cluster data to a decommissioned node would be the
    // worse behaviour. The design says so directly (spec 2026-07-13, risk
    // table): "Known-source guard + tombstones (structural); self-halt on
    // seeing own removal" — structural first, self-halt conditional.
    //
    // Measured before this was written: the removed node loses that race on
    // roughly 6% of runs (5/86 locally, higher on contended CI runners), and
    // the earlier form of this test asserted adoption within 20 s as if it were
    // guaranteed — the single remaining nightly failure for days. So: give the
    // common path a bounded window, then assert what IS guaranteed either way.
    let adopted = |h: &NodeH, want: u64| -> bool {
        let deadline = deadline_secs(10);
        while h.node.config_version() < want {
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::yield_now();
        }
        true
    };
    let adopted_60 = adopted(removed_60, 6);
    let adopted_61 = adopted(removed_61, 8);
    eprintln!(
        "removed-node self-halt (best effort): n60 adopted v6 = {adopted_60}, \
         n61 adopted v8 = {adopted_61}"
    );

    // THE GUARANTEE, asserted unconditionally: whether or not they ever saw it,
    // both removed ids are structurally excluded from the surviving cluster —
    // gone from every survivor's peer band, so no survivor addresses them —
    // and tombstoned in the durable config, so neither can ever be re-admitted
    // (`restart_of_removed_node_refuses_to_start` covers the boot half).
    //
    // Settle-poll BEFORE the hard assert, the same discipline the rest of this
    // suite uses: `publish_peer_band` runs inline with config adoption, so a
    // reader can catch a beat mid-transition. Asserting the instant the last
    // admin op returns reads an in-flight band and fails ~45% of the time.
    let survivors: Vec<&NodeH> = c.nodes.iter().filter(|h| h.id != 60 && h.id != 61).collect();
    let expected: Vec<NodeId> = survivors.iter().map(|h| h.id).collect();
    let deadline = deadline_secs(20);
    loop {
        let all_clean = survivors
            .iter()
            .all(|h| peer_band_is_clean(&open_cnc(&h.instance_dir), &expected));
        if all_clean {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "survivors never dropped the removed ids from their peer bands"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    for h in &survivors {
        assert_peer_band_clean(&open_cnc(&h.instance_dir), &expected);
    }

    let survivors: Vec<&NodeH> = c.nodes.iter().filter(|h| h.id < 3).collect();
    let deadline = deadline_secs(30);
    while survivors.iter().any(|h| h.node.config_version() < 8) {
        assert!(Instant::now() < deadline, "config version 8 never converged among the original 3");
        std::thread::yield_now();
    }

    // The VOTER-role band publish can lag a beat behind the config-version
    // bump, so poll for the settled shape before asserting on it with clear
    // messages. This only checks VOTER-role membership (the property that
    // actually matters for quorum); the T11 ghost-slot regression check right
    // below additionally covers the full 8-slot band, including the two
    // demoted-then-removed learners (60, 61) — with the `publish_peer_band`
    // fix, neither may linger anywhere once the shrink to v8 settles.
    let deadline = deadline_secs(10);
    loop {
        let settled = survivors.iter().all(|h| {
            let cnc = open_cnc(&h.instance_dir);
            let mut got = voter_ids_via_cnc(&cnc);
            got.sort();
            let mut want: Vec<NodeId> = [0u32, 1, 2].into_iter().filter(|&id| id != h.id).collect();
            want.sort();
            got == want
        });
        if settled {
            break;
        }
        assert!(Instant::now() < deadline, "final voter set never settled");
        std::thread::yield_now();
    }
    for h in &survivors {
        let cnc = open_cnc(&h.instance_dir);
        let mut got = voter_ids_via_cnc(&cnc);
        got.sort();
        let mut want: Vec<NodeId> = [0u32, 1, 2].into_iter().filter(|&id| id != h.id).collect();
        want.sort();
        assert_eq!(got, want, "node {}: final voter set must be exactly the original 3", h.id);
    }

    // T11 review (peer-band ghost-slot regression): the shrink 5 -> 3 above
    // must not leave a ghost/duplicate slot ANYWHERE in the band — all 8
    // slots, not just the voter-role ones checked above. Poll for settled
    // first (same rationale as the voter-set poll above), then hard-assert.
    let deadline = deadline_secs(10);
    loop {
        let clean = survivors.iter().all(|h| {
            let cnc = open_cnc(&h.instance_dir);
            let want: Vec<NodeId> = [0u32, 1, 2].into_iter().filter(|&id| id != h.id).collect();
            peer_band_is_clean(&cnc, &want)
        });
        if clean {
            break;
        }
        assert!(Instant::now() < deadline, "final peer band never settled clean (ghost/duplicate slot)");
        std::thread::yield_now();
    }
    for h in &survivors {
        let cnc = open_cnc(&h.instance_dir);
        let want: Vec<NodeId> = [0u32, 1, 2].into_iter().filter(|&id| id != h.id).collect();
        assert_peer_band_clean(&cnc, &want);
    }

    // Both removed nodes genuinely fail-stopped: over a settle window during
    // which the survivors keep committing, neither advances any further
    // (frozen, not merely lagging).
    let (v60, v61) = (removed_60.node.config_version(), removed_61.node.config_version());
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(removed_60.node.config_version(), v60, "node 60 must stay frozen (fail-stopped)");
    assert_eq!(removed_61.node.config_version(), v61, "node 61 must stay frozen (fail-stopped)");

    for h in c.nodes {
        h.node.stop();
    }
}

/// Every refusal reason surfaces with its EXACT wire code: `AlreadyPresent`
/// (5, add-learner an existing voter id), `NotFound` (6, demote an unknown
/// id), `WrongRole` (7, promote an existing voter), `ChangePending` (3, a
/// second proposal while the first is durably un-committable — see below),
/// `NotCaughtUp` (10, promote a learner with no real backing process),
/// `Tombstoned` (4, re-add a removed id), `TooManyMembers` (9, the 8-member
/// cap), `ZeroVoters` (8, removing the last voter), `SelfDemote` (12, the
/// leader demoting its own id), and the node-level malformed/unknown-op
/// catch-all (11, an op code the node doesn't recognize).
///
/// `ChangePending` construction: rather than racing two proposals against
/// an uncontrollable local commit latency (flaky either way it lands), the
/// leader is partitioned from BOTH followers first. `propose_config`'s
/// local append succeeds regardless (no network needed to append to one's
/// own journal — the isolated-leader-keeps-writing behavior `failover.rs`
/// already proves), but `config_pending()` cannot ever clear without a
/// quorum ack, which is categorically impossible while every link is cut.
/// So a second, distinct proposal on the SAME (still-isolated) leader is a
/// deterministic `ChangePending`, not a timing bet.
#[test]
fn every_refusal_surfaces() {
    let _g = serialize();
    let c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 20);
    let mut leader_cnc = open_cnc(&c.nodes[leader].instance_dir);
    await_config_settled(&leader_cnc, 20);
    // Post-M7 (0.3.0): the node mirrors its configured admission window onto
    // the cnc page once at boot (`make_config`'s `admission_bytes: 256 * 1024`).
    assert_eq!(leader_cnc.admission_bytes(), 256 * 1024);

    // ---- AlreadyPresent: add-learner on an existing VOTER id ----
    let resp = admin_request(&leader_cnc, 1 /* AddLearner */, 0, 0, 0);
    assert_eq!(resp.status, 1);
    assert_eq!(resp.reason, 5, "AlreadyPresent expected, got {resp:?}");

    // ---- NotFound: demote an unknown id ----
    let resp = admin_request(&leader_cnc, 3 /* DemoteVoter */, 999, 0, 0);
    assert_eq!(resp.status, 1);
    assert_eq!(resp.reason, 6, "NotFound expected, got {resp:?}");

    // ---- WrongRole: promote an existing VOTER id ----
    let resp = admin_request(&leader_cnc, 2 /* PromoteLearner */, 0, 0, 0);
    assert_eq!(resp.status, 1);
    assert_eq!(resp.reason, 7, "WrongRole expected, got {resp:?}");

    // ---- SelfDemote (12): demote the LEADER's own id ----
    let resp = admin_request(&leader_cnc, 3 /* DemoteVoter */, leader as u32, 0, 0);
    assert_eq!(resp.status, 1);
    assert_eq!(resp.reason, 12, "SelfDemote expected, got {resp:?}");

    // ---- Malformed op (11): an op code the node doesn't know ----
    let resp = admin_request(&leader_cnc, 99, 5, 0, 0);
    assert_eq!(resp.status, 1);
    assert_eq!(resp.reason, 11, "malformed-op reason expected, got {resp:?}");

    // ---- ChangePending (see doc comment above) ----
    let followers: Vec<usize> = (0..c.nodes.len()).filter(|&i| i != leader).collect();
    for &f in &followers {
        partition(&c.nodes[leader], &c.nodes[f]);
    }
    let resp1 = admin_request(&leader_cnc, 1 /* AddLearner */, 300, 0, 0);
    assert_eq!(resp1.status, 0, "op1's local append must be accepted even while isolated");
    let resp2 = admin_request(&leader_cnc, 1 /* AddLearner */, 301, 0, 0);
    assert_eq!(resp2.status, 1);
    assert_eq!(resp2.reason, 3, "ChangePending expected, got {resp2:?}");
    for h in &c.nodes {
        heal_link(h);
    }
    // Reconverge to a single, STABLE leader (either the same one or a fresh
    // one elected during the isolation — either is fine; the ChangePending
    // observation above is already captured) before continuing.
    let leader = find_stable_leader(&c.nodes, 20);
    leader_cnc = open_cnc(&c.nodes[leader].instance_dir);
    await_config_settled(&leader_cnc, 20);

    // ---- NotCaughtUp: promote a learner with no real backing process, on
    // a cluster with a tiny admission slack so ordinary traffic already
    // exceeds it (avoids needing thousands of baseline writes). ----
    {
        let c2 = spawn_cluster_admission(3, 4096);
        let leader2 = await_single_leader(&c2.nodes, 20);
        let leader2_cnc = open_cnc(&c2.nodes[leader2].instance_dir);
        submit_batch(&c2.nodes[leader2].node, 100);
        let deadline = deadline_secs(20);
        while c2.nodes[leader2].node.counters().commit.load_acquire() <= 4096 {
            assert!(Instant::now() < deadline, "commit never exceeded the tiny admission slack");
            std::thread::yield_now();
        }
        let never_addr: SocketAddr = "127.0.0.1:59500".parse().unwrap(); // never bound/started
        let (ip, port) = addr_to_wire(never_addr);
        let resp = admin_request_ok(&leader2_cnc, 1 /* AddLearner */, 500, ip, port, 20);
        assert_eq!(resp.version, 1);
        await_config_settled(&leader2_cnc, 20);
        let resp = admin_request(&leader2_cnc, 2 /* PromoteLearner */, 500, 0, 0);
        assert_eq!(resp.status, 1);
        assert_eq!(resp.reason, 10, "NotCaughtUp expected, got {resp:?}");
        for h in c2.nodes {
            h.node.stop();
        }
    }

    // ---- Tombstoned: remove a learner, then try to re-add the same id ----
    let learner_addr: SocketAddr = "127.0.0.1:59600".parse().unwrap();
    let (ip, port) = addr_to_wire(learner_addr);
    let resp = admin_request_ok(&leader_cnc, 1 /* AddLearner */, 600, ip, port, 20);
    assert!(resp.version >= 1);
    await_config_settled(&leader_cnc, 20);
    let resp = admin_request_ok(&leader_cnc, 4 /* RemoveLearner */, 600, 0, 0, 20);
    assert!(resp.version >= 2);
    await_config_settled(&leader_cnc, 20);
    let resp = admin_request(&leader_cnc, 1 /* AddLearner */, 600, ip, port);
    assert_eq!(resp.status, 1);
    assert_eq!(resp.reason, 4, "Tombstoned expected, got {resp:?}");

    // ---- TooManyMembers: add learners until the 8-member cap refuses ----
    fn add_learner_until_capped(cnc: &CncPage, id: u32, ip: u32, port: u16, secs: u64) -> AdminResp {
        let deadline = deadline_secs(secs);
        loop {
            let resp = admin_request(cnc, 1 /* AddLearner */, id, ip, port);
            match resp.status {
                0 => return resp,
                1 if resp.reason == 9 => return resp, // the cap: a terminal, expected outcome
                1 if matches!(resp.reason, 2 | 3 | 10) => {}
                2 => {}
                _ => panic!("add-learner {id} refused unexpectedly: {resp:?}"),
            }
            assert!(Instant::now() < deadline, "add-learner {id} never resolved (last {resp:?})");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    let mut hit_cap = false;
    for k in 0u32..10 {
        let id = 700 + k;
        let addr: SocketAddr = format!("127.0.0.1:{}", 20000 + k).parse().unwrap();
        let (ip, port) = addr_to_wire(addr);
        let resp = add_learner_until_capped(&leader_cnc, id, ip, port, 20);
        if resp.status == 1 && resp.reason == 9 {
            hit_cap = true;
            break;
        }
    }
    assert!(hit_cap, "never observed TooManyMembers filling to the 8-member cap");

    // ---- ZeroVoters: remove voters down to 1; the third refuses ----
    let leader_id = c.nodes[leader].id;
    let followers_ids: Vec<NodeId> = (0..3u32).filter(|&id| id != leader_id).collect();
    assert_eq!(followers_ids.len(), 2);
    let resp = admin_request_ok(&leader_cnc, 5 /* RemoveVoter */, followers_ids[0], 0, 0, 20);
    assert_eq!(resp.status, 0);
    await_config_settled(&leader_cnc, 20);
    let resp = admin_request_ok(&leader_cnc, 5 /* RemoveVoter */, followers_ids[1], 0, 0, 20);
    assert_eq!(resp.status, 0);
    await_config_settled(&leader_cnc, 20);
    let resp = admin_request(&leader_cnc, 5 /* RemoveVoter */, leader_id, 0, 0);
    assert_eq!(resp.status, 1);
    assert_eq!(resp.reason, 8, "ZeroVoters expected, got {resp:?}");

    for h in c.nodes {
        h.node.stop();
    }
}

/// Craft a divergent-leader shape whose divergent tail CONTAINS a config
/// frame: partition the leader from both followers, propose a config change
/// while it still believes it serves (accepted, appended, adopted LOCALLY —
/// `config_version` bumps to 1 on that node alone), let the majority elect
/// a fresh leader (whose config stays at genesis, v0 — it never saw the
/// phantom change), heal, and prove the ex-leader TRUNCATES its divergent
/// tail and REVERTS `config_version` back to 0 — deterministically staying
/// there (there is nothing else to converge to yet, so this is not a
/// transient blip to race against). Then prove the cluster still works
/// post-revert: a FRESH config change lands at v1 (the phantom v1 never
/// counted) and converges everywhere, with the phantom id never surfacing.
#[test]
fn truncation_revert_e2e() {
    let _g = serialize();
    let c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 20);

    let followers: Vec<usize> = (0..c.nodes.len()).filter(|&i| i != leader).collect();
    for &f in &followers {
        partition(&c.nodes[leader], &c.nodes[f]);
    }

    // Still isolated, the old leader still believes it serves (exactly like
    // failover.rs's phantom-write proof) — propose while it does.
    let leader_cnc = open_cnc(&c.nodes[leader].instance_dir);
    let phantom_id: NodeId = 900;
    let phantom_addr: SocketAddr = "127.0.0.1:59900".parse().unwrap();
    let (ip, port) = addr_to_wire(phantom_addr);
    let resp = admin_request(&leader_cnc, 1 /* AddLearner */, phantom_id, ip, port);
    assert_eq!(resp.status, 0, "isolated leader refused its own local append: {resp:?}");
    assert_eq!(resp.version, 1);
    assert_eq!(
        c.nodes[leader].node.config_version(),
        1,
        "the phantom config is adopted LOCALLY (optimistically), before any replication"
    );

    // The majority elects a NEW leader; its config stays at genesis (v0) —
    // it never saw the phantom change.
    let new = await_serving_among(&c.nodes, &followers, 20);
    assert!(c.nodes[new].node.current_term() > 0);
    for &f in &followers {
        assert_eq!(
            c.nodes[f].node.config_version(),
            0,
            "the majority must not see the isolated leader's phantom config"
        );
    }

    // Heal. The ex-leader must adopt the higher term, TRUNCATE its
    // divergent config-bearing tail, and REVERT.
    for h in &c.nodes {
        heal_link(h);
    }
    let deadline = deadline_secs(30);
    while c.nodes[leader].node.truncations() == 0 {
        assert!(Instant::now() < deadline, "ex-leader never truncated its divergent config tail");
        std::thread::yield_now();
    }
    let deadline = deadline_secs(20);
    while c.nodes[leader].node.config_version() != 0 {
        assert!(Instant::now() < deadline, "ex-leader's config never reverted to genesis (v0)");
        std::thread::yield_now();
    }
    // Deterministic settle window: with no competing majority config change
    // to converge to yet, the reverted version must STAY at 0.
    let settle = Instant::now() + Duration::from_millis(500);
    while Instant::now() < settle {
        assert_eq!(c.nodes[leader].node.config_version(), 0, "reverted config regressed off v0");
        std::thread::yield_now();
    }
    // Journal-record consistency: the reverted record is no longer pending
    // — it is backed by committed (and thus durable) bytes, not a dangling
    // local-only append.
    assert_eq!(leader_cnc.config_pending(), 0, "reverted config must not read back as pending");

    // Full data-plane reconvergence to a single, stable frontier (the new
    // leader is idle — no submissions since heal).
    let final_target = {
        let deadline = deadline_secs(30);
        loop {
            let a = c.nodes[new].node.counters().append.load_acquire();
            let d = c.nodes[new].node.counters().durable.load_acquire();
            let cm = c.nodes[new].node.counters().commit.load_acquire();
            if d == a && cm == a {
                break a;
            }
            assert!(Instant::now() < deadline, "new leader never quiesced");
            std::thread::yield_now();
        }
    };
    for h in &c.nodes {
        let deadline = deadline_secs(30);
        while h.node.counters().durable.load_acquire() < final_target {
            assert!(Instant::now() < deadline, "node {} never reconverged durable", h.id);
            std::thread::yield_now();
        }
    }

    // The cluster still works post-revert: a FRESH config change lands at
    // v1 (the phantom v1 never counted) and converges everywhere.
    let new_leader_cnc = open_cnc(&c.nodes[new].instance_dir);
    let real_id: NodeId = 901;
    let real_addr: SocketAddr = "127.0.0.1:59901".parse().unwrap();
    let (ip2, port2) = addr_to_wire(real_addr);
    let resp = admin_request_ok(&new_leader_cnc, 1 /* AddLearner */, real_id, ip2, port2, 20);
    assert_eq!(resp.version, 1, "the fresh post-revert config is v1 — the phantom v1 never counted");
    await_config_converged(&c.nodes, 1, real_id, CNC_PEER_ROLE_LEARNER, 20);

    // Final: every node agrees on the SAME version; the phantom id never
    // surfaces anywhere.
    for h in &c.nodes {
        assert_eq!(h.node.config_version(), 1, "node {} did not converge to the final version", h.id);
        let cnc = open_cnc(&h.instance_dir);
        for i in 0..CNC_MAX_PEER_SLOTS {
            let raw = cnc.peer_slot(i).id_and_role.load_acquire();
            if raw == 0 {
                continue;
            }
            let id = (raw >> 8) as u32;
            assert_ne!(id, phantom_id, "node {}: the truncated phantom learner must never surface", h.id);
        }
    }

    for h in c.nodes {
        h.node.stop();
    }
}

/// SIGKILL-free crash-mid-pending recovery: crash a follower FIRST (so the
/// leader still holds quorum without it), propose a config change that
/// commits via the remaining 2-of-3, restart the crashed follower from the
/// SAME dirs/port, and confirm it re-adopts the config it missed straight
/// from the journal/replicated stream — rejoining as an ordinary follower,
/// with no spurious election.
#[test]
fn crash_mid_pending_recovers() {
    let _g = serialize();
    let mut c = spawn_cluster(3);
    let members = c.members.clone();
    let leader = await_single_leader(&c.nodes, 20);
    let leader_id = c.nodes[leader].id;
    let follower = (0..c.nodes.len()).find(|&i| i != leader).expect("a follower exists");
    let follower_id = c.nodes[follower].id;
    let follower_addr = c.nodes[follower].addr;
    let follower_dir = c.nodes[follower].instance_dir.clone();

    // Crash the follower FIRST — while it is down, the leader still has
    // quorum (2 of 3) to commit a config change without it.
    c.nodes.remove(follower).node.crash();

    let leader_idx = c.nodes.iter().position(|h| h.id == leader_id).unwrap();
    let leader_cnc = open_cnc(&c.nodes[leader_idx].instance_dir);

    let new_id: NodeId = 70;
    let learner_addr: SocketAddr = "127.0.0.1:59070".parse().unwrap(); // structural only
    let (ip, port) = addr_to_wire(learner_addr);
    let resp = admin_request_ok(&leader_cnc, 1 /* AddLearner */, new_id, ip, port, 20);
    assert_eq!(resp.version, 1);

    // It commits via the remaining quorum (leader + the other live follower).
    await_config_converged(&c.nodes, 1, new_id, CNC_PEER_ROLE_LEARNER, 20);

    // Restart the crashed follower from the SAME dirs on the SAME port.
    let sock = rebind(follower_addr);
    let cfg =
        make_config(follower_id, members, follower_addr, follower_dir.clone(), seed_for(follower_id as usize));
    let node = Node::start_with_socket(cfg, sock).expect("restart follower");
    c.nodes.push(NodeH { id: follower_id, addr: follower_addr, instance_dir: follower_dir, node });

    // It re-adopts the config it missed from the journal/replicated stream,
    // rejoining as an ordinary follower — no spurious election / leader claim.
    let restarted_idx = c.nodes.iter().position(|h| h.id == follower_id).unwrap();
    let deadline = deadline_secs(30);
    while c.nodes[restarted_idx].node.config_version() < 1 {
        assert!(Instant::now() < deadline, "restarted follower never re-adopted the missed config");
        std::thread::yield_now();
    }
    assert!(!c.nodes[restarted_idx].node.is_leader(), "restarted follower unexpectedly became leader");

    await_config_converged(&c.nodes, 1, new_id, CNC_PEER_ROLE_LEARNER, 20);

    for h in c.nodes {
        h.node.stop();
    }
}

/// Post-M7 follow-up: a node restarted on an instance dir whose recovered
/// config tombstones its OWN id must refuse to start (previously: booted as
/// a permanently-idle zombie — the runtime HaltRemoved latch is version-
/// gated and never re-fires on boot).
#[test]
fn restart_of_removed_node_refuses_to_start() {
    let _g = serialize();
    let mut c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 20);
    let leader_cnc = open_cnc(&c.nodes[leader].instance_dir);

    let removed_id: NodeId = 100;
    add_learner_and_boot(&mut c, &leader_cnc, removed_id, 1);
    await_config_converged(&c.nodes, 1, removed_id, CNC_PEER_ROLE_LEARNER, 20);

    // A real gap of committed traffic before the removal, same as this
    // file's other add/promote/demote/remove sequences.
    let before = commit_high_water(&c.nodes);
    submit_batch(&c.nodes[leader].node, 100);
    await_commit_advanced(&c.nodes, before, "commit must advance after adding learner 100");

    let resp = admin_request_ok(&leader_cnc, 4 /* RemoveLearner */, removed_id, 0, 0, 20);
    assert_eq!(resp.version, 2);

    // Wait for the REMOVED node itself to adopt its own removal
    // (config_version reaches v2). For a non-leader, `ConfigObserved` is
    // fed only from a durably-ARCHIVED CONFIG frame ("this is how everyone
    // else learns once the frame is durable" — node.rs `Consensus::do_work`
    // step 1c), so by the time this loop exits the removal frame's position
    // is already <= this node's own recovered `durable`: no window left for
    // `recover_config_record`'s T5-carry revert to undo the tombstone on
    // restart (that revert only ever fires for a record ahead of durable).
    let removed_idx = c.nodes.iter().position(|h| h.id == removed_id).unwrap();
    let deadline = deadline_secs(20);
    while c.nodes[removed_idx].node.config_version() < 2 {
        assert!(Instant::now() < deadline, "removed learner never adopted its own removal");
        std::thread::yield_now();
    }

    // Drop the removed node's OWN handle — a clean stop, which releases the
    // instance-dir flock and fully joins every agent — before restarting on
    // the SAME instance dir. The old process's runtime HaltRemoved latch is
    // irrelevant here; this proves the NEW process's construction-time
    // refusal instead.
    let removed_dir = c.nodes[removed_idx].instance_dir.clone();
    let removed_addr = c.nodes[removed_idx].addr;
    let removed = c.nodes.remove(removed_idx);
    removed.node.stop();

    // Restart with the same NodeConfig shape the harness uses for every
    // spawned node (`make_config`/`add_learner_and_boot`): same instance
    // dir, id, and bind address.
    let sock = rebind(removed_addr);
    let cfg = make_config(
        removed_id,
        c.members.clone(),
        removed_addr,
        removed_dir,
        seed_for(removed_id as usize),
    );
    // `Node` doesn't implement `Debug`, so `expect_err` isn't available —
    // match directly instead.
    let err = match Node::start_with_socket(cfg, sock) {
        Ok(_) => panic!("a tombstoned id must not boot"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(msg.contains("tombstoned"), "error must name the cause: {msg}");
    assert!(msg.contains("fresh id"), "error must name the recourse: {msg}");

    for h in c.nodes {
        h.node.stop();
    }
}
