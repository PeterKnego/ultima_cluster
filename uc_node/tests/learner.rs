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
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use uc_consensus::election::NodeId;
use uc_log::cnc::{AdminReq, CncPage};
use uc_log::state::{ConfigRecord, NodeState, StoredConfig, StoredMember};
use uc_net::fault::FaultConfig;
use uc_net::receiver::RefusalKind;
use uc_node::{Node, NodeConfig, PurgePolicy};
use uc_protocol::identity::{FsmName, pack_version};
use uc_protocol::v2::cnc::{ADMIN_OP_SCHEDULE_APPLY, CNC_MAX_PEER_SLOTS, CNC_PEER_ROLE_LEARNER};
use uc_protocol::v2::config::decode_config;
use uc_protocol::v2::schedule::{
    ScheduleEntry, ScheduleRule, ScheduleTable, decode_schedule_table, encode_schedule_table,
};

const PAYLOAD: usize = 96;

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn serialize() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// Wire 0.7.0 (Ruling 1): `snapshot_set_for` now declines outright for a
/// `ServicesConfig::none_for_tests()` node — a harness node with nothing
/// NAMED can never be part of a positional identity exchange, so the tests
/// below that drive real snapshot sessions need a REAL declared FSM row
/// instead. But with a row genuinely declared, FSM-lag admission control
/// (`Consensus::publish_service_mins` / `admission_open`) engages against
/// `cnc.service_slot(id).applied` — and these tests submit raw bytes through
/// `Node::submit` with no real service ever attached to advance it, which
/// would deadlock the submit loop once cumulative `append` crosses the lag
/// bound. This is a cheap stand-in for "a service is attached and instantly
/// applying": it mirrors `durable` into `service_slot(id).applied` so
/// admission never blocks, without pulling in a real `uc_service` (whose own
/// automatic snapshot builder would race the test's own hand-staged
/// snapshot floor/artifact). Stop + join it once the submit loop is done —
/// nothing after that in these tests submits again.
fn spawn_applied_mirror(
    cnc: std::sync::Arc<CncPage>,
    id: usize,
) -> (
    std::sync::Arc<std::sync::atomic::AtomicBool>,
    std::thread::JoinHandle<()>,
) {
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop2 = std::sync::Arc::clone(&stop);
    let handle = std::thread::spawn(move || {
        while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
            let durable = cnc.counters().durable.load_acquire();
            cnc.service_slot(id).applied.store_release(durable);
            std::thread::sleep(Duration::from_micros(200));
        }
    });
    (stop, handle)
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
    fn try_submit(&self, payload: Vec<u8>) -> Result<(), uc_node::SubmitError> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match self.n().submit(payload.clone()) {
                Ok(()) => return Ok(()),
                Err(uc_node::SubmitError::Full) => {
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
        purge: uc_node::PurgePolicy::Disabled,
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::none_for_tests(),
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
    let socks: Vec<UdpSocket> = (0..total)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind"))
        .collect();
    let all: Vec<(NodeId, SocketAddr)> = socks
        .iter()
        .enumerate()
        .map(|(i, s)| (i as NodeId, s.local_addr().unwrap()))
        .collect();
    let members: Vec<(NodeId, SocketAddr)> = all[..n_voters].to_vec();
    let learners: Vec<(NodeId, SocketAddr)> = all[n_voters..].to_vec();

    let mut nodes = Vec::with_capacity(total);
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = all[i].1;
        let instance_dir = dir.path().join(format!("n{i}"));
        let seed = seed_for(i);
        let is_learner = i >= n_voters;
        let cfg = make_config(
            i as NodeId,
            members.clone(),
            learners.clone(),
            instance_dir.clone(),
            seed,
            addr,
        );
        let node = Node::start_with_socket(cfg, sock).expect("start");
        nodes.push(NodeH {
            id: i as NodeId,
            addr,
            instance_dir,
            seed,
            is_learner,
            node: Some(node),
        });
    }
    Cluster {
        _dir: dir,
        members,
        learners,
        nodes,
    }
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
            assert!(
                !h.can_serve() && !h.is_leader(),
                "learner {} became a leader",
                h.id
            );
        }
        let serving: Vec<usize> = (0..nodes.len())
            .filter(|&i| nodes[i].node.is_some() && nodes[i].can_serve())
            .collect();
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
        let serving: Vec<usize> = idxs
            .iter()
            .copied()
            .filter(|&i| nodes[i].can_serve())
            .collect();
        assert!(
            serving.len() <= 1,
            "split-brain among {idxs:?}: {serving:?}"
        );
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
    await_until(
        20,
        "commit stalled after learner died (phantom quorum coupling)",
        || c.nodes[leader].commit() > commit0,
    );

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
    assert!(
        new_leader < 3,
        "the new leader must be a voter, got {new_leader}"
    );
    assert!(
        !c.nodes[learner_idx].is_leader(),
        "the learner must never lead"
    );
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

/// M6 Task 8 Step 4 — a FRESH learner joins a cluster whose leader has PURGED its
/// log prefix, and catches up by installing the shipped snapshot then tail-replaying.
///
/// A single voter (deterministic leader) drives megabytes through a small ring,
/// publishes a snapshot floor (the service builder is stood in for by writing the
/// cnc position + a snapshot file, as `purge_safety.rs` does — `uc_node` never
/// parses the file), and purges `[0, floor)`. A learner then starts with a FRESH
/// instance dir: it NAKs from 0 BELOW the leader's ring floor, the leader cannot
/// serve the purged prefix from ring or journal so it upgrades to a snapshot
/// SESSION (Task 6); the learner adopts the shipped floor (AdoptFloor) — seeding
/// the leader's term-map lineage so reconcile finds the below-floor common prefix
/// (Task 8 fix) instead of trying to truncate below the floor — and tail-replays
/// the retained `[floor, append)`, reaching a frontier it could NEVER have reached
/// by replay alone (those bytes are gone). The leader's commit never gates on it.
///
/// M7 Task 9: the pre-seeded config also adds a learner absent from the
/// joiner's own boot seed, so the fiat-installed config genuinely diverges
/// from the seed the joiner started with — proving the install rebuilds peer
/// routing (`rebuild_net_for_config`), not just the SM/record/cnc version.
#[test]
fn fresh_learner_joins_a_purged_leader_via_snapshot_session() {
    let _g = serialize();
    let dir = tempfile::Builder::new()
        .prefix("uc2-learner-join-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    const SEG: u64 = 64 * 1024;
    let app = "learner-join";

    let v_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let l_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let v_addr = v_sock.local_addr().unwrap();
    let l_addr = l_sock.local_addr().unwrap();
    let members = vec![(0u32, v_addr)];
    let learners = vec![(1u32, l_addr)];

    let cfg = |id: NodeId, sock_addr: SocketAddr, d: PathBuf| NodeConfig {
        id,
        members: members.clone(),
        learners: learners.clone(),
        bind: sock_addr,
        instance_dir: d,
        app_id: app.into(),
        // A SMALL ring so a fresh learner's NAK from 0 falls BELOW the ring floor
        // (durable - capacity) into the PURGED journal region → snapshot session.
        buffer_bytes: 1 << 18,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 0xC0FFEE ^ id as u64,
        faults: FaultConfig::default(),
        purge: PurgePolicy::BelowSnapshot { slack_bytes: 0 },
        journal_segment_bytes: SEG,
        crypto: uc_node::CryptoConfig::Disabled,
        // Wire 0.7.0 (Ruling 1): a `none_for_tests` node can no longer ship
        // (or accept) a snapshot session — see `spawn_applied_mirror`'s doc.
        services: uc_node::ServicesConfig::single("fsm0"),
    };

    let v_dir = dir.path().join("v0");
    // M7 Task 9: the pre-seeded v1 config now ADDS a member absent from both
    // nodes' boot seed (`members`/`learners` above) — a distinct learner id 9
    // at an addr neither node ever binds (fire-and-forget UDP fan-out to an
    // unbound port is harmless, same convention `reconfig.rs` uses for
    // members that don't need a real running node). This is what turns the
    // T7-review gap real: the joiner's snapshot-installed config genuinely
    // DIFFERS from its own boot seed (not just a version bump on identical
    // membership), so converging `config_version` alone can't distinguish
    // "adopted the config" from "adopted the config AND rebuilt routing" —
    // the peer-band assertion below is what actually pins the rebuild.
    let extra_learner_id: NodeId = 9;
    let extra_learner_addr: SocketAddr = "127.0.0.1:59909".parse().unwrap();
    // M7 Task 6: pre-seed the voter's `ConfigRecord` at version 1 BEFORE it
    // boots, so the config the snapshot session carries is genuinely
    // non-genesis (and, as of Task 9, genuinely different membership) — the
    // only way to prove the wire carry (and now the routing rebuild) end to
    // end rather than asserting a trivial 0 == 0 / seed == seed coincidence.
    let stored_member = |id: NodeId, a: SocketAddr| StoredMember {
        id,
        ip: match a.ip() {
            std::net::IpAddr::V4(v4) => u32::from(v4),
            std::net::IpAddr::V6(_) => panic!("ipv4 only"),
        },
        port: a.port(),
    };
    std::fs::create_dir_all(v_dir.join("state")).unwrap();
    {
        let cfg_v1 = StoredConfig {
            version: 1,
            voters: members
                .iter()
                .map(|(id, a)| stored_member(*id, *a))
                .collect(),
            learners: learners
                .iter()
                .map(|(id, a)| stored_member(*id, *a))
                .chain(std::iter::once(stored_member(
                    extra_learner_id,
                    extra_learner_addr,
                )))
                .collect(),
            tombstones: Vec::new(),
        };
        let rec = ConfigRecord {
            position: 0,
            config: cfg_v1.clone(),
            prev_position: 0,
            prev: cfg_v1,
        };
        NodeState::open(&v_dir.join("state"))
            .unwrap()
            .store_config_record(&rec)
            .unwrap();
    }

    let voter =
        Node::start_with_socket(cfg(0, v_addr, v_dir.clone()), v_sock).expect("start voter");
    await_until(30, "voter serves", || voter.can_serve());
    assert_eq!(
        voter.config_version(),
        1,
        "voter booted from the pre-seeded v1 record"
    );

    // Publish a snapshot floor + a real snapshot file for the sender to ship.
    let cnc = CncPage::open_file(&v_dir.join("cnc2.dat"), app).expect("open voter cnc");

    // See `spawn_applied_mirror`'s doc: row 0 is now a REAL declared FSM
    // ("fsm0"), so FSM-lag admission is live against `applied` — mirror it
    // from `durable` for the duration of the raw submit loop below.
    let (mirror_stop, mirror_handle) = spawn_applied_mirror(std::sync::Arc::clone(&cnc), 0);

    for i in 0u64..24000 {
        let mut p = vec![0u8; PAYLOAD];
        p[..8].copy_from_slice(&i.to_le_bytes());
        loop {
            match voter.submit(p.clone()) {
                Ok(()) => break,
                Err(_) => std::thread::yield_now(),
            }
        }
    }
    await_until(30, "voter quiesced", || {
        let c = voter.counters();
        let a = c.append.load_acquire();
        a > 0 && c.commit.load_acquire() == a && c.durable.load_acquire() == a
    });
    mirror_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    mirror_handle.join().unwrap();

    let durable = voter.counters().durable.load_acquire();
    // Frame-aligned (a real service publishes a snapshot at an apply boundary —
    // a 128 B frame end for these 96 B payloads); a mid-frame floor would land the
    // journal-replay datagram below the adopted position and be dropped as a dup.
    let floor = (durable / 2) / 128 * 128;
    assert!(
        floor > SEG,
        "need >1 segment below the floor (durable={durable})"
    );
    let snap_dir = v_dir.join("snapshots").join("0");
    std::fs::create_dir_all(&snap_dir).unwrap();
    std::fs::write(
        snap_dir.join(format!("snap-{floor}.ultsnap")),
        vec![0x5Au8; 4096],
    )
    .unwrap();
    // M14c: the source closure ships each declared id's own newest artifact, so
    // the test must publish the SLOT the service owns as well as the page-1
    // aggregate the node would normally derive from it — this voter now
    // declares a real "fsm0" (wire 0.7.0 Ruling 1: a `none_for_tests` node
    // can no longer ship a snapshot session at all, see
    // `spawn_applied_mirror`'s doc), but still has no REAL service attached
    // to publish the aggregate on its own, so the test publishes it by hand.
    cnc.service_slot(0).snapshot_pos.store_release(floor);
    cnc.snapshots().service_snapshot_pos.store_release(floor);

    await_until(30, "voter purged its prefix", || {
        voter.archive_first_base() > 0
    });
    let first_base = voter.archive_first_base();
    assert!(
        first_base > 0,
        "the prefix must be gone so replay-from-0 is impossible"
    );
    let frontier = voter.counters().append.load_acquire();

    // A FRESH learner joins with no prior state.
    let l_dir = dir.path().join("l1");
    let learner =
        Node::start_with_socket(cfg(1, l_addr, l_dir.clone()), l_sock).expect("start learner");

    // It cannot replay `[0, first_base)` (purged) — the ONLY way it reaches the
    // frontier is the snapshot session + AdoptFloor (+ lineage seed) + tail replay.
    await_until(40, "learner caught up across the purged prefix", || {
        learner.counters().durable.load_acquire() >= frontier
            && learner.counters().commit.load_acquire() >= frontier
    });
    assert!(
        learner.archive_first_base() >= first_base,
        "the learner must have adopted the shipped snapshot floor, not replayed from 0"
    );
    assert!(!learner.is_leader(), "a learner never leads");

    // Task 10 regression canary (NOT discriminating): after the join the
    // pending mirror reads 0. On its own this cannot prove the fiat install's
    // `store_config_pending(false)` ran — a fresh joiner's SM has nothing
    // pending, so do_work's periodic mirror-clear (step 12) holds the mirror
    // at 0 within one duty cycle regardless. The discriminating proof (the
    // periodic clear BLOCKED in the same cycle, so only the fiat store line
    // can clear it) is the node.rs harness test
    // `fiat_snapshot_install_clears_config_pending_mirror`.
    let joiner_cnc = CncPage::open_file(&l_dir.join("cnc2.dat"), app).expect("open learner cnc");
    assert_eq!(
        joiner_cnc.config_pending(),
        0,
        "a fiat install is never pending — the cnc mirror must read clear"
    );

    // M7 Task 6: the snapshot session carries the leader's config alongside its
    // lineage (`SnapBeginBody.config`) — the joiner decodes + adopts it by fiat
    // (`adopt_snapshot_config`) on install completion, so its `config_version`
    // converges with the leader's PRE-SEEDED v1 (not the learner's own genesis
    // v0) — a real cross-node version bump, not a trivial 0 == 0 coincidence.
    assert_eq!(
        voter.config_version(),
        1,
        "sanity: voter still reports the pre-seeded version"
    );
    assert_eq!(
        learner.config_version(),
        voter.config_version(),
        "the joiner's config_version must converge with the leader's after install"
    );

    // M7 Task 9: the fiat install must also rebuild peer routing — the
    // joiner's own boot seed never contained `extra_learner_id`, so the ONLY
    // way its cnc peer band knows about it is `rebuild_net_for_config` having
    // run off the snapshot-installed config (`rebuild_peer_maps` +
    // `publish_peer_band`), not the stale seed it started from. This is the
    // observable, stable proof the TODO's routing gap asked for — cheaper and
    // more direct than trying to observe it via the sender's fan-out.
    let learner_cnc = CncPage::open_file(&l_dir.join("cnc2.dat"), app).expect("open learner cnc");
    let mut found_extra_learner = false;
    for i in 0..CNC_MAX_PEER_SLOTS {
        let raw = learner_cnc.peer_slot(i).id_and_role.load_acquire();
        if raw == 0 {
            continue;
        }
        let id = (raw >> 8) as u32;
        let role = (raw & 0xff) as u8;
        if id == extra_learner_id {
            assert_eq!(
                role, CNC_PEER_ROLE_LEARNER,
                "joiner's peer slot for id {extra_learner_id} has role {role}, want learner"
            );
            found_extra_learner = true;
        }
    }
    assert!(
        found_extra_learner,
        "joiner's peer band never picked up id {extra_learner_id} from the installed \
         config — the snapshot-fiat install did not rebuild peer routing"
    );

    // Final-review fix (Item 1): assert the SNAP_BEGIN config-carry cache
    // itself converged — not just `config_version`/peer-routing, which are
    // proxies. Before this fix, `maybe_adopt_incoming_snapshot`'s fiat-install
    // block persisted the record and rebuilt peer routing but never refreshed
    // `config_bytes`, so the joiner's cache would still hold ITS OWN stale
    // boot-seed derivation (version 0, no `extra_learner_id`) rather than the
    // leader's installed v1 config — meaning a below-floor rejoiner that later
    // became leader would ship the WRONG config to the next joiner. Compare
    // the decoded MEMBERSHIP content, not the raw bytes: `prev_position` is a
    // deliberately audit-trail-only field (per `cluster_to_wire`'s doc) and
    // legitimately differs here — the voter's cache carries its genuinely
    // historical prev_position (0, from the pre-seeded record), while the
    // joiner's fiat wholesale-replace install collapses prev_position to the
    // installed floor itself (`rebuild_net_for_config(&cfg, pos)` — see the
    // fiat-install call site's comment); the two are not supposed to match.
    let voter_decoded =
        decode_config(&voter.snapshot_config_bytes()).expect("voter's cached config must decode");
    let learner_decoded = decode_config(&learner.snapshot_config_bytes())
        .expect("joiner's cached config must decode");
    assert_eq!(
        learner_decoded.version, voter_decoded.version,
        "cache version must converge"
    );
    assert_eq!(
        learner_decoded.voters, voter_decoded.voters,
        "cache voters must converge"
    );
    assert_eq!(
        learner_decoded.learners, voter_decoded.learners,
        "cache learners must converge"
    );
    assert_eq!(
        learner_decoded.tombstones, voter_decoded.tombstones,
        "cache tombstones must converge"
    );
    assert_eq!(
        learner_decoded.version, 1,
        "decoded cache must carry the installed v1 config"
    );
    assert!(
        learner_decoded
            .learners
            .iter()
            .any(|m| m.id == extra_learner_id),
        "decoded cache must contain the extra learner from the installed config"
    );

    learner.stop();
    voter.stop();
}

/// A snapshot-capable RAW state machine (bytes in, bytes out — no serde, so the
/// test can submit plain byte payloads through `Node::submit` exactly as the
/// single-FSM join test above does). `freeze` pins `(total, last_applied)`.
#[derive(Default)]
struct SumSm {
    total: u64,
    last: Option<u64>,
}

impl uc_service::RawStateMachine for SumSm {
    const NAME: &'static str = "sum";

    fn apply(&mut self, ctx: &mut uc_service::ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
        if cmd.len() >= 8 {
            self.total = self
                .total
                .wrapping_add(u64::from_le_bytes(cmd[..8].try_into().unwrap()));
        }
        self.last = Some(ctx.position);
        out.extend_from_slice(&self.total.to_le_bytes());
    }
    fn query(&self, _q: &[u8], out: &mut Vec<u8>) {
        out.extend_from_slice(&self.total.to_le_bytes());
    }
    fn last_applied(&self) -> Option<u64> {
        self.last
    }
}

impl uc_service::SnapshotStateMachine for SumSm {
    type SnapshotHandle = Vec<u8>;
    fn freeze(&self) -> Result<(Vec<u8>, u64), uc_service::SnapshotError> {
        let pos = self.last.unwrap_or(0);
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&self.total.to_le_bytes());
        buf.extend_from_slice(&pos.to_le_bytes());
        Ok((buf, pos))
    }
    fn stream_snapshot(
        handle: Vec<u8>,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc_service::SnapshotError> {
        dst.write_all(&handle)?;
        Ok(())
    }
    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc_service::SnapshotError> {
        let mut buf = Vec::new();
        src.read_to_end(&mut buf)?;
        assert!(buf.len() >= 16, "a SumSm artifact is 16 bytes");
        self.total = u64::from_le_bytes(buf[..8].try_into().unwrap());
        self.last = Some(position);
        Ok(position)
    }
}

fn start_sum_service(dir: &Path, app: &str) -> uc_service::Service<SumSm> {
    let cfg =
        uc_service::ServiceConfig::new(dir, app).snapshot_policy(uc_service::SnapshotPolicy {
            interval_bytes: 256 * 1024,
        });
    uc_service::ServiceBuilder::new(cfg, SumSm::default())
        .start_with_snapshots()
        .expect("service start")
}

/// FSM identity: `SumSm` is raw-tier (`RawStateMachine` directly, not
/// `StateMachine`), so `uc_service::Tagged` — which only forwards the typed
/// tier — cannot wrap it (see Task 5's ruling on `apply_bench`'s `TaggedRaw`).
/// This is the same shape, local to this file, so a second FSM can attach at
/// row 1 (declared name `"fsm1"`) with the same raw logic.
#[derive(Default)]
struct TaggedSum(SumSm);
impl uc_service::RawStateMachine for TaggedSum {
    const NAME: &'static str = "fsm1";
    fn apply(&mut self, ctx: &mut uc_service::ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
        self.0.apply(ctx, cmd, out)
    }
    fn query(&self, q: &[u8], out: &mut Vec<u8>) {
        self.0.query(q, out)
    }
    fn last_applied(&self) -> Option<u64> {
        self.0.last_applied()
    }
}
impl uc_service::SnapshotStateMachine for TaggedSum {
    type SnapshotHandle = Vec<u8>;
    fn freeze(&self) -> Result<(Vec<u8>, u64), uc_service::SnapshotError> {
        self.0.freeze()
    }
    fn stream_snapshot(
        handle: Vec<u8>,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc_service::SnapshotError> {
        SumSm::stream_snapshot(handle, dst)
    }
    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc_service::SnapshotError> {
        self.0.install_snapshot(position, src)
    }
}

fn start_sum_service_row1(dir: &Path, app: &str) -> uc_service::Service<TaggedSum> {
    let cfg =
        uc_service::ServiceConfig::new(dir, app).snapshot_policy(uc_service::SnapshotPolicy {
            interval_bytes: 256 * 1024,
        });
    uc_service::ServiceBuilder::new(cfg, TaggedSum::default())
        .start_with_snapshots()
        .expect("service start")
}

/// M14c (spec §7.3/§14.3): a fresh learner joins a PURGED **two-FSM** leader.
/// One session carries BOTH artifacts (one `SNAP_BEGIN` per declared id, chunk
/// offsets stream-global); the learner writes each to `snapshots/<id>/`, adopts
/// the floor only once both landed, and each of its FSMs installs its OWN
/// artifact and tail-replays. The first test anywhere that combines two FSMs
/// with a below-floor join.
#[test]
fn fresh_learner_joins_a_purged_two_fsm_leader_and_both_fsms_converge() {
    let _g = serialize();
    let dir = tempfile::Builder::new()
        .prefix("uc2-learner-2fsm-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    const SEG: u64 = 64 * 1024;
    let app = "learner-join-2fsm";

    let v_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let l_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let v_addr = v_sock.local_addr().unwrap();
    let l_addr = l_sock.local_addr().unwrap();
    let members = vec![(0u32, v_addr)];
    let learners = vec![(1u32, l_addr)];

    let cfg = |id: NodeId, sock_addr: SocketAddr, d: PathBuf| NodeConfig {
        id,
        members: members.clone(),
        learners: learners.clone(),
        bind: sock_addr,
        instance_dir: d,
        app_id: app.into(),
        buffer_bytes: 1 << 18, // small ring: the learner's NAK from 0 falls below it
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 0xC0FFEE ^ id as u64,
        faults: FaultConfig::default(),
        purge: PurgePolicy::BelowSnapshot { slack_bytes: 4096 },
        journal_segment_bytes: SEG,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::from_names(
            &[<SumSm as uc_service::RawStateMachine>::NAME, "fsm1"],
            None,
        )
        .unwrap(),
    };

    let v_dir = dir.path().join("v0");
    let voter =
        Node::start_with_socket(cfg(0, v_addr, v_dir.clone()), v_sock).expect("start voter");
    let _v0 = start_sum_service(&v_dir, app);
    let _v1 = start_sum_service_row1(&v_dir, app);
    await_until(30, "voter serves", || voter.can_serve());

    // Drive well past one snapshot interval per FSM so both slots publish a
    // position and the node floor (their min) leaves the journal's first
    // segment behind.
    for i in 0u64..24000 {
        let mut p = vec![0u8; PAYLOAD];
        p[..8].copy_from_slice(&i.to_le_bytes());
        loop {
            match voter.submit(p.clone()) {
                Ok(()) => break,
                Err(_) => std::thread::yield_now(),
            }
        }
    }
    await_until(30, "voter quiesced", || {
        let c = voter.counters();
        let a = c.append.load_acquire();
        a > 0 && c.commit.load_acquire() == a && c.durable.load_acquire() == a
    });

    let v_cnc = CncPage::open_file(&v_dir.join("cnc2.dat"), app).expect("open voter cnc");
    await_until(30, "both FSMs published a snapshot", || {
        v_cnc.service_slot(0).snapshot_pos.load_acquire() > SEG
            && v_cnc.service_slot(1).snapshot_pos.load_acquire() > SEG
    });
    await_until(30, "voter purged its prefix", || {
        voter.archive_first_base() > 0
    });
    let first_base = voter.archive_first_base();
    let frontier = voter.counters().append.load_acquire();
    let commit = voter.counters().commit.load_acquire();

    // A FRESH learner joins with no prior state — and with its own two FSMs.
    let l_dir = dir.path().join("l1");
    let learner =
        Node::start_with_socket(cfg(1, l_addr, l_dir.clone()), l_sock).expect("start learner");
    let _l0 = start_sum_service(&l_dir, app);
    let _l1 = start_sum_service_row1(&l_dir, app);

    await_until(60, "learner caught up across the purged prefix", || {
        learner.counters().durable.load_acquire() >= frontier
            && learner.counters().commit.load_acquire() >= frontier
    });
    assert!(
        learner.archive_first_base() >= first_base,
        "the learner must have adopted the shipped snapshot floor, not replayed from 0"
    );

    // Both artifacts landed, each in its OWN directory — and each AT THE
    // VOTER'S OWN SNAPSHOT POSITION for that id (M14c2 T10b fix 1). Checking
    // non-emptiness alone was not an oracle: a file of any provenance, at any
    // position, passed it. The position is read from the artifact's NAME
    // (`snap-<pos>.ultsnap` — the tag the harness exposes; the learner's slot
    // `snapshot_pos` cannot stand in, because that word is written by the
    // learner's OWN builder, not by an install).
    //
    // `contains`, not `==`: the learner runs its own snapshot-capable
    // services, which publish LOCALLY-built artifacts into the same directory
    // once they have applied enough, so extra positions there are legitimate.
    // The shipped one being ABSENT is not. (Measured on this fixture: each
    // directory holds exactly one artifact, the shipped one — the learner's own
    // builders have not tripped by then.)
    //
    // Measured caveat on how to mutation-test this: BOTH declared FSMs
    // snapshot at the SAME position here (2 883 456 — they apply the same log
    // with the same interval, so their builders trip on the same applied byte),
    // so comparing against the OTHER id's position is a no-op mutation. Perturb
    // the position itself to check this assertion still bites.
    for id in [0u8, 1] {
        let d = l_dir.join("snapshots").join(id.to_string());
        let installed: Vec<u64> = std::fs::read_dir(&d)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let n = e.file_name();
                let n = n.to_string_lossy();
                n.strip_prefix("snap-")
                    .and_then(|rest| rest.strip_suffix(".ultsnap"))
                    .and_then(|pos| pos.parse::<u64>().ok())
            })
            .collect();
        assert!(
            !installed.is_empty(),
            "learner {d:?} holds no installed artifact"
        );
        let shipped = v_cnc.service_slot(id as usize).snapshot_pos.load_acquire();
        assert!(
            installed.contains(&shipped),
            "learner {d:?} holds artifacts at {installed:?}, but NOT the voter's FSM {id} \
             artifact at position {shipped} — the session did not deliver this id's artifact"
        );
    }

    // And both learner FSMs reached the leader's commit — each installed its own
    // artifact and tail-replayed the retained window.
    let l_cnc = CncPage::open_file(&l_dir.join("cnc2.dat"), app).expect("open learner cnc");
    await_until(
        60,
        "both learner FSMs applied to the leader's commit",
        || {
            l_cnc.service_slot(0).applied.load_acquire() >= commit
                && l_cnc.service_slot(1).applied.load_acquire() >= commit
        },
    );
    assert_eq!(
        learner.snapshot_session_refusals(),
        (0, 0, 0),
        "matching declared identities/versions and a wire-0.7.0 peer: no refusal may fire"
    );
    // M14c2 T10b: the two artifacts landing is not by itself the M14c claim —
    // they must have arrived through the snapshot SESSION path, whole. Only the
    // SENDER counts sessions, so this reads the voter's counter.
    //
    // The deferral asked for `snap_sessions == 1` here; that is NOT true at
    // cluster scale and never was. A fresh learner re-NAKs below the floor until
    // its adoption sticks, so the leader opens the session more than once —
    // measured 3, stably, across repeated runs of this test. "One session
    // carries the whole set" — one `SNAP_BEGIN` per declared id, stream-global
    // chunk offsets, even under 20 % loss — is pinned exactly, at the seam that
    // owns it, by `uc_net/tests/snapshot_session.rs::
    // a_two_artifact_stream_lands_in_per_id_dirs_under_chunk_loss`.
    //
    // The DISCRIMINATING oracle here is the per-id position check above: both
    // artifacts on disk at the VOTER's positions is what says the session
    // delivered this id's artifact rather than something else producing a file.
    assert!(
        voter
            .observability()
            .sender
            .snap_sessions
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 1,
        "the artifacts must have come from a snapshot session, not a log replay"
    );
    // A cheap guard, NOT a proof of anything: on a converging run neither of
    // these can plausibly fire (the intake timeout is 60 s against a
    // convergence measured in seconds), so treat a non-zero here as "the
    // transfer plane hit an I/O error or a timeout", nothing more.
    assert_eq!(
        (
            learner
                .crypto_stats()
                .snap_intake_abandoned
                .load(std::sync::atomic::Ordering::Relaxed),
            learner
                .crypto_stats()
                .snap_intake_io_failures
                .load(std::sync::atomic::Ordering::Relaxed),
        ),
        (0, 0),
        "guard: no intake I/O error and no intake timeout fired during the join"
    );
    assert!(!learner.is_leader(), "a learner never leads");

    learner.stop();
    voter.stop();
}

/// Restores the process-global log sink when it goes out of scope — including
/// on a panic, so a failing assertion below cannot leave every LATER test in
/// this binary appending to a capture buffer nobody drains.
struct CaptureGuard;

impl Drop for CaptureGuard {
    fn drop(&mut self) {
        uc_node::obs::log::stderr_for_tests();
    }
}

/// M14c (spec §8/§14.3), controller amendment 2: a joiner whose declared FSM
/// set differs from the leader's must refuse the snapshot session **by name** —
/// the counter increments AND the node emits `snapshot_session_refused` with
/// `reason = "identity mismatch"`. Refusing keeps the joiner stalled-but-safe
/// (it re-NAKs forever) instead of installing a set that covers only some of its
/// FSMs; the log line plus the counter are what tell an operator which it is.
#[test]
fn a_declared_set_mismatch_refuses_the_session_and_names_it_in_a_log_line() {
    let _g = serialize();
    let buf = uc_node::obs::log::capture_for_tests();
    let _restore = CaptureGuard;
    let dir = tempfile::Builder::new()
        .prefix("uc2-learner-mismatch-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    const SEG: u64 = 64 * 1024;
    let app = "learner-mismatch";

    let v_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let l_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let v_addr = v_sock.local_addr().unwrap();
    let l_addr = l_sock.local_addr().unwrap();
    let members = vec![(0u32, v_addr)];
    let learners = vec![(1u32, l_addr)];

    let cfg = |id: NodeId, sock_addr: SocketAddr, d: PathBuf, services| NodeConfig {
        id,
        members: members.clone(),
        learners: learners.clone(),
        bind: sock_addr,
        instance_dir: d,
        app_id: app.into(),
        buffer_bytes: 1 << 18,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 0xC0FFEE ^ id as u64,
        faults: FaultConfig::default(),
        purge: PurgePolicy::BelowSnapshot { slack_bytes: 0 },
        journal_segment_bytes: SEG,
        crypto: uc_node::CryptoConfig::Disabled,
        services,
    };

    // The leader declares row 0 as "fsm0" (a REAL name — wire 0.7.0 Ruling 1:
    // a `none_for_tests` node can no longer ship a snapshot session at all,
    // see `spawn_applied_mirror`'s doc), so every SNAP_BEGIN it sends carries
    // `identity[0] = hash("fsm0")`.
    let v_dir = dir.path().join("v0");
    let voter = Node::start_with_socket(
        cfg(
            0,
            v_addr,
            v_dir.clone(),
            uc_node::ServicesConfig::single("fsm0"),
        ),
        v_sock,
    )
    .expect("start voter");
    await_until(30, "voter serves", || voter.can_serve());

    let cnc_for_mirror =
        CncPage::open_file(&v_dir.join("cnc2.dat"), app).expect("open voter cnc for mirror");
    let (mirror_stop, mirror_handle) = spawn_applied_mirror(cnc_for_mirror, 0);

    for i in 0u64..24000 {
        let mut p = vec![0u8; PAYLOAD];
        p[..8].copy_from_slice(&i.to_le_bytes());
        loop {
            match voter.submit(p.clone()) {
                Ok(()) => break,
                Err(_) => std::thread::yield_now(),
            }
        }
    }
    await_until(30, "voter quiesced", || {
        let c = voter.counters();
        let a = c.append.load_acquire();
        a > 0 && c.commit.load_acquire() == a && c.durable.load_acquire() == a
    });
    mirror_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    mirror_handle.join().unwrap();

    // Publish a floor + a real artifact for FSM 0.
    let cnc = CncPage::open_file(&v_dir.join("cnc2.dat"), app).expect("open voter cnc");
    let durable = voter.counters().durable.load_acquire();
    let floor = (durable / 2) / 128 * 128;
    assert!(
        floor > SEG,
        "need >1 segment below the floor (durable={durable})"
    );
    let snap_dir = v_dir.join("snapshots").join("0");
    std::fs::create_dir_all(&snap_dir).unwrap();
    std::fs::write(
        snap_dir.join(format!("snap-{floor}.ultsnap")),
        vec![0x5Au8; 4096],
    )
    .unwrap();
    cnc.service_slot(0).snapshot_pos.store_release(floor);
    cnc.snapshots().service_snapshot_pos.store_release(floor);
    await_until(30, "voter purged its prefix", || {
        voter.archive_first_base() > 0
    });

    // The joiner declares {0, 1} — a genuine `[services] names` mismatch.
    let l_dir = dir.path().join("l1");
    let learner = Node::start_with_socket(
        cfg(
            1,
            l_addr,
            l_dir.clone(),
            uc_node::ServicesConfig::from_names(&["fsm0", "fsm1"], None).unwrap(),
        ),
        l_sock,
    )
    .expect("start learner");

    await_until(60, "the joiner refused the mismatched session", || {
        learner.snapshot_session_refusals().1 >= 1
    });
    assert_eq!(
        learner.snapshot_session_refusals().0,
        0,
        "a wire-0.7.0 peer must never count as 'peer wire <= 0.6.0'"
    );
    await_until(30, "the refusal was named in a log line", || {
        let captured = String::from_utf8_lossy(&buf.lock().unwrap()).into_owned();
        captured.contains("snapshot_session_refused") && captured.contains("identity mismatch")
    });
    // Stalled-but-safe: nothing was half-installed under the joiner's own root.
    // The directory itself always exists — `Node::start` creates `snapshots/<id>/`
    // for every DECLARED id — so the old `!exists() || empty` disjunct's first
    // half was dead and could only have weakened the check (M14c2 T10b). What
    // has to hold is that the directory is EMPTY.
    let refused_dir = l_dir.join("snapshots").join("1");
    assert!(
        refused_dir.is_dir(),
        "the joiner declares id 1, so its snapshot dir exists"
    );
    assert_eq!(
        std::fs::read_dir(&refused_dir)
            .expect("read the joiner's snapshot dir")
            .count(),
        0,
        "a refused session must leave no artifact behind"
    );

    learner.stop();
    voter.stop();
}

/// The most recent captured log line naming `event` (`"event":"<event>"`),
/// for asserting on its other fields. Panics if none was captured — every
/// caller pairs this with an `await_until` on the counter that says it is
/// safe to look, so a miss here is a genuine gap, not a timing race.
fn last_obs_record(buf: &std::sync::Arc<Mutex<Vec<u8>>>, event: &str) -> String {
    let captured =
        String::from_utf8_lossy(&buf.lock().unwrap_or_else(|e| e.into_inner())).into_owned();
    let needle = format!("\"event\":\"{event}\"");
    captured
        .lines()
        .rfind(|l| l.contains(needle.as_str()))
        .unwrap_or_else(|| panic!("no captured line names event {event:?}:\n{captured}"))
        .to_string()
}

/// Wire 0.7.0 (spec §8, Task 9): the SAME names, declared in the OTHER
/// ORDER, are not the same declared set — identity is positional. Leader
/// declares `["sum", "fsm1"]`; joiner declares `["fsm1", "sum"]`. Every name
/// is individually valid on both sides (this is NOT
/// `a_declared_set_mismatch_...`'s different-cardinality case), so a naive
/// SET comparison would wrongly accept it. Refused at row 0 (the first
/// differing row) with `ours = hash("fsm1")` (the joiner's own row-0 name)
/// and `theirs = hash("sum")` (the leader's row-0 name).
#[test]
fn a_joiner_whose_rows_are_named_in_the_other_order_is_refused_by_name_and_stalls() {
    let _g = serialize();
    let buf = uc_node::obs::log::capture_for_tests();
    let _restore = CaptureGuard;
    let dir = tempfile::Builder::new()
        .prefix("uc2-learner-order-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    const SEG: u64 = 64 * 1024;
    let app = "learner-order-mismatch";

    let v_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let l_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let v_addr = v_sock.local_addr().unwrap();
    let l_addr = l_sock.local_addr().unwrap();
    let members = vec![(0u32, v_addr)];
    let learners = vec![(1u32, l_addr)];

    let cfg = |id: NodeId, sock_addr: SocketAddr, d: PathBuf, services| NodeConfig {
        id,
        members: members.clone(),
        learners: learners.clone(),
        bind: sock_addr,
        instance_dir: d,
        app_id: app.into(),
        buffer_bytes: 1 << 18,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 0xC0FFEE ^ id as u64,
        faults: FaultConfig::default(),
        purge: PurgePolicy::BelowSnapshot { slack_bytes: 0 },
        journal_segment_bytes: SEG,
        crypto: uc_node::CryptoConfig::Disabled,
        services,
    };

    // The leader declares row 0 = "sum", row 1 = "fsm1".
    let v_dir = dir.path().join("v0");
    let voter = Node::start_with_socket(
        cfg(
            0,
            v_addr,
            v_dir.clone(),
            uc_node::ServicesConfig::from_names(&["sum", "fsm1"], None).unwrap(),
        ),
        v_sock,
    )
    .expect("start voter");
    await_until(30, "voter serves", || voter.can_serve());

    // Two declared rows both need admission-control's `applied` mirrored, or
    // the submit loop below deadlocks against the FSM-lag bound (same reason
    // `a_declared_set_mismatch_...` mirrors row 0).
    let cnc_for_mirror0 =
        CncPage::open_file(&v_dir.join("cnc2.dat"), app).expect("open voter cnc for mirror 0");
    let cnc_for_mirror1 =
        CncPage::open_file(&v_dir.join("cnc2.dat"), app).expect("open voter cnc for mirror 1");
    let (mirror0_stop, mirror0_handle) = spawn_applied_mirror(cnc_for_mirror0, 0);
    let (mirror1_stop, mirror1_handle) = spawn_applied_mirror(cnc_for_mirror1, 1);

    for i in 0u64..24000 {
        let mut p = vec![0u8; PAYLOAD];
        p[..8].copy_from_slice(&i.to_le_bytes());
        loop {
            match voter.submit(p.clone()) {
                Ok(()) => break,
                Err(_) => std::thread::yield_now(),
            }
        }
    }
    await_until(30, "voter quiesced", || {
        let c = voter.counters();
        let a = c.append.load_acquire();
        a > 0 && c.commit.load_acquire() == a && c.durable.load_acquire() == a
    });
    mirror0_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    mirror0_handle.join().unwrap();
    mirror1_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    mirror1_handle.join().unwrap();

    // Publish a floor + a real (hand-staged) artifact for BOTH declared rows —
    // the sender's `snapshot_set_for` refuses (missing artifact) unless every
    // declared id has one, so a two-row leader needs two.
    let cnc = CncPage::open_file(&v_dir.join("cnc2.dat"), app).expect("open voter cnc");
    let durable = voter.counters().durable.load_acquire();
    let floor = (durable / 2) / 128 * 128;
    assert!(
        floor > SEG,
        "need >1 segment below the floor (durable={durable})"
    );
    for id in [0u8, 1] {
        let snap_dir = v_dir.join("snapshots").join(id.to_string());
        std::fs::create_dir_all(&snap_dir).unwrap();
        std::fs::write(
            snap_dir.join(format!("snap-{floor}.ultsnap")),
            vec![0x5Au8; 4096],
        )
        .unwrap();
        cnc.service_slot(id as usize)
            .snapshot_pos
            .store_release(floor);
    }
    cnc.snapshots().service_snapshot_pos.store_release(floor);
    await_until(30, "voter purged its prefix", || {
        voter.archive_first_base() > 0
    });

    // The joiner declares the SAME two names, in the OTHER order: row 0 =
    // "fsm1", row 1 = "sum".
    let l_dir = dir.path().join("l1");
    let learner = Node::start_with_socket(
        cfg(
            1,
            l_addr,
            l_dir.clone(),
            uc_node::ServicesConfig::from_names(&["fsm1", "sum"], None).unwrap(),
        ),
        l_sock,
    )
    .expect("start learner");

    await_until(
        60,
        "the joiner refused the order-mismatched session",
        || learner.snapshot_session_refusals().1 >= 1,
    );
    assert_eq!(
        learner.snapshot_session_refusals().2,
        0,
        "an ORDER mismatch is an identity refusal, never a version refusal"
    );
    let r = learner
        .crypto_stats()
        .identity_refusal
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .expect("identity refusal detail recorded");
    assert_eq!(r.row, 0, "row 0 is the first (and only) differing row here");
    assert_eq!(r.kind, RefusalKind::Identity);
    assert_eq!(
        r.ours,
        FsmName::parse("fsm1").unwrap().hash(),
        "the joiner's OWN row-0 name is fsm1"
    );
    assert_eq!(
        r.theirs,
        FsmName::parse("sum").unwrap().hash(),
        "the leader's row-0 name is sum"
    );

    // Stalled-but-safe: nothing was half-installed under either declared row.
    for id in [0u8, 1] {
        let refused_dir = l_dir.join("snapshots").join(id.to_string());
        assert!(
            refused_dir.is_dir(),
            "the joiner declares id {id}, so its snapshot dir exists"
        );
        assert_eq!(
            std::fs::read_dir(&refused_dir)
                .expect("read the joiner's snapshot dir")
                .count(),
            0,
            "a refused session must leave no artifact behind"
        );
    }

    await_until(30, "the refusal was named in a log line", || {
        let captured = String::from_utf8_lossy(&buf.lock().unwrap()).into_owned();
        captured.contains("snapshot_session_refused") && captured.contains("identity mismatch")
    });
    let rec = last_obs_record(&buf, "snapshot_session_refused");
    assert!(
        rec.contains("\"ours\":\"fsm1\"") && rec.contains("\"theirs\":\"sum\""),
        "{rec}"
    );

    learner.stop();
    voter.stop();
}

/// Wire 0.7.0 (spec §8, Task 9): same names on both sides — no identity
/// refusal — but the joiner's attached service reports a DIFFERENT packed
/// VERSION for row 0 than the leader's. Both versions are hand-staged
/// directly onto each node's own cnc `service_slot(0).status` (the same
/// live cell a real service's attach publishes, and the same technique
/// `a_declared_set_mismatch_...` already uses for `snapshot_pos` — the
/// sender/receiver read it fresh on every `SNAP_BEGIN`, so no real service
/// needs to be running for this comparison to exercise the real wire path).
/// Refused with `RefusalKind::Version`, both packed versions recorded.
#[test]
fn a_joiner_running_another_fsm_version_is_refused_with_both_versions() {
    let _g = serialize();
    let dir = tempfile::Builder::new()
        .prefix("uc2-learner-version-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    const SEG: u64 = 64 * 1024;
    let app = "learner-version-mismatch";

    let v_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let l_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let v_addr = v_sock.local_addr().unwrap();
    let l_addr = l_sock.local_addr().unwrap();
    let members = vec![(0u32, v_addr)];
    let learners = vec![(1u32, l_addr)];

    let cfg = |id: NodeId, sock_addr: SocketAddr, d: PathBuf| NodeConfig {
        id,
        members: members.clone(),
        learners: learners.clone(),
        bind: sock_addr,
        instance_dir: d,
        app_id: app.into(),
        buffer_bytes: 1 << 18,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 0xC0FFEE ^ id as u64,
        faults: FaultConfig::default(),
        purge: PurgePolicy::BelowSnapshot { slack_bytes: 0 },
        journal_segment_bytes: SEG,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::single("sum"),
    };

    let v_dir = dir.path().join("v0");
    let voter =
        Node::start_with_socket(cfg(0, v_addr, v_dir.clone()), v_sock).expect("start voter");
    await_until(30, "voter serves", || voter.can_serve());

    let cnc_for_mirror =
        CncPage::open_file(&v_dir.join("cnc2.dat"), app).expect("open voter cnc for mirror");
    // The leader's row-0 service reports version 1.0.0.
    cnc_for_mirror
        .service_slot(0)
        .status
        .store_version(pack_version(1, 0, 0));
    let (mirror_stop, mirror_handle) = spawn_applied_mirror(cnc_for_mirror, 0);

    for i in 0u64..24000 {
        let mut p = vec![0u8; PAYLOAD];
        p[..8].copy_from_slice(&i.to_le_bytes());
        loop {
            match voter.submit(p.clone()) {
                Ok(()) => break,
                Err(_) => std::thread::yield_now(),
            }
        }
    }
    await_until(30, "voter quiesced", || {
        let c = voter.counters();
        let a = c.append.load_acquire();
        a > 0 && c.commit.load_acquire() == a && c.durable.load_acquire() == a
    });
    mirror_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    mirror_handle.join().unwrap();

    let cnc = CncPage::open_file(&v_dir.join("cnc2.dat"), app).expect("open voter cnc");
    let durable = voter.counters().durable.load_acquire();
    let floor = (durable / 2) / 128 * 128;
    assert!(
        floor > SEG,
        "need >1 segment below the floor (durable={durable})"
    );
    let snap_dir = v_dir.join("snapshots").join("0");
    std::fs::create_dir_all(&snap_dir).unwrap();
    std::fs::write(
        snap_dir.join(format!("snap-{floor}.ultsnap")),
        vec![0x5Au8; 4096],
    )
    .unwrap();
    cnc.service_slot(0).snapshot_pos.store_release(floor);
    cnc.snapshots().service_snapshot_pos.store_release(floor);
    await_until(30, "voter purged its prefix", || {
        voter.archive_first_base() > 0
    });

    // The joiner declares the SAME name, but its row-0 service reports
    // version 2.0.0.
    let l_dir = dir.path().join("l1");
    let learner =
        Node::start_with_socket(cfg(1, l_addr, l_dir.clone()), l_sock).expect("start learner");
    let l_cnc = CncPage::open_file(&l_dir.join("cnc2.dat"), app).expect("open learner cnc");
    l_cnc
        .service_slot(0)
        .status
        .store_version(pack_version(2, 0, 0));

    await_until(
        60,
        "the joiner refused the version-mismatched session",
        || learner.snapshot_session_refusals().2 >= 1,
    );
    assert_eq!(
        learner.snapshot_session_refusals().1,
        0,
        "matching names must never count as an identity refusal"
    );
    let r = learner
        .crypto_stats()
        .version_refusal
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .expect("version refusal detail recorded");
    assert_eq!(
        (r.kind, r.ours_version, r.theirs_version),
        (
            RefusalKind::Version,
            pack_version(2, 0, 0),
            pack_version(1, 0, 0)
        )
    );
    assert_eq!(r.row, 0);

    let refused_dir = l_dir.join("snapshots").join("0");
    assert!(refused_dir.is_dir());
    assert_eq!(
        std::fs::read_dir(&refused_dir)
            .expect("read the joiner's snapshot dir")
            .count(),
        0,
        "a refused session must leave no artifact behind"
    );

    learner.stop();
    voter.stop();
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
        c.nodes[learner_idx]
            .node
            .as_ref()
            .unwrap()
            .counters()
            .durable
            .load_acquire()
            >= c.nodes[leader].commit(),
        "the learner should still be replicating bytes it just cannot vote on"
    );

    for node in &mut c.nodes {
        node.stop();
    }
}

// ------------------------- time-and-timers plan 3: the table on the session

/// The declared row every fixture below names, and the identity hash a
/// schedule entry addresses it by.
const ROW0: &str = "fsm0";

fn row0_hash() -> u64 {
    FsmName::parse(ROW0).expect("a valid FSM name").hash()
}

/// Wall-clock ns — the same clock the leader stamps the log with, so a
/// deadline built from it is genuinely in the log's future.
fn wall_now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after the epoch")
        .as_nanos() as u64
}

/// Two entries on row 0, both armed and both an HOUR out, so the table is
/// adopted and stays armed without a single TIMER frame ever being appended:
/// this fixture is about what the SESSION carries, and a firing table would
/// only add log traffic (and, on a node with no service attached to apply it,
/// nothing else) to the thing under test.
fn two_far_future_entries() -> ScheduleTable {
    let hour = wall_now_ns() + 3_600_000_000_000;
    ScheduleTable {
        entries: vec![
            ScheduleEntry {
                identity_hash: row0_hash(),
                timer_id: 1,
                rule: ScheduleRule::Every {
                    period_ns: 600_000_000_000,
                    anchor_ns: hour,
                },
            },
            ScheduleEntry {
                identity_hash: row0_hash(),
                timer_id: 2,
                rule: ScheduleRule::Once { at_ns: hour },
            },
        ],
    }
}

/// `timers.rs::apply_schedule_table`, duplicated rather than shared because
/// each integration test file is its own binary: stage the table under the
/// instance dir and drive `ADMIN_OP_SCHEDULE_APPLY` through the cnc admin
/// band (`uc2ctl schedule apply` minus the bin; the default
/// [`uc_node::AdminPolicy::Filesystem`] ignores the auth line). Returns the
/// accepted table's position — the frame END.
fn apply_schedule_table(dir: &Path, cnc: &CncPage, table: &ScheduleTable) -> u64 {
    let mut bytes = Vec::new();
    encode_schedule_table(table, &mut bytes);
    for _ in 0..20 {
        std::fs::write(dir.join(uc_node::SCHEDULE_PENDING_FILE), &bytes).expect("stage the table");
        let (id, ip, port) = uc_node::schedule_digest(&bytes);
        let seq = cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0) + 1;
        cnc.write_admin_req(&AdminReq {
            seq,
            nonce: rand::random::<u64>(),
            op: ADMIN_OP_SCHEDULE_APPLY,
            id,
            ip,
            port,
        });
        let deadline = deadline_secs(20);
        let resp = loop {
            if let Some(resp) = cnc.read_admin_resp(seq) {
                break resp;
            }
            assert!(Instant::now() < deadline, "schedule apply timed out");
            std::thread::sleep(Duration::from_millis(10));
        };
        if resp.status == 0 {
            return resp.version;
        }
        // `2` = retry, side-effect-free (a leader whose leader-open collapse
        // has not finished has no appender yet). Anything else is a genuine
        // refusal and the test should say so.
        assert_eq!(
            resp.status, 2,
            "schedule apply was refused: reason {}",
            resp.reason
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("schedule apply never left the retry window");
}

/// What a below-floor join leaves behind, for the caller to assert on.
struct JoinFixture {
    _dir: tempfile::TempDir,
    voter: Node,
    learner: Node,
    v_dir: PathBuf,
    l_dir: PathBuf,
}

impl JoinFixture {
    fn stop(self) {
        self.learner.stop();
        self.voter.stop();
    }
}

/// The `fresh_learner_joins_a_purged_leader_via_snapshot_session` fixture,
/// trimmed to what plan 3 needs (no pre-seeded config record, no routing
/// assertions — those stay that test's job) and parameterised by the schedule
/// table the voter adopts BEFORE it purges.
///
/// The table frame therefore lands below the floor the learner adopts: the
/// learner can never replay it, so a table it holds afterwards came off the
/// session's `SNAP_TABLE` and nowhere else.
fn below_floor_join(app: &str, table: Option<&ScheduleTable>) -> JoinFixture {
    let dir = tempfile::Builder::new()
        .prefix("uc2-learner-sched-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    const SEG: u64 = 64 * 1024;

    let v_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let l_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let v_addr = v_sock.local_addr().unwrap();
    let l_addr = l_sock.local_addr().unwrap();
    let members = vec![(0u32, v_addr)];
    let learners = vec![(1u32, l_addr)];

    let cfg = |id: NodeId, sock_addr: SocketAddr, d: PathBuf| NodeConfig {
        id,
        members: members.clone(),
        learners: learners.clone(),
        bind: sock_addr,
        instance_dir: d,
        app_id: app.into(),
        // A SMALL ring so a fresh learner's NAK from 0 falls BELOW the ring
        // floor into the PURGED journal region → snapshot session.
        buffer_bytes: 1 << 18,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 0xC0FFEE ^ id as u64,
        faults: FaultConfig::default(),
        purge: PurgePolicy::BelowSnapshot { slack_bytes: 0 },
        journal_segment_bytes: SEG,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::single(ROW0),
    };

    let v_dir = dir.path().join("v0");
    let voter =
        Node::start_with_socket(cfg(0, v_addr, v_dir.clone()), v_sock).expect("start voter");
    await_until(30, "voter serves", || voter.can_serve());
    let cnc = CncPage::open_file(&v_dir.join("cnc2.dat"), app).expect("open voter cnc");

    // The table goes on FIRST, so its frame is inside the prefix the purge
    // below destroys.
    if let Some(t) = table {
        let position = apply_schedule_table(&v_dir, &cnc, t);
        assert!(position > 0, "an accepted apply reports the frame END");
        // Ruling 3: the ship gate (`shippable_schedule`) only offers a record
        // at or below the sender's COMMIT counter, so a table still in flight
        // would correctly ship as `prev`/none and this fixture would be
        // asserting the wrong thing. One voter commits on its own durable
        // report, but not instantly — wait for it explicitly.
        await_until(30, "the applied table committed", || {
            voter.counters().commit.load_acquire() >= position
        });
    }

    // See `spawn_applied_mirror`'s doc: row 0 is a REAL declared FSM with no
    // service attached, so FSM-lag admission needs `applied` mirrored from
    // `durable` for the duration of the raw submit loop.
    let (mirror_stop, mirror_handle) = spawn_applied_mirror(std::sync::Arc::clone(&cnc), 0);
    for i in 0u64..24000 {
        let mut p = vec![0u8; PAYLOAD];
        p[..8].copy_from_slice(&i.to_le_bytes());
        loop {
            match voter.submit(p.clone()) {
                Ok(()) => break,
                Err(_) => std::thread::yield_now(),
            }
        }
    }
    await_until(30, "voter quiesced", || {
        let c = voter.counters();
        let a = c.append.load_acquire();
        a > 0 && c.commit.load_acquire() == a && c.durable.load_acquire() == a
    });
    mirror_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    mirror_handle.join().unwrap();

    // Publish a snapshot floor + a real snapshot file for the sender to ship
    // (frame-aligned: a mid-frame floor lands the journal-replay datagram
    // below the adopted position and is dropped as a dup).
    let durable = voter.counters().durable.load_acquire();
    let floor = (durable / 2) / 128 * 128;
    assert!(
        floor > SEG,
        "need >1 segment below the floor (durable={durable})"
    );
    let snap_dir = v_dir.join("snapshots").join("0");
    std::fs::create_dir_all(&snap_dir).unwrap();
    std::fs::write(
        snap_dir.join(format!("snap-{floor}.ultsnap")),
        vec![0x5Au8; 4096],
    )
    .unwrap();
    cnc.service_slot(0).snapshot_pos.store_release(floor);
    cnc.snapshots().service_snapshot_pos.store_release(floor);
    await_until(30, "voter purged its prefix", || {
        voter.archive_first_base() > 0
    });
    let first_base = voter.archive_first_base();
    let frontier = voter.counters().append.load_acquire();

    let l_dir = dir.path().join("l1");
    let learner =
        Node::start_with_socket(cfg(1, l_addr, l_dir.clone()), l_sock).expect("start learner");
    await_until(40, "learner caught up across the purged prefix", || {
        learner.counters().durable.load_acquire() >= frontier
            && learner.counters().commit.load_acquire() >= frontier
    });
    assert!(
        learner.archive_first_base() >= first_base,
        "the learner must have adopted the shipped snapshot floor, not replayed from 0"
    );

    JoinFixture {
        _dir: dir,
        voter,
        learner,
        v_dir,
        l_dir,
    }
}

/// Plan 3 (spec §5), the headline: a fresh learner whose join is BELOW the
/// leader's purge floor ends up holding the leader's schedule table — record
/// for record — and with both of its entries ARMED.
///
/// The table frame is below the floor by construction (it is appended before
/// the churn the purge destroys), so replay cannot be the source: the only
/// path from the leader's record to the learner's is the session's
/// `SNAP_TABLE` and the fiat install at the floor.
#[test]
fn a_fresh_learner_below_the_floor_installs_the_leaders_schedule_table() {
    let _g = serialize();
    let table = two_far_future_entries();
    let f = below_floor_join("learner-sched", Some(&table));

    let want = uc_node::read_record(&f.v_dir)
        .expect("read the voter's record")
        .expect("the voter adopted a table");
    assert_eq!(
        decode_schedule_table(&want.table).as_ref(),
        Some(&table),
        "sanity: the voter's record holds the table this test applied"
    );

    // The install happens on the consensus agent at floor adoption, which the
    // catch-up wait above does not itself order against — poll for it.
    await_until(30, "the learner installed a schedule record", || {
        uc_node::read_record(&f.l_dir)
            .expect("read the learner's record")
            .is_some()
    });
    let got = uc_node::read_record(&f.l_dir).unwrap().unwrap();
    assert_eq!(
        got.position, want.position,
        "the learner's record is at the leader's table position"
    );
    assert_eq!(got.time_ns, want.time_ns, "…with the leader's frame stamp");
    assert_eq!(got.table, want.table, "…and the leader's table bytes");
    assert_eq!(
        decode_schedule_table(&got.table).as_ref(),
        Some(&table),
        "the installed bytes decode to the applied table"
    );
    assert!(
        got.prev.is_none(),
        "a fiat install keeps no history — nothing below the floor is \
         truncatable, so there is nothing to revert to: {:?}",
        got.prev
    );

    // Both entries are ARMED on the learner, not merely recorded: the fiat
    // install runs `install_table`, and the consensus agent publishes the
    // row's pending count into its cnc identity slot every pass.
    let l_cnc = CncPage::open_file(&f.l_dir.join("cnc2.dat"), "learner-sched").expect("open cnc");
    await_until(30, "the learner armed both table entries", || {
        l_cnc.service_slot(0).identity.timers_pending() == 2
    });

    f.stop();
}

/// Plan 3 (spec §5), the other half: a leader that has NEVER adopted a table
/// still sends a `SNAP_TABLE` — carrying the wire's honest "no table",
/// `(position 0, stamp 0, no bytes)`. Two things must hold, and the second is
/// what the receiver's withhold rule puts at risk: the joiner installs the
/// canonical no-table record, AND the session still completes.
///
/// The completion half is not a spare assertion — the receiver refuses to
/// emit `SNAP_DONE` until a table has landed, so a sender that stayed silent
/// for want of a table would wedge every joiner. The catch-up wait inside
/// `below_floor_join` is that check: without the `SNAP_TABLE`, the learner
/// never adopts the floor and never reaches the frontier.
#[test]
fn a_leader_without_a_table_ships_none_and_the_joiner_installs_none() {
    let _g = serialize();
    let f = below_floor_join("learner-nosched", None);

    assert!(
        uc_node::read_record(&f.v_dir)
            .expect("read the voter's record")
            .is_none(),
        "sanity: this leader never adopted a table"
    );

    // `install_snapshot_table` canonicalises "no table" rather than leaving
    // the record absent — the stored bytes must ALWAYS decode, because boot
    // arming and `revert_schedule_below` both read them back. So the joiner
    // holds a record, at position 0, whose table is empty.
    await_until(30, "the learner installed the no-table record", || {
        uc_node::read_record(&f.l_dir)
            .expect("read the learner's record")
            .is_some()
    });
    let got = uc_node::read_record(&f.l_dir).unwrap().unwrap();
    assert_eq!(
        got.position, 0,
        "position 0 is the wire's 'this leader has no table'"
    );
    assert_eq!(got.time_ns, 0);
    assert_eq!(
        decode_schedule_table(&got.table),
        Some(ScheduleTable {
            entries: Vec::new()
        }),
        "the canonical empty encoding, not raw empty bytes"
    );
    assert!(got.prev.is_none());

    // Nothing is armed, on either side.
    let l_cnc = CncPage::open_file(&f.l_dir.join("cnc2.dat"), "learner-nosched").expect("open cnc");
    assert_eq!(l_cnc.service_slot(0).identity.timers_pending(), 0);

    f.stop();
}
