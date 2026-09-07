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
use uc_protocol::v2::cnc::{
    ADMIN_OP_SCHEDULE_APPLY, CNC_MAX_PEER_SLOTS, CNC_PEER_ROLE_LEARNER,
    CNC_SVC_STATUS_SNAPSHOT_CAPABLE,
};
use uc_protocol::v2::schedule::{
    ScheduleEntry, ScheduleRule, ScheduleTable, encode_schedule_table,
};

const PAYLOAD: usize = 96;

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn serialize() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// `uc2ctl snapshot`, in process (coordinated-snapshot spec §5.5): command a
/// coordinated instant and return its position **P**, polling through the
/// `retry` window a leader legitimately answers while it has the role but not
/// yet an appender. Duplicated per test binary, like `serialize`.
fn command_instant(node: &Node) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match node.command_snapshot(false) {
            Ok(p) => return p,
            Err(uc_node::SnapshotRefusal::Retry) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => panic!("uc2ctl snapshot refused: {e}"),
        }
    }
}

/// Command instants on `node` until one **completes** — every declared row
/// and the cluster FSM at the same P — and return that P. `on_command` runs
/// once per attempt with the freshly commanded position; `rows` is what the
/// diagnostic prints (and, for the faked fixtures, what `on_command` writes).
///
/// # Why it RETRIES (fix round 1)
///
/// An instant commanded while a row's apply loop or the `uc2-cluster` agent is
/// LAPPING is abandoned — by design, and it never completes however long you
/// wait. These fixtures run a 256 KiB ring and push megabytes through it, so a
/// walker overruns and catches up through the journal (Ruling R18), and
/// `ClusterAgent::replay_from_journal` deliberately does **not** act on a
/// `FRAME_TYPE_SNAPSHOT` frame buried in a catch-up span (see its comment:
/// freezing at an instant the walker is already past would tag a stale
/// artifact). The frame is consumed, the cursor moves past it, and nothing is
/// ever written at that P.
///
/// That is spec §10's documented outcome — "a row never reaches P: the set
/// stays incomplete; the next instant supersedes it" — reachable for the
/// CLUSTER row and for a user row alike. In production the cadence supersedes
/// on its own; here the fixture does what the operator would, and
/// `command_snapshot_operator` (which `Node::command_snapshot` runs) always
/// supersedes. Each attempt leaves the walker's cursor further forward, so
/// once the ring stops lapping the next attempt is walked LIVE and freezes.
///
/// Reproduced at ~1 run in 20 with the wait instrumented:
/// `P=695328 set=0 cluster_artifact=0 view=0 append=commit=durable=1536256`
/// — the whole log durable and committed with the agent's artifact still at 0.
/// A stall, not slowness, which is why the wait stays at 30 s per attempt
/// rather than being widened.
fn instant_until_complete(
    node: &Node,
    cnc: &CncPage,
    rows: &[u8],
    mut on_command: impl FnMut(u64),
) -> u64 {
    const ATTEMPTS: usize = 5;
    for attempt in 1..=ATTEMPTS {
        let p = command_instant(node);
        on_command(p);
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if node.snapshot_set_position() >= p {
                // Plan 2 T10, controller ruling: Ruling P10 (a replayed span
                // ACTS on its last `SNAPSHOT` frame) removed the reason a
                // first attempt could be abandoned here, so the retry above is
                // belt-and-braces and reaching attempt 2 is a REGRESSION, not
                // a tolerated outcome. The loop stays — production cadence
                // supersedes the same way, and a fixture that silently gave up
                // would be worse — but a second attempt fails the test loudly.
                assert_eq!(
                    attempt, 1,
                    "instant {p} completed only on attempt {attempt}: an earlier instant was \
                     abandoned (spec §10), which Ruling P10 should have made unreachable on \
                     this fixture — see the diagnostic above"
                );
                return p;
            }
            std::thread::yield_now();
        }
        let c = node.counters();
        let state: Vec<(u8, u64, u64)> = rows
            .iter()
            .map(|&r| {
                (
                    r,
                    cnc.service_slot(r as usize).snapshot_pos.load_acquire(),
                    cnc.service_slot(r as usize).applied.load_acquire(),
                )
            })
            .collect();
        eprintln!(
            "instant {p} abandoned (attempt {attempt}/{ATTEMPTS}): set={} cluster_artifact={} \
             append={} commit={} durable={} rows(row,snapshot_pos,applied)={state:?} \
             — a walker was catching up through the journal and skipped the SNAPSHOT frame \
             (spec §10); superseding",
            node.snapshot_set_position(),
            node.cluster_snapshot_position(),
            c.append.load_acquire(),
            c.commit.load_acquire(),
            c.durable.load_acquire(),
        );
    }
    panic!(
        "no instant completed in {ATTEMPTS} attempts — a walker never caught up to the live \
         ring, which is NOT the documented abandonment this retry covers"
    );
}

/// Command an instant on a fixture whose rows have NO real service, and fake
/// what the service would have done: the snapshot-capability bit before the
/// command (spec §5.5 refuses `48` without it) and each row's artifact +
/// `snapshot_pos` after it. Returns **P**.
///
/// `uc_node` never parses a row's artifact, so a blob of the right NAME at
/// the right position is a complete row as far as the node and the snapshot
/// session are concerned — the same stand-in `purge_safety.rs` uses. What
/// cannot be faked is the CLUSTER artifact: the `uc2-cluster` agent writes it,
/// at the instant, which is exactly why the floor now needs a real command
/// rather than a poked cnc word.
///
/// Call it in the MIDDLE of the fixture's traffic, not after it: P is the
/// frame end of the frame this appends, so everything submitted afterwards is
/// the retained `[P, append)` tail a below-floor joiner must replay once it
/// has installed the set. That tail is the property the old hand-picked
/// `durable / 2` floor provided.
///
/// Retries through an abandoned instant — see [`instant_until_complete`].
fn instant_with_faked_rows(node: &Node, v_dir: &Path, cnc: &CncPage, rows: &[u8]) -> u64 {
    for &row in rows {
        let slot = cnc.service_slot(row as usize);
        slot.status
            .store_release(slot.status.load_acquire() | CNC_SVC_STATUS_SNAPSHOT_CAPABLE);
    }
    instant_until_complete(node, cnc, rows, |p| {
        for &row in rows {
            let snap_dir = v_dir.join("snapshots").join(row.to_string());
            std::fs::create_dir_all(&snap_dir).unwrap();
            std::fs::write(
                snap_dir.join(format!("snap-{p}.ultsnap")),
                vec![0x5Au8; 4096],
            )
            .unwrap();
            cnc.service_slot(row as usize).snapshot_pos.store_release(p);
        }
        // Observability only since spec §5.3, but `uc2ctl status` and the
        // backup report read it — keep it truthful.
        cnc.snapshots().service_snapshot_pos.store_release(p);
    })
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
    /// The declared FSM set this node booted with, kept so [`NodeH::restart`]
    /// reproduces it. Plan 2 T10: the standby/fetch fixtures declare a REAL
    /// row (and attach a real service to it), where every pre-plan-2 test in
    /// this file declares nothing.
    services: uc_node::ServicesConfig,
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
            self.services,
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
    services: uc_node::ServicesConfig,
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
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed,
        faults: FaultConfig::default(),
        purge: uc_node::PurgePolicy::Disabled,
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services,
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

/// [`spawn_cluster_with_learner_services`] with nothing declared — the
/// pre-plan-2 posture every quorum-shape test in this file uses.
fn spawn_cluster_with_learner(n_voters: usize, n_learners: usize) -> Cluster {
    spawn_cluster_with_learner_services(
        n_voters,
        n_learners,
        uc_node::ServicesConfig::none_for_tests(),
    )
}

/// Bind `n_voters` voter sockets + `n_learners` learner sockets, then start each
/// node with the full (members, learners) maps. Learner ids are `n_voters..`.
fn spawn_cluster_with_learner_services(
    n_voters: usize,
    n_learners: usize,
    services: uc_node::ServicesConfig,
) -> Cluster {
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
            services,
        );
        let node = Node::start_with_socket(cfg, sock).expect("start");
        nodes.push(NodeH {
            id: i as NodeId,
            addr,
            instance_dir,
            seed,
            is_learner,
            services,
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
// Ruling P8: below-floor join needs a cluster artifact at the floor; until
// Task 5 commands instants none exists.
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
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
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

    // The instant goes in the MIDDLE of the traffic (coordinated-snapshot spec
    // §5.5), inside the mirror's window: half the log below P becomes the
    // purged prefix, half above it the tail the joiner replays after
    // installing the set. That is exactly the shape the old hand-picked
    // `durable / 2` floor had — the difference is that P is now a real
    // `SNAPSHOT` frame's end, and the cluster artifact at it is real too.
    submit_frames(&voter, 12000);
    let floor = instant_with_faked_rows(&voter, &v_dir, &cnc, &[0]);
    submit_frames(&voter, 12000);
    await_until(30, "voter quiesced", || {
        let c = voter.counters();
        let a = c.append.load_acquire();
        a > 0 && c.commit.load_acquire() == a && c.durable.load_acquire() == a
    });
    mirror_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    mirror_handle.join().unwrap();

    assert!(
        floor > SEG,
        "need >1 segment below the floor (floor={floor})"
    );

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

    // M7 Task 6 / cluster-FSM spec §5.6: the snapshot session carries the
    // leader's membership alongside its lineage — inside the CLUSTER ARTIFACT
    // now, not on every `SNAP_BEGIN` — and the joiner adopts it by fiat
    // (`adopt_snapshot_config`, fed from the installed image) on install
    // completion, so its `config_version` converges with the leader's
    // PRE-SEEDED v1 (not the learner's own genesis v0) — a real cross-node
    // version bump, not a trivial 0 == 0 coincidence.
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

    // Cluster-FSM spec §5.6: assert the joiner's CLUSTER VIEW converged, not
    // just `config_version`/peer-routing, which are proxies for it. This
    // replaces the old `SnapBeginBody.config` cache assertion — with the carry
    // retired there is no cache to go stale, and the view IS what a
    // below-floor rejoiner that later becomes leader would ship on: the
    // `uc2-cluster` agent freezes the same state into the next artifact.
    //
    // Compared field by field rather than as a whole `ClusterConfig`: this is
    // the membership the joiner installed BY FIAT off the leader's image, so
    // every member the leader knows must be in it, `extra_learner_id`
    // included — which the joiner's own boot seed never contained.
    let voter_membership = voter.cluster_view().membership();
    let learner_membership = learner.cluster_view().membership();
    assert_eq!(
        learner_membership.version, voter_membership.version,
        "the installed view's version must converge"
    );
    assert_eq!(
        learner_membership.voters, voter_membership.voters,
        "…its voters"
    );
    assert_eq!(
        learner_membership.learners, voter_membership.learners,
        "…its learners"
    );
    assert_eq!(
        learner_membership.tombstones, voter_membership.tombstones,
        "…and its tombstones"
    );
    assert_eq!(
        learner_membership.version, 1,
        "the installed view carries the leader's v1 config"
    );
    assert!(
        learner_membership
            .learners
            .iter()
            .any(|(id, _)| *id == extra_learner_id),
        "…including the extra learner the joiner's own boot seed never had"
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
        // Coordinated-snapshot spec §5.2: the tag is the instant P, an
        // EXCLUSIVE frontier — restore the cursor the artifact recorded (the
        // second 8 bytes), never the tag, or the framework's
        // `pos > last_applied` guard swallows the frame that starts at P.
        self.last = Some(u64::from_le_bytes(buf[8..16].try_into().unwrap()));
        Ok(position)
    }
}

fn start_sum_service(dir: &Path, app: &str) -> uc_service::Service<SumSm> {
    // Snapshot-CAPABLE only (coordinated-snapshot spec §5.2's cnc status
    // bit): the 256 KiB byte cadence is gone, and this row freezes at the
    // instants the leader commands (`command_instant`).
    let cfg = uc_service::ServiceConfig::new(dir, app);
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
    // Capable, never self-triggering (as `start_sum_service`).
    let cfg = uc_service::ServiceConfig::new(dir, app);
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
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
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

    // Drive past a segment, then command a coordinated instant (spec §5.5) —
    // no faking here: both rows run REAL snapshot-capable services, so both
    // freeze at the SAME P, which is the point. `instant_until_complete`
    // supersedes an instant a lapping walker skipped (spec §10, and see its
    // doc); nothing is faked in the callback, so what completes the set here
    // is genuinely two services and the cluster agent agreeing on one P.
    let v_cnc = CncPage::open_file(&v_dir.join("cnc2.dat"), app).expect("open voter cnc");
    submit_frames(&voter, 12000);
    let instant = instant_until_complete(&voter, &v_cnc, &[0, 1], |_| {});
    submit_frames(&voter, 12000);
    await_until(30, "voter quiesced", || {
        let c = voter.counters();
        let a = c.append.load_acquire();
        a > 0 && c.commit.load_acquire() == a && c.durable.load_acquire() == a
    });

    // The set completing IS "every declared row at P", but say it out loud:
    // this is the first test anywhere that two independent FSMs freeze at one
    // log position, and it would still read as green if the assertion below
    // were the only thing checked and it were checking something weaker.
    assert_eq!(
        v_cnc.service_slot(0).snapshot_pos.load_acquire(),
        instant,
        "row 0 froze at the instant"
    );
    assert_eq!(
        v_cnc.service_slot(1).snapshot_pos.load_acquire(),
        instant,
        "row 1 froze at the SAME instant"
    );
    assert!(
        instant > SEG,
        "need >1 segment below the instant (instant={instant})"
    );
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
        (0, 0, 0, 0, 0),
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
// Ruling P8: below-floor join needs a cluster artifact at the floor; until
// Task 5 commands instants none exists.
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
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
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

    // The instant mid-traffic (coordinated-snapshot spec §5.5), inside the
    // mirror's window — see `instant_with_faked_rows`.
    let cnc = CncPage::open_file(&v_dir.join("cnc2.dat"), app).expect("open voter cnc");
    submit_frames(&voter, 12000);
    let floor = instant_with_faked_rows(&voter, &v_dir, &cnc, &[0]);
    submit_frames(&voter, 12000);
    await_until(30, "voter quiesced", || {
        let c = voter.counters();
        let a = c.append.load_acquire();
        a > 0 && c.commit.load_acquire() == a && c.durable.load_acquire() == a
    });
    mirror_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    mirror_handle.join().unwrap();
    assert!(
        floor > SEG,
        "need >1 segment below the floor (floor={floor})"
    );
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
// Ruling P8: below-floor join needs a cluster artifact at the floor; until
// Task 5 commands instants none exists.
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
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
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

    // The instant mid-traffic, faking BOTH declared rows' freeze at it — the
    // sender's `snapshot_set_for` refuses (missing artifact) unless every
    // declared id has an artifact AT the floor, so a two-row leader needs two.
    let cnc = CncPage::open_file(&v_dir.join("cnc2.dat"), app).expect("open voter cnc");
    submit_frames(&voter, 12000);
    let floor = instant_with_faked_rows(&voter, &v_dir, &cnc, &[0, 1]);
    submit_frames(&voter, 12000);
    await_until(30, "voter quiesced", || {
        let c = voter.counters();
        let a = c.append.load_acquire();
        a > 0 && c.commit.load_acquire() == a && c.durable.load_acquire() == a
    });
    mirror0_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    mirror0_handle.join().unwrap();
    mirror1_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    mirror1_handle.join().unwrap();
    assert!(
        floor > SEG,
        "need >1 segment below the floor (floor={floor})"
    );
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
// Ruling P8: below-floor join needs a cluster artifact at the floor; until
// Task 5 commands instants none exists.
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
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
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

    // The instant mid-traffic — see `instant_with_faked_rows`.
    let cnc = CncPage::open_file(&v_dir.join("cnc2.dat"), app).expect("open voter cnc");
    submit_frames(&voter, 12000);
    let floor = instant_with_faked_rows(&voter, &v_dir, &cnc, &[0]);
    submit_frames(&voter, 12000);
    await_until(30, "voter quiesced", || {
        let c = voter.counters();
        let a = c.append.load_acquire();
        a > 0 && c.commit.load_acquire() == a && c.durable.load_acquire() == a
    });
    mirror_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    mirror_handle.join().unwrap();
    assert!(
        floor > SEG,
        "need >1 segment below the floor (floor={floor})"
    );
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
    /// The second voter, present only for [`JoinOpts::restart_shipper`] — it
    /// is what holds the restarted leader's commit counter down (see the
    /// fixture's own comment).
    peer_voter: Option<Node>,
    learner: Node,
    /// The VOTER's instance dir. Unused since plan 1 task 5 re-pointed the
    /// schedule assertions at `Node::cluster_view` — kept because it is the
    /// obvious thing a future assertion on this fixture reaches for.
    #[allow(dead_code)]
    v_dir: PathBuf,
    l_dir: PathBuf,
    /// The frame-end position the voter's `schedule apply` reported, or `0`
    /// when the fixture staged no table.
    table_position: u64,
}

impl JoinFixture {
    fn stop(self) {
        self.learner.stop();
        self.voter.stop();
        if let Some(p) = self.peer_voter {
            p.stop();
        }
    }
}

/// How a [`below_floor_join_with`] fixture is staged.
#[derive(Default)]
struct JoinOpts<'a> {
    /// The schedule table the voter adopts BEFORE it purges.
    table: Option<&'a ScheduleTable>,
    /// Frames appended before the table is applied. `0` for the plain
    /// fixtures; the restart case needs the table's frame-end position to
    /// land above the post-restart report ceiling — see `restart_shipper`.
    pre_frames: u64,
    /// Stop and restart the voter between the purge and the joiner's first
    /// NAK, and let NOTHING advance its commit counter afterwards.
    restart_shipper: bool,
}

/// `voter.submit`, `n` times, spinning on a full ring — the raw byte path the
/// fixture drives instead of a real client (there is no service attached).
fn submit_frames(node: &Node, n: u64) {
    for i in 0u64..n {
        let mut p = vec![0u8; PAYLOAD];
        p[..8].copy_from_slice(&i.to_le_bytes());
        loop {
            match node.submit(p.clone()) {
                Ok(()) => break,
                Err(_) => std::thread::yield_now(),
            }
        }
    }
}

/// The two plain callers' form: no pre-traffic, no restart.
fn below_floor_join(app: &str, table: Option<&ScheduleTable>) -> JoinFixture {
    below_floor_join_with(
        app,
        JoinOpts {
            table,
            ..Default::default()
        },
    )
}

/// The `fresh_learner_joins_a_purged_leader_via_snapshot_session` fixture,
/// trimmed to what the schedule-table capstones need (no pre-seeded config
/// record, no routing assertions — those stay that test's job) and
/// parameterised by the schedule table the voter adopts BEFORE it purges.
///
/// The table frame therefore lands below the floor the learner adopts: the
/// learner can never replay it, so a table it holds afterwards came off the
/// session's CLUSTER ARTIFACT (spec §5.6) and nowhere else.
fn below_floor_join_with(app: &str, opts: JoinOpts<'_>) -> JoinFixture {
    let dir = tempfile::Builder::new()
        .prefix("uc2-learner-sched-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    const SEG: u64 = 64 * 1024;

    let v_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let l_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let v_addr = v_sock.local_addr().unwrap();
    let l_addr = l_sock.local_addr().unwrap();

    // The restart case needs a SECOND voter, and it is not decoration: a
    // leader's own durable position enters its commit ranking UN-ceilinged
    // (`Consensus::refresh_durable` feeds the raw counter; the FSM report
    // ceiling clamps only what a node REPORTS to a leader,
    // `publish_validated_frontier`). A solo voter therefore re-derives commit
    // straight back to its whole log the moment it boots, and "before its
    // first commit advance" cannot be held open at all. With two voters the
    // quorum is both, so the peer's ceilinged report is the commit — and a
    // peer whose declared row has no service pins it at `fsm_lag` forever.
    // Its election window is skewed ~40x so node 0, the only node holding
    // snapshot artifacts, is always the leader and always the shipper.
    let peer = opts.restart_shipper;
    let w_sock = peer.then(|| UdpSocket::bind("127.0.0.1:0").unwrap());
    let w_addr = w_sock.as_ref().map(|s| s.local_addr().unwrap());
    let l_id: NodeId = if peer { 2 } else { 1 };
    let mut members = vec![(0u32, v_addr)];
    if let Some(a) = w_addr {
        members.push((1u32, a));
    }
    let learners = vec![(l_id, l_addr)];
    const FAST_ELECTION_NS: (u64, u64) = (50_000_000, 100_000_000);
    const SLOW_ELECTION_NS: (u64, u64) = (3_000_000_000, 5_000_000_000);

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
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
        election_timeout_min_ns: if id == 1 && peer {
            SLOW_ELECTION_NS.0
        } else {
            FAST_ELECTION_NS.0
        },
        election_timeout_max_ns: if id == 1 && peer {
            SLOW_ELECTION_NS.1
        } else {
            FAST_ELECTION_NS.1
        },
        seed: 0xC0FFEE ^ id as u64,
        faults: FaultConfig::default(),
        purge: PurgePolicy::BelowSnapshot { slack_bytes: 0 },
        journal_segment_bytes: SEG,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::single(ROW0),
    };

    let v_dir = dir.path().join("v0");
    let w_dir = dir.path().join("v1");
    let mut voter =
        Node::start_with_socket(cfg(0, v_addr, v_dir.clone()), v_sock).expect("start voter");
    let mut peer_voter = w_sock.map(|s| {
        Node::start_with_socket(cfg(1, w_addr.unwrap(), w_dir.clone()), s)
            .expect("start peer voter")
    });
    await_until(30, "voter serves", || voter.can_serve());
    let mut cnc = CncPage::open_file(&v_dir.join("cnc2.dat"), app).expect("open voter cnc");

    // See `spawn_applied_mirror`'s doc: row 0 is a REAL declared FSM with no
    // service attached, so FSM-lag admission needs `applied` mirrored from
    // `durable` for the duration of the raw submit loops. Hoisted ABOVE the
    // table apply because `pre_frames` runs before it. The peer voter needs
    // the same mirror, for the same reason on the REPORT side: without it its
    // report is ceilinged at `fsm_lag` from the first byte and nothing ever
    // commits.
    let (mirror_stop, mirror_handle) = spawn_applied_mirror(std::sync::Arc::clone(&cnc), 0);
    let peer_mirror = peer_voter.as_ref().map(|_| {
        let p = CncPage::open_file(&w_dir.join("cnc2.dat"), app).expect("open peer cnc");
        spawn_applied_mirror(p, 0)
    });

    // Pre-traffic, so the table's frame lands high enough in the log that a
    // restarted shipper's report ceiling cannot reach it (see the restart
    // block below for the arithmetic). Zero for the plain fixtures.
    submit_frames(&voter, opts.pre_frames);

    // The table goes on before the churn, so its frame is inside the prefix
    // the purge below destroys.
    let mut table_position = 0;
    if let Some(t) = opts.table {
        let position = apply_schedule_table(&v_dir, &cnc, t);
        assert!(position > 0, "an accepted apply reports the frame END");
        // The apply must COMMIT before the fixture goes on: a table still in
        // flight is not yet cluster state, so the artifact the `uc2-cluster`
        // agent writes would not carry it and this fixture would be asserting
        // the wrong thing. One voter commits on its own durable report, but
        // not instantly — wait for it explicitly.
        await_until(30, "the applied table committed", || {
            voter.counters().commit.load_acquire() >= position
        });
        table_position = position;
    }

    // The instant in the MIDDLE of the churn (coordinated-snapshot spec
    // §5.5), so the table's frame stays inside the purged prefix and the
    // joiner still has a retained tail to replay — see
    // `instant_with_faked_rows`.
    submit_frames(&voter, 12000);
    let floor = instant_with_faked_rows(&voter, &v_dir, &cnc, &[0]);
    submit_frames(&voter, 12000);
    await_until(30, "voter quiesced", || {
        let c = voter.counters();
        let a = c.append.load_acquire();
        a > 0 && c.commit.load_acquire() == a && c.durable.load_acquire() == a
    });
    mirror_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    mirror_handle.join().unwrap();
    if let Some((stop, handle)) = peer_mirror {
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        handle.join().unwrap();
    }
    assert!(
        floor > SEG,
        "need >1 segment below the floor (floor={floor})"
    );
    assert!(
        table_position == 0 || table_position < floor,
        "the table's frame must be inside the purged prefix \
         (table_position={table_position} floor={floor})"
    );
    await_until(30, "voter purged its prefix", || {
        voter.archive_first_base() > 0
    });

    if opts.restart_shipper {
        // ---- the residual, staged (spec §11).
        //
        // What must be true before the shipper goes down: its CLUSTER ARTIFACT
        // must exist, because the artifact — not any live read — is what the
        // session ships. The `uc2-cluster` agent's bridging trigger fires once
        // every declared row has snapshotted, which the hand-published
        // `snapshot_pos` above has only just made true, so poll for it rather
        // than racing the agent's next duty cycle.
        await_until(30, "the voter wrote a cluster artifact", || {
            std::fs::read_dir(uc_node::cluster_agent::snapshot_dir_of(&v_dir))
                .map(|rd| {
                    rd.flatten()
                        .any(|e| e.file_name().to_string_lossy().ends_with(".ultcluster"))
                })
                .unwrap_or(false)
        });
        if table_position > 0 {
            await_until(30, "…and the artifact carries the applied table", || {
                uc_node::cluster_agent::read_committed_table(&v_dir)
                    .map(|(p, _)| p)
                    .unwrap_or(0)
                    == table_position
            });
        }

        // Both voters go down and come back — a cluster restart, the honest
        // shape of "the shipper was restarted". Nothing else is touched: the
        // journals, the artifacts and the purged prefix are all still there.
        voter.stop();
        if let Some(p) = peer_voter.take() {
            p.stop();
        }
        voter = Node::start(cfg(0, v_addr, v_dir.clone())).expect("restart the voter");
        peer_voter =
            w_addr.map(|a| Node::start(cfg(1, a, w_dir.clone())).expect("restart the peer voter"));
        cnc = CncPage::open_file(&v_dir.join("cnc2.dat"), app).expect("reopen voter cnc");
        // The cnc page is recreated ZEROED at every boot, and this fixture has
        // no real service to re-publish row 0's newest artifact position at
        // attach — so re-publish it by hand, exactly as `uc_service`'s builder
        // agent would. Without it `snapshot_set_for` declines the session
        // outright ("missing artifact") and there is no residual to test.
        cnc.service_slot(0).snapshot_pos.store_release(floor);
        cnc.snapshots().service_snapshot_pos.store_release(floor);
        // `is_leader`, not `can_serve`: a leader starts SERVING only once its
        // own `NewTerm` frame COMMITS (`ElectionSm::can_serve`), and the whole
        // point of this staging is that nothing above `fsm_lag` commits. That
        // is not a contrivance — it is exactly the state a cluster is in when
        // a restarted peer's FSM has not caught up — and shipping a snapshot
        // set is a SENDER-agent job that does not consult the serving flag.
        await_until(30, "the restarted voter took the leadership again", || {
            voter.is_leader()
        });
        // NOTHING advances the cluster's commit counter from here on: no
        // client traffic, no ticks, no mirrored `applied` on either node.
        // `LogCounters` is deliberately not primed at boot
        // (`uc_log/src/counters.rs:55`), and the PEER's report is clamped to
        // `min_applied + fsm_lag` (`services::report_ceiling`) with
        // `min_applied` back at 0 on its fresh page — so with a two-voter
        // quorum the leader's commit sits at `fsm_lag` (`buffer_bytes / 4` =
        // 64 KiB here) for as long as the test runs, far below a table frame
        // that `pre_frames` put a quarter of a megabyte up the log.
        let settle = Instant::now() + Duration::from_millis(500);
        while Instant::now() < settle {
            let c = voter.counters().commit.load_acquire();
            assert!(
                c < table_position.max(1),
                "the restarted cluster's commit ({c}) climbed past the table at \
                 {table_position} — the residual is no longer staged"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    let first_base = voter.archive_first_base();
    let frontier = voter.counters().append.load_acquire();

    let l_dir = dir.path().join("l1");
    let learner =
        Node::start_with_socket(cfg(l_id, l_addr, l_dir.clone()), l_sock).expect("start learner");
    await_until(40, "learner caught up across the purged prefix", || {
        learner.counters().durable.load_acquire() >= frontier
            // A restarted shipper's own commit counter is pinned at its report
            // ceiling (see above), and a learner's commit is that number
            // gossiped — so this half of the catch-up is meaningless there and
            // would simply time out. Durable + the floor assertion below are
            // what say the session completed.
            && (opts.restart_shipper || learner.counters().commit.load_acquire() >= frontier)
    });
    assert!(
        learner.archive_first_base() >= first_base,
        "the learner must have adopted the shipped snapshot floor, not replayed from 0"
    );

    JoinFixture {
        _dir: dir,
        voter,
        peer_voter,
        learner,
        v_dir,
        l_dir,
        table_position,
    }
}

/// The headline (spec §5.6): a fresh learner whose join is BELOW the leader's
/// purge floor ends up holding the leader's schedule table, record for record.
///
/// The table frame is below the floor by construction (it is appended before
/// the churn the purge destroys), so replay cannot be the source: the only
/// path from the leader's record to the learner's is the CLUSTER ARTIFACT the
/// session streams under id 255 and the `uc2-cluster` agent's fiat install of
/// it, before the floor advances.
#[test]
// Ruling P8: below-floor join needs a cluster artifact at the floor; until
// Task 5 commands instants none exists.
fn a_fresh_learner_below_the_floor_installs_the_leaders_schedule_table() {
    let _g = serialize();
    let table = two_far_future_entries();
    let f = below_floor_join("learner-sched", Some(&table));

    let want = f.voter.cluster_view().snapshot_inner();
    assert_eq!(
        want.table, table,
        "sanity: the voter's committed view holds the table this test applied"
    );
    // …and so does the ARTIFACT the voter shipped, which is the thing that
    // actually travelled: the artifact IS the content (no live read at ship
    // time — the whole reason the `SNAP_TABLE` carry was retired).
    let (shipped_position, shipped) = uc_node::cluster_agent::read_committed_table(&f.v_dir)
        .expect("the voter's newest cluster artifact must be readable");
    assert_eq!(
        shipped, table,
        "the voter's cluster artifact carries the table it shipped"
    );
    assert_eq!(shipped_position, want.table_position);

    // The install happens on the consensus agent at floor adoption, which the
    // catch-up wait above does not itself order against — poll for it.
    await_until(30, "the learner installed the cluster table", || {
        f.learner.cluster_view().snapshot_inner().table_position == want.table_position
    });
    let got = f.learner.cluster_view().snapshot_inner();
    assert_eq!(got.table, want.table, "…and the leader's table");

    // NOT armed, and that is the rule, not a gap: the row heap is LEADER-ONLY
    // (spec §4.9) and this joiner is a learner, so it holds the table without
    // a single timer instance in it. This is a LIVE reading, not the page's
    // initial zero — the consensus agent republishes every declared row's
    // pending count on EVERY pass (`publish_timers_pending`), so after a
    // settle of many passes the word is whatever the row's heap holds.
    //
    // What arms it is a PROMOTION, and the end-to-end proof of that is
    // `timers.rs`'s `a_promoted_below_floor_joiner_keeps_the_schedule_ticking_
    // when_it_leads`: this same install, then a promotion, then real ticks.
    let l_cnc = CncPage::open_file(&f.l_dir.join("cnc2.dat"), "learner-sched").expect("open cnc");
    let settle = Instant::now() + Duration::from_millis(300);
    while Instant::now() < settle {
        assert_eq!(
            l_cnc.service_slot(0).identity.timers_pending(),
            0,
            "the row heap is leader-only: a learner that installed a table must arm nothing"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    f.stop();
}

/// The other half (spec §5.6): a leader that has NEVER adopted a table still
/// ships a CLUSTER ARTIFACT — one carrying an EMPTY table. Two things must
/// hold, and the second is what the receiver's completion rule puts at risk:
/// the joiner installs that empty-table image, AND the session still
/// completes.
///
/// The completion half is not a spare assertion — the receiver refuses to emit
/// `SNAP_DONE` until the cluster artifact has landed, so a leader that shipped
/// a set without one would wedge every joiner. The catch-up wait inside
/// `below_floor_join` is that check: without the artifact, the learner never
/// adopts the floor and never reaches the frontier.
#[test]
// Ruling P8: below-floor join needs a cluster artifact at the floor; until
// Task 5 commands instants none exists.
fn a_leader_without_a_table_ships_none_and_the_joiner_installs_none() {
    let _g = serialize();
    let f = below_floor_join("learner-nosched", None);

    assert!(
        f.voter
            .cluster_view()
            .snapshot_inner()
            .table
            .entries
            .is_empty(),
        "sanity: this leader never adopted a table"
    );
    // What makes this non-vacuous — a fresh joiner's untouched genesis view
    // ALSO reads "no table" — is the ordering the fixture already asserted: it
    // waited for the learner to adopt the shipped floor
    // (`archive_first_base() >= first_base`), and since spec §5.6 the floor is
    // adopted only AFTER the `uc2-cluster` agent acknowledges installing the
    // session's cluster artifact (`maybe_adopt_incoming_snapshot` returns early
    // until then). So the state read here IS the installed image, and what it
    // says is: the leader had no table, and neither does the joiner.
    let installed = f.learner.cluster_view().snapshot_inner();
    assert_eq!(
        installed.table_position, 0,
        "the installed image carries no table: schedule position 0"
    );
    assert!(installed.table.entries.is_empty(), "…and 0 entries in it");
    // A regression canary, NOT discriminating on its own: the view's position
    // tag moved off genesis. An install is the only way it can move on a joiner
    // whose every CLUSTER frame is below the purged floor — but a CLUSTER frame
    // arriving ABOVE the floor would move it too, so this corroborates the
    // assertions above rather than proving them.
    assert!(
        f.learner
            .cluster_view()
            .position
            .load(std::sync::atomic::Ordering::Acquire)
            > 0,
        "the joiner's cluster view never moved off genesis"
    );

    // Nothing is armed on the learner — and this is a LIVE reading, not the
    // page's initial zero. Two things make it one: the record wait above
    // orders it after the fiat install, and the consensus agent republishes
    // every declared row's pending count on EVERY pass
    // (`publish_timers_pending`, step 6), so after a settle of many passes the
    // word is whatever the row's heap currently holds. An install that wrongly
    // armed something would have overwritten it by now.
    let l_cnc = CncPage::open_file(&f.l_dir.join("cnc2.dat"), "learner-nosched").expect("open cnc");
    let settle = Instant::now() + Duration::from_millis(300);
    while Instant::now() < settle {
        assert_eq!(
            l_cnc.service_slot(0).identity.timers_pending(),
            0,
            "the no-table install armed something"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    f.stop();
}

/// Spec §11, the residual staged exactly: a leader ships a set, is
/// **restarted**, and serves a joiner **before its first commit advance** —
/// the joiner must still install the cluster's table.
///
/// **Why this used to be red.** The retired `SNAP_TABLE` carry read LIVE state
/// at ship time and gated it on the sender's commit counter
/// (`shippable_schedule(ship, cnc.counters().commit)` — a leader only offered
/// a record at or below what it knew to be committed). `LogCounters` is
/// deliberately not primed at boot (`uc_log/src/counters.rs:55`), so a
/// restarted node re-derives commit from live quorum reports, and a node whose
/// own report is ceilinged never gets back past the ceiling on its own. The
/// leader therefore shipped the wire's honest "no table", `(0, 0, [])`, and
/// the joiner installed none — the "restarted node under-ships for one window"
/// residual, which on this fixture is not a window at all but permanent.
///
/// **Why it is green now.** The session carries the CLUSTER ARTIFACT (id 255),
/// a file the `uc2-cluster` agent wrote at a position it had already applied.
/// The artifact IS the content: there is no counter to consult at ship time,
/// and `cluster_snapshot_pos` is seeded from the recovered artifact when the
/// agent is constructed, so a node that has just booted can ship it on its
/// first NAK.
///
/// **What makes the staging real, and not a green test proving nothing.**
/// Three assertions below, in order: the shipper's commit counter really is
/// below the table's frame position when the joiner is served (so the retired
/// gate would have shut); the artifact on the shipper's disk really carries
/// the table; and the joiner's installed view really holds it at the same
/// position. The pre-traffic in the fixture is what buys the first — the
/// report ceiling after a restart is `0 + fsm_lag` = 64 KiB, and 2 000
/// pre-frames put the table's frame end a quarter of a megabyte above it.
#[test]
// Ruling P8: below-floor join needs a cluster artifact at the floor; until
// Task 5 commands instants none exists.
fn a_joiner_served_by_a_leader_restarted_before_its_first_commit_advance_still_installs_the_table()
{
    let _g = serialize();
    let table = two_far_future_entries();
    let f = below_floor_join_with(
        "learner-restart",
        JoinOpts {
            table: Some(&table),
            // The table's frame end must clear the post-restart report ceiling
            // (`fsm_lag` = `buffer_bytes / 4` = 64 KiB): 2 000 frames of
            // `PAYLOAD` bytes plus a 32-byte header, frame-aligned, is ~256 KiB.
            pre_frames: 2_000,
            restart_shipper: true,
        },
    );

    // 1. The staging: the shipper's commit counter is BELOW the table's frame
    //    position — the state the retired gate read as "I know of no
    //    committed table".
    let commit = f.voter.counters().commit.load_acquire();
    assert!(
        commit < f.table_position,
        "the residual is not staged: the restarted shipper's commit ({commit}) already \
         reached the table at {}",
        f.table_position
    );

    // 2. The artifact is the thing that travelled, and it carries the table.
    let (shipped_position, shipped) = uc_node::cluster_agent::read_committed_table(&f.v_dir)
        .expect("the restarted voter's cluster artifact must be readable");
    assert_eq!(
        shipped, table,
        "the restarted voter's cluster artifact carries the table it shipped"
    );
    assert_eq!(shipped_position, f.table_position);
    // …and the restarted node's own live view came back from that artifact.
    let want = f.voter.cluster_view().snapshot_inner();
    assert_eq!(
        want.table, table,
        "the restarted voter re-derived the table"
    );
    assert_eq!(want.table_position, f.table_position);

    // 3. The joiner installed it. The table's frame is far below the floor the
    //    joiner adopted, so replay cannot be the source; and no CLUSTER frame
    //    was appended after the restart, so a live carry cannot be either.
    await_until(30, "the joiner installed the cluster table", || {
        f.learner.cluster_view().snapshot_inner().table_position == f.table_position
    });
    let got = f.learner.cluster_view().snapshot_inner();
    assert_eq!(got.table, table, "…record for record");
    // Spec §11 asks for "table AND membership": the image carries the whole
    // cluster row, and the membership half is what lets the joiner serve and
    // vote — a session that shipped the table but not the membership would
    // pass every assertion above. WEAK HERE BY CONSTRUCTION, and deliberately
    // so: this fixture's nodes all start from the same genesis membership
    // (`cfg`'s shared `members`/`learners`), so an equal reading does not by
    // itself prove the joiner took it from the image. The NON-genesis case —
    // a membership at version 1, present only in the leader's artifact — is
    // `fresh_learner_joins_a_purged_leader_via_snapshot_session`'s
    // `learner_membership` block. What this adds is that the restarted
    // shipper's own re-derived row and the joiner's agree in FULL, not only
    // on the table.
    assert_eq!(
        got.membership, want.membership,
        "…and the shipper's membership, which came from the same image"
    );

    // And nothing is armed on it: the row heap is leader-only (spec §4.9) and
    // this joiner is a learner. A live reading — the consensus agent
    // republishes the count every pass.
    let l_cnc = CncPage::open_file(&f.l_dir.join("cnc2.dat"), "learner-restart").expect("open cnc");
    let settle = Instant::now() + Duration::from_millis(300);
    while Instant::now() < settle {
        assert_eq!(
            l_cnc.service_slot(0).identity.timers_pending(),
            0,
            "the row heap is leader-only: a learner that installed a table must arm nothing"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    f.stop();
}

// ---------------------------------------- standby instants (plan 2, spec §5.7)

/// `uc2ctl snapshot --standby`, in process: only LEARNERS freeze (spec §5.7
/// item 1). Same `retry` polling as [`command_instant`].
fn command_standby_instant(node: &Node) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match node.command_snapshot(true) {
            Ok(p) => return p,
            Err(uc_node::SnapshotRefusal::Retry) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => panic!("uc2ctl snapshot --standby refused: {e}"),
        }
    }
}

/// A 3-voter + 1-learner cluster with ONE declared row (`SumSm`, "sum") and a
/// REAL snapshot-capable service attached on every node — voters included.
///
/// Real services, not `instant_with_faked_rows`' stand-ins: the property both
/// standby tests turn on is that a voter's apply loop does NOT freeze while a
/// learner's does, and a row with no service attached cannot freeze either way.
struct StandbyCluster {
    c: Cluster,
    leader: usize,
    learner: usize,
    svcs: Vec<Option<uc_service::Service<SumSm>>>,
    cncs: Vec<std::sync::Arc<CncPage>>,
}

impl StandbyCluster {
    fn node(&self, i: usize) -> &Node {
        self.c.nodes[i].n()
    }
    /// The voter indices, leader first.
    fn voters(&self) -> Vec<usize> {
        let mut v: Vec<usize> = (0..3).collect();
        v.sort_by_key(|&i| i != self.leader);
        v
    }
    fn stop(mut self) {
        for s in self.svcs.iter_mut() {
            if let Some(s) = s.take() {
                s.stop();
            }
        }
        for h in self.c.nodes.iter_mut() {
            h.stop();
        }
    }
}

const STANDBY_APP: &str = "learner";

fn standby_cluster() -> StandbyCluster {
    let c = spawn_cluster_with_learner_services(
        3,
        1,
        uc_node::ServicesConfig::single(<SumSm as uc_service::RawStateMachine>::NAME),
    );
    let svcs: Vec<Option<uc_service::Service<SumSm>>> = c
        .nodes
        .iter()
        .map(|h| Some(start_sum_service(&h.instance_dir, STANDBY_APP)))
        .collect();
    let cncs: Vec<std::sync::Arc<CncPage>> = c
        .nodes
        .iter()
        .map(|h| {
            CncPage::open_file(&h.instance_dir.join("cnc2.dat"), STANDBY_APP).expect("open cnc")
        })
        .collect();
    let leader = await_single_leader(&c.nodes, 30);
    // The capability bit is what makes an instant commandable at all (spec
    // §5.5 refuses `48` without it), and it is published by the service's
    // attach — so wait for the ATTACH, not for a timer.
    await_until(
        30,
        "every row published the snapshot-capability bit",
        || {
            cncs.iter().all(|p| {
                p.service_slot(0).status.load_acquire() & CNC_SVC_STATUS_SNAPSHOT_CAPABLE != 0
            })
        },
    );
    StandbyCluster {
        c,
        leader,
        learner: 3,
        svcs,
        cncs,
    }
}

/// Spec §5.7 items 1-3: a `--standby` instant is a real instant that only the
/// LEARNER pays for.
///
/// The three halves, in the order the spec states them:
///
/// * the learner's set at **P** completes — its row froze at P and the
///   `uc2-cluster` row did too, so `snapshot_set_position == P`;
/// * every VOTER's row `snapshot_pos` is still `0` and its own set position is
///   still `0` (§5.7 item 3: "a voter has no set at P and its floor does not
///   move — yet"), and no voter row ever published a freeze duration; and
/// * every voter's `applied` keeps MOVING across the instant, past P, under
///   continued load. This is the point of the flag: an all-nodes instant caps
///   every voter's durable report at `P + fsm_lag` for the length of its
///   freeze (§5.7's commit-stall argument), and a standby instant must not.
///
/// The cheap red twin for the middle half is to assert a voter's
/// `snapshot_pos == p` instead of `0`: that is exactly what a build that
/// ignored `FLAG_SNAPSHOT_STANDBY` (or read the learner bit from the wrong
/// word) would produce, and it fails on all three voters.
#[test]
fn a_standby_instant_freezes_only_the_learner_and_voters_applied_keep_moving() {
    let _g = serialize();
    let f = standby_cluster();

    submit_n(&f.c.nodes[f.leader], 0, 800);
    await_until(30, "every node applied the pre-instant load", || {
        let commit = f.c.nodes[f.leader].commit();
        commit > 0
            && f.cncs
                .iter()
                .all(|p| p.service_slot(0).applied.load_acquire() >= commit)
    });
    let applied_before: Vec<u64> = f
        .cncs
        .iter()
        .map(|p| p.service_slot(0).applied.load_acquire())
        .collect();

    let p = command_standby_instant(f.node(f.leader));

    // Half 1: the learner, and only the learner, builds the set.
    await_until(60, "the learner completed the standby set", || {
        f.node(f.learner).snapshot_set_position() >= p
    });
    assert_eq!(
        f.node(f.learner).snapshot_set_position(),
        p,
        "the learner's completed set must be AT the standby instant"
    );
    assert_eq!(
        f.cncs[f.learner]
            .service_slot(0)
            .snapshot_pos
            .load_acquire(),
        p,
        "the learner's row froze at the standby instant"
    );
    assert_eq!(
        f.node(f.learner).cluster_snapshot_position(),
        p,
        "the learner's uc2-cluster row froze at the standby instant"
    );

    // Half 3: the voters keep applying, past P, under fresh load.
    submit_n(&f.c.nodes[f.leader], 800, 800);
    for &v in &f.voters() {
        await_until(
            30,
            "a voter's applied advanced past the standby instant",
            || f.cncs[v].service_slot(0).applied.load_acquire() > p.max(applied_before[v]),
        );
    }

    // Half 2: no voter froze — checked AFTER the load above, so this is not a
    // race the voters simply had not lost yet.
    for &v in &f.voters() {
        assert_eq!(
            f.cncs[v].service_slot(0).snapshot_pos.load_acquire(),
            0,
            "voter {v} froze for a STANDBY instant at {p} — the flag was ignored"
        );
        assert_eq!(
            f.node(v).snapshot_set_position(),
            0,
            "voter {v} completed a set at a standby instant; §5.7 item 3 says its floor \
             does not move until it FETCHES"
        );
        assert_eq!(
            f.cncs[v].service_slot(0).identity.freeze_ns(),
            0,
            "voter {v} recorded a freeze duration — it ran freeze() for a standby instant"
        );
        assert_eq!(
            f.node(v).snapshot_instants_abandoned(),
            0,
            "voter {v} abandoned an instant; only one was commanded and it was never superseded"
        );
    }
    // And the leader's own view of it (Ruling P13(b)): it commanded the
    // instant, holds no set at it, and — because the set was never its to
    // complete — does not report it on the FULL-instant gauge either. That
    // gauge is what `Uc2SnapshotStalled` pairs with `snapshot_set_position`,
    // so advancing it here would make this exact healthy state page.
    assert_eq!(
        f.node(f.leader).snapshot_instant_position(),
        0,
        "a standby instant is not a FULL instant on the voter that commanded it"
    );
    assert_eq!(
        f.node(f.leader).snapshot_standby_instant_position(),
        0,
        "and a voter never ACTS on one, so its standby gauge stays 0"
    );
    // The learner is where the standby instant is observable: its uc2-cluster
    // agent acted on P, which is what `Uc2StandbySnapshotStalled` watches.
    assert_eq!(
        f.node(f.learner).snapshot_standby_instant_position(),
        p,
        "the learner published the standby instant its rows acted on"
    );

    f.stop();
}

/// Spec §5.7 items 4-5: a voter pulls a learner's complete set **store-only**
/// (`Node::request_fetch`, the in-process twin of `uc2ctl snapshot fetch
/// --from <learner-id>`), and that pull — not the learner's freeze — is what
/// moves the voter's floor.
///
/// Four claims:
///
/// 1. **Refused above the log.** `request_fetch(learner, Some(durable + 4096))`
///    is `FetchRefusal::AboveDurable` (wire reason 50). Run FIRST, because
///    `start_fetch` answers `Retry` while another fetch is pending and the
///    positive case below leaves one.
/// 2. **The artifacts land, whole.** Every declared row's artifact AND the
///    cluster artifact appear under the voter's own instance dir, named at the
///    learner's P.
/// 3. **Store-only: nothing is installed.** The voter's FSM `applied` never
///    rewinds — sampled on every iteration of the wait, not just before and
///    after, so an install-then-replay would have to hide inside a single
///    sample to escape.
/// 4. **The floor moves.** `snapshot_set_position == P` (through the
///    completeness poll, `source = "fetch"`), and after the 100 ms persist
///    throttle the cnc `node_snapshot_floor` reads P too.
///
/// Not driven through `uc2ctl`: `uc_ctl` is a bin-only crate, so the CLI's own
/// bin test owns the verb parse and this owns the behaviour.
#[test]
fn a_voter_fetches_a_learners_set_store_only_and_its_floor_moves() {
    let _g = serialize();
    let f = standby_cluster();
    let learner_id = f.c.learners[0].0;

    submit_n(&f.c.nodes[f.leader], 0, 800);
    await_until(30, "every node applied the pre-instant load", || {
        let commit = f.c.nodes[f.leader].commit();
        commit > 0
            && f.cncs
                .iter()
                .all(|p| p.service_slot(0).applied.load_acquire() >= commit)
    });
    let p = command_standby_instant(f.node(f.leader));
    await_until(60, "the learner completed the standby set", || {
        f.node(f.learner).snapshot_set_position() >= p
    });
    // …and PERSISTED it as its floor. `position: None` is "the learner's
    // newest complete set", which the serving side reads off its own
    // `node_snapshot_floor` — a word the consensus agent writes on the 100 ms
    // persist throttle. Fetching before it moves is answered `floor 0`
    // (observed: `snapshot_session_declined reason="floor 0"`), and since a
    // pending fetch blocks the next request for the 60 s intake timeout, the
    // wait belongs HERE rather than in a retry loop around the verb.
    await_until(
        30,
        "the learner persisted the standby set as its floor",
        || {
            f.cncs[f.learner]
                .snapshots()
                .node_snapshot_floor
                .load_acquire()
                == p
        },
    );

    // The fetching VOTER: a follower, not the leader — the verb is node-local
    // and is never forwarded, so a follower answering it is the honest shape.
    let v = *f
        .voters()
        .iter()
        .find(|&&i| i != f.leader)
        .expect("a follower voter");
    let v_node = f.node(v);
    let v_dir = f.c.nodes[v].instance_dir.clone();
    await_until(30, "the fetching voter's log reached the instant", || {
        v_node.counters().durable.load_acquire() >= p
    });
    assert_eq!(
        v_node.snapshot_set_position(),
        0,
        "the fetching voter must hold no set before the fetch"
    );

    // Claim 1: the named refusal, before anything is pending.
    let above = v_node.counters().durable.load_acquire() + 4096;
    assert_eq!(
        v_node.request_fetch(learner_id, Some(above)),
        Err(uc_node::FetchRefusal::AboveDurable),
        "a fetch above this node's own durable frontier is refused by name (reason 50)"
    );

    // Claim 3's baseline, and the pull itself. `None` = the learner's NEWEST
    // complete set, which is the one at `p`.
    let applied_before = f.cncs[v].service_slot(0).applied.load_acquire();
    v_node
        .request_fetch(learner_id, None)
        .expect("the fetch was accepted");
    let mut applied_high = applied_before;
    await_until(60, "the fetched set completed on the voter", || {
        let applied = f.cncs[v].service_slot(0).applied.load_acquire();
        assert!(
            applied >= applied_high,
            "the voter's FSM REWOUND from {applied_high} to {applied} — a store-only fetch \
             must never install anything"
        );
        applied_high = applied;
        v_node.snapshot_set_position() >= p
    });
    assert_eq!(
        v_node.snapshot_set_position(),
        p,
        "the fetched set is the learner's set at the standby instant"
    );
    assert_eq!(
        v_node.snapshot_fetched_position(),
        p,
        "and the node records it as FETCHED, not locally built"
    );

    // Claim 2: the artifacts, by name, in the voter's own dirs.
    let row = v_dir
        .join("snapshots")
        .join("0")
        .join(format!("snap-{p}.ultsnap"));
    assert!(
        row.is_file(),
        "the fetched row-0 artifact is missing at {}",
        row.display()
    );
    let cluster =
        uc_node::cluster_agent::snapshot_dir_of(&v_dir).join(format!("snap-{p}.ultcluster"));
    assert!(
        cluster.is_file(),
        "the fetched CLUSTER artifact is missing at {} — a set without it is not a set",
        cluster.display()
    );

    // Claim 4: the floor. Persisted on the consensus agent's 100 ms throttle,
    // so poll rather than reading once.
    await_until(30, "the voter persisted the fetched floor", || {
        f.cncs[v].snapshots().node_snapshot_floor.load_acquire() == p
    });
    // Claim 3, stated as the outcome rather than the sample: the voter's FSM
    // is still ahead of where it was, never behind.
    assert!(
        f.cncs[v].service_slot(0).applied.load_acquire() >= applied_before,
        "store-only: the voter's FSM must not have been rewound by the fetch"
    );
    assert_eq!(
        v_node.snapshot_session_refusals(),
        (0, 0, 0, 0, 0),
        "a fetch between matched nodes must trip no session refusal"
    );

    f.stop();
}

/// Spec §5.7 item 6: a joiner below the voters' floor is **redirected** to the
/// learner that holds a set, and converges there.
///
/// **How the MISSING case is constructed honestly.** With node-owned retention
/// (Ruling P1) the leader's own set AT ITS FLOOR always exists, so
/// `SNAP_REDIRECT` fires only when those artifacts are ABSENT. The fixture
/// stages exactly that, in the order an operator could reach it:
///
/// 1. a plain instant `P0` — every row on every node freezes, the voter's floor
///    moves to `P0` and it purges below it;
/// 2. a `--standby` instant `P1` — only the learner freezes, so the voter's
///    floor stays at `P0` while the learner holds a newer set;
/// 3. the voter's `snapshots/*/snap-<P0>.*` files are **deleted**. This is a
///    node RESTORED FROM A BACKUP TAKEN BEFORE `P0` (`uc2ctl backup restore`
///    lays down the journal and the artifacts it had at backup time): the cnc
///    page and the persisted floor say `P0`, and the artifacts that floor names
///    are not on disk;
/// 4. a fresh learner joins from 0, below the purged prefix.
///
/// The leader then cannot serve the below-floor NAK from its own set
/// (`SNAP_DECLINE_MISSING`), answers `SNAP_REDIRECT` naming the learner and
/// `P1`, and the joiner's `SNAP_REQUEST` to the learner installs the set at
/// `P1`.
///
/// **What is asserted, and why not `snapshot_redirected`.** The controller's
/// brief names a leader-side `snapshot_redirected` obs record; the tree has
/// none — the redirect is sent inside `uc_net`'s sender, which carries no
/// logging dependency, and its only leader-side witness is
/// `SenderStats::snap_redirects`. So this asserts that counter on the LEADER
/// and the `snapshot_redirect_followed` record on the JOINER (the receiving
/// half, which does live in `uc_node`), which together pin both ends of the
/// same datagram.
#[test]
fn a_joiner_below_the_voters_floor_is_redirected_to_the_learner() {
    let _g = serialize();
    let buf = uc_node::obs::log::capture_for_tests();
    let _cap = CaptureGuard;

    let dir = tempfile::Builder::new()
        .prefix("uc2-learner-redirect-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    const SEG: u64 = 64 * 1024;
    let app = "learner-redirect";

    let v_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let s_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let j_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let v_addr = v_sock.local_addr().unwrap();
    let s_addr = s_sock.local_addr().unwrap();
    let j_addr = j_sock.local_addr().unwrap();
    let members = vec![(0u32, v_addr)];
    // BOTH learners are in the genesis membership: `command_snapshot(standby)`
    // addresses `config().learners.first()`, so the standby holder must be
    // id 1 and the joiner id 2 for the redirect to name the node that has the
    // set. The joiner's node is not started until step 4.
    let learners = vec![(1u32, s_addr), (2u32, j_addr)];

    let cfg = |id: NodeId, sock_addr: SocketAddr, d: PathBuf| NodeConfig {
        id,
        members: members.clone(),
        learners: learners.clone(),
        bind: sock_addr,
        instance_dir: d,
        app_id: app.into(),
        buffer_bytes: 1 << 18, // small ring: the joiner's NAK from 0 falls below it
        max_payload: 256,
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 0xC0FFEE ^ id as u64,
        faults: FaultConfig::default(),
        purge: PurgePolicy::BelowSnapshot { slack_bytes: 0 },
        journal_segment_bytes: SEG,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::single(<SumSm as uc_service::RawStateMachine>::NAME),
    };

    let v_dir = dir.path().join("v0");
    let s_dir = dir.path().join("l1");
    let voter =
        Node::start_with_socket(cfg(0, v_addr, v_dir.clone()), v_sock).expect("start voter");
    let standby =
        Node::start_with_socket(cfg(1, s_addr, s_dir.clone()), s_sock).expect("start standby");
    let _v_svc = start_sum_service(&v_dir, app);
    let _s_svc = start_sum_service(&s_dir, app);
    await_until(30, "voter serves", || voter.can_serve());
    let v_cnc = CncPage::open_file(&v_dir.join("cnc2.dat"), app).expect("open voter cnc");
    let s_cnc = CncPage::open_file(&s_dir.join("cnc2.dat"), app).expect("open standby cnc");
    await_until(30, "both rows snapshot-capable", || {
        [&v_cnc, &s_cnc]
            .iter()
            .all(|p| p.service_slot(0).status.load_acquire() & CNC_SVC_STATUS_SNAPSHOT_CAPABLE != 0)
    });

    // 1. the plain instant, in the MIDDLE of the churn so a retained tail
    //    survives the purge.
    submit_frames(&voter, 12000);
    let p0 = instant_until_complete(&voter, &v_cnc, &[0], |_| {});
    submit_frames(&voter, 12000);
    await_until(30, "voter quiesced", || {
        let c = voter.counters();
        let a = c.append.load_acquire();
        a > 0 && c.commit.load_acquire() == a && c.durable.load_acquire() == a
    });
    assert!(p0 > SEG, "need >1 segment below the floor (p0={p0})");
    await_until(30, "voter purged its prefix", || {
        voter.archive_first_base() > 0
    });
    await_until(30, "the voter persisted the floor at p0", || {
        v_cnc.snapshots().node_snapshot_floor.load_acquire() == p0
    });

    // 2. the standby instant: only the learner freezes.
    await_until(30, "the standby learner caught up", || {
        standby.counters().durable.load_acquire() >= voter.counters().append.load_acquire()
    });
    let p1 = command_standby_instant(&voter);
    await_until(60, "the standby learner completed its set", || {
        standby.snapshot_set_position() >= p1
    });
    assert_eq!(standby.snapshot_set_position(), p1);
    assert_eq!(
        voter.snapshot_set_position(),
        p0,
        "the voter must NOT have frozen for the standby instant"
    );

    // 3. the restore-from-an-older-backup shape: the artifacts the voter's own
    //    floor names are gone. Deleted while the voter is live, so this also
    //    says what happens if retention or an open sender fd raced the unlink
    //    — nothing did, in every run: the files are unlinked on the first try
    //    and no session is in flight (the only peer is caught up).
    let mut removed = 0;
    for f in [
        v_dir
            .join("snapshots")
            .join("0")
            .join(format!("snap-{p0}.ultsnap")),
        uc_node::cluster_agent::snapshot_dir_of(&v_dir).join(format!("snap-{p0}.ultcluster")),
    ] {
        assert!(
            f.is_file(),
            "expected the voter's set at {p0}: {}",
            f.display()
        );
        std::fs::remove_file(&f).unwrap_or_else(|e| panic!("remove {}: {e}", f.display()));
        removed += 1;
    }
    assert_eq!(
        removed, 2,
        "both members of the voter's set at p0 were removed"
    );
    assert_eq!(
        v_cnc.snapshots().node_snapshot_floor.load_acquire(),
        p0,
        "the voter's floor still NAMES p0 — that is what makes the ship gate decline MISSING"
    );

    // 4. the joiner, from nothing.
    let redirects_before = voter
        .observability()
        .sender
        .snap_redirects
        .load(std::sync::atomic::Ordering::Relaxed);
    let j_dir = dir.path().join("l2");
    let joiner =
        Node::start_with_socket(cfg(2, j_addr, j_dir.clone()), j_sock).expect("start joiner");
    let _j_svc = start_sum_service(&j_dir, app);
    let frontier = voter.counters().append.load_acquire();
    await_until(60, "the joiner converged through the redirect", || {
        joiner.counters().durable.load_acquire() >= frontier
    });

    // The leader could not serve, and said so by redirecting.
    assert!(
        voter
            .observability()
            .sender
            .snap_redirects
            .load(std::sync::atomic::Ordering::Relaxed)
            > redirects_before,
        "the leader never sent a SNAP_REDIRECT — its below-floor NAK was served from \
         somewhere, so the MISSING case was not staged"
    );
    // The joiner followed it, to the learner, at p1 — and installed there.
    await_until(
        30,
        "the joiner recorded the redirect and the install",
        || {
            let captured = String::from_utf8_lossy(&buf.lock().unwrap()).into_owned();
            captured.contains("snapshot_redirect_followed")
                && captured.contains("snapshot_installed")
        },
    );
    let followed = last_obs_record(&buf, "snapshot_redirect_followed");
    assert!(
        followed.contains("\"node\":2") && followed.contains("\"from\":1"),
        "the joiner must have been redirected to the STANDBY learner: {followed}"
    );
    assert!(
        followed.contains(&format!("\"position\":{p1}")),
        "the redirect must name the standby instant p1={p1}: {followed}"
    );
    let installed = last_obs_record(&buf, "snapshot_installed");
    assert!(
        installed.contains("\"node\":2") && installed.contains(&format!("\"pos\":{p1}")),
        "the joiner must have installed the learner's set at p1={p1}: {installed}"
    );
    assert!(
        joiner.archive_first_base() >= p1,
        "the joiner must have adopted the shipped floor at {p1}, not replayed from 0 \
         (first_base={})",
        joiner.archive_first_base()
    );
    assert_eq!(
        joiner.snapshot_session_refusals(),
        (0, 0, 0, 0, 0),
        "a redirected session between matched nodes must trip no refusal"
    );
    assert!(
        standby
            .observability()
            .sender
            .snap_sessions
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0,
        "the LEARNER must have opened the session that served the joiner"
    );

    joiner.stop();
    standby.stop();
    voter.stop();
}
