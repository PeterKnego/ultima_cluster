// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Issue #99 — a live, joined cluster must keep committing after an election
//! even when the followers hold an uncommitted tail more than `fsm_lag` past
//! their applied position (the post-join sibling of #95).
//!
//! The deadlock: apply is gated on commit, so after an election every service
//! sits at applied = commit = C. Each node caps its durable report at
//! `min_applied + fsm_lag` (`services::report_ceiling`). A new leader advances
//! commit only once a quorum reports past its NewTerm frame (Raft §5.4.2),
//! and that frame sits at the old log end E. When `E + 32 > C + fsm_lag`, no
//! node can report it, nothing commits, nothing applies, the cap never moves.
//!
//! Staging (deterministic, no timing races): the two followers' SERVICES are
//! stopped, freezing their slot `applied` at A, so commit caps at
//! `A + fsm_lag`. The leader is then flooded through `Node::submit`, whose
//! doors read `append` at submit time while the payloads queue in the
//! 8192-deep in-process ingress channel and are appended later without a
//! second door check — so append, and the followers' DURABLE (replication is
//! not report-paced), run far past `commit + fsm_lag`. The leader node is then
//! stopped, the followers' services restarted (they catch up to the commit
//! they learned, applied == commit), and a new write must commit past E.
//!
//! Fixture copied from `cold_restart.rs` (each integration test file is its
//! own binary), trimmed. Expected on `main` (c77cdf0d): the tail test FAILS
//! (stall), the short-tail control PASSES.

use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use uc_consensus::election::NodeId;
use uc_log::cnc::CncPage;
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig, PurgePolicy, ServicesConfig, Settings};
use uc_protocol::v2::cnc::CNC_SVC_STATUS_SNAPSHOT_CAPABLE;

const PAYLOAD: usize = 96;
const FRAME_BYTES: u64 = 32 + PAYLOAD as u64;
const FSM_LAG: u64 = 64 * 1024;

static TEST_LOCK: Mutex<()> = Mutex::new(());

struct Serial {
    _lock: MutexGuard<'static, ()>,
}

fn serialize() -> Serial {
    Serial {
        _lock: TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner()),
    }
}

fn deadline_secs(secs: u64) -> Instant {
    Instant::now() + Duration::from_secs(secs)
}

fn await_until(secs: u64, msg: impl Fn() -> String, mut f: impl FnMut() -> bool) {
    let deadline = deadline_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "{}", msg());
        std::thread::sleep(Duration::from_millis(1));
    }
}

// ------------------------------------------------------------------ fixture

struct NodeH {
    instance_dir: PathBuf,
    node: Option<Node>,
    svc: Option<uc_service::Service<SumSm>>,
}

impl NodeH {
    fn n(&self) -> &Node {
        self.node.as_ref().expect("node stopped")
    }
    fn running(&self) -> bool {
        self.node.is_some()
    }
    fn stop_service(&mut self) {
        if let Some(s) = self.svc.take() {
            s.stop();
        }
    }
    fn stop(&mut self) {
        self.stop_service();
        if let Some(node) = self.node.take() {
            node.stop();
        }
    }
    fn cnc(&self, app: &str) -> Arc<CncPage> {
        CncPage::open_file(&self.instance_dir.join("cnc2.dat"), app).expect("open cnc")
    }
}

fn make_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    instance_dir: PathBuf,
    seed: u64,
    addr: SocketAddr,
    app: &str,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        learners: Vec::new(),
        bind: addr,
        instance_dir,
        app_id: app.into(),
        buffer_bytes: 1 << 22,
        max_payload: 256,
        admission_bytes_default: 256 * 1024,
        settings_genesis: Settings {
            fsm_lag_bytes: FSM_LAG,
            ..Settings::genesis_default()
        },
        force_jumbo_frames: false,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed,
        faults: FaultConfig::default(),
        purge: PurgePolicy::Disabled,
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services: ServicesConfig::single(<SumSm as uc_service::RawStateMachine>::NAME),
    }
}

fn seed_for(i: usize) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

struct Cluster {
    _dir: tempfile::TempDir,
    app: &'static str,
    nodes: Vec<NodeH>,
}

impl Cluster {
    fn node(&self, i: usize) -> &Node {
        self.nodes[i].n()
    }
    fn cnc(&self, i: usize) -> Arc<CncPage> {
        self.nodes[i].cnc(self.app)
    }
    fn running(&self) -> Vec<usize> {
        (0..self.nodes.len())
            .filter(|&i| self.nodes[i].running())
            .collect()
    }
    fn applied(&self, i: usize) -> u64 {
        self.cnc(i).service_slot(0).applied.load_acquire()
    }
    fn start_service(&mut self, i: usize) {
        let mut cfg = uc_service::ServiceConfig::new(&self.nodes[i].instance_dir, self.app);
        cfg.boot_wait = Duration::from_secs(30);
        let s = uc_service::ServiceBuilder::new(
            cfg,
            SumSm {
                total: 0,
                last: None,
            },
        )
        .start()
        .unwrap_or_else(|e| panic!("service {i} start: {e}; {}", self.counters()));
        self.nodes[i].svc = Some(s);
    }
    fn counters(&self) -> String {
        self.nodes
            .iter()
            .enumerate()
            .map(|(i, h)| match &h.node {
                Some(n) => {
                    let c = n.counters();
                    format!(
                        "n{i}{}: append={} durable={} commit={} applied={} svc={}",
                        if n.is_leader() { "(L)" } else { "" },
                        c.append.load_acquire(),
                        c.durable.load_acquire(),
                        c.commit.load_acquire(),
                        self.applied(i),
                        if h.svc.is_some() { "up" } else { "down" },
                    )
                }
                None => format!("n{i}: stopped"),
            })
            .collect::<Vec<_>>()
            .join("; ")
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for h in self.nodes.iter_mut() {
            h.stop();
        }
    }
}

fn spawn(n_voters: usize, app: &'static str) -> Cluster {
    let dir = tempfile::Builder::new()
        .prefix("uc2-post-election-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let socks: Vec<UdpSocket> = (0..n_voters)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind"))
        .collect();
    let members: Vec<(NodeId, SocketAddr)> = socks
        .iter()
        .enumerate()
        .map(|(i, s)| (i as NodeId, s.local_addr().unwrap()))
        .collect();
    let mut nodes = Vec::with_capacity(n_voters);
    for (i, sock) in socks.into_iter().enumerate() {
        let instance_dir = dir.path().join(format!("n{i}"));
        let cfg = make_config(
            i as NodeId,
            members.clone(),
            instance_dir.clone(),
            seed_for(i),
            members[i].1,
            app,
        );
        let node = Node::start_with_socket(cfg, sock).expect("start node");
        nodes.push(NodeH {
            instance_dir,
            node: Some(node),
            svc: None,
        });
    }
    Cluster {
        _dir: dir,
        app,
        nodes,
    }
}

fn await_single_leader(c: &Cluster, secs: u64) -> usize {
    let deadline = deadline_secs(secs);
    loop {
        let serving: Vec<usize> = c
            .running()
            .into_iter()
            .filter(|&i| c.node(i).can_serve())
            .collect();
        assert!(serving.len() <= 1, "split-brain: {serving:?} all serve");
        if let [i] = serving[..] {
            return i;
        }
        assert!(Instant::now() < deadline, "no single leader elected");
        std::thread::yield_now();
    }
}

/// Submit `n` frames to `node` and wait until they commit.
fn submit_frames(node: &Node, n: u64) {
    let before = node.counters().append.load_acquire();
    for i in 0u64..n {
        let mut p = vec![0u8; PAYLOAD];
        p[..8].copy_from_slice(&i.to_le_bytes());
        let deadline = deadline_secs(60);
        loop {
            match node.submit(p.clone()) {
                Ok(()) => break,
                Err(uc_node::SubmitError::NotServing) => panic!("submit refused NotServing"),
                Err(uc_node::SubmitError::Full) => {
                    assert!(Instant::now() < deadline, "ingress full for 60 s");
                    std::thread::yield_now();
                }
            }
        }
    }
    let target = before + n * FRAME_BYTES;
    await_until(
        60,
        || "submitted frames did not commit within 60 s".into(),
        || node.counters().commit.load_acquire() >= target,
    );
}

fn await_capable(c: &Cluster) {
    let pages: Vec<Arc<CncPage>> = c.running().into_iter().map(|i| c.cnc(i)).collect();
    await_until(
        30,
        || "every row published the snapshot-capability bit".into(),
        || {
            pages.iter().all(|p| {
                p.service_slot(0).status.load_acquire() & CNC_SVC_STATUS_SNAPSHOT_CAPABLE != 0
            })
        },
    );
}

// -------------------------------------------------------- state machine

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
        assert!(buf.len() >= 16, "a SumSm artifact is at least 16 bytes");
        self.total = u64::from_le_bytes(buf[..8].try_into().unwrap());
        self.last = Some(u64::from_le_bytes(buf[8..16].try_into().unwrap()));
        Ok(position)
    }
}

// ================================================================== tests

/// Flood `leader` from `threads` threads with `Node::submit` until every
/// follower's DURABLE is at least `tail` past the leader's commit, or 30 s.
fn flood_until_tail(c: &Cluster, leader: usize, followers: &[usize], tail: u64) {
    let stop = AtomicBool::new(false);
    let node = c.node(leader);
    std::thread::scope(|s| {
        for t in 0..4u64 {
            let stop = &stop;
            s.spawn(move || {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let mut p = vec![0u8; PAYLOAD];
                    p[..8].copy_from_slice(&(t << 32 | i).to_le_bytes());
                    match node.submit(p) {
                        Ok(()) => i += 1,
                        Err(uc_node::SubmitError::NotServing) => panic!("leader lost serving"),
                        Err(uc_node::SubmitError::Full) => std::thread::yield_now(),
                    }
                }
            });
        }
        let deadline = deadline_secs(30);
        loop {
            let commit = node.counters().commit.load_acquire();
            let ok = followers
                .iter()
                .all(|&f| c.node(f).counters().durable.load_acquire() >= commit + tail);
            if ok {
                break;
            }
            if Instant::now() >= deadline {
                stop.store(true, Ordering::Relaxed);
                panic!(
                    "staging: the followers' durable never reached commit + {tail} \
                     (the flood could not build the tail); {}",
                    c.counters()
                );
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        stop.store(true, Ordering::Relaxed);
    });
}

/// The scenario. Both arms stop the followers' services, write, stop the
/// leader, restart the followers' services and require a new commit past the
/// old log end. `long_tail`: flood until the followers' durable is ≥ commit +
/// 3·fsm_lag (the #99 tail); otherwise write a 16 KiB tail inside the lag.
fn leader_change(app: &'static str, long_tail: bool) {
    let _g = serialize();
    let mut c = spawn(3, app);
    let leader = await_single_leader(&c, 30);
    for i in 0..3 {
        c.start_service(i);
    }
    await_capable(&c);
    submit_frames(c.node(leader), 256);
    let followers: Vec<usize> = (0..3).filter(|&i| i != leader).collect();

    // Freeze the followers' applied at A: commit now caps at A + fsm_lag.
    for &f in &followers {
        c.nodes[f].stop_service();
    }
    if long_tail {
        flood_until_tail(&c, leader, &followers, 3 * FSM_LAG);
    } else {
        // A short tail: 128 frames (16 KiB), well inside the lag, which
        // commits even with the followers' services down.
        submit_frames(c.node(leader), 128);
        let end = c.node(leader).counters().append.load_acquire();
        await_until(
            30,
            || format!("control: followers durable at {end}; {}", c.counters()),
            || {
                followers
                    .iter()
                    .all(|&f| c.node(f).counters().durable.load_acquire() >= end)
            },
        );
    }
    let staged = c.counters();
    eprintln!("staged: {staged}");

    // The leader dies (service first, then node).
    c.nodes[leader].stop();
    // The survivors' old log end: their durable keeps climbing briefly while
    // in-flight datagrams land and the archive records them, so take it once
    // it has been still for 300 ms (it may include the new leader's 32-byte
    // NewTerm frame; the write submitted below is past it either way).
    let survivors_end = || {
        followers
            .iter()
            .map(|&f| c.node(f).counters().durable.load_acquire())
            .max()
            .unwrap()
    };
    let end = {
        let deadline = deadline_secs(30);
        let (mut last, mut since) = (survivors_end(), Instant::now());
        loop {
            let now = survivors_end();
            if now != last {
                (last, since) = (now, Instant::now());
            } else if since.elapsed() >= Duration::from_millis(300) {
                break now;
            }
            assert!(
                Instant::now() < deadline,
                "survivors' durable never settled"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    };

    // A new leader among the two survivors (elected — it may not be able to
    // SERVE in the stall, so wait on the leader flag, not can_serve).
    await_until(
        30,
        || format!("no new leader elected; {}", c.counters()),
        || followers.iter().any(|&f| c.node(f).is_leader()),
    );

    // The survivors' services come back up and catch up to the commit
    // they learned: applied == commit, waiting.
    for &f in &followers {
        c.start_service(f);
    }
    await_until(
        30,
        || {
            format!(
                "survivors' services never caught up to commit; {}",
                c.counters()
            )
        },
        || {
            followers
                .iter()
                .all(|&f| c.applied(f) >= c.node(f).counters().commit.load_acquire())
        },
    );
    let caught_up = c.counters();
    let commit_at_catch_up = followers
        .iter()
        .map(|&f| c.node(f).counters().commit.load_acquire())
        .max()
        .unwrap();
    eprintln!(
        "post-election: survivors' end={end} commit={commit_at_catch_up} \
         end-commit={} fsm_lag={FSM_LAG}; {caught_up}",
        end.saturating_sub(commit_at_catch_up)
    );

    // A new write must commit past the old log end within 20 s.
    let deadline = deadline_secs(20);
    let mut submitted = false;
    loop {
        if !submitted {
            for &f in &followers {
                if c.node(f).submit(vec![7u8; PAYLOAD]).is_ok() {
                    submitted = true;
                    break;
                }
            }
        }
        let best = followers
            .iter()
            .map(|&f| c.node(f).counters().commit.load_acquire())
            .max()
            .unwrap();
        if submitted && best > end {
            eprintln!("committed past the old end: {}", c.counters());
            return;
        }
        assert!(
            Instant::now() < deadline,
            "#99: after the leader change commit never passed the survivors' old log end \
             {end} within 20 s (submitted={submitted}, commit at catch-up \
             {commit_at_catch_up}, fsm_lag {FSM_LAG});\n  staged:      {staged}\n  \
             caught up:   {caught_up}\n  now:         {}",
            c.counters()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// #99: the followers hold a tail ≥ 3·fsm_lag past commit when the leader
/// dies; their services catch up to commit and wait. The new leader's
/// NewTerm frame sits past every node's report ceiling. Fails on main.
#[test]
fn a_leader_change_with_a_tail_past_fsm_lag_still_commits() {
    leader_change("uc2-pe-tail", true);
}

/// Control: the same stop/restart choreography with a tail inside the lag.
/// Must pass on main.
#[test]
fn a_leader_change_with_a_short_tail_commits() {
    leader_change("uc2-pe-short", false);
}
