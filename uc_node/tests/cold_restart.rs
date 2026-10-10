// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Issue #95 — a whole-cluster cold restart must commit even when the log end
//! is more than the effective `fsm_lag` past the rows' applied position.
//!
//! At boot the cnc page is fresh, so every declared row's slot `applied` is 0
//! and `min_applied = 0`; every node clamps its durable report at
//! `min_applied + fsm_lag`, the new leader's NEW_TERM frame at the old log
//! end never commits, `services_declared` is never published, the services'
//! attach is refused `NodeBooting`, and `applied` stays 0. A deadlock.
//!
//! Fixture copied from `catalog.rs` (each integration test file is its own
//! binary), trimmed. Expected on `main`: tests 1 and 2 FAIL, test 3 (the
//! control, log end inside the lag) PASSES.

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use uc_consensus::election::NodeId;
use uc_log::cnc::CncPage;
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig, PurgePolicy, ServicesConfig, Settings};
use uc_protocol::v2::cnc::CNC_SVC_STATUS_SNAPSHOT_CAPABLE;

const PAYLOAD: usize = 96;
const FRAME_BYTES: u64 = 32 + PAYLOAD as u64;

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

fn await_until(secs: u64, msg: &str, mut f: impl FnMut() -> bool) {
    let deadline = deadline_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "{msg}");
        std::thread::yield_now();
    }
}

// ------------------------------------------------------------------ fixture

struct NodeH {
    addr: SocketAddr,
    instance_dir: PathBuf,
    /// `NodeConfig` is not `Clone`; every (re)start builds a fresh one.
    cfg: Box<dyn Fn() -> NodeConfig>,
    /// The bound socket, held until the node's FIRST start (a node started
    /// later than its peers keeps its port reserved meanwhile).
    sock: Option<UdpSocket>,
    node: Option<Node>,
}

impl NodeH {
    fn n(&self) -> &Node {
        self.node.as_ref().expect("node stopped")
    }
    fn running(&self) -> bool {
        self.node.is_some()
    }
    fn start(&mut self) {
        assert!(self.node.is_none(), "start of a live node");
        let sock = self.sock.take().unwrap_or_else(|| rebind(self.addr));
        self.node = Some(Node::start_with_socket((self.cfg)(), sock).expect("start node"));
    }
    fn stop(&mut self) {
        if let Some(node) = self.node.take() {
            node.stop();
        }
    }
    fn cnc(&self, app: &str) -> Arc<CncPage> {
        CncPage::open_file(&self.instance_dir.join("cnc2.dat"), app).expect("open cnc")
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

#[derive(Clone, Copy)]
struct Opts {
    app: &'static str,
    services: ServicesConfig,
    buffer_bytes: u64,
    settings: Settings,
}

fn make_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    instance_dir: PathBuf,
    seed: u64,
    addr: SocketAddr,
    o: &Opts,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        learners: Vec::new(),
        bind: addr,
        instance_dir,
        app_id: o.app.into(),
        buffer_bytes: o.buffer_bytes as usize,
        max_payload: 256,
        admission_bytes_default: 256 * 1024,
        settings_genesis: o.settings,
        force_jumbo_frames: false,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed,
        faults: FaultConfig::default(),
        purge: PurgePolicy::Disabled,
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services: o.services,
    }
}

fn seed_for(i: usize) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

struct Cluster {
    _dir: tempfile::TempDir,
    app: &'static str,
    nodes: Vec<NodeH>,
    svcs: Vec<Box<dyn FnOnce()>>,
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
    fn stop_services(&mut self) {
        for s in self.svcs.drain(..) {
            s();
        }
    }
    fn stop_nodes(&mut self) {
        for h in self.nodes.iter_mut() {
            h.stop();
        }
    }
    fn counters(&self) -> String {
        self.nodes
            .iter()
            .enumerate()
            .map(|(i, h)| match &h.node {
                Some(n) => {
                    let c = n.counters();
                    format!(
                        "n{i}: append={} durable={} commit={}",
                        c.append.load_acquire(),
                        c.durable.load_acquire(),
                        c.commit.load_acquire()
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
        self.stop_services();
        self.stop_nodes();
    }
}

fn spawn(n_voters: usize, o: Opts) -> Cluster {
    let dir = tempfile::Builder::new()
        .prefix("uc2-cold-")
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
        let addr = members[i].1;
        let instance_dir = dir.path().join(format!("n{i}"));
        let (m, d) = (members.clone(), instance_dir.clone());
        let cfg: Box<dyn Fn() -> NodeConfig> =
            Box::new(move || make_config(i as NodeId, m.clone(), d.clone(), seed_for(i), addr, &o));
        let mut h = NodeH {
            addr,
            instance_dir,
            cfg,
            sock: Some(sock),
            node: None,
        };
        h.start();
        nodes.push(h);
    }
    Cluster {
        _dir: dir,
        app: o.app,
        nodes,
        svcs: Vec::new(),
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

/// Submit `n` frames to `node` and wait until they commit (catalog.rs's
/// `submit_frames`, trimmed).
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
                    assert!(Instant::now() < deadline, "ingress ring full for 60 s");
                    std::thread::yield_now();
                }
            }
        }
    }
    let target = before + n * FRAME_BYTES;
    await_until(60, "submitted frames did not commit within 60 s", || {
        node.counters().commit.load_acquire() >= target
    });
}

/// Keep submitting (and committing) until the leader's append counter is at
/// least `target`.
fn submit_until_append(node: &Node, target: u64) {
    while node.counters().append.load_acquire() < target {
        submit_frames(node, 256);
    }
}

fn await_capable(c: &Cluster) {
    let pages: Vec<Arc<CncPage>> = c.running().into_iter().map(|i| c.cnc(i)).collect();
    await_until(
        30,
        "every row published the snapshot-capability bit",
        || {
            pages.iter().all(|p| {
                p.service_slot(0).status.load_acquire() & CNC_SVC_STATUS_SNAPSHOT_CAPABLE != 0
            })
        },
    );
}

/// Command a FULL instant on the leader and wait until every node holds the
/// complete set at it.
fn instant_until_complete(c: &Cluster, leader: usize) -> u64 {
    let deadline = deadline_secs(30);
    let p = loop {
        match c.node(leader).command_snapshot(false) {
            Ok(p) => break p,
            Err(uc_node::SnapshotRefusal::Retry) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => panic!("uc2ctl snapshot refused: {e}"),
        }
    };
    await_until(
        30,
        "the early instant did not complete on every node",
        || {
            c.running()
                .iter()
                .all(|&i| c.node(i).snapshot_set_position() >= p)
        },
    );
    p
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

fn svc_cfg(dir: &Path, app: &str, boot_wait: Duration) -> uc_service::ServiceConfig {
    let mut cfg = uc_service::ServiceConfig::new(dir, app);
    cfg.boot_wait = boot_wait;
    cfg
}

fn stopper(s: uc_service::Service<SumSm>) -> Box<dyn FnOnce()> {
    Box::new(move || s.stop())
}

// ================================================================== tests

/// Run one cold-restart scenario. `target_past_set` is how far past the early
/// instant's position the log must end before the cluster is stopped.
fn cold_restart(app: &'static str, buffer_bytes: u64, settings: Settings, past_set: u64) {
    let _g = serialize();
    let o = Opts {
        app,
        services: ServicesConfig::single(<SumSm as uc_service::RawStateMachine>::NAME),
        buffer_bytes,
        settings,
    };
    let mut c = spawn(3, o);
    let leader = await_single_leader(&c, 30);
    for i in c.running() {
        let s = uc_service::ServiceBuilder::new(
            svc_cfg(&c.nodes[i].instance_dir, app, Duration::from_secs(60)),
            SumSm {
                total: 0,
                last: None,
            },
        )
        .start()
        .expect("service start");
        c.svcs.push(stopper(s));
    }
    await_capable(&c);

    // One early, complete instant, so the set exists on every node.
    submit_frames(c.node(leader), 64);
    let set = instant_until_complete(&c, leader);

    // Push the log end well past the set (and so past applied + fsm_lag at
    // the next boot, where applied reads 0).
    submit_until_append(c.node(leader), set + past_set);
    let end = c.node(leader).counters().append.load_acquire();
    // Let every node's durable reach the end so the old log is whole on disk.
    await_until(30, "pre-stop: every node durable at the log end", || {
        c.running()
            .iter()
            .all(|&i| c.node(i).counters().durable.load_acquire() >= end)
    });
    eprintln!("pre-stop: set={set} end={end} ({})", c.counters());

    // Whole-cluster cold restart: services down, nodes down, nodes up,
    // services up.
    c.stop_services();
    c.stop_nodes();
    for h in c.nodes.iter_mut() {
        h.start();
    }
    for i in 0..c.nodes.len() {
        let started = uc_service::ServiceBuilder::new(
            svc_cfg(&c.nodes[i].instance_dir, app, Duration::from_secs(30)),
            SumSm {
                total: 0,
                last: None,
            },
        )
        .start();
        match started {
            Ok(s) => c.svcs.push(stopper(s)),
            Err(e) => panic!(
                "#95: service {i} could not attach after the cold restart ({e}); \
                 pre-stop end={end}; {}",
                c.counters()
            ),
        }
    }

    // A new write must commit past the pre-stop log end.
    let deadline = deadline_secs(20);
    let mut submitted = false;
    loop {
        if !submitted {
            for i in 0..c.nodes.len() {
                if c.node(i).submit(vec![7u8; PAYLOAD]).is_ok() {
                    submitted = true;
                    break;
                }
            }
        }
        let best = (0..c.nodes.len())
            .map(|i| c.node(i).counters().commit.load_acquire())
            .max()
            .unwrap();
        if submitted && best > end {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "#95: after a whole-cluster cold restart commit never passed the pre-stop log end \
             {end} (submitted={submitted}); {}",
            c.counters()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// An explicit `fsm_lag` of 64 KiB on a 4 MiB ring; the log ends ≥ 512 KiB
/// past the set.
#[test]
fn a_whole_cluster_cold_restart_past_fsm_lag_commits() {
    cold_restart(
        "uc2-cold-lag",
        1 << 22,
        Settings {
            fsm_lag_bytes: 64 * 1024,
            ..Settings::genesis_default()
        },
        8 * 64 * 1024,
    );
}

/// DEFAULT settings on a 256 KiB ring: the default lag is `buffer / 4` =
/// 64 KiB; the log ends ≥ 8 default lags past the set. Decides whether a
/// default config deadlocks.
#[test]
fn a_whole_cluster_cold_restart_past_the_default_lag_commits() {
    let buffer = 1u64 << 18;
    cold_restart(
        "uc2-cold-default",
        buffer,
        Settings::genesis_default(),
        8 * (buffer / 4),
    );
}

/// Control: the log ends inside the lag (default lag = 1 MiB on a 4 MiB
/// ring; ~100 KiB of frames). Must pass on main.
#[test]
fn a_whole_cluster_cold_restart_inside_the_lag_commits() {
    cold_restart(
        "uc2-cold-inside",
        1 << 22,
        Settings::genesis_default(),
        100 * 1024,
    );
}
