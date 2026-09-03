// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Time and timers, end to end (spec §4): a service schedules through
//! `ApplyCtx::schedule`, the record reaches the node's per-row heap over the
//! sched ring, the leader appends a `TIMER` frame at the top of a pass in
//! global deadline order, and `Timed<S>` delivers it to `on_timer` exactly
//! once on every replica.
//!
//! Two tests:
//!
//! 1. One node: a timer fires once, at its deadline, in pass order (the §4.3
//!    partition property), a cancelled timer never fires, and the pending
//!    word on the cnc page reflects the still-armed instance.
//! 2. Three nodes: a timer in flight at a leader change is delivered exactly
//!    once, at the same log position on every surviving replica, either on
//!    time or late — never twice, never at diverging positions.
//!
//! Every instance dir is on the ext4 cargo target volume, never /tmp.

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use uc_client::Client;
use uc_consensus::election::NodeId;
use uc_log::cnc::CncPage;
use uc_net::fault::FaultConfig;
use uc_node::{CryptoConfig, FsmLag, Node, NodeConfig, PurgePolicy, ServicesConfig};
use uc_service::{
    ApplyCtx, RawStateMachine, Service, ServiceBuilder, ServiceConfig, StateMachine, Timed,
    TimerEvent,
};

pub const APP: &str = "uc2-timers";

static TEST_LOCK: Mutex<()> = Mutex::new(());
fn serialize() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("uc2-timers-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

fn names(names: &[&str], lag: Option<FsmLag>) -> ServicesConfig {
    ServicesConfig::from_names(names, lag).unwrap()
}

fn config(dir: &Path, services: ServicesConfig) -> NodeConfig {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    node_config(0, vec![(0, bind)], dir.to_path_buf(), 1, bind, services)
}

/// One member's config. Shared by the single-node test (which lets `Node::start`
/// bind) and the cluster helper (which pre-binds and calls
/// `start_with_socket`); the `bind` field is ignored in the latter case but is
/// still required.
fn node_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    instance_dir: PathBuf,
    seed: u64,
    addr: SocketAddr,
    services: ServicesConfig,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        learners: Vec::new(),
        bind: addr,
        instance_dir,
        app_id: APP.into(),
        buffer_bytes: 1 << 22, // 4 MiB
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed,
        faults: FaultConfig::default(),
        purge: PurgePolicy::Disabled,
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: CryptoConfig::Disabled,
        services,
    }
}

fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !f() {
        assert!(Instant::now() < deadline, "timeout waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn open_cnc(dir: &Path) -> std::sync::Arc<CncPage> {
    CncPage::open_file(&dir.join("cnc2.dat"), APP).expect("open cnc")
}

/// `services.rs::start_service`, but for an already-constructed state machine
/// (`Timed<ClockSm>` has no `Default`, and wrapping is the point here) and for
/// the raw tier (`Timed<S>` implements `RawStateMachine`, not `StateMachine`).
fn start_service_with<S: RawStateMachine>(dir: &Path, sm: S) -> Service<S> {
    ServiceBuilder::new(ServiceConfig::new(dir, APP), sm)
        .start()
        .expect("service start")
}

// ------------------------------------------------------------ the clock SM

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Cmd {
    At { id: u64, in_ms: u64 },
    Cancel { id: u64 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Fired {
    position: u64,
    id: u64,
    deadline_ns: u64,
    time_ns: u64,
    late: bool,
}

#[derive(Default)]
struct ClockSm {
    fired: Vec<Fired>,
    stamps: Vec<(u64, u64)>,
    last: Option<u64>,
}

impl StateMachine for ClockSm {
    const NAME: &'static str = "clock";

    type Command = Cmd;
    /// The leader stamp the command was applied at.
    type Response = u64;
    type Query = ();
    type QueryResponse = (Vec<Fired>, Vec<(u64, u64)>);

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Cmd) -> u64 {
        match cmd {
            Cmd::At { id, in_ms } => ctx.schedule(id, ctx.time_ns + in_ms * 1_000_000),
            Cmd::Cancel { id } => ctx.cancel(id),
        }
        self.stamps.push((ctx.position, ctx.time_ns));
        self.last = Some(ctx.position);
        ctx.time_ns
    }
    fn query(&self, _q: ()) -> Self::QueryResponse {
        (self.fired.clone(), self.stamps.clone())
    }
    fn last_applied(&self) -> Option<u64> {
        self.last
    }
    fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {
        self.fired.push(Fired {
            position: ctx.position,
            id: ev.id,
            deadline_ns: ev.deadline_ns,
            time_ns: ctx.time_ns,
            late: ev.late(ctx),
        });
        self.stamps.push((ctx.position, ctx.time_ns));
        self.last = Some(ctx.position);
    }
}

/// `Service::query` is typed-tier only, and `Timed<S>` is a RAW state machine
/// (no `StateMachine` impl, so no associated `Query` type to route). Go through
/// the raw path with the same bincode-standard codec the blanket impl uses —
/// `Timed::query` forwards straight to the inner SM's.
fn query(svc: &Service<Timed<ClockSm>>) -> (Vec<Fired>, Vec<(u64, u64)>) {
    let q = bincode::serde::encode_to_vec((), bincode::config::standard()).expect("encode");
    let mut out = Vec::new();
    svc.query_raw(&q, &mut out);
    bincode::serde::decode_from_slice(&out, bincode::config::standard())
        .expect("decode")
        .0
}

// ------------------------------------------------------- 1. the single node

#[test]
fn a_scheduled_timer_fires_at_its_deadline_in_order_and_once() {
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), names(&["clock"], None))).unwrap();
    wait_until("serving", || node.can_serve());
    let svc = start_service_with(dir.path(), Timed::new(ClockSm::default()));
    let client = Client::connect(dir.path(), APP).unwrap();

    let t0: u64 = client.submit(&Cmd::At { id: 1, in_ms: 200 }).unwrap();
    let _: u64 = client.submit(&Cmd::At { id: 2, in_ms: 50 }).unwrap();
    let _: u64 = client.submit(&Cmd::Cancel { id: 2 }).unwrap();

    // Keep the log moving so stamps around the deadline exist (and leave a
    // long-dated instance armed for the pending-word check).
    let until = Instant::now() + Duration::from_millis(600);
    while Instant::now() < until {
        let _: u64 = client
            .submit(&Cmd::At {
                id: 99,
                in_ms: 10_000,
            })
            .unwrap();
        std::thread::sleep(Duration::from_millis(5));
    }

    wait_until("timer 1 fired", || query(&svc).0.iter().any(|f| f.id == 1));
    let (fired, stamps) = query(&svc);

    let f1: Vec<_> = fired.iter().filter(|f| f.id == 1).collect();
    assert_eq!(f1.len(), 1, "exactly once: {fired:?}");
    assert_eq!(f1[0].deadline_ns, t0 + 200_000_000);
    assert!(!f1[0].late, "{f1:?}");
    // A timer frame is stamped with its own deadline unless an earlier frame
    // already carried the clock past it — which pass order forbids here.
    assert_eq!(f1[0].time_ns, f1[0].deadline_ns);
    assert!(fired.iter().all(|f| f.id != 2), "cancelled: {fired:?}");

    // §4.3: every frame before the timer is stamped <= its deadline, every
    // frame after it >= the deadline.
    let (before, after): (Vec<_>, Vec<_>) = stamps.iter().partition(|(p, _)| *p < f1[0].position);
    assert!(
        before.iter().all(|(_, t)| *t <= f1[0].deadline_ns),
        "{before:?}"
    );
    assert!(
        after.iter().all(|(_, t)| *t >= f1[0].deadline_ns),
        "{after:?}"
    );
    assert!(
        stamps.windows(2).all(|w| w[0].1 <= w[1].1),
        "monotone: {stamps:?}"
    );

    let cnc = open_cnc(dir.path());
    // The id-99 instance is still armed (each At{99} replaces the last).
    wait_until("pending word", || {
        cnc.service_slot(0).identity.timers_pending() >= 1
    });

    client.shutdown();
    svc.stop();
    node.stop();
}

/// Final-review C1: the log-time seed survives a node restart. The cnc page is
/// recreated zeroed at every boot, so without the archive's `open`-time
/// recovery a restarted node that wins the next election would seed its
/// appender with 0 and stamp from its raw wall clock — below the previous
/// leader's last stamp if this host's clock lags. Read the word immediately
/// after `Node::start` returns, i.e. before the election can append anything.
#[test]
fn the_log_time_seed_survives_a_restart_of_the_same_instance_dir() {
    let _g = serialize();
    let dir = tempdir();
    let before: u64;
    {
        let node = Node::start(config(dir.path(), names(&["clock"], None))).unwrap();
        wait_until("serving", || node.can_serve());
        let svc = start_service_with(dir.path(), Timed::new(ClockSm::default()));
        let client = Client::connect(dir.path(), APP).unwrap();
        let _: u64 = client
            .submit(&Cmd::At {
                id: 7,
                in_ms: 10_000,
            })
            .unwrap();
        // The word only moves once the archive has recorded the block holding
        // the frame; wait for it rather than racing the archive agent.
        let cnc = open_cnc(dir.path());
        wait_until("archive published a stamp", || cnc.log_time_ns() > 0);
        before = cnc.log_time_ns();
        client.shutdown();
        svc.stop();
        node.stop();
    }

    let node = Node::start(config(dir.path(), names(&["clock"], None))).unwrap();
    let seeded = open_cnc(dir.path()).log_time_ns();
    assert!(
        seeded >= before,
        "log time went backwards across a restart: {seeded} < {before}"
    );
    assert_ne!(
        seeded, 0,
        "the seed must be recovered from the journal before any new frame"
    );
    node.stop();
}

// -------------------------------------------------------- 2. leader change

/// One cluster member: its instance dir (a service attaches there) and the
/// live node, taken out of the `Option` on crash so the handle survives.
struct NodeH {
    instance_dir: PathBuf,
    node: Option<Node>,
}

impl NodeH {
    fn can_serve(&self) -> bool {
        self.node.as_ref().is_some_and(|n| n.can_serve())
    }
    fn is_leader(&self) -> bool {
        self.node.as_ref().is_some_and(|n| n.is_leader())
    }
}

/// `failover.rs`'s spawn pattern (bind every socket first so the full member
/// map is known before any agent runs), with one declared FSM row so each node
/// can carry a `Timed<ClockSm>` service.
struct Cluster {
    _dir: tempfile::TempDir,
    nodes: Vec<NodeH>,
}

fn spawn_cluster(n: usize) -> Cluster {
    let dir = tempdir();
    let socks: Vec<UdpSocket> = (0..n)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind"))
        .collect();
    let members: Vec<(NodeId, SocketAddr)> = socks
        .iter()
        .enumerate()
        .map(|(i, s)| (i as NodeId, s.local_addr().unwrap()))
        .collect();
    let mut nodes = Vec::with_capacity(n);
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = members[i].1;
        let instance_dir = dir.path().join(format!("n{i}"));
        // A distinct, index-derived seed so the randomized election timeouts
        // differ and a clean boot elects exactly one leader.
        let seed = 0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let cfg = node_config(
            i as NodeId,
            members.clone(),
            instance_dir.clone(),
            seed,
            addr,
            names(&["clock"], None),
        );
        let node = Node::start_with_socket(cfg, sock).expect("start");
        nodes.push(NodeH {
            instance_dir,
            node: Some(node),
        });
    }
    Cluster { _dir: dir, nodes }
}

/// Wait for exactly one serving leader among `idxs`, asserting no split-brain.
fn await_leader_among(nodes: &[NodeH], idxs: &[usize], secs: u64) -> usize {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let serving: Vec<usize> = idxs
            .iter()
            .copied()
            .filter(|&i| nodes[i].can_serve())
            .collect();
        assert!(serving.len() <= 1, "split-brain: {serving:?} all serve");
        if let [i] = serving[..] {
            assert!(
                nodes[i].is_leader(),
                "serving node {i} is not flagged leader"
            );
            return i;
        }
        assert!(Instant::now() < deadline, "no single leader among {idxs:?}");
        std::thread::yield_now();
    }
}

#[test]
fn a_timer_in_flight_at_a_leader_change_fires_late_and_is_delivered_once() {
    let _g = serialize();
    let mut c = spawn_cluster(3);
    let all: Vec<usize> = (0..3).collect();
    let mut svcs: Vec<Option<Service<Timed<ClockSm>>>> = c
        .nodes
        .iter()
        .map(|h| {
            Some(start_service_with(
                &h.instance_dir,
                Timed::new(ClockSm::default()),
            ))
        })
        .collect();
    let leader = await_leader_among(&c.nodes, &all, 30);

    // Schedule +300 ms on the leader, then take the leader out immediately —
    // the instance is in flight (armed, undelivered) across the election.
    let client = Client::connect(&c.nodes[leader].instance_dir, APP).unwrap();
    let t0: u64 = client.submit(&Cmd::At { id: 5, in_ms: 300 }).unwrap();
    let deadline_ns = t0 + 300_000_000;
    client.shutdown();

    svcs[leader].take().unwrap().crash();
    c.nodes[leader].node.take().unwrap().crash();

    let survivors: Vec<usize> = all.iter().copied().filter(|&i| i != leader).collect();
    await_leader_among(&c.nodes, &survivors, 30);

    for &i in &survivors {
        let svc = svcs[i].as_ref().unwrap();
        wait_until(&format!("node {i} saw timer 5"), || {
            query(svc).0.iter().any(|f| f.id == 5)
        });
    }

    // Give any duplicate a chance to land before reading the final answer:
    // the re-arm path would append a SECOND TIMER frame for the same
    // instance, and `Timed` is what must drop it.
    std::thread::sleep(Duration::from_millis(300));

    let mut positions: Vec<(usize, u64)> = Vec::new();
    for &i in &survivors {
        let (fired, stamps) = query(svcs[i].as_ref().unwrap());
        let f5: Vec<_> = fired.iter().filter(|f| f.id == 5).collect();
        assert_eq!(f5.len(), 1, "node {i}: exactly once: {fired:?}");
        let f = f5[0];
        assert_eq!(f.deadline_ns, deadline_ns, "node {i}: {f:?}");
        if f.late {
            assert!(
                f.time_ns > f.deadline_ns,
                "node {i}: late but not past: {f:?}"
            );
        } else {
            assert_eq!(f.time_ns, f.deadline_ns, "node {i}: on time: {f:?}");
        }
        assert!(
            stamps.windows(2).all(|w| w[0].1 <= w[1].1),
            "node {i} monotone: {stamps:?}"
        );
        positions.push((i, f.position));
    }
    let (first, pos) = positions[0];
    for &(i, p) in &positions[1..] {
        assert_eq!(
            p, pos,
            "node {i} delivered timer 5 at {p}, node {first} at {pos}"
        );
    }

    for &i in &survivors {
        svcs[i].take().unwrap().stop();
    }
    for h in &mut c.nodes {
        if let Some(n) = h.node.take() {
            n.stop();
        }
    }
}
