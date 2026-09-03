// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Time and timers, end to end (spec §4): a service schedules through
//! `ApplyCtx::schedule`, the record reaches the node's per-row heap over the
//! sched ring, the leader appends a `TIMER` frame at the top of a pass in
//! global deadline order, and `Timed<S>` delivers it to `on_timer` exactly
//! once on every replica.
//!
//! The tests:
//!
//! 1. One node: a timer fires once, at its deadline, in pass order (the §4.3
//!    partition property), a cancelled timer never fires, and the pending
//!    word on the cnc page reflects the still-armed instance.
//! 2. Three nodes: a timer in flight at a leader change is delivered exactly
//!    once, at the same log position on every surviving replica, either on
//!    time or late — never twice, never at diverging positions.
//! 3. One node, final-review I2: MORE than `TIMERS_PER_PASS` (64) timers due
//!    at one instant, under continuous pipelined client load — the pass bound
//!    is hit, so the pass appends NO client frame and the 100 TIMER frames
//!    land contiguously. The held clients are answered normally one pass
//!    later and every fire is on time.
//! 4. Plan 2 (spec §5), one node: an applied SCHEDULE TABLE ticks its `every`
//!    entry once per occurrence and advances from the tick, and its `once`
//!    entry fires once and parks (leaving the cnc pending word at 1).
//! 5. Plan 2, across a restart: five periods of downtime are caught up by ONE
//!    tick, at the latest occurrence at or before the leader's clock, and the
//!    already-delivered `once` is not delivered a second time even though the
//!    node may re-append it.
//!
//! Every instance dir is on the ext4 cargo target volume, never /tmp.

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use uc_client::{Client, PipelinedClient, PipelinedConfig};
use uc_consensus::election::NodeId;
use uc_log::cnc::{AdminReq, AdminResp, CncPage};
use uc_net::fault::FaultConfig;
use uc_node::{CryptoConfig, FsmLag, Node, NodeConfig, PurgePolicy, ServicesConfig};
use uc_protocol::identity::FsmName;
use uc_protocol::v2::cnc::{ADMIN_OP_SCHEDULE_APPLY, CNC_MAX_PEER_SLOTS};
use uc_protocol::v2::schedule::{
    ScheduleEntry, ScheduleRule, ScheduleTable, decode_schedule_table, encode_schedule_table,
};
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
    At {
        id: u64,
        in_ms: u64,
    },
    /// Schedule at an ABSOLUTE log time — so a batch of these all share one
    /// deadline and all become due in the same leader pass (test 3).
    AtAbs {
        id: u64,
        at_ns: u64,
    },
    Cancel {
        id: u64,
    },
    /// Pure load: touches no timer, just puts a MESSAGE frame on the log.
    Nop,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Fired {
    position: u64,
    id: u64,
    deadline_ns: u64,
    time_ns: u64,
    late: bool,
    /// Plan 2: the fire came from the replicated SCHEDULE TABLE
    /// (`FLAG_TIMER_TABLE`), not from a `ctx.schedule` this SM asked for.
    table: bool,
}

#[derive(Default)]
struct ClockSm {
    fired: Vec<Fired>,
    /// `(position, time_ns, kind)`; kind 0 = MESSAGE apply, 1 = TIMER fire.
    stamps: Vec<(u64, u64, u8)>,
    last: Option<u64>,
}

impl StateMachine for ClockSm {
    const NAME: &'static str = "clock";

    type Command = Cmd;
    /// The leader stamp the command was applied at.
    type Response = u64;
    type Query = ();
    type QueryResponse = (Vec<Fired>, Vec<(u64, u64, u8)>);

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Cmd) -> u64 {
        match cmd {
            Cmd::At { id, in_ms } => ctx.schedule(id, ctx.time_ns + in_ms * 1_000_000),
            Cmd::AtAbs { id, at_ns } => ctx.schedule(id, at_ns),
            Cmd::Cancel { id } => ctx.cancel(id),
            Cmd::Nop => {}
        }
        self.stamps.push((ctx.position, ctx.time_ns, 0));
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
            table: ev.table,
        });
        self.stamps.push((ctx.position, ctx.time_ns, 1));
        self.last = Some(ctx.position);
    }
}

/// Plan 3's capstone needs a below-floor joiner, and a joiner only falls below
/// a floor that MOVES — which takes a real snapshotting service. `ClockSm`'s
/// whole state is its two vectors, so the artifact is just their serde image.
/// (`Timed<S>` gets `SnapshotStateMachine` for free from this one, wrapping it
/// with its own exactly-once image.)
#[derive(Serialize, Deserialize)]
struct ClockImage {
    fired: Vec<Fired>,
    stamps: Vec<(u64, u64, u8)>,
}

impl uc_service::SnapshotStateMachine for ClockSm {
    type SnapshotHandle = Vec<u8>;

    fn freeze(&self) -> Result<(Vec<u8>, u64), uc_service::SnapshotError> {
        let img = ClockImage {
            fired: self.fired.clone(),
            stamps: self.stamps.clone(),
        };
        let bytes = bincode::serde::encode_to_vec(&img, bincode::config::standard())
            .map_err(|e| uc_service::SnapshotError::Codec(format!("clock image encode: {e}")))?;
        Ok((bytes, self.last.unwrap_or(0)))
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
        let (img, _): (ClockImage, _) =
            bincode::serde::decode_from_slice(&buf, bincode::config::standard()).map_err(|e| {
                uc_service::SnapshotError::Codec(format!("clock image decode: {e}"))
            })?;
        self.fired = img.fired;
        self.stamps = img.stamps;
        self.last = Some(position);
        Ok(position)
    }
}

/// `Service::query` is typed-tier only, and `Timed<S>` is a RAW state machine
/// (no `StateMachine` impl, so no associated `Query` type to route). Go through
/// the raw path with the same bincode-standard codec the blanket impl uses —
/// `Timed::query` forwards straight to the inner SM's.
fn query(svc: &Service<Timed<ClockSm>>) -> (Vec<Fired>, Vec<(u64, u64, u8)>) {
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
    let (before, after): (Vec<_>, Vec<_>) =
        stamps.iter().partition(|(p, _, _)| *p < f1[0].position);
    assert!(
        before.iter().all(|(_, t, _)| *t <= f1[0].deadline_ns),
        "{before:?}"
    );
    assert!(
        after.iter().all(|(_, t, _)| *t >= f1[0].deadline_ns),
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

// ------------------------------------------------ 3. the per-pass timer bound

/// Final-review I2: `TIMERS_PER_PASS = 64` is the rule that keeps §4.3's
/// ordering guarantee honest — when step 2 hits the bound, step 3 appends no
/// client frame at all, because a client frame between two due timers would
/// clamp the log clock past the second one's deadline and make it late.
///
/// 100 timers share one absolute deadline, so they all come due in the same
/// pass: 64 fire, clients are held, 36 fire next pass. A pipelined client
/// keeps the ingress ring continuously non-empty across that window, so the
/// hold is what keeps the TIMER frames contiguous — remove the `hold_clients`
/// skip in `do_work` and a MESSAGE frame lands between them.
#[test]
fn the_timer_bound_holds_client_frames_for_a_pass() {
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), names(&["clock"], None))).unwrap();
    wait_until("serving", || node.can_serve());
    let svc = start_service_with(dir.path(), Timed::new(ClockSm::default()));
    let client =
        Arc::new(PipelinedClient::connect(dir.path(), APP, PipelinedConfig::default()).unwrap());

    // GROUPS batches of PER_GROUP timers. Each batch shares one absolute
    // deadline, so its members all come due in the same pass and, being more
    // than `TIMERS_PER_PASS = 64`, span several passes that must append no
    // client frame at all. The batches are 20 ms apart, so the test looks at
    // the rule several times over, at different points of the client load.
    const GROUPS: u64 = 10;
    const PER_GROUP: u64 = 200;
    const N: u64 = GROUPS * PER_GROUP;
    const _: () = assert!(PER_GROUP > 64, "a batch must exceed TIMERS_PER_PASS");

    // One command to learn the log's clock, then absolute deadlines far
    // enough out that every schedule is pending before the first one arrives.
    let t0: u64 = client.submit(&Cmd::Nop).unwrap().wait().unwrap();
    let deadline = |g: u64| t0 + 300_000_000 + g * 20_000_000;
    let armed: Vec<_> = (0..GROUPS)
        .flat_map(|g| {
            (0..PER_GROUP)
                .map(move |i| (g, g * PER_GROUP + i))
                .map(|(g, id)| {
                    client
                        .submit::<_, u64>(&Cmd::AtAbs {
                            id,
                            at_ns: deadline(g),
                        })
                        .unwrap()
                })
        })
        .collect();
    for t in armed {
        t.wait().unwrap();
    }

    // Continuous pipelined load: eight INDEPENDENT clients (one `SendHalf`
    // mutex each — sharing one `PipelinedClient` across threads serialises the
    // producers and empties the ring), each holding a deep window of
    // outstanding MESSAGE frames, so the ingress ring is non-empty when the
    // timers come due.
    let stop = Arc::new(AtomicBool::new(false));
    let loads: Vec<_> = (0..8)
        .map(|_| {
            let stop = Arc::clone(&stop);
            let dir = dir.path().to_path_buf();
            std::thread::spawn(move || {
                let c = PipelinedClient::connect(&dir, APP, PipelinedConfig::default()).unwrap();
                let mut inflight = std::collections::VecDeque::new();
                let mut answered = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    while inflight.len() < 256 {
                        match c.try_submit::<_, u64>(&Cmd::Nop) {
                            Ok(t) => inflight.push_back(t),
                            Err(_) => break, // ring full: exactly the state we want
                        }
                    }
                    if let Some(t) = inflight.pop_front() {
                        // A held client command must still be ANSWERED — the
                        // hold is one pass of backpressure, not a refusal.
                        t.wait().expect("a held client command is still answered");
                        answered += 1;
                    }
                }
                for t in inflight {
                    let _ = t.wait();
                }
                c.shutdown();
                answered
            })
        })
        .collect();

    wait_until("all timers fired", || query(&svc).0.len() as u64 >= N);
    stop.store(true, Ordering::Relaxed);
    let answered: u64 = loads
        .into_iter()
        .map(|h| h.join().expect("load thread"))
        .sum();
    assert!(answered > 0, "the load threads answered nothing");

    let (fired, stamps) = query(&svc);
    assert_eq!(fired.len() as u64, N, "each instance fired exactly once");

    for g in 0..GROUPS {
        let d = deadline(g);
        let grp: Vec<_> = fired.iter().filter(|f| f.deadline_ns == d).collect();
        assert_eq!(
            grp.len() as u64,
            PER_GROUP,
            "group {g}: wrong size, {} fired",
            grp.len()
        );
        // (a) the batch's TIMER frames are contiguous: no MESSAGE frame
        // between the first and the last, though client load was in flight
        // the whole time. This is the `hold_clients` rule.
        let first = grp.iter().map(|f| f.position).min().unwrap();
        let last = grp.iter().map(|f| f.position).max().unwrap();
        let between: Vec<_> = stamps
            .iter()
            .filter(|(p, _, kind)| *kind == 0 && *p > first && *p < last)
            .collect();
        assert!(
            between.is_empty(),
            "group {g}: {} client frames landed between two due timers \
             (timers {first}..={last}); first few: {:?}",
            between.len(),
            &between[..between.len().min(4)]
        );
        // (c) every fire on time — which is the consequence: a client frame
        // inside the run would clamp the clock past the deadline.
        assert!(
            grp.iter().all(|f| !f.late && f.time_ns == d),
            "group {g}: a fire was late: {:?}",
            grp.iter().filter(|f| f.late).take(4).collect::<Vec<_>>()
        );
    }

    // The load really was flowing across the whole window: client frames on
    // both sides of the first and the last timer run.
    let (lo, hi) = (
        fired.iter().map(|f| f.position).min().unwrap(),
        fired.iter().map(|f| f.position).max().unwrap(),
    );
    assert!(
        stamps.iter().any(|(p, _, k)| *k == 0 && *p < lo),
        "no client frame before the timer runs"
    );
    assert!(
        stamps.iter().any(|(p, _, k)| *k == 0 && *p > hi),
        "no client frame after the timer runs — the hold was never released"
    );
    // (b) the held frames were applied, not dropped (the load threads also
    // unwrapped every ticket).
    assert!(
        stamps.iter().filter(|(_, _, k)| *k == 0).count() as u64 > N,
        "client commands were applied normally"
    );
    // The whole log stays monotone in time across every hold.
    assert!(
        stamps.windows(2).all(|w| w[0].1 <= w[1].1),
        "monotone: {} stamps",
        stamps.len()
    );

    Arc::try_unwrap(client).ok().unwrap().shutdown();
    svc.stop();
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

// --------------------------------------------- 4. the replicated schedule table

/// The table tests' period. Long enough that a tick lands in its own pass on a
/// busy box, short enough that five of them fit in a test.
const TABLE_PERIOD_NS: u64 = 200_000_000;

/// Stage a table in `dir` and drive `ADMIN_OP_SCHEDULE_APPLY` through the cnc
/// admin band — `uc2ctl schedule apply` minus the bin. Under the default
/// [`uc_node::AdminPolicy::Filesystem`] the auth line is ignored, so nothing is
/// signed here; `admin_auth.rs` covers the signed path. Returns the accepted
/// table's position (the frame END).
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
        let deadline = Instant::now() + Duration::from_secs(20);
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
        // `2` = retry, side-effect-free: a leader that has the role but has
        // not finished its leader-open collapse has no appender yet, and
        // `uc2ctl` polls through exactly that window. Anything else is a
        // genuine refusal and the test should say so.
        assert_eq!(
            resp.status, 2,
            "schedule apply was refused: reason {}",
            resp.reason
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("schedule apply never left the retry window");
}

/// Row 0's identity hash — what a table entry names its FSM by.
fn clock_hash() -> u64 {
    FsmName::parse(<ClockSm as StateMachine>::NAME)
        .expect("a valid FSM name")
        .hash()
}

/// The two-entry table both tests apply: a repeating `every` and a one-shot
/// `once` 300 ms out, both on row 0.
fn two_entry_table(anchor_ns: u64, once_at_ns: u64) -> ScheduleTable {
    ScheduleTable {
        entries: vec![
            ScheduleEntry {
                identity_hash: clock_hash(),
                timer_id: 1,
                rule: ScheduleRule::Every {
                    period_ns: TABLE_PERIOD_NS,
                    anchor_ns,
                },
            },
            ScheduleEntry {
                identity_hash: clock_hash(),
                timer_id: 2,
                rule: ScheduleRule::Once { at_ns: once_at_ns },
            },
        ],
    }
}

/// Plan 2 (spec §5), one node: an applied table ticks its `every` entry once
/// per occurrence and ADVANCES FROM THE TICK (never from the pass clock, which
/// would drift), and its `once` entry fires exactly once and then parks — the
/// parked entry drops out of the cnc pending word while the repeating one
/// stays.
///
/// The FIRST tick is deliberately a catch-up: the anchor is `now` rounded down
/// to a period boundary, so the entry is already due the moment it is adopted.
/// It is therefore stamped at the table frame's own clock (the appender clamps
/// a TIMER frame's stamp up to `last_stamp`) and reads LATE — every tick after
/// it is stamped exactly at its deadline, which is what "advances from the
/// tick" means.
#[test]
fn a_schedule_table_ticks_exactly_once_per_deadline_and_advances_from_the_tick() {
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), names(&["clock"], None))).unwrap();
    wait_until("serving", || node.can_serve());
    let svc = start_service_with(dir.path(), Timed::new(ClockSm::default()));
    let client = Client::connect(dir.path(), APP).unwrap();
    let cnc = open_cnc(dir.path());

    // One command to learn the LOG's clock: an anchor in any other clock is a
    // different rule (the node arms and fires against log time alone).
    let t0: u64 = client.submit(&Cmd::Nop).unwrap();
    let anchor = t0 - t0 % TABLE_PERIOD_NS;
    let once_at = t0 + 300_000_000;
    let table = two_entry_table(anchor, once_at);
    let position = apply_schedule_table(dir.path(), &cnc, &table);
    assert!(position > 0, "an accepted apply reports the frame END");

    wait_until("five ticks of the every entry", || {
        query(&svc).0.iter().filter(|f| f.id == 1).count() >= 5
    });
    wait_until("the once entry fired", || {
        query(&svc).0.iter().any(|f| f.id == 2)
    });
    // The parked `once` must fall out of the pending word; the repeating entry
    // must not.
    wait_until("the parked once left the pending word", || {
        cnc.service_slot(0).identity.timers_pending() == 1
    });

    let (fired, _) = query(&svc);
    assert!(
        fired.iter().all(|f| f.table),
        "every fire here came from the table: {fired:?}"
    );
    let mut seen: Vec<(u64, u64)> = fired.iter().map(|f| (f.id, f.deadline_ns)).collect();
    let before = seen.len();
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(
        seen.len(),
        before,
        "a (id, deadline) fired twice: {fired:?}"
    );

    let every: Vec<&Fired> = fired.iter().filter(|f| f.id == 1).collect();
    assert!(every.len() >= 5, "{every:?}");
    assert_eq!(
        (every[0].deadline_ns - anchor) % TABLE_PERIOD_NS,
        0,
        "the first tick is an occurrence of the rule: {:?}",
        every[0]
    );
    assert!(every[0].time_ns >= every[0].deadline_ns, "{:?}", every[0]);
    for w in every.windows(2) {
        assert_eq!(
            w[1].deadline_ns - w[0].deadline_ns,
            TABLE_PERIOD_NS,
            "consecutive ticks are exactly one period apart: {:?} then {:?}",
            w[0],
            w[1]
        );
        assert!(
            w[1].position > w[0].position,
            "ticks arrive in log order: {:?} then {:?}",
            w[0],
            w[1]
        );
    }
    // Every tick after the catch-up is stamped AT its deadline: the entry
    // advanced from the tick that fired, not from the clock that fired it.
    for f in &every[1..] {
        assert!(!f.late, "a steady-state tick was late: {f:?}");
        assert_eq!(f.time_ns, f.deadline_ns, "{f:?}");
    }

    let once: Vec<&Fired> = fired.iter().filter(|f| f.id == 2).collect();
    assert_eq!(once.len(), 1, "the once fired exactly once: {once:?}");
    assert_eq!(once[0].deadline_ns, once_at, "{:?}", once[0]);
    assert!(
        !once[0].late,
        "the once was in the future when applied: {once:?}"
    );

    // ... and it stays fired-once: give the heap several more periods to
    // re-offer it before believing the park.
    std::thread::sleep(Duration::from_millis(600));
    let (fired, _) = query(&svc);
    assert_eq!(
        fired.iter().filter(|f| f.id == 2).count(),
        1,
        "a parked once fired again: {fired:?}"
    );

    // The durable record is what a restart re-arms from (test 5): the table
    // is on disk, at the position the apply reported, with both entries.
    let rec = uc_node::read_record(dir.path())
        .expect("read the record")
        .expect("a record exists once a table has been adopted");
    assert_eq!(rec.position, position);
    assert_eq!(
        decode_schedule_table(&rec.table).expect("the record holds wire bytes"),
        table
    );

    client.shutdown();
    svc.stop();
    node.stop();
}

/// Plan 2 (spec §5), the one-tick catch-up across a restart: a node down for
/// five periods comes back and fires the `every` entry ONCE, at the latest
/// occurrence at or before the leader's clock — not five times walking the
/// backlog. The `once` entry is a second, sharper case: boot arming has no
/// delivered set (the record does not carry one), so the node MAY re-append
/// the already-fired instance before the service announces its `table_last`,
/// and `Timed` is what must drop that duplicate — the FSM sees it at most once
/// across both runs.
#[test]
fn a_restarted_node_resumes_the_table_with_one_catch_up_tick() {
    let _g = serialize();
    let dir = tempdir();
    let anchor;
    let once_at;
    let position;
    let pre: Vec<Fired>;
    {
        let node = Node::start(config(dir.path(), names(&["clock"], None))).unwrap();
        wait_until("serving", || node.can_serve());
        let svc = start_service_with(dir.path(), Timed::new(ClockSm::default()));
        let client = Client::connect(dir.path(), APP).unwrap();
        let cnc = open_cnc(dir.path());

        let t0: u64 = client.submit(&Cmd::Nop).unwrap();
        anchor = t0 - t0 % TABLE_PERIOD_NS;
        once_at = t0 + 300_000_000;
        position = apply_schedule_table(dir.path(), &cnc, &two_entry_table(anchor, once_at));

        wait_until("three ticks of the every entry", || {
            query(&svc).0.iter().filter(|f| f.id == 1).count() >= 3
        });
        wait_until("the once entry fired", || {
            query(&svc).0.iter().any(|f| f.id == 2)
        });
        // The record's clock is the LOG's, published by the archive agent;
        // boot arming reads that word, so wait for it to be real rather than
        // racing the archive.
        wait_until("the archive published a stamp", || cnc.log_time_ns() > 0);
        pre = query(&svc).0;
        client.shutdown();
        svc.stop();
        node.stop();
    }
    let last_pre = pre
        .iter()
        .filter(|f| f.id == 1)
        .map(|f| f.deadline_ns)
        .max()
        .expect("the every entry ticked before the restart");

    // Five periods of downtime — the backlog a naive "advance one period per
    // fire" implementation would replay.
    std::thread::sleep(Duration::from_millis(1_000));

    let node = Node::start(config(dir.path(), names(&["clock"], None))).unwrap();
    // Read BEFORE anything can be adopted off the log: the table a restarted
    // node runs comes from the durable record, not from a replay.
    let rec = uc_node::read_record(dir.path())
        .expect("read the record")
        .expect("the record survived the restart");
    assert_eq!(rec.position, position, "the record was reloaded as adopted");
    assert_eq!(
        decode_schedule_table(&rec.table).map(|t| t.entries.len()),
        Some(2)
    );

    let svc = start_service_with(dir.path(), Timed::new(ClockSm::default()));
    wait_until("serving again", || node.can_serve());
    wait_until("three ticks after the restart", || {
        query(&svc)
            .0
            .iter()
            .filter(|f| f.id == 1 && f.deadline_ns > last_pre)
            .count()
            >= 3
    });
    let (fired, _) = query(&svc);

    // The FSM's whole record — the pre-restart fires come back through
    // journal replay, so this is both runs at once.
    let rule = ScheduleRule::Every {
        period_ns: TABLE_PERIOD_NS,
        anchor_ns: anchor,
    };
    let every: Vec<&Fired> = fired.iter().filter(|f| f.id == 1).collect();
    let post: Vec<&Fired> = every
        .iter()
        .copied()
        .filter(|f| f.deadline_ns > last_pre)
        .collect();
    let catch_up = post[0];
    // ONE tick, not five: the first fire after the gap is the LATEST
    // occurrence at or before the leader's clock when it fired (ruling R11),
    // and the four or so occurrences the downtime covered are simply absent.
    assert_eq!(
        rule.latest_at_or_before(catch_up.time_ns),
        Some(catch_up.deadline_ns),
        "the catch-up tick is not the newest occurrence its own stamp admits \
         — a backlog was replayed: {catch_up:?}"
    );
    assert!(
        catch_up.deadline_ns >= last_pre + 4 * TABLE_PERIOD_NS,
        "the 1 s gap covered at least four occurrences, so the catch-up must \
         skip them: last before the restart {last_pre}, first after {catch_up:?}"
    );
    assert_eq!(
        every
            .iter()
            .filter(|f| f.deadline_ns > last_pre && f.deadline_ns <= catch_up.deadline_ns)
            .count(),
        1,
        "exactly one tick covered the whole gap: {every:?}"
    );
    // ... and the entry keeps ticking from there, one period at a time.
    for w in post.windows(2) {
        assert_eq!(
            w[1].deadline_ns - w[0].deadline_ns,
            TABLE_PERIOD_NS,
            "{:?} then {:?}",
            w[0],
            w[1]
        );
    }
    assert!(
        post[1..].iter().all(|f| !f.late),
        "a steady-state tick after the restart was late: {post:?}"
    );
    assert!(
        fired.iter().all(|f| f.table),
        "every fire here came from the table: {fired:?}"
    );

    // The parked `once`: the node may well have re-appended it (boot arming
    // has no delivered set), but the FSM must not see it twice.
    assert_eq!(
        fired.iter().filter(|f| f.id == 2).count(),
        1,
        "the parked once was delivered again after the restart: {fired:?}"
    );

    svc.stop();
    node.stop();
}

// ------------- 6. plan 3: the promoted below-floor joiner keeps the schedule

/// The capstone's journal segment (and snapshot interval): small, so a few
/// thousand commands are enough to move the purge floor a whole segment and
/// put a fresh joiner genuinely below it.
const CAPSTONE_SEG: u64 = 64 * 1024;

/// The joiner's election window, and the original voters'. Skewed by a factor
/// of ~30 — when the leader dies the promoted joiner is the only node whose
/// timer can plausibly expire, so the survivor grants rather than campaigns.
/// See the test's own comment for why that is sound and not just convenient.
/// (The joiner's floor stays well above the sender's 20 ms heartbeat, so a
/// live leader keeps resetting it and it never campaigns spuriously.)
const JOINER_ELECTION_NS: (u64, u64) = (100_000_000, 150_000_000);
const ORIGINAL_ELECTION_NS: (u64, u64) = (3_000_000_000, 5_000_000_000);

/// The capstone's own table period — deliberately LONGER than
/// [`TABLE_PERIOD_NS`], which the single-node table tests use. Every fire is
/// checked with `latest_at_or_before(time_ns) == Some(deadline)`, and the
/// margin that assertion has before a stamp slips into the next occurrence is
/// one period minus however late the stamp is; a cluster of three debug-build
/// nodes on a loaded box wants two periods' worth of it, not one. Five fires
/// is then a ~2 s window — still far inside the budget.
const CAPSTONE_PERIOD_NS: u64 = 400_000_000;

/// Headroom on top of one period plus one election in the gap bar below: the
/// new leader's leader-open collapse and its service's re-attach both sit
/// between the election and the first append, and neither is instant in a
/// debug build. Not derived from a measurement — it is deliberate slack.
/// Measured on this fixture (at the 200 ms period it first used): a real
/// outage of roughly 300 ms showed up as a **400 ms** two-period gap.
const CAPSTONE_GAP_SLACK_NS: u64 = 300_000_000;

/// The gap bar: one period plus one election plus the slack above — 850 ms at
/// these constants.
///
/// MEASURED on this fixture (six instrumented runs, every link printed): EVERY
/// gap in the chain is exactly one period, 400 ms, the failover gap included.
/// A failover does not skip an occurrence here — the pending occurrence is
/// fired LATE at its own grid point rather than passed over — so the bar needs
/// no rounding onto the grid, and 850 ms is a 2.1x margin over what is
/// observed. It still bites on the regression shape that matters: a node that
/// arms from the wrong clock, or walks a backlog, skips whole occurrences and
/// lands at 1 200 ms or beyond.
const CAPSTONE_MAX_GAP_NS: u64 = CAPSTONE_PERIOD_NS + JOINER_ELECTION_NS.1 + CAPSTONE_GAP_SLACK_NS;

/// Both survivors hold every byte the leader has appended. This is the
/// precondition that makes the promoted joiner's vote credentials
/// (`(last_term, last_durable)`) at least as good as the surviving original's,
/// and therefore the precondition for the joiner winning the election the
/// crash below starts.
fn survivors_caught_up(leader: &Node, joiner: &Node, survivor: &Node) -> bool {
    let a = leader.counters().append.load_acquire();
    joiner.counters().durable.load_acquire() >= a && survivor.counters().durable.load_acquire() >= a
}

/// [`node_config`] with purge ON, a small ring (so a fresh joiner's NAK from 0
/// falls below the floor into the purged region → snapshot session) and a
/// caller-chosen election window.
fn capstone_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    instance_dir: PathBuf,
    seed: u64,
    addr: SocketAddr,
    election_ns: (u64, u64),
) -> NodeConfig {
    NodeConfig {
        buffer_bytes: 1 << 18,
        purge: PurgePolicy::BelowSnapshot { slack_bytes: 0 },
        journal_segment_bytes: CAPSTONE_SEG,
        election_timeout_min_ns: election_ns.0,
        election_timeout_max_ns: election_ns.1,
        ..node_config(
            id,
            members,
            instance_dir,
            seed,
            addr,
            names(&["clock"], None),
        )
    }
}

/// `start_service_with`, but snapshot-capable: the capstone needs the FLOOR to
/// move on its own (a real service publishing real artifacts), because that is
/// what puts the joiner below it.
fn start_snapshot_service(dir: &Path) -> Service<Timed<ClockSm>> {
    let cfg = ServiceConfig::new(dir, APP).snapshot_policy(uc_service::SnapshotPolicy {
        interval_bytes: CAPSTONE_SEG,
    });
    ServiceBuilder::new(cfg, Timed::new(ClockSm::default()))
        .start_with_snapshots()
        .expect("service start")
}

/// `reconfig.rs::admin_request_ok`, duplicated because each integration test
/// file is its own binary: drive one `uc2ctl` mutating command through the cnc
/// admin band, tolerating the transient refusals a live cluster legitimately
/// produces (`status=2` retry, and the NotServing / ChangePending / NotCaughtUp
/// reasons 2/3/10). Any other refusal panics — every op here is legal.
fn admin_request_ok(cnc: &CncPage, op: u32, id: u32, ip: u32, port: u16, secs: u64) -> AdminResp {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let seq = cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0) + 1;
        cnc.write_admin_req(&AdminReq {
            seq,
            nonce: rand::random::<u64>(),
            op,
            id,
            ip,
            port,
        });
        let resp = loop {
            if let Some(resp) = cnc.read_admin_resp(seq) {
                break resp;
            }
            assert!(
                Instant::now() < deadline,
                "admin op {op} response timed out"
            );
            std::thread::sleep(Duration::from_millis(5));
        };
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

/// The capstone's table: one repeating entry on row 0, every
/// [`CAPSTONE_PERIOD_NS`] from `anchor_ns`.
fn every_table(anchor_ns: u64) -> ScheduleTable {
    ScheduleTable {
        entries: vec![ScheduleEntry {
            identity_hash: clock_hash(),
            timer_id: 1,
            rule: ScheduleRule::Every {
                period_ns: CAPSTONE_PERIOD_NS,
                anchor_ns,
            },
        }],
    }
}

/// Plan 3's capstone (spec §5): a node that joined the cluster ENTIRELY below
/// the purge floor — it never saw the table's frame and never can — is
/// promoted to voter, wins the next election, and goes on firing the
/// replicated schedule without a beat missed.
///
/// The chain under test, end to end: the leader's `SNAP_TABLE` → the
/// receiver's withhold-and-publish → the consensus agent's fiat
/// `install_snapshot_table` → the row's armed heap → `fire_due_timers` on a
/// node that has just taken the leadership. Break any link and the joiner
/// leads with an empty table: the log goes silent and the ≥ 5 assertion below
/// times out.
///
/// **Why the joiner wins the election deterministically.** Two things, both
/// necessary. (1) The election windows are skewed ~30x — the joiner's timer
/// expires in 100–150 ms, the surviving original's in 3–5 s — so the joiner is
/// the only node that can campaign in the window. (2) A vote is granted on
/// `(last_term, last_durable) >= (ours, our durable)`, so the joiner must be
/// at least as up to date as the survivor: the test QUIESCES first, waiting
/// until both survivors' durable counters have reached the leader's append,
/// re-checks that on a fresh reading, and kills the leader with NOTHING in
/// between (the `d_kill` read, an IPC round trip, was deliberately hoisted
/// above the quiesce for this). The residual race — a TIMER frame reaching one
/// survivor and not the other inside those few instructions — is far under a
/// millisecond against a 400 ms period, and it fails LOUDLY (no leader) rather
/// than silently.
///
/// **What that loud failure looks like on a loaded box.** The wait-until
/// above can itself stall for ~100 ms under scheduler contention (the same
/// busy-spin-agents-vs-CPU-count pressure every capstone runs under on a
/// noisy dev box) — long enough for the intended leader's own election timer
/// (3–5 s, so normally nowhere near firing) to still not be the risk, but
/// long enough for the JOINER's much tighter 100–150 ms window to lapse and
/// let it campaign early, before the kill. When that happens the test fails
/// on the `assert!(nodes[leader].is_leader(), ...)` a few lines below with
/// "the intended leader lost the leadership before the kill — the election
/// skew let another node campaign while the leader was alive" — a
/// timing-sensitivity flake, not a schedule-table defect; re-run to confirm
/// before suspecting the chain under test.
#[test]
fn a_promoted_below_floor_joiner_keeps_the_schedule_ticking_when_it_leads() {
    let _g = serialize();
    let dir = tempdir();

    // ---- two voters, both slow to campaign, each with a real snapshotting
    // ---- service so the purge floor moves on its own.
    let socks: Vec<UdpSocket> = (0..2)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind"))
        .collect();
    let members: Vec<(NodeId, SocketAddr)> = socks
        .iter()
        .enumerate()
        .map(|(i, s)| (i as NodeId, s.local_addr().unwrap()))
        .collect();
    let mut nodes: Vec<NodeH> = Vec::new();
    for (i, sock) in socks.into_iter().enumerate() {
        let instance_dir = dir.path().join(format!("n{i}"));
        let seed = 0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let cfg = capstone_config(
            i as NodeId,
            members.clone(),
            instance_dir.clone(),
            seed,
            members[i].1,
            ORIGINAL_ELECTION_NS,
        );
        nodes.push(NodeH {
            instance_dir,
            node: Some(Node::start_with_socket(cfg, sock).expect("start")),
        });
    }
    let mut svcs: Vec<Option<Service<Timed<ClockSm>>>> = nodes
        .iter()
        .map(|h| Some(start_snapshot_service(&h.instance_dir)))
        .collect();
    let leader = await_leader_among(&nodes, &[0, 1], 40);
    let survivor = 1 - leader;
    let leader_dir = nodes[leader].instance_dir.clone();
    let leader_cnc = open_cnc(&leader_dir);

    // ---- the table, adopted and COMMITTED before anything is purged.
    let client = Client::connect(&leader_dir, APP).unwrap();
    let t0: u64 = client.submit(&Cmd::Nop).unwrap();
    let anchor = t0 - t0 % CAPSTONE_PERIOD_NS;
    let table = every_table(anchor);
    let rule = ScheduleRule::Every {
        period_ns: CAPSTONE_PERIOD_NS,
        anchor_ns: anchor,
    };
    let table_position = apply_schedule_table(&leader_dir, &leader_cnc, &table);
    // Ruling 3: the ship gate only offers a record at or below the sender's
    // COMMIT counter, so an uncommitted table would correctly ship as none and
    // the joiner would install nothing — a green test proving the wrong thing.
    wait_until("the applied table committed", || {
        nodes[leader]
            .node
            .as_ref()
            .unwrap()
            .counters()
            .commit
            .load_acquire()
            >= table_position
    });
    client.shutdown();

    // ---- churn, so the floor rises well ABOVE the table's frame and the
    // ---- prefix holding it is gone from the journal.
    {
        let c = PipelinedClient::connect(&leader_dir, APP, PipelinedConfig::default()).unwrap();
        let mut inflight = std::collections::VecDeque::new();
        for _ in 0..6000u32 {
            loop {
                match c.try_submit::<_, u64>(&Cmd::Nop) {
                    Ok(t) => {
                        inflight.push_back(t);
                        break;
                    }
                    // The ring is full: retire one outstanding ticket to make
                    // room (or yield, if this producer holds none).
                    Err(_) => match inflight.pop_front() {
                        Some(t) => {
                            t.wait().expect("answered");
                        }
                        None => std::thread::yield_now(),
                    },
                }
            }
            if inflight.len() >= 256 {
                inflight.pop_front().unwrap().wait().expect("answered");
            }
        }
        for t in inflight {
            t.wait().expect("answered");
        }
        c.shutdown();
    }
    wait_until(
        "the leader purged the prefix holding the table frame",
        || nodes[leader].node.as_ref().unwrap().archive_first_base() > table_position,
    );
    let leader_first_base = nodes[leader].node.as_ref().unwrap().archive_first_base();

    // ---- a FRESH node joins as a learner, entirely below that floor.
    const JOINER: NodeId = 50;
    let j_sock = UdpSocket::bind("127.0.0.1:0").expect("bind");
    let j_addr = j_sock.local_addr().unwrap();
    let (j_ip, j_port) = match j_addr {
        SocketAddr::V4(a) => (u32::from(*a.ip()), a.port()),
        SocketAddr::V6(_) => panic!("ipv4 loopback only"),
    };
    let resp = admin_request_ok(
        &leader_cnc,
        1, /* AddLearner */
        JOINER,
        j_ip,
        j_port,
        30,
    );
    assert_eq!(resp.version, 1, "add-learner landed at v1");
    let j_dir = dir.path().join("n50");
    let j_node = Node::start_with_socket(
        capstone_config(
            JOINER,
            members.clone(),
            j_dir.clone(),
            0xDEAD_BEEF,
            j_addr,
            JOINER_ELECTION_NS,
        ),
        j_sock,
    )
    .expect("start the joiner");
    let j_svc = start_snapshot_service(&j_dir);
    let j_cnc = open_cnc(&j_dir);

    let frontier = nodes[leader]
        .node
        .as_ref()
        .unwrap()
        .counters()
        .append
        .load_acquire();
    wait_until("the joiner caught up across the purged prefix", || {
        j_node.counters().durable.load_acquire() >= frontier
            && j_node.counters().commit.load_acquire() >= frontier
    });
    assert!(
        j_node.archive_first_base() >= leader_first_base,
        "the joiner must have adopted the shipped snapshot floor, not replayed from 0"
    );

    // ---- and it holds the leader's table, record for record. The frame is
    // ---- below the floor it just adopted, so `SNAP_TABLE` is the only way
    // ---- these bytes could be here.
    let want = uc_node::read_record(&leader_dir)
        .expect("read the leader's record")
        .expect("the leader adopted a table");
    assert_eq!(want.position, table_position);
    wait_until("the joiner installed the schedule record", || {
        uc_node::read_record(&j_dir).expect("read").is_some()
    });
    let got = uc_node::read_record(&j_dir).unwrap().unwrap();
    assert_eq!(
        got.position, want.position,
        "at the leader's table position"
    );
    assert_eq!(got.time_ns, want.time_ns, "with the leader's frame stamp");
    assert_eq!(
        decode_schedule_table(&got.table).as_ref(),
        Some(&table),
        "and the leader's table"
    );
    assert!(got.prev.is_none(), "a fiat install keeps no history");
    wait_until("the joiner armed the table entry", || {
        j_cnc.service_slot(0).identity.timers_pending() == 1
    });

    // ---- promote it. Poll the leader's own view of its catch-up first, so
    // ---- the promote is a decision and not a retry loop.
    let target = nodes[leader]
        .node
        .as_ref()
        .unwrap()
        .counters()
        .commit
        .load_acquire();
    wait_until("the leader saw the joiner catch up", || {
        (0..CNC_MAX_PEER_SLOTS).any(|i| {
            let raw = leader_cnc.peer_slot(i).id_and_role.load_acquire();
            raw != 0
                && (raw >> 8) as u32 == JOINER
                && leader_cnc.peer_slot(i).reported_durable.load_acquire() >= target
        })
    });
    let resp = admin_request_ok(&leader_cnc, 2 /* PromoteLearner */, JOINER, 0, 0, 30);
    assert_eq!(resp.version, 2, "promote landed at v2");
    wait_until("the promotion converged cluster-wide", || {
        j_node.config_version() >= 2
            && nodes
                .iter()
                .all(|h| h.node.as_ref().unwrap().config_version() >= 2)
    });

    // ---- a LOWER BOUND on the last tick fired before the leadership moves.
    // Taken HERE, before the quiesce, on purpose: reading it is a full
    // `query` IPC round trip to the joiner's service, and anything between
    // the quiesce and the `crash()` below widens the window in which a TIMER
    // frame can reach one survivor and not the other — which is the one race
    // that could hand the election to the wrong node. A `d_kill` that is only
    // a lower bound costs nothing: ticks the old leader appends after it just
    // lengthen the chain checked at the end, all of them steady-state.
    wait_until("the joiner's FSM saw a table tick", || {
        query(&j_svc).0.iter().any(|f| f.id == 1)
    });
    let d_kill = query(&j_svc)
        .0
        .iter()
        .filter(|f| f.id == 1)
        .map(|f| f.deadline_ns)
        .max()
        .expect("the wait above returned on a tick");

    // ---- quiesce (see the doc comment: this is what makes the joiner's vote
    // ---- credentials at least as good as the survivor's), then kill the
    // ---- leader's service and node together. NOTHING may sit between the
    // ---- final check and the kill.
    wait_until("both survivors hold the leader's whole log", || {
        survivors_caught_up(
            nodes[leader].node.as_ref().unwrap(),
            &j_node,
            nodes[survivor].node.as_ref().unwrap(),
        )
    });
    assert!(
        nodes[leader].is_leader(),
        "the intended leader lost the leadership before the kill — the \
         election skew let another node campaign while the leader was alive"
    );
    // Re-check on a fresh reading: the leader has kept appending TIMER frames
    // while the assertion above ran, so the state the first wait returned on
    // is already one tick old. This converges immediately in the steady state
    // and is what actually bounds the kill window.
    wait_until("both survivors still hold the leader's whole log", || {
        survivors_caught_up(
            nodes[leader].node.as_ref().unwrap(),
            &j_node,
            nodes[survivor].node.as_ref().unwrap(),
        )
    });
    svcs[leader].take().unwrap().crash();
    nodes[leader].node.take().unwrap().crash();

    wait_until("the promoted joiner took the leadership", || {
        j_node.is_leader()
    });
    assert!(
        !nodes[survivor].is_leader(),
        "the surviving ORIGINAL voter won instead — the election skew did not hold"
    );

    // Everything the joiner's FSM has fired up to HERE could still be the dead
    // leader's work replayed. Let its apply agent drain to the commit it holds
    // as leader, then take the high-water deadline: every table tick after
    // this one was appended by the joiner itself.
    wait_until("the joiner's service caught up to its own commit", || {
        j_cnc.service_slot(0).applied.load_acquire() >= j_node.counters().commit.load_acquire()
    });
    let d_lead = query(&j_svc)
        .0
        .iter()
        .filter(|f| f.id == 1)
        .map(|f| f.deadline_ns)
        .max()
        .expect("the joiner's FSM holds the ticks it replayed");
    assert!(d_lead >= d_kill);

    wait_until("five table ticks appended by the new leader", || {
        query(&j_svc)
            .0
            .iter()
            .filter(|f| f.id == 1 && f.deadline_ns > d_lead)
            .count()
            >= 5
    });

    let fired = query(&j_svc).0;
    let post: Vec<&Fired> = fired
        .iter()
        .filter(|f| f.id == 1 && f.deadline_ns > d_lead)
        .collect();
    assert!(
        post.len() >= 5,
        "fewer than five ticks after the joiner led: {post:?}"
    );
    // The whole run across the leadership change: every tick from the last one
    // the OLD leader appended (`d_kill`) onwards.
    let chain: Vec<&Fired> = fired
        .iter()
        .filter(|f| f.id == 1 && f.deadline_ns > d_kill)
        .collect();
    assert!(
        chain.iter().all(|f| f.table),
        "every fire here came from the replicated table: {chain:?}"
    );
    // Each deadline is an occurrence of the rule, and each is the NEWEST
    // occurrence its own stamp admits — the one-tick catch-up property, which
    // is what says the joiner is running the RULE rather than walking a
    // backlog it inherited.
    for f in &chain {
        assert_eq!(
            (f.deadline_ns - anchor) % CAPSTONE_PERIOD_NS,
            0,
            "not an occurrence of the rule: {f:?}"
        );
        assert_eq!(
            rule.latest_at_or_before(f.time_ns),
            Some(f.deadline_ns),
            "the tick is not the newest occurrence its own stamp admits: {f:?}"
        );
    }
    // [`CAPSTONE_MAX_GAP_NS`] is the whole budget, and the FAILOVER gap
    // (`d_kill` → the first tick the new leader appended) is the first link of
    // this chain, so it is measured under the same bar as every steady-state
    // gap after it.
    let max_gap = CAPSTONE_MAX_GAP_NS;
    let mut prev = d_kill;
    for f in &chain {
        assert!(
            f.deadline_ns > prev,
            "deadlines must strictly increase: {prev} then {f:?}"
        );
        assert!(
            f.deadline_ns - prev <= max_gap,
            "the schedule skipped {} ns (> one period + one election) at {f:?}",
            f.deadline_ns - prev
        );
        prev = f.deadline_ns;
    }

    j_svc.stop();
    j_node.stop();
    for s in svcs.iter_mut() {
        if let Some(s) = s.take() {
            s.stop();
        }
    }
    for h in &mut nodes {
        if let Some(n) = h.node.take() {
            n.stop();
        }
    }
}
