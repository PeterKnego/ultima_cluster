//! M14a multi-service integration tests: per-id rings, the declared set on
//! the page, attach refusals (Task 6), the lag bound (Task 7), the door and
//! the report ceiling (Task 8). Single node unless stated; every instance dir
//! is on the ext4 target volume, never /tmp.

use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use uc2_client::Client;
use uc2_log::cnc::CncPage;
use uc2_node::{CryptoConfig, FsmLag, Node, NodeConfig, PurgePolicy, ServicesConfig};
use uc2_service::{ServiceBuilder, ServiceConfig, StateMachine};

pub const APP: &str = "m14-services";

static TEST_LOCK: Mutex<()> = Mutex::new(());
pub fn serialize() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

pub fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("uc2-m14-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

pub fn config(dir: &Path, services: ServicesConfig) -> NodeConfig {
    let bind: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        learners: Vec::new(),
        bind,
        instance_dir: dir.to_path_buf(),
        app_id: APP.into(),
        buffer_bytes: 1 << 22, // 4 MiB
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: 1,
        faults: uc2_net::fault::FaultConfig::default(),
        purge: PurgePolicy::Disabled,
        journal_segment_bytes: 64 * 1024,
        crypto: CryptoConfig::Disabled,
        services,
    }
}

pub fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !f() {
        assert!(Instant::now() < deadline, "timeout waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

pub fn open_cnc(dir: &Path) -> std::sync::Arc<CncPage> {
    CncPage::open_file(&dir.join("cnc2.dat"), APP).expect("open cnc")
}

pub fn ids(ids: &[u8], lag: Option<FsmLag>) -> ServicesConfig {
    ServicesConfig::from_ids(ids, lag).unwrap()
}

#[test]
fn node_creates_per_id_rings_dirs_and_publishes_the_declared_set() {
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), ids(&[0, 2], Some(FsmLag::Bounded(64 << 10))))).unwrap();
    wait_until("serving", || node.can_serve());
    for id in [0u8, 2] {
        assert!(dir.path().join(format!("svc_query.{id}.ring")).is_file(), "svc_query.{id}.ring");
        assert!(dir.path().join(format!("egress_service.{id}.broadcast")).is_file(), "egress {id}");
        assert!(dir.path().join("snapshots").join(id.to_string()).is_dir(), "snapshots/{id}");
    }
    assert!(!dir.path().join("svc_query.1.ring").exists(), "undeclared id gets no ring");
    assert!(!dir.path().join("svc_query.ring").exists(), "legacy singular name is not created");
    let cnc = open_cnc(dir.path());
    assert_eq!(cnc.services_declared(), 0b101);
    assert_eq!(cnc.fsm_lag_bytes(), 64 << 10);
    node.stop();
}

#[test]
fn lockstep_publishes_zero_and_none_for_tests_still_rings_fsm_zero() {
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), ids(&[0], Some(FsmLag::Lockstep)))).unwrap();
    wait_until("serving", || node.can_serve());
    assert_eq!(open_cnc(dir.path()).fsm_lag_bytes(), 0, "0 ⇔ lockstep");
    node.stop();

    let dir2 = tempdir();
    let node = Node::start(config(dir2.path(), ServicesConfig::none_for_tests())).unwrap();
    wait_until("serving", || node.can_serve());
    assert!(dir2.path().join("egress_service.0.broadcast").is_file());
    assert_eq!(open_cnc(dir2.path()).services_declared(), 0);
    node.stop();
}

#[test]
fn a_bad_lag_bound_is_a_named_startup_refusal_before_any_file_exists() {
    let _g = serialize();
    let dir = tempdir();
    let cfg = config(dir.path(), ids(&[0], Some(FsmLag::Bounded(2 << 20)))); // == buffer/2
    let err = Node::start(cfg).err().expect("must refuse");
    assert!(err.to_string().contains("services.fsm_lag must be below buffer_bytes / 2"), "{err}");
    assert!(!dir.path().join("cnc2.dat").exists(), "refused before creating the page");
    assert!(!dir.path().join("instance.lock").exists(), "refused before taking the lock");
}

#[derive(Serialize, Deserialize)]
pub enum Cmd { Add(u64) }

#[derive(Default)]
pub struct CountSm { total: u64, last: Option<u64> }
impl StateMachine for CountSm {
    type Command = Cmd;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;
    fn apply(&mut self, position: u64, cmd: Cmd) -> u64 {
        let Cmd::Add(n) = cmd;
        self.total += n;
        self.last = Some(position);
        self.total
    }
    fn query(&self, _q: ()) -> u64 { self.total }
    fn last_applied(&self) -> Option<u64> { self.last }
}

pub fn start_service(dir: &Path, id: u8) -> uc2_service::Service<CountSm> {
    ServiceBuilder::new(ServiceConfig::new(dir, APP).service_id(id), CountSm::default())
        .start()
        .expect("service start")
}

#[test]
fn page_one_service_band_is_the_min_over_declared_ids() {
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), ids(&[0, 1], None))).unwrap();
    wait_until("serving", || node.can_serve());
    let svc0 = start_service(dir.path(), 0);
    let cnc = open_cnc(dir.path());
    let s0 = cnc.service_slot(0);
    wait_until("slot 0 attached", || {
        uc2_log::cnc::unpack_service_status(s0.status.load_acquire()) == (0, true, 1)
    });
    assert_eq!(s0.epoch.load_acquire(), 1);
    assert_eq!(cnc.service().service_epoch.load_acquire(), 0, "page-1 epoch is retired");

    let client = Client::connect(dir.path(), APP).unwrap();
    for _ in 0..20 {
        let _: u64 = client.submit(&Cmd::Add(1)).unwrap();
    }
    let applied0 = s0.applied.load_acquire();
    assert!(applied0 > 0, "FSM 0 applied {applied0}");
    // FSM 1 is declared and absent: every page-1 aggregate is held at 0.
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(cnc.service().service_applied.load_acquire(), 0);
    assert_eq!(cnc.status().service_heartbeat_ns.load_acquire(), 0);
    assert_eq!(cnc.snapshots().service_snapshot_pos.load_acquire(), 0);
    assert!(s0.heartbeat_ns.load_acquire() > 0, "the slot's own heartbeat ticks");

    client.shutdown();
    svc0.stop();
    wait_until("slot 0 detached", || !uc2_log::cnc::unpack_service_status(s0.status.load_acquire()).1);
    assert_eq!(s0.epoch.load_acquire(), 1, "detach does not bump the epoch");
    node.stop();
}

#[test]
fn an_undeclared_id_is_refused_by_name_and_a_second_attach_on_the_same_id_is_refused() {
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), ids(&[0, 1], None))).unwrap();
    wait_until("serving", || node.can_serve());
    let err = ServiceBuilder::new(ServiceConfig::new(dir.path(), APP).service_id(2), CountSm::default())
        .start()
        .err()
        .expect("id 2 is not declared");
    assert!(matches!(err, uc2_service::ServiceError::ServiceNotDeclared { id: 2, declared: 0b11 }), "{err:?}");
    assert!(err.to_string().contains("service id 2 is not declared"), "{err}");

    let svc1 = start_service(dir.path(), 1);
    let err = ServiceBuilder::new(ServiceConfig::new(dir.path(), APP).service_id(1), CountSm::default())
        .start()
        .err()
        .expect("id 1 is held");
    assert!(matches!(err, uc2_service::ServiceError::AlreadyAttached { id: 1 }), "{err:?}");
    svc1.stop();
    // The lock is released with the process's handle: a re-attach succeeds.
    let svc1b = start_service(dir.path(), 1);
    assert_eq!(svc1b.epoch(), 2);
    svc1b.stop();
    node.stop();
}

/// Review fix (fix round 1): `service_id` is a `u8` never range-checked
/// before the declared-set gate's `1u64 << cfg.service_id`. The brief's
/// original `||` order evaluated the shift first, so a `service_id` of 200
/// (well past `CNC_MAX_SERVICES`) panicked with "attempt to shift left with
/// overflow" in a debug/test build instead of returning the named
/// `ServiceNotDeclared` refusal. This test runs in the default (debug,
/// overflow-checked) test profile, so it would have panicked under the old
/// `||` ordering.
#[test]
fn an_out_of_range_service_id_is_a_named_refusal_not_a_shift_overflow_panic() {
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), ids(&[0, 1], None))).unwrap();
    wait_until("serving", || node.can_serve());
    let err = ServiceBuilder::new(ServiceConfig::new(dir.path(), APP).service_id(200), CountSm::default())
        .start()
        .err()
        .expect("id 200 is out of range");
    assert!(matches!(err, uc2_service::ServiceError::ServiceNotDeclared { id: 200, declared: 0b11 }), "{err:?}");
    node.stop();
}

#[test]
fn two_fsms_apply_the_same_log_and_fsm_zero_answers_the_client() {
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), ids(&[0, 1], None))).unwrap();
    wait_until("serving", || node.can_serve());
    let svc0 = start_service(dir.path(), 0);
    let svc1 = start_service(dir.path(), 1);
    let client = Client::connect(dir.path(), APP).unwrap();
    let mut last: u64 = 0;
    for _ in 0..100 {
        last = client.submit(&Cmd::Add(1)).unwrap();
    }
    assert_eq!(last, 100, "FSM 0's answers reach the client in order");
    let cnc = open_cnc(dir.path());
    wait_until("FSM 1 caught up", || {
        cnc.service_slot(1).applied.load_acquire() == cnc.service_slot(0).applied.load_acquire()
    });
    assert_eq!(cnc.service().service_applied.load_acquire(), cnc.service_slot(0).applied.load_acquire());
    assert_eq!(svc0.query(()), 100);
    assert_eq!(svc1.query(()), 100, "same log, same deterministic SM ⇒ same state");
    assert!(dir.path().join("snapshots").join("1").is_dir());
    client.shutdown();
    svc0.stop();
    svc1.stop();
    node.stop();
}

/// FSM 1's stand-in: 1 ms per apply, so FSM 0 would run ~1000 frames ahead
/// per second without the barrier.
#[derive(Default)]
pub struct SlowCountSm(CountSm);
impl StateMachine for SlowCountSm {
    type Command = Cmd;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;
    fn apply(&mut self, position: u64, cmd: Cmd) -> u64 {
        std::thread::sleep(Duration::from_millis(1));
        self.0.apply(position, cmd)
    }
    fn query(&self, q: ()) -> u64 { self.0.query(q) }
    fn last_applied(&self) -> Option<u64> { self.0.last_applied() }
}

/// Drive `n` submits through the pipelined client while a sampler thread
/// records the largest `applied_0 - applied_1` it sees (applied_0 read FIRST,
/// so a racing sample can only under-read the gap). Returns `(max_gap, total)`.
fn drive_and_sample_gap(dir: &Path, n: u64) -> (u64, u64) {
    use uc2_client::{PipelinedClient, PipelinedConfig};
    let cnc = open_cnc(dir);
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler = {
        let cnc = std::sync::Arc::clone(&cnc);
        let stop = std::sync::Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut max_gap = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let a0 = cnc.service_slot(0).applied.load_acquire();
                let a1 = cnc.service_slot(1).applied.load_acquire();
                max_gap = max_gap.max(a0.saturating_sub(a1));
                std::thread::sleep(Duration::from_micros(200));
            }
            max_gap
        })
    };
    // A long deadline: under lockstep every ticket waits behind the slow FSM.
    let client = PipelinedClient::connect(
        dir,
        APP,
        PipelinedConfig { request_timeout: Duration::from_secs(30), ..PipelinedConfig::default() },
    )
    .unwrap();
    let mut tickets = Vec::with_capacity(n as usize);
    for _ in 0..n {
        tickets.push(client.submit::<Cmd, u64>(&Cmd::Add(1)).unwrap());
    }
    let mut total = 0;
    for t in tickets {
        total = t.wait().unwrap();
    }
    wait_until("FSM 1 caught up", || {
        cnc.service_slot(1).applied.load_acquire() == cnc.service_slot(0).applied.load_acquire()
    });
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    client.shutdown();
    (sampler.join().unwrap(), total)
}

#[test]
fn bounded_lag_holds_between_a_fast_and_a_slow_fsm() {
    let _g = serialize();
    let dir = tempdir();
    const BOUND: u64 = 64 << 10;
    let node = Node::start(config(dir.path(), ids(&[0, 1], Some(FsmLag::Bounded(BOUND))))).unwrap();
    wait_until("serving", || node.can_serve());
    let svc0 = start_service(dir.path(), 0);
    let svc1 = ServiceBuilder::new(ServiceConfig::new(dir.path(), APP).service_id(1), SlowCountSm::default())
        .start()
        .unwrap();
    // 3000 frames × 128 B = 384 KiB of log — six times the bound.
    let (max_gap, total) = drive_and_sample_gap(dir.path(), 3000);
    assert_eq!(total, 3000);
    assert!(max_gap <= BOUND, "applied_0 - applied_1 reached {max_gap} > bound {BOUND}");
    assert!(max_gap > BOUND / 2, "vacuity: the fast FSM never approached the bound (max gap {max_gap})");
    let cnc = open_cnc(dir.path());
    assert!(cnc.service_slot(0).lag_waits.load_acquire() > 0, "FSM 0 must have waited at least once");
    assert_eq!(cnc.service_slot(1).lag_waits.load_acquire(), 0, "the slow FSM never waits");
    svc0.stop();
    svc1.stop();
    node.stop();
}

#[test]
fn lockstep_holds_the_fsms_within_one_frame() {
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), ids(&[0, 1], Some(FsmLag::Lockstep)))).unwrap();
    wait_until("serving", || node.can_serve());
    let svc0 = start_service(dir.path(), 0);
    let svc1 = ServiceBuilder::new(ServiceConfig::new(dir.path(), APP).service_id(1), SlowCountSm::default())
        .start()
        .unwrap();
    let (max_gap, total) = drive_and_sample_gap(dir.path(), 500);
    assert_eq!(total, 500);
    // One frame: header 32 + payload (≤ max_payload 256), 32-byte aligned.
    let one_frame = uc_protocol::v2::frame::align_frame_len(32 + 256) as u64;
    assert!(max_gap <= one_frame, "lockstep gap {max_gap} > one frame {one_frame}");
    assert!(max_gap > 0, "vacuity: no gap ever observed");
    svc0.stop();
    svc1.stop();
    node.stop();
}

#[test]
fn the_leader_door_closes_at_the_bound_while_a_declared_fsm_is_absent() {
    use uc2_client::{ClientError, PipelinedClient, PipelinedConfig};
    let _g = serialize();
    let dir = tempdir();
    const BOUND: u64 = 64 << 10;
    let node = Node::start(config(dir.path(), ids(&[0, 1], Some(FsmLag::Bounded(BOUND))))).unwrap();
    wait_until("serving", || node.can_serve());
    let svc0 = start_service(dir.path(), 0);
    // `max_inflight` comfortably above the ~1024 records that fit under the
    // 64 KiB door (so the client's own window isn't what stops the burst —
    // the full door-capped backlog reaches the ring either way) yet well
    // below the loop bound (so the window itself is what the client
    // eventually observes), combined with `try_submit` (fail-fast, no
    // retry): plain `submit` retries `Backpressure` for a 1 s grace, and the
    // poll driver's deadline sweep (`request_timeout`) frees a swept
    // ticket's window slot every sweep — with a sweep interval well under
    // that 1 s grace, `submit` can ALWAYS eventually claim a freed slot, so
    // it never truly observes `BackpressureFull` here (confirmed: with
    // plain `submit` every one of 4000 submits eventually succeeds, just
    // increasingly slowly). `try_submit` reads the window synchronously,
    // with no grace/sweep interaction, so it reliably surfaces the shut
    // door once `max_inflight` outstanding tickets have piled up.
    let client = PipelinedClient::connect(
        dir.path(),
        APP,
        PipelinedConfig {
            request_timeout: Duration::from_millis(500),
            max_inflight: 2000,
            ..PipelinedConfig::default()
        },
    )
    .unwrap();
    // Fire until the door refuses.
    let mut refused = false;
    let mut tickets = Vec::new();
    for _ in 0..4000 {
        match client.try_submit::<Cmd, u64>(&Cmd::Add(1)) {
            Ok(t) => tickets.push(t),
            Err(ClientError::BackpressureFull) => {
                refused = true;
                break;
            }
            Err(e) => panic!("unexpected {e:?}"),
        }
    }
    assert!(refused, "4000 × 128 B = 512 KiB must not all get through a 64 KiB door");
    let cnc = open_cnc(dir.path());
    let one_frame = uc_protocol::v2::frame::align_frame_len(32 + 256) as u64;
    let append = cnc.counters().append.load_acquire();
    assert!(append <= BOUND + one_frame, "append {append} ran past the door ({BOUND} + one frame)");
    let _ = tickets; // drop: whatever timed out, timed out
    // Attaching the missing FSM re-opens the door: both catch up, writes flow.
    let svc1 = start_service(dir.path(), 1);
    wait_until("door reopens", || {
        client.submit::<Cmd, u64>(&Cmd::Add(1)).and_then(|t| t.wait()).is_ok()
    });
    wait_until("both past the bound", || cnc.service_slot(1).applied.load_acquire() > BOUND);
    client.shutdown();
    svc0.stop();
    svc1.stop();
    node.stop();
}

/// Three in-process nodes. The LEADER declares nothing (door inert), the two
/// FOLLOWERS declare {0,1} with a 64 KiB bound and have no FSM attached, so
/// their reports are capped at 64 KiB and commit stalls there although the
/// leader appends freely. Attaching both FSMs on both followers releases it.
#[test]
fn q_a_follower_quorum_with_absent_fsms_stalls_commit_at_the_bound() {
    let _g = serialize();
    let root = tempdir();
    const BOUND: u64 = 64 << 10;
    let socks: Vec<std::net::UdpSocket> =
        (0..3).map(|_| std::net::UdpSocket::bind("127.0.0.1:0").unwrap()).collect();
    let members: Vec<(uc2_consensus::election::NodeId, std::net::SocketAddr)> =
        socks.iter().enumerate().map(|(i, s)| (i as u32, s.local_addr().unwrap())).collect();
    fn node_cfg(dir: &Path, i: usize, members: &[(u32, std::net::SocketAddr)], services: ServicesConfig) -> NodeConfig {
        let mut cfg = config(dir, services);
        cfg.id = i as u32;
        cfg.bind = members[i].1;
        cfg.members = members.to_vec();
        cfg.seed = 1 + i as u64;
        cfg
    }
    // `Node::crash(self)` consumes, so the slots are `Option`s.
    let mut nodes: Vec<Option<Node>> = Vec::new();
    let mut dirs = Vec::new();
    for (i, sock) in socks.into_iter().enumerate() {
        let dir = root.path().join(format!("n{i}"));
        let cfg = node_cfg(&dir, i, &members, ServicesConfig::none_for_tests());
        nodes.push(Some(Node::start_with_socket(cfg, sock).unwrap()));
        dirs.push(dir);
    }
    let serving = |n: &Option<Node>| n.as_ref().is_some_and(|n| n.is_leader() && n.can_serve());
    let mut leader = 0;
    wait_until("single serving leader", || {
        let ls: Vec<usize> = (0..3).filter(|&i| serving(&nodes[i])).collect();
        if ls.len() == 1 { leader = ls[0]; }
        ls.len() == 1
    });
    // Restart the two followers with the declared set (config is per node;
    // crash-then-rebind exactly as lincheck_v2::kill_and_restart_leader).
    for i in (0..3).filter(|&i| i != leader) {
        nodes[i].take().unwrap().crash();
        let sock = std::net::UdpSocket::bind(members[i].1).unwrap();
        let cfg = node_cfg(&dirs[i], i, &members, ids(&[0, 1], Some(FsmLag::Bounded(BOUND))));
        nodes[i] = Some(Node::start_with_socket(cfg, sock).unwrap());
    }
    wait_until("leader serving again", || serving(&nodes[leader]));
    let leader_node = nodes[leader].as_ref().unwrap();
    // 2000 × 64 B payloads ≈ 200 KiB of frames through the leader's own door
    // (256 KiB admission window, no FSM term). A valid `bincode`-encoded
    // `Cmd::Add(1)` padded to 64 B with trailing zeros: `decode_from_slice`
    // only consumes what the type needs and ignores the rest, so this both
    // decodes cleanly once the FSMs attach below AND matches the frame-size
    // arithmetic the assertions below assume.
    let mut payload = bincode::serde::encode_to_vec(Cmd::Add(1), bincode::config::standard()).unwrap();
    payload.resize(64, 0);
    let mut sent = 0;
    while sent < 2000 {
        match leader_node.submit(payload.clone()) {
            Ok(()) => sent += 1,
            Err(_) => std::thread::sleep(Duration::from_millis(1)),
        }
    }
    std::thread::sleep(Duration::from_secs(2));
    let one_frame = uc_protocol::v2::frame::align_frame_len(32 + 256) as u64;
    let c = leader_node.counters();
    let (append, commit) = (c.append.load_acquire(), c.commit.load_acquire());
    assert!(append > 2 * BOUND, "vacuity: the leader appended only {append}");
    assert!(commit <= BOUND + one_frame, "commit {commit} ran past the followers' capped reports ({BOUND})");
    let lcnc = open_cnc(&dirs[leader]);
    for i in 0..8 {
        let s = lcnc.peer_slot(i);
        if s.id_and_role.load_acquire() == 0 { continue; }
        let rd = s.reported_durable.load_acquire();
        assert!(rd <= BOUND + one_frame, "peer slot {i} reported {rd} > cap");
    }
    // Release: attach both FSMs on both followers; each applies to commit,
    // min_applied rises, the ceiling rises, commit follows — to the end.
    let mut services = Vec::new();
    for i in (0..3).filter(|&i| i != leader) {
        services.push(start_service(&dirs[i], 0));
        services.push(start_service(&dirs[i], 1));
    }
    wait_until("commit reaches append", || {
        let c = leader_node.counters();
        c.commit.load_acquire() == c.append.load_acquire()
    });
    for s in services { s.stop(); }
    for n in nodes.into_iter().flatten() { n.stop(); }
}

#[test]
fn submit_to_submit_all_and_query_on_route_by_id_end_to_end() {
    use uc2_client::{Client, ClientError, PipelinedClient, PipelinedConfig};
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), ids(&[0, 1], None))).unwrap();
    wait_until("serving", || node.can_serve());
    let svc0 = start_service(dir.path(), 0);
    let svc1 = start_service(dir.path(), 1);
    let client = PipelinedClient::connect(dir.path(), APP, PipelinedConfig::default()).unwrap();
    assert_eq!(client.declared(), 0b11);
    let t1: u64 = client.submit_to::<Cmd, u64>(1, &Cmd::Add(5)).unwrap().wait().unwrap();
    assert_eq!(t1, 5, "FSM 1 answered its own total");
    let all = client.submit_all::<Cmd, u64>(&Cmd::Add(1)).unwrap().wait().unwrap();
    assert_eq!(all, vec![(0, 6), (1, 6)], "same log, same SM ⇒ identical totals, ordered by id");
    assert_eq!(client.query_snapshot_on::<(), u64>(1, &()).unwrap().wait().unwrap(), 6);
    assert_eq!(client.query_linearizable_on::<(), u64>(1, &()).unwrap().wait().unwrap(), 6);
    assert_eq!(client.query_linearizable_on::<(), u64>(0, &()).unwrap().wait().unwrap(), 6);
    assert!(matches!(client.submit_to::<Cmd, u64>(2, &Cmd::Add(1)), Err(ClientError::ServiceNotDeclared { id: 2, declared: 0b11 })));
    assert!(matches!(client.query_snapshot_on::<(), u64>(7, &()), Err(ClientError::ServiceNotDeclared { id: 7, declared: 0b11 })));
    // `try_submit_to`/`try_submit` put an IDENTICAL wire frame on the
    // ingress ring — `expected` is client-local, never transmitted — so
    // every declared FSM applies and answers every frame regardless of who
    // asked; only the reader's own `expected` mask decides whether an
    // answer completes its request or gets dropped. `PollHalf::poll` drains
    // each declared FSM's ring in ascending id order (ring 0 before ring
    // 1), every cycle. That makes the earlier `submit_to(1, ..)` call above
    // a STRUCTURALLY guaranteed wrong-ring drop: ring 0 (FSM 0's answer) is
    // read first and is NOT the expected ring (`expected = bit(1)`), so it
    // is rejected as `WrongRing` and counted, unconditionally, before ring
    // 1 (FSM 1's real, expected answer) is even reached in the same cycle —
    // `dropped_before` (below) already counts ≥ 1 because of it. The DEFAULT
    // `submit` below is the opposite case: it targets FSM 0
    // (`expected = bit(0)`), and ring 0 is BOTH the expected ring and the
    // one read first, so FSM 0's answer resolves (and frees) the slot
    // before FSM 1's late answer for the same wire seq is drained; that
    // straggler then finds the slot already free and is counted as a
    // `duplicates` (`Resolve::Miss`) drop, not a `wrong_ring` one — the two
    // stats are siblings on the very same `handle_record` match (`engine.rs`),
    // one per outcome of the SAME race, so summing them isolates "was FSM
    // 1's answer to THIS submit dropped and counted" without depending on
    // which specific outcome an FSM-vs-FSM apply/publish race lands on.
    let dropped_before = client.stats().wrong_ring + client.stats().duplicates;
    let d: u64 = client.submit::<Cmd, u64>(&Cmd::Add(1)).unwrap().wait().unwrap();
    assert_eq!(d, 7);
    wait_until("FSM 1's answer to the default submit was dropped", || {
        client.stats().wrong_ring + client.stats().duplicates > dropped_before
    });
    client.shutdown();
    // The blocking shim mirrors all four.
    let c = Client::connect(dir.path(), APP).unwrap();
    assert_eq!(c.declared(), 0b11);
    assert_eq!(c.submit_to::<Cmd, u64>(1, &Cmd::Add(1)).unwrap(), 8);
    assert_eq!(c.submit_all::<Cmd, u64>(&Cmd::Add(1)).unwrap(), vec![(0, 9), (1, 9)]);
    assert_eq!(c.query_snapshot_on::<(), u64>(1, &()).unwrap(), 9);
    assert_eq!(c.query_linearizable_on::<(), u64>(1, &()).unwrap(), 9);
    c.shutdown();
    svc0.stop();
    svc1.stop();
    node.stop();
}

/// A raw query record naming an id the node has no ring for is answered
/// MSG_V2_BAD_SERVICE on the node broadcast (the SDK refuses such ids
/// locally, so this drives the ring directly).
#[test]
fn a_raw_query_for_an_id_without_a_ring_gets_bad_service_from_the_node() {
    use uc_protocol::ring::{BroadcastRing, MpscRing};
    use uc_protocol::v2::ipc::{MSG_V2_BAD_SERVICE, MSG_V2_QUERY, client_from_extra, extra_client, write_query_payload};
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), ids(&[0, 1], None))).unwrap();
    wait_until("serving", || node.can_serve());
    let mut node_egress = BroadcastRing::open(&dir.path().join("egress_node.broadcast")).unwrap().subscribe();
    let (producer, _c) = MpscRing::open(&dir.path().join("query.ring")).unwrap().into_split();
    let mut payload = Vec::new();
    write_query_payload(5, b"q", &mut payload);
    producer.try_write(MSG_V2_QUERY, 0, extra_client(0x77, 1), &payload).unwrap();
    let mut buf = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(Instant::now() < deadline, "no BAD_SERVICE within 10 s");
        match node_egress.try_read(&mut buf) {
            Ok(Some(rec)) if client_from_extra(rec.header_extra) == (0x77, 1) => {
                assert_eq!(rec.msg_type, MSG_V2_BAD_SERVICE);
                assert_eq!(buf, [5]);
                break;
            }
            Ok(_) => std::thread::sleep(Duration::from_millis(1)),
            Err(e) => panic!("egress_node read: {e}"),
        }
    }
    node.stop();
}

/// M14c (spec §9): the `[log]` transition records name each FSM's arrival
/// and departure. Attach is keyed on the slot's epoch (bumped once per
/// incarnation by `uc2_service::attach`); departure is keyed on liveness =
/// ATTACHED bit AND a fresh heartbeat, so an orderly `stop()` is reported on
/// the next duty cycle and a killed service is reported once its heartbeat
/// ages past `services::SERVICE_STALE_NS`.
#[test]
fn attaching_and_stopping_an_fsm_emits_the_transition_records() {
    let _g = serialize();
    let buf = uc2_node::obs::log::capture_for_tests();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), ids(&[0, 1], None))).unwrap();
    wait_until("serving", || node.can_serve());

    let svc1 = start_service(dir.path(), 1);
    wait_until("service_attached record for FSM 1", || {
        let t = String::from_utf8_lossy(&buf.lock().unwrap()).into_owned();
        t.lines().any(|l| {
            l.contains(r#""event":"service_attached""#)
                && l.contains(r#""service":1"#)
                && l.contains(r#""epoch":1"#)
        })
    });

    svc1.stop();
    wait_until("service_detached record for FSM 1", || {
        let t = String::from_utf8_lossy(&buf.lock().unwrap()).into_owned();
        t.lines().any(|l| {
            l.contains(r#""event":"service_detached""#) && l.contains(r#""service":1"#)
        })
    });

    // FSM 0 was never started: it must not be reported as attaching or
    // departing — the events are edges, not a per-cycle status dump.
    let t = String::from_utf8_lossy(&buf.lock().unwrap()).into_owned();
    assert!(
        !t.lines().any(|l| l.contains(r#""event":"service_attached""#)
            && l.contains(r#""service":0"#)),
        "{t}"
    );

    node.stop();
    uc2_node::obs::log::stderr_for_tests();
}
