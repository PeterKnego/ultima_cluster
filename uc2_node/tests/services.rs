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
