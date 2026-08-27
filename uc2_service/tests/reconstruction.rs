// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 9: journal-replay reconstruction (task14 semantics). A fresh or
//! restarted service, attaching to a node whose live log buffer has long since
//! scrolled past position 0, rebuilds its in-memory state by replaying the
//! ARCHIVED log and then rejoins the live buffer — one rejoin mechanism
//! (try-live-then-replay) for both a first attach and a service restart.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig, PurgePolicy};
use uc2_service::{Service, ServiceBuilder, ServiceConfig, SnapshotPolicy, StateMachine};
use uc2_log::cnc::CncPage;
use uc_protocol::ring::{MpscProducer, MpscRing, RingError};
use uc_protocol::v2::ipc::{MSG_V2_SUBMIT, extra_client};

use uc_lincheck::register::{Cmd as RegCmd, RegisterSm};

// A tiny ring so the committed history scrolls out of the live buffer fast,
// forcing the attaching service down the journal-replay reconstruction path.
const RING_BYTES: usize = 64 * 1024;
const CLIENT_ID: u32 = 7;

// ------------------------------------------------------------- the state machine

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Cmd {
    Add(u64),
}

/// A running total; `query(())` returns the current total. `last_applied` tracks
/// the byte position of the last applied frame (the idempotency key).
#[derive(Default)]
struct CountSm {
    total: u64,
    last_applied: Option<u64>,
}

impl StateMachine for CountSm {
    type Command = Cmd;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, position: u64, cmd: Cmd) -> u64 {
        let Cmd::Add(n) = cmd;
        self.total += n;
        self.last_applied = Some(position);
        self.total
    }

    fn query(&self, _q: ()) -> u64 {
        self.total
    }

    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

/// M6 Task 5: `CountSm` is ACCUMULATING (`Add`), so — unlike a last-write-wins
/// register — dropping a purged prefix yields a wrong total. That makes a
/// snapshot-capable `CountSm` the load-bearing silent-gap regression: correct
/// reconstruction below the floor is only possible if the install actually runs.
impl uc2_service::SnapshotStateMachine for CountSm {
    type SnapshotHandle = (u64, Option<u64>);

    fn freeze(&self) -> Result<((u64, Option<u64>), u64), uc2_service::SnapshotError> {
        Ok(((self.total, self.last_applied), self.last_applied.unwrap_or(0)))
    }

    fn stream_snapshot(
        handle: (u64, Option<u64>),
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc2_service::SnapshotError> {
        let bytes = bincode::serde::encode_to_vec(handle, bincode::config::standard())
            .map_err(|e| uc2_service::SnapshotError::Codec(e.to_string()))?;
        std::io::Write::write_all(dst, &bytes)?;
        Ok(())
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc2_service::SnapshotError> {
        let mut buf = Vec::new();
        std::io::Read::read_to_end(src, &mut buf)?;
        let ((total, _last), _): ((u64, Option<u64>), usize) =
            bincode::serde::decode_from_slice(&buf, bincode::config::standard())
                .map_err(|e| uc2_service::SnapshotError::Codec(e.to_string()))?;
        self.total = total;
        self.last_applied = Some(position);
        Ok(position)
    }
}

// --------------------------------------------------------------------- harness

fn start_single_node_with_buffer(dir: &Path, app_id: &str, buffer_bytes: usize) -> Node {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    Node::start(NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        bind,
        instance_dir: dir.to_path_buf(),
        app_id: app_id.into(),
        buffer_bytes,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: FaultConfig::default(),
        purge: uc2_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc2_node::CryptoConfig::Disabled,
        // M14a: node-only harness — these tests drive 2000 submits before any
        // service ever attaches. `ServicesConfig::default()` declares FSM 0,
        // whose `applied` (permanently 0 with no service) would close the
        // admission door at `append - min_applied <= buffer_bytes / 4` = 16
        // KiB, so `append > RING_BYTES` (64 KiB) could never hold.
        // `none_for_tests()` declares nothing: no door term, no report
        // ceiling, page 1 left untouched.
        services: uc2_node::ServicesConfig::none_for_tests(),
    })
    .unwrap()
}

fn cfg(dir: &Path, app_id: &str) -> ServiceConfig {
    ServiceConfig::new(dir, app_id)
}

fn open_cnc(dir: &Path, app_id: &str) -> Arc<CncPage> {
    CncPage::open_file(&dir.join("cnc2.dat"), app_id).unwrap()
}

fn open_ingress(dir: &Path) -> MpscProducer {
    let ring = MpscRing::open(&dir.join("ingress.ring")).unwrap();
    let (prod, _consumer) = ring.into_split();
    prod
}

fn wait_until(mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !f() {
        assert!(Instant::now() < deadline, "condition never held");
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Submit one `Cmd` through the real ingress ring, retrying while the ring is
/// momentarily full (the node drains it into the log continuously). `retries`
/// scales the attempt budget; a genuinely wedged ring fails the test loudly.
fn write_submit_retrying(prod: &MpscProducer, retries: u32, local_seq: u32, cmd: &Cmd) {
    let payload = bincode::serde::encode_to_vec(cmd, bincode::config::standard()).unwrap();
    let extra = extra_client(CLIENT_ID, local_seq);
    let cap = (retries.max(1) as u64) * 20_000;
    for attempt in 0.. {
        match prod.try_write(MSG_V2_SUBMIT, 0, extra, &payload) {
            Ok(()) => return,
            Err(RingError::Full) => {
                assert!(attempt < cap, "ingress ring never drained");
                std::thread::sleep(Duration::from_micros(50));
            }
            Err(e) => panic!("submit failed: {e}"),
        }
    }
}

/// Wait until every submitted command has been committed AND locally durable —
/// the pipeline is drained (append == commit == durable, stable). By that point
/// `append` has crossed the ring capacity many times over, so the live buffer
/// has scrolled and a fresh service must reconstruct from the journal.
///
/// Note: `append == commit == durable, stable` cannot by itself distinguish
/// "every submit landed" from "the admission door closed early" (a stalled
/// FSM term would present the same stable reading) — that is what the
/// `append > RING_BYTES` precondition assertions in the tests below are for.
fn wait_commit_covers_all(node: &Node) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = 0u64;
    let mut stable_since = Instant::now();
    loop {
        let c = node.counters();
        let append = c.append.load_acquire();
        let commit = c.commit.load_acquire();
        let durable = c.durable.load_acquire();
        if append > 4096 && append == commit && append == durable {
            if append == last {
                if stable_since.elapsed() > Duration::from_millis(300) {
                    return;
                }
            } else {
                last = append;
                stable_since = Instant::now();
            }
        } else {
            last = 0;
            stable_since = Instant::now();
        }
        assert!(
            Instant::now() < deadline,
            "commit never covered all submits (append={append} commit={commit} durable={durable})"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn query_total(svc: &Service<CountSm>) -> u64 {
    svc.query(())
}

/// The service catches up to the apply frontier = min(commit, durable). These
/// tests build the node `none_for_tests()`, which never mirrors slots to page
/// 1's `ServiceProgress` — so poll the service's own FSM-0 slot on page 2
/// (`cnc.service_slot(0).applied`) instead of page 1's `service().service_applied`.
fn wait_service_caught_up(cnc: &CncPage) {
    wait_until(|| {
        let target =
            cnc.counters().commit.load_acquire().min(cnc.counters().durable.load_acquire());
        target > 0 && cnc.service_slot(0).applied.load_acquire() >= target
    });
}

// ------------------------------------------------------------------------ tests

#[test]
fn fresh_service_reconstructs_from_journal_after_ring_scrolled() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let node = start_single_node_with_buffer(dir.path(), "rec", RING_BYTES);
    wait_until(|| node.can_serve());

    let prod = open_ingress(dir.path());
    for i in 1..=2_000u32 {
        write_submit_retrying(&prod, 5, i, &Cmd::Add(1)); // >> ring capacity
    }
    wait_commit_covers_all(&node);
    assert!(
        node.counters().append.load_acquire() > RING_BYTES as u64,
        "precondition: the live ring must have scrolled so replay is exercised"
    );

    // FIRST service attaches only now: the ring long since scrolled → the fresh
    // SM at cursor 0 hits Overrun immediately → journal replay reconstruction.
    let svc =
        ServiceBuilder::new(cfg(dir.path(), "rec"), CountSm::default()).start().unwrap();
    let cnc = open_cnc(dir.path(), "rec");
    wait_service_caught_up(&cnc);
    assert_eq!(query_total(&svc), 2_000, "every committed Add applied exactly once");

    svc.stop();
    node.stop();
}

#[test]
fn restarted_service_epoch_bumps_and_state_rebuilds() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let node = start_single_node_with_buffer(dir.path(), "rst", RING_BYTES);
    wait_until(|| node.can_serve());

    let prod = open_ingress(dir.path());
    for i in 1..=2_000u32 {
        write_submit_retrying(&prod, 5, i, &Cmd::Add(1));
    }
    wait_commit_covers_all(&node);
    assert!(
        node.counters().append.load_acquire() > RING_BYTES as u64,
        "precondition: the ring must have scrolled before svc1 attaches"
    );

    // First incarnation: reconstructs from the journal, converges to 2000.
    let svc1 =
        ServiceBuilder::new(cfg(dir.path(), "rst"), CountSm::default()).start().unwrap();
    let cnc = open_cnc(dir.path(), "rst");
    wait_service_caught_up(&cnc);
    assert_eq!(query_total(&svc1), 2_000);
    let old_epoch = svc1.epoch();

    // Hard-crash it (no graceful teardown), then attach a FRESH SM on the same
    // dir. The node (and its cnc page) stay up across the service restart.
    svc1.crash();

    let svc2 =
        ServiceBuilder::new(cfg(dir.path(), "rst"), CountSm::default()).start().unwrap();
    let new_epoch = svc2.epoch();
    assert_eq!(new_epoch, old_epoch + 1, "each attach bumps service_epoch exactly once");

    // The fresh in-memory SM rebuilds the SAME total purely from the journal.
    wait_until(|| query_total(&svc2) == 2_000);
    assert_eq!(query_total(&svc2), 2_000, "in-memory state fully reconstructed from the journal");

    svc2.stop();
    node.stop();
}

// ================================ M6 Task 5 ================================
// Below-the-floor reconstruction: when the journal has been PURGED below what a
// fresh/restarted service needs, replay alone leaves a hole. A snapshot-capable
// SM installs a covering snapshot then tail-replays; an incapable one fail-stops
// with the contract named (the silent-gap bug class, shut).

/// Tiny journal segments + tiny log ring so a few hundred KiB of writes rolls
/// many segments (purge is observable) AND the live buffer scrolls (a fresh
/// service is forced onto the reconstruction path).
const PURGE_SEG: u64 = 64 * 1024;
const PURGE_BUF: usize = 64 * 1024;

fn start_purge_node(dir: &Path, app_id: &str) -> Node {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    Node::start(NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        bind,
        instance_dir: dir.to_path_buf(),
        app_id: app_id.into(),
        buffer_bytes: PURGE_BUF,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: FaultConfig::default(),
        purge: PurgePolicy::BelowSnapshot { slack_bytes: 0 },
        learners: Vec::new(),
        journal_segment_bytes: PURGE_SEG,
        crypto: uc2_node::CryptoConfig::Disabled,
        services: uc2_node::ServicesConfig::default(),
    })
    .unwrap()
}

/// Submit one `RegisterSm` `Write(val)` through the raw ingress ring (no service
/// response awaited — works whether or not a service is attached).
fn write_reg(prod: &MpscProducer, local_seq: u32, val: u64) {
    let payload = bincode::serde::encode_to_vec(RegCmd::Write(val), bincode::config::standard())
        .unwrap();
    let extra = extra_client(CLIENT_ID, local_seq);
    for attempt in 0.. {
        match prod.try_write(MSG_V2_SUBMIT, 0, extra, &payload) {
            Ok(()) => return,
            Err(RingError::Full) => {
                assert!(attempt < 200_000, "ingress ring never drained");
                std::thread::sleep(Duration::from_micros(50));
            }
            Err(e) => panic!("submit failed: {e}"),
        }
    }
}

/// Bring up a purge-enabled node, run a snapshotting `RegisterSm` that writes
/// `1..=n`, wait until the journal is ACTUALLY purged below a built snapshot,
/// then crash the service. Leaves the node up with: journal purged
/// (`archive_first_base > 0`), a covering snapshot on disk, all `n` writes
/// committed + durable. Returns the ingress producer so the caller can drive
/// further node-only commits.
fn purged_node_after_snapshotting_service(dir: &Path, app: &str, n: u32) -> (Node, MpscProducer) {
    let node = start_purge_node(dir, app);
    wait_until(|| node.can_serve());

    let svc1 = ServiceBuilder::new(
        ServiceConfig::new(dir, app).snapshot_policy(SnapshotPolicy { interval_bytes: 4 * 1024 }),
        RegisterSm::default(),
    )
    .start_with_snapshots()
    .unwrap();

    let prod = open_ingress(dir);
    for i in 1..=n {
        write_reg(&prod, i, i as u64);
    }
    wait_commit_covers_all(&node);
    // A snapshot was published AND the node purged below it.
    let cnc = open_cnc(dir, app);
    wait_until(|| cnc.snapshots().service_snapshot_pos.load_acquire() > 0);
    wait_until(|| node.archive_first_base() > 0);

    svc1.crash();
    (node, prod)
}

fn query_reg(svc: &Service<RegisterSm>) -> Option<u64> {
    svc.query(())
}

#[test]
fn fresh_service_below_purge_floor_installs_snapshot_then_tail_replays() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let app = "rec_snap";
    let n = 4_000u32;
    let (node, prod) = purged_node_after_snapshotting_service(dir.path(), app, n);

    // Node-only commits AFTER the service died: these land above the last
    // snapshot, so reconstruction must be snapshot-install + a real tail replay.
    let final_val = n + 200;
    for i in (n + 1)..=final_val {
        write_reg(&prod, i, i as u64);
    }
    wait_commit_covers_all(&node);

    // Service #2: a FRESH snapshot-capable RegisterSm. Its cursor 0 is below the
    // purge floor → the gap guard installs the covering snapshot, then tail
    // replay carries it to the live frontier — state == snapshot prefix + tail,
    // exactly once.
    let svc2 = ServiceBuilder::new(
        ServiceConfig::new(dir.path(), app).snapshot_policy(SnapshotPolicy { interval_bytes: 0 }),
        RegisterSm::default(),
    )
    .start_with_snapshots()
    .unwrap();
    let cnc = open_cnc(dir.path(), app);
    wait_service_caught_up(&cnc);
    assert_eq!(
        query_reg(&svc2),
        Some(final_val as u64),
        "state == snapshot prefix + journal tail (install + tail replay)"
    );

    svc2.stop();
    node.stop();
}

/// A snapshot capture buffer for the fail-stop test: the apply thread's
/// `SnapshotRequired` panic is recorded by a scoped panic hook (the panic
/// unwinds a background thread, so it never fails the test thread directly).
static PANIC_LOG: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

#[test]
fn gap_without_snapshot_capability_fails_stop_with_named_contract() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let app = "rec_nosnap";
    let (node, _prod) = purged_node_after_snapshotting_service(dir.path(), app, 4_000);

    // Record any panic message globally for the duration of this test. Success
    // paths never panic, so cross-talk from sibling tests is a non-issue; we only
    // assert on the SnapshotRequired substring.
    PANIC_LOG.lock().unwrap().clear();
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|info| {
        PANIC_LOG.lock().unwrap().push(info.to_string());
    }));

    // Service #2 is a CountSm — NO SnapshotStateMachine capability, so `.start()`
    // leaves `snapshot_restore = None`. Attaching below the purge floor cannot
    // install a snapshot: the apply agent must fail-stop with the contract named,
    // never silently replay a partial prefix from `first_base` onto a phantom
    // cursor.
    let svc2 = ServiceBuilder::new(cfg(dir.path(), app), CountSm::default()).start().unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    let fired = loop {
        if PANIC_LOG.lock().unwrap().iter().any(|m| m.contains("SnapshotRequired")) {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    std::panic::set_hook(prev);
    assert!(fired, "the apply agent must fail-stop with SnapshotRequired within the deadline");

    // The apply thread is dead; `crash()` joins via Drop (swallowing the panic),
    // so teardown does not re-raise it.
    svc2.crash();
    node.stop();
}

/// The load-bearing silent-gap pin: an ACCUMULATING snapshot-capable SM below
/// the purge floor must reconstruct the EXACT total. Without the gap guard +
/// install, replay would silently start at `first_base`, drop the purged
/// prefix's contributions, and converge to a total short by exactly that
/// prefix's sum — "succeeding" with wrong state. The install closes it.
#[test]
fn snapshotting_count_sm_below_floor_recovers_exact_total() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let app = "rec_count_snap";
    let n = 4_000u32;

    let node = start_purge_node(dir.path(), app);
    wait_until(|| node.can_serve());
    let svc1 = ServiceBuilder::new(
        ServiceConfig::new(dir.path(), app).snapshot_policy(SnapshotPolicy { interval_bytes: 4 * 1024 }),
        CountSm::default(),
    )
    .start_with_snapshots()
    .unwrap();
    let prod = open_ingress(dir.path());
    for i in 1..=n {
        write_submit_retrying(&prod, 5, i, &Cmd::Add(1));
    }
    wait_commit_covers_all(&node);
    let cnc = open_cnc(dir.path(), app);
    wait_until(|| cnc.snapshots().service_snapshot_pos.load_acquire() > 0);
    wait_until(|| node.archive_first_base() > 0);
    svc1.crash();

    // Node-only commits above the last snapshot.
    let m = 200u32;
    for i in (n + 1)..=(n + m) {
        write_submit_retrying(&prod, 5, i, &Cmd::Add(1));
    }
    wait_commit_covers_all(&node);

    // Fresh snapshot-capable CountSm: install the covering snapshot (its prefix
    // total), then tail-replay the rest → the exact grand total, no prefix lost.
    let svc2 = ServiceBuilder::new(
        ServiceConfig::new(dir.path(), app).snapshot_policy(SnapshotPolicy { interval_bytes: 0 }),
        CountSm::default(),
    )
    .start_with_snapshots()
    .unwrap();
    wait_service_caught_up(&cnc);
    assert_eq!(
        query_total(&svc2),
        (n + m) as u64,
        "accumulated total reconstructed EXACTLY — no purged-prefix contributions lost"
    );

    svc2.stop();
    node.stop();
}
