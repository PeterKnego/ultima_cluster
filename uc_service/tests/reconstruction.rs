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

use uc_log::cnc::CncPage;
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig, PurgePolicy};
use uc_protocol::ring::{MpscProducer, MpscRing, RingError};
use uc_protocol::v2::ipc::{MSG_V2_SUBMIT, extra_client};
use uc_service::{ApplyCtx, Service, ServiceBuilder, ServiceConfig, StateMachine};

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
    const NAME: &'static str = "count";

    type Command = Cmd;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Cmd) -> u64 {
        let Cmd::Add(n) = cmd;
        self.total += n;
        self.last_applied = Some(ctx.position);
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
impl uc_service::SnapshotStateMachine for CountSm {
    type SnapshotHandle = (u64, Option<u64>);

    fn freeze(&self) -> Result<((u64, Option<u64>), u64), uc_service::SnapshotError> {
        Ok((
            (self.total, self.last_applied),
            self.last_applied.unwrap_or(0),
        ))
    }

    fn stream_snapshot(
        handle: (u64, Option<u64>),
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc_service::SnapshotError> {
        let bytes = bincode::serde::encode_to_vec(handle, bincode::config::standard())
            .map_err(|e| uc_service::SnapshotError::Codec(e.to_string()))?;
        std::io::Write::write_all(dst, &bytes)?;
        Ok(())
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc_service::SnapshotError> {
        let mut buf = Vec::new();
        std::io::Read::read_to_end(src, &mut buf)?;
        let ((total, last), _): ((u64, Option<u64>), usize) =
            bincode::serde::decode_from_slice(&buf, bincode::config::standard())
                .map_err(|e| uc_service::SnapshotError::Codec(e.to_string()))?;
        self.total = total;
        // Spec §5.2: the tag is the instant P, an EXCLUSIVE frontier — restore
        // the cursor the artifact recorded, or the framework's
        // `pos > last_applied` guard swallows the frame that starts at P.
        self.last_applied = last;
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
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: FaultConfig::default(),
        purge: uc_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        // M14a: node-only harness — these tests drive 2000 submits before any
        // service ever attaches. Declaring FSM 0 (`ServicesConfig::single`)
        // would set its `applied` (permanently 0 with no service), closing the
        // admission door at `append - min_applied <= buffer_bytes / 4` = 16
        // KiB, so `append > RING_BYTES` (64 KiB) could never hold.
        // `none_for_tests()` declares nothing: no door term, no report
        // ceiling, page 1 left untouched.
        services: uc_node::ServicesConfig::none_for_tests(),
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

/// `uc2ctl snapshot`, in process (coordinated-snapshot spec §5.5): command an
/// instant and return its position **P**, polling through the `retry` window a
/// leader legitimately answers while it has the role but not yet an appender.
/// Duplicated per test binary, like `admin_request_ok` elsewhere.
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
        let target = cnc
            .counters()
            .commit
            .load_acquire()
            .min(cnc.counters().durable.load_acquire());
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
    let svc = ServiceBuilder::new(cfg(dir.path(), "rec"), CountSm::default())
        .start()
        .unwrap();
    let cnc = open_cnc(dir.path(), "rec");
    wait_service_caught_up(&cnc);
    assert_eq!(
        query_total(&svc),
        2_000,
        "every committed Add applied exactly once"
    );

    svc.stop();
    node.stop();
}

// --- Ruling P10's `last_applied` guard: a stale instant is history ----------
//
// These two statics are touched by ONE test
// (`a_replayed_instant_at_or_below_the_applied_frontier_is_not_frozen_at`)
// and by nothing else in this binary, so the file's default test parallelism
// cannot race them.

/// Nanoseconds `SlowCountSm::apply` sleeps — the lever that makes the live
/// ring lap a running row on purpose, so an OVERRUN replay is a certainty and
/// not a scheduling coincidence.
static SLOW_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// While false, `freeze()` fails — which is how the test makes a row DECLINE
/// a live instant (spec §10) and then apply on past it, the state the guard
/// under test exists for.
static FREEZE_OK: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

#[derive(Default)]
struct SlowCountSm {
    total: u64,
    last_applied: Option<u64>,
}

impl StateMachine for SlowCountSm {
    const NAME: &'static str = "slowcount";
    type Command = Cmd;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Cmd) -> u64 {
        let ns = SLOW_NS.load(std::sync::atomic::Ordering::Relaxed);
        if ns > 0 {
            std::thread::sleep(Duration::from_nanos(ns));
        }
        let Cmd::Add(n) = cmd;
        self.total += n;
        self.last_applied = Some(ctx.position);
        self.total
    }

    fn query(&self, _q: ()) -> u64 {
        self.total
    }

    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

impl uc_service::SnapshotStateMachine for SlowCountSm {
    type SnapshotHandle = (u64, Option<u64>);

    fn freeze(&self) -> Result<((u64, Option<u64>), u64), uc_service::SnapshotError> {
        if !FREEZE_OK.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(uc_service::SnapshotError::Codec(
                "declined by the test".into(),
            ));
        }
        Ok((
            (self.total, self.last_applied),
            self.last_applied.unwrap_or(0),
        ))
    }

    fn stream_snapshot(
        handle: (u64, Option<u64>),
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc_service::SnapshotError> {
        let bytes = bincode::serde::encode_to_vec(handle, bincode::config::standard())
            .map_err(|e| uc_service::SnapshotError::Codec(e.to_string()))?;
        std::io::Write::write_all(dst, &bytes)?;
        Ok(())
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc_service::SnapshotError> {
        let mut buf = Vec::new();
        std::io::Read::read_to_end(src, &mut buf)?;
        let ((total, last), _): ((u64, Option<u64>), usize) =
            bincode::serde::decode_from_slice(&buf, bincode::config::standard())
                .map_err(|e| uc_service::SnapshotError::Codec(e.to_string()))?;
        self.total = total;
        self.last_applied = last;
        Ok(position)
    }
}

fn snapshot_files(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir.join("snapshots").join("0"))
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.ends_with(".ultsnap"))
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

/// Ruling P10 + the `last_applied` guard (fix round 3): a `SNAPSHOT` frame at
/// or below the row's applied frontier is HISTORY, and a replayed span must
/// never freeze at one.
///
/// **The determinism bug this pins.** P10 shipped without the
/// `Some(pos) > last_applied()` bound its MESSAGE and TIMER siblings carry,
/// and pass 1 filtered only on the cnc `snapshot_pos`. `scan_from` always
/// yields the COVERING segment, i.e. frames below `start_pos`. So: an instant
/// P is declined live (a freeze failure leaves `snapshot_pos` below P), the
/// row applies on past P, and a later overrun replays a span whose only
/// actionable instant is that stale P — the row would freeze with state ABOVE
/// P and tag the artifact P. The envelope check passes (the tag IS P), so
/// nothing catches it: a live replica's artifact at P holds different bytes,
/// and a joiner installing this one double-applies `(P, last_applied]`.
///
/// Phase B is the red one; phase C is its anti-vacuity (the same row, the same
/// replay path, freezes correctly at an instant that IS above the frontier).
#[test]
fn a_replayed_instant_at_or_below_the_applied_frontier_is_not_frozen_at() {
    use std::sync::atomic::Ordering as O;
    use uc_service::SnapshotStateMachine;

    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let node = start_single_node_with_buffer(dir.path(), "recp10g", RING_BYTES);
    wait_until(|| node.can_serve());
    let prod = open_ingress(dir.path());
    let cnc = open_cnc(dir.path(), "recp10g");

    SLOW_NS.store(0, O::Relaxed);
    FREEZE_OK.store(false, O::Relaxed); // every freeze DECLINES
    let svc = ServiceBuilder::new(cfg(dir.path(), "recp10g"), SlowCountSm::default())
        .start_with_snapshots()
        .unwrap();

    // ---- phase A: an instant the row DECLINES, then applies past.
    let mut submitted = 0u32;
    for _ in 0..200 {
        submitted += 1;
        write_submit_retrying(&prod, 5, submitted, &Cmd::Add(1));
    }
    wait_commit_covers_all(&node);
    let p1 = command_instant(&node);
    submitted += 1;
    write_submit_retrying(&prod, 5, submitted, &Cmd::Add(1));
    for _ in 0..200 {
        submitted += 1;
        write_submit_retrying(&prod, 5, submitted, &Cmd::Add(1));
    }
    wait_commit_covers_all(&node);
    wait_until(|| cnc.service_slot(0).applied.load_acquire() > p1);
    assert_eq!(
        cnc.service_slot(0).snapshot_pos.load_acquire(),
        0,
        "phase A: the freeze failed, so the row holds no artifact — and its \
         applied frontier is now ABOVE p1, which is the state under test"
    );
    assert!(snapshot_files(dir.path()).is_empty());

    // ---- phase B: make the ring lap the row, with NO new instant in the
    // ---- span. Its only actionable-looking instant is the stale p1.
    FREEZE_OK.store(true, O::Relaxed); // freezes would now SUCCEED...
    SLOW_NS.store(200_000, O::Relaxed); // ...and the row falls far behind
    for _ in 0..3_000 {
        submitted += 1;
        write_submit_retrying(&prod, 5, submitted, &Cmd::Add(1));
    }
    wait_commit_covers_all(&node);
    // The precondition is not "the log is big" but "the row is more than a
    // ring behind", which is exactly what makes its next batch `Overrun`.
    let behind = node.counters().append.load_acquire() - cnc.service_slot(0).applied.load_acquire();
    assert!(
        behind > RING_BYTES as u64,
        "precondition: the slow row must be more than a ring behind so its \
         next batch OVERRUNS ({behind} B behind a {RING_BYTES} B ring)"
    );
    // Let the replay run and the row catch up.
    SLOW_NS.store(0, O::Relaxed);
    wait_service_caught_up(&cnc);
    assert_eq!(
        cnc.service_slot(0).snapshot_pos.load_acquire(),
        0,
        "a replayed span must NOT freeze at an instant the row already passed"
    );
    assert!(
        snapshot_files(dir.path()).is_empty(),
        "no artifact at all: {:?}",
        snapshot_files(dir.path())
    );

    // ---- phase C (anti-vacuity): the same row, an instant that IS above the
    // ---- applied frontier — one artifact, at it, holding the state strictly
    // ---- below it.
    //
    // Final wave M6: this said "the same replay path", which it does not
    // assert. Nothing here forces an overrun, so on a healthy box the row
    // almost certainly takes the LIVE freeze arm (`apply.rs`'s
    // `on_snapshot_frame`), not the replayed one. That is fine — the claim
    // being pinned is "an instant above the frontier IS acted on, exactly
    // once, at P", which both arms must satisfy and which makes phase B's
    // "no artifact at all" non-vacuous. The replay arm's own coverage is the
    // P10 tests (`SlowCountSm` and the deterministic overrun repros), which
    // do force the overrun.
    SLOW_NS.store(200_000, O::Relaxed);
    for _ in 0..600 {
        submitted += 1;
        write_submit_retrying(&prod, 5, submitted, &Cmd::Add(1));
    }
    wait_commit_covers_all(&node);
    let below_p2 = submitted as u64;
    let p2 = command_instant(&node);
    assert!(p2 > p1);
    for _ in 0..600 {
        submitted += 1;
        write_submit_retrying(&prod, 5, submitted, &Cmd::Add(1));
    }
    wait_commit_covers_all(&node);
    SLOW_NS.store(0, O::Relaxed);
    wait_until(|| cnc.service_slot(0).snapshot_pos.load_acquire() == p2);
    assert_eq!(
        snapshot_files(dir.path()),
        vec![format!("snap-{p2}.ultsnap")],
        "exactly one artifact, at the instant above the frontier — the stale \
         p1 was never frozen at"
    );

    let store = uc_service::snapshots::SnapshotStore::open(dir.path(), 0).unwrap();
    let (pos, path) = store.newest(u64::MAX).unwrap().expect("an artifact at p2");
    assert_eq!(pos, p2);
    let mut f = std::fs::File::open(&path).unwrap();
    uc_service::snapshots::verify_snapshot_envelope(&mut f, p2).expect("envelope names p2");
    let mut restored = SlowCountSm::default();
    restored.install_snapshot(p2, &mut f).unwrap();
    assert_eq!(
        restored.total, below_p2,
        "P6: the artifact is the state strictly BELOW p2 — the 600 adds above \
         it must not have leaked in"
    );

    svc.stop();
    node.stop();
}

/// Ruling P10, the CATCH-UP path: a span replayed from the journal freezes at
/// its last `SNAPSHOT` frame.
///
/// Before P10 `replay_into` ignored every instant, so a row whose ring lapped
/// it silently skipped the instant the leader was waiting on — the set could
/// never complete, at any timeout. Here the instant is commanded with NO
/// service attached at all, so the live apply loop cannot possibly be what
/// freezes: the artifact exists only if the journal walk produced it.
///
/// It also pins P6: the artifact is the state STRICTLY BELOW P. The 1 000
/// `Add(1)`s below the instant must be in it and the 1 000 above must not,
/// which is what makes one instant's artifacts position-aligned across rows.
#[test]
fn a_replayed_span_freezes_at_its_last_snapshot_frame_below_it_only() {
    use uc_service::SnapshotStateMachine;

    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let node = start_single_node_with_buffer(dir.path(), "recp10", RING_BYTES);
    wait_until(|| node.can_serve());

    let prod = open_ingress(dir.path());
    for i in 1..=1_000u32 {
        write_submit_retrying(&prod, 5, i, &Cmd::Add(1));
    }
    wait_commit_covers_all(&node);
    // The instant, with nothing attached. `ServicesConfig::none_for_tests`
    // declares no row, so the capability gate (spec §5.5) is vacuous here —
    // which is exactly the fixture this needs.
    let p = command_instant(&node);
    for i in 1_001..=2_000u32 {
        write_submit_retrying(&prod, 5, i, &Cmd::Add(1));
    }
    wait_commit_covers_all(&node);
    assert!(
        node.counters().append.load_acquire() > RING_BYTES as u64,
        "precondition: the ring must have scrolled so the instant is inside a \
         REPLAYED span and not a live one"
    );

    let svc = ServiceBuilder::new(cfg(dir.path(), "recp10"), CountSm::default())
        .start_with_snapshots()
        .unwrap();
    let cnc = open_cnc(dir.path(), "recp10");
    wait_service_caught_up(&cnc);
    wait_until(|| cnc.service_slot(0).snapshot_pos.load_acquire() == p);
    assert_eq!(query_total(&svc), 2_000, "the whole span still applied");

    let store = uc_service::snapshots::SnapshotStore::open(dir.path(), 0).unwrap();
    let (pos, path) = store.newest(u64::MAX).unwrap().expect("an artifact at P");
    assert_eq!(pos, p, "tagged with the instant, not the SM's own cursor");
    let mut f = std::fs::File::open(&path).unwrap();
    uc_service::snapshots::verify_snapshot_envelope(&mut f, p).expect("envelope names P");
    let mut restored = CountSm::default();
    restored.install_snapshot(p, &mut f).unwrap();
    assert_eq!(
        restored.total, 1_000,
        "P6: the artifact is the log STRICTLY BELOW P — the 1 000 adds above \
         the instant must not have leaked into it"
    );

    svc.stop();
    node.stop();
}

/// Ruling P10, the RECONSTRUCTION path: a restart whose journal tail carries a
/// `SNAPSHOT` frame above the artifact it installs completes that instant.
///
/// `replay_into` is the single entry point for both — the live loop's
/// `Batch::Overrun` arm and a fresh/restarted service's first cycle both call
/// it — so this is the same code as the test above, reached the other way:
/// here the SM starts from an installed artifact at P1 rather than from zero,
/// and the tail above it holds P2.
#[test]
fn a_restart_completes_an_instant_sitting_in_its_journal_tail() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let node = start_single_node_with_buffer(dir.path(), "recp10r", RING_BYTES);
    wait_until(|| node.can_serve());
    let prod = open_ingress(dir.path());

    // First incarnation: catches up, then takes a LIVE instant at P1.
    for i in 1..=500u32 {
        write_submit_retrying(&prod, 5, i, &Cmd::Add(1));
    }
    wait_commit_covers_all(&node);
    let svc1 = ServiceBuilder::new(cfg(dir.path(), "recp10r"), CountSm::default())
        .start_with_snapshots()
        .unwrap();
    let cnc = open_cnc(dir.path(), "recp10r");
    wait_service_caught_up(&cnc);
    let p1 = command_instant(&node);
    wait_until(|| cnc.service_slot(0).snapshot_pos.load_acquire() == p1);
    svc1.crash();

    // With the row DOWN: more traffic, a second instant at P2, more traffic —
    // and enough of it that the ring has scrolled, so the restart genuinely
    // replays rather than reading the tail live.
    for i in 501..=1_500u32 {
        write_submit_retrying(&prod, 5, i, &Cmd::Add(1));
    }
    wait_commit_covers_all(&node);
    let p2 = command_instant(&node);
    assert!(p2 > p1);
    for i in 1_501..=2_500u32 {
        write_submit_retrying(&prod, 5, i, &Cmd::Add(1));
    }
    wait_commit_covers_all(&node);

    // The restart installs the artifact at P1 (or replays to it) and then
    // walks a tail containing P2: the builder must receive P2.
    let svc2 = ServiceBuilder::new(cfg(dir.path(), "recp10r"), CountSm::default())
        .start_with_snapshots()
        .unwrap();
    wait_service_caught_up(&cnc);
    wait_until(|| cnc.service_slot(0).snapshot_pos.load_acquire() == p2);
    assert_eq!(query_total(&svc2), 2_500, "the whole tail applied too");

    let store = uc_service::snapshots::SnapshotStore::open(dir.path(), 0).unwrap();
    assert_eq!(
        store.newest(u64::MAX).unwrap().expect("an artifact").0,
        p2,
        "the instant in the tail completed on the restarted row"
    );

    svc2.stop();
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
    let svc1 = ServiceBuilder::new(cfg(dir.path(), "rst"), CountSm::default())
        .start()
        .unwrap();
    let cnc = open_cnc(dir.path(), "rst");
    wait_service_caught_up(&cnc);
    assert_eq!(query_total(&svc1), 2_000);
    let old_epoch = svc1.epoch();

    // Hard-crash it (no graceful teardown), then attach a FRESH SM on the same
    // dir. The node (and its cnc page) stay up across the service restart.
    svc1.crash();

    let svc2 = ServiceBuilder::new(cfg(dir.path(), "rst"), CountSm::default())
        .start()
        .unwrap();
    let new_epoch = svc2.epoch();
    assert_eq!(
        new_epoch,
        old_epoch + 1,
        "each attach bumps service_epoch exactly once"
    );

    // The fresh in-memory SM rebuilds the SAME total purely from the journal.
    wait_until(|| query_total(&svc2) == 2_000);
    assert_eq!(
        query_total(&svc2),
        2_000,
        "in-memory state fully reconstructed from the journal"
    );

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

fn start_purge_node(dir: &Path, app_id: &str, sm_name: &str) -> Node {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    Node::start(NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        bind,
        instance_dir: dir.to_path_buf(),
        app_id: app_id.into(),
        buffer_bytes: PURGE_BUF,
        max_payload: 256,
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: FaultConfig::default(),
        purge: PurgePolicy::BelowSnapshot { slack_bytes: 0 },
        learners: Vec::new(),
        journal_segment_bytes: PURGE_SEG,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::single(sm_name),
    })
    .unwrap()
}

/// Submit one `RegisterSm` `Write(val)` through the raw ingress ring (no service
/// response awaited — works whether or not a service is attached).
fn write_reg(prod: &MpscProducer, local_seq: u32, val: u64) {
    let payload =
        bincode::serde::encode_to_vec(RegCmd::Write(val), bincode::config::standard()).unwrap();
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
    let node = start_purge_node(dir, app, RegisterSm::NAME);
    wait_until(|| node.can_serve());

    let svc1 = ServiceBuilder::new(ServiceConfig::new(dir, app), RegisterSm::default())
        .start_with_snapshots()
        .unwrap();

    let prod = open_ingress(dir);
    for i in 1..=n {
        write_reg(&prod, i, i as u64);
    }
    wait_commit_covers_all(&node);
    // Coordinated-snapshot spec §5.2: the instant comes from the LOG. Command
    // one once every write is committed, so P sits at the live frontier and
    // the purge below it drops essentially the whole prefix — which is what
    // puts the next incarnation below the floor.
    command_instant(&node);
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
    let svc2 = ServiceBuilder::new(ServiceConfig::new(dir.path(), app), RegisterSm::default())
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

    // Service #2 attaches via plain `.start()` (not `.start_with_snapshots()`),
    // which never wires `snapshot_restore` regardless of the SM's capability.
    // Attaching below the purge floor cannot install a snapshot: the apply
    // agent must fail-stop with the contract named, never silently replay a
    // partial prefix from `first_base` onto a phantom cursor. Must be a
    // `RegisterSm` (not `CountSm`) — FSM identity: this node declares
    // `RegisterSm::NAME` ("register") at row 0, and attach now finds the row
    // by name.
    let svc2 = ServiceBuilder::new(cfg(dir.path(), app), RegisterSm::default())
        .start()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    let fired = loop {
        if PANIC_LOG
            .lock()
            .unwrap()
            .iter()
            .any(|m| m.contains("SnapshotRequired"))
        {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    std::panic::set_hook(prev);
    assert!(
        fired,
        "the apply agent must fail-stop with SnapshotRequired within the deadline"
    );

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

    let node = start_purge_node(dir.path(), app, CountSm::NAME);
    wait_until(|| node.can_serve());
    let svc1 = ServiceBuilder::new(ServiceConfig::new(dir.path(), app), CountSm::default())
        .start_with_snapshots()
        .unwrap();
    let prod = open_ingress(dir.path());
    for i in 1..=n {
        write_submit_retrying(&prod, 5, i, &Cmd::Add(1));
    }
    wait_commit_covers_all(&node);
    // Spec §5.2: command the instant (see `purged_node_after_snapshotting_service`).
    command_instant(&node);
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
    let svc2 = ServiceBuilder::new(ServiceConfig::new(dir.path(), app), CountSm::default())
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

/// Coordinated-snapshot ruling P6. The artifact TAG is a file name, and a name
/// can lie: a `uc2ctl restore` of a mis-copied backup — or any rename — can
/// present an artifact built at `P0` as a later `P`. Installing it would leave
/// every frame in `(P0, P)` unapplied: a SILENT state gap, exactly the class
/// the reconstruction gap guard exists to fail-stop on, and one no SM-side
/// payload check can catch (the tag is an exclusive frontier, so the payload's
/// cursor legitimately sits below it). The framework's 16-byte envelope names
/// the instant the artifact was really built at, and reconstruction refuses by
/// name when the two disagree.
#[test]
fn a_renamed_artifact_is_refused_by_name_and_a_correct_one_installs() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let app = "rec_mistag";
    let (node, prod) = purged_node_after_snapshotting_service(dir.path(), app, 4_000);
    // Node-only commits above the instant, so a wrongly-installed artifact
    // would visibly lose the frames between the two positions.
    for i in 4_001..=4_200u32 {
        write_reg(&prod, i, i as u64);
    }
    wait_commit_covers_all(&node);

    let store = uc_service::snapshots::SnapshotStore::open(dir.path(), 0).unwrap();
    let (p0, real) = store.newest(u64::MAX).unwrap().expect("an artifact at P0");
    // The envelope says P0 whatever the file is called.
    let head = std::fs::read(&real).unwrap();
    assert_eq!(
        uc_service::snapshots::decode_snapshot_envelope(&head),
        Ok(p0),
        "the artifact carries UC's envelope naming its own instant"
    );

    // Rename it to claim a LATER position — the newest artifact `newest()`
    // will now pick, above the purge floor, so reconstruction reaches for it.
    let liar = real.with_file_name(format!("snap-{}.ultsnap", p0 + 64));
    std::fs::rename(&real, &liar).unwrap();

    // The refusal lands on the apply thread's first below-floor cycle, so
    // capture it the way `gap_without_snapshot_capability_fails_stop_with_named_contract`
    // does: a scoped panic hook over this test's window.
    PANIC_LOG.lock().unwrap().clear();
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|info| {
        PANIC_LOG.lock().unwrap().push(info.to_string());
    }));
    let svc_bad = ServiceBuilder::new(ServiceConfig::new(dir.path(), app), RegisterSm::default())
        .start_with_snapshots()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let fired = loop {
        if PANIC_LOG
            .lock()
            .unwrap()
            .iter()
            .any(|m| m.contains("MistaggedSnapshot"))
        {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    std::panic::set_hook(prev);
    assert!(
        fired,
        "a renamed artifact must be refused BY NAME, never installed into a gap"
    );
    svc_bad.crash();

    // Put the name back: the same artifact now verifies and installs, and the
    // frames above it replay.
    std::fs::rename(&liar, &real).unwrap();
    let svc =
        uc_service::ServiceBuilder::new(ServiceConfig::new(dir.path(), app), RegisterSm::default())
            .start_with_snapshots()
            .unwrap();
    let cnc = open_cnc(dir.path(), app);
    wait_service_caught_up(&cnc);
    assert_eq!(
        query_reg(&svc),
        Some(4_200),
        "install + tail replay: nothing between P0 and the frontier was lost"
    );
    svc.stop();
    node.stop();
}
