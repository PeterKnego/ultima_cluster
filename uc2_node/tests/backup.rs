// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M11 Task 1: `uc2_node::backup` — ordered-copy artifact + offline verify.
//!
//! Construction follows `lifecycle.rs`'s `single_node` pattern (a sole voter
//! on a freshly bound loopback socket) and `purge_safety.rs`'s pattern of
//! faking the service side — the node's purge driver only ever reads the
//! `service_snapshot_pos` marker on the cnc page (`purge_safety.rs`'s own
//! comment: "Task 4's node code never reads the snapshot FILE anyway"). Here
//! we DO need real snapshot files on disk (the backup/verify coverage check
//! reads them), so snapshot publish is faked with the real
//! `uc2_service::snapshots::SnapshotStore` (a `uc2_node` dev-dependency)
//! writing arbitrary bytes at the chosen position, while the purge trigger
//! itself is still the direct cnc-marker write `purge_safety.rs` uses.
//!
//! Journals use tiny segments (`SEG_BYTES`) on the ext4 `CARGO_TARGET_TMPDIR`
//! so a purge cycle actually drops whole segment files without megabytes of
//! churn — never `/tmp` (RAM-backed tmpfs, see CLAUDE.md "Local box").

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use uc2_client::Client;
use uc2_log::cnc::CncPage;
use uc2_net::fault::FaultConfig;
use uc2_node::backup::{backup_instance, restore_artifact, verify_artifact, BackupError};
use uc2_node::{InstanceDir, Node, NodeConfig, PurgePolicy};
use uc2_service::snapshots::SnapshotStore;
use uc2_service::{ServiceBuilder, ServiceConfig, StateMachine};

const SEG_BYTES: u64 = 64 * 1024;

fn config(dir: &Path, app: &str, purge: PurgePolicy) -> NodeConfig {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        bind,
        instance_dir: dir.to_path_buf(),
        app_id: app.into(),
        buffer_bytes: 1 << 20,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: FaultConfig::default(),
        purge,
        learners: Vec::new(),
        journal_segment_bytes: SEG_BYTES,
        crypto: uc2_node::CryptoConfig::Disabled,
    }
}

// Fix round 1 (M11 Task 2 review): this file's `wait_until` used the same
// 20s deadline as its sibling `purge_safety.rs`, but under this suite's
// actual parallel load (13 tests, each a real multi-agent node cluster —
// several spin up FOUR busy-spin polling-agent threads apiece — sharing a
// 4-core dev box) that margin genuinely isn't generous enough: at the
// default `cargo test` parallelism (num_cpus concurrent test threads), a
// worst case has ~4 of these tests' node clusters running at once, i.e.
// several-fold oversubscription of the 4 cores, and a specific polling
// agent (here, the purge/archive-floor agent `publish_snapshot_and_wait_for_purge`
// waits on) can go starved for tens of seconds at a stretch. Reproduced
// directly: `ordered_backup_never_produces_a_hole_under_purge_churn` (which
// shares this same `wait_until` via `publish_snapshot_and_wait_for_purge`)
// hit this exact `assert!` under default parallelism repeatedly before this
// fix, including once at a 60s deadline during this fix's own validation —
// 120s was chosen empirically to clear that same load with margin (see the
// commit message for the full 10x-green validation at this deadline). A
// longer deadline costs nothing on the success path (the loop returns the
// instant `f()` is true) — it only widens how long a genuinely loaded box is
// given before a real hang is reported.
fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while !f() {
        assert!(Instant::now() < deadline, "condition never held: {what}");
        std::thread::yield_now();
    }
}

fn open_cnc(dir: &Path, app: &str) -> Arc<CncPage> {
    CncPage::open_file(&dir.join("cnc2.dat"), app).expect("open cnc2.dat")
}

fn start_node(dir: &Path, app: &str, purge: PurgePolicy) -> Node {
    let node = Node::start(config(dir, app, purge)).expect("start");
    wait_until("can_serve", || node.can_serve());
    node
}

fn drive_and_quiesce(node: &Node, n: usize) {
    // Fix round 1 (M11 Task 2 review): widened alongside `wait_until`'s
    // deadline, same load-margin rationale (that function's doc).
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut sent = 0usize;
    while sent < n {
        assert!(Instant::now() < deadline, "submits stalled at {sent}/{n}");
        match node.submit(vec![0xAB; 64]) {
            Ok(()) => sent += 1,
            Err(_) => std::thread::yield_now(),
        }
    }
    let mut last = u64::MAX;
    wait_until("log quiescent", || {
        let c = node.counters();
        let (a, cm, d) =
            (c.append.load_acquire(), c.commit.load_acquire(), c.durable.load_acquire());
        let quiescent = a > 0 && a == cm && cm == d && a == last;
        last = a;
        quiescent
    });
}

/// Fix round 1 (M11 Task 2 review): keep driving further `drive_and_quiesce`
/// rounds (4000 more submits each) until the node's durable position exceeds
/// `target`, bounded by `timeout` overall — a deadline-bounded poll over
/// MORE THROUGHPUT, not a single fixed-count round asserted against once.
///
/// `a_wrong_order_copy_across_a_purge_is_detected_as_a_hole`'s setup used to
/// call `drive_and_quiesce(&node, 4000)` exactly once and then assert the
/// resulting durable position was big enough (`pos1 > SEG_BYTES`,
/// `pos2 > newest_copied`) — a FIXED cycle count standing in for "enough
/// data landed durably". One round is comfortably enough in the common case
/// (4000×64B ≈ 256KB versus a 64KB `SEG_BYTES`), but "comfortably enough in
/// the common case" is exactly the shape of margin that a genuinely loaded
/// box can eat into — this file's own `wait_until` deadline needed the same
/// widening (see that function's doc) for the same underlying reason: this
/// suite's default parallelism (12+ tests, each a real multi-agent node
/// cluster) on a 4-core dev box. Rather than assert a fixed round was
/// enough and panic if it wasn't, this loops: if durable still falls short
/// after a round, drive another one, bounded only by `timeout` — so the test
/// converges on real throughput instead of a point-in-time guess about how
/// much one round yields under contention.
fn drive_until_durable_exceeds(node: &Node, target: u64, timeout: Duration) -> u64 {
    let deadline = Instant::now() + timeout;
    loop {
        drive_and_quiesce(node, 4000);
        let durable = node.counters().durable.load_acquire();
        if durable > target {
            return durable;
        }
        assert!(
            Instant::now() < deadline,
            "durable position never exceeded {target} (stuck at {durable}) even after \
             repeated drive_and_quiesce rounds"
        );
    }
}

/// Publish a real (fake-content) snapshot file at `pos`, then drive the node's
/// purge machinery exactly as `purge_safety.rs` does (write the cnc marker
/// directly), and wait for `archive_first_base` to advance.
fn publish_snapshot_and_wait_for_purge(dir: &Path, app: &str, node: &Node, pos: u64) {
    let store = SnapshotStore::open(dir).expect("open snapshot store");
    store.publish(pos, |w| Ok(w.write_all(b"fake-snapshot-bytes")?)).expect("publish snapshot");
    let cnc = open_cnc(dir, app);
    cnc.snapshots().service_snapshot_pos.store_release(pos);
    let before = node.archive_first_base();
    wait_until("purge advanced the floor", || node.archive_first_base() > before || pos == 0);
}

fn scratch() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("uc2-backup-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

/// Fix round 1 (M11 Task 2 review, batched item 2): serialize this whole
/// file's tests — same precedent as `lincheck_v2/mod.rs`'s `serialize()`.
/// EVERY test in this file boots a REAL multi-agent node cluster (4 real
/// busy-spin polling-agent OS threads per node), and `cargo test`'s default
/// parallelism runs up to `num_cpus` of this file's `#[test]` fns
/// concurrently — 4 on this dev box, i.e. up to ~16 competing busy-spin
/// threads for 4 cores in the worst case. Widening `wait_until`'s deadline
/// (see its doc) alone was diagnosed NOT sufficient: even at a 120s
/// deadline, `ordered_backup_never_produces_a_hole_under_purge_churn`'s
/// "purge advanced the floor" wait hard-stalled for the ENTIRE 120s in 3 of
/// 10 runs, every time the identical wait, with no partial progress —
/// consistent with genuine OS-scheduler starvation of the archive/purge
/// agent thread under this box's oversubscription, not "just needs more
/// wall time" (a longer deadline cannot fix indefinite starvation). At most
/// one of this file's real node clusters now runs at a time, which removes
/// the oversubscription at its root; the lighter, cheap-when-uncontended
/// tests in this file keep the whole suite fast in practice (see the commit
/// message for the 10x-green wall-clock evidence).
static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serialize() -> std::sync::MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

// ---------------------------------------------------------------------- Test 1

#[test]
fn backup_of_a_stopped_node_verifies_and_reports_positions() {
    let _serialize_guard = serialize();
    let root = scratch();
    let dir = root.path().join("n0");
    let app = "backup1";

    let node = start_node(&dir, app, PurgePolicy::Disabled);
    drive_and_quiesce(&node, 2000);

    match node.stop_draining(Duration::from_secs(10)) {
        uc2_node::DrainOutcome::Drained => {}
        other => panic!("expected Drained, got {other:?}"),
    }

    let out = root.path().join("backup1-out");
    let report = backup_instance(&dir, &out).expect("backup_instance");
    assert!(report.journal_last_pos > 0, "expected a non-trivial durable frontier");
    assert!(report.files > 0, "expected at least one file copied");

    let verify_report = verify_artifact(&out).expect("verify_artifact");
    assert_eq!(verify_report.journal_last_pos, report.journal_last_pos);

    let manifest = std::fs::read_to_string(out.join("MANIFEST")).expect("read MANIFEST");
    assert!(manifest.contains("format=uc2-backup-v1"), "manifest: {manifest}");
    assert!(
        manifest.contains(&format!("journal_last_pos={}", report.journal_last_pos)),
        "manifest: {manifest}"
    );
}

// ---------------------------------------------------------------------- Test 2

#[test]
fn backup_of_a_running_node_under_load_verifies() {
    let _serialize_guard = serialize();
    let root = scratch();
    let dir = root.path().join("n0");
    let app = "backup2";

    let node = start_node(&dir, app, PurgePolicy::Disabled);
    drive_and_quiesce(&node, 500);

    let stop = AtomicBool::new(false);
    // `thread::scope` joins the submit thread before returning OR propagating
    // a panic below — this guard flips `stop` on unwind too (Drop runs during
    // the closure's own stack unwind, strictly before `scope`'s join), so an
    // assertion failure never hangs joining the load thread forever.
    struct StopOnDrop<'a>(&'a AtomicBool);
    impl Drop for StopOnDrop<'_> {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    let out = root.path().join("backup2-out");
    let report = std::thread::scope(|scope| {
        let _stop_guard = StopOnDrop(&stop);
        scope.spawn(|| {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let mut p = vec![0u8; 64];
                p[..8].copy_from_slice(&i.to_le_bytes());
                let _ = node.submit(p);
                i += 1;
                std::thread::yield_now();
            }
        });

        backup_instance(&dir, &out).expect("backup_instance under load")
    });
    // healed_torn_tail may go either way; only verify passing matters.
    let _ = report.healed_torn_tail;

    verify_artifact(&out).expect("verify_artifact of a backup taken under load");

    node.stop();
}

// ---------------------------------------------------------------------- Test 3

/// Anti-vacuity for the whole task: build a BROKEN artifact by hand (copy
/// `snapshots/` FIRST, force a purge that overtakes it, THEN copy `journal/` +
/// `state/`) and confirm `verify_artifact` calls it out as a hole.
#[test]
fn a_wrong_order_copy_across_a_purge_is_detected_as_a_hole() {
    let _serialize_guard = serialize();
    let root = scratch();
    let dir = root.path().join("n0");
    let app = "backup3";

    let node =
        start_node(&dir, app, PurgePolicy::BelowSnapshot { slack_bytes: 0 });

    // Deadline-bounded, not a fixed cycle count (see `drive_until_durable_exceeds`'s
    // doc — this is the fix for the flake this test used to hit under load).
    let durable1 = drive_until_durable_exceeds(&node, 3 * SEG_BYTES, Duration::from_secs(120));
    let pos1 = durable1 / 3;
    assert!(
        pos1 > SEG_BYTES,
        "test setup: need >1 segment below the first floor (durable1={durable1} pos1={pos1})"
    );
    publish_snapshot_and_wait_for_purge(&dir, app, &node, pos1);
    let first_base_1 = node.archive_first_base();
    assert!(first_base_1 > 0, "first purge must have landed");

    // Copy `snapshots/` EARLY — it now holds (at least) the snap at pos1.
    let out = root.path().join("backup3-out");
    std::fs::create_dir_all(out.join("snapshots")).unwrap();
    for entry in std::fs::read_dir(dir.join("snapshots")).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), out.join("snapshots").join(entry.file_name())).unwrap();
    }
    let newest_copied = std::fs::read_dir(out.join("snapshots"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            e.file_name()
                .to_str()
                .and_then(|n| n.strip_prefix("snap-"))
                .and_then(|n| n.strip_suffix(".ultsnap"))
                .and_then(|n| n.parse::<u64>().ok())
        })
        .max()
        .expect("at least one snapshot copied early");

    // Force another snapshot + purge cycle that lands STRICTLY past the
    // copied snapshot's newest position, so the journal copy (taken after)
    // reflects a purge floor the early snapshots/ copy does not cover.
    // Same deadline-bounded-not-fixed-count fix as the pos1 setup above.
    let durable2 =
        drive_until_durable_exceeds(&node, newest_copied + SEG_BYTES, Duration::from_secs(120));
    let pos2 = durable2 - SEG_BYTES; // deep into the log, well past pos1/newest_copied
    assert!(
        pos2 > newest_copied,
        "test setup: second floor must overtake the copied snapshot \
         (durable2={durable2} pos2={pos2} newest_copied={newest_copied})"
    );
    publish_snapshot_and_wait_for_purge(&dir, app, &node, pos2);
    wait_until("first_base overtook the copied snapshot", || {
        node.archive_first_base() > newest_copied
    });

    // NOW copy journal/ + state/ (late) — the broken artifact.
    std::fs::create_dir_all(out.join("journal")).unwrap();
    for entry in std::fs::read_dir(dir.join("journal")).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            std::fs::copy(entry.path(), out.join("journal").join(entry.file_name())).unwrap();
        }
    }
    std::fs::create_dir_all(out.join("state")).unwrap();
    for entry in std::fs::read_dir(dir.join("state")).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), out.join("state").join(entry.file_name())).unwrap();
    }

    node.stop();

    match verify_artifact(&out) {
        Err(BackupError::Hole { first_base, newest_snapshot }) => {
            assert!(first_base > newest_snapshot.unwrap_or(0));
            eprintln!(
                "observed hole: first_base={first_base} newest_snapshot={newest_snapshot:?} \
                 (early-copied newest was {newest_copied})"
            );
        }
        other => panic!("expected Hole, got {other:?}"),
    }
}

// ---------------------------------------------------------------------- Test 4

#[test]
fn ordered_backup_never_produces_a_hole_under_purge_churn() {
    let _serialize_guard = serialize();
    let root = scratch();
    let dir = root.path().join("n0");
    let app = "backup4";

    let node =
        start_node(&dir, app, PurgePolicy::BelowSnapshot { slack_bytes: 0 });

    for cycle in 0..5u64 {
        drive_and_quiesce(&node, 3000);
        let cnc = open_cnc(&dir, app);
        let durable = cnc.counters().durable.load_acquire();
        let pos = durable.saturating_sub(SEG_BYTES / 2).max(1);
        publish_snapshot_and_wait_for_purge(&dir, app, &node, pos);

        let out = root.path().join(format!("backup4-out-{cycle}"));
        let report = backup_instance(&dir, &out).unwrap_or_else(|e| {
            panic!("backup_instance failed on cycle {cycle}: {e}")
        });
        assert!(report.files > 0);
        verify_artifact(&out)
            .unwrap_or_else(|e| panic!("verify_artifact failed on cycle {cycle}: {e}"));
    }

    node.stop();
}

// ------------------------------------------------- fix round 2: shipped racer
//
// The churn test above never races a live purge against an in-flight
// `backup_instance` copy — its purge-and-wait always completes before the
// next `backup_instance` call starts, so it alone never exercised the
// `copy_dir_sorted` vanished-file race a real backup-under-purge can hit.
//
// A first attempt at the stronger version (before `copy_dir_sorted` retried
// a whole directory on a vanished file, see `uc2_node/src/backup.rs`) failed
// reproducibly ~1-in-3 runs with `Io(NotFound)`: a concurrent purge
// (`journal/`) or snapshot-retention unlink (`snapshots/`) racing an
// already-listed file's `fs::copy`. That was the RED evidence for the fix
// (retry the whole directory, bounded — see `copy_dir_sorted`'s doc for why
// a per-file skip is unsafe). This is the fixed, GREEN version of that same
// test, now shipped as a real regression guard: background submit +
// snapshot-publish + purge-marker loop running DURING repeated
// `backup_instance` calls with no quiescence wait, so a purge can genuinely
// land mid-copy. Every artifact must still verify Ok.
#[test]
fn ordered_backup_survives_a_purge_racing_the_copy() {
    let _serialize_guard = serialize();
    let root = scratch();
    let dir = root.path().join("n0");
    let app = "backup9";

    let node = start_node(&dir, app, PurgePolicy::BelowSnapshot { slack_bytes: 0 });
    drive_and_quiesce(&node, 3000);

    let stop = AtomicBool::new(false);
    struct StopOnDrop<'a>(&'a AtomicBool);
    impl Drop for StopOnDrop<'_> {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    let cnc = open_cnc(&dir, app);
    let store = SnapshotStore::open(&dir).expect("open snapshot store");

    std::thread::scope(|scope| {
        let _guard = StopOnDrop(&stop);

        // Background: submit continuously, and every 400 submits publish a
        // fresh snapshot near the current durable frontier and advance the
        // purge marker — driving genuine, ongoing purge/retention unlinks in
        // both journal/ and snapshots/ throughout the whole scope.
        scope.spawn(|| {
            let mut i: u64 = 0;
            let mut next_publish_at = 400u64;
            while !stop.load(Ordering::Relaxed) {
                let mut p = vec![0u8; 64];
                p[..8].copy_from_slice(&i.to_le_bytes());
                let _ = node.submit(p);
                i += 1;
                if i >= next_publish_at {
                    next_publish_at = i + 400;
                    let durable = cnc.counters().durable.load_acquire();
                    if durable > SEG_BYTES {
                        let pos = durable.saturating_sub(SEG_BYTES / 2).max(1);
                        if store.publish(pos, |w| Ok(w.write_all(b"race-snapshot")?)).is_ok() {
                            cnc.snapshots().service_snapshot_pos.store_release(pos);
                        }
                    }
                }
                std::thread::yield_now();
            }
        });

        for cycle in 0..5u32 {
            // Deliberately no quiescence/purge wait here: the background
            // thread's submits and purges are free to land during the copy.
            let out = root.path().join(format!("backup9-out-{cycle}"));
            let report = backup_instance(&dir, &out).unwrap_or_else(|e| {
                panic!("backup_instance failed under a live purge race on cycle {cycle}: {e}")
            });
            assert!(report.files > 0);
            // Coherence sanity check on a report that may have come from a
            // retried (whole-directory-restarted) copy: the recovered
            // durable frontier can never be below the recovered floor.
            assert!(
                report.journal_last_pos >= report.journal_first_base,
                "incoherent report on cycle {cycle}: journal_last_pos {} < \
                 journal_first_base {}",
                report.journal_last_pos,
                report.journal_first_base
            );
            verify_artifact(&out).unwrap_or_else(|e| {
                panic!("verify_artifact failed under a live purge race on cycle {cycle}: {e}")
            });
        }
    });

    node.stop();
}

// ---------------------------------------------------------------------- Test 5

#[test]
fn manifest_tamper_is_caught() {
    let _serialize_guard = serialize();
    let root = scratch();
    let dir = root.path().join("n0");
    let app = "backup5";

    let node = start_node(&dir, app, PurgePolicy::Disabled);
    drive_and_quiesce(&node, 1000);
    node.stop();

    let out = root.path().join("backup5-out");
    let report = backup_instance(&dir, &out).expect("backup_instance");

    let manifest_path = out.join("MANIFEST");
    let original = std::fs::read_to_string(&manifest_path).unwrap();
    let tampered = original.replace(
        &format!("journal_first_base={}", report.journal_first_base),
        &format!("journal_first_base={}", report.journal_first_base + 1),
    );
    assert_ne!(original, tampered, "tamper must actually change the text");
    std::fs::write(&manifest_path, tampered).unwrap();

    match verify_artifact(&out) {
        Err(BackupError::ManifestMismatch(msg)) => {
            eprintln!("caught tamper: {msg}");
        }
        other => panic!("expected ManifestMismatch, got {other:?}"),
    }
}

// ---------------------------------------------------------------------- extra: input validation

#[test]
fn backup_instance_refuses_a_non_instance_dir() {
    let _serialize_guard = serialize();
    let root = scratch();
    let not_instance = root.path().join("not-an-instance");
    std::fs::create_dir_all(&not_instance).unwrap();
    let out = root.path().join("out");
    match backup_instance(&not_instance, &out) {
        Err(BackupError::NotAnInstanceDir) => {}
        other => panic!("expected NotAnInstanceDir, got {other:?}"),
    }
}

#[test]
fn backup_instance_refuses_a_nonempty_existing_out_dir() {
    let _serialize_guard = serialize();
    let root = scratch();
    let dir = root.path().join("n0");
    let app = "backup6";
    let node = start_node(&dir, app, PurgePolicy::Disabled);
    drive_and_quiesce(&node, 200);
    node.stop();

    let out = root.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(out.join("junk"), b"x").unwrap();

    match backup_instance(&dir, &out) {
        Err(BackupError::ArtifactExists) => {}
        other => panic!("expected ArtifactExists, got {other:?}"),
    }
}

#[test]
fn verify_artifact_refuses_a_non_artifact_dir() {
    let _serialize_guard = serialize();
    let root = scratch();
    let not_artifact = root.path().join("not-an-artifact");
    std::fs::create_dir_all(&not_artifact).unwrap();
    match verify_artifact(&not_artifact) {
        Err(BackupError::NotAnArtifact) => {}
        other => panic!("expected NotAnArtifact, got {other:?}"),
    }
}

// ------------------------------------------------------ regression: fix round 1

/// Recursive `relative path -> file size` snapshot of every regular file
/// under `root`.
fn file_sizes(root: &Path) -> BTreeMap<PathBuf, u64> {
    fn walk(dir: &Path, root: &Path, sizes: &mut BTreeMap<PathBuf, u64>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                walk(&path, root, sizes);
            } else {
                let rel = path.strip_prefix(root).unwrap().to_path_buf();
                sizes.insert(rel, entry.metadata().unwrap().len());
            }
        }
    }
    let mut sizes = BTreeMap::new();
    walk(root, root, &mut sizes);
    sizes
}

/// Fix round 1, CRITICAL: `verify_artifact` used to build its `ArchiveConfig`
/// with the (production, boot-matching) default `preallocate_segments: true`
/// and `segment_size_bytes: 64 MiB` — `Journal::open`'s post-recovery
/// re-preallocation step then unconditionally inflated the artifact's active
/// segment up to that mismatched default (this suite's own `SEG_BYTES = 64
/// KiB` config reproduced 65536 -> 67108864 bytes, 1024x growth, on every
/// verify call). The fix builds verify's journal config with
/// `preallocate_segments: false`, so the ONE permitted mutation (healing a
/// torn active-segment tail) is a physical `truncate` — shrink-only, never a
/// grow — and only fires when a torn tail was actually found.
///
/// This test creates a real artifact under the suite's small-segment config,
/// snapshots every file's size, runs `verify_artifact` TWICE, and asserts: no
/// file ever grows; any shrink is accompanied by a reported
/// `healed_torn_tail`; and the second verify changes nothing further
/// (idempotence) and reports the identical `BackupReport`.
#[test]
fn verify_never_grows_the_artifacts_files_and_is_idempotent() {
    let _serialize_guard = serialize();
    let root = scratch();
    let dir = root.path().join("n0");
    let app = "backup7";

    let node = start_node(&dir, app, PurgePolicy::Disabled);
    drive_and_quiesce(&node, 500);
    node.stop();

    let out = root.path().join("backup7-out");
    backup_instance(&dir, &out).expect("backup_instance");

    // Sanity: the active segment in this artifact is genuinely small (the
    // suite's SEG_BYTES), i.e. this test would actually catch the reported
    // 64 KiB -> 64 MiB regression if it reappeared.
    let active_segment_before = file_sizes(&out)
        .into_iter()
        .filter(|(p, _)| p.starts_with("journal") && p.to_string_lossy().ends_with(".log"))
        .map(|(_, size)| size)
        .max()
        .expect("artifact must contain at least one journal segment");
    assert!(
        active_segment_before <= SEG_BYTES * 2,
        "test setup: active segment {active_segment_before} bytes is not small \
         (SEG_BYTES={SEG_BYTES}) — this test would not catch a re-preallocation regression"
    );

    let before = file_sizes(&out);
    let report1 = verify_artifact(&out).expect("verify #1");
    let after1 = file_sizes(&out);

    for (path, &before_size) in &before {
        let after_size = *after1
            .get(path)
            .unwrap_or_else(|| panic!("file {path:?} disappeared during verify_artifact"));
        assert!(
            after_size <= before_size,
            "file {path:?} grew from {before_size} to {after_size} bytes during \
             verify_artifact — verify must never grow an artifact file"
        );
        if after_size < before_size {
            assert!(
                report1.healed_torn_tail,
                "file {path:?} shrank ({before_size} -> {after_size}) but the report did not \
                 say healed_torn_tail — an unreported mutation"
            );
        }
    }

    // Idempotence: a second verify must not change anything further, and
    // must report the exact same recovered positions.
    let report2 = verify_artifact(&out).expect("verify #2");
    let after2 = file_sizes(&out);
    assert_eq!(after1, after2, "a second verify_artifact must not mutate any file further");
    assert_eq!(report1, report2, "a second verify_artifact must report identical positions");
}

// ---------------------------------------------------------------------- M11 Task 2: restore

/// A running total, same minimal shape as `uc2_client/tests/roundtrip.rs`'s
/// `CountSm` — persists NOTHING itself, so a correct readback after restore
/// can only come from the restored node's journal being replayed by a freshly
/// (empty-)started service, exactly the M9 reconstruction path.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum RestoreCmd {
    Add(u64),
}

#[derive(Default)]
struct RestoreCountSm {
    total: u64,
    last_applied: Option<u64>,
}

impl StateMachine for RestoreCountSm {
    type Command = RestoreCmd;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, position: u64, cmd: RestoreCmd) -> u64 {
        let RestoreCmd::Add(n) = cmd;
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

/// End-to-end: back up a stopped single node's instance dir, restore the
/// artifact into a FRESH instance dir, boot a node + service there, and
/// confirm the restored cluster elects, serves, and reads back a value
/// submitted (and committed) BEFORE the backup was taken.
#[test]
fn restore_roundtrip_boots_and_serves() {
    let _serialize_guard = serialize();
    let root = scratch();
    let dir = root.path().join("n0");
    let app = "restore1";

    let node = start_node(&dir, app, PurgePolicy::Disabled);
    let svc = ServiceBuilder::new(ServiceConfig::new(&dir, app), RestoreCountSm::default())
        .start()
        .expect("start service");

    let client = Client::connect(&dir, app).expect("connect client");
    let mut expected_total = 0u64;
    for _ in 0..50u64 {
        let got: u64 = client.submit(&RestoreCmd::Add(1)).expect("submit");
        expected_total += 1;
        assert_eq!(got, expected_total, "apply order must match submission order");
    }
    client.shutdown();

    // Node-first-then-service teardown (see `lincheck_v2/mod.rs`'s doc for
    // why): the node's read barrier / apply-progress waits can otherwise
    // block on a service that's already gone.
    match node.stop_draining(Duration::from_secs(10)) {
        uc2_node::DrainOutcome::Drained => {}
        other => panic!("expected Drained, got {other:?}"),
    }
    svc.stop();

    let artifact = root.path().join("restore1-artifact");
    let backup_report = backup_instance(&dir, &artifact).expect("backup_instance");

    let fresh_dir = root.path().join("n0-restored");
    let restore_report = restore_artifact(&artifact, &fresh_dir).expect("restore_artifact");
    assert_eq!(
        restore_report.journal_last_pos, backup_report.journal_last_pos,
        "restore_artifact must report the artifact's own recovered positions"
    );

    let restored_node = start_node(&fresh_dir, app, PurgePolicy::Disabled);
    let restored_svc = ServiceBuilder::new(
        ServiceConfig::new(&fresh_dir, app),
        RestoreCountSm::default(),
    )
    .start()
    .expect("start restored service");

    let restored_client = Client::connect(&fresh_dir, app).expect("connect restored client");
    let got: u64 = restored_client
        .query_linearizable(&())
        .expect("linearizable query after restore");
    assert_eq!(got, expected_total, "restored cluster must serve the pre-backup value");

    restored_client.shutdown();
    restored_node.stop();
    restored_svc.stop();
}

/// Fix round 1 (M11 Task 2 review, Finding 1): the empty-dirs/stale-volatile-
/// files tolerance is the review's named risk for `restore_artifact` — correct
/// by inspection (it only checks `journal/`/`state/`/`snapshots/` for a
/// non-empty `read_dir`, and never looks at `cnc2.dat`/`instance.lock` at
/// all) but untested until now. Pre-creates EMPTY `journal/`, `state/`,
/// `snapshots/` dirs plus a stale `instance.lock` and a garbage `cnc2.dat`
/// (arbitrary bytes — both are volatile, boot recreates/re-takes them
/// unconditionally) in the restore target BEFORE calling `restore_artifact`,
/// then reuses `restore_roundtrip_boots_and_serves`'s tail to confirm the
/// restore still succeeds and the restored node actually boots and serves.
#[test]
fn restore_accepts_a_target_with_empty_dirs_and_a_stale_lock() {
    let _serialize_guard = serialize();
    let root = scratch();
    let dir = root.path().join("n0");
    let app = "restore3";

    let node = start_node(&dir, app, PurgePolicy::Disabled);
    let svc = ServiceBuilder::new(ServiceConfig::new(&dir, app), RestoreCountSm::default())
        .start()
        .expect("start service");

    let client = Client::connect(&dir, app).expect("connect client");
    let mut expected_total = 0u64;
    for _ in 0..50u64 {
        let got: u64 = client.submit(&RestoreCmd::Add(1)).expect("submit");
        expected_total += 1;
        assert_eq!(got, expected_total, "apply order must match submission order");
    }
    client.shutdown();

    match node.stop_draining(Duration::from_secs(10)) {
        uc2_node::DrainOutcome::Drained => {}
        other => panic!("expected Drained, got {other:?}"),
    }
    svc.stop();

    let artifact = root.path().join("restore3-artifact");
    backup_instance(&dir, &artifact).expect("backup_instance");

    // The target: EMPTY durable subdirectories (not absent — `restore_artifact`
    // must tolerate a target that already has the layout, just nothing in it)
    // plus a stale `instance.lock` and a garbage `cnc2.dat` — both volatile,
    // neither should gate (or be touched meaningfully by) a restore.
    let fresh_dir = root.path().join("n0-restored");
    std::fs::create_dir_all(fresh_dir.join("journal")).unwrap();
    std::fs::create_dir_all(fresh_dir.join("state")).unwrap();
    std::fs::create_dir_all(fresh_dir.join("snapshots")).unwrap();
    std::fs::write(fresh_dir.join("instance.lock"), b"stale-lock-bytes").unwrap();
    std::fs::write(fresh_dir.join("cnc2.dat"), b"garbage-not-a-real-cnc-page").unwrap();

    restore_artifact(&artifact, &fresh_dir).expect("restore_artifact must accept an empty-dirs, stale-volatile-files target");

    let restored_node = start_node(&fresh_dir, app, PurgePolicy::Disabled);
    let restored_svc = ServiceBuilder::new(
        ServiceConfig::new(&fresh_dir, app),
        RestoreCountSm::default(),
    )
    .start()
    .expect("start restored service");

    let restored_client = Client::connect(&fresh_dir, app).expect("connect restored client");
    let got: u64 = restored_client
        .query_linearizable(&())
        .expect("linearizable query after restore");
    assert_eq!(got, expected_total, "restored cluster must serve the pre-backup value");

    restored_client.shutdown();
    restored_node.stop();
    restored_svc.stop();
}

/// `restore_artifact` must refuse a target `instance_dir` whose `journal/` is
/// already non-empty (a leftover from some earlier, un-decommissioned
/// instance) rather than merge or overwrite into it — and must leave that
/// leftover completely untouched.
#[test]
fn restore_refuses_a_dirty_target() {
    let _serialize_guard = serialize();
    let root = scratch();
    let dir = root.path().join("n0");
    let app = "restore2";

    let node = start_node(&dir, app, PurgePolicy::Disabled);
    drive_and_quiesce(&node, 200);
    match node.stop_draining(Duration::from_secs(10)) {
        uc2_node::DrainOutcome::Drained => {}
        other => panic!("expected Drained, got {other:?}"),
    }

    let artifact = root.path().join("restore2-artifact");
    backup_instance(&dir, &artifact).expect("backup_instance");

    let target = root.path().join("dirty-target");
    std::fs::create_dir_all(target.join("journal")).unwrap();
    std::fs::write(target.join("journal").join("leftover.log"), b"junk").unwrap();

    match restore_artifact(&artifact, &target) {
        Err(BackupError::TargetNotEmpty) => {}
        other => panic!("expected TargetNotEmpty, got {other:?}"),
    }

    // Refused, not partially applied: the leftover file is untouched and
    // nothing else was created under the target.
    let leftover = std::fs::read(target.join("journal").join("leftover.log")).unwrap();
    assert_eq!(leftover, b"junk");
    assert!(!target.join("state").exists(), "refused restore must not create state/");
    assert!(!target.join("snapshots").exists(), "refused restore must not create snapshots/");
}

// ---------------------------------------------------------------------- Final review, Important 1

/// Final review, Important 1: `verify_artifact` must refuse a RUNNING node's
/// own instance directory rather than heal (physically truncate) a torn tail
/// out from under its live writer — a narrow, real acked-write-loss race an
/// operator typo (or swapped restore/verify arguments) could trigger.
/// `refuse_if_live_instance_dir` (`uc2_node::backup`) closes this: a backup
/// artifact never contains `instance.lock`, so its presence AND a held flock
/// means `verify_artifact` was pointed at a live node's own directory.
#[test]
fn verify_artifact_refuses_a_running_instance_dir() {
    let _serialize_guard = serialize();
    let root = scratch();
    let dir = root.path().join("n0");
    let app = "live-verify";

    let node = start_node(&dir, app, PurgePolicy::Disabled);
    drive_and_quiesce(&node, 200);

    match verify_artifact(&dir) {
        Err(BackupError::LooksLikeLiveInstanceDir(p)) => assert_eq!(p, dir),
        other => panic!("expected LooksLikeLiveInstanceDir, got {other:?}"),
    }

    match node.stop_draining(Duration::from_secs(10)) {
        uc2_node::DrainOutcome::Drained => {}
        other => panic!("expected Drained, got {other:?}"),
    }
}

/// Final review, Important 1: `restore_artifact`'s `artifact` argument gets
/// the same refusal for free, via `verify_artifact`'s internal probe — a
/// live node's own instance directory is never a valid artifact to restore
/// FROM either. Refused before anything is read from or written to `target`.
#[test]
fn restore_artifact_refuses_a_running_artifact_argument() {
    let _serialize_guard = serialize();
    let root = scratch();
    let dir = root.path().join("n0");
    let app = "live-restore-src";

    let node = start_node(&dir, app, PurgePolicy::Disabled);
    drive_and_quiesce(&node, 200);

    let target = root.path().join("fresh-target");
    match restore_artifact(&dir, &target) {
        Err(BackupError::LooksLikeLiveInstanceDir(p)) => assert_eq!(p, dir),
        other => panic!("expected LooksLikeLiveInstanceDir, got {other:?}"),
    }
    assert!(!target.exists(), "a refused restore must not create the target at all");

    match node.stop_draining(Duration::from_secs(10)) {
        uc2_node::DrainOutcome::Drained => {}
        other => panic!("expected Drained, got {other:?}"),
    }
}

/// Final review, Important 1: `restore_artifact`'s TARGET gets the same
/// probe too, separately from its `artifact` argument — the window this
/// closes is a target whose `journal/`/`state/`/`snapshots/` are all still
/// EMPTY (so `TargetNotEmpty` alone would not catch it) because a node has
/// only just called `InstanceDir::acquire` and not yet written anything.
/// That boot window is real but narrow for a genuinely running node, so this
/// pins the guard directly with `InstanceDir::acquire` (the same primitive a
/// real boot's first step uses) rather than racing to observe it.
#[test]
fn restore_artifact_refuses_a_live_but_empty_target() {
    let _serialize_guard = serialize();
    let root = scratch();

    // A real, verifiable artifact — its own source node is unrelated to the
    // target under test and is fully stopped before restore is attempted.
    let src_dir = root.path().join("n0-src");
    let app = "live-restore-target";
    let node = start_node(&src_dir, app, PurgePolicy::Disabled);
    drive_and_quiesce(&node, 200);
    match node.stop_draining(Duration::from_secs(10)) {
        uc2_node::DrainOutcome::Drained => {}
        other => panic!("expected Drained, got {other:?}"),
    }
    let artifact = root.path().join("live-restore-target-artifact");
    backup_instance(&src_dir, &artifact).expect("backup_instance");

    let target = root.path().join("live-empty-target");
    let held = InstanceDir::acquire(&target).expect("acquire");

    match restore_artifact(&artifact, &target) {
        Err(BackupError::LooksLikeLiveInstanceDir(p)) => assert_eq!(p, target),
        other => panic!("expected LooksLikeLiveInstanceDir, got {other:?}"),
    }

    drop(held);
}

/// Final review, Important 1: the flip side of the refusal — a STOPPED
/// node's own instance directory has a leftover `instance.lock` FILE
/// (shutdown releases the flock but never deletes the file, same as the
/// pre-existing `restore_accepts_a_target_with_empty_dirs_and_a_stale_lock`
/// fixture's stale lock) and must still verify fine IN PLACE, not only via a
/// copied-out artifact. This is the legitimate case the fix's blueprint
/// explicitly calls out as one the refusal must not break.
#[test]
fn verify_artifact_proceeds_on_a_stopped_nodes_dir_with_a_stale_lock() {
    let _serialize_guard = serialize();
    let root = scratch();
    let dir = root.path().join("n0");
    let app = "stopped-verify-in-place";

    let node = start_node(&dir, app, PurgePolicy::Disabled);
    drive_and_quiesce(&node, 200);
    match node.stop_draining(Duration::from_secs(10)) {
        uc2_node::DrainOutcome::Drained => {}
        other => panic!("expected Drained, got {other:?}"),
    }

    assert!(dir.join("instance.lock").is_file(), "shutdown must leave the lock FILE in place");

    let report =
        verify_artifact(&dir).expect("a stopped node's own instance dir must verify in place");
    assert!(report.journal_last_pos > 0);
}
