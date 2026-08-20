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

use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use uc2_log::cnc::CncPage;
use uc2_net::fault::FaultConfig;
use uc2_node::backup::{backup_instance, verify_artifact, BackupError};
use uc2_node::{Node, NodeConfig, PurgePolicy};
use uc2_service::snapshots::SnapshotStore;

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

fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
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
    let deadline = Instant::now() + Duration::from_secs(30);
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

// ---------------------------------------------------------------------- Test 1

#[test]
fn backup_of_a_stopped_node_verifies_and_reports_positions() {
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
    let root = scratch();
    let dir = root.path().join("n0");
    let app = "backup3";

    let node =
        start_node(&dir, app, PurgePolicy::BelowSnapshot { slack_bytes: 0 });
    drive_and_quiesce(&node, 4000);

    let cnc = open_cnc(&dir, app);
    let durable1 = cnc.counters().durable.load_acquire();
    let pos1 = durable1 / 3;
    assert!(pos1 > SEG_BYTES, "test setup: need >1 segment below the first floor");
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
    drive_and_quiesce(&node, 4000);
    let durable2 = cnc.counters().durable.load_acquire();
    let pos2 = durable2 - SEG_BYTES; // deep into the log, well past pos1/newest_copied
    assert!(pos2 > newest_copied, "test setup: second floor must overtake the copied snapshot");
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

// ---------------------------------------------------------------------- Test 5

#[test]
fn manifest_tamper_is_caught() {
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
    let root = scratch();
    let not_artifact = root.path().join("not-an-artifact");
    std::fs::create_dir_all(&not_artifact).unwrap();
    match verify_artifact(&not_artifact) {
        Err(BackupError::NotAnArtifact) => {}
        other => panic!("expected NotAnArtifact, got {other:?}"),
    }
}
