// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M6 Task 4 — durable snapshot floor + policy-gated purge driver.
//!
//! These tests drive the NODE half in isolation: a real single node over
//! loopback, with the cnc `service_snapshot_pos` slot written DIRECTLY (standing
//! in for the service's snapshot builder, which is exercised end-to-end in
//! `uc2_service`). That split is deliberate — `uc2_node` cannot depend on
//! `uc2_service` (it is the other way round), and Task 4's node code never reads
//! the snapshot FILE anyway (Task 5 same-host / Task 6 remote reconstruction
//! do). So there is nothing to fake but the published position.
//!
//! Journals live on the ext4 `CARGO_TARGET_TMPDIR` (tiny 64 KiB segments so a
//! purge actually drops whole segment files without writing gigabytes — `/tmp`
//! is a quota'd RAM tmpfs).

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use uc2_log::cnc::CncPage;
use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig, PurgePolicy};

/// Tiny journal segments so a handful of KiB of frames rolls many segment files
/// and `purge_below` has non-active segments to drop.
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

/// Submit `n` 64-byte payloads (retrying through admission backpressure), then
/// wait until the log is quiescent (`append == commit == durable`, stable across
/// two reads) so `durable` is a settled byte position we can slice.
fn drive_and_quiesce(node: &Node, n: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut sent = 0usize;
    while sent < n {
        assert!(Instant::now() < deadline, "submits stalled at {sent}/{n}");
        match node.submit(vec![0xAB; 64]) {
            Ok(()) => sent += 1,
            Err(_) => std::thread::yield_now(), // admission window closed / draining
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

/// The whole Task 4 loop on one host: a published snapshot position is validated,
/// persisted as the durable floor, mirrored, drives a purge below it — and only
/// below it — and the durable floor survives a node restart without regressing.
#[test]
fn marker_persists_and_purge_advances_only_below_it() {
    let root = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let dir = root.path();
    let app = "purge1";

    let node = Node::start(config(dir, app, PurgePolicy::BelowSnapshot { slack_bytes: 0 })).unwrap();
    wait_until("can_serve", || node.can_serve());
    drive_and_quiesce(&node, 6000);

    let cnc = open_cnc(dir, app);

    // No service snapshot published yet -> the floor is 0 and purge never fires,
    // no matter how much log exists (purge is gated on the marker).
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(node.archive_first_base(), 0, "purge must stay gated on the marker");
    assert_eq!(
        cnc.snapshots().node_snapshot_floor.load_acquire(),
        0,
        "no floor persisted without a marker"
    );

    // The service publishes a snapshot at an applied position. Pick a byte
    // position well into the journal (below the active segment) so purging below
    // it drops whole segment files. `s <= durable` by construction.
    let durable = cnc.counters().durable.load_acquire();
    let s = durable / 2;
    assert!(s > SEG_BYTES, "test setup: need >1 segment below the marker");
    cnc.snapshots().service_snapshot_pos.store_release(s);

    // The node validates (`<= durable`), durably persists the floor, mirrors it,
    // and commands the purge.
    wait_until("purge advanced the floor", || node.archive_first_base() > 0);
    assert!(node.archive_first_base() <= s, "never purged at/above the snapshot floor");
    assert_eq!(
        cnc.snapshots().node_snapshot_floor.load_acquire(),
        s,
        "the mirrored floor equals the validated position"
    );

    // Restart: the durable floor is a high-water mark. A fresh cnc page starts at
    // 0, but boot re-seeds the mirror + the persister's shadow from the durable
    // value, so the floor never regresses (the marker-clobber lesson).
    node.stop();
    let node = Node::start(config(dir, app, PurgePolicy::BelowSnapshot { slack_bytes: 0 })).unwrap();
    wait_until("can_serve after restart", || node.can_serve());
    let cnc2 = open_cnc(dir, app);
    assert_eq!(
        cnc2.snapshots().node_snapshot_floor.load_acquire(),
        s,
        "durable snapshot floor recovered onto the fresh page, not regressed to 0"
    );
    node.stop();
}

/// A `service_snapshot_pos` ahead of this node's durable frontier is a torn/racy
/// read (or a snapshot at a not-yet-durable position). It must NEVER become the
/// floor — a purge floor is only ever a position whose covering block is durable
/// here.
#[test]
fn snapshot_pos_above_durable_is_never_persisted() {
    let root = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let dir = root.path();
    let app = "purge2";

    let node = Node::start(config(dir, app, PurgePolicy::BelowSnapshot { slack_bytes: 0 })).unwrap();
    wait_until("can_serve", || node.can_serve());
    drive_and_quiesce(&node, 2000);

    let cnc = open_cnc(dir, app);
    let durable = cnc.counters().durable.load_acquire();
    // Publish a position far beyond durable — an insane/torn value.
    cnc.snapshots().service_snapshot_pos.store_release(durable + 1_000_000);

    // Give the consensus loop many duty cycles to (not) act on it.
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        cnc.snapshots().node_snapshot_floor.load_acquire(),
        0,
        "a value above durable is ignored — never persisted, never mirrored"
    );
    assert_eq!(node.archive_first_base(), 0, "and nothing is purged");
    node.stop();
}
