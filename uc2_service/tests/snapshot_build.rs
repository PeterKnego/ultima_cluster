// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M6 Task 3 capstone: a single node + a snapshot-capable service
//! (`uc-lincheck`'s `RegisterSm`, feature `v2`) started via
//! `start_with_snapshots` builds position-tagged on-disk snapshot files on a
//! real byte-interval policy, publishes the newest complete position onto the
//! cnc marker only AFTER the atomic rename, and enforces keep-newest-2
//! retention — end to end through the real ingress/cnc/client IPC (spec M6
//! Task 3, brief step 1's verbatim test).

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use uc2_client::Client;
use uc2_log::cnc::CncPage;
use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};
use uc2_service::{ServiceBuilder, ServiceConfig, SnapshotPolicy};
use uc_lincheck::register::{Cmd, CmdResp, RegisterSm};

// --------------------------------------------------------------------- harness

fn node_config(dir: &Path, app_id: &str) -> NodeConfig {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        bind,
        instance_dir: dir.to_path_buf(),
        app_id: app_id.into(),
        buffer_bytes: 1 << 20,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: FaultConfig::default(),
        purge: uc2_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
    }
}

fn start_single_node(dir: &Path, app_id: &str) -> Node {
    Node::start(node_config(dir, app_id)).unwrap()
}

fn cfg_with_policy(dir: &Path, app_id: &str, interval_bytes: u64) -> ServiceConfig {
    ServiceConfig::new(dir, app_id).snapshot_policy(SnapshotPolicy { interval_bytes })
}

fn open_cnc(dir: &Path, app_id: &str) -> Arc<CncPage> {
    CncPage::open_file(&dir.join("cnc2.dat"), app_id).unwrap()
}

fn wait_until(mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "condition never held");
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Count complete (`.ultsnap`) snapshot files on disk — a leftover `.tmp` from
/// an in-progress build (there shouldn't be one in this test, since every
/// publish either fully succeeds or is cleaned up) would NOT be counted.
fn count_snapshots(dir: &Path) -> usize {
    std::fs::read_dir(dir.join("snapshots"))
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().ends_with(".ultsnap"))
                .count()
        })
        .unwrap_or(0)
}

// ------------------------------------------------------------------------ test

#[test]
fn builder_publishes_position_tagged_snapshot_and_cnc_marker() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_single_node(dir.path(), "snapb");
    wait_until(|| node.can_serve());

    let svc =
        ServiceBuilder::new(cfg_with_policy(dir.path(), "snapb", 4 * 1024), RegisterSm::default())
            .start_with_snapshots()
            .unwrap();
    let client = Client::connect(dir.path(), "snapb").unwrap();
    for i in 0..400u64 {
        let _: CmdResp = client.submit(&Cmd::Write(i)).unwrap();
    } // >> 4 KiB of frames

    let cnc = open_cnc(dir.path(), "snapb");
    wait_until(|| cnc.snapshots().service_snapshot_pos.load_acquire() > 0);
    let s = cnc.snapshots().service_snapshot_pos.load_acquire();
    assert!(s <= cnc.service().service_applied.load_acquire(), "snapshot at an applied position");

    let store = uc2_service::snapshots::SnapshotStore::open(dir.path()).unwrap();
    let (pos, path) = store.newest(u64::MAX).unwrap().expect("file exists");
    assert_eq!(pos, s);
    assert!(path.ends_with(format!("snap-{s}.ultsnap")));

    // A second interval produces a newer one and retention holds (<= 2 files).
    for i in 0..400u64 {
        let _: CmdResp = client.submit(&Cmd::Write(i)).unwrap();
    }
    wait_until(|| cnc.snapshots().service_snapshot_pos.load_acquire() > s);
    assert!(count_snapshots(dir.path()) <= 2);

    client.shutdown();
    svc.stop();
    node.stop();
}

/// Default policy ("never" — `interval_bytes: 0`) means `start_with_snapshots`
/// spawns the builder thread machinery but it structurally never trips: no
/// file is ever written and the cnc marker stays `0`, even under real commit
/// traffic. Pins "no snapshots, no marker, no purge" as the observable default.
#[test]
fn default_policy_never_builds_a_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_single_node(dir.path(), "snapdef");
    wait_until(|| node.can_serve());

    let svc = ServiceBuilder::new(ServiceConfig::new(dir.path(), "snapdef"), RegisterSm::default())
        .start_with_snapshots()
        .unwrap();
    let client = Client::connect(dir.path(), "snapdef").unwrap();
    for i in 0..200u64 {
        let _: CmdResp = client.submit(&Cmd::Write(i)).unwrap();
    }

    let cnc = open_cnc(dir.path(), "snapdef");
    wait_until(|| cnc.service().service_applied.load_acquire() > 0);
    // Give the (structurally-never-tripping) builder thread ample cycles.
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(cnc.snapshots().service_snapshot_pos.load_acquire(), 0, "never policy: no marker");
    assert_eq!(count_snapshots(dir.path()), 0, "never policy: no file");

    client.shutdown();
    svc.stop();
    node.stop();
}
