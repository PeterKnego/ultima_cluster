// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M6 Task 3 capstone, as re-pointed by coordinated-snapshot plan 2 Task 3: a
//! single node + a snapshot-capable service (`uc_lincheck`'s `RegisterSm`,
//! feature `v2`) started via `start_with_snapshots` builds position-tagged
//! on-disk snapshot files **at the instants the LOG names** — a
//! `FRAME_TYPE_SNAPSHOT` frame, spec §5.2, in place of the deleted M6 byte
//! interval — and publishes the artifact's position onto
//! the cnc marker only AFTER the atomic rename, end to end through the real
//! ingress/cnc/client IPC.
//!
//! Retention is NOT asserted here any more (ruling P1): `publish` no longer
//! prunes, because only the node can see which artifacts form a complete set.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use uc_client::Client;
use uc_lincheck::register::{Cmd, CmdResp, RegisterSm};
use uc_log::cnc::CncPage;
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig};
use uc_service::{ServiceBuilder, ServiceConfig, StateMachine};

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
        services: uc_node::ServicesConfig::single(RegisterSm::NAME),
    }
}

fn start_single_node(dir: &Path, app_id: &str) -> Node {
    Node::start(node_config(dir, app_id)).unwrap()
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

/// `uc2ctl snapshot`, in process (coordinated-snapshot spec §5.5): command an
/// instant and return its position **P**, polling through the `retry` window a
/// leader legitimately answers while it has the role but not yet an appender.
/// Duplicated per test binary, like `admin_request_ok` elsewhere.
fn command_instant(node: &Node) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(10);
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

/// Count complete (`.ultsnap`) snapshot files on disk — a leftover `.tmp` from
/// an in-progress build (there shouldn't be one in this test, since every
/// publish either fully succeeds or is cleaned up) would NOT be counted.
fn count_snapshots(dir: &Path) -> usize {
    std::fs::read_dir(dir.join("snapshots").join("0"))
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

    let svc = ServiceBuilder::new(
        ServiceConfig::new(dir.path(), "snapb"),
        RegisterSm::default(),
    )
    .start_with_snapshots()
    .unwrap();
    let client = Client::connect(dir.path(), "snapb").unwrap();
    for i in 0..400u64 {
        let _: CmdResp = client.submit(&Cmd::Write(i)).unwrap();
    }

    let cnc = open_cnc(dir.path(), "snapb");
    // Nothing is built until the log says so — the trigger is the frame, not
    // a byte count (spec §5.2).
    assert_eq!(
        cnc.snapshots().service_snapshot_pos.load_acquire(),
        0,
        "no instant commanded yet: no artifact"
    );
    // `command_snapshot` RETURNS P — the `SNAPSHOT` frame's end — so the
    // instant's identity comes from the command rather than from reading the
    // append counter and hoping nothing else appended.
    let p = command_instant(&node);

    wait_until(|| cnc.snapshots().service_snapshot_pos.load_acquire() > 0);
    let s = cnc.snapshots().service_snapshot_pos.load_acquire();
    assert_eq!(
        s, p,
        "the artifact is tagged with the instant P — the SNAPSHOT frame's END, \
         not the SM's own cursor and not merely 'some applied position'"
    );
    assert!(
        s <= cnc.service().service_applied.load_acquire(),
        "snapshot at an applied position"
    );

    let store = uc_service::snapshots::SnapshotStore::open(dir.path(), 0).unwrap();
    let (pos, path) = store.newest(u64::MAX).unwrap().expect("file exists");
    assert_eq!(pos, s);
    assert!(path.ends_with(format!("snap-{s}.ultsnap")));

    // A second instant tags a NEWER artifact, and `publish` still prunes
    // nothing (ruling P1). What removes the older one is the NODE's set
    // retention, on the pass the set at p2 completes — so the steady state
    // here is exactly one artifact, at the newest complete set's position.
    for i in 0..400u64 {
        let _: CmdResp = client.submit(&Cmd::Write(i)).unwrap();
    }
    let p2 = command_instant(&node);
    wait_until(|| cnc.snapshots().service_snapshot_pos.load_acquire() == p2);
    wait_until(|| count_snapshots(dir.path()) == 1);
    assert_eq!(
        store.newest(u64::MAX).unwrap().expect("file exists").0,
        p2,
        "the artifact that survives is the one AT the complete set's position"
    );
    assert_eq!(
        node.snapshot_set_position(),
        p2,
        "the node completed its own set at the second instant"
    );

    client.shutdown();
    svc.stop();
    node.stop();
}

/// An UNCOMMANDED cluster never snapshots: `start_with_snapshots` spawns the
/// builder thread machinery, but with no `SNAPSHOT` frame on the log it never
/// trips — no file is written and the cnc marker stays `0`, even under real
/// commit traffic. Pins "no snapshots, no marker, no purge" as the observable
/// default, which is what keeps purge-off-by-default true (spec §5.5: a
/// cluster that is never asked to snapshot is legitimate).
#[test]
fn a_service_never_commanded_an_instant_never_builds_a_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_single_node(dir.path(), "snapdef");
    wait_until(|| node.can_serve());

    let svc = ServiceBuilder::new(
        ServiceConfig::new(dir.path(), "snapdef"),
        RegisterSm::default(),
    )
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
    assert_eq!(
        cnc.snapshots().service_snapshot_pos.load_acquire(),
        0,
        "never commanded: no marker"
    );
    assert_eq!(count_snapshots(dir.path()), 0, "never commanded: no file");

    client.shutdown();
    svc.stop();
    node.stop();
}
