// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M6 Task 4 — durable snapshot floor + policy-gated purge driver.
//!
//! These tests drive the NODE half in isolation: a real single node over
//! loopback, with the cnc `service_snapshot_pos` slot written DIRECTLY (standing
//! in for the service's snapshot builder, which is exercised end-to-end in
//! `uc_service`). That split is deliberate — `uc_node` cannot depend on
//! `uc_service` (it is the other way round), and Task 4's node code never reads
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

use uc_log::cnc::CncPage;
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig, PurgePolicy};
use uc_protocol::v2::frame::{HEADER_LEN, align_frame_len};

/// Tiny journal segments so a handful of KiB of frames rolls many segment files
/// and `purge_below` has non-active segments to drop.
const SEG_BYTES: u64 = 64 * 1024;

/// Payload of every `drive_and_quiesce` submit.
const PAYLOAD_BYTES: usize = 64;
/// One such submit as it lands in the log: header + payload rounded up to the
/// 32-byte frame slot (`uc_protocol::v2::frame`) — 96 bytes.
const FRAME_BYTES: u64 = align_frame_len(HEADER_LEN + PAYLOAD_BYTES) as u64;

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
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
        force_jumbo_frames: false,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: FaultConfig::default(),
        purge,
        learners: Vec::new(),
        journal_segment_bytes: SEG_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::none_for_tests(),
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

/// `uc2ctl snapshot`, in process (coordinated-snapshot spec §5.5): command a
/// coordinated instant and return its position **P**, polling through the
/// `retry` window a leader legitimately answers while it has the role but not
/// yet an appender. Duplicated per test binary, like `wait_until`.
fn command_instant(node: &Node) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(20);
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

/// Submit `n` 64-byte payloads (retrying through admission backpressure), then
/// wait until the log is quiescent (`append == commit == durable`, stable across
/// two reads) so `durable` is a settled byte position we can slice.
fn drive_and_quiesce(node: &Node, n: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    // Nightly flake 2026-08-23 + 2026-08-25 (`backup.rs`'s
    // `ordered_backup_never_produces_a_hole_under_purge_churn`: "purge advanced
    // the floor" stalled the FULL 120s deadline with zero progress, cycle 0,
    // one serialized cluster on an otherwise idle 4-vCPU runner — so not
    // starvation). `Node::submit` only enqueues into the ingress channel
    // (`INGRESS_CAPACITY` 8192 > n); the append happens later, on the
    // consensus agent. The old quiescence test — `append == commit == durable`,
    // unchanged across two back-to-back polls — is trivially true whenever
    // that agent simply has not run yet (the leader's 32-byte term-start frame
    // already satisfies `append > 0`), so on an oversubscribed box a whole
    // round of submits was declared quiescent with NONE of them appended. The
    // caller then read a tiny `durable`, published its snapshot at pos 1, and
    // `Archive::purge_below(1)` is a permanent no-op (block 0 is both the first
    // and the covering block) — a state no deadline can wait out. Reproduced
    // deterministically under `taskset -c 0,1` (2 of 6 runs). Quiescence now
    // also requires this round's bytes to have landed: `append` must have
    // advanced by at least `sent` frames of `PAYLOAD_BYTES`.
    let append0 = node.counters().append.load_acquire();
    let mut sent = 0usize;
    while sent < n {
        assert!(Instant::now() < deadline, "submits stalled at {sent}/{n}");
        match node.submit(vec![0xAB; PAYLOAD_BYTES]) {
            Ok(()) => sent += 1,
            Err(_) => std::thread::yield_now(), // admission window closed / draining
        }
    }
    let expected_append = append0 + sent as u64 * FRAME_BYTES;
    let mut last = u64::MAX;
    wait_until("log quiescent", || {
        let c = node.counters();
        let (a, cm, d) = (
            c.append.load_acquire(),
            c.commit.load_acquire(),
            c.durable.load_acquire(),
        );
        let quiescent = a >= expected_append && a == cm && cm == d && a == last;
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

    let node = Node::start(config(
        dir,
        app,
        PurgePolicy::BelowSnapshot { slack_bytes: 0 },
    ))
    .unwrap();
    wait_until("can_serve", || node.can_serve());
    drive_and_quiesce(&node, 6000);

    let cnc = open_cnc(dir, app);

    // No instant commanded yet -> no complete SET, so the floor is 0 and purge
    // never fires, no matter how much log exists (coordinated-snapshot spec
    // §5.3: purge is gated on the set, and a cluster nobody asks to snapshot
    // is legitimate — purge is off by default).
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        node.archive_first_base(),
        0,
        "purge must stay gated on a complete set"
    );
    assert_eq!(
        cnc.snapshots().node_snapshot_floor.load_acquire(),
        0,
        "no floor persisted without a set"
    );

    // Command one (spec §5.5). This node declares NO services, so the set at
    // P is the `uc2-cluster` agent's artifact alone; P is well into the
    // journal (6000 submits above), so purging below it drops whole segments.
    let s = command_instant(&node);
    assert!(
        s > SEG_BYTES,
        "test setup: need >1 segment below the instant"
    );

    // The node validates (`<= durable`), durably persists the floor, mirrors it,
    // and commands the purge.
    wait_until("purge advanced the floor", || node.archive_first_base() > 0);
    assert!(
        node.archive_first_base() <= s,
        "never purged at/above the snapshot floor"
    );
    assert_eq!(
        cnc.snapshots().node_snapshot_floor.load_acquire(),
        s,
        "the mirrored floor equals the validated position"
    );

    // Restart: the durable floor is a high-water mark. A fresh cnc page starts at
    // 0, but boot re-seeds the mirror + the persister's shadow from the durable
    // value, so the floor never regresses (the marker-clobber lesson).
    node.stop();
    let node = Node::start(config(
        dir,
        app,
        PurgePolicy::BelowSnapshot { slack_bytes: 0 },
    ))
    .unwrap();
    wait_until("can_serve after restart", || node.can_serve());
    let cnc2 = open_cnc(dir, app);
    assert_eq!(
        cnc2.snapshots().node_snapshot_floor.load_acquire(),
        s,
        "durable snapshot floor recovered onto the fresh page, not regressed to 0"
    );
    node.stop();
}

/// Coordinated-snapshot spec §5.3: the page-1 `service_snapshot_pos` word is
/// **observability only** — whatever it says, it never becomes the purge
/// floor. Before this work it WAS the floor input, and a torn or racy value
/// above the durable frontier was refused by a `<= durable` belt; now the only
/// thing that moves the floor is a complete SET (every declared row's artifact
/// at P plus the cluster FSM's), which is why this test writes an absurd value
/// and asserts nothing at all happens.
///
/// The `<= durable` belt still guards the set's own position
/// (`maybe_persist_snapshot_floor`) and is pinned in `node.rs`'s unit tests,
/// where a set position can be constructed directly.
#[test]
fn a_poked_service_snapshot_pos_never_becomes_the_floor() {
    let root = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let dir = root.path();
    let app = "purge2";

    let node = Node::start(config(
        dir,
        app,
        PurgePolicy::BelowSnapshot { slack_bytes: 0 },
    ))
    .unwrap();
    wait_until("can_serve", || node.can_serve());
    drive_and_quiesce(&node, 2000);

    let cnc = open_cnc(dir, app);
    let durable = cnc.counters().durable.load_acquire();
    // Publish a position far beyond durable — an insane/torn value.
    cnc.snapshots()
        .service_snapshot_pos
        .store_release(durable + 1_000_000);

    // Give the consensus loop many duty cycles to (not) act on it.
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        cnc.snapshots().node_snapshot_floor.load_acquire(),
        0,
        "the page-1 word is not a floor input — never persisted, never mirrored"
    );
    assert_eq!(node.archive_first_base(), 0, "and nothing is purged");
    node.stop();
}
