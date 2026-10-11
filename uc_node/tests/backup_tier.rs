// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Backup-tier spec §6.2 — the report path, end to end on a real cluster.
//!
//! A backup writer (Task 8) hands its chain's watermark to the consensus
//! agent through `backup_claim`; these tests put that claim there directly
//! (`Node::offer_backup_claim_for_test`) and read the result where an
//! operator reads it: the committed `ClusterView::backups()` on EVERY node,
//! and the leader's `uc2_backup_reports_*` counters. Each asserts a rule:
//!
//! 1. a FOLLOWER's claim travels as one `BACKUP_REPORT` datagram to the
//!    leader, which appends one `BackupWatermark` that commits everywhere;
//! 2. reports for one tier coalesce to the furthest `through` — and a report
//!    no further than the committed entry appends nothing at all.
//!
//! The fixture is `admin_auth.rs`'s, trimmed (each integration test file is
//! its own binary): three voters over loopback UDP, no declared FSM (a
//! `CLUSTER` command commits on a cluster with no rows, as `settings apply`
//! does in `uc_ctl/tests/settings_apply_bin.rs`), instance dirs under
//! `CARGO_TARGET_TMPDIR`, whole-binary serialization.

use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::Ordering;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use uc_consensus::election::NodeId;
use uc_net::fault::FaultConfig;
use uc_node::{AdminPolicy, Node, NodeConfig, StartOpts};
use uc_protocol::v2::backup::{BackupTierEntry, BackupWatermark, WatermarkOp};

const APP: &str = "backuptier";

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn serialize() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

struct Cluster {
    _dir: tempfile::TempDir,
    nodes: Vec<Node>,
}

impl Cluster {
    fn stop(self) {
        for n in self.nodes {
            n.stop();
        }
    }
}

fn spawn_cluster(n: usize) -> Cluster {
    let dir = tempfile::Builder::new()
        .prefix("uc2-backup-tier-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let socks: Vec<UdpSocket> = (0..n)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind"))
        .collect();
    let members: Vec<(NodeId, SocketAddr)> = socks
        .iter()
        .enumerate()
        .map(|(i, s)| (i as NodeId, s.local_addr().unwrap()))
        .collect();
    let mut nodes = Vec::with_capacity(n);
    for (i, sock) in socks.into_iter().enumerate() {
        let cfg = NodeConfig {
            id: i as NodeId,
            members: members.clone(),
            learners: Vec::new(),
            bind: members[i].1,
            instance_dir: dir.path().join(format!("n{i}")),
            app_id: APP.into(),
            buffer_bytes: 1 << 22,
            max_payload: 256,
            admission_bytes_default: 256 * 1024,
            settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
            force_jumbo_frames: false,
            election_timeout_min_ns: 150_000_000,
            election_timeout_max_ns: 300_000_000,
            seed: 0xB4C_0000_0000_0001 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
            faults: FaultConfig::default(),
            purge: uc_node::PurgePolicy::Disabled,
            journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
            crypto: uc_node::CryptoConfig::Disabled,
            services: uc_node::ServicesConfig::none_for_tests(),
        };
        let opts = StartOpts {
            socket: Some(sock),
            admin: AdminPolicy::Filesystem,
        };
        nodes.push(Node::start_with(cfg, opts).expect("start"));
    }
    Cluster { _dir: dir, nodes }
}

fn await_single_leader(nodes: &[Node], secs: u64) -> usize {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let serving: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].can_serve()).collect();
        assert!(serving.len() <= 1, "split-brain: {serving:?} all serve");
        if serving.len() == 1 {
            return serving[0];
        }
        assert!(Instant::now() < deadline, "no single leader elected");
        std::thread::yield_now();
    }
}

fn await_until(secs: u64, msg: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "{msg}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn report(tier: u64, oldest: u64, newest: u64, through: u64, reporter: u64) -> BackupWatermark {
    BackupWatermark {
        op: WatermarkOp::Report,
        tier_id: tier,
        oldest_set: oldest,
        newest_set: newest,
        through,
        reporter,
    }
}

/// The one committed entry for `tier` on `node`, if any.
fn entry(node: &Node, tier: u64) -> Option<BackupTierEntry> {
    node.cluster_view()
        .backups()
        .into_iter()
        .find(|e| e.tier_id == tier)
}

/// Spec §6.2, the base case: a follower's writer claim is delivered to the
/// leader as ONE `BACKUP_REPORT` datagram (the follower's `sent` counter),
/// the leader appends ONE `BackupWatermark` (its `appended` counter), and
/// the committed entry — the claim's fields, `reporter` the follower — is
/// identical on all three nodes.
#[test]
fn a_follower_report_reaches_the_leader_and_commits_one_watermark() {
    let _g = serialize();
    let c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 20);
    let follower = (leader + 1) % 3;
    let tier = 0x7E57_0000_0000_0001;
    let w = report(tier, 4096, 8192, 12288, follower as u64);

    c.nodes[follower].offer_backup_claim_for_test(w);

    for (i, n) in c.nodes.iter().enumerate() {
        await_until(
            20,
            &format!("node {i} never committed the watermark"),
            || entry(n, tier).is_some(),
        );
        let e = entry(n, tier).unwrap();
        assert_eq!(
            (e.oldest_set, e.newest_set, e.through, e.reporter),
            (4096, 8192, 12288, follower as u64),
            "node {i}'s committed entry"
        );
        assert_eq!(n.cluster_view().backups().len(), 1, "node {i}: one tier");
    }
    let fobs = c.nodes[follower].observability();
    assert_eq!(fobs.backup_reports_sent.load(Ordering::Relaxed), 1);
    assert_eq!(fobs.backup_reports_appended.load(Ordering::Relaxed), 0);
    let lobs = c.nodes[leader].observability();
    assert_eq!(
        lobs.backup_reports_appended.load(Ordering::Relaxed),
        1,
        "the leader appends exactly one record for one report"
    );
    assert_eq!(lobs.backup_reports_sent.load(Ordering::Relaxed), 0);
    c.stop();
}

/// Spec §6.2: "the leader coalesces pending reports per tier (newest
/// `through` wins)". Two followers report the same tier; whatever order the
/// leader sees them in, the committed entry ends at the FURTHER one. Then a
/// report BEHIND the committed entry (a slower writer's older news) is
/// dropped at the leader before it is ever appended — `appended` does not
/// move and the committed entry does not change — while one AHEAD appends.
#[test]
fn two_claims_for_one_tier_coalesce_to_the_furthest() {
    let _g = serialize();
    let c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 20);
    let (fa, fb) = ((leader + 1) % 3, (leader + 2) % 3);
    let tier = 0x7E57_0000_0000_0002;

    c.nodes[fa].offer_backup_claim_for_test(report(tier, 64, 128, 4096, fa as u64));
    c.nodes[fb].offer_backup_claim_for_test(report(tier, 64, 256, 8192, fb as u64));
    for (i, n) in c.nodes.iter().enumerate() {
        await_until(20, &format!("node {i} never reached the furthest"), || {
            entry(n, tier).is_some_and(|e| e.through == 8192)
        });
        let e = entry(n, tier).unwrap();
        assert_eq!((e.newest_set, e.reporter), (256, fb as u64), "node {i}");
    }
    let lobs = c.nodes[leader].observability();
    let appended = lobs.backup_reports_appended.load(Ordering::Relaxed);
    assert!(
        (1..=2).contains(&appended),
        "two reports for one tier append at most two records, got {appended}"
    );

    // Behind the committed entry: never appended.
    c.nodes[fa].offer_backup_claim_for_test(report(tier, 64, 128, 4096, fa as u64));
    await_until(
        10,
        "the stale report never reached the follower's wire",
        || {
            c.nodes[fa]
                .observability()
                .backup_reports_sent
                .load(Ordering::Relaxed)
                >= 2
        },
    );
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        lobs.backup_reports_appended.load(Ordering::Relaxed),
        appended,
        "a report no further than the committed entry is dropped, not appended"
    );
    assert_eq!(entry(&c.nodes[leader], tier).unwrap().through, 8192);

    // Ahead of it: appended, and committed everywhere.
    c.nodes[fa].offer_backup_claim_for_test(report(tier, 128, 512, 16384, fa as u64));
    for (i, n) in c.nodes.iter().enumerate() {
        await_until(20, &format!("node {i} never took the newer report"), || {
            entry(n, tier).is_some_and(|e| e.through == 16384)
        });
    }
    assert_eq!(
        lobs.backup_reports_appended.load(Ordering::Relaxed),
        appended + 1
    );
    c.stop();
}

/// Spec §6.2: "A leader reports in-process." The leader's own claim moves
/// no wire counter and still commits.
#[test]
fn a_leaders_own_claim_commits_without_a_datagram() {
    let _g = serialize();
    let c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 20);
    let tier = 0x7E57_0000_0000_0003;
    c.nodes[leader].offer_backup_claim_for_test(report(tier, 0, 0, 512, leader as u64));
    for (i, n) in c.nodes.iter().enumerate() {
        await_until(20, &format!("node {i} never committed"), || {
            entry(n, tier).is_some()
        });
    }
    let lobs = c.nodes[leader].observability();
    assert_eq!(lobs.backup_reports_sent.load(Ordering::Relaxed), 0);
    assert_eq!(lobs.backup_reports_unsent.load(Ordering::Relaxed), 0);
    assert_eq!(lobs.backup_reports_appended.load(Ordering::Relaxed), 1);
    c.stop();
}
