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
            backup: None,
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

/// Every node's current term. The tests below make their EXACT counter
/// claims (who sent, who appended, how many records) only over a span in
/// which no term moved: a loaded box can stall the whole process past an
/// election timeout, and an election legitimately moves the claim to another
/// node and can drop a leader's pending reports (ruling R8e's note). Across
/// an election they still assert every committed outcome.
fn terms(c: &Cluster) -> Vec<u32> {
    c.nodes.iter().map(|n| n.current_term()).collect()
}

/// `backup_reports_appended` summed over every node: a record appended by
/// any leader of the span, which survives an election.
fn appended_total(c: &Cluster) -> u64 {
    c.nodes
        .iter()
        .map(|n| {
            n.observability()
                .backup_reports_appended
                .load(Ordering::Relaxed)
        })
        .sum()
}

/// Offer each `(node, claim)` and wait until `done` holds, offering them
/// again every 500 ms — what the real writer does with an unchanged claim
/// (`REOFFER_NS`, every 10 s), because one delivery can be lost for good: a
/// follower still naming a deposed leader sends to a node that drops it, and
/// a leader that steps down drops its pending reports. An offer the leader
/// already holds or has committed appends nothing more (single-in-flight,
/// then the R8e advance filter), so re-offering never inflates a count.
fn offer_until(
    c: &Cluster,
    claims: &[(usize, BackupWatermark)],
    secs: u64,
    msg: &str,
    mut done: impl FnMut() -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        for &(i, w) in claims {
            c.nodes[i].offer_backup_claim_for_test(w);
        }
        let next = Instant::now() + Duration::from_millis(500);
        while Instant::now() < next {
            if done() {
                return;
            }
            assert!(Instant::now() < deadline, "{msg}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

/// Spec §6.2, the base case: a follower's writer claim is delivered to the
/// leader as a `BACKUP_REPORT` datagram (the follower's `sent` counter — a
/// re-offer sends again and appends nothing), the leader appends ONE
/// `BackupWatermark` (its `appended` counter), and
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
    let t0 = terms(&c);

    offer_until(
        &c,
        &[(follower, w)],
        20,
        "the watermark never committed everywhere",
        || c.nodes.iter().all(|n| entry(n, tier).is_some()),
    );

    for (i, n) in c.nodes.iter().enumerate() {
        let e = entry(n, tier).unwrap();
        assert_eq!(
            (e.oldest_set, e.newest_set, e.through, e.reporter),
            (4096, 8192, 12288, follower as u64),
            "node {i}'s committed entry"
        );
        assert_eq!(n.cluster_view().backups().len(), 1, "node {i}: one tier");
    }
    if terms(&c) != t0 {
        eprintln!("an election moved leadership mid-test; exact counters not judged");
        c.stop();
        return;
    }
    let fobs = c.nodes[follower].observability();
    assert!(fobs.backup_reports_sent.load(Ordering::Relaxed) >= 1);
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
/// dropped at the leader before it is ever appended — no node's `appended`
/// moves and the committed entry does not change — while one AHEAD appends.
#[test]
fn two_claims_for_one_tier_coalesce_to_the_furthest() {
    let _g = serialize();
    let c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 20);
    let (fa, fb) = ((leader + 1) % 3, (leader + 2) % 3);
    let tier = 0x7E57_0000_0000_0002;
    let t0 = terms(&c);

    offer_until(
        &c,
        &[
            (fa, report(tier, 64, 128, 4096, fa as u64)),
            (fb, report(tier, 64, 256, 8192, fb as u64)),
        ],
        20,
        "not every node reached the furthest",
        || {
            c.nodes
                .iter()
                .all(|n| entry(n, tier).is_some_and(|e| e.through == 8192))
        },
    );
    for (i, n) in c.nodes.iter().enumerate() {
        let e = entry(n, tier).unwrap();
        assert_eq!((e.newest_set, e.reporter), (256, fb as u64), "node {i}");
    }
    let appended = appended_total(&c);
    if terms(&c) == t0 {
        assert!(
            (1..=2).contains(&appended),
            "two reports for one tier append at most two records, got {appended}"
        );
    }

    // Behind the committed entry: never appended, by any leader. Offered
    // the same way (an election can lose one delivery); whichever node
    // leads drops it against the committed entry.
    let sent_before = c.nodes[fa]
        .observability()
        .backup_reports_sent
        .load(Ordering::Relaxed);
    let t1 = terms(&c);
    let stale = report(tier, 64, 128, 4096, fa as u64);
    c.nodes[fa].offer_backup_claim_for_test(stale);
    await_until(10, "the stale report never left the follower", || {
        c.nodes[fa]
            .observability()
            .backup_reports_sent
            .load(Ordering::Relaxed)
            > sent_before
            || terms(&c) != t1
    });
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        appended_total(&c),
        appended,
        "a report no further than the committed entry is dropped, not appended"
    );
    for n in &c.nodes {
        assert_eq!(entry(n, tier).unwrap().through, 8192);
    }

    // Ahead of it: appended, and committed everywhere.
    let t2 = terms(&c);
    offer_until(
        &c,
        &[(fa, report(tier, 128, 512, 16384, fa as u64))],
        20,
        "not every node took the newer report",
        || {
            c.nodes
                .iter()
                .all(|n| entry(n, tier).is_some_and(|e| e.through == 16384))
        },
    );
    if terms(&c) == t2 {
        assert_eq!(appended_total(&c), appended + 1);
    } else {
        assert!(appended_total(&c) > appended);
    }
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
    let t0 = terms(&c);
    offer_until(
        &c,
        &[(leader, report(tier, 0, 0, 512, leader as u64))],
        20,
        "not every node committed",
        || c.nodes.iter().all(|n| entry(n, tier).is_some()),
    );
    if terms(&c) != t0 {
        eprintln!("an election moved leadership mid-test; exact counters not judged");
        c.stop();
        return;
    }
    let lobs = c.nodes[leader].observability();
    assert_eq!(lobs.backup_reports_sent.load(Ordering::Relaxed), 0);
    assert_eq!(lobs.backup_reports_unsent.load(Ordering::Relaxed), 0);
    assert_eq!(lobs.backup_reports_appended.load(Ordering::Relaxed), 1);
    c.stop();
}

/// Backup-tier spec §5 / §11.2 — the `uc2-backup` writer, end to end: real
/// nodes with real snapshot-capable services, real coordinated instants,
/// and a real `FsTarget` under `CARGO_TARGET_TMPDIR` (ruling PF4). The
/// fixture is `catalog.rs`'s, trimmed, plus a per-node `[backup]` config
/// handed in through `StartOpts::backup`.
mod writer_e2e {
    use std::collections::BTreeMap;
    use std::net::{SocketAddr, UdpSocket};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    use uc_consensus::election::NodeId;
    use uc_log::cnc::CncPage;
    use uc_net::fault::FaultConfig;
    use uc_node::tier::chain::{list_tier, reach_from, verify, watermark_of};
    use uc_node::tier::layout::chunk_object;
    use uc_node::tier::materialize::{Mode, materialize};
    use uc_node::tier::object::{Codec, ObjectReader};
    use uc_node::tier::target::{BackupTarget, FsTarget, open_or_create_tier};
    use uc_node::tier::writer::{BackupConfig, WriterError};
    use uc_node::{
        AdminPolicy, Node, NodeConfig, PurgePolicy, ServicesConfig, Settings, StartOpts,
    };
    use uc_protocol::v2::backup::BackupTierEntry;
    use uc_protocol::v2::cnc::CNC_SVC_STATUS_SNAPSHOT_CAPABLE;
    use uc_protocol::v2::frame::{FRAME_TYPE_PADDING, HEADER_LEN, align_frame_len, read_header};

    use super::serialize;

    const PAYLOAD: usize = 96;
    const FRAME_BYTES: u64 = 32 + PAYLOAD as u64;
    const SEG: u64 = 64 * 1024;

    fn await_until(secs: u64, msg: &str, mut f: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(secs);
        while !f() {
            assert!(Instant::now() < deadline, "{msg}");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    // ------------------------------------------------------------ fixture

    struct NodeH {
        addr: SocketAddr,
        dir: PathBuf,
        is_learner: bool,
        cfg: Box<dyn Fn() -> NodeConfig>,
        backup: Option<BackupConfig>,
        sock: Option<UdpSocket>,
        node: Option<Node>,
        svc: Option<Box<dyn FnOnce()>>,
    }

    impl NodeH {
        fn start(&mut self) {
            assert!(self.node.is_none(), "start of a live node");
            let sock = self.sock.take().unwrap_or_else(|| rebind(self.addr));
            let opts = StartOpts {
                socket: Some(sock),
                admin: AdminPolicy::Filesystem,
                backup: self.backup.clone(),
            };
            self.node = Some(Node::start_with((self.cfg)(), opts).expect("start node"));
        }
        fn start_service(&mut self, app: &str, i: usize) {
            let s = uc_service::ServiceBuilder::new(svc_cfg(&self.dir, app), SumSm::on(i))
                .start()
                .expect("service start");
            self.svc = Some(Box::new(move || s.stop()));
        }
        /// Service first, then node.
        fn stop(&mut self) {
            if let Some(s) = self.svc.take() {
                s();
            }
            if let Some(n) = self.node.take() {
                n.stop();
            }
        }
    }

    fn rebind(addr: SocketAddr) -> UdpSocket {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match UdpSocket::bind(addr) {
                Ok(s) => return s,
                Err(_) if Instant::now() < deadline => std::thread::yield_now(),
                Err(e) => panic!("rebind {addr} failed: {e}"),
            }
        }
    }

    #[derive(Clone, Copy)]
    struct Opts {
        app: &'static str,
        purge: bool,
        /// Genesis `Settings::backup_retain_sets` (0 = keep everything).
        retain_sets: u16,
    }

    fn opts(app: &'static str, purge: bool) -> Opts {
        Opts {
            app,
            purge,
            retain_sets: 0,
        }
    }

    struct Cluster {
        _dir: tempfile::TempDir,
        app: &'static str,
        nodes: Vec<NodeH>,
    }

    impl Cluster {
        fn node(&self, i: usize) -> &Node {
            self.nodes[i].node.as_ref().expect("node stopped")
        }
        fn running(&self) -> Vec<usize> {
            (0..self.nodes.len())
                .filter(|&i| self.nodes[i].node.is_some())
                .collect()
        }
        fn voters(&self) -> Vec<usize> {
            (0..self.nodes.len())
                .filter(|&i| !self.nodes[i].is_learner)
                .collect()
        }
        fn cnc(&self, i: usize) -> Arc<CncPage> {
            CncPage::open_file(&self.nodes[i].dir.join("cnc2.dat"), self.app).expect("open cnc")
        }
        fn dirs(&self) -> Vec<PathBuf> {
            self.nodes.iter().map(|h| h.dir.clone()).collect()
        }
        /// Every service, then every node; the instance dirs outlive the
        /// call (the returned guard), so a test can read a stopped journal.
        fn stop(mut self) -> tempfile::TempDir {
            for h in self.nodes.iter_mut() {
                if let Some(s) = h.svc.take() {
                    s();
                }
            }
            for h in self.nodes.iter_mut() {
                h.stop();
            }
            self._dir
        }
    }

    /// `n_voters` voters then `n_learners` learners, every node in the
    /// genesis membership; node `i` runs a writer iff `writer(i)` is `Some`.
    fn spawn(
        n_voters: usize,
        n_learners: usize,
        o: Opts,
        writer: impl Fn(usize) -> Option<BackupConfig>,
    ) -> Cluster {
        let dir = tempfile::Builder::new()
            .prefix("uc2-backup-writer-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .expect("tempdir");
        let total = n_voters + n_learners;
        let socks: Vec<UdpSocket> = (0..total)
            .map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind"))
            .collect();
        let all: Vec<(NodeId, SocketAddr)> = socks
            .iter()
            .enumerate()
            .map(|(i, s)| (i as NodeId, s.local_addr().unwrap()))
            .collect();
        let members = all[..n_voters].to_vec();
        let learners = all[n_voters..].to_vec();
        let mut nodes = Vec::with_capacity(total);
        for (i, sock) in socks.into_iter().enumerate() {
            let addr = all[i].1;
            let node_dir = dir.path().join(format!("n{i}"));
            let (m, l, d) = (members.clone(), learners.clone(), node_dir.clone());
            let cfg: Box<dyn Fn() -> NodeConfig> = Box::new(move || NodeConfig {
                id: i as NodeId,
                members: m.clone(),
                learners: l.clone(),
                bind: addr,
                instance_dir: d.clone(),
                app_id: o.app.into(),
                buffer_bytes: if o.purge { 1 << 18 } else { 1 << 22 },
                max_payload: 256,
                admission_bytes_default: 256 * 1024,
                settings_genesis: Settings {
                    backup_retain_sets: o.retain_sets,
                    ..Settings::genesis_default()
                },
                force_jumbo_frames: false,
                election_timeout_min_ns: 150_000_000,
                election_timeout_max_ns: 300_000_000,
                seed: 0xB4C_0000_0000_0008 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
                faults: FaultConfig::default(),
                purge: if o.purge {
                    PurgePolicy::BelowSnapshot { slack_bytes: 0 }
                } else {
                    PurgePolicy::Disabled
                },
                journal_segment_bytes: if o.purge {
                    SEG
                } else {
                    uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES
                },
                crypto: uc_node::CryptoConfig::Disabled,
                services: ServicesConfig::single(<SumSm as uc_service::RawStateMachine>::NAME),
            });
            let mut h = NodeH {
                addr,
                dir: node_dir,
                is_learner: i >= n_voters,
                cfg,
                backup: writer(i),
                sock: Some(sock),
                node: None,
                svc: None,
            };
            h.start();
            nodes.push(h);
        }
        Cluster {
            _dir: dir,
            app: o.app,
            nodes,
        }
    }

    fn start_sums(c: &mut Cluster) {
        let app = c.app;
        for i in c.running() {
            c.nodes[i].start_service(app, i);
        }
    }

    fn await_single_leader(c: &Cluster, secs: u64) -> usize {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            let serving: Vec<usize> = c
                .running()
                .into_iter()
                .filter(|&i| c.node(i).can_serve())
                .collect();
            if let [i] = serving[..] {
                return i;
            }
            assert!(Instant::now() < deadline, "no single leader elected");
            std::thread::yield_now();
        }
    }

    fn await_capable(c: &Cluster) {
        let pages: Vec<Arc<CncPage>> = c.running().into_iter().map(|i| c.cnc(i)).collect();
        await_until(
            30,
            "every row published the snapshot-capability bit",
            || {
                pages.iter().all(|p| {
                    p.service_slot(0).status.load_acquire() & CNC_SVC_STATUS_SNAPSHOT_CAPABLE != 0
                })
            },
        );
    }

    /// Submit `n` frames on `node` WITHOUT waiting for commit; returns how
    /// many the node took. Stops at the first `NotServing`, or once the
    /// ingress ring has stayed full for 2 s (an isolated leader's admission
    /// window fills and never drains).
    fn submit_some(node: &Node, n: u64, tag: u64) -> u64 {
        for i in 0..n {
            let mut p = vec![0u8; PAYLOAD];
            p[..8].copy_from_slice(&(tag + i).to_le_bytes());
            let mut full_since: Option<Instant> = None;
            loop {
                match node.submit(p.clone()) {
                    Ok(()) => break,
                    Err(uc_node::SubmitError::NotServing) => return i,
                    Err(uc_node::SubmitError::Full) => {
                        let since = *full_since.get_or_insert_with(Instant::now);
                        if since.elapsed() >= Duration::from_secs(2) {
                            return i;
                        }
                        std::thread::yield_now();
                    }
                }
            }
        }
        n
    }

    /// Submit `n` frames to whichever running node serves, following
    /// leadership while it settles, and wait for them to commit; returns the
    /// node that took the last of them.
    fn submit_following(c: &Cluster, n: u64) -> usize {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut left = n;
        let mut at = 0;
        while left > 0 {
            assert!(Instant::now() < deadline, "no leader took the writes");
            let Some(i) = c.running().into_iter().find(|&i| c.node(i).can_serve()) else {
                std::thread::yield_now();
                continue;
            };
            at = i;
            left -= submit_some(c.node(i), left, 3_000_000 + left);
        }
        let target = c.node(at).counters().append.load_acquire();
        await_until(60, "submitted frames committed", || {
            c.node(at).counters().commit.load_acquire() >= target
        });
        at
    }

    /// [`submit_following`] for a test that runs long enough to see an
    /// election under a loaded box: the same leader-following submit, then
    /// [`await_settled`] instead of waiting on the node that took the last
    /// frame — if that node is deposed, its uncommitted tail is truncated
    /// and never commits. Frames lost that way are simply lost; the caller
    /// counts no frames, only positions the cluster then commits. Returns
    /// the settled leader.
    ///
    /// Issue #99: a "no leader took the writes" / "no leader committed its
    /// own appends within 60 s" / "no instant completed" failure under load
    /// was the post-election commit stall, now fixed: the log end sat more
    /// than `fsm_lag` past `applied` at the election, so every follower's
    /// report ceiling (`applied + fsm_lag`) sat below the new leader's NewTerm
    /// frame and nothing ever committed again. The ceiling now never sits
    /// below the current term's opening frame; a recurrence means that fix
    /// regressed (`uc_node/tests/post_election_stall.rs` pins it).
    fn submit_settled(c: &Cluster, n: u64) -> usize {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut left = n;
        // `Node::submit` only ENQUEUES; the leader appends later. So the last
        // batch is known to be in the log only once that node's `append` has
        // grown by at least `accepted * FRAME_BYTES` from where it stood
        // before the batch (other frames — cluster, timers — only add).
        // Without this floor CI read commit with most of a batch still in
        // the ingress queue (#100's restore and fence tests).
        let mut floor: Option<(usize, u64)> = None;
        while left > 0 {
            assert!(
                Instant::now() < deadline,
                "no leader took the writes (if under load, see #99)"
            );
            let Some(i) = c.running().into_iter().find(|&i| c.node(i).can_serve()) else {
                std::thread::yield_now();
                continue;
            };
            let before = c.node(i).counters().append.load_acquire();
            let took = submit_some(c.node(i), left, 3_000_000 + left);
            left -= took;
            if took > 0 {
                floor = Some((i, before + took * FRAME_BYTES));
            }
        }
        await_settled_from(c, floor)
    }

    /// Wait until whichever running node leads has committed everything it
    /// appended, following an election that lands meanwhile (the deposed
    /// leader's uncommitted tail is truncated and never commits, so waiting
    /// on ITS append position would wait forever). Returns that leader.
    ///
    /// Issue #99: a "no leader took the writes" / "no leader committed its
    /// own appends within 60 s" / "no instant completed" failure under load
    /// was the post-election commit stall, now fixed: the log end sat more
    /// than `fsm_lag` past `applied` at the election, so every follower's
    /// report ceiling (`applied + fsm_lag`) sat below the new leader's NewTerm
    /// frame and nothing ever committed again. The ceiling now never sits
    /// below the current term's opening frame; a recurrence means that fix
    /// regressed (`uc_node/tests/post_election_stall.rs` pins it).
    ///
    /// `floor`: when the serving node is the one that took the last batch,
    /// wait until its `append` has reached the batch's end before taking the
    /// commit target. A different leader means that batch's unreplicated tail
    /// was truncated, so the floor no longer applies and the leader's own
    /// `append` is the target.
    fn await_settled_from(c: &Cluster, floor: Option<(usize, u64)>) -> usize {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            assert!(
                Instant::now() < deadline,
                "no leader committed its own appends within 60 s (if under load, see #99)"
            );
            let Some(l) = c.running().into_iter().find(|&i| c.node(i).can_serve()) else {
                std::thread::yield_now();
                continue;
            };
            let need = floor.and_then(|(i, f)| (i == l).then_some(f));
            let mut target: Option<u64> = None;
            while c.node(l).can_serve() && Instant::now() < deadline {
                let t = match target {
                    Some(t) => t,
                    None => {
                        let append = c.node(l).counters().append.load_acquire();
                        if need.is_some_and(|f| append < f) {
                            std::thread::sleep(Duration::from_millis(2));
                            continue; // the batch is still in the ingress queue
                        }
                        *target.insert(append)
                    }
                };
                if c.node(l).counters().commit.load_acquire() >= t {
                    return l;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        }
    }

    /// Submit `n` frames on the leader and wait until they committed.
    fn submit_frames(node: &Node, n: u64) {
        let before = node.counters().append.load_acquire();
        assert_eq!(submit_some(node, n, 0), n, "leadership moved mid-submit");
        let target = before + n * FRAME_BYTES;
        await_until(60, "submitted frames committed", || {
            node.counters().commit.load_acquire() >= target
        });
    }

    /// Command an instant and wait until every node in `idxs` holds its
    /// complete set; returns its position. `leader` is a hint: an election
    /// that lands meanwhile moves the command to whichever node now serves
    /// (a deposed leader answers `Retry` for good).
    ///
    /// Issue #99: a "no leader took the writes" / "no leader committed its
    /// own appends within 60 s" / "no instant completed" failure under load
    /// was the post-election commit stall, now fixed: the log end sat more
    /// than `fsm_lag` past `applied` at the election, so every follower's
    /// report ceiling (`applied + fsm_lag`) sat below the new leader's NewTerm
    /// frame and nothing ever committed again. The ceiling now never sits
    /// below the current term's opening frame; a recurrence means that fix
    /// regressed (`uc_node/tests/post_election_stall.rs` pins it).
    fn instant_until_complete(c: &Cluster, leader: usize, idxs: &[usize]) -> u64 {
        let mut leader = leader;
        for _ in 0..5 {
            let deadline = Instant::now() + Duration::from_secs(60);
            let p = loop {
                match c.node(leader).command_snapshot(false) {
                    Ok(p) => break p,
                    Err(uc_node::SnapshotRefusal::Retry) => {
                        assert!(Instant::now() < deadline, "no leader took the instant");
                        if !c.node(leader).can_serve()
                            && let Some(i) =
                                c.running().into_iter().find(|&i| c.node(i).can_serve())
                        {
                            leader = i;
                        }
                        std::thread::sleep(Duration::from_millis(5))
                    }
                    Err(e) => panic!("uc2ctl snapshot refused: {e}"),
                }
            };
            let deadline = Instant::now() + Duration::from_secs(30);
            while Instant::now() < deadline {
                if idxs.iter().all(|&i| c.node(i).snapshot_set_position() >= p) {
                    return p;
                }
                std::thread::yield_now();
            }
        }
        panic!("no instant completed (if under load, see #99)");
    }

    fn await_agreed(c: &Cluster, idxs: &[usize], p: u64) {
        await_until(60, "every node agreed the set", || {
            idxs.iter().all(|&i| {
                c.node(i)
                    .cluster_view()
                    .snapshot_inner()
                    .catalog
                    .iter()
                    .any(|e| e.position == p && e.is_agreed())
            })
        });
    }

    /// The committed watermark of the (one) registered tier, on `node`.
    fn watermark(node: &Node) -> Option<BackupTierEntry> {
        node.cluster_view().backups().into_iter().next()
    }

    fn await_watermark(
        c: &Cluster,
        at: usize,
        secs: u64,
        mut f: impl FnMut(&BackupTierEntry) -> bool,
    ) -> BackupTierEntry {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            if let Some(w) = watermark(c.node(at))
                && f(&w)
            {
                return w;
            }
            assert!(
                Instant::now() < deadline,
                "watermark never satisfied; last = {:?}",
                watermark(c.node(at))
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn tmp_tier() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("uc2-tier-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .expect("tier tempdir")
    }

    fn writer_cfg(tier: &Path, chunk_bytes: u64, quiet: Duration) -> BackupConfig {
        BackupConfig {
            target: tier.to_path_buf(),
            chunk_bytes,
            quiet_interval: quiet,
            compression: Codec::Zstd,
            compression_level: 3,
            ..BackupConfig::default()
        }
    }

    // --------------------------------------------------------- the service

    struct SumSm {
        total: u64,
        last: Option<u64>,
    }

    impl SumSm {
        fn on(_node: usize) -> SumSm {
            SumSm {
                total: 0,
                last: None,
            }
        }
    }

    impl uc_service::RawStateMachine for SumSm {
        const NAME: &'static str = "sum";
        fn apply(&mut self, ctx: &mut uc_service::ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
            if cmd.len() >= 8 {
                self.total = self
                    .total
                    .wrapping_add(u64::from_le_bytes(cmd[..8].try_into().unwrap()));
            }
            self.last = Some(ctx.position);
            out.extend_from_slice(&self.total.to_le_bytes());
        }
        fn query(&self, _q: &[u8], out: &mut Vec<u8>) {
            out.extend_from_slice(&self.total.to_le_bytes());
        }
        fn last_applied(&self) -> Option<u64> {
            self.last
        }
    }

    impl uc_service::SnapshotStateMachine for SumSm {
        type SnapshotHandle = Vec<u8>;
        fn freeze(&self) -> Result<(Vec<u8>, u64), uc_service::SnapshotError> {
            let pos = self.last.unwrap_or(0);
            let mut buf = Vec::with_capacity(16);
            buf.extend_from_slice(&self.total.to_le_bytes());
            buf.extend_from_slice(&pos.to_le_bytes());
            Ok((buf, pos))
        }
        fn stream_snapshot(
            handle: Vec<u8>,
            dst: &mut dyn std::io::Write,
        ) -> Result<(), uc_service::SnapshotError> {
            dst.write_all(&handle)?;
            Ok(())
        }
        fn install_snapshot(
            &mut self,
            position: u64,
            src: &mut dyn std::io::Read,
        ) -> Result<u64, uc_service::SnapshotError> {
            let mut buf = Vec::new();
            src.read_to_end(&mut buf)?;
            self.total = u64::from_le_bytes(buf[..8].try_into().unwrap());
            self.last = Some(u64::from_le_bytes(buf[8..16].try_into().unwrap()));
            Ok(position)
        }
    }

    fn svc_cfg(dir: &Path, app: &str) -> uc_service::ServiceConfig {
        let mut cfg = uc_service::ServiceConfig::new(dir, app);
        cfg.boot_wait = Duration::from_secs(60);
        cfg
    }

    /// A linearizable read of the SumSm total through node `i`. SumSm
    /// answers its raw 8-byte LE total; bincode decodes `[u8; 8]` as exactly
    /// those 8 bytes.
    fn query_total(c: &Cluster, i: usize) -> u64 {
        let client = uc_client::Client::connect(&c.nodes[i].dir, c.app).expect("client");
        let raw: [u8; 8] = client.query_linearizable(&()).expect("linearizable query");
        client.shutdown();
        u64::from_le_bytes(raw)
    }

    // ------------------------------------------------- reading the tier back

    /// One frame read out of a tier chunk: `(position, term, type, payload)`.
    type ChunkFrame = (u64, u32, u8, Vec<u8>);

    /// Every frame of every chunk in the tier, walked with the frame header
    /// (the restore path's view). Padding bodies must be zero (the writer's
    /// canonical form).
    fn chunk_frames(t: &FsTarget) -> Vec<ChunkFrame> {
        let l = list_tier(t).unwrap();
        let mut out = Vec::new();
        for &(from, to) in &l.chunks {
            let f = t.open(&chunk_object(from, to)).unwrap().expect("chunk");
            let mut r = ObjectReader::open(f).unwrap();
            assert_eq!((r.header().from, r.header().to), (from, to));
            let mut raw = Vec::new();
            std::io::Read::read_to_end(&mut r, &mut raw).unwrap();
            assert_eq!(raw.len() as u64, to - from, "chunk ({from},{to}) length");
            let mut off = 0usize;
            while off < raw.len() {
                let h = read_header(&raw[off..]);
                assert!(h.length as usize >= HEADER_LEN, "short frame in chunk");
                let aligned = align_frame_len(h.length as usize);
                assert!(off + aligned <= raw.len(), "frame runs past its chunk");
                if h.frame_type == FRAME_TYPE_PADDING {
                    assert!(
                        raw[off + HEADER_LEN..off + aligned].iter().all(|&b| b == 0),
                        "a padding body in the tier is zero"
                    );
                } else {
                    out.push((
                        from + off as u64,
                        h.leadership_term_id,
                        h.frame_type,
                        raw[off + HEADER_LEN..off + h.length as usize].to_vec(),
                    ));
                }
                off += aligned;
            }
        }
        out
    }

    /// Every non-padding frame of the (stopped) node's journal, by position.
    fn journal_frames(dir: &Path) -> BTreeMap<u64, (u32, u8, Vec<u8>)> {
        let j = uc_journal::Journal::open(uc_journal::JournalConfig::new(dir.join("journal")))
            .expect("open journal");
        let first = match j.first_seq() {
            Some(s) => j.read(s).unwrap().unwrap().0,
            None => return BTreeMap::new(),
        };
        let mut r = uc_log::archive::replay_journal_from(&j, first)
            .unwrap()
            .expect("replay");
        let mut out = BTreeMap::new();
        while let Some(f) = r.next().unwrap() {
            out.insert(
                f.position,
                (f.header.leadership_term_id, f.header.frame_type, f.payload),
            );
        }
        out
    }

    fn assert_chunks_match(t: &FsTarget, survivor: &BTreeMap<u64, (u32, u8, Vec<u8>)>) -> usize {
        let frames = chunk_frames(t);
        for (pos, term, ty, payload) in &frames {
            let Some((sterm, sty, spayload)) = survivor.get(pos) else {
                // Below the survivor's purge floor: nothing to compare.
                assert!(
                    survivor.keys().next().is_some_and(|&f| *pos < f),
                    "chunk frame at {pos} has no frame at that position in the surviving log"
                );
                continue;
            };
            assert_eq!(
                (term, ty, payload),
                (sterm, sty, spayload),
                "chunk frame at {pos} disagrees with the surviving log"
            );
        }
        frames.len()
    }

    // ================================================================ tests

    /// Spec §5.2/§5.3/§5.5: a learner's writer copies the agreed set and the
    /// committed journal after it, and its report commits as the tier's
    /// watermark — never beyond `min(commit, durable)`. Every chunk frame
    /// equals the writer's own log, and `tier verify --deep` is clean.
    #[test]
    fn a_writer_copies_the_agreed_set_and_committed_journal_and_reports_it() {
        let _g = serialize();
        let tier = tmp_tier();
        let cfg = writer_cfg(tier.path(), 1 << 20, Duration::from_millis(500));
        let mut c = spawn(3, 1, opts("tier-w1", false), |i| {
            (i == 3).then(|| cfg.clone())
        });
        start_sums(&mut c);
        let l = await_single_leader(&c, 30);
        await_capable(&c);
        submit_settled(&c, 2000);
        let all = c.running();
        let p = instant_until_complete(&c, l, &all);
        await_agreed(&c, &all, p);
        let lead = submit_settled(&c, 20_000);
        let target = c.node(lead).counters().commit.load_acquire();
        let w = await_watermark(&c, l, 60, |w| w.newest_set == p && w.through >= target);
        let wn = c.node(3);
        let ceiling = wn
            .counters()
            .commit
            .load_acquire()
            .min(wn.counters().durable.load_acquire());
        assert!(
            w.through <= ceiling,
            "through {} above the writer's ceiling {ceiling}",
            w.through
        );
        assert_eq!(w.oldest_set, p);
        assert_eq!(w.reporter, 3, "reporter is the writer node's own id");

        let t = FsTarget::new(tier.path());
        let listing = list_tier(&t).unwrap();
        assert!(listing.sets.iter().any(|s| s.position == p), "{listing:?}");
        // 3b: the SET records the committed lineage strictly below the set.
        let mut text = String::new();
        std::io::Read::read_to_string(
            &mut t
                .open(&uc_node::tier::layout::set_label(p))
                .unwrap()
                .expect("SET present"),
            &mut text,
        )
        .unwrap();
        let label = uc_node::tier::layout::SetLabel::parse(&text).expect("SET parses");
        let expect: Vec<(u32, u64)> = c
            .node(1)
            .term_map_for_test()
            .into_iter()
            .filter(|&(_, b)| b < p)
            .collect();
        assert!(!label.lineage.is_empty());
        assert_eq!(label.lineage, expect);
        assert!(
            listing.chunks.len() >= 2,
            "1 MiB chunks over ~2.5 MiB of history: {:?}",
            listing.chunks
        );
        assert!(
            listing
                .chunks
                .iter()
                .all(|&(f, t)| t - f <= (1 << 20) + 32 * 1024),
            "a chunk is at most chunk_bytes (plus one frame): {:?}",
            listing.chunks
        );
        assert!(reach_from(&listing.chunks, p) >= w.through);
        let report = verify(&t, true).unwrap();
        assert!(report.findings.is_empty(), "{:?}", report.findings);
        let shared = c.node(3).backup_writer().expect("node 3 runs a writer");
        assert!(shared.stats.bytes_set.load(Ordering::Relaxed) > 0);
        assert!(shared.stats.bytes_journal.load(Ordering::Relaxed) > 0);
        assert!(shared.progress.load(Ordering::Acquire) >= w.through);
        assert!(
            c.node(0).backup_writer().is_none(),
            "no [backup], no writer"
        );

        let dirs = c.dirs();
        let _keep = c.stop();
        let n = assert_chunks_match(&t, &journal_frames(&dirs[l]));
        assert!(n > 20_000, "chunks hold the submitted frames ({n})");
    }

    /// 3b / RA6: a set with no lineage is deferred ALONE. The writer's
    /// published map is replaced by one whose first term opens AT the older
    /// set (so nothing lies strictly below it) but below the newer one: the
    /// newer set is copied while the older is left out of the tier.
    #[test]
    fn a_lineage_less_set_is_deferred_alone_and_a_newer_one_is_copied() {
        let _g = serialize();
        let tier = tmp_tier();
        let cfg = writer_cfg(tier.path(), 1 << 20, Duration::from_millis(300));
        let mut c = spawn(3, 1, opts("tier-defer", false), |i| {
            (i == 3).then(|| cfg.clone())
        });
        start_sums(&mut c);
        let l = await_single_leader(&c, 30);
        await_capable(&c);
        let shared = c.node(3).backup_writer().expect("node 3 runs a writer");
        shared.paused_for_test.store(true, Ordering::Release);
        let all = c.running();
        submit_settled(&c, 2000);
        let p1 = instant_until_complete(&c, l, &all);
        await_agreed(&c, &all, p1);
        submit_settled(&c, 2000);
        let p2 = instant_until_complete(&c, l, &all);
        await_agreed(&c, &all, p2);
        assert!(p2 > p1);
        *shared.term_map.lock().unwrap() = vec![(1, p1)];
        shared.paused_for_test.store(false, Ordering::Release);
        let w = await_watermark(&c, l, 60, |w| w.newest_set == p2);
        assert_eq!(w.oldest_set, p2, "the deferred set is not in the chain");
        let t = FsTarget::new(tier.path());
        let listing = list_tier(&t).unwrap();
        assert!(listing.sets.iter().any(|s| s.position == p2), "{listing:?}");
        assert!(
            !listing.sets.iter().any(|s| s.position == p1),
            "the lineage-less set waits: {listing:?}"
        );
        let _keep = c.stop();
    }

    /// Spec §8.1/§8.2, whole-cluster restore: a writer backs up a 3-voter
    /// cluster; every node stops; the builder materialises the tier at its
    /// `through` into three FRESH instance directories (Recovery, same Q on
    /// all three); `verify_artifact` accepts each; three nodes and their
    /// services boot on them unchanged, elect a leader, commit a new write
    /// past Q and complete a coordinated instant.
    #[test]
    fn materialize_builds_a_dir_verify_accepts_and_a_node_boots_from() {
        let _g = serialize();
        let tier = tmp_tier();
        let cfg = writer_cfg(tier.path(), 1 << 20, Duration::from_millis(500));
        let mut c = spawn(3, 0, opts("tier-restore", false), |i| {
            (i == 1).then(|| cfg.clone())
        });
        start_sums(&mut c);
        let l = await_single_leader(&c, 30);
        await_capable(&c);
        let all = c.running();
        submit_settled(&c, 500);
        let p = instant_until_complete(&c, l, &all);
        await_agreed(&c, &all, p);
        let lead = submit_settled(&c, 3000);
        let target = c.node(lead).counters().commit.load_acquire();
        // The oracle: the SumSm total once EVERY submit has applied. `submit`
        // only enqueues on the ingress ring and the leader appends later, so
        // `target` (read right after the last submit) can sit below the end of
        // the batch — CI once read the total with 3000 of 3500 writes still in
        // the ring. A write moves the total only after it is appended,
        // committed and applied, so a total that holds still across three
        // linearizable reads 300 ms apart has every write in it.
        let mut total_before = query_total(&c, l);
        let mut still = 0;
        let settle = Instant::now() + Duration::from_secs(60);
        while still < 3 {
            assert!(Instant::now() < settle, "the SumSm total never settled");
            std::thread::sleep(Duration::from_millis(300));
            let t = query_total(&c, l);
            still = if t == total_before { still + 1 } else { 0 };
            total_before = t;
        }
        assert_ne!(total_before, 0, "the writes moved the total");
        await_watermark(&c, l, 60, |w| w.newest_set == p && w.through >= target);
        // ...and the tier must hold the LAST client write. The writer's next
        // quiet cut (500 ms here) takes every substantive frame up to commit;
        // after that only bookkeeping follows, which it deliberately never
        // backs up — so `through` stops moving once it covers the last write.
        // Wait for it to hold still for 4 quiet intervals.
        let mut last = watermark(c.node(l)).map(|w| w.through);
        let mut since = Instant::now();
        let deadline = Instant::now() + Duration::from_secs(60);
        while since.elapsed() < Duration::from_secs(2) {
            assert!(Instant::now() < deadline, "the tier's reach never settled");
            std::thread::sleep(Duration::from_millis(50));
            let now = watermark(c.node(l)).map(|w| w.through);
            if now != last {
                last = now;
                since = Instant::now();
            }
        }

        // Stop everything: services, then nodes.
        for h in c.nodes.iter_mut() {
            if let Some(s) = h.svc.take() {
                s();
            }
        }
        for h in c.nodes.iter_mut() {
            h.stop();
        }

        let t = FsTarget::new(tier.path());
        let (_, _, through) = watermark_of(&list_tier(&t).unwrap()).expect("a watermark");
        assert!(through >= target, "the tier holds the committed writes");
        let fresh = tempfile::Builder::new()
            .prefix("uc2-tier-restored-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .expect("tempdir");
        for (i, h) in c.nodes.iter_mut().enumerate() {
            let nd = fresh.path().join(format!("r{i}"));
            let m = materialize(&t, Some(through), &nd, Mode::Recovery, 7).expect("materialize");
            assert_eq!((m.q, m.mode), (through, Mode::Recovery));
            assert!(m.start_set >= p && m.start_set < through);
            let rep = uc_node::backup::verify_artifact(&nd).expect("verify accepts");
            assert_eq!(
                (rep.journal_first_base, rep.journal_last_pos),
                (m.start_set, through)
            );
            // Re-home node i onto its restored directory: same id, address,
            // membership and app; no writer.
            let old = std::mem::replace(&mut h.cfg, Box::new(|| unreachable!()));
            let d = nd.clone();
            h.cfg = Box::new(move || NodeConfig {
                instance_dir: d.clone(),
                ..old()
            });
            h.dir = nd;
            h.backup = None;
        }

        for h in c.nodes.iter_mut() {
            h.start();
        }
        start_sums(&mut c);
        let l2 = await_single_leader(&c, 60);
        await_capable(&c);
        assert_eq!(
            query_total(&c, l2),
            total_before,
            "the restored cluster answers as the original did at Q"
        );
        let l2 = submit_settled(&c, 10);
        let commit = c.node(l2).counters().commit.load_acquire();
        assert!(
            commit > through,
            "a new write committed past Q ({commit} <= {through})"
        );
        let all = c.running();
        let p2 = instant_until_complete(&c, l2, &all);
        assert!(p2 > through, "the instant lies past Q");
        let _keep = c.stop();
        drop(fresh);
    }

    /// Ruling RF1 (final review F1): a whole-cluster restore at a Q BELOW
    /// the tier's reach R rolls the history back, and the restored cluster
    /// writes a different history past Q. Its writer, restarted on the SAME
    /// target, must not append that history to the old chain `[S, R)`:
    /// the restore fenced the tier (`fence/<Q>`), the writer stops by name
    /// (`backup_target_fenced`), the tier gains nothing, and the writer's
    /// purge hold is released (a stopped writer holds nothing, R9a).
    #[test]
    fn a_restore_below_the_reach_fences_the_tier_and_its_writer_stops() {
        let _g = serialize();
        let tier = tmp_tier();
        let cfg = writer_cfg(tier.path(), 1 << 20, Duration::from_millis(300));
        let mut c = spawn(3, 0, opts("tier-fence", false), |i| {
            (i == 1).then(|| cfg.clone())
        });
        start_sums(&mut c);
        let l = await_single_leader(&c, 30);
        await_capable(&c);
        let all = c.running();
        submit_settled(&c, 500);
        let p = instant_until_complete(&c, l, &all);
        await_agreed(&c, &all, p);
        let lead = submit_settled(&c, 1000);
        let t1 = c.node(lead).counters().commit.load_acquire();
        // Q: a chain end the tier reached (a chunk end, so a frame end).
        let q = await_watermark(&c, l, 60, |w| w.newest_set == p && w.through >= t1).through;
        let lead = submit_settled(&c, 1000);
        let t2 = c.node(lead).counters().commit.load_acquire();
        await_watermark(&c, l, 60, |w| w.through >= t2);
        for h in c.nodes.iter_mut() {
            if let Some(s) = h.svc.take() {
                s();
            }
        }
        for h in c.nodes.iter_mut() {
            h.stop();
        }

        let t = FsTarget::new(tier.path());
        let before = list_tier(&t).unwrap();
        let (_, _, reach) = watermark_of(&before).expect("a watermark");
        assert!(
            p < q && q < reach,
            "precondition: S={p} < Q={q} < R={reach}"
        );
        let fresh = tempfile::Builder::new()
            .prefix("uc2-tier-fenced-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .expect("tempdir");
        for (i, h) in c.nodes.iter_mut().enumerate() {
            let nd = fresh.path().join(format!("r{i}"));
            let m = materialize(&t, Some(q), &nd, Mode::Recovery, 7).expect("materialize");
            assert_eq!(m.q, q);
            let old = std::mem::replace(&mut h.cfg, Box::new(|| unreachable!()));
            let d = nd.clone();
            h.cfg = Box::new(move || NodeConfig {
                instance_dir: d.clone(),
                ..old()
            });
            h.dir = nd;
            // Node 1 keeps its `[backup]` writer on the SAME target.
        }
        let listed = list_tier(&t).unwrap();
        assert_eq!(listed.fences, vec![q], "the restore fenced the tier at Q");

        for h in c.nodes.iter_mut() {
            h.start();
        }
        start_sums(&mut c);
        let l2 = await_single_leader(&c, 60);
        await_capable(&c);
        // A NEW history past Q: an instant P' inside the abandoned (Q, R]
        // — an unfenced writer copies it as a set "inside the chain" (pick
        // case (b)) — then more than R, which it would cut from R onto the
        // old chain.
        let lead = submit_settled(&c, (reach - q) / FRAME_BYTES / 2);
        let all = c.running();
        let p2 = instant_until_complete(&c, lead, &all);
        await_agreed(&c, &all, p2);
        assert!(
            q < p2 && p2 < reach,
            "precondition: Q={q} < P'={p2} < R={reach}"
        );
        let lead = submit_settled(&c, (reach - q) / FRAME_BYTES + 1000);
        let commit = c.node(lead).counters().commit.load_acquire();
        assert!(
            commit > reach,
            "the new history passes R ({commit} <= {reach})"
        );
        let shared = c.node(1).backup_writer().expect("node 1 runs a writer");
        // Several quiet intervals for a writer that is going to write.
        let deadline = Instant::now() + Duration::from_secs(20);
        while !shared.stopped.load(Ordering::Acquire) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        std::thread::sleep(Duration::from_millis(1500));
        let after = list_tier(&t).unwrap();
        let new_chunks: Vec<(u64, u64)> = after
            .chunks
            .iter()
            .copied()
            .filter(|ch| !before.chunks.contains(ch))
            .collect();
        let new_sets: Vec<u64> = after
            .sets
            .iter()
            .map(|s| s.position)
            .filter(|p| !before.sets.iter().any(|b| b.position == *p))
            .collect();
        assert!(
            new_chunks.is_empty() && new_sets.is_empty(),
            "the writer appended the restored cluster's NEW history to the rolled-back chain \
             (S={p}, Q={q}, R={reach}): new chunks {new_chunks:?}, new sets {new_sets:?}, tier \
             reach now {:?}; writer errors {:?}",
            watermark_of(&after),
            WriterError::ALL
                .iter()
                .map(|e| (e.label(), shared.stats.errors(*e)))
                .collect::<Vec<_>>()
        );
        assert!(
            shared.stopped.load(Ordering::Acquire),
            "the writer stopped on the fenced target"
        );
        assert_eq!(
            shared.stats.errors(WriterError::Fenced),
            1,
            "backup_target_fenced counted once"
        );
        assert_eq!(shared.purge_cap(), None, "a stopped writer holds nothing");
        assert_eq!(shared.keep(), None);
        let _ = l2;
        let _keep = c.stop();
        drop(fresh);
    }

    /// Ruling R11a: an IDLE cluster's freshly copied set must become
    /// restorable. After the instant only bookkeeping frames (padding, the
    /// writer's own `BackupWatermark` reports) follow it, which R8b's quiet
    /// filter declines — so the writer allows ONE bookkeeping-only cut when
    /// the chain ends exactly at its newest set. The tier's `through` then
    /// passes the set, a restore builds and verifies, and over several more
    /// quiet intervals no further chunk is cut (the R8b loop stays broken).
    /// (In this fixture the instant's own `SnapshotReport` frames — CLUSTER
    /// kind 5, substantive — already follow the set, so this guards the
    /// outcome; the predicate's exception is pinned by the writer's unit
    /// test.)
    #[test]
    fn an_idle_clusters_new_set_becomes_restorable() {
        let _g = serialize();
        let tier = tmp_tier();
        let cfg = writer_cfg(tier.path(), 64 << 20, Duration::from_millis(500));
        let mut c = spawn(3, 0, opts("tier-idle", false), |i| {
            (i == 1).then(|| cfg.clone())
        });
        start_sums(&mut c);
        let l = await_single_leader(&c, 30);
        await_capable(&c);
        let all = c.running();
        submit_settled(&c, 50);
        let s = instant_until_complete(&c, l, &all);
        await_agreed(&c, &all, s);
        // No client traffic from here on.

        let t = FsTarget::new(tier.path());
        let deadline = Instant::now() + Duration::from_secs(20);
        let through = loop {
            let l = list_tier(&t).unwrap();
            if let Some((_, newest, through)) = watermark_of(&l)
                && newest == s
                && through > s
            {
                break through;
            }
            assert!(
                Instant::now() < deadline,
                "the idle tier never reached past its set {s}: {l:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        };

        let fresh = tempfile::Builder::new()
            .prefix("uc2-tier-idle-restore-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .expect("tempdir");
        let nd = fresh.path().join("r");
        let m = materialize(&t, None, &nd, Mode::Recovery, 1).expect("materialize");
        assert_eq!(m.start_set, s);
        assert!(m.q >= through);
        let rep = uc_node::backup::verify_artifact(&nd).expect("verify accepts");
        assert_eq!((rep.journal_first_base, rep.journal_last_pos), (s, m.q));

        // The loop stays broken: once the tier has settled, six more quiet
        // intervals add at most one chunk (an endless bookkeeping loop would
        // add one per interval).
        std::thread::sleep(Duration::from_millis(1500));
        let settled = list_tier(&t).unwrap().chunks.len();
        std::thread::sleep(Duration::from_millis(3000));
        let after = list_tier(&t).unwrap().chunks;
        assert!(
            after.len() <= settled + 1,
            "an idle tier kept cutting chunks: {settled} -> {} ({after:?})",
            after.len()
        );
        let _keep = c.stop();
    }

    /// Spec §5.3: with less than `chunk_bytes` of new history, the writer
    /// still cuts once `quiet_interval` has passed — a quiet cluster's tail
    /// reaches the tier within seconds, not at the next 64 MiB.
    #[test]
    fn a_quiet_cluster_is_backed_up_after_the_quiet_interval() {
        let _g = serialize();
        let tier = tmp_tier();
        let cfg = writer_cfg(tier.path(), 64 << 20, Duration::from_millis(500));
        let mut c = spawn(3, 0, opts("tier-quiet", false), |i| {
            (i == 1).then(|| cfg.clone())
        });
        start_sums(&mut c);
        let l = await_single_leader(&c, 30);
        await_capable(&c);
        submit_settled(&c, 200);
        let all = c.running();
        let p = instant_until_complete(&c, l, &all);
        await_agreed(&c, &all, p);
        let lead = submit_settled(&c, 1);
        let target = c.node(lead).counters().commit.load_acquire();
        let started = Instant::now();
        let w = await_watermark(&c, l, 10, |w| w.through >= target);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the quiet tail reached the tier only after {:?}",
            started.elapsed()
        );
        assert!(w.through - p < 64 << 20, "far below one chunk_bytes");
        let t = FsTarget::new(tier.path());
        let before = list_tier(&t).unwrap().chunks.len();
        assert!(before > 0);

        // An idle cluster goes quiet: every report commits a watermark
        // frame, but a quiet cut over nothing else is declined — otherwise
        // the writer would back up its own report each interval, forever.
        std::thread::sleep(Duration::from_millis(2500));
        let after = list_tier(&t).unwrap().chunks;
        assert!(
            after.len() <= before + 1,
            "an idle cluster kept cutting chunks: {before} -> {} ({after:?})",
            after.len()
        );
        let _keep = c.stop();
    }

    /// Ruling R8d / spec §5.3: a newly agreed set ABOVE the chain end is
    /// deferred until the chain passes it, then copied INTO the chain — it
    /// never starts a second chain on a healthy cluster. A live writer with
    /// a long quiet interval holds the tier at `S0 … E`; an instant `P > E`
    /// is taken; the committed watermark's `oldest_set` must stay `S0` at
    /// every sample until `through > P`, set `P` must end up in the tier,
    /// the chain from `S0` must reach `through`, and no gap is counted.
    #[test]
    fn a_second_instant_above_the_chain_end_extends_the_chain_not_a_new_one() {
        let _g = serialize();
        let tier = tmp_tier();
        let cfg = writer_cfg(tier.path(), 64 << 20, Duration::from_secs(3));
        let mut c = spawn(3, 0, opts("tier-second", false), |i| {
            (i == 1).then(|| cfg.clone())
        });
        start_sums(&mut c);
        let l = await_single_leader(&c, 30);
        await_capable(&c);
        let all = c.running();
        submit_settled(&c, 200);
        let s0 = instant_until_complete(&c, l, &all);
        await_agreed(&c, &all, s0);
        submit_settled(&c, 200);
        let w0 = await_watermark(&c, l, 30, |w| w.through > s0);
        assert_eq!(w0.oldest_set, s0);

        // A second instant just after a cut: P lies above the chain end.
        submit_settled(&c, 300);
        let p = instant_until_complete(&c, l, &all);
        let at_p = watermark(c.node(l)).unwrap();
        assert!(
            at_p.through < p,
            "precondition: the chain end {} must lie below the new instant {p}",
            at_p.through
        );
        await_agreed(&c, &all, p);
        submit_settled(&c, 100);

        // No further load: the cut after P makes P eligible, and the report
        // that names it as `newest_set` at an UNCHANGED `through` commits
        // (ruling R8e).
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut samples = 0u64;
        let last = loop {
            let w = watermark(c.node(l)).unwrap();
            samples += 1;
            assert_eq!(
                w.oldest_set, s0,
                "the watermark started a new chain at a healthy instant: {w:?} (S0 = {s0}, P = {p})"
            );
            if w.through > p && w.newest_set == p {
                break w;
            }
            assert!(
                Instant::now() < deadline,
                "the chain never passed P = {p} with P in the tier: {w:?}"
            );
            std::thread::sleep(Duration::from_millis(2));
        };
        let shared = c.node(1).backup_writer().unwrap();
        assert_eq!(
            shared.stats.gaps.load(Ordering::Relaxed),
            0,
            "no backup_chain_gap on a healthy cluster"
        );
        let _keep = c.stop();
        let listing = list_tier(&FsTarget::new(tier.path())).unwrap();
        assert!(listing.sets.iter().any(|x| x.position == s0));
        assert!(listing.sets.iter().any(|x| x.position == p), "{listing:?}");
        assert!(
            reach_from(&listing.chunks, s0) >= last.through,
            "the chain from S0 reaches through: {:?}",
            listing.chunks
        );
        eprintln!("second instant: S0={s0} P={p} final={last:?} samples={samples}");
    }

    /// Spec §5.3 / §11.2, the Review-Focus-1 test: a chunk never holds
    /// bytes a failover truncated. EVERY voter runs a writer into its own
    /// tier, so whichever node leads is a writer. The leader is cut off from
    /// both peers and keeps appending (durable on its own disk, never
    /// committed) while its writer cuts every 300 ms — a writer that read up
    /// to `durable` instead of `min(commit, durable)` would put those bytes
    /// in a chunk now. The majority elects, commits different bytes at those
    /// positions, the partition heals, the old leader TRUNCATES (asserted),
    /// and every frame in its tier must equal the surviving log.
    #[test]
    fn a_chunk_never_holds_bytes_a_failover_truncated() {
        let _g = serialize();
        let tiers: Vec<tempfile::TempDir> = (0..3).map(|_| tmp_tier()).collect();
        let cfgs: Vec<BackupConfig> = tiers
            .iter()
            .map(|t| writer_cfg(t.path(), 1 << 20, Duration::from_millis(300)))
            .collect();
        let mut c = spawn(3, 0, opts("tier-trunc", false), |i| Some(cfgs[i].clone()));
        start_sums(&mut c);
        let first = await_single_leader(&c, 30);
        await_capable(&c);
        submit_settled(&c, 500);
        let all = c.running();
        let p = instant_until_complete(&c, first, &all);
        await_agreed(&c, &all, p);
        // Whoever leads NOW is the one cut off below (every voter writes).
        let old = submit_settled(&c, 500);
        let commit0 = c.node(old).counters().commit.load_acquire();
        await_watermark(&c, old, 30, |w| w.through >= commit0);

        // Cut the leader off from both peers.
        let others: Vec<usize> = all.iter().copied().filter(|&i| i != old).collect();
        for &o in &others {
            for h in c.node(old).partition_handles() {
                h.block(c.nodes[o].addr);
            }
            for h in c.node(o).partition_handles() {
                h.block(c.nodes[old].addr);
            }
        }
        let took = submit_some(c.node(old), 3000, 1_000_000);
        // Let the archive record them and the writer run several cuts.
        std::thread::sleep(Duration::from_millis(1500));
        let ok = c.node(old).counters();
        let (doomed_from, doomed_to) = (ok.commit.load_acquire(), ok.durable.load_acquire());
        eprintln!(
            "isolated leader {old} took {took} frames: commit={doomed_from} durable={doomed_to}"
        );
        assert!(
            doomed_to > doomed_from + 100 * FRAME_BYTES,
            "the isolated leader must hold uncommitted durable bytes (took {took}, \
             commit {doomed_from}, durable {doomed_to})"
        );
        let old_tier = FsTarget::new(tiers[old].path());
        let reach_during = list_tier(&old_tier)
            .unwrap()
            .chunks
            .iter()
            .map(|&(_, t)| t)
            .max()
            .unwrap_or(0);
        assert!(
            reach_during <= doomed_from,
            "during the partition the old leader's tier reached {reach_during}, past its \
             commit {doomed_from}: uncommitted bytes were backed up"
        );

        // The majority elects and commits different bytes over that range
        // (following leadership while it settles).
        let serving_other = |c: &Cluster| others.iter().copied().find(|&i| c.node(i).can_serve());
        let mut left = (doomed_to - doomed_from) / FRAME_BYTES + 500;
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut new = others[0];
        while left > 0 {
            assert!(
                Instant::now() < deadline,
                "the majority never took the writes"
            );
            let Some(i) = serving_other(&c) else {
                std::thread::yield_now();
                continue;
            };
            new = i;
            left -= submit_some(c.node(i), left, 2_000_000 + left);
        }
        await_until(60, "the majority committed past the doomed range", || {
            c.node(new).counters().commit.load_acquire() >= doomed_to
        });
        let truncs_before = c.node(old).truncations();
        for h in c.nodes.iter().filter_map(|h| h.node.as_ref()) {
            for ph in h.partition_handles() {
                ph.clear();
            }
        }
        await_until(
            30,
            "the old leader truncated its uncommitted suffix",
            || c.node(old).truncations() > truncs_before,
        );
        // The position to wait on is the commit BEFORE the 500 frames
        // below, never after them: the tail past the last client frame is
        // tier bookkeeping (BackupWatermark reports) a quiet cut declines,
        // so a commit read after the writes may never be backed up. The 500
        // substantive frames past `target` guarantee a cut reaches it.
        let target = c.node(new).counters().commit.load_acquire();
        let new = submit_following(&c, 500);
        // Wait for the old leader's writer to back up past the doomed range,
        // but compare frames FIRST if it never does: a chunk of doomed bytes
        // ends off the surviving log's frame grid, so a writer that wrote
        // one stalls there — the comparison below names why.
        let deadline = Instant::now() + Duration::from_secs(30);
        let reached = loop {
            let r = list_tier(&old_tier)
                .unwrap()
                .chunks
                .iter()
                .any(|&(_, t)| t >= target.max(doomed_to));
            if r || Instant::now() >= deadline {
                break r;
            }
            std::thread::sleep(Duration::from_millis(10));
        };

        let dirs = c.dirs();
        let _keep = c.stop();
        let survivor = journal_frames(&dirs[new]);
        // The survivor really holds different bytes over the doomed range.
        assert!(
            survivor
                .range(doomed_from..doomed_to)
                .any(|(_, (term, _, _))| *term > 1),
            "the surviving log rewrote the doomed range in a later term"
        );
        let n = assert_chunks_match(&old_tier, &survivor);
        assert!(
            reached,
            "the old leader's writer backed up past the doomed range"
        );
        let covered = chunk_frames(&old_tier)
            .iter()
            .filter(|f| (doomed_from..doomed_to).contains(&f.0))
            .count();
        eprintln!("old leader's tier: {n} frames, {covered} inside the doomed range");
        assert!(
            covered > 100,
            "the tier covers the truncated range ({covered})"
        );
        for (i, t) in tiers.iter().enumerate() {
            if i != old {
                assert_chunks_match(&FsTarget::new(t.path()), &survivor);
            }
        }
    }

    /// Spec §5.3 "a gap" / §4.5: two learners write into ONE target; both
    /// are stopped while the voters keep committing, snapshotting and
    /// purging, so the tier's chain end falls below every journal. A
    /// restarted writer starts a new chain at the newest set it holds and
    /// the watermark's `oldest_set` jumps; at no point does any committed
    /// watermark claim `[oldest_set, through)` across a gap in the final
    /// tier.
    ///
    /// Issue #99 (fixed): under load an election mid-test used to hit the
    /// post-election commit stall (log end more than `fsm_lag` past
    /// `applied`, so the report ceiling sat below the new leader's NewTerm
    /// frame) — it failed here as "no leader took the writes" / "no leader committed its
    /// own appends within 60 s" / "no instant completed", a product defect,
    /// not this test's. Also seen twice while deflaking, unexplained: "no
    /// single leader elected" — no election at all for 20 s after start,
    /// only under a SIGSTOP stopper whose scripts may have left a STOP
    /// un-continued; never reproduced with a logged stopper (0/104).
    #[test]
    fn two_writers_share_a_target_and_the_watermark_never_crosses_a_gap() {
        let _g = serialize();
        let tier = tmp_tier();
        let cfg = writer_cfg(tier.path(), 1 << 20, Duration::from_millis(300));
        let mut c = spawn(3, 2, opts("tier-gap", true), |i| {
            (i >= 3).then(|| cfg.clone())
        });
        start_sums(&mut c);
        let l = await_single_leader(&c, 30);
        await_capable(&c);
        // `l` is the OBSERVER below (its committed view and commit counter),
        // never assumed to stay leader: this test runs long enough that an
        // election under a loaded box is routine, so every write follows
        // leadership (`submit_settled`) and every instant is commanded on
        // whichever voter then serves (`instant_until_complete`).
        let voters = c.voters();
        let all = c.running();
        let mut seen: Vec<BackupTierEntry> = Vec::new();
        let record = |c: &Cluster, seen: &mut Vec<BackupTierEntry>| {
            if let Some(w) = watermark(c.node(l))
                && seen.last() != Some(&w)
            {
                seen.push(w);
            }
        };

        submit_settled(&c, 4000);
        let p0 = instant_until_complete(&c, l, &all);
        await_agreed(&c, &all, p0);
        let lead = submit_settled(&c, 4000);
        let t0 = c.node(lead).counters().commit.load_acquire();
        await_watermark(&c, l, 60, |w| w.through >= t0);
        record(&c, &mut seen);

        // Both writers down; the voters move on and purge.
        c.nodes[3].stop();
        c.nodes[4].stop();
        let tier_end = watermark(c.node(l)).unwrap().through;
        let mut p_last = p0;
        for _ in 0..3 {
            let lead = submit_settled(&c, 6000);
            p_last = instant_until_complete(&c, lead, &voters);
            await_agreed(&c, &voters, p_last);
            record(&c, &mut seen);
        }
        await_until(60, "every voter purged past the tier's end", || {
            voters
                .iter()
                .all(|&v| c.node(v).archive_first_base() > tier_end)
        });

        // Writer B returns: below the floor, it installs the newest set and
        // its journal starts there — a gap from the tier's end.
        c.nodes[4].start();
        c.nodes[4].start_service(c.app, 4);
        let lead = submit_settled(&c, 2000);
        let t1 = c.node(lead).counters().commit.load_acquire();
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            record(&c, &mut seen);
            if seen.last().is_some_and(|w| w.through >= t1) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "writer B never extended a new chain: {seen:?}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let w = *seen.last().unwrap();
        assert!(
            w.oldest_set > tier_end,
            "oldest_set jumped past the gap: {w:?}"
        );
        assert!(
            w.oldest_set >= p_last,
            "the new chain starts at the newest set writer B holds ({p_last}): {w:?}"
        );

        // Writer A returns too; both share the target from here.
        c.nodes[3].start();
        c.nodes[3].start_service(c.app, 3);
        let lead = submit_settled(&c, 4000);
        let t2 = c.node(lead).counters().commit.load_acquire();
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            record(&c, &mut seen);
            if seen.last().is_some_and(|w| w.through >= t2) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the shared tier stalled: {seen:?}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let a = c.node(3).backup_writer().unwrap();
        let b = c.node(4).backup_writer().unwrap();
        let _keep = c.stop();

        let t = FsTarget::new(tier.path());
        let listing = list_tier(&t).unwrap();
        for w in &seen {
            let reach = reach_from(&listing.chunks, w.oldest_set);
            assert!(
                reach >= w.through,
                "watermark {w:?} claims [{}, {}) but the tier's chain from {} reaches only \
                 {reach}",
                w.oldest_set,
                w.through,
                w.oldest_set
            );
            assert!(listing.sets.iter().any(|s| s.position == w.oldest_set));
        }
        assert!(
            listing.corrupt_sets.is_empty(),
            "the two writers' lineages agreed: {:?}",
            listing.corrupt_sets
        );
        let report = verify(&t, true).unwrap();
        assert!(report.findings.is_empty(), "{:?}", report.findings);
        assert!(
            b.stats.gaps.load(Ordering::Relaxed) >= 1,
            "writer B named the gap (backup_chain_gap)"
        );
        assert_eq!(a.stats.errors(WriterError::OverlapMismatch), 0);
        assert_eq!(b.stats.errors(WriterError::OverlapMismatch), 0);
        eprintln!("watermarks seen: {seen:?}");
    }

    /// Ruling RF2 (final review F2): two LIVE writers on one target. Writer
    /// A has a chain `S0 … e_A` and lags (paused — a slow target); its node
    /// holds its journal from `e_A`. Writer B, a learner that joins below
    /// the floor, starts a NEWER chain at the set it installed, `P_B >
    /// e_A`, which becomes the tier's `through`. A resumed must FILL `[e_A,
    /// P_B)` from its own journal before anything else — its purge cap the
    /// fillable chain end, never `through` — so the two chains join, and
    /// A's node never purges past its own chain end while the gap exists.
    #[test]
    fn a_lagging_writer_fills_the_gap_below_a_newer_chain() {
        let _g = serialize();
        let tier = tmp_tier();
        let cfg = writer_cfg(tier.path(), 1 << 20, Duration::from_millis(300));
        let mut c = spawn(3, 2, opts("tier-fill", true), |i| {
            (i >= 3).then(|| cfg.clone())
        });
        // Writer B (node 4) joins later, below the floor.
        c.nodes[4].stop();
        start_sums(&mut c);
        let l = await_single_leader(&c, 30);
        await_capable(&c);
        let running = c.running();
        let voters = c.voters();
        submit_settled(&c, 4000);
        let s0 = instant_until_complete(&c, l, &running);
        await_agreed(&c, &running, s0);
        let lead = submit_settled(&c, 4000);
        let t0 = c.node(lead).counters().commit.load_acquire();
        await_watermark(&c, l, 60, |w| w.through >= t0);

        // Writer A lags: its duty cycle stops; a cut in flight lands.
        let a = c.node(3).backup_writer().expect("A runs a writer");
        a.paused_for_test.store(true, Ordering::Relaxed);
        std::thread::sleep(Duration::from_secs(1));
        let t = FsTarget::new(tier.path());
        let e_a = reach_from(&list_tier(&t).unwrap().chunks, s0);
        assert!(e_a >= t0, "A's chain reached the committed writes");
        let mut p_last = s0;
        for _ in 0..3 {
            let lead = submit_settled(&c, 6000);
            p_last = instant_until_complete(&c, lead, &running);
            await_agreed(&c, &running, p_last);
        }
        await_until(60, "every voter purged past A's chain end", || {
            voters.iter().all(|&v| c.node(v).archive_first_base() > e_a)
        });
        assert!(
            c.node(3).archive_first_base() <= e_a,
            "A's node holds its journal from its chain end"
        );

        // Writer B joins below the floor and starts a newer chain.
        c.nodes[4].start();
        c.nodes[4].start_service(c.app, 4);
        submit_settled(&c, 2000);
        let deadline = Instant::now() + Duration::from_secs(90);
        let p_b = loop {
            let l = list_tier(&t).unwrap();
            if let Some((oldest, _, through)) = watermark_of(&l)
                && oldest > e_a
                && through > oldest
            {
                break oldest;
            }
            assert!(
                Instant::now() < deadline,
                "writer B never started a newer chain: {l:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        assert!(p_b >= p_last, "B's chain starts at the set it installed");
        assert_eq!(
            reach_from(&list_tier(&t).unwrap().chunks, s0),
            e_a,
            "precondition: nothing filled [e_A, P_B) while A lagged"
        );

        // A resumes. While the gap exists A's node never purges past the
        // lower chain's end (read first_base BEFORE the listing: the end
        // only grows). Sampled from the moment A resumes, then again across
        // one more instant, which moves every floor.
        let mut samples = 0u64;
        let mut sample = |c: &Cluster| -> bool {
            let fb = c.node(3).archive_first_base();
            let l = list_tier(&t).unwrap();
            let lower = reach_from(&l.chunks, s0);
            let (_, _, through) = watermark_of(&l).expect("a watermark");
            samples += 1;
            assert!(
                lower >= through || fb <= lower,
                "A's node purged to {fb}, past its own chain end {lower} while the gap \
                 [{lower}, {p_b}) exists (S0={s0} e_A={e_a} through={through})"
            );
            lower >= through
        };
        a.paused_for_test.store(false, Ordering::Relaxed);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !sample(&c) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
        }
        let lead = submit_settled(&c, 2000);
        let all = c.running();
        let p_more = instant_until_complete(&c, lead, &all);
        await_agreed(&c, &all, p_more);
        let deadline = Instant::now() + Duration::from_secs(90);
        while !sample(&c) {
            assert!(
                Instant::now() < deadline,
                "the chains never joined: S0={s0} e_A={e_a} P_B={p_b}; tier {:?}",
                list_tier(&t).unwrap()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let lead = submit_settled(&c, 2000);
        let t2 = c.node(lead).counters().commit.load_acquire();
        let w = await_watermark(&c, l, 60, |w| w.through >= t2);
        let b = c.node(4).backup_writer().unwrap();
        let _keep = c.stop();
        let listing = list_tier(&t).unwrap();
        assert!(
            chain_covers(&listing.chunks, e_a, p_b),
            "[e_A, P_B) = [{e_a}, {p_b}) filled: {:?}",
            listing.chunks
        );
        assert!(
            reach_from(&listing.chunks, s0) >= w.through,
            "the chain from S0 reaches the final through {}: {:?}",
            w.through,
            listing.chunks
        );
        let report = verify(&t, true).unwrap();
        assert!(report.findings.is_empty(), "{:?}", report.findings);
        assert_eq!(a.stats.errors(WriterError::OverlapMismatch), 0);
        assert_eq!(b.stats.errors(WriterError::OverlapMismatch), 0);
        eprintln!("fill: S0={s0} e_A={e_a} P_B={p_b} final={w:?} samples={samples}");
    }

    /// `[from, to)` is covered by one gap-free run of chunks.
    fn chain_covers(chunks: &[(u64, u64)], from: u64, to: u64) -> bool {
        reach_from(chunks, from) >= to
    }

    /// Spec §4.2 / §10: a target whose `TIER` names another `app_id` is
    /// refused by name and nothing is written to it — not a set, not a
    /// chunk, not a temporary file.
    #[test]
    fn a_foreign_target_is_refused_and_nothing_is_written() {
        let _g = serialize();
        let tier = tmp_tier();
        let t = FsTarget::new(tier.path());
        open_or_create_tier(&t, "other", 1).unwrap();
        let cfg = writer_cfg(tier.path(), 1 << 20, Duration::from_millis(300));
        let mut c = spawn(3, 0, opts("tier-foreign", false), |i| {
            (i == 2).then(|| cfg.clone())
        });
        start_sums(&mut c);
        let l = await_single_leader(&c, 30);
        await_capable(&c);
        submit_settled(&c, 500);
        let all = c.running();
        let p = instant_until_complete(&c, l, &all);
        await_agreed(&c, &all, p);
        submit_settled(&c, 500);
        let shared = c.node(2).backup_writer().unwrap();
        await_until(10, "the writer refused the foreign target", || {
            shared.stats.errors(WriterError::Foreign) == 1
        });
        std::thread::sleep(Duration::from_secs(2));
        assert_eq!(shared.stats.errors(WriterError::Foreign), 1, "logged once");
        assert!(
            watermark(c.node(l)).is_none(),
            "no watermark for a refused tier"
        );
        let _keep = c.stop();
        let mut names: Vec<String> = std::fs::read_dir(tier.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        let tmp_empty = std::fs::read_dir(tier.path().join(".tmp"))
            .map(|d| d.count() == 0)
            .unwrap_or(true);
        assert!(
            names.iter().all(|n| n == "TIER" || n == ".tmp") && tmp_empty,
            "only the foreign TIER is on the target: {names:?}"
        );
    }

    // ------------------------------------------------- the purge hold (Task 9)

    /// Makes `dir` unwritable (0o500) for the guard's life and restores
    /// 0o755 on drop, so a failed assertion still leaves the tempdir
    /// removable.
    struct Unwritable(PathBuf);

    impl Unwritable {
        fn new(dir: &Path) -> Unwritable {
            use std::os::unix::fs::PermissionsExt;
            std::fs::create_dir_all(dir).expect("mkdir");
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o500)).expect("chmod");
            Unwritable(dir.to_path_buf())
        }
    }

    impl Drop for Unwritable {
        fn drop(&mut self) {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
        }
    }

    /// `rounds` × (6000 frames, an instant on every node in `idxs`, agreed).
    /// Returns the last instant.
    fn churn(c: &Cluster, idxs: &[usize], rounds: usize) -> u64 {
        let mut p = 0;
        for _ in 0..rounds {
            let l = submit_settled(c, 6000);
            p = instant_until_complete(c, l, idxs);
            await_agreed(c, idxs, p);
        }
        p
    }

    /// Spec §5.4: the writer's node never purges journal its tier lacks. The
    /// target cannot be created at all (its parent forbids it), so the
    /// writer has no chain — progress `0` — and its node purges NOTHING,
    /// while a voter without a writer purges as usual over the same history.
    #[test]
    fn the_writers_node_does_not_purge_journal_the_tier_lacks() {
        let _g = serialize();
        let tier = tmp_tier();
        let parent = tier.path().join("locked");
        let _lock = Unwritable::new(&parent);
        let cfg = writer_cfg(&parent.join("tier"), 1 << 20, Duration::from_millis(300));
        let mut c = spawn(3, 0, opts("tier-hold", true), |i| {
            (i == 1).then(|| cfg.clone())
        });
        start_sums(&mut c);
        await_single_leader(&c, 30);
        await_capable(&c);
        let all = c.running();
        churn(&c, &all, 3);
        await_until(60, "a non-writer voter purged", || {
            c.node(0).archive_first_base() > 0 && c.node(2).archive_first_base() > 0
        });
        // Give the writer node's persister every chance to purge too.
        std::thread::sleep(Duration::from_secs(1));
        let shared = c.node(1).backup_writer().unwrap();
        assert!(shared.stats.errors(WriterError::Unavailable) >= 1);
        assert_eq!(shared.progress.load(Ordering::Acquire), 0, "no chain");
        assert_eq!(
            c.node(1).archive_first_base(),
            0,
            "the writer's node purged journal its tier lacks (others at {} / {})",
            c.node(0).archive_first_base(),
            c.node(2).archive_first_base()
        );
        assert!(c.node(1).snapshot_set_position() > 0);
        let _keep = c.stop();
    }

    /// Spec §5.4: the hold lets go when space runs low. A writer with a
    /// chain loses its target (scratch area unwritable), the cluster moves
    /// on, and its node holds its journal at the chain end. Low space (the
    /// test seam) releases the hold — counted once — and the node purges
    /// past the chain end. Space and target back: the writer names the gap
    /// and starts a NEW chain at a set at or above its journal's start.
    #[test]
    fn the_hold_lets_go_when_space_runs_low_and_records_a_gap() {
        let _g = serialize();
        let tier = tmp_tier();
        let cfg = writer_cfg(tier.path(), 1 << 20, Duration::from_millis(300));
        let mut c = spawn(3, 0, opts("tier-release", true), |i| {
            (i == 1).then(|| cfg.clone())
        });
        start_sums(&mut c);
        let l = await_single_leader(&c, 30);
        await_capable(&c);
        let all = c.running();
        submit_settled(&c, 2000);
        let p0 = instant_until_complete(&c, l, &all);
        await_agreed(&c, &all, p0);
        let lead = submit_settled(&c, 2000);
        let t0 = c.node(lead).counters().commit.load_acquire();
        await_watermark(&c, l, 60, |w| w.through >= t0);
        let shared = c.node(1).backup_writer().unwrap();

        let guard = Unwritable::new(&tier.path().join(".tmp"));
        let p_last = churn(&c, &all, 3);
        await_until(30, "the writer lost its target", || {
            shared.stats.errors(WriterError::Unavailable) >= 1
        });
        let held_at = shared.progress.load(Ordering::Acquire);
        assert!(held_at >= t0 && held_at < p_last);
        await_until(
            60,
            "the non-writer voters purged past the chain end",
            || c.node(0).archive_first_base() > held_at && c.node(2).archive_first_base() > held_at,
        );
        std::thread::sleep(Duration::from_secs(1));
        assert_eq!(shared.progress.load(Ordering::Acquire), held_at);
        let fb = c.node(1).archive_first_base();
        assert!(
            fb <= held_at,
            "the writer's node purged to {fb}, past its chain end {held_at}"
        );
        // The keep hold is the deferred set the chain reaches next (the
        // writer rescans on its retry ladder, up to 60 s apart).
        await_until(90, "the keep hold names the deferred set", || {
            shared.set_hold.load(Ordering::Acquire) == p_last
        });
        let released = || {
            c.node(1)
                .observability()
                .backup_hold_released
                .load(Ordering::Relaxed)
        };
        assert_eq!(released(), 0);

        c.node(1).set_free_bytes_for_test(1);
        await_until(30, "low space released the hold", || {
            c.node(1).archive_first_base() > held_at
        });
        assert!(shared.hold_released.load(Ordering::Acquire));
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(released(), 1, "one release per episode");
        let fb = c.node(1).archive_first_base();

        drop(guard);
        c.node(1).set_free_bytes_for_test(0);
        let w = await_watermark(&c, l, 120, |w| w.oldest_set > held_at);
        assert!(
            w.oldest_set >= fb,
            "the new chain starts at a set ({}) at or above the journal's start {fb}",
            w.oldest_set
        );
        assert!(
            shared.stats.gaps.load(Ordering::Relaxed) >= 1,
            "gap counted"
        );
        // The next floor move re-arms the hold.
        churn(&c, &all, 1);
        await_until(30, "space back: the hold re-armed", || {
            !shared.hold_released.load(Ordering::Acquire)
        });
        assert_eq!(released(), 1);
        let _keep = c.stop();
    }

    /// Ruling R9a: a writer that stopped for good (a foreign `TIER`)
    /// releases its hold entirely — its node purges like any other.
    #[test]
    fn a_stopped_writer_releases_its_hold() {
        let _g = serialize();
        let tier = tmp_tier();
        open_or_create_tier(&FsTarget::new(tier.path()), "other", 1).unwrap();
        let cfg = writer_cfg(tier.path(), 1 << 20, Duration::from_millis(300));
        let mut c = spawn(3, 0, opts("tier-stopped", true), |i| {
            (i == 1).then(|| cfg.clone())
        });
        start_sums(&mut c);
        await_single_leader(&c, 30);
        await_capable(&c);
        let shared = c.node(1).backup_writer().unwrap();
        await_until(30, "the writer stopped", || {
            shared.stopped.load(Ordering::Acquire)
        });
        let all = c.running();
        churn(&c, &all, 3);
        await_until(60, "the stopped writer's node purged", || {
            c.node(1).archive_first_base() > 0
        });
        let _keep = c.stop();
    }

    /// Ruling R9b: with `retain_sets = 1` (the default), every new agreed
    /// instant retires the previous set from the catalog — so a set above
    /// the chain end must reach the tier BEFORE the next instant, not when
    /// `chunk_bytes` or `quiet_interval` next cuts. Here neither ever cuts
    /// (64 MiB, 60 s): only set boundaries drive the chain, and each of
    /// three instants must land in the tier, on one chain from `S0`.
    #[test]
    fn frequent_instants_each_reach_the_tier() {
        let _g = serialize();
        let tier = tmp_tier();
        let cfg = writer_cfg(tier.path(), 64 << 20, Duration::from_secs(60));
        let mut c = spawn(3, 0, opts("tier-frequent", false), |i| {
            (i == 1).then(|| cfg.clone())
        });
        start_sums(&mut c);
        let l = await_single_leader(&c, 30);
        await_capable(&c);
        let all = c.running();
        submit_settled(&c, 200);
        let s0 = instant_until_complete(&c, l, &all);
        await_agreed(&c, &all, s0);
        await_watermark(&c, l, 30, |w| w.oldest_set == s0);
        let t = FsTarget::new(tier.path());
        let mut sets = Vec::new();
        for _ in 0..3 {
            submit_settled(&c, 2000); // ~256 KiB
            let p = instant_until_complete(&c, l, &all);
            await_agreed(&c, &all, p);
            await_until(20, &format!("set {p} reached the tier"), || {
                list_tier(&t).unwrap().sets.iter().any(|x| x.position == p)
            });
            sets.push(p);
        }
        let last = *sets.last().unwrap();
        let w = await_watermark(&c, l, 30, |w| w.newest_set == last);
        assert_eq!(w.oldest_set, s0, "one chain from S0: {w:?}");
        let shared = c.node(1).backup_writer().unwrap();
        assert_eq!(shared.stats.gaps.load(Ordering::Relaxed), 0, "no gap");
        let _keep = c.stop();
        let listing = list_tier(&t).unwrap();
        for p in std::iter::once(s0).chain(sets.iter().copied()) {
            assert!(
                listing.sets.iter().any(|x| x.position == p),
                "set {p} complete in the tier: {listing:?}"
            );
        }
        assert!(reach_from(&listing.chunks, s0) >= w.through);
        assert!(
            listing
                .chunks
                .iter()
                .all(|&(_, to)| to == s0 || sets.contains(&to)),
            "every chunk ends at a set boundary: {:?} (sets {sets:?})",
            listing.chunks
        );
        let report = verify(&t, true).unwrap();
        assert!(report.findings.is_empty(), "{:?}", report.findings);
    }

    /// What must never move backward while retention runs: the committed
    /// watermark's `through` and the writer's `progress` (the purge cap).
    #[derive(Default)]
    struct Monotone {
        through: u64,
        progress: u64,
        samples: u64,
    }

    impl Monotone {
        fn sample(&mut self, c: &Cluster, l: usize, writer: usize) -> Option<BackupTierEntry> {
            let w = watermark(c.node(l));
            if let Some(w) = &w {
                assert!(
                    w.through >= self.through,
                    "the committed through moved back: {} -> {w:?}",
                    self.through
                );
                self.through = w.through;
            }
            let shared = c.node(writer).backup_writer().unwrap();
            let p = shared.progress.load(Ordering::Acquire);
            assert!(
                p >= self.progress,
                "the writer's progress moved back: {} -> {p}",
                self.progress
            );
            self.progress = p;
            self.samples += 1;
            w
        }

        fn await_wm(
            &mut self,
            c: &Cluster,
            l: usize,
            writer: usize,
            secs: u64,
            mut f: impl FnMut(&BackupTierEntry) -> bool,
        ) -> BackupTierEntry {
            let deadline = Instant::now() + Duration::from_secs(secs);
            loop {
                if let Some(w) = self.sample(c, l, writer)
                    && f(&w)
                {
                    return w;
                }
                assert!(
                    Instant::now() < deadline,
                    "watermark never satisfied; last = {:?}",
                    watermark(c.node(l))
                );
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }

    /// Spec §7: `backup_retain_sets = 1`, committed at genesis. Each new
    /// instant reaches the tier and retention then deletes every older set
    /// (its `SET` first) and the chunks wholly below the newest; the writer
    /// reports the new `oldest_set`, which commits at an unchanged `through`
    /// (ruling R8e) until `oldest_set == newest_set`. Neither the committed
    /// `through` nor the writer's `progress` ever moves back, and the tier
    /// that is left verifies clean.
    #[test]
    fn retention_deletes_old_sets_and_reports_the_new_oldest() {
        let _g = serialize();
        let tier = tmp_tier();
        let mut cfg = writer_cfg(tier.path(), 64 << 20, Duration::from_secs(60));
        cfg.retention_interval = Duration::from_millis(200);
        let mut o = opts("tier-retain", false);
        o.retain_sets = 1;
        let mut c = spawn(3, 0, o, |i| (i == 1).then(|| cfg.clone()));
        start_sums(&mut c);
        let l = await_single_leader(&c, 30);
        await_capable(&c);
        let all = c.running();
        let mut m = Monotone::default();
        submit_settled(&c, 200);
        let s0 = instant_until_complete(&c, l, &all);
        await_agreed(&c, &all, s0);
        m.await_wm(&c, l, 1, 30, |w| w.oldest_set == s0);
        let mut sets = Vec::new();
        for _ in 0..3 {
            submit_settled(&c, 2000);
            let p = instant_until_complete(&c, l, &all);
            await_agreed(&c, &all, p);
            m.await_wm(&c, l, 1, 30, |w| w.newest_set == p);
            sets.push(p);
        }
        let last = *sets.last().unwrap();
        let w = m.await_wm(&c, l, 1, 30, |w| {
            w.oldest_set == last && w.newest_set == last
        });
        let shared = c.node(1).backup_writer().unwrap();
        assert!(
            shared.stats.sets_deleted.load(Ordering::Relaxed) >= 3,
            "S0 and the two older instants were deleted"
        );
        assert!(shared.stats.chunks_deleted.load(Ordering::Relaxed) >= 1);
        assert_eq!(shared.stats.gaps.load(Ordering::Relaxed), 0, "no gap");
        let _keep = c.stop();
        let t = FsTarget::new(tier.path());
        let listing = list_tier(&t).unwrap();
        assert_eq!(
            listing.sets.iter().map(|s| s.position).collect::<Vec<_>>(),
            [last],
            "only the newest set is left: {listing:?}"
        );
        assert!(listing.incomplete_sets.is_empty(), "{listing:?}");
        assert!(
            listing.chunks.iter().all(|&(_, to)| to > last),
            "every chunk wholly below the newest set is gone: {:?}",
            listing.chunks
        );
        assert!(reach_from(&listing.chunks, last) >= w.through);
        let report = verify(&t, true).unwrap();
        assert!(report.findings.is_empty(), "{:?}", report.findings);
        eprintln!(
            "retention: S0={s0} sets={sets:?} final={w:?} samples={}",
            m.samples
        );
    }

    // ------------------------------------------- Task 12: fork, new node

    /// An `OutputHandler` that records every position it is called with.
    struct Outs(Arc<std::sync::Mutex<Vec<u64>>>);

    impl uc_service::RawOutputHandler<SumSm> for Outs {
        async fn on_committed(
            &self,
            position: u64,
            _cmd: &[u8],
            _state: &SumSm,
        ) -> Result<(), uc_service::OutputError> {
            self.0.lock().unwrap().push(position);
            Ok(())
        }
    }

    /// A 3-voter cluster with a writer on node 1, `n1` frames, an agreed
    /// instant, `n2` more frames, and a watermark through the last of them.
    /// Returns `(cluster, tier, set, committed)`.
    fn tiered(app: &'static str, n1: u64, n2: u64) -> (Cluster, tempfile::TempDir, u64, u64) {
        let tier = tmp_tier();
        let cfg = writer_cfg(tier.path(), 1 << 20, Duration::from_millis(500));
        let mut c = spawn(3, 0, opts(app, false), |i| (i == 1).then(|| cfg.clone()));
        start_sums(&mut c);
        let l = await_single_leader(&c, 30);
        await_capable(&c);
        let all = c.running();
        submit_settled(&c, n1);
        let p = instant_until_complete(&c, l, &all);
        await_agreed(&c, &all, p);
        let lead = submit_settled(&c, n2);
        let target = c.node(lead).counters().commit.load_acquire();
        await_watermark(&c, l, 60, |w| w.newest_set == p && w.through >= target);
        (c, tier, p, target)
    }

    /// Spec §8.2, D10, Ruling PF-fork: `tier fork` builds a separate cluster
    /// as of Q. It boots ALONE under a new app id (`force-single-member` on
    /// the result), its cluster FSM's committed membership follows the
    /// kernel's single voter (the leader appends the `Membership` command the
    /// force left owed), and the outputs that already happened at or below Q
    /// are never replayed: one new command, one `on_committed` call.
    #[test]
    fn a_fork_boots_alone_under_its_new_app_id_and_replays_no_outputs() {
        let _g = serialize();
        let (c, tier, _p, target) = tiered("tier-fork-src", 300, 1000);
        let old_version = c.node(0).config_version();
        let _keep = c.stop();

        let t = FsTarget::new(tier.path());
        let (_, _, through) = watermark_of(&list_tier(&t).unwrap()).expect("a watermark");
        assert!(through >= target);
        let fresh = tempfile::Builder::new()
            .prefix("uc2-tier-fork-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .expect("tempdir");
        let fd = fresh.path().join("f0");
        let f =
            uc_node::tier::materialize::fork(&t, Some(through), &fd, "fork", 0, 3).expect("fork");
        assert_eq!(f.materialized.q, through);
        assert_eq!(f.materialized.mode, Mode::Fork { node_id: 0 });
        assert_eq!(f.force.dropped_peers, vec![1, 2]);
        let rc = uc_node::recovery::recovered_config(&fd).expect("recovered config");
        assert_eq!(
            rc.voters.iter().map(|v| v.0).collect::<Vec<_>>(),
            vec![0],
            "the kernel's config is the single voter"
        );
        assert_eq!(rc.version, old_version + 1);

        // Boot it alone, under the new app id, on a fresh address.
        let sock = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr = sock.local_addr().unwrap();
        let cfg = NodeConfig {
            id: 0,
            members: vec![(0, addr)],
            learners: Vec::new(),
            bind: addr,
            instance_dir: fd.clone(),
            app_id: "fork".into(),
            buffer_bytes: 1 << 22,
            max_payload: 256,
            admission_bytes_default: 256 * 1024,
            settings_genesis: Settings::genesis_default(),
            force_jumbo_frames: false,
            election_timeout_min_ns: 150_000_000,
            election_timeout_max_ns: 300_000_000,
            seed: 0xF0F0,
            faults: FaultConfig::default(),
            purge: PurgePolicy::Disabled,
            journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
            crypto: uc_node::CryptoConfig::Disabled,
            services: ServicesConfig::single(<SumSm as uc_service::RawStateMachine>::NAME),
        };
        let node = Node::start_with(
            cfg,
            StartOpts {
                socket: Some(sock),
                admin: AdminPolicy::Filesystem,
                backup: None,
            },
        )
        .expect("start the fork");
        // Fix round 2: an admin membership change issued the moment the
        // fork boots must not race ahead of the reconcile — the leader
        // answers retry until the forced config is in the FSM, then the
        // learner chains on top of it (`admin_ok` loops on retry).
        let learner = UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let (lip, lport) = match learner {
            SocketAddr::V4(a) => (u32::from(*a.ip()), a.port()),
            SocketAddr::V6(_) => unreachable!(),
        };
        let fcnc = CncPage::open_file(&fd.join("cnc2.dat"), "fork").expect("fork cnc");
        let v = admin_ok(&fcnc, 1 /* AddLearner */, 7, lip, lport);
        assert_eq!(
            v,
            old_version + 2,
            "the learner chains on the forced config"
        );
        await_until(30, "the fork never led alone", || node.can_serve());
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let svc = uc_service::ServiceBuilder::new(svc_cfg(&fd, "fork"), SumSm::on(0))
            .raw_output_handler(Outs(seen.clone()))
            .start()
            .expect("service on the fork");
        await_until(30, "the fork's FSM membership follows the kernel", || {
            let m = node.cluster_view().membership();
            m.voters.len() == 1
                && m.voters[0].0 == 0
                && m.learners.iter().map(|l| l.0).collect::<Vec<_>>() == [7]
                && m.version == old_version + 2
        });
        submit_frames(&node, 1);
        await_until(30, "the one new output was delivered", || {
            !seen.lock().unwrap().is_empty()
        });
        // Give a stray replay time to show itself.
        std::thread::sleep(Duration::from_millis(500));
        let calls = seen.lock().unwrap().clone();
        assert_eq!(
            calls.len(),
            1,
            "exactly the new command's output: {calls:?}"
        );
        assert!(
            calls[0] >= through,
            "no output at or below Q={through} is replayed: {calls:?}"
        );
        // The membership holds across an instant (the FSM's artifact).
        let p = loop {
            match node.command_snapshot(false) {
                Ok(p) => break p,
                Err(uc_node::SnapshotRefusal::Retry) => {
                    std::thread::sleep(Duration::from_millis(5))
                }
                Err(e) => panic!("snapshot refused on the fork: {e}"),
            }
        };
        await_until(30, "the fork's instant completed", || {
            node.snapshot_set_position() >= p
        });
        assert_eq!(node.cluster_view().membership().voters.len(), 1);
        svc.stop();
        node.stop();
        let rc = uc_node::recovery::recovered_config(&fd).expect("recovered config");
        assert_eq!(rc.voters.iter().map(|v| v.0).collect::<Vec<_>>(), vec![0]);
        assert_eq!(rc.learners.iter().map(|v| v.0).collect::<Vec<_>>(), vec![7]);
        assert_eq!(rc.version, old_version + 2);
        drop(fresh);
    }

    /// Spec §8.2: a fork under the tier's own app id is refused by name and
    /// builds nothing.
    #[test]
    fn fork_refuses_the_same_app_id() {
        let _g = serialize();
        let (c, tier, _p, _target) = tiered("tier-fork-same", 50, 50);
        let _keep = c.stop();
        let t = FsTarget::new(tier.path());
        let fresh = tempfile::Builder::new()
            .prefix("uc2-tier-fork-same-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .expect("tempdir");
        let fd = fresh.path().join("f");
        let e = uc_node::tier::materialize::fork(&t, None, &fd, "tier-fork-same", 0, 1)
            .expect_err("same app id refused");
        assert!(e.to_string().contains("fork_same_app_id"), "{e}");
        assert!(!fd.exists(), "nothing was built");
        let e = uc_node::tier::materialize::fork(&t, None, &fd, "other", 42, 1)
            .expect_err("an id outside the membership is refused");
        assert!(e.to_string().contains("fork_node_unknown"), "{e}");
        assert!(!fd.exists(), "nothing was built");
    }

    /// The `uc2ctl` admin flow minus the bin (reconfig.rs's helper).
    fn admin_ok(cnc: &CncPage, op: u32, id: u32, ip: u32, port: u16) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let old = cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0);
            let seq = old + 1;
            cnc.write_admin_req(&uc_log::cnc::AdminReq {
                seq,
                nonce: rand::random::<u64>(),
                op,
                id,
                ip,
                port,
            });
            let resp = loop {
                if let Some(r) = cnc.read_admin_resp(seq) {
                    break r;
                }
                assert!(Instant::now() < deadline, "admin op {op} timed out");
                std::thread::yield_now();
            };
            match resp.status {
                0 => return resp.version,
                2 => {}
                1 if matches!(resp.reason, 2 | 3 | 10) => {}
                _ => panic!(
                    "admin op {op} on {id} refused: status={} reason={}",
                    resp.status, resp.reason
                ),
            }
            assert!(Instant::now() < deadline, "admin op {op} never accepted");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Spec §8.2 + the R8a carry: a node seeded from the tier under a NEW id
    /// joins a LIVE, appending cluster as a learner, catches up, rides a
    /// leader change, and once promoted its durable reports carry the quorum
    /// (one other voter is down) — no truncation, no wipe, no unattested
    /// report anywhere. Its term map starts at (term, S) and its padding
    /// bodies are zero where peers hold leftover bytes; neither may matter.
    #[test]
    fn a_new_node_seeded_from_the_tier_joins_and_catches_up() {
        let _g = serialize();
        let (mut c, tier, p, _target) = tiered("tier-new-node", 300, 1500);
        let t = FsTarget::new(tier.path());
        let fresh = tempfile::Builder::new()
            .prefix("uc2-tier-new-node-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .expect("tempdir");
        let nd = fresh.path().join("n9");
        let m = materialize(&t, None, &nd, Mode::NewNode { node_id: 9 }, 5).expect("seed");
        assert!(m.start_set >= p);

        // The cluster keeps appending while node 9 is added and boots.
        let l = await_single_leader(&c, 30);
        submit_frames(c.node(l), 500);
        let sock9 = UdpSocket::bind("127.0.0.1:0").expect("bind 9");
        let addr9 = sock9.local_addr().unwrap();
        let (ip9, port9) = match addr9 {
            SocketAddr::V4(a) => (u32::from(*a.ip()), a.port()),
            SocketAddr::V6(_) => unreachable!(),
        };
        let cnc_l = c.cnc(l);
        admin_ok(&cnc_l, 1 /* AddLearner */, 9, ip9, port9);
        let voters: Vec<(NodeId, SocketAddr)> =
            (0..3).map(|i| (i as NodeId, c.nodes[i].addr)).collect();
        let d9 = nd.clone();
        let app = c.app;
        let cfg9: Box<dyn Fn() -> NodeConfig> = Box::new(move || NodeConfig {
            id: 9,
            members: voters.clone(),
            learners: Vec::new(),
            bind: addr9,
            instance_dir: d9.clone(),
            app_id: app.into(),
            buffer_bytes: 1 << 22,
            max_payload: 256,
            admission_bytes_default: 256 * 1024,
            settings_genesis: Settings::genesis_default(),
            force_jumbo_frames: false,
            election_timeout_min_ns: 150_000_000,
            election_timeout_max_ns: 300_000_000,
            seed: 0x9999,
            faults: FaultConfig::default(),
            purge: PurgePolicy::Disabled,
            journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
            crypto: uc_node::CryptoConfig::Disabled,
            services: ServicesConfig::single(<SumSm as uc_service::RawStateMachine>::NAME),
        });
        let mut h9 = NodeH {
            addr: addr9,
            dir: nd.clone(),
            is_learner: true,
            cfg: cfg9,
            backup: None,
            sock: Some(sock9),
            node: None,
            svc: None,
        };
        h9.start();
        h9.start_service(app, 9);
        c.nodes.push(h9);
        let n9 = c.nodes.len() - 1;
        assert_eq!(
            c.node(n9).archive_first_base(),
            m.start_set,
            "journal_first stays S"
        );

        // Load while it catches up, then a leader change mid-catch-up.
        submit_frames(c.node(l), 1000);
        c.nodes[l].stop();
        let l2 = submit_following(&c, 1000);
        assert_ne!(l2, l);
        let commit = c.node(l2).counters().commit.load_acquire();
        await_until(60, "node 9 caught up to the new leader's commit", || {
            c.node(n9).counters().durable.load_acquire() >= commit
        });

        // Promote it: with node `l` down, 3 of the 4 voters are live, so
        // every commit from here needs node 9's (attested) durable report.
        admin_ok(&c.cnc(l2), 2 /* PromoteLearner */, 9, 0, 0);
        await_until(30, "node 9 adopted its promotion", || {
            c.node(n9).config_version() >= c.node(l2).config_version()
        });
        let l3 = submit_following(&c, 500);
        let commit = c.node(l3).counters().commit.load_acquire();
        await_until(60, "node 9 holds the new commits", || {
            c.node(n9).counters().durable.load_acquire() >= commit
        });
        let total = query_total(&c, l3);
        // Node 9 is a follower (a linearizable read is leader-only), so read
        // its applied state and wait for it to reach the leader's total.
        await_until(30, "node 9 serves the same state", || {
            let client = uc_client::Client::connect(&c.nodes[n9].dir, c.app).expect("client");
            let raw: Result<[u8; 8], _> = client.query_snapshot(&());
            client.shutdown();
            raw.is_ok_and(|r| u64::from_le_bytes(r) == total)
        });

        assert_eq!(
            c.node(n9).archive_first_base(),
            m.start_set,
            "journal_first stays S"
        );
        assert_eq!(c.node(n9).truncations(), 0, "node 9 truncated nothing");
        assert_eq!(c.node(n9).wipes(), 0, "node 9 was never wiped");
        for i in c.running() {
            assert_eq!(
                c.node(i).reports_unattested(),
                0,
                "node {i} declined an unattested report"
            );
        }
        let _keep = c.stop();
        drop(fresh);
    }
}
