// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Plan B3 T6 — live snapshot reports, end to end on a real cluster.
//!
//! Tasks 1-5 built the pipeline one seam at a time, each proven in isolation:
//! the builder's `artifact_hash` word, the local set-complete edge that sends a
//! `SNAP_REPORT`, the leader's collector, the `CLUSTER kind = 5` append, and the
//! apply-side record. This file is the first place all of it runs at once —
//! three real nodes over loopback UDP, three real snapshot-capable services,
//! one commanded coordinated instant — and asks the only question the feature
//! exists to answer: **did every node's artifact for that instant come out the
//! same, and if not, who differs?**
//!
//! Three shapes:
//!
//! * [`three_voters_agree`] — the healthy case. Every node commits the same
//!   record, `verdict().agreed` holds, and `uc2_snapshot_hash_mismatch` reads
//!   `0` on every node.
//! * [`one_divergent_node_is_named`] — node 2 runs a state machine whose
//!   `freeze()` appends its own node id to the image (a deliberate
//!   nondeterminism, same FSM identity, different bytes). The verdict names
//!   node 2 as the minority, the gauge reads `1` cluster-wide, and the
//!   `snapshot_hash_diverged` record names the row and the node.
//! * [`a_learner_reports_but_does_not_count_toward_quorum`] — 2 voters + 1
//!   learner. The append is paced by the VOTERS' reports, not the learner's,
//!   and it does not wait out the 5 s `SNAP_REPORT_TIMEOUT_NS` fallback.
//!
//! All three turn on ruling R-B3-1's release rule: the leader holds a row's
//! collection until EVERY VOTER has reported it, or until that timeout — so a
//! three-voter record names three nodes, deterministically, and the divergent
//! replica in (2) cannot be the one the trigger left out.
//!
//! Sizing and shape are `learner.rs`'s (journals under `CARGO_TARGET_TMPDIR`,
//! 4 MiB no-wrap ring, 150-300 ms election timeouts, whole-box serialization);
//! its harness is copied here rather than shared, since `uc_node/tests/` has no
//! `common/` module and a test file must not be `include!`d.

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use uc_consensus::election::NodeId;
use uc_log::cnc::CncPage;
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig};
use uc_protocol::v2::cnc::CNC_SVC_STATUS_SNAPSHOT_CAPABLE;
use uc_protocol::v2::upgrade::{SnapshotReport, verdict};

/// The app name every fixture here shares — it keys the IPC attach, so the
/// node's `app_id` and the service's `ServiceConfig` must agree on it.
const APP: &str = "snapreport";
const PAYLOAD: usize = 96;

static TEST_LOCK: Mutex<()> = Mutex::new(());

/// Whole-box serialization: every test in this file runs three busy-spin nodes
/// plus three services, and [`one_divergent_node_is_named`] swaps the
/// process-global obs sink, which no sibling test may be running under.
fn serialize() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// The process-global obs sink, held for a scope. `capture_for_tests` swaps
/// the sink and `stderr_for_tests` swaps it back; doing the swap-back by hand
/// at the end of a test means a panicking assertion — the case the capture
/// exists to diagnose — leaves the whole process writing into a buffer no one
/// reads, so every later test in this binary loses its records. A drop guard
/// restores it on the panic path too.
struct ObsCapture(Arc<Mutex<Vec<u8>>>);

impl ObsCapture {
    fn take() -> Self {
        Self(uc_node::obs::log::capture_for_tests())
    }

    /// The capture so far, as text. Takes the lock briefly and copies, so an
    /// assertion built from it cannot hold the sink's lock while it panics.
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap_or_else(|e| e.into_inner())).into_owned()
    }
}

impl Drop for ObsCapture {
    fn drop(&mut self) {
        uc_node::obs::log::stderr_for_tests();
    }
}

/// `uc2ctl snapshot`, in process (coordinated-snapshot spec §5.5): command a
/// coordinated instant and return its position **P**, polling through the
/// `retry` window a leader legitimately answers while it has the role but not
/// yet an appender. Duplicated per test binary, like `serialize`.
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

struct NodeH {
    id: NodeId,
    instance_dir: PathBuf,
    is_learner: bool,
    node: Option<Node>,
}

impl NodeH {
    fn n(&self) -> &Node {
        self.node.as_ref().expect("node stopped")
    }
    fn is_leader(&self) -> bool {
        self.node.as_ref().is_some_and(|n| n.is_leader())
    }
    fn can_serve(&self) -> bool {
        self.node.as_ref().is_some_and(|n| n.can_serve())
    }
    fn commit(&self) -> u64 {
        self.n().counters().commit.load_acquire()
    }
    fn try_submit(&self, payload: Vec<u8>) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match self.n().submit(payload.clone()) {
                Ok(()) => return,
                Err(uc_node::SubmitError::Full) => {
                    assert!(Instant::now() < deadline, "ingress stayed full");
                    std::thread::yield_now();
                }
                Err(e) => panic!("submit to serving leader: {e:?}"),
            }
        }
    }
    fn stop(&mut self) {
        if let Some(node) = self.node.take() {
            node.stop();
        }
    }
}

fn make_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    learners: Vec<(NodeId, SocketAddr)>,
    instance_dir: PathBuf,
    seed: u64,
    addr: SocketAddr,
    services: uc_node::ServicesConfig,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        learners,
        bind: addr,
        instance_dir,
        app_id: APP.into(),
        buffer_bytes: 1 << 22,
        max_payload: 256,
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
        force_jumbo_frames: false,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed,
        faults: FaultConfig::default(),
        purge: uc_node::PurgePolicy::Disabled,
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services,
    }
}

fn seed_for(i: usize) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

struct Cluster {
    _dir: tempfile::TempDir,
    nodes: Vec<NodeH>,
}

/// Bind `n_voters` voter sockets + `n_learners` learner sockets, then start
/// **every node** with the full (members, learners) maps — and only then does
/// the caller attach services.
///
/// The two passes are not incidental (plan B3 T5): `services_declared` is
/// published once a node knows its leader and its cluster walk has reached
/// commit, so a service attached before its node can elect would sit in the
/// builder's bounded `NodeBooting` wait for no reason.
fn spawn_cluster(n_voters: usize, n_learners: usize, services: uc_node::ServicesConfig) -> Cluster {
    let dir = tempfile::Builder::new()
        .prefix("uc2-snapreport-")
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
    let members: Vec<(NodeId, SocketAddr)> = all[..n_voters].to_vec();
    let learners: Vec<(NodeId, SocketAddr)> = all[n_voters..].to_vec();

    let mut nodes = Vec::with_capacity(total);
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = all[i].1;
        let instance_dir = dir.path().join(format!("n{i}"));
        let cfg = make_config(
            i as NodeId,
            members.clone(),
            learners.clone(),
            instance_dir.clone(),
            seed_for(i),
            addr,
            services,
        );
        let node = Node::start_with_socket(cfg, sock).expect("start");
        nodes.push(NodeH {
            id: i as NodeId,
            instance_dir,
            is_learner: i >= n_voters,
            node: Some(node),
        });
    }
    Cluster { _dir: dir, nodes }
}

fn await_until(secs: u64, msg: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "{msg}");
        std::thread::yield_now();
    }
}

/// Exactly one serving leader among the VOTERS; a learner must never serve.
fn await_single_leader(nodes: &[NodeH], secs: u64) -> usize {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        for h in nodes.iter().filter(|h| h.is_learner) {
            assert!(
                !h.can_serve() && !h.is_leader(),
                "learner {} became a leader",
                h.id
            );
        }
        let serving: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].can_serve()).collect();
        assert!(serving.len() <= 1, "split-brain: {serving:?} all serve");
        if serving.len() == 1 {
            let i = serving[0];
            assert!(nodes[i].is_leader(), "serving node {i} not flagged leader");
            assert!(!nodes[i].is_learner, "a learner must never lead");
            return i;
        }
        assert!(Instant::now() < deadline, "no single leader elected");
        std::thread::yield_now();
    }
}

fn submit_n(node: &NodeH, base: u64, n: u64) {
    for i in base..base + n {
        let mut p = vec![0u8; PAYLOAD];
        p[..8].copy_from_slice(&i.to_le_bytes());
        node.try_submit(p);
    }
}

// ---------------------------------------------------------------------------
// The state machines
// ---------------------------------------------------------------------------

/// A snapshot-capable RAW state machine (bytes in, bytes out — no serde, so the
/// fixtures submit plain byte payloads through `Node::submit`). `freeze` pins
/// `(total, last_applied)`, which is deterministic across replicas: every node
/// applies the same frames to the same instant, so every node's image — and
/// therefore its artifact hash — is byte-identical. That is the property
/// [`three_voters_agree`] asserts and [`DivergentSum`] breaks.
#[derive(Default)]
struct SumSm {
    total: u64,
    last: Option<u64>,
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
        assert!(buf.len() >= 16, "a SumSm artifact is at least 16 bytes");
        self.total = u64::from_le_bytes(buf[..8].try_into().unwrap());
        // Coordinated-snapshot spec §5.2: the tag is the instant P, an
        // EXCLUSIVE frontier — restore the cursor the artifact recorded (the
        // second 8 bytes), never the tag.
        self.last = Some(u64::from_le_bytes(buf[8..16].try_into().unwrap()));
        Ok(position)
    }
}

/// [`SumSm`] with ONE difference: `freeze()` appends this node's id to the
/// image. Same FSM identity — the same `NAME`, the same (default) `VERSION` —
/// so the snapshot session's positional identity check is satisfied and
/// nothing is refused for a reason other than the one under test. Only the
/// artifact BYTES differ, which is exactly the class of nondeterminism a
/// snapshot report exists to catch: two replicas that agree on every frame and
/// still write different images.
struct DivergentSum {
    inner: SumSm,
    node_id: u64,
}

impl DivergentSum {
    fn new(node_id: u64) -> DivergentSum {
        DivergentSum {
            inner: SumSm::default(),
            node_id,
        }
    }
}

impl uc_service::RawStateMachine for DivergentSum {
    const NAME: &'static str = <SumSm as uc_service::RawStateMachine>::NAME;
    const VERSION: u32 = <SumSm as uc_service::RawStateMachine>::VERSION;

    fn apply(&mut self, ctx: &mut uc_service::ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
        self.inner.apply(ctx, cmd, out)
    }
    fn query(&self, q: &[u8], out: &mut Vec<u8>) {
        self.inner.query(q, out)
    }
    fn last_applied(&self) -> Option<u64> {
        self.inner.last_applied()
    }
}

impl uc_service::SnapshotStateMachine for DivergentSum {
    type SnapshotHandle = Vec<u8>;
    fn freeze(&self) -> Result<(Vec<u8>, u64), uc_service::SnapshotError> {
        let (mut buf, pos) = self.inner.freeze()?;
        buf.extend_from_slice(&self.node_id.to_le_bytes());
        Ok((buf, pos))
    }
    fn stream_snapshot(
        handle: Vec<u8>,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc_service::SnapshotError> {
        SumSm::stream_snapshot(handle, dst)
    }
    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc_service::SnapshotError> {
        self.inner.install_snapshot(position, src)
    }
}

/// Snapshot-CAPABLE only (coordinated-snapshot spec §5.2's cnc status bit):
/// there is no byte cadence, so this row freezes exactly at the instants the
/// leader commands.
fn start_sum_service(dir: &Path) -> uc_service::Service<SumSm> {
    uc_service::ServiceBuilder::new(uc_service::ServiceConfig::new(dir, APP), SumSm::default())
        .start_with_snapshots()
        .expect("service start")
}

fn start_divergent_service(dir: &Path, node_id: u64) -> uc_service::Service<DivergentSum> {
    uc_service::ServiceBuilder::new(
        uc_service::ServiceConfig::new(dir, APP),
        DivergentSum::new(node_id),
    )
    .start_with_snapshots()
    .expect("service start")
}

// ---------------------------------------------------------------------------
// Shared fixture steps
// ---------------------------------------------------------------------------

fn open_cncs(c: &Cluster) -> Vec<std::sync::Arc<CncPage>> {
    c.nodes
        .iter()
        .map(|h| CncPage::open_file(&h.instance_dir.join("cnc2.dat"), APP).expect("open cnc"))
        .collect()
}

/// Elect a leader, wait out every row's ATTACH (the capability bit is what
/// makes an instant commandable at all — spec §5.5 refuses `48` without it),
/// then drive `n` commands and wait until every node has applied them.
fn settle(c: &Cluster, cncs: &[std::sync::Arc<CncPage>], n: u64) -> usize {
    let leader = await_single_leader(&c.nodes, 30);
    await_until(
        30,
        "every row published the snapshot-capability bit",
        || {
            cncs.iter().all(|p| {
                p.service_slot(0).status.load_acquire() & CNC_SVC_STATUS_SNAPSHOT_CAPABLE != 0
            })
        },
    );
    submit_n(&c.nodes[leader], 0, n);
    await_until(30, "every node applied the pre-instant load", || {
        let commit = c.nodes[leader].commit();
        commit > 0
            && cncs
                .iter()
                .all(|p| p.service_slot(0).applied.load_acquire() >= commit)
    });
    leader
}

/// Poll every node until each holds the COMMITTED report for row 0 at `p`, and
/// return the one copy they all hold. The record is cluster state, so the wait
/// is for all of them, not just the leader that appended it.
fn await_report_everywhere(c: &Cluster, p: u64, secs: u64) -> SnapshotReport {
    await_until(
        secs,
        &format!("every node committed the row-0 snapshot report for instant {p}"),
        || {
            c.nodes
                .iter()
                .all(|h| h.n().snapshot_report(0).is_some_and(|r| r.position == p))
        },
    );
    let reports: Vec<SnapshotReport> = c
        .nodes
        .iter()
        .map(|h| h.n().snapshot_report(0).expect("just awaited"))
        .collect();
    for (i, r) in reports.iter().enumerate() {
        assert_eq!(
            r, &reports[0],
            "node {i} holds a DIFFERENT row-0 report than node 0 — the record is committed \
             cluster state and must be identical everywhere: {r:?} vs {:?}",
            reports[0]
        );
    }
    reports.into_iter().next().expect("non-empty cluster")
}

fn metrics(node: &Node) -> String {
    uc_node::obs::metrics::render_prometheus(&node.observability())
}

fn stop(mut c: Cluster) {
    for h in c.nodes.iter_mut() {
        h.stop();
    }
}

// ---------------------------------------------------------------------------
// (1) the healthy case
// ---------------------------------------------------------------------------

/// Plan B3 (spec §6.5.2), the whole pipeline on three voters that agree.
///
/// Three real snapshot-capable services apply the same frames to the same
/// commanded instant, so all three artifacts are byte-identical. The record
/// that reaches the log must say so: every reporting node's hash equal,
/// `verdict().agreed`, no minority — and `uc2_snapshot_hash_mismatch` reading
/// `0` on every node, since the gauge is recomputed from this same record at
/// scrape time.
#[test]
fn three_voters_agree() {
    let _g = serialize();
    let c = spawn_cluster(3, 0, uc_node::ServicesConfig::single("sum"));
    let svcs: Vec<uc_service::Service<SumSm>> = c
        .nodes
        .iter()
        .map(|h| start_sum_service(&h.instance_dir))
        .collect();
    let cncs = open_cncs(&c);
    let leader = settle(&c, &cncs, 400);

    let p = command_instant(c.nodes[leader].n());
    let report = await_report_everywhere(&c, p, 60);

    assert_eq!(report.row, 0);
    assert_eq!(report.position, p, "the record names the commanded instant");
    assert_eq!(
        report
            .hashes
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<u32>>(),
        vec![0u32, 1, 2],
        "every voter is attested, by node id — the leader holds the collection until \
         all three have reported (ruling R-B3-1): {report:?}"
    );
    let v = verdict(&report);
    assert!(
        v.agreed,
        "three deterministic replicas froze at the same instant and must hash alike: {report:?}"
    );
    assert!(
        v.minority.is_empty(),
        "an agreed report names nobody: {v:?}"
    );
    assert_eq!(
        v.majority_hash,
        Some(report.hashes[0].1),
        "the agreed hash IS the majority hash: {report:?}"
    );

    for h in &c.nodes {
        let text = metrics(h.n());
        assert!(
            text.contains("uc2_snapshot_hash_mismatch{service=\"sum\",row=\"0\"} 0"),
            "node {} must report no mismatch:\n{text}",
            h.id
        );
    }

    for s in svcs {
        s.stop();
    }
    stop(c);
}

// ---------------------------------------------------------------------------
// (2) the divergent node
// ---------------------------------------------------------------------------

/// The case the feature exists for: one replica writes a different image.
///
/// Node 2 runs [`DivergentSum`] — same FSM identity, an extra eight bytes in
/// the frozen image — so its artifact hashes differently while nothing else
/// about the cluster changes. Three things must follow, and they are three
/// different readers of the SAME committed record: the verdict names node 2
/// (and only node 2) as the minority; `uc2_snapshot_hash_mismatch` reads `1`
/// on EVERY node, not just the leader's; and the `uc2-cluster` agent's
/// `snapshot_hash_diverged` record names the row and the node.
///
/// The cheap red twin is to give node 2 a plain [`SumSm`]: the report then
/// agrees, `minority` is empty, and all three assertions fail.
#[test]
fn one_divergent_node_is_named() {
    let _g = serialize();
    let _sink = ObsCapture::take();
    let c = spawn_cluster(3, 0, uc_node::ServicesConfig::single("sum"));
    let honest: Vec<uc_service::Service<SumSm>> = c.nodes[..2]
        .iter()
        .map(|h| start_sum_service(&h.instance_dir))
        .collect();
    let rogue = start_divergent_service(&c.nodes[2].instance_dir, c.nodes[2].id as u64);
    let cncs = open_cncs(&c);
    let leader = settle(&c, &cncs, 400);

    let p = command_instant(c.nodes[leader].n());
    let report = await_report_everywhere(&c, p, 60);

    assert_eq!(
        report.hashes.len(),
        3,
        "all three voters must be attested — two hashes out of three name no majority \
         at all, and so no minority either: {report:?}"
    );
    let v = verdict(&report);
    assert!(
        !v.agreed,
        "node 2 froze a different image and the record must say so: {report:?}"
    );
    assert_eq!(
        v.minority,
        vec![2u32],
        "node 2, and only node 2, is the minority: {report:?} {v:?}"
    );
    let majority = v.majority_hash.expect("two of three agree");
    assert_eq!(
        report
            .hashes
            .iter()
            .filter(|(_, h)| *h == majority)
            .map(|(id, _)| *id)
            .collect::<Vec<u32>>(),
        vec![0u32, 1],
        "the majority hash is the one nodes 0 and 1 hold: {report:?}"
    );

    for h in &c.nodes {
        let text = metrics(h.n());
        assert!(
            text.contains("uc2_snapshot_hash_mismatch{service=\"sum\",row=\"0\"} 1"),
            "node {} must report exactly one divergent replica:\n{text}",
            h.id
        );
    }

    // The obs record. Every node in this process applies the same frame and
    // emits its own copy into the shared capture, so this asserts on CONTENT —
    // row 0, node 2 — and not on how many copies landed.
    let text = _sink.text();
    assert!(
        text.lines().any(|l| {
            l.contains(r#""event":"snapshot_hash_diverged""#)
                && l.contains(r#""row":0"#)
                && l.contains(r#""node":2"#)
        }),
        "no snapshot_hash_diverged record named row 0 / node 2; \
         the {} snapshot_hash_diverged record(s) in the capture were:\n{}",
        text.lines()
            .filter(|l| l.contains(r#""event":"snapshot_hash_diverged""#))
            .count(),
        text.lines()
            .filter(|l| l.contains(r#""event":"snapshot_hash_diverged""#))
            .take(8)
            .collect::<Vec<&str>>()
            .join("\n")
    );

    for s in honest {
        s.stop();
    }
    rogue.stop();
    stop(c);
}

// ---------------------------------------------------------------------------
// (3) the learner
// ---------------------------------------------------------------------------

/// Spec §6.5.2 under ruling R-B3-1: the append is paced by the VOTERS'
/// reports, and a learner is not one of them.
///
/// Two voters and one learner, all three with a real snapshot-capable service,
/// so all three genuinely report. The release waits for both VOTERS and for
/// neither more nor less — whether the learner's hash arrived in time to ride
/// the same record is a race on the wire and is deliberately not asserted, so
/// the record carries at least the two voters and possibly three entries.
/// What IS asserted is that it went in because those two voters reported, NOT
/// because the collection timed out: the timeout is the fallback for a node
/// that never reports, and if it were doing the work here the record would
/// arrive about `SNAP_REPORT_TIMEOUT_NS` (5 s) after the instant instead of
/// within milliseconds of it.
#[test]
fn a_learner_reports_but_does_not_count_toward_quorum() {
    let _g = serialize();
    // Taken before the instant: the discriminator below is the leader's own
    // `snapshot_report_appended` record (final review, minor 12).
    let sink = ObsCapture::take();
    let c = spawn_cluster(2, 1, uc_node::ServicesConfig::single("sum"));
    let svcs: Vec<uc_service::Service<SumSm>> = c
        .nodes
        .iter()
        .map(|h| start_sum_service(&h.instance_dir))
        .collect();
    let cncs = open_cncs(&c);
    let leader = settle(&c, &cncs, 400);
    assert!(leader < 2, "the leader must be a voter, got {leader}");

    let p = command_instant(c.nodes[leader].n());
    let report = await_report_everywhere(&c, p, 60);

    assert!(
        report.hashes.len() >= 2,
        "the record must attest at least the two voters that paced it: {report:?}"
    );
    let voters: Vec<u32> = report
        .hashes
        .iter()
        .map(|(id, _)| *id)
        .filter(|id| *id < 2)
        .collect();
    assert_eq!(
        voters,
        vec![0u32, 1],
        "both voters must be in the record — they are what released it: {report:?}"
    );
    assert!(
        verdict(&report).agreed,
        "every replica here is deterministic: {report:?}"
    );

    // The voters released it, not the clock — asserted on the property
    // itself. The leader's record names which mechanism fired
    // (`by="all_voters"` vs `by="timeout"`), so this says exactly what the
    // old 3 s wall-clock bound under `SNAP_REPORT_TIMEOUT_NS` was trying to
    // say, without being a timing assertion in a test that also brings up
    // three busy-spin nodes and three services (final review, minor 12).
    let text = sink.text();
    let line = text
        .lines()
        .find(|l| {
            l.contains(r#""event":"snapshot_report_appended""#)
                && l.contains(&format!(r#""position":{p}"#))
        })
        .unwrap_or_else(|| {
            panic!("the leader appended no record for the instant {p}: {text}");
        });
    assert!(
        line.contains(r#""by":"all_voters""#),
        "the record was released by the two voters' reports, not by the 5 s \
         SNAP_REPORT_TIMEOUT_NS fallback: {line}"
    );

    for s in svcs {
        s.stop();
    }
    stop(c);
}
