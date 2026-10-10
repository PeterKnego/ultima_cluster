// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Catalog spec §10.5 — the snapshot catalog, end to end on a real cluster.
//!
//! Every test here drives real nodes over loopback UDP with real
//! snapshot-capable services attached, commands real coordinated instants,
//! and reads the result through the surfaces an operator (or project 2's
//! chooser) reads: the committed catalog in `Node::cluster_view()`, the
//! leader's soft table through [`CatalogQuery`], the node's purge floor
//! (`node_snapshot_floor`, `archive_first_base`) and the `uc2_catalog_*`
//! gauges. Each asserts a RULE, not an incidental reading:
//!
//! 1. a diverged set completes but never moves anybody's floor, and a
//!    joiner is served the previous AGREED set;
//! 2. a stalled set stays `Commanded` and is named by `stalled()`, and the
//!    next instant completes regardless;
//! 3. `retain_sets = 2` retires the oldest agreed set — from the catalog and
//!    from every node's disk — while the journal follows the newest agreed
//!    set and a pinned origin is kept beside the retained sets, never
//!    counted toward `retain_sets` (ruling R21);
//! 4. the flag-day window (a pre-catalog `v3` cluster artifact on disk) is
//!    `Empty`, serves a joiner, keeps the set the node stands on, and ends
//!    at the first instant that completes (ruling R26; here it also agrees);
//! 5. a stopped node leaves `holders()` after the soft staleness timeout and
//!    a fetch routed by `holders()` lands from the surviving holder;
//! 6. on a learner-only cluster the voters know the cluster floor yet purge
//!    nothing until a fetch lands the set on them.
//!
//! The harness is copied from `learner.rs` (each integration test file is its
//! own binary; `uc_node/tests/` has no shared module), trimmed and widened to
//! start a node later than its peers. Sizing is `learner.rs`'s: journals on
//! disk under `CARGO_TARGET_TMPDIR`, 150–300 ms election timeouts (so the
//! soft table's staleness timeout is 3 × 300 ms = 900 ms), whole-box
//! serialization; a purging fixture uses the small ring + 64 KiB segments of
//! `fresh_learner_joins_a_purged_leader_via_snapshot_session`, so a fresh
//! joiner's NAK from 0 falls into the purged prefix.

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use uc_consensus::election::NodeId;
use uc_log::cnc::{AdminReq, CncPage};
use uc_net::fault::FaultConfig;
use uc_node::catalog::CatalogQuery;
use uc_node::{Node, NodeConfig, PurgePolicy, ServicesConfig, Settings};
use uc_protocol::v2::catalog::{RowVerdict, SetEntry, SetKind, SetState};
use uc_protocol::v2::cnc::{
    ADMIN_OP_SETTINGS_APPLY, ADMIN_OP_UPGRADE_PIN, CNC_SVC_STATUS_SNAPSHOT_CAPABLE,
};

const PAYLOAD: usize = 96;
/// The purging fixtures' journal segment: small, so a few MiB of traffic
/// spans many segments and a purge is observable as `archive_first_base > 0`.
const SEG: u64 = 64 * 1024;

static TEST_LOCK: Mutex<()> = Mutex::new(());

/// The whole-box serialization guard, plus the reset of every process-global
/// fault knob this file's state machines read ([`SALT`], [`FREEZE_FAILS`]).
/// The reset runs in `Drop` — on a normal return AND on a panic's unwind —
/// and BEFORE the lock is released (a struct's own `drop` runs before its
/// fields'), so a test that fails mid-way can never leak a salted row or a
/// failing freeze into the next test that takes the lock.
struct Serial {
    _lock: MutexGuard<'static, ()>,
}

impl Drop for Serial {
    fn drop(&mut self) {
        for s in &SALT {
            s.store(0, Ordering::Release);
        }
        FREEZE_FAILS.store(false, Ordering::Release);
    }
}

fn serialize() -> Serial {
    Serial {
        _lock: TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner()),
    }
}

/// UNIX-epoch wall nanoseconds — the clock the soft table stamps
/// `last_seen_ns` with and the leader stamps a `SNAPSHOT` frame's `time_ns`
/// with, so a query's `now_ns` must come from it.
fn unix_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after the epoch")
        .as_nanos() as u64
}

fn deadline_secs(secs: u64) -> Instant {
    Instant::now() + Duration::from_secs(secs)
}

fn await_until(secs: u64, msg: &str, mut f: impl FnMut() -> bool) {
    let deadline = deadline_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "{msg}");
        std::thread::yield_now();
    }
}

// ------------------------------------------------------------------ fixture

struct NodeH {
    id: NodeId,
    addr: SocketAddr,
    instance_dir: PathBuf,
    is_learner: bool,
    /// `NodeConfig` is not `Clone`; every (re)start builds a fresh one.
    cfg: Box<dyn Fn() -> NodeConfig>,
    /// The bound socket, held until the node's FIRST start (a node started
    /// later than its peers keeps its port reserved meanwhile).
    sock: Option<UdpSocket>,
    node: Option<Node>,
}

impl NodeH {
    fn n(&self) -> &Node {
        self.node.as_ref().expect("node stopped")
    }
    fn running(&self) -> bool {
        self.node.is_some()
    }
    fn start(&mut self) {
        assert!(self.node.is_none(), "start of a live node");
        let sock = self.sock.take().unwrap_or_else(|| rebind(self.addr));
        self.node = Some(Node::start_with_socket((self.cfg)(), sock).expect("start node"));
    }
    fn stop(&mut self) {
        if let Some(node) = self.node.take() {
            node.stop();
        }
    }
    fn cnc(&self, app: &str) -> Arc<CncPage> {
        CncPage::open_file(&self.instance_dir.join("cnc2.dat"), app).expect("open cnc")
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
    services: ServicesConfig,
    /// `PurgePolicy::BelowSnapshot { slack_bytes: 0 }` with a 256 KiB ring
    /// and 64 KiB segments; otherwise purge off and a 4 MiB ring.
    purge: bool,
    settings: Settings,
}

#[allow(clippy::too_many_arguments)]
fn make_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    learners: Vec<(NodeId, SocketAddr)>,
    instance_dir: PathBuf,
    seed: u64,
    addr: SocketAddr,
    o: &Opts,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        learners,
        bind: addr,
        instance_dir,
        app_id: o.app.into(),
        buffer_bytes: if o.purge { 1 << 18 } else { 1 << 22 },
        max_payload: 256,
        admission_bytes_default: 256 * 1024,
        settings_genesis: o.settings,
        force_jumbo_frames: false,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed,
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
        services: o.services,
    }
}

fn seed_for(i: usize) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

struct Cluster {
    _dir: tempfile::TempDir,
    app: &'static str,
    nodes: Vec<NodeH>,
    /// Service stoppers, run in order by [`Cluster::stop`].
    svcs: Vec<Box<dyn FnOnce()>>,
}

impl Cluster {
    fn node(&self, i: usize) -> &Node {
        self.nodes[i].n()
    }
    fn cnc(&self, i: usize) -> Arc<CncPage> {
        self.nodes[i].cnc(self.app)
    }
    fn dir(&self, i: usize) -> &Path {
        &self.nodes[i].instance_dir
    }
    fn running(&self) -> Vec<usize> {
        (0..self.nodes.len())
            .filter(|&i| self.nodes[i].running())
            .collect()
    }
    fn voters(&self) -> Vec<usize> {
        (0..self.nodes.len())
            .filter(|&i| !self.nodes[i].is_learner)
            .collect()
    }
    fn stop(mut self) {
        for s in self.svcs.drain(..) {
            s();
        }
        for h in self.nodes.iter_mut() {
            h.stop();
        }
    }
}

/// Keep a service running until the cluster stops (or the caller runs the
/// returned stopper itself).
fn stopper<S: uc_service::RawStateMachine + 'static>(
    s: uc_service::Service<S>,
) -> Box<dyn FnOnce()> {
    Box::new(move || s.stop())
}

/// Bind every socket, build every config, and start the nodes `start(i)`
/// selects (the rest keep their socket bound and start later through
/// [`NodeH::start`]). Voters are ids `0..n_voters`, learners after them, and
/// every node — started or not — is in the genesis membership.
fn spawn(n_voters: usize, n_learners: usize, o: Opts, start: impl Fn(usize) -> bool) -> Cluster {
    let dir = tempfile::Builder::new()
        .prefix("uc2-catalog-")
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
        let instance_dir = dir.path().join(format!("n{i}"));
        let (m, l, d) = (members.clone(), learners.clone(), instance_dir.clone());
        let cfg: Box<dyn Fn() -> NodeConfig> = Box::new(move || {
            make_config(
                i as NodeId,
                m.clone(),
                l.clone(),
                d.clone(),
                seed_for(i),
                addr,
                &o,
            )
        });
        let mut h = NodeH {
            id: i as NodeId,
            addr,
            instance_dir,
            is_learner: i >= n_voters,
            cfg,
            sock: Some(sock),
            node: None,
        };
        if start(i) {
            h.start();
        }
        nodes.push(h);
    }
    Cluster {
        _dir: dir,
        app: o.app,
        nodes,
        svcs: Vec::new(),
    }
}

/// Exactly one serving leader among the RUNNING voters; a learner never serves.
fn await_single_leader(c: &Cluster, secs: u64) -> usize {
    let deadline = deadline_secs(secs);
    loop {
        let serving: Vec<usize> = c
            .running()
            .into_iter()
            .filter(|&i| c.node(i).can_serve())
            .collect();
        assert!(serving.len() <= 1, "split-brain: {serving:?} all serve");
        if let [i] = serving[..] {
            assert!(!c.nodes[i].is_learner, "a learner must never lead");
            return i;
        }
        assert!(Instant::now() < deadline, "no single leader elected");
        std::thread::yield_now();
    }
}

/// One submitted frame's aligned log footprint: 32 B header + [`PAYLOAD`].
const FRAME_BYTES: u64 = 32 + PAYLOAD as u64;

/// `node.submit`, `n` times, retrying a full ring (bounded, Ruling R23).
/// Payload `i` as a little-endian `u64` — what every state machine here sums.
///
/// Then waits until the leader has COMMITTED them. `submit` only enqueues
/// on the ingress ring, while an instant is appended straight to the log —
/// so without this wait the instant the caller commands next can land
/// AHEAD of the traffic it was meant to follow (observed: a standby instant
/// at P = 128 after 6000 "submitted" frames).
///
/// The commit target, `append-before + n × FRAME_BYTES`, is a LOWER BOUND
/// on where these frames end: anything else appended meanwhile (a `CLUSTER`
/// command, an instant, a `NEW_TERM`) lands in the same log, so commit
/// reaching the target proves only that at least that many bytes committed
/// — enough to order the next instant after the traffic, not an exact
/// position of the last frame.
fn submit_frames(node: &Node, n: u64) {
    /// How long ONE frame may keep meeting a full ingress ring (Ruling R23:
    /// fail by name, never hang).
    const FULL_DEADLINE: Duration = Duration::from_secs(60);
    let counters = || {
        let c = node.counters();
        format!(
            "append={} commit={} durable={}",
            c.append.load_acquire(),
            c.commit.load_acquire(),
            c.durable.load_acquire()
        )
    };
    let before = node.counters().append.load_acquire();
    for i in 0u64..n {
        let mut p = vec![0u8; PAYLOAD];
        p[..8].copy_from_slice(&i.to_le_bytes());
        let mut first_full: Option<Instant> = None;
        loop {
            match node.submit(p.clone()) {
                Ok(()) => break,
                Err(uc_node::SubmitError::NotServing) => panic!(
                    "submit_frames: frame {i} of {n} refused NotServing — leadership moved off \
                     the node this fixture submits to. The fixture deliberately does NOT follow \
                     leadership: re-shape the test (re-resolve the leader between phases), do \
                     not add retries here ({})",
                    counters()
                ),
                Err(uc_node::SubmitError::Full) => {
                    let since = *first_full.get_or_insert_with(Instant::now);
                    if since.elapsed() >= FULL_DEADLINE {
                        panic!(
                            "submit_frames: frame {i} of {n} met a full ingress ring for \
                             {FULL_DEADLINE:?} — the log is not draining (a stalled `durable` \
                             means the archive cannot write): {}",
                            counters()
                        );
                    }
                    std::thread::yield_now();
                }
            }
        }
    }
    let target = before + n * FRAME_BYTES;
    let deadline = deadline_secs(60);
    while node.counters().commit.load_acquire() < target {
        assert!(
            Instant::now() < deadline,
            "submit_frames: the {n} submitted frames did not commit within 60 s \
             (target commit {target}): {}",
            counters()
        );
        std::thread::yield_now();
    }
}

/// Wait until every running node's rows `rows` carry the snapshot-capability
/// bit — published by the service's ATTACH, so this is the wait for attach.
fn await_capable(c: &Cluster, rows: &[usize]) {
    let pages: Vec<Arc<CncPage>> = c.running().into_iter().map(|i| c.cnc(i)).collect();
    await_until(
        30,
        "every row published the snapshot-capability bit",
        || {
            pages.iter().all(|p| {
                rows.iter().all(|&r| {
                    p.service_slot(r).status.load_acquire() & CNC_SVC_STATUS_SNAPSHOT_CAPABLE != 0
                })
            })
        },
    );
}

/// Wait until every node in `idxs` has applied every row in `rows` up to the
/// leader's current commit — a "quiesced" wait on the NODE counters is not
/// enough on a small ring (`learner.rs:1120`): a row's own apply loop can
/// still be catching up through an overrun.
fn await_applied(c: &Cluster, leader: usize, idxs: &[usize], rows: &[usize]) {
    let commit = c.node(leader).counters().commit.load_acquire();
    let pages: Vec<Arc<CncPage>> = idxs.iter().map(|&i| c.cnc(i)).collect();
    await_until(60, "every row applied to the leader's commit", || {
        pages.iter().all(|p| {
            rows.iter()
                .all(|&r| p.service_slot(r).applied.load_acquire() >= commit)
        })
    });
}

// ----------------------------------------------------- instants and the catalog

/// `uc2ctl snapshot [--standby]`, in process, polling through `retry`.
fn command(node: &Node, standby: bool) -> u64 {
    let deadline = deadline_secs(30);
    loop {
        match node.command_snapshot(standby) {
            Ok(p) => return p,
            Err(uc_node::SnapshotRefusal::Retry) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => panic!("uc2ctl snapshot refused: {e}"),
        }
    }
}

fn command_instant(node: &Node) -> u64 {
    command(node, false)
}

fn command_standby_instant(node: &Node) -> u64 {
    command(node, true)
}

/// Command a FULL instant on the leader and wait until every node in `idxs`
/// holds the complete set at it. Retries an instant a lapping walker
/// abandoned (`learner.rs`'s `instant_until_complete`, spec §10), but fails
/// on a first-attempt abandonment, which Ruling P10 made unreachable.
fn instant_until_complete(c: &Cluster, leader: usize, idxs: &[usize]) -> u64 {
    const ATTEMPTS: usize = 5;
    for attempt in 1..=ATTEMPTS {
        let p = command_instant(c.node(leader));
        let deadline = deadline_secs(30);
        while Instant::now() < deadline {
            if idxs.iter().all(|&i| c.node(i).snapshot_set_position() >= p) {
                return p;
            }
            std::thread::yield_now();
        }
        let sets: Vec<u64> = idxs
            .iter()
            .map(|&i| c.node(i).snapshot_set_position())
            .collect();
        eprintln!("instant {p} abandoned (attempt {attempt}/{ATTEMPTS}): sets={sets:?}");
        if attempt == 1 {
            eprintln!("{}", dump_instant(c, leader, idxs, p));
        }
        assert_ne!(
            attempt, 1,
            "instant {p} was abandoned on the FIRST attempt (spec §10)"
        );
    }
    panic!("no instant completed in {ATTEMPTS} attempts");
}

fn catalog(node: &Node) -> Vec<SetEntry> {
    node.cluster_view().snapshot_inner().catalog
}

fn positions(node: &Node) -> Vec<u64> {
    catalog(node).iter().map(|e| e.position).collect()
}

fn entry(node: &Node, p: u64) -> Option<SetEntry> {
    catalog(node).into_iter().find(|e| e.position == p)
}

fn agreed_position(node: &Node) -> u64 {
    node.cluster_view()
        .catalog_agreed_position
        .load(Ordering::Acquire)
}

/// Wait until every node in `idxs` has committed the catalog's agreement on
/// `p` (the newest agreed set), and return.
fn await_agreed(c: &Cluster, idxs: &[usize], p: u64) {
    let deadline = deadline_secs(60);
    while !idxs.iter().all(|&i| agreed_position(c.node(i)) == p) {
        if Instant::now() >= deadline {
            panic!(
                "every node agreed the set at {p} — per node:\n{}",
                dump_row0(c, idxs, p)
            );
        }
        std::thread::yield_now();
    }
}

/// Failure-path diagnostic (ruling R32): per node, the catalog's row-0
/// verdict at `p`, the committed row-0 report, and the row-0 artifact — its
/// length, first 32 bytes, and `SumSm`'s `total ‖ last` decoded past the
/// 24-byte envelope — so a divergence says whether the FILES differ or only
/// the reported hash does.
/// #82: per-voter state when an instant is abandoned on its first attempt —
/// which voter lags, and whether its row is behind, unattached, or frozen
/// without a cluster artifact.
fn dump_instant(c: &Cluster, leader: usize, idxs: &[usize], p: u64) -> String {
    let mut out = format!("instant {p} dump (leader {leader}):");
    for &i in idxs {
        let n = c.node(i);
        let k = n.counters();
        let cnc = c.cnc(i);
        let slot = cnc.service_slot(0);
        let dir = &c.nodes[i].instance_dir;
        out.push_str(&format!(
            "\n  node {i}: set={} append={} durable={} commit={} row0 applied={} \
             snapshot_pos={} status={:#x} | on disk at p: row0={} cluster={}",
            n.snapshot_set_position(),
            k.append.load_acquire(),
            k.durable.load_acquire(),
            k.commit.load_acquire(),
            slot.applied.load_acquire(),
            slot.snapshot_pos.load_acquire(),
            slot.status.load_acquire(),
            row_artifact(dir, 0, p).is_file(),
            cluster_artifact(dir, p).is_file(),
        ));
    }
    out
}

fn dump_row0(c: &Cluster, idxs: &[usize], p: u64) -> String {
    const ENVELOPE: usize = 24;
    let u64_at = |b: &[u8], o: usize| {
        b.get(o..o + 8)
            .map(|x| u64::from_le_bytes(x.try_into().unwrap()))
    };
    idxs.iter()
        .map(|&i| {
            let n = c.node(i);
            let row = entry(n, p).map(|e| (e.state, e.rows[0].verdict, e.rows[0].hash));
            let art = match std::fs::read(row_artifact(c.dir(i), 0, p)) {
                Ok(b) => format!(
                    "len={} head={} total={:?} last={:?}",
                    b.len(),
                    b.iter()
                        .take(32)
                        .map(|x| format!("{x:02x}"))
                        .collect::<String>(),
                    u64_at(&b, ENVELOPE),
                    u64_at(&b, ENVELOPE + 8)
                ),
                Err(e) => format!("<{e}>"),
            };
            format!(
                "node {i}: catalog row0 (state, verdict, hash)={row:?}\n  \
                 report(0)={:?}\n  artifact {art}",
                n.snapshot_report(0).map(|r| r.hashes)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The leader's query over its own committed catalog and soft table, as
/// project 2's chooser and `uc2ctl` would build it.
fn query_holders(node: &Node, p: u64) -> Vec<NodeId> {
    let soft = node.soft_table();
    let view = node.cluster_view().snapshot_inner();
    let q = CatalogQuery {
        sets: &view.catalog,
        catalog_position: node.cluster_view().catalog_version.load(Ordering::Acquire),
        soft: &soft,
        now_ns: unix_ns(),
        stale_ns: node.soft_stale_ns(),
    };
    let mut h = q.holders(p);
    h.sort_unstable();
    h
}

fn metric(node: &Node, name: &str) -> u64 {
    let text = uc_node::obs::metrics::render_prometheus(&node.observability());
    text.lines()
        .find_map(|l| l.strip_prefix(&format!("{name} ")))
        .unwrap_or_else(|| panic!("no {name} sample in:\n{text}"))
        .trim()
        .parse()
        .unwrap_or_else(|e| panic!("{name}: {e}"))
}

fn floor(c: &Cluster, i: usize) -> u64 {
    c.cnc(i).snapshots().node_snapshot_floor.load_acquire()
}

/// One labeled sample, e.g.
/// `metric_labeled(n, "uc2_snapshot_auto_fetch_total", "outcome=\"ok\"")`.
fn metric_labeled(node: &Node, name: &str, labels: &str) -> u64 {
    let text = uc_node::obs::metrics::render_prometheus(&node.observability());
    let prefix = format!("{name}{{{labels}}} ");
    text.lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("no {name}{{{labels}}} sample in:\n{text}"))
        .trim()
        .parse()
        .unwrap_or_else(|e| panic!("{name}{{{labels}}}: {e}"))
}

/// The five `uc2_snapshot_auto_fetch_total` outcomes (snapshot-lifecycle
/// spec §6).
const FETCH_OUTCOMES: [&str; 5] = ["ok", "refused", "timeout", "no_space", "no_holder"];

/// Snapshot-lifecycle failure-path diagnostic: per node, everything the five
/// lifecycle tests wait on — set position, persisted floor, purge base, the
/// catalog's agreed position and the newest-agreed size, the row's start set
/// and applied, the free-bytes figure the space check reads, and every
/// auto-fetch outcome counter.
fn dump_lifecycle(c: &Cluster, p: u64) -> String {
    let mut out = format!("lifecycle dump at p={p}:");
    for i in c.running() {
        let n = c.node(i);
        let k = n.counters();
        let cnc = c.cnc(i);
        let slot = cnc.service_slot(0);
        let free = n
            .soft_table()
            .by_node
            .get(&c.nodes[i].id)
            .map(|e| e.holdings.free_bytes);
        let fetches: Vec<String> = FETCH_OUTCOMES
            .iter()
            .map(|o| {
                format!(
                    "{o}={}",
                    metric_labeled(
                        n,
                        "uc2_snapshot_auto_fetch_total",
                        &format!("outcome=\"{o}\"")
                    )
                )
            })
            .collect();
        out.push_str(&format!(
            "\n  node {i}{}: set={} floor={} first_base={} agreed={} agreed_bytes={} \
             commit={} durable={} row0 applied={} start_set={:?} status={:#x} free={free:?} \
             fetch[{}] | on disk at p: {}",
            if c.nodes[i].is_learner {
                " (learner)"
            } else {
                ""
            },
            n.snapshot_set_position(),
            floor(c, i),
            n.archive_first_base(),
            agreed_position(n),
            metric(n, "uc2_snapshot_newest_agreed_bytes"),
            k.commit.load_acquire(),
            k.durable.load_acquire(),
            slot.applied.load_acquire(),
            slot.snapshot_pos.start_set(),
            slot.status.load_acquire(),
            fetches.join(" "),
            holds_on_disk(c.dir(i), &[0], p),
        ));
    }
    out
}

/// [`await_until`] that prints [`dump_lifecycle`] before it fails.
fn await_or_dump(c: &Cluster, p: u64, secs: u64, msg: &str, mut f: impl FnMut() -> bool) {
    let deadline = deadline_secs(secs);
    while !f() {
        if Instant::now() >= deadline {
            panic!("{msg} (within {secs} s)\n{}", dump_lifecycle(c, p));
        }
        std::thread::yield_now();
    }
}

/// [`await_capable`] over the nodes in `idxs` only.
fn await_capable_on(c: &Cluster, idxs: &[usize], rows: &[usize]) {
    let pages: Vec<Arc<CncPage>> = idxs.iter().map(|&i| c.cnc(i)).collect();
    await_until(
        30,
        "the named nodes' rows published the capability bit",
        || {
            pages.iter().all(|p| {
                rows.iter().all(|&r| {
                    p.service_slot(r).status.load_acquire() & CNC_SVC_STATUS_SNAPSHOT_CAPABLE != 0
                })
            })
        },
    );
}

/// The process-global obs sink, captured for a scope and restored to stderr
/// on drop (a panic's unwind included); [`serialize`] keeps captures from
/// overlapping.
struct ObsCapture(Arc<Mutex<Vec<u8>>>);

impl ObsCapture {
    fn take() -> Self {
        Self(uc_node::obs::log::capture_for_tests())
    }
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap_or_else(|e| e.into_inner())).into_owned()
    }
}

impl Drop for ObsCapture {
    fn drop(&mut self) {
        uc_node::obs::log::stderr_for_tests();
    }
}

fn row_artifact(dir: &Path, row: u8, p: u64) -> PathBuf {
    dir.join("snapshots")
        .join(row.to_string())
        .join(format!("snap-{p}.ultsnap"))
}

fn cluster_artifact(dir: &Path, p: u64) -> PathBuf {
    uc_node::cluster_agent::snapshot_dir_of(dir).join(format!("snap-{p}.ultcluster"))
}

/// Every member of the set at `p` (each row in `rows` plus the cluster
/// artifact) is on disk under `dir`.
fn holds_on_disk(dir: &Path, rows: &[u8], p: u64) -> bool {
    rows.iter().all(|&r| row_artifact(dir, r, p).is_file()) && cluster_artifact(dir, p).is_file()
}

/// No member of the set at `p` is left on disk under `dir`.
fn none_on_disk(dir: &Path, rows: &[u8], p: u64) -> bool {
    rows.iter().all(|&r| !row_artifact(dir, r, p).exists()) && !cluster_artifact(dir, p).exists()
}

// ------------------------------------------------- admin ops over the cnc band

/// Stage `bytes` at `<dir>/<file>` (0600, fsync, rename — `uc2ctl`'s shape)
/// and drive admin `op` through the node's cnc admin band (filesystem admin
/// policy: no auth line), polling through `retry` (status 2). Returns
/// `(status, reason, version)` of the first non-retry answer.
fn admin_staged(
    node_dir: &Path,
    cnc: &CncPage,
    file: &str,
    op: u32,
    bytes: &[u8],
) -> (u32, u32, u64) {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    let (id, ip, port) = uc_node::staged_digest(bytes);
    let deadline = deadline_secs(30);
    loop {
        let tmp = node_dir.join(format!("{file}.tmp"));
        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&tmp)
                .unwrap();
            f.write_all(bytes).unwrap();
            f.sync_all().unwrap();
        }
        std::fs::rename(&tmp, node_dir.join(file)).unwrap();
        let seq = cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0) + 1;
        cnc.write_admin_req(&AdminReq {
            seq,
            nonce: seq,
            op,
            id,
            ip,
            port,
        });
        let resp_deadline = deadline_secs(15);
        let resp = loop {
            if let Some(r) = cnc.read_admin_resp(seq) {
                break r;
            }
            assert!(Instant::now() < resp_deadline, "admin op {op} timed out");
            std::thread::yield_now();
        };
        if resp.status != 2 || Instant::now() >= deadline {
            return (resp.status, resp.reason, resp.version);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// `uc2ctl settings apply` with `retain_sets = n`, on the leader: the
/// committed record with that one field changed (so the leader-owned
/// `datagram_mtu` is re-proposed as it stands).
fn apply_retain_sets(c: &Cluster, leader: usize, n: u16) -> u64 {
    let mut s = c.node(leader).cluster_view().to_state().settings;
    s.retain_sets = n;
    let mut bytes = Vec::new();
    uc_protocol::v2::settings::encode_settings(&s, &mut bytes);
    let (status, reason, pos) = admin_staged(
        c.dir(leader),
        &c.cnc(leader),
        uc_node::SETTINGS_PENDING_FILE,
        ADMIN_OP_SETTINGS_APPLY,
        &bytes,
    );
    assert_eq!((status, reason), (0, 0), "settings apply refused");
    pos
}

/// The staged bytes of `uc2ctl upgrade pin` naming `origin` as `row`'s
/// origin, same line (`from == to == version`).
fn pin_bytes(row: u8, version: u32, origin: u64) -> Vec<u8> {
    use uc_protocol::v2::upgrade::{UpgradePin, encode_upgrade_pin};
    let mut bytes = Vec::new();
    encode_upgrade_pin(
        &UpgradePin {
            row,
            from: version,
            to: version,
            origin,
        },
        &mut bytes,
    );
    bytes
}

/// `uc2ctl upgrade pin` on the leader, retried through `54 pin_no_set` (the
/// set's position is published a moment after its artifact lands) or `61
/// pin_origin_not_agreed` (the catalog agrees it a moment after that).
fn pin(c: &Cluster, leader: usize, row: u8, version: u32, origin: u64) -> u64 {
    let bytes = pin_bytes(row, version, origin);
    let deadline = deadline_secs(30);
    loop {
        let (status, reason, pos) = admin_staged(
            c.dir(leader),
            &c.cnc(leader),
            uc_node::UPGRADE_PENDING_FILE,
            ADMIN_OP_UPGRADE_PIN,
            &bytes,
        );
        if status == 0 {
            return pos;
        }
        assert!(
            (reason == uc_node::REASON_PIN_NO_SET
                || reason == uc_node::REASON_PIN_ORIGIN_NOT_AGREED)
                && Instant::now() < deadline,
            "upgrade pin refused: status={status} reason={reason}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

// -------------------------------------------------------- state machines

/// `learner.rs`'s raw-tier summing FSM, plus a per-NODE salt read at freeze:
/// a node whose salt is non-zero appends it to the image, so its artifact
/// hashes differently from its peers' for the same instant while every frame
/// it applied is the same. Salt `0` is byte-for-byte `learner.rs`'s `SumSm`.
/// All nodes run in one process, so the salt is a `static` the test sets per
/// node index BEFORE the instant it wants to diverge.
struct SumSm {
    total: u64,
    last: Option<u64>,
    node: usize,
}

static SALT: [AtomicU64; 8] = [const { AtomicU64::new(0) }; 8];

/// Snapshot-lifecycle e2e 3: frames each node's [`SumSm`] applied in this
/// process — how a restart's replay length is read.
static APPLIES: [AtomicU64; 8] = [const { AtomicU64::new(0) }; 8];

impl SumSm {
    fn on(node: usize) -> SumSm {
        SumSm {
            total: 0,
            last: None,
            node,
        }
    }
}

impl uc_service::RawStateMachine for SumSm {
    const NAME: &'static str = "sum";

    fn apply(&mut self, ctx: &mut uc_service::ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
        APPLIES[self.node].fetch_add(1, Ordering::Relaxed);
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
        let mut buf = Vec::with_capacity(24);
        buf.extend_from_slice(&self.total.to_le_bytes());
        buf.extend_from_slice(&pos.to_le_bytes());
        let salt = SALT[self.node].load(Ordering::Acquire);
        if salt != 0 {
            buf.extend_from_slice(&salt.to_le_bytes());
        }
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
        // The tag is an EXCLUSIVE frontier: restore the recorded cursor.
        self.last = Some(u64::from_le_bytes(buf[8..16].try_into().unwrap()));
        Ok(position)
    }
}

/// A service config whose attach waits out a node that has not JOINED its
/// cluster yet for up to 60 s rather than the default 10 s: a restarted
/// pair of voters must elect and the cluster agent walk to commit first, and
/// on a loaded box that can outlast the default.
fn svc_cfg(dir: &Path, app: &str) -> uc_service::ServiceConfig {
    let mut cfg = uc_service::ServiceConfig::new(dir, app);
    cfg.boot_wait = Duration::from_secs(60);
    cfg
}

fn start_sum(dir: &Path, app: &str, node: usize) -> uc_service::Service<SumSm> {
    uc_service::ServiceBuilder::new(svc_cfg(dir, app), SumSm::on(node))
        .start()
        .expect("service start")
}

/// Row 1 of the stalled-set fixture: [`SumSm`]'s logic under the name
/// `"fsm1"`, whose `freeze()` FAILS while [`FREEZE_FAILS`] is set. The SDK
/// declines the instant for this row ("this row is incomplete for that
/// instant; the next one retries"), so the set at that instant can never
/// complete on any node — the stalled set of spec §8, reached through a real
/// service rather than a poked status word.
struct FlakySum(SumSm);

static FREEZE_FAILS: AtomicBool = AtomicBool::new(false);

impl uc_service::RawStateMachine for FlakySum {
    const NAME: &'static str = "fsm1";
    fn apply(&mut self, ctx: &mut uc_service::ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
        self.0.apply(ctx, cmd, out)
    }
    fn query(&self, q: &[u8], out: &mut Vec<u8>) {
        self.0.query(q, out)
    }
    fn last_applied(&self) -> Option<u64> {
        self.0.last_applied()
    }
}

impl uc_service::SnapshotStateMachine for FlakySum {
    type SnapshotHandle = Vec<u8>;
    fn freeze(&self) -> Result<(Vec<u8>, u64), uc_service::SnapshotError> {
        if FREEZE_FAILS.load(Ordering::Acquire) {
            return Err(std::io::Error::other("injected: this row cannot freeze").into());
        }
        self.0.freeze()
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
        self.0.install_snapshot(position, src)
    }
}

fn start_flaky(dir: &Path, app: &str, node: usize) -> uc_service::Service<FlakySum> {
    uc_service::ServiceBuilder::new(svc_cfg(dir, app), FlakySum(SumSm::on(node)))
        .start()
        .expect("service start")
}

fn sum_services() -> ServicesConfig {
    ServicesConfig::single(<SumSm as uc_service::RawStateMachine>::NAME)
}

fn opts(app: &'static str, purge: bool) -> Opts {
    Opts {
        app,
        services: sum_services(),
        purge,
        settings: Settings::genesis_default(),
    }
}

/// Start a [`SumSm`] service on every running node.
fn start_sums(c: &mut Cluster) {
    for i in c.running() {
        let s = start_sum(&c.nodes[i].instance_dir, c.app, i);
        c.svcs.push(stopper(s));
    }
}

// ================================================================== tests

/// Catalog spec §8 "Diverged set" / §6.1: a set whose row hashes disagree
/// is `Complete` with that row `Diverged`, and is NEVER the floor.
///
/// Three voters (so a 2-of-3 majority exists and the verdict is `Diverged`,
/// not `NoMajority`) and one learner that starts only at the end, as the
/// joiner. The first instant `p0` agrees everywhere and becomes the floor;
/// then node 2's salt is set and the second instant `p1` completes on every
/// node with node 2's row hashing differently.
///
/// The rule, from every side it can be read: the catalog lists `p1` complete
/// with row 0 `Diverged`; the newest agreed set is still `p0` on every node;
/// every voter's PERSISTED floor stays at `p0` and its journal is not purged
/// past it, although every voter holds `p1` complete; `p0`'s set stays on
/// disk; `uc2_catalog_diverged >= 1`; and a joiner below the purged prefix is
/// served — and installs — `p0`, not `p1`.
///
/// Red twin: leave the salt at 0 — `p1` agrees, `catalog_agreed_position`
/// moves to `p1` and the floors follow it.
#[test]
fn a_diverged_row_completes_the_set_but_never_moves_the_floor() {
    let _g = serialize();
    for s in &SALT {
        s.store(0, Ordering::Release);
    }
    let joiner = 3usize;
    let mut c = spawn(3, 1, opts("catalog-diverged", true), |i| i != joiner);
    start_sums(&mut c);
    let leader = await_single_leader(&c, 30);
    let voters = c.voters();
    await_capable(&c, &[0]);

    submit_frames(c.node(leader), 12000);
    let p0 = instant_until_complete(&c, leader, &voters);
    submit_frames(c.node(leader), 12000);
    await_agreed(&c, &voters, p0);
    assert!(p0 > SEG, "need >1 segment below p0 (p0={p0})");
    await_until(30, "every voter persisted p0 as its floor", || {
        voters.iter().all(|&v| floor(&c, v) == p0)
    });
    await_until(30, "every voter purged below p0", || {
        voters.iter().all(|&v| c.node(v).archive_first_base() > 0)
    });

    // Node 2 now writes a different image for the same state.
    SALT[2].store(0xD1F, Ordering::Release);
    let p1 = instant_until_complete(&c, leader, &voters);
    submit_frames(c.node(leader), 4000);
    await_until(
        60,
        "every node recorded p1 complete with row 0 diverged",
        || {
            voters.iter().all(|&v| {
                entry(c.node(v), p1).is_some_and(|e| {
                    e.state == SetState::Complete && e.rows[0].verdict == RowVerdict::Diverged
                })
            })
        },
    );
    for &v in &voters {
        let e = entry(c.node(v), p1).unwrap();
        assert!(
            !e.is_agreed(),
            "node {v}: a diverged set is never agreed: {e:?}"
        );
        assert_eq!(
            e.cluster.verdict,
            RowVerdict::Agreed,
            "node {v}: only the user row diverged; the cluster artifact agrees"
        );
        assert_eq!(
            agreed_position(c.node(v)),
            p0,
            "node {v}: the newest agreed set must stay p0 — a diverged set never moves it"
        );
        assert!(
            c.node(v)
                .cluster_view()
                .catalog_diverged
                .load(Ordering::Acquire)
                >= 1,
            "node {v}: the diverged count must name the row"
        );
        assert!(
            metric(c.node(v), "uc2_catalog_diverged") >= 1,
            "node {v}: uc2_catalog_diverged must read >= 1"
        );
        assert_eq!(
            c.node(v).snapshot_set_position(),
            p1,
            "node {v} holds p1 complete — the set is complete, it is just not agreed"
        );
    }

    // The floor rule. A NEGATIVE, so it is held for a span (well past the
    // 100 ms persist throttle and many purge ticks) rather than read once.
    let hold = Instant::now() + Duration::from_secs(3);
    while Instant::now() < hold {
        for &v in &voters {
            assert_eq!(
                floor(&c, v),
                p0,
                "voter {v} persisted a floor past p0 — onto a diverged set"
            );
            assert!(
                c.node(v).archive_first_base() <= p0,
                "voter {v} purged its journal above the newest agreed set p0"
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    for &v in &voters {
        assert!(
            holds_on_disk(c.dir(v), &[0], p0),
            "voter {v} deleted the agreed set p0 it stands on"
        );
    }

    // A joiner below the purged prefix is served the previous AGREED set.
    let first_base = c.node(leader).archive_first_base();
    c.nodes[joiner].start();
    let s = start_sum(&c.nodes[joiner].instance_dir, c.app, joiner);
    c.svcs.push(stopper(s));
    let frontier = c.node(leader).counters().append.load_acquire();
    await_until(60, "the joiner caught up across the purged prefix", || {
        c.node(joiner).counters().durable.load_acquire() >= frontier
    });
    let j_base = c.node(joiner).archive_first_base();
    assert!(
        j_base >= first_base && j_base >= p0 && j_base < p1,
        "the joiner must have installed the agreed set p0={p0}, not the diverged p1={p1} \
         (its adopted first_base={j_base})"
    );
    assert!(
        holds_on_disk(c.dir(joiner), &[0], p0),
        "the joiner holds the shipped set at p0"
    );
    // The installed cluster artifact at p0 was frozen BEFORE p0's reports
    // existed, so it lists p0 as `Commanded`; the joiner learns p0 is agreed
    // only when its cluster agent applies the report records committed after
    // it — which trails the archive's `durable` waited on above. Wait for the
    // catalog, not just the log (a slow runner read 0 here, PR #83's nightly).
    await_until(30, "the joiner's catalog agrees on p0", || {
        agreed_position(c.node(joiner)) == p0
    });
    assert_eq!(
        agreed_position(c.node(joiner)),
        p0,
        "and its catalog agrees on p0"
    );

    SALT[2].store(0, Ordering::Release);
    c.stop();
}

/// Catalog spec §8 "Stalled set": an instant some row cannot freeze stays
/// `Commanded`; `stalled()` names it; and the NEXT instant proceeds and
/// completes. ("Retention ignores a stalled set" is part of the same §8 row
/// but is NOT observable here — with `retain_sets = 1` there is only ever
/// one agreed set to retain; test 3 covers retention, and the cluster FSM's
/// unit tests cover a stall above the floor.)
///
/// Two rows, real services on every node: row 0 `SumSm`, row 1 `FlakySum`,
/// whose `freeze()` fails while [`FREEZE_FAILS`] is set. With it set, the
/// instant `p1` completes row 0 everywhere and row 1 nowhere — no node holds
/// the set, nobody reports it, and the catalog keeps it `Commanded`. Clear the
/// flag and the next instant `p2` completes, agrees, and supersedes `p1`,
/// which is dropped as history the moment `p2` turns `Complete` (catalog
/// ruling R25; before R25 it went at `p2`'s agreement, §4.4 step 2).
///
/// Red twin: leave the flag clear for `p1` — it completes, and the
/// `Commanded` / `stalled()` assertions fail.
#[test]
fn a_stalled_set_stays_commanded_and_the_next_instant_completes() {
    let _g = serialize();
    FREEZE_FAILS.store(false, Ordering::Release);
    let o = Opts {
        app: "catalog-stalled",
        services: ServicesConfig::from_names(&["sum", "fsm1"], None).unwrap(),
        purge: false,
        settings: Settings::genesis_default(),
    };
    let mut c = spawn(2, 1, o, |_| true);
    let all = c.running();
    for &i in &all {
        let s0 = start_sum(c.dir(i), c.app, i);
        c.svcs.push(stopper(s0));
        let s1 = start_flaky(c.dir(i), c.app, i);
        c.svcs.push(stopper(s1));
    }
    let leader = await_single_leader(&c, 30);
    await_capable(&c, &[0, 1]);
    submit_frames(c.node(leader), 2000);
    await_applied(&c, leader, &all, &[0, 1]);

    FREEZE_FAILS.store(true, Ordering::Release);
    let p1 = command_instant(c.node(leader));
    // Row 0 froze at p1 everywhere: the instant was walked live and acted on,
    // so its set's incompleteness is row 1's alone.
    let pages: Vec<Arc<CncPage>> = all.iter().map(|&i| c.cnc(i)).collect();
    await_until(30, "row 0 froze at p1 on every node", || {
        pages
            .iter()
            .all(|p| p.service_slot(0).snapshot_pos.load_acquire() == p1)
    });
    await_until(30, "every node lists p1", || {
        all.iter().all(|&i| entry(c.node(i), p1).is_some())
    });
    submit_frames(c.node(leader), 1000);
    // A negative: held for longer than the 5 s report-collection fallback,
    // so a late report would have had every chance to land.
    let hold = Instant::now() + Duration::from_secs(7);
    while Instant::now() < hold {
        for &i in &all {
            let e = entry(c.node(i), p1).expect("p1 stays listed");
            assert_eq!(
                e.state,
                SetState::Commanded,
                "node {i}: a set a row could not freeze must stay Commanded: {e:?}"
            );
            assert_eq!(
                c.node(i).snapshot_set_position(),
                0,
                "node {i} completed p1"
            );
            assert_eq!(agreed_position(c.node(i)), 0, "node {i}: nothing agreed");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    {
        let node = c.node(leader);
        let soft = node.soft_table();
        let view = node.cluster_view().snapshot_inner();
        let q = CatalogQuery {
            sets: &view.catalog,
            catalog_position: node.cluster_view().catalog_version.load(Ordering::Acquire),
            soft: &soft,
            now_ns: unix_ns(),
            stale_ns: node.soft_stale_ns(),
        };
        assert_eq!(q.stalled(0), vec![p1], "stalled() names the commanded set");
        assert_eq!(
            q.stalled(3_600_000_000_000),
            Vec::<u64>::new(),
            "…and only once it is older than the timeout"
        );
    }
    assert!(
        c.node(leader)
            .cluster_view()
            .catalog_stalled
            .load(Ordering::Acquire)
            >= 1
    );
    assert!(metric(c.node(leader), "uc2_catalog_stalled") >= 1);

    // The row can freeze again: the NEXT instant completes and agrees.
    FREEZE_FAILS.store(false, Ordering::Release);
    let p2 = instant_until_complete(&c, leader, &all);
    assert!(p2 > p1);
    await_agreed(&c, &all, p2);
    for &i in &all {
        let e = entry(c.node(i), p2).expect("p2 listed");
        assert_eq!(e.state, SetState::Complete);
        assert!(e.is_agreed(), "node {i}: p2 agreed: {e:?}");
        assert_eq!(
            positions(c.node(i)),
            vec![p2],
            "node {i}: the stalled p1 is superseded history once an agreed set is newer"
        );
    }
    assert_eq!(
        c.node(leader)
            .cluster_view()
            .catalog_stalled
            .load(Ordering::Acquire),
        0
    );

    c.stop();
}

/// Catalog spec §4.4 (ruling R21): `retain_sets = 2` keeps the two newest
/// UNPINNED agreed sets plus every pinned origin, retires the oldest from the
/// catalog AND from every node's disk, and a pinned origin is never counted
/// and never retired; the journal floor follows the newest agreed set the
/// node holds, not the oldest retained.
///
/// Instants `p1`, `p2` (catalog `[p1, p2]`, journal purged past `p1`); a
/// same-line pin naming `p2` as a row's origin; `p3` — the unpinned agreed
/// sets are `[p1, p3]`, exactly `retain_sets`, so nothing retires and the
/// catalog reads `[p1, p2, p3]` (the pin costs no retention slot); `p4` —
/// the unpinned sets are `[p1, p3, p4]`, so the oldest, `p1`, retires from
/// the catalog and every node's disk, the catalog reads `[p2, p3, p4]`, and
/// `p3` SURVIVES on disk beside the pinned `p2`.
///
/// Red twins: skip the settings apply (retention 1 drops `p1` at `p2`), skip
/// the pin (`p2` leaves at `p4`), or count the pin toward `retain_sets` (as
/// built before R21: `p1` leaves at `p3` and `p3` at `p4`).
#[test]
fn retain_sets_2_retires_the_oldest_and_keeps_the_pinned_origin() {
    let _g = serialize();
    let mut c = spawn(2, 1, opts("catalog-retain", true), |_| true);
    start_sums(&mut c);
    let leader = await_single_leader(&c, 30);
    let all = c.running();
    await_capable(&c, &[0]);
    apply_retain_sets(&c, leader, 2);
    await_until(30, "every node committed retain_sets = 2", || {
        all.iter()
            .all(|&i| c.node(i).cluster_view().retain_sets.load(Ordering::Acquire) == 2)
    });

    submit_frames(c.node(leader), 8000);
    let p1 = instant_until_complete(&c, leader, &all);
    submit_frames(c.node(leader), 8000);
    let p2 = instant_until_complete(&c, leader, &all);
    submit_frames(c.node(leader), 2000);
    await_agreed(&c, &all, p2);
    for &i in &all {
        assert_eq!(
            positions(c.node(i)),
            vec![p1, p2],
            "node {i}: both retained"
        );
        assert!(
            holds_on_disk(c.dir(i), &[0], p1),
            "node {i}: a retained set stays on disk"
        );
    }
    // The journal follows the NEWEST agreed set held (p2), not the oldest
    // retained one (p1): retention keeps artifacts, not journal.
    assert!(p2 - p1 > 2 * SEG, "need segments between p1 and p2");
    await_until(30, "every node purged its journal past p1", || {
        all.iter().all(|&i| {
            let b = c.node(i).archive_first_base();
            b > p1 && b <= p2
        })
    });

    // A same-line pin naming p2 as row 0's origin.
    let version = c.cnc(leader).service_slot(0).status.version();
    pin(&c, leader, 0, version, p2);
    await_until(30, "every node committed the pin", || {
        all.iter().all(|&i| {
            c.node(i)
                .cluster_view()
                .snapshot_inner()
                .pins
                .iter()
                .any(|p| p.origin == p2)
        })
    });

    submit_frames(c.node(leader), 4000);
    let p3 = instant_until_complete(&c, leader, &all);
    await_agreed(&c, &all, p3);
    // Ruling R21: the pinned p2 is not counted, so the unpinned agreed sets
    // are [p1, p3] — exactly retain_sets — and nothing retires.
    await_until(30, "every node lists [p1, p2, p3]", || {
        all.iter()
            .all(|&i| positions(c.node(i)) == vec![p1, p2, p3])
    });
    for &i in &all {
        assert!(
            holds_on_disk(c.dir(i), &[0], p1),
            "node {i}: p1 is still retained — the pin cost no retention slot"
        );
    }

    submit_frames(c.node(leader), 4000);
    let p4 = instant_until_complete(&c, leader, &all);
    await_agreed(&c, &all, p4);
    // Settle: the floor's persist throttle and the pruner run after agreement.
    await_until(30, "every node's floor reached p4", || {
        all.iter().all(|&i| floor(&c, i) == p4)
    });
    // Ruling R21 (`ClusterState::retire`): a pinned origin is never counted
    // toward `retain_sets` — with `[p1, p2 (pinned), p3, p4]` agreed and
    // `retain_sets = 2`, the unpinned sets are [p1, p3, p4], so p1 (the
    // oldest) retires and p2 and p3 both stay.
    await_until(30, "every node retired p1's files", || {
        all.iter().all(|&i| none_on_disk(c.dir(i), &[0], p1))
    });
    for &i in &all {
        assert_eq!(
            positions(c.node(i)),
            vec![p2, p3, p4],
            "node {i}: p1 retires; the pinned p2 is kept beside two retained sets"
        );
        assert!(
            holds_on_disk(c.dir(i), &[0], p2),
            "node {i}: the pinned origin's set must survive on disk"
        );
        assert!(
            holds_on_disk(c.dir(i), &[0], p3),
            "node {i}: p3 is retained (the pin does not take its slot) and survives on disk"
        );
        assert!(
            holds_on_disk(c.dir(i), &[0], p4),
            "node {i}: the newest agreed set is on disk"
        );
        await_until(30, "the purge followed the floor", || {
            c.node(i).archive_first_base() > p3
        });
        let b = c.node(i).archive_first_base();
        assert!(
            b > p3 && b <= p4,
            "node {i}: the journal follows the newest agreed set p4 (first_base={b}, \
             p3={p3}, p4={p4}) — a same-line pin is consumed by the attached row, so it \
             holds artifacts, not journal"
        );
    }

    c.stop();
}

/// Rewrite a cluster artifact in place as the pre-catalog `v3` layout: the
/// report blob narrowed to its unsized 12 B entries (snapshot-lifecycle spec
/// §7.2 — a v3 image never carried sizes; `cluster_fsm.rs`'s
/// `rewrite_image_as_v4` does the same), the trailing catalog blob and its
/// length prefix dropped, the version word set to 3, re-CRC
/// (`cluster_fsm.rs`'s `a_v3_image_installs_with_an_empty_catalog`,
/// generalised to a non-empty catalog). This is what a `2.13.x` node left on
/// disk before the flag day.
fn rewrite_as_v3(path: &Path) {
    use uc_protocol::v2::cluster_image::{
        ClusterImageParts, decode_cluster_image, encode_cluster_image,
    };
    use uc_protocol::v2::upgrade::{
        SNAPSHOT_REPORT_ENTRY_LEN, SNAPSHOT_REPORT_ENTRY_LEN_UNSIZED, SNAPSHOT_REPORT_HEADER_LEN,
    };
    let img = std::fs::read(path).unwrap();
    let parts = decode_cluster_image(&img).expect("a valid current image");
    let mut reports = Vec::new();
    let mut o = 0;
    while o < parts.reports.len() {
        let len = u32::from_le_bytes(parts.reports[o..o + 4].try_into().unwrap()) as usize;
        let rec = &parts.reports[o + 4..o + 4 + len];
        let mut old = rec[..SNAPSHOT_REPORT_HEADER_LEN].to_vec();
        for e in rec[SNAPSHOT_REPORT_HEADER_LEN..].chunks(SNAPSHOT_REPORT_ENTRY_LEN) {
            old.extend_from_slice(&e[..SNAPSHOT_REPORT_ENTRY_LEN_UNSIZED]);
        }
        reports.extend_from_slice(&(old.len() as u32).to_le_bytes());
        reports.extend_from_slice(&old);
        o += 4 + len;
    }
    let mut v3 = Vec::new();
    encode_cluster_image(
        &ClusterImageParts {
            reports: &reports,
            catalog: &[],
            ..parts
        },
        &mut v3,
    )
    .unwrap();
    v3.truncate(v3.len() - 4); // CRC
    let catalog_prefix = v3.split_off(v3.len() - 4);
    assert_eq!(catalog_prefix, 0u32.to_le_bytes(), "empty catalog blob");
    v3[8..12].copy_from_slice(&3u32.to_le_bytes());
    let crc = crc32fast::hash(&v3);
    v3.extend_from_slice(&crc.to_le_bytes());
    let parts = decode_cluster_image(&v3).expect("a valid v3");
    assert!(parts.catalog.is_empty());
    let mut o = 0;
    while o < parts.reports.len() {
        let len = u32::from_le_bytes(parts.reports[o..o + 4].try_into().unwrap()) as usize;
        let count = parts.reports[o + 4 + 1] as usize;
        assert_eq!(
            len,
            SNAPSHOT_REPORT_HEADER_LEN + count * SNAPSHOT_REPORT_ENTRY_LEN_UNSIZED,
            "the v3 report blob is unsized"
        );
        o += 4 + len;
    }
    assert!(uc_protocol::v2::upgrade::decode_report_list(parts.reports).is_some());
    std::fs::write(path, &v3).unwrap();
}

/// Catalog spec §4.5 / §8 "Flag-day window", as amended by ruling R13.
///
/// Two voters run purging, take two instants and stop; every cluster
/// artifact on their disks is rewritten to the pre-catalog `v3` layout — the
/// state a node is in on the first boot after the `0.11.0` flag day, with
/// pre-existing sets on disk and no catalog. Restarted, both nodes read
/// `Empty` (`catalog_agreed_position == 0`, `uc2_catalog_empty == 1`); a fresh
/// learner below the purged prefix is SERVED from the pre-existing set; the
/// set each node stands on (its own newest complete set, R13) survives on
/// disk; and the first instant after the restart seeds the catalog and ends
/// the window.
///
/// Red twin: skip the rewrite — the restarted nodes load the catalog and the
/// `Empty` assertions fail.
#[test]
fn the_flag_day_window_is_empty_and_deletes_nothing() {
    let _g = serialize();
    let joiner = 2usize;
    let mut c = spawn(2, 1, opts("catalog-flagday", true), |i| i != joiner);
    start_sums(&mut c);
    let leader = await_single_leader(&c, 30);
    let voters = c.voters();
    await_capable(&c, &[0]);
    submit_frames(c.node(leader), 12000);
    let _p1 = instant_until_complete(&c, leader, &voters);
    submit_frames(c.node(leader), 12000);
    let p2 = instant_until_complete(&c, leader, &voters);
    submit_frames(c.node(leader), 4000);
    await_agreed(&c, &voters, p2);
    await_until(30, "every voter persisted p2 and purged below it", || {
        voters
            .iter()
            .all(|&v| floor(&c, v) == p2 && c.node(v).archive_first_base() > 0)
    });
    await_applied(&c, leader, &voters, &[0]);

    // The flag day: stop everything, rewrite every cluster artifact as v3.
    for s in c.svcs.drain(..) {
        s();
    }
    for &v in &voters {
        c.nodes[v].stop();
    }
    let mut rewritten = 0;
    for &v in &voters {
        let dir = uc_node::cluster_agent::snapshot_dir_of(c.dir(v));
        for e in std::fs::read_dir(&dir).unwrap().flatten() {
            if e.file_name().to_string_lossy().ends_with(".ultcluster") {
                rewrite_as_v3(&e.path());
                rewritten += 1;
            }
        }
        assert!(
            holds_on_disk(c.dir(v), &[0], p2),
            "voter {v} holds p2 before boot"
        );
    }
    assert!(
        rewritten >= 2,
        "every voter had a cluster artifact to rewrite"
    );

    for &v in &voters {
        c.nodes[v].start();
    }
    // WORKAROUND for a cold-restart wedge that is NOT the catalog's (it
    // reproduces with the v3 rewrite skipped): on a fresh boot every row's
    // `applied` reads 0, so each voter's durable REPORT is capped at
    // `0 + fsm_lag` (64 KiB on this ring, M14a's report ceiling) and the new
    // leader's NewTerm frame at the log end (MiB in) can never commit; and
    // since plan B3 a service cannot attach until its node has learned a
    // commit (`NodeBooting`). Neither side can move first. The stand-in
    // `learner.rs` uses for an un-serviced row (`spawn_applied_mirror`)
    // breaks it: mirror `durable` into row 0's `applied` until the node
    // publishes its declared set, then stop and let the real service attach.
    let mirror_stop = Arc::new(AtomicBool::new(false));
    let mirrors: Vec<_> = voters
        .iter()
        .map(|&v| {
            let cnc = c.cnc(v);
            let stop = Arc::clone(&mirror_stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let d = cnc.counters().durable.load_acquire();
                    cnc.service_slot(0).applied.store_release(d);
                    std::thread::sleep(Duration::from_micros(200));
                }
            })
        })
        .collect();
    await_until(60, "every restarted voter joined its cluster", || {
        voters.iter().all(|&v| c.cnc(v).services_declared() != 0)
    });
    mirror_stop.store(true, Ordering::Relaxed);
    for m in mirrors {
        m.join().unwrap();
    }
    start_sums(&mut c);
    let leader = await_single_leader(&c, 30);
    await_capable(&c, &[0]);
    await_until(30, "every voter restored its set position", || {
        voters
            .iter()
            .all(|&v| c.node(v).snapshot_set_position() == p2)
    });
    for &v in &voters {
        assert_eq!(
            agreed_position(c.node(v)),
            0,
            "voter {v}: a v3 cluster artifact loads with an EMPTY catalog"
        );
        assert!(positions(c.node(v)).is_empty(), "voter {v}: no set listed");
        assert_eq!(
            metric(c.node(v), "uc2_catalog_empty"),
            1,
            "voter {v}: Empty is named"
        );
    }

    // The Empty fallback serves a joiner below the purged prefix.
    submit_frames(c.node(leader), 4000);
    let first_base = c.node(leader).archive_first_base();
    c.nodes[joiner].start();
    let s = start_sum(c.dir(joiner), c.app, joiner);
    c.svcs.push(stopper(s));
    let frontier = c.node(leader).counters().append.load_acquire();
    await_until(60, "the joiner caught up across the purged prefix", || {
        c.node(joiner).counters().durable.load_acquire() >= frontier
    });
    assert!(
        c.node(joiner).archive_first_base() >= first_base.max(p2),
        "the joiner must have installed the pre-existing set at p2, not replayed from 0"
    );
    assert!(
        holds_on_disk(c.dir(joiner), &[0], p2),
        "the joiner holds the set it was served"
    );
    // Nothing the nodes could still need was deleted: the set each one stands
    // on is intact, and the window is still Empty.
    for &v in &voters {
        assert!(
            holds_on_disk(c.dir(v), &[0], p2),
            "voter {v} deleted the pre-existing set it stands on during the Empty window"
        );
        assert_eq!(agreed_position(c.node(v)), 0, "voter {v}: still Empty");
    }

    // The first instant seeds the catalog and ends the window.
    let all = c.running();
    await_capable(&c, &[0]);
    let p3 = instant_until_complete(&c, leader, &all);
    await_agreed(&c, &all, p3);
    for &i in &all {
        assert_eq!(
            metric(c.node(i), "uc2_catalog_empty"),
            0,
            "node {i}: Empty ended"
        );
        assert_eq!(positions(c.node(i)), vec![p3], "node {i}: seeded with p3");
    }

    c.stop();
}

/// Catalog spec §5.3 / §8 "Stale or wrong soft state": a node whose
/// `STATUS` stops arriving leaves `holders()` after the soft staleness
/// timeout, and a fetch routed by `holders()` lands from the surviving
/// holder.
///
/// Two voters, two learners; a `--standby` instant makes both learners (and
/// only them) holders of `p`. Learner 2 is stopped. Within the staleness
/// timeout it may still be listed; after it, `holders(p) == [3]`. A follower
/// voter then fetches from `holders(p)[0]` and the set lands there.
///
/// Red twin: leave learner 2 running — `holders(p)` stays `[2, 3]`.
#[test]
fn a_killed_node_leaves_holders_after_the_stale_timeout() {
    let _g = serialize();
    let mut c = spawn(
        2,
        2,
        Opts {
            settings: Settings {
                auto_fetch: false,
                ..Settings::genesis_default()
            },
            ..opts("catalog-stale", false)
        },
        |_| true,
    );
    let mut learner2_svc = None;
    for i in c.running() {
        let s = start_sum(c.dir(i), c.app, i);
        if i == 2 {
            learner2_svc = Some(s);
        } else {
            c.svcs.push(stopper(s));
        }
    }
    let leader = await_single_leader(&c, 30);
    let all = c.running();
    await_capable(&c, &[0]);
    submit_frames(c.node(leader), 2000);
    await_applied(&c, leader, &all, &[0]);

    let p = command_standby_instant(c.node(leader));
    await_until(60, "both learners completed the standby set", || {
        [2usize, 3]
            .iter()
            .all(|&l| c.node(l).snapshot_set_position() >= p)
    });
    await_agreed(&c, &all, p);
    let (l2, l3) = (c.nodes[2].id, c.nodes[3].id);
    await_until(
        30,
        "the leader's soft table names both learners as holders",
        || query_holders(c.node(leader), p) == vec![l2, l3],
    );
    let stale = Duration::from_nanos(c.node(leader).soft_stale_ns());

    // Measured BEFORE the stop begins, not after it: the learner keeps
    // sending STATUS until its agents are joined, so its last STATUS is at or
    // after this instant, and "left after the staleness timeout" then reads
    // `elapsed >= stale` with no dependence on how long the service and node
    // stops take. The 50 ms slack covers the clock-domain difference only
    // (the soft table stamps wall time; this is the monotonic clock).
    let stopped = Instant::now();
    learner2_svc.take().unwrap().stop();
    c.nodes[2].stop();
    await_until(30, "the stopped learner left holders(p)", || {
        query_holders(c.node(leader), p) == vec![l3]
    });
    assert!(
        stopped.elapsed() + Duration::from_millis(50) >= stale,
        "the learner left holders() after {:?}, before the {stale:?} staleness timeout — \
         something other than staleness removed it",
        stopped.elapsed()
    );

    // A fetch routed by holders() lands from the surviving holder.
    let v = *c.voters().iter().find(|&&i| i != leader).unwrap();
    let from = query_holders(c.node(leader), p)[0];
    await_until(30, "the holder persisted the set as its floor", || {
        floor(&c, from as usize) == p
    });
    assert_eq!(
        c.node(v).snapshot_set_position(),
        0,
        "the voter holds nothing yet"
    );
    c.node(v)
        .request_fetch(from, None)
        .expect("the fetch was accepted");
    await_until(60, "the fetched set landed on the voter", || {
        c.node(v).snapshot_set_position() >= p
    });
    assert!(
        holds_on_disk(c.dir(v), &[0], p),
        "the voter holds the fetched set"
    );
    await_until(
        30,
        "the leader's soft table lists the voter as a holder too",
        || query_holders(c.node(leader), p).contains(&c.nodes[v].id),
    );

    c.stop();
}

/// Catalog spec §4.6: on a learner-only cluster the standby set is agreed
/// and the voters KNOW the cluster floor, yet purge nothing below a set they
/// do not hold — until a fetch lands it on one of them, and then only that
/// one's floor moves.
///
/// Snapshot-lifecycle spec §11 e2e 2: with `[settings] auto_fetch = false`
/// voters on a learner-only cluster hold nothing and never purge — the
/// documented trade of the switch — and every
/// `uc2_snapshot_auto_fetch_total` outcome stays 0 on them: the switch off
/// attempts nothing.
///
/// Red twins: assert a voter's `archive_first_base > 0` after the 10 s hold —
/// it is not; drop the fetch — voter 0's floor never moves; flip the
/// fixture's `auto_fetch` to `true` — "voter 0 holds nothing" fails inside
/// the hold.
///
/// Which assertions are the CATALOG's teeth: the no-purge half alone ("a
/// voter that holds nothing purges nothing") would pass on a pre-catalog
/// build too — such a voter never had a complete set to float its floor on.
/// The catalog-specific ones are that every voter reads the AGREED standby
/// set as the cluster floor (`agreed_position == p`) while holding nothing;
/// that the entry is catalogued as `Standby`; that the leader's soft table
/// names ONLY the learner as a holder; and that a fetch moves exactly the
/// fetching voter's floor to that agreed set and no one else's.
#[test]
fn learner_only_voters_do_not_purge_until_they_fetch() {
    let _g = serialize();
    let mut c = spawn(
        2,
        1,
        Opts {
            settings: Settings {
                auto_fetch: false,
                ..Settings::genesis_default()
            },
            ..opts("catalog-learner-only", true)
        },
        |_| true,
    );
    start_sums(&mut c);
    let leader = await_single_leader(&c, 30);
    let learner = 2usize;
    let learner_id = c.nodes[learner].id;
    await_capable(&c, &[0]);
    submit_frames(c.node(leader), 6000);
    let p = command_standby_instant(c.node(leader));
    await_until(60, "the learner completed the standby set", || {
        c.node(learner).snapshot_set_position() >= p
    });
    await_until(60, "the catalog agreed the standby set", || {
        agreed_position(c.node(leader)) == p
    });
    let e = entry(c.node(leader), p).unwrap();
    assert_eq!(e.kind, SetKind::Standby, "the set is catalogued as Standby");
    await_until(30, "the leader's soft table names the learner", || {
        query_holders(c.node(leader), p) == vec![learner_id]
    });
    assert_eq!(
        query_holders(c.node(leader), p),
        vec![learner_id],
        "only the learner holds the standby set"
    );
    assert!(p > SEG, "need >1 segment below p (p={p})");

    submit_frames(c.node(leader), 6000);
    // A negative, so it is HELD for a span (twice the 5 s report timeout,
    // many purge ticks and auto-fetch passes) and asserted throughout,
    // rather than read once after a sleep.
    await_until(30, "every voter learned the agreed standby set", || {
        [0usize, 1].iter().all(|&v| agreed_position(c.node(v)) == p)
    });
    let hold = Instant::now() + Duration::from_secs(10);
    while Instant::now() < hold {
        for v in [0usize, 1] {
            assert_eq!(
                c.node(v).snapshot_set_position(),
                0,
                "voter {v} holds nothing\n{}",
                dump_lifecycle(&c, p)
            );
            assert_eq!(
                c.node(v).archive_first_base(),
                0,
                "voter {v} must not purge below a set it does not hold"
            );
            assert_eq!(
                agreed_position(c.node(v)),
                p,
                "…though it knows the cluster floor"
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    // Snapshot-lifecycle spec §11 e2e 2: the switch off attempts NOTHING —
    // not a fetch that failed, not a skip; every outcome reads 0.
    for v in [0usize, 1] {
        for outcome in FETCH_OUTCOMES {
            assert_eq!(
                metric_labeled(
                    c.node(v),
                    "uc2_snapshot_auto_fetch_total",
                    &format!("outcome=\"{outcome}\"")
                ),
                0,
                "voter {v}: auto_fetch = false attempts nothing ({outcome})"
            );
        }
    }
    await_until(
        30,
        "the learner persisted the standby set as its floor",
        || floor(&c, learner) == p,
    );

    c.node(0)
        .request_fetch(learner_id, None)
        .expect("fetch from the learner");
    await_until(60, "voter 0 landed the set", || {
        c.node(0).snapshot_set_position() >= p
    });
    await_until(60, "voter 0's floor moved", || {
        c.node(0).archive_first_base() > 0
    });
    assert_eq!(floor(&c, 0), p, "voter 0's floor is the fetched set");
    assert_eq!(
        c.node(1).archive_first_base(),
        0,
        "voter 1 still holds nothing"
    );
    assert_eq!(floor(&c, 1), 0, "voter 1's floor never moved");

    c.stop();
}

// --------------------------------------------- snapshot lifecycle (spec §11)

/// Snapshot-lifecycle spec §11 e2e 1: a learner-only cluster with auto-fetch
/// ON (the genesis default). After a standby instant every voter fetches the
/// agreed set in the background, holds it, and purges below it — no operator
/// step. Each voter counts the fetch `ok`, audits it as `actor = "auto"`, and
/// — the set was agreed over ONE reporter — names that (review focus 5).
///
/// Red twin: make `maybe_auto_fetch` return at its first line — "voter 0
/// auto-fetched the agreed set" times out.
#[test]
fn learner_only_voters_auto_fetch_the_agreed_set_and_purge_below_it() {
    let _g = serialize();
    let obs = ObsCapture::take();
    let mut c = spawn(2, 1, opts("lifecycle-auto-fetch", true), |_| true);
    start_sums(&mut c);
    let leader = await_single_leader(&c, 30);
    let learner = 2usize;
    await_capable(&c, &[0]);
    submit_frames(c.node(leader), 6000);
    let p = command_standby_instant(c.node(leader));
    await_or_dump(&c, p, 60, "the learner completed the standby set", || {
        c.node(learner).snapshot_set_position() >= p
    });
    await_agreed(&c, &[0, 1, 2], p);
    assert!(p > SEG, "need >1 segment below p (p={p})");
    for v in [0usize, 1] {
        await_or_dump(
            &c,
            p,
            90,
            &format!("voter {v} auto-fetched the agreed set"),
            || c.node(v).snapshot_set_position() >= p,
        );
        assert!(
            holds_on_disk(c.dir(v), &[0], p),
            "voter {v} holds the set on disk"
        );
    }
    submit_frames(c.node(leader), 6000);
    for v in [0usize, 1] {
        await_or_dump(
            &c,
            p,
            60,
            &format!("voter {v} persisted the fetched set and purged below it"),
            || floor(&c, v) == p && c.node(v).archive_first_base() > 0,
        );
        let base = c.node(v).archive_first_base();
        assert!(
            base <= p,
            "voter {v} purged at most to the fetched set (first_base={base}, p={p})"
        );
        await_or_dump(
            &c,
            p,
            30,
            &format!("voter {v} counted its fetch ok"),
            || metric_labeled(c.node(v), "uc2_snapshot_auto_fetch_total", "outcome=\"ok\"") >= 1,
        );
        let audit = std::fs::read_to_string(c.dir(v).join("audit.jsonl")).unwrap_or_default();
        assert!(
            audit.lines().any(|l| l.contains("\"actor\":\"auto\"")
                && l.contains("\"op_name\":\"snapshot_fetch\"")),
            "voter {v}: no auto snapshot_fetch audit record:\n{audit}"
        );
    }
    let p_text = p.to_string();
    assert!(
        obs.text()
            .lines()
            .any(|l| l.contains("snapshot_fetch_single_reporter") && l.contains(&p_text)),
        "a set agreed over one learner must be named as such"
    );
    c.stop();
}

/// Snapshot-lifecycle spec §11 e2e 3: a voter's in-memory service restarted
/// after an agreed instant installs the node's START SET at attach and
/// replays only the tail — `applied` jumps to the set rather than climbing
/// from 0.
///
/// Read race-free through [`APPLIES`]: `SumSm` counts a frame BEFORE the
/// service publishes `applied` past it, so the count read once `applied` is
/// first seen at or above `p` covers every frame the restarted service
/// applied to get there. Climbing from 0 makes that ≈ 4000 (every frame
/// below `p`); a start-set install makes it ≈ 0.
///
/// Red twin: make `install_start_set` return `Ok(None)` at its top — the
/// restart replays from 0 and the count reads ≈ 4000.
#[test]
fn a_restarted_in_memory_service_starts_from_the_local_set() {
    let _g = serialize();
    let mut c = spawn(3, 0, opts("lifecycle-start-set", false), |_| true);
    let leader = await_single_leader(&c, 30);
    let v = *c.voters().iter().find(|&&i| i != leader).unwrap();
    let mut v_svc = None;
    for i in c.running() {
        let s = start_sum(&c.nodes[i].instance_dir, c.app, i);
        if i == v {
            v_svc = Some(s);
        } else {
            c.svcs.push(stopper(s));
        }
    }
    await_capable(&c, &[0]);
    submit_frames(c.node(leader), 4000);
    let p = instant_until_complete(&c, leader, &[0, 1, 2]);
    await_agreed(&c, &[0, 1, 2], p);
    let version = <SumSm as uc_service::RawStateMachine>::VERSION;
    let page = c.cnc(v);
    await_or_dump(&c, p, 30, "the node published its start set", || {
        page.service_slot(0).snapshot_pos.start_set() == Some((p, version))
    });
    submit_frames(c.node(leader), 500);
    v_svc.take().unwrap().stop();
    APPLIES[v].store(0, Ordering::Relaxed);
    let svc = start_sum(c.dir(v), c.app, v);
    await_or_dump(
        &c,
        p,
        60,
        "the restarted row applied to the start set",
        || page.service_slot(0).applied.load_acquire() >= p,
    );
    let at_set = APPLIES[v].load(Ordering::Relaxed);
    assert!(
        at_set < 1000,
        "the restarted row applied {at_set} frames to reach the start set {p} — it climbed \
         from 0 (≈4000) instead of installing the set (≈0)"
    );
    await_applied(&c, leader, &[v], &[0]);
    let replayed = APPLIES[v].load(Ordering::Relaxed);
    assert!(
        replayed < 2000,
        "the restart applied {replayed} frames — it replayed from 0 (≈4500) instead of the \
         tail (≈500)"
    );
    c.svcs.push(stopper(svc));
    c.stop();
}

/// Snapshot-lifecycle spec §11 e2e 4: a pin naming an origin that is
/// complete on the leader but not yet AGREED is refused `61
/// pin_origin_not_agreed`; once agreement lands the same pin is accepted.
///
/// Controller ruling PF4: the door lets a pin through on an EMPTY catalog by
/// design, and the first instant on a fresh cluster leaves it empty — so a
/// first instant is taken and agreed (the catalog is then non-empty), and the
/// pin names the SECOND. "Not yet agreed" is made deterministic by one voter
/// whose row never attaches: it never reports, so the leader holds each
/// instant's row-0 record for the full 5 s report timeout
/// (`SNAP_REPORT_TIMEOUT_NS`) — the window the refused pin lands in.
/// Auto-fetch is off so the quiet voter's disk does not change under the
/// test.
///
/// Red twin: remove the 61 block in `apply_upgrade_pin` — the first pin is
/// accepted, `(0, 0) != (1, 61)`.
#[test]
fn a_pin_on_a_not_yet_agreed_origin_is_refused_61_then_accepted() {
    let _g = serialize();
    let mut c = spawn(
        3,
        0,
        Opts {
            settings: Settings {
                auto_fetch: false,
                ..Settings::genesis_default()
            },
            ..opts("lifecycle-pin-agreed", false)
        },
        |_| true,
    );
    let leader = await_single_leader(&c, 30);
    let quiet = *c.voters().iter().find(|&&i| i != leader).unwrap();
    let attached: Vec<usize> = c.running().into_iter().filter(|&i| i != quiet).collect();
    for &i in &attached {
        let s = start_sum(&c.nodes[i].instance_dir, c.app, i);
        c.svcs.push(stopper(s));
    }
    await_capable_on(&c, &attached, &[0]);
    submit_frames(c.node(leader), 2000);

    // The first instant, agreed (after the report timeout): the catalog is
    // no longer Empty, so the door must answer for agreement.
    let p1 = instant_until_complete(&c, leader, &attached);
    await_agreed(&c, &attached, p1);
    assert!(
        entry(c.node(leader), p1).is_some_and(|e| e.is_agreed()),
        "p1 is listed agreed — the catalog is not Empty"
    );

    submit_frames(c.node(leader), 2000);
    let p2 = command_instant(c.node(leader));
    await_or_dump(&c, p2, 30, "the leader completed the set at p2", || {
        c.node(leader).snapshot_set_position() == p2
    });
    assert_ne!(
        agreed_position(c.node(leader)),
        p2,
        "the premise: p2 must not be agreed yet (the quiet voter has not reported, and the \
         report timeout has not run out)\n{}",
        dump_lifecycle(&c, p2)
    );
    let version = c.cnc(leader).service_slot(0).status.version();
    let (status, reason, _) = admin_staged(
        c.dir(leader),
        &c.cnc(leader),
        uc_node::UPGRADE_PENDING_FILE,
        ADMIN_OP_UPGRADE_PIN,
        &pin_bytes(0, version, p2),
    );
    assert_eq!(
        (status, reason),
        (1, uc_node::REASON_PIN_ORIGIN_NOT_AGREED),
        "complete on the leader, not yet agreed (the quiet voter has not reported)\n{}",
        dump_lifecycle(&c, p2)
    );
    assert!(
        c.node(leader)
            .cluster_view()
            .snapshot_inner()
            .pins
            .is_empty(),
        "a refused pin commits nothing"
    );

    await_agreed(&c, &attached, p2);
    let at = pin(&c, leader, 0, version, p2);
    assert!(at > p2, "the pin was appended once the origin agreed");
    await_until(30, "every attached node committed the pin", || {
        attached.iter().all(|&i| {
            c.node(i)
                .cluster_view()
                .snapshot_inner()
                .pins
                .iter()
                .any(|pin| pin.origin == p2)
        })
    });
    c.stop();
}

/// Snapshot-lifecycle spec §11 e2e 5: a node whose free space is below the
/// §7.3 check skips the fetch (`no_space`), names it once with the set's
/// position, downloads nothing, and its gauge reports the agreed set's
/// (known) size. The other voter, with room, fetches as usual.
///
/// Red twin: make `auto_fetch::fits` return `true` unconditionally — the
/// starved voter fetches, and "a no_space skip" never counts.
#[test]
fn a_node_without_room_skips_the_fetch_names_it_and_reports_the_size() {
    let _g = serialize();
    let obs = ObsCapture::take();
    let mut c = spawn(2, 1, opts("lifecycle-no-space", true), |_| true);
    start_sums(&mut c);
    let leader = await_single_leader(&c, 30);
    let learner = 2usize;
    let starved = *c.voters().iter().find(|&&i| i != leader).unwrap();
    let starved_id = c.nodes[starved].id;
    c.node(starved).set_free_bytes_for_test(1);
    await_until(30, "the starved voter's probe reads the override", || {
        c.node(starved)
            .soft_table()
            .by_node
            .get(&starved_id)
            .is_some_and(|e| e.holdings.free_bytes == 1)
    });
    await_capable(&c, &[0]);
    submit_frames(c.node(leader), 3000);
    let p = command_standby_instant(c.node(leader));
    await_or_dump(&c, p, 60, "the learner completed the standby set", || {
        c.node(learner).snapshot_set_position() >= p
    });
    await_agreed(&c, &[0, 1, 2], p);
    await_or_dump(&c, p, 90, "the leader auto-fetched the set", || {
        c.node(leader).snapshot_set_position() >= p
    });
    await_or_dump(
        &c,
        p,
        30,
        "the starved voter counted a no_space skip",
        || {
            metric_labeled(
                c.node(starved),
                "uc2_snapshot_auto_fetch_total",
                "outcome=\"no_space\"",
            ) >= 1
        },
    );
    // A negative, held across several backoff retries rather than read once.
    let hold = Instant::now() + Duration::from_secs(3);
    while Instant::now() < hold {
        assert_eq!(
            c.node(starved).snapshot_set_position(),
            0,
            "no download on the starved voter\n{}",
            dump_lifecycle(&c, p)
        );
        assert_eq!(
            metric_labeled(
                c.node(starved),
                "uc2_snapshot_auto_fetch_total",
                "outcome=\"ok\""
            ),
            0,
            "the starved voter fetched nothing"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let total = entry(c.node(starved), p).unwrap().total_size();
    assert!(
        total > 0,
        "a set catalogued after this change has a known size"
    );
    assert_eq!(
        metric(c.node(starved), "uc2_snapshot_newest_agreed_bytes"),
        total,
        "the starved voter's gauge reports the agreed set's size"
    );
    let p_text = p.to_string();
    let starved_field = format!("\"node\":{starved_id}");
    let named: Vec<String> = obs
        .text()
        .lines()
        .filter(|l| l.contains("snapshot_fetch_skipped_no_space") && l.contains(&starved_field))
        .map(str::to_owned)
        .collect();
    assert_eq!(
        named.len(),
        1,
        "the skip is named ONCE by the starved voter: {named:?}"
    );
    assert!(
        named[0].contains(&p_text),
        "the skip names the set's position {p}: {}",
        named[0]
    );
    c.stop();
}

/// Ruling R23: the fixture fails BY NAME, never hangs. A follower answers
/// `NotServing` to every submit, and [`submit_frames`] must panic on the
/// first one — naming the frame and that leadership moved — rather than
/// spin. Cheap: two voters, no services, no instant.
#[test]
fn submit_frames_fails_by_name_on_a_node_that_does_not_serve() {
    let _g = serialize();
    let o = Opts {
        app: "catalog-fixture",
        services: ServicesConfig::none_for_tests(),
        purge: false,
        settings: Settings::genesis_default(),
    };
    let c = spawn(2, 0, o, |_| true);
    let leader = await_single_leader(&c, 30);
    let follower = 1 - leader;
    let t0 = Instant::now();
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        submit_frames(c.node(follower), 10)
    }));
    let elapsed = t0.elapsed();
    let msg = r
        .expect_err("submit_frames on a follower must panic, not return")
        .downcast::<String>()
        .map(|b| *b)
        .unwrap_or_default();
    assert!(
        msg.contains("frame 0 of 10 refused NotServing") && msg.contains("leadership moved"),
        "the panic must name the frame and the cause: {msg}"
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "it must fail at once, not after a deadline: {elapsed:?}"
    );
    c.stop();
}
