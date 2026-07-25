// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Linearizable-read profile harness. See
//! `docs/superpowers/specs/2026-07-25-uc2-read-profile-design.md`.

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};

use hdrhistogram::Histogram;
use uc2_consensus::election::NodeId;
use uc2_log::cnc::CncPage;
use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};
use uc2_service::{ServiceBuilder, ServiceConfig, StateMachine};
use uc_protocol::ring::{BroadcastConsumer, BroadcastRing, MpscRing, RingError};
use uc_protocol::v2::cnc::NODE_FLAG_CAN_SERVE;
use uc_protocol::v2::ipc::{
    FLAG_V2_LINEARIZABLE, MSG_V2_NOT_LEADER, MSG_V2_QUERY, MSG_V2_RESPONSE, MSG_V2_RETRY,
    client_from_extra, extra_client,
};

/// Well-known file names under the instance dir — the shared contract with
/// `uc2_node::InstanceDir` (`uc2_node/src/ipc.rs`). Hardcoded here rather than
/// via `InstanceDir`, which requires the exclusive flock only the owning node
/// may take: this harness is an ATTACHING party, exactly like `uc2_client`.
const CNC_FILE: &str = "cnc2.dat";
const QUERY_RING: &str = "query.ring";
const EGRESS_SERVICE: &str = "egress_service.broadcast";
const EGRESS_NODE: &str = "egress_node.broadcast";

const ALL_APP_ID: &str = "uc2-read-profile-smoke";

const NODE_BUFFER_BYTES: usize = 256 << 20;
/// In-process smoke/ladder buffer. Deliberately far smaller than the fleet's
/// 256 MiB: `all`/`ladder` boot THREE nodes in one process, and this box is
/// shared with a concurrent model-checking session (no swap — an OOM SIGKILLs
/// the largest process). Local runs prove wiring, not throughput, so the hot
/// window does not need to be large.
const SMOKE_BUFFER_BYTES: usize = 32 << 20;
const NODE_MAX_PAYLOAD: usize = 512;
const ELECTION_TIMEOUT_MIN_NS: u64 = 150_000_000;
const ELECTION_TIMEOUT_MAX_NS: u64 = 300_000_000;

/// Slot-array size for the correlation tables — vastly larger than any
/// realistic in-flight window, so a slot is never reused while its previous
/// occupant is outstanding.
const SLOTS: usize = 1 << 20;
const SLOT_MASK: usize = SLOTS - 1;
const HIST_MAX_NS: u64 = 60_000_000_000;
const DRAIN_GRACE: Duration = Duration::from_secs(5);
const LEADER_WAIT: Duration = Duration::from_secs(30);

#[derive(Parser)]
#[command(
    name = "read_profile",
    about = "UC v2 linearizable-read profile: does the ReadIndex barrier cost read capacity?"
)]
struct Cli {
    #[command(subcommand)]
    role: Role,
}

#[derive(Subcommand)]
enum Role {
    /// Cluster-member node (fleet: one process per host).
    Node(NodeArgs),
    /// State-machine service, attached to a running node.
    Service(ServiceArgs),
    /// The measuring read client (bypasses uc2_client — see `run_read_measurement`).
    Client(ClientArgs),
    /// Local smoke: 3 nodes + 3 services + 1 read client, in-process. NOT a fleet number.
    All(AllArgs),
    /// Local smoke: sweep the concurrency ladder across both arms and both mixes.
    Ladder(LadderArgs),
}

#[derive(clap::Args)]
struct NodeArgs {
    #[arg(long)]
    id: NodeId,
    #[arg(long)]
    bind: SocketAddr,
    #[arg(long)]
    instance_dir: PathBuf,
    /// Comma-separated `id@addr` member list (every member INCLUDING self).
    #[arg(long)]
    members: String,
    #[arg(long, default_value = "uc2-read-profile")]
    app_id: String,
    #[arg(long, default_value_t = 256)]
    admission_kib: u64,
}

#[derive(clap::Args)]
struct ServiceArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "uc2-read-profile")]
    app_id: String,
}

/// Which arm of the A/B to run. The ONLY difference is `FLAG_V2_LINEARIZABLE`
/// on the query record — `node.rs:1956` is the fork.
#[derive(Copy, Clone, Debug, PartialEq, Eq, clap::ValueEnum)]
enum Mode {
    /// Linearizable read: READ_PROBE quorum barrier + frontier wait.
    Lin,
    /// Snapshot read: forwarded straight to the service (`node.rs:1958`).
    Snap,
}

#[derive(clap::Args)]
struct ClientArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "uc2-read-profile")]
    app_id: String,
    #[arg(long, default_value_t = 10)]
    secs: u64,
    /// Concurrent in-flight reads — the ladder axis.
    #[arg(long, default_value_t = 64)]
    readers: u64,
    #[arg(long, value_enum, default_value_t = Mode::Lin)]
    mode: Mode,
    /// Background writes/sec (0 = the read-only arm).
    #[arg(long, default_value_t = 0)]
    write_rate: u64,
    /// PID of the node process, for /proc agent-occupancy sampling. Omit to
    /// skip occupancy (the client cannot see another process's threads).
    #[arg(long)]
    node_pid: Option<u32>,
}

#[derive(clap::Args)]
struct AllArgs {
    #[arg(long, default_value_t = 10)]
    secs: u64,
    #[arg(long, default_value_t = 64)]
    readers: u64,
    #[arg(long, value_enum, default_value_t = Mode::Lin)]
    mode: Mode,
    #[arg(long, default_value_t = 0)]
    write_rate: u64,
    #[arg(long)]
    root: Option<PathBuf>,
}

#[derive(clap::Args)]
struct LadderArgs {
    /// Seconds per rung (each rung is one arm at one concurrency).
    #[arg(long, default_value_t = 6)]
    secs: u64,
    /// Concurrency rungs to sweep.
    #[arg(long, value_delimiter = ',', default_values_t = [1u64, 4, 16, 64, 256, 1024])]
    readers: Vec<u64>,
    /// Background writes/sec for the mixed arm.
    #[arg(long, default_value_t = 20_000)]
    write_rate: u64,
    #[arg(long)]
    root: Option<PathBuf>,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.role {
        Role::Node(a) => run_node(a),
        Role::Service(a) => run_service(a),
        Role::Client(a) => run_client_role(a),
        Role::All(a) => run_all(a),
        Role::Ladder(a) => run_ladder(a),
    }
}

/// One agent thread's yield rate over a measurement window.
///
/// **Why yields and not CPU time:** the node's agents idle on
/// `IdleStrategy::Yield` (`uc2_log/src/agent.rs:28` → `std::thread::yield_now()`),
/// so an IDLE agent still burns a core in a yield loop and CPU% is saturated by
/// construction. Each empty duty cycle costs one `sched_yield`, which the kernel
/// counts in `voluntary_ctxt_switches` — so a LOW yield rate means a BUSY agent.
/// This is an ordinal signal (it ranks agents); it is not a duty-cycle percentage.
#[derive(Debug, Clone, PartialEq)]
struct Occupancy {
    pub name: String,
    pub yields_per_sec: f64,
}

/// Read `(thread_name, voluntary_ctxt_switches)` for every thread under a
/// `/proc/<pid>/task` directory. Threads that vanish mid-scan (exited between
/// readdir and read) are skipped rather than failing the sample.
fn sample_yields(task_dir: &Path) -> std::io::Result<Vec<(String, u64)>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(task_dir)? {
        let path = entry?.path();
        let Ok(comm) = std::fs::read_to_string(path.join("comm")) else { continue };
        let Ok(status) = std::fs::read_to_string(path.join("status")) else { continue };
        let yields = status
            .lines()
            .find_map(|l| l.strip_prefix("voluntary_ctxt_switches:"))
            .and_then(|v| v.trim().parse::<u64>().ok());
        let Some(yields) = yields else { continue };
        out.push((comm.trim().to_string(), yields));
    }
    Ok(out)
}

/// Join two samples by thread name and rank by yield rate ASCENDING — fewest
/// yields first, i.e. busiest agent first. Threads missing from either sample
/// are dropped (they did not exist for the whole window, so their rate is not
/// comparable).
fn occupancy_delta(
    before: &[(String, u64)],
    after: &[(String, u64)],
    secs: f64,
) -> Vec<Occupancy> {
    let mut out: Vec<Occupancy> = after
        .iter()
        .filter_map(|(name, late)| {
            let (_, early) = before.iter().find(|(n, _)| n == name)?;
            Some(Occupancy {
                name: name.clone(),
                yields_per_sec: late.saturating_sub(*early) as f64 / secs,
            })
        })
        .collect();
    out.sort_by(|a, b| a.yields_per_sec.total_cmp(&b.yields_per_sec));
    out
}

/// The harness state machine: a counter. `apply` is the cheapest possible
/// deterministic mutation and `query` returns the count, so the measurement
/// isolates the read pipeline rather than any user business logic. The count
/// is monotonically non-decreasing, which is what makes the Task 4 monotonic
/// read guard possible.
#[derive(Default)]
struct ProfileSm {
    count: u64,
    last_applied: Option<u64>,
}

impl StateMachine for ProfileSm {
    type Command = Vec<u8>;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, position: u64, _cmd: Vec<u8>) -> u64 {
        self.count += 1;
        self.last_applied = Some(position);
        self.count
    }

    fn query(&self, _q: ()) -> u64 {
        self.count
    }

    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

/// Sandbox safety cap (m1–m5 pattern): clip `requested` to the env var when set
/// and nonzero; unset/zero is a no-op (the fleet's mode).
fn env_cap(var: &str, requested: u64) -> u64 {
    match std::env::var(var).ok().and_then(|s| s.parse::<u64>().ok()) {
        Some(cap) if cap > 0 => requested.min(cap),
        _ => requested,
    }
}

/// A distinct, index-derived election seed so each node's randomized timeout
/// differs — a clean boot then elects exactly one leader (m4/m5 precedent).
fn seed_for(id: NodeId) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn parse_members(s: &str) -> Vec<(NodeId, SocketAddr)> {
    s.split(',')
        .map(|part| {
            let (id, addr) = part
                .split_once('@')
                .unwrap_or_else(|| panic!("bad --members entry {part:?}, expected id@addr"));
            let id: NodeId = id.parse().unwrap_or_else(|e| panic!("bad member id {id:?}: {e}"));
            let addr: SocketAddr =
                addr.parse().unwrap_or_else(|e| panic!("bad member addr {addr:?}: {e}"));
            (id, addr)
        })
        .collect()
}

fn node_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    bind: SocketAddr,
    instance_dir: PathBuf,
    app_id: String,
    admission_bytes: u64,
    buffer_bytes: usize,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        bind,
        instance_dir,
        app_id,
        buffer_bytes,
        max_payload: NODE_MAX_PAYLOAD,
        admission_bytes,
        election_timeout_min_ns: ELECTION_TIMEOUT_MIN_NS,
        election_timeout_max_ns: ELECTION_TIMEOUT_MAX_NS,
        seed: seed_for(id),
        faults: FaultConfig::default(),
        purge: uc2_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
    }
}

fn run_node(a: NodeArgs) -> anyhow::Result<()> {
    assert!(
        !a.instance_dir.starts_with("/tmp"),
        "node instance_dir must be on a real filesystem (never /tmp — RAM tmpfs, no swap)"
    );
    let id = a.id;
    let members = parse_members(&a.members);
    let cfg = node_config(
        a.id,
        members,
        a.bind,
        a.instance_dir,
        a.app_id,
        a.admission_kib * 1024,
        NODE_BUFFER_BYTES,
    );
    let _node = Node::start(cfg)?;
    println!("read_profile node {id} up (pid {}); parking", std::process::id());
    loop {
        std::thread::park();
    }
}

fn run_service(a: ServiceArgs) -> anyhow::Result<()> {
    let cnc = a.instance_dir.join(CNC_FILE);
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cnc.exists() {
        anyhow::ensure!(Instant::now() < deadline, "timed out waiting for cnc2.dat at {cnc:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
    let cfg = ServiceConfig::new(a.instance_dir, a.app_id);
    let _svc = ServiceBuilder::new(cfg, ProfileSm::default()).start()?;
    println!("read_profile service up; parking");
    loop {
        std::thread::park();
    }
}

/// Wait for EXACTLY one serving leader; assert no split-brain throughout
/// (m4/m5/lincheck_v2 precedent). Returns the leader's index.
fn await_single_leader(nodes: &[Node], secs: u64) -> usize {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let serving: Vec<usize> =
            (0..nodes.len()).filter(|&i| nodes[i].can_serve() && nodes[i].is_leader()).collect();
        assert!(serving.len() <= 1, "split-brain in smoke cluster: nodes {serving:?} all serve");
        if serving.len() == 1 {
            return serving[0];
        }
        assert!(Instant::now() < deadline, "no leader elected within {secs}s");
        std::thread::yield_now();
    }
}

/// Boot a 3-node in-process cluster with a service per node, elect a leader.
/// Returns `(nodes, services, instance_dirs, leader_index)`.
#[allow(clippy::type_complexity)]
fn boot_cluster(
    root: &Path,
    app_id: &str,
) -> anyhow::Result<(Vec<Node>, Vec<uc2_service::Service<ProfileSm>>, Vec<PathBuf>, usize)> {
    assert!(
        !root.starts_with("/tmp"),
        "root must be on a real filesystem (never /tmp — RAM tmpfs, no swap); got {root:?}"
    );
    let _ = std::fs::remove_dir_all(root);
    std::fs::create_dir_all(root)?;

    const N: usize = 3;
    let socks: Vec<UdpSocket> =
        (0..N).map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind")).collect();
    let members: Vec<(NodeId, SocketAddr)> =
        socks.iter().enumerate().map(|(i, s)| (i as NodeId, s.local_addr().unwrap())).collect();

    let mut nodes = Vec::with_capacity(N);
    let mut services = Vec::with_capacity(N);
    let mut dirs = Vec::with_capacity(N);
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = members[i].1;
        let instance_dir = root.join(format!("n{i}"));
        let cfg = node_config(
            i as NodeId,
            members.clone(),
            addr,
            instance_dir.clone(),
            app_id.into(),
            256 * 1024,
            SMOKE_BUFFER_BYTES,
        );
        let node = Node::start_with_socket(cfg, sock).expect("node start");
        let svc =
            ServiceBuilder::new(ServiceConfig::new(&instance_dir, app_id), ProfileSm::default())
                .start()
                .expect("service start");
        nodes.push(node);
        services.push(svc);
        dirs.push(instance_dir);
    }
    let leader = await_single_leader(&nodes, 30);
    Ok((nodes, services, dirs, leader))
}

/// Node-first-then-service teardown, per the v1/lincheck_v2 precedent: a node's
/// shutdown must not wait on a service that tore down first.
fn stop_cluster(nodes: Vec<Node>, services: Vec<uc2_service::Service<ProfileSm>>) {
    for node in nodes {
        node.stop();
    }
    for svc in services {
        svc.stop();
    }
}

struct ReadStats {
    reads: u64,
    retried: u64,
    not_leader: u64,
    duplicates: u64,
    overwritten: u64,
    inflight_at_end: u64,
    elapsed: Duration,
    reads_per_sec: f64,
    p50_ms: f64,
    p99_ms: f64,
    /// Highest counter value any read returned — fed to the Task 4 monotonic guard.
    max_read_value: u64,
}

struct MatcherCtx {
    send_ns: Arc<Box<[AtomicU64]>>,
    owner: Arc<Box<[AtomicU64]>>,
    resolved: Arc<AtomicU64>,
    reads: Arc<AtomicU64>,
    not_leader: Arc<AtomicU64>,
    retried: Arc<AtomicU64>,
    duplicates: Arc<AtomicU64>,
    overwritten: Arc<AtomicU64>,
    last_response_ns: Arc<AtomicU64>,
    max_read_value: Arc<AtomicU64>,
    hist: Arc<Mutex<Histogram<u64>>>,
    client_id: u32,
    t0: Instant,
}

/// Decode a query answer's payload: `0u64 LE placeholder ++ bincode(u64)`
/// (`uc2_service/src/egress.rs:62-66`). Returns None for a write response,
/// whose payload is the applied position + the write's own response.
fn decode_query_answer(payload: &[u8]) -> Option<u64> {
    let rest = payload.get(8..)?;
    bincode::serde::decode_from_slice::<u64, _>(rest, bincode::config::standard())
        .ok()
        .map(|(v, _)| v)
}

/// One duty cycle of the matcher: drain one record and resolve it if it is
/// addressed to this client.
///
/// Duplicate tolerance is the m5_gate contract verbatim: `owner[idx]` holds
/// `local_seq + 1` while outstanding and is cleared by whichever delivery wins
/// the CAS, so a service replay that re-publishes a historical response is
/// counted as a duplicate rather than double-timed.
fn poll_egress(ring: &mut BroadcastConsumer, ctx: &MatcherCtx, buf: &mut Vec<u8>) -> bool {
    match ring.try_read(buf) {
        Ok(Some(rec)) => {
            let (cid, local_seq) = client_from_extra(rec.header_extra);
            if cid != ctx.client_id {
                return true; // addressed to another client
            }
            let idx = (local_seq as usize) & SLOT_MASK;
            let expected = local_seq as u64 + 1;
            let claimed = ctx.owner[idx]
                .compare_exchange(expected, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();
            match rec.msg_type {
                MSG_V2_RESPONSE if claimed => {
                    let now = ctx.t0.elapsed().as_nanos() as u64;
                    let send = ctx.send_ns[idx].load(Ordering::Acquire);
                    let lat = now.saturating_sub(send).min(HIST_MAX_NS);
                    let _ = ctx.hist.lock().unwrap().record(lat);
                    if let Some(v) = decode_query_answer(buf) {
                        ctx.max_read_value.fetch_max(v, Ordering::Relaxed);
                    }
                    ctx.reads.fetch_add(1, Ordering::Relaxed);
                    ctx.resolved.fetch_add(1, Ordering::Relaxed);
                    ctx.last_response_ns.fetch_max(now, Ordering::Relaxed);
                }
                MSG_V2_RESPONSE => {
                    ctx.duplicates.fetch_add(1, Ordering::Relaxed);
                }
                MSG_V2_NOT_LEADER if claimed => {
                    ctx.not_leader.fetch_add(1, Ordering::Relaxed);
                    ctx.resolved.fetch_add(1, Ordering::Relaxed);
                }
                MSG_V2_RETRY if claimed => {
                    ctx.retried.fetch_add(1, Ordering::Relaxed);
                    ctx.resolved.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
            true
        }
        Ok(None) => false,
        Err(RingError::Overwritten) => {
            ctx.overwritten.fetch_add(1, Ordering::Relaxed);
            true
        }
        Err(_) => true,
    }
}

/// The measuring read client. Issues `--readers` concurrent reads, pipelined
/// through `query.ring` with this harness's own `local_seq`, and correlates
/// answers off both egress broadcasts.
///
/// **The A/B:** `mode` sets exactly one bit — `FLAG_V2_LINEARIZABLE`. With it
/// set the read takes the nonce + READ_PROBE + AwaitQuorum path; clear, the node
/// forwards it straight to the service (`node.rs:1956-1958`). Everything else —
/// admission, the per-cycle drain cap, the service, the egress path — is
/// identical, so the delta between arms IS the barrier's end-to-end cost.
fn run_read_measurement(
    instance_dir: &Path,
    app_id: &str,
    secs: u64,
    readers: u64,
    mode: Mode,
    write_rate: u64,
    node_task_dir: Option<PathBuf>,
) -> (ReadStats, Vec<Occupancy>) {
    let cnc = CncPage::open_file(&instance_dir.join(CNC_FILE), app_id)
        .unwrap_or_else(|e| panic!("cnc attach {instance_dir:?}: {e}"));
    let client_id = cnc.status().next_client_id.fetch_add(1) as u32;
    await_serving(&cnc, LEADER_WAIT);

    let (query_producer, _query_consumer) = MpscRing::open(&instance_dir.join(QUERY_RING))
        .unwrap_or_else(|e| panic!("open query.ring: {e}"))
        .into_split();
    let mut egress_service = BroadcastRing::open(&instance_dir.join(EGRESS_SERVICE))
        .unwrap_or_else(|e| panic!("open egress_service.broadcast: {e}"))
        .subscribe();
    let mut egress_node = BroadcastRing::open(&instance_dir.join(EGRESS_NODE))
        .unwrap_or_else(|e| panic!("open egress_node.broadcast: {e}"))
        .subscribe();

    // `ProfileSm::Query = ()`, so the query payload is bincode's encoding of
    // the unit type — encoded once and reused, keeping the send loop allocation-free.
    let query_bytes = bincode::serde::encode_to_vec((), bincode::config::standard())
        .expect("encode unit query");
    let flags = match mode {
        Mode::Lin => FLAG_V2_LINEARIZABLE,
        Mode::Snap => 0,
    };

    let send_ns: Arc<Box<[AtomicU64]>> =
        Arc::new((0..SLOTS).map(|_| AtomicU64::new(0)).collect::<Vec<_>>().into_boxed_slice());
    let owner: Arc<Box<[AtomicU64]>> =
        Arc::new((0..SLOTS).map(|_| AtomicU64::new(0)).collect::<Vec<_>>().into_boxed_slice());
    let sent = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let ctx = MatcherCtx {
        send_ns: Arc::clone(&send_ns),
        owner: Arc::clone(&owner),
        resolved: Arc::new(AtomicU64::new(0)),
        reads: Arc::new(AtomicU64::new(0)),
        not_leader: Arc::new(AtomicU64::new(0)),
        retried: Arc::new(AtomicU64::new(0)),
        duplicates: Arc::new(AtomicU64::new(0)),
        overwritten: Arc::new(AtomicU64::new(0)),
        last_response_ns: Arc::new(AtomicU64::new(0)),
        max_read_value: Arc::new(AtomicU64::new(0)),
        hist: Arc::new(Mutex::new(
            Histogram::new_with_bounds(1, HIST_MAX_NS, 3).expect("histogram"),
        )),
        client_id,
        t0: Instant::now(),
    };
    let t0 = ctx.t0;
    let resolved = Arc::clone(&ctx.resolved);
    let reads = Arc::clone(&ctx.reads);
    let not_leader = Arc::clone(&ctx.not_leader);
    let retried = Arc::clone(&ctx.retried);
    let duplicates = Arc::clone(&ctx.duplicates);
    let overwritten = Arc::clone(&ctx.overwritten);
    let last_response_ns = Arc::clone(&ctx.last_response_ns);
    let max_read_value = Arc::clone(&ctx.max_read_value);
    let hist = Arc::clone(&ctx.hist);

    let matcher = {
        let stop = Arc::clone(&stop);
        thread::Builder::new()
            .name("rp-matcher".into())
            .spawn(move || {
                let ctx = ctx;
                let mut buf = Vec::new();
                loop {
                    let mut did = false;
                    did |= poll_egress(&mut egress_service, &ctx, &mut buf);
                    did |= poll_egress(&mut egress_node, &ctx, &mut buf);
                    if !did {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        thread::sleep(Duration::from_micros(20));
                    }
                }
            })
            .expect("spawn matcher thread")
    };

    let writer = spawn_writer(instance_dir, app_id, write_rate, Arc::clone(&stop));

    // Sample agent occupancy across the measurement window only (after warm-up
    // attach, before drain) so boot-time yields do not pollute the rate.
    let occ_before = node_task_dir.as_ref().and_then(|d| sample_yields(d).ok());
    let occ_t0 = Instant::now();

    // Send loop: keep `readers` reads in flight. `RingError::Full` means
    // yield+retry, exactly like the real uc2_client.
    let mut local_seq: u32 = 0;
    let deadline = t0 + Duration::from_secs(secs);
    'send: while Instant::now() < deadline {
        // Pause while the attached node is not a serving leader: without this a
        // leadership flip degenerates into a NOT_LEADER feedback flood that
        // measures nothing (the m5_gate lesson).
        if cnc.status().flags.load_acquire() & NODE_FLAG_CAN_SERVE == 0 {
            thread::sleep(Duration::from_millis(1));
            continue;
        }
        while sent.load(Ordering::Relaxed).wrapping_sub(resolved.load(Ordering::Relaxed))
            >= readers
        {
            if Instant::now() >= deadline {
                break 'send;
            }
            thread::yield_now();
        }
        let idx = (local_seq as usize) & SLOT_MASK;
        send_ns[idx].store(t0.elapsed().as_nanos() as u64, Ordering::Release);
        owner[idx].store(local_seq as u64 + 1, Ordering::Release);
        let extra = extra_client(client_id, local_seq);
        loop {
            match query_producer.try_write(MSG_V2_QUERY, flags, extra, &query_bytes) {
                Ok(()) => break,
                Err(RingError::Full) => thread::yield_now(),
                Err(e) => panic!("query.ring write error: {e}"),
            }
        }
        sent.fetch_add(1, Ordering::Relaxed);
        local_seq = local_seq.wrapping_add(1);
    }
    let send_window_end_ns = t0.elapsed().as_nanos() as u64;
    let occ_secs = occ_t0.elapsed().as_secs_f64();
    let occ_after = node_task_dir.as_ref().and_then(|d| sample_yields(d).ok());

    let drain_deadline = Instant::now() + DRAIN_GRACE;
    while resolved.load(Ordering::Relaxed) < sent.load(Ordering::Relaxed)
        && Instant::now() < drain_deadline
    {
        thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Ordering::Relaxed);
    matcher.join().expect("matcher thread panicked");
    if let Some(w) = writer {
        w.join().expect("writer thread panicked");
    }

    let sends = sent.load(Ordering::Relaxed);
    let resolved_n = resolved.load(Ordering::Relaxed);
    // Drain-inclusive clock floored at the send window's end: a run whose
    // responses stop arriving mid-window must not excise its dead tail from the
    // denominator (the m5_gate lesson).
    let elapsed =
        Duration::from_nanos(last_response_ns.load(Ordering::Relaxed).max(send_window_end_ns));
    let n_reads = reads.load(Ordering::Relaxed);
    let reads_per_sec =
        if elapsed.as_secs_f64() > 0.0 { n_reads as f64 / elapsed.as_secs_f64() } else { 0.0 };
    let (p50_ms, p99_ms) = {
        let h = hist.lock().unwrap();
        let ms = |ns: u64| ns as f64 / 1e6;
        (ms(h.value_at_quantile(0.50)), ms(h.value_at_quantile(0.99)))
    };

    let occupancy = match (occ_before, occ_after) {
        (Some(b), Some(a)) => occupancy_delta(&b, &a, occ_secs.max(1e-9)),
        _ => Vec::new(),
    };

    (
        ReadStats {
            reads: n_reads,
            retried: retried.load(Ordering::Relaxed),
            not_leader: not_leader.load(Ordering::Relaxed),
            duplicates: duplicates.load(Ordering::Relaxed),
            overwritten: overwritten.load(Ordering::Relaxed),
            inflight_at_end: sends.saturating_sub(resolved_n),
            elapsed,
            reads_per_sec,
            p50_ms,
            p99_ms,
            max_read_value: max_read_value.load(Ordering::Relaxed),
        },
        occupancy,
    )
}

fn await_serving(cnc: &CncPage, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if cnc.status().flags.load_acquire() & NODE_FLAG_CAN_SERVE != 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "no serving leader at this instance_dir within {timeout:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

/// Background write load. Task 3 ships the no-op (`write_rate` is ignored);
/// Task 4 replaces the body with the real paced writer.
fn spawn_writer(
    _instance_dir: &Path,
    _app_id: &str,
    _write_rate: u64,
    _stop: Arc<AtomicBool>,
) -> Option<thread::JoinHandle<()>> {
    None
}

fn print_read_report(mode: Mode, readers: u64, write_rate: u64, s: &ReadStats, occ: &[Occupancy]) {
    let arm = match mode {
        Mode::Lin => "linearizable (probe barrier)",
        Mode::Snap => "snapshot (no barrier)",
    };
    println!();
    println!("========== uc2 read profile: {arm} ==========");
    println!("readers (in-flight)   : {readers}");
    println!("background writes/s   : {write_rate}");
    println!("reads resolved        : {}", s.reads);
    println!("retries               : {}", s.retried);
    println!("not_leader redirects  : {}", s.not_leader);
    println!("dup answers dropped   : {}", s.duplicates);
    println!("broadcast overwritten : {}", s.overwritten);
    println!("in-flight at end      : {}", s.inflight_at_end);
    println!("elapsed (drain-incl.) : {:.3} s", s.elapsed.as_secs_f64());
    println!("reads/s               : {:.0}", s.reads_per_sec);
    println!("p50                   : {:.3} ms", s.p50_ms);
    println!("p99                   : {:.3} ms", s.p99_ms);
    if occ.is_empty() {
        println!("agent occupancy       : (not sampled — pass --node-pid)");
    } else {
        println!("agent occupancy (busiest first, fewer yields = busier):");
        for o in occ {
            println!("    {:<22} {:>12.0} yields/s", o.name, o.yields_per_sec);
        }
    }
    println!("=================================================================");
}

fn run_all(a: AllArgs) -> anyhow::Result<()> {
    let root = a.root.unwrap_or_else(|| PathBuf::from("target/read_profile_smoke"));
    let (nodes, services, dirs, leader) = boot_cluster(&root, ALL_APP_ID)?;
    println!("leader elected: n{leader} at {:?}", dirs[leader]);
    let secs = env_cap("UC2_RP_MAX_SECS", a.secs);
    let readers = env_cap("UC2_RP_MAX_READERS", a.readers);
    println!("*** LOCAL SMOKE — NOT a fleet number *** (3 nodes + 3 services + client, one box)");
    let (stats, occ) = run_read_measurement(
        &dirs[leader],
        ALL_APP_ID,
        secs,
        readers,
        a.mode,
        a.write_rate,
        Some(PathBuf::from("/proc/self/task")),
    );
    print_read_report(a.mode, readers, a.write_rate, &stats, &occ);
    stop_cluster(nodes, services);
    anyhow::ensure!(stats.inflight_at_end == 0, "{} reads never resolved", stats.inflight_at_end);
    Ok(())
}

fn run_client_role(a: ClientArgs) -> anyhow::Result<()> {
    let secs = env_cap("UC2_RP_MAX_SECS", a.secs);
    let readers = env_cap("UC2_RP_MAX_READERS", a.readers);
    let task_dir = a.node_pid.map(|p| PathBuf::from(format!("/proc/{p}/task")));
    let (stats, occ) = run_read_measurement(
        &a.instance_dir,
        &a.app_id,
        secs,
        readers,
        a.mode,
        a.write_rate,
        task_dir,
    );
    print_read_report(a.mode, readers, a.write_rate, &stats, &occ);
    anyhow::ensure!(
        stats.inflight_at_end == 0,
        "{} reads never resolved — the run did not complete; its numbers describe nothing",
        stats.inflight_at_end
    );
    Ok(())
}

fn run_ladder(_a: LadderArgs) -> anyhow::Result<()> {
    anyhow::bail!("ladder role lands in Task 5")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Build a synthetic `/proc/<pid>/task` tree: one dir per thread, each
    /// holding a `comm` and a `status` file in the kernel's format.
    fn fake_task_dir(threads: &[(&str, u64)]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        for (i, (name, yields)) in threads.iter().enumerate() {
            let t = dir.path().join(format!("{}", 1000 + i));
            fs::create_dir(&t).unwrap();
            fs::write(t.join("comm"), format!("{name}\n")).unwrap();
            fs::write(
                t.join("status"),
                format!(
                    "Name:\t{name}\nThreads:\t1\nvoluntary_ctxt_switches:\t{yields}\n\
                     nonvoluntary_ctxt_switches:\t7\n"
                ),
            )
            .unwrap();
        }
        dir
    }

    #[test]
    fn samples_name_and_yield_count_per_thread() {
        let dir = fake_task_dir(&[("uc2-consensus", 100), ("uc2-sender", 250)]);
        let mut got = sample_yields(dir.path()).expect("sample");
        got.sort();
        assert_eq!(
            got,
            vec![
                ("uc2-consensus".to_string(), 100),
                ("uc2-sender".to_string(), 250)
            ]
        );
    }

    #[test]
    fn skips_threads_missing_files_rather_than_failing() {
        let dir = fake_task_dir(&[("uc2-consensus", 100)]);
        // A thread that exited between readdir and read: dir exists, files don't.
        fs::create_dir(dir.path().join("2000")).unwrap();
        let got = sample_yields(dir.path()).expect("sample");
        assert_eq!(got, vec![("uc2-consensus".to_string(), 100)]);
    }

    #[test]
    fn delta_ranks_busiest_first_and_normalizes_by_time() {
        let before = vec![("uc2-consensus".into(), 100u64), ("uc2-sender".into(), 100)];
        // Over 2 s: consensus yielded 20 times (busy), sender 2000 (idle).
        let after = vec![("uc2-consensus".into(), 120u64), ("uc2-sender".into(), 2100)];
        let got = occupancy_delta(&before, &after, 2.0);
        assert_eq!(got[0].name, "uc2-consensus", "busiest (fewest yields) ranks first");
        assert_eq!(got[0].yields_per_sec, 10.0);
        assert_eq!(got[1].name, "uc2-sender");
        assert_eq!(got[1].yields_per_sec, 1000.0);
    }

    #[test]
    fn delta_ignores_threads_absent_from_either_sample() {
        let before = vec![("uc2-consensus".into(), 100u64), ("gone".into(), 5)];
        let after = vec![("uc2-consensus".into(), 120u64), ("new".into(), 5)];
        let got = occupancy_delta(&before, &after, 1.0);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name, "uc2-consensus");
    }
}
