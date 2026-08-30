// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M6 gate: **learner join under load** + **snapshot-backed purge with
//! below-floor reconstruction**, both under sustained write load (spec §9 M6
//! row). Unlike m5_gate (a throughput headline), this gate measures the two M6
//! correctness/liveness properties that make purge safe:
//!
//! 1. **`learner-join`** — 3 voters under load, purge on; a fresh learner joins.
//!    PASS iff the learner's `durable` reaches the commit-at-join-start within
//!    `JOIN_BUDGET` (60 s) AND the leader's commit rate during the join dips
//!    < 10 % vs the pre-join baseline (no quorum stall — a learner is never in
//!    the quorum, so a correct join is invisible to commit progress).
//! 2. **`purge-cycle`** — with load running, drive N cycles of
//!    snapshot → purge → crash a follower's service → reconstruct. PASS iff every
//!    reconstruction converges (`service_applied` catches `commit`) within 10 s
//!    AND linearizable reads stay consistent (monotonic, matching the write
//!    stream — zero committed-value divergence).
//!
//! ```text
//! cargo run -p uc_node --release --example m6_gate -- all      --secs S   # local smoke, NOT the gate
//! cargo run -p uc_node --release --example m6_gate -- node     --id N --bind A --members … --instance-dir D [--learners …]
//! cargo run -p uc_node --release --example m6_gate -- service  --instance-dir D
//! cargo run -p uc_node --release --example m6_gate -- learner  --id N --bind A --members … --instance-dir D
//! ```
//!
//! **`node`/`service`/`learner`** are thin fleet-role wrappers (one process per
//! host, systemd-run-friendly) that park forever; the harness owns their
//! lifecycle. `learner` is a `node` whose own id is in the members' `--learners`
//! list, not `--members`.
//!
//! **`all`** boots everything in-process (real file-backed shmem, not separate OS
//! processes) under a tempdir guarded OFF `/tmp` and runs BOTH scenarios against
//! it. This is **SMOKE ONLY**: one dev box shares its cores across 3–4 nodes'
//! agents + services + the load driver — nothing like the fleet's one-role-per-
//! host budget. The strict < 10 % dip / N-cycle numbers are only claimable on the
//! fleet run (a 4th `c6id.2xlarge` learner host, same placement group).
//!
//! **Loud disclaimer:** completing this binary is NOT passing the M6 gate. The
//! gate is claimed only after the fleet run + the doc's verdict section says so.

use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};

use uc_client::{Client, ClientError};
use uc_consensus::election::NodeId;
use uc_log::cnc::CncPage;
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig, PurgePolicy};
use uc_protocol::v2::cnc::{NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER};
use uc_service::{
    ServiceBuilder, ServiceConfig, SnapshotError, SnapshotPolicy, SnapshotStateMachine, StateMachine,
};

// ------------------------------------------------------------------ CLI

#[derive(Parser)]
#[command(name = "m6_gate", about = "UC v2 M6 gate: learner-join + purge-cycle under load (spec §9)")]
struct Cli {
    #[command(subcommand)]
    role: Role,
}

#[derive(Subcommand)]
enum Role {
    /// Local smoke: 3 voters + a learner + services + load, in-process. NOT the gate.
    All(AllArgs),
    /// Voting cluster-member node (fleet: one process per host).
    Node(NodeArgs),
    /// A learner node (replicated-to, never counted). Own id NOT in --members.
    Learner(NodeArgs),
    /// State-machine service, attached to a running node.
    Service(ServiceArgs),
    /// Read a node's counters from its local cnc page and print one JSON line
    /// (the fleet orchestrator's observability channel — cnc is same-host only).
    Probe(ProbeArgs),
    /// Sustained write load + a monotonic linearizable-read consistency guard,
    /// attached to the LOCAL node (run on the leader host). Exits 1 on a read
    /// regression (committed-value divergence) — the fleet divergence check.
    #[command(name = "loadclient")]
    LoadClient(LoadClientArgs),
}

#[derive(clap::Args)]
struct AllArgs {
    /// Real-filesystem root for the instance dirs (never /tmp — RAM tmpfs).
    #[arg(long)]
    root: Option<PathBuf>,
    /// Seconds of baseline load before each scenario's action.
    #[arg(long, default_value_t = 6)]
    secs: u64,
    /// Purge-cycle count.
    #[arg(long, default_value_t = 3)]
    cycles: u32,
}

#[derive(clap::Args)]
struct NodeArgs {
    #[arg(long)]
    id: NodeId,
    #[arg(long)]
    bind: SocketAddr,
    #[arg(long)]
    instance_dir: PathBuf,
    /// Comma-separated `id@addr` VOTING member list (every voter INCLUDING self,
    /// or — for a learner — every voter, NOT self).
    #[arg(long)]
    members: String,
    /// Comma-separated `id@addr` learner list (optional).
    #[arg(long, default_value = "")]
    learners: String,
    #[arg(long, default_value = "m6-gate")]
    app_id: String,
}

#[derive(clap::Args)]
struct ServiceArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "m6-gate")]
    app_id: String,
}

#[derive(clap::Args)]
struct ProbeArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "m6-gate")]
    app_id: String,
}

#[derive(clap::Args)]
struct LoadClientArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "m6-gate")]
    app_id: String,
    /// Run for this many seconds (0 = until --stop-file appears).
    #[arg(long, default_value_t = 0)]
    secs: u64,
    /// Stop when this file exists (the orchestrator's kill switch).
    #[arg(long, default_value = "")]
    stop_file: String,
}

fn main() {
    let cli = Cli::parse();
    let r = match cli.role {
        Role::All(a) => run_all(a),
        Role::Node(a) => run_node(a, false),
        Role::Learner(a) => run_node(a, true),
        Role::Service(a) => run_service(a),
        Role::Probe(a) => run_probe(a),
        Role::LoadClient(a) => run_loadclient(a),
    };
    if let Err(e) = r {
        eprintln!("m6_gate error: {e:#}");
        std::process::exit(1);
    }
}

// ------------------------------------------------------------- the SM

/// A snapshot-capable register: `apply` stores the 8-byte LE payload as the
/// current value; `query` returns it. Snapshot-capable so the node can build
/// snapshots, advance the purge floor, and reconstruct a crashed service by
/// installing a snapshot below the floor. `SnapshotHandle` = the frozen
/// `(value, last_applied)` bytes.
#[derive(Default)]
struct RegSm {
    value: u64,
    last_applied: Option<u64>,
}

impl StateMachine for RegSm {
    type Command = Vec<u8>;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, position: u64, cmd: Vec<u8>) -> u64 {
        if cmd.len() >= 8 {
            self.value = u64::from_le_bytes(cmd[..8].try_into().unwrap());
        }
        self.last_applied = Some(position);
        self.value
    }
    fn query(&self, _q: ()) -> u64 {
        self.value
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

impl SnapshotStateMachine for RegSm {
    type SnapshotHandle = Vec<u8>;

    fn freeze(&self) -> Result<(Vec<u8>, u64), SnapshotError> {
        let pos = self.last_applied.unwrap_or(0);
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&self.value.to_le_bytes());
        buf.extend_from_slice(&pos.to_le_bytes());
        Ok((buf, pos))
    }

    fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        dst.write_all(&handle)?;
        Ok(())
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, SnapshotError> {
        let mut buf = Vec::new();
        src.read_to_end(&mut buf)?;
        if buf.len() < 16 {
            return Err(SnapshotError::Codec(format!("short snapshot: {} bytes", buf.len())));
        }
        let v = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let pos = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        if pos != position {
            return Err(SnapshotError::Codec(format!(
                "snapshot payload position {pos} != requested {position}"
            )));
        }
        self.value = v;
        self.last_applied = Some(position);
        Ok(position)
    }
}

// --------------------------------------------------------- shared config

const APP: &str = "m6-gate";
/// 4 MiB ring (matches the lincheck/failover harnesses).
const BUFFER_BYTES: usize = 1 << 22;
/// Small journal segments + snapshot cadence so purge actually drops prefixes
/// under the modest smoke workload (mirrors the lin_v2 purge capstone).
const SEGMENT_BYTES: u64 = 16 * 1024;
const SNAPSHOT_INTERVAL_BYTES: u64 = 32 * 1024;

fn seed_for(id: NodeId) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn parse_members(s: &str) -> Vec<(NodeId, SocketAddr)> {
    if s.is_empty() {
        return Vec::new();
    }
    s.split(',')
        .map(|part| {
            let (id, addr) =
                part.split_once('@').unwrap_or_else(|| panic!("bad member {part:?}, want id@addr"));
            (
                id.parse().unwrap_or_else(|e| panic!("bad id {id:?}: {e}")),
                addr.parse().unwrap_or_else(|e| panic!("bad addr {addr:?}: {e}")),
            )
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn make_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    learners: Vec<(NodeId, SocketAddr)>,
    bind: SocketAddr,
    instance_dir: PathBuf,
    app_id: String,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        learners,
        bind,
        instance_dir,
        app_id,
        buffer_bytes: BUFFER_BYTES,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: seed_for(id),
        faults: FaultConfig::default(),
        purge: PurgePolicy::BelowSnapshot { slack_bytes: 0 },
        journal_segment_bytes: SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::default(),
    }
}

fn spawn_service(dir: &std::path::Path) -> uc_service::Service<RegSm> {
    let cfg = ServiceConfig::new(dir, APP)
        .snapshot_policy(SnapshotPolicy { interval_bytes: SNAPSHOT_INTERVAL_BYTES });
    ServiceBuilder::new(cfg, RegSm::default())
        .start_with_snapshots()
        .expect("snapshot service start")
}

// ------------------------------------------------------- fleet role wrappers

fn run_node(a: NodeArgs, is_learner: bool) -> anyhow::Result<()> {
    anyhow::ensure!(
        !a.instance_dir.starts_with("/tmp"),
        "instance_dir must be on a real filesystem (never /tmp — RAM tmpfs)"
    );
    let members = parse_members(&a.members);
    let learners = parse_members(&a.learners);
    let cfg = make_config(a.id, members, learners, a.bind, a.instance_dir, a.app_id);
    let node = Node::start(cfg)?;
    let kind = if is_learner { "learner" } else { "node" };
    println!("m6_gate {kind} {} up; parking (harness owns lifecycle)", a.id);
    // Protocol 0.5.0: `reports_unattested` is a process-local counter, so the
    // cnc-reading `probe` role cannot see it. Emit it (with the wipe count) on
    // change to this unit's log — the orchestrator greps the logs, and a
    // SUSTAINED climb on a single-version fleet is the signal that honest
    // reports are being declined (liveness), or that the fleet is mixed.
    let mut last = (u64::MAX, u64::MAX);
    loop {
        let now = (node.reports_unattested(), node.wipes());
        if now != last {
            println!(
                "m6_gate {kind} {} stats: reports_unattested={} wipes={}",
                a.id, now.0, now.1
            );
            last = now;
        }
        thread::sleep(Duration::from_millis(500));
    }
}

fn run_service(a: ServiceArgs) -> anyhow::Result<()> {
    let cnc = a.instance_dir.join("cnc2.dat");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cnc.exists() {
        anyhow::ensure!(Instant::now() < deadline, "timed out waiting for {cnc:?}");
        thread::sleep(Duration::from_millis(20));
    }
    let _svc = spawn_service(&a.instance_dir);
    println!("m6_gate service up; parking (harness owns lifecycle)");
    loop {
        thread::park();
    }
}

/// Read the local node's counters from its cnc page and print ONE JSON line.
/// The fleet orchestrator's observability channel: cnc is shared memory, so this
/// must run ON the host whose node it probes. Never touches the node — read-only.
fn run_probe(a: ProbeArgs) -> anyhow::Result<()> {
    let cnc = CncPage::open_file(&a.instance_dir.join("cnc2.dat"), &a.app_id)
        .map_err(|e| anyhow::anyhow!("open cnc: {e:?}"))?;
    let flags = cnc.status().flags.load_acquire();
    let c = cnc.counters();
    println!(
        "{{\"leader\":{},\"can_serve\":{},\"term\":{},\"commit\":{},\"durable\":{},\
         \"append\":{},\"service_applied\":{},\"archive_first_base\":{},\"leader_hint\":{}}}",
        (flags & NODE_FLAG_LEADER) != 0,
        (flags & NODE_FLAG_CAN_SERVE) != 0,
        cnc.status().term.load_acquire(),
        c.commit.load_acquire(),
        c.durable.load_acquire(),
        c.append.load_acquire(),
        cnc.service().service_applied.load_acquire(),
        cnc.archive_first_base().load_acquire(),
        cnc.status().leader_hint.load_acquire(),
    );
    Ok(())
}

/// Sustained write load against the LOCAL node (run on the leader host), with a
/// monotonic linearizable-read guard: every so often it reads the register and
/// asserts the value never regresses and never exceeds what has been written —
/// the fleet's committed-value-divergence check. Exits 1 on a violation. Runs
/// until `--secs` elapse or `--stop-file` appears (0 secs + no stop-file = until
/// killed). Prints a periodic heartbeat and a final summary line.
fn run_loadclient(a: LoadClientArgs) -> anyhow::Result<()> {
    let stop_file = (!a.stop_file.is_empty()).then(|| PathBuf::from(&a.stop_file));
    let deadline = (a.secs > 0).then(|| Instant::now() + Duration::from_secs(a.secs));

    // Attach; retry briefly (the node may still be electing).
    let attach_deadline = Instant::now() + Duration::from_secs(30);
    let client = loop {
        match Client::connect(&a.instance_dir, &a.app_id) {
            Ok(c) => break c,
            Err(e) => {
                anyhow::ensure!(Instant::now() < attach_deadline, "attach failed: {e:?}");
                thread::sleep(Duration::from_millis(50));
            }
        }
    };

    let mut next_val = 1u64;
    let mut committed_max = 0u64;
    let mut last_read = 0u64;
    let mut writes = 0u64;
    let mut reads = 0u64;
    let mut last_beat = Instant::now();
    let start = Instant::now();

    loop {
        if deadline.is_some_and(|d| Instant::now() >= d) {
            break;
        }
        if stop_file.as_ref().is_some_and(|sf| sf.exists()) {
            break;
        }

        // Write.
        let payload = next_val.to_le_bytes().to_vec();
        match client.submit::<Vec<u8>, u64>(&payload) {
            Ok(_) => {
                committed_max = committed_max.max(next_val);
                next_val += 1;
                writes += 1;
            }
            Err(ClientError::NotLeader { .. }) | Err(ClientError::Retry) => {
                // Not the leader host (or a transient election) — wait; the
                // orchestrator runs this on the leader, so this is brief.
                thread::sleep(Duration::from_millis(20));
            }
            Err(ClientError::BackpressureFull) => thread::sleep(Duration::from_millis(2)),
            Err(_) => thread::sleep(Duration::from_millis(20)),
        }

        // Periodic monotonic linearizable read (the divergence guard). A transient
        // error carries no info, so it is simply skipped.
        if writes.is_multiple_of(64)
            && let Ok(v) = client.query_linearizable::<(), u64>(&())
        {
            reads += 1;
            if v < last_read {
                eprintln!(
                    "DIVERGENCE: linearizable read REGRESSED {last_read} -> {v} \
                     (committed_max={committed_max})"
                );
                std::process::exit(1);
            }
            if v > committed_max {
                eprintln!(
                    "DIVERGENCE: read {v} exceeds anything written (committed_max={committed_max})"
                );
                std::process::exit(1);
            }
            last_read = v;
        }

        if last_beat.elapsed() >= Duration::from_secs(2) {
            let rate = writes as f64 / start.elapsed().as_secs_f64();
            println!(
                "loadclient: writes={writes} reads={reads} committed_max={committed_max} \
                 last_read={last_read} rate={rate:.0}/s"
            );
            last_beat = Instant::now();
        }
    }

    let rate = writes as f64 / start.elapsed().as_secs_f64().max(1e-6);
    println!(
        "loadclient DONE: writes={writes} reads={reads} committed_max={committed_max} \
         last_read={last_read} rate={rate:.0}/s elapsed={:.1}s",
        start.elapsed().as_secs_f64()
    );
    Ok(())
}

// ------------------------------------------------------------ load driver

/// A background write-load driver: submits monotonically increasing 8-byte values
/// to the leader, reconnecting on `NOT_LEADER`. `next_val` is the value it is
/// about to write; `committed_max` is the highest value a submit confirmed `Ok`.
struct Load {
    stop: Arc<AtomicBool>,
    next_val: Arc<AtomicU64>,
    committed_max: Arc<AtomicU64>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Load {
    fn start(dirs: Arc<Vec<PathBuf>>) -> Load {
        let stop = Arc::new(AtomicBool::new(false));
        let next_val = Arc::new(AtomicU64::new(1));
        let committed_max = Arc::new(AtomicU64::new(0));
        let (s, nv, cm, d) =
            (Arc::clone(&stop), Arc::clone(&next_val), Arc::clone(&committed_max), Arc::clone(&dirs));
        let handle = thread::spawn(move || load_loop(s, nv, cm, d));
        Load { stop, next_val, committed_max, handle: Some(handle) }
    }
    fn committed_max(&self) -> u64 {
        self.committed_max.load(Ordering::Relaxed)
    }
    fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn load_loop(
    stop: Arc<AtomicBool>,
    next_val: Arc<AtomicU64>,
    committed_max: Arc<AtomicU64>,
    dirs: Arc<Vec<PathBuf>>,
) {
    let mut target = 0usize;
    let mut client: Option<Client> = None;
    while !stop.load(Ordering::Relaxed) {
        if client.is_none() {
            match Client::connect(&dirs[target], APP) {
                Ok(c) => client = Some(c),
                Err(_) => {
                    thread::sleep(Duration::from_millis(20));
                    target = (target + 1) % dirs.len();
                    continue;
                }
            }
        }
        let v = next_val.fetch_add(1, Ordering::Relaxed);
        let payload = v.to_le_bytes().to_vec();
        match client.as_ref().unwrap().submit::<Vec<u8>, u64>(&payload) {
            Ok(_) => {
                committed_max.fetch_max(v, Ordering::Relaxed);
            }
            Err(ClientError::NotLeader { hint }) => {
                let c = client.take().unwrap();
                c.shutdown();
                target = hint.map(|h| h as usize % dirs.len()).unwrap_or((target + 1) % dirs.len());
            }
            Err(ClientError::BackpressureFull) | Err(ClientError::Retry) => {
                thread::sleep(Duration::from_millis(2));
            }
            Err(_) => {
                // maybe-committed / transient: drop the client, keep going.
                if let Some(c) = client.take() {
                    c.shutdown();
                }
                thread::sleep(Duration::from_millis(10));
            }
        }
    }
    if let Some(c) = client {
        c.shutdown();
    }
}

/// A leader-routed linearizable read of the register value; `None` on any
/// transient error (routing/timeout/restart).
fn read_value(dirs: &[PathBuf], start: usize) -> Option<u64> {
    let mut target = start;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let client = match Client::connect(&dirs[target], APP) {
            Ok(c) => c,
            Err(_) => {
                target = (target + 1) % dirs.len();
                thread::sleep(Duration::from_millis(20));
                continue;
            }
        };
        let out = client.query_linearizable::<(), u64>(&());
        client.shutdown();
        match out {
            Ok(v) => return Some(v),
            Err(ClientError::NotLeader { hint }) => {
                target = hint.map(|h| h as usize % dirs.len()).unwrap_or((target + 1) % dirs.len());
            }
            Err(_) => {
                thread::sleep(Duration::from_millis(20));
            }
        }
    }
    None
}

// ------------------------------------------------------------- helpers

fn await_single_leader(nodes: &[Node], secs: u64) -> usize {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let serving: Vec<usize> =
            (0..nodes.len()).filter(|&i| nodes[i].can_serve() && nodes[i].is_leader()).collect();
        assert!(serving.len() <= 1, "split-brain: {serving:?}");
        if serving.len() == 1 {
            return serving[0];
        }
        assert!(Instant::now() < deadline, "no single serving leader within {secs}s");
        thread::yield_now();
    }
}

// --------------------------------------------------------------- the gate

struct Verdict {
    scenario: &'static str,
    pass: bool,
    detail: String,
}

fn run_all(a: AllArgs) -> anyhow::Result<()> {
    let root = a.root.unwrap_or_else(|| PathBuf::from("target/m6_gate_smoke"));
    anyhow::ensure!(
        !root.starts_with("/tmp"),
        "m6_gate all: root must be on a real filesystem (never /tmp — RAM tmpfs); got {root:?}"
    );
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root)?;

    println!("== uc2 M6 gate: SMOKE (local, in-process; 3 voters + 1 learner + services + load) ==");
    println!(
        "*** THIS IS NOT THE M6 GATE *** — one dev box, core-starved; the real bar (learner \
         join under fleet load + N purge cycles) is only claimable on the 3+1 c6id fleet run. \
         See docs/benchmarks/uc2-m6-gate-2026-07-12.md."
    );

    // 3 voters + 1 pre-provisioned learner (id 3). The learner is in every
    // voter's `learners` from boot (so they fan DATA out to it), but its NODE is
    // not started until the learner-join scenario.
    const NV: usize = 3;
    let socks: Vec<UdpSocket> =
        (0..NV + 1).map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind")).collect();
    let addrs: Vec<SocketAddr> = socks.iter().map(|s| s.local_addr().unwrap()).collect();
    let members: Vec<(NodeId, SocketAddr)> =
        (0..NV).map(|i| (i as NodeId, addrs[i])).collect();
    let learners: Vec<(NodeId, SocketAddr)> = vec![(NV as NodeId, addrs[NV])];

    let mut socks = socks;
    let learner_sock = socks.pop().unwrap(); // id 3's socket, held for join time
    let learner_dir = root.join("l3");

    let mut nodes: Vec<Node> = Vec::with_capacity(NV);
    let mut services: Vec<Option<uc_service::Service<RegSm>>> = Vec::with_capacity(NV);
    let mut dirs: Vec<PathBuf> = Vec::with_capacity(NV);
    for (i, sock) in socks.into_iter().enumerate() {
        let instance_dir = root.join(format!("n{i}"));
        let cfg = make_config(
            i as NodeId,
            members.clone(),
            learners.clone(),
            addrs[i],
            instance_dir.clone(),
            APP.into(),
        );
        nodes.push(Node::start_with_socket(cfg, sock).expect("node start"));
        services.push(Some(spawn_service(&instance_dir)));
        dirs.push(instance_dir);
    }

    let leader = await_single_leader(&nodes, 30);
    println!("leader elected: n{leader}");

    let dirs = Arc::new(dirs);
    let load = Load::start(Arc::clone(&dirs));

    let verdicts = vec![
        scenario_learner_join(
            &nodes,
            &members,
            &learners,
            &learner_dir,
            learner_sock,
            &load,
            a.secs,
        ),
        scenario_purge_cycle(&nodes, &mut services, &dirs, &load, a.cycles),
    ];

    load.stop();

    // Teardown: nodes first, then services (a node's shutdown must not wait on a
    // service torn down first).
    for node in nodes {
        node.stop();
    }
    for svc in services.into_iter().flatten() {
        svc.stop();
    }

    println!("\n== M6 gate smoke results ==");
    let mut all_pass = true;
    for v in &verdicts {
        println!("  [{}] {} — {}", if v.pass { "PASS" } else { "FAIL" }, v.scenario, v.detail);
        all_pass &= v.pass;
    }
    if all_pass {
        println!("RESULT: PASS (smoke) — both M6 scenarios held locally.");
        Ok(())
    } else {
        println!("RESULT: FAIL (honest) — a scenario did not hold.");
        std::process::exit(1);
    }
}

/// Scenario 1: a fresh learner joins voters under load; join must complete in
/// budget with no quorum stall (commit-rate dip < 10 %).
fn scenario_learner_join(
    nodes: &[Node],
    members: &[(NodeId, SocketAddr)],
    learners: &[(NodeId, SocketAddr)],
    learner_dir: &std::path::Path,
    learner_sock: UdpSocket,
    load: &Load,
    baseline_secs: u64,
) -> Verdict {
    const JOIN_BUDGET: Duration = Duration::from_secs(60);
    let leader = await_single_leader(nodes, 20);

    // Baseline commit rate over `baseline_secs` before the join.
    let c0 = nodes[leader].counters().commit.load_acquire();
    let t0 = Instant::now();
    thread::sleep(Duration::from_secs(baseline_secs));
    let c1 = nodes[leader].counters().commit.load_acquire();
    let baseline_rate = (c1 - c0) as f64 / t0.elapsed().as_secs_f64();

    // The commit position at join start — the learner must reach this.
    let commit_at_join = nodes[leader].counters().commit.load_acquire();

    // Start the pre-provisioned learner (id = the highest).
    let lid = learners[0].0;
    let cfg = make_config(
        lid,
        members.to_vec(),
        learners.to_vec(),
        learners[0].1,
        learner_dir.to_path_buf(),
        APP.into(),
    );
    let learner = Node::start_with_socket(cfg, learner_sock).expect("learner start");
    let lsvc = spawn_service(learner_dir);

    // Measure commit rate during the join window while waiting for catch-up.
    let jc0 = nodes[leader].counters().commit.load_acquire();
    let jt0 = Instant::now();
    let joined = loop {
        if learner.counters().durable.load_acquire() >= commit_at_join {
            break true;
        }
        if jt0.elapsed() > JOIN_BUDGET {
            break false;
        }
        thread::yield_now();
    };
    let jc1 = nodes[leader].counters().commit.load_acquire();
    let join_rate = (jc1.saturating_sub(jc0)) as f64 / jt0.elapsed().as_secs_f64().max(1e-6);

    let dip = if baseline_rate > 0.0 {
        ((baseline_rate - join_rate) / baseline_rate * 100.0).max(0.0)
    } else {
        0.0
    };
    let learner_led = learner.is_leader();
    let cm = load.committed_max();
    let join_secs = jt0.elapsed().as_secs_f64();
    learner.stop();
    lsvc.stop();

    // Pass: joined in budget, learner never led, no quorum stall (commit advanced
    // during the join). The < 10 % dip is the FLEET gate; in this smoke the learner
    // catches up almost instantly (a register's snapshot install is trivial), so
    // the during-join window is far too short to measure a rate — the dip is
    // reported but only gated when the window is long enough to be meaningful.
    let dip_measurable = join_secs > 1.0;
    let dip_ok = !dip_measurable || dip < 10.0;
    let pass = joined && !learner_led && join_rate > 0.0 && dip_ok;
    let dip_note = if dip_measurable {
        format!("dip={dip:.1}% (fleet gate <10%)")
    } else {
        format!("dip={dip:.1}% (window {join_secs:.2}s too short to gate on this box — informational)")
    };
    Verdict {
        scenario: "learner-join",
        pass,
        detail: format!(
            "joined={joined} in {join_secs:.2}s (budget 60s), baseline={:.0} commits/s, \
             during-join={:.0} commits/s, {dip_note}, learner_led={learner_led}, \
             load_committed_max={cm}",
            baseline_rate, join_rate,
        ),
    }
}

/// Scenario 2: N cycles of snapshot → purge → crash a follower's service →
/// reconstruct. Every reconstruction must converge in ≤ 10 s, and linearizable
/// reads must stay monotonic + match the write stream (no divergence).
fn scenario_purge_cycle(
    nodes: &[Node],
    services: &mut [Option<uc_service::Service<RegSm>>],
    dirs: &Arc<Vec<PathBuf>>,
    load: &Load,
    cycles: u32,
) -> Verdict {
    const CONVERGE_BUDGET: Duration = Duration::from_secs(10);
    let mut last_read = 0u64;
    let mut purged = false;
    let mut max_converge = Duration::ZERO;

    for cycle in 0..cycles {
        let leader = await_single_leader(nodes, 20);
        // Wait for purge to advance the floor (snapshot built + purged) so the
        // follower reconstruction goes below-floor via snapshot install.
        let pfloor_deadline = Instant::now() + Duration::from_secs(15);
        while nodes.iter().map(|n| n.archive_first_base()).max().unwrap_or(0) == 0 {
            if Instant::now() > pfloor_deadline {
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
        if nodes.iter().map(|n| n.archive_first_base()).max().unwrap_or(0) > 0 {
            purged = true;
        }

        // Crash a follower's service (not the leader), THEN restart a fresh empty
        // one — crash-before-spawn so two services never map the same dir at once.
        // The fresh empty service reconstructs from the node (snapshot install +
        // tail-replay, since the log prefix it needs was purged).
        let follower = (0..nodes.len()).find(|&i| i != leader).expect("a follower");
        let dir = dirs[follower].clone();
        if let Some(old) = services[follower].take() {
            old.crash();
        }
        services[follower] = Some(spawn_service(&dir));

        // Wait for the follower's service to reconstruct (service_applied catches
        // the node's commit).
        let ct0 = Instant::now();
        let converged = loop {
            let commit = nodes[follower].counters().commit.load_acquire();
            if commit > 0 && nodes[follower].service_applied() >= commit {
                break true;
            }
            if ct0.elapsed() > CONVERGE_BUDGET {
                break false;
            }
            thread::yield_now();
        };
        max_converge = max_converge.max(ct0.elapsed());
        if !converged {
            return Verdict {
                scenario: "purge-cycle",
                pass: false,
                detail: format!(
                    "cycle {cycle}: follower n{follower} service did not reconstruct within 10s \
                     (commit={}, applied={})",
                    nodes[follower].counters().commit.load_acquire(),
                    nodes[follower].service_applied(),
                ),
            };
        }

        // No committed-value divergence: a linearizable read must be monotonic
        // non-decreasing and never exceed what the load has committed.
        let leader = await_single_leader(nodes, 20);
        let Some(v) = read_value(dirs, leader) else {
            return Verdict {
                scenario: "purge-cycle",
                pass: false,
                detail: format!("cycle {cycle}: linearizable read did not resolve within 10s"),
            };
        };
        let cm = load.committed_max();
        if v < last_read {
            return Verdict {
                scenario: "purge-cycle",
                pass: false,
                detail: format!("cycle {cycle}: linearizable read REGRESSED {last_read} -> {v}"),
            };
        }
        if v > cm.max(load.next_val.load(Ordering::Relaxed)) {
            return Verdict {
                scenario: "purge-cycle",
                pass: false,
                detail: format!("cycle {cycle}: read {v} exceeds anything written (max {cm})"),
            };
        }
        last_read = v;
    }

    Verdict {
        scenario: "purge-cycle",
        pass: true,
        detail: format!(
            "{cycles} cycles: every follower-service reconstruction converged (worst {:.1}s / 10s \
             budget), reads monotonic (last={last_read}), purge_fired={purged}",
            max_converge.as_secs_f64(),
        ),
    }
}
