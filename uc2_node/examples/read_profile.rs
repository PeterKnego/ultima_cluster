// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Linearizable-read profile harness. See
//! `docs/superpowers/specs/2026-07-25-uc2-read-profile-design.md`.

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};

use uc2_consensus::election::NodeId;
use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};
use uc2_service::{ServiceBuilder, ServiceConfig, StateMachine};

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

fn run_all(a: AllArgs) -> anyhow::Result<()> {
    let root = a.root.unwrap_or_else(|| PathBuf::from("target/read_profile_smoke"));
    let (nodes, services, dirs, leader) = boot_cluster(&root, ALL_APP_ID)?;
    println!("leader elected: n{leader} at {:?}", dirs[leader]);
    let _ = (a.secs, a.readers, a.mode, a.write_rate); // measurement lands in Task 3
    stop_cluster(nodes, services);
    Ok(())
}

fn run_client_role(_a: ClientArgs) -> anyhow::Result<()> {
    anyhow::bail!("client role lands in Task 3")
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
