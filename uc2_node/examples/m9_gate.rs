// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M9 gate: **restart cost of a deployable node** (spec
//! `docs/superpowers/specs/2026-08-19-uc2-production-readiness-design.md` §4).
//!
//! Unlike every earlier gate harness, this one does NOT define its own `node`
//! role. M9's whole thesis is that a real `uc2-node` binary exists, so the gate
//! drives that binary as a child process — a gate-specific node role would
//! measure something the operator never runs. The service, probe, and load
//! roles mirror `m7_gate.rs` so `bench-infra/scripts/m6_fleet_gate.py` can
//! drive this the same way.
//!
//! The four pre-committed bars live in
//! `docs/benchmarks/uc2-m9-gate-2026-08-19.md` and are restated in `BARS`
//! below. The binary prints the bar and exits 1 on FAIL.
//!
//! Snapshots and purge are ON in the smoke. Without them a snapshot install is
//! impossible and row 2 would be vacuously green.

use std::io::Read;
use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};

use uc2_client::{Client, ClientError};
use uc2_consensus::election::NodeId;
use uc2_log::cnc::CncPage;
use uc_protocol::v2::cnc::{NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER};
use uc2_service::{
    Service, ServiceBuilder, ServiceConfig, SnapshotError, SnapshotPolicy, SnapshotStateMachine,
    StateMachine,
};

const APP: &str = "m9-gate";
const BUFFER_BYTES: usize = 1 << 22;
const MAX_PAYLOAD: usize = 256;
const SEGMENT_BYTES: u64 = 16 * 1024;
const SNAPSHOT_INTERVAL_BYTES: u64 = 32 * 1024;
const PURGE_SLACK_BYTES: u64 = 0;

/// The pre-committed bars. Changing a number here without changing the gate
/// doc — and without a commit that says why — is the thing the honest-failure
/// protocol exists to prevent.
const BAR_STOP_SECS: f64 = 1.0;
const BAR_DIP_PCT: f64 = 10.0;

// ------------------------------------------------------------------ CLI

#[derive(Parser)]
#[command(name = "m9_gate", about = "UC v2 M9 gate: restart cost of a deployable node")]
struct Cli {
    #[command(subcommand)]
    role: Role,
}

#[derive(Subcommand)]
enum Role {
    /// Local smoke: 3 real uc2-node processes + services + load, leader
    /// SIGTERM + restart, and the refusal battery. NOT the gate.
    All(AllArgs),
    /// State-machine service, attached to a running node.
    Service(ServiceArgs),
    /// One JSON line of a node's counters, including incoming_snapshot_pos.
    Probe(ProbeArgs),
    /// Sustained write load against the local node.
    #[command(name = "loadclient")]
    LoadClient(LoadClientArgs),
}

#[derive(clap::Args)]
struct AllArgs {
    /// Real-filesystem root for the instance dirs (never /tmp — RAM tmpfs).
    #[arg(long)]
    root: Option<PathBuf>,
    /// Seconds of load in each measurement window.
    #[arg(long, default_value_t = 6)]
    secs: u64,
    /// Path to the uc2-node binary. Defaults to a sibling of this example.
    #[arg(long)]
    uc2_node_bin: Option<PathBuf>,
}

#[derive(clap::Args)]
struct ServiceArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = APP)]
    app_id: String,
    #[arg(long, default_value_t = SNAPSHOT_INTERVAL_BYTES)]
    snapshot_interval_bytes: u64,
}

#[derive(clap::Args)]
struct ProbeArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = APP)]
    app_id: String,
    /// Seconds to sample the commit rate over, on-host (zero ssh skew).
    #[arg(long, default_value_t = 0.0)]
    rate_secs: f64,
}

#[derive(clap::Args)]
struct LoadClientArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = APP)]
    app_id: String,
    #[arg(long, default_value_t = 30)]
    secs: u64,
}

fn main() {
    let cli = Cli::parse();
    let r = match cli.role {
        Role::All(a) => run_all(a),
        Role::Service(a) => run_service_role(a),
        Role::Probe(a) => run_probe(a),
        Role::LoadClient(a) => run_loadclient(a),
    };
    if let Err(e) = r {
        eprintln!("m9_gate error: {e:#}");
        std::process::exit(1);
    }
}

// ------------------------------------------------------------- the SM

/// A register: `apply` stores the 8-byte LE payload; `query` returns it.
/// Snapshot-capable so purge can actually fire — see the module doc on why
/// row 2 needs that to be non-vacuous. Identical to `m7_gate.rs`'s `RegSm`.
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

// --------------------------------------------------------- fleet roles

fn spawn_service(dir: &Path, app_id: &str, snapshot_interval_bytes: u64) -> Service<RegSm> {
    let cfg = ServiceConfig::new(dir, app_id)
        .snapshot_policy(SnapshotPolicy { interval_bytes: snapshot_interval_bytes });
    ServiceBuilder::new(cfg, RegSm::default())
        .start_with_snapshots()
        .expect("snapshot service start")
}

fn run_service_role(a: ServiceArgs) -> anyhow::Result<()> {
    let svc = spawn_service(&a.instance_dir, &a.app_id, a.snapshot_interval_bytes);
    println!("m9_gate service attached at {}", a.instance_dir.display());
    let stop = Arc::new(AtomicBool::new(false));
    for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        signal_hook::flag::register(sig, Arc::clone(&stop))?;
    }
    while !stop.load(Ordering::Relaxed) {
        anyhow::ensure!(svc.is_alive(), "apply agent fail-stopped");
        thread::sleep(Duration::from_millis(100));
    }
    svc.stop();
    Ok(())
}

fn run_probe(a: ProbeArgs) -> anyhow::Result<()> {
    let cnc = CncPage::open_file(&a.instance_dir.join("cnc2.dat"), &a.app_id)
        .map_err(|e| anyhow::anyhow!("open cnc: {e:?}"))?;
    let rate = (a.rate_secs > 0.0).then(|| {
        let c0 = cnc.counters().commit.load_acquire();
        thread::sleep(Duration::from_secs_f64(a.rate_secs));
        let c1 = cnc.counters().commit.load_acquire();
        c1.saturating_sub(c0) as f64 / a.rate_secs
    });
    let flags = cnc.status().flags.load_acquire();
    let c = cnc.counters();
    let mut line = format!(
        "{{\"leader\":{},\"can_serve\":{},\"term\":{},\"commit\":{},\"durable\":{},\
         \"append\":{},\"incoming_snapshot_pos\":{},\"node_snapshot_floor\":{},\
         \"service_snapshot_pos\":{}",
        (flags & NODE_FLAG_LEADER) != 0,
        (flags & NODE_FLAG_CAN_SERVE) != 0,
        cnc.status().term.load_acquire(),
        c.commit.load_acquire(),
        c.durable.load_acquire(),
        c.append.load_acquire(),
        cnc.snapshots().incoming_snapshot_pos.load_acquire(),
        cnc.snapshots().node_snapshot_floor.load_acquire(),
        // Row 2's anti-vacuity evidence (Ruling 13): "incoming_snapshot_pos
        // unchanged at 0" is ALSO what a cluster that never snapshots looks
        // like. The in-process `all` mode reads this directly; the fleet
        // orchestrator can only see what `probe` prints, so without this the
        // fleet could only report row 2 INCONCLUSIVE.
        cnc.snapshots().service_snapshot_pos.load_acquire(),
    );
    if let Some(r) = rate {
        line.push_str(&format!(",\"rate\":{r:.3}"));
    }
    line.push('}');
    println!("{line}");
    Ok(())
}

fn run_loadclient(a: LoadClientArgs) -> anyhow::Result<()> {
    let registry: Registry = Arc::new(Mutex::new(vec![(0, a.instance_dir.clone())]));
    let load = Load::start(Arc::clone(&registry), a.app_id.clone());
    thread::sleep(Duration::from_secs(a.secs));
    load.stop();
    Ok(())
}

// --------------------------------------------------------------- load

type Registry = Arc<Mutex<Vec<(NodeId, PathBuf)>>>;

struct Load {
    stop: Arc<AtomicBool>,
    submitted: Arc<Mutex<u64>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Load {
    fn start(registry: Registry, app_id: String) -> Load {
        let stop = Arc::new(AtomicBool::new(false));
        let submitted = Arc::new(Mutex::new(0u64));
        let (s, n) = (Arc::clone(&stop), Arc::clone(&submitted));
        let handle = thread::spawn(move || load_loop(s, registry, app_id, n));
        Load { stop, submitted, handle: Some(handle) }
    }
    fn stop(mut self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
        *self.submitted.lock().unwrap()
    }
}

fn load_loop(stop: Arc<AtomicBool>, registry: Registry, app_id: String, submitted: Arc<Mutex<u64>>) {
    let mut next_val = 1u64;
    let mut cur = 0usize;
    let mut client: Option<Client> = None;
    while !stop.load(Ordering::Relaxed) {
        if client.is_none() {
            let snapshot = registry.lock().unwrap().clone();
            if snapshot.is_empty() {
                thread::sleep(Duration::from_millis(20));
                continue;
            }
            cur %= snapshot.len();
            match Client::connect(&snapshot[cur].1, &app_id) {
                Ok(c) => client = Some(c),
                Err(_) => {
                    cur = (cur + 1) % snapshot.len();
                    thread::sleep(Duration::from_millis(20));
                    continue;
                }
            }
        }
        let payload = next_val.to_le_bytes().to_vec();
        match client.as_ref().unwrap().submit::<Vec<u8>, u64>(&payload) {
            Ok(_) => {
                next_val += 1;
                *submitted.lock().unwrap() = next_val - 1;
            }
            Err(ClientError::NotLeader { hint }) => {
                let c = client.take().unwrap();
                c.shutdown();
                let snapshot = registry.lock().unwrap().clone();
                if !snapshot.is_empty() {
                    cur = hint
                        .and_then(|h| snapshot.iter().position(|(id, _)| *id == h))
                        .unwrap_or((cur + 1) % snapshot.len());
                }
            }
            Err(ClientError::BackpressureFull) | Err(ClientError::Retry) => {
                thread::sleep(Duration::from_millis(2));
            }
            Err(_) => {
                // A dead target must advance the cursor, or the driver can get
                // stuck hammering one node forever (an m7_gate lesson).
                if let Some(c) = client.take() {
                    c.shutdown();
                }
                let len = registry.lock().unwrap().len();
                if len > 0 {
                    cur = (cur + 1) % len;
                }
                thread::sleep(Duration::from_millis(10));
            }
        }
    }
    if let Some(c) = client {
        c.shutdown();
    }
}

// ------------------------------------------------------------ verdicts

struct Verdict {
    row: &'static str,
    pass: bool,
    detail: String,
}

impl Verdict {
    fn new(row: &'static str, pass: bool, detail: String) -> Verdict {
        Verdict { row, pass, detail }
    }
}

// -------------------------------------------------------------- smoke

/// One node under the smoke: its config file, instance dir, and live child.
struct NodeProc {
    id: NodeId,
    dir: PathBuf,
    cfg_path: PathBuf,
    child: Option<Child>,
    svc: Option<Service<RegSm>>,
}

impl NodeProc {
    fn cnc(&self) -> Option<std::sync::Arc<CncPage>> {
        CncPage::open_file(&self.dir.join("cnc2.dat"), APP).ok()
    }
    fn commit(&self) -> u64 {
        self.cnc().map(|c| c.counters().commit.load_acquire()).unwrap_or(0)
    }
    fn is_leader(&self) -> bool {
        self.cnc()
            .map(|c| (c.status().flags.load_acquire() & NODE_FLAG_LEADER) != 0)
            .unwrap_or(false)
    }
    fn can_serve(&self) -> bool {
        self.cnc()
            .map(|c| (c.status().flags.load_acquire() & NODE_FLAG_CAN_SERVE) != 0)
            .unwrap_or(false)
    }
    fn service_snapshot_pos(&self) -> u64 {
        self.cnc().map(|c| c.snapshots().service_snapshot_pos.load_acquire()).unwrap_or(0)
    }
    fn instance_id(&self) -> Option<u128> {
        self.cnc().and_then(|c| c.try_instance_id())
    }
    fn incoming_snapshot_pos(&self) -> u64 {
        self.cnc().map(|c| c.snapshots().incoming_snapshot_pos.load_acquire()).unwrap_or(0)
    }
}

/// Kill any surviving child on unwind. A leaked `uc2-node` is a BUSY-SPIN
/// process, so a panicking smoke must never leave one behind.
impl Drop for NodeProc {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn write_config(path: &Path, id: NodeId, addr: SocketAddr, dir: &Path, members: &[(NodeId, SocketAddr)]) {
    let mut s = format!(
        "id = {id}\nbind = \"{addr}\"\ninstance_dir = \"{}\"\napp_id = \"{APP}\"\n\
         buffer_bytes = {BUFFER_BYTES}\nmax_payload = {MAX_PAYLOAD}\n\
         journal_segment_bytes = {SEGMENT_BYTES}\n\n\
         [purge]\nbelow_snapshot_slack_bytes = {PURGE_SLACK_BYTES}\n\n",
        dir.display()
    );
    for (mid, maddr) in members {
        s.push_str(&format!("[[members]]\nid = {mid}\naddr = \"{maddr}\"\n\n"));
    }
    std::fs::write(path, s).expect("write config");
}

fn spawn_node(bin: &Path, cfg_path: &Path) -> Child {
    Command::new(bin)
        .arg("--config")
        .arg(cfg_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn uc2-node")
}

fn default_uc2_node_bin() -> PathBuf {
    // target/<profile>/examples/m9_gate -> target/<profile>/uc2-node
    let me = std::env::current_exe().expect("current_exe");
    me.parent().and_then(|p| p.parent()).map(|p| p.join("uc2-node")).unwrap_or_default()
}

fn await_leader(nodes: &[NodeProc], secs: u64) -> Option<usize> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        let leaders: Vec<usize> =
            (0..nodes.len()).filter(|&i| nodes[i].is_leader() && nodes[i].can_serve()).collect();
        if leaders.len() == 1 {
            return Some(leaders[0]);
        }
        thread::sleep(Duration::from_millis(50));
    }
    None
}

/// Cluster progress = the furthest commit any live node has reached.
fn max_commit(nodes: &[NodeProc]) -> u64 {
    nodes.iter().map(|n| n.commit()).max().unwrap_or(0)
}

fn rate_over(nodes: &[NodeProc], secs: f64) -> f64 {
    let c0 = max_commit(nodes);
    thread::sleep(Duration::from_secs_f64(secs));
    let c1 = max_commit(nodes);
    c1.saturating_sub(c0) as f64 / secs
}

fn run_all(a: AllArgs) -> anyhow::Result<()> {
    // SAFETY: single-threaded, before any thread is spawned.
    unsafe {
        std::env::set_var("UC2_CLIENT_TIMEOUT_MS", "500");
    }

    let root = a.root.unwrap_or_else(|| PathBuf::from("target/m9_gate_smoke"));
    anyhow::ensure!(
        !root.starts_with("/tmp"),
        "m9_gate all: root must be on a real filesystem (never /tmp — RAM tmpfs); got {root:?}"
    );
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root)?;

    let bin = a.uc2_node_bin.unwrap_or_else(default_uc2_node_bin);
    anyhow::ensure!(
        bin.is_file(),
        "uc2-node binary not found at {bin:?} — build it first \
         (cargo build --release -p uc2_node --bin uc2-node) or pass --uc2-node-bin"
    );

    println!("== uc2 M9 gate: SMOKE (local; 3 real uc2-node processes + services + load) ==");
    println!(
        "*** THIS IS NOT THE M9 GATE *** — one dev box. The bars in \
         docs/benchmarks/uc2-m9-gate-2026-08-19.md are only claimable on a fleet run."
    );
    println!("uc2-node binary: {}", bin.display());

    const NV: usize = 3;
    // Pick free ports by binding and releasing. A small race, but the
    // alternative (handing the daemon a pre-bound socket) is exactly the
    // thing this gate refuses to do — it must exercise the real binary.
    let addrs: Vec<SocketAddr> = (0..NV)
        .map(|_| {
            let s = UdpSocket::bind("127.0.0.1:0").expect("bind");
            s.local_addr().unwrap()
        })
        .collect();
    let members: Vec<(NodeId, SocketAddr)> = (0..NV).map(|i| (i as NodeId, addrs[i])).collect();

    let mut nodes: Vec<NodeProc> = Vec::with_capacity(NV);
    for (i, addr) in addrs.iter().enumerate() {
        let dir = root.join(format!("n{i}"));
        std::fs::create_dir_all(&dir)?;
        let cfg_path = root.join(format!("n{i}.toml"));
        write_config(&cfg_path, i as NodeId, *addr, &dir, &members);
        let child = spawn_node(&bin, &cfg_path);
        nodes.push(NodeProc {
            id: i as NodeId,
            dir,
            cfg_path,
            child: Some(child),
            svc: None,
        });
    }

    // Attach a service to each node once its control page exists.
    for n in nodes.iter_mut() {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !n.dir.join("cnc2.dat").exists() {
            anyhow::ensure!(Instant::now() < deadline, "node {} never created its cnc page", n.id);
            thread::sleep(Duration::from_millis(20));
        }
        n.svc = Some(spawn_service(&n.dir, APP, SNAPSHOT_INTERVAL_BYTES));
    }

    let leader = await_leader(&nodes, 30).ok_or_else(|| anyhow::anyhow!("no single leader"))?;
    println!("leader elected: n{}", nodes[leader].id);

    let registry: Registry =
        Arc::new(Mutex::new(nodes.iter().map(|n| (n.id, n.dir.clone())).collect()));
    let load = Load::start(Arc::clone(&registry), APP.into());

    // ---- baseline rate, with the leader serving under load
    let baseline = rate_over(&nodes, a.secs as f64);
    println!("baseline commit rate: {baseline:.0} B/s");

    let snap_before = nodes[leader].incoming_snapshot_pos();
    // The cnc file SURVIVES the restart, so "the file exists" is not evidence
    // the new node is up. Remember the instance id and wait for it to change —
    // otherwise the replacement service attaches to the stale page, sees the id
    // move under it, and fail-stops exactly as designed. (Caught by the first
    // smoke run of this harness.)
    let instance_before = nodes[leader].instance_id();

    // ---- row 1: SIGTERM the leader process, time its exit
    let pid = nodes[leader].child.as_ref().unwrap().id();
    println!("SIGTERM -> n{} (pid {pid})", nodes[leader].id);
    // The service must stop first: it is the reader, and a node restart
    // changes the instance id under it (a deliberate fail-stop).
    if let Some(s) = nodes[leader].svc.take() {
        s.stop();
    }
    let t0 = Instant::now();
    unsafe { libc::kill(pid as i32, libc::SIGTERM) };
    let status = nodes[leader].child.take().unwrap().wait().expect("wait leader");
    let stop_elapsed = t0.elapsed();
    let row1_pass = stop_elapsed.as_secs_f64() < BAR_STOP_SECS && status.success();
    let row1 = Verdict::new(
        "1 SIGTERM->exit",
        row1_pass,
        format!(
            "{:.3}s, exit {:?} (bar: < {BAR_STOP_SECS}s and exit 0)",
            stop_elapsed.as_secs_f64(),
            status.code()
        ),
    );

    // ---- restart it, and measure the window across stop+restart
    let child = spawn_node(&bin, &nodes[leader].cfg_path);
    nodes[leader].child = Some(child);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let now = nodes[leader].instance_id();
        if now.is_some() && now != instance_before {
            break;
        }
        anyhow::ensure!(Instant::now() < deadline, "restarted node never stamped a new instance id");
        thread::sleep(Duration::from_millis(20));
    }
    nodes[leader].svc = Some(spawn_service(&nodes[leader].dir, APP, SNAPSHOT_INTERVAL_BYTES));

    let window = rate_over(&nodes, a.secs as f64);
    let dip_pct =
        if baseline > 0.0 { ((baseline - window) / baseline * 100.0).max(0.0) } else { 100.0 };
    println!("window commit rate: {window:.0} B/s");

    // ---- row 2: no snapshot install across the cycle
    //
    // Anti-vacuity: "unchanged at 0" is also what you see in a cluster where
    // snapshots never fire at all, and that cluster could not have installed
    // one no matter how badly the restart went. So the row only counts as
    // evidence if some node actually built a snapshot; otherwise it reports
    // INCONCLUSIVE and fails, rather than passing for the wrong reason.
    let snap_after = nodes[leader].incoming_snapshot_pos();
    let built: Vec<u64> = nodes.iter().map(|n| n.service_snapshot_pos()).collect();
    let any_built = built.iter().any(|&p| p > 0);
    let row2 = if !any_built {
        Verdict::new(
            "2 no snapshot install",
            false,
            format!(
                "INCONCLUSIVE — no node built a snapshot (service_snapshot_pos {built:?}), so an \
                 install was impossible and 'unchanged' proves nothing. Raise the load or lower \
                 SNAPSHOT_INTERVAL_BYTES."
            ),
        )
    } else {
        Verdict::new(
            "2 no snapshot install",
            snap_after == snap_before,
            format!(
                "incoming_snapshot_pos {snap_before} -> {snap_after} (bar: unchanged; snapshots \
                 WERE being built: service_snapshot_pos {built:?})"
            ),
        )
    };

    // ---- row 3: dip — MEASURED AND PRINTED, NOT GATED LOCALLY.
    //
    // The < 10 % dip is a FLEET bar, exactly as in m6_gate and m7_gate (whose
    // AllArgs says the in-process gate "does not hard-gate the fleet dip bar").
    // This harness originally did hard-gate it and produced 2.1 / 10.1 / 14.2 /
    // 17.5 % across four identical runs: a run-to-run spread of 15 points
    // against a 10-point bar. A measurement whose noise exceeds the bar cannot
    // adjudicate it in EITHER direction, so gating on it here would be a coin
    // flip dressed as a verdict. One dev box runs 12 busy-spin agent threads
    // plus three apply agents plus the load driver; the restarted node's four
    // fresh spinning threads land on top of that.
    //
    // The bar in docs/benchmarks/uc2-m9-gate-2026-08-19.md is UNCHANGED at
    // 10 % and is adjudicated on the fleet.
    println!(
        "INFO commit-rate dip {dip_pct:.1}% (baseline {baseline:.0} -> {window:.0} B/s) — \
         NOT gated locally; the < {BAR_DIP_PCT}% bar is fleet-only (see m6_gate/m7_gate)."
    );

    let submitted = load.stop();
    println!("load submitted {submitted} values");

    // ---- row 4: refusals name their field
    let row4 = check_refusals(&bin, &root, &members);

    for mut n in nodes {
        if let Some(s) = n.svc.take() {
            s.stop();
        }
        if let Some(mut c) = n.child.take() {
            unsafe { libc::kill(c.id() as i32, libc::SIGTERM) };
            let _ = c.wait();
        }
    }

    let verdicts = [row1, row2, row4];
    println!("\n== M9 gate smoke results ==");
    let mut all_pass = true;
    for v in &verdicts {
        println!("  [{}] {} — {}", if v.pass { "PASS" } else { "FAIL" }, v.row, v.detail);
        all_pass &= v.pass;
    }
    if all_pass {
        println!(
            "RESULT: PASS (smoke) — rows 1, 2 and 4 held locally. Row 3 (commit-rate dip) was \
             measured and printed above but is fleet-gated, so this is NOT an M9 PASS."
        );
        Ok(())
    } else {
        println!("RESULT: FAIL (honest) — a locally-gated row (1, 2 or 4) did not hold.");
        std::process::exit(1);
    }
}

/// Row 4: drive the real binary with a config violating exactly one rule and
/// require the refusal to name the offending field. A refusal that does not
/// name the field is the failure mode M9 exists to remove, so it is gated.
fn check_refusals(bin: &Path, root: &Path, members: &[(NodeId, SocketAddr)]) -> Verdict {
    let dir = root.join("refusals");
    let _ = std::fs::create_dir_all(&dir);
    let good_dir = dir.join("inst");
    let _ = std::fs::create_dir_all(&good_dir);

    let m0 = members[0];
    let base = format!(
        "id = 0\nbind = \"{}\"\ninstance_dir = \"{}\"\napp_id = \"{APP}\"\n",
        m0.1,
        good_dir.display()
    );
    let member_block: String = members
        .iter()
        .map(|(id, addr)| format!("\n[[members]]\nid = {id}\naddr = \"{addr}\"\n"))
        .collect();

    // (name, config body, substring the refusal must contain)
    let cases: Vec<(&str, String, &str)> = vec![
        (
            "unknown-key",
            format!("{base}buffer_bytez = 4096\n{member_block}"),
            "buffer_bytez",
        ),
        (
            "bind-mismatch",
            format!(
                "id = 0\nbind = \"0.0.0.0:{}\"\ninstance_dir = \"{}\"\napp_id = \"{APP}\"\n{member_block}",
                m0.1.port(),
                good_dir.display()
            ),
            "bind",
        ),
        (
            "buffer-not-pow2",
            format!("{base}buffer_bytes = 4097\n{member_block}"),
            "buffer_bytes",
        ),
        (
            "payload-over-mtu",
            format!("{base}max_payload = 65536\n{member_block}"),
            "max_payload",
        ),
        (
            "self-not-a-member",
            format!(
                "id = 99\nbind = \"{}\"\ninstance_dir = \"{}\"\napp_id = \"{APP}\"\n{member_block}",
                m0.1,
                good_dir.display()
            ),
            "99",
        ),
        (
            "election-window",
            format!("{base}election_timeout_min_ns = 400000000\n{member_block}"),
            "election_timeout",
        ),
    ];

    let mut failures = Vec::new();
    for (name, body, want) in &cases {
        let p = dir.join(format!("{name}.toml"));
        if std::fs::write(&p, body).is_err() {
            failures.push(format!("{name}: could not write config"));
            continue;
        }
        let out = Command::new(bin).arg("--config").arg(&p).output();
        match out {
            Ok(o) => {
                let err = String::from_utf8_lossy(&o.stderr);
                if o.status.success() {
                    failures.push(format!("{name}: STARTED (must refuse)"));
                } else if !err.contains(want) {
                    failures.push(format!("{name}: refusal did not name {want:?}: {err}"));
                }
            }
            Err(e) => failures.push(format!("{name}: spawn failed: {e}")),
        }
    }

    let n = cases.len();
    if failures.is_empty() {
        Verdict::new("4 refusals name the field", true, format!("{n}/{n} rules refused by name"))
    } else {
        Verdict::new(
            "4 refusals name the field",
            false,
            format!("{}/{n} failed: {}", failures.len(), failures.join("; ")),
        )
    }
}

/// Unused today, but the fleet orchestrator reads gate stdout; keeping the
/// helper here means a fleet role can stream a child's output without
/// reinventing it.
#[allow(dead_code)]
fn drain_child_output(child: &mut Child) -> String {
    let mut s = String::new();
    if let Some(mut o) = child.stderr.take() {
        let _ = o.read_to_string(&mut s);
    }
    s
}
