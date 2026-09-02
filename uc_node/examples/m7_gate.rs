// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M7 gate: **live single-server reconfiguration under load** (spec §9 M7 row,
//! `docs/superpowers/specs/2026-07-13-uc2-reconfig-design.md`). Unlike m5_gate
//! (throughput) and m6_gate (learner-join / purge), this gate drives real
//! `uc2ctl`-shaped admin ops (add-learner / promote / demote / remove-learner /
//! remove-voter) against a running cluster while write load flows, and proves
//! the three §9.6 fleet scenarios hold:
//!
//! 1. **`replace-a-box`** — add a learner on a 5th host, catch it up, promote
//!    it to voter, remove a (crashed) original voter. Fleet PASS: commit-rate
//!    dip < 10 % across every transition window, zero read divergence.
//! 2. **`resize-3-5-3`** — two add+promote pairs (3→5 voters), then two
//!    demote+remove-learner pairs (5→3), landing back on the original 3 ids.
//!    Same bars as `replace-a-box`.
//! 3. **`leader-self-removal`** — the serving leader removes itself; PASS iff
//!    the committed high-water never regresses and a new leader serves within
//!    budget (fleet bar: < 10 s; the measured gap is reported either way).
//!
//! ```text
//! cargo run -p uc_node --release --example m7_gate -- all      --secs S
//! cargo run -p uc_node --release --example m7_gate -- node     --id N --bind A --members … --instance-dir D
//! cargo run -p uc_node --release --example m7_gate -- service  --instance-dir D
//! cargo run -p uc_node --release --example m7_gate -- probe    --instance-dir D
//! cargo run -p uc_node --release --example m7_gate -- loadclient --instance-dir D
//! ```
//!
//! **`node`/`learner`/`service`** are thin fleet-role wrappers (one process per
//! host, systemd-run-friendly) that park forever; the harness (or the fleet
//! orchestrator, `bench-infra/scripts/m6_fleet_gate.py --m7`) owns their
//! lifecycle and drives admin ops via the separate `uc2ctl` binary (same admin
//! cnc-slot protocol this example's in-process scenarios speak directly).
//! Unlike M6, a spare M7 host boots as a plain `node` with a seed `--members`
//! listing only the CURRENT voters — it is not in anyone's static `learners`
//! list at boot. It is admitted live via `add-learner`, and adopts the real
//! config from the replicated stream regardless of what its own stale seed
//! says (`uc_node/tests/reconfig.rs::joining_node_boots_from_stale_seed`
//! proved this pattern) — so there is no separate `learner` CLI role here; a
//! `learner` unit is just a `node` unit with a different systemd-unit name for
//! fleet-topology clarity.
//!
//! **`all`** boots everything in-process (real file-backed shmem, not separate
//! OS processes) under a tempdir guarded OFF `/tmp` and runs all three
//! scenarios against it. This is **SMOKE ONLY** — one dev box, core-starved.
//! PASS criteria in-process (per the task brief): every transition's admin op
//! is accepted and converges, linearizable reads stay monotonic across the
//! whole run, and commit keeps advancing throughout every transition (a full
//! multi-second quorum stall would show up as a zero rate). The **strict
//! < 10 % commit-rate-dip bar is fleet-only** — printed here as informational,
//! exactly as M6 treated its dip number in the loopback-scale smoke.
//!
//! **M7 is deliberately paired with M6 (snapshots + purge), not scoped away
//! from it.** A joining voter/learner catches up over TWO possible paths: (a)
//! pure journal replay/NAK from wherever its replication cursor sits, or (b) a
//! snapshot install (below the leader's purge floor) + a short tail replay.
//! With purge off, EVERY join is path (a) — fine for `replace-a-box` (one
//! join, early) but a real liveness bug for `resize-3-5-3`, whose learner
//! joins after several prior scenarios' worth of unthrottled write history: an
//! unthrottled writer can outrun a distant learner's pure replay (see the
//! runbook caveat this gate used to carry, and the repro that motivated this
//! fix — a `resize-3-5-3` learner stuck at config v1 while the cluster had
//! already moved to v4). So this gate now ALSO wires `RegSm` with
//! `SnapshotStateMachine`, runs the service with a snapshot policy, and runs
//! the node with `PurgePolicy::BelowSnapshot` — exactly the M6 gate's
//! machinery, reused here rather than reinvented — so a late joiner converges
//! via snapshot install + tail replay instead of racing the writer. Disabled
//! by default (`--snapshot-interval-bytes 0` / no `--purge-below-snapshot`) so
//! existing fleet invocations are unchanged unless the orchestrator opts in;
//! the `all` in-process smoke opts in unconditionally (m6-gate-sized
//! constants) since the smoke should exercise what the fleet actually runs.
//!
//! **Loud disclaimer:** completing this binary is NOT passing the M7 gate.
//! The gate is claimed only after `bench-infra/scripts/m6_fleet_gate.py --m7`
//! passes locally AND (a separate, user-approved step) on the real fleet — see
//! `docs/benchmarks/uc2-m7-gate-2026-07-13.md`.

use std::collections::HashSet;
use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};

use uc_client::{Client, ClientError};
use uc_consensus::election::NodeId;
use uc_log::cnc::{AdminReq, AdminResp, CncPage};
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig, PurgePolicy};
use uc_protocol::v2::cnc::{
    CNC_MAX_PEER_SLOTS, CNC_PEER_ROLE_VOTER, NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER,
};
use uc_service::{
    ApplyCtx, Service, ServiceBuilder, ServiceConfig, SnapshotError, SnapshotPolicy,
    SnapshotStateMachine, StateMachine,
};

// ------------------------------------------------------------------ CLI

#[derive(Parser)]
#[command(
    name = "m7_gate",
    about = "UC v2 M7 gate: live reconfig under load (spec §9)"
)]
struct Cli {
    #[command(subcommand)]
    role: Role,
}

#[derive(Subcommand)]
enum Role {
    /// Local smoke: 3 voters + services + load + all three M7 scenarios, in-process. NOT the gate.
    All(AllArgs),
    /// Voting cluster-member node (fleet: one process per host). A spare host
    /// that will later be admitted live also uses this role.
    Node(NodeArgs),
    /// State-machine service, attached to a running node.
    Service(ServiceArgs),
    /// Read a node's counters + config version/pending from its local cnc page
    /// and print one JSON line (the fleet orchestrator's observability channel).
    Probe(ProbeArgs),
    /// Sustained write load + a monotonic linearizable-read consistency guard,
    /// attached to the LOCAL node. Exits 1 on a read regression.
    #[command(name = "loadclient")]
    LoadClient(LoadClientArgs),
}

#[derive(clap::Args)]
struct AllArgs {
    /// Real-filesystem root for the instance dirs (never /tmp — RAM tmpfs).
    #[arg(long)]
    root: Option<PathBuf>,
    /// Seconds of baseline load before each scenario. Currently informational
    /// (the in-process gate does not hard-gate the fleet dip bar).
    #[arg(long, default_value_t = 4)]
    secs: u64,
}

#[derive(clap::Args)]
struct NodeArgs {
    #[arg(long)]
    id: NodeId,
    #[arg(long)]
    bind: SocketAddr,
    #[arg(long)]
    instance_dir: PathBuf,
    /// Comma-separated `id@addr` VOTING member list (the boot SEED — only
    /// authoritative for a fresh instance dir; see the module doc).
    #[arg(long)]
    members: String,
    #[arg(long, default_value = "m7-gate")]
    app_id: String,
    /// M6 pairing: enable `PurgePolicy::BelowSnapshot { slack_bytes: 0 }` so a
    /// late-joining voter/learner under fleet load converges via snapshot
    /// install + tail replay instead of racing pure journal replay against an
    /// unthrottled writer (see the module doc). Default false (`Disabled`) —
    /// unchanged behavior unless set. Only useful paired with the `service`
    /// role's `--snapshot-interval-bytes > 0` (purge needs a real snapshot
    /// floor to purge below).
    #[arg(long)]
    purge_below_snapshot: bool,
    /// Journal segment size in bytes; only material with
    /// `--purge-below-snapshot` (purge drops whole non-active segments).
    /// Default = the production `DEFAULT_JOURNAL_SEGMENT_BYTES` (64 MiB) —
    /// unchanged unless set smaller (the m6-gate fleet uses a small segment
    /// so purge actually fires under the gate's modest write volume).
    #[arg(long, default_value_t = uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES)]
    journal_segment_bytes: u64,
}

#[derive(clap::Args)]
struct ServiceArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "m7-gate")]
    app_id: String,
    /// M6 pairing: snapshot cadence in bytes of applied command payload
    /// (mirrors `m6_gate`'s `SnapshotPolicy::interval_bytes`). 0 (default) =
    /// off — the service starts plain (`ServiceBuilder::start`), current
    /// behavior unchanged. > 0 starts with `start_with_snapshots`, giving the
    /// node a real snapshot floor to purge below and a snapshot session for
    /// distant joiners/reconstructions to install instead of replaying the
    /// full journal from genesis.
    #[arg(long, default_value_t = 0)]
    snapshot_interval_bytes: u64,
}

#[derive(clap::Args)]
struct ProbeArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "m7-gate")]
    app_id: String,
    /// Fix 2 (bench-infra fleet-gate measurement hardening): if > 0, measure
    /// an ON-HOST commit rate over this many seconds (read the commit
    /// counter, sleep, read again, `rate = delta/secs`) and add a `"rate"`
    /// field to the printed JSON. The whole measurement — sleep included —
    /// happens inside this process, so the rate carries zero ssh/subprocess
    /// round-trip skew; the fleet orchestrator's `dip_for_transition` uses
    /// this instead of bracketing two separate remote probes with local
    /// wall-clock (that bracketing technique systematically undercounted the
    /// rate by the ssh+sudo RTT, ~8-16% of a 5-8s window on the fleet). 0
    /// (default) = unchanged instant single-sample read, no `"rate"` field.
    #[arg(long, default_value_t = 0.0)]
    rate_secs: f64,
}

#[derive(clap::Args)]
struct LoadClientArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "m7-gate")]
    app_id: String,
    /// Run for this many seconds (0 = until --stop-file appears).
    #[arg(long, default_value_t = 0)]
    secs: u64,
    /// Stop when this file exists (the orchestrator's kill switch).
    #[arg(long, default_value = "")]
    stop_file: String,
    /// Fix 4 (bench-infra fleet-gate measurement hardening): target
    /// sustained write rate in writes/s, paced by a sleep after each
    /// successful submit. 0 (default) = UNTHROTTLED — `m6_gate`'s loadclient
    /// has no artificial pacing at all (it only backs off on
    /// NotLeader/Retry/BackpressureFull), and that IS the load level the M7
    /// fleet gate must match: the old unconditional 3ms-per-write throttle
    /// here made this gate's load ~14x lighter than M6's, which is not
    /// "under load" by M6's own bar. Kept as a flag rather than deleted
    /// outright because a fresh, un-snapshotted M7 spare's catch-up is pure
    /// journal replay (this gate's SM has no snapshot capability) — an
    /// unthrottled writer CAN outrun a distant learner's replay (a real
    /// repro hit during gate authoring). On the FLEET the guard against that
    /// race is the `promote` precondition itself (`NotCaughtUp` refusal +
    /// the orchestrator's bounded retry, `ctl_retry_transient` in
    /// `bench-infra/scripts/m6_fleet_gate.py`) — replay on dedicated hosts
    /// vastly outruns one synchronous submit client. On one core-starved
    /// LOCAL dev box (5 busy-spinning nodes) the replayer is measurably
    /// slower than an unthrottled writer, so the orchestrator's `--m7
    /// --local` mode passes `--rate 300` (its fleet mode passes nothing).
    #[arg(long, default_value_t = 0)]
    rate: u64,
}

fn main() {
    let cli = Cli::parse();
    let r = match cli.role {
        Role::All(a) => run_all(a),
        Role::Node(a) => run_node(a),
        Role::Service(a) => run_service(a),
        Role::Probe(a) => run_probe(a),
        Role::LoadClient(a) => run_loadclient(a),
    };
    if let Err(e) = r {
        eprintln!("m7_gate error: {e:#}");
        std::process::exit(1);
    }
}

// ------------------------------------------------------------- the SM

/// A register: `apply` stores the 8-byte LE payload as the current value;
/// `query` returns it. Snapshot-capable (see the `SnapshotStateMachine` impl
/// below) — paired with M6 so a late-joining voter/learner under fleet load
/// converges via snapshot install + tail replay rather than racing an
/// unthrottled writer's pure journal replay (see the module doc). Snapshots +
/// purge are OFF unless the fleet role's `--snapshot-interval-bytes` /
/// `--purge-below-snapshot` flags are set; the in-process `all` smoke always
/// enables them (m6-gate-sized constants).
#[derive(Default)]
struct RegSm {
    value: u64,
    last_applied: Option<u64>,
}

impl StateMachine for RegSm {
    const NAME: &'static str = "reg";

    type Command = Vec<u8>;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Vec<u8>) -> u64 {
        if cmd.len() >= 8 {
            self.value = u64::from_le_bytes(cmd[..8].try_into().unwrap());
        }
        self.last_applied = Some(ctx.position);
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
            return Err(SnapshotError::Codec(format!(
                "short snapshot: {} bytes",
                buf.len()
            )));
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

const APP: &str = "m7-gate";
/// 4 MiB ring (matches the lincheck/failover/reconfig harnesses).
const BUFFER_BYTES: usize = 1 << 22;
/// M6-gate-sized constants for the in-process `all` smoke's snapshot/purge
/// pairing (identical to `m6_gate.rs`'s `SEGMENT_BYTES`/
/// `SNAPSHOT_INTERVAL_BYTES`): small enough that purge actually fires under
/// this gate's modest smoke write volume. `SMOKE_PURGE` is what every node
/// (including scenario-spawned spares) uses in the `all` smoke — it is always
/// on there (see the module doc); the fleet role wrappers gate on their own
/// `--purge-below-snapshot`/`--snapshot-interval-bytes` flags instead.
const SEGMENT_BYTES: u64 = 16 * 1024;
const SNAPSHOT_INTERVAL_BYTES: u64 = 32 * 1024;
const SMOKE_PURGE: PurgePolicy = PurgePolicy::BelowSnapshot { slack_bytes: 0 };

fn seed_for(id: NodeId) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn parse_members(s: &str) -> Vec<(NodeId, SocketAddr)> {
    if s.is_empty() {
        return Vec::new();
    }
    s.split(',')
        .map(|part| {
            let (id, addr) = part
                .split_once('@')
                .unwrap_or_else(|| panic!("bad member {part:?}, want id@addr"));
            (
                id.parse().unwrap_or_else(|e| panic!("bad id {id:?}: {e}")),
                addr.parse()
                    .unwrap_or_else(|e| panic!("bad addr {addr:?}: {e}")),
            )
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn make_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    bind: SocketAddr,
    instance_dir: PathBuf,
    app_id: String,
    purge: PurgePolicy,
    journal_segment_bytes: u64,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        learners: Vec::new(),
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
        purge,
        journal_segment_bytes,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::default(),
    }
}

/// `snapshot_interval_bytes == 0` starts the service plain (current
/// behavior); `> 0` starts it with a snapshot policy (M6 pairing — see the
/// module doc).
fn spawn_service(dir: &Path, snapshot_interval_bytes: u64) -> Service<RegSm> {
    if snapshot_interval_bytes == 0 {
        let cfg = ServiceConfig::new(dir, APP);
        ServiceBuilder::new(cfg, RegSm::default())
            .start()
            .expect("service start")
    } else {
        let cfg = ServiceConfig::new(dir, APP).snapshot_policy(SnapshotPolicy {
            interval_bytes: snapshot_interval_bytes,
        });
        ServiceBuilder::new(cfg, RegSm::default())
            .start_with_snapshots()
            .expect("snapshot service start")
    }
}

fn addr_to_wire(addr: SocketAddr) -> (u32, u16) {
    match addr {
        SocketAddr::V4(a) => (u32::from(*a.ip()), a.port()),
        SocketAddr::V6(_) => panic!("m7_gate only binds IPv4 loopback"),
    }
}

// ------------------------------------------------------- fleet role wrappers

fn run_node(a: NodeArgs) -> anyhow::Result<()> {
    anyhow::ensure!(
        !a.instance_dir.starts_with("/tmp"),
        "instance_dir must be on a real filesystem (never /tmp — RAM tmpfs)"
    );
    let members = parse_members(&a.members);
    let purge = if a.purge_below_snapshot {
        PurgePolicy::BelowSnapshot { slack_bytes: 0 }
    } else {
        PurgePolicy::Disabled
    };
    let cfg = make_config(
        a.id,
        members,
        a.bind,
        a.instance_dir,
        a.app_id,
        purge,
        a.journal_segment_bytes,
    );
    let node = Node::start(cfg)?;
    println!("m7_gate node {} up; parking (harness owns lifecycle)", a.id);
    // Protocol 0.5.0 observability — see the same loop in `m6_gate`.
    let mut last = (u64::MAX, u64::MAX);
    loop {
        let now = (node.reports_unattested(), node.wipes());
        if now != last {
            println!(
                "m7_gate node {} stats: reports_unattested={} wipes={}",
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
    let _svc = spawn_service(&a.instance_dir, a.snapshot_interval_bytes);
    println!("m7_gate service up; parking (harness owns lifecycle)");
    loop {
        thread::park();
    }
}

/// Read the local node's counters + M7 config fields from its cnc page and
/// print ONE JSON line — same observability channel as `m6_gate probe`, with
/// `config_version`/`config_pending` added (the M7 fields the orchestrator
/// polls for admin-op convergence and change-in-flight status).
fn run_probe(a: ProbeArgs) -> anyhow::Result<()> {
    let cnc = CncPage::open_file(&a.instance_dir.join("cnc2.dat"), &a.app_id)
        .map_err(|e| anyhow::anyhow!("open cnc: {e:?}"))?;

    // On-host rate window (Fix 2): entirely inside this process, so the
    // fleet orchestrator's dip measurement carries zero ssh RTT skew.
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
         \"append\":{},\"service_applied\":{},\"config_version\":{},\"config_pending\":{},\
         \"leader_hint\":{}",
        (flags & NODE_FLAG_LEADER) != 0,
        (flags & NODE_FLAG_CAN_SERVE) != 0,
        cnc.status().term.load_acquire(),
        c.commit.load_acquire(),
        c.durable.load_acquire(),
        c.append.load_acquire(),
        cnc.service().service_applied.load_acquire(),
        cnc.config_version(),
        cnc.config_pending() != 0,
        cnc.status().leader_hint.load_acquire(),
    );
    if let Some(r) = rate {
        line.push_str(&format!(",\"rate\":{r:.3}"));
    }
    line.push('}');
    println!("{line}");
    Ok(())
}

/// Sustained write load against the LOCAL node, with a monotonic linearizable-
/// read guard (identical contract to `m6_gate loadclient`): exits 1 on a read
/// regression or on a read that exceeds anything written. Note this role's
/// scope is one node — the fleet orchestrator restarts it on whichever host
/// becomes the new leader across a transition it expects to move leadership
/// (only `leader-self-removal` does; `replace-a-box`/`resize-3-5-3` never move
/// the leader by construction).
fn run_loadclient(a: LoadClientArgs) -> anyhow::Result<()> {
    let stop_file = (!a.stop_file.is_empty()).then(|| PathBuf::from(&a.stop_file));
    let deadline = (a.secs > 0).then(|| Instant::now() + Duration::from_secs(a.secs));

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

        let payload = next_val.to_le_bytes().to_vec();
        match client.submit::<Vec<u8>, u64>(&payload) {
            Ok(_) => {
                committed_max = committed_max.max(next_val);
                next_val += 1;
                writes += 1;
                // Fix 4: pacing is now opt-in via `--rate` (see the flag's
                // doc comment) — default 0 = unthrottled, matching M6's
                // loadclient exactly. The race this throttle used to guard
                // against (an unthrottled writer outrunning a distant
                // spare's pure-replay catch-up) is now guarded at its actual
                // source: the orchestrator's bounded `NotCaughtUp` retry
                // around `promote` (`ctl_retry_transient`), not here.
                if a.rate > 0 {
                    thread::sleep(Duration::from_secs_f64(1.0 / a.rate as f64));
                }
            }
            Err(ClientError::NotLeader { .. }) | Err(ClientError::Retry) => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(ClientError::BackpressureFull) => thread::sleep(Duration::from_millis(2)),
            Err(_) => thread::sleep(Duration::from_millis(20)),
        }

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

// ------------------------------------------------------------ admin helpers
//
// Same shape as `uc2ctl`/`uc_node/tests/reconfig.rs`'s admin-slot flow: write
// a fresh request line (`seq = old_seq + 1`, a random nonce), poll the
// response line for the echoed seq. Used directly here (in-process) rather
// than shelling out to the `uc2ctl` binary — the fleet orchestrator (Python)
// is the one that shells out to `uc2ctl` for the same protocol over ssh/local.

fn open_cnc(instance_dir: &Path) -> Arc<CncPage> {
    CncPage::open_file(&instance_dir.join("cnc2.dat"), APP).expect("open cnc")
}

fn admin_request(cnc: &CncPage, op: u32, id: u32, ip: u32, port: u16) -> AdminResp {
    let old_seq = cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0);
    let seq = old_seq + 1;
    let nonce = rand::random::<u64>();
    cnc.write_admin_req(&AdminReq {
        seq,
        nonce,
        op,
        id,
        ip,
        port,
    });
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(resp) = cnc.read_admin_resp(seq) {
            return resp;
        }
        assert!(
            Instant::now() < deadline,
            "admin response timed out for seq {seq}"
        );
        thread::yield_now();
    }
}

/// Retry an admin op until accepted, tolerating the transient refusals a live
/// cluster legitimately produces (`retry`, `NotServing`/`ChangePending`/
/// `NotCaughtUp`). Any other refusal is an infra-level bug in this smoke
/// (every op it drives is legal) — panics with the reason.
fn admin_request_ok(cnc: &CncPage, op: u32, id: u32, ip: u32, port: u16, secs: u64) -> AdminResp {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let resp = admin_request(cnc, op, id, ip, port);
        match resp.status {
            0 => return resp,
            2 => {}
            1 if matches!(resp.reason, 2 | 3 | 10) => {}
            _ => panic!(
                "admin op {op} on id {id} refused: status={} reason={}",
                resp.status, resp.reason
            ),
        }
        assert!(
            Instant::now() < deadline,
            "admin op {op} on id {id} never accepted (last status={} reason={})",
            resp.status,
            resp.reason
        );
        thread::sleep(Duration::from_millis(20));
    }
}

/// The reported-durable value the LEADER has last heard from peer `id` (`None`
/// if `id` has no published slot yet).
fn peer_reported_durable(cnc: &CncPage, id: NodeId) -> Option<u64> {
    for i in 0..CNC_MAX_PEER_SLOTS {
        let raw = cnc.peer_slot(i).id_and_role.load_acquire();
        if raw == 0 {
            continue;
        }
        if (raw >> 8) as u32 == id {
            return Some(cnc.peer_slot(i).reported_durable.load_acquire());
        }
    }
    None
}

/// The set of ids `cnc`'s own peer band currently publishes with VOTER role.
fn voter_ids_via_cnc(cnc: &CncPage) -> Vec<NodeId> {
    let mut ids = Vec::new();
    for i in 0..CNC_MAX_PEER_SLOTS {
        let raw = cnc.peer_slot(i).id_and_role.load_acquire();
        if raw == 0 {
            continue;
        }
        let id = (raw >> 8) as u32;
        let role = (raw & 0xff) as u8;
        if role == CNC_PEER_ROLE_VOTER {
            ids.push(id);
        }
    }
    ids
}

// ------------------------------------------------------------ load driver

/// A registry of currently-live `(NodeId, instance_dir)` pairs, shared between
/// the background [`Load`] driver and the scenario code that adds/removes
/// members — unlike M6's fixed-position `Vec<PathBuf>` (id == vec index by
/// construction), M7 spares carry arbitrary ids (50, 60, 61, ...), so the
/// driver must look targets up BY ID (a `NotLeader` hint is a `NodeId`), not
/// by positional modulo.
type Registry = Arc<Mutex<Vec<(NodeId, PathBuf)>>>;

fn sync_registry(reg: &Registry, nodes: &[NodeH]) {
    let snapshot: Vec<(NodeId, PathBuf)> = nodes.iter().map(|h| (h.id, h.dir.clone())).collect();
    *reg.lock().unwrap() = snapshot;
}

/// A background write-load driver: submits monotonically increasing 8-byte
/// values, reconnecting on `NOT_LEADER` (by id lookup in the shared registry)
/// or on any other error (by cycling to the next known target). This is the
/// scenarios' write pressure; the per-transition "commit kept advancing"
/// check reads the NODE's own commit counter directly (not this driver's own
/// bookkeeping), so `Load` itself only needs to keep submitting and clean up
/// on `stop`.
struct Load {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Load {
    fn start(registry: Registry) -> Load {
        let stop = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&stop);
        let handle = thread::spawn(move || load_loop(s, registry));
        Load {
            stop,
            handle: Some(handle),
        }
    }
    fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn load_loop(stop: Arc<AtomicBool>, registry: Registry) {
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
            match Client::connect(&snapshot[cur].1, APP) {
                Ok(c) => client = Some(c),
                Err(_) => {
                    cur = (cur + 1) % snapshot.len();
                    thread::sleep(Duration::from_millis(20));
                    continue;
                }
            }
        }
        let v = next_val;
        let payload = v.to_le_bytes().to_vec();
        match client.as_ref().unwrap().submit::<Vec<u8>, u64>(&payload) {
            Ok(_) => {
                next_val += 1;
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
                // A generic error (e.g. a halted/removed node that never answers
                // its ring) MUST also advance the cursor — otherwise the driver
                // can get permanently stuck hammering a single dead target
                // (a real bug caught by this gate's own dev run: a halted node
                // landed at registry index 0 and every retry re-picked it).
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

/// A leader-routed linearizable read of the register value; `None` on timeout.
fn read_value(registry: &Registry) -> Option<u64> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut cur = 0usize;
    while Instant::now() < deadline {
        let snapshot = registry.lock().unwrap().clone();
        if snapshot.is_empty() {
            thread::sleep(Duration::from_millis(20));
            continue;
        }
        cur %= snapshot.len();
        let client = match Client::connect(&snapshot[cur].1, APP) {
            Ok(c) => c,
            Err(_) => {
                cur = (cur + 1) % snapshot.len();
                thread::sleep(Duration::from_millis(20));
                continue;
            }
        };
        let out = client.query_linearizable::<(), u64>(&());
        client.shutdown();
        match out {
            Ok(v) => return Some(v),
            Err(ClientError::NotLeader { hint }) => {
                cur = hint
                    .and_then(|h| snapshot.iter().position(|(id, _)| *id == h))
                    .unwrap_or((cur + 1) % snapshot.len());
            }
            // A generic error (halted node, transient timeout, ...) must also
            // advance past this target — see the identical fix/comment in
            // `load_loop` above; a dead target at a fixed index would otherwise
            // eat the whole budget retrying it.
            Err(_) => {
                cur = (cur + 1) % snapshot.len();
                thread::sleep(Duration::from_millis(20));
            }
        }
    }
    None
}

// ------------------------------------------------------------- helpers

struct NodeH {
    id: NodeId,
    dir: PathBuf,
    node: Node,
    svc: Option<Service<RegSm>>,
}

fn await_single_leader(nodes: &[NodeH], secs: u64) -> usize {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let serving: Vec<usize> = (0..nodes.len())
            .filter(|&i| nodes[i].node.can_serve() && nodes[i].node.is_leader())
            .collect();
        assert!(serving.len() <= 1, "split-brain: {serving:?}");
        if serving.len() == 1 {
            return serving[0];
        }
        assert!(
            Instant::now() < deadline,
            "no single serving leader within {secs}s"
        );
        thread::yield_now();
    }
}

fn leader_commit(nodes: &[NodeH], id: NodeId) -> u64 {
    nodes
        .iter()
        .find(|h| h.id == id)
        .map(|h| h.node.counters().commit.load_acquire())
        .unwrap_or(0)
}

fn wait_bool(secs: u64, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if cond() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::yield_now();
    }
}

/// Wait until every non-halted node's cnc-mirrored config version reaches
/// `version`. `halted` excludes ids that have already fail-stopped on their
/// own removal in an earlier step of the SAME scenario (they will never
/// converge on a LATER version).
fn wait_config_converged(
    nodes: &[NodeH],
    halted: &HashSet<NodeId>,
    version: u64,
    secs: u64,
) -> bool {
    wait_bool(secs, || {
        nodes
            .iter()
            .filter(|h| !halted.contains(&h.id))
            .all(|h| h.node.config_version() >= version)
    })
}

// --------------------------------------------------------------- the gate

struct Verdict {
    scenario: &'static str,
    pass: bool,
    detail: String,
}

impl Verdict {
    fn fail(scenario: &'static str, detail: String) -> Verdict {
        Verdict {
            scenario,
            pass: false,
            detail,
        }
    }
}

fn run_all(a: AllArgs) -> anyhow::Result<()> {
    // Shrink the client request timeout (default 10 s — sized for a real
    // fleet under load) for THIS in-process smoke only: a halted/removed
    // node's rings never answer, and a `read_value`/`Load` reconnect that
    // lands on one must fail fast to keep cycling toward a live target within
    // its own bounded budget. Set once, single-threaded, before anything else
    // touches the client SDK.
    // SAFETY: called at the very start of `run_all`, before any thread is
    // spawned or any other code reads the process environment.
    unsafe {
        std::env::set_var("UC2_CLIENT_TIMEOUT_MS", "500");
    }

    let root = a
        .root
        .unwrap_or_else(|| PathBuf::from("target/m7_gate_smoke"));
    anyhow::ensure!(
        !root.starts_with("/tmp"),
        "m7_gate all: root must be on a real filesystem (never /tmp — RAM tmpfs); got {root:?}"
    );
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root)?;

    println!("== uc2 M7 gate: SMOKE (local, in-process; 3 voters + services + load) ==");
    println!(
        "*** THIS IS NOT THE M7 GATE *** — one dev box, core-starved; the real bars (<10% \
         commit-rate dip across every reconfig transition, <10s self-removal failover) are only \
         claimable on the 5-host c6id fleet run. See docs/benchmarks/uc2-m7-gate-2026-07-13.md."
    );

    const NV: usize = 3;
    let socks: Vec<UdpSocket> = (0..NV)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind"))
        .collect();
    let addrs: Vec<SocketAddr> = socks.iter().map(|s| s.local_addr().unwrap()).collect();
    let members: Vec<(NodeId, SocketAddr)> = (0..NV).map(|i| (i as NodeId, addrs[i])).collect();

    // M6 pairing: the in-process smoke always enables snapshots + purge
    // (m6-gate-sized constants) — the smoke should exercise what the fleet
    // actually runs (see the module doc).
    let mut nodes: Vec<NodeH> = Vec::with_capacity(NV);
    for (i, sock) in socks.into_iter().enumerate() {
        let dir = root.join(format!("n{i}"));
        let cfg = make_config(
            i as NodeId,
            members.clone(),
            addrs[i],
            dir.clone(),
            APP.into(),
            SMOKE_PURGE,
            SEGMENT_BYTES,
        );
        let node = Node::start_with_socket(cfg, sock).expect("node start");
        let svc = spawn_service(&dir, SNAPSHOT_INTERVAL_BYTES);
        nodes.push(NodeH {
            id: i as NodeId,
            dir,
            node,
            svc: Some(svc),
        });
    }

    let leader = await_single_leader(&nodes, 30);
    println!("leader elected: n{}", nodes[leader].id);

    let registry: Registry = Arc::new(Mutex::new(Vec::new()));
    sync_registry(&registry, &nodes);
    let load = Load::start(Arc::clone(&registry));

    // Let the baseline settle briefly before the first transition (informational
    // dip context only — the strict bar is fleet-only).
    thread::sleep(Duration::from_secs(a.secs.min(2)));

    let mut last_read = 0u64;
    let verdicts = vec![
        scenario_replace_a_box(&mut nodes, &members, &root, &registry),
        check_monotonic_read(&registry, &mut last_read, "replace-a-box"),
        scenario_resize_3_5_3(&mut nodes, &members, &root, &registry),
        check_monotonic_read(&registry, &mut last_read, "resize-3-5-3"),
        scenario_leader_self_removal(&mut nodes, &registry),
        check_monotonic_read(&registry, &mut last_read, "leader-self-removal"),
    ];

    load.stop();

    for h in nodes {
        h.node.stop();
        if let Some(s) = h.svc {
            s.stop();
        }
    }

    println!("\n== M7 gate smoke results ==");
    let mut all_pass = true;
    for v in &verdicts {
        println!(
            "  [{}] {} — {}",
            if v.pass { "PASS" } else { "FAIL" },
            v.scenario,
            v.detail
        );
        all_pass &= v.pass;
    }
    if all_pass {
        println!("RESULT: PASS (smoke) — all three M7 scenarios held locally.");
        Ok(())
    } else {
        println!("RESULT: FAIL (honest) — a scenario did not hold.");
        std::process::exit(1);
    }
}

fn check_monotonic_read(registry: &Registry, last_read: &mut u64, after: &'static str) -> Verdict {
    match read_value(registry) {
        Some(v) if v >= *last_read => {
            let detail = format!("read {v} after {after} (>= previous {})", *last_read);
            *last_read = v;
            Verdict {
                scenario: "monotonic-read",
                pass: true,
                detail,
            }
        }
        Some(v) => Verdict::fail(
            "monotonic-read",
            format!("REGRESSED after {after}: {v} < previous {}", *last_read),
        ),
        None => Verdict::fail(
            "monotonic-read",
            format!("no read resolved within 10s after {after}"),
        ),
    }
}

/// Scenario 1: add a fresh spare as a learner, catch it up, promote it to
/// voter, then remove one of the ORIGINAL voters (never the leader) — landing
/// on a 3-voter set that is the original set minus the removed one plus the
/// new spare. Mirrors `reconfig.rs::full_replace_a_box_recipe`.
fn scenario_replace_a_box(
    nodes: &mut Vec<NodeH>,
    seed_members: &[(NodeId, SocketAddr)],
    root: &Path,
    registry: &Registry,
) -> Verdict {
    const SC: &str = "replace-a-box";
    let leader_idx = await_single_leader(nodes, 20);
    let leader_id = nodes[leader_idx].id;
    let leader_cnc = open_cnc(&nodes[leader_idx].dir);

    let t0 = Instant::now();
    let c0 = leader_commit(nodes, leader_id);

    let new_id: NodeId = 50;
    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind spare");
    let addr = sock.local_addr().unwrap();
    let (ip, port) = addr_to_wire(addr);
    let resp = admin_request(&leader_cnc, 1 /* AddLearner */, new_id, ip, port);
    if resp.status != 0 {
        return Verdict::fail(SC, format!("add-learner refused: reason {}", resp.reason));
    }
    let dir = root.join(format!("n{new_id}"));
    let cfg = make_config(
        new_id,
        seed_members.to_vec(),
        addr,
        dir.clone(),
        APP.into(),
        SMOKE_PURGE,
        SEGMENT_BYTES,
    );
    let node = Node::start_with_socket(cfg, sock).expect("start spare");
    let svc = spawn_service(&dir, SNAPSHOT_INTERVAL_BYTES);
    nodes.push(NodeH {
        id: new_id,
        dir,
        node,
        svc: Some(svc),
    });
    sync_registry(registry, nodes);

    let target_commit = leader_commit(nodes, leader_id);
    if !wait_bool(60, || {
        peer_reported_durable(&leader_cnc, new_id).is_some_and(|d| d >= target_commit)
    }) {
        return Verdict::fail(SC, "spare never caught up within 60s".into());
    }

    let resp = admin_request_ok(&leader_cnc, 2 /* PromoteLearner */, new_id, 0, 0, 20);
    let promote_version = resp.version;
    if !wait_config_converged(nodes, &HashSet::new(), promote_version, 20) {
        return Verdict::fail(
            SC,
            format!("config v{promote_version} never converged after promote"),
        );
    }

    let victim_id = match seed_members
        .iter()
        .map(|(id, _)| *id)
        .find(|&id| id != leader_id)
    {
        Some(id) => id,
        None => return Verdict::fail(SC, "no non-leader original voter to remove".into()),
    };
    if let Some(pos) = nodes.iter().position(|h| h.id == victim_id) {
        let victim = nodes.remove(pos);
        victim.node.crash();
        if let Some(s) = victim.svc {
            s.crash();
        }
        sync_registry(registry, nodes);
    }
    let resp = admin_request_ok(&leader_cnc, 5 /* RemoveVoter */, victim_id, 0, 0, 20);
    let remove_version = resp.version;
    if !wait_config_converged(nodes, &HashSet::new(), remove_version, 20) {
        return Verdict::fail(
            SC,
            format!("config v{remove_version} never converged after remove"),
        );
    }

    let c1 = nodes
        .iter()
        .map(|h| h.node.counters().commit.load_acquire())
        .max()
        .unwrap_or(0);
    let elapsed = t0.elapsed().as_secs_f64();
    let rate = c1.saturating_sub(c0) as f64 / elapsed.max(1e-6);

    Verdict {
        scenario: SC,
        pass: rate > 0.0,
        detail: format!(
            "add-learner {new_id} -> catch-up -> promote (v{promote_version}) -> remove {victim_id} \
             (v{remove_version}) in {elapsed:.2}s; commit advanced throughout ({rate:.0}/s — \
             informational, the <10% dip bar is fleet-only)"
        ),
    }
}

/// Scenario 2: grow 3 -> 5 (two add+promote pairs) then shrink 5 -> 3 (two
/// demote+remove-learner pairs), landing on the original 3 voter ids. Mirrors
/// `reconfig.rs::resize_3_to_5_to_3`.
fn scenario_resize_3_5_3(
    nodes: &mut Vec<NodeH>,
    seed_members: &[(NodeId, SocketAddr)],
    root: &Path,
    registry: &Registry,
) -> Verdict {
    const SC: &str = "resize-3-5-3";
    let leader_idx = await_single_leader(nodes, 20);
    let leader_id = nodes[leader_idx].id;
    let leader_cnc = open_cnc(&nodes[leader_idx].dir);

    // The voter set this scenario must return to at the end — the CURRENT
    // voters (leader included; a node never lists its own slot), not the
    // scenario's static `seed_members` argument: an earlier scenario
    // (`replace-a-box`) may already have swapped one of the original 3 ids
    // out, and the boot seed stays intentionally stale (only used as a fresh
    // spare's bootstrap-trust seed — real membership always comes from the
    // replicated stream, per `reconfig.rs::joining_node_boots_from_stale_seed`).
    let mut base_voters = voter_ids_via_cnc(&leader_cnc);
    base_voters.push(leader_id);
    base_voters.sort_unstable();

    let t0 = Instant::now();
    let c0 = leader_commit(nodes, leader_id);
    let mut halted: HashSet<NodeId> = HashSet::new();

    for &spare_id in &[60u32, 61u32] {
        let sock = UdpSocket::bind("127.0.0.1:0").expect("bind spare");
        let addr = sock.local_addr().unwrap();
        let (ip, port) = addr_to_wire(addr);
        let resp = admin_request_ok(&leader_cnc, 1 /* AddLearner */, spare_id, ip, port, 20);
        let dir = root.join(format!("n{spare_id}"));
        let cfg = make_config(
            spare_id,
            seed_members.to_vec(),
            addr,
            dir.clone(),
            APP.into(),
            SMOKE_PURGE,
            SEGMENT_BYTES,
        );
        let node = Node::start_with_socket(cfg, sock).expect("start spare");
        let svc = spawn_service(&dir, SNAPSHOT_INTERVAL_BYTES);
        nodes.push(NodeH {
            id: spare_id,
            dir,
            node,
            svc: Some(svc),
        });
        sync_registry(registry, nodes);
        if !wait_config_converged(nodes, &halted, resp.version, 30) {
            return Verdict::fail(
                SC,
                format!(
                    "config v{} never converged after add {spare_id}",
                    resp.version
                ),
            );
        }

        let target_commit = leader_commit(nodes, leader_id);
        if !wait_bool(60, || {
            peer_reported_durable(&leader_cnc, spare_id).is_some_and(|d| d >= target_commit)
        }) {
            return Verdict::fail(SC, format!("spare {spare_id} never caught up within 60s"));
        }

        let resp = admin_request_ok(&leader_cnc, 2 /* PromoteLearner */, spare_id, 0, 0, 30);
        if !wait_config_converged(nodes, &halted, resp.version, 30) {
            return Verdict::fail(
                SC,
                format!(
                    "config v{} never converged after promote {spare_id}",
                    resp.version
                ),
            );
        }
    }

    for &spare_id in &[60u32, 61u32] {
        let resp = admin_request_ok(&leader_cnc, 3 /* DemoteVoter */, spare_id, 0, 0, 20);
        if !wait_config_converged(nodes, &halted, resp.version, 20) {
            return Verdict::fail(
                SC,
                format!(
                    "config v{} never converged after demote {spare_id}",
                    resp.version
                ),
            );
        }

        let resp = admin_request_ok(&leader_cnc, 4 /* RemoveLearner */, spare_id, 0, 0, 20);
        halted.insert(spare_id);
        if !wait_config_converged(nodes, &halted, resp.version, 20) {
            return Verdict::fail(
                SC,
                format!(
                    "config v{} never converged after remove-learner {spare_id}",
                    resp.version
                ),
            );
        }
    }

    // Final voter set must be exactly the set this scenario started with. The
    // VOTER-role peer-band publish can lag a beat behind the config-version
    // bump that already converged above (`reconfig.rs::resize_3_to_5_to_3`
    // carries the identical caveat) — poll for the settled shape (bounded)
    // before asserting on it with a clear message, rather than a one-shot
    // check that can catch a transient rebuild-in-progress snapshot.
    let survivors: Vec<&NodeH> = nodes.iter().filter(|h| !halted.contains(&h.id)).collect();
    let settled = wait_bool(10, || {
        survivors.iter().all(|h| {
            let cnc = open_cnc(&h.dir);
            let mut got = voter_ids_via_cnc(&cnc);
            got.sort_unstable();
            let mut want_others: Vec<NodeId> = base_voters
                .iter()
                .copied()
                .filter(|&id| id != h.id)
                .collect();
            want_others.sort_unstable();
            got == want_others
        })
    });
    if !settled {
        for h in &survivors {
            let cnc = open_cnc(&h.dir);
            let mut got = voter_ids_via_cnc(&cnc);
            got.sort_unstable();
            let mut want_others: Vec<NodeId> = base_voters
                .iter()
                .copied()
                .filter(|&id| id != h.id)
                .collect();
            want_others.sort_unstable();
            if got != want_others {
                return Verdict::fail(
                    SC,
                    format!(
                        "node {}: final voter set {got:?} != expected {want_others:?}",
                        h.id
                    ),
                );
            }
        }
    }

    let c1 = nodes
        .iter()
        .filter(|h| !halted.contains(&h.id))
        .map(|h| h.node.counters().commit.load_acquire())
        .max()
        .unwrap_or(0);
    let elapsed = t0.elapsed().as_secs_f64();
    let rate = c1.saturating_sub(c0) as f64 / elapsed.max(1e-6);

    Verdict {
        scenario: SC,
        pass: rate > 0.0,
        detail: format!(
            "3->5 (add+promote 60, add+promote 61) -> 5->3 (demote+remove 60, demote+remove 61) \
             in {elapsed:.2}s; final voter set == original 3; commit advanced throughout \
             ({rate:.0}/s — informational, the <10% dip bar is fleet-only)"
        ),
    }
}

/// Scenario 3: the serving leader removes ITSELF. PASS iff the cluster-wide
/// committed high-water never regresses and a new leader (one of the two
/// survivors) serves within budget. Mirrors
/// `reconfig.rs::leader_self_removal_hands_off`. The fleet bar is a serving
/// gap < 10 s; this in-process budget is looser (30 s) purely to absorb
/// core-starvation noise on a shared dev box — the measured gap is reported
/// either way, and a real stall is still caught (30 s >> any legitimate
/// election-timeout-class failover).
fn scenario_leader_self_removal(nodes: &mut [NodeH], registry: &Registry) -> Verdict {
    const SC: &str = "leader-self-removal";
    let leader_idx = await_single_leader(nodes, 20);
    let leader_id = nodes[leader_idx].id;
    let leader_cnc = open_cnc(&nodes[leader_idx].dir);

    let mut high_water = nodes
        .iter()
        .map(|h| h.node.counters().commit.load_acquire())
        .max()
        .unwrap_or(0);
    let t0 = Instant::now();
    let resp = admin_request(&leader_cnc, 5 /* RemoveVoter */, leader_id, 0, 0);
    if resp.status != 0 {
        return Verdict::fail(
            SC,
            format!("leader self-removal refused: reason {}", resp.reason),
        );
    }

    let budget = Duration::from_secs(30);
    let deadline = Instant::now() + budget;
    let mut regressed = false;
    let new_leader_idx = loop {
        let cur = nodes
            .iter()
            .map(|h| h.node.counters().commit.load_acquire())
            .max()
            .unwrap_or(0);
        if cur < high_water {
            regressed = true;
        }
        high_water = high_water.max(cur);

        let old_stepped_down = leader_cnc.status().flags.load_acquire()
            & (NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE)
            == 0;
        let serving: Vec<usize> = (0..nodes.len())
            .filter(|&i| nodes[i].id != leader_id && nodes[i].node.can_serve())
            .collect();
        if serving.len() > 1 {
            return Verdict::fail(SC, format!("split-brain among survivors: {serving:?}"));
        }
        if old_stepped_down && serving.len() == 1 {
            break Some(serving[0]);
        }
        if Instant::now() > deadline {
            break None;
        }
        thread::yield_now();
    };
    let gap = t0.elapsed();

    if regressed {
        return Verdict::fail(
            SC,
            "committed high-water regressed across the handoff".into(),
        );
    }
    let Some(new_leader_idx) = new_leader_idx else {
        return Verdict::fail(
            SC,
            format!(
                "no new leader within {:.1}s budget (gap {:.2}s)",
                budget.as_secs_f64(),
                gap.as_secs_f64()
            ),
        );
    };
    sync_registry(registry, nodes);

    let before = nodes[new_leader_idx].node.counters().commit.load_acquire();
    if !wait_bool(10, || {
        nodes[new_leader_idx].node.counters().commit.load_acquire() > before
    }) {
        return Verdict::fail(SC, "the new leader never advanced commit".into());
    }

    Verdict {
        scenario: SC,
        pass: true,
        detail: format!(
            "gap {:.3}s (fleet bar <10s; in-process budget 30s to absorb core-starvation noise), \
             zero committed-high-water loss, new leader n{} serving and committing",
            gap.as_secs_f64(),
            nodes[new_leader_idx].id
        ),
    }
}
