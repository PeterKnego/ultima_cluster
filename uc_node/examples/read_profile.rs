// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Linearizable-read profile harness: does UC's ReadIndex barrier cost read
//! *capacity*, or are the single-writer agents bottlenecked elsewhere (apply
//! frontier, egress broadcast)? This is the gating open question (§6.1) of
//! `docs/superpowers/specs/2026-07-24-uc2-leader-lease-design.md`, which
//! proposes batch-probe coalescing (Rung A) and a clock-based lease (Rung B)
//! but says to measure before building either. Full design:
//! `docs/superpowers/specs/2026-07-25-uc2-read-profile-design.md`.
//!
//! ```text
//! cargo run -p uc_node --release --example read_profile -- node    --id N --bind A --members ID@ADDR,... --instance-dir D [--admission-kib K]
//! cargo run -p uc_node --release --example read_profile -- service --instance-dir D
//! cargo run -p uc_node --release --example read_profile -- client  --instance-dir D --secs S --readers K [--mode lin|snap] [--write-rate W] [--node-pid P] [--service-pid Q]
//! cargo run -p uc_node --release --example read_profile -- all     --secs S --readers K [--mode lin|snap] [--write-rate W]   # local smoke, NOT a fleet number
//! cargo run -p uc_node --release --example read_profile -- ladder --secs S --readers 1,4,16,64,256,1024 [--write-rate W]     # local smoke sweep, NOT a fleet number
//! cargo run -p uc_node --release --example read_profile -- decide --rungs FILE                                              # evaluate the rule over collected rung JSON lines
//! ```
//!
//! **`node`/`service`** are thin fleet-role wrappers (one process per host,
//! systemd-run-friendly) over the real SDK stack: `Node::start` for the
//! consensus/log/IPC side, `ServiceBuilder` running the trivial [`ProfileSm`]
//! counter for the apply side. Both park forever once started — the harness
//! owns their lifecycle.
//!
//! **`client`** is the measuring role, `m5_gate`'s pattern reused verbatim: it
//! bypasses `uc_client`'s per-op channel machinery and instead opens the same
//! `cnc.dat` + `query.ring` + both egress broadcasts a real client would,
//! stamping its own `local_seq` and correlating answers through a
//! preallocated `Box<[AtomicU64]>` slot array. See `run_read_measurement`.
//!
//! ## The A/B: what a "linearizable" vs. "snapshot" read actually differs by
//!
//! `--mode` sets exactly one bit on the query record — `FLAG_V2_LINEARIZABLE`.
//! The fork is `uc_node/src/node.rs:1956`, inside `drain_query_ring`: with the
//! flag clear the node forwards the query straight to the service
//! (`node.rs:1958`); with it set, the node opens a READ_PROBE quorum barrier
//! and waits for the service to catch up to the confirmed read position before
//! forwarding. Admission, the per-cycle query drain cap, the service, and the
//! egress path are all identical between the two arms. That means the
//! **lin-vs-snap throughput delta is the barrier's end-to-end cost, measured
//! without instrumenting any production code** — the harness is entirely an
//! attaching party (like `uc_client`), never a participant in the traced path.
//!
//! **Caveat that changes what the delta means between the two workload arms:**
//! a snapshot read skips BOTH the READ_PROBE barrier AND the wait for the
//! service to catch up to the leader's commit frontier ("the frontier wait").
//! In the read-only arm (`--write-rate 0`) the frontier wait is free regardless
//! — with nothing committing, `service_applied >= commit_at` already holds
//! when a read is admitted — so there the delta isolates the barrier alone. In
//! the mixed arm the frontier wait is real work a linearizable read pays and a
//! snapshot read does not, so there the delta is barrier-cost **plus**
//! frontier-wait cost, not the barrier alone. `run_ladder`'s closing
//! `REMINDER:` line restates this so it cannot be missed reading the output.
//!
//! ## The occupancy proxy: yield rate, not CPU time
//!
//! **The original plan.** The four node agents are already named threads
//! (`uc2-consensus`, `uc2-sender`, `uc2-receiver`, `uc2-archive` — see
//! `AgentRunner::spawn`), so in principle `/proc/<pid>/task/*/stat` CPU time
//! would rank them directly.
//! It doesn't: every agent idles on `IdleStrategy::Yield`
//! (`uc_log/src/agent.rs:28` → `std::thread::yield_now()`), so an agent with
//! nothing to do still burns a core spinning through empty duty cycles and its
//! CPU% saturates by construction — CPU time carries no signal about which
//! agent is actually busy. What differs is the yield RATE: each empty duty
//! cycle costs one `sched_yield`, which the kernel counts in
//! `/proc/<pid>/task/<tid>/status`'s `voluntary_ctxt_switches`, so a BUSY agent
//! (spending its time on real work between polls) yields LESS often than an
//! idle one. `sample_yields`/`occupancy_delta` turn that into an ORDINAL
//! ranking (fewest yields/sec = busiest) — it is not a duty-cycle percentage
//! and is not meant to be read as one.
//!
//! **Rows are keyed by `(pid, tid)`, never by thread name.** Agent names are
//! static (`AgentRunner::spawn`), so a process running three nodes — which is
//! exactly what `all`/`ladder` do — has three threads called `uc2-consensus`,
//! three called `uc2-apply`, and so on. Joining the before/after samples by name
//! would difference unrelated threads, and a mis-paired row saturates to zero,
//! which then sorts to the FRONT of the ascending ranking and impersonates the
//! busiest agent. Rows are therefore labelled `name#tid` for humans and joined
//! on `(pid, tid)` for the ranking.
//!
//! **On a fleet the service is a separate process**, so `--node-pid` alone
//! cannot see `uc2-apply` in the diagnostic sample. Pass `--service-pid` too;
//! the sampler takes the union of both task dirs. (Diagnostic only — see
//! below; this does not affect the decision rule.)
//!
//! ### The yield-rate proxy is DEAD — it is diagnostic output, not a signal
//!
//! The premise above — that `sched_yield` increments `voluntary_ctxt_switches` —
//! **does not hold**. Measured twice independently: 1,483,000 `sched_yield()`
//! calls produced **+1** voluntary context switch (Rust/Python probe), and
//! 2,000,000 calls produced **+0** (C probe, +677 nonvoluntary = preemption
//! noise). `sched_yield` leaves the task `TASK_RUNNING`, so any switch it causes
//! is accounted NONvoluntary — and with no other runnable task on the CPU it
//! often causes no switch at all.
//!
//! This is not fixable with a different `/proc` field. A yield-idling agent and
//! a busy one are **indistinguishable at the OS level**: both burn 100% of a
//! core, neither ever blocks. Only feature-gated duty-cycle counters inside
//! `AgentRunner` could measure true occupancy, and adding those means touching
//! `src/` — out of scope here.
//!
//! So the ranking ranks nothing, and **clause (b) was reformulated to be
//! answerable from data the harness already collects** (see
//! [`evaluate_clause_b`]). The sampler is kept — correctly keyed by
//! `(pid, tid)`, tested, and printed under an explicit caveat — because it
//! becomes useful the moment real duty-cycle counters land. It does **not** feed
//! the decision rule.
//!
//! ## Env caps (sandbox safety, m1–m5 pattern)
//!
//! `UC2_RP_MAX_SECS`, `UC2_RP_MAX_READERS` clip `--secs`/`--readers` from
//! ABOVE when set to a nonzero value; unset (the fleet's mode) is a no-op.
//! `env_cap` is applied per-role and, in `ladder`, per-rung.
//!
//! ## The decision rule is a pre-commitment, not a tunable
//!
//! `evaluate_decision_rule` implements the design spec's §2 rule VERBATIM:
//! build Rung A iff (a) the linearizable plateau is ≤70% of the snapshot
//! plateau (65–75% is a borderline band that does NOT license a decision on
//! local smoke) AND (b) that gap is present in the **read-only arm**, the client
//! sustained ≥90% of target concurrency there, and neither read-only arm is
//! degraded. The 70% figure and the band were fixed before any run and are not
//! adjusted once numbers exist — changing this function to fit a result would
//! defeat the entire point of pre-committing it.
//!
//! Clause (b) was **amended on 2026-07-25, before any measurement data existed**,
//! because its original formulation (which agent is top-occupancy) rested on a
//! metric that turned out to measure nothing — see the dead-proxy section below
//! and the spec's dated amendment note. The amended clause discharges the same
//! job: rule out that something other than the barrier explains the gap. The
//! read-only arm is the substantive part — with no writes in flight the frontier
//! wait is free, so a gap there is barrier cost, whereas a gap that appears only
//! under write load is frontier-wait cost.
//!
//! Two guards sit AROUND (not inside) those thresholds, because a broken run
//! must not read as a favourable one:
//!
//! - **Degraded arms are inconclusive.** A linearizable rung that collapses into
//!   `MSG_V2_RETRY` (read-barrier timeout, momentary `!can_serve`) resolves few
//!   genuine reads over the full elapsed window, which is a LOW ratio — i.e. it
//!   presents exactly like the result that justifies building Rung A. An arm
//!   whose `(retried + not_leader)` share of resolved ops exceeds
//!   [`DEGRADED_FRACTION`] yields INCONCLUSIVE instead of any verdict.
//! - **The rule is reachable from the fleet path.** The `client` role emits one
//!   JSON rung record per run (`rung_to_json`) and `decide --rungs FILE` feeds
//!   those lines to the SAME `evaluate_decision_rule`, so the externally
//!   orchestrated fleet ladder never re-implements the rule in Python.
//!
//! ## *** LOCAL RUNS ARE WIRING VERIFICATION, NOT MEASUREMENT ***
//!
//! `all` and `ladder` boot 3 nodes + 3 services in-process (real file-backed
//! shmem, just not real separate OS processes) — one dev box shares its core
//! count across 3 nodes' worth of polling agents (consensus/sender/receiver/
//! archive ×3) plus 3 services plus the load generator, nothing like the
//! fleet's one-role-per-host budget (`m5_gate`'s precedent, restated here).
//!
//! On THIS box it is worse than the usual local-smoke disclaimer: **this box
//! runs a concurrent Veil model-checking session with no swap.** Any number
//! this harness prints locally describes contention with that neighbour, not
//! the read path. The real ladder runs on a 3-host AWS fleet — a separate,
//! user-approved step, per the design spec §3.1. Local runs exist ONLY to
//! prove the harness resolves reads, the monotonic-read guard holds, and
//! teardown is clean; their numbers must never be recorded in a doc or used to
//! evaluate the decision rule for the record.

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};

use hdrhistogram::Histogram;
use uc_consensus::election::NodeId;
use uc_log::cnc::CncPage;
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig};
use uc_service::{ServiceBuilder, ServiceConfig, StateMachine};
use uc_protocol::ring::{BroadcastConsumer, BroadcastRing, MpscRing, RingError};
use uc_protocol::v2::cnc::NODE_FLAG_CAN_SERVE;
use uc_protocol::v2::ipc::{
    FLAG_V2_IS_QUERY, FLAG_V2_LINEARIZABLE, MSG_V2_NOT_LEADER, MSG_V2_QUERY, MSG_V2_RESPONSE,
    MSG_V2_RETRY, MSG_V2_SUBMIT, client_from_extra, extra_client, write_query_payload,
};

/// Well-known file names under the instance dir — the shared contract with
/// `uc_node::InstanceDir` (`uc_node/src/ipc.rs`). Hardcoded here rather than
/// via `InstanceDir`, which requires the exclusive flock only the owning node
/// may take: this harness is an ATTACHING party, exactly like `uc_client`.
const CNC_FILE: &str = "cnc2.dat";
const QUERY_RING: &str = "query.ring";
const EGRESS_SERVICE: &str = "egress_service.0.broadcast";
const EGRESS_NODE: &str = "egress_node.broadcast";
const INGRESS_RING: &str = "ingress.ring";

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

/// Sampling period for client-side in-flight depth (spec §6, threat 3: "report
/// client-side in-flight depth and confirm the target concurrency is actually
/// sustained; a plateau caused by the harness proves nothing about the node").
const DEPTH_SAMPLE_PERIOD: Duration = Duration::from_millis(10);
/// In-flight depth samples taken inside this warm-up window are discarded: the
/// send loop starts at zero in flight and needs a moment to fill the pipeline,
/// so including the ramp would report a minimum of ~0 for every rung.
const DEPTH_WARMUP: Duration = Duration::from_millis(100);

/// An arm is DEGRADED — and therefore cannot certify a plateau — when this
/// fraction of its resolved ops came back as `MSG_V2_RETRY`/`MSG_V2_NOT_LEADER`
/// rather than as genuine answers. A degraded linearizable arm resolves few
/// reads over the full elapsed window, which looks exactly like the low ratio
/// that justifies Rung A; refusing to rule on it is what keeps the failure mode
/// and the build signal distinguishable.
const DEGRADED_FRACTION: f64 = 0.05;

/// Clause (a)'s pre-committed threshold: the linearizable plateau must be at or
/// below this percentage of the snapshot plateau. Fixed before any run existed
/// and NOT a tunable. The 65-75% borderline band around it is spelled out
/// literally at the one place it is tested.
const RATIO_THRESHOLD: f64 = 70.0;

/// Clause (b)'s concurrency floor: the client must have sustained at least this
/// fraction of the target in-flight depth, or the plateau describes the load
/// generator rather than the node (spec §6, threat 3).
///
/// Gated on the MEAN sampled depth, not the minimum: the minimum is a single
/// 10 ms sample and is dominated by scheduler noise (one descheduling of the
/// single send thread drives it to 0), whereas "sustained the target
/// concurrency" over a throughput plateau is a statement about the window. The
/// minimum is still reported alongside as context.
const SUSTAINED_FRACTION: f64 = 0.90;

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
    /// The measuring read client (bypasses uc_client — see `run_read_measurement`).
    Client(ClientArgs),
    /// Local smoke: 3 nodes + 3 services + 1 read client, in-process. NOT a fleet number.
    All(AllArgs),
    /// Local smoke: sweep the concurrency ladder across both arms and both mixes.
    Ladder(LadderArgs),
    /// Evaluate the pre-committed decision rule over rung JSON lines collected
    /// from `client` runs (the fleet path — see `run_decide`).
    Decide(DecideArgs),
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
    /// skip its threads (the client cannot see another process's threads).
    #[arg(long)]
    node_pid: Option<u32>,
    /// PID of the SERVICE process. On a fleet the service is a separate
    /// process, so without this `uc2-apply` cannot appear in the diagnostic
    /// occupancy sample at all. Diagnostic only — clause (b) no longer uses
    /// the occupancy ranking (see the module doc).
    #[arg(long)]
    service_pid: Option<u32>,
}

#[derive(clap::Args)]
struct DecideArgs {
    /// File of rung JSON lines (one per `client` run; other lines ignored).
    #[arg(long)]
    rungs: PathBuf,
    /// Which write mix to evaluate; must match the rungs' `write_rate`.
    #[arg(long, default_value_t = 0)]
    write_rate: u64,
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
        Role::Decide(a) => run_decide(a),
    }
}

/// One agent thread's yield rate over a measurement window.
///
/// **Why yields and not CPU time:** the node's agents idle on
/// `IdleStrategy::Yield` (`uc_log/src/agent.rs:28` → `std::thread::yield_now()`),
/// so an IDLE agent still burns a core in a yield loop and CPU% is saturated by
/// construction. Each empty duty cycle costs one `sched_yield`, which the kernel
/// counts in `voluntary_ctxt_switches` — so a LOW yield rate means a BUSY agent.
/// This is an ordinal signal (it ranks agents); it is not a duty-cycle percentage.
#[derive(Debug, Clone, PartialEq)]
struct Occupancy {
    /// The bare thread name (`uc2-consensus`, …) — used only to label rows of
    /// the diagnostic occupancy sample; clause (b) no longer matches against
    /// it (see the module doc).
    pub name: String,
    pub pid: u32,
    pub tid: u32,
    pub yields_per_sec: f64,
}

impl Occupancy {
    /// Human-facing row label. `name` alone is ambiguous whenever more than one
    /// instance of an agent exists in the sampled set (three nodes in one
    /// process, in `all`/`ladder`), so rows are shown as `name#tid`.
    fn label(&self) -> String {
        format!("{}#{}", self.name, self.tid)
    }
}

/// One thread's `voluntary_ctxt_switches` reading, keyed by `(pid, tid)`.
///
/// **The key is the point.** Thread names are static and repeat across agent
/// instances, so a before/after join on NAME differences unrelated threads; the
/// mis-paired rows saturate to zero and then sort to the front of the
/// ascending-by-yields ranking, impersonating the busiest agent.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ThreadSample {
    pub pid: u32,
    pub tid: u32,
    pub name: String,
    pub yields: u64,
}

/// Read `(pid, tid, name, voluntary_ctxt_switches)` for every thread under a
/// `/proc/<pid>/task` directory. Threads that vanish mid-scan (exited between
/// readdir and read) are skipped rather than failing the sample; so are
/// directory entries whose name is not a tid.
fn sample_yields(pid: u32, task_dir: &Path) -> std::io::Result<Vec<ThreadSample>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(task_dir)? {
        let path = entry?.path();
        let Some(tid) = path.file_name().and_then(|n| n.to_str()).and_then(|n| n.parse().ok())
        else {
            continue;
        };
        let Ok(comm) = std::fs::read_to_string(path.join("comm")) else { continue };
        let Ok(status) = std::fs::read_to_string(path.join("status")) else { continue };
        let yields = status
            .lines()
            .find_map(|l| l.strip_prefix("voluntary_ctxt_switches:"))
            .and_then(|v| v.trim().parse::<u64>().ok());
        let Some(yields) = yields else { continue };
        out.push(ThreadSample { pid, tid, name: comm.trim().to_string(), yields });
    }
    Ok(out)
}

/// Sample the UNION of several processes' task dirs. On a fleet the node and
/// the service are separate processes; sampling only the node's makes
/// `uc2-apply` structurally invisible to the diagnostic occupancy sample.
/// Diagnostic only — clause (b) no longer uses this ranking (see the module
/// doc). Returns `None` only when NO dir could be sampled.
fn sample_all(dirs: &[(u32, PathBuf)]) -> Option<Vec<ThreadSample>> {
    let mut out = Vec::new();
    let mut any = false;
    for (pid, dir) in dirs {
        if let Ok(mut s) = sample_yields(*pid, dir) {
            any = true;
            out.append(&mut s);
        }
    }
    any.then_some(out)
}

/// Join two samples by `(pid, tid)` and rank by yield rate ASCENDING — fewest
/// yields first, i.e. busiest agent first. Threads missing from either sample
/// are dropped (they did not exist for the whole window, so their rate is not
/// comparable).
fn occupancy_delta(before: &[ThreadSample], after: &[ThreadSample], secs: f64) -> Vec<Occupancy> {
    let mut out: Vec<Occupancy> = after
        .iter()
        .filter_map(|late| {
            let early = before.iter().find(|e| e.pid == late.pid && e.tid == late.tid)?;
            Some(Occupancy {
                name: late.name.clone(),
                pid: late.pid,
                tid: late.tid,
                yields_per_sec: late.yields.saturating_sub(early.yields) as f64 / secs,
            })
        })
        .collect();
    out.sort_by(|a, b| {
        a.yields_per_sec.total_cmp(&b.yields_per_sec).then_with(|| a.tid.cmp(&b.tid))
    });
    out
}

/// The agent rows of an occupancy ranking, busiest first: only threads whose
/// name starts with `uc2-` (the harness's own threads and the runtime's share
/// the sampled process in `all`/`ladder`).
fn agent_rows(occ: &[Occupancy]) -> Vec<&Occupancy> {
    occ.iter().filter(|o| o.name.starts_with("uc2-")).collect()
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
        purge: uc_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::default(),
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
) -> anyhow::Result<(Vec<Node>, Vec<uc_service::Service<ProfileSm>>, Vec<PathBuf>, usize)> {
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
fn stop_cluster(nodes: Vec<Node>, services: Vec<uc_service::Service<ProfileSm>>) {
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
    /// `MSG_V2_RESPONSE` records addressed to this client with
    /// `FLAG_V2_IS_QUERY` CLEAR — a submit response, not a query answer. The
    /// read client sends only queries, so this must be 0; it is counted (and
    /// deliberately NOT resolved, so it surfaces as an unresolved read) rather
    /// than being silently counted as a read.
    wrong_kind: u64,
    overwritten: u64,
    inflight_at_end: u64,
    /// Client-side in-flight depth sampled ACROSS the send window (spec §6,
    /// threat 3), not at the end: if the load generator cannot sustain the
    /// target concurrency, a plateau says nothing about the node.
    inflight_mean: f64,
    inflight_min: u64,
    inflight_samples: u64,
    /// Background-write load actually delivered, not merely requested.
    writes_attempted: u64,
    writes_accepted: u64,
    writes_dropped: u64,
    elapsed: Duration,
    reads_per_sec: f64,
    p50_ms: f64,
    p99_ms: f64,
    /// Highest counter value any read returned — fed to the Task 4 monotonic guard.
    max_read_value: u64,
    /// Largest observed regression (`prev - v` for a read that returned less
    /// than a previously-returned value). Zero means the guard never fired.
    regression: u64,
}

struct MatcherCtx {
    send_ns: Arc<Box<[AtomicU64]>>,
    owner: Arc<Box<[AtomicU64]>>,
    resolved: Arc<AtomicU64>,
    reads: Arc<AtomicU64>,
    not_leader: Arc<AtomicU64>,
    retried: Arc<AtomicU64>,
    duplicates: Arc<AtomicU64>,
    wrong_kind: Arc<AtomicU64>,
    overwritten: Arc<AtomicU64>,
    last_response_ns: Arc<AtomicU64>,
    max_read_value: Arc<AtomicU64>,
    /// Highest value returned by any read SO FAR, used to detect a REGRESSION.
    /// `ProfileSm::query` returns a monotonically non-decreasing counter, so a
    /// linearizable read that returns less than a previously-returned value is a
    /// stale answer — a harness or read-path defect either way. Snapshot reads
    /// are NOT guarded: a snapshot read is served from local applied state with
    /// no barrier, so it may legitimately regress on a follower.
    guard_monotonic: bool,
    regression: Arc<AtomicU64>,
    hist: Arc<Mutex<Histogram<u64>>>,
    client_id: u32,
    t0: Instant,
}

/// Decode a query answer's payload: `0u64 LE placeholder ++ bincode(u64)`
/// (`uc_service/src/egress.rs:62-66`). Returns None for a write response,
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
            // A genuine query answer carries FLAG_V2_IS_QUERY
            // (`uc_service/src/egress.rs:66`). This client sends nothing but
            // queries, so the check cannot fire today — having it makes "the
            // responses counted were query answers" structural rather than
            // incidental, matching `uc_client`'s own matcher kind check.
            let is_query = rec.flags & FLAG_V2_IS_QUERY != 0;
            match rec.msg_type {
                MSG_V2_RESPONSE if claimed && is_query => {
                    let now = ctx.t0.elapsed().as_nanos() as u64;
                    let send = ctx.send_ns[idx].load(Ordering::Acquire);
                    let lat = now.saturating_sub(send).min(HIST_MAX_NS);
                    let _ = ctx.hist.lock().unwrap().record(lat);
                    if let Some(v) = decode_query_answer(buf) {
                        let prev = ctx.max_read_value.fetch_max(v, Ordering::Relaxed);
                        if ctx.guard_monotonic && v < prev {
                            ctx.regression.fetch_max(prev - v, Ordering::Relaxed);
                        }
                    }
                    ctx.reads.fetch_add(1, Ordering::Relaxed);
                    ctx.resolved.fetch_add(1, Ordering::Relaxed);
                    ctx.last_response_ns.fetch_max(now, Ordering::Relaxed);
                }
                // Claimed, but not a query answer: do NOT resolve it — the read
                // then shows up as unresolved and the run fails loudly instead
                // of counting a submit response as a read.
                MSG_V2_RESPONSE if claimed => {
                    ctx.wrong_kind.fetch_add(1, Ordering::Relaxed);
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
    task_dirs: &[(u32, PathBuf)],
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
        wrong_kind: Arc::new(AtomicU64::new(0)),
        overwritten: Arc::new(AtomicU64::new(0)),
        last_response_ns: Arc::new(AtomicU64::new(0)),
        max_read_value: Arc::new(AtomicU64::new(0)),
        guard_monotonic: mode == Mode::Lin,
        regression: Arc::new(AtomicU64::new(0)),
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
    let wrong_kind = Arc::clone(&ctx.wrong_kind);
    let overwritten = Arc::clone(&ctx.overwritten);
    let last_response_ns = Arc::clone(&ctx.last_response_ns);
    let max_read_value = Arc::clone(&ctx.max_read_value);
    let regression = Arc::clone(&ctx.regression);
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

    // The writer stops with the SEND window, not with the matcher: background
    // writes running on through the drain would keep loading the node while the
    // read tail is being collected.
    let writer_stop = Arc::new(AtomicBool::new(false));
    let writer = spawn_writer(instance_dir, app_id, write_rate, Arc::clone(&writer_stop));

    // Sample agent occupancy across the measurement window only (after warm-up
    // attach, before drain) so boot-time yields do not pollute the rate.
    let occ_before = sample_all(task_dirs);
    let occ_t0 = Instant::now();

    let deadline = t0 + Duration::from_secs(secs);

    // Client-side in-flight depth sampler (spec §6, threat 3). A coarse timer
    // on its own thread: the send loop must not pay for this, and the matcher
    // already holds the histogram lock per response.
    //
    // Bounded by the send DEADLINE, not by the stop flag: a sample that lands
    // in the gap between the send loop ending and the flag being observed reads
    // the drain, and since the drain empties the pipeline it would drive the
    // reported MINIMUM to zero on every rung — destroying the one number that
    // says whether the target concurrency was sustained.
    let depth = Arc::new(DepthStats::default());
    let depth_sampler = {
        let depth = Arc::clone(&depth);
        let sent = Arc::clone(&sent);
        let resolved = Arc::clone(&resolved);
        let stop = Arc::clone(&writer_stop);
        let start = Instant::now();
        thread::Builder::new()
            .name("rp-depth".into())
            .spawn(move || {
                loop {
                    thread::sleep(DEPTH_SAMPLE_PERIOD);
                    if stop.load(Ordering::Relaxed) || Instant::now() >= deadline {
                        break;
                    }
                    if start.elapsed() < DEPTH_WARMUP {
                        continue; // pipeline still filling; the ramp is not a plateau
                    }
                    let d = sent
                        .load(Ordering::Relaxed)
                        .wrapping_sub(resolved.load(Ordering::Relaxed));
                    depth.record(d);
                }
            })
            .expect("spawn depth sampler thread")
    };

    // Send loop: keep `readers` reads in flight. `RingError::Full` means
    // yield+retry, exactly like the real uc_client.
    let mut local_seq: u32 = 0;
    // M14b: the query payload is now `service_id: u8 ++ query`; built once per
    // send into a reused scratch buffer, keeping the send loop allocation-free.
    let mut query_payload = Vec::new();
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
        write_query_payload(0, &query_bytes, &mut query_payload);
        loop {
            match query_producer.try_write(MSG_V2_QUERY, flags, extra, &query_payload) {
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
    let occ_after = sample_all(task_dirs);
    // End of the send window: stop the write load and the depth sampler here,
    // BEFORE the drain, so neither describes (or perturbs) the tail.
    writer_stop.store(true, Ordering::Relaxed);
    depth_sampler.join().expect("depth sampler thread panicked");
    let writes = match writer {
        Some(w) => {
            let counters = Arc::clone(&w.counters);
            w.handle.join().expect("writer thread panicked");
            counters.snapshot()
        }
        None => (0, 0, 0),
    };

    let drain_deadline = Instant::now() + DRAIN_GRACE;
    while resolved.load(Ordering::Relaxed) < sent.load(Ordering::Relaxed)
        && Instant::now() < drain_deadline
    {
        thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Ordering::Relaxed);
    matcher.join().expect("matcher thread panicked");

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
            wrong_kind: wrong_kind.load(Ordering::Relaxed),
            overwritten: overwritten.load(Ordering::Relaxed),
            inflight_at_end: sends.saturating_sub(resolved_n),
            inflight_mean: depth.mean(),
            inflight_min: depth.min(),
            inflight_samples: depth.samples(),
            writes_attempted: writes.0,
            writes_accepted: writes.1,
            writes_dropped: writes.2,
            elapsed,
            reads_per_sec,
            p50_ms,
            p99_ms,
            max_read_value: max_read_value.load(Ordering::Relaxed),
            regression: regression.load(Ordering::Relaxed),
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

/// Client-side in-flight depth, sampled across the send window (spec §6,
/// threat 3). Mean AND minimum: a mean near the target hides a stall, and the
/// question the threat asks is whether the target concurrency was *sustained*.
#[derive(Debug)]
struct DepthStats {
    sum: AtomicU64,
    samples: AtomicU64,
    min: AtomicU64,
}

impl Default for DepthStats {
    fn default() -> Self {
        Self { sum: AtomicU64::new(0), samples: AtomicU64::new(0), min: AtomicU64::new(u64::MAX) }
    }
}

impl DepthStats {
    fn record(&self, depth: u64) {
        self.sum.fetch_add(depth, Ordering::Relaxed);
        self.samples.fetch_add(1, Ordering::Relaxed);
        self.min.fetch_min(depth, Ordering::Relaxed);
    }
    fn samples(&self) -> u64 {
        self.samples.load(Ordering::Relaxed)
    }
    fn mean(&self) -> f64 {
        let n = self.samples();
        if n == 0 { 0.0 } else { self.sum.load(Ordering::Relaxed) as f64 / n as f64 }
    }
    fn min(&self) -> u64 {
        match self.min.load(Ordering::Relaxed) {
            u64::MAX => 0,
            v => v,
        }
    }
}

/// Attempted / accepted / dropped-full counts for the background writer.
///
/// **Why this exists:** the writer discards on `RingError::Full` and its pacing
/// clock advances regardless, so without counters a mixed arm whose writes were
/// all dropped IS the read-only arm — while the report prints the REQUESTED
/// rate in the same shape as a measured quantity, and the A/B delta gets
/// attributed to the frontier wait that never happened.
#[derive(Debug, Default)]
struct WriterCounters {
    attempted: AtomicU64,
    accepted: AtomicU64,
    dropped: AtomicU64,
}

impl WriterCounters {
    /// Fold one `try_write` result into the counters. `Full` is a DROP (the
    /// write never reached the node); any other error is returned to the caller
    /// to fail the run.
    fn record(&self, res: Result<(), RingError>) -> Result<(), RingError> {
        self.attempted.fetch_add(1, Ordering::Relaxed);
        match res {
            Ok(()) => {
                self.accepted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(RingError::Full) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// `(attempted, accepted, dropped)`.
    fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.attempted.load(Ordering::Relaxed),
            self.accepted.load(Ordering::Relaxed),
            self.dropped.load(Ordering::Relaxed),
        )
    }
}

struct WriterHandle {
    handle: thread::JoinHandle<()>,
    counters: Arc<WriterCounters>,
}

/// Background write load for the mixed arm: a paced submitter on its OWN
/// `client_id`, so its responses are filtered out of the read matcher at the
/// `cid != ctx.client_id` check and cannot inflate the read rate.
///
/// Returns `None` when `write_rate == 0` (the read-only arm), which is what
/// makes that arm the clean isolation: with no writes in flight,
/// `service_applied >= commit_at` already holds when a read is admitted, so the
/// frontier wait is free and the A/B delta is the barrier alone.
///
/// The delivered rate is COUNTED, not assumed — see [`WriterCounters`].
fn spawn_writer(
    instance_dir: &Path,
    app_id: &str,
    write_rate: u64,
    stop: Arc<AtomicBool>,
) -> Option<WriterHandle> {
    if write_rate == 0 {
        return None;
    }
    let dir = instance_dir.to_path_buf();
    let app_id = app_id.to_string();
    let counters = Arc::new(WriterCounters::default());
    let thread_counters = Arc::clone(&counters);
    let handle = thread::Builder::new()
        .name("rp-writer".into())
        .spawn(move || {
            let cnc = CncPage::open_file(&dir.join(CNC_FILE), &app_id)
                .unwrap_or_else(|e| panic!("writer cnc attach: {e}"));
            let client_id = cnc.status().next_client_id.fetch_add(1) as u32;
            let (ingress, _c) = MpscRing::open(&dir.join(INGRESS_RING))
                .unwrap_or_else(|e| panic!("writer open ingress.ring: {e}"))
                .into_split();
            let payload =
                bincode::serde::encode_to_vec(vec![0xABu8; 64], bincode::config::standard())
                    .expect("encode write payload");
            let period = Duration::from_nanos(1_000_000_000 / write_rate.max(1));
            let mut local_seq: u32 = 0;
            let mut next = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                let now = Instant::now();
                if now < next {
                    thread::sleep((next - now).min(Duration::from_millis(1)));
                    continue;
                }
                next += period;
                if cnc.status().flags.load_acquire() & NODE_FLAG_CAN_SERVE == 0 {
                    continue;
                }
                let extra = extra_client(client_id, local_seq);
                let res = ingress.try_write(MSG_V2_SUBMIT, 0, extra, &payload);
                if let Err(e) = thread_counters.record(res) {
                    panic!("writer ingress.ring error: {e}");
                }
                local_seq = local_seq.wrapping_add(1);
            }
        })
        .expect("spawn writer thread");
    Some(WriterHandle { handle, counters })
}

fn print_read_report(mode: Mode, readers: u64, write_rate: u64, s: &ReadStats, occ: &[Occupancy]) {
    let arm = match mode {
        Mode::Lin => "linearizable (probe barrier)",
        Mode::Snap => "snapshot (no barrier)",
    };
    println!();
    println!("========== uc2 read profile: {arm} ==========");
    println!("readers (in-flight)   : {readers}");
    println!(
        "background writes/s   : target {write_rate}, accepted {}, dropped {} (of {} attempts)",
        s.writes_accepted, s.writes_dropped, s.writes_attempted
    );
    println!("reads resolved        : {}", s.reads);
    println!("retries               : {}", s.retried);
    println!("not_leader redirects  : {}", s.not_leader);
    println!(
        "degraded fraction     : {:.1}%  [>{:.0}% = arm degraded, no verdict]",
        degraded_fraction(s.reads, s.retried, s.not_leader) * 100.0,
        DEGRADED_FRACTION * 100.0
    );
    println!("dup answers dropped   : {}", s.duplicates);
    println!("wrong-kind answers    : {}", s.wrong_kind);
    println!("broadcast overwritten : {}", s.overwritten);
    println!("in-flight at end      : {}", s.inflight_at_end);
    println!(
        "in-flight depth       : mean {:.1}, min {} (target {readers}, {} samples)",
        s.inflight_mean, s.inflight_min, s.inflight_samples
    );
    println!("read regression       : {}", s.regression);
    println!("max read value        : {}", s.max_read_value);
    println!("elapsed (drain-incl.) : {:.3} s", s.elapsed.as_secs_f64());
    println!("reads/s               : {:.0}", s.reads_per_sec);
    println!("p50                   : {:.3} ms", s.p50_ms);
    println!("p99                   : {:.3} ms", s.p99_ms);
    if occ.is_empty() {
        println!("agent occupancy       : (not sampled — pass --node-pid and --service-pid)");
    } else {
        println!(
            "agent yield rates (DIAGNOSTIC ONLY — does NOT feed the decision rule; a \
             yield-idling agent is indistinguishable from a busy one, see below):"
        );
        for o in occ {
            println!("    pid {:<8} {:<24} {:>12.0} yields/s", o.pid, o.label(), o.yields_per_sec);
        }
        println!(
            "    ^ sched_yield does NOT increment voluntary_ctxt_switches (measured: 2,000,000 \
             yields -> +0). Near-zero rows are the expected reading, not a bug, and rank \
             nothing. Clause (b) is evaluated from the read-only arm instead (spec §2, §4.3)."
        );
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
        &self_task_dirs(),
    );
    print_read_report(a.mode, readers, a.write_rate, &stats, &occ);
    stop_cluster(nodes, services);
    anyhow::ensure!(
        stats.regression == 0,
        "LINEARIZABLE READ REGRESSED by {} — a read returned a value lower than one \
         already returned. Either the harness is mis-wired or the read path is serving \
         stale state; the throughput numbers above are meaningless either way.",
        stats.regression
    );
    anyhow::ensure!(stats.inflight_at_end == 0, "{} reads never resolved", stats.inflight_at_end);
    Ok(())
}

/// `all`/`ladder` run every role inside ONE process, so its own task dir holds
/// all three nodes' agents AND all three services' apply threads. Correctly
/// keyed by `(pid, tid)`, that is exactly the union the fleet builds from two
/// PIDs.
fn self_task_dirs() -> Vec<(u32, PathBuf)> {
    vec![(std::process::id(), PathBuf::from("/proc/self/task"))]
}

fn run_client_role(a: ClientArgs) -> anyhow::Result<()> {
    let secs = env_cap("UC2_RP_MAX_SECS", a.secs);
    let readers = env_cap("UC2_RP_MAX_READERS", a.readers);
    // The union of node + service task dirs, for the DIAGNOSTIC occupancy
    // sample only (does not feed the decision rule). Without the service's,
    // `uc2-apply` is not in the sampled set at all on a fleet, where the
    // service is its own process.
    let task_dirs: Vec<(u32, PathBuf)> = [a.node_pid, a.service_pid]
        .into_iter()
        .flatten()
        .map(|p| (p, PathBuf::from(format!("/proc/{p}/task"))))
        .collect();
    if task_dirs.is_empty() {
        eprintln!(
            "NOTE: neither --node-pid nor --service-pid given — the agent-occupancy \
             diagnostic sample will be empty. This does not affect the decision rule: \
             clause (b) no longer uses the occupancy ranking (see the module doc)."
        );
    } else if a.service_pid.is_none() {
        eprintln!(
            "NOTE: --service-pid not given — uc2-apply is invisible to the occupancy \
             diagnostic sample. This does not affect the decision rule: clause (b) no \
             longer uses the occupancy ranking (see the module doc)."
        );
    }
    let (stats, occ) = run_read_measurement(
        &a.instance_dir,
        &a.app_id,
        secs,
        readers,
        a.mode,
        a.write_rate,
        &task_dirs,
    );
    print_read_report(a.mode, readers, a.write_rate, &stats, &occ);
    anyhow::ensure!(
        stats.regression == 0,
        "LINEARIZABLE READ REGRESSED by {} — a read returned a value lower than one \
         already returned. Either the harness is mis-wired or the read path is serving \
         stale state; the throughput numbers above are meaningless either way.",
        stats.regression
    );
    anyhow::ensure!(
        stats.inflight_at_end == 0,
        "{} reads never resolved — the run did not complete; its numbers describe nothing",
        stats.inflight_at_end
    );
    // Emit the machine-readable rung JSON only AFTER both validity checks
    // above pass. Spec §6 threat 6: a rung with inflight_at_end != 0 (or a
    // regression) is INVALID, not merely suspicious — its numbers describe
    // nothing. An orchestrator that tees stdout without checking exit codes
    // must never see an invalid rung's JSON on the machine-readable line. Do
    // not hoist this back above the ensures.
    println!("{}", rung_to_json(&Rung::from_stats(readers, a.mode, a.write_rate, &stats, &occ)));
    Ok(())
}

/// One agent's place in a rung's DIAGNOSTIC occupancy ranking (does not feed
/// the decision rule — see the module doc). `name` is the bare thread name;
/// `label` is `name#tid` (what a human needs to tell three `uc2-consensus`
/// threads apart).
#[derive(Debug, Clone, PartialEq)]
struct AgentRank {
    pub name: String,
    pub label: String,
    pub yields_per_sec: f64,
}

/// One measured point: an arm at one concurrency under one write mix.
///
/// This carries the health of the run, not just its rate. A rung that collapsed
/// into retries, or one whose load generator never sustained the target depth,
/// produces a rate that means nothing — and the decision rule has to be able to
/// see that rather than reading it as a favourable number.
#[derive(Debug, Clone)]
struct Rung {
    pub readers: u64,
    pub mode: Mode,
    pub write_rate: u64,
    pub reads: u64,
    pub reads_per_sec: f64,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub retried: u64,
    pub not_leader: u64,
    pub overwritten: u64,
    pub inflight_at_end: u64,
    pub inflight_mean: f64,
    pub inflight_min: u64,
    pub regression: u64,
    /// Occupancy ranking, busiest (fewest yields) first, agent threads only.
    pub agents: Vec<AgentRank>,
}

impl Rung {
    fn from_stats(
        readers: u64,
        mode: Mode,
        write_rate: u64,
        s: &ReadStats,
        occ: &[Occupancy],
    ) -> Rung {
        Rung {
            readers,
            mode,
            write_rate,
            reads: s.reads,
            reads_per_sec: s.reads_per_sec,
            p50_ms: s.p50_ms,
            p99_ms: s.p99_ms,
            retried: s.retried,
            not_leader: s.not_leader,
            overwritten: s.overwritten,
            inflight_at_end: s.inflight_at_end,
            inflight_mean: s.inflight_mean,
            inflight_min: s.inflight_min,
            regression: s.regression,
            agents: agent_rows(occ)
                .into_iter()
                .map(|o| AgentRank {
                    name: o.name.clone(),
                    label: o.label(),
                    yields_per_sec: o.yields_per_sec,
                })
                .collect(),
        }
    }

    fn mode_str(&self) -> &'static str {
        match self.mode {
            Mode::Lin => "lin",
            Mode::Snap => "snap",
        }
    }

    fn arm_name(&self) -> &'static str {
        match self.mode {
            Mode::Lin => "linearizable",
            Mode::Snap => "snapshot",
        }
    }

    /// Share of resolved ops that came back as a retry or a redirect rather
    /// than as a genuine answer. See [`DEGRADED_FRACTION`].
    fn degraded_fraction(&self) -> f64 {
        degraded_fraction(self.reads, self.retried, self.not_leader)
    }

    fn is_degraded(&self) -> bool {
        self.degraded_fraction() > DEGRADED_FRACTION
    }

    /// Mean sustained in-flight depth as a fraction of the target concurrency.
    /// See [`SUSTAINED_FRACTION`] for why the mean and not the minimum.
    fn sustained_fraction(&self) -> f64 {
        if self.readers == 0 { 0.0 } else { self.inflight_mean / self.readers as f64 }
    }
}

/// `(retried + not_leader) / (reads + retried + not_leader)`; 0 when nothing
/// resolved at all (an empty run is caught by the zero-rate check instead).
fn degraded_fraction(reads: u64, retried: u64, not_leader: u64) -> f64 {
    let denom = reads + retried + not_leader;
    if denom == 0 { 0.0 } else { (retried + not_leader) as f64 / denom as f64 }
}

/// Serialize a rung as one JSON object on one line.
///
/// Hand-rolled on purpose: this example must not add a dependency, and the
/// format only has to survive the round trip to [`rung_from_json`] that
/// `decide` performs. That round trip is what makes the pre-committed rule
/// reachable from the FLEET path — where each rung is a separate `client`
/// process under external orchestration — instead of being re-implemented in
/// the orchestrator, which would defeat the point of pinning it in tested code.
fn rung_to_json(r: &Rung) -> String {
    let agents = r
        .agents
        .iter()
        .map(|a| {
            format!(
                "{{\"name\":\"{}\",\"label\":\"{}\",\"yields_per_sec\":{}}}",
                json_escape(&a.name),
                json_escape(&a.label),
                a.yields_per_sec
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"rung\":1,\"mode\":\"{}\",\"readers\":{},\"write_rate\":{},\"reads\":{},\
         \"reads_per_sec\":{},\"p50_ms\":{},\"p99_ms\":{},\"retried\":{},\"not_leader\":{},\
         \"overwritten\":{},\"inflight_at_end\":{},\"inflight_mean\":{},\"inflight_min\":{},\
         \"regression\":{},\"agents\":[{}]}}",
        r.mode_str(),
        r.readers,
        r.write_rate,
        r.reads,
        r.reads_per_sec,
        r.p50_ms,
        r.p99_ms,
        r.retried,
        r.not_leader,
        r.overwritten,
        r.inflight_at_end,
        r.inflight_mean,
        r.inflight_min,
        r.regression,
        agents
    )
}

/// Minimal escaping for the only strings this emits — thread names out of
/// `/proc/<pid>/task/<tid>/comm`.
fn json_escape(s: &str) -> String {
    s.chars()
        .filter(|c| !c.is_control())
        .flat_map(|c| match c {
            '"' | '\\' => vec!['\\', c],
            c => vec![c],
        })
        .collect()
}

/// Pull `"<key>":<scalar>` out of a flat JSON object slice. Deliberately
/// literal-minded: the only producer is [`rung_to_json`].
fn json_field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let pat = format!("\"{key}\":");
    let start = line.find(&pat)? + pat.len();
    let rest = &line[start..];
    let end = rest.find([',', '}', ']']).unwrap_or(rest.len());
    Some(rest[..end].trim())
}

fn json_str<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let pat = format!("\"{key}\":\"");
    let start = line.find(&pat)? + pat.len();
    let rest = &line[start..];
    let end = rest.find('"')?;
    Some(&rest[..end])
}

fn json_u64(line: &str, key: &str) -> Option<u64> {
    json_field(line, key)?.parse().ok()
}

fn json_f64(line: &str, key: &str) -> Option<f64> {
    json_field(line, key)?.parse().ok()
}

/// Parse one rung JSON line. Returns `None` for anything that is not one (blank
/// lines, the human report the same stdout carries), so an orchestrator can tee
/// a whole `client` run into the rung file without filtering it first.
fn rung_from_json(line: &str) -> Option<Rung> {
    let line = line.trim();
    if !line.starts_with('{') || !line.contains("\"rung\":") {
        return None;
    }
    let (agents_blob, head) = match line.find("\"agents\":[") {
        Some(i) => {
            let rest = &line[i + "\"agents\":[".len()..];
            let end = rest.find(']')?;
            (&rest[..end], &line[..i])
        }
        None => ("", line),
    };
    let agents = agents_blob
        .split("},{")
        .filter(|s| s.contains("\"name\":"))
        .filter_map(|s| {
            Some(AgentRank {
                name: json_str(s, "name")?.to_string(),
                label: json_str(s, "label").unwrap_or_default().to_string(),
                yields_per_sec: json_f64(s, "yields_per_sec")?,
            })
        })
        .collect();
    let mode = match json_str(head, "mode")? {
        "lin" => Mode::Lin,
        "snap" => Mode::Snap,
        _ => return None,
    };
    Some(Rung {
        readers: json_u64(head, "readers")?,
        mode,
        write_rate: json_u64(head, "write_rate")?,
        reads: json_u64(head, "reads")?,
        reads_per_sec: json_f64(head, "reads_per_sec")?,
        p50_ms: json_f64(head, "p50_ms").unwrap_or(0.0),
        p99_ms: json_f64(head, "p99_ms").unwrap_or(0.0),
        retried: json_u64(head, "retried").unwrap_or(0),
        not_leader: json_u64(head, "not_leader").unwrap_or(0),
        overwritten: json_u64(head, "overwritten").unwrap_or(0),
        inflight_at_end: json_u64(head, "inflight_at_end").unwrap_or(0),
        inflight_mean: json_f64(head, "inflight_mean").unwrap_or(0.0),
        inflight_min: json_u64(head, "inflight_min").unwrap_or(0),
        regression: json_u64(head, "regression").unwrap_or(0),
        agents,
    })
}

/// Which sub-condition of clause (b) failed, if any.
#[derive(Debug, Clone, PartialEq)]
enum ClauseB {
    Met,
    /// The read-only arm has no rungs to evaluate.
    NoReadOnlyData,
    /// The gap is NOT present in the read-only arm — so it is frontier-wait
    /// cost, not barrier cost.
    NoReadOnlyGap { ratio: f64 },
    /// The client did not sustain the target concurrency, so the plateau
    /// describes the harness rather than the node.
    ConcurrencyNotSustained { arm: &'static str, mean: f64, target: u64, pct: f64 },
    /// A read-only arm collapsed into retries/redirects.
    ReadOnlyArmDegraded { arm: &'static str, pct: f64 },
}

/// Evaluate clause (b) over the READ-ONLY rungs, whatever mix is being reported.
///
/// **Why the read-only arm is the substantive test.** With no writes in flight,
/// `service_applied >= commit_at` already holds when a read is admitted, so the
/// frontier wait is free and the lin-vs-snap delta there is the barrier ALONE.
/// A gap that shows up only in the mixed arm is frontier-wait cost — the exact
/// misattribution clause (b) exists to catch. The concurrency floor uses the
/// depth sampling from spec §6 threat 3: if the client was the ceiling, the
/// plateau says nothing about the node.
fn evaluate_clause_b(rungs: &[Rung]) -> ClauseB {
    let plateau = |mode: Mode| -> Option<&Rung> {
        rungs
            .iter()
            .filter(|r| r.mode == mode && r.write_rate == 0)
            .max_by(|a, b| a.reads_per_sec.total_cmp(&b.reads_per_sec))
    };
    let (Some(lin), Some(snap)) = (plateau(Mode::Lin), plateau(Mode::Snap)) else {
        return ClauseB::NoReadOnlyData;
    };
    if snap.reads_per_sec <= 0.0 {
        return ClauseB::NoReadOnlyData;
    }
    for r in [lin, snap] {
        if r.is_degraded() {
            return ClauseB::ReadOnlyArmDegraded {
                arm: r.arm_name(),
                pct: r.degraded_fraction() * 100.0,
            };
        }
    }
    let ratio = lin.reads_per_sec / snap.reads_per_sec * 100.0;
    if ratio > RATIO_THRESHOLD {
        return ClauseB::NoReadOnlyGap { ratio };
    }
    // Both arms: a client-limited SNAPSHOT arm corrupts the ratio just as badly
    // as a client-limited linearizable one.
    for r in [lin, snap] {
        let pct = r.sustained_fraction() * 100.0;
        if r.sustained_fraction() < SUSTAINED_FRACTION {
            return ClauseB::ConcurrencyNotSustained {
                arm: r.arm_name(),
                mean: r.inflight_mean,
                target: r.readers,
                pct,
            };
        }
    }
    ClauseB::Met
}

/// The decision rule from the spec (§2), evaluated verbatim and never tuned:
///
///   Build Rung A iff (a) the linearizable plateau is <=RATIO_THRESHOLD% of the
///   snapshot plateau at the reported mix, AND (b) that gap is also present in
///   the READ-ONLY arm, the client sustained >=90% of target concurrency there,
///   and neither read-only arm is degraded.
///   Borderline 65-75% => NOT justified without a fleet run.
///
/// Clause (b) was AMENDED on 2026-07-25, **before any measurement data existed**
/// (see the spec's dated amendment note): the original clause asked which agent
/// was top-occupancy, which the yield-rate proxy turned out to be incapable of
/// answering — a yield-idling agent is indistinguishable from a busy one at the
/// OS level. The amended clause discharges the same job (rule out that something
/// other than the barrier explains the gap) from data the harness already
/// collects. Clause (a)'s threshold, the borderline band, the
/// borderline-before-justified ordering, and the degraded guard are unchanged.
///
/// Plateau = the best rate that arm reached across the ladder (the ladder's
/// point is to climb until the rate stops improving, so the max IS the plateau).
fn evaluate_decision_rule(rungs: &[Rung], write_rate: u64) -> String {
    let plateau = |mode: Mode| -> Option<&Rung> {
        rungs
            .iter()
            .filter(|r| r.mode == mode && r.write_rate == write_rate)
            .max_by(|a, b| a.reads_per_sec.total_cmp(&b.reads_per_sec))
    };
    let (Some(lin), Some(snap)) = (plateau(Mode::Lin), plateau(Mode::Snap)) else {
        return "VERDICT: INCONCLUSIVE — both arms must have at least one rung.".into();
    };
    if snap.reads_per_sec <= 0.0 {
        return "VERDICT: INCONCLUSIVE — the snapshot arm measured zero reads/s.".into();
    }
    let ratio = lin.reads_per_sec / snap.reads_per_sec * 100.0;
    // Clause (b) is always evaluated over the READ-ONLY rungs, even when the
    // mixed arm is the one being reported.
    let clause_b = evaluate_clause_b(rungs);

    let mut out = String::new();
    out.push_str(&format!(
        "  linearizable plateau : {:>12.0} reads/s (at {} readers, p50={:.3}ms p99={:.3}ms)\n",
        lin.reads_per_sec, lin.readers, lin.p50_ms, lin.p99_ms
    ));
    out.push_str(&format!(
        "  snapshot plateau     : {:>12.0} reads/s (at {} readers, p50={:.3}ms p99={:.3}ms)\n",
        snap.reads_per_sec, snap.readers, snap.p50_ms, snap.p99_ms
    ));
    out.push_str(&format!(
        "  ratio (lin/snap)     : {ratio:.1}%  [clause (a): <={RATIO_THRESHOLD:.0}% and not \
         65-75%]\n"
    ));
    // Short tag here; the verdict line below carries the full explanation.
    out.push_str(&format!(
        "  clause (b)           : {}  [read-only gap + >={:.0}% sustained concurrency + \
         no degraded arm]\n",
        clause_b_tag(&clause_b),
        SUSTAINED_FRACTION * 100.0
    ));
    for r in [lin, snap] {
        out.push_str(&format!(
            "  {:<20} : {} reads, {} retried, {} not_leader ({:.1}% degraded), \
             in-flight mean {:.1} / min {} vs target {}\n",
            format!("{} arm health", r.arm_name()),
            r.reads,
            r.retried,
            r.not_leader,
            r.degraded_fraction() * 100.0,
            r.inflight_mean,
            r.inflight_min,
            r.readers
        ));
    }

    // Both arms cross the same per-cycle query drain, so equal plateaus point at
    // the drain cap — or at the harness — rather than at the barrier
    // (spec §6, threats 2 and 3).
    if (ratio - 100.0).abs() < 2.0 {
        out.push_str(
            "  NOTE: the two arms plateau within 2% of each other — suspect \
             QUERY_DRAIN_PER_CYCLE (node.rs:186) as the ceiling, or the LOAD GENERATOR \
             itself (single send thread, single matcher thread taking the histogram lock \
             per response): if both arms hit the CLIENT's ceiling the ratio goes to 100% \
             and the barrier reads as free. Check the in-flight depth lines above against \
             the target before believing either.\n",
        );
    }
    if ratio > 100.0 {
        out.push_str(
            "  NOTE: the linearizable arm OUT-RAN the snapshot arm (ratio > 100%). The \
             barrier cannot make reads faster, so this rung pair is measuring something \
             else — a stalled or query-losing snapshot arm, or run-to-run noise — and must \
             be re-examined rather than read as 'the barrier is free'.\n",
        );
    }

    let verdict = if lin.is_degraded() || snap.is_degraded() {
        // A degraded linearizable arm resolves few genuine reads over the full
        // elapsed window: a LOW ratio, which is exactly the shape of the result
        // that justifies building. Refuse to rule rather than certify it.
        let bad = if lin.is_degraded() { lin } else { snap };
        format!(
            "VERDICT: INCONCLUSIVE — {} arm degraded ({:.1}% retries/redirects)",
            bad.arm_name(),
            bad.degraded_fraction() * 100.0
        )
    } else if (65.0..=75.0).contains(&ratio) {
        format!(
            "VERDICT: BORDERLINE ({ratio:.1}% is inside the 65-75% band) — \
             NOT justified on this data; resolve with a fleet run or decline."
        )
    } else if ratio <= RATIO_THRESHOLD && clause_b == ClauseB::Met {
        "VERDICT: Rung A JUSTIFIED — both clauses met.".to_string()
    } else if ratio > RATIO_THRESHOLD {
        format!(
            "VERDICT: Rung A NOT JUSTIFIED — clause (a) unmet: the barrier costs \
             at most {:.1}% of read capacity.",
            (100.0 - ratio).max(0.0)
        )
    } else {
        format!(
            "VERDICT: Rung A NOT JUSTIFIED — clause (b) unmet: {}.",
            clause_b_reason(&clause_b)
        )
    };
    out.push_str(&verdict);
    out
}

/// Short tag for the summary line.
fn clause_b_tag(c: &ClauseB) -> &'static str {
    match c {
        ClauseB::Met => "met",
        ClauseB::NoReadOnlyData => "UNMET (no read-only rungs)",
        ClauseB::NoReadOnlyGap { .. } => "UNMET (no gap in the read-only arm)",
        ClauseB::ConcurrencyNotSustained { .. } => "UNMET (concurrency not sustained)",
        ClauseB::ReadOnlyArmDegraded { .. } => "UNMET (read-only arm degraded)",
    }
}

/// Spell out WHICH sub-condition of clause (b) failed — a bare "unmet" is
/// useless in a report.
fn clause_b_reason(c: &ClauseB) -> String {
    match c {
        ClauseB::Met => "met".to_string(),
        ClauseB::NoReadOnlyData => "the read-only arm (write_rate=0) has no usable rungs, so \
             the barrier's cost cannot be isolated from the frontier wait"
            .to_string(),
        ClauseB::NoReadOnlyGap { ratio } => format!(
            "the gap is ABSENT in the read-only arm (lin/snap = {ratio:.1}% there, above the \
             {RATIO_THRESHOLD:.0}% threshold). With no writes in flight the frontier wait is \
             free, so a gap that appears only under write load is frontier-wait cost, not \
             barrier cost — removing probe traffic would not move it"
        ),
        // One decimal: at {:.0} a 89.4% shortfall prints as "90%, floor 90%",
        // which reads as a contradiction of the verdict it is explaining.
        ClauseB::ConcurrencyNotSustained { arm, mean, target, pct } => format!(
            "the client did not sustain the target concurrency in the read-only {arm} arm \
             (mean in-flight depth {mean:.1} of target {target} = {pct:.1}%, floor {:.1}%). \
             The plateau describes the LOAD GENERATOR, not the node",
            SUSTAINED_FRACTION * 100.0
        ),
        ClauseB::ReadOnlyArmDegraded { arm, pct } => format!(
            "the read-only {arm} arm is degraded ({pct:.1}% retries/redirects), so its ratio \
             is not a measurement"
        ),
    }
}

fn run_ladder(a: LadderArgs) -> anyhow::Result<()> {
    let root = a.root.unwrap_or_else(|| PathBuf::from("target/read_profile_ladder"));
    let secs = env_cap("UC2_RP_MAX_SECS", a.secs);
    println!("*** LOCAL SMOKE — NOT a fleet number *** (3 nodes + 3 services on one box)");

    let mut rungs: Vec<Rung> = Vec::new();
    for &write_rate in &[0u64, a.write_rate] {
        for mode in [Mode::Snap, Mode::Lin] {
            for &readers in &a.readers {
                let readers = env_cap("UC2_RP_MAX_READERS", readers);
                // A fresh cluster per rung: a rung must not inherit the previous
                // rung's warm caches, log-buffer fill, or leader.
                let (nodes, services, dirs, leader) = boot_cluster(&root, ALL_APP_ID)?;
                let (stats, occ) = run_read_measurement(
                    &dirs[leader],
                    ALL_APP_ID,
                    secs,
                    readers,
                    mode,
                    write_rate,
                    &self_task_dirs(),
                );
                stop_cluster(nodes, services);
                anyhow::ensure!(
                    stats.regression == 0,
                    "linearizable read regressed by {} at readers={readers}",
                    stats.regression
                );
                if stats.inflight_at_end != 0 {
                    println!(
                        "  WARNING: {} reads unresolved at readers={readers} \
                         (rung recorded, treat with suspicion)",
                        stats.inflight_at_end
                    );
                }
                let r = Rung::from_stats(readers, mode, write_rate, &stats, &occ);
                // Health goes on the rung line, not just in the report: a rung
                // that collapsed into retries has a LOW rate, which is the same
                // shape as the result that justifies building Rung A.
                println!(
                    "  rung: mode={:<5} readers={readers:<5} writes/s={write_rate:<7} \
                     reads/s={:>10.0}  p50={:.3}ms  retried={} not_leader={} \
                     ({:.1}% degraded) overwritten={} inflight_end={} depth(mean/min)={:.1}/{} \
                     top(diag)={}",
                    r.mode_str(),
                    r.reads_per_sec,
                    r.p50_ms,
                    r.retried,
                    r.not_leader,
                    r.degraded_fraction() * 100.0,
                    r.overwritten,
                    r.inflight_at_end,
                    r.inflight_mean,
                    r.inflight_min,
                    r.agents.first().map(|a| a.label.clone()).unwrap_or_else(|| "-".into())
                );
                rungs.push(r);
            }
        }
        if write_rate == a.write_rate && a.write_rate == 0 {
            break; // read-only and mixed are the same sweep; do not run it twice
        }
    }

    println!();
    println!("================== decision rule (spec §2) ==================");
    for &write_rate in &[0u64, a.write_rate] {
        let mix = if write_rate == 0 { "read-only arm" } else { "mixed arm" };
        println!("-- {mix} (writes/s = {write_rate}) --");
        println!("{}", evaluate_decision_rule(&rungs, write_rate));
        if write_rate == a.write_rate && a.write_rate == 0 {
            break;
        }
    }
    println!("============================================================");
    println!(
        "REMINDER: local smoke. The read-only arm is the clean isolation; the mixed arm's \
         delta includes the frontier wait, since a snapshot read skips that too."
    );
    Ok(())
}

/// Evaluate the pre-committed rule over rung records collected from `client`
/// runs — the FLEET path. Each fleet rung is a separate `client` process under
/// external orchestration, so without this the orchestrator would have to
/// re-implement the rule, and a rule re-implemented outside its unit tests is
/// no longer a pre-commitment.
fn run_decide(a: DecideArgs) -> anyhow::Result<()> {
    let text = std::fs::read_to_string(&a.rungs)
        .map_err(|e| anyhow::anyhow!("read {:?}: {e}", a.rungs))?;
    let rungs: Vec<Rung> = text.lines().filter_map(rung_from_json).collect();
    anyhow::ensure!(!rungs.is_empty(), "no rung records found in {:?}", a.rungs);
    println!("rungs parsed: {} (write_rate filter: {})", rungs.len(), a.write_rate);
    for r in &rungs {
        println!(
            "  {:<4} readers={:<6} writes/s={:<7} reads/s={:>10.0} p50={:.3}ms \
             ({:.1}% degraded) inflight_end={} depth(mean/min)={:.1}/{}",
            r.mode_str(),
            r.readers,
            r.write_rate,
            r.reads_per_sec,
            r.p50_ms,
            r.degraded_fraction() * 100.0,
            r.inflight_at_end,
            r.inflight_mean,
            r.inflight_min
        );
        if r.regression != 0 {
            println!("    WARNING: this rung recorded a read regression of {}", r.regression);
        }
    }
    println!();
    println!("================== decision rule (spec §2) ==================");
    println!("{}", evaluate_decision_rule(&rungs, a.write_rate));
    println!("============================================================");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Scratch root for test artifacts: the workspace `target/` directory, on
    /// real ext4. NEVER `/tmp` — RAM-backed tmpfs with no swap on the dev box.
    fn scratch_root() -> PathBuf {
        let root = std::env::var("CARGO_TARGET_TMPDIR").map(PathBuf::from).unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/read_profile_tests")
        });
        fs::create_dir_all(&root).expect("create scratch root");
        assert!(!root.starts_with("/tmp"), "test scratch must not live on tmpfs: {root:?}");
        root
    }

    /// Build a synthetic `/proc/<pid>/task` tree: one dir per thread, each
    /// holding a `comm` and a `status` file in the kernel's format. Threads are
    /// numbered from 1000, so tids are `1000 + index`.
    ///
    /// `tempdir_in(<target dir>)` rather than `tempdir()`: `/tmp` on the dev box
    /// is RAM-backed tmpfs with no swap (CLAUDE.md), which is also why
    /// `run_node`/`boot_cluster` refuse a `/tmp` instance dir. Writing test
    /// scratch there would contradict the assertion this file makes. (Examples
    /// do not get `CARGO_TARGET_TMPDIR` — that is integration-test-only — so
    /// [`scratch_root`] derives the same ext4 location from the manifest dir.)
    fn fake_task_dir(threads: &[(&str, u64)]) -> tempfile::TempDir {
        let dir = tempfile::tempdir_in(scratch_root()).expect("tempdir");
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

    fn sample(pid: u32, tid: u32, name: &str, yields: u64) -> ThreadSample {
        ThreadSample { pid, tid, name: name.to_string(), yields }
    }

    #[test]
    fn samples_pid_tid_name_and_yield_count_per_thread() {
        let dir = fake_task_dir(&[("uc2-consensus", 100), ("uc2-sender", 250)]);
        let mut got = sample_yields(7, dir.path()).expect("sample");
        got.sort();
        assert_eq!(
            got,
            vec![sample(7, 1000, "uc2-consensus", 100), sample(7, 1001, "uc2-sender", 250)]
        );
    }

    #[test]
    fn skips_threads_missing_files_rather_than_failing() {
        let dir = fake_task_dir(&[("uc2-consensus", 100)]);
        // A thread that exited between readdir and read: dir exists, files don't.
        fs::create_dir(dir.path().join("2000")).unwrap();
        let got = sample_yields(7, dir.path()).expect("sample");
        assert_eq!(got, vec![sample(7, 1000, "uc2-consensus", 100)]);
    }

    #[test]
    fn sample_all_unions_task_dirs_and_keeps_pids_distinct() {
        // The fleet shape: node process and service process, sampled together.
        let node = fake_task_dir(&[("uc2-consensus", 100)]);
        let svc = fake_task_dir(&[("uc2-apply", 400)]);
        let mut got = sample_all(&[(11, node.path().into()), (22, svc.path().into())])
            .expect("union sample");
        got.sort();
        assert_eq!(
            got,
            vec![
                sample(11, 1000, "uc2-consensus", 100),
                sample(22, 1000, "uc2-apply", 400),
            ]
        );
    }

    #[test]
    fn delta_ranks_busiest_first_and_normalizes_by_time() {
        let before = vec![sample(1, 10, "uc2-consensus", 100), sample(1, 11, "uc2-sender", 100)];
        // Over 2 s: consensus yielded 20 times (busy), sender 2000 (idle).
        let after = vec![sample(1, 10, "uc2-consensus", 120), sample(1, 11, "uc2-sender", 2100)];
        let got = occupancy_delta(&before, &after, 2.0);
        assert_eq!(got[0].name, "uc2-consensus", "busiest (fewest yields) ranks first");
        assert_eq!(got[0].yields_per_sec, 10.0);
        assert_eq!(got[1].name, "uc2-sender");
        assert_eq!(got[1].yields_per_sec, 1000.0);
    }

    #[test]
    fn delta_ignores_threads_absent_from_either_sample() {
        let before = vec![sample(1, 10, "uc2-consensus", 100), sample(1, 11, "gone", 5)];
        let after = vec![sample(1, 10, "uc2-consensus", 120), sample(1, 12, "new", 5)];
        let got = occupancy_delta(&before, &after, 1.0);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name, "uc2-consensus");
    }

    /// C1: `all`/`ladder` sample a process running THREE nodes, so three
    /// threads are named `uc2-consensus`. A name-keyed join differences
    /// unrelated threads; the mis-paired rows saturate to 0 and 0 sorts to the
    /// FRONT of the ascending ranking, impersonating the busiest agent.
    #[test]
    fn delta_pairs_same_named_threads_by_tid_not_by_name() {
        let before = vec![
            sample(1, 10, "uc2-consensus", 100),
            sample(1, 11, "uc2-consensus", 5_000),
            sample(1, 12, "uc2-consensus", 9_000),
        ];
        let after = vec![
            sample(1, 10, "uc2-consensus", 200),   // +100
            sample(1, 11, "uc2-consensus", 5_020), // +20  <- the busy one
            sample(1, 12, "uc2-consensus", 9_300), // +300
        ];
        let got = occupancy_delta(&before, &after, 1.0);
        assert_eq!(got.len(), 3);
        // Name-keyed, tids 11 and 12 would difference against tid 10's `before`
        // (5000-100, 9000-100 — or saturate to 0 in the other direction).
        assert_eq!(
            got.iter().map(|o| (o.tid, o.yields_per_sec)).collect::<Vec<_>>(),
            vec![(11, 20.0), (10, 100.0), (12, 300.0)]
        );
        assert_eq!(got[0].label(), "uc2-consensus#11", "rows must be distinguishable");
        assert!(
            got.iter().all(|o| o.yields_per_sec > 0.0),
            "no row may saturate to 0 through a mis-pair: {got:?}"
        );
    }

    /// Same tid number in two different processes must not be joined either
    /// (tids are per-process in principle; `(pid, tid)` is the key).
    #[test]
    fn delta_does_not_join_across_processes() {
        let before = vec![sample(1, 10, "uc2-apply", 100), sample(2, 10, "uc2-apply", 7_000)];
        let after = vec![sample(1, 10, "uc2-apply", 150), sample(2, 10, "uc2-apply", 7_400)];
        let got = occupancy_delta(&before, &after, 1.0);
        assert_eq!(
            got.iter().map(|o| (o.pid, o.yields_per_sec)).collect::<Vec<_>>(),
            vec![(1, 50.0), (2, 400.0)]
        );
    }

    fn agent(name: &str, tid: u32, yields: f64) -> AgentRank {
        AgentRank { name: name.to_string(), label: format!("{name}#{tid}"), yields_per_sec: yields }
    }

    /// A HEALTHY read-only rung: no retries, target concurrency fully
    /// sustained. `top` only populates the diagnostic yield ranking, which no
    /// longer gates anything.
    fn rung(readers: u64, mode: Mode, rps: f64, top: &str) -> Rung {
        Rung {
            readers,
            mode,
            write_rate: 0,
            reads: rps as u64,
            reads_per_sec: rps,
            p50_ms: 0.1,
            p99_ms: 0.2,
            retried: 0,
            not_leader: 0,
            overwritten: 0,
            inflight_at_end: 0,
            inflight_mean: readers as f64,
            inflight_min: readers,
            regression: 0,
            agents: vec![agent(top, 10, 100.0), agent("uc2-archive", 11, 1_000.0)],
        }
    }

    #[test]
    fn rule_justifies_rung_a_when_the_read_only_gap_is_real() {
        let rungs = vec![
            rung(64, Mode::Snap, 1_000_000.0, "uc2-apply"),
            rung(64, Mode::Lin, 500_000.0, "uc2-consensus"), // 50% of snapshot
        ];
        let out = evaluate_decision_rule(&rungs, 0);
        // "NOT JUSTIFIED" also contains "JUSTIFIED", so assert both directions.
        assert!(out.contains("Rung A JUSTIFIED"), "got: {out}");
        assert!(!out.contains("NOT JUSTIFIED"), "got: {out}");
    }

    #[test]
    fn rule_declines_when_ratio_is_above_the_band() {
        let rungs = vec![
            rung(64, Mode::Snap, 1_000_000.0, "uc2-apply"),
            rung(64, Mode::Lin, 900_000.0, "uc2-consensus"), // 90%
        ];
        let out = evaluate_decision_rule(&rungs, 0);
        assert!(out.contains("NOT JUSTIFIED"), "got: {out}");
    }

    #[test]
    fn rule_declines_in_the_borderline_band_even_when_clause_b_is_met() {
        let rungs = vec![
            rung(64, Mode::Snap, 1_000_000.0, "uc2-apply"),
            rung(64, Mode::Lin, 700_000.0, "uc2-consensus"), // 70% — inside 65..=75
        ];
        let out = evaluate_decision_rule(&rungs, 0);
        assert!(out.contains("BORDERLINE"), "got: {out}");
        assert!(
            !out.contains("Rung A JUSTIFIED"),
            "borderline must not read as justified: {out}"
        );
    }

    /// The occupancy ranking no longer gates anything: an identical gap is
    /// justified regardless of which agent tops the (dead) yield ranking.
    #[test]
    fn rule_ignores_the_diagnostic_agent_ranking() {
        let with_apply_top = vec![
            rung(64, Mode::Snap, 1_000_000.0, "uc2-apply"),
            rung(64, Mode::Lin, 400_000.0, "uc2-apply"),
        ];
        let with_consensus_top = vec![
            rung(64, Mode::Snap, 1_000_000.0, "uc2-apply"),
            rung(64, Mode::Lin, 400_000.0, "uc2-consensus"),
        ];
        assert_eq!(
            evaluate_decision_rule(&with_apply_top, 0),
            evaluate_decision_rule(&with_consensus_top, 0),
            "the yield ranking must not change the verdict"
        );
        assert!(evaluate_decision_rule(&with_apply_top, 0).contains("Rung A JUSTIFIED"));
    }

    #[test]
    fn rule_flags_equal_plateaus_as_the_drain_cap_suspect() {
        let rungs = vec![
            rung(64, Mode::Snap, 500_000.0, "uc2-consensus"),
            rung(64, Mode::Lin, 499_000.0, "uc2-consensus"), // within 1%
        ];
        let out = evaluate_decision_rule(&rungs, 0);
        assert!(out.contains("QUERY_DRAIN_PER_CYCLE"), "got: {out}");
        // I5: the load generator is the OTHER candidate for an equal plateau —
        // if both arms hit the client's ceiling the barrier reads as free.
        assert!(out.contains("LOAD GENERATOR"), "got: {out}");
    }

    // --- I2: the borderline band's EDGES, both directions ------------------
    //
    // The pre-existing tests probed only 70.0, which is simultaneously the
    // in-band value and the clause-(a) threshold — so narrowing the band to
    // (69.5..=70.5) passed all of them while turning a 66% result from
    // BORDERLINE into JUSTIFIED. Narrowing is the dangerous direction; these
    // pin both edges.

    fn ratio_verdict(lin_rps: f64) -> String {
        let rungs = vec![
            rung(64, Mode::Snap, 1_000_000.0, "uc2-apply"),
            rung(64, Mode::Lin, lin_rps, "uc2-consensus"),
        ];
        evaluate_decision_rule(&rungs, 0)
    }

    #[test]
    fn rule_treats_both_band_edges_as_borderline() {
        for lin in [650_000.0, 750_000.0] {
            let out = ratio_verdict(lin);
            assert!(out.contains("BORDERLINE"), "{lin} reads/s must be BORDERLINE: {out}");
            assert!(!out.contains("Rung A JUSTIFIED"), "got: {out}");
        }
    }

    #[test]
    fn rule_does_not_extend_the_band_past_its_edges() {
        // 64.9%: below the band, clause (a) met, probe agent on top -> JUSTIFIED.
        let low = ratio_verdict(649_000.0);
        assert!(!low.contains("BORDERLINE"), "64.9% is outside the band: {low}");
        assert!(low.contains("Rung A JUSTIFIED"), "got: {low}");
        assert!(!low.contains("NOT JUSTIFIED"), "got: {low}");
        // 75.1%: above the band and above the threshold -> clause (a) unmet.
        let high = ratio_verdict(751_000.0);
        assert!(!high.contains("BORDERLINE"), "75.1% is outside the band: {high}");
        assert!(high.contains("NOT JUSTIFIED"), "got: {high}");
    }

    // --- C3: a degraded arm must not certify a plateau ---------------------

    #[test]
    fn rule_refuses_to_rule_on_a_degraded_linearizable_arm() {
        // The dangerous shape: the lin arm collapsed into RETRY, so it resolved
        // few genuine reads — a LOW ratio, i.e. the same signal as "the barrier
        // is expensive, build Rung A".
        let mut lin = rung(64, Mode::Lin, 100_000.0, "uc2-consensus");
        lin.reads = 100_000;
        lin.retried = 20_000; // 16.7% > 5%
        let rungs = vec![rung(64, Mode::Snap, 1_000_000.0, "uc2-apply"), lin];
        let out = evaluate_decision_rule(&rungs, 0);
        assert!(out.contains("VERDICT: INCONCLUSIVE"), "got: {out}");
        assert!(out.contains("linearizable arm degraded"), "got: {out}");
        assert!(out.contains("16.7% retries/redirects"), "got: {out}");
        assert!(!out.contains("JUSTIFIED"), "a degraded arm must not reach a verdict: {out}");
    }

    #[test]
    fn rule_refuses_to_rule_on_a_degraded_snapshot_arm() {
        let mut snap = rung(64, Mode::Snap, 1_000_000.0, "uc2-apply");
        snap.reads = 100_000;
        snap.not_leader = 10_000; // 9.1% > 5%
        let rungs = vec![snap, rung(64, Mode::Lin, 500_000.0, "uc2-consensus")];
        let out = evaluate_decision_rule(&rungs, 0);
        assert!(out.contains("VERDICT: INCONCLUSIVE"), "got: {out}");
        assert!(out.contains("snapshot arm degraded"), "got: {out}");
    }

    #[test]
    fn rule_still_rules_just_below_the_degraded_threshold() {
        let mut lin = rung(64, Mode::Lin, 500_000.0, "uc2-consensus");
        lin.reads = 96_000;
        lin.retried = 4_000; // exactly 4% — under the 5% bar
        let rungs = vec![rung(64, Mode::Snap, 1_000_000.0, "uc2-apply"), lin];
        let out = evaluate_decision_rule(&rungs, 0);
        assert!(!out.contains("INCONCLUSIVE"), "got: {out}");
        assert!(out.contains("Rung A JUSTIFIED"), "got: {out}");
    }

    #[test]
    fn degraded_fraction_is_zero_when_nothing_resolved() {
        assert_eq!(degraded_fraction(0, 0, 0), 0.0);
        assert_eq!(degraded_fraction(90, 5, 5), 0.1);
    }

    // --- the amended clause (b) (2026-07-25) --------------------------------
    //
    // (b) = the gap is present in the READ-ONLY arm, the client sustained >=90%
    // of target concurrency there, and neither read-only arm is degraded.

    /// Helper: a mixed-arm rung (write_rate = 20_000).
    fn mixed(readers: u64, mode: Mode, rps: f64) -> Rung {
        let mut r = rung(readers, mode, rps, "uc2-consensus");
        r.write_rate = 20_000;
        r
    }

    /// THE case clause (b) exists to catch: no gap without writes, a big gap
    /// with them. That is frontier-wait cost, not barrier cost.
    #[test]
    fn rule_declines_when_the_gap_is_absent_in_the_read_only_arm() {
        let rungs = vec![
            // Read-only: lin/snap = 95% — no barrier cost when nothing commits.
            rung(64, Mode::Snap, 1_000_000.0, "uc2-consensus"),
            rung(64, Mode::Lin, 950_000.0, "uc2-consensus"),
            // Mixed: lin/snap = 40% — but that is the frontier wait.
            mixed(64, Mode::Snap, 1_000_000.0),
            mixed(64, Mode::Lin, 400_000.0),
        ];
        let out = evaluate_decision_rule(&rungs, 20_000);
        assert!(out.contains("NOT JUSTIFIED"), "got: {out}");
        assert!(out.contains("clause (b) unmet"), "got: {out}");
        assert!(out.contains("ABSENT in the read-only arm"), "got: {out}");
        assert!(out.contains("frontier-wait cost"), "the reason must be named: {out}");
        assert!(out.contains("95.0%"), "the read-only ratio must be quoted: {out}");
    }

    /// The same mixed gap IS justified when the read-only arm shows it too.
    #[test]
    fn rule_justifies_a_mixed_gap_that_is_also_present_read_only() {
        let rungs = vec![
            rung(64, Mode::Snap, 1_000_000.0, "uc2-consensus"),
            rung(64, Mode::Lin, 500_000.0, "uc2-consensus"), // 50% read-only
            mixed(64, Mode::Snap, 1_000_000.0),
            mixed(64, Mode::Lin, 400_000.0),
        ];
        let out = evaluate_decision_rule(&rungs, 20_000);
        assert!(out.contains("Rung A JUSTIFIED"), "got: {out}");
        assert!(!out.contains("NOT JUSTIFIED"), "got: {out}");
    }

    #[test]
    fn rule_declines_when_the_client_did_not_sustain_concurrency() {
        let mut lin = rung(64, Mode::Lin, 500_000.0, "uc2-consensus");
        lin.inflight_mean = 40.0; // 62.5% of target 64 — the client was the ceiling
        let rungs = vec![rung(64, Mode::Snap, 1_000_000.0, "uc2-consensus"), lin];
        let out = evaluate_decision_rule(&rungs, 0);
        assert!(out.contains("NOT JUSTIFIED"), "got: {out}");
        assert!(out.contains("did not sustain the target concurrency"), "got: {out}");
        assert!(out.contains("LOAD GENERATOR"), "got: {out}");
        assert!(out.contains("62.5%"), "the shortfall must be quantified: {out}");
    }

    /// A client-limited SNAPSHOT arm corrupts the ratio just as badly.
    #[test]
    fn rule_declines_when_the_snapshot_arm_was_client_limited() {
        let mut snap = rung(64, Mode::Snap, 1_000_000.0, "uc2-consensus");
        snap.inflight_mean = 32.0; // 50%
        let rungs = vec![snap, rung(64, Mode::Lin, 500_000.0, "uc2-consensus")];
        let out = evaluate_decision_rule(&rungs, 0);
        assert!(out.contains("did not sustain the target concurrency"), "got: {out}");
        assert!(out.contains("snapshot arm"), "the arm must be named: {out}");
    }

    #[test]
    fn rule_accepts_concurrency_exactly_at_the_floor() {
        let mut lin = rung(64, Mode::Lin, 500_000.0, "uc2-consensus");
        lin.inflight_mean = 57.6; // exactly 90% of 64
        let rungs = vec![rung(64, Mode::Snap, 1_000_000.0, "uc2-consensus"), lin];
        let out = evaluate_decision_rule(&rungs, 0);
        assert!(out.contains("Rung A JUSTIFIED"), "got: {out}");
    }

    /// Clause (b) reads the READ-ONLY arm, so a degraded read-only arm blocks a
    /// verdict on the MIXED arm even though the mixed arm itself is healthy.
    #[test]
    fn rule_declines_when_the_read_only_arm_is_degraded() {
        let mut ro_lin = rung(64, Mode::Lin, 500_000.0, "uc2-consensus");
        ro_lin.reads = 90_000;
        ro_lin.retried = 10_000; // 10%
        let rungs = vec![
            rung(64, Mode::Snap, 1_000_000.0, "uc2-consensus"),
            ro_lin,
            mixed(64, Mode::Snap, 1_000_000.0),
            mixed(64, Mode::Lin, 400_000.0),
        ];
        let out = evaluate_decision_rule(&rungs, 20_000);
        assert!(out.contains("NOT JUSTIFIED"), "got: {out}");
        assert!(out.contains("read-only linearizable arm is degraded"), "got: {out}");
    }

    #[test]
    fn rule_declines_a_mixed_verdict_with_no_read_only_rungs_at_all() {
        let rungs = vec![mixed(64, Mode::Snap, 1_000_000.0), mixed(64, Mode::Lin, 400_000.0)];
        let out = evaluate_decision_rule(&rungs, 20_000);
        assert!(out.contains("NOT JUSTIFIED"), "got: {out}");
        assert!(out.contains("no usable rungs"), "got: {out}");
    }

    // --- I8: the verdict must never print a negative cost -------------------

    #[test]
    fn rule_never_reports_a_negative_barrier_cost() {
        // The lin arm out-ran the snapshot arm: ratio 114.2%.
        let rungs = vec![
            rung(64, Mode::Snap, 700_000.0, "uc2-apply"),
            rung(64, Mode::Lin, 800_000.0, "uc2-consensus"),
        ];
        let out = evaluate_decision_rule(&rungs, 0);
        assert!(!out.contains("-14.2%"), "negative cost must not be printed: {out}");
        assert!(out.contains("at most 0.0%"), "got: {out}");
        assert!(out.contains("OUT-RAN"), "ratio > 100% must be called out: {out}");
    }

    // --- I3: the rule is reachable from the fleet path ----------------------

    #[test]
    fn rung_json_round_trips() {
        let mut r = rung(256, Mode::Lin, 123_456.75, "uc2-receiver");
        r.write_rate = 20_000;
        r.retried = 3;
        r.not_leader = 4;
        r.overwritten = 5;
        r.inflight_at_end = 6;
        r.inflight_mean = 251.5;
        r.inflight_min = 240;
        r.regression = 0;
        let line = rung_to_json(&r);
        let back = rung_from_json(&line).expect("parses");
        assert_eq!(back.readers, r.readers);
        assert_eq!(back.mode, r.mode);
        assert_eq!(back.write_rate, r.write_rate);
        assert_eq!(back.reads, r.reads);
        assert_eq!(back.reads_per_sec, r.reads_per_sec);
        assert_eq!(back.p50_ms, r.p50_ms);
        assert_eq!(back.p99_ms, r.p99_ms);
        assert_eq!(back.retried, r.retried);
        assert_eq!(back.not_leader, r.not_leader);
        assert_eq!(back.overwritten, r.overwritten);
        assert_eq!(back.inflight_at_end, r.inflight_at_end);
        assert_eq!(back.inflight_mean, r.inflight_mean);
        assert_eq!(back.inflight_min, r.inflight_min);
        assert_eq!(back.agents, r.agents);
    }

    #[test]
    fn rung_json_ignores_non_rung_lines() {
        assert!(rung_from_json("").is_none());
        assert!(rung_from_json("========== uc2 read profile: snapshot ==========").is_none());
        assert!(rung_from_json("{\"something\":1}").is_none());
    }

    /// The whole point of I3: the SAME `evaluate_decision_rule` runs over rungs
    /// that made the round trip through stdout, so the fleet orchestrator never
    /// re-implements the pre-committed rule.
    #[test]
    fn decision_rule_is_identical_over_serialized_rungs() {
        let rungs = vec![
            rung(64, Mode::Snap, 1_000_000.0, "uc2-apply"),
            rung(64, Mode::Lin, 500_000.0, "uc2-consensus"),
        ];
        let text: String =
            rungs.iter().map(|r| format!("noise\n{}\n", rung_to_json(r))).collect();
        let parsed: Vec<Rung> = text.lines().filter_map(rung_from_json).collect();
        assert_eq!(parsed.len(), 2);
        assert_eq!(evaluate_decision_rule(&parsed, 0), evaluate_decision_rule(&rungs, 0));
    }

    // --- I4: the write load is measured, not asserted -----------------------

    #[test]
    fn writer_counters_separate_accepted_from_dropped() {
        let c = WriterCounters::default();
        c.record(Ok(())).expect("ok");
        c.record(Err(RingError::Full)).expect("full is a drop, not a failure");
        c.record(Ok(())).expect("ok");
        assert_eq!(c.snapshot(), (3, 2, 1), "(attempted, accepted, dropped)");
    }

    #[test]
    fn writer_counters_surface_non_full_errors() {
        let c = WriterCounters::default();
        assert!(c.record(Err(RingError::BadCrc)).is_err(), "a real ring error must not be hidden");
        assert_eq!(c.snapshot(), (1, 0, 0), "a failed write is neither accepted nor a drop");
    }

    // --- I5: in-flight depth is sampled across the window -------------------

    #[test]
    fn depth_stats_report_mean_and_minimum() {
        let d = DepthStats::default();
        for v in [64, 64, 32, 64] {
            d.record(v);
        }
        assert_eq!(d.samples(), 4);
        assert_eq!(d.mean(), 56.0);
        assert_eq!(d.min(), 32, "a mean near target must not hide a stall");
    }

    #[test]
    fn depth_stats_are_empty_not_wrong_when_never_sampled() {
        let d = DepthStats::default();
        assert_eq!(d.samples(), 0);
        assert_eq!(d.mean(), 0.0);
        assert_eq!(d.min(), 0, "u64::MAX sentinel must never escape");
    }
}
