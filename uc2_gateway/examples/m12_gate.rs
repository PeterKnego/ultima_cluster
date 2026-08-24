// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M12a gate: gateway (`Edge` + `RemoteClient`) vs direct `Engine` throughput
//! (spec `docs/superpowers/specs/2026-08-22-uc2-m12-adoptable-design.md` §4.6
//! item 5, §8 row 2).
//!
//! ```text
//! # local smoke — both arms in-process, one after the other (NOT the gate)
//! cargo run -p uc2_gateway --release --example m12_gate -- \
//!     [--arm direct|gateway|both] [--secs 6] [--payload 64] [--inflight 4096] \
//!     [--envelope on|off] [--root DIR]
//!
//! # fleet roles — one process per role per host, driven by
//! # bench-infra/scripts/m12_fleet_gate.py (gate rows 2 and 3)
//! m12_gate node          --id N --bind A --instance-dir D --members id@addr,… [--admission-kib K]
//! m12_gate service       --instance-dir D [--envelope on|off] [--raw-sm]
//! m12_gate edge          --instance-dir D --listen A --members id@gw_addr,… [--envelope on|off] [--inflight N]
//! m12_gate client-direct --instance-dir D --secs S [--payload P] [--inflight N] [--envelope on|off]
//! m12_gate client-remote --gateways A,… --secs S [--payload P] [--inflight N]
//! ```
//!
//! The two client roles each print ONE machine-readable
//! `RESULT {"arm":…,"responses_per_sec":…,…}` line, which is all the fleet
//! driver parses. Everything else they print is for a human reading the unit
//! log.
//!
//! **`direct`** is `m5_gate`'s measuring client (`uc2_node/examples/m5_gate.rs`)
//! copied verbatim: three in-process nodes + three typed [`CountSm`] services,
//! the client attached straight to the leader's instance dir over the public
//! `uc2_client::Engine`.
//!
//! **`gateway`** boots a SEPARATE three-node cluster, one [`Edge`] per node,
//! and connects ONE [`RemoteClient`] to the leader's edge over the framed TCP
//! remote protocol. `--envelope on` (the default) runs the service as
//! `Sessioned<CountSm>` and the edge's `session_envelope: true` — the edge
//! prepends the 16-byte `client_id ++ seq` header, the client sends the same
//! raw command bytes the direct arm does. `--envelope off` runs bare
//! [`CountSm`] with `session_envelope: false` (raw pass-through, at-least-once
//! on a re-send — see `Sessioned`'s and `RemoteConfig::resend_on_unknown`'s
//! docs). Both arms use the same [`CountSm`] state machine (`apply` never
//! inspects the bytes; the codec-share A/B is `m5_gate --raw-sm`'s job, not
//! this one's) and the same inflight cap, so the ratio isolates the edge +
//! remote-protocol hop.
//!
//! Both arms print the same [`ClientStats`] shape `m5_gate` uses
//! (responses/s, p50/p90/p99/max, sends/responses/lost/in-flight-at-end); with
//! `--arm both` (the default) the harness also prints `ratio gateway/direct`
//! for responses/s, p50, and p99.
//!
//! **This is a dev-box smoke number, never the gate.** The proposed bar (spec
//! §8: gateway throughput cost vs direct `Engine` ≥ 0.8× at equal inflight) is
//! fleet-only, per the dev-box-is-not-a-bench rule (`CLAUDE.md`). The banner
//! below prints on every run unless `UC2_GATE_FLEET=1` is set (it never is,
//! for a local run — this box has no fleet to be on).

use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand, ValueEnum};
use hdrhistogram::Histogram;

use uc2_client::{Engine, EngineConfig, Outcome, SubmitError};
use uc2_gateway::{Edge, EdgeConfig, Member};
use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};
use uc2_remote::{RemoteClient, RemoteConfig};
use uc2_service::{
    RawStateMachine, SESSION_HEADER_LEN, Service, ServiceBuilder, ServiceConfig, SessionConfig,
    Sessioned, StateMachine, TAG_FRESH,
};

// --------------------------------------------------------------- CLI shape

#[derive(Parser)]
#[command(
    name = "m12_gate",
    about = "uc2 M12a gate: gateway (Edge + RemoteClient) vs direct Engine throughput (spec §8 row 2)"
)]
struct Cli {
    /// Fleet per-role subcommand. Omitted = the in-process local smoke below
    /// (`--arm both` and friends), byte-for-byte the pre-M12-fleet behaviour.
    #[command(subcommand)]
    role: Option<Role>,
    /// Which arm(s) to run.
    #[arg(long, value_enum, default_value_t = Arm::Both)]
    arm: Arm,
    #[arg(long, default_value_t = 6)]
    secs: u64,
    #[arg(long, default_value_t = 64)]
    payload: usize,
    #[arg(long, default_value_t = 4096)]
    inflight: u64,
    /// Gateway arm only: whether the edge runs the session envelope
    /// (`Sessioned<CountSm>` + `session_envelope: true`) or raw pass-through
    /// (`CountSm` + `session_envelope: false`).
    #[arg(long, value_enum, default_value_t = Envelope::On)]
    envelope: Envelope,
    /// Scratch root for the in-process clusters' instance dirs. Defaults to
    /// `$HOME/.cache/cargo-target/m12_gate` (never `/tmp` — see the guard
    /// below; `/tmp` is RAM-backed tmpfs on this box, CLAUDE.md "Local box").
    #[arg(long)]
    root: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Arm {
    Direct,
    Gateway,
    Both,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Envelope {
    On,
    Off,
}

// ------------------------------------------------- fleet per-role subcommands
//
// The in-process arms above build BOTH clusters on loopback inside one
// process, which is what makes the local smoke's ratio uninterpretable (the
// gate doc's "Local smoke numbers" section says so at length: one 4-vCPU box,
// every role oversubscribed). The roles below are the fleet shape — one
// process per role per host, dedicated cores — driven by
// `bench-infra/scripts/m12_fleet_gate.py`. They deliberately reuse the SAME
// `CountSm`/`RawCountSm` pair, the same `Edge`, the same `RemoteClient` and
// the same two measurement cores as the in-process arms, so the only thing
// that changes between smoke and fleet is where the processes run.

#[derive(Subcommand)]
enum Role {
    /// Cluster-member node (one process per host). Parks until killed.
    Node(NodeArgs),
    /// State-machine service attached to this host's node. Parks until killed.
    Service(ServiceArgs),
    /// Gateway edge over this host's node. Parks until killed.
    Edge(EdgeArgs),
    /// Measuring client over the LOCAL shmem `Engine` — must run on the
    /// leader's host (the direct arm of row 2, and row 3's load generator).
    ClientDirect(ClientDirectArgs),
    /// Measuring client over the framed remote protocol — runs on a host with
    /// no node of its own (the gateway arm of row 2).
    ClientRemote(ClientRemoteArgs),
}

#[derive(clap::Args)]
struct NodeArgs {
    #[arg(long)]
    id: u32,
    #[arg(long)]
    bind: SocketAddr,
    #[arg(long)]
    instance_dir: PathBuf,
    /// Comma-separated `id@addr` member list (every member INCLUDING self).
    #[arg(long)]
    members: String,
    #[arg(long, default_value = "m12-gate")]
    app_id: String,
    /// Ingress admission window in KiB (`append - commit` backpressure gate).
    #[arg(long, default_value_t = 256)]
    admission_kib: u64,
}

#[derive(clap::Args)]
struct ServiceArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "m12-gate")]
    app_id: String,
    /// `on` wraps the state machine in `Sessioned<_>` (exactly-once over the
    /// remote hop). MUST match the edge's `--envelope` and the direct
    /// client's: with `on`, every submitted frame carries the 16-byte
    /// `client_id ++ seq` header, whoever put it there.
    #[arg(long, value_enum, default_value_t = Envelope::On)]
    envelope: Envelope,
    /// Row 3: run the RAW-tier twin (`RawCountSm`, bytes-in/bytes-out, no
    /// decode) instead of the typed `CountSm`. Paired with
    /// `--features uc2_service/apply-profile` this is the codec-share A/B.
    #[arg(long, default_value_t = false)]
    raw_sm: bool,
}

#[derive(clap::Args)]
struct EdgeArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "m12-gate")]
    app_id: String,
    /// TCP address this edge accepts remote clients on.
    #[arg(long)]
    listen: SocketAddr,
    /// Comma-separated `id@gateway_addr` map — every member's EDGE address
    /// (not its UDP `bind`), used for `REDIRECT`/`LEADER_CHANGED`.
    #[arg(long)]
    members: String,
    #[arg(long, value_enum, default_value_t = Envelope::On)]
    envelope: Envelope,
    /// Engine window and per-connection credit ceiling (kept equal, as the
    /// in-process arm does, so the two arms' inflight really is equal).
    #[arg(long, default_value_t = 4096)]
    inflight: u64,
}

#[derive(clap::Args)]
struct ClientDirectArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "m12-gate")]
    app_id: String,
    #[arg(long, default_value_t = 10)]
    secs: u64,
    #[arg(long, default_value_t = 64)]
    payload: usize,
    #[arg(long, default_value_t = 4096)]
    inflight: u64,
    /// Must match the service's. With `on` this client prepends the SAME
    /// 16-byte `client_id ++ seq` envelope the edge would have prepended, so
    /// the two arms submit byte-identical frames to an identical service.
    #[arg(long, value_enum, default_value_t = Envelope::On)]
    envelope: Envelope,
}

#[derive(clap::Args)]
struct ClientRemoteArgs {
    /// Comma-separated gateway addresses; the first is dialled first.
    #[arg(long)]
    gateways: String,
    #[arg(long, default_value = "m12-gate")]
    app_id: String,
    #[arg(long, default_value_t = 10)]
    secs: u64,
    #[arg(long, default_value_t = 64)]
    payload: usize,
    #[arg(long, default_value_t = 4096)]
    inflight: u64,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // A fleet role short-circuits everything below: no in-process cluster, no
    // smoke banner, no ratio — one process, one job.
    if let Some(role) = cli.role {
        return match role {
            Role::Node(a) => run_node_role(a),
            Role::Service(a) => run_service_role(a),
            Role::Edge(a) => run_edge_role(a),
            Role::ClientDirect(a) => run_client_direct_role(a),
            Role::ClientRemote(a) => run_client_remote_role(a),
        };
    }

    let fleet = std::env::var("UC2_GATE_FLEET").as_deref() == Ok("1");
    if !fleet {
        println!("SMOKE (dev box) — not a gate number");
    }

    let root = cli.root.unwrap_or_else(default_root);
    assert!(
        !root.starts_with("/tmp"),
        "m12_gate: root must be on a real filesystem (never /tmp — RAM tmpfs); got {root:?}"
    );
    let _ = std::fs::remove_dir_all(&root); // fresh root per run
    std::fs::create_dir_all(&root)?;

    println!("arm                   : {:?}", cli.arm);
    println!("envelope (gateway arm): {:?}", cli.envelope);
    println!(
        "secs / payload / inflight: {} / {} / {}",
        cli.secs, cli.payload, cli.inflight
    );

    let direct_stats = if cli.arm != Arm::Gateway {
        Some(run_direct_arm(
            &root.join("direct"),
            cli.secs,
            cli.payload,
            cli.inflight,
        ))
    } else {
        None
    };

    let gateway_stats = if cli.arm != Arm::Direct {
        Some(run_gateway_arm(
            &root.join("gateway"),
            cli.secs,
            cli.payload,
            cli.inflight,
            cli.envelope == Envelope::On,
        ))
    } else {
        None
    };

    if let Some(s) = &direct_stats {
        print_report("direct (Engine)", s);
    }
    if let Some(s) = &gateway_stats {
        print_report("gateway (Edge + RemoteClient)", s);
    }

    if let (Some(d), Some(g)) = (&direct_stats, &gateway_stats) {
        if !fleet {
            println!("SMOKE (dev box) — not a gate number");
        }
        println!("================ ratio gateway/direct (spec §8 row 2) =====================");
        println!(
            "responses/s ratio     : {:.3}  ({:.0} / {:.0})",
            g.responses_per_sec / d.responses_per_sec,
            g.responses_per_sec,
            d.responses_per_sec
        );
        println!(
            "p50 ratio (gw/direct) : {:.3}  ({:.3} ms / {:.3} ms)",
            g.p50_ms / d.p50_ms,
            g.p50_ms,
            d.p50_ms
        );
        println!(
            "p99 ratio (gw/direct) : {:.3}  ({:.3} ms / {:.3} ms)",
            g.p99_ms / d.p99_ms,
            g.p99_ms,
            d.p99_ms
        );
        println!(
            "(proposed bar, fleet-only: responses/s ratio >= 0.8 at equal inflight — \
             docs/benchmarks/uc2-m12-gate-2026-08-22.md)"
        );
        println!("============================================================================");
    }

    Ok(())
}

fn default_root() -> PathBuf {
    let home = std::env::var("HOME").expect("HOME not set");
    PathBuf::from(home).join(".cache/cargo-target/m12_gate")
}

// -------------------------------------------------------------- CountSm

/// The gate's trivial state machine (copied from `m5_gate`): the cheapest
/// possible deterministic `apply` so the measurement isolates the transport
/// path rather than any user business logic. Command bytes are OPAQUE —
/// `apply` never inspects them — and the response is a single `u64`.
#[derive(Default)]
struct CountSm {
    count: u64,
    last_applied: Option<u64>,
}

impl StateMachine for CountSm {
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

// ------------------------------------------------------------ cluster boot

const NODE_BUFFER_BYTES: usize = 64 << 20;
/// See `m5_gate`'s identical constant doc: this door is enforced on
/// `try_submit`'s bincode-ENCODED bytes, not on `--payload` itself.
const NODE_MAX_PAYLOAD: usize = 512;
/// The in-process arms' admission window, unchanged (the fleet `node` role
/// takes `--admission-kib` instead).
const DEFAULT_ADMISSION_BYTES: u64 = 256 * 1024;
/// Log-buffer ring capacity for a FLEET node — the hot window the archive
/// drains, sized like `m5_gate`'s (256 MiB) rather than the in-process smoke's
/// 64 MiB, because a fleet host really does push M5-ladder rates through it.
const FLEET_BUFFER_BYTES: usize = 256 << 20;
const ELECTION_TIMEOUT_MIN_NS: u64 = 150_000_000;
const ELECTION_TIMEOUT_MAX_NS: u64 = 300_000_000;

/// A distinct, index-derived election seed per node so a clean boot elects
/// exactly one leader (m5_gate / lincheck_v2 precedent).
fn seed_for(id: u32) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn node_config(
    id: u32,
    members: Vec<(u32, SocketAddr)>,
    bind: SocketAddr,
    instance_dir: PathBuf,
    app_id: &str,
    buffer_bytes: usize,
    admission_bytes: u64,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        bind,
        instance_dir,
        app_id: app_id.to_string(),
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
        crypto: uc2_node::CryptoConfig::Disabled,
    }
}

/// Wait for EXACTLY one serving leader across the live cluster; assert no
/// split-brain (m5_gate precedent). Returns its index.
fn await_single_leader(nodes: &[Node], secs: u64) -> usize {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let serving: Vec<usize> = (0..nodes.len())
            .filter(|&i| nodes[i].can_serve() && nodes[i].is_leader())
            .collect();
        assert!(
            serving.len() <= 1,
            "split-brain in smoke cluster: nodes {serving:?} all serve"
        );
        if serving.len() == 1 {
            return serving[0];
        }
        assert!(
            Instant::now() < deadline,
            "no leader elected within {secs}s"
        );
        thread::yield_now();
    }
}

/// Bring up an `n`-member in-process cluster + one service per member,
/// generic over the state-machine tier so the direct arm's plain [`CountSm`]
/// and the gateway arm's [`CountSm`] / `Sessioned<CountSm>` share this one
/// boot path (mirrors `m5_gate`'s `run_all_generic` dispatch).
fn boot_cluster<S, F>(
    root: &std::path::Path,
    app_id: &str,
    n: usize,
    make_sm: F,
) -> (Vec<Node>, Vec<Service<S>>, Vec<PathBuf>)
where
    S: RawStateMachine,
    F: Fn() -> S,
{
    let socks: Vec<UdpSocket> = (0..n)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind"))
        .collect();
    let members: Vec<(u32, SocketAddr)> = socks
        .iter()
        .enumerate()
        .map(|(i, s)| (i as u32, s.local_addr().unwrap()))
        .collect();

    let mut nodes = Vec::with_capacity(n);
    let mut services = Vec::with_capacity(n);
    let mut dirs = Vec::with_capacity(n);
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = members[i].1;
        let instance_dir = root.join(format!("n{i}"));
        std::fs::create_dir_all(&instance_dir).expect("instance dir");
        let cfg = node_config(
            i as u32,
            members.clone(),
            addr,
            instance_dir.clone(),
            app_id,
            NODE_BUFFER_BYTES,
            DEFAULT_ADMISSION_BYTES,
        );
        let node = Node::start_with_socket(cfg, sock).expect("node start");
        let svc = ServiceBuilder::new(ServiceConfig::new(&instance_dir, app_id), make_sm())
            .start()
            .expect("service start");
        nodes.push(node);
        services.push(svc);
        dirs.push(instance_dir);
    }
    (nodes, services, dirs)
}

/// A loopback TCP address nothing is listening on *right now* (bind-then-drop
/// reservation — `uc2_gateway/tests/common/mod.rs`'s `free_tcp_addr` trick):
/// every edge needs the whole node-id -> gateway-address map before any of
/// them starts, so the addresses must be chosen up front.
fn free_tcp_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").expect("reserve a port");
    l.local_addr().unwrap()
}

// ----------------------------------------------------------- stats + report

const SLOTS: usize = 1 << 20;
const SLOT_MASK: usize = SLOTS - 1;
const HIST_MAX_NS: u64 = 60_000_000_000;
const DRAIN_GRACE: Duration = Duration::from_secs(5);
const LEADER_WAIT: Duration = Duration::from_secs(30);
/// The M5 bar, reused only as an informational reference line in the printed
/// report — NOT a pass/fail gate for this harness. What this harness reports
/// is the gateway/direct RATIO; see spec §8 row 2 for the (fleet-only)
/// proposed bar on that ratio.
const RESPONSES_PER_SEC_BAR: f64 = 400_000.0;
const P50_MS_BAR: f64 = 1.0;

struct ClientStats {
    sends: u64,
    responses: u64,
    /// Direct arm with the session envelope on: responses NOT tagged
    /// `TAG_FRESH` (see the counter's comment in `run_client_measurement`).
    /// Always 0 for the gateway arm and for the in-process direct arm.
    not_fresh: u64,
    not_leader: u64,
    retried: u64,
    duplicates: u64,
    overwritten: u64,
    inflight_at_end: u64,
    lost: u64,
    elapsed: Duration,
    p50_ms: f64,
    p90_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
    responses_per_sec: f64,
    pass: bool,
}

fn print_report(label: &str, s: &ClientStats) {
    println!();
    println!("================ uc2 M12a gate: {label} ================");
    println!("sends                 : {}", s.sends);
    println!("responses             : {}", s.responses);
    println!("not FRESH (envelope)  : {}", s.not_fresh);
    println!("not_leader / retried  : {} / {}", s.not_leader, s.retried);
    println!(
        "dup / overwritten     : {} / {}",
        s.duplicates, s.overwritten
    );
    println!("in-flight at end      : {}", s.inflight_at_end);
    println!("lost (timeout/error)  : {}", s.lost);
    println!("elapsed (drain-incl.) : {:.3} s", s.elapsed.as_secs_f64());
    println!("responses/s           : {:.0}", s.responses_per_sec);
    println!("p50                   : {:.3} ms", s.p50_ms);
    println!("p90                   : {:.3} ms", s.p90_ms);
    println!("p95                   : {:.3} ms", s.p95_ms);
    println!("p99                   : {:.3} ms", s.p99_ms);
    println!("max                   : {:.3} ms", s.max_ms);
    println!(
        "(m5 reference bar, informational only: responses/s >= {RESPONSES_PER_SEC_BAR:.0} && \
         p50 <= {P50_MS_BAR:.1} ms — this harness's own bar is the gateway/direct RATIO, printed below)"
    );
    println!(
        "{}",
        if s.pass {
            "reference bar: PASS"
        } else {
            "reference bar: FAIL (not this harness's gate)"
        }
    );
    println!("============================================================================");
}

// ----------------------------------------------------------- direct arm

fn run_direct_arm(root: &std::path::Path, secs: u64, payload: usize, inflight: u64) -> ClientStats {
    const APP_ID: &str = "uc2-m12-gate-direct";
    let (nodes, services, dirs) = boot_cluster(root, APP_ID, 3, CountSm::default);
    let leader = await_single_leader(&nodes, 30);
    println!("[direct] leader elected: n{leader}");

    let stats = run_client_measurement(&dirs[leader], APP_ID, secs, payload, inflight, None);

    for node in nodes {
        node.stop();
    }
    for svc in services {
        svc.stop();
    }
    stats
}

/// The measuring client's core loop, copied verbatim (module-for-module) from
/// `uc2_node/examples/m5_gate.rs::run_client_measurement` — same public
/// `uc2_client::Engine` path, same slot-array latency correlation, same
/// drain-inclusive clock and PASS computation (the PASS bar here is only an
/// informational reference line — see [`ClientStats`]'s doc).
fn run_client_measurement(
    instance_dir: &std::path::Path,
    app_id: &str,
    secs: u64,
    payload_len: usize,
    inflight_cap: u64,
    session_client_id: Option<u64>,
) -> ClientStats {
    let (send, mut poll) = Engine::attach(
        instance_dir,
        app_id,
        EngineConfig {
            max_inflight: inflight_cap as u32,
            request_timeout: Duration::from_secs(30),
            max_payload: Some(NODE_MAX_PAYLOAD),
            serving_gate: true,
            ..EngineConfig::default()
        },
    )
    .unwrap_or_else(|e| panic!("engine attach {instance_dir:?}: {e}"));

    let serve_deadline = Instant::now() + LEADER_WAIT;
    while !send.can_serve() {
        assert!(
            Instant::now() < serve_deadline,
            "no serving leader at this instance_dir within {LEADER_WAIT:?} — \
             is this host's node the elected leader?"
        );
        thread::sleep(Duration::from_millis(20));
    }

    let raw_payload = vec![0xABu8; payload_len];
    let cmd_bytes = bincode::serde::encode_to_vec(&raw_payload, bincode::config::standard())
        .expect("encode fixed payload");

    let send_ns: Arc<Box<[AtomicU64]>> = Arc::new(
        (0..SLOTS)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    );
    let resolved = Arc::new(AtomicU64::new(0));
    let responses = Arc::new(AtomicU64::new(0));
    let not_leader = Arc::new(AtomicU64::new(0));
    let retried = Arc::new(AtomicU64::new(0));
    let lost = Arc::new(AtomicU64::new(0));
    // Anti-vacuity for the session envelope (fleet `client-direct` only). A
    // frame whose 16-byte header this client got WRONG does not fail: it comes
    // back tagged `TAG_EXPIRED`, having never reached the inner state machine
    // at all — i.e. a broken envelope would read as a FASTER direct arm. So
    // every response is checked to carry `TAG_FRESH`, and a nonzero count here
    // fails the role rather than being reported as throughput.
    let not_fresh = Arc::new(AtomicU64::new(0));
    let last_response_ns = Arc::new(AtomicU64::new(0));
    let hist: Arc<Mutex<Histogram<u64>>> = Arc::new(Mutex::new(
        Histogram::new_with_bounds(1, HIST_MAX_NS, 3).expect("histogram"),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();

    let matcher = thread::Builder::new()
        .name("m12-gate-poll".into())
        .spawn({
            let send_ns = Arc::clone(&send_ns);
            let resolved = Arc::clone(&resolved);
            let responses = Arc::clone(&responses);
            let not_leader = Arc::clone(&not_leader);
            let retried = Arc::clone(&retried);
            let lost = Arc::clone(&lost);
            let not_fresh = Arc::clone(&not_fresh);
            let last_response_ns = Arc::clone(&last_response_ns);
            let hist = Arc::clone(&hist);
            let stop = Arc::clone(&stop);
            move || {
                while !stop.load(Ordering::Relaxed) {
                    let n = poll.poll(|c| {
                        match c.outcome {
                            Outcome::Response(body) => {
                                if session_client_id.is_some()
                                    && body.first() != Some(&TAG_FRESH)
                                {
                                    not_fresh.fetch_add(1, Ordering::Relaxed);
                                }
                                let idx = (c.user_data as usize) & SLOT_MASK;
                                let now = t0.elapsed().as_nanos() as u64;
                                let lat = now
                                    .saturating_sub(send_ns[idx].load(Ordering::Acquire))
                                    .min(HIST_MAX_NS);
                                let _ = hist.lock().unwrap().record(lat);
                                responses.fetch_add(1, Ordering::Relaxed);
                                last_response_ns.fetch_max(now, Ordering::Relaxed);
                            }
                            Outcome::NotLeader { .. } => {
                                not_leader.fetch_add(1, Ordering::Relaxed);
                            }
                            Outcome::Retry => {
                                retried.fetch_add(1, Ordering::Relaxed);
                            }
                            Outcome::TimedOut | Outcome::InstanceRestart { .. } => {
                                lost.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        resolved.fetch_add(1, Ordering::Relaxed);
                    });
                    if n == 0 {
                        thread::sleep(Duration::from_micros(20));
                    }
                }
            }
        })
        .expect("spawn poll thread");

    // `session_client_id` is `Some` only for the fleet `client-direct` role
    // running against a `Sessioned<_>` service: it prepends the SAME 16-byte
    // `client_id ++ seq` header the EDGE prepends for the gateway arm, so both
    // arms hand the service byte-identical frames and the ratio is not
    // measuring one arm paying for an envelope the other skipped. `sent_idx`
    // is the seq — strictly increasing, so every frame classifies FRESH.
    // The in-process direct arm passes `None` (unchanged behaviour).
    let mut frame: Vec<u8> = Vec::with_capacity(SESSION_HEADER_LEN + cmd_bytes.len());
    let mut sent_idx: u64 = 0;
    let deadline = t0 + Duration::from_secs(secs);
    while Instant::now() < deadline {
        let idx = (sent_idx as usize) & SLOT_MASK;
        send_ns[idx].store(t0.elapsed().as_nanos() as u64, Ordering::Release);
        let submit_bytes: &[u8] = match session_client_id {
            Some(cid) => {
                frame.clear();
                frame.extend_from_slice(&cid.to_le_bytes());
                frame.extend_from_slice(&sent_idx.to_le_bytes());
                frame.extend_from_slice(&cmd_bytes);
                &frame
            }
            None => &cmd_bytes,
        };
        match send.try_submit(sent_idx, submit_bytes) {
            Ok(()) => sent_idx += 1,
            Err(SubmitError::Backpressure) => thread::yield_now(),
            Err(SubmitError::NotServing) => thread::sleep(Duration::from_millis(1)),
            Err(e) => panic!("try_submit: {e}"),
        }
    }
    let send_window_end_ns = t0.elapsed().as_nanos() as u64;

    let drain_deadline = Instant::now() + DRAIN_GRACE;
    while resolved.load(Ordering::Relaxed) < sent_idx && Instant::now() < drain_deadline {
        thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Ordering::Relaxed);
    matcher.join().expect("poll thread panicked");

    let sends = sent_idx;
    let resp = responses.load(Ordering::Relaxed);
    let inflight_at_end = send.inflight();
    let elapsed = Duration::from_nanos(
        last_response_ns
            .load(Ordering::Relaxed)
            .max(send_window_end_ns),
    );
    let responses_per_sec = if elapsed.as_secs_f64() > 0.0 {
        resp as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    let (p50_ms, p90_ms, p95_ms, p99_ms, max_ms) = {
        let h = hist.lock().unwrap();
        let ms = |ns: u64| ns as f64 / 1e6;
        (
            ms(h.value_at_quantile(0.50)),
            ms(h.value_at_quantile(0.90)),
            ms(h.value_at_quantile(0.95)),
            ms(h.value_at_quantile(0.99)),
            ms(h.max()),
        )
    };

    let pass = responses_per_sec >= RESPONSES_PER_SEC_BAR
        && p50_ms <= P50_MS_BAR
        && inflight_at_end == 0
        && lost.load(Ordering::Relaxed) == 0;

    let engine_stats = send.stats();

    ClientStats {
        sends,
        responses: resp,
        not_fresh: not_fresh.load(Ordering::Relaxed),
        not_leader: not_leader.load(Ordering::Relaxed),
        retried: retried.load(Ordering::Relaxed),
        duplicates: engine_stats.duplicates,
        overwritten: engine_stats.overwritten,
        inflight_at_end,
        lost: lost.load(Ordering::Relaxed),
        elapsed,
        p50_ms,
        p90_ms,
        p95_ms,
        p99_ms,
        max_ms,
        responses_per_sec,
        pass,
    }
}

// ----------------------------------------------------------- gateway arm

fn run_gateway_arm(
    root: &std::path::Path,
    secs: u64,
    payload: usize,
    inflight: u64,
    envelope_on: bool,
) -> ClientStats {
    if envelope_on {
        run_gateway_arm_generic(root, secs, payload, inflight, true, || {
            Sessioned::new(CountSm::default(), SessionConfig::default())
        })
    } else {
        run_gateway_arm_generic(root, secs, payload, inflight, false, CountSm::default)
    }
}

fn run_gateway_arm_generic<S, F>(
    root: &std::path::Path,
    secs: u64,
    payload: usize,
    inflight: u64,
    envelope_on: bool,
    make_sm: F,
) -> ClientStats
where
    S: RawStateMachine,
    F: Fn() -> S,
{
    const APP_ID: &str = "uc2-m12-gate-gateway";
    const N: usize = 3;

    let (nodes, services, dirs) = boot_cluster(root, APP_ID, N, make_sm);
    let leader = await_single_leader(&nodes, 30);
    println!("[gateway] leader elected: n{leader}");

    // One Edge per node, against the shared static node-id -> gateway map
    // (`uc2_gateway/tests/failover.rs` precedent).
    let listen: Vec<SocketAddr> = (0..N).map(|_| free_tcp_addr()).collect();
    let members: Vec<Member> = listen
        .iter()
        .enumerate()
        .map(|(i, a)| Member {
            node_id: i as u32,
            gateway: a.to_string(),
        })
        .collect();

    let mut edges = Vec::with_capacity(N);
    for (i, dir) in dirs.iter().enumerate() {
        let edge = Edge::start(EdgeConfig {
            instance_dir: dir.clone(),
            app_id: APP_ID.into(),
            listen: listen[i],
            members: members.clone(),
            session_envelope: envelope_on,
            max_inflight: inflight as u32,
            per_conn_inflight: inflight as u32,
            status_interval: Duration::from_millis(200),
            request_timeout: Duration::from_secs(30),
            ..EdgeConfig::defaults()
        })
        .unwrap_or_else(|e| panic!("edge start n{i}: {e}"));
        edges.push(edge);
    }

    // ONE RemoteClient, connected straight to the leader's edge (this
    // harness measures steady-state throughput, not failover — that is
    // `failover.rs` / `remote_lin.rs`'s job).
    let leader_addr = edges[leader].local_addr();
    let remote = RemoteClient::connect(RemoteConfig {
        app_id: APP_ID.into(),
        members: vec![leader_addr.to_string()],
        client_id: None,
        max_inflight: inflight as u32,
        request_timeout: Duration::from_secs(30),
        ..RemoteConfig::default()
    })
    .unwrap_or_else(|e| panic!("remote connect {leader_addr}: {e}"));

    let stats = run_remote_measurement(&remote, secs, payload);
    print_remote_stats(&remote);

    remote.shutdown();
    for edge in edges {
        edge.stop();
    }
    for node in nodes {
        node.stop();
    }
    for svc in services {
        svc.stop();
    }
    stats
}

/// `RemoteClient`-side measurement core: one sender thread issuing
/// `submit()` under the client's own credit/inflight gating, and a small pool
/// of ticket-wait threads so pipelining is not serialized behind
/// `Ticket::wait` (mirrors [`run_client_measurement`]'s slot-array latency
/// correlation, adapted to `RemoteClient`'s `Ticket` handle rather than the
/// `Engine`'s `user_data` callback).
fn run_remote_measurement(remote: &RemoteClient, secs: u64, payload_len: usize) -> ClientStats {
    const N_WAITERS: usize = 8;
    /// Bound on one ticket's wait — generous relative to a healthy run so it
    /// never becomes the limiter, but finite so a stuck response cannot hang
    /// the harness forever.
    const TICKET_WAIT: Duration = Duration::from_secs(10);

    let raw_payload = vec![0xABu8; payload_len];
    let cmd_bytes = bincode::serde::encode_to_vec(&raw_payload, bincode::config::standard())
        .expect("encode fixed payload");

    let send_ns: Arc<Box<[AtomicU64]>> = Arc::new(
        (0..SLOTS)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    );
    let responses = Arc::new(AtomicU64::new(0));
    let resolved = Arc::new(AtomicU64::new(0));
    let lost = Arc::new(AtomicU64::new(0));
    let last_response_ns = Arc::new(AtomicU64::new(0));

    // Each waiter owns its OWN histogram (no shared lock on the hot path —
    // a `Mutex<Histogram>` shared across all N_WAITERS threads would
    // serialize every response recording and inflate the measured latency
    // with lock-contention time that has nothing to do with the gateway or
    // the network). Merged into one histogram after every thread has joined.
    let t0 = Instant::now();
    let mut senders = Vec::with_capacity(N_WAITERS);
    let mut waiters = Vec::with_capacity(N_WAITERS);
    for _ in 0..N_WAITERS {
        let (tx, rx) = mpsc::channel::<(u64, uc2_remote::Ticket)>();
        senders.push(tx);
        let send_ns = Arc::clone(&send_ns);
        let responses = Arc::clone(&responses);
        let resolved = Arc::clone(&resolved);
        let lost = Arc::clone(&lost);
        let last_response_ns = Arc::clone(&last_response_ns);
        waiters.push(
            thread::Builder::new()
                .name("m12-gw-wait".into())
                .spawn(move || {
                    let mut local_hist =
                        Histogram::<u64>::new_with_bounds(1, HIST_MAX_NS, 3).expect("histogram");
                    for (idx, ticket) in rx {
                        let outcome = ticket.wait_timeout(TICKET_WAIT);
                        let now = t0.elapsed().as_nanos() as u64;
                        match outcome {
                            Ok(_resp) => {
                                let slot = (idx as usize) & SLOT_MASK;
                                let lat = now
                                    .saturating_sub(send_ns[slot].load(Ordering::Acquire))
                                    .min(HIST_MAX_NS);
                                let _ = local_hist.record(lat);
                                responses.fetch_add(1, Ordering::Relaxed);
                                last_response_ns.fetch_max(now, Ordering::Relaxed);
                            }
                            Err(_e) => {
                                lost.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        resolved.fetch_add(1, Ordering::Relaxed);
                    }
                    local_hist
                })
                .expect("spawn waiter thread"),
        );
    }

    let mut sent_idx: u64 = 0;
    let deadline = t0 + Duration::from_secs(secs);
    while Instant::now() < deadline {
        let idx = sent_idx;
        let slot = (idx as usize) & SLOT_MASK;
        send_ns[slot].store(t0.elapsed().as_nanos() as u64, Ordering::Release);
        // `RemoteClient::submit`'s own doc (`uc2_remote/src/client.rs`) is
        // explicit: it BLOCKS while the edge's credits (or the local
        // `max_inflight` cap) are exhausted, pacing this loop itself —
        // there is no transient "backpressure, retry" error to yield on
        // here, unlike the direct arm's `SubmitError::Backpressure`
        // (`Engine::try_submit` is non-blocking by design; `RemoteClient`
        // is not). The only `Err` this can resolve to is `TimedOut`
        // (credits never reopened within `request_timeout`) or `Closed`
        // (the client shut down under us) — both are genuine harness
        // failures for a healthy run, hence the `panic!` rather than a
        // retry loop.
        match remote.submit(&cmd_bytes) {
            Ok(ticket) => {
                let w = (idx as usize) % N_WAITERS;
                senders[w].send((idx, ticket)).expect("waiter thread alive");
                sent_idx += 1;
            }
            Err(e) => panic!("remote submit: {e}"),
        }
    }
    let send_window_end_ns = t0.elapsed().as_nanos() as u64;

    // Closing the channels lets each waiter thread exit once it has drained
    // (and resolved, one way or another) everything already queued.
    drop(senders);
    let mut hist = Histogram::<u64>::new_with_bounds(1, HIST_MAX_NS, 3).expect("histogram");
    for w in waiters {
        if let Ok(local_hist) = w.join() {
            hist.add(local_hist).expect("merge waiter histogram");
        }
    }

    let sends = sent_idx;
    let resp = responses.load(Ordering::Relaxed);
    let resolved_total = resolved.load(Ordering::Relaxed);
    let inflight_at_end = sends.saturating_sub(resolved_total);
    let elapsed = Duration::from_nanos(
        last_response_ns
            .load(Ordering::Relaxed)
            .max(send_window_end_ns),
    );
    let responses_per_sec = if elapsed.as_secs_f64() > 0.0 {
        resp as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    let (p50_ms, p90_ms, p95_ms, p99_ms, max_ms) = {
        let ms = |ns: u64| ns as f64 / 1e6;
        (
            ms(hist.value_at_quantile(0.50)),
            ms(hist.value_at_quantile(0.90)),
            ms(hist.value_at_quantile(0.95)),
            ms(hist.value_at_quantile(0.99)),
            ms(hist.max()),
        )
    };

    let lost_count = lost.load(Ordering::Relaxed);
    let pass = responses_per_sec >= RESPONSES_PER_SEC_BAR
        && p50_ms <= P50_MS_BAR
        && inflight_at_end == 0
        && lost_count == 0;

    let rs = remote.stats();

    ClientStats {
        sends,
        responses: resp,
        not_fresh: 0, // the EDGE owns the envelope on this arm, not the client
        // Mapped from `RemoteStats` (there is no Engine-side "not_leader" /
        // "retried" concept over the remote protocol): see
        // `print_remote_stats` for the full breakdown.
        not_leader: rs.redirects,
        retried: rs.retries,
        duplicates: rs.resends,
        overwritten: 0, // no analog: the edge holds no broadcast ring to overwrite
        inflight_at_end,
        lost: lost_count,
        elapsed,
        p50_ms,
        p90_ms,
        p95_ms,
        p99_ms,
        max_ms,
        responses_per_sec,
        pass,
    }
}

/// The gateway arm's own stats the direct arm has no analog for — printed
/// separately rather than shoehorned into [`ClientStats`]'s field names.
fn print_remote_stats(remote: &RemoteClient) {
    let s = remote.stats();
    println!("---------------------------- gateway/remote plane -------------------------");
    println!(
        "redirects {} | leader_changes {} | reconnects {} | resends {} | retries {} | \
         unknown {} | expired {} | refused_members {} | max_credits_seen {}",
        s.redirects,
        s.leader_changes,
        s.reconnects,
        s.resends,
        s.retries,
        s.unknown,
        s.expired,
        s.refused_members,
        s.max_credits_seen
    );
    println!("============================================================================");
}

// ======================================================================
// Fleet roles (rows 2 and 3)
// ======================================================================
//
// Everything below runs ONE role in ONE process. The orchestrator is
// `bench-infra/scripts/m12_fleet_gate.py`; see that file's module doc for the
// topology and for why the two row-2 arms are measured against the SAME
// cluster generation (holding hardware AND leadership constant is exactly
// what the in-process smoke could not do).

/// Raw-tier twin of [`CountSm`], copied from `m5_gate`: sees the frame bytes,
/// decodes nothing. Same deterministic increment and a `u64` response either
/// way — but not the same bytes (8 LE here, bincode varint through the typed
/// tier). Which side of the [`RawStateMachine`] boundary does the (de)coding
/// is precisely what row 3's `apply-profile` A/B measures.
#[derive(Default)]
struct RawCountSm {
    count: u64,
    last_applied: Option<u64>,
}

impl RawStateMachine for RawCountSm {
    fn apply(&mut self, position: u64, _cmd: &[u8], out: &mut Vec<u8>) {
        self.count += 1;
        self.last_applied = Some(position);
        out.extend_from_slice(&self.count.to_le_bytes());
    }

    fn query(&self, _q: &[u8], out: &mut Vec<u8>) {
        out.extend_from_slice(&self.count.to_le_bytes());
    }

    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

/// `id@addr,...` — used for both the node role's UDP member list and the edge
/// role's node-id -> gateway-address map (the two are different addresses for
/// the same ids, which is why the edge takes its own flag).
fn parse_id_addr_list(s: &str) -> Vec<(u32, String)> {
    s.split(',')
        .filter(|p| !p.trim().is_empty())
        .map(|part| {
            let (id, addr) = part
                .trim()
                .split_once('@')
                .unwrap_or_else(|| panic!("bad entry {part:?}, expected id@addr"));
            let id: u32 = id
                .parse()
                .unwrap_or_else(|e| panic!("bad member id {id:?}: {e}"));
            (id, addr.to_string())
        })
        .collect()
}

/// A per-process session identity for the direct arm. Random enough that two
/// successive `client-direct` runs against the same long-lived service do NOT
/// collide on `(client_id, seq)` — a collision would make the second run's
/// frames REPLAYED (served from the dedup cache, never reaching `apply`),
/// which would silently inflate the direct arm's throughput.
fn fresh_client_id() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    nanos ^ (std::process::id() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn assert_durable_dir(dir: &std::path::Path) {
    assert!(
        !dir.starts_with("/tmp"),
        "instance_dir must be on a real filesystem (never /tmp — RAM tmpfs, and \
         fsync there is fiction); got {dir:?}"
    );
}

// ------------------------------------------------------------- node role

fn run_node_role(a: NodeArgs) -> anyhow::Result<()> {
    assert_durable_dir(&a.instance_dir);
    let members: Vec<(u32, SocketAddr)> = parse_id_addr_list(&a.members)
        .into_iter()
        .map(|(id, addr)| {
            (
                id,
                addr.parse()
                    .unwrap_or_else(|e| panic!("bad member addr {addr:?}: {e}")),
            )
        })
        .collect();
    let id = a.id;
    let cfg = node_config(
        id,
        members,
        a.bind,
        a.instance_dir,
        &a.app_id,
        FLEET_BUFFER_BYTES,
        a.admission_kib * 1024,
    );
    let node = Node::start(cfg)?;
    println!("m12_gate node {id} up; parking (killed externally by the harness)");
    // Protocol 0.5.0 observability, same as `m5_gate`'s node role: the
    // attestation counter is process-local, so it cannot come out through the
    // cnc page. On a healthy throughput run it must stay 0.
    let mut last = u64::MAX;
    loop {
        let now = node.reports_unattested();
        if now != last {
            println!("m12_gate node {id} stats: reports_unattested={now}");
            last = now;
        }
        thread::sleep(Duration::from_millis(500));
    }
}

// ---------------------------------------------------------- service role

fn run_service_role(a: ServiceArgs) -> anyhow::Result<()> {
    let cnc = a.instance_dir.join("cnc2.dat");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cnc.exists() {
        anyhow::ensure!(
            Instant::now() < deadline,
            "timed out waiting for cnc2.dat at {cnc:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }
    let cfg = ServiceConfig::new(&a.instance_dir, &a.app_id);
    let envelope = a.envelope == Envelope::On;
    // Each arm diverges (parks forever), so the four `Service<_>` types never
    // need to unify — `m5_gate`'s service role does the same.
    match (envelope, a.raw_sm) {
        (true, false) => {
            let _svc = ServiceBuilder::new(
                cfg,
                Sessioned::new(CountSm::default(), SessionConfig::default()),
            )
            .start()?;
            park_service("Sessioned<CountSm> (typed tier, envelope on)")
        }
        (false, false) => {
            let _svc = ServiceBuilder::new(cfg, CountSm::default()).start()?;
            park_service("CountSm (typed tier, envelope off)")
        }
        (true, true) => {
            let _svc = ServiceBuilder::new(
                cfg,
                Sessioned::new(RawCountSm::default(), SessionConfig::default()),
            )
            .start()?;
            park_service("Sessioned<RawCountSm> (raw tier, envelope on)")
        }
        (false, true) => {
            let _svc = ServiceBuilder::new(cfg, RawCountSm::default()).start()?;
            park_service("RawCountSm (raw tier, envelope off)")
        }
    }
}

fn park_service(what: &str) -> ! {
    println!("m12_gate service up ({what}); parking (killed externally by the harness)");
    loop {
        thread::park();
    }
}

// ------------------------------------------------------------- edge role

fn run_edge_role(a: EdgeArgs) -> anyhow::Result<()> {
    let members: Vec<Member> = parse_id_addr_list(&a.members)
        .into_iter()
        .map(|(node_id, gateway)| Member { node_id, gateway })
        .collect();
    anyhow::ensure!(!members.is_empty(), "--members must name at least one edge");
    let edge = Edge::start(EdgeConfig {
        instance_dir: a.instance_dir,
        app_id: a.app_id,
        listen: a.listen,
        members,
        session_envelope: a.envelope == Envelope::On,
        // Kept equal to each other and to the client's `--inflight`, exactly
        // as the in-process gateway arm does: the whole point of row 2 is a
        // ratio "at equal inflight".
        max_inflight: a.inflight as u32,
        per_conn_inflight: a.inflight as u32,
        status_interval: Duration::from_millis(200),
        request_timeout: Duration::from_secs(30),
        ..EdgeConfig::defaults()
    })
    .map_err(|e| anyhow::anyhow!("edge start: {e}"))?;
    println!(
        "m12_gate edge up on {}; parking (killed externally by the harness)",
        edge.local_addr()
    );
    loop {
        thread::park();
    }
}

// --------------------------------------------------------- client roles

/// One machine-readable line per measured arm. The orchestrator parses ONLY
/// this line (`RESULT ` + JSON); everything else a client role prints is for
/// a human reading the unit log.
fn print_result_json(arm: &str, s: &ClientStats, secs: u64, payload: usize, inflight: u64) {
    println!(
        "RESULT {{\"arm\":\"{arm}\",\"responses_per_sec\":{:.1},\"payload\":{payload},\
         \"inflight\":{inflight},\"secs\":{secs},\"sends\":{},\"responses\":{},\
         \"lost\":{},\"not_fresh\":{},\"inflight_at_end\":{},\"p50_ms\":{:.3},\
         \"p90_ms\":{:.3},\"p95_ms\":{:.3},\"p99_ms\":{:.3},\"max_ms\":{:.3},\"elapsed_secs\":{:.3}}}",
        s.responses_per_sec,
        s.sends,
        s.responses,
        s.lost,
        s.not_fresh,
        s.inflight_at_end,
        s.p50_ms,
        s.p90_ms,
        s.p95_ms,
        s.p99_ms,
        s.max_ms,
        s.elapsed.as_secs_f64(),
    );
}

fn run_client_direct_role(a: ClientDirectArgs) -> anyhow::Result<()> {
    assert_durable_dir(&a.instance_dir);
    let envelope_on = a.envelope == Envelope::On;
    // `--payload` is a RAW length; the frame is its bincode encoding (length
    // varint + bytes) plus, with the envelope on, 16 more. The node's
    // `max_payload` door is enforced on the whole frame — refuse up front with
    // the arithmetic rather than letting `try_submit` panic mid-run.
    let encoded_len =
        bincode::serde::encode_to_vec(vec![0xABu8; a.payload], bincode::config::standard())
            .expect("encode fixed payload")
            .len()
        + if envelope_on { SESSION_HEADER_LEN } else { 0 };
    anyhow::ensure!(
        encoded_len <= NODE_MAX_PAYLOAD,
        "--payload {} encodes to {} B{} which exceeds the node's max_payload of {} B",
        a.payload,
        encoded_len,
        if envelope_on {
            " (incl. the 16-byte session envelope)"
        } else {
            ""
        },
        NODE_MAX_PAYLOAD
    );

    let session_client_id = envelope_on.then(fresh_client_id);
    println!(
        "m12_gate client-direct: {} s, payload {}, inflight {}, envelope {}",
        a.secs,
        a.payload,
        a.inflight,
        if envelope_on { "on" } else { "off" }
    );
    let stats = run_client_measurement(
        &a.instance_dir,
        &a.app_id,
        a.secs,
        a.payload,
        a.inflight,
        session_client_id,
    );
    print_report("direct (Engine)", &stats);
    print_result_json("direct", &stats, a.secs, a.payload, a.inflight);
    anyhow::ensure!(
        stats.not_fresh == 0,
        "{} of {} responses were not TAG_FRESH — this client's session envelope \
         did not reach the inner state machine, so the measured rate is not a \
         rate for the work the gateway arm does",
        stats.not_fresh,
        stats.responses
    );
    Ok(())
}

fn run_client_remote_role(a: ClientRemoteArgs) -> anyhow::Result<()> {
    let gateways: Vec<String> = a
        .gateways
        .split(',')
        .map(|g| g.trim().to_string())
        .filter(|g| !g.is_empty())
        .collect();
    anyhow::ensure!(
        !gateways.is_empty(),
        "--gateways must name at least one edge address"
    );
    println!(
        "m12_gate client-remote: {} s, payload {}, inflight {}, gateways {:?}",
        a.secs, a.payload, a.inflight, gateways
    );
    let remote = RemoteClient::connect(RemoteConfig {
        app_id: a.app_id,
        members: gateways,
        client_id: None,
        max_inflight: a.inflight as u32,
        request_timeout: Duration::from_secs(30),
        ..RemoteConfig::default()
    })
    .map_err(|e| anyhow::anyhow!("remote connect: {e}"))?;

    let stats = run_remote_measurement(&remote, a.secs, a.payload);
    print_remote_stats(&remote);
    remote.shutdown();
    print_report("gateway (Edge + RemoteClient)", &stats);
    print_result_json("gateway", &stats, a.secs, a.payload, a.inflight);
    Ok(())
}
