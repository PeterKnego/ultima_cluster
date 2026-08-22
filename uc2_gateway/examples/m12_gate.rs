// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M12a gate: gateway (`Edge` + `RemoteClient`) vs direct `Engine` throughput
//! (spec `docs/superpowers/specs/2026-08-22-uc2-m12-adoptable-design.md` §4.6
//! item 5, §8 row 2).
//!
//! ```text
//! cargo run -p uc2_gateway --release --example m12_gate -- \
//!     [--arm direct|gateway|both] [--secs 6] [--payload 64] [--inflight 4096] \
//!     [--envelope on|off] [--root DIR]
//! ```
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

use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;

use uc2_client::{Engine, EngineConfig, Outcome, SubmitError};
use uc2_gateway::{Edge, EdgeConfig, Member};
use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};
use uc2_remote::{RemoteClient, RemoteConfig};
use uc2_service::{
    RawStateMachine, Service, ServiceBuilder, ServiceConfig, SessionConfig, Sessioned, StateMachine,
};

// --------------------------------------------------------------- CLI shape

#[derive(Parser)]
#[command(
    name = "m12_gate",
    about = "uc2 M12a gate: gateway (Edge + RemoteClient) vs direct Engine throughput (spec §8 row 2)"
)]
struct Cli {
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

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

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
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        bind,
        instance_dir,
        app_id: app_id.to_string(),
        buffer_bytes: NODE_BUFFER_BYTES,
        max_payload: NODE_MAX_PAYLOAD,
        admission_bytes: 256 * 1024,
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
    not_leader: u64,
    retried: u64,
    duplicates: u64,
    overwritten: u64,
    inflight_at_end: u64,
    lost: u64,
    elapsed: Duration,
    p50_ms: f64,
    p90_ms: f64,
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

    let stats = run_client_measurement(&dirs[leader], APP_ID, secs, payload, inflight);

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
            let last_response_ns = Arc::clone(&last_response_ns);
            let hist = Arc::clone(&hist);
            let stop = Arc::clone(&stop);
            move || {
                while !stop.load(Ordering::Relaxed) {
                    let n = poll.poll(|c| {
                        match c.outcome {
                            Outcome::Response(_) => {
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

    let mut sent_idx: u64 = 0;
    let deadline = t0 + Duration::from_secs(secs);
    while Instant::now() < deadline {
        let idx = (sent_idx as usize) & SLOT_MASK;
        send_ns[idx].store(t0.elapsed().as_nanos() as u64, Ordering::Release);
        match send.try_submit(sent_idx, &cmd_bytes) {
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

    let (p50_ms, p90_ms, p99_ms, max_ms) = {
        let h = hist.lock().unwrap();
        let ms = |ns: u64| ns as f64 / 1e6;
        (
            ms(h.value_at_quantile(0.50)),
            ms(h.value_at_quantile(0.90)),
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
        not_leader: not_leader.load(Ordering::Relaxed),
        retried: retried.load(Ordering::Relaxed),
        duplicates: engine_stats.duplicates,
        overwritten: engine_stats.overwritten,
        inflight_at_end,
        lost: lost.load(Ordering::Relaxed),
        elapsed,
        p50_ms,
        p90_ms,
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

    let (p50_ms, p90_ms, p99_ms, max_ms) = {
        let ms = |ns: u64| ns as f64 / 1e6;
        (
            ms(hist.value_at_quantile(0.50)),
            ms(hist.value_at_quantile(0.90)),
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
