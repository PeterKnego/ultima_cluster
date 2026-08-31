// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M10 gate: **observable cluster** (see
//! `docs/superpowers/specs/2026-08-19-uc2-production-readiness-design.md` §5;
//! plan `docs/superpowers/plans/2026-08-19-uc2-m10-observable-cluster.md`).
//!
//! The four pre-committed bars live in
//! `docs/benchmarks/uc2-m10-gate-2026-08-20.md` and are restated in [`BAR`]
//! below. This binary exercises rows 1-3:
//!
//! - `coverage`  — row 1: every `CONTRACT_SERIES` family present in a live
//!   scrape of a serving 3-node cluster, plus >=1 occupied per-peer series
//!   on the leader. GATED.
//! - `probes`    — row 2: under load, kill the leader; a poller reads every
//!   surviving node's `/readyz` AND (in the same sample, via a second
//!   read-only cnc attach) its flags, and must never see `readyz==200`
//!   coincide with the elected-not-serving `0x01` window. GATED.
//! - `perturb-smoke` — row 3 (scrape-on vs scrape-off commit rate) is a
//!   FLEET-ONLY bar (cache traffic under load is not honestly measurable on
//!   one dev-box process). This subcommand prints both numbers, labeled
//!   SMOKE, and never gates.
//! - `all` — coverage + probes (gated) + perturb-smoke (ungated). Prints the
//!   bar, then each row's PASS/FAIL, `exit(1)` on any GATED FAIL.
//!
//! Row 4 (alert rules) is `scripts/m10_alert_fire.sh` (Task 9's harness,
//! adjudicated in the gate doc) and is not re-run by this binary.
//!
//! Cluster plumbing (`NodeH`/`spawn_cluster`/`await_stable_leader`/
//! `make_config`) is copied — not imported, examples cannot import each
//! other's code — from `uc_node/examples/m10_alerts.rs`, itself copied from
//! `uc_node/tests/failover.rs`. `/readyz` never returns 200 without an
//! attached service (`service_heartbeat_ns` is only ever stamped by
//! `uc_service`'s apply agent, so a service-less node reads as
//! permanently-stale), so `probes` attaches a trivial no-op `StateMachine`
//! to every node — the brief's role sketch omits this, but it is required
//! for row 2 to be satisfiable at all.
//!
//! Every cluster's instance dirs live under a `tempfile::tempdir_in` rooted
//! on real disk (never `/tmp` — RAM tmpfs, no swap on this box). The plan's
//! literal `env!("CARGO_TARGET_TMPDIR")` is a `cargo test`-only compile-time
//! variable (verified: it does not resolve for an `examples/` binary under
//! `cargo run`), so this mirrors `m9_gate.rs`'s pattern instead: a real-disk
//! default derived from the running binary's own target dir, with an
//! explicit `/tmp` guard, overridable with `--root`.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};

use uc_consensus::election::NodeId;
use uc_log::cnc::CncPage;
use uc_net::fault::FaultConfig;
use uc_node::obs::http::ObsServer;
use uc_node::obs::metrics::CONTRACT_SERIES;
use uc_node::{Node, NodeConfig};
use uc_protocol::v2::cnc::{NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER};
use uc_service::{Service, ServiceBuilder, ServiceConfig, StateMachine};

const APP: &str = "m10-gate";
const RING_BYTES: usize = 1 << 20;
const MAX_PAYLOAD: usize = 256;
const ADMISSION_BYTES: u64 = 256 * 1024;

/// The pre-committed bar, restated from
/// `docs/benchmarks/uc2-m10-gate-2026-08-20.md`. Row 4 is printed for
/// context but exercised by `scripts/m10_alert_fire.sh`, not this binary.
const BAR: &[(&str, &str)] = &[
    (
        "1",
        "/metrics coverage: every CONTRACT_SERIES family present in a live scrape of the \
         leader, plus >=1 occupied per-peer series — LOCAL, GATED",
    ),
    (
        "2",
        "probe regime across a leader kill: zero samples where readyz==200 coincides with \
         flags==0x01 (elected, not CAN_SERVE); some node serves 200 within 5s — LOCAL, GATED",
    ),
    (
        "3",
        "scrape perturbation: scrape-on vs scrape-off commit-rate ratio >= 0.95 — FLEET ONLY; \
         local run is SMOKE, ungated (see gate doc: an 18-point local spread against a \
         10-point bar in M9)",
    ),
    (
        "4",
        "alert rules: scripts/m10_alert_fire.sh exits 0, every shipped rule fired once — run \
         separately (Task 9); see docs/benchmarks/uc2-m10-gate-2026-08-20.md",
    ),
];

fn print_bar() {
    println!("== uc2 M10 gate bar (pre-committed docs/benchmarks/uc2-m10-gate-2026-08-20.md) ==");
    for (n, desc) in BAR {
        println!("  [{n}] {desc}");
    }
}

// ------------------------------------------------------------------ CLI

#[derive(Parser)]
#[command(name = "m10_gate", about = "UC v2 M10 gate: observable cluster")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Row 1: /metrics contract coverage on a serving 3-node cluster.
    Coverage(RootArgs),
    /// Row 2: readyz/flags probe regime across a real leader kill.
    Probes(RootArgs),
    /// Row 3, SMOKE ONLY: scrape-on vs scrape-off commit rate, one dev box.
    #[command(name = "perturb-smoke")]
    PerturbSmoke(PerturbArgs),
    /// Rows 1-3 in sequence: bar, then PASS/FAIL, exit 1 on any GATED FAIL.
    All(PerturbArgs),
}

#[derive(clap::Args)]
struct RootArgs {
    /// Real-filesystem root for instance dirs (never /tmp — RAM tmpfs).
    #[arg(long)]
    root: Option<PathBuf>,
}

#[derive(clap::Args)]
struct PerturbArgs {
    #[arg(long)]
    root: Option<PathBuf>,
    /// Seconds of load in each perturb-smoke measurement window.
    #[arg(long, default_value_t = 6)]
    secs: u64,
}

fn main() {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Coverage(a) => {
            let root = resolve_root(a.root);
            print_bar();
            let v = run_coverage(&root.join("coverage"));
            report_one(&v);
        }
        Cmd::Probes(a) => {
            let root = resolve_root(a.root);
            print_bar();
            let v = run_probes(&root.join("probes"));
            report_one(&v);
        }
        Cmd::PerturbSmoke(a) => {
            let root = resolve_root(a.root);
            run_perturb_smoke(&root.join("perturb"), a.secs);
        }
        Cmd::All(a) => {
            let root = resolve_root(a.root);
            print_bar();
            println!();
            let v1 = run_coverage(&root.join("coverage"));
            let v2 = run_probes(&root.join("probes"));
            run_perturb_smoke(&root.join("perturb"), a.secs);

            println!("\n== M10 gate local results ==");
            let mut all_pass = true;
            for v in [&v1, &v2] {
                println!(
                    "  [{}] {} — {}",
                    if v.pass { "PASS" } else { "FAIL" },
                    v.row,
                    v.detail
                );
                all_pass &= v.pass;
            }
            println!(
                "  [INFO] 3 scrape perturbation — printed above as SMOKE, ungated (fleet-only bar)"
            );
            println!(
                "  [INFO] 4 alert rules — run separately: scripts/m10_alert_fire.sh (Task 9; \
                 13/13 PASS recorded in the gate doc)"
            );
            if all_pass {
                println!(
                    "RESULT: PASS (local rows 1-2; row 3 is smoke, row 4 is a separate script)"
                );
            } else {
                println!("RESULT: FAIL (honest) — a locally-gated row (1 or 2) did not hold.");
                std::process::exit(1);
            }
        }
    }
}

fn resolve_root(root: Option<PathBuf>) -> PathBuf {
    let root = root.unwrap_or_else(default_root);
    assert!(
        !root.starts_with("/tmp"),
        "m10_gate: root must be on a real filesystem (never /tmp — RAM tmpfs); got {root:?}"
    );
    std::fs::create_dir_all(&root).expect("create root");
    root
}

/// `target/<profile>/examples/m10_gate` -> `target/<profile>/m10_gate_scratch`
/// — real disk (this workspace's shared target-dir lives under
/// `/home/claude/.cache/cargo-target`, not `/tmp`), never a hardcoded path.
fn default_root() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|exe| {
            exe.parent()
                .and_then(|p| p.parent())
                .map(|p| p.join("m10_gate_scratch"))
        })
        .unwrap_or_else(|| PathBuf::from("target/m10_gate_scratch"))
}

fn report_one(v: &Verdict) {
    println!(
        "\n  [{}] {} — {}",
        if v.pass { "PASS" } else { "FAIL" },
        v.row,
        v.detail
    );
    if v.pass {
        println!("RESULT: PASS (local)");
    } else {
        println!("RESULT: FAIL (honest)");
        std::process::exit(1);
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

// --------------------------------------------------------- no-op state machine

/// Trivial state machine so a cluster's nodes have an attached service —
/// `/readyz` checks `service_heartbeat_ns` staleness, and that field is only
/// ever stamped by `uc_service`'s apply agent (`uc_service/src/apply.rs`).
/// Without a service, `/readyz` reads permanently stale and never returns
/// 200, which would make row 2's "some node serves 200 within 5s" bar
/// unsatisfiable by construction, not by the property under test.
struct NoopSm;
impl StateMachine for NoopSm {
    type Command = ();
    type Response = ();
    type Query = ();
    type QueryResponse = ();
    fn apply(&mut self, _position: u64, _cmd: ()) {}
    fn query(&self, _q: ()) {}
    fn last_applied(&self) -> Option<u64> {
        None
    }
}

// ------------------------------------------------------------- row 1: coverage

/// Boundary-matched family presence — see the identical helper (and its
/// rationale: `uc2_term` is a prefix of `uc2_term_change_discards_total`, so
/// a bare `contains` would pass even if `uc2_term` never rendered) in
/// `uc_node/src/obs/metrics.rs`'s `every_contract_series_is_present` test.
fn series_present(text: &str, name: &str) -> bool {
    text.contains(&format!("\n{name} "))
        || text.contains(&format!("\n{name}{{"))
        || text.contains(&format!("\n# TYPE {name} "))
}

fn run_coverage(scratch_root: &Path) -> Verdict {
    let _ = std::fs::create_dir_all(scratch_root);
    println!("== row 1: /metrics coverage ==");
    let (_dir, mut nodes) = spawn_cluster(scratch_root, "coverage", 3, ADMISSION_BYTES);
    let leader_idx = await_stable_leader(&nodes, 20);
    println!("leader elected: n{leader_idx}");

    // Submit some load so peer slots occupy with real, non-default values —
    // a coverage check that never touches the cluster could pass on a
    // family whose *rendering* is broken but whose zero value looks fine.
    for _ in 0..500 {
        let _ = nodes[leader_idx].n().submit(vec![0u8; 64]);
    }
    thread::sleep(Duration::from_millis(300));

    // M11 (Task 5): free_disk_bytes's only writer is the real daemon's ~1s
    // loop (see uc2-node.rs), absent here (this cluster runs in-process
    // Node/ObsServer, no daemon binary) — store it directly so the coverage
    // row doesn't fail on an honestly-omitted-when-0 family. The field
    // itself is real; only its writer lives outside this harness.
    nodes[leader_idx]
        .n()
        .observability()
        .cnc
        .store_free_disk_bytes(1);

    let addr = nodes[leader_idx].obs_addr();
    let body = scrape(addr);

    let mut missing = Vec::new();
    for name in CONTRACT_SERIES {
        if !series_present(&body, name) {
            missing.push(*name);
            println!("  MISSING family: {name}");
        }
    }
    let peer_occupied = body.contains("uc2_peer_reported_durable_bytes{peer=");
    if !peer_occupied {
        println!(
            "  MISSING: no occupied uc2_peer_reported_durable_bytes{{peer=...}} sample on the leader"
        );
    }

    for n in nodes.iter_mut() {
        n.stop();
    }

    let pass = missing.is_empty() && peer_occupied;
    let detail = if pass {
        format!(
            "all {} CONTRACT_SERIES families present on the leader's scrape; >=1 occupied \
             per-peer series confirmed",
            CONTRACT_SERIES.len()
        )
    } else {
        format!(
            "{} of {} families missing ({missing:?}); peer series occupied: {peer_occupied}",
            missing.len(),
            CONTRACT_SERIES.len()
        )
    };
    Verdict::new("1 /metrics coverage", pass, detail)
}

// -------------------------------------------------------- row 2: probes

fn run_probes(scratch_root: &Path) -> Verdict {
    let _ = std::fs::create_dir_all(scratch_root);
    println!("\n== row 2: probe regime across a leader kill ==");
    let (_dir, mut nodes) = spawn_cluster(scratch_root, "probes", 3, ADMISSION_BYTES);

    // Every node needs an attached service — see NoopSm's doc comment.
    let mut svcs: Vec<Service<NoopSm>> = Vec::with_capacity(nodes.len());
    for n in &nodes {
        svcs.push(
            ServiceBuilder::new(ServiceConfig::new(&n.instance_dir, APP), NoopSm)
                .start()
                .expect("service attaches"),
        );
    }
    for n in &nodes {
        wait_ready(n.obs_addr(), 20);
    }

    let leader_idx = await_stable_leader(&nodes, 20);
    println!("leader elected: n{leader_idx}");

    // A little load before the kill, so the cluster isn't idle when it happens.
    for _ in 0..200 {
        let _ = nodes[leader_idx].n().submit(vec![0u8; 64]);
    }
    thread::sleep(Duration::from_millis(100));

    let survivors: Vec<usize> = (0..nodes.len()).filter(|&i| i != leader_idx).collect();
    // Open the dual read-only cnc attaches ONCE, before the kill — the point
    // of the row is a second, INDEPENDENT read of the flags in the same
    // sample as the HTTP call, not a fresh mmap open on every poll.
    let cnc_reads: Vec<std::sync::Arc<CncPage>> = survivors
        .iter()
        .map(|&i| {
            CncPage::open_file(&nodes[i].instance_dir.join("cnc2.dat"), APP)
                .expect("second read-only cnc attach")
        })
        .collect();

    println!("Node::crash() -> n{leader_idx}");
    // The crashed node's own Service (svcs[leader_idx]) is left running,
    // orphaned against a node that stopped writing — harmless (its apply
    // agent just idles), and it is stopped along with every other service
    // in the cleanup below.
    nodes[leader_idx].crash();

    let deadline = Instant::now() + Duration::from_secs(5);
    let t0 = Instant::now();
    let mut samples = 0u64;
    let mut violations = 0u64;
    let mut windowed_samples = 0u64;
    let mut ready_at: Option<Duration> = None;

    while Instant::now() < deadline {
        for (k, &i) in survivors.iter().enumerate() {
            // Modest load: keep submitting through the transition. Harmless
            // no-op (Err(NotServing)) on whichever survivor isn't leader yet.
            let _ = nodes[i].n().submit(vec![0u8; 64]);

            let addr = nodes[i].obs_addr();
            let status = get_status(addr, "/readyz");
            let flags = cnc_reads[k].status().flags.load_acquire();
            let elected_not_serving =
                (flags & NODE_FLAG_LEADER != 0) && (flags & NODE_FLAG_CAN_SERVE == 0);
            samples += 1;
            if elected_not_serving {
                // Non-vacuity: this row proves nothing if the sampler never
                // actually lands a sample inside the 0x01 window it exists
                // to police (same concern row 1 names for zero occupied
                // peer slots) — count it so the verdict can require >0.
                windowed_samples += 1;
            }
            if status == 200 && elected_not_serving {
                violations += 1;
                println!(
                    "  VIOLATION n{i}: readyz=200 while flags=0x{flags:02x} (elected, not CAN_SERVE)"
                );
            }
            if status == 200 && ready_at.is_none() {
                ready_at = Some(t0.elapsed());
            }
        }
        thread::sleep(Duration::from_millis(5));
    }

    for &i in &survivors {
        nodes[i].stop();
    }
    // svcs for the crashed leader and every survivor: best-effort stop.
    for svc in svcs {
        svc.stop();
    }

    let some_ready = ready_at.is_some();
    // Non-vacuous: the check proves nothing about the 0x01 window unless the
    // sampler actually landed at least one sample inside it.
    let non_vacuous = windowed_samples > 0;
    let pass = violations == 0 && some_ready && non_vacuous;
    let detail = format!(
        "{samples} samples across {} survivor(s) over 5s ({windowed_samples} landed inside the \
         elected-not-serving 0x01 window), {violations} violation(s) (readyz=200 coinciding with \
         flags=0x01); first 200 at {} (bar: zero violations, some node 200 within 5s, and the \
         window must have been sampled at all)",
        survivors.len(),
        ready_at
            .map(|d| format!("{:.3}s", d.as_secs_f64()))
            .unwrap_or_else(|| "never".into()),
    );
    if !non_vacuous {
        println!(
            "  WARNING: zero samples landed inside the 0x01 window — the zero-violation result \
             would be vacuous"
        );
    }
    Verdict::new("2 probe regime across a leader kill", pass, detail)
}

// ------------------------------------------------ row 3: perturbation smoke

fn run_perturb_smoke(scratch_root: &Path, secs: u64) {
    let _ = std::fs::create_dir_all(scratch_root);
    println!("\n== row 3: scrape perturbation (SMOKE — see gate doc; rate bars are fleet-only) ==");
    let (_dir, mut nodes) = spawn_cluster(scratch_root, "perturb", 3, ADMISSION_BYTES);
    let leader_idx = await_stable_leader(&nodes, 20);
    println!("leader elected: n{leader_idx}");

    let with_scrape = commit_rate_window(&nodes, leader_idx, secs, true);
    let without_scrape = commit_rate_window(&nodes, leader_idx, secs, false);

    for n in nodes.iter_mut() {
        n.stop();
    }

    let delta_pct = if without_scrape > 0.0 {
        (without_scrape - with_scrape) / without_scrape * 100.0
    } else {
        0.0
    };
    println!(
        "SMOKE (ungated — see gate doc; rate bars are fleet-only): scrape-on {with_scrape:.0} \
         B/s, scrape-off {without_scrape:.0} B/s, delta {delta_pct:.1}% \
         (fleet bar: ratio >= 0.95, i.e. delta <= 5%)"
    );
}

/// Busy-submit into the leader for `secs` seconds, optionally scraping every
/// node's `/metrics` at ~1 Hz throughout, and return the cluster's commit
/// rate over the window (bytes/sec, measured off the furthest-along node —
/// the same "cluster progress" definition `m9_gate.rs` uses).
fn commit_rate_window(nodes: &[NodeH], leader_idx: usize, secs: u64, scrape_on: bool) -> f64 {
    let c0 = max_commit(nodes);
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut next_scrape = Instant::now();
    while Instant::now() < deadline {
        let _ = nodes[leader_idx].n().submit(vec![0u8; 128]);
        if scrape_on && Instant::now() >= next_scrape {
            for n in nodes {
                let _ = scrape(n.obs_addr());
            }
            next_scrape = Instant::now() + Duration::from_secs(1);
        }
    }
    let c1 = max_commit(nodes);
    c1.saturating_sub(c0) as f64 / secs as f64
}

fn max_commit(nodes: &[NodeH]) -> u64 {
    nodes
        .iter()
        .map(|n| n.n().observability().cnc.counters().commit.load_acquire())
        .max()
        .unwrap_or(0)
}

// ============================================================ cluster kit
//
// Copied (not imported — examples cannot import each other) from
// `uc_node/examples/m10_alerts.rs`, itself derived from
// `uc_node/tests/failover.rs`.

struct NodeH {
    instance_dir: PathBuf,
    node: Option<Node>,
    obs: Option<ObsServer>,
}

impl NodeH {
    fn n(&self) -> &Node {
        self.node.as_ref().expect("node stopped")
    }
    fn obs_addr(&self) -> SocketAddr {
        self.obs.as_ref().expect("obs server stopped").local_addr()
    }
    fn stop(&mut self) {
        if let Some(obs) = self.obs.take() {
            obs.stop();
        }
        if let Some(node) = self.node.take() {
            node.stop();
        }
    }
    fn crash(&mut self) {
        if let Some(obs) = self.obs.take() {
            obs.stop();
        }
        if let Some(node) = self.node.take() {
            node.crash();
        }
    }
}

fn seed_for(i: usize) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn make_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    instance_dir: PathBuf,
    seed: u64,
    addr: SocketAddr,
    buffer_bytes: usize,
    admission_bytes: u64,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        learners: Vec::new(),
        bind: addr,
        instance_dir,
        app_id: APP.into(),
        buffer_bytes,
        max_payload: MAX_PAYLOAD,
        admission_bytes,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed,
        faults: FaultConfig::default(),
        purge: uc_node::PurgePolicy::Disabled,
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::default(),
    }
}

/// Bind every node's socket first (so the full member map is known before
/// any agent runs), then start each on its pre-bound socket with a real obs
/// HTTP server attached.
fn spawn_cluster(
    scratch_root: &Path,
    label: &str,
    n: usize,
    admission_bytes: u64,
) -> (tempfile::TempDir, Vec<NodeH>) {
    let dir = tempfile::Builder::new()
        .prefix(&format!("m10-gate-{label}-"))
        .tempdir_in(scratch_root)
        .expect("tempdir");

    let socks: Vec<UdpSocket> = (0..n)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind"))
        .collect();
    let members: Vec<(NodeId, SocketAddr)> = socks
        .iter()
        .enumerate()
        .map(|(i, s)| (i as NodeId, s.local_addr().unwrap()))
        .collect();

    let mut nodes = Vec::with_capacity(n);
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = members[i].1;
        let instance_dir = dir.path().join(format!("n{i}"));
        let cfg = make_config(
            i as NodeId,
            members.clone(),
            instance_dir.clone(),
            seed_for(i),
            addr,
            RING_BYTES,
            admission_bytes,
        );
        let node = Node::start_with_socket(cfg, sock).expect("start");
        let obs = ObsServer::serve(node.observability(), "127.0.0.1:0".parse().unwrap())
            .expect("bind obs");
        nodes.push(NodeH {
            instance_dir,
            node: Some(node),
            obs: Some(obs),
        });
    }
    (dir, nodes)
}

fn await_single_leader(nodes: &[NodeH], secs: u64) -> usize {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let serving: Vec<usize> = (0..nodes.len())
            .filter(|&i| nodes[i].node.is_some() && nodes[i].n().can_serve())
            .collect();
        assert!(
            serving.len() <= 1,
            "split-brain: nodes {serving:?} all serve"
        );
        if serving.len() == 1 {
            return serving[0];
        }
        assert!(Instant::now() < deadline, "no single leader elected");
        thread::sleep(Duration::from_millis(15));
    }
}

/// Like `await_single_leader`, but also waits for that leader to hold still
/// before returning — see `m10_alerts.rs`'s doc comment on the same helper
/// for why (a 4-core dev box running 12+ busy-polling agent threads can see
/// a spurious re-election right after the first leader settles).
fn await_stable_leader(nodes: &[NodeH], secs: u64) -> usize {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let l = await_single_leader(nodes, secs);
        let check_until = Instant::now() + Duration::from_millis(4000);
        let mut stable = true;
        while Instant::now() < check_until {
            if !(nodes[l].node.is_some() && nodes[l].n().can_serve()) {
                stable = false;
                break;
            }
            thread::sleep(Duration::from_millis(15));
        }
        if stable {
            return l;
        }
        assert!(Instant::now() < deadline, "leader never stabilized");
    }
}

// ------------------------------------------------------------- HTTP scrape
//
// Copied (not imported) from `uc_node/examples/m10_alerts.rs`'s minimal
// blocking HTTP/1.1 GET client, same shape as `uc_node/tests/obs_http.rs`.

fn scrape(addr: SocketAddr) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect to obs server");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set_read_timeout");
    write!(
        stream,
        "GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .expect("write request");
    stream.flush().expect("flush request");

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read response");
    let text = String::from_utf8_lossy(&raw).to_string();
    let mut halves = text.splitn(2, "\r\n\r\n");
    let _head = halves.next().unwrap_or("");
    halves.next().unwrap_or("").to_string()
}

fn get_status(addr: SocketAddr, path: &str) -> u16 {
    let mut stream = match TcpStream::connect(addr) {
        Ok(s) => s,
        Err(_) => return 0,
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    if write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .is_err()
    {
        return 0;
    }
    let _ = stream.flush();
    let mut raw = Vec::new();
    if stream.read_to_end(&mut raw).is_err() {
        return 0;
    }
    let text = String::from_utf8_lossy(&raw);
    text.lines()
        .next()
        .unwrap_or("")
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0)
}

fn wait_ready(addr: SocketAddr, secs: u64) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if get_status(addr, "/readyz") == 200 {
            return;
        }
        assert!(Instant::now() < deadline, "never became ready");
        thread::sleep(Duration::from_millis(15));
    }
}
