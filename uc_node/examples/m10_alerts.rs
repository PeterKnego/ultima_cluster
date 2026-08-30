// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M10 Task 9: fire every shipped alert rule (`packaging/prometheus/uc2-alerts.yml`)
//! against a deliberately broken cluster.
//!
//! Method (per the task brief + controller ruling): each scenario constructs
//! the failure — a real in-process cluster (fault-injection partitions,
//! `Node::crash()`, a stopped service) where that's honestly constructible,
//! else a synthetic `ObsSources` with the failure state injected directly —
//! and scrapes every node's REAL `/metrics` HTTP endpoint (the same
//! `ObsServer` production runs) once per second over a short bounded real
//! window (~10-15s). The `.series` files this binary writes carry those raw
//! scraped values, unmodified. Turning them into `promtool` `for:`/range-
//! window-sized timelines (holding the last real value constant, or placing
//! a real jump inside a delta range) is `scripts/m10_alert_fire.sh`'s job —
//! that's the "time-dilation" step, and it never touches what got scraped.
//!
//! Every rule fires through the real exporter; which scenarios are REAL
//! cluster brokenness vs SYNTHETIC injected state is disclosed on stdout,
//! one line per scenario, and mirrored in the gate doc (Task 10). This
//! mirrors the elle gate's precedent of recording structural-can't-expose
//! findings rather than pretending coverage (see `docs/benchmarks/uc2-elle-gate-2026-07-16.md`).

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;

use uc_consensus::election::NodeId;
use uc_log::cnc::{CncMeta, CncPage};
use uc_net::fault::FaultConfig;
use uc_net::receiver::FollowerStats;
use uc_net::sender::SenderStats;
use uc_node::obs::ObsSources;
use uc_node::obs::http::ObsServer;
use uc_node::{Node, NodeConfig};
use uc_service::{ServiceBuilder, ServiceConfig, StateMachine};
use uc_protocol::v2::cnc::NODE_FLAG_LEADER;

const APP: &str = "m10-alerts";
const RING_BYTES: usize = 1 << 20;
const MAX_PAYLOAD: usize = 256;

const ALL_SCENARIOS: &[&str] = &[
    "agent_dead",
    "no_leader",
    "leader_not_serving",
    "service_wedged",
    "leader_isolated",
    "peer_never_heard",
    "follower_partitioned",
    "purge_stalled",
    "repeated_wipes",
    "crypto_counters",
    "disk_low",
    "service_absent",
    "fsm_pinned",
];

// ------------------------------------------------------------------ CLI

#[derive(Parser)]
#[command(name = "m10_alerts", about = "UC v2 M10: fire every shipped alert rule once")]
struct Cli {
    /// Run one named scenario (see ALL_SCENARIOS).
    #[arg(long)]
    scenario: Option<String>,
    /// Run every scenario.
    #[arg(long)]
    all: bool,
    /// Output dir for the `.series` files. Real disk only — never `/tmp`
    /// (RAM-backed tmpfs, no swap on this box). Defaults under `$HOME/.cache`.
    #[arg(long)]
    out: Option<PathBuf>,
}

fn main() {
    let cli = Cli::parse();
    let out_dir = cli.out.unwrap_or_else(default_out_dir);
    fs::create_dir_all(&out_dir).expect("create out dir");
    let scratch_root = out_dir.join("_scratch");
    fs::create_dir_all(&scratch_root).expect("create scratch dir");

    let names: Vec<String> = if cli.all {
        ALL_SCENARIOS.iter().map(|s| s.to_string()).collect()
    } else {
        vec![cli.scenario.expect("--scenario NAME or --all")]
    };
    let multi = names.len() > 1;

    // Fix round 1, Finding 1: one scenario's panic (e.g. `await_stable_leader`
    // timing out on a loaded box, or the UC2_M10_FORCE_FAIL test hook below)
    // used to take down the WHOLE `--all` run before it reached the promtool
    // step — a gate script has to always end in per-rule verdict lines, never
    // a bare backtrace. In `--all` mode each scenario runs behind
    // `catch_unwind`: a panicking scenario is reported by name and SKIPPED
    // (no `.series` file written for it — `scripts/m10_alert_fire.sh`'s
    // Python step turns that absence into `FAIL rule=... (scenario did not
    // produce series)` for every rule that scenario was to feed, per rule,
    // instead of aborting there too), and the run continues to the rest.
    // Single-scenario mode (`--scenario X`, not `--all`) still fails loudly
    // with a nonzero exit — there's only one scenario to isolate FROM.
    let mut failed_count = 0usize;
    for name in names {
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_scenario(&name, &scratch_root)));
        match result {
            Ok((sf, disc)) => {
                let path = out_dir.join(format!("{name}.series"));
                sf.write(&path).unwrap_or_else(|e| panic!("write {path:?}: {e}"));
                println!(
                    "scenario={} rules={} state={} method: {}",
                    disc.scenario,
                    disc.rules.join(","),
                    disc.state,
                    disc.method
                );
                println!("  wrote {} ({} series)", path.display(), sf.data.len());
            }
            Err(_) => {
                failed_count += 1;
                println!(
                    "scenario_failed name={name} — panicked (see panic message printed above by \
                     the default panic hook); no .series file written for this scenario"
                );
                if !multi {
                    std::process::exit(1);
                }
            }
        }
    }

    if failed_count > 0 {
        eprintln!(
            "m10_alerts: {failed_count} of {} scenario(s) panicked — exiting nonzero. \
             Downstream adjudication should turn each affected scenario's missing .series file \
             into a per-rule FAIL, not abort.",
            ALL_SCENARIOS.len()
        );
        std::process::exit(1);
    }
}

fn default_out_dir() -> PathBuf {
    let home = std::env::var("HOME").expect("HOME set");
    PathBuf::from(home).join(".cache").join("uc2-m10-alerts")
}

/// Test-only forced-failure hook (Fix round 1, Finding 1's validation path):
/// `UC2_M10_FORCE_FAIL=<scenario>` makes that one scenario panic
/// deliberately, so the `--all` isolation path above can be exercised
/// end-to-end without waiting for a real flake. Checked for every scenario
/// (both `--scenario` and `--all` modes) so either invocation can be used to
/// validate it. Documented, not removed — cheap to keep, useful for any
/// future regression in the isolation path.
fn run_scenario(name: &str, scratch_root: &Path) -> (SeriesFile, Disclosure) {
    if std::env::var("UC2_M10_FORCE_FAIL").as_deref() == Ok(name) {
        panic!("UC2_M10_FORCE_FAIL={name} — deliberate forced failure for scenario-isolation testing");
    }
    match name {
        "agent_dead" => scenario_agent_dead(),
        "no_leader" => scenario_no_leader(scratch_root),
        "leader_not_serving" => scenario_leader_not_serving(),
        "service_wedged" => scenario_service_wedged(scratch_root),
        "leader_isolated" => scenario_leader_isolated(scratch_root),
        "peer_never_heard" => scenario_peer_never_heard(scratch_root),
        "follower_partitioned" => scenario_follower_partitioned(scratch_root),
        "purge_stalled" => scenario_purge_stalled(),
        "repeated_wipes" => scenario_repeated_wipes(),
        "crypto_counters" => scenario_crypto_counters(),
        "disk_low" => scenario_disk_low(),
        "service_absent" => scenario_service_absent(scratch_root),
        "fsm_pinned" => scenario_fsm_pinned(scratch_root),
        other => panic!("unknown scenario {other:?} — one of {ALL_SCENARIOS:?}"),
    }
}

// ------------------------------------------------------------- disclosure

struct Disclosure {
    scenario: &'static str,
    rules: &'static [&'static str],
    state: &'static str, // "real" | "synthetic"
    method: String,
}

// ------------------------------------------------------------ series file

/// One row per (metric family, full label set incl. `instance`), values in
/// scrape order. Written as `<name>{<labels>}<TAB><v1> <v2> ...`.
struct SeriesFile {
    data: BTreeMap<(String, String), Vec<f64>>,
}

impl SeriesFile {
    fn new() -> Self {
        SeriesFile { data: BTreeMap::new() }
    }

    /// Parse one scrape body and record every sample whose family is in
    /// `families`, tagging it with `instance` (Prometheus's own job — this
    /// harness adds it because there's no real scrape config here).
    fn record_round(&mut self, instance: &str, body: &str, families: &[&str]) {
        for (name, labels, v) in parse_metrics(body) {
            if !families.contains(&name.as_str()) {
                continue;
            }
            let labels_full = if labels.is_empty() {
                format!("instance=\"{instance}\"")
            } else {
                format!("{labels},instance=\"{instance}\"")
            };
            self.data.entry((name, labels_full)).or_default().push(v);
        }
    }

    fn write(&self, path: &Path) -> std::io::Result<()> {
        let mut out = String::new();
        for ((name, labels), values) in &self.data {
            let vals: Vec<String> = values.iter().map(|v| fmt_val(*v)).collect();
            out.push_str(name);
            out.push('{');
            out.push_str(labels);
            out.push('}');
            out.push('\t');
            out.push_str(&vals.join(" "));
            out.push('\n');
        }
        fs::write(path, out)
    }
}

fn fmt_val(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 { format!("{}", v as i64) } else { format!("{v}") }
}

/// Minimal Prometheus text-format line parser: `name{labels} value` or
/// `name value`. Skips `#` comment/type lines and blanks.
fn parse_metrics(text: &str) -> Vec<(String, String, f64)> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim_end();
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        if let Some(brace) = line.find('{') {
            let name = line[..brace].to_string();
            if let Some(close_rel) = line[brace..].find('}') {
                let close = brace + close_rel;
                let labels = line[brace + 1..close].to_string();
                let rest = line[close + 1..].trim();
                if let Ok(v) = rest.parse::<f64>() {
                    out.push((name, labels, v));
                }
            }
        } else {
            let mut it = line.splitn(2, ' ');
            let name = it.next().unwrap_or("").to_string();
            let rest = it.next().unwrap_or("").trim();
            if let Ok(v) = rest.parse::<f64>() {
                out.push((name, String::new(), v));
            }
        }
    }
    out
}

// ------------------------------------------------------------- HTTP scrape

/// A minimal blocking HTTP/1.1 GET client — same shape as
/// `uc_node/tests/obs_http.rs`'s `get` helper, copied (examples can't
/// import test code).
fn scrape(addr: SocketAddr) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect to obs server");
    stream.set_read_timeout(Some(Duration::from_secs(5))).expect("set_read_timeout");
    write!(stream, "GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
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
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(5))).expect("timeout");
    write!(stream, "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("write");
    stream.flush().expect("flush");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read");
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

// --------------------------------------------------------- synthetic build

/// Build a bare `ObsSources` over a heap-backed `CncPage`, matching the
/// `synthetic_sources()` fixture in `uc_node/src/obs/metrics.rs` — copied,
/// not imported (examples can't reach `#[cfg(test)]` code).
fn synthetic_sources(node_id: u32) -> ObsSources {
    let meta = CncMeta {
        node_id,
        instance_id: 0xC0FF_EE00_0000_0000u128 | node_id as u128,
        app_id: APP.into(),
        buffer_bytes: RING_BYTES as u64,
        max_payload: 1200,
    };
    let cnc = CncPage::heap(&meta);
    ObsSources {
        node_id,
        cnc,
        sender: Arc::new(SenderStats::default()),
        receiver: Arc::new(FollowerStats::default()),
        truncations: Arc::new(AtomicU64::new(0)),
        wipes: Arc::new(AtomicU64::new(0)),
        reports_unattested: Arc::new(AtomicU64::new(0)),
        reports_implausible: Arc::new(AtomicU64::new(0)),
        crypto_handshake_failures: Arc::new(AtomicU64::new(0)),
        crypto_enabled: false,
        purge_enabled: false,
        journal_segment_bytes: 64 << 20,
        agents: vec![
            ("consensus", Arc::new(AtomicBool::new(false))),
            ("sender", Arc::new(AtomicBool::new(false))),
            ("receiver", Arc::new(AtomicBool::new(false))),
            ("archive", Arc::new(AtomicBool::new(false))),
        ],
    }
}

// ------------------------------------------------------------ scenario 1

/// Uc2AgentDead — **synthetic, disclosed**. Flipping a real agent to
/// fail-stopped in-process (e.g. poisoning the journal dir mid-run) is a
/// much bigger harness than an alert test warrants; the brief names the
/// flag-flip as the simpler, honest option. Real exporter, real HTTP scrape.
fn scenario_agent_dead() -> (SeriesFile, Disclosure) {
    let sources = synthetic_sources(0);
    // agents = [consensus, sender, receiver, archive] — flip archive's flag
    // true, which the encoder reads as fail-stopped (renders alive=0).
    sources.agents[3].1.store(true, Ordering::Release);

    let srv = ObsServer::serve(sources.clone(), "127.0.0.1:0".parse().unwrap()).expect("bind");
    let addr = srv.local_addr();
    let mut sf = SeriesFile::new();
    for _ in 0..3 {
        sf.record_round("n0", &scrape(addr), &["uc2_agent_alive"]);
        thread::sleep(Duration::from_millis(200));
    }
    srv.stop();

    (
        sf,
        Disclosure {
            scenario: "agent_dead",
            rules: &["Uc2AgentDead"],
            state: "synthetic",
            method: "synthetic ObsSources on a heap CncPage: the archive agent's liveness \
                     AtomicBool flipped directly (not a real fail-stop), rendered through the \
                     real /metrics HTTP exporter and scraped 3x over a live TCP connection."
                .into(),
        },
    )
}

// ------------------------------------------------------------ scenario 2

/// Uc2NoLeader — **real**. 3-node cluster, crash the leader and one
/// follower; the lone survivor cannot reach quorum and never re-elects.
fn scenario_no_leader(scratch_root: &Path) -> (SeriesFile, Disclosure) {
    let (_dir, mut nodes) = spawn_cluster(scratch_root, "no-leader", 3, 256 * 1024);
    let leader_idx = await_stable_leader(&nodes, 20);
    let follower_idx = (0..nodes.len()).find(|&i| i != leader_idx).expect("a follower exists");

    nodes[leader_idx].crash();
    nodes[follower_idx].crash();

    let survivor_idx =
        (0..nodes.len()).find(|&i| i != leader_idx && i != follower_idx).expect("a survivor exists");
    // Give the survivor time to notice it can't win an election (minority).
    thread::sleep(Duration::from_secs(2));

    let addr = nodes[survivor_idx].obs_addr();
    let instance = format!("n{survivor_idx}");
    let mut sf = SeriesFile::new();
    for _ in 0..12 {
        sf.record_round(&instance, &scrape(addr), &["uc2_is_leader"]);
        thread::sleep(Duration::from_secs(1));
    }
    nodes[survivor_idx].stop();

    (
        sf,
        Disclosure {
            scenario: "no_leader",
            rules: &["Uc2NoLeader"],
            state: "real",
            method: format!(
                "real 3-node cluster; Node::crash() the leader and one follower — the lone \
                 survivor (n{survivor_idx}) is a minority and never re-elects. 12 real 1s \
                 scrapes of its /metrics all read uc2_is_leader=0; the cluster-scoped rule's \
                 max() over just this one live series is exactly what Prometheus would see \
                 with the other two targets simply down (no series)."
            ),
        },
    )
}

// ------------------------------------------------------------ scenario 3

/// Uc2LeaderNotServing — **synthetic, disclosed**: flags 0x01 (elected, no
/// CAN_SERVE) served through the real exporter. The brief: the real state is
/// a sub-RTT race window the seeded fault layer can't cut selectively;
/// constructing it live is a consensus-harness project, not an alert test.
fn scenario_leader_not_serving() -> (SeriesFile, Disclosure) {
    let sources = synthetic_sources(0);
    sources.cnc.status().flags.store_release(NODE_FLAG_LEADER); // 0x01, no CAN_SERVE

    let srv = ObsServer::serve(sources.clone(), "127.0.0.1:0".parse().unwrap()).expect("bind");
    let addr = srv.local_addr();
    let mut sf = SeriesFile::new();
    for _ in 0..3 {
        sf.record_round("n0", &scrape(addr), &["uc2_is_leader", "uc2_can_serve"]);
        thread::sleep(Duration::from_millis(200));
    }
    srv.stop();

    (
        sf,
        Disclosure {
            scenario: "leader_not_serving",
            rules: &["Uc2LeaderNotServing"],
            state: "synthetic",
            method: "synthetic ObsSources: cnc status flags stored as NODE_FLAG_LEADER alone \
                     (0x01, no CAN_SERVE) — the elected-but-not-yet-quorum-committed window — \
                     rendered through the real exporter."
                .into(),
        },
    )
}

// ------------------------------------------------------------ scenario 4

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

/// Uc2ServiceWedged — **real**. A real node + a real attached service; stop
/// the service's apply agent while the node keeps running.
fn scenario_service_wedged(scratch_root: &Path) -> (SeriesFile, Disclosure) {
    let (_dir, mut nodes) = spawn_cluster(scratch_root, "svc-wedged", 1, 256 * 1024);
    await_stable_leader(&nodes, 20);

    let instance_dir = nodes[0].instance_dir.clone();
    let svc = ServiceBuilder::new(ServiceConfig::new(&instance_dir, APP), NoopSm)
        .start()
        .expect("service attaches");
    let addr = nodes[0].obs_addr();
    wait_ready(addr, 10);
    svc.stop(); // apply agent stops; node keeps running.

    let mut sf = SeriesFile::new();
    for _ in 0..12 {
        sf.record_round(
            "n0",
            &scrape(addr),
            &["uc_service_heartbeat_age_seconds", "uc_node_heartbeat_age_seconds"],
        );
        thread::sleep(Duration::from_secs(1));
    }
    nodes[0].stop();

    (
        sf,
        Disclosure {
            scenario: "service_wedged",
            rules: &["Uc2ServiceWedged"],
            state: "real",
            method: "real single-node cluster + a real attached service; Service::stop() halts \
                     the apply agent while the node keeps polling. 12 real 1s scrapes show \
                     service_heartbeat_age_seconds climbing past 5s while \
                     node_heartbeat_age_seconds stays under 3s."
                .into(),
        },
    )
}

// ------------------------------------------------------------ scenario 5

/// Uc2ReplicationStalled + Uc2AdmissionSaturated — **real**. 3-node cluster
/// under load; partition the leader from BOTH followers, so it keeps
/// appending into a (deliberately tiny) admission window while commit stalls.
fn scenario_leader_isolated(scratch_root: &Path) -> (SeriesFile, Disclosure) {
    let admission_bytes = 8 * 1024;
    let (_dir, mut nodes) = spawn_cluster(scratch_root, "leader-isolated", 3, admission_bytes);
    let leader_idx = await_stable_leader(&nodes, 20);
    let others: Vec<usize> = (0..nodes.len()).filter(|&i| i != leader_idx).collect();
    for &o in &others {
        partition_pair(&nodes, leader_idx, o);
    }

    let addr = nodes[leader_idx].obs_addr();
    let instance = format!("n{leader_idx}");
    let families =
        ["uc2_commit_bytes", "uc2_append_bytes", "uc2_admission_saturation", "uc2_admission_bytes"];
    let mut sf = SeriesFile::new();
    // Baseline BEFORE any load: append==commit==32 (just the NewTerm frame).
    // The busy-submit burst below floods the tiny window to its ceiling
    // within a single round, so without this baseline the whole captured
    // trace would already be flat at the plateau — this sample is what
    // proves the real JUMP (append rising while commit stays put), which
    // the dilation step places inside the delta range window.
    sf.record_round(&instance, &scrape(addr), &families);
    // Drive load into the isolated leader while sampling — the append
    // counter climbs (until the admission window closes it), commit never
    // moves (no quorum reachable). A single submit per round is far too
    // sparse against an 8 KiB window (~1 KiB/round at 500ms cadence never
    // saturates); busy-submit for most of each round instead.
    for _ in 0..12 {
        let round_deadline = Instant::now() + Duration::from_millis(450);
        while Instant::now() < round_deadline {
            let _ = nodes[leader_idx].n().submit(vec![0u8; 64]);
        }
        sf.record_round(&instance, &scrape(addr), &families);
        thread::sleep(Duration::from_millis(50));
    }

    for &o in &others {
        heal_pair(&nodes, leader_idx, o);
    }
    for n in nodes.iter_mut() {
        n.stop();
    }

    (
        sf,
        Disclosure {
            scenario: "leader_isolated",
            rules: &["Uc2ReplicationStalled", "Uc2AdmissionSaturated"],
            state: "real",
            method: format!(
                "real 3-node cluster, admission_bytes={admission_bytes} (deliberately tiny); \
                 partition the leader (n{leader_idx}) from BOTH followers and keep submitting — \
                 12 real ~0.5s-spaced scrapes show uc2_commit_bytes frozen while \
                 uc2_append_bytes climbs and uc2_admission_saturation crosses 1.0 as the window \
                 fills."
            ),
        },
    )
}

// ------------------------------------------------------------ scenario 6

/// Uc2PeerNeverHeard — **real**. A 3-member static config where member 2's
/// address is reserved but its node process never starts; the two running
/// nodes' `id_and_role` cnc slot for it is latched from the static config on
/// boot (occupied), but `reported_durable` is never written (never reports).
fn scenario_peer_never_heard(scratch_root: &Path) -> (SeriesFile, Disclosure) {
    let dir = tempfile::Builder::new()
        .prefix("m10-peer-never-heard-")
        .tempdir_in(scratch_root)
        .expect("tempdir");

    let socks: Vec<UdpSocket> =
        (0..3).map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind")).collect();
    let members: Vec<(NodeId, SocketAddr)> =
        socks.iter().enumerate().map(|(i, s)| (i as NodeId, s.local_addr().unwrap())).collect();

    let mut socks = socks;
    let sock2 = socks.pop().unwrap();
    drop(sock2); // member 2's address is known but its node never starts.
    let sock1 = socks.pop().unwrap();
    let sock0 = socks.pop().unwrap();

    let mut nodes = Vec::new();
    for (i, sock) in [(0usize, sock0), (1usize, sock1)] {
        let instance_dir = dir.path().join(format!("n{i}"));
        let cfg = make_config(
            i as NodeId,
            members.clone(),
            instance_dir.clone(),
            seed_for(i),
            members[i].1,
            RING_BYTES,
            256 * 1024,
            // Unchanged pre-M14c default declared set: `{0}`.
            uc_node::ServicesConfig::default(),
        );
        let node = Node::start_with_socket(cfg, sock).expect("start");
        let obs =
            ObsServer::serve(node.observability(), "127.0.0.1:0".parse().unwrap()).expect("bind");
        nodes.push(NodeH { addr: members[i].1, instance_dir, node: Some(node), obs: Some(obs) });
    }
    await_stable_leader(&nodes, 20);

    let mut sf = SeriesFile::new();
    for _ in 0..12 {
        for (i, n) in nodes.iter().enumerate() {
            sf.record_round(
                &format!("n{i}"),
                &scrape(n.obs_addr()),
                &["uc2_peer_reported_durable_bytes", "uc2_is_leader"],
            );
        }
        thread::sleep(Duration::from_secs(1));
    }
    for n in nodes.iter_mut() {
        n.stop();
    }

    (
        sf,
        Disclosure {
            scenario: "peer_never_heard",
            rules: &["Uc2PeerNeverHeard"],
            state: "real",
            method: "real 3-member static config; member 2's UDP address is bound-then-dropped \
                     (known, never started). n0/n1's cnc peer slot for id=2 is latched occupied \
                     from the static config on boot, but reported_durable is never written — 12 \
                     real 1s scrapes show uc2_peer_reported_durable_bytes{peer=\"2\"}=0 on both \
                     running nodes."
                .into(),
        },
    )
}

// ------------------------------------------------------------ scenario 7

/// Uc2PeerLagging — **real**. 3 nodes under load; partition ONE follower
/// (leader + the other follower keep a 2/3 quorum and keep committing), so
/// the partitioned follower's replication lag grows past the leader's
/// admission window while the leader itself stays healthy.
fn scenario_follower_partitioned(scratch_root: &Path) -> (SeriesFile, Disclosure) {
    let admission_bytes = 16 * 1024;
    let (_dir, mut nodes) = spawn_cluster(scratch_root, "peer-lagging", 3, admission_bytes);
    let leader_idx = await_stable_leader(&nodes, 20);
    let victim_idx = (0..nodes.len()).find(|&i| i != leader_idx).expect("a follower exists");

    // Isolate the victim from BOTH other nodes, not just from the leader.
    // Cutting ONLY leader<->victim was tried first and is UNSAFE for this
    // scenario: UC's election has no pre-vote/leader-stickiness check (a
    // candidate is granted a vote purely on term+log currency), so the
    // moment the victim's own heartbeat timer lapses (which happens almost
    // immediately once it can't hear the leader) it can canvass the THIRD
    // node and legitimately steal the term — even though that third node
    // was still hearing the real leader the whole time. Observed
    // reproducibly: leader<->victim-only partition flipped leadership TO
    // the victim within ~4s, every run. Isolating the victim from
    // everyone (a real network partition of that one follower) leaves the
    // leader<->other-follower pair genuinely undisturbed.
    let others: Vec<usize> = (0..nodes.len()).filter(|&i| i != victim_idx).collect();
    for &o in &others {
        partition_pair(&nodes, victim_idx, o);
    }

    let families = ["uc2_peer_replication_lag_bytes", "uc2_admission_bytes", "uc2_is_leader"];
    let mut sf = SeriesFile::new();
    let instance = format!("n{leader_idx}");
    let addr = nodes[leader_idx].obs_addr();
    sf.record_round(&instance, &scrape(addr), &families); // baseline: lag == 0
    for _ in 0..10 {
        for _ in 0..40 {
            let _ = nodes[leader_idx].n().submit(vec![0u8; 200]);
            thread::sleep(Duration::from_micros(500));
        }
        sf.record_round(&instance, &scrape(addr), &families);
        thread::sleep(Duration::from_millis(300));
    }

    for &o in &others {
        heal_pair(&nodes, victim_idx, o);
    }
    for n in nodes.iter_mut() {
        n.stop();
    }

    (
        sf,
        Disclosure {
            scenario: "follower_partitioned",
            rules: &["Uc2PeerLagging"],
            state: "real",
            method: format!(
                "real 3-node cluster, leader admission_bytes={admission_bytes}; fully isolate \
                 ONE follower (n{victim_idx}, both links) while the leader (n{leader_idx}) and \
                 the other follower keep committing under load — n{victim_idx} stops reporting \
                 durable positions, so the leader's view of \
                 uc2_peer_replication_lag_bytes{{peer=\"{victim_idx}\"}} grows past its own \
                 admission window."
            ),
        },
    )
}

// ------------------------------------------------------------ scenario 8

/// Uc2PurgeStalled — **synthetic, disclosed**: purge-enabled sources with
/// floor - first_base > 2 segments. Driving a real purge stall requires
/// breaking the archive mid-run — out of scope for an alert-firing test.
fn scenario_purge_stalled() -> (SeriesFile, Disclosure) {
    let sources = synthetic_sources(0);
    let segment_bytes = 1u64 << 20; // 1 MiB
    sources.cnc.snapshots().node_snapshot_floor.store_release(10 * segment_bytes);
    sources.cnc.archive_first_base().store_release(0);
    let mut sources = sources;
    sources.purge_enabled = true;
    sources.journal_segment_bytes = segment_bytes;

    let srv = ObsServer::serve(sources.clone(), "127.0.0.1:0".parse().unwrap()).expect("bind");
    let addr = srv.local_addr();
    let mut sf = SeriesFile::new();
    for _ in 0..3 {
        sf.record_round(
            "n0",
            &scrape(addr),
            &[
                "uc2_purge_enabled",
                "uc_node_snapshot_floor_bytes",
                "uc2_archive_first_base_bytes",
                "uc2_journal_segment_bytes",
            ],
        );
        thread::sleep(Duration::from_millis(200));
    }
    srv.stop();

    (
        sf,
        Disclosure {
            scenario: "purge_stalled",
            rules: &["Uc2PurgeStalled"],
            state: "synthetic",
            method: format!(
                "synthetic ObsSources: purge_enabled=true, node_snapshot_floor={} \
                 ({segment_bytes}-byte segments), archive_first_base=0 — floor - first_base = \
                 10 segments > the rule's 2-segment bar — rendered through the real exporter."
                    , 10 * segment_bytes
            ),
        },
    )
}

// ------------------------------------------------------------ scenario 9

/// Uc2RepeatedWipes — **synthetic, disclosed**: the counter is bumped
/// directly rather than driven through a real NoCommonPrefix wipe-and-rejoin
/// (no compact real scenario for that within this harness's budget — the
/// brief allows synthetic here). The TRANSITION itself is real: two live
/// HTTP scrapes of the same running exporter, before and after the bump.
fn scenario_repeated_wipes() -> (SeriesFile, Disclosure) {
    let sources = synthetic_sources(0);
    let srv = ObsServer::serve(sources.clone(), "127.0.0.1:0".parse().unwrap()).expect("bind");
    let addr = srv.local_addr();

    let mut sf = SeriesFile::new();
    sf.record_round("n0", &scrape(addr), &["uc2_wipes_total"]); // 0
    sources.wipes.fetch_add(3, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(200));
    for _ in 0..3 {
        sf.record_round("n0", &scrape(addr), &["uc2_wipes_total"]); // 3, held
        thread::sleep(Duration::from_millis(200));
    }
    srv.stop();

    (
        sf,
        Disclosure {
            scenario: "repeated_wipes",
            rules: &["Uc2RepeatedWipes"],
            state: "synthetic",
            method: "synthetic ObsSources: the wipes counter is a directly-injected atomic bump \
                     (not a real NoCommonPrefix wipe-and-rejoin), but the 0->3 TRANSITION is two \
                     genuine live HTTP scrapes of the real exporter, before and after the bump."
                .into(),
        },
    )
}

// ----------------------------------------------------------- scenario 10

/// Uc2UnattestedReports / Uc2CleartextPeer / Uc2FollowerSealFailures —
/// **synthetic, disclosed**: their triggering conditions (a 0.4.0 peer, a
/// mixed-crypto cluster, a corrupting adversary) are not honestly
/// constructible in-process, so the counters are bumped directly through the
/// real encoder — same real-transition-over-synthetic-state pattern as
/// `repeated_wipes`.
fn scenario_crypto_counters() -> (SeriesFile, Disclosure) {
    let sources = synthetic_sources(0); // is_leader defaults to 0 (flags=0).
    let srv = ObsServer::serve(sources.clone(), "127.0.0.1:0".parse().unwrap()).expect("bind");
    let addr = srv.local_addr();

    let families =
        ["uc2_reports_unattested_total", "uc2_cleartext_peer_datagrams_total", "uc2_receiver_seal_failures_total", "uc2_is_leader"];
    let mut sf = SeriesFile::new();
    sf.record_round("n0", &scrape(addr), &families); // all zero, is_leader=0

    sources.reports_unattested.fetch_add(2, Ordering::Relaxed);
    sources.receiver.peer_appears_cleartext.fetch_add(2, Ordering::Relaxed);
    sources.receiver.seal_failures.fetch_add(2, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(200));
    for _ in 0..3 {
        sf.record_round("n0", &scrape(addr), &families); // bumped, held
        thread::sleep(Duration::from_millis(200));
    }
    srv.stop();

    (
        sf,
        Disclosure {
            scenario: "crypto_counters",
            rules: &["Uc2UnattestedReports", "Uc2CleartextPeer", "Uc2FollowerSealFailures"],
            state: "synthetic",
            method: "synthetic ObsSources: reports_unattested / peer_appears_cleartext / \
                     seal_failures bumped directly (their real triggers — a 0.4.0 peer, a \
                     mixed-crypto cluster, a corrupting adversary — can't be honestly hosted \
                     in-process); is_leader=0 throughout (real default). Transition captured by \
                     two live HTTP scrapes of the real exporter, before and after the bump."
                .into(),
        },
    )
}

// ----------------------------------------------------------- scenario 11

/// Uc2DiskLow — **synthetic, disclosed** (Task 5 brief): heap-page sources
/// with `store_free_disk_bytes(small)` — the genuinely-broken-disk state
/// (actually filling a filesystem) is out of scope for an in-process harness
/// and left for a fixture in a later task. `uc2_journal_segment_bytes` is
/// real (the sources' own configured value), not synthetic — only the
/// disk-free reading is injected.
fn scenario_disk_low() -> (SeriesFile, Disclosure) {
    let mut sources = synthetic_sources(0);
    let segment_bytes = 1u64 << 20; // 1 MiB
    sources.journal_segment_bytes = segment_bytes;
    // < 4 segments of free disk — the rule's exact bar.
    sources.cnc.store_free_disk_bytes(segment_bytes);

    let srv = ObsServer::serve(sources.clone(), "127.0.0.1:0".parse().unwrap()).expect("bind");
    let addr = srv.local_addr();
    let mut sf = SeriesFile::new();
    for _ in 0..3 {
        sf.record_round(
            "n0",
            &scrape(addr),
            &["uc2_free_disk_bytes", "uc2_journal_segment_bytes"],
        );
        thread::sleep(Duration::from_millis(200));
    }
    srv.stop();

    (
        sf,
        Disclosure {
            scenario: "disk_low",
            rules: &["Uc2DiskLow"],
            state: "synthetic",
            method: format!(
                "synthetic ObsSources: free_disk_bytes={segment_bytes} with \
                 journal_segment_bytes={segment_bytes} (1 segment free < the rule's 4-segment \
                 bar) — rendered through the real exporter."
            ),
        },
    )
}

// ----------------------------------------------------------- scenario 12

/// Uc2ServiceAbsent — **real**. A node declaring `{0, 1}` with only FSM 0
/// ever attached. FSM 1's slot stays unattached for the whole capture, and
/// the exporter renders it as `uc_service_attached{service="1"} 0` because
/// the per-FSM band iterates the DECLARED bitmask, not occupied slots.
///
/// No `wait_ready` here: `/readyz` keys on page 1's service heartbeat, which
/// is the `min` over declared FSMs, so an absent FSM 1 holds this node at
/// 503 forever by design. `await_stable_leader` (can_serve) is the right
/// gate — the node is serving, it is admission that is shut.
fn scenario_service_absent(scratch_root: &Path) -> (SeriesFile, Disclosure) {
    let services = uc_node::ServicesConfig::from_ids(&[0, 1], None).expect("declared set");
    let (_dir, mut nodes) =
        spawn_cluster_with_services(scratch_root, "svc-absent", 1, 256 * 1024, services);
    await_stable_leader(&nodes, 20);

    let instance_dir = nodes[0].instance_dir.clone();
    let svc0 = ServiceBuilder::new(ServiceConfig::new(&instance_dir, APP).service_id(0), NoopSm)
        .start()
        .expect("FSM 0 attaches");
    // FSM 1 is deliberately never started.
    let addr = nodes[0].obs_addr();

    let mut sf = SeriesFile::new();
    for _ in 0..6 {
        sf.record_round("n0", &scrape(addr), &["uc_service_attached", "uc_services_declared"]);
        thread::sleep(Duration::from_millis(500));
    }
    svc0.stop();
    nodes[0].stop();

    (
        sf,
        Disclosure {
            scenario: "service_absent",
            rules: &["Uc2ServiceAbsent"],
            state: "real",
            method: "real single-node cluster declaring services.ids = [0, 1]; FSM 0 attaches, \
                     FSM 1 is never started. 6 real scrapes of the node's /metrics all read \
                     uc_service_attached{service=\"1\"} 0 (and service=\"0\" 1) — the per-FSM \
                     band renders a row for every DECLARED id, so the absent FSM is a 0 sample, \
                     not a missing series."
                .into(),
        },
    )
}

// ----------------------------------------------------------- scenario 13

/// FSM 1's stand-in for `fsm_pinned`: 20 ms per apply, so it can never keep
/// up with the load loop and the lag barrier pins the whole node to it.
struct SlowSm;
impl StateMachine for SlowSm {
    type Command = ();
    type Response = ();
    type Query = ();
    type QueryResponse = ();
    fn apply(&mut self, _position: u64, _cmd: ()) {
        thread::sleep(Duration::from_millis(20));
    }
    fn query(&self, _q: ()) {}
    fn last_applied(&self) -> Option<u64> {
        None
    }
}

/// `fsm_pinned`'s payload size. 96 B + the 32 B frame header aligns to a
/// 128 B frame, and 8192 is a whole number of those — so the door
/// (`append - min_applied <= fsm_lag`) lands the append head exactly ON the
/// bound or one frame past it, never straddling it. With a 64 B payload
/// (96 B frames, 8192/96 = 85.33) the same steady state oscillates between
/// 8160 and 8256, i.e. one frame BELOW the bound half the time, and the
/// rule's `>=` would be adjudicating a coin flip.
const PINNED_PAYLOAD: usize = 96;

/// True when this scrape body shows the commit head level with the append
/// head. See `scenario_fsm_pinned`'s loop for why that matters.
fn commit_at_append_head(body: &str) -> bool {
    let (mut append, mut commit) = (None, None);
    for (name, _labels, v) in parse_metrics(body) {
        match name.as_str() {
            "uc2_append_bytes" => append = Some(v),
            "uc2_commit_bytes" => commit = Some(v),
            _ => {}
        }
    }
    append.is_some() && append == commit
}

/// Uc2ServicePinnedAtLagBound — **real**. A node declaring `{0, 1}` with
/// `fsm_lag = Bounded(8 KiB)` and a 256 KiB admission window, so the FSM
/// door — not the admission window — is what stops the log. FSM 0 is a
/// no-op, FSM 1 sleeps 20 ms per record. Under sustained load the door
/// (`append - min_applied <= fsm_lag`) holds the backlog on the bound, and
/// on a single-node cluster commit is that same head, so
/// `commit - applied_1` settles at a FLAT 8320 for the whole capture:
/// the 8192-byte bound plus one 128-byte frame, because the door is checked
/// BEFORE a record is admitted, so the append head lands one frame past the
/// bound and stops. (Measured 12/12 samples on 5 consecutive runs. The
/// brief predicted exactly 8192 via `services::report_ceiling`; the
/// ceiling is what this node REPORTS, and on a single-node cluster the
/// commit head is its own append head, one frame past the door — hence the
/// rule's `>=`, which holds either way.)
///
/// Disclosure worth reading twice: FSM 1's heartbeat also ages while it
/// sleeps inside `apply`, so a real deployment in this state would fire
/// `Uc2ServiceWedged` too. That is honest — an FSM this slow IS wedged from
/// the cluster's point of view — but only `Uc2ServicePinnedAtLagBound` is
/// adjudicated from this capture.
fn scenario_fsm_pinned(scratch_root: &Path) -> (SeriesFile, Disclosure) {
    let services =
        uc_node::ServicesConfig::from_ids(&[0, 1], Some(uc_node::FsmLag::Bounded(8192)))
            .expect("declared set");
    let (_dir, mut nodes) =
        spawn_cluster_with_services(scratch_root, "fsm-pinned", 1, 256 * 1024, services);
    await_stable_leader(&nodes, 20);

    let instance_dir = nodes[0].instance_dir.clone();
    let svc0 = ServiceBuilder::new(ServiceConfig::new(&instance_dir, APP).service_id(0), NoopSm)
        .start()
        .expect("FSM 0 attaches");
    let svc1 = ServiceBuilder::new(ServiceConfig::new(&instance_dir, APP).service_id(1), SlowSm)
        .start()
        .expect("FSM 1 attaches");

    let addr = nodes[0].obs_addr();
    let families = ["uc_service_lag_bytes", "uc2_fsm_lag_bytes", "uc_service_attached"];
    let mut sf = SeriesFile::new();
    for _ in 0..12 {
        // Each round: top the log up until the FSM door refuses — that
        // refusal IS the pinned state — and scrape at that moment, so the
        // series is a level rather than a sawtooth.
        //
        // Two properties of the real node make the naive form misread it,
        // and both are worked around here rather than papered over:
        //
        //  - `Node::submit`'s door reads the APPEND COUNTER, which the
        //    consensus agent advances a cycle later, while the in-process
        //    ingress queue is 8192 records deep. An UNPACED spinner
        //    therefore enqueues hundreds of records past the door before
        //    the counter moves (measured: 4x the bound, then a multi-second
        //    drain). Hence the 1 ms pace.
        //  - `uc_service_lag_bytes` is `commit - applied`, so it only
        //    equals the backlog the door is holding when the commit head is
        //    level with the append head AND the door is still shut across
        //    the scrape. Both are checked below, and both are needed:
        //    without the second, one sample per 12-round capture read the
        //    drain instead of the level (measured 5 runs, dips of 3-5 KiB),
        //    because the slow FSM applies a chunk during the HTTP round
        //    trip and the door reopens mid-request.
        let deadline = Instant::now() + Duration::from_secs(30);
        let body = loop {
            while nodes[0].n().submit(vec![0u8; PINNED_PAYLOAD]).is_ok() {
                assert!(Instant::now() < deadline, "FSM door never shut under load");
                thread::sleep(Duration::from_millis(1));
            }
            let body = scrape(addr);
            // Was the node still pinned WHILE that scrape was taken? On this
            // 4-core box the request's round trip is long enough (6 busy
            // polling agents competing with the harness thread) for the slow
            // FSM to drain part of the backlog and reopen the door mid-
            // request; such a sample reads the drain, not the pinned state.
            // A refused submit right after the scrape says the door was shut
            // across it — and a `Full` probe appends nothing.
            let still_pinned = matches!(
                nodes[0].n().submit(vec![0u8; PINNED_PAYLOAD]),
                Err(uc_node::SubmitError::Full)
            );
            if still_pinned && commit_at_append_head(&body) {
                break body;
            }
            assert!(Instant::now() < deadline, "node never held still at the lag bound");
            thread::sleep(Duration::from_millis(10));
        };
        sf.record_round("n0", &body, &families);
        // Let the slow FSM drain a few frames, so the next round genuinely
        // refills to the bound rather than re-reading a frozen counter.
        thread::sleep(Duration::from_millis(200));
    }
    svc1.stop();
    svc0.stop();
    nodes[0].stop();

    (
        sf,
        Disclosure {
            scenario: "fsm_pinned",
            rules: &["Uc2ServicePinnedAtLagBound"],
            state: "real",
            method: "real single-node cluster, services.ids = [0, 1], fsm_lag = 8 KiB, admission \
                     window 256 KiB (so the FSM door binds first). FSM 1 sleeps 20 ms per apply, \
                     and a paced load tops the log up to the door each round; each scrape is \
                     taken while the door is verifiably still shut and the commit head is level \
                     with the append head. 12 real scrapes read \
                     uc_service_lag_bytes{service=\"1\"} flat at 8320 — the 8192-byte bound \
                     plus the one 128-byte frame the door admits before it shuts — while \
                     uc2_fsm_lag_bytes reads 8192. PAYLOAD CHOICE (disclosed because it is \
                     load-bearing, not incidental): the 96-byte payload makes a 128-byte frame, \
                     which DIVIDES the 8 KiB bound, so the pinned level is a flat 8320 rather \
                     than a value that straddles it. A non-dividing size — a 64-byte payload, \
                     96-byte frames, 8192/96 = 85.33 — oscillates between 8160 and 8256, i.e. \
                     one frame BELOW the bound half the time. This scenario therefore \
                     adjudicates the rule on its EASY input. The rule carries a tolerance for \
                     the general case — the TIGHTER of (bound - MTU_DEFAULT 1408) and \
                     0.9 * bound, so at this 8192-byte bound the threshold is 7372.8, not 8192 \
                     — and 8320 clears the un-toleranced bound anyway, so this run does NOT \
                     exercise the tolerance. A second arm at a non-dividing payload would."
                .into(),
        },
    )
}

// ============================================================ cluster kit
//
// Shapes copied from `uc_node/tests/failover.rs` (spawn_cluster / NodeH /
// partition / await_single_leader) and `uc_node/tests/obs_http.rs`
// (synthetic ObsSources construction, HTTP get) — examples can't import
// test code, so this is a deliberate, minimal re-derivation.

struct NodeH {
    addr: SocketAddr,
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
    fn block(&self, peer: SocketAddr) {
        for h in self.n().partition_handles() {
            h.block(peer);
        }
    }
    fn heal(&self) {
        for h in self.n().partition_handles() {
            h.clear();
        }
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

fn partition_pair(nodes: &[NodeH], a: usize, b: usize) {
    nodes[a].block(nodes[b].addr);
    nodes[b].block(nodes[a].addr);
}
fn heal_pair(nodes: &[NodeH], a: usize, b: usize) {
    nodes[a].heal();
    nodes[b].heal();
}

fn seed_for(i: usize) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

// One more knob than clippy likes; every one of them varies per scenario
// and a struct here would just be `NodeConfig` again.
#[allow(clippy::too_many_arguments)]
fn make_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    instance_dir: PathBuf,
    seed: u64,
    addr: SocketAddr,
    buffer_bytes: usize,
    admission_bytes: u64,
    services: uc_node::ServicesConfig,
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
        services,
    }
}

/// Bind every node's socket first (so the full member map is known before
/// any agent runs), then start each on its pre-bound socket with a real obs
/// HTTP server attached. Every declared FSM's rings and `snapshots/<id>/`
/// are created by the node at boot, whether or not anything attaches.
fn spawn_cluster_with_services(
    scratch_root: &Path,
    label: &str,
    n: usize,
    admission_bytes: u64,
    services: uc_node::ServicesConfig,
) -> (tempfile::TempDir, Vec<NodeH>) {
    let dir = tempfile::Builder::new()
        .prefix(&format!("m10-{label}-"))
        .tempdir_in(scratch_root)
        .expect("tempdir");

    let socks: Vec<UdpSocket> =
        (0..n).map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind")).collect();
    let members: Vec<(NodeId, SocketAddr)> =
        socks.iter().enumerate().map(|(i, s)| (i as NodeId, s.local_addr().unwrap())).collect();

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
            services,
        );
        let node = Node::start_with_socket(cfg, sock).expect("start");
        let obs =
            ObsServer::serve(node.observability(), "127.0.0.1:0".parse().unwrap()).expect("bind obs");
        nodes.push(NodeH { addr, instance_dir, node: Some(node), obs: Some(obs) });
    }
    (dir, nodes)
}

/// The default declared set (`{0}`, lag bound = buffer/4) — every pre-M14c
/// scenario.
fn spawn_cluster(
    scratch_root: &Path,
    label: &str,
    n: usize,
    admission_bytes: u64,
) -> (tempfile::TempDir, Vec<NodeH>) {
    spawn_cluster_with_services(
        scratch_root,
        label,
        n,
        admission_bytes,
        uc_node::ServicesConfig::default(),
    )
}

fn await_single_leader(nodes: &[NodeH], secs: u64) -> usize {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let serving: Vec<usize> = (0..nodes.len())
            .filter(|&i| nodes[i].node.is_some() && nodes[i].n().can_serve())
            .collect();
        assert!(serving.len() <= 1, "split-brain: nodes {serving:?} all serve");
        if serving.len() == 1 {
            return serving[0];
        }
        assert!(Instant::now() < deadline, "no single leader elected");
        // A tight yield_now() spin here competes for CPU with the very
        // node agent threads this harness is trying to observe — on this
        // 4-core box (3 nodes x 4 busy-polling agents = 12 threads already)
        // that starvation is enough to CAUSE the spurious re-elections this
        // harness is trying to ride out. Sleep instead of spin.
        thread::sleep(Duration::from_millis(15));
    }
}

/// Like `await_single_leader`, but also waits for that leader to hold
/// still — `can_serve()` continuously true for a follow-on window — before
/// returning. This 4-core dev box runs 3 nodes x 4 busy-polling agents
/// (12 threads); a spurious re-election right after the FIRST leader
/// settles is observed even with zero client load and zero partitions —
/// see the M10 Task 9 report's self-review for the investigation. Scenarios
/// that partition/crash relative to "the leader" need that leader to
/// actually be settled, not mid-flip.
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


