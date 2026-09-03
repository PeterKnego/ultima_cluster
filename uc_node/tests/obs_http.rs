// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M10 Task 6: `/metrics`, `/healthz`, `/readyz` — including the exact
//! naive-probe mistake the spec names: an elected leader that hasn't yet
//! picked up `NODE_FLAG_CAN_SERVE` (the bare `0x01` state) must read as
//! live but NOT ready.
//!
//! Construction follows `lifecycle.rs`: instance dirs live under
//! `CARGO_TARGET_TMPDIR` (ext4), never `/tmp` (RAM-backed tmpfs, no swap —
//! see CLAUDE.md "Local box").

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use uc_log::cnc::{CncMeta, CncPage};
use uc_net::receiver::FollowerStats;
use uc_net::sender::SenderStats;
use uc_node::obs::ObsSources;
use uc_node::obs::http::ObsServer;
use uc_node::obs::metrics::now_unix_ns;
use uc_node::{Node, NodeConfig};
use uc_protocol::v2::cnc::NODE_FLAG_LEADER;
use uc_service::{ApplyCtx, ServiceBuilder, ServiceConfig, StateMachine};

/// A minimal blocking HTTP/1.1 GET client: connect, send the request line,
/// read until the peer closes (the server never keeps a connection alive),
/// split status code and body.
fn get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect to obs server");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set_read_timeout");
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .expect("write request");
    stream.flush().expect("flush request");

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read response");
    let text = String::from_utf8_lossy(&raw);

    let mut halves = text.splitn(2, "\r\n\r\n");
    let head = halves.next().unwrap_or("");
    let body = halves.next().unwrap_or("").to_string();
    let code = head
        .lines()
        .next()
        .unwrap_or("")
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    (code, body)
}

/// `CncPage::heap`-backed `ObsSources`, matching `obs::metrics`'s unit-test
/// fixture but built entirely over public APIs (integration tests can't
/// reach `pub(crate)`). `ObsSources` is `Clone` (all fields are `Arc`/scalar)
/// so the test can both hand one copy to `ObsServer::serve` (which takes
/// `ObsSources` by value) and keep a second copy — over the SAME underlying
/// `Arc` allocations — to mutate flags/heartbeats the running server reads.
fn synthetic_server() -> (ObsServer, ObsSources) {
    let meta = CncMeta {
        node_id: 7,
        instance_id: 0x1122_3344_5566_7788,
        app_id: "test-app".into(),
        buffer_bytes: 1 << 20,
        max_payload: 1200,
        services: [None; uc_protocol::v2::cnc::CNC_MAX_SERVICES],
    };
    let cnc = CncPage::heap(&meta);

    let sources = ObsSources {
        node_id: 7,
        cnc,
        sender: Arc::new(SenderStats::default()),
        receiver: Arc::new(FollowerStats::default()),
        truncations: Arc::new(AtomicU64::new(0)),
        wipes: Arc::new(AtomicU64::new(0)),
        timer_stats: Arc::new(uc_node::TimerStats::default()),
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
    };

    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let srv = ObsServer::serve(sources.clone(), bind).expect("obs server binds");
    (srv, sources)
}

#[test]
fn metrics_healthz_readyz_serve_and_404_otherwise() {
    let (srv, sources) = synthetic_server();
    sources
        .cnc
        .status()
        .node_heartbeat_ns
        .store_release(now_unix_ns());
    sources
        .cnc
        .status()
        .service_heartbeat_ns
        .store_release(now_unix_ns());

    assert_eq!(get(srv.local_addr(), "/metrics").0, 200);
    assert!(
        get(srv.local_addr(), "/metrics")
            .1
            .contains("uc2_commit_bytes")
    );
    assert_eq!(get(srv.local_addr(), "/healthz").0, 200);
    assert_eq!(get(srv.local_addr(), "/readyz").0, 200);
    assert_eq!(get(srv.local_addr(), "/nope").0, 404);
    srv.stop();
}

#[test]
fn an_elected_but_not_serving_leader_is_not_ready() {
    let (srv, sources) = synthetic_server();
    sources
        .cnc
        .status()
        .node_heartbeat_ns
        .store_release(now_unix_ns());
    sources
        .cnc
        .status()
        .service_heartbeat_ns
        .store_release(now_unix_ns());
    sources.cnc.status().flags.store_release(NODE_FLAG_LEADER); // 0x01, no CAN_SERVE

    let (code, body) = get(srv.local_addr(), "/readyz");
    assert_eq!(code, 503);
    assert!(body.contains("NewTerm"), "{body}");
    assert_eq!(
        get(srv.local_addr(), "/healthz").0,
        200,
        "liveness must NOT flap on 0x01"
    );
    srv.stop();
}

#[test]
fn a_dead_agent_fails_liveness_by_name() {
    let (srv, sources) = synthetic_server();
    sources
        .cnc
        .status()
        .node_heartbeat_ns
        .store_release(now_unix_ns());
    sources
        .cnc
        .status()
        .service_heartbeat_ns
        .store_release(now_unix_ns());
    sources.agents[3].1.store(true, Ordering::Release); // archive fail-stopped

    let (code, body) = get(srv.local_addr(), "/healthz");
    assert_eq!(code, 503);
    assert!(body.contains("archive"), "{body}");
    srv.stop();
}

#[test]
fn a_stale_service_heartbeat_fails_readiness_but_not_liveness() {
    let (srv, sources) = synthetic_server();
    // Node heartbeat fresh, service heartbeat never stamped (reads as a
    // huge age, by the same "never written = stale" convention as the
    // metrics encoder).
    sources
        .cnc
        .status()
        .node_heartbeat_ns
        .store_release(now_unix_ns());

    let (code, body) = get(srv.local_addr(), "/readyz");
    assert_eq!(code, 503);
    assert!(body.contains("service heartbeat stale"), "{body}");
    assert_eq!(get(srv.local_addr(), "/healthz").0, 200);
    srv.stop();
}

/// Fix-round-1 regression: the accept loop is single-threaded and
/// synchronous, so an unbounded `handle_conn` stalls the WHOLE server, not
/// just the slow connection. Open a connection, send a partial request (no
/// `\r\n\r\n`), then go silent and hold the socket open — a SECOND client's
/// `GET /healthz` must still complete, and within a bounded time, proving
/// the wall-clock connection deadline (not just a per-`read()` timeout)
/// frees the server on its own. A single partial write followed by silence
/// is enough: the deadline must fire with no further bytes ever arriving,
/// so the test's own wall-clock time stays bounded too.
#[test]
fn a_trickling_client_cannot_stall_the_server() {
    let (srv, sources) = synthetic_server();
    sources
        .cnc
        .status()
        .node_heartbeat_ns
        .store_release(now_unix_ns());
    sources
        .cnc
        .status()
        .service_heartbeat_ns
        .store_release(now_unix_ns());

    // Held open for the whole test (not dropped) — a closed socket sends
    // FIN, which would let the server's read return `Ok(0)` and exit the
    // read loop on its own. The point here is a connection that stays OPEN
    // but silent, so only the wall-clock deadline can end it.
    let mut slow = TcpStream::connect(srv.local_addr()).expect("slow client connect");
    slow.write_all(b"GET /heal")
        .expect("slow client partial write");
    slow.flush().expect("slow client flush");

    let start = Instant::now();
    let (code, _) = get(srv.local_addr(), "/healthz");
    let elapsed = start.elapsed();

    assert_eq!(code, 200, "a second client must still be served");
    assert!(
        elapsed < Duration::from_secs(8),
        "second client waited {elapsed:?} for a response — the trickling/silent \
         connection stalled the single-threaded accept loop past its wall-clock budget"
    );

    drop(slow);
    srv.stop();
}

const APP: &str = "obs-http";

/// A state machine that does nothing but exist — `a_real_single_node_cluster_serves_and_becomes_ready`
/// only needs a real attached service so `service_heartbeat_ns` gets
/// stamped (readiness for a leader is `can_serve AND service heartbeat age
/// < 3s`; a bare `Node` with no service ever attached correctly stays
/// not-ready forever, so the test attaches one).
#[derive(Default)]
struct NoopSm;

impl StateMachine for NoopSm {
    const NAME: &'static str = "noop";

    type Command = ();
    type Response = ();
    type Query = ();
    type QueryResponse = ();

    fn apply(&mut self, _ctx: &mut ApplyCtx, _cmd: ()) {}
    fn query(&self, _q: ()) {}
    fn last_applied(&self) -> Option<u64> {
        None
    }
}

fn config_for(addr: SocketAddr, instance_dir: std::path::PathBuf) -> NodeConfig {
    NodeConfig {
        id: 0,
        members: vec![(0, addr)],
        learners: Vec::new(),
        bind: addr,
        instance_dir,
        app_id: APP.into(),
        buffer_bytes: 1 << 22,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: 0xA1B2_C3D4_5566_7788,
        faults: Default::default(),
        purge: uc_node::PurgePolicy::Disabled,
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::single(NoopSm::NAME),
    }
}

/// Sole-voter node, per `lifecycle.rs`'s `single_node` pattern.
fn single_node(instance_dir: &std::path::Path) -> Node {
    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind");
    let addr = sock.local_addr().unwrap();
    let node =
        Node::start_with_socket(config_for(addr, instance_dir.to_path_buf()), sock).expect("start");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !node.can_serve() {
        assert!(Instant::now() < deadline, "sole voter never became leader");
        std::thread::yield_now();
    }
    node
}

#[test]
fn a_real_single_node_cluster_serves_and_becomes_ready() {
    let dir = tempfile::Builder::new()
        .prefix("uc2-obs-http-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let instance_dir = dir.path().join("n0");

    let node = single_node(&instance_dir);
    // A real attached service, so `service_heartbeat_ns` is genuinely live
    // (readiness requires it fresh; a node with no service ever attached
    // must NOT read as ready).
    let svc = ServiceBuilder::new(ServiceConfig::new(&instance_dir, APP), NoopSm)
        .start()
        .expect("service attaches");

    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let srv = ObsServer::serve(node.observability(), bind).expect("obs server binds");

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (code, _) = get(srv.local_addr(), "/readyz");
        if code == 200 {
            break;
        }
        assert!(Instant::now() < deadline, "sole voter never became ready");
        std::thread::yield_now();
    }

    let (code, body) = get(srv.local_addr(), "/metrics");
    assert_eq!(code, 200);
    assert!(body.contains("uc2_is_leader 1"), "{body}");
    assert!(body.contains("uc2_can_serve 1"), "{body}");

    srv.stop();
    drop(svc);
    node.stop();
}

#[test]
fn timer_and_log_time_families_are_in_the_contract() {
    for name in [
        "uc2_timers_pending",
        "uc2_timers_fired_total",
        "uc2_timers_late_total",
        "uc2_timers_rearmed_total",
        "uc2_log_time_ns",
        "uc2_log_time_lag_seconds",
    ] {
        assert!(
            uc_node::obs::metrics::CONTRACT_SERIES.contains(&name),
            "{name}"
        );
    }
}
