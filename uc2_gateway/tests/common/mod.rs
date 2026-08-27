// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Shared rig for the gateway round-trip tests: a one-node cluster on
//! loopback UDP with its instance directory on **ext4** (never `/tmp`, which
//! is RAM-backed on the dev box — see CLAUDE.md "Local box").
//!
//! The `NodeConfig` shape is the one the lincheck harness uses
//! (`uc2_node/tests/lincheck_v2/mod.rs`), reduced to `n = 1` for the
//! round-trip tests: a single member elects itself immediately, so
//! `can_serve()` goes true without any peer traffic.
//!
//! [`start_cluster`] is the `n`-member form, used by the failover capstone:
//! bind every UDP socket FIRST so the whole member map is known before any
//! agent runs, then start each node on its own pre-bound socket
//! (`Node::start_with_socket`) — the same port-stable pattern
//! `LinClusterV2::start_cfg` uses, and the reason a crashed node could be
//! restarted on its own address if a test ever wanted to.

#![allow(dead_code)] // each test file uses a different subset

use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};
use uc2_service::{Service, ServiceBuilder, ServiceConfig, SessionConfig, Sessioned};
use uc_lincheck::register::RegisterSm;

pub const APP: &str = "gw-roundtrip";

/// A temp dir on real disk. `CARGO_TARGET_TMPDIR` lives under `target/`
/// (ext4), so journal segments never land on the RAM-backed `/tmp`.
pub fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("uc2-gw-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

/// Start a single-member node under `root/node0` and return it with its
/// instance directory. It elects itself within ~100 ms.
pub fn start_single_node(root: &Path) -> (Node, PathBuf) {
    start_single_node_with_election(root, 50_000_000, 100_000_000)
}

/// As [`start_single_node`], with the election timeout under the test's
/// control — a long one gives a test a deterministic window in which the node
/// exists but **cannot serve**, which is otherwise a ~50 ms race to hit.
pub fn start_single_node_with_election(
    root: &Path,
    election_min_ns: u64,
    election_max_ns: u64,
) -> (Node, PathBuf) {
    let dir = root.join("node0");
    std::fs::create_dir_all(&dir).expect("instance dir");
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let cfg = NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        bind,
        instance_dir: dir.clone(),
        app_id: APP.into(),
        buffer_bytes: 1 << 22, // 4 MiB
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: election_min_ns,
        election_timeout_max_ns: election_max_ns,
        seed: 1,
        faults: FaultConfig::default(),
        purge: uc2_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc2_node::CryptoConfig::Disabled,
        services: uc2_node::ServicesConfig::default(),
    };
    let node = Node::start(cfg).expect("node start");
    (node, dir)
}

/// Poll until the node is a serving leader, or panic after `secs`.
pub fn await_serving(node: &Node, secs: u64) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !node.can_serve() {
        assert!(Instant::now() < deadline, "node never became a serving leader");
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// The state machine every multi-node rig runs: the shared `RegisterSm` under
/// the session envelope, so a re-send across failover is answered `replayed`
/// rather than applied twice.
pub type Sm = Sessioned<RegisterSm>;

/// One cluster member: its identity and instance directory, plus the live
/// node + service (taken out of their `Option`s when it is crashed).
pub struct Slot {
    pub id: u32,
    pub instance_dir: PathBuf,
    pub node: Option<Node>,
    pub service: Option<Service<Sm>>,
}

impl Slot {
    /// A live node that both flags leader and passes the serving gate.
    pub fn is_serving_leader(&self) -> bool {
        self.node.as_ref().is_some_and(|n| n.can_serve() && n.is_leader())
    }

    /// Hard crash-stop, **node before service** — a node's read barrier can
    /// block awaiting the service, so tearing the service down first risks
    /// wedging the node's shutdown join (lincheck-v2 module doc, v1 task12
    /// finding #3). No flush on either: whatever the archive already fsynced
    /// is the recovery point.
    pub fn crash(&mut self) {
        if let Some(node) = self.node.take() {
            node.crash();
        }
        if let Some(service) = self.service.take() {
            service.crash();
        }
    }

    /// Clean stop, same order.
    pub fn stop(&mut self) {
        if let Some(node) = self.node.take() {
            node.stop();
        }
        if let Some(service) = self.service.take() {
            service.stop();
        }
    }
}

/// Bring up an `n`-member cluster under `root`, one service per member.
///
/// A follower's service follows the committed log too, so every member carries
/// one from boot — the new leader after a failover already has its state
/// machine caught up.
pub fn start_cluster(root: &Path, n: usize) -> Vec<Slot> {
    let socks: Vec<UdpSocket> =
        (0..n).map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind")).collect();
    let members: Vec<(u32, SocketAddr)> =
        socks.iter().enumerate().map(|(i, s)| (i as u32, s.local_addr().unwrap())).collect();

    let mut slots = Vec::with_capacity(n);
    for (i, sock) in socks.into_iter().enumerate() {
        let dir = root.join(format!("n{i}"));
        std::fs::create_dir_all(&dir).expect("instance dir");
        let cfg = NodeConfig {
            id: i as u32,
            members: members.clone(),
            bind: members[i].1,
            instance_dir: dir.clone(),
            app_id: APP.into(),
            buffer_bytes: 1 << 22, // 4 MiB
            max_payload: 256,
            admission_bytes: 256 * 1024,
            // The lincheck-v2 3-node shape: sub-second failover, and each node
            // a distinct seed so a clean boot elects exactly one leader.
            election_timeout_min_ns: 150_000_000,
            election_timeout_max_ns: 300_000_000,
            seed: seed_for(i),
            faults: FaultConfig::default(),
            purge: uc2_node::PurgePolicy::Disabled,
            learners: Vec::new(),
            journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
            crypto: uc2_node::CryptoConfig::Disabled,
            services: uc2_node::ServicesConfig::default(),
        };
        let node = Node::start_with_socket(cfg, sock).expect("node start");
        let service = ServiceBuilder::new(
            ServiceConfig::new(&dir, APP),
            Sessioned::new(RegisterSm::default(), SessionConfig::default()),
        )
        .start()
        .expect("service start");
        slots.push(Slot { id: i as u32, instance_dir: dir, node: Some(node), service: Some(service) });
    }
    slots
}

/// A distinct, index-derived election seed per member, so a clean boot elects
/// exactly one leader (same trick as the lincheck harness).
fn seed_for(i: usize) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Wait for exactly one serving leader across `slots` and return its index.
pub fn await_single_leader(slots: &[Slot], secs: u64) -> usize {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let leaders: Vec<usize> =
            (0..slots.len()).filter(|&i| slots[i].is_serving_leader()).collect();
        if leaders.len() == 1 {
            return leaders[0];
        }
        assert!(
            Instant::now() < deadline,
            "no single serving leader after {secs}s (found {leaders:?})"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// A loopback TCP address nothing is listening on *right now*.
///
/// The edge binds its own listener, and every edge needs the whole node-id →
/// gateway-address map before any of them starts — so the addresses have to be
/// chosen before the first `Edge::start`. Bind-then-drop is the same
/// reservation trick `LinClusterV2` uses for its spare UDP address; the race
/// (someone else taking the port in between) is why `start_edges` retries.
pub fn free_tcp_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").expect("reserve a port");
    l.local_addr().unwrap()
}

/// Names of this process's live threads that the gateway spawned.
///
/// `Edge::stop` promises to join every thread it started; this is how the
/// tests hold it to that rather than taking its word. Linux exposes each
/// thread's name at `/proc/self/task/<tid>/comm` (truncated to 15 bytes, which
/// all the `uc2-gw-*` names fit inside).
pub fn gateway_threads() -> Vec<String> {
    let mut out = Vec::new();
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else { return out };
    for t in tasks.flatten() {
        if let Ok(name) = std::fs::read_to_string(t.path().join("comm")) {
            let name = name.trim().to_string();
            if name.starts_with("uc2-gw-") {
                out.push(name);
            }
        }
    }
    out
}

/// Assert every gateway thread is gone. Joining is synchronous in
/// `Edge::stop`, but a joined thread's `/proc` entry can linger for an instant,
/// so allow a brief settle.
pub fn assert_no_gateway_threads() {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let live = gateway_threads();
        if live.is_empty() {
            return;
        }
        assert!(Instant::now() < deadline, "gateway threads outlived Edge::stop: {live:?}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

// ------------------------------------------------------ raw framed client
//
// A `FramedConn` driven by hand, for the properties a `RemoteClient` cannot
// report: what is on the wire, in what order. Shared by `credits_wire.rs`
// (frame ordering) and `credits.rs` (the grant budget).

use uc2_remote::conn::FramedConn;
use uc2_remote::frame::{FrameType, Header, Hello, PROTOCOL_VERSION};

/// Mid-frame stall budget for the raw reads below. Nothing in these tests
/// writes a partial frame, so it only bounds a wedged test.
pub const READ_STALL: Duration = Duration::from_secs(10);

/// Open a raw framed connection, with a short read timeout so a silent edge
/// shows up as `Ok(None)` rather than a hang.
pub fn dial_raw(addr: std::net::SocketAddr) -> FramedConn {
    let s = std::net::TcpStream::connect(addr).expect("connect");
    let c = FramedConn::new(s).unwrap();
    c.set_read_timeout(Some(Duration::from_millis(10))).unwrap();
    c.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
    c
}

pub fn send_hello(c: &mut FramedConn, client_id: u64, app_id: &str) {
    let mut out = Vec::new();
    Hello { app_id }.encode(&mut out);
    let h = Header { ty: FrameType::Hello, flags: 0, version: PROTOCOL_VERSION, client_id, seq: 0 };
    c.write_frame(h, &out).expect("write HELLO");
}

/// Read frames until `want` arrives or `budget` runs out, returning its
/// header and payload.
pub fn read_until_frame(
    c: &mut FramedConn,
    want: FrameType,
    budget: Duration,
) -> Option<(Header, Vec<u8>)> {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        match c.read_frame(READ_STALL) {
            Ok(Some((h, p))) if h.ty == want => return Some((h, p.to_vec())),
            Ok(Some(_)) | Ok(None) => {}
            Err(_) => return None,
        }
    }
    None
}

/// [`read_until_frame`] when only the header matters.
pub fn read_until(c: &mut FramedConn, want: FrameType, budget: Duration) -> Option<Header> {
    read_until_frame(c, want, budget).map(|(h, _)| h)
}
