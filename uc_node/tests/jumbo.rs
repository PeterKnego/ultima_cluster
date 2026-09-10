// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Jumbo spec §10, in-process items (a), (b), (e): discovery lands on the
//! capped rung on every node; the ceiling never rises while one member is
//! silent; a client attached before the raise sees it.
//!
//! Three real loopback nodes per test (harness shaped after
//! `query_barrier.rs`), with [`FaultConfig::max_datagram`] standing in for a
//! narrow hop: a send longer than the cap is lost whole, exactly as a DF'd
//! datagram is at a link that cannot carry it. Loopback itself carries 65 kB,
//! so an uncapped cluster resolves at the ladder's top rung.

use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use uc_client::{Client, ClientError};
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig};
use uc_service::{ApplyCtx, ServiceBuilder, ServiceConfig, StateMachine};

const APP: &str = "jumbo";

/// The bound this harness sizes its buffers for — `payload_ceiling(MTU_BOUND,
/// crypto off)` is 8896, so 8864 is the binding half of the live
/// `min(bound, payload_ceiling(rung, crypto))` at the top rung, and pins that
/// the door really is a `min`.
const MAX_PAYLOAD: usize = 8864;

// ------------------------------------------------------------- state machine

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Cmd {
    /// A command whose ENCODED length is what matters: 4000 payload bytes are
    /// far above the baseline ceiling (1344) and far below the discovered one.
    Blob(Vec<u8>),
}

#[derive(Default)]
struct CountSm {
    total: u64,
    last_applied: Option<u64>,
}

impl StateMachine for CountSm {
    const NAME: &'static str = "count";

    type Command = Cmd;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Cmd) -> u64 {
        let Cmd::Blob(bytes) = cmd;
        self.total += bytes.len() as u64;
        self.last_applied = Some(ctx.position);
        self.total
    }
    fn query(&self, _q: ()) -> u64 {
        self.total
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

// ------------------------------------------------------------------ harness

fn make_config(
    id: u32,
    members: Vec<(u32, SocketAddr)>,
    instance_dir: PathBuf,
    seed: u64,
    addr: SocketAddr,
    faults: FaultConfig,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        bind: addr,
        instance_dir,
        app_id: APP.into(),
        buffer_bytes: 1 << 22, // 4 MiB: no wrap within the test
        max_payload: MAX_PAYLOAD,
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed,
        faults,
        purge: uc_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::single(CountSm::NAME),
    }
}

/// Every socket bound and every instance dir named up front, so the full
/// member map is known before any node starts — including for a member this
/// test never starts (the silent-member case): its port stays bound, so
/// nothing else can take it and nothing ever answers on it.
struct Fleet {
    _dir: tempfile::TempDir,
    dirs: Vec<PathBuf>,
    socks: Vec<Option<UdpSocket>>,
    members: Vec<(u32, SocketAddr)>,
}

fn bind_fleet(n: usize) -> Fleet {
    let dir = tempfile::Builder::new()
        .prefix("uc2-jumbo-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let socks: Vec<UdpSocket> = (0..n)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind"))
        .collect();
    let members: Vec<(u32, SocketAddr)> = socks
        .iter()
        .enumerate()
        .map(|(i, s)| (i as u32, s.local_addr().unwrap()))
        .collect();
    let dirs = (0..n).map(|i| dir.path().join(format!("n{i}"))).collect();
    Fleet {
        _dir: dir,
        dirs,
        socks: socks.into_iter().map(Some).collect(),
        members,
    }
}

impl Fleet {
    fn start(&mut self, i: usize, faults: FaultConfig) -> Node {
        let sock = self.socks[i].take().expect("socket already handed out");
        let addr = self.members[i].1;
        let seed = 0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let cfg = make_config(
            i as u32,
            self.members.clone(),
            self.dirs[i].clone(),
            seed,
            addr,
            faults,
        );
        Node::start_with_socket(cfg, sock).expect("start")
    }
}

fn spawn_cluster(n: usize, faults: FaultConfig) -> (Fleet, Vec<Node>) {
    let mut fleet = bind_fleet(n);
    let nodes = (0..n).map(|i| fleet.start(i, faults)).collect();
    (fleet, nodes)
}

/// Wait for exactly one serving leader; assert no split-brain throughout.
fn await_single_leader(nodes: &[Node], secs: u64) -> usize {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let serving: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].can_serve()).collect();
        assert!(
            serving.len() <= 1,
            "split-brain: nodes {serving:?} all serve"
        );
        if serving.len() == 1 {
            assert!(
                nodes[serving[0]].is_leader(),
                "serving node not flagged leader"
            );
            return serving[0];
        }
        assert!(Instant::now() < deadline, "no single leader elected");
        std::thread::yield_now();
    }
}

/// Wait until EVERY node reports the committed rung `want` — the raise is a
/// replicated `Settings` record, so a follower adopts it through the cluster
/// FSM exactly as the leader does.
fn await_rung(nodes: &[Node], want: u32, secs: u64) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if nodes.iter().all(|n| n.datagram_mtu() == want) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "rungs {:?}, wanted {want} everywhere",
            nodes.iter().map(|n| n.datagram_mtu()).collect::<Vec<_>>()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn stop_all(nodes: Vec<Node>) {
    for n in nodes {
        n.stop();
    }
}

// --------------------------------------------------------------------- tests

/// Spec §10(a): a path that carries 8832 but not 8960 resolves at 8832 —
/// every node, leader and followers alike, and the client-facing ceiling
/// follows it (crypto off: `payload_ceiling(8832, false) == 8768`).
#[test]
fn discovery_lands_on_the_capped_rung_on_every_node() {
    let (_fleet, nodes) = spawn_cluster(
        3,
        FaultConfig {
            max_datagram: 8832,
            ..FaultConfig::default()
        },
    );
    await_single_leader(&nodes, 10);
    await_rung(&nodes, 8832, 20);
    for n in &nodes {
        assert_eq!(n.payload_ceiling(), 8768, "crypto-off ceiling at 8832");
    }
    stop_all(nodes);
}

/// Spec §10(a), the uncapped half: loopback carries the whole ladder, so
/// discovery lands on the top rung and the door is the node's own BOUND —
/// `min(8864, payload_ceiling(8960, false) = 8896)`.
#[test]
fn an_uncapped_loopback_cluster_reaches_the_top_rung() {
    let (_fleet, nodes) = spawn_cluster(3, FaultConfig::default());
    await_single_leader(&nodes, 10);
    await_rung(&nodes, 8960, 20);
    for n in &nodes {
        assert_eq!(
            n.payload_ceiling(),
            MAX_PAYLOAD,
            "min(bound 8864, ceiling(8960, off) = 8896)"
        );
    }
    stop_all(nodes);
}

/// Spec §10(b): the leader raises the rung only once EVERY member has
/// answered. A bound-but-unstarted third voter is a member that never will,
/// so the two live nodes hold the baseline indefinitely — even though the
/// path between THEM carries the top rung. Starting the third releases it.
#[test]
fn the_ceiling_holds_at_baseline_while_one_member_is_silent() {
    let mut fleet = bind_fleet(3);
    let mut nodes: Vec<Node> = (0..2)
        .map(|i| fleet.start(i, FaultConfig::default()))
        .collect();
    await_single_leader(&nodes, 10); // 2 of 3 is a quorum

    // Comfortably longer than an all-up cluster needs to resolve: the other
    // two tests here reach their rung ~1.0 s after start, on a 1 s x 5 fast
    // cadence. Whatever these two nodes proved about the path BETWEEN them,
    // the third has not answered, so the rung cannot move.
    std::thread::sleep(Duration::from_secs(3));
    for n in &nodes {
        assert_eq!(n.datagram_mtu(), 1408, "a silent member pins the rung");
        assert_eq!(
            n.payload_ceiling(),
            1344,
            "crypto-off ceiling at the 1408 baseline"
        );
    }

    // Task 8b: a rejoining peer's own PROBE is proof of life and resets our
    // cadence toward it, so the raise lands in seconds rather than waiting
    // out the slow cadence's up-to-31 s tick.
    nodes.push(fleet.start(2, FaultConfig::default()));
    await_rung(&nodes, 8960, 20);
    stop_all(nodes);
}

/// Spec §10(e): a client attached BEFORE the raise sees it without
/// reattaching — the submit door is the live cnc word, read per submit, not a
/// number copied at attach. The pre-raise submit's outcome is deliberately
/// not depended on (discovery may already have landed by the time the client
/// is up); what is asserted is that it is one of exactly two answers, and
/// that after the raise the same 4000 B command commits.
#[test]
fn a_client_attached_before_the_raise_sees_it() {
    let (fleet, nodes) = spawn_cluster(3, FaultConfig::default());
    let leader = await_single_leader(&nodes, 10);
    let leader_dir = fleet.dirs[leader].clone();

    let svc = ServiceBuilder::new(ServiceConfig::new(&leader_dir, APP), CountSm::default())
        .start()
        .unwrap();
    let client = Client::connect(&leader_dir, APP).unwrap();

    // ~4008 B once bincode-encoded: above the baseline door (1344), below
    // both the discovered ceiling (8864) and the buffer's bound.
    let big = Cmd::Blob(vec![7u8; 4000]);
    let early: Result<u64, ClientError> = client.submit(&big);
    assert!(
        matches!(early, Err(ClientError::PayloadTooLarge { .. }) | Ok(_)),
        "a pre-raise submit is either refused at the 1344 door or accepted \
         because discovery already landed — never anything else: {early:?}"
    );

    await_rung(&nodes, 8960, 20);

    let after: Result<u64, ClientError> = client.submit(&big);
    let total = after.expect("4000 B under a discovered ceiling of 8864");
    assert!(
        total >= 4000,
        "the blob applied through the raised door: total {total}"
    );

    client.shutdown();
    svc.stop();
    stop_all(nodes);
}
