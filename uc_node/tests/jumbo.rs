// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Jumbo spec §10, in-process items (a) through (e): discovery lands on the
//! capped rung on every node; the ceiling never rises while one member is
//! silent; a client attached before the raise sees it; and — plan 2 — the two
//! startup gates, `force_jumbo_frames` (spec §6) and the committed-rung join
//! refusal (spec §5.4), each fail-stopping the consensus agent by name.
//!
//! Three real loopback nodes per test (harness shaped after
//! `query_barrier.rs`), with [`FaultConfig::max_datagram`] standing in for a
//! narrow hop: a send longer than the cap is lost whole, exactly as a DF'd
//! datagram is at a link that cannot carry it. Loopback itself carries 65 kB,
//! so an uncapped cluster resolves at the ladder's top rung.

use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard};
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
    force_jumbo_frames: bool,
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
        force_jumbo_frames,
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
        self.start_forced(i, faults, false)
    }

    /// `force` is jumbo spec §6's `force_jumbo_frames`.
    fn start_forced(&mut self, i: usize, faults: FaultConfig, force: bool) -> Node {
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
            force,
        );
        Node::start_with_socket(cfg, sock).expect("start")
    }

    /// Re-bind member `i`'s ORIGINAL address after its node was stopped, so
    /// the same member can be started again against the same instance dir —
    /// what a restart is. UDP has no TIME_WAIT, so the port is free the
    /// moment the old socket is dropped.
    fn rebind(&mut self, i: usize) {
        assert!(self.socks[i].is_none(), "member {i} was never started");
        self.socks[i] = Some(UdpSocket::bind(self.members[i].1).expect("rebind"));
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

// --------------------------------------------------- plan 2: the two gates

/// The three gate tests below share the process-global `uc_obs` capture sink
/// ([`uc_node::obs::log::capture_for_tests`]), so only one may hold it at a
/// time — same discipline as `obs_log.rs`'s `serialize()`. It also keeps three
/// 3-node clusters from running at once beside the four discovery tests above.
static TEST_LOCK: Mutex<()> = Mutex::new(());

fn serialize() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// Has this node's consensus agent fail-stopped? The agent's panic sets its
/// `finished` flag from a drop guard inside the worker thread — the SAME flag
/// `uc2-node`'s monitor loop turns into `agent_failstopped` + exit 1 — so this
/// is exactly what the daemon sees.
fn consensus_failed(n: &Node) -> bool {
    n.observability()
        .agents
        .iter()
        .find(|(name, _)| *name == "consensus")
        .map(|(_, flag)| flag.load(Ordering::Acquire))
        .expect("a node always has a consensus agent")
}

/// Wait until every node in `nodes` has fail-stopped its consensus agent AND
/// the named refusal has landed in the capture buffer.
///
/// Both halves matter: the flag alone would pass for any panic in that agent,
/// and the log record alone would pass for a record some OTHER node emitted.
/// The panic message itself goes to the agent thread's stderr and is not
/// reachable from the process, which is why the gate emits the reason as an
/// `obs_event!(Error, …)` first — that record is the machine-readable half.
fn await_agent_failstop(nodes: &[&Node], buf: &Arc<Mutex<Vec<u8>>>, reason: &str, secs: u64) {
    let needle = format!("\"event\":\"{reason}\"");
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        if nodes.iter().all(|n| consensus_failed(n)) && text.contains(&needle) {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "{reason} never fired on all {} nodes within {secs}s (failed: {:?})\
                 \n--- capture buffer ---\n{text}--- end capture buffer ---",
                nodes.len(),
                nodes
                    .iter()
                    .map(|n| consensus_failed(n))
                    .collect::<Vec<_>>(),
            );
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Spec §10(c)/§6: under `force_jumbo_frames`, a path that cannot carry
/// `JUMBO_MIN_RUNG` fail-stops the node BY NAME within the window — and the
/// node never serves in the meantime, which is the half that makes the gate
/// worth having (a node that served first and died later would have taken
/// commands it could not replicate).
///
/// The cap is the baseline rung itself, so the 1408 probe IS acked and the two
/// jumbo probes are not: every peer answered, too small. That is
/// `jumbo_path_too_narrow`, not `jumbo_peer_silent`.
///
/// Expect three `consensus fatal (fail-stop)` panics on stderr: that is the
/// agent dying as designed, one line per node.
#[test]
fn the_force_gate_refuses_a_path_too_narrow() {
    let _g = serialize();
    let buf = uc_node::obs::log::capture_for_tests();
    let mut fleet = bind_fleet(3);
    let faults = FaultConfig {
        max_datagram: 1408,
        ..FaultConfig::default()
    };
    let nodes: Vec<Node> = (0..3)
        .map(|i| fleet.start_forced(i, faults, true))
        .collect();

    let refs: Vec<&Node> = nodes.iter().collect();
    await_agent_failstop(&refs, &buf, "jumbo_path_too_narrow", 45);
    for n in &nodes {
        assert!(
            !n.can_serve(),
            "a node that never proved its paths must not serve"
        );
        // And the readiness signal the obs layer reads (review fix 2) — proof
        // that a REAL gated node, not just a synthetic `ObsSources`, makes
        // `/readyz` refuse.
        assert!(
            n.observability().jumbo_gate_pending.load(Ordering::Acquire),
            "/readyz must see the pending gate"
        );
    }
    // Dropped, not stopped: `stop()` joins the agent threads and re-raises the
    // panic that killed the consensus one. The drop path swallows it (and
    // still joins every thread).
    drop(nodes);
}

/// Spec §10(c)/§6: a member that never answers is a LIVENESS fact and is
/// worded as one — `jumbo_peer_silent`, naming the member and how long it was
/// waited for, rather than claiming anything about its path MTU (nothing is
/// known about it).
///
/// The third member's socket stays bound and unserved, so the two live nodes
/// have a peer that provably never answers. They elect a leader between them
/// (2 of 3 is a quorum) and still refuse to serve.
///
/// Expect two `consensus fatal (fail-stop)` panics on stderr.
#[test]
fn the_force_gate_refuses_a_silent_peer() {
    let _g = serialize();
    let buf = uc_node::obs::log::capture_for_tests();
    let mut fleet = bind_fleet(3);
    let nodes: Vec<Node> = (0..2)
        .map(|i| fleet.start_forced(i, FaultConfig::default(), true))
        .collect();

    let refs: Vec<&Node> = nodes.iter().collect();
    await_agent_failstop(&refs, &buf, "jumbo_peer_silent", 45);
    drop(nodes);
}

/// Spec §10(d)/§5.4: a node whose path is narrower than the rung the cluster
/// already committed refuses to JOIN rather than serving and stalling commit
/// — the log already holds frames it cannot receive.
///
/// The restarted member keeps its instance dir, so it recovers its own log and
/// learns the committed 8960 through the cluster FSM exactly as it would from
/// an artifact or a snapshot session. Behind a 1408 cap its probes at the
/// BASELINE rung still land and are acked, so every peer ANSWERS at 1408 —
/// proven degradation, which review fix 1 refuses at once, with no window.
/// (The cap must stay at or above 1408 for that reason: a lower cap would make
/// the peers SILENT instead, which by design never refuses.)
///
/// Expect one `consensus fatal (fail-stop)` panic on stderr.
#[test]
fn a_restart_below_the_committed_rung_refuses_to_join() {
    let _g = serialize();
    let buf = uc_node::obs::log::capture_for_tests();
    // Three nodes on an uncapped loopback commit 8960 (plan 1's proof).
    let (mut fleet, mut nodes) = spawn_cluster(3, FaultConfig::default());
    let leader = await_single_leader(&nodes, 10);
    await_rung(&nodes, 8960, 20);

    // Restart a FOLLOWER, so the surviving two keep both the quorum and the
    // leadership they already have.
    let victim = (leader + 1) % 3;
    nodes.remove(victim).stop();
    fleet.rebind(victim);
    let restarted = fleet.start(
        victim,
        FaultConfig {
            max_datagram: 1408,
            ..FaultConfig::default()
        },
    );

    await_agent_failstop(&[&restarted], &buf, "path_below_committed_mtu", 45);
    // The peer ANSWERED at the baseline rung — that is what makes this a proven
    // narrow path rather than silence, and silence would (by design) never
    // refuse. Pinned so the test cannot start passing for the wrong reason.
    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(
        text.contains(r#""carried":1408,"committed":8960"#),
        "the refusal must name the ANSWERED rung: {text}"
    );
    assert!(
        !restarted.can_serve(),
        "a node below the committed rung must not serve"
    );
    // The two that stayed up are untouched: one gate firing on a joiner must
    // never take the cluster with it.
    for n in &nodes {
        assert!(!consensus_failed(n), "a live node fail-stopped too");
    }
    drop(restarted);
    stop_all(nodes);
}

/// Spec §5.4's other half, and the case review fix 2 exists for: a HEALTHY
/// restart onto a jumbo cluster must just work. The node learns the committed
/// 8960 from the cluster replay at an arbitrary point in its own probe ladder —
/// typically with peers still silent or answering only the baseline rung — so a
/// gate that refused either state would fail-stop a node whose paths are
/// perfectly fine, and under systemd's `Restart=on-failure` it would crash-loop.
///
/// `can_serve` is deliberately NOT the assertion: `ElectionSm::serving` is
/// LEADER-only in UC, so a restarted follower never reports it, gate or no gate.
/// What the gate controls — and all it controls — is the pending flag that
/// masks `can_serve` and `/readyz`, so the test asserts that it CLEARS (the
/// gate passed, i.e. the node proved the rung and would serve if elected) and
/// that the consensus agent never fail-stopped.
#[test]
fn a_healthy_restart_on_a_jumbo_cluster_does_not_refuse() {
    let _g = serialize();
    let buf = uc_node::obs::log::capture_for_tests();
    let (mut fleet, mut nodes) = spawn_cluster(3, FaultConfig::default());
    let leader = await_single_leader(&nodes, 10);
    await_rung(&nodes, 8960, 20);

    let victim = (leader + 1) % 3;
    nodes.remove(victim).stop();
    fleet.rebind(victim);
    let restarted = fleet.start(victim, FaultConfig::default());

    // Two things must both become true, and neither may be read before the
    // other: the node ADOPTS the committed rung (the cluster replay reaches
    // it — before that there is no gate to clear, so a bare pending check
    // would pass vacuously), and the gate then CLEARS. The first round of
    // probes is due immediately, so this is a ~1 s wait.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        assert!(
            !consensus_failed(&restarted),
            "a healthy restart must never fail-stop\n--- capture buffer ---\n{}--- end ---",
            String::from_utf8(buf.lock().unwrap().clone()).unwrap()
        );
        let pending = restarted
            .observability()
            .jumbo_gate_pending
            .load(Ordering::Acquire);
        if restarted.datagram_mtu() == 8960 && !pending {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "restart never converged: rung {}, gate pending {pending}",
            restarted.datagram_mtu()
        );
        std::thread::sleep(Duration::from_millis(25));
    }
    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(
        !text.contains("path_below_committed_mtu"),
        "no refusal anywhere: {text}"
    );

    nodes.push(restarted);
    stop_all(nodes);
}
