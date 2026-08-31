// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M8 Task 17 capstone: **a crypto-enabled cluster that actually forms.**
//!
//! This is the payoff the whole M8 milestone has been building toward, and it
//! is only reachable now. T10 sealed the sender agent's `DATA`/`HEARTBEAT`
//! fan-out. T11 made the receive path DROP anything unsealed once crypto is
//! on. T12 wired the handshake plane. Between them that left a cluster which
//! could not run at all: the node's own `REQUEST_VOTE`/`VOTE`/
//! `COMMIT_POSITION`/`TERM_MAP`/`READ_PROBE` were still cleartext and were
//! therefore dropped by every peer — no elections, no commit gossip, no
//! linearizable reads. T17 seals them, and this test is what proves it.
//!
//! Three real [`Node`]s over loopback UDP, each with its own X25519 identity
//! and a real allowlist on disk, all four polling agents live. Nothing is
//! stubbed: the Noise-IK handshakes, the group-key mint/deliver/ack, and every
//! consensus datagram are the production paths.
//!
//! ## What makes this discriminating rather than decorative
//!
//! "The cluster works with crypto enabled" is only interesting if the traffic
//! is genuinely sealed — a build that quietly sent everything in the clear
//! would pass it too. So the second test runs a MIXED cluster: two nodes with
//! crypto on, one with it off. The cleartext node must be unable to
//! participate (its datagrams fail authentication at both peers, and theirs
//! fail its length checks), while the two sealed nodes still form a quorum and
//! serve. If sealing were not actually happening on the wire, the cleartext
//! node would join right in and that test would fail.
//!
//! Scratch (key files, journals) lives under `CARGO_TARGET_TMPDIR` — real
//! ext4, never `/tmp` (RAM-backed tmpfs with no swap on the dev box; see
//! CLAUDE.md).

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use uc_client::Client;
use uc_net::fault::FaultConfig;
use uc_node::{CryptoConfig, Node, NodeConfig};
use uc_service::{ServiceBuilder, ServiceConfig, StateMachine};

const APP: &str = "crypto-cluster";

/// Process-global lock: three nodes x four busy-spin agents x two tests is
/// well past the core count, and the election budget is timing-sensitive.
/// Same reasoning (and the same shape) as `failover.rs`'s `serialize`.
static TEST_LOCK: Mutex<()> = Mutex::new(());
fn serialize() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

// ------------------------------------------------------------ state machine

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Cmd {
    Add(u64),
}

#[derive(Default)]
struct CountSm {
    total: u64,
    last_applied: Option<u64>,
}

impl StateMachine for CountSm {
    type Command = Cmd;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, position: u64, cmd: Cmd) -> u64 {
        let Cmd::Add(n) = cmd;
        self.total += n;
        self.last_applied = Some(position);
        self.total
    }
    fn query(&self, _q: ()) -> u64 {
        self.total
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

// --------------------------------------------------------- crypto fixtures

/// Deterministic per-node private keys — the fixture must be reproducible, and
/// these never leave `CARGO_TARGET_TMPDIR`.
fn private_key(i: usize) -> [u8; 32] {
    [0x60u8 + i as u8; 32]
}

fn write_key_file(path: &Path, private: [u8; 32]) {
    std::fs::write(path, private).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}

/// Standard-alphabet base64 with padding, matching `uc_crypto::identity`'s
/// allowlist parser (the `base64` crate's `STANDARD` engine). Hand-rolled
/// rather than adding a `base64` dev-dependency for one fixture.
fn b64_32(bytes: &[u8; 32]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[((n >> 18) & 0x3F) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3F) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[((n >> 6) & 0x3F) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(n & 0x3F) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// Writes every node's key file, derives every public key through
/// `uc_crypto::identity::Identity` (the crate's own accessor — this test has
/// no X25519 dependency of its own), and writes one allowlist naming all `n`
/// nodes. Returns per-node `(key_path, allowlist_path)`.
fn write_crypto_material(dir: &Path, n: usize) -> Vec<(PathBuf, PathBuf)> {
    let mut key_paths = Vec::with_capacity(n);
    let mut publics = Vec::with_capacity(n);
    for i in 0..n {
        let node_dir = dir.join(format!("keys{i}"));
        std::fs::create_dir_all(&node_dir).unwrap();
        let key_path = node_dir.join("node.key");
        write_key_file(&key_path, private_key(i));
        publics.push(
            uc_crypto::identity::Identity::load(&key_path)
                .unwrap()
                .public_bytes(),
        );
        key_paths.push(key_path);
    }
    let mut text = String::new();
    for (i, public) in publics.iter().enumerate() {
        text.push_str(&format!("{i} {}\n", b64_32(public)));
    }
    // One shared allowlist file: every node trusts every other node, which is
    // exactly what an operator's `uc2ctl`-managed allowlist looks like.
    let allow_path = dir.join("allowlist");
    std::fs::write(&allow_path, text).unwrap();
    key_paths
        .into_iter()
        .map(|k| (k, allow_path.clone()))
        .collect()
}

// ------------------------------------------------------------------ cluster

fn make_config(
    id: u32,
    members: Vec<(u32, SocketAddr)>,
    instance_dir: PathBuf,
    seed: u64,
    addr: SocketAddr,
    crypto: CryptoConfig,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        bind: addr,
        instance_dir,
        app_id: APP.into(),
        buffer_bytes: 1 << 22, // 4 MiB: no wrap within the test
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed,
        faults: FaultConfig::default(),
        purge: uc_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto,
        services: uc_node::ServicesConfig::default(),
    }
}

struct Cluster {
    _dir: tempfile::TempDir,
    dirs: Vec<PathBuf>,
    nodes: Vec<Node>,
}

/// `crypto_on[i]` decides whether node `i` boots with wire crypto enabled.
fn spawn_cluster(crypto_on: &[bool]) -> Cluster {
    let n = crypto_on.len();
    let dir = tempfile::Builder::new()
        .prefix("uc2-crypto-cluster-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    assert!(
        !dir.path().starts_with("/tmp"),
        "test scratch must not live on tmpfs"
    );

    let material = write_crypto_material(dir.path(), n);

    // Bind every socket first so the full member map is known before any agent
    // runs (a node that learned a peer's address late would drop its traffic).
    let socks: Vec<UdpSocket> = (0..n)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind"))
        .collect();
    let members: Vec<(u32, SocketAddr)> = socks
        .iter()
        .enumerate()
        .map(|(i, s)| (i as u32, s.local_addr().unwrap()))
        .collect();

    let mut dirs = Vec::with_capacity(n);
    let mut nodes = Vec::with_capacity(n);
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = members[i].1;
        let instance_dir = dir.path().join(format!("n{i}"));
        dirs.push(instance_dir.clone());
        let seed = 0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let crypto = if crypto_on[i] {
            let (key_path, allowlist_path) = material[i].clone();
            CryptoConfig::Enabled {
                key_path,
                allowlist_path,
                rotation: uc_crypto::rotation::RotationPolicy::default(),
            }
        } else {
            CryptoConfig::Disabled
        };
        let cfg = make_config(i as u32, members.clone(), instance_dir, seed, addr, crypto);
        nodes.push(Node::start_with_socket(cfg, sock).expect("start"));
    }
    Cluster {
        _dir: dir,
        dirs,
        nodes,
    }
}

/// Wait for exactly one serving leader among `candidates`; assert no
/// split-brain throughout.
fn await_single_leader(nodes: &[Node], candidates: &[usize], secs: u64) -> usize {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let serving: Vec<usize> = candidates
            .iter()
            .copied()
            .filter(|&i| nodes[i].can_serve())
            .collect();
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
        assert!(
            Instant::now() < deadline,
            "no single leader elected within {secs}s — a crypto-enabled cluster did not form"
        );
        std::thread::yield_now();
    }
}

// -------------------------------------------------------------------- tests

/// **The payoff.** A crypto-enabled cluster elects a leader, replicates
/// committed writes to every node, and serves a linearizable read.
///
/// Each of those three exercises a different sealed path that did not exist
/// before this task:
/// * the election needs sealed `REQUEST_VOTE`/`VOTE` (pairwise, `Consensus::send`),
/// * replication convergence needs sealed `COMMIT_POSITION` gossip and
///   `TERM_MAP` (group + pairwise, `fan_out_group` and `send`) on top of T10's
///   `DATA` fan-out and T17's sealed follower `APPEND_POSITION`/`STATUS`/`NAK`,
/// * the linearizable read needs sealed `READ_PROBE` (group) and
///   `READ_PROBE_ACK` (pairwise).
///
/// Any one of those left cleartext and this test hangs on its deadline —
/// but note WHY, because the dependency is not self-evident (T17 review, M3):
/// it hangs because T11's receive rule DROPS unsealed traffic once crypto is
/// on, not because this test can see the wire. A hypothetical build with both
/// sides relaxed — sends left cleartext AND the receive rule admitting them —
/// would pass this test and the one below it. The on-the-wire property is
/// pinned separately and directly, by
/// `uc_node::node::tests::every_consensus_datagram_the_node_emits_is_sealed`,
/// which opens each datagram on a peer's real `ReceiveHalf`. This test proves
/// the two halves COMPOSE into a working cluster; that one proves the bytes
/// are actually sealed.
#[test]
fn a_crypto_enabled_cluster_elects_replicates_and_serves_a_linearizable_read() {
    let _g = serialize();
    let mut c = spawn_cluster(&[true, true, true]);
    let all: Vec<usize> = (0..3).collect();
    let leader = await_single_leader(&c.nodes, &all, 60);

    // The leader minted a group epoch on winning (rotation trigger 1) — the
    // group-scope traffic above is sealed under it, not under nothing.
    assert!(
        c.nodes[leader].crypto_epoch().is_some(),
        "the elected leader must have minted a group epoch"
    );

    let leader_dir = c.dirs[leader].clone();
    let svc = ServiceBuilder::new(ServiceConfig::new(&leader_dir, APP), CountSm::default())
        .start()
        .unwrap();
    let client = Client::connect(&leader_dir, APP).unwrap();

    // Twenty committed writes. Each `submit` returns only after the command is
    // quorum-committed and applied, so this is already proof that DATA
    // replication and the follower's sealed AppendPosition reports are working.
    for i in 1..=20u64 {
        let total: u64 = client
            .submit(&Cmd::Add(1))
            .expect("submit to a serving crypto leader");
        assert_eq!(
            total, i,
            "the state machine advanced exactly once per submit"
        );
    }

    // A linearizable read: READ_PROBE fan-out (group scope, sealed through
    // `SharedTransport::seal_control` — the branch this task added) plus the
    // followers' READ_PROBE_ACKs (pairwise).
    let seen: u64 = client
        .query_linearizable(&())
        .expect("a healthy crypto leader answers");
    assert_eq!(
        seen, 20,
        "the linearizable read must observe every committed write"
    );

    // Every node — not just the quorum that acked — converged on the same
    // commit position, which needs the sealed commit gossip to be landing.
    let target = c.nodes[leader].counters().commit.load_acquire();
    assert!(target > 0, "the leader actually committed something");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let laggards: Vec<usize> = (0..3)
            .filter(|&i| c.nodes[i].counters().commit.load_acquire() < target)
            .collect();
        if laggards.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "nodes {laggards:?} never reached commit {target} — sealed replication or commit \
             gossip is not landing"
        );
        std::thread::yield_now();
    }

    client.shutdown();
    svc.stop();
    for n in c.nodes.drain(..) {
        n.stop();
    }
}

/// The discriminating control: **sealing is actually enforced on the wire.**
///
/// Two nodes with crypto on, one with it off. The two sealed nodes are a
/// majority, so they must still elect and serve — while the cleartext node
/// must be unable to participate at all: its datagrams fail authentication at
/// both peers (T11's receive rule), and theirs are unparseable to it.
///
/// This is what stops the test above from being decorative. A build that
/// enabled crypto but quietly shipped everything in the clear would pass that
/// one and fail this one, because the cleartext node would join right in —
/// its commit would track the leader's instead of staying at 0.
#[test]
fn a_cleartext_node_cannot_join_a_sealed_cluster() {
    let _g = serialize();
    let mut c = spawn_cluster(&[true, true, false]);
    // Only the two sealed nodes can lead: node 2's votes never authenticate.
    let leader = await_single_leader(&c.nodes, &[0, 1], 60);
    assert_ne!(leader, 2);

    let leader_dir = c.dirs[leader].clone();
    let svc = ServiceBuilder::new(ServiceConfig::new(&leader_dir, APP), CountSm::default())
        .start()
        .unwrap();
    let client = Client::connect(&leader_dir, APP).unwrap();
    for _ in 0..10 {
        let _: u64 = client
            .submit(&Cmd::Add(1))
            .expect("the sealed majority still commits");
    }
    let committed = c.nodes[leader].counters().commit.load_acquire();
    assert!(committed > 0, "the sealed pair committed");

    // The follower that shares the leader's crypto config caught up.
    let sealed_follower = if leader == 0 { 1 } else { 0 };
    let deadline = Instant::now() + Duration::from_secs(20);
    while c.nodes[sealed_follower].counters().append.load_acquire() < committed {
        assert!(
            Instant::now() < deadline,
            "the sealed follower never caught up"
        );
        std::thread::yield_now();
    }

    // The cleartext one is frozen out. The counter to assert on is `append`
    // (bytes this node actually validated into its log buffer), NOT `commit`:
    // the datagram HEADER is cleartext by design (spec §4 — it is
    // authenticated as AAD, not hidden, because the receiver must demux by
    // `kind` and locate by `position` before it can look up a key), so a
    // crypto-disabled node still reads `COMMIT_POSITION`'s header and follows
    // the gossiped position. That is a degenerate but bounded state — the
    // apply agent serves `min(commit, durable)`, so a commit counter running
    // ahead of an empty log applies nothing — and it is the operator error
    // T11's `peer_appears_cleartext` diagnostic exists to surface on the
    // sealed side. What the node CANNOT do is obtain a single replicated
    // byte: `DATA`'s payload is ciphertext, so its frame headers do not parse
    // and every datagram is dropped as malformed.
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(
        c.nodes[2].counters().append.load_acquire(),
        0,
        "a cleartext node replicated real bytes inside a sealed cluster — the wire is not \
         actually sealed"
    );
    assert!(!c.nodes[2].can_serve(), "and it can never serve reads");

    client.shutdown();
    svc.stop();
    for n in c.nodes.drain(..) {
        n.stop();
    }
}

/// **Regression pin for the cold-start livelock T17's capstone uncovered**
/// (fixed in `uc_crypto::group::GroupPlane::mint` — see its
/// `a_superseding_mint_inherits_an_unactivated_epochs_activation_clock`).
///
/// A 3-member cluster whose third member never comes up: an ordinary rolling
/// restart, or one host slow to boot. The two live nodes are a majority and
/// must elect, replicate and serve.
///
/// Before the fix this hung for the full deadline, printing
/// `dropped a kind-6 fan-out: no usable group key` forever. The absent member
/// can never ack the group key, so activation depended entirely on the 2 s
/// grace — but the node layer mints on every `BecomeLeader`, and with no
/// sealable `DATA`/`HEARTBEAT` the live follower timed out every 150-300 ms
/// and forced a fresh election, whose winner minted again and reset the grace.
/// The grace never elapsed; the cluster never formed.
///
/// Discriminating BECAUSE the third member is absent from the very start:
/// once any epoch has activated once, `sealing_epoch` falls back to it and
/// the livelock is unreachable — which is exactly why the healthy 3-of-3 test
/// above passed throughout.
#[test]
fn a_cluster_forms_even_when_one_member_never_comes_up() {
    let _g = serialize();
    let dir = tempfile::Builder::new()
        .prefix("uc2-crypto-coldstart-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let material = write_crypto_material(dir.path(), 3);

    // Bind all three sockets so the member map is complete and node 2's
    // address is real (it just never has a node behind it).
    let socks: Vec<UdpSocket> = (0..3)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind"))
        .collect();
    let members: Vec<(u32, SocketAddr)> = socks
        .iter()
        .enumerate()
        .map(|(i, s)| (i as u32, s.local_addr().unwrap()))
        .collect();

    let mut socks = socks.into_iter();
    let mut dirs = Vec::new();
    let mut nodes = Vec::new();
    for i in 0..2usize {
        let sock = socks.next().unwrap();
        let addr = members[i].1;
        let instance_dir = dir.path().join(format!("n{i}"));
        dirs.push(instance_dir.clone());
        let seed = 0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let (key_path, allowlist_path) = material[i].clone();
        let cfg = make_config(
            i as u32,
            members.clone(),
            instance_dir,
            seed,
            addr,
            CryptoConfig::Enabled {
                key_path,
                allowlist_path,
                rotation: uc_crypto::rotation::RotationPolicy::default(),
            },
        );
        nodes.push(Node::start_with_socket(cfg, sock).expect("start"));
    }

    let leader = await_single_leader(&nodes, &[0, 1], 60);
    let leader_dir = dirs[leader].clone();
    let svc = ServiceBuilder::new(ServiceConfig::new(&leader_dir, APP), CountSm::default())
        .start()
        .unwrap();
    let client = Client::connect(&leader_dir, APP).unwrap();
    for i in 1..=5u64 {
        let total: u64 = client
            .submit(&Cmd::Add(1))
            .expect("the live majority commits");
        assert_eq!(total, i);
    }
    let seen: u64 = client
        .query_linearizable(&())
        .expect("and serves a linearizable read");
    assert_eq!(seen, 5);

    client.shutdown();
    svc.stop();
    for n in nodes.drain(..) {
        n.stop();
    }
}
