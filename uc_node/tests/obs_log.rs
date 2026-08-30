// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M10 Task 3: the structured-log transition records at the consensus sites,
//! proven against a real networked cluster (not the unit-level `emit` tests
//! in `uc_node::obs::log`).
//!
//! Two tests share the process-global capture sink
//! ([`uc_node::obs::log::capture_for_tests`]) — serialized via a file-local
//! [`TEST_LOCK`], same discipline as `failover.rs`'s `serialize()`. Cluster
//! plumbing below is a trimmed copy of `failover.rs`'s helpers (test files
//! are separate crates and cannot import each other): just enough to boot a
//! 3-node ring, elect a leader, and drive the M4-era minority-partition heal
//! that forces a truncation.

use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use uc_consensus::election::NodeId;
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig};

/// On-wire size of a 96 B-payload message frame (32 B header + 96 B).
const FRAME: u64 = 128;
/// On-wire size of the header-only NewTerm frame that opens a leadership term.
const NEW_TERM: u64 = 32;
/// Payload every submit carries.
const PAYLOAD: usize = 96;

/// Process-global lock: the capture sink is process-global, so only one of
/// these tests (or a `uc_node::obs::log` unit test, if run in the same
/// process) may hold it at a time.
static TEST_LOCK: Mutex<()> = Mutex::new(());

fn serialize() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

// ------------------------------------------------------------- node handle

struct NodeH {
    addr: SocketAddr,
    node: Option<Node>,
}

impl NodeH {
    fn n(&self) -> &Node {
        self.node.as_ref().expect("node stopped")
    }
    fn is_leader(&self) -> bool {
        self.n().is_leader()
    }
    fn can_serve(&self) -> bool {
        self.n().can_serve()
    }
    fn commit(&self) -> u64 {
        self.n().counters().commit.load_acquire()
    }
    fn truncations(&self) -> u64 {
        self.n().truncations()
    }

    fn try_submit(&self, payload: Vec<u8>) -> Result<(), uc_node::SubmitError> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match self.n().submit(payload.clone()) {
                Ok(()) => return Ok(()),
                Err(uc_node::SubmitError::Full) => {
                    assert!(Instant::now() < deadline, "ingress stayed full");
                    std::thread::yield_now();
                }
                Err(e) => return Err(e),
            }
        }
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
        if let Some(node) = self.node.take() {
            node.stop();
        }
    }
}

fn make_config_ring(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    instance_dir: PathBuf,
    seed: u64,
    addr: SocketAddr,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        bind: addr,
        instance_dir,
        app_id: "obs-log".into(),
        buffer_bytes: 1 << 22,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed,
        faults: FaultConfig::default(),
        purge: uc_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::none_for_tests(),
    }
}

fn seed_for(i: usize) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

struct Cluster {
    _dir: tempfile::TempDir,
    nodes: Vec<NodeH>,
}

fn spawn_cluster(n: usize) -> Cluster {
    let dir = tempfile::Builder::new()
        .prefix("uc2-obs-log-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");

    let socks: Vec<UdpSocket> =
        (0..n).map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind")).collect();
    let members: Vec<(NodeId, SocketAddr)> = socks
        .iter()
        .enumerate()
        .map(|(i, s)| (i as NodeId, s.local_addr().unwrap()))
        .collect();

    let mut nodes = Vec::with_capacity(n);
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = members[i].1;
        let instance_dir = dir.path().join(format!("n{i}"));
        let seed = seed_for(i);
        let cfg = make_config_ring(i as NodeId, members.clone(), instance_dir, seed, addr);
        let node = Node::start_with_socket(cfg, sock).expect("start");
        nodes.push(NodeH { addr, node: Some(node) });
    }
    Cluster { _dir: dir, nodes }
}

impl Cluster {
    fn stop_all(&mut self) {
        for node in &mut self.nodes {
            node.stop();
        }
    }
}

// ------------------------------------------------------------- wait helpers

fn deadline_secs(secs: u64) -> Instant {
    Instant::now() + Duration::from_secs(secs)
}

fn await_until(secs: u64, msg: &str, mut f: impl FnMut() -> bool) {
    let deadline = deadline_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "{msg}");
        std::thread::yield_now();
    }
}

/// Wait for exactly one serving leader and return its index.
fn await_single_leader(nodes: &[NodeH], secs: u64) -> usize {
    let deadline = deadline_secs(secs);
    loop {
        let serving: Vec<usize> =
            (0..nodes.len()).filter(|&i| nodes[i].node.is_some() && nodes[i].can_serve()).collect();
        assert!(serving.len() <= 1, "split-brain: nodes {serving:?} all serve");
        if serving.len() == 1 {
            let i = serving[0];
            assert!(nodes[i].is_leader(), "serving node {i} is not flagged leader");
            return i;
        }
        assert!(Instant::now() < deadline, "no single leader elected");
        std::thread::yield_now();
    }
}

fn await_serving_among(nodes: &[NodeH], idxs: &[usize], secs: u64) -> usize {
    let deadline = deadline_secs(secs);
    loop {
        let serving: Vec<usize> = idxs.iter().copied().filter(|&i| nodes[i].can_serve()).collect();
        assert!(serving.len() <= 1, "split-brain among {idxs:?}: {serving:?} serve");
        if serving.len() == 1 {
            return serving[0];
        }
        assert!(Instant::now() < deadline, "no leader among {idxs:?}");
        std::thread::yield_now();
    }
}

fn submit_n(node: &NodeH, n: u64) {
    for i in 0..n {
        let mut p = vec![0u8; PAYLOAD];
        p[..8].copy_from_slice(&i.to_le_bytes());
        node.try_submit(p).expect("submit to serving leader");
    }
}

fn partition(a: &NodeH, b: &NodeH) {
    a.block(b.addr);
    b.block(a.addr);
}

/// The M4-era minority-partition-then-heal choreography (trimmed from
/// `failover.rs`'s `drive_minority_partition`): isolate the leader with a
/// minority, commit fresh data on the majority under a new term, then return
/// the indices/positions the caller needs to heal and observe truncation.
fn drive_minority_partition(c: &mut Cluster, phantom: u64, fresh: u64) -> (usize, usize) {
    let old = await_single_leader(&c.nodes, 30);
    let pre = NEW_TERM + 1000 * FRAME;
    submit_n(&c.nodes[old], 1000);
    await_until(60, "pre-partition commit", || c.nodes[old].commit() >= pre);
    for i in (0..c.nodes.len()).filter(|&i| i != old) {
        await_until(60, "pre-partition commit (follower)", || c.nodes[i].commit() >= pre);
    }

    let followers: Vec<usize> = (0..c.nodes.len()).filter(|&i| i != old).collect();
    for &f in &followers {
        partition(&c.nodes[old], &c.nodes[f]);
    }

    for _ in 0..phantom {
        if c.nodes[old].try_submit(vec![0xEE; PAYLOAD]).is_err() {
            break;
        }
    }

    let new = await_serving_among(&c.nodes, &followers, 30);
    submit_n(&c.nodes[new], fresh);
    let majority_target = pre + NEW_TERM + fresh * FRAME;
    for &f in &followers {
        await_until(60, &format!("majority node {f} commit"), || {
            c.nodes[f].commit() >= majority_target
        });
    }
    (old, new)
}

// =================================================================== tests

/// Every line the capture sink accumulated must be one valid JSON object:
/// starts with `{`, ends with `}`, one line each (`\n`-terminated), no
/// half-written record. Hand-rolled brace/quote sanity — no serde_json dep.
fn assert_json_lines(text: &str) {
    for line in text.lines() {
        assert!(line.starts_with('{') && line.ends_with('}'), "not a JSON object line: {line}");
        // Every `"` must be part of a balanced, non-escaped pair count.
        let mut in_str = false;
        let mut escaped = false;
        let mut depth = 0i32;
        for c in line.chars() {
            if in_str {
                if escaped {
                    escaped = false;
                } else if c == '\\' {
                    escaped = true;
                } else if c == '"' {
                    in_str = false;
                }
                continue;
            }
            match c {
                '"' => in_str = true,
                '{' => depth += 1,
                '}' => depth -= 1,
                _ => {}
            }
        }
        assert!(!in_str, "unterminated string in line: {line}");
        assert_eq!(depth, 0, "unbalanced braces in line: {line}");
    }
}

/// Poll the capture buffer until `pred` matches the accumulated text, or
/// panic (with the buffer's contents) after `secs`.
///
/// Closes a real race, not a flaky-test workaround: `can_serve()`/
/// `truncations()` etc. observe the state-machine transition directly
/// (`ElectionSm`/atomics), while the matching `obs_event!` record is written
/// by `publish_status`/the `exec` arm on its own cadence — there is a window
/// (up to one duty cycle) where the state has already flipped but the record
/// has not yet landed in the sink. A single post-`await_until` snapshot of
/// the buffer can race that window; polling the buffer itself with its own
/// deadline (not just the state) closes it the same way `await_until` closes
/// the state race.
fn await_capture(
    buf: &Arc<Mutex<Vec<u8>>>,
    secs: u64,
    msg: &str,
    mut pred: impl FnMut(&str) -> bool,
) -> String {
    let deadline = deadline_secs(secs);
    loop {
        let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        if pred(&text) {
            return text;
        }
        if Instant::now() >= deadline {
            panic!("{msg}\n--- capture buffer ---\n{text}--- end capture buffer ---");
        }
        std::thread::sleep(Duration::from_millis(15));
    }
}

/// A clean boot elects one leader: the winner emits `became_leader`, and the
/// serving-edge (leader or a follower once it can serve) emits
/// `serving_changed` with `can_serve:true`.
#[test]
fn an_election_emits_became_leader_and_followers_note_it() {
    let _g = serialize();
    let buf = uc_node::obs::log::capture_for_tests();
    let mut cluster = spawn_cluster(3);
    let leader = await_single_leader(&cluster.nodes, 10);
    await_until(10, "leader never reported can_serve", || cluster.nodes[leader].can_serve());

    let text = await_capture(
        &buf,
        5,
        "became_leader/serving_changed records never landed in the capture buffer",
        |t| {
            t.lines().any(|l| l.contains(r#""event":"became_leader""#))
                && t.lines().any(|l| {
                    l.contains(r#""event":"serving_changed""#) && l.contains(r#""can_serve":true"#)
                })
        },
    );
    assert_json_lines(&text);

    cluster.stop_all();
    uc_node::obs::log::stderr_for_tests();
}

/// The M4-era choreography: partition the leader with a minority, commit on
/// the majority side under a new term, heal — the old leader must truncate
/// its uncommitted tail, and M10 must say so.
#[test]
fn a_healed_deposed_leader_emits_log_truncated() {
    let _g = serialize();
    let buf = uc_node::obs::log::capture_for_tests();
    let mut cluster = spawn_cluster(3);

    let (old, _new) = drive_minority_partition(&mut cluster, 64, 200);
    assert_eq!(cluster.nodes[old].truncations(), 0, "premature truncation before heal");

    for node in &cluster.nodes {
        node.heal();
    }
    await_until(30, "ex-leader did not reconcile/truncate", || {
        cluster.nodes[old].truncations() >= 1
    });

    let text = await_capture(&buf, 5, "log_truncated record never landed in the capture buffer", |t| {
        t.lines().any(|l| l.contains(r#""event":"log_truncated""#))
    });
    assert_json_lines(&text);

    cluster.stop_all();
    uc_node::obs::log::stderr_for_tests();
}
