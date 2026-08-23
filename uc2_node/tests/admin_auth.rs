// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M12b Task 3 — the node authenticates an admin request BEFORE it does
//! anything with it: leader and follower alike, so a follower never forwards
//! an unauthenticated proposal to the leader.
//!
//! These drive a real in-process cluster through the `uc2ctl` codepath minus
//! the bin (write the auth line, then the request line, then poll the
//! response line), exercising both admin policies:
//!
//! * [`AdminPolicy::Filesystem`] (the `Default`, and byte-for-byte the
//!   pre-M12b posture): the auth line is IGNORED entirely — a request with no
//!   auth line and a request carrying garbage both reach the normal path.
//! * [`AdminPolicy::Hmac`]: unsigned / bad-tag / expired / unknown-key are
//!   refused with reason codes 20-23 and never reach `propose_config`.
//!
//! M12b Task 4 rides along in the same rig: every admin answer these tests
//! provoke must ALSO be on disk in `<instance_dir>/audit.jsonl` by the time
//! the answer is readable. [`admin_request`] proves that ordering on every
//! single request it makes (see its doc), and each test then asserts what the
//! record actually says.
//!
//! Sizing mirrors `reconfig.rs` (journals on ext4 under `CARGO_TARGET_TMPDIR`,
//! 4 MiB no-wrap ring, 150-300 ms election timeouts, whole-box serialization).

use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use uc2_consensus::election::NodeId;
use uc2_crypto::admin::{AdminMessage, sign};
use uc2_log::cnc::{AdminAuth, AdminReq, AdminResp, CncPage};
use uc2_net::fault::FaultConfig;
use uc2_node::{
    AdminKey, AdminPolicy, Node, NodeConfig, REASON_AUTH_BAD_TAG, REASON_AUTH_EXPIRED,
    REASON_AUTH_MISSING, REASON_AUTH_UNKNOWN_KEY, StartOpts,
};

/// The admin-key TTL every `Hmac` test runs under; the far-future-expiry case
/// derives its out-of-window `expiry_ns` from it rather than hardcoding one.
const TTL: Duration = Duration::from_secs(30);

const APP: &str = "adminauth";

/// The `AddLearner` wire op — the one admin op a freshly-booted cluster
/// accepts trivially (see `reconfig.rs`'s
/// `add_learner_via_leader_cnc_is_accepted_and_converges`).
const OP_ADD_LEARNER: u32 = 1;

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn serialize() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn unix_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

fn test_key() -> AdminKey {
    AdminKey::new("ops-test", [7u8; 32])
}

fn hmac_policy() -> AdminPolicy {
    AdminPolicy::Hmac { keys: Arc::new(vec![test_key()]), ttl: TTL }
}

struct NodeH {
    id: NodeId,
    instance_dir: PathBuf,
    node: Node,
}

struct Cluster {
    _dir: tempfile::TempDir,
    nodes: Vec<NodeH>,
}

impl Cluster {
    fn stop(self) {
        for h in self.nodes {
            h.node.stop();
        }
    }
}

fn make_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    addr: SocketAddr,
    instance_dir: PathBuf,
    seed: u64,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        learners: Vec::new(),
        bind: addr,
        instance_dir,
        app_id: APP.into(),
        buffer_bytes: 1 << 22,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed,
        faults: FaultConfig::default(),
        purge: uc2_node::PurgePolicy::Disabled,
        journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc2_node::CryptoConfig::Disabled,
    }
}

fn seed_for(i: usize) -> u64 {
    0x5150_1234_ABCD_0F0F ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Bind every voter's socket first (so the full member map is known up
/// front), then start each node under `policy`.
fn spawn_cluster(n: usize, policy: AdminPolicy) -> Cluster {
    let dir = tempfile::Builder::new()
        .prefix("uc2-admin-auth-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");

    let socks: Vec<UdpSocket> =
        (0..n).map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind")).collect();
    let members: Vec<(NodeId, SocketAddr)> =
        socks.iter().enumerate().map(|(i, s)| (i as NodeId, s.local_addr().unwrap())).collect();

    let mut nodes = Vec::with_capacity(n);
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = members[i].1;
        let instance_dir = dir.path().join(format!("n{i}"));
        let cfg = make_config(i as NodeId, members.clone(), addr, instance_dir.clone(), seed_for(i));
        let opts = StartOpts { socket: Some(sock), admin: policy.clone() };
        let node = Node::start_with(cfg, opts).expect("start");
        nodes.push(NodeH { id: i as NodeId, instance_dir, node });
    }
    Cluster { _dir: dir, nodes }
}

fn deadline_secs(secs: u64) -> Instant {
    Instant::now() + Duration::from_secs(secs)
}

fn await_single_leader(nodes: &[NodeH], secs: u64) -> usize {
    let deadline = deadline_secs(secs);
    loop {
        let serving: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].node.can_serve()).collect();
        assert!(serving.len() <= 1, "split-brain: {serving:?} all serve");
        if serving.len() == 1 {
            return serving[0];
        }
        assert!(Instant::now() < deadline, "no single leader elected");
        std::thread::yield_now();
    }
}

/// Open a node's cnc page directly by its instance dir — exactly the `uc2ctl`
/// attach path, reached here without the bin.
fn open_cnc(instance_dir: &std::path::Path) -> Arc<CncPage> {
    CncPage::open_file(&instance_dir.join("cnc2.dat"), APP).expect("open cnc")
}

// ---------------------------------------------------------------------------
// The audit log (M12b Task 4).
// ---------------------------------------------------------------------------

/// Every line currently in a node's `audit.jsonl` (empty vec if the file does
/// not exist yet — a node that has answered nothing has recorded nothing).
fn audit_lines(instance_dir: &std::path::Path) -> Vec<String> {
    match std::fs::read_to_string(instance_dir.join("audit.jsonl")) {
        Ok(text) => text.lines().map(str::to_string).collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => panic!("reading audit.jsonl: {e}"),
    }
}

/// One field out of an audit line, as text — `"ops-test"` comes back as
/// `ops-test`, `20` as `20`. A deliberately small reader: these lines are a
/// pinned key order and a flat object, so this needs no JSON dependency.
fn field(line: &str, key: &str) -> String {
    let needle = format!("\"{key}\":");
    let start = line.find(&needle).unwrap_or_else(|| panic!("no {key} in {line}")) + needle.len();
    let rest = &line[start..];
    if let Some(stripped) = rest.strip_prefix('"') {
        let end = stripped.find('"').expect("unterminated string value");
        stripped[..end].to_string()
    } else {
        let end = rest.find([',', '}']).expect("unterminated value");
        rest[..end].to_string()
    }
}

/// The (single) record for a locally-submitted admin request, by its band
/// `seq`. Forwarded records at the leader carry `seq` 0 and are matched by
/// `nonce` instead.
fn audit_for_seq(instance_dir: &std::path::Path, seq: u64) -> Option<String> {
    let want = format!("\"seq\":{seq},");
    audit_lines(instance_dir).into_iter().find(|l| l.contains(&want))
}

/// The record for a given nonce and origin — used on the leader for a
/// proposal a follower forwarded.
fn audit_for_nonce(instance_dir: &std::path::Path, nonce: u64, origin: &str) -> Option<String> {
    let want = format!("\"nonce\":{nonce},");
    audit_lines(instance_dir)
        .into_iter()
        .find(|l| l.contains(&want) && field(l, "origin") == origin)
}

/// Assert the standard shape of a record: who, what outcome, what reason.
#[track_caller]
fn assert_record(line: &str, actor: &str, outcome: &str, reason: u32) {
    assert_eq!(field(line, "actor"), actor, "actor in {line}");
    assert_eq!(field(line, "outcome"), outcome, "outcome in {line}");
    assert_eq!(field(line, "reason"), reason.to_string(), "reason in {line}");
    assert_eq!(field(line, "event"), "admin_op", "event in {line}");
}

/// What the admin client puts in the cnc auth line for a request.
enum Auth<'a> {
    /// Clear the line (`AdminAuth::ZERO`) — an unsigned request.
    None,
    /// A non-zero line that is not a valid tag for anything: what a
    /// `Filesystem`-policy node must ignore rather than inspect.
    Garbage,
    /// A real signature, with hooks to corrupt exactly one dimension of it.
    Signed {
        key: &'a AdminKey,
        ttl: Duration,
        /// Flip one byte of the tag after signing (bad-tag case).
        corrupt_tag: bool,
        /// Use this `expiry_ns` instead of `now + ttl`, signing over it
        /// honestly (so the refusal can only come from the expiry rule).
        expiry_override: Option<u64>,
    },
}

/// The `uc2ctl` mutating-command flow, minus the bin: read the admin band's
/// current seq, write the auth line, write a fresh request (`seq = old + 1`,
/// a random nonce), poll the response line for the echoed seq.
///
/// Auth line BEFORE request line — the M12b cnc discipline: `write_admin_req`'s
/// `seq` store is the release that publishes both.
///
/// **The ordering proof (Task 4) lives here**, so every scenario in this file
/// exercises it: the instant `read_admin_resp` first returns the answer, this
/// reads `audit.jsonl` and requires the matching record to already be in it.
/// The read order is what makes the proof sound — the response is observed
/// FIRST and the file only afterwards, so "response present, record absent"
/// can only mean the node published an answer it had not yet recorded. (The
/// other order would be inconclusive: a record written between the two reads
/// would look like a violation.) The record's own text is returned as the
/// third element for the caller to assert on.
fn admin_request(
    cnc: &CncPage,
    instance_dir: &std::path::Path,
    op: u32,
    id: u32,
    addr: (u32, u16),
    auth: Auth<'_>,
) -> (AdminReq, AdminResp, String) {
    let seq = cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0) + 1;
    let nonce = rand::random::<u64>();
    let (ip, port) = addr;

    match auth {
        Auth::None => cnc.write_admin_auth(&AdminAuth::ZERO),
        Auth::Garbage => cnc.write_admin_auth(&AdminAuth {
            tag: [0x5a; 32],
            expiry_ns: unix_ns() + 60_000_000_000,
            key_name_hash: 0xdead_beef_dead_beef,
        }),
        Auth::Signed { key, ttl, corrupt_tag, expiry_override } => {
            let meta = cnc.meta();
            let expiry_ns = expiry_override.unwrap_or_else(|| unix_ns() + ttl.as_nanos() as u64);
            let m = AdminMessage {
                app_id: &meta.app_id,
                instance_id: meta.instance_id,
                seq,
                nonce,
                op,
                id,
                ip,
                port,
                expiry_ns,
            };
            let mut tag = sign(key, &m);
            if corrupt_tag {
                tag[0] ^= 0x01;
            }
            cnc.write_admin_auth(&AdminAuth { tag, expiry_ns, key_name_hash: key.name_hash });
        }
    }

    let req = AdminReq { seq, nonce, op, id, ip, port };
    cnc.write_admin_req(&req);

    let deadline = deadline_secs(10);
    loop {
        if let Some(resp) = cnc.read_admin_resp(seq) {
            let line = audit_for_seq(instance_dir, seq).unwrap_or_else(|| {
                panic!(
                    "record-before-respond violated: seq {seq} was answered \
                     (status={} reason={}) but audit.jsonl holds no record for it.\nfile: {:#?}",
                    resp.status,
                    resp.reason,
                    audit_lines(instance_dir)
                )
            });
            return (req, resp, line);
        }
        assert!(Instant::now() < deadline, "admin response timed out for seq {seq}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A fresh learner id/address per call, so no test's op collides with
/// another's inside the same cluster.
fn fresh_learner(n: u32) -> (u32, (u32, u16)) {
    (100 + n, (u32::from_be_bytes([127, 0, 0, 1]), 59_100 + n as u16))
}

// ---------------------------------------------------------------------------
// Filesystem policy: the auth line is ignored entirely.
// ---------------------------------------------------------------------------

#[test]
fn filesystem_policy_ignores_the_auth_line() {
    let _g = serialize();
    let c = spawn_cluster(1, AdminPolicy::Filesystem);
    let leader = await_single_leader(&c.nodes, 20);
    let dir = c.nodes[leader].instance_dir.clone();
    let cnc = open_cnc(&dir);

    // (a) no auth line at all — today's behaviour, unchanged.
    let (id, addr) = fresh_learner(1);
    let (_, resp, line) = admin_request(&cnc, &dir, OP_ADD_LEARNER, id, addr, Auth::None);
    assert_eq!(resp.status, 0, "unsigned request refused under Filesystem: reason {}", resp.reason);
    // Nothing authenticated the operator — the directory's permissions were
    // the whole boundary, and the record says exactly that.
    assert_record(&line, "filesystem", "accepted", 0);
    assert_eq!(field(&line, "origin"), "local");
    assert_eq!(field(&line, "op_name"), "add_learner");
    assert_eq!(field(&line, "id"), id.to_string());
    assert_eq!(field(&line, "addr"), format!("127.0.0.1:{}", addr.1));
    assert_eq!(field(&line, "config_version"), "1");

    // (b) a garbage auth line — never inspected, so it changes nothing.
    let (id, addr) = fresh_learner(2);
    let (_, resp, line) = admin_request(&cnc, &dir, OP_ADD_LEARNER, id, addr, Auth::Garbage);
    assert!(
        resp.status != 1 || resp.reason < 20,
        "Filesystem policy must not produce an auth refusal: status={} reason={}",
        resp.status,
        resp.reason
    );
    assert_eq!(field(&line, "actor"), "filesystem", "{line}");

    assert_eq!(audit_lines(&dir).len(), 2, "one record per request, no more");
    c.stop();
}

// ---------------------------------------------------------------------------
// Hmac policy: reason codes 20-23.
// ---------------------------------------------------------------------------

#[test]
fn hmac_policy_refuses_unsigned() {
    let _g = serialize();
    let c = spawn_cluster(1, hmac_policy());
    let leader = await_single_leader(&c.nodes, 20);
    let dir = c.nodes[leader].instance_dir.clone();
    let cnc = open_cnc(&dir);

    let version_before = c.nodes[leader].node.config_version();
    let (id, addr) = fresh_learner(1);
    let (_, resp, line) = admin_request(&cnc, &dir, OP_ADD_LEARNER, id, addr, Auth::None);
    assert_eq!(resp.status, 1, "an unsigned request must be refused");
    assert_eq!(resp.reason, REASON_AUTH_MISSING);
    // A refusal is recorded like any other answer — that is the whole point
    // of recording before responding. No key proved who this was, so the
    // record must not claim one (nor claim `filesystem` trust).
    assert_record(&line, "unverified", "refused", REASON_AUTH_MISSING);
    assert_eq!(field(&line, "config_version"), "0", "{line}");
    assert_eq!(
        c.nodes[leader].node.config_version(),
        version_before,
        "a refused request must never reach propose_config"
    );

    c.stop();
}

#[test]
fn hmac_policy_accepts_a_valid_signature() {
    let _g = serialize();
    let c = spawn_cluster(1, hmac_policy());
    let leader = await_single_leader(&c.nodes, 20);
    let dir = c.nodes[leader].instance_dir.clone();
    let cnc = open_cnc(&dir);
    let key = test_key();

    let (id, addr) = fresh_learner(1);
    let (_, resp, line) = admin_request(
        &cnc,
        &dir,
        OP_ADD_LEARNER,
        id,
        addr,
        Auth::Signed {
            key: &key,
            ttl: TTL,
            corrupt_tag: false,
            expiry_override: None,
        },
    );
    assert_eq!(resp.status, 0, "a validly signed add-learner was refused: reason {}", resp.reason);
    assert_eq!(resp.version, 1, "the accepted change bumped the config version");
    // The audit names the KEY that signed it — this is the row that makes the
    // log worth keeping: "ops-test added learner N at 15:04".
    assert_record(&line, "ops-test", "accepted", 0);
    assert_eq!(field(&line, "origin"), "local");
    assert_eq!(field(&line, "op"), OP_ADD_LEARNER.to_string());
    assert_eq!(field(&line, "config_version"), "1", "{line}");

    c.stop();
}

#[test]
fn bad_tag_is_refused() {
    let _g = serialize();
    let c = spawn_cluster(1, hmac_policy());
    let leader = await_single_leader(&c.nodes, 20);
    let dir = c.nodes[leader].instance_dir.clone();
    let cnc = open_cnc(&dir);
    let key = test_key();

    let (id, addr) = fresh_learner(1);
    let (_, resp, line) = admin_request(
        &cnc,
        &dir,
        OP_ADD_LEARNER,
        id,
        addr,
        Auth::Signed {
            key: &key,
            ttl: TTL,
            corrupt_tag: true,
            expiry_override: None,
        },
    );
    assert_eq!(resp.status, 1);
    assert_eq!(resp.reason, REASON_AUTH_BAD_TAG);
    assert_record(&line, "unverified", "refused", REASON_AUTH_BAD_TAG);
    assert_eq!(c.nodes[leader].node.config_version(), 0, "nothing was proposed");

    c.stop();
}

#[test]
fn expired_is_refused() {
    let _g = serialize();
    let c = spawn_cluster(1, hmac_policy());
    let leader = await_single_leader(&c.nodes, 20);
    let dir = c.nodes[leader].instance_dir.clone();
    let cnc = open_cnc(&dir);
    let key = test_key();

    // (a) already past: expiry_ns <= now.
    let (id, addr) = fresh_learner(1);
    let past = unix_ns() - 1_000_000;
    let (_, resp, line) = admin_request(
        &cnc,
        &dir,
        OP_ADD_LEARNER,
        id,
        addr,
        Auth::Signed {
            key: &key,
            ttl: TTL,
            corrupt_tag: false,
            expiry_override: Some(past),
        },
    );
    assert_eq!(resp.status, 1);
    assert_eq!(resp.reason, REASON_AUTH_EXPIRED, "an already-expired request must be refused");
    assert_record(&line, "unverified", "refused", REASON_AUTH_EXPIRED);

    // (b) the other side of the window: a far-future expiry (a clock game)
    // is refused just as hard, even though the signature over it is honest.
    let (id, addr) = fresh_learner(2);
    // Comfortably past the acceptance window, which is `now + 2 * ttl` —
    // derived from the policy's own TTL rather than a hardcoded constant, so
    // retuning `TTL` cannot silently make this case land INSIDE the window.
    let far = unix_ns() + 10 * TTL.as_nanos() as u64;
    let (_, resp, line) = admin_request(
        &cnc,
        &dir,
        OP_ADD_LEARNER,
        id,
        addr,
        Auth::Signed {
            key: &key,
            ttl: TTL,
            corrupt_tag: false,
            expiry_override: Some(far),
        },
    );
    assert_eq!(resp.status, 1);
    assert_eq!(resp.reason, REASON_AUTH_EXPIRED, "a far-future expiry must be refused");
    assert_record(&line, "unverified", "refused", REASON_AUTH_EXPIRED);

    assert_eq!(audit_lines(&dir).len(), 2, "both refusals are on disk");
    assert_eq!(c.nodes[leader].node.config_version(), 0, "nothing was proposed");
    c.stop();
}

#[test]
fn unknown_key_is_refused() {
    let _g = serialize();
    let c = spawn_cluster(1, hmac_policy());
    let leader = await_single_leader(&c.nodes, 20);
    let dir = c.nodes[leader].instance_dir.clone();
    let cnc = open_cnc(&dir);

    // A perfectly valid signature — under a key name the policy never loaded.
    let stranger = AdminKey::new("ops-not-in-policy", [7u8; 32]);
    let (id, addr) = fresh_learner(1);
    let (_, resp, line) = admin_request(
        &cnc,
        &dir,
        OP_ADD_LEARNER,
        id,
        addr,
        Auth::Signed {
            key: &stranger,
            ttl: TTL,
            corrupt_tag: false,
            expiry_override: None,
        },
    );
    assert_eq!(resp.status, 1);
    assert_eq!(resp.reason, REASON_AUTH_UNKNOWN_KEY);
    // Not `ops-not-in-policy`: the record never repeats a name the node could
    // not verify, or an audit reader could be steered by an attacker's own
    // choice of key name.
    assert_record(&line, "unverified", "refused", REASON_AUTH_UNKNOWN_KEY);
    assert_eq!(c.nodes[leader].node.config_version(), 0, "nothing was proposed");

    c.stop();
}

/// Documents the no-replay-ring argument (Task 3's ruled deviation from spec
/// §5.2): a captured request cannot be re-presented AT ITS ORIGINAL `seq`,
/// because the consensus agent only ever reads `seq > last_admin_seq`. So
/// re-writing the identical `(seq, nonce, tag)` triple after acceptance is
/// simply never seen — the node does not re-verify it, does not re-propose
/// it, and does not overwrite its original answer. (Re-presenting it at a
/// HIGHER seq invalidates the tag, which `bad_tag_is_refused` covers.)
#[test]
fn a_replayed_request_cannot_be_re_presented() {
    let _g = serialize();
    let c = spawn_cluster(1, hmac_policy());
    let leader = await_single_leader(&c.nodes, 20);
    let dir = c.nodes[leader].instance_dir.clone();
    let cnc = open_cnc(&dir);
    let key = test_key();

    let (id, addr) = fresh_learner(1);
    let (req, resp, line) = admin_request(
        &cnc,
        &dir,
        OP_ADD_LEARNER,
        id,
        addr,
        Auth::Signed {
            key: &key,
            ttl: TTL,
            corrupt_tag: false,
            expiry_override: None,
        },
    );
    assert_eq!(resp.status, 0, "setup: the first presentation is accepted (reason {})", resp.reason);
    assert_record(&line, "ops-test", "accepted", 0);
    let version_after = c.nodes[leader].node.config_version();
    let audit_before = audit_lines(&dir);
    assert_eq!(audit_before.len(), 1);
    let auth_line = cnc.read_admin_auth();

    // Replay the captured bytes verbatim: same auth line, same request line.
    cnc.write_admin_auth(&auth_line);
    cnc.write_admin_req(&req);

    let until = Instant::now() + Duration::from_millis(500);
    while Instant::now() < until {
        std::thread::sleep(Duration::from_millis(20));
    }

    assert_eq!(
        c.nodes[leader].node.config_version(),
        version_after,
        "the replayed request must not have been applied a second time"
    );
    let now = cnc.read_admin_resp(req.seq).expect("the original answer is still there");
    assert_eq!(now, resp, "the replay produced no second answer — the request was never re-read");
    // And nothing new was recorded: a request that is never read is never
    // answered, so there is nothing to account for.
    assert_eq!(audit_lines(&dir), audit_before, "the replay produced a second audit record");

    c.stop();
}

/// The point of verifying FIRST: a follower must refuse an unauthenticated
/// request itself rather than forwarding it (kind 16) to the leader. The
/// refusal reason comes back from the FOLLOWER's own admin band, and the
/// leader's config version never moves.
#[test]
fn follower_verifies_before_forwarding() {
    let _g = serialize();
    let c = spawn_cluster(3, hmac_policy());
    let leader = await_single_leader(&c.nodes, 20);
    let follower = (0..c.nodes.len()).find(|&i| i != leader).expect("a follower exists");

    let leader_version_before = c.nodes[leader].node.config_version();
    let dir = c.nodes[follower].instance_dir.clone();
    let cnc = open_cnc(&dir);
    let (id, addr) = fresh_learner(1);
    let (_, resp, line) = admin_request(&cnc, &dir, OP_ADD_LEARNER, id, addr, Auth::None);

    assert_eq!(resp.status, 1, "the follower must refuse, not forward and not retry");
    assert_eq!(resp.reason, REASON_AUTH_MISSING);
    assert_eq!(
        c.nodes[leader].node.config_version(),
        leader_version_before,
        "an unauthenticated request must never reach the leader"
    );
    // The refusal is recorded WHERE IT HAPPENED — on the follower, as a
    // local request. The leader never saw it and so has nothing to record.
    assert_record(&line, "unverified", "refused", REASON_AUTH_MISSING);
    assert_eq!(field(&line, "origin"), "local");
    assert!(
        audit_lines(&c.nodes[leader].instance_dir).is_empty(),
        "the leader recorded a request it never received: {:#?}",
        audit_lines(&c.nodes[leader].instance_dir)
    );

    c.stop();
}

/// Task 4's two-node story: a request that a follower authenticates and
/// forwards is recorded TWICE — once by the follower (`origin: local`, the
/// operator's key, written when the leader's reply comes back and the
/// response line is finally published) and once by the leader
/// (`origin: forwarded`, actor `peer:<follower id>` — the leader cannot
/// re-check the operator's signature, so it attests to the peer that
/// vouched). The two records join on `nonce`.
#[test]
fn a_forwarded_request_is_recorded_on_both_nodes() {
    let _g = serialize();
    let c = spawn_cluster(3, hmac_policy());
    let leader = await_single_leader(&c.nodes, 20);
    let follower = (0..c.nodes.len()).find(|&i| i != leader).expect("a follower exists");
    let key = test_key();

    let fdir = c.nodes[follower].instance_dir.clone();
    let ldir = c.nodes[leader].instance_dir.clone();
    let cnc = open_cnc(&fdir);

    // The follower answers `retry` (status 2) while it has no leader hint yet
    // — that is a real answer, and it is audited too. Re-present the request
    // until the forward actually happens.
    let deadline = deadline_secs(20);
    let (req, resp, line) = loop {
        let (id, addr) = fresh_learner(1);
        let got = admin_request(
            &cnc,
            &fdir,
            OP_ADD_LEARNER,
            id,
            addr,
            Auth::Signed { key: &key, ttl: TTL, corrupt_tag: false, expiry_override: None },
        );
        if got.1.status != 2 {
            break got;
        }
        assert_record(&got.2, "ops-test", "retry", 0);
        assert!(Instant::now() < deadline, "the follower never learned a leader hint");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(resp.status, 0, "the forwarded add-learner was refused: reason {}", resp.reason);

    // The follower's record: the operator, locally submitted.
    assert_record(&line, "ops-test", "accepted", 0);
    assert_eq!(field(&line, "origin"), "local");
    assert_eq!(field(&line, "nonce"), req.nonce.to_string());
    assert_eq!(field(&line, "seq"), req.seq.to_string());

    // The leader's record for the SAME nonce: forwarded, attributed to the
    // peer that forwarded it, with seq 0 (the band sequence is the
    // follower's, and the wire proposal does not carry it).
    let deadline = deadline_secs(10);
    let lline = loop {
        if let Some(l) = audit_for_nonce(&ldir, req.nonce, "forwarded") {
            break l;
        }
        assert!(
            Instant::now() < deadline,
            "the leader recorded no forwarded proposal for nonce {}: {:#?}",
            req.nonce,
            audit_lines(&ldir)
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_record(&lline, &format!("peer:{}", c.nodes[follower].id), "accepted", 0);
    assert_eq!(field(&lline, "seq"), "0", "{lline}");
    assert_eq!(field(&lline, "op_name"), "add_learner");
    assert_eq!(field(&lline, "config_version"), resp.version.to_string());

    c.stop();
}
