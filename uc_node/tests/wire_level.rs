// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Wire-level spec §4 and §6.2, in process: the committed cluster wire level
//! on real loopback clusters whose members run different binary ranges
//! (`StartOpts.wire`), so one tree plays both sides of a roll.
//!
//! 1. a holdout member blocks the auto raise, and its restart lands it;
//! 2. manual mode (flipped by admin op 7) holds until `wire raise` (op 12),
//!    which a follower answers `retry`, and which at level is a no-op success
//!    that appends nothing (Ruling 9);
//! 3. removing a learner holdout (admin op 4) unblocks the raise;
//! 4. a rolling restart that changes the leader mid-roll still raises;
//! 5. a binary whose `WIRE_MIN` is above the committed level fail-stops its
//!    cluster agent by name (`WireLevelBelowBinary`) once JOINED, while the
//!    rest of the cluster keeps serving.
//!
//! Harness after `reconfig.rs` / `learner.rs` / `jumbo.rs` (sockets pre-bound
//! so every member's address is known before any agent runs, journals under
//! `CARGO_TARGET_TMPDIR`, 150–300 ms election timeouts, whole-file
//! serialization — the scenarios each run three busy-spin nodes, and
//! scenario 5 swaps the process-global obs capture sink).

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use uc_consensus::election::NodeId;
use uc_log::cnc::{AdminReq, AdminResp, CncPage};
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig, StartOpts};
use uc_protocol::v2::cnc::{ADMIN_OP_SETTINGS_APPLY, ADMIN_OP_WIRE_RAISE};
use uc_protocol::v2::settings::{Settings, WIRE_RAISE_AUTO, WIRE_RAISE_MANUAL};
use uc_protocol::version::{WIRE_LEVEL_FIRST, WireRange};

const APP: &str = "wire_level";

/// Admin op 4 (`uc_node::audit::op_name`): remove a learner.
const ADMIN_OP_REMOVE_LEARNER: u32 = 4;

/// A binary that can roll one level forward from the first level.
const NEXT: WireRange = WireRange {
    min: WIRE_LEVEL_FIRST,
    max: WIRE_LEVEL_FIRST + 1,
};
/// A binary that speaks only the first level — the holdout.
const OLD: WireRange = WireRange {
    min: WIRE_LEVEL_FIRST,
    max: WIRE_LEVEL_FIRST,
};

static TEST_LOCK: Mutex<()> = Mutex::new(());

/// The whole-file lock, plus the process-global obs sink swapped to a
/// capture buffer for the test's life (so the nodes' JSON lines stay out of
/// the test output, and scenario 5 can read its fault record); the sink goes
/// back to stderr on drop.
struct Serial {
    buf: Arc<Mutex<Vec<u8>>>,
    _lock: MutexGuard<'static, ()>,
}

impl Drop for Serial {
    fn drop(&mut self) {
        uc_node::obs::log::stderr_for_tests();
    }
}

fn serialize() -> Serial {
    let lock = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    Serial {
        buf: uc_node::obs::log::capture_for_tests(),
        _lock: lock,
    }
}

// ------------------------------------------------------------------ harness

fn seed_for(i: usize) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Every socket bound and every instance dir named up front. Voters are ids
/// `0..voters`, learners the rest (as `learner.rs` lays them out).
struct Fleet {
    _dir: tempfile::TempDir,
    dirs: Vec<PathBuf>,
    socks: Vec<Option<UdpSocket>>,
    addrs: Vec<SocketAddr>,
    members: Vec<(NodeId, SocketAddr)>,
    learners: Vec<(NodeId, SocketAddr)>,
    genesis: Settings,
    nodes: Vec<Option<Node>>,
}

fn fleet(voters: usize, learners: usize) -> Fleet {
    fleet_with(voters, learners, Settings::genesis_default())
}

fn fleet_with(voters: usize, learners: usize, genesis: Settings) -> Fleet {
    let dir = tempfile::Builder::new()
        .prefix("uc2-wire-level-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let total = voters + learners;
    let socks: Vec<UdpSocket> = (0..total)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind"))
        .collect();
    let all: Vec<(NodeId, SocketAddr)> = socks
        .iter()
        .enumerate()
        .map(|(i, s)| (i as NodeId, s.local_addr().unwrap()))
        .collect();
    Fleet {
        dirs: (0..total)
            .map(|i| dir.path().join(format!("n{i}")))
            .collect(),
        _dir: dir,
        socks: socks.into_iter().map(Some).collect(),
        addrs: all.iter().map(|&(_, a)| a).collect(),
        members: all[..voters].to_vec(),
        learners: all[voters..].to_vec(),
        genesis,
        nodes: (0..total).map(|_| None).collect(),
    }
}

impl Fleet {
    /// Start member `i` with binary range `wire` on its pre-bound socket.
    fn start(&mut self, i: usize, wire: WireRange) {
        assert!(self.nodes[i].is_none(), "member {i} already running");
        let sock = self.socks[i].take().expect("socket already handed out");
        let cfg = NodeConfig {
            id: i as NodeId,
            members: self.members.clone(),
            learners: self.learners.clone(),
            bind: self.addrs[i],
            instance_dir: self.dirs[i].clone(),
            app_id: APP.into(),
            buffer_bytes: 1 << 22,
            max_payload: 256,
            admission_bytes_default: 256 * 1024,
            settings_genesis: self.genesis,
            force_jumbo_frames: false,
            election_timeout_min_ns: 150_000_000,
            election_timeout_max_ns: 300_000_000,
            seed: seed_for(i),
            faults: FaultConfig::default(),
            purge: uc_node::PurgePolicy::Disabled,
            journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
            crypto: uc_node::CryptoConfig::Disabled,
            services: uc_node::ServicesConfig::none_for_tests(),
        };
        let node = Node::start_with(
            cfg,
            StartOpts {
                socket: Some(sock),
                wire: Some(wire),
                ..Default::default()
            },
        )
        .expect("start");
        assert_eq!(node.wire_range(), wire, "StartOpts.wire took");
        self.nodes[i] = Some(node);
    }

    /// Stop member `i` and re-bind its original address, so it can be
    /// started again against the same instance dir — what a restart is.
    fn stop(&mut self, i: usize) {
        drop(self.nodes[i].take().expect("member not running"));
        self.socks[i] = Some(rebind(self.addrs[i]));
    }

    fn restart(&mut self, i: usize, wire: WireRange) {
        self.stop(i);
        self.start(i, wire);
    }

    fn node(&self, i: usize) -> &Node {
        self.nodes[i].as_ref().expect("member not running")
    }

    fn running(&self) -> Vec<usize> {
        (0..self.nodes.len())
            .filter(|&i| self.nodes[i].is_some())
            .collect()
    }

    fn cnc(&self, i: usize) -> Arc<CncPage> {
        CncPage::open_file(&self.dirs[i].join("cnc2.dat"), APP).expect("open cnc")
    }
}

impl Drop for Fleet {
    fn drop(&mut self) {
        for n in &mut self.nodes {
            drop(n.take());
        }
    }
}

fn rebind(addr: SocketAddr) -> UdpSocket {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match UdpSocket::bind(addr) {
            Ok(s) => return s,
            Err(_) if Instant::now() < deadline => std::thread::yield_now(),
            Err(e) => panic!("rebind {addr} failed: {e}"),
        }
    }
}

fn await_until(secs: u64, what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "timed out after {secs}s: {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Exactly one running member among `among` that leads and serves.
fn await_leader_among(f: &Fleet, among: &[usize], secs: u64) -> usize {
    let mut found = None;
    await_until(secs, "a single serving leader", || {
        let leaders: Vec<usize> = among
            .iter()
            .copied()
            .filter(|&i| {
                f.nodes[i]
                    .as_ref()
                    .is_some_and(|n| n.is_leader() && n.can_serve())
            })
            .collect();
        found = (leaders.len() == 1).then(|| leaders[0]);
        found.is_some()
    });
    found.unwrap()
}

fn await_leader(f: &Fleet, secs: u64) -> usize {
    await_leader_among(f, &f.running(), secs)
}

fn committed_level(n: &Node) -> u8 {
    n.cluster_view().wire_level.load(Ordering::Acquire)
}

fn levels(f: &Fleet, idx: &[usize]) -> Vec<u8> {
    idx.iter().map(|&i| committed_level(f.node(i))).collect()
}

/// Every node in `idx` reaches committed level `want` within `secs`.
fn wait_level(f: &Fleet, idx: &[usize], want: u8, secs: u64) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while idx.iter().any(|&i| committed_level(f.node(i)) != want) {
        assert!(
            Instant::now() < deadline,
            "level {want} not reached on {idx:?}: {:?}",
            levels(f, idx)
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Hold for `secs`, asserting every node in `idx` stays at `want` on EVERY
/// poll — a holdout's effect is an absence, so it is checked continuously.
fn hold_level(f: &Fleet, idx: &[usize], want: u8, secs: u64, why: &str) {
    let end = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < end {
        assert_eq!(levels(f, idx), vec![want; idx.len()], "{why}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A restarted member has rejoined: a leader is known among the running
/// members and member `i`'s commit has reached the leader's (non-zero).
fn await_rejoined(f: &Fleet, i: usize, secs: u64) {
    let l = await_leader(f, secs);
    await_until(
        secs,
        "restarted member caught up to the leader's commit",
        || {
            let lc = f.node(l).counters().commit.load_acquire();
            lc > 0 && f.node(i).counters().commit.load_acquire() >= lc
        },
    );
}

/// The `uc2ctl` mutating-command flow minus the bin (`reconfig.rs`'s
/// `admin_request`): bump the request seq, write the line, poll the reply.
fn admin_request(cnc: &CncPage, op: u32, id: u32, ip: u32, port: u16) -> AdminResp {
    let seq = cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0) + 1;
    cnc.write_admin_req(&AdminReq {
        seq,
        nonce: rand::random::<u64>(),
        op,
        id,
        ip,
        port,
    });
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(resp) = cnc.read_admin_resp(seq) {
            return resp;
        }
        assert!(
            Instant::now() < deadline,
            "admin op {op} timed out (seq {seq})"
        );
        std::thread::yield_now();
    }
}

/// Retry through `status = 2` (retry: single-in-flight, no hint yet) and the
/// transient reconfiguration refusals 2/3/10, as `reconfig.rs`'s
/// `admin_request_ok` does; any other answer is returned to the caller.
fn admin_settled(cnc: &CncPage, op: u32, id: u32, ip: u32, port: u16) -> AdminResp {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let resp = admin_request(cnc, op, id, ip, port);
        let transient = resp.status == 2
            || (op == ADMIN_OP_REMOVE_LEARNER
                && resp.status == 1
                && matches!(resp.reason, 2 | 3 | 10));
        if !transient || Instant::now() >= deadline {
            return resp;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// `uc2ctl wire raise` on member `i`: one request, no retry.
fn admin_wire_raise(f: &Fleet, i: usize) -> (u32, u32) {
    let r = admin_request(&f.cnc(i), ADMIN_OP_WIRE_RAISE, 0, 0, 0);
    (r.status, r.reason)
}

/// `uc2ctl settings apply` in process (`backup_tier.rs`'s `apply_settings`):
/// the leader's committed settings with `edit` applied, staged as
/// `settings.pending` and signed by its digest in `id ‖ ip ‖ port`; waits
/// until every running node has committed them.
fn apply_settings(f: &Fleet, leader: usize, edit: impl FnOnce(&mut Settings)) {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    let mut s = f.node(leader).cluster_view().to_state().settings;
    edit(&mut s);
    let want = s;
    s.wire_level = 0; // leader-owned: `uc2ctl` never writes it
    let mut bytes = Vec::new();
    uc_protocol::v2::settings::encode_settings(&s, &mut bytes);
    let (id, ip, port) = uc_node::staged_digest(&bytes);
    let dir: &Path = &f.dirs[leader];
    let tmp = dir.join("settings.pending.tmp");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)
        .expect("stage");
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
    std::fs::rename(&tmp, dir.join(uc_node::SETTINGS_PENDING_FILE)).unwrap();
    let resp = admin_settled(&f.cnc(leader), ADMIN_OP_SETTINGS_APPLY, id, ip, port);
    assert_eq!((resp.status, resp.reason), (0, 0), "settings apply refused");
    await_until(30, "the new settings committed on every node", || {
        f.running()
            .into_iter()
            .all(|i| f.node(i).cluster_view().to_state().settings == want)
    });
}

fn agent_finished(n: &Node, agent: &str) -> bool {
    n.observability()
        .agents
        .iter()
        .find(|(name, _)| *name == agent)
        .map(|(_, flag)| flag.load(Ordering::Acquire))
        .unwrap_or_else(|| panic!("a node always has a {agent} agent"))
}

// ---------------------------------------------------------------- scenarios

/// Spec §4.2: the auto raise waits for every member's report at or above the
/// target; a `[12,12]` member holds the level, and once it restarts on a
/// `[12,13]` binary the leader raises without any operator step.
#[test]
fn a_holdout_blocks_the_raise_and_its_restart_lands_it() {
    let _g = serialize();
    let mut f = fleet(3, 0);
    f.start(0, NEXT);
    f.start(1, NEXT);
    f.start(2, OLD);
    await_leader(&f, 30);
    hold_level(&f, &[0, 1, 2], 12, 2, "a [12,12] member holds the level");

    f.restart(2, NEXT);
    wait_level(&f, &[0, 1, 2], 13, 30);
    assert!(
        f.running().iter().all(
            |&i| f.node(i).cluster_view().wire_raise.load(Ordering::Acquire) == WIRE_RAISE_AUTO
        ),
        "the raise kept the auto mode"
    );
}

/// Spec §4.3 + Ruling 9: in manual mode nothing raises on its own even with
/// every member able to; `wire raise` is leader-only (a follower answers
/// retry), lands the level, and at level it succeeds with nothing appended.
///
/// The cluster starts with a `[12,12]` holdout so the auto raise cannot land
/// before op 7 flips the mode; the holdout then restarts on `[12,13]`, so
/// the hold that follows is the manual mode's alone.
#[test]
fn manual_mode_holds_until_wire_raise_and_the_verb_is_a_noop_at_level() {
    let _g = serialize();
    let mut f = fleet(3, 0);
    f.start(0, NEXT);
    f.start(1, NEXT);
    f.start(2, OLD);
    let l = await_leader(&f, 30);
    apply_settings(&f, l, |s| s.wire_raise = WIRE_RAISE_MANUAL);
    assert_eq!(levels(&f, &[0, 1, 2]), vec![12; 3]);

    f.restart(2, NEXT);
    await_rejoined(&f, 2, 30);
    hold_level(&f, &[0, 1, 2], 12, 2, "manual mode holds the level");

    let l = await_leader(&f, 30);
    let follower = (l + 1) % 3;
    assert_eq!(
        admin_wire_raise(&f, follower).0,
        2,
        "a follower answers retry"
    );
    let r = admin_settled(&f.cnc(l), ADMIN_OP_WIRE_RAISE, 0, 0, 0);
    assert_eq!((r.status, r.reason), (0, 0), "the leader raises");
    wait_level(&f, &[0, 1, 2], 13, 30);
    assert!(
        f.running().iter().all(
            |&i| f.node(i).cluster_view().wire_raise.load(Ordering::Acquire) == WIRE_RAISE_MANUAL
        ),
        "the raise kept the manual mode"
    );

    // Ruling 9: at level the verb succeeds and appends nothing. Wait for the
    // raise record to be committed everywhere first, so the position read
    // below is the settled one.
    let l = await_leader(&f, 30);
    await_until(10, "the raise committed on every node", || {
        let a = f.node(l).counters().append.load_acquire();
        f.running()
            .iter()
            .all(|&i| f.node(i).counters().commit.load_acquire() == a)
    });
    let before = f.node(l).counters().append.load_acquire();
    assert_eq!(
        admin_wire_raise(&f, l),
        (0, 0),
        "at level: success, nothing to raise"
    );
    let end = Instant::now() + Duration::from_millis(500);
    while Instant::now() < end {
        assert_eq!(
            f.node(l).counters().append.load_acquire(),
            before,
            "the at-level raise appended nothing"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(levels(&f, &[0, 1, 2]), vec![13; 3]);
}

/// Review Focus 3: learners are members too — a `[12,12]` learner holds the
/// level, and removing it (admin op 4) lets the auto raise land on the voters.
#[test]
fn a_removed_holdout_unblocks_the_raise() {
    let _g = serialize();
    let mut f = fleet(2, 1);
    f.start(0, NEXT);
    f.start(1, NEXT);
    f.start(2, OLD);
    let l = await_leader_among(&f, &[0, 1], 30);
    hold_level(&f, &[0, 1, 2], 12, 2, "a [12,12] learner holds the level");

    let r = admin_settled(&f.cnc(l), ADMIN_OP_REMOVE_LEARNER, 2, 0, 0);
    assert_eq!((r.status, r.reason), (0, 0), "remove the learner");
    wait_level(&f, &[0, 1], 13, 30);
}

/// Spec §4.2: the leader's per-member table is term-scoped, so a leader
/// change mid-roll starts it empty — the new leader refills it from fresh
/// reports and still raises once the roll completes.
#[test]
fn a_leader_change_mid_roll_refills_the_table_and_raises() {
    let _g = serialize();
    let mut f = fleet(3, 0);
    for i in 0..3 {
        f.start(i, OLD);
    }
    let first = await_leader(&f, 30);
    hold_level(&f, &[0, 1, 2], 12, 1, "nothing to raise at [12,12]");

    // Roll, leader first: its stop forces an election among the [12,12]s.
    f.restart(first, NEXT);
    await_rejoined(&f, first, 30);
    let second = await_leader(&f, 30);
    assert_ne!(second, first, "the roll changed the leader");
    let mut order = vec![second];
    order.extend((0..3).filter(|&i| i != first && i != second));
    for &i in &order {
        assert_eq!(levels(&f, &[0, 1, 2]), vec![12; 3], "mid-roll: no raise");
        f.restart(i, NEXT);
        await_rejoined(&f, i, 30);
    }
    wait_level(&f, &[0, 1, 2], 13, 30);
}

/// Spec §2.3 + Ruling 8: a `[13,14]` binary joining a level-12 cluster
/// fail-stops its cluster agent by name once JOINED; the rest serve on.
/// The cluster runs in manual mode from genesis, so the newcomer's report
/// cannot pull the level up to it before it judges.
#[test]
fn a_two_above_binary_refuses_to_start() {
    let g = serialize();

    let mut f = fleet_with(
        3,
        0,
        Settings {
            wire_raise: WIRE_RAISE_MANUAL,
            ..Settings::genesis_default()
        },
    );
    f.start(0, NEXT);
    f.start(1, NEXT);
    let l = await_leader(&f, 30);
    let ahead = WireRange {
        min: WIRE_LEVEL_FIRST + 1,
        max: WIRE_LEVEL_FIRST + 2,
    };
    f.start(2, ahead);

    await_until(15, "n2's cluster agent fail-stopped", || {
        agent_finished(f.node(2), "cluster")
    });
    let mut line = None;
    await_until(5, "wire_level_fault in the obs log", || {
        let text = String::from_utf8_lossy(&g.buf.lock().unwrap()).into_owned();
        line = text
            .lines()
            .find(|l| l.contains(r#""event":"wire_level_fault""#))
            .map(str::to_owned);
        line.is_some()
    });
    let line = line.unwrap();
    assert!(
        line.contains(r#""wire_level":12"#) && line.contains(r#""binary_min":13"#),
        "the fault names the committed level and the binary: {line}"
    );
    assert!(!agent_finished(f.node(0), "cluster"));
    assert!(!agent_finished(f.node(1), "cluster"));
    assert!(
        !agent_finished(f.node(2), "consensus"),
        "only the cluster agent stops"
    );

    // n0/n1 keep serving at level 12.
    assert!(
        f.node(l).is_leader() && f.node(l).can_serve(),
        "the leader serves on"
    );
    assert_eq!(levels(&f, &[0, 1]), vec![12; 2]);
}

/// Spec §2.4 + ruling 16: a stop-all straight to a binary TWO levels ahead
/// (`[13,14]` over a level-12 cluster) stops every node — the middle binary
/// must be installed first. Every member reports 14, but the leader's own
/// range excludes the committed 12, so it must not raise (not even by one);
/// each node's cluster agent then judges Below once JOINED.
#[test]
fn a_stop_all_two_levels_behind_stops_every_node() {
    let g = serialize();

    let mut f = fleet(3, 0);
    for i in 0..3 {
        f.start(i, OLD);
    }
    await_leader(&f, 30);
    assert_eq!(levels(&f, &[0, 1, 2]), vec![WIRE_LEVEL_FIRST; 3]);

    // Stop every node, then start every node on the two-ahead binary.
    for i in 0..3 {
        f.stop(i);
    }
    let ahead = WireRange {
        min: WIRE_LEVEL_FIRST + 1,
        max: WIRE_LEVEL_FIRST + 2,
    };
    for i in 0..3 {
        f.start(i, ahead);
    }

    await_until(20, "every node's cluster agent fail-stopped", || {
        (0..3).all(|i| agent_finished(f.node(i), "cluster"))
    });
    let mut faults = Vec::new();
    await_until(5, "three WireLevelBelowBinary records", || {
        let text = String::from_utf8_lossy(&g.buf.lock().unwrap()).into_owned();
        faults = text
            .lines()
            .filter(|l| l.contains(r#""event":"wire_level_fault""#))
            .map(str::to_owned)
            .collect();
        faults.len() >= 3
    });
    assert_eq!(faults.len(), 3, "one per node: {faults:#?}");
    for line in &faults {
        assert!(
            line.contains(r#""fault":"WireLevelBelowBinary""#)
                && line.contains(&format!(r#""wire_level":{WIRE_LEVEL_FIRST}"#)),
            "{line}"
        );
    }
    // No raise was ever appended: the committed level is still the first.
    assert_eq!(levels(&f, &[0, 1, 2]), vec![WIRE_LEVEL_FIRST; 3]);
}
