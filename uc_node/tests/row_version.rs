// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! #33: two builds of one row must never both apply the log. See
//! docs/superpowers/specs/2026-09-27-uc2-row-running-version-design.md.
//!
//! The harness is a real three-node cluster over loopback UDP, in-process,
//! with one declared row `kv`. Two builds of that row's state machine exist:
//! `KvV1` (1.0.0) knows only `Put`; `KvV2` (2.0.0) also knows `Append`. A v1
//! build answers an `Append` with `BAD_REQUEST` and leaves its state alone —
//! deterministically, which is exactly why nothing notices the divergence.
//!
//! Since `2.13.0` a service attaches only once its node has JOINED (leader
//! known), so the harness starts nodes, waits for a leader, then starts
//! services.

use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use uc_client::Client;
use uc_net::fault::FaultConfig;
use uc_node::{
    CryptoConfig, DEFAULT_JOURNAL_SEGMENT_BYTES, Node, NodeConfig, PurgePolicy, ServicesConfig,
};
use uc_protocol::identity::pack_version;

/// Process-global lock: three nodes × five agents plus three services is a
/// lot of busy-polling threads; one cluster test at a time.
static TEST_LOCK: Mutex<()> = Mutex::new(());

fn serialize() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// Commands: tag byte 1 = Put(u64), tag byte 2 = Append(u64) (v2 only).
/// Response: 0 = ok, 0xBA = BAD_REQUEST. Query: the stored u64.
#[derive(Default)]
struct Kv {
    value: u64,
    last: Option<u64>,
}

macro_rules! kv_build {
    ($ty:ident, $ver:expr, $append:expr) => {
        #[derive(Default)]
        struct $ty(Kv);
        impl uc_service::RawStateMachine for $ty {
            const NAME: &'static str = "kv";
            const VERSION: u32 = $ver;
            fn apply(&mut self, ctx: &mut uc_service::ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
                out.clear();
                self.0.last = Some(ctx.position);
                let v = u64::from_le_bytes(cmd[1..9].try_into().unwrap());
                match cmd[0] {
                    1 => {
                        self.0.value = v;
                        out.push(0)
                    }
                    2 if $append => {
                        self.0.value += v;
                        out.push(0)
                    }
                    _ => out.push(0xBA),
                }
            }
            fn query(&self, _q: &[u8], out: &mut Vec<u8>) {
                out.clear();
                out.extend_from_slice(&self.0.value.to_le_bytes());
            }
            fn last_applied(&self) -> Option<u64> {
                self.0.last
            }
        }
        impl uc_service::SnapshotStateMachine for $ty {
            type SnapshotHandle = Vec<u8>;
            fn freeze(&self) -> Result<(Vec<u8>, u64), uc_service::SnapshotError> {
                let mut b = self.0.value.to_le_bytes().to_vec();
                b.extend_from_slice(&self.0.last.unwrap_or(0).to_le_bytes());
                Ok((b, self.0.last.unwrap_or(0)))
            }
            fn stream_snapshot(
                h: Vec<u8>,
                dst: &mut dyn std::io::Write,
            ) -> Result<(), uc_service::SnapshotError> {
                dst.write_all(&h).map_err(Into::into)
            }
            fn install_snapshot(
                &mut self,
                position: u64,
                src: &mut dyn std::io::Read,
            ) -> Result<u64, uc_service::SnapshotError> {
                let mut b = [0u8; 16];
                src.read_exact(&mut b)?;
                self.0.value = u64::from_le_bytes(b[..8].try_into().unwrap());
                self.0.last = Some(u64::from_le_bytes(b[8..].try_into().unwrap()));
                Ok(position)
            }
        }
    };
}
kv_build!(KvV1, pack_version(1, 0, 0), false);
kv_build!(KvV2, pack_version(2, 0, 0), true);
// Same line as KvV2 (2.0), different patch: admitted wherever 2.0.x runs (D3).
kv_build!(KvV2Patch, pack_version(2, 0, 7), true);

/// A command is exactly 9 bytes. It travels as a `[u8; 9]` through the typed
/// client because bincode encodes a fixed array as its raw bytes, with no
/// length prefix — so the raw-tier FSM sees the 9 bytes unchanged.
type Cmd = [u8; 9];

fn put(v: u64) -> Cmd {
    let mut c = [0u8; 9];
    c[0] = 1;
    c[1..].copy_from_slice(&v.to_le_bytes());
    c
}

fn append(v: u64) -> Cmd {
    let mut c = [0u8; 9];
    c[0] = 2;
    c[1..].copy_from_slice(&v.to_le_bytes());
    c
}

// ---------------------------------------------------------------- harness

const N: usize = 3;

fn make_config(
    id: u32,
    members: &[(u32, SocketAddr)],
    instance_dir: PathBuf,
    app: &str,
) -> NodeConfig {
    NodeConfig {
        id,
        members: members.to_vec(),
        learners: Vec::new(),
        bind: members[id as usize].1,
        instance_dir,
        app_id: app.into(),
        buffer_bytes: 1 << 22,
        max_payload: 256,
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
        force_jumbo_frames: false,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: 0x5150_1234_ABCD_0F0F ^ (u64::from(id) + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15),
        faults: FaultConfig::default(),
        purge: PurgePolicy::Disabled,
        journal_segment_bytes: DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: CryptoConfig::Disabled,
        services: ServicesConfig::from_names(&["kv"], None).unwrap(),
    }
}

/// Three in-process nodes on loopback, one declared row `kv`.
struct Cluster {
    app: String,
    dirs: Vec<PathBuf>,
    nodes: Mutex<Vec<Option<Node>>>,
    _root: tempfile::TempDir,
    _guard: MutexGuard<'static, ()>,
}

fn three_node_cluster(app: &str) -> Cluster {
    let guard = serialize();
    let root = tempfile::Builder::new()
        .prefix(&format!("uc2-{app}-"))
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let socks: Vec<UdpSocket> = (0..N)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind"))
        .collect();
    let members: Vec<(u32, SocketAddr)> = socks
        .iter()
        .enumerate()
        .map(|(i, s)| (i as u32, s.local_addr().unwrap()))
        .collect();
    let dirs: Vec<PathBuf> = (0..N).map(|i| root.path().join(format!("n{i}"))).collect();
    let nodes = socks
        .into_iter()
        .enumerate()
        .map(|(i, s)| {
            let cfg = make_config(i as u32, &members, dirs[i].clone(), app);
            Some(Node::start_with_socket(cfg, s).expect("node start"))
        })
        .collect();
    Cluster {
        app: app.into(),
        dirs,
        nodes: Mutex::new(nodes),
        _root: root,
        _guard: guard,
    }
}

impl Cluster {
    /// The index of the serving leader; polls until exactly one live node
    /// leads and can serve.
    fn wait_leader(&self) -> usize {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            {
                let nodes = self.nodes.lock().unwrap();
                let leaders: Vec<usize> = nodes
                    .iter()
                    .enumerate()
                    .filter_map(|(i, n)| {
                        n.as_ref()
                            .filter(|n| n.is_leader() && n.can_serve())
                            .map(|_| i)
                    })
                    .collect();
                if let [one] = leaders[..] {
                    return one;
                }
            }
            assert!(Instant::now() < deadline, "no serving leader within 20 s");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn others(&self, i: usize) -> impl Iterator<Item = usize> {
        (0..N).filter(move |&j| j != i)
    }

    fn start<S>(&self, i: usize) -> uc_service::Service<S>
    where
        S: uc_service::SnapshotStateMachine + Default,
    {
        self.try_start::<S>(i)
            .result
            .unwrap_or_else(|e| panic!("service start on node {i}: {e:?}"))
    }

    /// Start `S` on every node, so whichever node leads (now or after a
    /// leadership move) has an attached service for the row.
    fn start_all<S>(&self) -> Vec<uc_service::Service<S>>
    where
        S: uc_service::SnapshotStateMachine + Default,
    {
        (0..N).map(|i| self.start::<S>(i)).collect()
    }

    fn try_start<S>(&self, i: usize) -> Attempt<S>
    where
        S: uc_service::SnapshotStateMachine + Default,
    {
        let cfg = uc_service::ServiceConfig::new(&self.dirs[i], &self.app);
        Attempt {
            node: i,
            result: uc_service::ServiceBuilder::new(cfg, S::default()).start_with_snapshots(),
        }
    }

    fn client(&self, i: usize) -> KvClient {
        KvClient(Client::connect(&self.dirs[i], &self.app).expect("client connect"))
    }

    /// Node `i`'s cnc page, opened read-side the way an attacher opens it.
    fn page(&self, i: usize) -> std::sync::Arc<uc_log::cnc::CncPage> {
        uc_log::cnc::CncPage::open_file(&self.dirs[i].join("cnc2.dat"), &self.app)
            .unwrap_or_else(|e| panic!("open node {i}'s cnc page: {e:?}"))
    }

    /// Poll `pred` until it holds; panic after 20 s.
    fn wait(&self, pred: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while !pred() {
            assert!(Instant::now() < deadline, "condition not met within 20 s");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Node `i`'s `audit.jsonl`, one entry per line (empty if absent).
    fn audit_lines(&self, i: usize) -> Vec<String> {
        std::fs::read_to_string(self.dirs[i].join("audit.jsonl"))
            .map(|s| s.lines().map(str::to_owned).collect())
            .unwrap_or_default()
    }

    /// Wait until node `i`'s page shows a committed running version for
    /// `row` (the cluster agent has applied its genesis or pin record).
    fn wait_versioned(&self, i: usize, row: u8) {
        let page = self.page(i);
        self.wait(|| {
            matches!(
                page.service_slot(usize::from(row)).status.row_view(),
                uc_log::cnc::RowRead::View {
                    running: Some(_),
                    ..
                }
            )
        });
    }

    fn stop_node(&self, i: usize) {
        let n = self.nodes.lock().unwrap()[i]
            .take()
            .expect("node already stopped");
        n.stop();
    }
}

/// A service start whose refusal is observable.
struct Attempt<S: uc_service::RawStateMachine> {
    node: usize,
    result: Result<uc_service::Service<S>, uc_service::ServiceError>,
}

impl<S: uc_service::RawStateMachine> Attempt<S> {
    /// The refusal's `Display` text; panics if the start was admitted.
    fn unwrap_err_string(self) -> String {
        match self.result {
            Err(e) => e.to_string(),
            Ok(s) => panic!(
                "expected node {}'s start to be refused, but it attached to row {}",
                self.node,
                s.service_id()
            ),
        }
    }

    /// True when the start was refused, or the service stops applying
    /// (`is_alive()` turns false) within `d`.
    fn is_refused_or_stopped_within(&self, d: Duration) -> bool {
        let svc = match &self.result {
            Err(_) => return true,
            Ok(s) => s,
        };
        let deadline = Instant::now() + d;
        while Instant::now() < deadline {
            if !svc.is_alive() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        !svc.is_alive()
    }
}

impl<S: uc_service::RawStateMachine> std::fmt::Debug for Attempt<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.result {
            Err(e) => write!(f, "Attempt {{ node: {}, refused: {e:?} }}", self.node),
            Ok(s) => write!(
                f,
                "Attempt {{ node: {}, attached: row {}, alive: {} }}",
                self.node,
                s.service_id(),
                s.is_alive()
            ),
        }
    }
}

/// The byte-level client the scenario speaks: 9-byte commands, a 1-byte
/// response, an 8-byte LE query answer.
struct KvClient(Client);

impl KvClient {
    fn submit(&self, cmd: &Cmd) -> Result<Vec<u8>, uc_client::ClientError> {
        self.0.submit::<Cmd, u8>(cmd).map(|b| vec![b])
    }

    fn query_u64(&self) -> u64 {
        let b: [u8; 8] = self.0.query_linearizable(&()).expect("linearizable query");
        u64::from_le_bytes(b)
    }
}

// ------------------------------------------------------------------- test

/// #33 spec §6.1: the leader records its own attached service's version as
/// the row's running version (`RowGenesis`), every node applies it at
/// commit, and the proposing leader audits the append as `source =
/// "genesis"`.
///
/// KvV2 runs on EVERY node: a leader with no attached service never appends
/// genesis (by design), so attaching only on the node that led at
/// `wait_leader` would time out whenever leadership moved afterwards.
#[test]
fn the_leader_records_its_attached_version_as_genesis() {
    let c = three_node_cluster("rowgen");
    c.wait_leader();
    let _svcs = c.start_all::<KvV2>();
    // Every node agrees (applied at commit everywhere).
    for i in 0..N {
        let page = c.page(i);
        c.wait(|| {
            matches!(
                page.service_slot(0).status.row_view(),
                uc_log::cnc::RowRead::View { running: Some(v), record_pos, .. }
                    if v == pack_version(2, 0, 0) && record_pos > 0
            )
        });
    }
    // The audit line is on whichever node proposed it. `>= 1`, not `== 1`:
    // a leader audits right AFTER its append, before commit, so a leader
    // deposed with its genesis frame uncommitted leaves a line for a frame
    // that was later truncated, and its successor appends and audits again.
    // Only one can ever be APPLIED (a second is refused 60), which the
    // `running` check above already covers.
    let genesis: Vec<String> = (0..N)
        .flat_map(|i| c.audit_lines(i))
        .filter(|l| {
            l.contains("\"source\":\"genesis\"")
                && l.contains("\"op_name\":\"row_genesis\"")
                && l.contains("\"detail\":\"row=0\"")
        })
        .collect();
    assert!(
        !genesis.is_empty(),
        "no genesis audit line on any node's audit.jsonl"
    );
}

/// The #33 scenario (spec §10.1, controller ruling R4). Before the fix: the
/// leader's v2 acknowledges `Append(5)`; the v1 followers apply it as
/// BAD_REQUEST; once the leader stops, a v1 follower leads, answers through
/// its live v1 service, and the value reads 10, not 15 — an acknowledged
/// write lost. After the fix: every v1 service is refused at attach or
/// stopped at the genesis record, so the survivors start v2, and the value
/// reads 15.
///
/// The value check comes FIRST, so a tree without the fix fails on the loss
/// itself; the "every v1 was refused or stopped, at exactly the record"
/// checks follow it as the second line of defence.
///
/// Leadership robustness (Task 9): the scenario needs the v2 node to lead
/// from `wait_leader` until both acks. If leadership moves before then —
/// genesis records a v1 build, or the acks never come from the v2 node — the
/// PRECONDITION failed, not the property, and the scenario reruns on a fresh
/// cluster (at most three times). An ack that did come is never retried
/// away: a wrong `Append` answer fails at once.
#[test]
fn a_mixed_version_row_never_loses_an_acknowledged_write() {
    for attempt in 1..=3 {
        match mixed_version_scenario(&format!("rowver33-{attempt}")) {
            Ok(()) => return,
            Err(why) => eprintln!("attempt {attempt}: leadership moved ({why}); rerunning"),
        }
    }
    panic!("leadership moved before the acks on three fresh clusters");
}

/// One run of the #33 scenario. `Err` = leadership moved before the acks
/// (the precondition, not the property); every property failure panics.
fn mixed_version_scenario(app: &str) -> Result<(), String> {
    let c = three_node_cluster(app);
    let leader = c.wait_leader();
    // Followers FIRST, so they are attached before the genesis record
    // commits (Review Focus 3).
    let mut followers_v1: Vec<_> = c.others(leader).map(|i| c.try_start::<KvV1>(i)).collect();
    let leader_v2 = match c.try_start::<KvV2>(leader).result {
        Ok(s) => s,
        // Only a v1 node leading first records genesis at 1.0.
        Err(e) => return Err(format!("v2 refused on node {leader}: {e}")),
    };
    c.wait_versioned(leader, 0);
    match c.page(leader).service_slot(0).status.row_view() {
        uc_log::cnc::RowRead::View {
            running: Some(v), ..
        } if v == pack_version(2, 0, 0) => {}
        other => return Err(format!("genesis was not the v2 node's: {other:?}")),
    }
    let client = c.client(leader);
    for (what, cmd) in [("put", put(10)), ("append", append(5))] {
        match client.submit(&cmd) {
            Ok(r) => assert_eq!(r, vec![0], "{what} acked with an error"),
            Err(e) if c.wait_leader() != leader => {
                return Err(format!("{what} not acked ({e:?}); leader moved"));
            }
            Err(e) => panic!("{what} not acked, leader unchanged: {e:?}"),
        }
    }
    drop(client);

    // Stop the leader first; a survivor leads.
    c.stop_node(leader);
    drop(leader_v2);
    let new_leader = c.wait_leader();

    // Each v1 attempt's verdict (refused or stopped?), with its Debug line —
    // gathered BEFORE reading `is_alive()` below: a v1 follower meets the
    // genesis record only once its node learns that commit, which may be
    // only after the new leader's first commit, so an instantaneous
    // `is_alive()` right after the election could still read true on a tree
    // with the fix. On a tree without it this costs the 10 s wait.
    let verdicts: Vec<(bool, String)> = followers_v1
        .iter()
        .map(|a| {
            (
                a.is_refused_or_stopped_within(Duration::from_secs(10)),
                format!("{a:?}"),
            )
        })
        .collect();
    // `new_leader_v1_alive` false selects the fixed-world branch below. It
    // cannot be SPURIOUSLY false there — a v1 that died for some reason
    // other than the version gate would not stop at exactly the genesis
    // record, and the branch asserts that exact stop for every attached v1,
    // the new leader's included. A refused v1 (`Err`) reads false too, which
    // is the fix working. `new_leader` is always one of the two followers
    // (the old leader is stopped), so the `find` always hits.
    let new_leader_v1_alive = followers_v1
        .iter()
        .find(|a| a.node == new_leader)
        .and_then(|a| a.result.as_ref().ok())
        .is_some_and(|s| s.is_alive());
    let _v2s: Vec<uc_service::Service<KvV2>>;
    let got = if new_leader_v1_alive {
        // No fix: the v1 build still serves the row on the new leader.
        c.client(new_leader).query_u64()
    } else {
        // Every v1 refused or stopped FIRST, so a still-alive v1 fails with
        // this message rather than with the exact-stop one below.
        for (refused_or_stopped, dbg) in &verdicts {
            assert!(
                *refused_or_stopped,
                "a v1 service is still applying a row that runs 2.0: {dbg}"
            );
        }
        // Spec §4.3: a stopped v1 stopped at EXACTLY the record — its slot's
        // `applied` is the genesis frame's START (every earlier frame, none
        // after). Read before any v2 attaches and rewrites the slot.
        let genesis_len = uc_protocol::v2::frame::align_frame_len(
            uc_protocol::v2::frame::HEADER_LEN
                + uc_protocol::v2::frame::CLUSTER_BODY_PREFIX_LEN
                + uc_protocol::v2::upgrade::ROW_GENESIS_LEN,
        ) as u64;
        for a in followers_v1.iter().filter(|a| a.result.is_ok()) {
            let page = c.page(a.node);
            let uc_log::cnc::RowRead::View { record_pos, .. } =
                page.service_slot(0).status.row_view()
            else {
                panic!("node {}: row view contended", a.node);
            };
            assert_eq!(
                page.service_slot(0).applied.load_acquire(),
                record_pos - genesis_len,
                "node {}'s v1 must stop at exactly the genesis record",
                a.node
            );
        }
        followers_v1.clear(); // release every v1's row lock before v2 attaches
        _v2s = c.others(leader).map(|i| c.start::<KvV2>(i)).collect();
        c.client(new_leader).query_u64()
    };
    assert_eq!(got, 15, "the acknowledged Append(5) was lost");

    // Second line of defence on the live-v1 branch (the fixed branch checked
    // it before its exact-stop assertion): every v1 was refused or stopped.
    for (refused_or_stopped, dbg) in &verdicts {
        assert!(
            *refused_or_stopped,
            "a v1 service is still applying a row that runs 2.0: {dbg}"
        );
    }
    Ok(())
}

/// #33 Review Focus 2: attach records `attach_record_pos` = the record's
/// end, so a matching binary restarted after the record never re-adjudicates
/// it. (KvV2 on every node — leader-move robustness, as elsewhere here.)
#[test]
fn a_matching_restart_after_genesis_does_not_stop_again() {
    let c = three_node_cluster("rowrestart");
    let leader = c.wait_leader();
    let mut svcs: Vec<Option<uc_service::Service<KvV2>>> =
        c.start_all::<KvV2>().into_iter().map(Some).collect();
    c.wait_versioned(leader, 0);
    drop(svcs[leader].take());
    let s2 = c.start::<KvV2>(leader);
    let now = c.wait_leader();
    let client = c.client(now);
    assert_eq!(client.submit(&put(3)).unwrap(), vec![0]);
    assert!(
        s2.is_alive(),
        "a matching restart must not stop at the genesis record"
    );
}

// ------------------------------------------------------------ attach

/// #33 spec §7.1: once a row has a committed running version, a service
/// whose version is off that LINE (major.minor) is refused at attach, by
/// name — before any slot word is written. A patch build of the running line
/// is admitted (D3).
///
/// KvV2 starts on every node first (leader-move robustness: whichever node
/// leads appends genesis), then one follower's v2 is dropped to free its row
/// lock for the v1 attempt.
#[test]
fn attach_refuses_a_binary_off_the_running_line_by_name() {
    let c = three_node_cluster("rowattach");
    let leader = c.wait_leader();
    let mut v2s: Vec<Option<uc_service::Service<KvV2>>> =
        c.start_all::<KvV2>().into_iter().map(Some).collect();
    c.wait_versioned(leader, 0);
    let f = c.others(leader).next().unwrap();
    c.wait_versioned(f, 0);
    drop(v2s[f].take()); // release service.0.lock on node f
    let err = c.try_start::<KvV1>(f).unwrap_err_string();
    assert!(
        err.contains("row 0") && err.contains("2.0.0") && err.contains("1.0.0"),
        "{err}"
    );
    assert!(
        err.contains("\"kv\"") && err.contains("2.0.x") && err.contains("uc2ctl upgrade pin"),
        "the refusal names the FSM, the line to install, and the pin: {err}"
    );
    // Same line, different patch: admitted (D3).
    let _patch = c.start::<KvV2Patch>(f);
}

// ------------------------------------------------------- the client gate

impl Cluster {
    /// Wait until the cluster has nothing of its own left to append: every
    /// live node has reached the top jumbo rung (loopback proves all three,
    /// and each raise is a `CLUSTER` frame), and node `i`'s append counter
    /// equals its commit and has held still for 500 ms.
    fn settle(&self, i: usize) {
        let top = uc_protocol::v2::datagram::MTU_BOUND as u32;
        self.wait(|| {
            self.nodes
                .lock()
                .unwrap()
                .iter()
                .flatten()
                .all(|n| n.datagram_mtu() == top)
        });
        let page = self.page(i);
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut last = (u64::MAX, Instant::now());
        loop {
            let a = page.counters().append.load_acquire();
            let k = page.counters().commit.load_acquire();
            if a != last.0 || a != k {
                last = (a, Instant::now());
            } else if last.1.elapsed() >= Duration::from_millis(500) {
                return;
            }
            assert!(Instant::now() < deadline, "node {i} never settled");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

/// #33 spec §6.2 (controller ruling R1): while a declared row has no running
/// version, the leader appends no client frame — the LOG does not grow, the
/// record stays in the ingress ring. A timeout alone would prove nothing
/// (with no leader-side service nothing answers either way), so this reads
/// the leader's `append` counter across the attempt. Once the row gets a
/// version (a service attaches and genesis commits), the SAME waiting submit
/// is admitted and acknowledged.
#[test]
fn clients_wait_until_every_declared_row_has_a_version() {
    let c = three_node_cluster("rowgate");
    let leader = c.wait_leader();
    c.settle(leader);
    let page = c.page(leader);
    let before = page.counters().append.load_acquire();

    // No service anywhere: the row has no version. The submit blocks, so it
    // runs on its own thread, with an explicit 60 s request timeout: the
    // `Client` default (10 s) is shared between the 1 s held here and however
    // long `start_all` plus the genesis commit take, which is too tight.
    let client = uc_client::PipelinedClient::connect(
        &c.dirs[leader],
        &c.app,
        uc_client::PipelinedConfig {
            request_timeout: Duration::from_secs(60),
            serving_gate: false, // as `Client::connect` pins it
            ..Default::default()
        },
    )
    .expect("client connect");
    let waiting = std::thread::spawn(move || {
        client
            .submit::<Cmd, u8>(&put(1))
            .and_then(|t| t.wait())
            .map(|b| vec![b])
    });
    std::thread::sleep(Duration::from_millis(1_000));
    let after = page.counters().append.load_acquire();
    assert_eq!(
        after, before,
        "the leader appended while the row had no running version"
    );
    assert!(!waiting.is_finished(), "the submit must still be waiting");

    // Every node runs the row, so whichever leads appends genesis.
    let _svcs = c.start_all::<KvV2>();
    let got = waiting.join().expect("submit thread");
    assert_eq!(got.expect("admitted after genesis"), vec![0]);
}

/// Review Focus 5: a page that declares no rows
/// (`ServicesConfig::none_for_tests()`) has nothing to wait for — the gate
/// must not hold, or every harness that commits client frames with no
/// service would wedge. The raw ingress-ring pattern of `smoke.rs`.
#[test]
fn ingress_gate_is_open_with_nothing_declared() {
    let _guard = serialize();
    let root = tempfile::Builder::new()
        .prefix("uc2-rowgate0-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind");
    let members = [(0u32, sock.local_addr().unwrap())];
    let mut cfg = make_config(0, &members, root.path().join("n0"), "rowgate0");
    cfg.services = ServicesConfig::none_for_tests();
    let node = Node::start_with_socket(cfg, sock).expect("node start");
    let deadline = Instant::now() + Duration::from_secs(20);
    while !node.can_serve() {
        assert!(Instant::now() < deadline, "no serving leader within 20 s");
        std::thread::sleep(Duration::from_millis(10));
    }

    let ring = uc_protocol::ring::mpsc::MpscRing::open(&root.path().join("n0/ingress.ring"))
        .expect("open ingress ring");
    let (prod, _) = ring.into_split();
    // A solo cluster never raises its rung (jumbo erratum 4), so once it has
    // settled nothing else lands between `commit0` and the client's frame.
    std::thread::sleep(Duration::from_millis(500));
    let commit0 = node.counters().commit.load_acquire();
    prod.try_write(
        uc_protocol::v2::ipc::MSG_V2_SUBMIT,
        0,
        uc_protocol::v2::ipc::extra_client(7, 1),
        b"gate-open",
    )
    .expect("ring write");
    let deadline = Instant::now() + Duration::from_secs(10);
    while node.counters().commit.load_acquire() <= commit0 {
        assert!(
            Instant::now() < deadline,
            "a harness node must still admit client frames"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut buf = Vec::new();
    match node.read_frame_validated(commit0, &mut buf) {
        uc_log::buffer::FrameRead::Frame(h) => {
            assert_eq!((h.client_id, h.seq), (7, 1), "the client's frame")
        }
        other => panic!("expected the client frame at {commit0}, got {other:?}"),
    }
    node.stop();
}

// ------------------------------------------- the pin stops old services

/// `ServiceStatusLine::row_view` already retries its own seqlock read up to
/// 64 times, but a busy writer (or a paused thread mid-write, under a loaded
/// dev box or CI runner) can outlast even that. Retry the WHOLE read here
/// too, bounded, spin/yield only — never `thread::sleep`, so a permanently
/// contended line still returns (as `RowRead::Contended`) rather than
/// hanging forever (#33 follow-up: `restage_pin` used to give up on the
/// first `Contended` read).
const ROW_VIEW_RETRY_ATTEMPTS: u32 = 50_000;

fn row_view_retrying(status: &uc_log::cnc::ServiceStatusLine) -> uc_log::cnc::RowRead {
    for _ in 0..ROW_VIEW_RETRY_ATTEMPTS {
        match status.row_view() {
            uc_log::cnc::RowRead::Contended => std::thread::yield_now(),
            view => return view,
        }
    }
    uc_log::cnc::RowRead::Contended
}

/// A bare heap page for testing [`row_view_retrying`] without a cluster —
/// same shape as `uc_service::attach`'s test-only `page()` helper.
fn heap_page_for_row_view_test() -> std::sync::Arc<uc_log::cnc::CncPage> {
    uc_log::cnc::CncPage::heap(&uc_log::cnc::CncMeta {
        node_id: 1,
        instance_id: 1,
        app_id: "row-view-retry-test".into(),
        buffer_bytes: 1 << 20,
        max_payload: 256,
        services: [None; uc_protocol::v2::cnc::CNC_MAX_SERVICES],
    })
}

/// A write that stays mid-flight (seqlock ODD) longer than a single
/// `row_view()`'s own 64-spin can outlast proves the OUTER bound in
/// [`row_view_retrying`] is what recovers it, not the inner one.
#[test]
fn row_view_retrying_recovers_a_write_that_outlasts_one_inner_read() {
    let page = heap_page_for_row_view_test();
    let s = &page.service_slot(0).status;
    s.store_pin_begin_for_test(7, 8); // leaves pin_seq ODD: Contended
    let view = std::thread::scope(|scope| {
        scope.spawn(|| {
            std::thread::sleep(Duration::from_millis(2));
            s.store_pin_finish_for_test(4096);
        });
        row_view_retrying(s)
    });
    assert_eq!(
        view,
        uc_log::cnc::RowRead::View {
            pin: Some((4096, 7, 8)),
            running: None,
            record_pos: 0,
        },
        "the outer retry should have waited out the in-flight write"
    );
}

/// A permanently contended line (the writer never finishes) must still
/// RETURN — bounded spin/yield, not `sleep`-forever — rather than hang the
/// caller. This test itself hanging is the failure mode.
#[test]
fn row_view_retrying_gives_up_on_a_permanently_contended_line() {
    let page = heap_page_for_row_view_test();
    let s = &page.service_slot(0).status;
    s.store_pin_begin_for_test(7, 8); // never finished
    assert_eq!(row_view_retrying(s), uc_log::cnc::RowRead::Contended);
}

impl Cluster {
    /// Run `f` on node `i`'s in-process handle.
    fn with_node<R>(&self, i: usize, f: impl FnOnce(&Node) -> R) -> R {
        let nodes = self.nodes.lock().unwrap();
        f(nodes[i].as_ref().expect("node stopped"))
    }

    /// Submit through whichever node leads NOW, reconnecting and resending on
    /// an error (a leadership move mid-call). Returns the response and
    /// whether a resend happened — a resent command may have committed
    /// twice, which only an idempotent command can shrug off.
    fn submit_via_leader(&self, cmd: &Cmd) -> (Vec<u8>, bool) {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut resent = false;
        loop {
            let l = self.wait_leader();
            match self.client(l).submit(cmd) {
                Ok(r) => return (r, resent),
                Err(e) => {
                    assert!(Instant::now() < deadline, "submit never acked: {e:?}");
                    eprintln!("submit via node {l} failed ({e:?}); re-resolving the leader");
                    resent = true;
                }
            }
        }
    }

    /// `uc2ctl snapshot`, in process (admin op 8's exact body, as
    /// `Node::command_snapshot` runs it): command an instant on whichever
    /// node leads — re-resolved on every `Retry`, so a leadership move is
    /// absorbed — and wait until EVERY node holds the complete set at the
    /// returned **P** (each node's v2 will install that node's own copy of
    /// the origin, and the pin door requires the pinning node's newest set).
    fn snapshot_instant(&self) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(30);
        let p = loop {
            let l = self.wait_leader();
            match self.with_node(l, |n| n.command_snapshot(false)) {
                Ok(p) => break p,
                Err(uc_node::SnapshotRefusal::Retry) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(e) => panic!("uc2ctl snapshot refused: {e}"),
            }
        };
        for i in 0..N {
            self.wait(|| self.with_node(i, |n| n.snapshot_set_position()) == p);
        }
        p
    }

    /// `uc2ctl upgrade pin`, in process: stage the 20-byte `UpgradePin` at
    /// `<instance_dir>/upgrade.pending` (`uc_node::UPGRADE_PENDING_FILE`) on
    /// the node that leads NOW and submit admin op 10 through its cnc admin
    /// band (filesystem admin policy: no auth line) — the twin of
    /// `uc_service/tests/pinned_attach.rs`'s `pin_via_admin`. Returns
    /// `(node, END position)` from the accepted reply.
    ///
    /// Retried, re-resolving the leader each time: status 2 (not the leader
    /// any more, or single-in-flight) and reason 54 `pin_no_set` (the set's
    /// position is published a moment after the artifact lands). Anything
    /// else fails here, named.
    fn pin(&self, row: u8, from: u32, to: u32, origin: u64) -> (usize, u64) {
        self.try_pin(row, from, to, origin)
            .unwrap_or_else(|(l, status, reason)| {
                panic!("upgrade pin refused on node {l}: status={status} reason={reason}")
            })
    }

    /// [`Self::pin`], handing back a non-racy refusal as `(node, status,
    /// reason)` instead of failing on it.
    fn try_pin(
        &self,
        row: u8,
        from: u32,
        to: u32,
        origin: u64,
    ) -> Result<(usize, u64), (usize, u32, u32)> {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        use uc_protocol::v2::upgrade::{UpgradePin, encode_upgrade_pin};
        let mut bytes = Vec::new();
        encode_upgrade_pin(
            &UpgradePin {
                row,
                from,
                to,
                origin,
            },
            &mut bytes,
        );
        let (id, ip, port) = uc_node::staged_digest(&bytes);
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let l = self.wait_leader();
            let dir = &self.dirs[l];
            let pending = dir.join(uc_node::UPGRADE_PENDING_FILE);
            let tmp = dir.join(format!("{}.tmp", uc_node::UPGRADE_PENDING_FILE));
            {
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .mode(0o600)
                    .open(&tmp)
                    .unwrap();
                f.write_all(&bytes).unwrap();
                f.sync_all().unwrap();
            }
            std::fs::rename(&tmp, &pending).unwrap();

            let cnc = self.page(l);
            let seq = cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0) + 1;
            cnc.write_admin_req(&uc_log::cnc::AdminReq {
                seq,
                nonce: seq,
                op: uc_protocol::v2::cnc::ADMIN_OP_UPGRADE_PIN,
                id,
                ip,
                port,
            });
            let resp_deadline = Instant::now() + Duration::from_secs(15);
            let resp = loop {
                if let Some(r) = cnc.read_admin_resp(seq) {
                    break r;
                }
                assert!(
                    Instant::now() < resp_deadline,
                    "node {l}: admin response timed out for seq {seq}"
                );
                std::thread::yield_now();
            };
            if resp.status == 0 {
                return Ok((l, resp.version));
            }
            let racy = resp.status == 2 || resp.reason == uc_node::REASON_PIN_NO_SET;
            if !(racy && Instant::now() < deadline) {
                return Err((l, resp.status, resp.reason));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// The START of the pin frame whose END is `end`, derived from the pin
    /// frame's fixed length and then VERIFIED by reading node `i`'s log at
    /// that start: it must be a `CLUSTER` `UpgradePin` for `row` whose
    /// aligned length ends exactly at `end`. A wrong derivation fails here,
    /// not as an off-by-one below. (A walk from position 0 would work too,
    /// but under the recorded workload the 4 MiB buffer is not guaranteed
    /// never to wrap; the frame at `end` is recent, so it is still there.)
    fn frame_start_of(&self, end: u64, i: usize, row: u8) -> u64 {
        use uc_protocol::v2::frame::{
            CLUSTER_BODY_PREFIX_LEN, ClusterKind, FRAME_TYPE_CLUSTER, HEADER_LEN, align_frame_len,
        };
        let len = align_frame_len(
            HEADER_LEN + CLUSTER_BODY_PREFIX_LEN + uc_protocol::v2::upgrade::UPGRADE_PIN_LEN,
        ) as u64;
        let start = end - len;
        let mut buf = Vec::new();
        self.with_node(i, |n| match n.read_frame_validated(start, &mut buf) {
            uc_log::buffer::FrameRead::Frame(h) => {
                assert_eq!(
                    h.frame_type, FRAME_TYPE_CLUSTER,
                    "node {i}: frame at {start}"
                );
                assert_eq!(
                    start + align_frame_len(h.length as usize) as u64,
                    end,
                    "node {i}: the frame at {start} does not end at {end}"
                );
                assert_eq!(
                    (buf[HEADER_LEN], buf[HEADER_LEN + CLUSTER_BODY_PREFIX_LEN]),
                    (ClusterKind::UpgradePin as u8, row),
                    "node {i}: the frame ending at {end} is not row {row}'s pin"
                );
            }
            other => panic!("node {i}: read at {start} gave {other:?}"),
        });
        start
    }

    /// Op 10 answers at APPEND, not at commit. Wait (bounded) until node
    /// `on`'s committed row view shows this pin — `running = to` recorded at
    /// `end`. `false` on timeout: the appending leader lost leadership and
    /// the frame at `end` was truncated (or replaced) by its successor.
    fn pin_committed_within(&self, on: usize, end: u64, to: u32, d: Duration) -> bool {
        let page = self.page(on);
        let deadline = Instant::now() + d;
        loop {
            if let uc_log::cnc::RowRead::View {
                running: Some(v),
                record_pos,
                ..
            } = page.service_slot(0).status.row_view()
                && (v, record_pos) == (to, end)
            {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// A RE-STAGE of a pin whose earlier attempt was not seen to commit in
    /// time (final review M6). That attempt may have committed LATE, and then
    /// the re-stage is refused 55 (`pin_not_monotone`: the same origin is no
    /// longer above the row's pin). Recognise that case from the leader's
    /// row view — its pin names this `origin` and `to`, and the row's running
    /// version is `to` — and hand back `(leader, record_pos)`: the committed
    /// pin's END, exactly what an accepted reply would have returned. Any
    /// other refusal still fails, named. The row view read is
    /// [`row_view_retrying`], not a single `row_view()` call: a `Contended`
    /// read here used to panic immediately (#33 follow-up).
    fn restage_pin(&self, row: u8, from: u32, to: u32, origin: u64) -> (usize, u64) {
        match self.try_pin(row, from, to, origin) {
            Ok(hit) => hit,
            Err((_, _, reason)) if reason == uc_node::REASON_PIN_NOT_MONOTONE => {
                let l = self.wait_leader();
                match row_view_retrying(&self.page(l).service_slot(row as usize).status) {
                    uc_log::cnc::RowRead::View {
                        pin: Some((o, _, t)),
                        running: Some(v),
                        record_pos,
                    } if (o, t, v) == (origin, to, to) => {
                        eprintln!(
                            "re-staged pin refused 55: the earlier attempt committed late at \
                             {record_pos}; carrying on with it"
                        );
                        (l, record_pos)
                    }
                    other => panic!(
                        "upgrade pin refused 55 on node {l}, and no committed pin to {to:#x} at \
                         origin {origin} is on its row view: {other:?}"
                    ),
                }
            }
            Err((l, status, reason)) => {
                panic!("upgrade pin refused on node {l}: status={status} reason={reason}")
            }
        }
    }
}

/// A recorded register workload against row `kv` (spec §10.2's "under load"
/// and "linearizable history across the switch"). `Put(v)` is the model's
/// `Write(v)` and a linearizable query is its `Read`; `KvV1` and `KvV2`
/// give both exactly the same semantics (only `Append` differs, and the
/// workload never sends one), so the origin install cannot legitimately
/// change a value and the history must linearize across the switch.
///
/// Written values are never 0, so the store's initial 0 reads as the
/// model's `None` (never written).
///
/// Every op whose outcome was not observed — a timeout (between the v1 stop
/// and the v2 attach nothing answers), a NOT_LEADER, a dead connection — is
/// recorded INDETERMINATE: a write may or may not have committed. Nothing is
/// dropped or counted as failed. The worker then re-resolves the leader and
/// reconnects.
struct Workload<'a> {
    c: &'a Cluster,
    history: uc_lincheck::history::History,
    stop: std::sync::atomic::AtomicBool,
    /// Set once `pin` has returned; ops invoked after it are counted.
    pinned: std::sync::atomic::AtomicBool,
    invoked_after_pin: std::sync::atomic::AtomicU64,
    /// Set once every v2 is attached; Ok ops completed after it are counted.
    v2_up: std::sync::atomic::AtomicBool,
    ok_after_v2: std::sync::atomic::AtomicU64,
    ok_total: std::sync::atomic::AtomicU64,
}

/// Sets the flag on drop — on the success path and on unwind.
struct StopOnDrop<'a>(&'a std::sync::atomic::AtomicBool);

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}

/// Per-op request timeout for the workload's clients: long enough for a
/// loaded loopback cluster, short enough that the ops caught in the
/// stop-to-attach gap resolve (as indeterminate) quickly.
const WORKLOAD_TIMEOUT: Duration = Duration::from_secs(3);
/// Pause between one worker's ops: keeps the history (and the checker's
/// search) small and the log well inside the buffer.
const WORKLOAD_PACE: Duration = Duration::from_millis(3);

impl<'a> Workload<'a> {
    fn new(c: &'a Cluster) -> Self {
        use std::sync::atomic::{AtomicBool, AtomicU64};
        Workload {
            c,
            history: Default::default(),
            stop: AtomicBool::new(false),
            pinned: AtomicBool::new(false),
            invoked_after_pin: AtomicU64::new(0),
            v2_up: AtomicBool::new(false),
            ok_after_v2: AtomicU64::new(0),
            ok_total: AtomicU64::new(0),
        }
    }

    fn connect(&self) -> uc_client::PipelinedClient {
        let l = self.c.wait_leader();
        uc_client::PipelinedClient::connect(
            &self.c.dirs[l],
            &self.c.app,
            uc_client::PipelinedConfig {
                request_timeout: WORKLOAD_TIMEOUT,
                serving_gate: false, // as `Client::connect` pins it
                ..Default::default()
            },
        )
        .expect("client connect")
    }

    fn stamp_invoke(&self) -> u64 {
        use std::sync::atomic::Ordering::SeqCst;
        let inv = self.history.invoke();
        if self.pinned.load(SeqCst) {
            self.invoked_after_pin.fetch_add(1, SeqCst);
        }
        inv
    }

    fn ok(&self) {
        use std::sync::atomic::Ordering::SeqCst;
        self.ok_total.fetch_add(1, SeqCst);
        if self.v2_up.load(SeqCst) {
            self.ok_after_v2.fetch_add(1, SeqCst);
        }
    }

    /// Writer `w` writes `w * 1_000_000 + k` for k = 1, 2, … — unique and
    /// never 0.
    fn writer(&self, w: u32) {
        use std::sync::atomic::Ordering::SeqCst;
        use uc_lincheck::history::{Op, Outcome, RegResp};
        let mut client = self.connect();
        let mut k = 0u64;
        while !self.stop.load(SeqCst) {
            k += 1;
            let v = u64::from(w) * 1_000_000 + k;
            let inv = self.stamp_invoke();
            let r = client.submit::<Cmd, u8>(&put(v)).and_then(|t| t.wait());
            match r {
                Ok(0) => {
                    self.history
                        .record(w, Op::Write(v), inv, Outcome::Ok(RegResp::Ack));
                    self.ok();
                }
                Ok(other) => panic!("put({v}) answered {other:#x}"),
                Err(_) => {
                    self.history
                        .record(w, Op::Write(v), inv, Outcome::Indeterminate);
                    client = self.connect();
                }
            }
            std::thread::sleep(WORKLOAD_PACE);
        }
    }

    fn reader(&self, id: u32) {
        use std::sync::atomic::Ordering::SeqCst;
        use uc_lincheck::history::{Op, Outcome, RegResp};
        let mut client = self.connect();
        while !self.stop.load(SeqCst) {
            let inv = self.stamp_invoke();
            let r = client
                .query_linearizable::<(), [u8; 8]>(&())
                .and_then(|t| t.wait());
            match r {
                Ok(b) => {
                    let v = u64::from_le_bytes(b);
                    let seen = (v != 0).then_some(v);
                    self.history
                        .record(id, Op::Read, inv, Outcome::Ok(RegResp::Value(seen)));
                    self.ok();
                }
                Err(_) => {
                    self.history
                        .record(id, Op::Read, inv, Outcome::Indeterminate);
                    client = self.connect();
                }
            }
            std::thread::sleep(WORKLOAD_PACE);
        }
    }
}

/// Final review M6: a pin re-staged because its earlier attempt was not
/// SEEN to commit in time may find that attempt DID commit late — the
/// re-stage is then refused 55 (`pin_not_monotone`: same origin). The retry
/// must recognise the committed pin (the row's running version is `to`, its
/// pin names this origin) and carry on with it, not fail the test.
#[test]
fn a_restaged_pin_that_already_committed_is_recognised() {
    let c = three_node_cluster("rowrestage");
    c.wait_leader();
    let olds = c.start_all::<KvV1>();
    let from = pack_version(1, 0, 0);
    let to = pack_version(2, 0, 0);
    let origin = c.snapshot_instant();
    let (on, end) = c.pin(0, from, to, origin);
    assert!(c.pin_committed_within(on, end, to, Duration::from_secs(30)));
    // The earlier attempt committed; re-staging the same pin must hand it
    // back, not panic on the 55.
    let (_, again) = c.restage_pin(0, from, to, origin);
    assert_eq!(again, end, "the re-stage recognised the committed pin");
    drop(olds);
}

/// #33 spec §10.2 (and §4.3, §7.2): 1.0 services are attached and applying
/// UNDER LOAD when a pin to 2.0 commits. Each stops at EXACTLY the pin
/// record — its slot's `applied` is the record's frame START (every earlier
/// frame applied, nothing after) — and says so with a `version_superseded`
/// event. 2.0 services then attach on every node, install the pin's origin,
/// and the row resumes. A recorded register workload (two writers, one
/// reader) runs from before the instant until the 2.0 services are serving,
/// and its history is linearizable across the switch (WGL checker). After
/// the workload an `Append` (2.0-only) is acknowledged and adds to the value.
///
/// The stop message is asserted on the captured `uc_obs` sink. This binary's
/// `TEST_LOCK` (held by every test here, via `three_node_cluster`) is what
/// keeps the process-global capture from being stolen by a sibling — the
/// `OBS_CAPTURE_LOCK` discipline of `node.rs`'s unit tests.
///
/// Leadership: every "the leader" step re-resolves it (`submit_via_leader`,
/// `snapshot_instant`, `pin`, the workload's reconnects), and every node
/// runs the row, so a move at any point costs a retry, not the test. A pin
/// frame truncated by a leader move before it committed is re-staged and
/// retried (the op-10 reply is an append, not a commit).
#[test]
fn a_committed_pin_stops_every_old_service_at_exactly_the_record() {
    use std::sync::atomic::Ordering::SeqCst;
    let c = three_node_cluster("rowpin");
    c.wait_leader();
    let olds = c.start_all::<KvV1>();
    let from = pack_version(1, 0, 0);
    let to = pack_version(2, 0, 0);
    let w = Workload::new(&c);
    let sink = ObsCapture::take();

    let (pin_end, pin_start, committed_seq, _news) = std::thread::scope(|s| {
        // `thread::scope` joins every worker before re-raising a panic from
        // this closure, and the workers loop until `stop`: without this guard
        // any failing assertion below would HANG the test instead of failing
        // it. Dropped on success and on unwind alike.
        let _stop_workers = StopOnDrop(&w.stop);
        s.spawn(|| w.writer(1));
        s.spawn(|| w.writer(2));
        s.spawn(|| w.reader(3));
        // Some acknowledged state before the instant, so the origin carries it.
        c.wait(|| w.ok_total.load(SeqCst) >= 100);
        let origin = c.snapshot_instant();

        let mut attempt = 0;
        let (pinned_on, pin_end) = loop {
            attempt += 1;
            // A re-stage recognises an earlier attempt that committed late
            // (refused 55) instead of failing on it — final review M6.
            let (on, end) = if attempt == 1 {
                c.pin(0, from, to, origin)
            } else {
                c.restage_pin(0, from, to, origin)
            };
            w.pinned.store(true, SeqCst);
            if c.pin_committed_within(on, end, to, Duration::from_secs(10)) {
                break (on, end);
            }
            assert!(
                attempt < 3,
                "the pin appended at {end} on node {on} never committed there in 10 s, three \
                 times — each time truncated after a leader move?"
            );
            eprintln!("pin at {end} on node {on} not committed in 10 s; re-staging");
        };
        // Every op invoked after this stamp started after the pin committed.
        let committed_seq = w.history.invoke();
        let pin_start = c.frame_start_of(pin_end, pinned_on, 0);
        for (i, s) in olds.iter().enumerate() {
            c.wait(|| !s.is_alive());
            // The log is replicated byte for byte: every node agrees.
            assert_eq!(c.frame_start_of(pin_end, i, 0), pin_start, "node {i}");
            let page = c.page(i);
            assert_eq!(
                page.service_slot(0).applied.load_acquire(),
                pin_start,
                "node {i}'s v1 stopped somewhere other than the pin record"
            );
            match page.service_slot(0).status.row_view() {
                uc_log::cnc::RowRead::View {
                    running: Some(v),
                    record_pos,
                    ..
                } => assert_eq!((v, record_pos), (to, pin_end), "node {i}"),
                other => panic!("node {i}: row view {other:?}"),
            }
        }
        // Each v1 has fail-stopped; dropping the handle joins its thread
        // without re-raising the panic and releases `service.0.lock` for the
        // v2 attach.
        drop(olds);
        let news = c.start_all::<KvV2>();
        w.v2_up.store(true, SeqCst);
        // The row resumes under the recorded load.
        c.wait(|| w.ok_after_v2.load(SeqCst) >= 50);
        (pin_end, pin_start, committed_seq, news)
    });

    let text = sink.text();
    drop(sink);
    let stops: Vec<&str> = text
        .lines()
        .filter(|l| l.contains("\"event\":\"version_superseded\""))
        .collect();
    assert_eq!(stops.len(), N, "one version_superseded per v1:\n{text}");
    for l in &stops {
        assert!(
            l.contains(&format!("\"position\":{pin_start}"))
                && l.contains("\"running\":\"2.0.0\"")
                && l.contains("\"mine\":\"1.0.0\""),
            "{l}"
        );
    }

    // The load genuinely spans the pin.
    let after_pin = w.invoked_after_pin.load(SeqCst);
    assert!(after_pin >= 1, "no client op was issued after the pin");
    let entries = w.history.into_entries();
    let indeterminate = entries
        .iter()
        .filter(|e| matches!(e.outcome, uc_lincheck::history::GenOutcome::Indeterminate))
        .count();
    // ...and a client frame committed after the pin was acknowledged (by a
    // 2.0 service: every 1.0 stopped at the record).
    assert!(
        entries.iter().any(|e| e.invoke > committed_seq
            && matches!(e.op, uc_lincheck::history::Op::Write(_))
            && matches!(e.outcome, uc_lincheck::history::GenOutcome::Ok(_))),
        "no write invoked after the pin committed was acknowledged"
    );
    let (verdict, spent) = uc_lincheck::checker::check_register_reporting(
        &entries,
        uc_lincheck::checker::DEFAULT_BUDGET,
    );
    eprintln!(
        "pin at {pin_start}..{pin_end}: {} ops ({indeterminate} indeterminate, {after_pin} \
         invoked after the pin), checker spent {spent}",
        entries.len()
    );
    assert_eq!(
        verdict,
        uc_lincheck::checker::Verdict::Linearizable,
        "the history across the pin is not linearizable"
    );

    // The workload is over: `Append` (2.0-only) is acknowledged and adds.
    let before = c.client(c.wait_leader()).query_u64();
    let (r, resent) = c.submit_via_leader(&append(1));
    assert_eq!(r, vec![0], "the 2.0-only Append must be acknowledged");
    let got = c.client(c.wait_leader()).query_u64();
    if resent {
        // A resent Append may have committed twice.
        assert!(got == before + 1 || got == before + 2, "{before} -> {got}");
    } else {
        assert_eq!(got, before + 1, "Append(1) on {before}");
    }
}

/// The process-global obs sink, held for a scope and restored on drop (the
/// panic path included) — `uc_node/tests/snapshot_reports.rs`'s guard.
struct ObsCapture(std::sync::Arc<Mutex<Vec<u8>>>);

impl ObsCapture {
    fn take() -> Self {
        Self(uc_node::obs::log::capture_for_tests())
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap_or_else(|e| e.into_inner())).into_owned()
    }
}

impl Drop for ObsCapture {
    fn drop(&mut self) {
        uc_node::obs::log::stderr_for_tests();
    }
}
