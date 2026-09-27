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
/// The #33 scenario (spec §10.1, controller ruling R4). Before the fix: the
/// leader's v2 acknowledges `Append(5)`; the v1 followers apply it as
/// BAD_REQUEST; once the leader stops, a v1 follower leads, answers through
/// its live v1 service, and the value reads 10, not 15 — an acknowledged
/// write lost. After the fix: every v1 service is refused at attach or
/// stopped at the genesis record, so the survivors start v2, and the value
/// reads 15.
///
/// The value check comes FIRST, so today's tree fails on the loss itself; the
/// "every v1 was refused or stopped" check follows it as the second line of
/// defence once the fix lands.
#[test]
#[ignore = "#33: un-ignored in Task 12"]
fn a_mixed_version_row_never_loses_an_acknowledged_write() {
    let c = three_node_cluster("rowver33");
    let leader = c.wait_leader();
    // Followers FIRST, so they are attached before the genesis record
    // commits (Review Focus 3).
    let mut followers_v1: Vec<_> = c.others(leader).map(|i| c.try_start::<KvV1>(i)).collect();
    let leader_v2 = c.start::<KvV2>(leader);
    let client = c.client(leader);
    assert_eq!(client.submit(&put(10)).expect("put acked"), vec![0]);
    assert_eq!(client.submit(&append(5)).expect("append acked"), vec![0]);
    drop(client);

    // Stop the leader first; a survivor leads.
    c.stop_node(leader);
    drop(leader_v2);
    let new_leader = c.wait_leader();

    let new_leader_v1_alive = followers_v1
        .iter()
        .find(|a| a.node == new_leader)
        .and_then(|a| a.result.as_ref().ok())
        .is_some_and(|s| s.is_alive());
    // Each v1 attempt's verdict (refused or stopped?), with its Debug line.
    let verdicts: Vec<(bool, String)>;
    let _v2s: Vec<uc_service::Service<KvV2>>;
    let got = if new_leader_v1_alive {
        // Today's tree: the v1 build still serves the row on the new leader.
        let got = c.client(new_leader).query_u64();
        verdicts = followers_v1
            .iter()
            .map(|a| {
                (
                    a.is_refused_or_stopped_within(Duration::from_secs(10)),
                    format!("{a:?}"),
                )
            })
            .collect();
        got
    } else {
        // The fix stopped or refused v1: bring v2 up on both survivors.
        verdicts = followers_v1
            .iter()
            .map(|a| {
                (
                    a.is_refused_or_stopped_within(Duration::from_secs(10)),
                    format!("{a:?}"),
                )
            })
            .collect();
        followers_v1.clear(); // release every v1's row lock before v2 attaches
        _v2s = c.others(leader).map(|i| c.start::<KvV2>(i)).collect();
        c.client(new_leader).query_u64()
    };
    assert_eq!(got, 15, "the acknowledged Append(5) was lost");

    // Second line of defence: every v1 service was refused or has stopped.
    for (refused_or_stopped, dbg) in &verdicts {
        assert!(
            *refused_or_stopped,
            "a v1 service is still applying a row that runs 2.0: {dbg}"
        );
    }
}
