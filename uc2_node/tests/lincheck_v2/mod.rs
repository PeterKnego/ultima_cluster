// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! L3 lincheck harness for the v2 SDK (M5 Task 13, spec §8) — the v1
//! `uc_node/tests/lincheck/cluster.rs` capstone re-driven through the v2 stack:
//! real [`Node`]s over loopback UDP, a real per-node [`Service`] running the
//! non-persisting [`RegisterSm`], and real cross-process [`Client`]s over the
//! shared-memory IPC. The WGL checker / history / model in `uc-lincheck` are
//! reused UNTOUCHED — that reuse is the point of the port.
//!
//! ## Ownership model (differs from the v1 async harness)
//!
//! v2 is synchronous (polling agents on OS threads), so there is no
//! `tokio::sync::Mutex<Vec<Node>>`. Instead the **fault scheduler owns
//! `&mut LinClusterV2`** (the node/service handles) and drives kills/restarts on
//! the main thread, while **worker threads share only the immutable
//! `(node-id → instance-dir)` map** ([`LinClusterV2::dirs`]) plus the history /
//! stop flag. Workers never touch a `Node` handle: they attach a [`Client`] to a
//! node's shmem directory and route to the leader purely through the real
//! `NOT_LEADER{hint}` / `InstanceRestart` client-error contract — exactly the
//! cross-process path a production client uses. That removes all shared mutable
//! cluster state and matches the brief's `&mut self` mutator shape.
//!
//! ## Teardown ordering (node-first-then-service, per slot)
//!
//! On every kill/stop we drop the **node before its service** (v1 task12
//! finding #3). A node's linearizable-read barrier and (in v1) its apply loop can
//! block awaiting the service; tearing the service down first could wedge the
//! node's shutdown join. Dropping the node first stops its agents while the
//! service is still mapped, then the service (whose apply thread merely idles
//! once the node's commit counter freezes — its mmap of the on-disk ring/cnc
//! files stays valid) is stopped cleanly. Clients are cross-process and are shut
//! down independently (a restart's fresh `instance_id` retires a stale one).
//!
//! ## Sizing / environment (binding, mirrors `failover.rs`)
//!
//! Journals live on ext4 under `CARGO_TARGET_TMPDIR` (each [`Node`] preallocates
//! 64 MiB segments; three nodes on the ~2 GiB tmpfs `/tmp` would blow its quota).
//! The whole binary is serialized ([`serialize`]): three nodes × four agents +
//! three services + several worker/fault threads is well past the core count and
//! the sub-second failover budget is timing-sensitive.

#![allow(dead_code)] // each test file uses a different subset of the harness API

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use uc2_client::{Client, ClientError};
use uc2_consensus::election::NodeId;
use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};
use uc2_service::{ServiceBuilder, ServiceConfig};

use uc_lincheck::history::{History, Outcome};
use uc_lincheck::model::{Op, RegResp};
use uc_lincheck::register::{Cmd, CmdResp, RegisterSm};

const APP: &str = "lincheck-v2";

/// The full-box mutex: one lincheck-v2 test runs at a time (see module docs).
static TEST_LOCK: Mutex<()> = Mutex::new(());

/// Acquire the whole-box lock, recovering from a poisoned mutex so a panicking
/// test does not cascade-fail the rest.
pub fn serialize() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// A distinct, index-derived election seed so each node's randomized timeout
/// differs — a clean boot elects exactly one leader. Deterministic in `i` alone
/// (NOT the capstone seed) so a node restart reuses its own seed, exactly like
/// `failover.rs`.
fn seed_for(i: usize) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Per-node config with the shared harness knobs (election 150–300 ms for
/// sub-second failover, 4 MiB ring, small payloads). `faults` is applied to
/// every one of the node's sockets (drop/dup/reorder), used by the lossy-links
/// scenario; partitions are scripted separately through `partition_handles`.
fn make_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    instance_dir: PathBuf,
    addr: SocketAddr,
    faults: FaultConfig,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        bind: addr,
        instance_dir,
        app_id: APP.into(),
        buffer_bytes: 1 << 22, // 4 MiB
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: seed_for(id as usize),
        faults,
    }
}

/// Bind a fresh UDP socket on a specific loopback address, retrying briefly (the
/// old socket was just dropped on crash; rebinding the same loopback port
/// succeeds almost immediately for UDP).
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

/// Start a fresh service running an EMPTY `RegisterSm` on `dir`. The SM persists
/// nothing, so this is always a clean in-memory state; the node reconstructs it
/// from the replicated log (journal replay, Task 9) — that reconstruction is
/// exactly what the capstone's service-crash fault proves.
fn spawn_service(dir: &Path) -> uc2_service::Service<RegisterSm> {
    ServiceBuilder::new(ServiceConfig::new(dir, APP), RegisterSm::default())
        .start()
        .expect("service start")
}

// ------------------------------------------------------------------ one slot

/// One cluster member: its fixed identity/address/dir plus the live node +
/// service (taken out of their `Option`s across a crash/restart).
pub struct NodeSlot {
    id: NodeId,
    addr: SocketAddr,
    instance_dir: PathBuf,
    node: Option<Node>,
    service: Option<uc2_service::Service<RegisterSm>>,
}

impl NodeSlot {
    fn is_live(&self) -> bool {
        self.node.is_some()
    }
    /// Serving iff a live node that both flags leader and passes the serving gate.
    fn is_serving_leader(&self) -> bool {
        self.node.as_ref().is_some_and(|n| n.can_serve() && n.is_leader())
    }
}

// --------------------------------------------------------------- the cluster

pub struct LinClusterV2 {
    nodes: Vec<NodeSlot>,
    members: Vec<(NodeId, SocketAddr)>,
    faults: FaultConfig,
    root: PathBuf,
}

impl LinClusterV2 {
    /// Bring up an `n`-node cluster under `root` (a caller-owned tempdir): bind
    /// every socket first (so the full member map is known before any agent
    /// runs), start each node on its pre-bound socket, then attach one service
    /// per node. Every node gets `faults` on its sockets (default = none).
    pub fn start(root: &Path, n: usize, faults: FaultConfig) -> LinClusterV2 {
        let socks: Vec<UdpSocket> =
            (0..n).map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind")).collect();
        let members: Vec<(NodeId, SocketAddr)> =
            socks.iter().enumerate().map(|(i, s)| (i as NodeId, s.local_addr().unwrap())).collect();

        let mut nodes = Vec::with_capacity(n);
        for (i, sock) in socks.into_iter().enumerate() {
            let addr = members[i].1;
            let instance_dir = root.join(format!("n{i}"));
            let cfg = make_config(i as NodeId, members.clone(), instance_dir.clone(), addr, faults);
            let node = Node::start_with_socket(cfg, sock).expect("node start");
            // A follower's service follows the committed log too, so every node
            // carries a service from boot — the new leader after a failover
            // already has one attached.
            let service = spawn_service(&instance_dir);
            nodes.push(NodeSlot { id: i as NodeId, addr, instance_dir, node: Some(node), service: Some(service) });
        }
        LinClusterV2 { nodes, members, faults, root: root.to_owned() }
    }

    /// The fixed `(node-id → instance-dir)` map workers route over (index i = id
    /// i). Stable for the whole run: restarts reuse the same dir.
    pub fn dirs(&self) -> Vec<PathBuf> {
        self.nodes.iter().map(|s| s.instance_dir.clone()).collect()
    }

    /// Index of the current serving leader, or `None` in a transient window.
    pub fn leader(&self) -> Option<usize> {
        (0..self.nodes.len()).find(|&i| self.nodes[i].is_serving_leader())
    }

    /// A fresh client attached to node `node`'s shmem directory (the real
    /// cross-process path). Panics on attach failure — callers use this against a
    /// node they know is up.
    pub fn client(&self, node: usize) -> Client {
        Client::connect(&self.nodes[node].instance_dir, APP).expect("client attach")
    }

    // --------------------------------------------------------- wait helpers

    /// Wait for EXACTLY one serving leader across all live nodes; assert no
    /// split-brain throughout. Returns its index.
    pub fn await_single_serving(&self, secs: u64) -> usize {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            let serving: Vec<usize> =
                (0..self.nodes.len()).filter(|&i| self.nodes[i].is_serving_leader()).collect();
            assert!(serving.len() <= 1, "split-brain: nodes {serving:?} all serve");
            if serving.len() == 1 {
                return serving[0];
            }
            assert!(Instant::now() < deadline, "no single serving leader within {secs}s");
            std::thread::yield_now();
        }
    }

    /// Wait until the cluster has RECONVERGED to exactly one serving leader,
    /// TOLERATING the transient window in which two nodes both report serving —
    /// after healing a leader-isolation partition, the deposed ex-leader keeps
    /// believing it serves (its `can_serve`/`is_leader` flags stay set) until it
    /// learns the higher term and steps down. That transient is benign (the
    /// isolated leader could never COMMIT or confirm a read — proven separately
    /// by the `read_from` probes), so unlike [`await_single_serving`] this waiter
    /// does NOT assert `<= 1 serving`; it simply waits for the set to settle to
    /// one. Returns the surviving leader's index.
    pub fn await_reconverged(&self, secs: u64) -> usize {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            let serving: Vec<usize> =
                (0..self.nodes.len()).filter(|&i| self.nodes[i].is_serving_leader()).collect();
            if serving.len() == 1 {
                return serving[0];
            }
            assert!(Instant::now() < deadline, "cluster did not reconverge to one leader within {secs}s");
            std::thread::yield_now();
        }
    }

    /// Wait until exactly one node OTHER than `exclude` is serving (used after a
    /// leader kill, before the killed node has rejoined). Returns its index.
    fn await_serving_excluding(&self, exclude: usize, secs: u64) -> usize {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            let serving: Vec<usize> = (0..self.nodes.len())
                .filter(|&i| i != exclude && self.nodes[i].is_serving_leader())
                .collect();
            assert!(serving.len() <= 1, "split-brain among survivors: {serving:?}");
            if serving.len() == 1 {
                return serving[0];
            }
            assert!(Instant::now() < deadline, "no survivor leader within {secs}s");
            std::thread::yield_now();
        }
    }

    // ------------------------------------------------------------ the faults

    /// Kill the current leader's node + service (hard crash), then restart the
    /// node on the SAME dir + port and attach a FRESH, EMPTY service. The empty
    /// SM (state loss) is the point: the node reconstructs it from the replicated
    /// log. Node-first-then-service teardown (module docs). Quorum (2/3) holds
    /// throughout, so the survivors elect and keep serving during the restart.
    pub fn kill_and_restart_leader(&mut self) {
        let Some(li) = self.leader() else { return };
        let (id, addr, dir) = {
            let s = &self.nodes[li];
            (s.id, s.addr, s.instance_dir.clone())
        };
        // Node BEFORE service (see module docs).
        if let Some(node) = self.nodes[li].node.take() {
            node.crash();
        }
        if let Some(service) = self.nodes[li].service.take() {
            service.crash();
        }
        // Survivors re-elect (quorum 2/3 holds).
        self.await_serving_excluding(li, 20);
        // Restart on the persisted dir + same port (static membership): recovers
        // durable state, rejoins in the current term.
        let sock = rebind(addr);
        let cfg = make_config(id, self.members.clone(), dir.clone(), addr, self.faults);
        let node = Node::start_with_socket(cfg, sock).expect("leader node restart");
        let service = spawn_service(&dir);
        self.nodes[li].node = Some(node);
        self.nodes[li].service = Some(service);
        // Full cluster settles back to a single serving leader.
        self.await_single_serving(20);
    }

    /// Crash the current leader's SERVICE only (the node stays up and leader).
    /// A fresh, EMPTY service reattaches and reconstructs the SM from the log
    /// (Task 9). Submits keep committing on the node during the gap; reads
    /// `RETRY` until the fresh service catches up, then resume.
    pub fn crash_and_restart_leader_service(&mut self) {
        let Some(li) = self.leader() else { return };
        let dir = self.nodes[li].instance_dir.clone();
        if let Some(service) = self.nodes[li].service.take() {
            service.crash();
        }
        let service = spawn_service(&dir);
        self.nodes[li].service = Some(service);
        // The node never lost quorum, so a serving leader still exists.
        self.await_single_serving(20);
    }

    // -------------------------------------------------------- partitions

    /// Cut every link between live nodes `a` and `b` (both send directions).
    fn cut(&self, a: usize, b: usize) {
        if let (Some(na), Some(nb)) = (self.nodes[a].node.as_ref(), self.nodes[b].node.as_ref()) {
            for h in na.partition_handles() {
                h.block(self.nodes[b].addr);
            }
            for h in nb.partition_handles() {
                h.block(self.nodes[a].addr);
            }
        }
    }

    /// Isolate ONE follower from the other two (minority partition). Returns the
    /// isolated follower's index.
    pub fn partition_minority(&self) -> usize {
        let li = self.await_single_serving(20);
        let follower = (0..self.nodes.len())
            .find(|&i| i != li && self.nodes[i].is_live())
            .expect("a live follower");
        for other in (0..self.nodes.len()).filter(|&i| i != follower && self.nodes[i].is_live()) {
            self.cut(follower, other);
        }
        follower
    }

    /// Isolate the current leader from both followers; the majority elects a new
    /// one. Returns the (now-isolated) old leader's index.
    pub fn partition_leader(&self) -> usize {
        let li = self.await_single_serving(20);
        for other in (0..self.nodes.len()).filter(|&i| i != li && self.nodes[i].is_live()) {
            self.cut(li, other);
        }
        li
    }

    /// Three-way split — no side has a majority (total quorum loss).
    pub fn partition_quorum_loss(&self) {
        let live: Vec<usize> = (0..self.nodes.len()).filter(|&i| self.nodes[i].is_live()).collect();
        for a in 0..live.len() {
            for b in (a + 1)..live.len() {
                self.cut(live[a], live[b]);
            }
        }
    }

    /// Heal every partition on every live node.
    pub fn heal(&self) {
        for s in &self.nodes {
            if let Some(n) = s.node.as_ref() {
                for h in n.partition_handles() {
                    h.clear();
                }
            }
        }
    }

    /// Linearizable read addressed to a SPECIFIC node via a fresh client on its
    /// dir (not leader-routed) — used to probe a partitioned-away node, which
    /// must NEVER answer with a stale `Ok`. `Ok(v)` → `Outcome::Ok`; any client
    /// error → `Outcome::Indeterminate` (dropped by the checker), except a
    /// (de)serialization / wrong-cluster error, which is a harness bug and
    /// panics.
    pub fn read_from(&self, node: usize) -> Outcome {
        let dir = self.nodes[node].instance_dir.clone();
        let client = match Client::connect(&dir, APP) {
            Ok(c) => c,
            // A failed attach against a partitioned/idle node is not a stale
            // read — record it as indeterminate (the checker drops it).
            Err(_) => return Outcome::Indeterminate,
        };
        let out = match client.query_linearizable::<(), Option<u64>>(&()) {
            Ok(v) => Outcome::Ok(RegResp::Value(v)),
            Err(e) => classify_read_err(&e),
        };
        client.shutdown();
        out
    }

    /// Node-first-then-service teardown for every slot (module docs).
    pub fn stop(mut self) {
        for s in &mut self.nodes {
            if let Some(node) = s.node.take() {
                node.stop();
            }
            if let Some(service) = s.service.take() {
                service.stop();
            }
        }
    }
}

// ------------------------------------------------------ client op outcomes

#[derive(Debug)]
pub enum SubmitOutcome {
    Ok(CmdResp),
    /// May or may not have committed (timed out / answer lapped / gave up
    /// routing) — the WGL "indeterminate mutation".
    Indeterminate,
    /// A genuine harness/wiring bug (bad codec, wrong app/version) — never a
    /// legitimate operational outcome.
    Fatal(String),
}

#[derive(Debug)]
pub enum ReadOutcome {
    Ok(Option<u64>),
    Indeterminate,
    Fatal(String),
}

/// Classify a `query_linearizable` error for a probe/read: any operational error
/// (not-leader, retry, timeout, backpressure, restart, lapped, attach) carries
/// no committed information → drop it as `Indeterminate`. A codec / wrong-cluster
/// error is a harness bug and must surface — panic.
fn classify_read_err(e: &ClientError) -> Outcome {
    match e {
        ClientError::Decode(_)
        | ClientError::AppIdMismatch { .. }
        | ClientError::VersionMismatch { .. } => panic!("harness bug in read: {e:?}"),
        _ => Outcome::Indeterminate,
    }
}

// --------------------------------------------------------- worker routing

/// A worker's client connection state: the current target dir index plus the
/// live client (dropped + reconnected on `NOT_LEADER`/`InstanceRestart`).
struct WorkerConn {
    dirs: Arc<Vec<PathBuf>>,
    target: usize,
    client: Option<Client>,
}

impl WorkerConn {
    fn new(dirs: Arc<Vec<PathBuf>>, start: usize) -> Self {
        Self { dirs, target: start, client: None }
    }
    /// Ensure a client attached to `self.target`; `None` if the attach failed
    /// (node mid-restart / partitioned) — the caller rotates and retries.
    fn client(&mut self) -> Option<&Client> {
        if self.client.is_none() {
            match Client::connect(&self.dirs[self.target], APP) {
                Ok(c) => self.client = Some(c),
                Err(_) => return None,
            }
        }
        self.client.as_ref()
    }
    fn reconnect_to(&mut self, idx: usize) {
        if let Some(c) = self.client.take() {
            c.shutdown();
        }
        self.target = idx % self.dirs.len();
    }
    fn rotate(&mut self) {
        let next = (self.target + 1) % self.dirs.len();
        self.reconnect_to(next);
    }
    fn drop_client(&mut self) {
        if let Some(c) = self.client.take() {
            c.shutdown();
        }
    }
}

/// Submit `cmd` to the current leader. It is ONLY safe to retry a submit on an
/// error that GUARANTEES the command never entered the log — else a retry can
/// double-apply it (a duplicate CAS turns a later `true` into a `false`: a
/// textbook linearizability violation). Two errors carry that guarantee, and
/// the node's ingress drain proves it: it emits `NOT_LEADER` (and never
/// appends) while not serving, and refuses admission with `BackpressureFull`
/// before any append.
///
/// So `NotLeader{hint}` and `BackpressureFull` are guaranteed-not-committed and
/// route+retry (the whole point — the same logical op commits exactly once), and
/// `Retry` (query-only per its contract; it cannot occur for a submit, but if it
/// did it is likewise pre-append) retries too. EVERYTHING else —
/// `InstanceRestart`, `Timeout`, `ResponseOverwritten`, `Cnc`, `Ring` — is
/// MAYBE-committed, so it returns `Indeterminate` and is NEVER retried (the WGL
/// "indeterminate mutation": present-or-absent, response unconstrained). A stale
/// client (node restarted / attach raced a re-created page) is dropped so the
/// next op reconnects to the fresh page.
fn submit_cmd(conn: &mut WorkerConn, cmd: &Cmd, deadline: Instant) -> SubmitOutcome {
    loop {
        if Instant::now() > deadline {
            return SubmitOutcome::Indeterminate; // gave up routing → in-limbo
        }
        let Some(client) = conn.client() else {
            std::thread::sleep(Duration::from_millis(20));
            conn.rotate();
            continue;
        };
        match client.submit::<Cmd, CmdResp>(cmd) {
            Ok(r) => return SubmitOutcome::Ok(r),
            // Guaranteed-not-committed → safe to route + retry.
            Err(ClientError::NotLeader { hint }) => match hint {
                Some(h) => conn.reconnect_to(h as usize),
                None => {
                    std::thread::sleep(Duration::from_millis(20));
                    conn.rotate();
                }
            },
            Err(ClientError::BackpressureFull) | Err(ClientError::Retry) => {
                std::thread::sleep(Duration::from_millis(10));
            }
            // Maybe-committed → indeterminate, NEVER retried. Drop the (now
            // stale) client on a restart/attach fault so the next op reconnects.
            Err(ClientError::InstanceRestart { .. })
            | Err(ClientError::Cnc(_))
            | Err(ClientError::Ring(_)) => {
                conn.drop_client();
                return SubmitOutcome::Indeterminate;
            }
            Err(ClientError::Timeout(_)) | Err(ClientError::ResponseOverwritten) => {
                return SubmitOutcome::Indeterminate;
            }
            // Harness/wiring bugs.
            Err(e @ ClientError::Decode(_))
            | Err(e @ ClientError::AppIdMismatch { .. })
            | Err(e @ ClientError::VersionMismatch { .. })
            | Err(e @ ClientError::ShutDown) => return SubmitOutcome::Fatal(format!("{e:?}")),
        }
    }
}

/// Linearizable read against the current leader, same routing discipline as
/// [`submit_cmd`]. A `RETRY` (barrier could not confirm within its deadline) is
/// retried while routing; on the overall deadline it is `Indeterminate` (dropped
/// by the checker). An errored read never yields `Ok`, so a lost read simply
/// carries no information.
fn read_leader(conn: &mut WorkerConn, deadline: Instant) -> ReadOutcome {
    loop {
        if Instant::now() > deadline {
            return ReadOutcome::Indeterminate;
        }
        let Some(client) = conn.client() else {
            std::thread::sleep(Duration::from_millis(20));
            conn.rotate();
            continue;
        };
        match client.query_linearizable::<(), Option<u64>>(&()) {
            Ok(v) => return ReadOutcome::Ok(v),
            Err(ClientError::NotLeader { hint }) => match hint {
                Some(h) => conn.reconnect_to(h as usize),
                None => {
                    std::thread::sleep(Duration::from_millis(20));
                    conn.rotate();
                }
            },
            Err(ClientError::Retry) => std::thread::sleep(Duration::from_millis(15)),
            Err(ClientError::InstanceRestart { .. }) | Err(ClientError::Cnc(_))
            | Err(ClientError::Ring(_)) => {
                conn.drop_client();
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(ClientError::BackpressureFull) => std::thread::sleep(Duration::from_millis(10)),
            Err(ClientError::Timeout(_)) | Err(ClientError::ResponseOverwritten) => {
                return ReadOutcome::Indeterminate;
            }
            Err(e @ ClientError::Decode(_))
            | Err(e @ ClientError::AppIdMismatch { .. })
            | Err(e @ ClientError::VersionMismatch { .. })
            | Err(e @ ClientError::ShutDown) => return ReadOutcome::Fatal(format!("{e:?}")),
        }
    }
}

/// One worker: until `stop`, pick a seeded op (Write/Read/CAS), route it to the
/// leader, classify the outcome per the WGL contract, and record it. `last_seen`
/// is shared so CAS picks a recently-observed value often enough that some
/// succeed. `throttle` paces each op so per-failover op counts stay modest
/// (keeps the checker's search bounded). Each op is given a 15 s routing budget.
fn worker(
    id: u32,
    dirs: Arc<Vec<PathBuf>>,
    history: Arc<History>,
    stop: Arc<AtomicBool>,
    mut rng: StdRng,
    last_seen: Arc<AtomicU64>,
    throttle: Duration,
) {
    let mut conn = WorkerConn::new(dirs, id as usize);
    while !stop.load(Ordering::Relaxed) {
        if !throttle.is_zero() {
            std::thread::sleep(throttle);
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        match rng.random_range(0..3u8) {
            0 => {
                let v = rng.random_range(1..1000u64);
                let inv = history.invoke();
                let outcome = match submit_cmd(&mut conn, &Cmd::Write(v), deadline) {
                    SubmitOutcome::Ok(_) => Outcome::Ok(RegResp::Ack),
                    SubmitOutcome::Indeterminate => Outcome::Indeterminate,
                    SubmitOutcome::Fatal(e) => panic!("fatal submit: {e}"),
                };
                history.record(id, Op::Write(v), inv, outcome);
            }
            1 => {
                let inv = history.invoke();
                let outcome = match read_leader(&mut conn, deadline) {
                    ReadOutcome::Ok(v) => {
                        if let Some(x) = v {
                            last_seen.store(x, Ordering::Relaxed);
                        }
                        Outcome::Ok(RegResp::Value(v))
                    }
                    ReadOutcome::Indeterminate => Outcome::Indeterminate,
                    ReadOutcome::Fatal(e) => panic!("fatal read: {e}"),
                };
                history.record(id, Op::Read, inv, outcome);
            }
            _ => {
                let old = if rng.random_bool(0.7) {
                    last_seen.load(Ordering::Relaxed)
                } else {
                    rng.random_range(1..1000u64)
                };
                let new = rng.random_range(1..1000u64);
                let inv = history.invoke();
                let outcome = match submit_cmd(&mut conn, &Cmd::Cas { old, new }, deadline) {
                    SubmitOutcome::Ok(CmdResp::CasResult(b)) => {
                        if b {
                            last_seen.store(new, Ordering::Relaxed);
                        }
                        Outcome::Ok(RegResp::CasOk(b))
                    }
                    SubmitOutcome::Ok(other) => panic!("cas returned non-cas response: {other:?}"),
                    SubmitOutcome::Indeterminate => Outcome::Indeterminate,
                    SubmitOutcome::Fatal(e) => panic!("fatal cas: {e}"),
                };
                history.record(id, Op::Cas { old, new }, inv, outcome);
            }
        }
    }
    conn.drop_client();
}

/// Spawn `n_workers` op-driving threads. Each gets its own seeded RNG
/// (`seed ^ (w * φ)`) and starts routed at a distinct node.
pub fn spawn_workers(
    dirs: &Arc<Vec<PathBuf>>,
    history: &Arc<History>,
    stop: &Arc<AtomicBool>,
    last_seen: &Arc<AtomicU64>,
    seed: u64,
    throttle: Duration,
    n_workers: u32,
) -> Vec<JoinHandle<()>> {
    (0..n_workers)
        .map(|w| {
            let rng = StdRng::seed_from_u64(seed ^ (w as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let (dirs, history, stop, last_seen) =
                (Arc::clone(dirs), Arc::clone(history), Arc::clone(stop), Arc::clone(last_seen));
            std::thread::spawn(move || worker(w, dirs, history, stop, rng, last_seen, throttle))
        })
        .collect()
}

/// Join workers, re-raising any worker panic (a `Fatal` client error or a wrong
/// CAS response — both genuine bugs) so it fails the test rather than being
/// silently absorbed.
pub fn join_workers(handles: Vec<JoinHandle<()>>) {
    for h in handles {
        if let Err(e) = h.join() {
            std::panic::resume_unwind(e);
        }
    }
}
