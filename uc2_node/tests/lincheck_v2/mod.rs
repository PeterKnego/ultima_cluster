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

use std::collections::HashMap;
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
use uc2_crypto::identity::Identity;
use uc2_crypto::rotation::RotationPolicy;
use uc2_log::cnc::{AdminReq, AdminResp, CncPage};
use uc2_net::fault::FaultConfig;
use uc2_node::{CryptoConfig, Node, NodeConfig};
use uc2_service::{ServiceBuilder, ServiceConfig, SnapshotPolicy, SnapshotStateMachine};

use uc_lincheck::history::{History, Outcome};
use uc_lincheck::model::{Op, RegResp};
use uc_lincheck::register::{Cmd, CmdResp, RegisterSm};

/// `pub` since M14c2: the per-FSM cnc samplers/oracles in the capstone test
/// binaries open the same cnc pages this harness does, and must name the same
/// `app_id`.
pub const APP: &str = "lincheck-v2";

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

// --------------------------------------------------- M8 Task 15: crypto ON
//
// Re-running the project's correctness capstones with wire crypto enabled
// (spec 2026-07-28, task 15). `ClusterCfg::crypto` is the switch; when set,
// `LinClusterV2` generates one X25519 keypair per node id (base `0..n` plus,
// for `spare_node` clusters, a generous block of M7 spare ids) and a single
// shared allowlist, then boots every node with `CryptoConfig::Enabled`
// instead of `Disabled`. Mirrors `uc2_node/tests/crypto_cluster.rs`'s own
// fixture (`write_crypto_material`), generalized from a contiguous `0..n`
// index to an explicit id LIST — the M7 spare's ids are not contiguous with
// the base members and are allocated lazily (`LinClusterV2::next_spare_id`).

/// Deterministic per-id private key. Distinct byte pattern from
/// `crypto_cluster.rs`'s own fixture (pure coincidence would be harmless,
/// but there is no reason to share key material between two independent
/// test binaries).
fn crypto_private_key(id: NodeId) -> [u8; 32] {
    let mut k = [0x70u8; 32];
    k[0..4].copy_from_slice(&id.to_le_bytes());
    k
}

fn crypto_write_key_file(path: &Path, private: [u8; 32]) {
    std::fs::write(path, private).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}

/// Standard-alphabet base64 with padding, matching `uc2_crypto::identity`'s
/// allowlist parser — hand-rolled the same way `crypto_cluster.rs` does,
/// rather than adding a `base64` dev-dependency for one fixture.
fn crypto_b64_32(bytes: &[u8; 32]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[((n >> 18) & 0x3F) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3F) as usize] as char);
        out.push(if chunk.len() > 1 { ALPHABET[((n >> 6) & 0x3F) as usize] as char } else { '=' });
        out.push(if chunk.len() > 2 { ALPHABET[(n & 0x3F) as usize] as char } else { '=' });
    }
    out
}

/// Every id a crypto-enabled cluster of `n` base members might ever need a
/// key for: `0..n` plus, when `spare_node` is set, a generous block of the
/// M7 reconfig-churn capstone's spare ids (`LinClusterV2::next_spare_id`
/// starts at 100 and increments once per add/remove cycle — a full budgeted
/// run cycles through at most a couple dozen).
fn crypto_ids_for(n: usize, spare_node: bool) -> Vec<NodeId> {
    let mut ids: Vec<NodeId> = (0..n as NodeId).collect();
    if spare_node {
        ids.extend(100..200);
    }
    ids
}

/// One keypair per id in `ids` plus a single shared allowlist naming all of
/// them (every node trusts every other node — the same posture
/// `crypto_cluster.rs` and a `uc2ctl`-managed operator allowlist both use).
struct CryptoMaterial {
    key_paths: HashMap<NodeId, PathBuf>,
    allowlist_path: PathBuf,
}

fn write_crypto_material(dir: &Path, ids: &[NodeId]) -> CryptoMaterial {
    let mut key_paths = HashMap::with_capacity(ids.len());
    let mut publics = Vec::with_capacity(ids.len());
    for &id in ids {
        let node_dir = dir.join(format!("keys{id}"));
        std::fs::create_dir_all(&node_dir).unwrap();
        let key_path = node_dir.join("node.key");
        crypto_write_key_file(&key_path, crypto_private_key(id));
        let public = Identity::load(&key_path).unwrap().public_bytes();
        publics.push((id, public));
        key_paths.insert(id, key_path);
    }
    let mut text = String::new();
    for (id, public) in &publics {
        text.push_str(&format!("{id} {}\n", crypto_b64_32(public)));
    }
    let allowlist_path = dir.join("crypto-allowlist");
    std::fs::write(&allowlist_path, text).unwrap();
    CryptoMaterial { key_paths, allowlist_path }
}

/// M14c2: which FSM set every node declares. `Single` is byte-for-byte the
/// pre-M14c2 harness (one implicit FSM 0). `Two` declares ids {0, 1} with the
/// given lag policy and starts a second service (`SM1`) per node.
#[derive(Clone, Copy)]
pub enum FsmSet {
    Single,
    Two { lag: uc2_node::FsmLag },
}

/// Per-node config with the shared harness knobs (election 150–300 ms for
/// sub-second failover, 4 MiB ring, small payloads). `faults` is applied to
/// every one of the node's sockets (drop/dup/reorder), used by the lossy-links
/// scenario; partitions are scripted separately through `partition_handles`.
/// M6 Task 10: the purge/snapshot knobs a cluster boots (and re-boots) with.
/// `Default` = the M5 posture (no purge, no snapshots, 64 MiB segments) so the
/// failover capstone is byte-for-byte unchanged; the purge-churn capstone sets
/// all three to exercise snapshot-backed purge + below-floor reconstruction.
#[derive(Clone, Copy)]
pub struct ClusterCfg {
    pub purge: uc2_node::PurgePolicy,
    pub journal_segment_bytes: u64,
    /// `> 0` → services start via `start_with_snapshots` with this cadence; `0` →
    /// plain `start` (no snapshot builder), the M5 default.
    pub snapshot_interval_bytes: u64,
    /// M7 Task 10: reserve an extra (not-yet-a-member) address for
    /// [`LinClusterV2::random_config_op`] to cycle a "spare" node through
    /// add-learner -> promote -> demote -> remove-learner. `false` (the M5/M6
    /// default) reserves nothing — [`random_config_op`](LinClusterV2::random_config_op)
    /// panics if called on a cluster that didn't ask for one.
    pub spare_node: bool,
    /// M8 Task 15: boot every node (base members AND, for a `spare_node`
    /// cluster, every id the spare cycles through) with wire crypto
    /// `Enabled` instead of `Disabled` — see the module docs above this
    /// struct. `false` (the default) is byte-for-byte the pre-M8 posture:
    /// every existing capstone is unaffected.
    pub crypto: bool,
    /// M14c2: the declared FSM set every node boots with. [`FsmSet::Single`]
    /// (the default) is the pre-M14c2 posture — one implicit FSM 0, no second
    /// service — so every existing capstone is unaffected.
    pub services: FsmSet,
    /// M14c2 T11: the live log-buffer ring's capacity (`NodeConfig::buffer_bytes`).
    /// Must be a power of two. Default `1 << 22` (4 MiB) is byte-for-byte the
    /// pre-M14c2 hardcoded value, so every existing capstone is unaffected. A
    /// test that needs to force a fresh service's `start_pos == 0` read to
    /// OVERRUN the live ring (so reconstruction actually consults the
    /// archived journal / snapshot path, rather than reading position 0
    /// straight off a still-live buffer) sets this small instead of writing
    /// an enormous volume through the default 4 MiB ring.
    pub buffer_bytes: u64,
}

impl Default for ClusterCfg {
    fn default() -> Self {
        Self {
            purge: uc2_node::PurgePolicy::Disabled,
            journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
            snapshot_interval_bytes: 0,
            spare_node: false,
            crypto: false,
            services: FsmSet::Single,
            buffer_bytes: 1 << 22,
        }
    }
}

/// The `NodeConfig::services` a [`ClusterCfg`] declares. `Single` is
/// `ServicesConfig::default()` — the exact value every pre-M14c2 node booted
/// with, so the `Single` path is byte-identical.
fn services_config(ccfg: ClusterCfg) -> uc2_node::ServicesConfig {
    match ccfg.services {
        FsmSet::Single => uc2_node::ServicesConfig::default(),
        FsmSet::Two { lag } => {
            uc2_node::ServicesConfig::from_ids(&[0, 1], Some(lag)).expect("ids 0,1")
        }
    }
}

fn make_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    instance_dir: PathBuf,
    addr: SocketAddr,
    faults: FaultConfig,
    ccfg: ClusterCfg,
    crypto: CryptoConfig,
) -> NodeConfig {
    // M7 Task 10: a `spare_node` cluster runs a REAL 4th node (its own full
    // set of polling agent threads + a service) on top of the base 3 — on a
    // 4-core box that is real oversubscription (the module doc already
    // notes the base 3-node case is "well past the core count"), and the
    // 150-300 ms timeout tuned for 3 nodes was observed to livelock (no
    // node ever re-elected after the first kill under heavy scheduling
    // contention with 4 real nodes live). Widen it here — spare-enabled
    // clusters only, so the other three (already-tuned, already-green)
    // capstones' timing is byte-for-byte unchanged.
    let (timeout_min_ns, timeout_max_ns) =
        if ccfg.spare_node { (900_000_000, 1_600_000_000) } else { (150_000_000, 300_000_000) };
    NodeConfig {
        id,
        members,
        bind: addr,
        instance_dir,
        app_id: APP.into(),
        buffer_bytes: ccfg.buffer_bytes as usize,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: timeout_min_ns,
        election_timeout_max_ns: timeout_max_ns,
        seed: seed_for(id as usize),
        faults,
        purge: ccfg.purge,
        learners: Vec::new(),
        journal_segment_bytes: ccfg.journal_segment_bytes,
        crypto,
        services: services_config(ccfg),
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
///
/// M14c2: `id` is the FSM the service attaches as — `0` everywhere the
/// pre-M14c2 harness spawned a service (`ServiceConfig::service_id(0)` is the
/// default, so the `Single` path is unchanged), `1` for the second FSM under
/// [`FsmSet::Two`].
fn spawn_service<SM: SnapshotStateMachine + Default>(
    dir: &Path,
    snapshot_interval_bytes: u64,
    id: u8,
) -> uc2_service::Service<SM> {
    let cfg = ServiceConfig::new(dir, APP).service_id(id);
    if snapshot_interval_bytes == 0 {
        ServiceBuilder::new(cfg, SM::default()).start().expect("service start")
    } else {
        // M6 Task 10: snapshot-capable service — builds on-disk snapshots on the
        // policy cadence so the node can advance its purge floor. Below-floor
        // reconstruction after a service crash then goes via snapshot install.
        let cfg = cfg.snapshot_policy(SnapshotPolicy { interval_bytes: snapshot_interval_bytes });
        ServiceBuilder::new(cfg, SM::default())
            .start_with_snapshots()
            .expect("snapshot service start")
    }
}

/// The second FSM (id 1) under [`FsmSet::Two`], else `None`. Every path that
/// spawns a node's service pairs it with this one, so a node under `Two` always
/// has BOTH declared FSMs attached (a node missing one stalls commit by design
/// — the report ceiling).
fn spawn_service1<SM1: SnapshotStateMachine + Default>(
    dir: &Path,
    ccfg: ClusterCfg,
) -> Option<uc2_service::Service<SM1>> {
    matches!(ccfg.services, FsmSet::Two { .. })
        .then(|| spawn_service::<SM1>(dir, ccfg.snapshot_interval_bytes, 1))
}

// ------------------------------------------------------------------ one slot

/// One cluster member: its fixed identity/address/dir plus the live node +
/// service (taken out of their `Option`s across a crash/restart).
pub struct NodeSlot<SM: SnapshotStateMachine + Default, SM1: SnapshotStateMachine + Default> {
    id: NodeId,
    addr: SocketAddr,
    instance_dir: PathBuf,
    node: Option<Node>,
    service: Option<uc2_service::Service<SM>>,
    /// M14c2: the node's FSM-1 service under [`FsmSet::Two`]; `None` under
    /// `Single`. Taken/respawned in lockstep with `service` on every crash path.
    service1: Option<uc2_service::Service<SM1>>,
}

impl<SM: SnapshotStateMachine + Default, SM1: SnapshotStateMachine + Default> NodeSlot<SM, SM1> {
    fn is_live(&self) -> bool {
        self.node.is_some()
    }
    /// Serving iff a live node that both flags leader and passes the serving gate.
    fn is_serving_leader(&self) -> bool {
        self.node.as_ref().is_some_and(|n| n.can_serve() && n.is_leader())
    }
}

// --------------------------------------------------------------- the cluster

/// M7 Task 10: the "spare" node's cycle position — a single fresh id at a
/// time, walking add-learner -> promote -> demote -> remove-learner. Every
/// step is only attempted once the PREVIOUS admin op has actually committed
/// (see [`LinClusterV2::random_config_op`]'s `config_pending` gate at the top
/// of every call) — `status == 0` on the leader's own admin response is only
/// a LOCAL, optimistic accept (confirmed against `uc2_node/tests/reconfig.rs`'s
/// `truncation_revert_e2e`: an isolated leader gets `status: 0` from a change
/// that is later reverted wholesale), so phase never advances off a bare
/// `status == 0` alone — it advances on the NEXT call finding `config_pending`
/// cleared, i.e. genuinely committed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum SparePhase {
    /// No cycle in flight; the next call starts a fresh id at `AddLearner`.
    Idle,
    Added,
    Promoted,
    Demoted,
}

/// `SM1` is the second FSM's state machine, used ONLY when
/// `ClusterCfg::services` is [`FsmSet::Two`]; it defaults to `SM`, so every
/// pre-M14c2 spelling (`LinClusterV2`, `LinClusterV2<ListAppendSm>`) keeps
/// meaning exactly what it did.
pub struct LinClusterV2<
    SM: SnapshotStateMachine + Default = RegisterSm,
    SM1: SnapshotStateMachine + Default = SM,
> {
    nodes: Vec<NodeSlot<SM, SM1>>,
    members: Vec<(NodeId, SocketAddr)>,
    faults: FaultConfig,
    ccfg: ClusterCfg,
    root: PathBuf,
    /// M7 Task 10 (`ClusterCfg::spare_node`): the reserved address the spare
    /// cycles through; `None` when the cluster didn't ask for one.
    spare_addr: Option<SocketAddr>,
    spare_root: Option<PathBuf>,
    /// The spare's CURRENTLY live node+service, if a cycle is in flight
    /// (`spare_phase != Idle`). `NodeSlot::id` here is whatever fresh id the
    /// current cycle allocated — never the same id twice (tombstone rule).
    spare: Option<NodeSlot<SM, SM1>>,
    spare_phase: SparePhase,
    /// Monotonic fresh-id allocator for the spare (starts at 100, per the
    /// brief's tombstone rule — an id, once removed, can NEVER be re-added).
    next_spare_id: NodeId,
    /// M7 Task 10 non-vacuity counter: incremented once per admin op that
    /// actually committed (add/promote/demote/remove each count separately).
    /// `pub` — the capstone reads it directly (`cluster.config_ops_accepted`).
    // counts LOCAL leader accepts (status=0 replies), not durable commits — a late-crash accept may be reverted; the capstone's non-vacuity floor only needs "the arm exercised reconfig", so accepts are the right denominator.
    pub config_ops_accepted: u32,
    /// M8 Task 15: `Some` iff `ClusterCfg::crypto` was set — the provisioned
    /// keypairs/allowlist every node (base member or spare) boots with.
    crypto: Option<CryptoMaterial>,
}

impl<SM: SnapshotStateMachine + Default, SM1: SnapshotStateMachine + Default> LinClusterV2<SM, SM1> {
    /// Bring up an `n`-node cluster under `root` (a caller-owned tempdir): bind
    /// every socket first (so the full member map is known before any agent
    /// runs), start each node on its pre-bound socket, then attach one service
    /// per node. Every node gets `faults` on its sockets (default = none).
    pub fn start(root: &Path, n: usize, faults: FaultConfig) -> Self {
        Self::start_cfg(root, n, faults, ClusterCfg::default())
    }

    /// As [`start`](Self::start) but with an explicit purge/snapshot posture
    /// (M6 Task 10 purge-churn capstone). The posture is retained so every
    /// restart reuses it.
    pub fn start_cfg(root: &Path, n: usize, faults: FaultConfig, ccfg: ClusterCfg) -> Self {
        let socks: Vec<UdpSocket> =
            (0..n).map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind")).collect();
        let members: Vec<(NodeId, SocketAddr)> =
            socks.iter().enumerate().map(|(i, s)| (i as NodeId, s.local_addr().unwrap())).collect();

        // M8 Task 15: provision crypto material up front (base members plus,
        // for a `spare_node` cluster, the whole spare id block) so every
        // `make_config` call below — including later restarts and the
        // spare's cycle — can look its own `CryptoConfig` up by id.
        let crypto = ccfg
            .crypto
            .then(|| write_crypto_material(root, &crypto_ids_for(n, ccfg.spare_node)));
        let crypto_config_for = |id: NodeId| -> CryptoConfig {
            match &crypto {
                Some(m) => CryptoConfig::Enabled {
                    key_path: m.key_paths[&id].clone(),
                    allowlist_path: m.allowlist_path.clone(),
                    rotation: RotationPolicy::default(),
                },
                None => CryptoConfig::Disabled,
            }
        };

        let mut nodes = Vec::with_capacity(n);
        for (i, sock) in socks.into_iter().enumerate() {
            let addr = members[i].1;
            let instance_dir = root.join(format!("n{i}"));
            let cfg = make_config(
                i as NodeId,
                members.clone(),
                instance_dir.clone(),
                addr,
                faults,
                ccfg,
                crypto_config_for(i as NodeId),
            );
            let node = Node::start_with_socket(cfg, sock).expect("node start");
            // A follower's service follows the committed log too, so every node
            // carries a service from boot — the new leader after a failover
            // already has one attached.
            let service = spawn_service(&instance_dir, ccfg.snapshot_interval_bytes, 0);
            let service1 = spawn_service1::<SM1>(&instance_dir, ccfg);
            nodes.push(NodeSlot {
                id: i as NodeId,
                addr,
                instance_dir,
                node: Some(node),
                service: Some(service),
                service1,
            });
        }
        // M7 Task 10: reserve (bind-then-drop, same tolerance as `rebind`
        // elsewhere in this file) an extra address for the spare, outside the
        // initial member list — voters learn its address dynamically from the
        // replicated CONFIG frame when it's later added (see
        // `uc2_node/tests/reconfig.rs`'s `joining_node_boots_from_stale_seed`),
        // not from their own boot-time `members`.
        let (spare_addr, spare_root) = if ccfg.spare_node {
            let sock = UdpSocket::bind("127.0.0.1:0").expect("bind spare addr");
            let addr = sock.local_addr().unwrap();
            drop(sock);
            (Some(addr), Some(root.join("spare")))
        } else {
            (None, None)
        };
        Self {
            nodes,
            members,
            faults,
            ccfg,
            root: root.to_owned(),
            spare_addr,
            spare_root,
            spare: None,
            spare_phase: SparePhase::Idle,
            next_spare_id: 100,
            config_ops_accepted: 0,
            crypto,
        }
    }

    /// M8 Task 15: this id's `CryptoConfig` — `Enabled` (looked up in the
    /// provisioned material) iff `ClusterCfg::crypto` was set, else
    /// `Disabled`. Used by every (re)boot path — initial start, a leader
    /// restart, and the spare's fresh-id spawn — so a crypto-enabled
    /// cluster stays crypto-enabled across every fault this harness injects.
    fn crypto_config_for(&self, id: NodeId) -> CryptoConfig {
        match &self.crypto {
            Some(m) => CryptoConfig::Enabled {
                key_path: m
                    .key_paths
                    .get(&id)
                    .unwrap_or_else(|| panic!("no provisioned crypto key for id {id} — widen crypto_ids_for"))
                    .clone(),
                allowlist_path: m.allowlist_path.clone(),
                rotation: RotationPolicy::default(),
            },
            None => CryptoConfig::Disabled,
        }
    }

    /// M8 Task 15: the crypto epoch node `node` has MINTED, if any (see
    /// `Node::crypto_epoch`'s doc: leader-only, `None` under
    /// `CryptoConfig::Disabled` or before this node has ever led, `None`
    /// also if the node isn't currently live). The crypto-enabled capstone
    /// variants assert this is `Some` right after the initial election —
    /// proof crypto was genuinely exercised (a real group key minted and
    /// sealing traffic), not merely configured. A build where the `crypto`
    /// switch silently did nothing would still elect a leader and pass
    /// every liveness/linearizability bar; this is the check that catches
    /// that specific failure mode.
    pub fn crypto_epoch_of(&self, node: usize) -> Option<u16> {
        self.nodes[node].node.as_ref().and_then(|n| n.crypto_epoch())
    }

    /// The fixed `(node-id → instance-dir)` map workers route over (index i = id
    /// i). Stable for the whole run: restarts reuse the same dir.
    pub fn dirs(&self) -> Vec<PathBuf> {
        self.nodes.iter().map(|s| s.instance_dir.clone()).collect()
    }

    /// M6 Task 10: the highest archive first-retained position across live nodes —
    /// `> 0` proves purge actually dropped a journal prefix during the run (so the
    /// purge-churn capstone's below-floor reconstruction path was real, not
    /// vacuous). Call before [`stop`](Self::stop).
    pub fn max_archive_first_base(&self) -> u64 {
        self.nodes
            .iter()
            .filter_map(|s| s.node.as_ref().map(|n| n.archive_first_base()))
            .max()
            .unwrap_or(0)
    }

    /// Index of the current serving leader, or `None` in a transient window
    /// (including while the M7 Task 10 spare — a real voting member once
    /// promoted — is the one serving; it has no `self.nodes` index, so
    /// callers using the `let Some(li) = self.leader() else { return }`
    /// idiom already treat that as a harmless no-op).
    pub fn leader(&self) -> Option<usize> {
        (0..self.nodes.len()).find(|&i| self.nodes[i].is_serving_leader())
    }

    /// M7 Task 10: true iff the spare is live and reports itself the sole
    /// serving leader. Always `false` when `ClusterCfg::spare_node` was unset
    /// (`self.spare` stays `None` for every other capstone) — dormant there,
    /// so the `await_*` waiters below behave IDENTICALLY to before this task
    /// for every existing capstone.
    fn spare_is_serving(&self) -> bool {
        self.spare.as_ref().is_some_and(|s| s.is_serving_leader())
    }

    /// M7 Task 10: true while the spare is a full VOTER (`SparePhase::Promoted`,
    /// i.e. after `PromoteLearner` committed and before `DemoteVoter` does) —
    /// a real quorum member `partition_minority`'s `cut()` plumbing (scoped to
    /// `self.nodes`, the original `n`) does not know how to isolate. The
    /// reconfig-churn capstone's fault scheduler skips the partition arm
    /// during this (usually short) window rather than teaching every
    /// partition helper to reason about a DYNAMICALLY-joining/leaving 4th
    /// quorum member — always `false` when `ClusterCfg::spare_node` is unset.
    pub fn spare_is_voting(&self) -> bool {
        self.spare_phase == SparePhase::Promoted
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
            // M7 Task 10: a live, PROMOTED spare is a real voting member and
            // can legitimately win an election when none of the original `n`
            // nodes currently serves. `spare_is_serving()` is always `false`
            // for every other capstone (dormant `self.spare`), so this branch
            // never fires there. `self.nodes.len()` is an out-of-range
            // sentinel index — every call site that uses the return value
            // downstream (e.g. `partition_minority`'s `i != li` follower
            // pick) only compares it, never indexes `self.nodes` with it.
            if serving.is_empty() && self.spare_is_serving() {
                return self.nodes.len();
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
            // M7 Task 10: see `await_single_serving`'s doc — a live promoted
            // spare can be the sole server; dormant for every other capstone.
            if serving.is_empty() && self.spare_is_serving() {
                return self.nodes.len();
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
            // M7 Task 10: see `await_single_serving`'s doc — a live promoted
            // spare can be the sole server; dormant for every other capstone.
            if serving.is_empty() && self.spare_is_serving() {
                return self.nodes.len();
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
        if let Some(s1) = self.nodes[li].service1.take() {
            s1.crash();
        }
        // Survivors re-elect (quorum 2/3 holds).
        self.await_serving_excluding(li, 20);
        // Restart on the persisted dir + same port (static membership): recovers
        // durable state, rejoins in the current term.
        let sock = rebind(addr);
        let crypto = self.crypto_config_for(id);
        let cfg = make_config(id, self.members.clone(), dir.clone(), addr, self.faults, self.ccfg, crypto);
        let node = Node::start_with_socket(cfg, sock).expect("leader node restart");
        let service = spawn_service(&dir, self.ccfg.snapshot_interval_bytes, 0);
        let service1 = spawn_service1::<SM1>(&dir, self.ccfg);
        self.nodes[li].node = Some(node);
        self.nodes[li].service = Some(service);
        self.nodes[li].service1 = service1;
        // Full cluster settles back to a single serving leader.
        self.await_single_serving(20);
    }

    /// Crash the current leader's SERVICE only (the node stays up and leader).
    /// A fresh, EMPTY service reattaches and reconstructs the SM from the log
    /// (Task 9). Submits keep committing on the node during the gap; reads
    /// `RETRY` until the fresh service catches up, then resume.
    /// **Stand in for the production supervisor.** A service incarnation that
    /// hits a fail-stop contract (instance mismatch, or the log-rewind
    /// tripwire) kills its apply thread and stops applying. In production a
    /// supervisor respawns it; in-process nothing did, so the node silently
    /// stopped serving and the death only surfaced at teardown, when
    /// `AgentRunner::stop` re-raised the panic and failed the whole pass.
    /// Respawn every dead service against the same instance dir — the fresh
    /// incarnation reconstructs from the journal, exactly as the supervisor's
    /// would. Returns how many were respawned (0 in a healthy run).
    ///
    /// Call it on the nemesis cadence: it is two atomic loads per node when
    /// everything is alive.
    pub fn supervise_services(&mut self) -> usize {
        let mut respawned = 0;
        for i in 0..self.nodes.len() {
            let dead = self.nodes[i].service.as_ref().is_some_and(|s| !s.is_alive());
            // M14c2: FSM 1 is supervised the same way, INDEPENDENTLY — a
            // healthy sibling is never torn down for the other's death.
            // Always `false` under `FsmSet::Single` (`service1` is `None`), so
            // this loop's behaviour there is unchanged.
            let dead1 = self.nodes[i].service1.as_ref().is_some_and(|s| !s.is_alive());
            if !dead && !dead1 {
                continue;
            }
            let dir = self.nodes[i].instance_dir.clone();
            if dead {
                if let Some(service) = self.nodes[i].service.take() {
                    service.crash(); // drop-joins; swallows the fail-stop panic
                }
                self.nodes[i].service =
                    Some(spawn_service(&dir, self.ccfg.snapshot_interval_bytes, 0));
                respawned += 1;
            }
            if dead1 {
                if let Some(s1) = self.nodes[i].service1.take() {
                    s1.crash();
                }
                self.nodes[i].service1 =
                    Some(spawn_service::<SM1>(&dir, self.ccfg.snapshot_interval_bytes, 1));
                respawned += 1;
            }
        }
        respawned
    }

    pub fn crash_and_restart_leader_service(&mut self) {
        let Some(li) = self.leader() else { return };
        let dir = self.nodes[li].instance_dir.clone();
        if let Some(service) = self.nodes[li].service.take() {
            service.crash();
        }
        if let Some(s1) = self.nodes[li].service1.take() {
            s1.crash();
        }
        let service = spawn_service(&dir, self.ccfg.snapshot_interval_bytes, 0);
        let service1 = spawn_service1::<SM1>(&dir, self.ccfg);
        self.nodes[li].service = Some(service);
        self.nodes[li].service1 = service1;
        // The node never lost quorum, so a serving leader still exists.
        self.await_single_serving(20);
    }

    /// M6 Task 10: crash a random live FOLLOWER's service (the node stays up).
    /// Under the purge-churn posture the leader has purged the log below where the
    /// fresh empty service needs to start, so the node reconstructs it via a
    /// SNAPSHOT INSTALL + tail-replay (Task 5), not plain journal replay — the
    /// below-floor reconstruction path this capstone exists to stress. A no-op if
    /// there is no live follower. Quorum is untouched (a follower's service is not
    /// in any quorum), so the cluster keeps serving throughout.
    pub fn crash_and_restart_random_follower_service(&mut self, rng: &mut StdRng) {
        let Some(li) = self.leader() else { return };
        let followers: Vec<usize> = (0..self.nodes.len())
            .filter(|&i| i != li && self.nodes[i].is_live() && self.nodes[i].service.is_some())
            .collect();
        if followers.is_empty() {
            return;
        }
        let fi = followers[rng.random_range(0..followers.len())];
        let dir = self.nodes[fi].instance_dir.clone();
        if let Some(service) = self.nodes[fi].service.take() {
            service.crash();
        }
        if let Some(s1) = self.nodes[fi].service1.take() {
            s1.crash();
        }
        let service = spawn_service(&dir, self.ccfg.snapshot_interval_bytes, 0);
        let service1 = spawn_service1::<SM1>(&dir, self.ccfg);
        self.nodes[fi].service = Some(service);
        self.nodes[fi].service1 = service1;
        // The leader never lost quorum; a serving leader still exists.
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

    /// Node-first-then-service teardown for every slot (module docs), plus
    /// the spare (M7 Task 10) if a cycle was left in flight.
    pub fn stop(mut self) {
        if let Some(mut spare) = self.spare.take() {
            if let Some(node) = spare.node.take() {
                node.stop();
            }
            if let Some(service) = spare.service.take() {
                service.stop();
            }
            if let Some(s1) = spare.service1.take() {
                s1.stop();
            }
        }
        for s in &mut self.nodes {
            if let Some(node) = s.node.take() {
                node.stop();
            }
            if let Some(service) = s.service.take() {
                service.stop();
            }
            if let Some(s1) = s.service1.take() {
                s1.stop();
            }
        }
    }

    /// M14c2: FSM `id`'s `applied` byte position as published on `node`'s cnc
    /// page — the per-FSM progress every two-FSM capstone samples (an FSM that
    /// never advances is a stalled FSM, whatever the client saw).
    pub fn service_applied(&self, node: usize, id: u8) -> u64 {
        let cnc = CncPage::open_file(&self.nodes[node].instance_dir.join("cnc2.dat"), APP)
            .expect("open cnc");
        cnc.service_slot(id as usize).applied.load_acquire()
    }

    // -------------------------------------------------- M7 Task 10: reconfig

    /// Open `dir`'s cnc page directly — the `uc2ctl` attach path minus the
    /// bin (mirrors `uc2_node/tests/reconfig.rs`'s `open_cnc`).
    fn open_cnc(dir: &Path) -> Arc<CncPage> {
        CncPage::open_file(&dir.join("cnc2.dat"), APP).expect("open cnc")
    }

    /// Write an admin request into `cnc`'s admin slot and poll for its
    /// response (mirrors `reconfig.rs`'s `admin_request`). A timeout here
    /// means the targeted node never answered at all (e.g. it's mid-restart
    /// with no forwarding partner) — a real harness/timing bug worth failing
    /// loudly on, unlike the per-status handling in `random_config_op` below.
    fn admin_request(cnc: &CncPage, op: u32, id: u32, ip: u32, port: u16, secs: u64) -> AdminResp {
        let old_seq = cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0);
        let seq = old_seq + 1;
        let nonce = rand::random::<u64>();
        cnc.write_admin_req(&AdminReq { seq, nonce, op, id, ip, port });
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            if let Some(resp) = cnc.read_admin_resp(seq) {
                return resp;
            }
            assert!(Instant::now() < deadline, "admin response timed out for seq {seq}");
            std::thread::yield_now();
        }
    }

    fn addr_to_wire(addr: SocketAddr) -> (u32, u16) {
        match addr {
            SocketAddr::V4(a) => (u32::from(*a.ip()), a.port()),
            SocketAddr::V6(_) => panic!("this harness only binds IPv4 loopback"),
        }
    }

    /// M7 Task 10: drive the spare's cycle one step at a time —
    /// add-learner -> promote -> demote -> remove-learner — allocating a
    /// FRESH id (monotonic from 100) at the start of every cycle (the
    /// tombstone rule: a removed id can never rejoin, see `uc2_consensus`'s
    /// `ClusterConfig::apply`). Returns whether THIS call committed an admin
    /// op; `false` is a legitimate no-op (a change is already pending, a
    /// learner isn't caught up yet, a transient `Retry`/`ChangePending`, or
    /// the jitter skip below) — the natural pacing that keeps this arm from
    /// swamping the fault scheduler. Never blocks longer than one admin
    /// round-trip (bounded, see `admin_request`).
    ///
    /// Panics if `ClusterCfg::spare_node` wasn't set — calling this on a
    /// cluster with nowhere to put the spare is a harness bug, not a runtime
    /// condition to tolerate.
    pub fn random_config_op(&mut self, rng: &mut StdRng) -> bool {
        let spare_addr = self.spare_addr.expect("random_config_op requires ClusterCfg::spare_node");
        let spare_root = self.spare_root.clone().unwrap();

        // Light jitter: don't fire on every eligible tick, spreading this
        // arm's admin round-trips out a bit even when nothing else gates it.
        if rng.random_bool(0.2) {
            return false;
        }

        // The current leader is USUALLY one of the original `n` nodes, but
        // once the spare is promoted (`SparePhase::Promoted`) it is a real
        // voter and can legitimately WIN an election itself — `self.leader()`
        // only scans `self.nodes`, so fall back to the spare's own cnc when
        // IT is the one reporting itself the sole server (mirrors
        // `spare_is_serving`'s doc: this is a live cluster doing its job, not
        // a stall).
        let leader_dir = match self.leader() {
            Some(li) => self.nodes[li].instance_dir.clone(),
            None if self.spare_is_serving() => {
                self.spare.as_ref().expect("spare_is_serving implies a live spare").instance_dir.clone()
            }
            None => return false,
        };
        let leader_cnc = Self::open_cnc(&leader_dir);

        // The brief's core discipline: never start a new step while the
        // previous one hasn't genuinely committed yet (a bare `status == 0`
        // is only a local/optimistic accept — see `SparePhase`'s doc).
        if leader_cnc.config_pending() != 0 {
            return false;
        }

        match self.spare_phase {
            SparePhase::Idle => {
                let id = self.next_spare_id;
                self.next_spare_id += 1;
                let dir = spare_root.join(format!("id{id}"));
                std::fs::create_dir_all(&dir).expect("spare instance dir");
                let crypto = self.crypto_config_for(id);
                let cfg = make_config(
                    id,
                    self.members.clone(),
                    dir.clone(),
                    spare_addr,
                    self.faults,
                    self.ccfg,
                    crypto,
                );
                let sock = rebind(spare_addr);
                let node = Node::start_with_socket(cfg, sock).expect("spare node start");
                // M14c2: the spare is a FULL node — under `FsmSet::Two` it must
                // boot BOTH declared FSMs or the leader's declared-set check
                // refuses its join.
                let service = spawn_service(&dir, self.ccfg.snapshot_interval_bytes, 0);
                let service1 = spawn_service1::<SM1>(&dir, self.ccfg);
                self.spare = Some(NodeSlot {
                    id,
                    addr: spare_addr,
                    instance_dir: dir,
                    node: Some(node),
                    service: Some(service),
                    service1,
                });
                let (ip, port) = Self::addr_to_wire(spare_addr);
                let resp = Self::admin_request(&leader_cnc, 1 /* AddLearner */, id, ip, port, 10);
                if resp.status == 0 {
                    self.spare_phase = SparePhase::Added;
                    self.config_ops_accepted += 1;
                    true
                } else {
                    // Transient (Retry/ChangePending) or a genuine structural
                    // refusal racing a concurrent fault — abandon this id
                    // (fresh-forever anyway) and let the next call try again.
                    eprintln!("[random_config_op] add-learner {id} not accepted: {resp:?} — retrying later");
                    if let Some(mut slot) = self.spare.take() {
                        if let Some(n) = slot.node.take() {
                            n.stop();
                        }
                        if let Some(s) = slot.service.take() {
                            s.stop();
                        }
                        if let Some(s1) = slot.service1.take() {
                            s1.stop();
                        }
                    }
                    false
                }
            }
            SparePhase::Added => {
                let id = self.spare.as_ref().expect("spare live while Added").id;
                let resp = Self::admin_request(&leader_cnc, 2 /* PromoteLearner */, id, 0, 0, 10);
                match resp.status {
                    0 => {
                        self.spare_phase = SparePhase::Promoted;
                        self.config_ops_accepted += 1;
                        true
                    }
                    // NotCaughtUp (10) / ChangePending (3) / Retry (status 2):
                    // natural, retry on a later call once caught up / settled.
                    1 if matches!(resp.reason, 3 | 10) => false,
                    2 => false,
                    _ => {
                        eprintln!("[random_config_op] promote {id} unexpected refusal: {resp:?} — abandoning cycle");
                        self.abandon_spare();
                        false
                    }
                }
            }
            SparePhase::Promoted => {
                let id = self.spare.as_ref().expect("spare live while Promoted").id;
                let resp = Self::admin_request(&leader_cnc, 3 /* DemoteVoter */, id, 0, 0, 10);
                match resp.status {
                    0 => {
                        self.spare_phase = SparePhase::Demoted;
                        self.config_ops_accepted += 1;
                        true
                    }
                    1 if resp.reason == 3 => false,
                    2 => false,
                    _ => {
                        eprintln!("[random_config_op] demote {id} unexpected refusal: {resp:?} — abandoning cycle");
                        self.abandon_spare();
                        false
                    }
                }
            }
            SparePhase::Demoted => {
                let id = self.spare.as_ref().expect("spare live while Demoted").id;
                let resp = Self::admin_request(&leader_cnc, 4 /* RemoveLearner */, id, 0, 0, 10);
                match resp.status {
                    0 => {
                        // Tombstoned now (forever) — no reason to keep the
                        // process up; tear it down and free the address/dir
                        // for the NEXT cycle's fresh id.
                        self.teardown_spare();
                        self.spare_phase = SparePhase::Idle;
                        self.config_ops_accepted += 1;
                        true
                    }
                    1 if resp.reason == 3 => false,
                    2 => false,
                    _ => {
                        eprintln!("[random_config_op] remove-learner {id} unexpected refusal: {resp:?} — abandoning cycle");
                        self.abandon_spare();
                        false
                    }
                }
            }
        }
    }

    /// Stop the spare's node+service (if any) without touching `spare_phase`
    /// — used by the RemoveLearner success path, where the phase is reset by
    /// the caller right after.
    fn teardown_spare(&mut self) {
        if let Some(mut slot) = self.spare.take() {
            if let Some(n) = slot.node.take() {
                n.stop();
            }
            if let Some(s) = slot.service.take() {
                s.stop();
            }
            if let Some(s1) = slot.service1.take() {
                s1.stop();
            }
        }
    }

    /// An unexpected mid-cycle refusal (racing some OTHER concurrent fault,
    /// e.g. a leader failover reverting the not-yet-committed change this
    /// cycle depended on) — tear the spare down and reset to `Idle` so the
    /// NEXT call starts a brand-fresh cycle rather than retrying a step whose
    /// precondition may no longer hold.
    fn abandon_spare(&mut self) {
        self.teardown_spare();
        self.spare_phase = SparePhase::Idle;
    }
}

// Register-typed probe used by the WGL partition scenarios only. Generic in
// `SM1` (M14c2) so a two-FSM cluster whose FSM 1 is a `Slow`/`Corrupt` wrapper
// still gets the FSM-0 probe.
impl<SM1: SnapshotStateMachine + Default> LinClusterV2<RegisterSm, SM1> {
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
}

// ----------------------------------------------- M14c2: FSM-1 stand-in SMs
//
// Both wrappers delegate to an inner `SM` and are written UFCS
// (`uc2_service::StateMachine::apply(&mut self.0, ..)`), never `self.0.apply(..)`:
// `RawStateMachine` has a blanket impl over every `StateMachine`, so a bare
// method call on a generic inner SM is ambiguous (E0034 — the M14d lesson).

/// FSM 1's stand-in for the slow-FSM oracle: `apply` sleeps `MICROS` then
/// delegates; output identical to `SM`'s, so the equivalence oracle holds.
#[derive(Default)]
pub struct Slow<SM, const MICROS: u64>(pub SM);

impl<SM: uc2_service::StateMachine, const MICROS: u64> uc2_service::StateMachine
    for Slow<SM, MICROS>
{
    type Command = SM::Command;
    type Response = SM::Response;
    type Query = SM::Query;
    type QueryResponse = SM::QueryResponse;
    fn apply(&mut self, position: u64, cmd: SM::Command) -> SM::Response {
        std::thread::sleep(Duration::from_micros(MICROS));
        uc2_service::StateMachine::apply(&mut self.0, position, cmd)
    }
    fn query(&self, q: SM::Query) -> SM::QueryResponse {
        uc2_service::StateMachine::query(&self.0, q)
    }
    fn last_applied(&self) -> Option<u64> {
        uc2_service::StateMachine::last_applied(&self.0)
    }
}

impl<SM: uc2_service::SnapshotStateMachine + uc2_service::StateMachine, const MICROS: u64>
    uc2_service::SnapshotStateMachine for Slow<SM, MICROS>
{
    type SnapshotHandle = SM::SnapshotHandle;
    fn freeze(&self) -> Result<(SM::SnapshotHandle, u64), uc2_service::SnapshotError> {
        self.0.freeze()
    }
    fn stream_snapshot(
        handle: SM::SnapshotHandle,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc2_service::SnapshotError> {
        SM::stream_snapshot(handle, dst)
    }
    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc2_service::SnapshotError> {
        self.0.install_snapshot(position, src)
    }
}

/// An FSM that answers every CAS with the OPPOSITE result — exists only to
/// prove the replication-equivalence oracle bites (`two_fsm_oracle_bites`).
#[derive(Default)]
pub struct Corrupt<SM>(pub SM);

impl uc2_service::StateMachine for Corrupt<RegisterSm> {
    type Command = Cmd;
    type Response = CmdResp;
    type Query = ();
    type QueryResponse = Option<u64>;
    fn apply(&mut self, position: u64, cmd: Cmd) -> CmdResp {
        match uc2_service::StateMachine::apply(&mut self.0, position, cmd) {
            CmdResp::CasResult(b) => CmdResp::CasResult(!b),
            other => other,
        }
    }
    fn query(&self, q: ()) -> Option<u64> {
        uc2_service::StateMachine::query(&self.0, q)
    }
    fn last_applied(&self) -> Option<u64> {
        uc2_service::StateMachine::last_applied(&self.0)
    }
}

impl uc2_service::SnapshotStateMachine for Corrupt<RegisterSm> {
    type SnapshotHandle = <RegisterSm as uc2_service::SnapshotStateMachine>::SnapshotHandle;
    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), uc2_service::SnapshotError> {
        self.0.freeze()
    }
    fn stream_snapshot(
        handle: Self::SnapshotHandle,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc2_service::SnapshotError> {
        RegisterSm::stream_snapshot(handle, dst)
    }
    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc2_service::SnapshotError> {
        self.0.install_snapshot(position, src)
    }
}

/// M14c2 T11: counts `install_snapshot` calls — the observable for "did
/// reconstruction install the newest snapshot artifact, or replay the whole
/// journal instead" (`uc2_service/src/replay.rs:73-78`'s gap guard: install
/// only fires when the journal no longer covers `start_pos`; with purge off
/// the journal always covers it, so reconstruction replays — the M14d run-1
/// lesson `snapshot_restart_installs_only_with_purge` pins in-process).
///
/// Process-global static: this whole test binary is already serialized
/// (`serialize()`), and `snapshot_restart_installs_only_with_purge`
/// (`uc2_node/tests/lin_v2.rs`) is the ONLY test in this binary that wraps an
/// SM in `InstallCounting`. If a second test ever does, it MUST also take
/// `serialize()` around its use of this counter or the two will race.
///
/// **Cluster-global, not leader-only (fix round 1).** All three in-process
/// nodes run `InstallCounting`, so this counts installs across the WHOLE
/// cluster, not just the crashed leader's fresh service. The test asserts
/// `== 1` under purge — if it ever reads `2`, that is NOT test noise: it
/// means a SIBLING service also fell more than one 64 KiB ring behind (under
/// this test's serial, one-command-in-flight load) and independently took
/// the snapshot-install path. Treat a `2` as a genuine finding to
/// investigate (why did a healthy follower's service lag past a full ring
/// under such light load?), not as a flake to retry past.
pub static INSTALLS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// Wraps `RegisterSm`, forwarding every call unchanged except
/// `install_snapshot`, which increments [`INSTALLS`] first.
#[derive(Default)]
pub struct InstallCounting(pub RegisterSm);

impl uc2_service::StateMachine for InstallCounting {
    type Command = Cmd;
    type Response = CmdResp;
    type Query = ();
    type QueryResponse = Option<u64>;
    fn apply(&mut self, position: u64, cmd: Cmd) -> CmdResp {
        uc2_service::StateMachine::apply(&mut self.0, position, cmd)
    }
    fn query(&self, q: ()) -> Option<u64> {
        uc2_service::StateMachine::query(&self.0, q)
    }
    fn last_applied(&self) -> Option<u64> {
        uc2_service::StateMachine::last_applied(&self.0)
    }
}

impl uc2_service::SnapshotStateMachine for InstallCounting {
    type SnapshotHandle = <RegisterSm as uc2_service::SnapshotStateMachine>::SnapshotHandle;
    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), uc2_service::SnapshotError> {
        self.0.freeze()
    }
    fn stream_snapshot(
        handle: Self::SnapshotHandle,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc2_service::SnapshotError> {
        RegisterSm::stream_snapshot(handle, dst)
    }
    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc2_service::SnapshotError> {
        INSTALLS.fetch_add(1, Ordering::Relaxed);
        self.0.install_snapshot(position, src)
    }
}

// ------------------------------------------------------ client op outcomes

#[derive(Debug)]
pub enum SubmitOutcome<R> {
    Ok(R),
    /// May or may not have committed (timed out / answer lapped / gave up
    /// routing) — the WGL "indeterminate mutation".
    Indeterminate,
    /// A genuine harness/wiring bug (bad codec, wrong app/version) — never a
    /// legitimate operational outcome.
    Fatal(String),
}

#[derive(Debug)]
pub enum ReadOutcome<QR> {
    Ok(QR),
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
pub struct WorkerConn {
    dirs: Arc<Vec<PathBuf>>,
    target: usize,
    client: Option<Client>,
}

impl WorkerConn {
    /// The node index this connection currently targets (which node answered
    /// the last successful op) — forensics for the stale-read hunt rig.
    pub fn target(&self) -> usize {
        self.target
    }
    pub fn new(dirs: Arc<Vec<PathBuf>>, start: usize) -> Self {
        // Wrap the start index to handle callers with more workers than cluster nodes
        // (e.g., 4 workers on a 3-node cluster). This mirrors the invariant enforced
        // by `reconnect_to` and `rotate`.
        let target = start % dirs.len();
        Self { dirs, target, client: None }
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
    pub fn drop_client(&mut self) {
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
pub fn submit_cmd<C: serde::Serialize, R: serde::de::DeserializeOwned>(
    conn: &mut WorkerConn,
    cmd: &C,
    deadline: Instant,
) -> SubmitOutcome<R> {
    loop {
        if Instant::now() > deadline {
            return SubmitOutcome::Indeterminate; // gave up routing → in-limbo
        }
        let Some(client) = conn.client() else {
            std::thread::sleep(Duration::from_millis(20));
            conn.rotate();
            continue;
        };
        match client.submit::<C, R>(cmd) {
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
            | Err(e @ ClientError::PayloadTooLarge { .. })
            // M14b: this harness only ever drives FSM 0, which every node
            // declares — naming an undeclared id here is a wiring bug.
            | Err(e @ ClientError::ServiceNotDeclared { .. })
            | Err(e @ ClientError::ShutDown) => return SubmitOutcome::Fatal(format!("{e:?}")),
        }
    }
}

/// M14c2: [`submit_cmd`]'s fan-in twin — one submit, EVERY declared FSM's
/// answer (ascending by service id). The routing/retry discipline is identical
/// (see [`submit_cmd`]'s doc for why only `NotLeader`/`BackpressureFull`/`Retry`
/// may be retried); only the client call and the `Ok` payload differ. The
/// caller splits the returned vector into one per-FSM history.
pub fn submit_all_cmd<C: serde::Serialize, R: serde::de::DeserializeOwned>(
    conn: &mut WorkerConn,
    cmd: &C,
    deadline: Instant,
) -> SubmitOutcome<Vec<(u8, R)>> {
    loop {
        if Instant::now() > deadline {
            return SubmitOutcome::Indeterminate; // gave up routing → in-limbo
        }
        let Some(client) = conn.client() else {
            std::thread::sleep(Duration::from_millis(20));
            conn.rotate();
            continue;
        };
        match client.submit_all::<C, R>(cmd) {
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
            // Maybe-committed → indeterminate, NEVER retried.
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
            | Err(e @ ClientError::PayloadTooLarge { .. })
            // This harness only ever fans in over declared ids, so naming an
            // undeclared one here is a wiring bug.
            | Err(e @ ClientError::ServiceNotDeclared { .. })
            | Err(e @ ClientError::ShutDown) => return SubmitOutcome::Fatal(format!("{e:?}")),
        }
    }
}

/// Linearizable read against the current leader, same routing discipline as
/// [`submit_cmd`]. A `RETRY` (barrier could not confirm within its deadline) is
/// retried while routing; on the overall deadline it is `Indeterminate` (dropped
/// by the checker). An errored read never yields `Ok`, so a lost read simply
/// carries no information.
pub fn read_leader<Q: serde::Serialize, QR: serde::de::DeserializeOwned>(
    conn: &mut WorkerConn,
    q: &Q,
    deadline: Instant,
) -> ReadOutcome<QR> {
    loop {
        if Instant::now() > deadline {
            return ReadOutcome::Indeterminate;
        }
        let Some(client) = conn.client() else {
            std::thread::sleep(Duration::from_millis(20));
            conn.rotate();
            continue;
        };
        match client.query_linearizable::<Q, QR>(q) {
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
            | Err(e @ ClientError::PayloadTooLarge { .. })
            // M14b: FSM 0 only, as in `submit_cmd` above.
            | Err(e @ ClientError::ServiceNotDeclared { .. })
            | Err(e @ ClientError::ShutDown) => return ReadOutcome::Fatal(format!("{e:?}")),
        }
    }
}

/// M14c2: [`read_leader`]'s FSM-`id` twin — same routing/retry discipline,
/// `client.query_linearizable_on::<Q, QR>(id, q)` instead of `query_linearizable`.
pub fn read_leader_on<Q: serde::Serialize, QR: serde::de::DeserializeOwned>(
    conn: &mut WorkerConn,
    id: u8,
    q: &Q,
    deadline: Instant,
) -> ReadOutcome<QR> {
    loop {
        if Instant::now() > deadline {
            return ReadOutcome::Indeterminate;
        }
        let Some(client) = conn.client() else {
            std::thread::sleep(Duration::from_millis(20));
            conn.rotate();
            continue;
        };
        match client.query_linearizable_on::<Q, QR>(id, q) {
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
            | Err(e @ ClientError::PayloadTooLarge { .. })
            | Err(e @ ClientError::ServiceNotDeclared { .. })
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
                let outcome = match submit_cmd::<_, CmdResp>(&mut conn, &Cmd::Write(v), deadline) {
                    SubmitOutcome::Ok(_) => Outcome::Ok(RegResp::Ack),
                    SubmitOutcome::Indeterminate => Outcome::Indeterminate,
                    SubmitOutcome::Fatal(e) => panic!("fatal submit: {e}"),
                };
                history.record(id, Op::Write(v), inv, outcome);
            }
            1 => {
                let inv = history.invoke();
                let outcome = match read_leader::<(), Option<u64>>(&mut conn, &(), deadline) {
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
                let outcome = match submit_cmd::<_, CmdResp>(&mut conn, &Cmd::Cas { old, new }, deadline) {
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

/// M14c2: [`worker`]'s two-FSM twin. Every Write/CAS goes through
/// [`submit_all_cmd`] (fanning in to both FSMs' answers). An unequal pair
/// between FSM 0's and FSM 1's response is a **replication-equivalence
/// violation** — both FSMs replay the identical committed command stream, so
/// their responses must agree; count it in `equiv_failures` and record
/// `Indeterminate` in BOTH histories (never feed the checker a lie about what
/// was observed). An equal pair is recorded with that response in both
/// histories (each stamped with its own `invoke`/`ret` — `History::seq` is
/// per-history). Reads alternate: FSM 0 via [`read_leader`] into `h0`, FSM 1
/// via [`read_leader_on`] into `h1`.
#[allow(clippy::too_many_arguments)]
fn worker2(
    id: u32,
    dirs: Arc<Vec<PathBuf>>,
    h0: Arc<History>,
    h1: Arc<History>,
    equiv_failures: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    mut rng: StdRng,
    last_seen: Arc<AtomicU64>,
    throttle: Duration,
) {
    let mut conn = WorkerConn::new(dirs, id as usize);
    let mut read_fsm1 = false;
    while !stop.load(Ordering::Relaxed) {
        if !throttle.is_zero() {
            std::thread::sleep(throttle);
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        match rng.random_range(0..3u8) {
            0 => {
                let v = rng.random_range(1..1000u64);
                let op = Op::Write(v);
                let (inv0, inv1) = (h0.invoke(), h1.invoke());
                match submit_all_cmd::<_, CmdResp>(&mut conn, &Cmd::Write(v), deadline) {
                    SubmitOutcome::Ok(resp) => {
                        let (a, b) = (&resp[0].1, &resp[1].1);
                        if a != b {
                            equiv_failures.fetch_add(1, Ordering::Relaxed);
                            h0.record(id, op.clone(), inv0, Outcome::Indeterminate);
                            h1.record(id, op, inv1, Outcome::Indeterminate);
                        } else {
                            h0.record(id, op.clone(), inv0, Outcome::Ok(RegResp::Ack));
                            h1.record(id, op, inv1, Outcome::Ok(RegResp::Ack));
                        }
                    }
                    SubmitOutcome::Indeterminate => {
                        h0.record(id, op.clone(), inv0, Outcome::Indeterminate);
                        h1.record(id, op, inv1, Outcome::Indeterminate);
                    }
                    SubmitOutcome::Fatal(e) => panic!("fatal submit_all: {e}"),
                }
            }
            1 => {
                read_fsm1 = !read_fsm1;
                if !read_fsm1 {
                    let inv = h0.invoke();
                    let outcome = match read_leader::<(), Option<u64>>(&mut conn, &(), deadline) {
                        ReadOutcome::Ok(v) => {
                            if let Some(x) = v {
                                last_seen.store(x, Ordering::Relaxed);
                            }
                            Outcome::Ok(RegResp::Value(v))
                        }
                        ReadOutcome::Indeterminate => Outcome::Indeterminate,
                        ReadOutcome::Fatal(e) => panic!("fatal read (fsm0): {e}"),
                    };
                    h0.record(id, Op::Read, inv, outcome);
                } else {
                    let inv = h1.invoke();
                    let outcome = match read_leader_on::<(), Option<u64>>(&mut conn, 1, &(), deadline) {
                        ReadOutcome::Ok(v) => Outcome::Ok(RegResp::Value(v)),
                        ReadOutcome::Indeterminate => Outcome::Indeterminate,
                        ReadOutcome::Fatal(e) => panic!("fatal read (fsm1): {e}"),
                    };
                    h1.record(id, Op::Read, inv, outcome);
                }
            }
            _ => {
                let old = if rng.random_bool(0.7) {
                    last_seen.load(Ordering::Relaxed)
                } else {
                    rng.random_range(1..1000u64)
                };
                let new = rng.random_range(1..1000u64);
                let op = Op::Cas { old, new };
                let (inv0, inv1) = (h0.invoke(), h1.invoke());
                match submit_all_cmd::<_, CmdResp>(&mut conn, &Cmd::Cas { old, new }, deadline) {
                    SubmitOutcome::Ok(resp) => {
                        let (a, b) = (&resp[0].1, &resp[1].1);
                        if a != b {
                            equiv_failures.fetch_add(1, Ordering::Relaxed);
                            h0.record(id, op.clone(), inv0, Outcome::Indeterminate);
                            h1.record(id, op, inv1, Outcome::Indeterminate);
                        } else {
                            match a {
                                CmdResp::CasResult(ok) => {
                                    if *ok {
                                        last_seen.store(new, Ordering::Relaxed);
                                    }
                                    h0.record(id, op.clone(), inv0, Outcome::Ok(RegResp::CasOk(*ok)));
                                    h1.record(id, op, inv1, Outcome::Ok(RegResp::CasOk(*ok)));
                                }
                                other => panic!("cas returned non-cas response: {other:?}"),
                            }
                        }
                    }
                    SubmitOutcome::Indeterminate => {
                        h0.record(id, op.clone(), inv0, Outcome::Indeterminate);
                        h1.record(id, op, inv1, Outcome::Indeterminate);
                    }
                    SubmitOutcome::Fatal(e) => panic!("fatal cas: {e}"),
                }
            }
        }
    }
    conn.drop_client();
}

/// Spawn `n_workers` two-FSM op-driving threads (see [`worker2`]). Same
/// per-worker seeding/starting-node discipline as [`spawn_workers`].
#[allow(clippy::too_many_arguments)]
pub fn spawn_workers2(
    dirs: &Arc<Vec<PathBuf>>,
    h0: &Arc<History>,
    h1: &Arc<History>,
    equiv_failures: &Arc<AtomicU64>,
    stop: &Arc<AtomicBool>,
    last_seen: &Arc<AtomicU64>,
    seed: u64,
    throttle: Duration,
    n_workers: u32,
) -> Vec<JoinHandle<()>> {
    (0..n_workers)
        .map(|w| {
            let rng = StdRng::seed_from_u64(seed ^ (w as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let (dirs, h0, h1, equiv_failures, stop, last_seen) = (
                Arc::clone(dirs),
                Arc::clone(h0),
                Arc::clone(h1),
                Arc::clone(equiv_failures),
                Arc::clone(stop),
                Arc::clone(last_seen),
            );
            std::thread::spawn(move || {
                worker2(w, dirs, h0, h1, equiv_failures, stop, rng, last_seen, throttle)
            })
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

// -------------------------------------------- committed-truncation witness

/// A background sampler that convicts UC of **truncating committed bytes** —
/// the leader-completeness violation the elle `skip-vote-order-check` tooth
/// injects (`scripts/elle_mutation.sh`, tooth 3/3).
///
/// ## Why this exists rather than a crash oracle
///
/// Until 2026-07-30 that tooth was scored purely on the driver exiting
/// non-zero, and what actually produced the non-zero exit was an `uc2-archive`
/// fail-stop: a stale winner opening its term below the archive's cursor
/// corrupted the record walk. That was **issue #6 — a real UC defect** — and
/// `2fd845e` fixed it by routing the leader-open collapse through the archive
/// agent. The injected bug still does its damage afterwards; it simply no
/// longer crashes anything, so the tooth went silent (weekly run 30736463470,
/// 0/5 tries). An oracle that depends on a bug elsewhere expires the day that
/// bug is fixed. This one names the safety property directly.
///
/// ## The predicate: the committed frontier must not vanish from the CLUSTER
///
/// Over `(durable, commit)` read straight off every node's cnc page:
///
/// * `C` = the running MAX of `commit` across all nodes — the furthest position
///   anyone in this cluster has ever considered committed, and therefore acked
///   to a client, applied, and output.
/// * the witness fires when **every** node's `durable` is below `C`: the
///   committed frontier is now held by nobody. If `C` was genuinely committed a
///   majority held it, so every future leader holds it — no node can ever be
///   asked to drop it, let alone all of them. This is the negation of
///   committed-never-truncated, observed.
///
/// **A single node dipping below `C` is deliberately NOT convicted**, and that
/// is the correction that makes this sound. Measurement, 2026-08-02: unmutated
/// control runs regularly show one node's `durable` step backward 17–20 KB
/// below its own commit view (107 such events in one 90 s run) while the other
/// two keep the frontier — a diverged tail being cut, not data lost. An earlier
/// draft of this witness convicted per node and needed an arbitrary byte margin
/// to stay clean, which is exactly the fudge this formulation removes. The
/// injected bug, by contrast, puts **all three** nodes below `C` at once.
///
/// `CONFIRM` consecutive samples are required so a torn read across the
/// three pages (they are sampled in a loop, not atomically) cannot convict.
///
/// ## What a firing does and does not establish (2026-08-04)
///
/// A 452-run fleet hunt fired this once on an UNMUTATED build, and elle ruled
/// that run's history VALID under both `serializable` and `strong-serializable`.
/// The two oracles disagree, and this one has the weaker case: the predicate
/// compares the frontier against `durable`, the RECORDED frontier, so it
/// detects the recorded frontier receding — which a prime does without the
/// bytes going anywhere. The message therefore now reports `append` (what a
/// node HOLDS) beside `durable` and says which case it saw:
///
/// * `append` still covers the frontier -> RECORDED-FRONTIER RECEDED, not loss;
/// * every `append` is below it too -> COMMITTED DATA LOST.
///
/// **The conviction rule is deliberately unchanged for now**: it still fires on
/// either. Narrowing it to the second case is the obvious next step and would
/// also remove a ~0.2 %/run spurious failure of the mutation tooth's CONTROL
/// arm — but only after checking that MUTATED firings really do report loss.
/// Assuming that from the shape of the code is exactly the reasoning that
/// produced two wrong "fixed" calls in this investigation.
///
/// Elle's disagreement is not proof either: that hunt ran `READ_FRAC=0.05` over
/// 64 keys, so its power to OBSERVE a lost append is low. Settling it wants a
/// re-run at real read coverage.
///
/// ## Scope
///
/// Armed by the vote-order pass with the mutation ON *and* OFF: under the
/// control it must never fire, and if it ever does that is a genuine UC bug
/// and failing loudly is the correct outcome. Restricted to passes that do not
/// kill and restart nodes — boot recovery legitimately republishes counters,
/// which this predicate does not model.
pub struct CommittedTruncationWitness {
    stop: Arc<AtomicBool>,
    hit: Arc<Mutex<Option<String>>>,
    handle: Option<JoinHandle<()>>,
}

impl CommittedTruncationWitness {
    /// Consecutive samples that must agree before convicting — see the type
    /// doc's note on torn reads across the per-node pages.
    const CONFIRM: u32 = 3;

    /// Start sampling every node's cnc page at `SAMPLE`. The pages are opened
    /// ONCE (the instance dirs are stable for the life of a pass) so the hot
    /// loop is two atomic loads per node.
    pub fn start(dirs: &[PathBuf]) -> Self {
        const SAMPLE: Duration = Duration::from_millis(20);
        let stop = Arc::new(AtomicBool::new(false));
        let hit = Arc::new(Mutex::new(None));
        let pages: Vec<Arc<CncPage>> =
            dirs.iter().map(|d| Self::open_page(d)).collect::<Option<_>>().unwrap_or_default();
        let (t_stop, t_hit) = (Arc::clone(&stop), Arc::clone(&hit));
        let handle = std::thread::spawn(move || {
            // The furthest position ANY node has ever called committed.
            let mut frontier = 0u64;
            let mut streak = 0u32;
            while !t_stop.load(Ordering::Relaxed) {
                // `commit` FIRST, then `durable`: commit only advances once a
                // quorum's durable has crossed it, so sampling the frontier
                // before the durables can never manufacture a frontier that the
                // durables have not had a chance to reflect. The other order
                // admits a stale-durable / fresh-commit snapshot.
                for page in &pages {
                    frontier = frontier.max(page.counters().commit.load_acquire());
                }
                let durables: Vec<u64> =
                    pages.iter().map(|p| p.counters().durable.load_acquire()).collect();
                // `append` is what a node HOLDS; `durable` only what it has
                // RECORDED. A prime moves `durable` back without the bytes
                // going anywhere, so a receded recorded-frontier is not by
                // itself data loss — if any node's `append` still covers the
                // frontier, the bytes exist and this is a false positive.
                // Sampled here so the report can say which it was; 2026-08-04,
                // a firing whose history elle ruled VALID could not be
                // classified from the message because only `durable` was in it.
                let appends: Vec<u64> =
                    pages.iter().map(|p| p.counters().append.load_acquire()).collect();
                let orphaned = !durables.is_empty() && durables.iter().all(|&d| d < frontier);
                streak = if orphaned { streak + 1 } else { 0 };
                if streak >= Self::CONFIRM {
                    let held = durables.iter().copied().max().unwrap_or(0);
                    let buffered = appends.iter().copied().max().unwrap_or(0);
                    let verdict = if buffered >= frontier {
                        "RECORDED-FRONTIER RECEDED (not loss: a node still HOLDS the \
                         frontier in its buffer — `append` covers it, only `durable` \
                         regressed, e.g. across a prime)"
                    } else {
                        "COMMITTED DATA LOST (no node holds the frontier: every `append` \
                         is below it too)"
                    };
                    // CONVICT ONLY ON GENUINE LOSS. A receded recorded-frontier
                    // is reported for the record but is not a safety violation:
                    // the bytes are still in a node's buffer and `durable`
                    // climbs back. Measured 2026-08-04 on a dedicated host —
                    // mutated firings were LOST 4, RECEDED 0 (one run's race
                    // did not land), by margins of 0.5-2.5 MB, so the tooth
                    // keeps its teeth. This also removes the ~0.2%/run spurious
                    // failure of the tooth's CONTROL arm, which is the same
                    // unmutated workload that fired once in the 452-run fleet
                    // hunt on a history elle ruled VALID.
                    if buffered >= frontier {
                        eprintln!("[trunc-witness] {verdict} — NOT convicted");
                        continue;
                    }
                    let mut slot = t_hit.lock().unwrap();
                    if slot.is_none() {
                        *slot = Some(format!(
                            "{verdict} — committed frontier {frontier}, highest durable {held} \
                             ({} short), highest append {buffered} ({}). \
                             Per-node durable: {durables:?} append: {appends:?}",
                            frontier - held,
                            if buffered >= frontier {
                                format!("covers it by {}", buffered - frontier)
                            } else {
                                format!("{} short", frontier - buffered)
                            }
                        ));
                    }
                }
                std::thread::sleep(SAMPLE);
            }
        });
        Self { stop, hit, handle: Some(handle) }
    }

    /// The witness, if it has fired at any point since `start` (sticky).
    pub fn check(&self) -> Option<String> {
        self.hit.lock().unwrap().clone()
    }

    /// Poll up to `budget`, returning as soon as the witness fires. The stale
    /// campaign this tooth provokes lands AFTER `heal()` returns — and after
    /// `await_reconverged`, which returns immediately because the majority
    /// leader never stopped serving — so the caller has to give the window
    /// explicit time rather than sample once and move on.
    pub fn check_within(&self, budget: Duration) -> Option<String> {
        let deadline = Instant::now() + budget;
        loop {
            if let Some(hit) = self.check() {
                return Some(hit);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn open_page(dir: &Path) -> Option<Arc<CncPage>> {
        CncPage::open_file(&dir.join("cnc2.dat"), APP).ok()
    }
}

impl Drop for CommittedTruncationWitness {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}
