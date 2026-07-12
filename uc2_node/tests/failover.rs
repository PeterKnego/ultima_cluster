// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Networked election proof (plan §Task 9): a real 3-node cluster over loopback
//! UDP, driven only through the public [`Node`] API, exercising the M4 gate
//! claims end to end — single-leader boot, sub-second failover, minority-
//! partition safety, heal-with-truncation, and restart-rejoin.
//!
//! Every node is a real [`Node`] with its four polling agents; partitions are
//! scripted through [`Node::partition_handles`] (send-side, per socket — so a
//! link cut blocks on BOTH endpoints), and correctness is proven by comparing
//! the replayed journal frame streams for byte equality (the NewTerm frames are
//! included in that comparison — they are ordinary frames).
//!
//! ## Sizing / environment choices (binding, documented here)
//!
//! * **Journals live on ext4, not tmpfs.** [`Node`]'s archive preallocates
//!   64 MiB segments; three nodes × several tests would blow the ~2 GiB tmpfs
//!   `/tmp` quota, so the per-test tempdir is created under
//!   `CARGO_TARGET_TMPDIR` (the cargo integration-test scratch, on the ext4
//!   target volume here).
//! * **4 MiB ring, no wrap.** The largest phase writes ~256 KiB of frames; a
//!   4 MiB ring never wraps within a test, so positions are exactly
//!   contiguous: every 96 B submit is one 128 B frame ([`FRAME`]) and the
//!   NewTerm frame is 32 B ([`NEW_TERM`]). That makes commit/durable targets
//!   exact arithmetic rather than guesswork.
//! * **Election timeouts 150–300 ms**, per-node seeds derived from the node
//!   index, so exactly one node times out first on a clean boot.
//! * **The test binary is serialized** via a process-global mutex
//!   ([`serialize`]): five tests × three nodes × four agents is ~60 busy
//!   polling threads, far past the core count, and the sub-second failover
//!   budget is timing-sensitive. Serializing keeps each test on the full box
//!   without weakening any deadline. `cargo test -p uc2_node --test failover`
//!   is therefore effectively single-threaded regardless of `--test-threads`.

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use uc2_consensus::election::NodeId;
use uc2_log::archive::{Archive, ArchiveConfig, ReplayFrame};
use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};

/// On-wire size of a 96 B-payload message frame (32 B header + 96 B, already
/// 32 B-aligned). The harness submits exactly 96 B payloads so each commit step
/// is this many bytes.
const FRAME: u64 = 128;
/// On-wire size of the header-only NewTerm frame that opens a leadership term.
const NEW_TERM: u64 = 32;
/// Payload every submit carries (96 B, see [`FRAME`]).
const PAYLOAD: usize = 96;

// ------------------------------------------------------------ serialization

/// Process-global lock: one failover test runs at a time (see module docs).
static TEST_LOCK: Mutex<()> = Mutex::new(());

/// Acquire the whole-box lock, recovering from a poisoned mutex so one panicking
/// test does not cascade-fail the rest.
fn serialize() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

// ------------------------------------------------------------- node handle

/// One cluster member: its identity/address/dirs plus the live [`Node`] (taken
/// out of the `Option` on stop/crash so the handle survives for replay).
struct NodeH {
    id: NodeId,
    addr: SocketAddr,
    instance_dir: PathBuf,
    /// Derived (`instance_dir/journal`) — kept for the replay-equality checks.
    journal_dir: PathBuf,
    seed: u64,
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
    fn term(&self) -> u32 {
        self.n().current_term()
    }
    fn commit(&self) -> u64 {
        self.n().counters().commit.load_acquire()
    }
    fn append(&self) -> u64 {
        self.n().counters().append.load_acquire()
    }
    fn durable(&self) -> u64 {
        self.n().counters().durable.load_acquire()
    }
    fn truncations(&self) -> u64 {
        self.n().truncations()
    }

    /// Best-effort submit: retry the bounded ingress while `Full`, fail fast on
    /// `NotServing` (the caller decides whether that is expected).
    fn try_submit(&self, payload: Vec<u8>) -> Result<(), uc2_node::SubmitError> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match self.n().submit(payload.clone()) {
                Ok(()) => return Ok(()),
                Err(uc2_node::SubmitError::Full) => {
                    assert!(Instant::now() < deadline, "ingress stayed full");
                    std::thread::yield_now();
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Block this node's outbound sends to `peer` on ALL of its sockets (one
    /// side of a link cut; the caller blocks the other side too).
    fn block(&self, peer: SocketAddr) {
        for h in self.n().partition_handles() {
            h.block(peer);
        }
    }
    /// Heal every partition on this node's sockets.
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
    fn crash(&mut self) {
        if let Some(node) = self.node.take() {
            node.crash();
        }
    }

    /// Restart from the SAME dirs on the SAME port (static membership). The old
    /// socket was dropped on stop; rebinding the loopback port succeeds
    /// immediately for UDP, but a small retry loop absorbs any transient bind
    /// race. Recovers durable state, so it rejoins in the current term.
    fn restart(&mut self, members: &[(NodeId, SocketAddr)]) {
        assert!(self.node.is_none(), "restart of a live node");
        let sock = rebind(self.addr);
        let cfg =
            make_config(self.id, members.to_vec(), self.instance_dir.clone(), self.seed, self.addr);
        self.node = Some(Node::start_with_socket(cfg, sock).expect("restart"));
    }
}

/// Bind a fresh UDP socket on a specific loopback address, retrying briefly.
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

/// Per-node config with the shared harness knobs (see module docs). `bind` is
/// ignored by `start_with_socket` but the field is required.
fn make_config(
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
        app_id: "failover".into(),
        buffer_bytes: 1 << 22, // 4 MiB: no wrap within a test (see module docs)
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed,
        faults: FaultConfig::default(),
        purge: uc2_node::PurgePolicy::Disabled,
        journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
    }
}

/// A distinct, index-derived seed so each node's randomized election timeout
/// (and receiver jitter) differs — a clean boot then elects exactly one leader.
fn seed_for(i: usize) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

// ------------------------------------------------------------- the cluster

struct Cluster {
    /// Kept alive for the whole test: journals/state live under here, and
    /// replay after stop reads back through these paths.
    _dir: tempfile::TempDir,
    members: Vec<(NodeId, SocketAddr)>,
    nodes: Vec<NodeH>,
}

/// Bind every node's socket FIRST (so the full member map is known before any
/// agent runs), then start each node on its pre-bound socket.
fn spawn_cluster(n: usize) -> Cluster {
    let dir = tempfile::Builder::new()
        .prefix("uc2-failover-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");

    // Phase 1: bind all sockets, learn all addresses.
    let socks: Vec<UdpSocket> = (0..n)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind"))
        .collect();
    let members: Vec<(NodeId, SocketAddr)> = socks
        .iter()
        .enumerate()
        .map(|(i, s)| (i as NodeId, s.local_addr().unwrap()))
        .collect();

    // Phase 2: start each node on its own socket with the full member map.
    let mut nodes = Vec::with_capacity(n);
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = members[i].1;
        let instance_dir = dir.path().join(format!("n{i}"));
        let journal_dir = instance_dir.join("journal");
        let seed = seed_for(i);
        let cfg = make_config(i as NodeId, members.clone(), instance_dir.clone(), seed, addr);
        let node = Node::start_with_socket(cfg, sock).expect("start");
        nodes.push(NodeH {
            id: i as NodeId,
            addr,
            instance_dir,
            journal_dir,
            seed,
            node: Some(node),
        });
    }
    Cluster { _dir: dir, members, nodes }
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

/// Spin (yielding) until `f` is true, or panic with `msg` after `secs`.
fn await_until(secs: u64, msg: &str, mut f: impl FnMut() -> bool) {
    let deadline = deadline_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "{msg}");
        std::thread::yield_now();
    }
}

/// Wait for EXACTLY one serving leader across the whole cluster and return its
/// index. Asserts no split-brain (never two serving leaders) throughout.
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

/// Wait until exactly one of `idxs` is serving and return its index (used when
/// some members are crashed/partitioned away and excluded from the check).
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

/// Wait until every live node's `sel` counter reaches `target`.
fn await_all(nodes: &[NodeH], target: u64, secs: u64, what: &str, sel: impl Fn(&NodeH) -> u64) {
    for (i, node) in nodes.iter().enumerate() {
        if node.node.is_none() {
            continue;
        }
        await_until(secs, &format!("{what}: node {i} stuck below {target}"), || sel(node) >= target);
    }
}

/// Wait until a leader has fully quiesced — ingress drained, own log fsynced,
/// and the last frame quorum-committed — and return that stable frontier. Used
/// before a replay comparison so no in-flight submit leaves the followers at
/// divergent positions.
fn await_quiesced(node: &NodeH, secs: u64) -> u64 {
    let deadline = deadline_secs(secs);
    loop {
        let a = node.append();
        if node.durable() == a && node.commit() == a && node.append() == a {
            return a;
        }
        assert!(Instant::now() < deadline, "leader never quiesced (append={a})");
        std::thread::yield_now();
    }
}

/// Submit `n` distinct 96 B payloads through `node` (retrying `Full`).
fn submit_n(node: &NodeH, n: u64) {
    for i in 0..n {
        let mut p = vec![0u8; PAYLOAD];
        p[..8].copy_from_slice(&i.to_le_bytes());
        node.try_submit(p).expect("submit to serving leader");
    }
}

// ------------------------------------------------------------- replay compare

/// Replay a node's whole journal into a frame vector (NewTerm frames included).
/// Uses the default archive config to match the segment layout `Node` wrote.
fn replayed(dir: &Path) -> Vec<ReplayFrame> {
    let arch = Archive::open(ArchiveConfig::new(dir)).expect("open for replay");
    let mut r = arch.replay_from(0).expect("replay");
    let mut out = Vec::new();
    while let Some(f) = r.next().expect("replay frame") {
        out.push(f);
    }
    out
}

/// Assert the replayed frame streams of the given journal dirs are byte-equal.
/// Every node must already be STOPPED (archives dropped) before calling.
fn assert_replay_equal(dirs: &[&PathBuf]) {
    let golden = replayed(dirs[0]);
    assert!(!golden.is_empty(), "golden journal empty");
    for d in &dirs[1..] {
        assert_eq!(replayed(d), golden, "journal {d:?} diverged from {:?}", dirs[0]);
    }
}

/// Cut every link between `a` and `b` (both send directions).
fn partition(a: &NodeH, b: &NodeH) {
    a.block(b.addr);
    b.block(a.addr);
}

// =================================================================== tests

/// Test 1 (+ review addition 6): three nodes from nothing elect exactly one
/// leader in a low term, the serving gate only opens once the NewTerm frame is
/// quorum-committed (commit ≥ base + 32), 1000 messages commit on ALL nodes,
/// and every journal replays byte-identically.
#[test]
fn boot_elects_exactly_one_leader_and_commits() {
    let _g = serialize();
    let mut c = spawn_cluster(3);

    let leader = await_single_leader(&c.nodes, 30);
    // Review addition 6 — serving gate requires quorum commit of the new term:
    // at the moment can_serve first turned true, commit already covers the
    // NewTerm frame's END. Boot term base is 0, so commit ≥ 0 + 32. The quorum
    // round-trip (unlike the single-node smoke) makes this a real 2-of-3 commit.
    assert!(
        c.nodes[leader].commit() >= NEW_TERM,
        "served before the NewTerm frame was quorum-committed (§5.4.2 gate)"
    );
    let term = c.nodes[leader].term();
    assert!((1..=5).contains(&term), "unexpected boot term {term}");

    // The NewTerm frame sits at [0, 32); 1000 data frames follow contiguously.
    let target = NEW_TERM + 1000 * FRAME;
    submit_n(&c.nodes[leader], 1000);
    await_all(&c.nodes, target, 60, "commit", NodeH::commit);
    await_all(&c.nodes, target, 60, "durable", NodeH::durable);

    c.stop_all();
    let dirs: Vec<&PathBuf> = c.nodes.iter().map(|n| &n.journal_dir).collect();
    assert_replay_equal(&dirs);
}

/// Test 2: crash the leader; a survivor must win a fresh election and serve in
/// under one second, with nothing committed lost; new traffic then commits and
/// the survivors' journals stay identical. Prints the measured failover
/// duration (Task 10's gate cites it).
#[test]
fn leader_kill_fails_over_subsecond_without_losing_committed_data() {
    let _g = serialize();
    let mut c = spawn_cluster(3);

    let leader = await_single_leader(&c.nodes, 30);
    let old_term = c.nodes[leader].term();
    let target1 = NEW_TERM + 1000 * FRAME;
    submit_n(&c.nodes[leader], 1000);
    await_all(&c.nodes, target1, 60, "commit", NodeH::commit);

    let survivors: Vec<usize> = (0..3).filter(|&i| i != leader).collect();
    // Committed floor observed by a survivor just before the kill — nothing at
    // or below this may be lost.
    let committed_floor = c.nodes[survivors[0]].commit().min(c.nodes[survivors[1]].commit());
    assert!(committed_floor >= target1);

    // Kill and time the failover: kill → a survivor serves.
    let t0 = Instant::now();
    c.nodes[leader].crash();
    let new_leader = await_serving_among(&c.nodes, &survivors, 30);
    let failover = t0.elapsed();
    println!(
        "FAILOVER kill->serve = {:.3} ms (new leader = node {new_leader}, term {})",
        failover.as_secs_f64() * 1e3,
        c.nodes[new_leader].term()
    );
    assert!(failover < Duration::from_secs(1), "failover {failover:?} exceeded 1 s budget");
    assert!(c.nodes[new_leader].term() > old_term, "new leader did not advance the term");

    // Nothing committed was lost: every survivor still holds the pre-kill floor.
    for &i in &survivors {
        assert!(c.nodes[i].commit() >= committed_floor, "survivor {i} lost committed data");
    }

    // New traffic commits under the new leader; survivors converge and stay
    // journal-identical. The new term added a NewTerm frame, so the durable
    // frontier is at least the old target + that frame + the fresh 1000.
    submit_n(&c.nodes[new_leader], 1000);
    let target2 = target1 + NEW_TERM + 1000 * FRAME;
    for &i in &survivors {
        await_until(60, &format!("survivor {i} commit"), || c.nodes[i].commit() >= target2);
        await_until(60, &format!("survivor {i} durable"), || c.nodes[i].durable() >= target2);
    }

    for &i in &survivors {
        c.nodes[i].stop();
    }
    let dirs: Vec<&PathBuf> = survivors.iter().map(|&i| &c.nodes[i].journal_dir).collect();
    assert_replay_equal(&dirs);
}

/// Build the minority-partition state shared by tests 3 and 4: boot, commit
/// 1000, isolate the leader from both followers, submit `phantom` messages to
/// the (still-believing) old leader, and let the majority elect + commit
/// `fresh` new messages. Returns `(old_leader, new_leader, pre_commit,
/// new_leader_base)`.
fn drive_minority_partition(
    c: &mut Cluster,
    phantom: u64,
    fresh: u64,
) -> (usize, usize, u64, u64) {
    let old = await_single_leader(&c.nodes, 30);
    let pre = NEW_TERM + 1000 * FRAME;
    submit_n(&c.nodes[old], 1000);
    await_all(&c.nodes, pre, 60, "commit", NodeH::commit);

    // Cut the old leader off from both followers (both send directions).
    let followers: Vec<usize> = (0..3).filter(|&i| i != old).collect();
    for &f in &followers {
        partition(&c.nodes[old], &c.nodes[f]);
    }

    // The old leader still believes it serves and accepts writes — these are
    // phantom appends: durable-locally, never quorum-committed.
    for _ in 0..phantom {
        // can_serve stays true until it learns the new term; if it flips mid-run
        // (it adopted a higher term already) we stop — those writes are refused.
        if c.nodes[old].try_submit(vec![0xEE; PAYLOAD]).is_err() {
            break;
        }
    }

    // The majority (the two followers) elects a new leader and commits fresh
    // data. Its base is the committed frontier at partition time (`pre`); it
    // opens term N+1 with a NewTerm frame there, so its serving commit ≥ pre+32.
    let new = await_serving_among(&c.nodes, &followers, 30);
    let new_base = pre; // where the new leader's NewTerm frame sits
    submit_n(&c.nodes[new], fresh);
    let majority_target = new_base + NEW_TERM + fresh * FRAME;
    for &f in &followers {
        await_until(60, &format!("majority node {f} commit"), || {
            c.nodes[f].commit() >= majority_target
        });
    }
    (old, new, pre, new_base)
}

/// Test 3: a minority-partitioned leader cannot manufacture a phantom commit.
/// Its commit stays pinned at the pre-partition point across a settle window
/// while the majority elects and commits fresh data beyond it.
#[test]
fn minority_partitioned_leader_cannot_commit_phantom() {
    let _g = serialize();
    let mut c = spawn_cluster(3);

    let (old, new, pre, _base) = drive_minority_partition(&mut c, 64, 200);

    // Settle window: the isolated old leader's commit must NOT advance past the
    // pre-partition point, even though it kept appending phantom frames.
    let settle = Instant::now() + Duration::from_millis(800);
    while Instant::now() < settle {
        assert_eq!(
            c.nodes[old].commit(),
            pre,
            "isolated old leader manufactured a phantom commit"
        );
        std::thread::yield_now();
    }
    // Its phantom appends are real bytes but uncommitted: append ran ahead of a
    // frozen commit.
    assert!(c.nodes[old].append() > pre, "old leader never took the phantom appends");
    assert_eq!(c.nodes[old].commit(), pre, "old leader commit moved");

    // The majority committed strictly beyond the old leader's frozen commit — no
    // split-brain, no phantom.
    assert!(
        c.nodes[new].commit() > c.nodes[old].commit(),
        "majority did not commit past the isolated leader"
    );

    c.stop_all();
}

/// Test 4 (+ review addition 7): heal the partition. The deposed ex-leader must
/// adopt the higher term, TRUNCATE its uncommitted divergent tail
/// (`truncations() >= 1`), reconcile BEFORE it rejoins the data plane, catch up,
/// and end replay-identical — including both NewTerm frames and none of its
/// phantom appends.
#[test]
fn heal_truncates_divergent_tail_and_reconverges() {
    let _g = serialize();
    let mut c = spawn_cluster(3);

    let phantom = 64u64;
    let fresh = 200u64;
    let (old, new, pre, base) = drive_minority_partition(&mut c, phantom, fresh);
    assert_eq!(base, pre);

    // Ceiling of what the old leader's phantom appends could have made durable.
    let phantom_ceiling = pre + phantom * FRAME;
    assert!(c.nodes[old].truncations() == 0, "premature truncation before heal");

    // Heal every link — into a FULLY IDLE cluster (no submissions).
    for node in &c.nodes {
        node.heal();
    }

    // Task-9 — idle-cluster RECONCILIATION proof (the review's core ask). With
    // ZERO new submissions, the ex-leader must adopt the new term and TRUNCATE
    // its divergent tail (review addition 7: reconciliation precedes rejoining
    // the data plane), driven purely by the new leader's gossip FLOOR re-shipping
    // the term map on its bounded (100ms) cadence. Before this fix `ShipTermMap`
    // rode only the commit-advance cadence, so an idle leader never handed the
    // reconnecting node a map and the divergent tail stayed un-truncated forever
    // — the hole the old submit-per-iteration loop masked. This asserts, in the
    // real node, the SAME property `uc2_sim`'s
    // `idle_cluster_reconciles_divergent_node_via_gossip_floor` pins at the model
    // layer.
    await_until(30, "ex-leader did not reconcile on the idle gossip floor alone", || {
        c.nodes[old].truncations() >= 1
    });
    assert!(c.nodes[old].term() >= c.nodes[new].term(), "old leader did not adopt the new term");

    // DATA-PLANE catch-up — with ZERO new submissions (the review's core ask;
    // previously blocked on a pre-existing `uc2_net` wedge, now fixed). After the
    // truncation above cut the divergence at `base`, the ex-leader catches up to
    // the majority's frontier PURELY via the new leader's gossip floor + the
    // follower NAK/replay path. This exercises the receiver's post-truncation
    // resync: the archive's `LogCounters::prime(to)` regresses the shared
    // `append` counter below the receiver's rebuilt frontier, and the receiver
    // re-syncs its tracker at the top of its duty cycle so the re-shipped DATA
    // below the (stale) frontier is ACCEPTED rather than dropped as a dup —
    // without which `durable` wedged at the truncation point forever.
    let _ = (base, fresh);

    // Full reconvergence to a single, stable frontier: the idle leader is already
    // quiesced (no submissions since heal); capture its committed frontier and
    // wait for every node — including the reconciled ex-leader — to reach that
    // exact position, the precondition for a byte-equal replay.
    let final_target = await_quiesced(&c.nodes[new], 60);
    await_all(&c.nodes, final_target, 60, "reconverge durable", NodeH::durable);
    // The ex-leader carries the majority's term-(N+1) tail, not its truncated
    // phantom tail: its durable climbed strictly PAST the phantom ceiling.
    assert!(
        c.nodes[old].durable() > phantom_ceiling,
        "ex-leader did not carry the majority tail past its phantom ceiling"
    );

    c.stop_all();
    let dirs: Vec<&PathBuf> = (0..3).map(|i| &c.nodes[i].journal_dir).collect();
    // Byte-equal, including BOTH NewTerm frames and none of the phantom appends.
    assert_replay_equal(&dirs);
}

/// Contested first election (M4 final-review I-2): a node wins term 1 and fsyncs
/// its NewTerm frame but is partitioned before any datagram lands; a peer wins
/// term 2 at base 0. On heal, the isolated node must truncate to 0 (a first-block
/// cut) WITHOUT panicking and rejoin; pre-fix this was a fail-stop panic loop on
/// every restart.
///
/// Constructing it means winning a race: `is_leader` flips at the end of
/// `BecomeLeader`, ahead of the sender agent shipping the NewTerm DATA, so we
/// spin on `is_leader` and cut the fresh leader off from both peers the instant
/// it appears — its term-1 NewTerm (recorded to its OWN journal at base 0) then
/// never reaches a follower, the other two time out, and one wins term 2 ALSO at
/// base 0. That race is ~50/50 (the sender can ship first), so we retry the
/// construction: a new leader that opens ABOVE base 0 (its serving commit exceeds
/// the lone NewTerm frame) means the isolated node's frame slipped through and
/// committed — not a first-block cut — so we tear down and rebuild. Once a clean
/// base-0 construction is in hand the outcome is deterministic.
#[test]
fn contested_first_election_first_block_truncation_recovers() {
    let _g = serialize();

    // Bounded retries: the isolate-before-ship race is ~50/50, so P(no clean
    // construction in 24 tries) is < 1e-7.
    const MAX_ATTEMPTS: usize = 24;
    for attempt in 0..MAX_ATTEMPTS {
        let mut c = spawn_cluster(3);

        // Catch the FIRST node to claim leadership and isolate it immediately,
        // before its NewTerm datagram can land on either peer.
        let deadline = deadline_secs(30);
        let leader = loop {
            if let Some(i) = (0..3).find(|&i| c.nodes[i].node.is_some() && c.nodes[i].is_leader()) {
                break i;
            }
            assert!(Instant::now() < deadline, "no node claimed leadership");
            std::thread::yield_now();
        };
        let others: Vec<usize> = (0..3).filter(|&i| i != leader).collect();
        for &o in &others {
            partition(&c.nodes[leader], &c.nodes[o]);
        }
        let old_term = c.nodes[leader].term();

        // The other two elect a new leader. If it opens its term at base 0 its
        // serving commit is exactly the NewTerm frame (`NEW_TERM`); anything
        // larger means the isolated node's term-1 NewTerm replicated + committed
        // before we cut the link — not a first-block cut. Rebuild.
        let new = await_serving_among(&c.nodes, &others, 30);
        assert!(c.nodes[new].term() > old_term, "new leader did not advance the term");
        if c.nodes[new].commit() != NEW_TERM {
            // Lost the race this time — the new term opened above base 0.
            c.stop_all();
            assert!(attempt + 1 < MAX_ATTEMPTS, "no clean base-0 construction in {MAX_ATTEMPTS} tries");
            continue;
        }
        // The term whose NewTerm now sits COMMITTED at base 0. It survives every
        // later election (its data below is committed), so the converged stream's
        // first frame carries exactly this term — strictly above the isolated
        // node's `old_term`, proving that divergent first block was truncated.
        let base0_term = c.nodes[new].term();

        // Clean construction: the new term opened at base 0. Commit a few
        // payloads so it holds a real frontier strictly beyond the NewTerm.
        let fresh = 8u64;
        submit_n(&c.nodes[new], fresh);
        let majority_target = NEW_TERM + fresh * FRAME;
        for &o in &others {
            await_until(30, &format!("majority node {o} commit"), || {
                c.nodes[o].commit() >= majority_target
            });
        }
        assert_eq!(c.nodes[leader].truncations(), 0, "premature truncation before heal");

        // Heal. The isolated ex-leader adopts the new term and truncates its
        // divergent first block (base 0) to 0 — the first-block path — driven by
        // the new leader's idle gossip-floor term-map re-ship. This is the exact
        // cut that pre-fix panicked the archive/consensus agent in a restart loop.
        for node in &c.nodes {
            node.heal();
        }
        await_until(30, "ex-leader did not truncate its divergent first block", || {
            c.nodes[leader].truncations() >= 1
        });
        assert!(c.nodes[leader].term() >= c.nodes[new].term(), "ex-leader did not adopt the new term");

        // Converge (the test completing without a panic IS the core assertion —
        // pre-fix the consensus agent panicked on the first-block cut).
        let final_target = await_quiesced(&c.nodes[new], 30);
        await_all(&c.nodes, final_target, 15, "reconverge commit", NodeH::commit);
        await_all(&c.nodes, final_target, 15, "reconverge durable", NodeH::durable);

        c.stop_all();
        let dirs: Vec<&PathBuf> = (0..3).map(|i| &c.nodes[i].journal_dir).collect();
        assert_replay_equal(&dirs);

        // The converged stream STARTS at position 0 with the base-0 term's
        // NewTerm frame — strictly above the isolated node's `old_term`, proof
        // its term-1 first block was cut to 0 (the first-block path), not shrunk
        // to a non-zero boundary.
        let golden = replayed(dirs[0]);
        assert_eq!(golden[0].position, 0, "converged stream must start at base 0");
        assert_eq!(
            golden[0].header.leadership_term_id, base0_term,
            "first frame must be the base-0 term's NewTerm — the divergent term-1 first block was fully truncated"
        );
        assert!(base0_term > old_term, "base-0 term must exceed the isolated node's term");
        return; // success
    }
}

/// Test 5: gracefully stop a follower, commit more traffic on the quorum, then
/// restart the follower from the SAME dirs on the SAME port. It must rejoin as a
/// follower in the CURRENT term (no spurious election / term bump), catch up,
/// and end replay-identical.
#[test]
fn restarted_follower_recovers_state_and_rejoins() {
    let _g = serialize();
    let mut c = spawn_cluster(3);
    let members = c.members.clone();

    let leader = await_single_leader(&c.nodes, 30);
    let target1 = NEW_TERM + 1000 * FRAME;
    submit_n(&c.nodes[leader], 1000);
    await_all(&c.nodes, target1, 60, "commit", NodeH::commit);
    let term_before = c.nodes[leader].term();

    // Pick a follower and stop it gracefully.
    let follower = (0..3).find(|&i| i != leader).expect("a follower exists");
    c.nodes[follower].stop();

    // The remaining quorum (leader + one follower) commits more traffic.
    submit_n(&c.nodes[leader], 500);
    let target2 = target1 + 500 * FRAME;
    let other = (0..3).find(|&i| i != leader && i != follower).unwrap();
    await_until(60, "leader commit after follower down", || c.nodes[leader].commit() >= target2);
    await_until(60, "quorum follower commit", || c.nodes[other].commit() >= target2);
    // No election happened while it was down: term is unchanged.
    assert_eq!(c.nodes[leader].term(), term_before, "term bumped while a follower was down");

    // Restart from the same dirs on the same port; it recovers durable state and
    // rejoins in the current term.
    c.nodes[follower].restart(&members);
    await_until(60, "restarted follower did not adopt the current term", || {
        c.nodes[follower].term() == term_before
    });
    // Catch up to the frontier committed while it was gone — the leader's replay
    // path may serve it — with NO term bump the whole time.
    await_until(60, "restarted follower did not catch up", || {
        c.nodes[follower].durable() >= target2
    });
    assert_eq!(
        c.nodes[follower].term(),
        term_before,
        "restarted follower forced a spurious election"
    );
    assert!(!c.nodes[follower].is_leader(), "restarted follower unexpectedly became leader");

    c.stop_all();
    let dirs: Vec<&PathBuf> = (0..3).map(|i| &c.nodes[i].journal_dir).collect();
    assert_replay_equal(&dirs);
}
