// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The simulator: a single-threaded, virtual-time discrete-event world driving
//! `n` [`ElectionSm`] instances over a lossy, partitionable, crash-injecting
//! model network. See the crate docs for the binding `(term_map, position)`
//! model; see [`crate::invariants`] for the safety checks run after every event.
//!
//! ## Model at a glance
//!
//! - **Node** = an `ElectionSm` + volatile `{append, commit}` + durable-across-
//!   crash `{durable, vote, term_map}` (the sim IS the `StableValue`) + `up`.
//! - **Time** = `u64` ns; a binary heap of `(time, seq, event)` with `seq`
//!   breaking ties deterministically.
//! - **Messages** mirror the wire; five of them (`Report`, `CommitGossip`,
//!   `RequestVote`, `Vote`, `TermMap`) translate 1:1 into SM events. `Data`
//!   drives replication + data-stamped term recording; `Ack` drives the
//!   leader's per-follower send cursor.
//! - **Faults** are seeded (drop / duplicate / delayed = reordered); partitions
//!   and vote-blackouts are consulted at delivery; crashes are injected on the
//!   crash-rate at each tick.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};

use uc2_consensus::config::{Addr, ClusterConfig};
use uc2_consensus::election::{Action, ElectionConfig, ElectionSm, Event, NodeId, Role};
use uc2_consensus::reconcile::MAX_TERM_MAP_WIRE_ENTRIES;

use crate::invariants::{InvariantChecker, InvariantViolation};

/// Bytes appended per served frame (a fixed 96-B command frame in the model).
const FRAME: u64 = 96;

/// The term covering byte `pos` in an ascending `(term, base)` map — the term of
/// the greatest entry whose base is `<= pos`, or 0 below the first entry. This
/// is the model's byte-content oracle: within a term, bytes are identical
/// cluster-wide (spec §6), so the term at a position IS its content identity.
fn term_at(map: &[(u32, u64)], pos: u64) -> u32 {
    let mut t = 0;
    for &(term, base) in map {
        if base <= pos {
            t = term;
        } else {
            break;
        }
    }
    t
}

/// The next term boundary strictly above `pos` in the map, if any (used to clip
/// a replicated `Data` segment to a single term so the follower stamps it at the
/// correct base).
fn next_boundary(map: &[(u32, u64)], pos: u64) -> Option<u64> {
    map.iter().map(|&(_, b)| b).find(|&b| b > pos)
}

/// Data-plane strength — an EXPLICIT, switchable contract (F1).
///
/// The modes differ in exactly two places (the follower's report clamp and the
/// DATA accept gate); both switch sites carry a pointer back here.
///
/// **`RawM3` reproduces the shipped M3 receiver; `Gated` is a structural
/// clamp that is STRICTLY STRONGER than what `uc2_node` implements; `Mechanism`
/// models the REAL boolean intake-gate discipline `uc2_node` actually runs** —
/// see the phantom-commit trace in the task-5 review for why the shipped
/// receiver is unsafe, and the M4 C-1 review for why `Gated`'s structural clamp
/// was too strong to catch the gate-reopen TOCTOU. The `RawM3` regression tests
/// (`raw_m3_*_is_caught`) and the `mechanism_*` pins keep the oracle honest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataPlane {
    /// A STRUCTURAL clamp — stronger than the shipped node. (a) A follower
    /// reports its AppendPosition clamped to its `matched` current-term frontier
    /// — never its raw durable after a term adoption until reconciliation
    /// re-confirms the bytes. (b) DATA acceptance rejects extensions of a
    /// divergent prefix (the `prevLogTerm`-equivalent check), so a follower never
    /// records a term at a base whose prefix it disagrees on. Together these make
    /// a phantom commit unreachable — but they are stronger than the boolean gate
    /// `uc2_node` runs, so a bug in the gate's OPEN/CLOSE discipline (M4 C-1) is
    /// invisible under `Gated`. That is exactly why [`DataPlane::Mechanism`]
    /// exists.
    Gated,
    /// The REAL M3 receiver's behavior (the shipped `uc2_net` FollowerReceiver).
    /// Reports are the raw durable immediately after a term adoption; DATA is
    /// accepted on position-contiguity + current-term ALONE (no prev-term gate),
    /// so the data-stamp base is wherever the first accepted frame lands even if
    /// the follower's prefix diverges. Reproduces the phantom-commit /
    /// wrong-base-stamp bugs the sim must catch.
    RawM3,
    /// The REAL mechanism `uc2_node` implements: a per-node boolean `intake_gate`
    /// (not a structural clamp). The gate CLOSES on adopting a strictly new term
    /// and re-arms reconciliation; while CLOSED, DATA is dropped and the
    /// AppendPosition report is suppressed ENTIRELY (not clamped). The gate
    /// REOPENS on a clean reconcile (a term-map that needs no truncation) or when
    /// an in-flight truncation's archive ack lands — and, iff `reopen_guard`, the
    /// clean-reconcile reopen is additionally guarded by "no truncation in
    /// flight". When open, the node behaves like [`DataPlane::RawM3`] (raw report,
    /// contiguity-only accept) — the gate is the ONLY protection, exactly as the
    /// binary ships it.
    ///
    /// `reopen_guard: true` mirrors `uc2_node` post-M4 (commit a8d98f4, the C-1
    /// fix). `reopen_guard: false` is the M4-C-1 COUNTERFACTUAL: a duplicate term
    /// map delivered mid-truncation reopens the gate early, the raw divergent
    /// durable (still un-truncated) escapes into the leader's commit ranking, and
    /// the oracle must catch the resulting phantom commit.
    Mechanism { reopen_guard: bool },
}

/// Deterministic xorshift64 — the crate-local RNG (matches the SM's / fault
/// layer's copy so the whole sim is dependency-free and reproducible).
struct XorShift64(u64);

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed })
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

/// Simulation configuration. `Default` yields a quiet 3-node cluster with the
/// SM's own election-timeout defaults; the scenario/fuzz tests layer fault rates
/// on top via struct-update syntax.
#[derive(Clone, Debug)]
pub struct SimConfig {
    pub n_nodes: usize,
    pub seed: u64,
    pub max_steps: u64,
    /// Per-message random drop rate (parts per million), applied at send.
    pub drop_per_million: u32,
    /// Per-message random duplication rate (ppm): a second delayed copy.
    pub dup_per_million: u32,
    /// Per-tick crash-injection rate (ppm): a live node crashes on a hit.
    pub crash_per_million: u32,
    pub tick_interval_ns: u64,
    pub archive_step_ns: u64,
    pub latency_min_ns: u64,
    pub latency_max_ns: u64,
    /// Max bytes an `ArchiveStep` can make durable in one step.
    pub archive_bytes_max: u64,
    pub election_timeout_min_ns: u64,
    pub election_timeout_max_ns: u64,
    /// Idle re-gossip floor (spec §6): the leader re-ships commit + term map on
    /// this cadence even with commit plateaued. Small relative to the fault
    /// timescales so idle reconciliation happens within a run.
    pub gossip_floor_ns: u64,
    /// Data-plane strength (F1). Defaults to [`DataPlane::Gated`] — the Task-7
    /// contract; the `raw_m3_*` regression tests flip it to [`DataPlane::RawM3`].
    pub data_plane: DataPlane,
    /// Archive-truncation latency: the virtual-time gap between an
    /// `Action::Truncate` firing and its `TruncatedFeedback` (the archive slot
    /// ack). Default 0 = instantaneous (the historical behavior; the physical
    /// truncate completes as the very next event). A NON-ZERO window is what lets
    /// the [`DataPlane::Mechanism`] C-1 counterfactual reproduce: a real async
    /// archive truncation takes time, and while it is in flight a newer term can
    /// adopt (re-arming reconcile) and re-ship its map — the exact TOCTOU the
    /// intake-gate reopen guard closes. Only `Mechanism` holds the divergent
    /// durable across this window; the other modes truncate instantly regardless.
    pub truncate_latency_ns: u64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            n_nodes: 3,
            seed: 0,
            max_steps: 20_000,
            drop_per_million: 0,
            dup_per_million: 0,
            crash_per_million: 0,
            tick_interval_ns: 10_000_000,  // 10ms
            archive_step_ns: 5_000_000,    // 5ms
            latency_min_ns: 1_000_000,     // 1ms
            latency_max_ns: 5_000_000,     // 5ms
            archive_bytes_max: 4 * FRAME,
            election_timeout_min_ns: 150_000_000, // 150ms — the SM's own default
            election_timeout_max_ns: 300_000_000, // 300ms
            gossip_floor_ns: 100_000_000,         // 100ms — spec §6 idle re-gossip
            data_plane: DataPlane::Gated,
            truncate_latency_ns: 0, // instantaneous archive ack (historical default)
        }
    }
}

/// Wire messages. `from`/`to` node ids equal node indices (members are `0..n`).
#[derive(Clone, Debug)]
pub enum Msg {
    /// Leader -> follower replication of a single-term byte segment.
    /// - `term` is the leader's current term (drives `LeaderSeen` liveness).
    /// - `seg_term` is the term of the bytes `[from_pos, to_pos)` (one term:
    ///   segments are clipped at term boundaries) — what the follower stamps.
    /// - `prev_term` is the term of the byte at `from_pos - 1` in the leader's
    ///   log (Raft's `prevLogTerm`): the follower accepts only if its own log
    ///   agrees there, so it can never append onto a divergent prefix.
    ///
    /// Accepted iff `term == current`, `from_pos == append` (contiguity), and
    /// the prev-term check passes; else dropped, and the leader retries from the
    /// follower's acked position (backing off on a later tick).
    Data { term: u32, seg_term: u32, from_pos: u64, to_pos: u64, prev_term: u32 },
    /// Follower -> leader replication ack: drives the per-follower send cursor.
    Ack { from: NodeId, term: u32, append: u64 },
    /// Follower -> leader durable report: drives quorum commit ranking.
    Report { from: NodeId, term: u32, durable: u64 },
    /// Leader -> follower commit gossip.
    CommitGossip { term: u32, commit: u64 },
    RequestVote { from: NodeId, new_term: u32, last_term: u32, last_durable: u64 },
    Vote { from: NodeId, term: u32, granted: bool },
    /// Leader -> follower term-map ship (drives reconciliation).
    TermMap { term: u32, entries: Vec<(u32, u64)> },
}

/// A scheduled simulation event.
#[derive(Clone, Debug)]
enum SimEvent {
    Deliver { to: usize, from: usize, msg: Msg },
    Tick { node: usize },
    ArchiveStep { node: usize },
    Restart { node: usize },
    /// Agent feedback for a completed truncation (`Event::Truncated`), scheduled
    /// as the next event to model the latch window. Carries the SM-allocated
    /// `epoch` so the feedback matches the in-flight truncation (M5).
    TruncatedFeedback { node: usize, epoch: u64, to: u64 },
}

/// Heap entry ordered strictly by `(time, seq)` — `seq` is globally unique so
/// the `SimEvent` payload is never compared (no `Ord` needed on `Msg`).
struct Queued {
    time: u64,
    seq: u64,
    ev: SimEvent,
}
impl PartialEq for Queued {
    fn eq(&self, o: &Self) -> bool {
        self.time == o.time && self.seq == o.seq
    }
}
impl Eq for Queued {}
impl PartialOrd for Queued {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Queued {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.time.cmp(&o.time).then(self.seq.cmp(&o.seq))
    }
}

struct Node {
    id: NodeId,
    sm: ElectionSm,
    /// Volatile: highest byte position appended (lost beyond `durable` on crash).
    append: u64,
    /// Durable-across-crash: the fsync'd byte position.
    durable: u64,
    /// Volatile: last certified commit (reset to 0 on crash).
    commit: u64,
    /// Persisted: the vote record (survives restart).
    vote: Option<(u32, NodeId)>,
    /// Persisted: the term map (survives restart; the invariant ground map).
    term_map: Vec<(u32, u64)>,
    up: bool,
    /// Leader: per-peer send cursor (index = peer id; self slot unused).
    cursors: Vec<u64>,
    /// Follower: replication frontier confirmed byte-consistent with the current
    /// leader (Raft's `matchIndex`) — reset on adopting a new term, advanced only
    /// by a prev-term-checked `Data` accept. The follower reports
    /// `min(durable, matched)`, so a divergent-but-un-truncated durable is never
    /// counted toward commit.
    matched: u64,
    /// Best-known current leader (to address Report/Ack to).
    leader_hint: Option<usize>,
    /// Leader: position of this term's NewTerm no-op frame, once appended.
    new_term_pos: Option<u64>,
    /// A truncation is in flight (data plane paused until `TruncatedFeedback`).
    /// Doubles as the C-1 guard's `pending_truncation.is_some()` marker under
    /// [`DataPlane::Mechanism`].
    truncating: bool,
    /// Snapshot captured when a `TermMap` is delivered, so a resulting
    /// `Truncate` can be checked against the pre-reconcile map + leader map.
    map_before_reconcile: Vec<(u32, u64)>,
    last_leader_map: Vec<(u32, u64)>,
    // ---- intake-gate model ([`DataPlane::Mechanism`] only; ignored otherwise) ----
    /// The real receiver's boolean intake gate: `true` = OPEN (DATA accepted,
    /// AppendPosition reported), `false` = CLOSED. Mirrors `uc2_node`'s
    /// `intake_gate: Arc<AtomicBool>` — the ONLY divergence protection the binary
    /// ships (weaker than `Gated`'s structural clamp).
    intake_gate: bool,
    /// Shadow of the SM's adopted term (`uc2_node::adopted_term`): the gate closes
    /// only on a STRICTLY new term, so we must know the last term we adopted.
    adopted_term: u32,
    /// Under `Mechanism`, the physical truncation is DEFERRED across the archive
    /// latency window: the divergent durable tail stays on disk (the closed gate
    /// suppresses reporting it) until `TruncatedFeedback` applies this target.
    /// This is what lets the C-1 counterfactual expose the still-present divergent
    /// durable when the gate reopens early.
    pending_trunc_to: Option<u64>,
}

/// The simulator. Construct with [`World::new`], drive with [`World::run`] (or
/// the scripted hooks), read out a [`Stats`].
pub struct World {
    cfg: SimConfig,
    nodes: Vec<Node>,
    queue: BinaryHeap<Reverse<Queued>>,
    rng: XorShift64,
    seq: u64,
    now: u64,
    steps: u64,
    checker: InvariantChecker,

    // faults / scripting.
    // DETERMINISM GUARD: these HashSets are ONLY ever probed with `.contains()`
    // (see `blocked`) — never iterated. A future `.iter()`/`.drain()` over them
    // would leak the platform's hash-random iteration order into event ordering
    // and break reproducibility; if you need to enumerate them, switch to a
    // BTreeSet or collect+sort first.
    isolated: HashSet<usize>,
    blocked_pairs: HashSet<(usize, usize)>,
    vote_drop_until: u64,
    crash_on_truncate: bool,
    /// When set, a serving leader stops appending NEW frames — modeling a client
    /// that has stopped submitting. The leader still heartbeats its existing tail
    /// and re-gossips commit + term map on the idle floor, so commit PLATEAUS.
    quiet: bool,

    // stats
    stat_leaders: u32,
    stat_truncations: u32,
    stat_wipes: u32,
    stat_restarts: u32,
}

/// Read-out of a completed run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stats {
    pub leaders_elected: u32,
    pub max_commit: u64,
    pub truncations: u32,
    /// M6 Task 8: wipe-and-rejoins (NoCommonPrefix → truncate-to-0). A subset of
    /// `truncations`.
    pub wipes: u32,
    pub restarts: u32,
    pub steps: u64,
}

impl World {
    pub fn new(cfg: SimConfig) -> Self {
        assert!(cfg.n_nodes >= 1, "n_nodes must be >= 1");
        assert!(cfg.election_timeout_min_ns <= cfg.election_timeout_max_ns);
        assert!(cfg.latency_min_ns <= cfg.latency_max_ns);
        let n = cfg.n_nodes;
        let mut nodes = Vec::with_capacity(n);
        for id in 0..n {
            let ecfg = Self::election_cfg(&cfg, id);
            let sm = ElectionSm::new(ecfg, None, &[], 0, 0);
            nodes.push(Node {
                id: id as NodeId,
                sm,
                append: 0,
                durable: 0,
                commit: 0,
                vote: None,
                term_map: Vec::new(),
                up: true,
                cursors: vec![0; n],
                matched: 0,
                leader_hint: None,
                new_term_pos: None,
                truncating: false,
                map_before_reconcile: Vec::new(),
                last_leader_map: Vec::new(),
                intake_gate: true, // open until a term is adopted
                adopted_term: 0,
                pending_trunc_to: None,
            });
        }
        let checker = InvariantChecker::new(cfg.seed, n);
        let mut w = World {
            rng: XorShift64::new(cfg.seed ^ 0xD1B5_4A32_D192_ED03),
            queue: BinaryHeap::new(),
            seq: 0,
            now: 0,
            steps: 0,
            checker,
            isolated: HashSet::new(),
            blocked_pairs: HashSet::new(),
            vote_drop_until: 0,
            crash_on_truncate: false,
            quiet: false,
            stat_leaders: 0,
            stat_truncations: 0,
            stat_wipes: 0,
            stat_restarts: 0,
            nodes,
            cfg,
        };
        // Seed the periodic timers, staggered by 1ns so identical-time ties are
        // still broken by `seq` deterministically.
        for id in 0..w.cfg.n_nodes {
            w.push(SimEvent::Tick { node: id }, id as u64);
            w.push(SimEvent::ArchiveStep { node: id }, w.cfg.archive_step_ns + id as u64);
        }
        w
    }

    fn election_cfg(cfg: &SimConfig, node: usize) -> ElectionConfig {
        // Genesis config: every node a voter, synthetic addr `(node_idx, 1)` — the
        // sim never opens sockets, so the addr is a dep-free placeholder (M7).
        let voters: Vec<(NodeId, Addr)> = (0..cfg.n_nodes as NodeId)
            .map(|id| (id, (id, 1)))
            .collect();
        ElectionConfig {
            id: node as NodeId,
            config: ClusterConfig::genesis(voters, Vec::new()),
            config_position: 0,
            election_timeout_min_ns: cfg.election_timeout_min_ns,
            election_timeout_max_ns: cfg.election_timeout_max_ns,
            gossip_floor_ns: cfg.gossip_floor_ns,
            // Distinct per-node seeds so timeouts spread (avoids lockstep splits).
            seed: cfg.seed ^ 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(node as u64 + 1),
        }
    }

    // ---------------------------------------------------------------- run loop

    /// Run to the step budget (or an empty queue, or the term-map wire cap).
    /// Returns the run [`Stats`], or the first [`InvariantViolation`].
    pub fn run(&mut self) -> Result<Stats, InvariantViolation> {
        while self.steps < self.cfg.max_steps {
            if self.term_map_cap_reached() {
                break;
            }
            if !self.step_once()? {
                break;
            }
        }
        Ok(self.stats())
    }

    /// Step until a serving leader exists (or the budget runs out).
    pub fn run_until_leader(&mut self) -> Result<(), InvariantViolation> {
        while self.current_leader().is_none() && self.steps < self.cfg.max_steps {
            if self.term_map_cap_reached() || !self.step_once()? {
                break;
            }
        }
        Ok(())
    }

    /// Step until `pred` holds (or the budget runs out).
    pub fn run_until(
        &mut self,
        mut pred: impl FnMut(&World) -> bool,
    ) -> Result<(), InvariantViolation> {
        while !pred(self) && self.steps < self.cfg.max_steps {
            if self.term_map_cap_reached() || !self.step_once()? {
                break;
            }
        }
        Ok(())
    }

    /// Step at most `k` events (bounded also by the global budget).
    pub fn run_steps(&mut self, k: u64) -> Result<(), InvariantViolation> {
        let target = self.steps.saturating_add(k);
        while self.steps < target && self.steps < self.cfg.max_steps {
            if self.term_map_cap_reached() || !self.step_once()? {
                break;
            }
        }
        Ok(())
    }

    /// Pop and process one event, then run the post-event invariant sweep.
    /// Returns `false` when the queue is empty.
    fn step_once(&mut self) -> Result<bool, InvariantViolation> {
        let Some(Reverse(q)) = self.queue.pop() else {
            return Ok(false);
        };
        self.now = q.time;
        self.steps += 1;
        let step = self.steps;
        self.handle(q.ev, q.time, step)?;
        // Invariant 2 (prefix consistency) after EVERY event. Invariants 1/3/4/5
        // are point-in-time and checked inline at the exact triggering action
        // (`BecomeLeader` / `AdvanceCommit` / `Truncate`) rather than in a
        // post-event sweep.
        //
        // DOCUMENTED DEVIATION from the brief's "after every event" wording, and
        // equivalent-or-stricter: these invariants are properties OF the action
        // (an election, a commit, a truncation), so checking them at the instant
        // the action fires catches the exact same violations a post-event sweep
        // would — and catches them mid-event, before any follow-up action in the
        // same feed can paper over the offending state. Nothing mutates the
        // checked quantities between the action and the sweep, so no violation
        // can slip through the gap.
        let maps: Vec<Vec<(u32, u64)>> = self.nodes.iter().map(|n| n.term_map.clone()).collect();
        self.checker.check_prefix_consistency(&maps, step)?;
        Ok(true)
    }

    /// True once any node's persisted term map nears the wire cap. Beyond this,
    /// a shipped map could slide its base-0 entry off the 64-entry window and
    /// spuriously surface `NoCommonPrefix` (`Fatal`) — a wire limitation, not a
    /// safety bug. Per the brief we cap the run rather than provoke it.
    fn term_map_cap_reached(&self) -> bool {
        self.nodes.iter().any(|n| n.term_map.len() >= MAX_TERM_MAP_WIRE_ENTRIES - 2)
    }

    fn stats(&self) -> Stats {
        Stats {
            leaders_elected: self.stat_leaders,
            max_commit: self.checker.global_max_commit,
            truncations: self.stat_truncations,
            wipes: self.stat_wipes,
            restarts: self.stat_restarts,
            steps: self.steps,
        }
    }

    // ------------------------------------------------------------- event queue

    fn draw(&mut self) -> u64 {
        self.rng.next_u64()
    }

    /// Raise a node's committed high-water from GENUINE byte-content ground truth
    /// (F3): the frontier up to which it durably holds bytes identical to the
    /// committed lineage, capped at the genuine global commit. Derived by the
    /// checker from real `(durable, term_map)` state — NOT the model's `matched`
    /// — so the bound is identical whether the data plane is `Gated` or `RawM3`.
    /// Called wherever a node's durable/content advances (commit / accept / fsync)
    /// and for every node whenever the lineage advances.
    fn record_committed(&mut self, node: usize) {
        let durable = self.nodes[node].durable;
        let map = self.nodes[node].term_map.clone();
        self.checker.record_held_content(node, durable, &map);
    }

    fn push(&mut self, ev: SimEvent, time: u64) {
        let seq = self.seq;
        self.seq += 1;
        self.queue.push(Reverse(Queued { time, seq, ev }));
    }

    /// Enqueue a message send with seeded drop / delay / duplication. Partition
    /// and vote-blackout are consulted at *delivery* (they can change between
    /// send and arrival), so they are not applied here.
    fn send(&mut self, from: usize, to: usize, msg: Msg, now: u64) {
        if self.cfg.drop_per_million > 0
            && self.draw() % 1_000_000 < self.cfg.drop_per_million as u64
        {
            return;
        }
        let span = self.cfg.latency_max_ns - self.cfg.latency_min_ns + 1;
        let lat = self.cfg.latency_min_ns + self.draw() % span;
        let will_dup = self.cfg.dup_per_million > 0
            && self.draw() % 1_000_000 < self.cfg.dup_per_million as u64;
        if will_dup {
            let lat2 = self.cfg.latency_min_ns + self.draw() % span;
            self.push(SimEvent::Deliver { to, from, msg: msg.clone() }, now + lat);
            self.push(SimEvent::Deliver { to, from, msg }, now + lat2);
        } else {
            self.push(SimEvent::Deliver { to, from, msg }, now + lat);
        }
    }

    /// Build a `Data` message from leader `node`'s log for the byte range
    /// starting at `from`, clipped to a single term segment (so the follower
    /// stamps it at the correct base) and carrying the Raft `prev_term`.
    fn make_data(&self, node: usize, from: u64, append: u64, term: u32) -> Msg {
        let map = &self.nodes[node].term_map;
        let to = match next_boundary(map, from) {
            Some(b) if b < append => b,
            _ => append,
        };
        let seg_term = term_at(map, from);
        let prev_term = if from == 0 { 0 } else { term_at(map, from - 1) };
        Msg::Data { term, seg_term, from_pos: from, to_pos: to, prev_term }
    }

    fn blocked(&self, a: usize, b: usize) -> bool {
        self.isolated.contains(&a)
            || self.isolated.contains(&b)
            || self.blocked_pairs.contains(&(a.min(b), a.max(b)))
    }

    fn vote_blocked(&self, msg: &Msg) -> bool {
        self.now < self.vote_drop_until
            && matches!(msg, Msg::RequestVote { .. } | Msg::Vote { .. })
    }

    // ------------------------------------------------------------- dispatch

    fn handle(&mut self, ev: SimEvent, now: u64, step: u64) -> Result<(), InvariantViolation> {
        match ev {
            SimEvent::Tick { node } => self.on_tick(node, now, step),
            SimEvent::ArchiveStep { node } => self.on_archive(node, now, step),
            SimEvent::Restart { node } => self.on_restart(node, now),
            SimEvent::TruncatedFeedback { node, epoch, to } => {
                self.on_truncated_feedback(node, epoch, to, now, step)
            }
            SimEvent::Deliver { to, from, msg } => {
                if !self.nodes[to].up || self.blocked(from, to) || self.vote_blocked(&msg) {
                    return Ok(());
                }
                self.deliver(to, from, msg, now, step)
            }
        }
    }

    fn on_tick(&mut self, node: usize, now: u64, step: u64) -> Result<(), InvariantViolation> {
        // Crash injection rides the tick cadence.
        if self.nodes[node].up
            && self.cfg.crash_per_million > 0
            && self.draw() % 1_000_000 < self.cfg.crash_per_million as u64
        {
            self.do_crash(node, now);
            self.push(SimEvent::Tick { node }, now + self.cfg.tick_interval_ns);
            return Ok(());
        }
        if !self.nodes[node].up {
            self.push(SimEvent::Tick { node }, now + self.cfg.tick_interval_ns);
            return Ok(());
        }

        // Drive the SM clock (timeouts, leader ranking).
        self.feed(node, Event::Tick { now_ns: now }, now, step)?;

        // Agent duty: a serving leader appends a frame; any leader (re)ships the
        // outstanding tail to each follower (also the liveness heartbeat).
        if matches!(self.nodes[node].sm.role(), Role::Leader) && !self.nodes[node].truncating {
            if self.nodes[node].sm.can_serve() && !self.quiet {
                self.nodes[node].append += FRAME;
            }
            let ap = self.nodes[node].append;
            let term = self.nodes[node].sm.current_term();
            for p in 0..self.cfg.n_nodes {
                if p == node {
                    continue;
                }
                let from = self.nodes[node].cursors[p].min(ap);
                let msg = self.make_data(node, from, ap, term);
                self.send(node, p, msg, now);
            }
        }

        self.push(SimEvent::Tick { node }, now + self.cfg.tick_interval_ns);
        Ok(())
    }

    fn on_archive(&mut self, node: usize, now: u64, step: u64) -> Result<(), InvariantViolation> {
        if self.nodes[node].up && !self.nodes[node].truncating {
            let nd = &self.nodes[node];
            if nd.append > nd.durable {
                let want = 1 + self.draw() % self.cfg.archive_bytes_max;
                let new_durable = (self.nodes[node].durable + want).min(self.nodes[node].append);
                self.nodes[node].durable = new_durable;
                self.record_committed(node);
                self.feed(node, Event::DurableAdvanced { durable: new_durable }, now, step)?;
                // A follower reports its fresh durable to the current leader —
                // but only up to `matched` (the frontier confirmed consistent
                // with this leader). Never report divergent bytes it has fsync'd
                // but not yet re-confirmed/truncated, or the leader would commit
                // them.
                if let Some(leader) = self.nodes[node].leader_hint
                    && leader != node
                {
                    let (id, term) = (self.nodes[node].id, self.nodes[node].sm.current_term());
                    // F1a data-plane contract switch — see `DataPlane`.
                    //   Gated     (structural clamp): clamp to `matched`, so a
                    //             divergent-but-un-truncated durable is never
                    //             counted toward commit.
                    //   RawM3     (shipped M3 receiver): report the raw durable —
                    //             the phantom-commit source the oracle must catch.
                    //   Mechanism (real intake gate): raw durable, but the report
                    //             is SUPPRESSED ENTIRELY while the gate is closed
                    //             (handled below) — never clamped.
                    let reportable = match self.cfg.data_plane {
                        DataPlane::Gated => new_durable.min(self.nodes[node].matched),
                        DataPlane::RawM3 | DataPlane::Mechanism { .. } => new_durable,
                    };
                    let suppressed = matches!(self.cfg.data_plane, DataPlane::Mechanism { .. })
                        && !self.nodes[node].intake_gate;
                    if !suppressed {
                        self.send(
                            node,
                            leader,
                            Msg::Report { from: id, term, durable: reportable },
                            now,
                        );
                    }
                }
            }
        }
        // Mechanism C-1 CONTINUOUS LEAK: an erroneously-OPEN intake gate during a
        // truncation ships the stale (un-re-primed) AppendPosition = the raw
        // divergent durable, on the archive cadence, until the ack finally
        // re-primes the counters. The guarded mechanism keeps the gate CLOSED
        // throughout a truncation, so this can only fire in the `reopen_guard:
        // false` counterfactual (after a duplicate term map reopened the gate
        // mid-truncation). The single reopen-time report is one datagram; a real
        // receiver keeps shipping it — which is what lets the leader actually rank
        // the divergent durable into a phantom commit.
        if self.nodes[node].up
            && matches!(self.cfg.data_plane, DataPlane::Mechanism { .. })
            && self.nodes[node].truncating
            && self.nodes[node].intake_gate
            && let Some(leader) = self.nodes[node].leader_hint
            && leader != node
        {
            let (id, term, durable) =
                (self.nodes[node].id, self.nodes[node].sm.current_term(), self.nodes[node].durable);
            self.send(node, leader, Msg::Report { from: id, term, durable }, now);
        }
        self.push(SimEvent::ArchiveStep { node }, now + self.cfg.archive_step_ns);
        Ok(())
    }

    fn on_restart(&mut self, node: usize, now: u64) -> Result<(), InvariantViolation> {
        if self.nodes[node].up {
            return Ok(()); // already recovered
        }
        let cfg = Self::election_cfg(&self.cfg, node);
        let vote = self.nodes[node].vote;
        let term_map = self.nodes[node].term_map.clone();
        let durable = self.nodes[node].durable;
        let sm = ElectionSm::new(cfg, vote, &term_map, durable, now);
        let nd = &mut self.nodes[node];
        nd.sm = sm;
        nd.up = true;
        nd.append = durable;
        nd.commit = 0;
        nd.truncating = false;
        nd.new_term_pos = None;
        nd.leader_hint = None;
        nd.matched = 0;
        for c in &mut nd.cursors {
            *c = 0;
        }
        // Rebuilt receiver: gate open, no reconcile armed, adopted term = the
        // term the persisted vote restored (mirrors uc2_node boot).
        nd.intake_gate = true;
        nd.adopted_term = nd.sm.current_term();
        nd.pending_trunc_to = None;
        self.checker.on_restart(node);
        self.stat_restarts += 1;
        Ok(())
    }

    fn on_truncated_feedback(
        &mut self,
        node: usize,
        epoch: u64,
        to: u64,
        now: u64,
        step: u64,
    ) -> Result<(), InvariantViolation> {
        if !self.nodes[node].up {
            return Ok(());
        }
        self.feed(node, Event::Truncated { epoch, to }, now, step)?;
        // The SM has adopted its pending map; mirror it into our persisted store.
        let m = self.nodes[node].sm.term_map().to_vec();
        let nd = &mut self.nodes[node];
        nd.term_map = m;
        nd.truncating = false;
        // Intake gate (Mechanism): the archive ack completes reconciliation. Apply
        // the deferred physical truncation (durable was held at the divergent value
        // across the window), then reopen the gate — UNLESS a newer term adopted
        // mid-truncation re-armed the reconcile latch, in which case that term's
        // fresh reconcile must complete first (mirrors uc2_node::on_truncated).
        if matches!(self.cfg.data_plane, DataPlane::Mechanism { .. }) {
            if let Some(t) = nd.pending_trunc_to.take() {
                nd.durable = t;
                nd.append = t;
            }
            self.record_committed(node);
            // Reconciliation for this term is complete (durable clamped to the
            // consistent truncation point, pruned map adopted): reopen the gate.
            self.reopen_gate(node, now);
        }
        Ok(())
    }

    /// Reopen a node's intake gate ([`DataPlane::Mechanism`]) and resume
    /// reporting: ship the node's CURRENT durable to its leader. In the guarded
    /// flow the gate only reopens once reconciliation is clean (durable already
    /// truncated/consistent), so this reports a SAFE position. Under the unguarded
    /// C-1 flow the gate reopens with a truncation still in flight, so the RAW
    /// divergent durable — not yet truncated — escapes into the leader's commit
    /// ranking, and the genuine-quorum oracle (inv5) catches the phantom.
    fn reopen_gate(&mut self, node: usize, now: u64) {
        self.nodes[node].intake_gate = true;
        if let Some(leader) = self.nodes[node].leader_hint
            && leader != node
        {
            let (id, term, durable) = (
                self.nodes[node].id,
                self.nodes[node].sm.current_term(),
                self.nodes[node].durable,
            );
            self.send(node, leader, Msg::Report { from: id, term, durable }, now);
        }
    }

    // ------------------------------------------------------------- delivery

    fn deliver(
        &mut self,
        to: usize,
        from: usize,
        msg: Msg,
        now: u64,
        step: u64,
    ) -> Result<(), InvariantViolation> {
        match msg {
            Msg::Data { term, seg_term, from_pos, to_pos, prev_term } => {
                self.nodes[to].leader_hint = Some(from);
                self.feed(to, Event::LeaderSeen { term }, now, step)?;
                if self.nodes[to].truncating {
                    return Ok(()); // data plane paused during truncation
                }
                let cur = self.nodes[to].sm.current_term();
                if term == cur {
                    // Contiguity AND prev-term agreement (Raft): our log must
                    // match the leader's at `from_pos - 1`, else appending here
                    // would extend a divergent prefix. A divergent tail is only
                    // removed by term-map reconciliation (Truncate), never by
                    // silently overwriting it — so we simply drop until then.
                    // F1b data-plane contract switch — see `DataPlane`.
                    //   Gated     (structural clamp): require prev-term agreement,
                    //             so a divergent prefix can never be extended (a
                    //             follower never stamps a term at a base whose
                    //             prefix it disagrees on).
                    //   RawM3     (shipped M3 receiver): accept on position-
                    //             contiguity + current-term ALONE — the wrong-base-
                    //             stamp source.
                    //   Mechanism (real intake gate): same contiguity-only accept
                    //             as RawM3, but ONLY while the gate is open; a
                    //             closed gate drops the DATA outright (below).
                    let ok_prev = match self.cfg.data_plane {
                        DataPlane::Gated => {
                            from_pos == 0
                                || term_at(&self.nodes[to].term_map, from_pos - 1) == prev_term
                        }
                        DataPlane::RawM3 | DataPlane::Mechanism { .. } => true,
                    };
                    let gate_open = match self.cfg.data_plane {
                        DataPlane::Mechanism { .. } => self.nodes[to].intake_gate,
                        _ => true,
                    };
                    if gate_open && from_pos == self.nodes[to].append && ok_prev {
                        if to_pos > from_pos {
                            self.nodes[to].append = to_pos;
                            // Data-stamped recording at the term's true base
                            // (contiguity guarantees first observation lands at
                            // the boundary); the SM is idempotent below its last.
                            self.feed(
                                to,
                                Event::DataTermObserved { term: seg_term, base: from_pos },
                                now,
                                step,
                            )?;
                        }
                        // Confirmed byte-consistent with the leader up to here.
                        self.nodes[to].matched = self.nodes[to].matched.max(to_pos);
                        self.record_committed(to);
                    }
                    // Ack our real append so the leader's cursor tracks us
                    // (advancing on match, backing off on a divergent/gap drop).
                    let (id, ap) = (self.nodes[to].id, self.nodes[to].append);
                    self.send(to, from, Msg::Ack { from: id, term: cur, append: ap }, now);
                }
                Ok(())
            }
            Msg::Ack { from: acker, term, append } => {
                let nd = &mut self.nodes[to];
                if matches!(nd.sm.role(), Role::Leader) && term == nd.sm.current_term() {
                    // Track the follower's authoritative frontier (allows backing
                    // OFF to a divergence/gap, not only advancing).
                    nd.cursors[acker as usize] = append;
                }
                Ok(())
            }
            Msg::Report { from: rep, term, durable } => {
                self.feed(to, Event::Report { from: rep, term, durable }, now, step)
            }
            Msg::CommitGossip { term, commit } => {
                self.nodes[to].leader_hint = Some(from);
                self.feed(to, Event::CommitGossip { term, commit }, now, step)
            }
            Msg::RequestVote { from: cand, new_term, last_term, last_durable } => self.feed(
                to,
                Event::RequestVote { from: cand, new_term, last_term, last_durable },
                now,
                step,
            ),
            Msg::Vote { from: voter, term, granted } => {
                self.feed(to, Event::Vote { from: voter, term, granted }, now, step)
            }
            Msg::TermMap { term, entries } => {
                self.nodes[to].leader_hint = Some(from);
                // Capture the pre-reconcile context so a resulting Truncate can
                // be validated (invariant 4 + T2 carry).
                self.nodes[to].map_before_reconcile = self.nodes[to].term_map.clone();
                self.nodes[to].last_leader_map = entries.clone();
                let term_before = self.nodes[to].sm.current_term();
                let truncs_before = self.stat_truncations;
                self.feed(to, Event::TermMapReceived { term, entries }, now, step)?;
                // Intake-gate CLEAN-RECONCILE reopen (mirrors uc2_node::feed): a
                // term-map that was actually processed (`term >= ours`) and needed
                // NO truncation completes reconciliation for the adopted term, so
                // the gate reopens and the reconcile latch clears.
                //
                // C-1 GUARD (iff `reopen_guard`): additionally require no
                // truncation in flight. A leader re-ships its map continuously, so
                // a DUPLICATE term-map routinely lands after we emitted a
                // `Truncate` but before the archive ack; the SM's truncating latch
                // drops it with zero actions (`produced_truncate` false, term
                // unchanged), and the UNGUARDED heuristic would reopen the gate
                // mid-truncation — letting the raw divergent durable escape into
                // the leader's commit ranking (the M4 C-1 phantom-commit path).
                if let DataPlane::Mechanism { reopen_guard } = self.cfg.data_plane {
                    // CLEAN-RECONCILE reopen: a term map that was processed
                    // (`term >= ours`) and needed NO truncation completes
                    // reconciliation for the adopted term, so a CLOSED gate
                    // reopens. C-1 GUARD (iff `reopen_guard`): additionally require
                    // no truncation in flight. A leader re-ships its map
                    // continuously, so a duplicate term map routinely lands after a
                    // `Truncate` but before the archive ack; the SM's truncating
                    // latch drops it with zero actions (`produced_truncate` false,
                    // term unchanged), and the UNGUARDED heuristic reopens the gate
                    // mid-truncation — letting the raw divergent durable escape into
                    // the leader's commit ranking (the M4 C-1 phantom-commit path,
                    // commit a8d98f4).
                    let produced_truncate = self.stat_truncations > truncs_before;
                    let guard_ok = !reopen_guard || !self.nodes[to].truncating;
                    if !self.nodes[to].intake_gate
                        && !produced_truncate
                        && guard_ok
                        && term >= term_before
                    {
                        self.reopen_gate(to, now);
                    }
                }
                Ok(())
            }
        }
    }

    // ------------------------------------------------------ SM feed + actions

    /// Feed one event into node `i`'s SM and translate every resulting action
    /// (including SM-local follow-ups such as `NewTermAppended`) into world
    /// effects. Down nodes ignore feeds.
    fn feed(
        &mut self,
        i: usize,
        ev: Event,
        now: u64,
        step: u64,
    ) -> Result<(), InvariantViolation> {
        if !self.nodes[i].up {
            return Ok(());
        }
        let mut work = vec![ev];
        while let Some(ev) = work.pop() {
            let mut out = Vec::new();
            self.nodes[i].sm.step(ev, &mut out);
            for act in out {
                self.apply_action(i, act, now, step, &mut work)?;
            }
        }
        Ok(())
    }

    fn apply_action(
        &mut self,
        node: usize,
        act: Action,
        now: u64,
        step: u64,
        work: &mut Vec<Event>,
    ) -> Result<(), InvariantViolation> {
        let n = self.cfg.n_nodes;
        match act {
            Action::PersistAndSendVote { to, vote } => {
                // Persist BEFORE the send — the model enforces the ordering
                // contract structurally.
                self.nodes[node].vote = Some((vote.term, vote.voted_for));
                if to != node as NodeId {
                    let from = node as NodeId;
                    self.send(
                        node,
                        to as usize,
                        Msg::Vote { from, term: vote.term, granted: true },
                        now,
                    );
                }
            }
            Action::SendVoteRejection { to, term } => {
                let from = node as NodeId;
                self.send(node, to as usize, Msg::Vote { from, term, granted: false }, now);
            }
            Action::StartElection { new_term, last_term, last_durable } => {
                let from = node as NodeId;
                for p in 0..n {
                    if p == node {
                        continue;
                    }
                    self.send(
                        node,
                        p,
                        Msg::RequestVote { from, new_term, last_term, last_durable },
                        now,
                    );
                }
            }
            Action::BecomeLeader { term, base } => {
                // Invariants 1 + 5 at the instant a term opens. The leader's own
                // SM map already holds the freshly-opened (term, base); inv5
                // checks it against the committed lineage (ground truth), so a
                // divergent peer can't excuse an incomplete leader.
                let leader_map = self.nodes[node].sm.term_map().to_vec();
                self.checker.on_become_leader(node as NodeId, term, base, &leader_map, step)?;
                self.stat_leaders += 1;

                // Collapse the volatile tail to the durable base, then append the
                // NewTerm no-op frame and feed it back; reset send cursors.
                let nd = &mut self.nodes[node];
                nd.append = base;
                nd.durable = nd.durable.max(base);
                nd.append += FRAME;
                let pos = nd.append;
                nd.new_term_pos = Some(pos);
                nd.leader_hint = None;
                for c in &mut nd.cursors {
                    *c = base;
                }
                nd.term_map = nd.sm.term_map().to_vec();
                work.push(Event::NewTermAppended { position: pos });

                // Immediate heartbeat so followers re-arm (avoids a spurious
                // competing election right after we win). The NewTerm frame
                // [base, pos) is the new term; prev_term is whatever preceded it.
                for p in 0..n {
                    if p == node {
                        continue;
                    }
                    let msg = self.make_data(node, base, pos, term);
                    self.send(node, p, msg, now);
                }
                // Intake gate (Mechanism): a leader is the source of truth — gate
                // open, no reconcile pending (mirrors uc2_node::exec BecomeLeader).
                let nd = &mut self.nodes[node];
                nd.adopted_term = term;
                        nd.intake_gate = true;
                nd.pending_trunc_to = None;
            }
            Action::BecomeFollower { term, leader } => {
                self.nodes[node].leader_hint = leader.map(|l| l as usize);
                self.nodes[node].new_term_pos = None;
                // A new term means a possibly-new leader: the replication match
                // must be re-confirmed from scratch (Raft resets matchIndex).
                self.nodes[node].matched = 0;
                // Intake gate (Mechanism): adopting a STRICTLY new term closes the
                // gate; it reopens only once reconciliation for this term completes
                // (a clean term map, or a truncation ack). The shadow
                // `adopted_term` is tracked in all modes; the gate field is only
                // read under `Mechanism`.
                if term > self.nodes[node].adopted_term {
                    self.nodes[node].intake_gate = false;
                }
                self.nodes[node].adopted_term = term;
            }
            Action::AdvanceCommit { commit } => {
                // Genuine-quorum oracle (F3): the checker judges the commit
                // against every member's REAL (durable, term_map), independent of
                // the data-plane mode. A phantom commit is caught here and aborts
                // the run before the global commit high-water can advance.
                let durables: Vec<u64> = self.nodes.iter().map(|nd| nd.durable).collect();
                let maps: Vec<Vec<(u32, u64)>> =
                    self.nodes.iter().map(|nd| nd.term_map.clone()).collect();
                let term = self.nodes[node].sm.current_term();
                let is_leader = matches!(self.nodes[node].sm.role(), Role::Leader);
                self.checker.on_advance_commit(
                    node as NodeId,
                    commit,
                    term,
                    is_leader,
                    &durables,
                    &maps,
                    step,
                )?;
                self.nodes[node].commit = commit;
                // The genuine commit / lineage may have advanced: refresh every
                // node's committed high-water against the new ground truth.
                for m in 0..n {
                    self.record_committed(m);
                }
            }
            Action::GossipCommit { commit } => {
                let term = self.nodes[node].sm.current_term();
                for p in 0..n {
                    if p == node {
                        continue;
                    }
                    self.send(node, p, Msg::CommitGossip { term, commit }, now);
                }
            }
            Action::ShipTermMap { entries } => {
                let term = self.nodes[node].sm.current_term();
                for p in 0..n {
                    if p == node {
                        continue;
                    }
                    self.send(node, p, Msg::TermMap { term, entries: entries.clone() }, now);
                }
            }
            Action::PersistTermMap { new_map } => {
                self.nodes[node].term_map = new_map;
            }
            Action::Truncate { epoch, to, new_map } => {
                let own_before = self.nodes[node].map_before_reconcile.clone();
                let leader = self.nodes[node].last_leader_map.clone();
                self.checker.on_truncate(node as NodeId, to, &own_before, &leader, step)?;
                self.stat_truncations += 1;

                let mechanism = matches!(self.cfg.data_plane, DataPlane::Mechanism { .. });
                let nd = &mut self.nodes[node];
                if mechanism {
                    // Real intake gate: emitting the truncate closes the gate and
                    // clears the reconcile latch — the truncate IS the reconcile
                    // decision for this term (mirrors uc2_node::exec Truncate).
                    //
                    // PERSIST-BEFORE-TRUNCATE (uc2_node): the PRUNED map (`new_map`,
                    // a clean prefix of the leader's lineage) is adopted durably
                    // NOW, so a crash in the window recovers a valid prefix and the
                    // map never claims terms above the still-present bytes. inv2
                    // stays clean even if the global commit advances during the
                    // window. What is DEFERRED is the physical byte truncation:
                    // `durable`/`append` stay at the divergent value (bytes still on
                    // disk) until the `TruncatedFeedback` ack applies `to`. Because
                    // the adopted map only covers up to `to`, the checker's content
                    // oracle already caps this node's genuine frontier at `to` — so
                    // an escaped report of the higher raw durable (the C-1
                    // counterfactual) is caught as a phantom commit (inv5).
                    nd.term_map = new_map;
                    nd.intake_gate = false;
                    nd.pending_trunc_to = Some(to);
                } else {
                    nd.durable = to;
                    nd.append = to;
                    // The persisted term map is NOT adopted until `Truncated`
                    // feedback — a crash in this window recovers the pre-truncate
                    // map (which reconcile handles again). `new_map` is only what
                    // the SM will adopt on feedback (it re-derives it too).
                    let _ = new_map;
                }
                nd.truncating = true;

                if self.crash_on_truncate {
                    self.crash_on_truncate = false;
                    // Crash the instant the truncate fires: no feedback follows;
                    // durable stayed at `to` (archive-durable), map stayed old.
                    self.do_crash(node, now);
                } else {
                    // Feed `Truncated` back after the archive-latency window
                    // (`truncate_latency_ns`; 0 = the very next event), carrying
                    // the SM-allocated epoch so it matches (M5). The non-zero
                    // window is the C-1 reproduction surface — see the field docs.
                    self.push(
                        SimEvent::TruncatedFeedback { node, epoch, to },
                        now + self.cfg.truncate_latency_ns,
                    );
                }
            }
            Action::CountWipe => {
                // M6 Task 8: a wipe-and-rejoin was decided; the substantive
                // `Truncate { to: 0 }` follows in the same batch (handled above).
                self.stat_wipes += 1;
            }
            Action::ConfigAdopted { .. } => {
                // M7 migration stub: config-frame modeling (mirror + revert-on-
                // truncate + inv6-9) lands in Task 4 Step 2, not this pure migration.
            }
            Action::HaltRemoved => {
                // M7 migration stub: see `Action::ConfigAdopted` above.
            }
            Action::Fatal { reason } => {
                // With wipe-and-rejoin ON (default), NoCommonPrefix never reaches
                // here. It only fires in the wipe-disabled COUNTERFACTUAL, which
                // deliberately reproduces the old fail-stop to document what the
                // wipe path changed — so surface it as the invariant breach.
                return Err(InvariantViolation {
                    invariant: "Fatal unreachable — NoCommonPrefix (spec §8)",
                    step,
                    seed: self.cfg.seed,
                    detail: format!("node {node} surfaced Fatal: {reason}"),
                });
            }
        }
        Ok(())
    }

    fn do_crash(&mut self, node: usize, now: u64) {
        {
            let nd = &mut self.nodes[node];
            nd.up = false;
            // Volatile state is lost; durable / vote / term_map persist.
            nd.append = nd.durable;
            nd.commit = 0;
            nd.truncating = false;
            nd.new_term_pos = None;
            nd.leader_hint = None;
            nd.matched = 0;
            for c in &mut nd.cursors {
                *c = 0;
            }
        }
        // Recover after ~1-2 election timeouts so the cluster can react first.
        let base = self.cfg.election_timeout_max_ns;
        let jitter = if base == 0 { 0 } else { self.draw() % base };
        self.push(SimEvent::Restart { node }, now + base + jitter);
    }

    // ------------------------------------------------------- scripting hooks

    /// Drop all `RequestVote`/`Vote` traffic until virtual time `t` (ns).
    pub fn drop_all_votes_until(&mut self, t: u64) {
        self.vote_drop_until = t;
    }

    /// Isolate a node from every peer (a total partition of that node).
    pub fn partition_node(&mut self, node: usize) {
        self.isolated.insert(node);
    }

    /// Block a specific directed-agnostic pair `(a, b)`.
    pub fn partition(&mut self, a: usize, b: usize) {
        self.blocked_pairs.insert((a.min(b), a.max(b)));
    }

    /// Heal all partitions (isolated nodes and blocked pairs).
    pub fn heal(&mut self) {
        self.isolated.clear();
        self.blocked_pairs.clear();
    }

    /// Selectively reconnect a single directed-agnostic pair `(a, b)` (the inverse
    /// of [`World::partition`]) without touching other blocked pairs — needed to
    /// script a partial heal, e.g. reconnect an ex-leader to the new leader while
    /// keeping a third node lagging. (Total isolation via [`World::partition_node`]
    /// is all-or-nothing; a precise partial heal uses pairwise `partition` +
    /// `unpartition`.)
    pub fn unpartition(&mut self, a: usize, b: usize) {
        self.blocked_pairs.remove(&(a.min(b), a.max(b)));
    }

    /// Quiesce (or resume) the cluster's write load: while set, a serving leader
    /// stops appending NEW frames, so commit plateaus. Models "no new client
    /// submissions" — the state in which only the idle gossip floor keeps a
    /// reconnecting divergent node reconciling.
    pub fn set_quiet(&mut self, quiet: bool) {
        self.quiet = quiet;
    }

    /// Number of truncations any node has performed so far (for mid-run assertions).
    pub fn truncations(&self) -> u32 {
        self.stat_truncations
    }

    /// Crash a node immediately (at the current virtual time).
    pub fn crash(&mut self, node: usize) {
        if self.nodes[node].up {
            let now = self.now;
            self.do_crash(node, now);
        }
    }

    /// Restart a crashed node immediately.
    pub fn restart(&mut self, node: usize) -> Result<(), InvariantViolation> {
        let now = self.now;
        self.on_restart(node, now)
    }

    /// Arm a one-shot: crash a node the moment its next `Truncate` action fires.
    pub fn crash_on_next_truncate(&mut self) {
        self.crash_on_truncate = true;
    }

    /// Deliver a single crafted `Data` wire frame straight into node `to`'s
    /// receiver (bypassing the partition table — this is exactly the byte-for-byte
    /// input the real M3 `FollowerReceiver` processes), then run the post-event
    /// invariant sweep. Used to script the reviewer's exact receiver trace where a
    /// leader's current-term frame lands on a follower whose prefix diverges — the
    /// timing a natural run can't force because the reconcile term-map (shipped on
    /// the commit-gossip cadence) otherwise repairs the follower first. Under
    /// [`DataPlane::RawM3`] the follower accepts on contiguity alone and stamps the
    /// segment's term at a wrong base; the returned violation is that catch.
    #[allow(clippy::too_many_arguments)]
    pub fn inject_data(
        &mut self,
        from: usize,
        to: usize,
        term: u32,
        seg_term: u32,
        from_pos: u64,
        to_pos: u64,
        prev_term: u32,
    ) -> Result<(), InvariantViolation> {
        self.steps += 1;
        let step = self.steps;
        let now = self.now;
        let msg = Msg::Data { term, seg_term, from_pos, to_pos, prev_term };
        self.deliver(to, from, msg, now, step)?;
        // Mirror `step_once`'s post-event invariant 2 sweep.
        let maps: Vec<Vec<(u32, u64)>> = self.nodes.iter().map(|n| n.term_map.clone()).collect();
        self.checker.check_prefix_consistency(&maps, step)
    }

    /// Enqueue a raw follower `Report{from, term, durable}` addressed to the
    /// current leader, regardless of data-plane mode (the T5-deferred forged-report
    /// pin). `durable` is taken at face value: a value ABOVE the sender's real
    /// durable models a forged/corrupt report that a real leader's CommitTracker
    /// would rank verbatim. The genuine byte-content-quorum oracle must catch any
    /// commit that rides it. No-op if there is no leader to report to.
    pub fn inject_report(&mut self, from: usize, term: u32, durable: u64) {
        let Some(to) = self
            .nodes
            .iter()
            .position(|n| n.up && matches!(n.sm.role(), Role::Leader))
        else {
            return;
        };
        let id = self.nodes[from].id;
        let now = self.now;
        // Deliver promptly (minimal link latency) so the injected report is ranked
        // before followers legitimately catch up to the leader — this keeps the
        // pin deterministic. Bypasses the drop/dup fault dice (a scripted inject).
        self.push(
            SimEvent::Deliver { to, from, msg: Msg::Report { from: id, term, durable } },
            now + self.cfg.latency_min_ns,
        );
    }

    /// M6 Task 8: deliver a crafted `TermMap` wire message straight into node
    /// `to`'s reconcile (bypassing the partition table), then run the post-event
    /// invariant sweep. This models the purged-leader case a natural sim run can't
    /// cheaply reach: a leader whose shipped term-map tail has slid PAST the
    /// target's first byte (its low-end entries dropped by log purge), so the
    /// target finds no common prefix and must WIPE-and-rejoin. `entries` is exactly
    /// the byte-for-byte wire tail such a leader would ship.
    pub fn inject_term_map(
        &mut self,
        from: usize,
        to: usize,
        term: u32,
        entries: Vec<(u32, u64)>,
    ) -> Result<(), InvariantViolation> {
        self.steps += 1;
        let step = self.steps;
        let now = self.now;
        self.deliver(to, from, Msg::TermMap { term, entries }, now, step)?;
        let maps: Vec<Vec<(u32, u64)>> = self.nodes.iter().map(|n| n.term_map.clone()).collect();
        self.checker.check_prefix_consistency(&maps, step)
    }

    /// M6 Task 8: flip a node's SM to the wipe-DISABLED counterfactual — a
    /// `NoCommonPrefix` reconcile then fail-stops (`Action::Fatal`) instead of
    /// wiping, documenting exactly what the wipe path changed.
    pub fn disable_wipe(&mut self, node: usize) {
        self.nodes[node].sm.set_wipe_on_no_common_prefix(false);
    }

    /// M6 Task 8: wipe-and-rejoin count observed so far.
    pub fn wipes(&self) -> u32 {
        self.stat_wipes
    }

    /// A node's current append (write) position.
    pub fn node_append(&self, node: usize) -> u64 {
        self.nodes[node].append
    }

    /// A node's current SM term.
    pub fn node_term(&self, node: usize) -> u32 {
        self.nodes[node].sm.current_term()
    }

    // ----------------------------------------------------------- accessors

    /// A serving leader (role `Leader` and past the NewTerm read gate), if any.
    pub fn current_leader(&self) -> Option<usize> {
        self.nodes
            .iter()
            .position(|n| n.up && matches!(n.sm.role(), Role::Leader) && n.sm.can_serve())
    }

    /// The global commit high-water (max commit any node ever certified).
    pub fn max_commit(&self) -> u64 {
        self.checker.global_max_commit
    }

    /// A node's own committed high-water (durable across restart).
    pub fn node_commit_high_water(&self, node: usize) -> u64 {
        self.checker.committed_hw[node]
    }

    /// The greatest committed high-water among the given nodes.
    pub fn max_commit_from(&self, nodes: &[usize]) -> u64 {
        nodes.iter().map(|&i| self.checker.committed_hw[i]).max().unwrap_or(0)
    }

    /// Every node except `node` (for a 3-node cluster, the surviving majority).
    pub fn majority_excluding(&self, node: usize) -> Vec<usize> {
        (0..self.cfg.n_nodes).filter(|&i| i != node).collect()
    }
}
