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
//!
//! ## The durable counter has TWO readers (issue #7)
//!
//! `Node.durable` is the shared counter. Two things read it, and they are
//! deliberately modelled as running on **separate schedules**, because in the
//! real node they run on separate threads:
//!
//! - the **report** path (`on_archive`) reads it and ships it to the leader,
//!   which ranks reports into `commit` — mirroring `uc2_net`'s receiver reading
//!   `counters().durable` directly;
//! - the **consensus agent** ([`SimEvent::ConsensusStep`]) absorbs it into
//!   `ElectionSm` via `Event::DurableAdvanced` — mirroring `Consensus::do_work`.
//!   Everything the SM decides with its own durable position (vote grants,
//!   the advertised election credential, a new leader's collapse base, the
//!   own-durable commit clamp) rides that absorbed copy.
//!
//! They can therefore DISAGREE, which is the whole point: a node can report one
//! durable position toward commit while judging votes by an older one. Fusing
//! them — as this model did until issue #7, advancing the counter and feeding
//! the SM as consecutive statements in one `ArchiveStep` — made that entire bug
//! class unreachable here regardless of how the scheduler interleaved. The
//! invariants were adequate the whole time; they simply had no trace to judge.
//! See `stale_vote_credential_opens_a_term_below_a_committed_position`.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

use uc2_consensus::config::{Addr, ClusterConfig, ConfigOp, ProposeError};
use uc2_consensus::election::{Action, ElectionConfig, ElectionSm, Event, NodeId, Role};
use uc2_consensus::reconcile::MAX_TERM_MAP_WIRE_ENTRIES;

use crate::invariants::{InvariantChecker, InvariantViolation};

// ---- T13: crypto-plane sim coverage --------------------------------------
//
// `Peers` (handshake.rs) and `GroupPlane` (group.rs) are pure `(input,
// now_ns) -> Vec<HandshakeAction>` transition functions with no sockets and
// no clock reads — a hard requirement laid on every earlier M8 task
// specifically so this sim could drive them exactly like `ElectionSm`. See
// `World::enable_crypto_plane` for the wiring and the module-level doc
// comment above `NodeCrypto` for the two deliberate sim-only modeling
// choices (the crypto-plane redelivery-sweep cadence, and the "DATA send
// withheld/gated by the group epoch" rule).
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use uc2_crypto::group::GroupPlane;
use uc2_crypto::handshake::{HandshakeAction, Peers};
use uc2_crypto::identity::{Allowlist, Identity};
use uc2_crypto::schedule::BootSalt;
use uc_protocol::v2::crypto::DGRAM_KIND_HS_KEY;

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
    ///
    /// `handle_keyed: true` mirrors `uc2_node` post-Finding-#9 (lean LC2, gate
    /// doc): BOTH gate-reopen arms (clean reconcile in `feed`, truncation ack in
    /// `on_truncated`) reopen ONLY when `current_term == adopted_term` — the SM's
    /// active term equals the data-plane term handle the receiver filters DATA at.
    /// `handle_keyed: false` is the Finding-#9 COUNTERFACTUAL: a CANDIDATE (whose
    /// handle lags its `StartElection`-bumped `current_term`) that cleanly
    /// reconciles a higher-term leader's map reopens intake for its stale
    /// handle-term stream, then accepts a cross-stream old-term byte its map never
    /// attributed and reports it — the acked-write-loss phantom the oracle catches
    /// (§5.4.2 / #6b family).
    Mechanism { reopen_guard: bool, handle_keyed: bool },
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

// ---- T13: crypto-plane sim fixtures --------------------------------------

/// Scratch root for T13's sim-generated crypto identities, on real disk
/// (never `/tmp` — RAM-backed tmpfs, no swap on the dev box; CLAUDE.md).
///
/// **Not `CARGO_TARGET_TMPDIR`.** That env var is a compile-time-only value
/// cargo sets for the specific `tests/*.rs` binary target being built —
/// retrievable via `env!()` from code compiled AS PART OF that target
/// (`uc2_crypto`'s own test helpers do exactly that, from inside
/// `tests/*.rs` files). `World::enable_crypto_plane` lives in THIS crate's
/// `src/` (the library target, a separate compilation unit from
/// `tests/scenarios.rs`), so `env!("CARGO_TARGET_TMPDIR")` here would fail
/// to compile — the same gap `uc2_crypto/src/identity.rs`'s test helper
/// documents for its own inline `#[cfg(test)]` module. A RUNTIME
/// `std::env::var("CARGO_TARGET_TMPDIR")` compiles, but confirmed dead:
/// the variable is never actually present in the running test process's
/// environment (`Err(NotPresent)`, checked directly). So this always uses
/// the package-relative fallback; no `env::var` attempt is made.
///
/// **Unique per call, not keyed by seed** (T13 review finding I-1): an
/// earlier version keyed this directory by `cfg.seed` alone, reasoning
/// "parallel test THREADS with different seeds never collide" — true, but
/// wrong scope: `cargo test` runs different TESTS concurrently, and
/// several committed scenarios pin seeds (7, 11, 13, 17, 21) that all sit
/// inside the oracle-twin fuzz's `0..60` sweep, so two unrelated tests can
/// legitimately race on the SAME seed at the SAME time — one test's
/// `fs::write` (truncate-then-write) racing another's concurrent
/// `Identity::load` (read) on the identical path. Reproduced: 118/1600
/// concurrent calls panicked with `KeyFileInvalid` before this fix. Keying
/// by a process-wide monotonic counter instead (nothing later ever needs
/// to find this directory again by seed — it is written and read back
/// within the same `enable_crypto_plane` call and never touched again)
/// makes every call's directory disjoint by construction; no seed
/// collision is possible regardless of how many tests share a seed.
fn crypto_scratch_dir() -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../target/uc2_sim_crypto_tests")
        .join("uc2-sim-crypto")
        .join(format!("call-{n}-{:?}", std::thread::current().id()));
    std::fs::create_dir_all(&d).expect("create sim crypto scratch dir");
    assert!(!d.starts_with("/tmp"), "sim crypto scratch must not live on tmpfs: {d:?}");
    d
}

/// Deterministic (NOT cryptographically meaningful — this is a sim fixture,
/// not key generation for anything real) 32-byte "private key" for a sim
/// node: this module's own `XorShift64`, seeded from the world seed and node
/// index, filled 8 bytes at a time. Any 32 bytes are a valid X25519 scalar
/// (`StaticSecret::from` clamps internally), so this only needs to be
/// deterministic and distinct per `(seed, node)`.
fn deterministic_key_bytes(seed: u64, node: u64) -> [u8; 32] {
    let mut rng = XorShift64::new(seed ^ 0xA11CE_u64.wrapping_mul(node.wrapping_add(1)) ^ 0x5EED);
    let mut bytes = [0u8; 32];
    for chunk in bytes.chunks_mut(8) {
        chunk.copy_from_slice(&rng.next_u64().to_le_bytes());
    }
    bytes
}

/// Deterministic 16-byte boot salt for a sim node — same rationale as
/// [`deterministic_key_bytes`], a disjoint RNG stream (distinct XOR
/// constants) so a node's key and its salt never accidentally coincide.
fn deterministic_salt_bytes(seed: u64, node: u64) -> [u8; 16] {
    let mut rng = XorShift64::new(seed ^ 0xB00F_u64.wrapping_mul(node.wrapping_add(1)) ^ 0x5A17);
    let mut bytes = [0u8; 16];
    for chunk in bytes.chunks_mut(8) {
        chunk.copy_from_slice(&rng.next_u64().to_le_bytes());
    }
    bytes
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
    /// Cadence of [`SimEvent::ConsensusStep`] — the consensus agent's duty
    /// cycle, which is what ABSORBS the durable counter into `ElectionSm` as
    /// `Event::DurableAdvanced`. Issue #7: this must be a SEPARATE scheduled
    /// event from `ArchiveStep`, because in the real node the counter and the
    /// SM's copy of it are read by two different threads, and fusing them makes
    /// a whole bug class unreachable.
    ///
    /// A free-running timer, deliberately, and it does cost `max_steps` budget
    /// on every node. A cheaper one-shot ("queue an absorb when the counter
    /// moves") was tried and REJECTED: it models the same lag but reached the
    /// target loss on 0 of 300 seeds where the periodic agent reaches it on 3 of
    /// 120, because absorption then always lands relative to the last advance
    /// and a frozen archive lets the SM catch up before any candidate times out.
    /// Teeth beat budget. Existing scenarios were re-verified against the added
    /// event load — including the RED arms that must still FIND their own
    /// findings (`mechanism_unguarded_reopen_is_caught_by_oracle`, both
    /// `finding9_*`), which still fire.
    ///
    /// Deliberately coarse relative to the real system (a busy-spin consensus
    /// agent absorbs within microseconds of an fsync, not milliseconds). The
    /// sim exaggerates every cadence — 5 ms archive steps, 10 ms ticks — because
    /// its job is REACHABILITY, not probability: a schedule that is rare on real
    /// hardware must be routinely explorable here or the invariants never get to
    /// judge it.
    pub consensus_step_ns: u64,
    /// Issue #7 ablation knob (default `true` = the SHIPPED behaviour). When
    /// true, delivering a `RequestVote` re-absorbs the durable counter into the
    /// SM immediately BEFORE the grant decision — mirroring `feed_net`'s
    /// `refresh_durable()` call in `uc2_node`.
    ///
    /// Set `false` to reproduce the pre-fix system, in which `log_ok` compared
    /// candidates against a self-view up to one consensus duty cycle stale while
    /// the receiver had already REPORTED the newer counter value for commit
    /// ranking. That is the acked-write loss fixed in `main` 26d4827, and it is
    /// what `inv4`/`inv5` must catch here — see
    /// `stale_vote_credential_loses_an_acked_write`.
    pub vote_refresh_durable: bool,
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
    /// M7: node indices that start as LEARNERS in the genesis config (every
    /// other index is a genesis voter). A learner is replicated-to but never
    /// counted toward quorum and never a candidate — until promoted.
    pub initial_learners: Vec<usize>,
    /// M7 Task 9 (T9 integration-catch coverage): node indices that are a
    /// real, ticking sim process but start ENTIRELY ABSENT from the genesis
    /// `ClusterConfig` — neither voter nor learner — mirroring a real cluster
    /// process that exists but hasn't been admitted yet (a `resize_3_to_5_to_3`
    /// -shaped SECOND joiner). Must be disjoint from `initial_learners`. Such a
    /// node receives ZERO replication/gossip/vote traffic (see `config_peers`)
    /// until `AddLearner` admits it, then genuinely replays every prior
    /// `ConfigObserved` version — including ones that exclude its own id —
    /// exactly like a fresh real process catching up from empty.
    pub genesis_absent: Vec<usize>,
    /// M7 COUNTERFACTUAL (default false): `propose_config` overrides a
    /// `NotServing` refusal — deleting the single-server-change precondition
    /// (Ongaro 2015; structurally the M4 serving gate). The crafted
    /// disjoint-quorum scenario must then trip inv7 — the red pin proving the
    /// gate is load-bearing.
    pub serving_gate_disabled: bool,
    /// M7 COUNTERFACTUAL (default false): delete revert-on-truncate — both the
    /// SM's revert (`ElectionSm::set_revert_on_truncate(false)`) and the sim's
    /// node-obligation mirror revert at `Truncate` exec. A truncation that
    /// removes a config frame then leaves the stale config adopted, and inv8
    /// must go red.
    pub revert_on_truncate_disabled: bool,
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
            consensus_step_ns: 1_000_000,  // 1ms
            vote_refresh_durable: true,
            latency_min_ns: 1_000_000,     // 1ms
            latency_max_ns: 5_000_000,     // 5ms
            archive_bytes_max: 4 * FRAME,
            election_timeout_min_ns: 150_000_000, // 150ms — the SM's own default
            election_timeout_max_ns: 300_000_000, // 300ms
            gossip_floor_ns: 100_000_000,         // 100ms — spec §6 idle re-gossip
            data_plane: DataPlane::Gated,
            truncate_latency_ns: 0, // instantaneous archive ack (historical default)
            initial_learners: Vec::new(),
            genesis_absent: Vec::new(),
            serving_gate_disabled: false,
            revert_on_truncate_disabled: false,
        }
    }
}

/// M7: a config frame appended into a leader's modeled stream — the World's
/// frame LEDGER entry. `end` is the frame-END byte position (the effect point);
/// `term` is the appending leader's term, which doubles as the frame's content
/// identity (spec §6): a node holds this frame iff `term_at(map, end - 1) ==
/// term`, i.e. its lineage at that byte is the appending term's.
#[derive(Clone, Debug)]
struct CfgFrame {
    term: u32,
    end: u64,
    config: ClusterConfig,
}

/// M7: promote-learner catch-up slack (max report gap) used by the sim's
/// `propose_config`. Generous relative to the archive cadence so a broadcast-fed
/// learner qualifies within a few retries; small enough to stay a real check.
const PROMOTE_SLACK: u64 = 64 * FRAME;

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
    ///
    /// `epoch` (T13): `None` for a crypto-disabled world (unchanged pre-M8
    /// behavior) or a scripted `inject_data` craft; `Some(e)` when the
    /// sending leader is crypto-enabled — the group-key epoch it is
    /// currently *sealing* under (`GroupPlane::sealing_epoch`). A receiver
    /// that has not installed `e` cannot open it in the real system; see the
    /// crypto gate at the top of `deliver`'s `Msg::Data` arm.
    Data { term: u32, seg_term: u32, from_pos: u64, to_pos: u64, prev_term: u32, epoch: Option<u16> },
    /// Follower -> leader replication ack: drives the per-follower send cursor.
    Ack { from: NodeId, term: u32, append: u64 },
    /// Follower -> leader durable report: drives quorum commit ranking.
    Report { from: NodeId, term: u32, durable: u64 },
    /// Leader -> follower commit gossip. `epoch` (T13, model-fidelity fix):
    /// production classifies `DGRAM_KIND_COMMIT_POSITION` as
    /// `Scope::Group` (`uc2_crypto::transport::scope_of`), sealed and
    /// gated exactly like `DATA`/`HEARTBEAT` — an earlier version of this
    /// sim left `CommitGossip` entirely ungated, which was a real
    /// model-fidelity gap (flagged in T13 review): the root-cause account
    /// for the churn finding above cited "CommitGossip is never
    /// crypto-gated" as part of why a follower can still learn about a new
    /// leader through an ungated path, which was false relative to
    /// production. Gated the same way `Data` is, below.
    CommitGossip { term: u32, commit: u64, epoch: Option<u16> },
    RequestVote { from: NodeId, new_term: u32, last_term: u32, last_durable: u64 },
    Vote { from: NodeId, term: u32, granted: bool },
    /// Leader -> follower term-map ship (drives reconciliation).
    TermMap { term: u32, entries: Vec<(u32, u64)> },
    /// T13: follower -> leader gap-repair request, addressed at whichever
    /// leader most recently reached it — the sim's model of the real
    /// reliable-UDP NAK path (`uc2_net`'s receiver-driven retransmit
    /// request). Only ever emitted here by the crypto gate (`Msg::Data`'s
    /// receive handling): the byte-push replication model this sim already
    /// runs is otherwise a continuous leader-driven resend, so `Msg::Nak` is
    /// the one place a genuine receiver-initiated repair round-trip is
    /// observable and countable (`World::nak_count`).
    Nak { from: NodeId, want_from: u64 },
    /// T13: an opaque handshake/rotation datagram — Noise `IK` message 1/2
    /// (kind 18/19, `uc2_crypto::handshake::Peers`) or an `HS_KEY`
    /// delivery/ack (kind 20, `uc2_crypto::group::GroupPlane`) — routed
    /// through the SAME lossy/partitionable `send`/`deliver` path as every
    /// consensus message, per the task brief ("routes kinds 18/19/20
    /// through the existing lossy link model").
    Handshake { kind: u8, body: Vec<u8> },
}

/// A scheduled simulation event.
#[derive(Clone, Debug)]
enum SimEvent {
    Deliver { to: usize, from: usize, msg: Msg },
    Tick { node: usize },
    ArchiveStep { node: usize },
    /// Issue #7: the consensus agent's duty cycle, scheduled INDEPENDENTLY of
    /// `ArchiveStep`. It is the only thing that absorbs the durable counter into
    /// `ElectionSm` (`Event::DurableAdvanced`), mirroring `Consensus::do_work`
    /// step 2 in `uc2_node`.
    ///
    /// Before this existed, `ArchiveStep` advanced the counter AND fed the SM in
    /// one indivisible handler, so the two could never disagree — and the whole
    /// class of "a node reports one durable to the leader while judging votes by
    /// another" was unreachable in this world, no matter how the scheduler
    /// interleaved. `inv4`/`inv5` were adequate all along; they simply had no
    /// trace to convict.
    ConsensusStep { node: usize },
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
    /// Durable-across-crash: the fsync'd byte position. THE COUNTER — written by
    /// the archive (`ArchiveStep`) and read DIRECTLY by the report path, exactly
    /// as `uc2_net`'s receiver reads `counters().durable` on its own thread.
    durable: u64,
    /// Issue #7: the consensus agent's absorbed copy of `durable` — the shadow
    /// `Consensus::durable_seen` keeps in `uc2_node`, used only to notice a
    /// change. `ElectionSm`'s own `durable` is what the SM decides with; this is
    /// the node-layer cursor that decides WHEN to tell it. The gap between this
    /// and `durable` above is the dual-reader skew that issue #7 exists to make
    /// expressible.
    durable_absorbed: u64,
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
    // ---- M7 config state ----
    /// Durable-across-crash mirror of the node's `ConfigRecord { cur, prev }`
    /// (the sim IS the StableValue, like `vote`/`term_map`). Written on every
    /// `Action::ConfigAdopted`; the node-obligation revert happens at
    /// `Action::Truncate` EXEC (persist-revert-before-truncate, spec §5) so a
    /// crash anywhere in the truncation window recovers a record consistent
    /// with the truncated log. Recovered into the SM at restart.
    cfg_cur: ClusterConfig,
    cfg_cur_pos: u64,
    cfg_prev: ClusterConfig,
    cfg_prev_pos: u64,
    /// `Action::HaltRemoved` fired: the node fail-stopped permanently (removed
    /// from the cluster). Like a crash, but no restart is ever scheduled and
    /// `restart()` refuses.
    halted: bool,
    // ---- T13: crypto plane ----
    /// `Some` once `World::enable_crypto_plane` has wired this node; `None`
    /// (the default) reproduces the pre-M8 abstract model byte-for-byte —
    /// every crypto-plane sim addition is a no-op unless this is set.
    crypto: Option<NodeCrypto>,
    /// T13: count of `Msg::Nak` gap-repair requests this node has SENT — see
    /// `World::nak_count`.
    nak_sent: u32,
}

/// T13: one node's crypto-plane state. `peers`/`group` are pure `(input,
/// now_ns) -> Vec<HandshakeAction>` transition functions (no sockets, no
/// clock reads) — `uc2_crypto::handshake`/`uc2_crypto::group`'s own design
/// constraint, which is what makes this drivable at all.
struct NodeCrypto {
    peers: Peers,
    group: GroupPlane,
    /// Next virtual time at which this node's `GroupPlane::unacked_peers`
    /// re-delivery sweep (the T12 fix for `mint`'s single-shot `HS_KEY`
    /// delivery — see `group.rs`'s module doc) may run again. See
    /// `CRYPTO_SWEEP_INTERVAL_NS`.
    next_sweep_ns: u64,
}

/// T13: how often (of virtual time) a crypto-enabled node's maintenance tick
/// re-polls `GroupPlane::unacked_peers` and re-sends via `redeliver_to`.
///
/// Fast, not slow — revised from an earlier 2.5s choice (T13 review): once
/// `Msg::CommitGossip` was correctly gated the same as `DATA`/`HEARTBEAT`
/// (matching production's `Scope::Group` classification of
/// `COMMIT_POSITION` — a model-fidelity fix, see `Msg::CommitGossip`'s
/// doc), a node that cannot open ANY current group traffic genuinely hears
/// NOTHING from its leader — no back-channel keeps it pacified — so it
/// calls its own election on the SM's normal ~150-300ms timeout regardless
/// of how the redelivery sweep is tuned. A sweep slower than that window
/// does not "cleanly separate" a scripted gap from the activation timeout
/// (the original rationale) — it just guarantees an election fires before
/// the sweep ever gets a chance to matter, which then recovers the peer
/// through an unrelated fresh mint instead of through
/// `redeliver_to` — not wrong, but not what a script trying to exercise
/// the redelivery path specifically wants. A sweep this fast, paired with
/// `World::block_key_delivery_to`'s explicit TIME WINDOW (rather than a
/// one-shot drop count) for holding a gap open exactly as long as a script
/// needs, keeps the redelivery path independently exercisable: the block
/// window can still be built wide enough to force the leader down the
/// `ACTIVATION_TIMEOUT_NS` (2s) path, while the sweep recovers the peer
/// within one election-timeout's worth of the window closing. Production's
/// own `uc2_node` cadence is a separate, independently-tuned parameter
/// outside this crate's concern; the sim only needs a genuine bounded
/// retry, not a specific number.
const CRYPTO_SWEEP_INTERVAL_NS: u64 = 50_000_000;

/// T13: what a crypto-enabled leader may do with an outgoing `DATA` send
/// right now — see `World::data_seal_gate`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SealGate {
    /// Crypto is off for this node: unchanged pre-M8 behavior (`Msg::Data`
    /// carries `epoch: None`, and the receive-side gate never triggers).
    NotGated,
    /// Crypto is on, but no group epoch has EVER activated yet (a fresh
    /// leader must mint + activate before it can seal `DATA` at all — the
    /// T9 review note this task's brief quotes). The caller withholds the
    /// send entirely rather than emit something claiming a nonexistent
    /// epoch.
    Withhold,
    /// Safe to seal under this epoch right now
    /// (`GroupPlane::sealing_epoch`'s answer).
    Sealed(u16),
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
    // ---- M7 config modeling ----
    /// T9 integration-catch coverage (M7 Task 9): the set of real node
    /// indices EVER admitted into SOME adopted config, at any point — starts
    /// as every id except `SimConfig::genesis_absent`, and gains an id the
    /// instant a leader's `propose_config` computes a new config that
    /// contains it (so the very frame that admits it is itself
    /// deliverable). Deliberately ONE-WAY (never removed): a later
    /// demotion/removal must still be able to reach the target at least
    /// once so it can adopt its own removal and fail-stop cleanly — gating
    /// replication fan-out on the SENDER's live/current adopted config
    /// instead would stop shipping the removal frame to the very node that
    /// needs to receive it. Used by `config_peers` to gate the five
    /// replication/gossip/vote broadcast sites; probed only via
    /// `.contains()` per the determinism guard above.
    admitted_ever: HashSet<usize>,

    /// The config-frame LEDGER: every config frame ever appended by any leader
    /// (any lineage). Follower observation (adopt-at-durable) and the inv6/inv8
    /// frontier implication are both recomputed from it.
    config_frames: Vec<CfgFrame>,
    /// The genesis config (identical on every node) — the version-0 baseline of
    /// the frontier implication.
    genesis_config: ClusterConfig,
    /// A violation raised inside a scripted call that cannot return it
    /// (`propose_config` returns `Result<_, ProposeError>`); surfaced by the
    /// next `step_once`.
    pending_violation: Option<InvariantViolation>,
    /// When set, a serving leader stops appending NEW frames — modeling a client
    /// that has stopped submitting. The leader still heartbeats its existing tail
    /// and re-gossips commit + term map on the idle floor, so commit PLATEAUS.
    quiet: bool,

    // stats
    stat_leaders: u32,
    stat_truncations: u32,
    stat_wipes: u32,
    stat_restarts: u32,
    stat_stale_vote_window: u64,

    // ---- T13: crypto plane ----
    /// `HS_KEY` (kind 20) deliveries addressed to a given node index are
    /// dropped while `self.now < deadline` — see
    /// `World::block_key_delivery_to`. Applied at the "socket," before the
    /// fault dice, so it is deterministic regardless of `drop_per_million`.
    /// A time WINDOW rather than a one-shot drop count (T13 review): with
    /// `CRYPTO_SWEEP_INTERVAL_NS` fast, a script needs to hold a gap open
    /// for a specific, possibly long, span of virtual time (e.g. past
    /// `ACTIVATION_TIMEOUT_NS`), not survive some number of retry attempts
    /// whose count would depend on the sweep cadence.
    key_delivery_blocked_until: HashMap<usize, u64>,
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
        assert!(
            cfg.genesis_absent.iter().all(|id| !cfg.initial_learners.contains(id)),
            "genesis_absent and initial_learners must be disjoint"
        );
        let n = cfg.n_nodes;
        let genesis = Self::genesis_config(&cfg);
        let mut nodes = Vec::with_capacity(n);
        for id in 0..n {
            let ecfg = Self::election_cfg(&cfg, id, genesis.clone(), 0);
            let mut sm = ElectionSm::new(ecfg, None, &[], 0, 0);
            if cfg.revert_on_truncate_disabled {
                sm.set_revert_on_truncate(false); // counterfactual: guard deleted
            }
            nodes.push(Node {
                id: id as NodeId,
                sm,
                append: 0,
                durable: 0,
                durable_absorbed: 0,
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
                cfg_cur: genesis.clone(),
                cfg_cur_pos: 0,
                cfg_prev: genesis.clone(),
                cfg_prev_pos: 0,
                halted: false,
                crypto: None,
                nak_sent: 0,
            });
        }
        let checker = InvariantChecker::new(cfg.seed, n);
        let admitted_ever: HashSet<usize> =
            (0..n).filter(|id| !cfg.genesis_absent.contains(id)).collect();
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
            config_frames: Vec::new(),
            genesis_config: genesis,
            admitted_ever,
            pending_violation: None,
            quiet: false,
            stat_leaders: 0,
            stat_truncations: 0,
            stat_wipes: 0,
            stat_restarts: 0,
            stat_stale_vote_window: 0,
            key_delivery_blocked_until: HashMap::new(),
            nodes,
            cfg,
        };
        // Seed the periodic timers, staggered by 1ns so identical-time ties are
        // still broken by `seq` deterministically.
        for id in 0..w.cfg.n_nodes {
            w.push(SimEvent::Tick { node: id }, id as u64);
            w.push(SimEvent::ArchiveStep { node: id }, w.cfg.archive_step_ns + id as u64);
            w.push(SimEvent::ConsensusStep { node: id }, w.cfg.consensus_step_ns + id as u64);
        }
        w
    }

    /// The genesis `ClusterConfig` (identical on every node): all indices are
    /// voters except `initial_learners`, and `genesis_absent` indices are
    /// omitted entirely (present as a running sim process, absent from the
    /// config — see `SimConfig::genesis_absent`); synthetic addrs
    /// `(node_idx, 1)` — the sim never opens sockets, so the addr is a
    /// dep-free placeholder (M7).
    fn genesis_config(cfg: &SimConfig) -> ClusterConfig {
        let mut voters: Vec<(NodeId, Addr)> = Vec::new();
        let mut learners: Vec<(NodeId, Addr)> = Vec::new();
        for id in 0..cfg.n_nodes {
            if cfg.genesis_absent.contains(&id) {
                continue;
            }
            let m = (id as NodeId, (id as u32, 1u16));
            if cfg.initial_learners.contains(&id) {
                learners.push(m);
            } else {
                voters.push(m);
            }
        }
        ClusterConfig::genesis(voters, learners)
    }

    /// The sender's replication/gossip/vote fan-out set: every OTHER real
    /// node index EVER admitted into some adopted config (`admitted_ever`)
    /// — deliberately NOT `0..n_nodes` unconditionally. A `genesis_absent`
    /// id must receive zero traffic until formally admitted via
    /// `AddLearner` (T9 integration-catch coverage: this is what lets a
    /// genesis-absent id genuinely replay pre-admission `ConfigObserved`
    /// history rather than passively tracking the stream from before it
    /// ever joined).
    ///
    /// Deliberately gated on `admitted_ever` (one-way, cluster-global), NOT
    /// on the SENDER's own CURRENT/live adopted config: a leader removing
    /// some OTHER member must keep shipping to it at least once past the
    /// point where the leader itself has already adopted the config that
    /// excludes it, or the removed node would never receive the very frame
    /// it needs to adopt its own removal and fail-stop (this was a real
    /// regression caught while adding `genesis_absent` support —
    /// `add_promote_demote_remove_cycle_under_faults` deadlocked on it).
    ///
    /// Also implicitly bounded to real processes: `admitted_ever` is seeded
    /// from `0..n_nodes` and only ever grows from a real config's own
    /// voter/learner ids (see `propose_config`), so a VIRTUAL id with no
    /// backing sim process (e.g. `AddLearner { id: 9, .. }` — "the config
    /// pipeline neither knows nor cares") is never a member of it and is
    /// never indexed into `self.nodes`/`cursors`.
    fn config_peers(&self, node: usize) -> Vec<usize> {
        (0..self.nodes.len()).filter(|&p| p != node && self.admitted_ever.contains(&p)).collect()
    }

    /// Build a node's `ElectionConfig` around the given adopted config — the
    /// genesis config at world construction, the durable mirror at restart.
    fn election_cfg(
        cfg: &SimConfig,
        node: usize,
        config: ClusterConfig,
        config_position: u64,
    ) -> ElectionConfig {
        ElectionConfig {
            id: node as NodeId,
            config,
            config_position,
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
        if let Some(v) = self.pending_violation.take() {
            return Err(v); // ledger minor g: don't drop a parked violation
        }
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
        if let Some(v) = self.pending_violation.take() {
            return Err(v); // ledger minor g: don't drop a parked violation
        }
        while self.current_leader().is_none() && self.steps < self.cfg.max_steps {
            if self.term_map_cap_reached() || !self.step_once()? {
                break;
            }
        }
        Ok(())
    }

    /// Step until `pred` holds (or the budget runs out). `Ok(true)` iff the
    /// predicate held; `Ok(false)` = budget/queue/cap exhausted first
    /// (ledger minor x: the old `Ok(())` let scenarios silently "pass"
    /// phases that had timed out).
    pub fn run_until(
        &mut self,
        mut pred: impl FnMut(&World) -> bool,
    ) -> Result<bool, InvariantViolation> {
        if let Some(v) = self.pending_violation.take() {
            return Err(v); // ledger minor g: don't drop a parked violation
        }
        while !pred(self) && self.steps < self.cfg.max_steps {
            if self.term_map_cap_reached() || !self.step_once()? {
                break;
            }
        }
        Ok(pred(self))
    }

    /// T13: step until `pred` holds, or `duration_ns` of VIRTUAL TIME has
    /// elapsed from the current instant (the step-count budget is still the
    /// ultimate backstop). Distinct from [`World::run_until`] — which is
    /// bounded only by the step count — because the crypto scenarios need a
    /// wall-clock-shaped deadline ("handshakes must converge within N
    /// seconds"), not a step count, and changing `run_until`'s own signature
    /// would touch every existing call site in this crate for no reason.
    /// Same `Ok(bool)` timeout contract as `run_until` (ledger minor x): a
    /// timeout is `Ok(false)`, never silently swallowed.
    pub fn run_until_within(
        &mut self,
        mut pred: impl FnMut(&World) -> bool,
        duration_ns: u64,
    ) -> Result<bool, InvariantViolation> {
        if let Some(v) = self.pending_violation.take() {
            return Err(v); // ledger minor g: don't drop a parked violation
        }
        let deadline = self.now.saturating_add(duration_ns);
        while !pred(self) {
            if self.steps >= self.cfg.max_steps || self.term_map_cap_reached() {
                break;
            }
            let Some(Reverse(next)) = self.queue.peek() else {
                break;
            };
            if next.time > deadline {
                break;
            }
            if !self.step_once()? {
                break;
            }
        }
        Ok(pred(self))
    }

    /// T13: run events until `duration_ns` of virtual time has elapsed from
    /// the current instant (or the step budget runs out) — for scenarios
    /// that need to let a scripted window of time pass (e.g. past
    /// `uc2_crypto::group::ACTIVATION_TIMEOUT_NS`) without a specific
    /// predicate to wait on. If the queue's next event already lands past
    /// the deadline, this is a no-op (the sim has nothing to do in the
    /// window — every crypto scenario keeps periodic ticks running, so this
    /// is not the common case).
    pub fn run_for(&mut self, duration_ns: u64) -> Result<(), InvariantViolation> {
        if let Some(v) = self.pending_violation.take() {
            return Err(v);
        }
        let deadline = self.now.saturating_add(duration_ns);
        loop {
            if self.steps >= self.cfg.max_steps || self.term_map_cap_reached() {
                break;
            }
            let Some(Reverse(next)) = self.queue.peek() else {
                break;
            };
            if next.time > deadline {
                break;
            }
            if !self.step_once()? {
                break;
            }
        }
        Ok(())
    }

    /// Step at most `k` events (bounded also by the global budget).
    pub fn run_steps(&mut self, k: u64) -> Result<(), InvariantViolation> {
        if let Some(v) = self.pending_violation.take() {
            return Err(v); // ledger minor g: don't drop a parked violation
        }
        let target = self.steps.saturating_add(k);
        while self.steps < target && self.steps < self.cfg.max_steps {
            if self.term_map_cap_reached() || !self.step_once()? {
                break;
            }
        }
        Ok(())
    }

    /// TEST-ONLY: park a violation exactly as the `propose_config` self-feed
    /// does at world.rs:1803 (a violation raised inside a scripted call whose
    /// signature cannot return it), without needing to stage a real
    /// inv9-triggering config trace. Used to prove ledger minor (g) — a
    /// parked violation must not be silently dropped by a `run`/`run_until*`
    /// call whose predicate/budget is already satisfied on entry — against
    /// the exact code path the entry checks guard, without depending on a
    /// scenario that only an actual SM bug would ever produce.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-support"))]
    pub fn test_only_park_violation(&mut self, v: InvariantViolation) {
        self.pending_violation = Some(v);
    }

    /// Pop and process one event, then run the post-event invariant sweep.
    /// Returns `false` when the queue is empty.
    fn step_once(&mut self) -> Result<bool, InvariantViolation> {
        // A violation raised inside a scripted call that could not return it
        // (M7 `propose_config` self-feed) surfaces before anything else runs.
        if let Some(v) = self.pending_violation.take() {
            return Err(v);
        }
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
        // inv6 — config determinism, swept like inv2: every settled node's
        // adopted config must be backed by its own log (durable frontier, or
        // the append frontier for the adopt-at-append window). Nodes with a
        // truncation in flight are exempt — the mid-window state (pruned map
        // adopted, physical cut pending) is a legitimate transient, and inv8
        // re-checks the settled state at the ack. Down/halted nodes are frozen.
        for i in 0..self.cfg.n_nodes {
            let nd = &self.nodes[i];
            if !nd.up || nd.truncating {
                continue;
            }
            let adopted = nd.sm.config().clone();
            let implied_d = self.implied_config_at(i, self.nodes[i].durable).clone();
            let implied_a = self.implied_config_at(i, self.nodes[i].append).clone();
            self.checker.check_config_determinism(
                i as NodeId,
                &adopted,
                &implied_d,
                &implied_a,
                step,
            )?;
        }
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
    /// `epoch` (T13) is stamped straight onto the message — see
    /// `World::data_seal_gate`, the only caller that computes a non-`None`
    /// value.
    fn make_data(
        &self,
        node: usize,
        from: u64,
        append: u64,
        term: u32,
        epoch: Option<u16>,
    ) -> Msg {
        let map = &self.nodes[node].term_map;
        let to = match next_boundary(map, from) {
            Some(b) if b < append => b,
            _ => append,
        };
        let seg_term = term_at(map, from);
        let prev_term = if from == 0 { 0 } else { term_at(map, from - 1) };
        Msg::Data { term, seg_term, from_pos: from, to_pos: to, prev_term, epoch }
    }

    /// T13: what `node` may do with an outgoing `DATA` send right now (see
    /// `SealGate`). Pure — does not mutate `GroupPlane` state (activation
    /// reconciliation happens inside `GroupPlane::mint`, exactly as it does
    /// on the send-key side of the real crate).
    fn data_seal_gate(&self, node: usize, now: u64) -> SealGate {
        match &self.nodes[node].crypto {
            None => SealGate::NotGated,
            Some(c) => match c.group.sealing_epoch(now) {
                Some(e) => SealGate::Sealed(e),
                None => SealGate::Withhold,
            },
        }
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
            SimEvent::ConsensusStep { node } => self.on_consensus_step(node, now, step),
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
            // T13: a crypto-enabled leader withholds DATA until a group epoch
            // has activated (mint + ack-or-timeout) — see `SealGate`. A
            // crypto-disabled node is `NotGated` and this is unconditionally
            // true, so the pre-M8 behavior is untouched.
            let gate = self.data_seal_gate(node, now);
            if gate != SealGate::Withhold {
                let epoch = match gate {
                    SealGate::Sealed(e) => Some(e),
                    _ => None,
                };
                let ap = self.nodes[node].append;
                let term = self.nodes[node].sm.current_term();
                for p in self.config_peers(node) {
                    let from = self.nodes[node].cursors[p].min(ap);
                    let msg = self.make_data(node, from, ap, term, epoch);
                    self.send(node, p, msg, now);
                }
            }
        }

        // T13: crypto-plane maintenance — the pairwise handshake driver's own
        // tick (retransmit backoff, allowlist reload, `pending` TTL expiry)
        // plus the group-key unacked-peer redelivery sweep (the T12 fix for
        // `GroupPlane::mint`'s single-shot `HS_KEY` delivery — see
        // `CRYPTO_SWEEP_INTERVAL_NS`). A no-op for a crypto-disabled node.
        let mut crypto_actions: Vec<HandshakeAction> = Vec::new();
        if let Some(crypto) = self.nodes[node].crypto.as_mut() {
            crypto_actions.extend(crypto.peers.tick(now));
            if now >= crypto.next_sweep_ns {
                crypto.next_sweep_ns = now + CRYPTO_SWEEP_INTERVAL_NS;
                let unacked = crypto.group.unacked_peers();
                crypto_actions.extend(crypto.group.redeliver_to(&unacked));
            }
        }
        if !crypto_actions.is_empty() {
            self.dispatch_handshake_actions(node, crypto_actions, now);
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
                // Issue #7: the SM is NOT fed here. Advancing the counter and
                // telling the consensus agent about it are two different
                // threads' work in the real node, and fusing them was what made
                // the dual-reader skew — and the acked-write loss it enables —
                // unreachable in this world. `SimEvent::ConsensusStep` absorbs
                // it on its own cadence; the report below deliberately reads the
                // COUNTER, as `uc2_net`'s receiver does.

                // M7 adopt-at-durable: the archive frame-scan surfaces config
                // frames whose END the fresh durable just crossed (and whose
                // bytes this node genuinely holds).
                self.observe_config_frames(node, now, step)?;
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
                    //   Mechanism (real intake gate): reports ride the report
                    //             FLOOR below (every archive step while the gate
                    //             is open, advance or not — the receiver.rs
                    //             20 ms floor mirror), not this advance-
                    //             triggered send; a closed gate suppresses the
                    //             report ENTIRELY — never clamped.
                    let reportable = match self.cfg.data_plane {
                        DataPlane::Gated => Some(new_durable.min(self.nodes[node].matched)),
                        DataPlane::RawM3 => Some(new_durable),
                        DataPlane::Mechanism { .. } => None,
                    };
                    if let Some(durable) = reportable {
                        self.send(node, leader, Msg::Report { from: id, term, durable }, now);
                    }
                }
            }
        }
        // Mechanism report FLOOR (mirrors `uc2_net/src/receiver.rs` 1052-1078):
        // the real receiver re-sends its AppendPosition on a 20 ms floor while
        // the intake gate is OPEN — WITHOUT requiring a durable advance.
        // Modeled on the archive cadence. This is the datagram that races the
        // leader's 100 ms idle map re-ship, and the delivery vehicle for
        // Finding #5 (lean gate doc 2026-07-16): a REBOOTED node's durable
        // never advances until it re-accepts data, so an advance-triggered-only
        // report model could not express the boot-open-gate phantom report at
        // all. Gate closed ⇒ suppressed ENTIRELY (never clamped), exactly like
        // the advance path.
        if self.nodes[node].up
            && !self.nodes[node].truncating
            && matches!(self.cfg.data_plane, DataPlane::Mechanism { .. })
            && self.nodes[node].intake_gate
            && let Some(leader) = self.nodes[node].leader_hint
            && leader != node
        {
            let (id, term, durable) =
                (self.nodes[node].id, self.nodes[node].sm.current_term(), self.nodes[node].durable);
            self.send(node, leader, Msg::Report { from: id, term, durable }, now);
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

    /// Issue #7: the consensus agent's duty cycle. Mirrors `Consensus::do_work`
    /// step 2 in `uc2_node` — poll the durable counter, feed `DurableAdvanced`
    /// on change. This is the ONLY path by which `ElectionSm` learns its own
    /// durable position, so everything the SM decides with it (`log_ok` grants,
    /// `start_election`'s advertised credential, `become_leader`'s base,
    /// `rank_leader`'s own-durable clamp) now rides a schedule independent of
    /// the archive's.
    fn on_consensus_step(&mut self, node: usize, now: u64, step: u64) -> Result<(), InvariantViolation> {
        if self.nodes[node].up {
            self.absorb_durable(node, now, step)?;
        }
        self.push(SimEvent::ConsensusStep { node }, now + self.cfg.consensus_step_ns);
        Ok(())
    }

    /// Absorb the durable counter into the SM if it moved. Idempotent, and the
    /// single implementation behind both the duty-cycle poll and the pre-vote
    /// refresh — the same shape as `uc2_node`'s `refresh_durable`.
    fn absorb_durable(&mut self, node: usize, now: u64, step: u64) -> Result<(), InvariantViolation> {
        let d = self.nodes[node].durable;
        if d == self.nodes[node].durable_absorbed {
            return Ok(());
        }
        self.nodes[node].durable_absorbed = d;
        self.feed(node, Event::DurableAdvanced { durable: d }, now, step)
    }

    fn on_restart(&mut self, node: usize, now: u64) -> Result<(), InvariantViolation> {
        if self.nodes[node].up || self.nodes[node].halted {
            return Ok(()); // already recovered — or removed for good (M7)
        }
        // M7 boot-revert (the recovery half of the node's revert obligation):
        // the durable record must never claim a config position ABOVE the
        // recovered log — a leader that adopted at APPEND and crashed before its
        // archive covered the frame recovers a log that ends below the record's
        // position, and boots on the record's prev level instead. Skipped in the
        // revert-disabled counterfactual (the same deleted guard).
        if !self.cfg.revert_on_truncate_disabled
            && self.nodes[node].cfg_cur_pos > self.nodes[node].durable
        {
            let nd = &mut self.nodes[node];
            nd.cfg_cur = nd.cfg_prev.clone();
            nd.cfg_cur_pos = nd.cfg_prev_pos;
        }
        let cfg = Self::election_cfg(
            &self.cfg,
            node,
            self.nodes[node].cfg_cur.clone(),
            self.nodes[node].cfg_cur_pos,
        );
        let vote = self.nodes[node].vote;
        let term_map = self.nodes[node].term_map.clone();
        let durable = self.nodes[node].durable;
        let mut sm = ElectionSm::new(cfg, vote, &term_map, durable, now);
        // Restore the record's PREV level (construction seeds prev == cur), so a
        // post-restart truncation below the config frame still reverts to the
        // genuine predecessor.
        sm.restore_prev_config(self.nodes[node].cfg_prev.clone(), self.nodes[node].cfg_prev_pos);
        if self.cfg.revert_on_truncate_disabled {
            sm.set_revert_on_truncate(false); // counterfactual persists across restarts
        }
        let nd = &mut self.nodes[node];
        nd.sm = sm;
        nd.up = true;
        // Issue #7: a fresh `ElectionSm` is seeded FROM `durable`, so the
        // consensus agent's shadow starts in step with it — no phantom "advance"
        // on the first post-restart poll.
        nd.durable_absorbed = durable;
        nd.append = durable;
        nd.commit = 0;
        nd.truncating = false;
        nd.new_term_pos = None;
        nd.leader_hint = None;
        nd.matched = 0;
        for c in &mut nd.cursors {
            *c = 0;
        }
        // Rebuilt receiver (mirrors uc2_node boot). Finding #5 (lean gate doc
        // 2026-07-16, leader-completeness effort): boot-open gate + persisted
        // vote over an unreconciled divergent tail = phantom commit; boot
        // closed iff vote_term > map_term — reconcile must complete before
        // this node's reports may certify. Recovery is `max(vote, map)`
        // (`ElectionSm::new`), so a voter that granted term T and crashed
        // before reconciling reboots AT term T over a tail no T-leader
        // validated; the report floor would certify it into a phantom commit
        // (the `rebooted_unreconciled_voter_must_not_certify_phantom_commit`
        // pin turns RED if this boots unconditionally open). Reopen rides the
        // existing arms (clean-reconcile / truncate-ack / BecomeLeader).
        // Adopted term = the recovered term.
        let vote_term = vote.map(|(t, _)| t).unwrap_or(0);
        let map_term = nd.term_map.last().map(|&(t, _)| t).unwrap_or(0);
        nd.intake_gate = vote_term <= map_term;
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
        if let DataPlane::Mechanism { handle_keyed, .. } = self.cfg.data_plane {
            if let Some(t) = nd.pending_trunc_to.take() {
                nd.durable = t;
                nd.append = t;
            }
            self.record_committed(node);
            // Reconciliation for this term is complete (durable clamped to the
            // consistent truncation point, pruned map adopted): reopen the gate —
            // UNLESS (Finding #9, lean LC2) this is a CANDIDATE whose handle lags
            // its bumped `current_term`. A candidate's truncating reconcile
            // settling must not reopen its stale handle-stream intake; it stays
            // closed until the candidate resolves (win / step-down / adopt).
            let handle_ok = !handle_keyed
                || self.nodes[node].sm.current_term() == self.nodes[node].adopted_term;
            if handle_ok {
                self.reopen_gate(node, now);
            }
        }
        // inv8 — revert correctness, pinned at the exact point the truncation
        // SETTLES (durable clamped, map adopted, config reverted/kept per spec
        // §5): the adopted config must re-equal the frontier-implied config.
        // This is the check the `revert_on_truncate_disabled` counterfactual
        // turns red.
        let implied = self.implied_config_at(node, self.nodes[node].durable).clone();
        let adopted = self.nodes[node].sm.config().clone();
        self.checker.check_revert_correctness(node as NodeId, &adopted, &implied, step)?;
        Ok(())
    }

    /// M7: feed `Event::ConfigObserved` for every ledger frame whose END is at
    /// or below the node's durable AND whose bytes the node genuinely holds
    /// (content identity: the node's lineage at the frame's last byte is the
    /// appending term — spec §6). This is the sim's archive frame-scan: the
    /// ONLY follower adoption path (adopt-at-durable), and also what RE-adopts
    /// a config after a truncation + refill re-crosses a surviving frame. The
    /// SM's version monotonicity makes re-feeding idempotent; the version
    /// filter here just avoids feed spam.
    fn observe_config_frames(
        &mut self,
        node: usize,
        now: u64,
        step: u64,
    ) -> Result<(), InvariantViolation> {
        let durable = self.nodes[node].durable;
        let cur_version = self.nodes[node].sm.config().version;
        let mut due: Vec<(u64, ClusterConfig)> = self
            .config_frames
            .iter()
            .filter(|f| {
                f.end <= durable
                    && f.end > 0
                    && term_at(&self.nodes[node].term_map, f.end - 1) == f.term
                    && f.config.version > cur_version
            })
            .map(|f| (f.end, f.config.clone()))
            .collect();
        due.sort_by_key(|(end, _)| *end); // ascending: adopt in stream order
        for (end, config) in due {
            self.feed(node, Event::ConfigObserved { position: end, config }, now, step)?;
        }
        Ok(())
    }

    /// M7 (inv6/inv8 oracle): the config implied by node `node`'s content at
    /// byte frontier `upto` — the highest-version ledger frame with `end <=
    /// upto` whose bytes the node genuinely holds (content identity), else the
    /// node's position-0 baseline: its own fiat config when the durable record
    /// sits at position 0 (genesis, a wipe's config-by-fiat, or a revert to
    /// genesis), the world genesis otherwise.
    fn implied_config_at(&self, node: usize, upto: u64) -> &ClusterConfig {
        let nd = &self.nodes[node];
        let mut best: &ClusterConfig =
            if nd.cfg_cur_pos == 0 { &nd.cfg_cur } else { &self.genesis_config };
        for f in &self.config_frames {
            if f.end <= upto
                && f.end > 0
                && term_at(&nd.term_map, f.end - 1) == f.term
                && f.config.version > best.version
            {
                best = &f.config;
            }
        }
        best
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

    // ------------------------------------------------------- T13: crypto plane

    /// Feeds a received handshake/rotation datagram (kind 18/19/20) into
    /// `to`'s crypto plane and dispatches whatever it emits. A no-op if `to`
    /// is not crypto-enabled (a stray `Msg::Handshake` can only originate
    /// from a crypto-enabled sender, so this is defensive, not a live path).
    fn deliver_handshake(&mut self, to: usize, from: usize, kind: u8, body: Vec<u8>, now: u64) {
        let from_id = self.nodes[from].id;
        let Some(crypto) = self.nodes[to].crypto.as_mut() else {
            return;
        };
        // `HS_KEY` (20) rides the pairwise channel but is `GroupPlane`'s
        // message, not `Peers`' — `Peers::on_message` deliberately ignores
        // it (see `handshake.rs`'s `on_message` doc); the node layer is what
        // splits the two, and this mirrors that split.
        let actions = if kind == DGRAM_KIND_HS_KEY {
            crypto.group.on_key_message(from_id, &body)
        } else {
            crypto.peers.on_message(from_id, kind, &body, now)
        };
        self.dispatch_handshake_actions(to, actions, now);
    }

    /// Executes a batch of [`HandshakeAction`]s emitted by `node`'s `Peers`
    /// or `GroupPlane`. `Send` rides the SAME lossy/partitionable
    /// `send`/`deliver` pipe as every other message (kinds 18/19/20), with
    /// one scripted exception applied at the "socket" (before the fault
    /// dice): every `HS_KEY` addressed to a node currently inside its
    /// [`World::block_key_delivery_to`] window is dropped, for as long as
    /// that window is open. `Established`/`Failed` need no world-level
    /// bookkeeping — session liveness is queried directly off
    /// `Peers`/`GroupPlane` by the accessors below, not shadowed here.
    fn dispatch_handshake_actions(&mut self, node: usize, actions: Vec<HandshakeAction>, now: u64) {
        for act in actions {
            if let HandshakeAction::Send { to, kind, body } = act {
                let to_idx = to as usize;
                if kind == DGRAM_KIND_HS_KEY
                    && self.key_delivery_blocked_until.get(&to_idx).is_some_and(|&d| now < d)
                {
                    continue; // dropped at the "socket" — inside the blocked window
                }
                self.send(node, to_idx, Msg::Handshake { kind, body }, now);
            }
        }
    }

    /// The mint half of [`World::rotate_group_key`], split out so
    /// `Action::BecomeLeader`'s auto-mint can call it without going back
    /// through `current_leader()`.
    fn mint_group_key_on(&mut self, node: usize) {
        if self.nodes[node].crypto.is_none() {
            return;
        }
        let now = self.now;
        let peers: Vec<u32> = self.config_peers(node).iter().map(|&p| p as u32).collect();
        let crypto = self.nodes[node].crypto.as_mut().unwrap();
        let (_epoch, actions) = crypto.group.mint(&peers, now);
        // Push this node's own redelivery-sweep schedule a fresh interval
        // out from THIS mint: a reasonable real policy on its own (no point
        // re-checking for unacked peers immediately after just delivering
        // everyone the newest key). With `CRYPTO_SWEEP_INTERVAL_NS` fast
        // (50ms) this is a minor phase-alignment nicety rather than the
        // load-bearing determinism guard it was when the sweep was slow —
        // `World::block_key_delivery_to`'s explicit time window is what
        // scripted scenarios rely on now.
        crypto.next_sweep_ns = now + CRYPTO_SWEEP_INTERVAL_NS;
        self.dispatch_handshake_actions(node, actions, now);
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
            Msg::Data { term, seg_term, from_pos, to_pos, prev_term, epoch } => {
                // T13: crypto-plane gate — checked FIRST, before ANYTHING else
                // touches state. `epoch` is only ever `Some` when the sending
                // leader is crypto-enabled (see `data_seal_gate`); a receiver
                // that has not installed that epoch cannot open the real
                // AEAD-sealed datagram this message stands in for. AEAD tag
                // verification covers the WHOLE frame (the v2 header is
                // authenticated as associated data — `uc2_protocol::v2`'s
                // sealed-datagram layout), so a failed open means NOTHING in
                // this message — not even `term` — is trustworthy yet; this
                // must behave exactly like the datagram never arrived at all
                // (zero side effects), not like "arrived but unusable." A
                // real network-level drop (the `send()` dice) already has
                // this shape for free (the whole event is never delivered);
                // moving the crypto gate to the top gives it the same shape
                // deliberately — putting it AFTER `leader_hint`/`LeaderSeen`
                // would credit an unauthenticated claim with live-leader
                // liveness effects, which was a genuine bug caught here (see
                // the task report for the reproduction: at high loss, an
                // early crypto-gate ordering let a node adopt term liveness
                // from a datagram it could not open, producing a real inv2
                // term-map divergence — `w.enable_crypto_plane` had no
                // effect on the SAME seed/config without this ordering bug).
                if let Some(epoch) = epoch {
                    let openable = self.nodes[to]
                        .crypto
                        .as_ref()
                        .is_some_and(|c| c.group.schedule().get(epoch).is_some());
                    if !openable {
                        // Exactly like a lost datagram, EXCEPT the follower can
                        // tell it happened (unlike a plain loss, which is
                        // invisible to it) and asks the leader to repair the
                        // gap: the SAME `Msg::Nak` request an ordinary
                        // lost-datagram gap would produce in the real
                        // reliable-UDP receiver. No new recovery mechanism —
                        // `uc2_crypto::group`'s own module doc is explicit this
                        // must self-heal through the existing repair path once
                        // `HS_KEY` lands, never a bespoke one.
                        self.nodes[to].nak_sent += 1;
                        let (id, want_from) = (self.nodes[to].id, self.nodes[to].append);
                        self.send(to, from, Msg::Nak { from: id, want_from }, now);
                        return Ok(());
                    }
                }
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
            Msg::CommitGossip { term, commit, epoch } => {
                // T13: same crypto gate as Msg::Data, checked FIRST and with
                // the same zero-side-effect shape (see the long comment on
                // the Data arm above for why the ordering matters) — an
                // unopenable Group-scope datagram is dropped BEFORE
                // leader_hint/feed, exactly like a lost datagram. No Nak
                // here: CommitGossip carries no byte range to repair, and
                // the leader re-ships it continuously (the idle gossip
                // floor), so it self-heals the same way a lost CommitGossip
                // already does today.
                if let Some(epoch) = epoch {
                    let openable = self.nodes[to]
                        .crypto
                        .as_ref()
                        .is_some_and(|c| c.group.schedule().get(epoch).is_some());
                    if !openable {
                        return Ok(());
                    }
                }
                self.nodes[to].leader_hint = Some(from);
                self.feed(to, Event::CommitGossip { term, commit }, now, step)
            }
            Msg::RequestVote { from: cand, new_term, last_term, last_durable } => {
                // Issue #7 / main 26d4827: re-absorb the counter IMMEDIATELY
                // before the grant decision, as `feed_net` does. Raft's vote rule
                // is sound only if a voter judges a candidate against everything
                // the voter has DURABLY STORED — the counter, not a copy of it
                // taken at the last duty cycle. Granting on an under-estimate of
                // our own log is the unsafe direction: it lets a candidate behind
                // a committed position win and collapse below it.
                //
                // `vote_refresh_durable: false` ablates this and reproduces the
                // pre-fix system.
                // Non-vacuity instrumentation: this vote is being answered while
                // the counter is AHEAD of the consensus agent's absorbed copy —
                // i.e. the dual-reader skew is live at the moment of the grant
                // decision. A directed test that never trips this counter proved
                // nothing about issue #7, whatever else it caught.
                if self.nodes[to].durable > self.nodes[to].durable_absorbed {
                    self.stat_stale_vote_window += 1;
                }
                if self.cfg.vote_refresh_durable {
                    self.absorb_durable(to, now, step)?;
                }
                self.feed(
                    to,
                    Event::RequestVote { from: cand, new_term, last_term, last_durable },
                    now,
                    step,
                )
            }
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
                if let DataPlane::Mechanism { reopen_guard, handle_keyed } = self.cfg.data_plane {
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
                    // Finding #9 (lean LC2): reopen only when the SM's active term
                    // equals the term handle the receiver filters DATA at
                    // (`adopted_term`). A candidate's handle lags its bumped
                    // `current_term` (StartElection stores no handle), so
                    // reconciling a higher-term map must not reopen its stale
                    // handle-stream intake. `handle_keyed: false` is the
                    // counterfactual that leaves the hole open.
                    let handle_ok = !handle_keyed
                        || self.nodes[to].sm.current_term() == self.nodes[to].adopted_term;
                    if !self.nodes[to].intake_gate
                        && !produced_truncate
                        && guard_ok
                        && handle_ok
                        && term >= term_before
                    {
                        self.reopen_gate(to, now);
                    }
                }
                Ok(())
            }
            Msg::Nak { from: acker, want_from } => {
                // T13: gap-repair request — see the crypto gate in the
                // `Msg::Data` arm above. `to` here is the leader the request
                // is addressed to; a stale/former leader simply ignores it
                // (the requester will re-address its next attempt once it
                // adopts the real current leader via a subsequent `Data`).
                if matches!(self.nodes[to].sm.role(), Role::Leader) {
                    let gate = self.data_seal_gate(to, now);
                    if gate != SealGate::Withhold {
                        let epoch = match gate {
                            SealGate::Sealed(e) => Some(e),
                            _ => None,
                        };
                        let ap = self.nodes[to].append;
                        let term = self.nodes[to].sm.current_term();
                        let from_pos = want_from.min(ap);
                        let msg = self.make_data(to, from_pos, ap, term, epoch);
                        self.send(to, acker as usize, msg, now);
                    }
                }
                Ok(())
            }
            Msg::Handshake { kind, body } => {
                self.deliver_handshake(to, from, kind, body, now);
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
                for p in self.config_peers(node) {
                    self.send(
                        node,
                        p,
                        Msg::RequestVote { from, new_term, last_term, last_durable },
                        now,
                    );
                }
            }
            Action::BecomeLeader { term, base } => {
                // Invariants 1 + 7(chain) + 5 at the instant a term opens. The
                // leader's own SM map already holds the freshly-opened (term,
                // base); inv5 checks it against the committed lineage (ground
                // truth), so a divergent peer can't excuse an incomplete leader.
                // inv7's chain half checks the winner's (adopted, prev) config
                // pair against the committed config — the disjoint-quorum
                // election guard (M7).
                let leader_map = self.nodes[node].sm.term_map().to_vec();
                let adopted = self.nodes[node].sm.config().clone();
                let prev = self.nodes[node].cfg_prev.clone();
                self.checker.on_become_leader(
                    node as NodeId,
                    term,
                    base,
                    &leader_map,
                    &adopted,
                    &prev,
                    step,
                )?;
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
                // T13: gated exactly like the on_tick replication loop — a
                // crypto-enabled leader that has never minted has nothing to
                // seal this frame under yet (the auto-mint below fires in
                // this SAME action batch, so the very next tick's resend
                // picks it up once activated).
                let gate = self.data_seal_gate(node, now);
                if gate != SealGate::Withhold {
                    let epoch = match gate {
                        SealGate::Sealed(e) => Some(e),
                        _ => None,
                    };
                    for p in self.config_peers(node) {
                        let msg = self.make_data(node, base, pos, term, epoch);
                        self.send(node, p, msg, now);
                    }
                }
                // Intake gate (Mechanism): a leader is the source of truth — gate
                // open, no reconcile pending (mirrors uc2_node::exec BecomeLeader).
                let nd = &mut self.nodes[node];
                nd.adopted_term = term;
                        nd.intake_gate = true;
                nd.pending_trunc_to = None;

                // T13/T17 parity: the real node layer mints a fresh group-key
                // epoch on every BecomeLeader (`uc2_crypto::rotation`'s
                // trigger 1). A REAL shipped bug (T17) was a cold-start
                // livelock caused by exactly this call racing the
                // (much shorter) election-timeout retry loop, fixed by
                // `GroupPlane::mint`'s inherited-activation-clock change —
                // mirroring the wiring here lets the sim catch a regression
                // of either that fix or the mint-on-elect call itself.
                self.mint_group_key_on(node);
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
                // M7 (inv7): the quorum is ranked over the ADOPTING node's config
                // VOTERS, and the commit's config must chain off the committed
                // config — see `InvariantChecker::on_advance_commit`.
                let durables: Vec<u64> = self.nodes.iter().map(|nd| nd.durable).collect();
                let maps: Vec<Vec<(u32, u64)>> =
                    self.nodes.iter().map(|nd| nd.term_map.clone()).collect();
                let term = self.nodes[node].sm.current_term();
                let is_leader = matches!(self.nodes[node].sm.role(), Role::Leader);
                let voters = self.nodes[node].sm.config().voter_ids();
                let adopted = self.nodes[node].sm.config().clone();
                let prev = self.nodes[node].cfg_prev.clone();
                let config_position = self.nodes[node].sm.config_position();
                self.checker.on_advance_commit(
                    node as NodeId,
                    commit,
                    term,
                    is_leader,
                    &durables,
                    &maps,
                    &voters,
                    &adopted,
                    &prev,
                    config_position,
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
                // T13: gated exactly like on_tick's DATA replication loop —
                // production classifies COMMIT_POSITION as Scope::Group too
                // (see Msg::CommitGossip's doc).
                let gate = self.data_seal_gate(node, now);
                if gate != SealGate::Withhold {
                    let epoch = match gate {
                        SealGate::Sealed(e) => Some(e),
                        _ => None,
                    };
                    let term = self.nodes[node].sm.current_term();
                    for p in self.config_peers(node) {
                        self.send(node, p, Msg::CommitGossip { term, commit, epoch }, now);
                    }
                }
            }
            Action::ShipTermMap { entries } => {
                let term = self.nodes[node].sm.current_term();
                for p in self.config_peers(node) {
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

                // M7 persist-revert-BEFORE-truncate (spec §5): the NODE's
                // obligation — the durable ConfigRecord (our mirror) is
                // reverted at EXEC time, before the physical truncation, so a
                // crash anywhere in the window recovers a record consistent
                // with the truncated log. The SM's own state reverts at the
                // matching-epoch ack, which re-emits `ConfigAdopted` with these
                // same values (an idempotent mirror re-write). `to == cfg_cur_pos`
                // preserves the frame — frame-END effect point, no revert.
                // The counterfactual deletes this half of the guard too.
                if !self.cfg.revert_on_truncate_disabled && to < self.nodes[node].cfg_cur_pos {
                    let nd = &mut self.nodes[node];
                    if to == 0 {
                        // Wipe: config-by-fiat — keep cur, reset the record to
                        // position 0 with prev == cur.
                        nd.cfg_cur_pos = 0;
                        nd.cfg_prev = nd.cfg_cur.clone();
                        nd.cfg_prev_pos = 0;
                    } else {
                        nd.cfg_cur = nd.cfg_prev.clone();
                        nd.cfg_cur_pos = nd.cfg_prev_pos;
                    }
                }

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
            Action::ConfigAdopted { position, config, prev_position, prev } => {
                // inv9 — tombstone permanence, judged on EVERY adoption
                // (forward, revert, wipe-fiat).
                self.checker.on_config_adopted(node as NodeId, &config, &prev, step)?;
                // The sim's durable ConfigRecord mirror (cur + prev): the
                // node-obligation persist. Survives crash; recovered into the
                // SM at restart.
                let nd = &mut self.nodes[node];
                nd.cfg_cur = config;
                nd.cfg_cur_pos = position;
                nd.cfg_prev = prev;
                nd.cfg_prev_pos = prev_position;
            }
            // M7 Task 8: `StepDownRemoved` is the leader-mid-self-removal twin
            // of `HaltRemoved` — it fires AFTER the commit crossing (the SM
            // kept the leader serving through the adoption window; the node
            // model here doesn't need to distinguish "removed follower halts
            // at adoption" from "removed leader halts at commit", both reduce
            // to the identical fail-stop model below). Same permanent-park
            // semantics either way: like a crash, but no restart is ever
            // scheduled (`restart()` refuses via the `halted` flag).
            Action::HaltRemoved | Action::StepDownRemoved => {
                // Removed from the cluster: fail-stop, PERMANENTLY — like a
                // crash, but no restart is ever scheduled and `restart()`
                // refuses (the `halted` flag). Volatile state is torn down the
                // same way `do_crash` does it.
                let nd = &mut self.nodes[node];
                nd.halted = true;
                nd.up = false;
                nd.append = nd.durable;
                nd.commit = 0;
                nd.truncating = false;
                nd.new_term_pos = None;
                nd.leader_hint = None;
                nd.matched = 0;
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

    // ---------------------------------------------------- T13: crypto plane

    /// Turns on the crypto plane for the first `n` nodes (normally
    /// `cfg.n_nodes`). Builds real on-disk X25519 identities and a shared
    /// allowlist under a per-seed scratch directory on real disk (never
    /// `/tmp` — RAM-backed tmpfs, no swap on the dev box; CLAUDE.md), wires a
    /// `Peers` + `GroupPlane` per node, and has every node initiate a
    /// handshake to every other. `HS_INIT`/`HS_RESP`/`HS_KEY` (kinds
    /// 18/19/20) then ride the SAME lossy/partitionable `send`/`deliver`
    /// path as every other message — see `Msg::Handshake`.
    ///
    /// The generated key BYTES are deterministic (derived from `cfg.seed`
    /// and the node index via this module's own `XorShift64`), so a seed's
    /// *schedule* — ordering, loss, reorder, partition, and the timer
    /// domain — replays byte-for-byte across runs. The Noise handshake's own
    /// ephemeral keys (`snow`, via the OS RNG) and `GroupPlane::mint`'s fresh
    /// group-key material do NOT reproduce byte-for-byte even so — this sim
    /// deliberately does not depend on that; see the task report for why
    /// `snow::Builder::fixed_ephemeral_key_for_testing_only` was not used.
    ///
    /// # Panics
    /// If `n` exceeds `cfg.n_nodes`, or the scratch-directory I/O fails.
    pub fn enable_crypto_plane(&mut self, n: usize) {
        assert!(
            n <= self.cfg.n_nodes,
            "enable_crypto_plane: n ({n}) exceeds the world's node count ({})",
            self.cfg.n_nodes
        );
        let dir = crypto_scratch_dir();

        let mut identities = Vec::with_capacity(n);
        for i in 0..n {
            let key_path = dir.join(format!("node-{i}.key"));
            std::fs::write(&key_path, deterministic_key_bytes(self.cfg.seed, i as u64))
                .expect("sim crypto scratch: write key file");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))
                    .expect("sim crypto scratch: chmod key file");
            }
            identities.push(Identity::load(&key_path).expect("sim-generated key must load"));
        }

        let allow_path = dir.join("allowlist");
        let mut text = String::new();
        for (i, id) in identities.iter().enumerate() {
            text.push_str(&format!("{i} {}\n", BASE64.encode(id.public_bytes())));
        }
        std::fs::write(&allow_path, text).expect("sim crypto scratch: write allowlist");

        for (i, identity) in identities.into_iter().enumerate() {
            let allowlist =
                Allowlist::load(&allow_path).expect("sim-generated allowlist must load");
            let boot_salt = BootSalt(deterministic_salt_bytes(self.cfg.seed, i as u64));
            self.nodes[i].crypto = Some(NodeCrypto {
                peers: Peers::new(identity, allowlist, i as u32, boot_salt),
                group: GroupPlane::new(i as u32),
                next_sweep_ns: 0,
            });
        }

        let now = self.now;
        for i in 0..n {
            for p in 0..n {
                if p == i {
                    continue;
                }
                let actions =
                    self.nodes[i].crypto.as_mut().unwrap().peers.initiate(p as u32, now);
                self.dispatch_handshake_actions(i, actions, now);
            }
        }
    }

    /// True once every crypto-enabled node's pairwise session with every
    /// other crypto-enabled node is established, in BOTH directions
    /// (`Peers::is_established` from each side — a one-directional check
    /// would pass for the WireGuard-style `current`/`pending` split even
    /// when only one side has promoted).
    pub fn all_peer_sessions_established(&self) -> bool {
        let ids: Vec<usize> =
            (0..self.nodes.len()).filter(|&i| self.nodes[i].crypto.is_some()).collect();
        ids.iter().all(|&i| {
            ids.iter().all(|&j| {
                i == j
                    || self.nodes[i]
                        .crypto
                        .as_ref()
                        .is_some_and(|c| c.peers.is_established(j as u32))
            })
        })
    }

    /// The current leader's most-recently-minted group-key epoch — the
    /// epoch `World::rotate_group_key` last produced, or its own
    /// auto-minted-on-`BecomeLeader` epoch if `rotate_group_key` was never
    /// called. `0` (the wire's reserved cleartext sentinel — never a real
    /// mint, see `GroupPlane::new`) if there is no current leader, or it
    /// isn't crypto-enabled, or it has never minted.
    pub fn current_epoch(&self) -> u16 {
        let Some(leader) = self.current_leader() else {
            return 0;
        };
        self.nodes[leader]
            .crypto
            .as_ref()
            .and_then(|c| c.group.schedule().current())
            .map(|(e, _)| e)
            .unwrap_or(0)
    }

    /// Whether `node` has installed `epoch` into its group-key schedule
    /// (openable — not necessarily the epoch it currently SEALS under; see
    /// `sealing_epoch` vs `schedule` in `uc2_crypto::group::GroupPlane`).
    /// `false` if `node` is not crypto-enabled.
    pub fn node_has_group_epoch(&self, node: usize, epoch: u16) -> bool {
        self.nodes[node]
            .crypto
            .as_ref()
            .is_some_and(|c| c.group.schedule().get(epoch).is_some())
    }

    /// Drops every `HS_KEY` (kind 20) delivery addressed to `node` while
    /// `self.now < until_ns` — applied at the "socket," before the fault
    /// dice, so it is deterministic regardless of `drop_per_million`.
    /// Models a key-distribution gap of a specific, scripted DURATION (the
    /// T12 regression this task's brief calls out is the `until_ns ==
    /// self.now` / "just the next one" special case) without perturbing
    /// the general loss model. A later call replaces any earlier deadline
    /// for the same node (does not stack); pass an already-past `until_ns`
    /// to clear a block early.
    pub fn block_key_delivery_to(&mut self, node: usize, until_ns: u64) {
        self.key_delivery_blocked_until.insert(node, until_ns);
    }

    /// The current leader (or node 0, if none is serving — e.g. before the
    /// first election) mints a fresh group-key epoch and fans out `HS_KEY`
    /// to every other admitted node, through the normal send/deliver path.
    /// Mirrors the node layer: only the leader ever mints
    /// (`uc2_crypto::rotation`). A no-op if the minter is not
    /// crypto-enabled.
    pub fn rotate_group_key(&mut self) {
        let node = self.current_leader().unwrap_or(0);
        self.mint_group_key_on(node);
    }

    /// The current leader's durable (fsync'd) position; `0` if there is no
    /// current leader.
    pub fn leader_durable(&self) -> u64 {
        self.current_leader().map(|l| self.nodes[l].durable).unwrap_or(0)
    }

    /// Directly advances the current leader's append (write) position by
    /// `bytes`, letting the normal per-tick replication loop carry it to
    /// followers — through the crypto gate, if enabled — exactly like an
    /// ordinary client write growing the log. A no-op if there is no
    /// current leader.
    pub fn append_and_replicate(&mut self, bytes: u64) {
        if let Some(leader) = self.current_leader() {
            self.nodes[leader].append += bytes;
        }
    }

    /// How many `Msg::Nak` gap-repair requests `node` has SENT so far — the
    /// sim's model of the existing reliable-UDP NAK-repair path (see the
    /// crypto gate in `deliver`'s `Msg::Data` arm). Proves a convergence
    /// went through that path specifically, not merely that it eventually
    /// happened some other way.
    pub fn nak_count(&self, node: usize) -> u32 {
        self.nodes[node].nak_sent
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
        // `epoch: None` — this bypasses the network model entirely (a
        // scripted craft), so it also bypasses the T13 crypto gate exactly
        // like the pre-M8 behavior; see `Msg::Data`'s doc.
        let msg = Msg::Data { term, seg_term, from_pos, to_pos, prev_term, epoch: None };
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

    /// M7: propose a membership change on `node`. The SM enforces every
    /// precondition (`NotLeader` / `NotServing` / `ChangePending` / the
    /// structural refusals / promote catch-up with [`PROMOTE_SLACK`]). On
    /// success the leader appends the config frame — [`FRAME`] bytes occupying
    /// real positions in its modeled stream, exactly like data — records it in
    /// the World's frame ledger, and adopts at APPEND (feeding itself
    /// `ConfigObserved{position: frame_end}`); followers observe when their
    /// durable crosses the frame end (`observe_config_frames`). Returns the new
    /// config version.
    ///
    /// `serving_gate_disabled` (counterfactual): a `NotServing` refusal is
    /// OVERRIDDEN — the one-in-flight check and the structural preconditions
    /// are re-applied by hand (only the gate is deleted; the promote catch-up
    /// check is skipped too, unused by the pin) and the op goes through,
    /// modeling a node that ignores the single-server-change precondition.
    pub fn propose_config(&mut self, node: usize, op: ConfigOp) -> Result<u64, ProposeError> {
        if !self.nodes[node].up {
            // A down (or halted-removed) process has no admin path; its SM's
            // frozen Role::Leader must not accept proposals into a dead stream.
            return Err(ProposeError::NotLeader);
        }
        let new_cfg = match self.nodes[node].sm.propose_config(op, PROMOTE_SLACK) {
            Ok(c) => c,
            Err(ProposeError::NotServing) if self.cfg.serving_gate_disabled => {
                if self.nodes[node].sm.config_pending() {
                    return Err(ProposeError::ChangePending);
                }
                self.nodes[node].sm.config().apply(op)?
            }
            Err(e) => return Err(e),
        };
        // T9 integration-catch coverage: admit the new config's FULL
        // membership into `admitted_ever` right away — at proposal time, not
        // at commit or even at adoption — so the very frame that ADMITS a
        // `genesis_absent` id is itself deliverable to it on the next tick
        // (chicken-and-egg otherwise: it can't receive the frame that tells
        // it to start receiving frames). One-way: never removed here even
        // for a `DemoteVoter`/`RemoveLearner`/`RemoveVoter` op, so a target
        // being removed keeps receiving traffic until it adopts its own
        // removal and fail-stops.
        for &(id, _) in new_cfg.voters.iter().chain(new_cfg.learners.iter()) {
            self.admitted_ever.insert(id as usize);
        }
        let version = new_cfg.version;
        let term = self.nodes[node].sm.current_term();
        let nd = &mut self.nodes[node];
        nd.append += FRAME;
        let end = nd.append;
        self.config_frames.push(CfgFrame { term, end, config: new_cfg.clone() });
        // Leader adopt-at-append. The self-feed can raise a violation
        // (inv9 at adoption) that this signature cannot return — park it for
        // the next step.
        self.steps += 1;
        let (now, step) = (self.now, self.steps);
        if let Err(v) =
            self.feed(node, Event::ConfigObserved { position: end, config: new_cfg }, now, step)
        {
            self.pending_violation = Some(v);
        }
        Ok(version)
    }

    /// M7: a node's adopted config version.
    pub fn node_config_version(&self, node: usize) -> u64 {
        self.nodes[node].sm.config().version
    }

    /// M7: true once `node` fail-stopped on adopting a config that removed it.
    pub fn halted_removed(&self, node: usize) -> bool {
        self.nodes[node].halted
    }

    /// M7: true iff `node` is up, `Role::Leader`, REGARDLESS of the serving
    /// gate — the gate-off counterfactual must act in the window before the
    /// NewTerm frame commits, which [`World::current_leader`] deliberately
    /// hides. Per-node (not a global scan): a stale deposed leader keeps its
    /// role while isolated, so scripts must be able to exclude a known-stale
    /// index instead of trusting a lowest-index scan.
    pub fn node_is_raw_leader(&self, node: usize) -> bool {
        self.nodes[node].up && matches!(self.nodes[node].sm.role(), Role::Leader)
    }

    /// M7: true iff `node` is up, `Role::Leader`, AND past the serving gate —
    /// the per-node form of [`World::current_leader`], for scripts that must
    /// exclude a known-stale ex-leader (which keeps role + serving while
    /// isolated and would win a lowest-index scan).
    pub fn node_is_serving_leader(&self, node: usize) -> bool {
        let nd = &self.nodes[node];
        nd.up && matches!(nd.sm.role(), Role::Leader) && nd.sm.can_serve()
    }

    /// A node's current durable (fsync'd) position.
    /// Issue #7 non-vacuity probe: how many `RequestVote`s were answered while
    /// this world had the durable counter ahead of a voter's absorbed copy — the
    /// skew that `vote_refresh_durable` exists to close. Zero means the scenario
    /// never exercised the dual-reader gap, whatever else it may have found.
    pub fn stale_vote_windows(&self) -> u64 {
        self.stat_stale_vote_window
    }

    pub fn node_durable(&self, node: usize) -> u64 {
        self.nodes[node].durable
    }

    /// A node's current append (write) position.
    pub fn node_append(&self, node: usize) -> u64 {
        self.nodes[node].append
    }

    /// A node's current SM term.
    pub fn node_term(&self, node: usize) -> u32 {
        self.nodes[node].sm.current_term()
    }

    /// True iff `node` is up and in `Role::Candidate` (mid-election). Finding #9
    /// scripts need to observe the candidate window (handle lags `current_term`).
    pub fn node_is_candidate(&self, node: usize) -> bool {
        self.nodes[node].up && matches!(self.nodes[node].sm.role(), Role::Candidate)
    }

    /// A node's data-plane term handle shadow (`uc2_node::adopted_term` ==
    /// `term_handle`, the term the receiver filters DATA at). Lags `current_term`
    /// for a candidate. Finding #9 discriminator.
    pub fn node_adopted_term(&self, node: usize) -> u32 {
        self.nodes[node].adopted_term
    }

    /// A node's intake-gate state (`Mechanism` only): `true` = OPEN. Finding #9's
    /// direct observable — the candidate cross-stream accept requires a wrongly
    /// reopened gate.
    pub fn node_intake_gate(&self, node: usize) -> bool {
        self.nodes[node].intake_gate
    }

    /// A node's data-stamped term map (`(term, base)` boundaries). Finding #9
    /// scripts read it to craft a clean-reconciling injected map.
    pub fn node_map(&self, node: usize) -> Vec<(u32, u64)> {
        self.nodes[node].term_map.clone()
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

    /// The world's current virtual-time instant (ns). T13: lets a scripted
    /// scenario compute an ABSOLUTE deadline for e.g.
    /// `World::block_key_delivery_to` relative to "now," without needing
    /// its own separate clock.
    pub fn now(&self) -> u64 {
        self.now
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
