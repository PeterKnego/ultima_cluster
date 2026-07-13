// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Election state machine (spec §6): Raft's safety core over byte positions.
//!
//! Pure, sync, deterministic — events in, actions out, time injected via
//! `Tick`. The SM never does I/O; the persist-before-answer contract is
//! encoded as action *semantics* the driving agent (uc2_net, Task 8) must
//! honor. Contract table:
//!
//! | Action                | Agent obligation                                                                                                              |
//! |-----------------------|-------------------------------------------------------------------------------------------------------------------------------|
//! | `PersistAndSendVote`  | Persist the vote record durably, THEN send the granted vote. Never send before the persist completes. `to == self` ⇒ persist only (self-vote), skip the network send. |
//! | `SendVoteRejection`   | Send a rejection carrying our term. Nothing was promised, so no persistence is required.                                       |
//! | `StartElection`       | Broadcast `RequestVote{new_term, last_term, last_durable}` to peers.                                                           |
//! | `BecomeLeader`        | IN ORDER: (1) append the new `TermMapEntry{term, base}` + persist the term map durably; (2) collapse volatile append to `base` (durable), discarding the unreplicated tail; (3) append the `NewTerm` no-op frame and feed `NewTermAppended` back; (4) switch data-plane roles to leader. |
//! | `BecomeFollower`      | Switch data-plane roles to follower of `term`.                                                                                |
//! | `AdvanceCommit`       | Store the commit counter (agent owns the store; single writer).                                                               |
//! | `GossipCommit`        | Gossip `CommitPosition{commit}` to followers (leader only). Rides commit advances AND the idle re-gossip floor (`gossip_floor_ns`). |
//!
//! Determinism note: election timeouts are re-randomized on every arming (a
//! follower reset on leader activity, or a candidate starting a fresh
//! election) from `[min, max)` via a crate-local xorshift — no dependency.

use crate::commit::CommitTracker;
use crate::config::{ClusterConfig, ConfigOp, ProposeError};
use crate::reconcile::{MAX_TERM_MAP_WIRE_ENTRIES, Outcome, Reconcile, reconcile};

pub type NodeId = u32;

/// Deterministic xorshift64 — no external RNG dependency (copied from
/// `uc2_net::fault` so `uc2_consensus` stays dep-free).
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

pub struct ElectionConfig {
    pub id: NodeId,
    /// The adopted cluster membership (M7): voters + learners + tombstones,
    /// versioned. `members` (the voting set, self included iff a voter) and
    /// `can_vote` are DERIVED from this at construction and again on every
    /// `Event::ConfigObserved` adoption — see `ElectionSm::adopt_config`.
    /// M6 Task 7's learner-candidacy switch (a learner's own id is NOT in the
    /// voting set, so it never occupies a `CommitTracker` slot and its
    /// `Report` is dropped by `follower_slot` — the two halves of "never
    /// affects quorum") now falls out of `config.is_voter(id)`.
    pub config: ClusterConfig,
    /// Recovered frame-end position of `config` (0 at genesis / fresh boot).
    pub config_position: u64,
    pub election_timeout_min_ns: u64, // default 150_000_000
    pub election_timeout_max_ns: u64, // default 300_000_000
    /// Idle re-gossip floor (spec §6): the maximum interval a leader will go
    /// without re-emitting `GossipCommit` + `ShipTermMap`, even when commit is
    /// not advancing. Bounds how long a follower that missed the last
    /// advance-gossip datagram — or a divergent node rejoining an *idle*
    /// cluster — waits to learn commit / reconcile. Default 100_000_000 (100ms).
    pub gossip_floor_ns: u64,
    pub seed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// Time advanced; the ONLY driver of timeouts.
    Tick { now_ns: u64 },
    /// Local durable advanced (from the archive, via the agent).
    DurableAdvanced { durable: u64 },
    /// Any datagram from the current leader observed (data, heartbeat,
    /// commit gossip) — leadership liveness (spec §6).
    LeaderSeen { term: u32 },
    /// AppendPosition report (leader role input to commit ranking).
    Report { from: NodeId, term: u32, durable: u64 },
    /// CommitPosition gossip (follower role input).
    CommitGossip { term: u32, commit: u64 },
    RequestVote { from: NodeId, new_term: u32, last_term: u32, last_durable: u64 },
    Vote { from: NodeId, term: u32, granted: bool },
    /// The NewTerm frame this node appended (leader) reached position P.
    NewTermAppended { position: u64 },
    /// The leader shipped its term map (follower role input; term-filtered like
    /// `CommitGossip`). Runs reconciliation against our own map + durable.
    TermMapReceived { term: u32, entries: Vec<(u32, u64)> },
    /// We durably wrote bytes stamped `term` starting at byte `base` (the
    /// data-plane observing a new term open in the stream we accept). This is
    /// the ONLY way a follower's own term map grows — data-stamped recording:
    /// we record a term precisely because we accepted its data. Idempotent
    /// (ignored unless it strictly extends the map). Latched out during
    /// truncation (a data-plane event; the term is re-observed once the data
    /// plane resumes).
    DataTermObserved { term: u32, base: u64 },
    /// Agent feedback: the archive was truncated to `to` under truncation `epoch`
    /// and the write counter re-primed. The durable clamp (`durable = min(durable,
    /// to)`) is UNCONDITIONAL — physical truncation is the truth about durability,
    /// even for a stale-epoch ack. The truncating latch is released and the
    /// pending map adopted IFF `epoch` matches the in-flight truncation the SM
    /// allocated in the corresponding `Action::Truncate`.
    Truncated { epoch: u64, to: u64 },
    /// M7: a `ClusterConfig` frame was observed (durably appended) ending at
    /// `position` — fed by the leader append path, the follower archive scan,
    /// AND boot recovery. Adopts iff `config.version` strictly exceeds the
    /// currently adopted version (idempotent / monotone). This is a DATA-PLANE
    /// event: it is deliberately NOT in the truncating latch's allow-list, so
    /// it is dropped while a truncation is in flight and re-observed once the
    /// data plane resumes — including the post-revert re-adoption case.
    ConfigObserved { position: u64, config: ClusterConfig },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// PERSIST the vote record durably, THEN send the (granted) vote.
    /// The agent MUST NOT send before the persist completes.
    PersistAndSendVote { to: NodeId, vote: VoteOut },
    /// Send a rejection (no persistence needed — nothing was promised).
    SendVoteRejection { to: NodeId, term: u32 },
    /// Broadcast RequestVote{new_term, last_term, last_durable} to peers.
    StartElection { new_term: u32, last_term: u32, last_durable: u64 },
    /// Open a term as leader: the agent must, IN ORDER: (1) append the new
    /// TermMapEntry{term, base} + persist the term map durably; (2) collapse
    /// volatile append to `base` (durable) — discarding the unreplicated
    /// tail; (3) append the NewTerm no-op frame and feed NewTermAppended
    /// back; (4) switch data-plane roles to leader.
    BecomeLeader { term: u32, base: u64 },
    /// Step down / adopt: switch data-plane roles to follower of `term`.
    BecomeFollower { term: u32, leader: Option<NodeId> },
    /// Store the commit counter (the agent owns the store; single writer).
    AdvanceCommit { commit: u64 },
    /// Gossip CommitPosition{commit} to followers (leader only).
    GossipCommit { commit: u64 },
    /// Truncate our log to `to`, THEN (via matching-`epoch` `Truncated` feedback)
    /// replace our term map with `new_map`. `epoch` is SM-allocated and monotonic:
    /// the SM owns the ack identity, the driving agent merely transports it back
    /// on `Truncated`. The agent must persist `new_map`, call
    /// `Archive::truncate_to(to)`, re-prime the write counter, then feed
    /// `Truncated{epoch, to}` back. The SM latches out data-plane events until the
    /// matching-epoch feedback arrives — even across a mid-flight term adoption.
    Truncate { epoch: u64, to: u64, new_map: Vec<(u32, u64)> },
    /// Ship our term map to followers (leader only): the last
    /// `MAX_TERM_MAP_WIRE_ENTRIES` entries. Emitted on `BecomeLeader`,
    /// piggybacked on the commit-gossip cadence, AND re-emitted on the idle
    /// gossip floor (`gossip_floor_ns`) so a divergent node rejoining an idle
    /// cluster still receives a map to reconcile against.
    ShipTermMap { entries: Vec<(u32, u64)> },
    /// Persist the reconciled term map durably (follower adopted leader entries
    /// with no truncation needed — keeps vote credentials honest).
    PersistTermMap { new_map: Vec<(u32, u64)> },
    /// Reconciliation found no common prefix — the divergence predates the
    /// shipped window. Incremental repair is impossible; the agent logs and
    /// panics (M6 snapshot install is the real fix). The sim asserts this never
    /// fires at `<= MAX_TERM_MAP_WIRE_ENTRIES` terms.
    Fatal { reason: &'static str },
    /// M6 Task 8: a wipe-and-rejoin was decided (NoCommonPrefix). This is a pure
    /// counter tag — the substantive work is the accompanying
    /// `Truncate { to: 0, new_map: [] }` this is emitted alongside. The agent bumps
    /// a `wipes` counter and takes no other action.
    CountWipe,
    /// M7: a higher-version `ClusterConfig` was adopted at `position` (`prev`/
    /// `prev_position` are the superseded config and its position). The agent
    /// must, in any order that preserves durability-before-effect: (1) persist
    /// the `ConfigRecord`; (2) rebuild the net layer (`SetPeers`) from the new
    /// config; (3) update `cnc.dat`. If this adoption also emitted
    /// `HaltRemoved`, the halt still applies.
    ConfigAdopted { position: u64, config: ClusterConfig, prev_position: u64, prev: ClusterConfig },
    /// M7: this node is not a member of the just-adopted config (and is not a
    /// leader mid-self-removal — a leader keeps serving until its own removal
    /// commits; Task 8 adds that commit-triggered step-down). The agent must
    /// fail-stop.
    HaltRemoved,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteOut {
    pub term: u32,
    pub voted_for: NodeId,
    pub granted_to: NodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

pub struct ElectionSm {
    id: NodeId,
    /// The adopted `ClusterConfig` (M7). `members`/`can_vote` below are
    /// derived from it at construction and on every adoption boundary.
    config: ClusterConfig,
    /// Frame-end position of `config` (the append offset it was observed at).
    config_position: u64,
    /// Derived voting set: `config.voter_ids()`. Kept as private derived
    /// state (rather than recomputed on every access) so the rest of the SM
    /// body — `majority()`, `follower_slot()`, the vote/report membership
    /// checks — stays unchanged.
    members: Vec<NodeId>,
    /// Derived: `config.is_voter(id)`. M6 Task 7: candidacy switch — see the
    /// doc on [`ElectionConfig::config`].
    can_vote: bool,
    /// Carried per-member durable reports (M7): upserted (max) on every
    /// current-term `Report` from a config member, cleared on `become_leader`
    /// (stale-term reports must not certify anything in the new term), and
    /// pruned to surviving config members at every `adopt_config` rebuild.
    /// Backs the `PromoteLearner` catch-up precondition and `last_report`.
    last_reports: Vec<(NodeId, u64)>,
    timeout_min_ns: u64,
    timeout_max_ns: u64,
    /// Idle re-gossip floor (see [`ElectionConfig::gossip_floor_ns`]).
    gossip_floor_ns: u64,
    rng: XorShift64,

    role: Role,
    current_term: u32,
    /// The vote this node has cast: `(term, candidate)`. Survives restart via
    /// `recovered_vote`. One vote per term.
    voted_for: Option<(u32, NodeId)>,
    /// Term map (term → base position), including the entry opened this run.
    term_map: Vec<(u32, u64)>,
    durable: u64,
    /// Monotonic commit position observed/produced this run (never regresses).
    commit_seen: u64,

    /// True when the leader was seen since the last `Tick`; re-arms the
    /// election timer instead of timing out.
    pending_leader_activity: bool,
    /// Absolute ns at which the current election timer fires.
    timeout_deadline_ns: u64,
    /// Absolute ns of the most recent `Tick` (the only time source). Used as the
    /// gossip-floor anchor for an advance driven off a `Report` (which carries no
    /// time of its own), so a report-driven gossip resets the floor too.
    last_tick_ns: u64,
    /// Absolute ns of the last leader gossip (advance-driven or floor-driven).
    /// The floor re-emits once `now - last_gossip_ns >= gossip_floor_ns`.
    last_gossip_ns: u64,

    /// Candidate: grants counted for `current_term` (self included).
    votes_received: Vec<NodeId>,

    /// Leader: quorum commit ranking over follower reports.
    tracker: CommitTracker,
    /// Leader: position of the NewTerm no-op frame this term (once appended).
    new_term_pos: Option<u64>,
    /// Leader: true once the NewTerm frame committed (Raft §5.4.2 read gate).
    serving: bool,

    /// Follower: truncation in flight, identified by its SM-allocated epoch. While
    /// `Some`, data-plane events are latched out (no commit/durable advance); only
    /// the vote path (RequestVote/Vote) and the `Truncated` feedback are
    /// processed. A higher term via the vote path stays adoptable AND does NOT
    /// clear the latch — the physical truncation must still complete and ack. The
    /// latch is released ONLY by a matching-epoch `Truncated`, so it is bounded by
    /// the agent's feedback (it cannot wedge indefinitely).
    truncating_epoch: Option<u64>,
    /// Monotonic truncation-epoch allocator. Bumped on every issued
    /// `Action::Truncate`; the SM owns this identity and the node transports it.
    truncation_epoch: u64,
    /// The reconciled map to adopt once the matching `Truncated` feedback arrives.
    pending_new_map: Option<Vec<(u32, u64)>>,
    /// M6 Task 8: on `Reconcile::NoCommonPrefix`, wipe-and-rejoin (truncate to 0 +
    /// empty map) instead of fail-stopping. Default `true`. The sim counterfactual
    /// flips it to `false` to reproduce the OLD `Action::Fatal` behavior and prove
    /// the same divergent world fail-stops without the wipe.
    wipe_on_no_common_prefix: bool,
}

impl ElectionSm {
    /// `recovered_vote`/`recovered_term_map` come from NodeState;
    /// `durable` from archive recovery. current_term starts at
    /// max(vote.term, last term-map term).
    pub fn new(
        cfg: ElectionConfig,
        recovered_vote: Option<(u32, NodeId)>,
        recovered_term_map: &[(u32, u64)],
        durable: u64,
        now_ns: u64,
    ) -> Self {
        // Derive the voting set + candidacy switch from the adopted config
        // (M7). `members` is voters-only; a learner's own id must NOT be in
        // it (M6 Task 7 — replicated-to without ever being counted).
        let members_ids = cfg.config.voter_ids();
        let can_vote = cfg.config.is_voter(cfg.id);

        // Fail loudly at construction, not via a usize underflow later
        // (n_members - 1) or a nonsensical timer span. A voter's own id must be
        // in `members` (it occupies a CommitTracker slot and votes for itself);
        // a learner's own id must NOT be — `members` is the voting set only, and
        // a learner is replicated-to without ever being counted (M6 Task 7).
        assert!(!members_ids.is_empty(), "election membership must be non-empty");
        if can_vote {
            assert!(
                members_ids.contains(&cfg.id),
                "a voter's id must be in the voting membership (id={}, members={:?})",
                cfg.id,
                members_ids
            );
        } else {
            assert!(
                !members_ids.contains(&cfg.id),
                "a learner's id must NOT be in the voting membership (id={}, members={:?})",
                cfg.id,
                members_ids
            );
        }
        // Hard-assert (M-2): a strictly positive timer span. An empty span
        // (`min == max`) collapses randomized election timeouts to a single
        // value across the cluster — a split-vote hazard — and a `min > max`
        // wraps the span in release mode. Fail loudly at construction.
        assert!(
            cfg.election_timeout_min_ns < cfg.election_timeout_max_ns,
            "election timeout span empty (min={} !< max={})",
            cfg.election_timeout_min_ns,
            cfg.election_timeout_max_ns
        );

        let map_term = recovered_term_map.last().map(|(t, _)| *t).unwrap_or(0);
        let vote_term = recovered_vote.map(|(t, _)| t).unwrap_or(0);
        let current_term = vote_term.max(map_term);

        let n_members = members_ids.len();
        let tracker = CommitTracker::new(n_members - 1, n_members);

        let mut sm = Self {
            id: cfg.id,
            config: cfg.config,
            config_position: cfg.config_position,
            members: members_ids,
            can_vote,
            last_reports: Vec::new(),
            timeout_min_ns: cfg.election_timeout_min_ns,
            timeout_max_ns: cfg.election_timeout_max_ns,
            gossip_floor_ns: cfg.gossip_floor_ns,
            rng: XorShift64::new(cfg.seed),
            role: Role::Follower,
            current_term,
            voted_for: recovered_vote,
            term_map: recovered_term_map.to_vec(),
            durable,
            commit_seen: 0,
            pending_leader_activity: false,
            timeout_deadline_ns: 0,
            last_tick_ns: now_ns,
            last_gossip_ns: now_ns,
            votes_received: Vec::new(),
            tracker,
            new_term_pos: None,
            serving: false,
            truncating_epoch: None,
            truncation_epoch: 0,
            pending_new_map: None,
            wipe_on_no_common_prefix: true,
        };
        sm.arm_timeout(now_ns);
        sm
    }

    /// M6 Task 8: toggle wipe-and-rejoin on `Reconcile::NoCommonPrefix`. Default
    /// on (`true`). The sim counterfactual sets it `false` to reproduce the old
    /// `Action::Fatal` fail-stop on the identical divergent world.
    pub fn set_wipe_on_no_common_prefix(&mut self, v: bool) {
        self.wipe_on_no_common_prefix = v;
    }

    pub fn step(&mut self, ev: Event, out: &mut Vec<Action>) {
        // Truncating latch: drop data-plane events while a truncation is in
        // flight. The allow-list is exactly `RequestVote | Vote | Truncated`:
        // a higher term via the *vote path* stays adoptable (an election must
        // never be blocked by an in-flight truncation), and the agent's
        // `Truncated` feedback is what releases the latch — so it is bounded,
        // not indefinite. NOTE: other higher-term carriers (LeaderSeen, Report,
        // CommitGossip, TermMapReceived) are deliberately latched out here; the
        // data plane is paused during truncation and they are re-delivered once
        // it resumes, so no adoption is lost.
        if self.truncating_epoch.is_some()
            && !matches!(ev, Event::RequestVote { .. } | Event::Vote { .. } | Event::Truncated { .. })
        {
            return;
        }
        match ev {
            Event::Tick { now_ns } => self.on_tick(now_ns, out),

            Event::DurableAdvanced { durable } => {
                self.durable = self.durable.max(durable);
            }

            Event::NewTermAppended { position } => {
                if matches!(self.role, Role::Leader) {
                    self.new_term_pos = Some(position);
                    if self.commit_seen >= position {
                        self.serving = true;
                    }
                }
            }

            Event::LeaderSeen { term } => {
                if term < self.current_term {
                    return;
                }
                if term > self.current_term {
                    self.adopt_term(term, None, out);
                }
                // term == current_term: leader liveness.
                self.pending_leader_activity = true;
                if matches!(self.role, Role::Candidate) {
                    // Another leader already owns our term — step down.
                    self.step_down_to_follower(out);
                }
            }

            Event::Report { from, term, durable } => {
                if term < self.current_term {
                    return; // stale report: dropped
                }
                if term > self.current_term {
                    self.adopt_term(term, None, out);
                    return;
                }
                // Carried reports (M7): upsert (max) for ANY current-term
                // report from a config MEMBER (voter or learner), regardless
                // of our own role — a follower may later become leader and
                // needs the last-known durable for the promote-learner
                // catch-up precondition without waiting for a fresh report.
                // A non-member's report (forged source, or a removed voter)
                // is dropped here just like `follower_slot` drops it below.
                if self.config.contains(from) && from != self.id {
                    match self.last_reports.iter_mut().find(|(id, _)| *id == from) {
                        Some((_, d)) => *d = (*d).max(durable),
                        None => self.last_reports.push((from, durable)),
                    }
                }
                if matches!(self.role, Role::Leader)
                    && let Some(slot) = self.follower_slot(from)
                {
                    self.tracker.on_durable(slot, durable);
                    // A Report carries no time; anchor a resulting advance-gossip
                    // to the last observed Tick so it resets the floor too.
                    self.rank_leader(self.last_tick_ns, out);
                }
            }

            Event::CommitGossip { term, commit } => {
                if term < self.current_term {
                    return; // stale-term gossip: dropped
                }
                if term > self.current_term {
                    self.adopt_term(term, None, out);
                }
                // term == current_term (or just adopted): leader liveness.
                self.pending_leader_activity = true;
                // Stuck-candidate wedge fix: a candidate that only hears the
                // legitimate leader of its term via commit gossip must step
                // down. Otherwise its election timer re-arms on this activity
                // every Tick, never fires, and it never steps down — coexisting
                // with a real leader forever. Mirror the LeaderSeen{term==
                // current} step-down, then intake the gossip as a follower.
                if matches!(self.role, Role::Candidate) {
                    self.step_down_to_follower(out);
                }
                if !matches!(self.role, Role::Leader) && commit > self.commit_seen {
                    self.commit_seen = commit;
                    out.push(Action::AdvanceCommit { commit });
                }
            }

            Event::RequestVote { from, new_term, last_term, last_durable } => {
                // Symmetry with the Vote-grant membership check (M4 Info): a
                // non-member RequestVote is ignored ENTIRELY — no rejection AND no
                // term adoption. Only configured members participate in elections,
                // and the safety core enforces that here rather than trusting the
                // transport (which a forged-source datagram could evade).
                if !self.members.contains(&from) {
                    return;
                }
                if !self.can_vote {
                    // Learner: adopt a higher term (liveness/reconcile) but never
                    // vote — it is not in the voting set and must not influence any
                    // election. No rejection either: a learner emits no vote traffic.
                    if new_term > self.current_term {
                        self.adopt_term(new_term, None, out);
                    }
                    return;
                }
                if new_term < self.current_term {
                    // Stale candidate: reject carrying our term so it learns.
                    out.push(Action::SendVoteRejection { to: from, term: self.current_term });
                    return;
                }
                if new_term > self.current_term {
                    self.adopt_term(new_term, None, out);
                }
                self.handle_request_vote(from, last_term, last_durable, out);
            }

            Event::Vote { from, term, granted } => {
                if term < self.current_term {
                    return;
                }
                if term > self.current_term {
                    self.adopt_term(term, None, out);
                    return;
                }
                // Split-brain guard: term == current_term. Only a configured
                // member's grant may count toward our election. `votes_received`
                // is seeded with self, so a single forged/non-member grant would
                // be a majority of 3 and elect us — the safety core enforces
                // membership here rather than delegating it to the transport.
                // (Mirrors how Report drops non-members via `follower_slot`.)
                if !self.members.contains(&from) {
                    return;
                }
                if granted && matches!(self.role, Role::Candidate) {
                    if !self.votes_received.contains(&from) {
                        self.votes_received.push(from);
                    }
                    if self.votes_received.len() >= self.majority() {
                        self.become_leader(out);
                    }
                }
            }

            Event::TermMapReceived { term, entries } => {
                if term < self.current_term {
                    return; // stale-term map: dropped
                }
                if term > self.current_term {
                    self.adopt_term(term, None, out);
                }
                self.reconcile_term_map(&entries, out);
            }

            Event::DataTermObserved { term, base } => {
                // Data-stamped recording: our own map grows ONLY here (and via
                // our own `become_leader`). Accept iff it strictly extends the
                // map — a term above our current last map term. Idempotent
                // otherwise (a re-observed or stale open is a no-op). This event
                // is a data-plane event, so the truncating latch (in `step`)
                // drops it while a truncation is in flight; the term is
                // re-observed once the data plane resumes.
                let last_term = self.term_map.last().map(|(t, _)| *t).unwrap_or(0);
                if term > last_term {
                    self.term_map.push((term, base));
                    out.push(Action::PersistTermMap { new_map: self.term_map.clone() });
                }
            }

            Event::Truncated { epoch, to } => {
                // I-1: physical truncation is always the truth about durability.
                // Clamp durable down BEFORE the epoch check so that even a stale-
                // or wrong-epoch ack (or one arriving when we are not truncating)
                // can never leave the SM overclaiming durable bytes that were
                // physically truncated away. `min` only ever shrinks — a `to`
                // above our durable reduces nothing.
                self.durable = self.durable.min(to);
                // Latch release / map adoption is scoped to the MATCHING epoch the
                // SM allocated for this truncation. A wrong-epoch ack (a late or
                // superseded truncation's feedback) clamps durable above but
                // changes nothing else — the in-flight truncation is still awaited.
                // Note the latch survives a mid-flight term adoption: the physical
                // truncation was issued and must complete; only its own epoch's ack
                // releases it and adopts its pruned map.
                if self.truncating_epoch != Some(epoch) {
                    return;
                }
                if let Some(m) = self.pending_new_map.take() {
                    self.term_map = m;
                }
                self.truncating_epoch = None;
            }

            Event::ConfigObserved { position, config } => {
                if config.version <= self.config.version {
                    return; // idempotent / stale re-observation
                }
                self.adopt_config(position, config, out);
            }
        }
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn current_term(&self) -> u32 {
        self.current_term
    }

    /// Leader-only: true once the NewTerm frame has committed (Raft §5.4.2).
    pub fn can_serve(&self) -> bool {
        self.serving
    }

    /// The term map including any entry opened this run.
    pub fn term_map(&self) -> &[(u32, u64)] {
        &self.term_map
    }

    /// M6 Task 8: adopt the leader's authoritative term-map lineage as our
    /// baseline on installing a snapshot at a purge floor.
    ///
    /// A below-floor joiner (`durable < floor`) discards its local bytes and
    /// installs the leader's snapshot, whose lineage IS the leader's committed
    /// history up to the floor. Its term-map must become that lineage — otherwise
    /// the shared prefix lives *inside* the snapshot, invisible to `reconcile`,
    /// which would then see the leader's below-floor entries as un-stamped
    /// divergence and clamp a truncate BELOW the adopted floor (a `PositionPurged`
    /// fail-stop). Seeding here makes the very next `reconcile` a clean prefix
    /// match. Entries at/above the floor are idempotently re-confirmed as the
    /// tail replays (`DataTermObserved` is monotone below its last).
    ///
    /// Safe because it runs ONLY on a snapshot install (the node gates it on
    /// `durable < floor`, i.e. we genuinely lack this history) with the leader's
    /// own shipped map — never against local, possibly-divergent bytes.
    pub fn adopt_snapshot_lineage(&mut self, lineage: &[(u32, u64)]) {
        if lineage.is_empty() {
            return;
        }
        self.term_map = lineage.to_vec();
        let last_term = lineage.last().map(|(t, _)| *t).unwrap_or(0);
        self.current_term = self.current_term.max(last_term);
    }

    /// True while a truncation is in flight (the data-plane latch is held).
    pub fn is_truncating(&self) -> bool {
        self.truncating_epoch.is_some()
    }

    /// The durable byte position the SM currently believes it holds.
    pub fn durable(&self) -> u64 {
        self.durable
    }

    /// The currently adopted cluster membership.
    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Frame-end position of the currently adopted config.
    pub fn config_position(&self) -> u64 {
        self.config_position
    }

    /// One membership change in flight at a time: true while the adopted
    /// config's position has not yet committed.
    pub fn config_pending(&self) -> bool {
        self.config_position > self.commit_seen
    }

    /// The last carried durable report for `id` (used by the promote-learner
    /// catch-up precondition and by `uc2ctl status`).
    pub fn last_report(&self, id: NodeId) -> Option<u64> {
        self.last_reports.iter().find(|(rid, _)| *rid == id).map(|(_, d)| *d)
    }

    /// Leader-only membership proposal (M7). `slack` = max catch-up gap a
    /// learner may have and still be promoted (the node passes its admission
    /// window). Returns the NEW config for the node to append as a
    /// FRAME_TYPE_CONFIG frame; adoption happens via the `ConfigObserved` the
    /// append path feeds back — one adoption path for leader and follower.
    /// The SM does NOT self-adopt here.
    pub fn propose_config(&mut self, op: ConfigOp, slack: u64) -> Result<ClusterConfig, ProposeError> {
        if !matches!(self.role, Role::Leader) {
            return Err(ProposeError::NotLeader);
        }
        if !self.serving {
            return Err(ProposeError::NotServing); // the single-server-change precondition
        }
        if self.config_pending() {
            return Err(ProposeError::ChangePending); // one in flight
        }
        if let ConfigOp::PromoteLearner { id } = op {
            let reported = self.last_report(id).unwrap_or(0);
            let target = self.commit_seen.saturating_sub(slack);
            if reported < target {
                return Err(ProposeError::NotCaughtUp { gap: target - reported });
            }
        }
        self.config.apply(op)
    }

    // ---- internals ----

    #[inline]
    fn majority(&self) -> usize {
        self.members.len() / 2 + 1
    }

    /// Randomize a fresh election deadline from `[min, max)` relative to `now`.
    fn arm_timeout(&mut self, now_ns: u64) {
        let span = self.timeout_max_ns - self.timeout_min_ns;
        let jitter = if span == 0 { 0 } else { self.rng.next_u64() % span };
        self.timeout_deadline_ns = now_ns + self.timeout_min_ns + jitter;
    }

    fn on_tick(&mut self, now_ns: u64, out: &mut Vec<Action>) {
        self.last_tick_ns = now_ns;
        match self.role {
            Role::Leader => {
                // Advance-driven gossip first (resets the floor when it fires).
                self.rank_leader(now_ns, out);
                // Idle re-emission floor (spec §6): even with commit plateaued,
                // re-ship commit + term map on a bounded cadence. Two liveness
                // holes this closes on an OTHERWISE-idle cluster: (1) a divergent
                // node rejoining never gets a term map (ShipTermMap rides only the
                // advance cadence) → never reconciles → gated forever; (2) a
                // follower that missed the last advance-gossip datagram never
                // learns the plateaued commit. Idempotent — monotonic intake
                // everywhere makes a re-emission a no-op for anyone already caught
                // up. `rank_leader` set `last_gossip_ns = now_ns` if it just
                // gossiped, so this cannot double-fire in the same tick.
                if now_ns.saturating_sub(self.last_gossip_ns) >= self.gossip_floor_ns {
                    self.last_gossip_ns = now_ns;
                    out.push(Action::GossipCommit { commit: self.commit_seen });
                    out.push(Action::ShipTermMap { entries: self.term_map_wire_tail() });
                }
            }
            Role::Follower | Role::Candidate => {
                if self.pending_leader_activity {
                    // Saw the leader since last tick: re-arm, do not time out.
                    self.pending_leader_activity = false;
                    self.arm_timeout(now_ns);
                } else if now_ns >= self.timeout_deadline_ns {
                    if self.can_vote {
                        self.start_election(now_ns, out);
                    } else {
                        // Learner: the timer fires but never becomes a candidacy —
                        // just re-arm so we keep tracking liveness without electing.
                        self.arm_timeout(now_ns);
                    }
                }
            }
        }
    }

    fn start_election(&mut self, now_ns: u64, out: &mut Vec<Action>) {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.serving = false;
        self.new_term_pos = None;
        self.voted_for = Some((self.current_term, self.id));
        self.votes_received.clear();
        self.votes_received.push(self.id);

        let last_term = self.term_map.last().map(|(t, _)| *t).unwrap_or(0);
        let last_durable = self.durable;

        // Persist-before-solicit: the self-vote (addressed to self, agent skips
        // the network send) is persisted BEFORE we solicit peers.
        out.push(Action::PersistAndSendVote {
            to: self.id,
            vote: VoteOut { term: self.current_term, voted_for: self.id, granted_to: self.id },
        });
        out.push(Action::StartElection { new_term: self.current_term, last_term, last_durable });

        self.arm_timeout(now_ns);
        self.pending_leader_activity = false;

        // Single-node cluster: a self-vote is already a majority.
        if self.votes_received.len() >= self.majority() {
            self.become_leader(out);
        }
    }

    fn become_leader(&mut self, out: &mut Vec<Action>) {
        self.role = Role::Leader;
        self.serving = false;
        self.new_term_pos = None;
        // Anchor the idle re-gossip floor to when leadership began (M-4). Without
        // this the floor stays anchored at construction, so the first leader tick
        // of a node that becomes leader long after boot immediately fires the idle
        // re-emission (harmless, but a spurious extra datagram) instead of the
        // first clean `gossip_floor_ns` after opening the term.
        self.last_gossip_ns = self.last_tick_ns;
        // Open the term in the term map (base = our durable = the collapse point).
        self.term_map.push((self.current_term, self.durable));
        // Fresh follower slots: stale-term reports must not certify the new term.
        self.tracker.reset_reports();
        // M7: stale-term carried reports must not certify a promote-learner
        // precondition either (mirrors the tracker reset above).
        self.last_reports.clear();
        out.push(Action::BecomeLeader { term: self.current_term, base: self.durable });
        // Ship the freshly-opened term map so followers can reconcile (spec §M4).
        out.push(Action::ShipTermMap { entries: self.term_map_wire_tail() });
    }

    fn adopt_term(&mut self, new_term: u32, leader: Option<NodeId>, out: &mut Vec<Action>) {
        self.current_term = new_term;
        self.role = Role::Follower;
        self.serving = false;
        self.new_term_pos = None;
        self.votes_received.clear();
        self.voted_for = None; // new term: no vote cast yet
        // A term change does NOT clear an in-flight truncation (M4 I-1 / M5): the
        // physical truncation was already issued to the archive and must complete
        // and ack. The `truncating_epoch` latch therefore persists ACROSS adoption
        // and is released only by its own matching-epoch `Truncated`. The pruned
        // map it adopts is a valid prefix regardless of the new term; the new
        // leader re-ships and we re-reconcile from there.
        out.push(Action::BecomeFollower { term: new_term, leader });
    }

    /// Run reconciliation against a leader-shipped map and emit the derived
    /// action: `Truncate` (+ latch) when a byte is invalid, `PersistTermMap`
    /// when the map merely grows, `Fatal` when there is no common prefix.
    fn reconcile_term_map(&mut self, entries: &[(u32, u64)], out: &mut Vec<Action>) {
        match reconcile(&self.term_map, self.durable, entries) {
            Reconcile::NoCommonPrefix if self.wipe_on_no_common_prefix => {
                // WIPE-AND-REJOIN (M6 Task 8). The divergence predates the leader's
                // shipped term-map window — in M6 this is the real case where the
                // leader PURGED its log prefix, so its earliest retained entry
                // begins above our first byte and no incremental cut exists. Rather
                // than fail-stop, discard our ENTIRE local log and rejoin as an
                // empty follower: truncate to 0 with an empty map, then let the live
                // stream refill us (NAK-replay, or a snapshot session when the
                // leader has purged the prefix — Task 6). We reuse the ordinary
                // truncate machinery verbatim (persist-empty-map → truncate_to(0)
                // first-block arm → truncate_all → prime(0) → epoch'd ack → gate
                // reopen), so a wipe is exactly `Truncate { to: 0, new_map: [] }`.
                //
                // SAFETY: this is only ever reached as a FOLLOWER adopting a
                // higher-or-equal-term leader's map (a leader never reconciles
                // against another map). The data-plane latch — allow-list
                // {RequestVote, Vote, Truncated} — holds from this emit until the
                // wipe's matching-epoch `Truncated` ack lands, so no post-wipe byte
                // can be accepted (or reported) out of order, and the empty map we
                // adopt is a trivial (zero-length) prefix of the leader's authority.
                self.truncation_epoch += 1;
                let epoch = self.truncation_epoch;
                self.truncating_epoch = Some(epoch);
                self.pending_new_map = Some(Vec::new());
                out.push(Action::CountWipe);
                out.push(Action::Truncate { epoch, to: 0, new_map: Vec::new() });
            }
            Reconcile::NoCommonPrefix => out.push(Action::Fatal {
                reason: "term-map reconciliation found no common prefix (snapshot install required)",
            }),
            Reconcile::Ok(Outcome { valid_up_to, new_map }) => {
                if valid_up_to < self.durable {
                    // A byte is invalid: latch out the data plane and truncate.
                    // The map is adopted only once the archive confirms with the
                    // matching epoch. The SM allocates that epoch here (it owns the
                    // ack identity); the node mirrors it from the action.
                    self.truncation_epoch += 1;
                    let epoch = self.truncation_epoch;
                    self.truncating_epoch = Some(epoch);
                    self.pending_new_map = Some(new_map.clone());
                    out.push(Action::Truncate { epoch, to: valid_up_to, new_map });
                } else if new_map != self.term_map {
                    // Nothing to truncate. Post-redesign (data-stamped recording)
                    // this branch only ever SHRINKS our map — dropping a phantom
                    // frontier entry we opened but never got covered — never adopts
                    // leader entries (our map grows solely via `DataTermObserved`).
                    // Persist the shrunk map so our vote credentials stay honest.
                    self.term_map = new_map.clone();
                    out.push(Action::PersistTermMap { new_map });
                }
            }
        }
    }

    /// The last `MAX_TERM_MAP_WIRE_ENTRIES` term-map entries (the wire tail).
    fn term_map_wire_tail(&self) -> Vec<(u32, u64)> {
        let start = self.term_map.len().saturating_sub(MAX_TERM_MAP_WIRE_ENTRIES);
        self.term_map[start..].to_vec()
    }

    fn step_down_to_follower(&mut self, out: &mut Vec<Action>) {
        self.role = Role::Follower;
        self.serving = false;
        self.new_term_pos = None;
        self.votes_received.clear();
        out.push(Action::BecomeFollower { term: self.current_term, leader: None });
    }

    fn handle_request_vote(
        &mut self,
        from: NodeId,
        last_term: u32,
        last_durable: u64,
        out: &mut Vec<Action>,
    ) {
        // At this point current_term == new_term (a higher term was adopted).
        if let Some((vt, vid)) = self.voted_for
            && vt == self.current_term
        {
            if vid == from {
                // Idempotent re-grant to the same candidate (lost datagram).
                self.grant_vote(from, out);
            } else {
                // Already voted for someone else this term: reject.
                out.push(Action::SendVoteRejection { to: from, term: self.current_term });
            }
            return;
        }
        if self.log_ok(last_term, last_durable) {
            self.grant_vote(from, out);
        } else {
            out.push(Action::SendVoteRejection { to: from, term: self.current_term });
        }
    }

    fn grant_vote(&mut self, to: NodeId, out: &mut Vec<Action>) {
        self.voted_for = Some((self.current_term, to));
        // Granting a vote resets our election timer (Raft): defer to next tick.
        self.pending_leader_activity = true;
        out.push(Action::PersistAndSendVote {
            to,
            vote: VoteOut { term: self.current_term, voted_for: to, granted_to: to },
        });
    }

    /// Lexicographic freshness: `(last_term, last_durable) >= (ours, durable)`.
    fn log_ok(&self, last_term: u32, last_durable: u64) -> bool {
        let our_term = self.term_map.last().map(|(t, _)| *t).unwrap_or(0);
        (last_term, last_durable) >= (our_term, self.durable)
    }

    /// Dense CommitTracker slot for a member id (members minus self, in order).
    fn follower_slot(&self, id: NodeId) -> Option<usize> {
        let mut slot = 0;
        for &m in &self.members {
            if m == self.id {
                continue;
            }
            if m == id {
                return Some(slot);
            }
            slot += 1;
        }
        None
    }

    /// Rebuild-at-boundary (M7): adopt `cfg` (observed ending at `position`),
    /// refresh the derived voting set, rebuild the `CommitTracker` sized to
    /// the new voting set, and re-feed the last durable report per surviving
    /// member so commit ranking does not restart from zero. Commit itself is
    /// monotonic in the cnc counter — a rebuild can only pause, never regress
    /// it.
    fn adopt_config(&mut self, position: u64, cfg: ClusterConfig, out: &mut Vec<Action>) {
        let prev = std::mem::replace(&mut self.config, cfg);
        let prev_position = self.config_position;
        self.config_position = position;
        self.members = self.config.voter_ids();
        self.can_vote = self.config.is_voter(self.id);

        // Sizing subtlety: `follower_slot` skips only `m == self.id`. If self
        // is (still) a voter, it occupies no slot and the tracker is sized as
        // before: n-1 tracked followers over a cluster of n. If self is NOT a
        // voter (a learner, or — Leader case — mid-self-removal, kept
        // serving until commit per the self-exclusion rule below), it
        // occupies NO slot to exclude, so ALL n voters get a dense slot
        // 0..n — using the old `n - 1` here would both under-track one
        // member (permanently reads zero) AND panic in `follower_slot` /
        // `CommitTracker::on_durable` once a real report maps to slot `n-1`,
        // one past the tracker's `n-1`-sized `reported` vec. `CommitTracker`
        // still requires `cluster_size > n_followers` ("the leader is a
        // member") — own_durable always occupies that "+1" ranking slot in
        // `advance`, self-as-voter or not — so cluster_size is always
        // `n_followers + 1`.
        let n = self.members.len();
        let n_followers = if self.can_vote { n - 1 } else { n };
        self.tracker = CommitTracker::new(n_followers, n_followers + 1);
        // Prune to SURVIVING config members (voter OR learner — not just
        // voters): a learner's carried report must survive an unrelated
        // rebuild (e.g. another AddLearner) so a later PromoteLearner still
        // sees it; a truly removed id (voter or learner) is correctly
        // dropped because `self.config.contains` is false for it.
        self.last_reports.retain(|(id, _)| self.config.contains(*id) && *id != self.id);
        for &(id, durable) in &self.last_reports {
            if let Some(slot) = self.follower_slot(id) {
                self.tracker.on_durable(slot, durable);
            }
        }
        out.push(Action::ConfigAdopted {
            position,
            config: self.config.clone(),
            prev_position,
            prev,
        });
        // Self-exclusion: a follower halts now; a leader removing itself keeps
        // serving until this entry COMMITS (the node executes the step-down —
        // Task 8), because C_new must be replicated by a leader that still
        // exists.
        if !self.config.contains(self.id) && !matches!(self.role, Role::Leader) {
            out.push(Action::HaltRemoved);
        }
    }

    fn rank_leader(&mut self, now_ns: u64, out: &mut Vec<Action>) {
        if let Some(c) = self.tracker.advance(self.durable)
            && c > self.commit_seen
        {
            self.commit_seen = c;
            // This advance carries the gossip; reset the idle floor so it does
            // not double-fire right after (spec §6 re-gossip cadence).
            self.last_gossip_ns = now_ns;
            out.push(Action::AdvanceCommit { commit: c });
            out.push(Action::GossipCommit { commit: c });
            // Piggyback the term map on the commit-gossip cadence so a lagging
            // or reconnecting follower can reconcile (spec §M4).
            out.push(Action::ShipTermMap { entries: self.term_map_wire_tail() });
            // Serving gate. Note we intentionally do NOT reset the embedded
            // `tracker.commit` across terms, yet this can only flip `serving`
            // true on a genuinely-new-term commit: byte positions are globally
            // monotone, so this term's `new_term_pos > base = durable >=
            // commit_seen`. A quorum on `new_term_pos` therefore requires a
            // commit strictly beyond anything a prior term produced, so a stale
            // carry-over commit can never satisfy `c >= pos` for this term.
            if let Some(pos) = self.new_term_pos
                && c >= pos
            {
                self.serving = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Synthetic per-test addr: `(id, 1)` — never a real `SocketAddr`.
    fn addr_of(id: NodeId) -> (u32, u16) {
        (id, 1)
    }

    /// A genesis `ClusterConfig` with `members` all as voters, no learners.
    fn genesis_voters(members: &[NodeId]) -> ClusterConfig {
        ClusterConfig::genesis(members.iter().map(|&id| (id, addr_of(id))).collect(), Vec::new())
    }

    fn cfg(id: NodeId) -> ElectionConfig {
        cfg_members(id, vec![0, 1, 2])
    }

    fn cfg_members(id: NodeId, members: Vec<NodeId>) -> ElectionConfig {
        ElectionConfig {
            id,
            config: genesis_voters(&members),
            config_position: 0,
            election_timeout_min_ns: 150,
            election_timeout_max_ns: 300,
            // Huge floor: these tests drive ranking via `Tick` at tiny `now_ns`
            // values, so the idle re-gossip floor must never fire and perturb the
            // exact action-list assertions. The floor's own behavior is covered by
            // `idle_leader_reemits_on_gossip_floor` with a small floor.
            gossip_floor_ns: u64::MAX,
            seed: 42 + id as u64,
        }
    }

    /// Like `cfg_members`, but with an explicit learner set (M7 promote tests).
    fn cfg_with_learners(id: NodeId, voters: Vec<NodeId>, learners: Vec<NodeId>) -> ElectionConfig {
        ElectionConfig {
            id,
            config: ClusterConfig::genesis(
                voters.iter().map(|&v| (v, addr_of(v))).collect(),
                learners.iter().map(|&l| (l, addr_of(l))).collect(),
            ),
            config_position: 0,
            election_timeout_min_ns: 150,
            election_timeout_max_ns: 300,
            gossip_floor_ns: u64::MAX,
            seed: 42 + id as u64,
        }
    }

    fn sm(id: NodeId) -> ElectionSm {
        ElectionSm::new(cfg(id), None, &[], 0, 0)
    }

    /// A fresh node driven to Leader of term 1 in the 3-node `[0,1,2]` cluster.
    fn leader_term1() -> ElectionSm {
        let mut s = sm(0);
        step(&mut s, Event::Tick { now_ns: 301 });
        step(&mut s, Event::Vote { from: 1, term: 1, granted: true });
        assert!(matches!(s.role(), Role::Leader));
        s
    }

    fn step(sm: &mut ElectionSm, ev: Event) -> Vec<Action> {
        let mut out = Vec::new();
        sm.step(ev, &mut out);
        out
    }

    /// A follower that has adopted term 3 with an own map (`[(1,0),(2,4096)]` at
    /// durable 6000) that diverges from the incoming term-3 leader map — so the
    /// next `TermMapReceived{term:3}` reconciles to a `Truncate`.
    fn sm_with_divergent_map() -> ElectionSm {
        let mut s = ElectionSm::new(cfg(1), None, &[(1, 0), (2, 4096)], 6000, 0);
        step(&mut s, Event::RequestVote { from: 0, new_term: 3, last_term: 1, last_durable: 7000 });
        s
    }

    /// A fresh follower at term 1 in the `[0,1,2]` cluster (id 0).
    fn fresh_follower() -> ElectionSm {
        ElectionSm::new(cfg(0), None, &[(1, 0)], 0, 0)
    }

    /// A learner (id 9, deliberately NOT in the voting set `[0,1,2]`): it is
    /// replicated-to but never counted (M6 Task 7).
    fn learner() -> ElectionSm {
        let cfg = ElectionConfig {
            id: 9,
            config: ClusterConfig::genesis(
                vec![(0, addr_of(0)), (1, addr_of(1)), (2, addr_of(2))],
                vec![(9, addr_of(9))],
            ),
            config_position: 0,
            election_timeout_min_ns: 150,
            election_timeout_max_ns: 300,
            gossip_floor_ns: u64::MAX,
            seed: 7,
        };
        ElectionSm::new(cfg, None, &[(1, 0)], 0, 0)
    }

    fn extract_truncate_epoch(out: &[Action]) -> u64 {
        out.iter()
            .find_map(|a| match a {
                Action::Truncate { epoch, .. } => Some(*epoch),
                _ => None,
            })
            .expect("truncate issued")
    }

    #[test]
    fn learner_never_becomes_a_candidate() {
        let mut s = learner();
        assert!(matches!(s.role(), Role::Follower));
        let term0 = s.current_term(); // seeded from the recovered map, not candidacy
        // Drive the election timer well past its max many times: a learner's timer
        // fires but re-arms — it never solicits votes, never casts one, never
        // leaves follower, and never bumps the term via candidacy.
        for t in 1..=50u64 {
            let acts = step(&mut s, Event::Tick { now_ns: t * 400 });
            assert!(
                !acts.iter().any(|a| matches!(a, Action::StartElection { .. })),
                "a learner must never solicit votes"
            );
            assert!(
                !acts.iter().any(|a| matches!(a, Action::PersistAndSendVote { .. })),
                "a learner must never cast a vote (self or otherwise)"
            );
        }
        assert!(matches!(s.role(), Role::Follower));
        assert_eq!(s.current_term(), term0, "candidacy never bumped the term");
    }

    #[test]
    fn learner_adopts_term_but_never_grants_a_vote() {
        let mut s = learner();
        // A voter solicits at term 5: the learner adopts the term (liveness /
        // reconcile) but emits ZERO vote traffic — no grant, no rejection.
        let acts = step(
            &mut s,
            Event::RequestVote { from: 0, new_term: 5, last_term: 1, last_durable: 10_000 },
        );
        assert_eq!(s.current_term(), 5, "learner adopted the higher term");
        assert!(
            !acts.iter().any(|a| matches!(a, Action::PersistAndSendVote { .. })),
            "learner never grants a vote"
        );
        assert!(
            !acts.iter().any(|a| matches!(a, Action::SendVoteRejection { .. })),
            "learner emits no vote rejection either"
        );
        assert!(matches!(s.role(), Role::Follower));
    }

    #[test]
    fn learner_report_never_advances_a_leaders_commit() {
        // The phantom-commit-via-learner hole, pinned shut: a leader whose voting
        // peers are silent must NOT commit on a learner's (sky-high) durable Report.
        let mut s = sm(0);
        step(&mut s, Event::Tick { now_ns: 301 }); // candidate, term 1
        step(&mut s, Event::Vote { from: 1, term: 1, granted: true }); // majority → leader
        assert!(matches!(s.role(), Role::Leader));
        // A learner (id 9, NOT a voting member) reports a huge durable at our term.
        let acts = step(&mut s, Event::Report { from: 9, term: 1, durable: 1 << 40 });
        assert!(
            !acts.iter().any(|a| matches!(a, Action::AdvanceCommit { .. })),
            "a learner's Report must never advance commit (follower_slot drops it)"
        );
    }

    #[test]
    fn timeout_starts_election_and_majority_wins() {
        let mut s = sm(0);
        assert!(matches!(s.role(), Role::Follower));
        // no leader activity: tick past the max timeout
        let acts = step(&mut s, Event::Tick { now_ns: 301 });
        assert!(matches!(s.role(), Role::Candidate));
        assert_eq!(s.current_term(), 1);
        // self-vote persisted + broadcast
        assert!(acts.iter().any(|a| matches!(
            a,
            Action::PersistAndSendVote { to, vote } if *to == 0 && vote.voted_for == 0 && vote.term == 1
        )));
        assert!(acts.iter().any(|a| matches!(
            a,
            Action::StartElection { new_term: 1, last_term: 0, last_durable: 0 }
        )));
        // one grant (self) + one from node 1 = majority of 3
        let acts = step(&mut s, Event::Vote { from: 1, term: 1, granted: true });
        assert!(acts.iter().any(|a| matches!(a, Action::BecomeLeader { term: 1, base: 0 })));
        assert!(matches!(s.role(), Role::Leader));
        assert!(!s.can_serve(), "must not serve before NewTerm commits");
    }

    #[test]
    fn vote_rule_lexicographic_on_durable_credentials() {
        // our node has durable 1000 in term 2
        let mut s = ElectionSm::new(cfg(1), None, &[(1, 0), (2, 500)], 1000, 0);
        // candidate behind on durable: reject
        let acts = step(
            &mut s,
            Event::RequestVote { from: 2, new_term: 3, last_term: 2, last_durable: 900 },
        );
        assert!(acts.iter().any(|a| matches!(a, Action::SendVoteRejection { to: 2, .. })));
        assert!(!acts.iter().any(|a| matches!(a, Action::PersistAndSendVote { .. })));
        // candidate with a newer last_term but lower durable: lexicographic -> grant
        let acts = step(
            &mut s,
            Event::RequestVote { from: 0, new_term: 4, last_term: 3, last_durable: 100 },
        );
        assert!(acts.iter().any(|a| matches!(
            a,
            Action::PersistAndSendVote { to: 0, vote } if vote.term == 4 && vote.voted_for == 0
        )));
        assert_eq!(s.current_term(), 4);
    }

    #[test]
    fn one_vote_per_term_and_idempotent_regrant() {
        let mut s = sm(1);
        let acts =
            step(&mut s, Event::RequestVote { from: 0, new_term: 1, last_term: 0, last_durable: 0 });
        assert!(acts.iter().any(|a| matches!(a, Action::PersistAndSendVote { to: 0, .. })));
        // different candidate, same term: reject (no double vote)
        let acts =
            step(&mut s, Event::RequestVote { from: 2, new_term: 1, last_term: 0, last_durable: 0 });
        assert!(acts.iter().any(|a| matches!(a, Action::SendVoteRejection { to: 2, .. })));
        // same candidate re-requests (lost datagram): idempotent re-grant
        let acts =
            step(&mut s, Event::RequestVote { from: 0, new_term: 1, last_term: 0, last_durable: 0 });
        assert!(acts.iter().any(|a| matches!(a, Action::PersistAndSendVote { to: 0, .. })));
    }

    #[test]
    fn recovered_vote_is_honored_across_restart() {
        // restarted node had voted for 2 in term 5
        let mut s = ElectionSm::new(cfg(1), Some((5, 2)), &[], 0, 0);
        assert_eq!(s.current_term(), 5);
        let acts =
            step(&mut s, Event::RequestVote { from: 0, new_term: 5, last_term: 0, last_durable: 0 });
        assert!(
            acts.iter().any(|a| matches!(a, Action::SendVoteRejection { to: 0, .. })),
            "must not double-vote in a term after restart"
        );
    }

    #[test]
    fn leader_gates_serving_on_new_term_commit_and_ranks_reports() {
        let mut s = sm(0);
        step(&mut s, Event::Tick { now_ns: 301 });
        step(&mut s, Event::Vote { from: 1, term: 1, granted: true });
        assert!(matches!(s.role(), Role::Leader));
        // agent appended the NewTerm frame at [0, 32)
        step(&mut s, Event::NewTermAppended { position: 32 });
        // own durable covers it; follower 1 reports durable 32
        step(&mut s, Event::DurableAdvanced { durable: 32 });
        let acts = step(&mut s, Event::Report { from: 1, term: 1, durable: 32 });
        let acts2 = step(&mut s, Event::Tick { now_ns: 310 });
        let advanced = acts
            .iter()
            .chain(acts2.iter())
            .any(|a| matches!(a, Action::AdvanceCommit { commit: 32 }));
        assert!(advanced, "quorum on the NewTerm frame must commit it");
        assert!(s.can_serve());
    }

    #[test]
    fn higher_term_deposes_leader_and_stale_events_ignored() {
        let mut s = sm(0);
        step(&mut s, Event::Tick { now_ns: 301 });
        step(&mut s, Event::Vote { from: 1, term: 1, granted: true });
        assert!(matches!(s.role(), Role::Leader));
        // stale report: ignored, no panic, no action
        let acts = step(&mut s, Event::Report { from: 1, term: 0, durable: 999 });
        assert!(acts.is_empty());
        // a higher-term RequestVote deposes
        let acts =
            step(&mut s, Event::RequestVote { from: 2, new_term: 2, last_term: 1, last_durable: 0 });
        assert!(acts.iter().any(|a| matches!(a, Action::BecomeFollower { term: 2, .. })));
        assert!(matches!(s.role(), Role::Follower));
    }

    #[test]
    fn follower_commit_gossip_is_monotonic_and_term_checked() {
        let mut s = sm(1);
        // adopt term 1 via a grant
        step(&mut s, Event::RequestVote { from: 0, new_term: 1, last_term: 0, last_durable: 0 });
        let acts = step(&mut s, Event::CommitGossip { term: 1, commit: 4096 });
        assert!(acts.iter().any(|a| matches!(a, Action::AdvanceCommit { commit: 4096 })));
        // stale-term and regressing gossip: no action
        assert!(step(&mut s, Event::CommitGossip { term: 0, commit: 9999 }).is_empty());
        assert!(step(&mut s, Event::CommitGossip { term: 1, commit: 1024 }).is_empty());
    }

    #[test]
    fn split_vote_retries_with_new_term_and_randomized_timeout() {
        let mut a = sm(0);
        step(&mut a, Event::Tick { now_ns: 301 });
        assert_eq!(a.current_term(), 1);
        // nobody answers; candidate times out again -> term 2
        step(&mut a, Event::Tick { now_ns: 1000 });
        assert!(a.current_term() >= 2);
        assert!(matches!(a.role(), Role::Candidate));
    }

    // ---- review fix-wave tests (F1/F2 + reviewer-required coverage) ----

    /// A duplicated grant from the same voter must count once. In a 5-node
    /// cluster (majority 3), self + node1 = 2; re-grants from node1 do not
    /// reach majority — only a distinct second voter (node2) elects.
    #[test]
    fn duplicate_voter_grant_counted_once() {
        let mut s = ElectionSm::new(cfg_members(0, vec![0, 1, 2, 3, 4]), None, &[], 0, 0);
        step(&mut s, Event::Tick { now_ns: 301 });
        assert!(matches!(s.role(), Role::Candidate));
        assert_eq!(s.current_term(), 1);
        step(&mut s, Event::Vote { from: 1, term: 1, granted: true });
        let dup = step(&mut s, Event::Vote { from: 1, term: 1, granted: true });
        assert!(
            !dup.iter().any(|a| matches!(a, Action::BecomeLeader { .. })),
            "a duplicated voter must not be counted twice"
        );
        assert!(matches!(s.role(), Role::Candidate));
        let elect = step(&mut s, Event::Vote { from: 2, term: 1, granted: true });
        assert!(elect.iter().any(|a| matches!(a, Action::BecomeLeader { term: 1, .. })));
        assert!(matches!(s.role(), Role::Leader));
    }

    /// F1: a grant from a non-member id must be ignored. `votes_received` is
    /// seeded with self, so without the membership check one forged grant would
    /// be a majority of 3 and elect the candidate (RED pre-F1).
    #[test]
    fn non_member_vote_is_ignored() {
        let mut s = sm(0);
        step(&mut s, Event::Tick { now_ns: 301 });
        assert!(matches!(s.role(), Role::Candidate));
        let acts = step(&mut s, Event::Vote { from: 99, term: 1, granted: true });
        assert!(
            !acts.iter().any(|a| matches!(a, Action::BecomeLeader { .. })),
            "a non-member grant must never elect us"
        );
        assert!(matches!(s.role(), Role::Candidate));
    }

    /// A grant carrying a stale term must be ignored while the candidate has
    /// already advanced to a newer term.
    #[test]
    fn stale_term_grant_ignored_while_candidate_ahead() {
        let mut s = sm(0);
        step(&mut s, Event::Tick { now_ns: 301 }); // -> candidate term 1
        step(&mut s, Event::Tick { now_ns: 5000 }); // no activity -> term 2
        assert_eq!(s.current_term(), 2);
        assert!(matches!(s.role(), Role::Candidate));
        let acts = step(&mut s, Event::Vote { from: 1, term: 1, granted: true });
        assert!(!acts.iter().any(|a| matches!(a, Action::BecomeLeader { .. })));
        assert!(matches!(s.role(), Role::Candidate));
    }

    /// Having voted for 0 this term, a second RequestVote from 2 — even with
    /// strictly better credentials — is rejected, never double-granted.
    #[test]
    fn double_vote_rejected_even_for_better_credentials() {
        let mut s = sm(1);
        let a =
            step(&mut s, Event::RequestVote { from: 0, new_term: 1, last_term: 0, last_durable: 0 });
        assert!(a.iter().any(|x| matches!(x, Action::PersistAndSendVote { to: 0, .. })));
        let acts = step(
            &mut s,
            Event::RequestVote { from: 2, new_term: 1, last_term: 9, last_durable: 9999 },
        );
        assert!(acts.iter().any(|a| matches!(a, Action::SendVoteRejection { to: 2, .. })));
        assert!(!acts.iter().any(|a| matches!(a, Action::PersistAndSendVote { .. })));
    }

    /// A recovered vote binds only its own term. A RequestVote in a strictly
    /// higher term must be granted (fresh term, no vote cast yet).
    #[test]
    fn recovered_vote_allows_next_term() {
        let mut s = ElectionSm::new(cfg(1), Some((5, 2)), &[], 0, 0);
        assert_eq!(s.current_term(), 5);
        let acts =
            step(&mut s, Event::RequestVote { from: 0, new_term: 6, last_term: 0, last_durable: 0 });
        assert!(acts.iter().any(|a| matches!(
            a,
            Action::PersistAndSendVote { to: 0, vote } if vote.term == 6 && vote.voted_for == 0
        )));
        assert_eq!(s.current_term(), 6);
    }

    /// `can_serve` is per-term and never inherited: after a leader is deposed
    /// and later re-elected, it must not serve until a FRESH NewTerm frame
    /// commits by quorum in the new term.
    #[test]
    fn can_serve_not_inherited_across_reelection() {
        let mut s = sm(0);
        // Win term 1 and commit its NewTerm frame -> serving true.
        step(&mut s, Event::Tick { now_ns: 301 });
        step(&mut s, Event::Vote { from: 1, term: 1, granted: true });
        assert!(matches!(s.role(), Role::Leader));
        assert!(!s.can_serve());
        step(&mut s, Event::NewTermAppended { position: 32 });
        step(&mut s, Event::DurableAdvanced { durable: 32 });
        step(&mut s, Event::Report { from: 1, term: 1, durable: 32 });
        step(&mut s, Event::Tick { now_ns: 310 });
        assert!(s.can_serve());
        // Deposed by a higher term: serving must drop immediately.
        step(&mut s, Event::RequestVote { from: 2, new_term: 2, last_term: 1, last_durable: 32 });
        assert!(!s.can_serve());
        // Re-elect in a later term (grant re-armed the timer, so two ticks).
        step(&mut s, Event::Tick { now_ns: 6000 });
        step(&mut s, Event::Tick { now_ns: 12000 });
        assert!(matches!(s.role(), Role::Candidate));
        let t = s.current_term();
        let elect = step(&mut s, Event::Vote { from: 1, term: t, granted: true });
        assert!(elect.iter().any(|a| matches!(a, Action::BecomeLeader { .. })));
        assert!(matches!(s.role(), Role::Leader));
        assert!(!s.can_serve(), "can_serve must not carry across re-election");
        // Only a fresh NewTerm commit in this term re-enables serving.
        step(&mut s, Event::NewTermAppended { position: 64 });
        step(&mut s, Event::DurableAdvanced { durable: 64 });
        step(&mut s, Event::Report { from: 1, term: t, durable: 64 });
        step(&mut s, Event::Tick { now_ns: 12010 });
        assert!(s.can_serve());
    }

    /// A leader adopts a strictly higher term from ANY event class, stepping
    /// down to follower of that term.
    #[test]
    fn leader_adopts_higher_term_from_any_event() {
        // Report{term:5}
        let mut s = leader_term1();
        let acts = step(&mut s, Event::Report { from: 1, term: 5, durable: 0 });
        assert!(acts.iter().any(|a| matches!(a, Action::BecomeFollower { term: 5, .. })));
        assert!(matches!(s.role(), Role::Follower));
        // LeaderSeen{term:5}
        let mut s = leader_term1();
        let acts = step(&mut s, Event::LeaderSeen { term: 5 });
        assert!(acts.iter().any(|a| matches!(a, Action::BecomeFollower { term: 5, .. })));
        assert!(matches!(s.role(), Role::Follower));
        // Vote{term:5}
        let mut s = leader_term1();
        let acts = step(&mut s, Event::Vote { from: 1, term: 5, granted: true });
        assert!(acts.iter().any(|a| matches!(a, Action::BecomeFollower { term: 5, .. })));
        assert!(matches!(s.role(), Role::Follower));
    }

    /// A higher-term granting RequestVote emits the depose BEFORE the grant:
    /// action order is exactly [BecomeFollower, PersistAndSendVote].
    #[test]
    fn adopt_then_grant_action_order() {
        let mut s = sm(1);
        let acts =
            step(&mut s, Event::RequestVote { from: 0, new_term: 3, last_term: 0, last_durable: 0 });
        assert_eq!(acts.len(), 2, "exactly a depose then a grant");
        assert!(matches!(acts[0], Action::BecomeFollower { term: 3, .. }));
        assert!(matches!(acts[1], Action::PersistAndSendVote { to: 0, .. }));
    }

    /// F2: a candidate that hears the legitimate leader of its own term only
    /// via commit gossip must step down to follower (and still intake the
    /// commit). Pre-F2 it stayed Candidate and wedged forever (RED pre-F2).
    #[test]
    fn candidate_steps_down_on_same_term_commit_gossip() {
        let mut s = sm(0);
        step(&mut s, Event::Tick { now_ns: 301 });
        assert!(matches!(s.role(), Role::Candidate));
        assert_eq!(s.current_term(), 1);
        let acts = step(&mut s, Event::CommitGossip { term: 1, commit: 4096 });
        assert!(
            matches!(s.role(), Role::Follower),
            "same-term commit gossip must depose a candidate"
        );
        assert!(acts.iter().any(|a| matches!(a, Action::AdvanceCommit { commit: 4096 })));
    }

    // ---- M4 reconciliation wiring ----

    #[test]
    fn follower_truncates_on_divergent_term_map_and_resumes_after_feedback() {
        // node 1 was a failed leader: own map (1,0),(2,4096), durable 6000
        let mut s = ElectionSm::new(cfg(1), None, &[(1, 0), (2, 4096)], 6000, 0);
        // adopt term 3 via a grant, then the term-3 leader ships its map
        step(&mut s, Event::RequestVote { from: 0, new_term: 3, last_term: 1, last_durable: 7000 });
        let acts = step(
            &mut s,
            Event::TermMapReceived { term: 3, entries: vec![(1, 0), (3, 4096)] },
        );
        let trunc = acts.iter().find_map(|a| match a {
            Action::Truncate { epoch, to, new_map } => Some((*epoch, *to, new_map.clone())),
            _ => None,
        });
        let (epoch, to, new_map) = trunc.expect("must truncate the divergent tail");
        assert_eq!(to, 4096);
        assert_eq!(new_map, vec![(1, 0)]);
        // while truncating: data-plane events latched (no commit advance)
        assert!(step(&mut s, Event::CommitGossip { term: 3, commit: 5000 }).is_empty());
        // agent feedback: truncation done (matching epoch releases the latch)
        step(&mut s, Event::Truncated { epoch, to: 4096 });
        assert_eq!(s.term_map(), &[(1, 0)]);
        // commit gossip clamps nothing here — it flows again (bounded by
        // durable at apply time, M5; the counter itself is raw)
        let acts = step(&mut s, Event::CommitGossip { term: 3, commit: 5000 });
        assert!(acts.iter().any(|a| matches!(a, Action::AdvanceCommit { commit: 5000 })));
    }

    /// The leader re-ships its term map on the commit-gossip cadence: whenever
    /// `rank_leader` advances the commit, a `ShipTermMap` rides alongside the
    /// `GossipCommit` so a lagging/reconnecting follower can reconcile.
    #[test]
    fn leader_reships_term_map_on_commit_advance() {
        let mut s = sm(0);
        step(&mut s, Event::Tick { now_ns: 301 });
        step(&mut s, Event::Vote { from: 1, term: 1, granted: true });
        assert!(matches!(s.role(), Role::Leader));
        step(&mut s, Event::NewTermAppended { position: 32 });
        step(&mut s, Event::DurableAdvanced { durable: 32 });
        // A follower report drives a quorum commit through rank_leader.
        let acts = step(&mut s, Event::Report { from: 1, term: 1, durable: 32 });
        assert!(acts.iter().any(|a| matches!(a, Action::AdvanceCommit { commit: 32 })));
        assert!(acts.iter().any(|a| matches!(a, Action::GossipCommit { commit: 32 })));
        assert!(
            acts.iter().any(|a| matches!(a, Action::ShipTermMap { entries } if entries == &vec![(1, 0)])),
            "commit advance must re-ship the term map on the gossip cadence"
        );
    }

    /// Task-9 fix: an IDLE leader (no commit movement) re-emits exactly one
    /// `GossipCommit` + one `ShipTermMap` each time the gossip floor elapses, and
    /// NOTHING between crossings. This is the mechanism that lets a divergent node
    /// rejoining an idle cluster still receive a term map to reconcile against.
    #[test]
    fn idle_leader_reemits_on_gossip_floor() {
        // Small floor (1000 ns) so a couple of ticks straddle it; distinct from
        // the election timeout so a leader tick is unambiguous.
        let cfg = ElectionConfig {
            id: 0,
            config: genesis_voters(&[0, 1, 2]),
            config_position: 0,
            election_timeout_min_ns: 150,
            election_timeout_max_ns: 300,
            gossip_floor_ns: 1000,
            seed: 42,
        };
        let mut s = ElectionSm::new(cfg, None, &[], 0, 0);
        // Drive to leader of term 1. `become_leader` re-anchors the idle floor to
        // when leadership began (M-4) — here the last observed tick, 301 — so the
        // first idle re-emission is a clean `gossip_floor_ns` (1000) after that.
        step(&mut s, Event::Tick { now_ns: 301 });
        step(&mut s, Event::Vote { from: 1, term: 1, granted: true });
        assert!(matches!(s.role(), Role::Leader));

        // No reports arrive → commit never advances; commit_seen stays 0. Each
        // floor crossing must emit exactly one commit-gossip + one term-map ship.
        let gossips = |acts: &[Action]| {
            (
                acts.iter().filter(|a| matches!(a, Action::GossipCommit { commit: 0 })).count(),
                acts.iter().filter(|a| matches!(a, Action::ShipTermMap { .. })).count(),
            )
        };

        // Before the first crossing (1000 - 301 = 699 < 1000): nothing.
        let pre = step(&mut s, Event::Tick { now_ns: 1000 });
        assert!(pre.is_empty(), "no re-emission before the first floor elapses");

        // First crossing: 1301 - 301 >= 1000.
        let a = step(&mut s, Event::Tick { now_ns: 1301 });
        assert_eq!(gossips(&a), (1, 1), "first floor crossing emits one commit + one map");
        assert!(!a.iter().any(|x| matches!(x, Action::AdvanceCommit { .. })), "idle: no commit advance");

        // Between crossings (1800 - 1301 = 499 < 1000): nothing at all.
        let b = step(&mut s, Event::Tick { now_ns: 1800 });
        assert!(b.is_empty(), "no re-emission before the floor elapses again");

        // Second crossing: 2301 - 1301 >= 1000.
        let c = step(&mut s, Event::Tick { now_ns: 2301 });
        assert_eq!(gossips(&c), (1, 1), "second floor crossing emits one commit + one map");

        // Immediately after: still nothing until the floor elapses once more.
        let d = step(&mut s, Event::Tick { now_ns: 2500 });
        assert!(d.is_empty(), "no re-emission immediately after a crossing");
    }

    #[test]
    fn leader_ships_term_map_on_open() {
        let mut s = sm(0);
        step(&mut s, Event::Tick { now_ns: 301 });
        let acts = step(&mut s, Event::Vote { from: 1, term: 1, granted: true });
        assert!(acts.iter().any(|a| matches!(a, Action::BecomeLeader { .. })));
        assert!(acts.iter().any(
            |a| matches!(a, Action::ShipTermMap { entries } if entries == &vec![(1, 0)])
        ));
    }

    /// Data-stamped recording (F3): a follower's own term map grows ONLY when it
    /// durably writes a new term's data (`DataTermObserved`), never by adopting a
    /// gossiped leader entry. The end-to-end Scenario-B pin at the SM level: once
    /// the term is data-stamped, the leader's identical map reconciles clean — no
    /// truncation.
    #[test]
    fn data_term_observed_grows_map_and_makes_reconcile_clean() {
        let mut s = ElectionSm::new(cfg(1), None, &[(1, 0)], 3000, 0);
        // We durably streamed term-2 data opening at 2000: the map grows and the
        // agent is told to persist it.
        let acts = step(&mut s, Event::DataTermObserved { term: 2, base: 2000 });
        assert!(acts.iter().any(
            |a| matches!(a, Action::PersistTermMap { new_map } if new_map == &vec![(1, 0), (2, 2000)])
        ));
        assert_eq!(s.term_map(), &[(1, 0), (2, 2000)]);
        // A second identical observation is idempotent — no-op (no PersistTermMap).
        let dup = step(&mut s, Event::DataTermObserved { term: 2, base: 2000 });
        assert!(dup.is_empty(), "re-observing the same term must be a no-op");
        assert_eq!(s.term_map(), &[(1, 0), (2, 2000)]);
        // Now the term-2 leader ships its identical map: because we data-stamped
        // term 2 ourselves, reconciliation is CLEAN — no Truncate.
        step(&mut s, Event::RequestVote { from: 0, new_term: 2, last_term: 2, last_durable: 4000 });
        let acts = step(
            &mut s,
            Event::TermMapReceived { term: 2, entries: vec![(1, 0), (2, 2000)] },
        );
        assert!(
            !acts.iter().any(|a| matches!(a, Action::Truncate { .. })),
            "a data-stamped term must reconcile clean, never truncate"
        );
        assert_eq!(s.term_map(), &[(1, 0), (2, 2000)]);
    }

    /// The Scenario-A safety pin at the SM level (mirrors the pure-fn test): a
    /// healed ex-leader that did NOT data-stamp term 2 — its own map is `[(1,0)]`
    /// at durable 3000 — truncates its divergent `[2000,3000)` tail when the
    /// leader ships `[(1,0),(2,2000)]`. Pre-fix this adopted the gossiped entry
    /// and served split-history.
    #[test]
    fn ex_leader_without_data_stamp_truncates() {
        let mut s = ElectionSm::new(cfg(1), None, &[(1, 0)], 3000, 0);
        step(&mut s, Event::RequestVote { from: 0, new_term: 2, last_term: 2, last_durable: 4000 });
        let acts = step(
            &mut s,
            Event::TermMapReceived { term: 2, entries: vec![(1, 0), (2, 2000)] },
        );
        let trunc = acts.iter().find_map(|a| match a {
            Action::Truncate { to, new_map, .. } => Some((*to, new_map.clone())),
            _ => None,
        });
        let (to, new_map) = trunc.expect("uncovered divergent tail must truncate");
        assert_eq!(to, 2000);
        assert_eq!(new_map, vec![(1, 0)]);
    }

    /// A stale-term map (term below ours) is dropped; no reconciliation runs.
    #[test]
    fn stale_term_map_is_dropped() {
        let mut s = ElectionSm::new(cfg(1), None, &[(1, 0), (2, 4096)], 6000, 0);
        step(&mut s, Event::RequestVote { from: 0, new_term: 3, last_term: 2, last_durable: 7000 });
        let acts = step(
            &mut s,
            Event::TermMapReceived { term: 1, entries: vec![(1, 0), (9, 0)] },
        );
        assert!(acts.is_empty());
        assert_eq!(s.term_map(), &[(1, 0), (2, 4096)]);
    }

    /// M5: a higher term adopted mid-truncation is ADOPTABLE (the latch never
    /// blocks an election) but does NOT clear the truncating latch — the physical
    /// truncation was already issued and must complete. The in-flight
    /// truncation's own matching-epoch ack then releases the latch and adopts its
    /// (valid-prefix) pruned map.
    #[test]
    fn higher_term_mid_truncation_is_adoptable_but_holds_latch() {
        let mut s = ElectionSm::new(cfg(1), None, &[(1, 0), (2, 4096)], 6000, 0);
        step(&mut s, Event::RequestVote { from: 0, new_term: 3, last_term: 1, last_durable: 7000 });
        let acts =
            step(&mut s, Event::TermMapReceived { term: 3, entries: vec![(1, 0), (3, 4096)] });
        let epoch = extract_truncate_epoch(&acts);
        // A newer election reaches us while truncating: adoptable, latch held.
        let acts =
            step(&mut s, Event::RequestVote { from: 2, new_term: 4, last_term: 3, last_durable: 8000 });
        assert!(acts.iter().any(|a| matches!(a, Action::BecomeFollower { term: 4, .. })));
        assert_eq!(s.current_term(), 4);
        assert!(s.is_truncating(), "adoption must NOT clear the latch (M5)");
        // The in-flight truncation acks with its own epoch → latch releases and
        // adopts the pruned map.
        step(&mut s, Event::Truncated { epoch, to: 4096 });
        assert!(!s.is_truncating());
        assert_eq!(s.term_map(), &[(1, 0)]);
    }

    /// I-1: physical truncation is the truth about durability. A stale/unexpected
    /// `Truncated` ack — one that arrives while we are NOT truncating — still
    /// clamps `durable` DOWN (never overclaim bytes physically truncated away) but
    /// changes nothing else and emits no actions. Here `to (500) < durable (1000)`.
    #[test]
    fn stale_truncated_ack_clamps_durable_only() {
        let mut s = ElectionSm::new(cfg(1), None, &[(1, 0)], 1000, 0);
        let before_term = s.current_term();
        let before_map = s.term_map().to_vec();
        // Not truncating: the ack clamps durable and returns no actions.
        let acts = step(&mut s, Event::Truncated { epoch: 1, to: 500 });
        assert!(acts.is_empty(), "a stale Truncated ack emits no actions");
        assert_eq!(s.current_term(), before_term, "term unchanged");
        assert_eq!(s.term_map(), before_map.as_slice(), "term map unchanged");
        assert!(!s.is_truncating(), "latch stays clear (we never asked to truncate)");
        // durable is now 500: the next election solicits with last_durable == 500,
        // proving the clamp took (it was 1000 before the ack).
        let acts = step(&mut s, Event::Tick { now_ns: 301 });
        let last_durable = acts.iter().find_map(|a| match a {
            Action::StartElection { last_durable, .. } => Some(*last_durable),
            _ => None,
        });
        assert_eq!(last_durable, Some(500), "durable was clamped to the truncated point");
    }

    /// M6 Task 8: reconciliation with no common prefix WIPES-and-rejoins by
    /// default — a `CountWipe` tag plus a truncate-to-0 with an empty map (never a
    /// `Fatal`). own=[(1,0)] @5000 vs a leader whose shipped tail begins at 1<<20:
    /// the leader's window slid past our first byte (its prefix was purged).
    #[test]
    fn no_common_prefix_wipes_and_rejoins() {
        let mut s = ElectionSm::new(cfg(1), None, &[(1, 0)], 5000, 0);
        step(&mut s, Event::RequestVote { from: 0, new_term: 41, last_term: 40, last_durable: 9000 });
        let acts = step(
            &mut s,
            Event::TermMapReceived { term: 41, entries: vec![(40, 1 << 20), (41, 2 << 20)] },
        );
        assert!(acts.iter().any(|a| matches!(a, Action::CountWipe)), "wipe was counted");
        assert!(
            acts.iter().any(|a| matches!(a, Action::Truncate { to: 0, new_map, .. } if new_map.is_empty())),
            "wipe is a truncate-to-0 with an empty map"
        );
        assert!(!acts.iter().any(|a| matches!(a, Action::Fatal { .. })), "no fail-stop");
    }

    /// The counterfactual: with wipe DISABLED, the identical world fail-stops with
    /// `Fatal` (the old behavior) — documenting exactly what the wipe path changed.
    #[test]
    fn no_common_prefix_fatal_when_wipe_disabled() {
        let mut s = ElectionSm::new(cfg(1), None, &[(1, 0)], 5000, 0);
        s.set_wipe_on_no_common_prefix(false);
        step(&mut s, Event::RequestVote { from: 0, new_term: 41, last_term: 40, last_durable: 9000 });
        let acts = step(
            &mut s,
            Event::TermMapReceived { term: 41, entries: vec![(40, 1 << 20), (41, 2 << 20)] },
        );
        assert!(acts.iter().any(|a| matches!(a, Action::Fatal { .. })));
        assert!(!acts.iter().any(|a| matches!(a, Action::Truncate { .. })));
        assert!(!acts.iter().any(|a| matches!(a, Action::CountWipe)));
    }

    // ---- M5 truncation-epoch protocol (SM-owned ack identity) ----

    /// A `Truncated` ack carrying a NON-matching epoch clamps durable (physical
    /// truncation is truth) but must NOT release the latch; only the matching
    /// epoch does.
    #[test]
    fn truncated_ack_with_wrong_epoch_keeps_latch() {
        let mut sm = sm_with_divergent_map();
        let mut out = Vec::new();
        sm.step(Event::TermMapReceived { term: 3, entries: vec![(1, 0), (3, 4096)] }, &mut out);
        let epoch = out
            .iter()
            .find_map(|a| match a {
                Action::Truncate { epoch, .. } => Some(*epoch),
                _ => None,
            })
            .expect("truncate issued");
        // stale ack (epoch-1): durable clamps, latch stays
        out.clear();
        sm.step(Event::Truncated { epoch: epoch - 1, to: 4096 }, &mut out);
        assert!(sm.is_truncating(), "stale-epoch ack must not release the latch");
        // matching ack: latch released
        out.clear();
        sm.step(Event::Truncated { epoch, to: 4096 }, &mut out);
        assert!(!sm.is_truncating());
    }

    /// A higher term adopted mid-truncation must NOT clear the latch; the
    /// in-flight truncation's own matching-epoch ack releases it (and the durable
    /// clamps to the physical truth).
    #[test]
    fn adoption_mid_truncation_holds_latch_until_matching_ack() {
        let mut sm = sm_with_divergent_map();
        let mut out = Vec::new();
        sm.step(Event::TermMapReceived { term: 3, entries: vec![(1, 0), (3, 4096)] }, &mut out);
        let epoch = extract_truncate_epoch(&out);
        // higher-term RequestVote adopts term 4 mid-truncation
        out.clear();
        sm.step(Event::RequestVote { from: 1, new_term: 4, last_term: 3, last_durable: 9000 }, &mut out);
        assert!(sm.is_truncating(), "adoption must NOT clear the truncating latch (M4 I-1)");
        // duplicate term maps in the window are dropped by the latch (no actions)
        out.clear();
        sm.step(Event::TermMapReceived { term: 4, entries: vec![(4, 0)] }, &mut out);
        assert!(out.is_empty());
        // the in-flight truncation acks with its own epoch → latch releases
        out.clear();
        sm.step(Event::Truncated { epoch, to: 4096 }, &mut out);
        assert!(!sm.is_truncating());
        assert!(sm.durable() <= 4096, "durable clamped to physical truth");
    }

    /// A RequestVote from a NON-member is ignored entirely — no answer AND no term
    /// adoption (symmetry with the Vote-grant membership check, M4 Info finding).
    #[test]
    fn request_vote_from_non_member_is_ignored() {
        let mut sm = fresh_follower(); // members [0,1,2], id 0
        let mut out = Vec::new();
        sm.step(Event::RequestVote { from: 9, new_term: 5, last_term: 4, last_durable: 1 << 20 }, &mut out);
        assert!(out.is_empty(), "non-member RequestVote must produce nothing");
        assert_eq!(sm.current_term(), 1, "and must not adopt the term");
    }

    // ---- M7 dynamic membership ----

    /// The tracker rebuild carries existing per-follower reports across an
    /// adoption boundary (commit ranking never restarts from zero), and the
    /// newly-added voter gets a dense `CommitTracker` slot from the rebuild.
    #[test]
    fn adopt_config_rebuilds_tracker_and_carries_reports() {
        // 3 voters {1,2,3}; self=1 becomes leader of term 1.
        let mut s = ElectionSm::new(cfg_members(1, vec![1, 2, 3]), None, &[], 0, 0);
        step(&mut s, Event::Tick { now_ns: 301 });
        step(&mut s, Event::Vote { from: 2, term: 1, granted: true }); // majority of 3
        assert!(matches!(s.role(), Role::Leader));
        step(&mut s, Event::NewTermAppended { position: 32 });
        step(&mut s, Event::DurableAdvanced { durable: 32 });
        // Majority of 3 is 2: own + node 2's report alone already commits 32.
        let acts = step(&mut s, Event::Report { from: 2, term: 1, durable: 32 });
        assert!(
            acts.iter().any(|a| matches!(a, Action::AdvanceCommit { commit: 32 })),
            "pre-adoption: quorum on the carried reports commits 32"
        );
        step(&mut s, Event::Report { from: 3, term: 1, durable: 32 });

        // Adopt v1: a higher-version config adding voter 5, fed directly as
        // the append path would (ConfigObserved does not care how the config
        // was derived, only that its version is higher).
        let mut new_cfg = s.config().clone();
        new_cfg.voters.push((5, (5, 5)));
        new_cfg.version = 1;
        let acts = step(&mut s, Event::ConfigObserved { position: 40, config: new_cfg.clone() });
        assert!(acts.iter().any(|a| matches!(a, Action::ConfigAdopted { position: 40, .. })));
        assert_eq!(s.config(), &new_cfg);
        assert_eq!(s.config_position(), 40);
        assert!(s.follower_slot(5).is_some(), "the new voter must get a tracked slot");

        // Post-adoption: fresh durable + reports from the carried members (2,3)
        // alone (no report from 5 yet) still reach quorum-of-4.
        step(&mut s, Event::DurableAdvanced { durable: 64 });
        step(&mut s, Event::Report { from: 2, term: 1, durable: 64 });
        let acts = step(&mut s, Event::Report { from: 3, term: 1, durable: 64 });
        assert!(
            acts.iter().any(|a| matches!(a, Action::AdvanceCommit { commit: 64 })),
            "commit advances post-adoption via the carried reports for 2 and 3"
        );
    }

    /// `propose_config` is refused: by a non-leader (`NotLeader`), by a leader
    /// that has not yet committed its own `NewTerm` frame (`NotServing` — the
    /// single-server-change precondition), and while a prior proposal is
    /// still uncommitted (`ChangePending` — one in flight).
    #[test]
    fn propose_refused_unless_serving_and_no_pending() {
        let mut s = sm(0); // voters [0,1,2], id 0
        step(&mut s, Event::Tick { now_ns: 301 });
        assert!(matches!(s.role(), Role::Candidate));
        assert_eq!(
            s.propose_config(ConfigOp::AddLearner { id: 5, addr: (5, 5) }, 0),
            Err(ProposeError::NotLeader)
        );

        step(&mut s, Event::Vote { from: 1, term: 1, granted: true });
        assert!(matches!(s.role(), Role::Leader));
        assert!(!s.can_serve());
        assert_eq!(
            s.propose_config(ConfigOp::AddLearner { id: 5, addr: (5, 5) }, 0),
            Err(ProposeError::NotServing)
        );

        // Commit the NewTerm frame -> serving.
        step(&mut s, Event::NewTermAppended { position: 32 });
        step(&mut s, Event::DurableAdvanced { durable: 32 });
        step(&mut s, Event::Report { from: 1, term: 1, durable: 32 });
        step(&mut s, Event::Tick { now_ns: 310 });
        assert!(s.can_serve());

        let new_cfg = s.propose_config(ConfigOp::AddLearner { id: 5, addr: (5, 5) }, 0).unwrap();
        assert_eq!(new_cfg.version, 1);

        // The append path feeds the observed config back; it has not
        // committed yet (commit_seen stays 32 < config_position 40).
        step(&mut s, Event::ConfigObserved { position: 40, config: new_cfg });
        assert!(s.config_pending());
        assert_eq!(
            s.propose_config(ConfigOp::AddLearner { id: 6, addr: (6, 6) }, 0),
            Err(ProposeError::ChangePending)
        );
    }

    /// `PromoteLearner` additionally requires the learner's last carried
    /// report to be within `slack` of `commit_seen`.
    #[test]
    fn promote_requires_caught_up_learner() {
        let mut s = ElectionSm::new(cfg_with_learners(0, vec![0, 1, 2], vec![5]), None, &[], 0, 0);
        step(&mut s, Event::Tick { now_ns: 301 });
        step(&mut s, Event::Vote { from: 1, term: 1, granted: true });
        assert!(matches!(s.role(), Role::Leader));
        step(&mut s, Event::NewTermAppended { position: 32 });
        step(&mut s, Event::DurableAdvanced { durable: 100_000 });
        let acts = step(&mut s, Event::Report { from: 1, term: 1, durable: 100_000 });
        assert!(acts.iter().any(|a| matches!(a, Action::AdvanceCommit { commit: 100_000 })));
        assert!(s.can_serve());

        step(&mut s, Event::Report { from: 5, term: 1, durable: 10_000 });
        assert_eq!(s.last_report(5), Some(10_000));
        assert!(!s.config_pending());

        assert_eq!(
            s.propose_config(ConfigOp::PromoteLearner { id: 5 }, 32_768),
            Err(ProposeError::NotCaughtUp { gap: 57_232 })
        );

        step(&mut s, Event::Report { from: 5, term: 1, durable: 90_000 });
        assert!(s.propose_config(ConfigOp::PromoteLearner { id: 5 }, 32_768).is_ok());
    }

    /// `ConfigObserved` adopts iff `config.version` strictly exceeds the
    /// current version: re-feeding the same version is a no-op, and a lower
    /// version arriving after a higher one is ignored (never regresses).
    #[test]
    fn config_observed_is_idempotent_and_monotone_by_version() {
        let mut s = sm(0); // voters [0,1,2], id 0
        let mut cfg_v1 = s.config().clone();
        cfg_v1.voters.push((5, (5, 5)));
        cfg_v1.version = 1;

        let acts = step(&mut s, Event::ConfigObserved { position: 40, config: cfg_v1.clone() });
        assert!(acts.iter().any(|a| matches!(a, Action::ConfigAdopted { .. })));
        assert_eq!(s.config().version, 1);

        // Re-observation of the SAME version: one adoption total, this is a no-op.
        let acts = step(&mut s, Event::ConfigObserved { position: 40, config: cfg_v1.clone() });
        assert!(acts.is_empty(), "same-version re-observation must be a no-op");
        assert_eq!(s.config().version, 1);

        // A LOWER version arriving after a higher one is ignored.
        let mut cfg_v0 = cfg_v1;
        cfg_v0.version = 0;
        let acts = step(&mut s, Event::ConfigObserved { position: 999, config: cfg_v0 });
        assert!(acts.is_empty());
        assert_eq!(s.config().version, 1, "a stale lower version must not regress adoption");
    }

    /// A follower not present in a just-adopted config fail-stops.
    #[test]
    fn removed_follower_emits_halt() {
        // 3 voters [1,2,3]; self = 3 (a plain follower, never elected).
        let mut s = ElectionSm::new(cfg_members(3, vec![1, 2, 3]), None, &[], 0, 0);
        assert!(matches!(s.role(), Role::Follower));
        let mut new_cfg = s.config().clone();
        new_cfg.voters.retain(|(id, _)| *id != 3);
        new_cfg.tombstones.push(3);
        new_cfg.version = 1;
        let acts = step(&mut s, Event::ConfigObserved { position: 40, config: new_cfg });
        assert!(acts.iter().any(|a| matches!(a, Action::HaltRemoved)));
    }

    /// After a voter is removed from the adopted config, its `Report` no
    /// longer moves commit (`follower_slot` drops it) and its `RequestVote`
    /// is ignored entirely — extending the existing forged-report /
    /// non-member tests to the dynamic (post-adoption) case.
    #[test]
    fn nonmember_vote_and_report_stay_dropped_after_adoption() {
        let mut s = ElectionSm::new(cfg_members(0, vec![0, 1, 2, 3]), None, &[], 0, 0);
        step(&mut s, Event::Tick { now_ns: 301 });
        step(&mut s, Event::Vote { from: 1, term: 1, granted: true });
        step(&mut s, Event::Vote { from: 2, term: 1, granted: true });
        assert!(matches!(s.role(), Role::Leader));

        let mut new_cfg = s.config().clone();
        new_cfg.voters.retain(|(id, _)| *id != 3);
        new_cfg.tombstones.push(3);
        new_cfg.version = 1;
        step(&mut s, Event::ConfigObserved { position: 40, config: new_cfg });
        assert!(!s.config().contains(3));
        assert!(s.follower_slot(3).is_none());

        // The removed voter's Report must not move commit.
        let acts = step(&mut s, Event::Report { from: 3, term: 1, durable: 1 << 30 });
        assert!(
            !acts.iter().any(|a| matches!(a, Action::AdvanceCommit { .. })),
            "a removed voter's Report must not move commit"
        );

        // The removed voter's RequestVote is ignored entirely: no answer, no
        // term adoption (mirrors `request_vote_from_non_member_is_ignored`).
        let before_term = s.current_term();
        let acts =
            step(&mut s, Event::RequestVote { from: 3, new_term: before_term + 5, last_term: 1, last_durable: 0 });
        assert!(acts.is_empty(), "a removed voter's RequestVote must produce nothing");
        assert_eq!(s.current_term(), before_term, "and must not adopt the term");
    }
}
