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
//! | `GossipCommit`        | Gossip `CommitPosition{commit}` to followers (leader only).                                                                    |
//!
//! Determinism note: election timeouts are re-randomized on every arming (a
//! follower reset on leader activity, or a candidate starting a fresh
//! election) from `[min, max)` via a crate-local xorshift — no dependency.

use crate::commit::CommitTracker;
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
    /// Static voting membership, self included. Position in this Vec is the
    /// follower index used by CommitTracker when leader.
    pub members: Vec<NodeId>,
    pub election_timeout_min_ns: u64, // default 150_000_000
    pub election_timeout_max_ns: u64, // default 300_000_000
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
    /// Agent feedback: the archive was truncated to `to` and the write counter
    /// re-primed. Lands durable at `to`, adopts the pending map, clears the
    /// truncating latch.
    Truncated { to: u64 },
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
    /// Truncate our log to `to`, THEN (via `Truncated` feedback) replace our
    /// term map with `new_map`. The agent must call `Archive::truncate_to(to)`,
    /// re-prime the write counter, then feed `Truncated{to}` back. The SM latches
    /// out data-plane events until that feedback arrives.
    Truncate { to: u64, new_map: Vec<(u32, u64)> },
    /// Ship our term map to followers (leader only): the last
    /// `MAX_TERM_MAP_WIRE_ENTRIES` entries. Emitted on `BecomeLeader` and
    /// piggybacked on the commit-gossip cadence.
    ShipTermMap { entries: Vec<(u32, u64)> },
    /// Persist the reconciled term map durably (follower adopted leader entries
    /// with no truncation needed — keeps vote credentials honest).
    PersistTermMap { new_map: Vec<(u32, u64)> },
    /// Reconciliation found no common prefix — the divergence predates the
    /// shipped window. Incremental repair is impossible; the agent logs and
    /// panics (M6 snapshot install is the real fix). The sim asserts this never
    /// fires at `<= MAX_TERM_MAP_WIRE_ENTRIES` terms.
    Fatal { reason: &'static str },
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
    members: Vec<NodeId>,
    timeout_min_ns: u64,
    timeout_max_ns: u64,
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

    /// Candidate: grants counted for `current_term` (self included).
    votes_received: Vec<NodeId>,

    /// Leader: quorum commit ranking over follower reports.
    tracker: CommitTracker,
    /// Leader: position of the NewTerm no-op frame this term (once appended).
    new_term_pos: Option<u64>,
    /// Leader: true once the NewTerm frame committed (Raft §5.4.2 read gate).
    serving: bool,

    /// Follower: truncation in flight. While set, data-plane events are latched
    /// out (no commit/durable advance); only term/vote events (and the
    /// `Truncated` feedback) are processed — a higher term stays adoptable.
    truncating: bool,
    /// The reconciled map to adopt once `Truncated` feedback arrives.
    pending_new_map: Option<Vec<(u32, u64)>>,
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
        // Fail loudly at construction, not via a usize underflow later
        // (n_members - 1) or a nonsensical timer span.
        assert!(
            !cfg.members.is_empty() && cfg.members.contains(&cfg.id),
            "election membership must be non-empty and contain self (id={}, members={:?})",
            cfg.id,
            cfg.members
        );
        debug_assert!(
            cfg.election_timeout_min_ns <= cfg.election_timeout_max_ns,
            "election_timeout_min_ns must be <= election_timeout_max_ns (span==0 is allowed)"
        );

        let map_term = recovered_term_map.last().map(|(t, _)| *t).unwrap_or(0);
        let vote_term = recovered_vote.map(|(t, _)| t).unwrap_or(0);
        let current_term = vote_term.max(map_term);

        let n_members = cfg.members.len();
        let tracker = CommitTracker::new(n_members - 1, n_members);

        let mut sm = Self {
            id: cfg.id,
            members: cfg.members,
            timeout_min_ns: cfg.election_timeout_min_ns,
            timeout_max_ns: cfg.election_timeout_max_ns,
            rng: XorShift64::new(cfg.seed),
            role: Role::Follower,
            current_term,
            voted_for: recovered_vote,
            term_map: recovered_term_map.to_vec(),
            durable,
            commit_seen: 0,
            pending_leader_activity: false,
            timeout_deadline_ns: 0,
            votes_received: Vec::new(),
            tracker,
            new_term_pos: None,
            serving: false,
            truncating: false,
            pending_new_map: None,
        };
        sm.arm_timeout(now_ns);
        sm
    }

    pub fn step(&mut self, ev: Event, out: &mut Vec<Action>) {
        // Truncating latch: drop data-plane events while a truncation is in
        // flight. Only term/vote events (which may adopt a strictly higher
        // term — that must always be possible) and the `Truncated` feedback
        // are processed.
        if self.truncating && !matches!(ev, Event::RequestVote { .. } | Event::Vote { .. } | Event::Truncated { .. })
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
                if matches!(self.role, Role::Leader)
                    && let Some(slot) = self.follower_slot(from)
                {
                    self.tracker.on_durable(slot, durable);
                    self.rank_leader(out);
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

            Event::Truncated { to } => {
                // Only meaningful while we asked for a truncation. A higher term
                // adopted mid-flight clears the latch and abandons the pending
                // map; ignore its stale feedback (the new leader re-ships).
                if !self.truncating {
                    return;
                }
                self.durable = to;
                if let Some(m) = self.pending_new_map.take() {
                    self.term_map = m;
                }
                self.truncating = false;
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
        match self.role {
            Role::Leader => self.rank_leader(out),
            Role::Follower | Role::Candidate => {
                if self.pending_leader_activity {
                    // Saw the leader since last tick: re-arm, do not time out.
                    self.pending_leader_activity = false;
                    self.arm_timeout(now_ns);
                } else if now_ns >= self.timeout_deadline_ns {
                    self.start_election(now_ns, out);
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
        // Open the term in the term map (base = our durable = the collapse point).
        self.term_map.push((self.current_term, self.durable));
        // Fresh follower slots: stale-term reports must not certify the new term.
        self.tracker.reset_reports();
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
        // A term change invalidates any pending truncation (computed against the
        // old leader's map); the new leader will re-ship and we re-reconcile.
        self.truncating = false;
        self.pending_new_map = None;
        out.push(Action::BecomeFollower { term: new_term, leader });
    }

    /// Run reconciliation against a leader-shipped map and emit the derived
    /// action: `Truncate` (+ latch) when a byte is invalid, `PersistTermMap`
    /// when the map merely grows, `Fatal` when there is no common prefix.
    fn reconcile_term_map(&mut self, entries: &[(u32, u64)], out: &mut Vec<Action>) {
        match reconcile(&self.term_map, self.durable, entries) {
            Reconcile::NoCommonPrefix => out.push(Action::Fatal {
                reason: "term-map reconciliation found no common prefix (snapshot install required)",
            }),
            Reconcile::Ok(Outcome { valid_up_to, new_map }) => {
                if valid_up_to < self.durable {
                    // A byte is invalid: latch out the data plane and truncate.
                    // The map is adopted only once the archive confirms.
                    self.truncating = true;
                    self.pending_new_map = Some(new_map.clone());
                    out.push(Action::Truncate { to: valid_up_to, new_map });
                } else if new_map != self.term_map {
                    // Nothing to truncate, but we adopted covering entries.
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

    fn rank_leader(&mut self, out: &mut Vec<Action>) {
        if let Some(c) = self.tracker.advance(self.durable)
            && c > self.commit_seen
        {
            self.commit_seen = c;
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

    fn cfg(id: NodeId) -> ElectionConfig {
        ElectionConfig {
            id,
            members: vec![0, 1, 2],
            election_timeout_min_ns: 150,
            election_timeout_max_ns: 300,
            seed: 42 + id as u64,
        }
    }

    fn cfg_members(id: NodeId, members: Vec<NodeId>) -> ElectionConfig {
        ElectionConfig {
            id,
            members,
            election_timeout_min_ns: 150,
            election_timeout_max_ns: 300,
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
            Action::Truncate { to, new_map } => Some((*to, new_map.clone())),
            _ => None,
        });
        let (to, new_map) = trunc.expect("must truncate the divergent tail");
        assert_eq!(to, 4096);
        assert_eq!(new_map, vec![(1, 0)]);
        // while truncating: data-plane events latched (no commit advance)
        assert!(step(&mut s, Event::CommitGossip { term: 3, commit: 5000 }).is_empty());
        // agent feedback: truncation done
        step(&mut s, Event::Truncated { to: 4096 });
        assert_eq!(s.term_map(), &[(1, 0)]);
        // commit gossip clamps nothing here — it flows again (bounded by
        // durable at apply time, M5; the counter itself is raw)
        let acts = step(&mut s, Event::CommitGossip { term: 3, commit: 5000 });
        assert!(acts.iter().any(|a| matches!(a, Action::AdvanceCommit { commit: 5000 })));
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

    /// A follower behind the leader (streamed bytes, stale map) adopts the
    /// covering entries with no truncation and persists the grown map.
    #[test]
    fn follower_adopts_covering_entries_without_truncation() {
        let mut s = ElectionSm::new(cfg(1), None, &[(1, 0)], 3000, 0);
        step(&mut s, Event::RequestVote { from: 0, new_term: 2, last_term: 1, last_durable: 4000 });
        let acts = step(
            &mut s,
            Event::TermMapReceived { term: 2, entries: vec![(1, 0), (2, 2000)] },
        );
        assert!(!acts.iter().any(|a| matches!(a, Action::Truncate { .. })));
        assert!(acts.iter().any(
            |a| matches!(a, Action::PersistTermMap { new_map } if new_map == &vec![(1, 0), (2, 2000)])
        ));
        assert_eq!(s.term_map(), &[(1, 0), (2, 2000)]);
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

    /// A higher term adopted mid-truncation clears the latch and abandons the
    /// pending map; stale `Truncated` feedback is then ignored.
    #[test]
    fn higher_term_mid_truncation_clears_latch() {
        let mut s = ElectionSm::new(cfg(1), None, &[(1, 0), (2, 4096)], 6000, 0);
        step(&mut s, Event::RequestVote { from: 0, new_term: 3, last_term: 1, last_durable: 7000 });
        let acts =
            step(&mut s, Event::TermMapReceived { term: 3, entries: vec![(1, 0), (3, 4096)] });
        assert!(acts.iter().any(|a| matches!(a, Action::Truncate { .. })));
        // A newer election reaches us while truncating: the latch must not block it.
        let acts =
            step(&mut s, Event::RequestVote { from: 2, new_term: 4, last_term: 3, last_durable: 8000 });
        assert!(acts.iter().any(|a| matches!(a, Action::BecomeFollower { term: 4, .. })));
        assert_eq!(s.current_term(), 4);
        // Stale truncation feedback for the abandoned request is ignored.
        step(&mut s, Event::Truncated { to: 4096 });
        assert_eq!(s.term_map(), &[(1, 0), (2, 4096)]);
    }

    /// Reconciliation with no common prefix surfaces `Fatal` (never truncates).
    #[test]
    fn no_common_prefix_surfaces_fatal() {
        let mut s = ElectionSm::new(cfg(1), None, &[(1, 0)], 5000, 0);
        step(&mut s, Event::RequestVote { from: 0, new_term: 41, last_term: 40, last_durable: 9000 });
        let acts = step(
            &mut s,
            Event::TermMapReceived { term: 41, entries: vec![(40, 1 << 20), (41, 2 << 20)] },
        );
        assert!(acts.iter().any(|a| matches!(a, Action::Fatal { .. })));
        assert!(!acts.iter().any(|a| matches!(a, Action::Truncate { .. })));
    }
}
