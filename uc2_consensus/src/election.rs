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
        };
        sm.arm_timeout(now_ns);
        sm
    }

    pub fn step(&mut self, ev: Event, out: &mut Vec<Action>) {
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
                self.pending_leader_activity = true;
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
                if granted && matches!(self.role, Role::Candidate) {
                    if !self.votes_received.contains(&from) {
                        self.votes_received.push(from);
                    }
                    if self.votes_received.len() >= self.majority() {
                        self.become_leader(out);
                    }
                }
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
    }

    fn adopt_term(&mut self, new_term: u32, leader: Option<NodeId>, out: &mut Vec<Action>) {
        self.current_term = new_term;
        self.role = Role::Follower;
        self.serving = false;
        self.new_term_pos = None;
        self.votes_received.clear();
        self.voted_for = None; // new term: no vote cast yet
        out.push(Action::BecomeFollower { term: new_term, leader });
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

    fn sm(id: NodeId) -> ElectionSm {
        ElectionSm::new(cfg(id), None, &[], 0, 0)
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
}
