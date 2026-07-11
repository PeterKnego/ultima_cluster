// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The five safety invariants (spec §8) plus the two carry-over obligations
//! from earlier Task-4 reviews (T2 truncate-bound, `Fatal`-is-unreachable).
//!
//! The checker owns the sim's *ground truth* — the values a real cluster would
//! only know from an oracle: which node opened which term, the global commit
//! high-water, and each node's own committed high-water (durable across
//! restart). Node-local state lives in [`crate::world`]; this module never
//! mutates a node, it only observes and judges.
//!
//! ## Invariant 4 note (per-node, not global)
//!
//! Spec §8's "committed-never-truncated" is written as `to >= global_max_commit`.
//! That literal *global* form is contradicted by the brief's own
//! `minority_partition_cannot_commit_and_heals` scenario: a deposed leader that
//! healed truncates its divergent tail down to the new leader's term base — a
//! position that is `>=` its own committed high-water but can be strictly `<`
//! the *global* commit, because the new-leader majority has committed fresh
//! bytes above that base in the meantime. Truncating there is correct and safe
//! (it discards bytes this node never committed, then re-replicates the real
//! committed history). The sound property — the one that actually protects
//! durability — is that a node never truncates below what **it itself** ever
//! committed. Global commit-safety at election is enforced separately and
//! literally by invariant 5 (`base >= global_max_commit`). We therefore check
//! invariant 4 per-node and invariant 5 globally; together they are exactly
//! Raft's State-Machine-Safety.

use std::collections::BTreeMap;
use std::fmt;

use uc2_consensus::election::NodeId;

/// A safety-invariant breach, carrying the `(seed, step)` needed to pin it as a
/// named regression (`Display`s as a single self-contained line).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvariantViolation {
    /// The invariant that broke (human name, e.g. `"election safety (inv1)"`).
    pub invariant: &'static str,
    /// The step number (1-based event count) at which it was detected.
    pub step: u64,
    /// The run seed (so a failing fuzz seed reproduces deterministically).
    pub seed: u64,
    /// Free-form detail: the concrete conflicting values.
    pub detail: String,
}

impl fmt::Display for InvariantViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "INVARIANT VIOLATION [{}] at step {} (seed {}): {}",
            self.invariant, self.step, self.seed, self.detail
        )
    }
}

/// Ground-truth tracker + the invariant checks. One per `World`.
pub struct InvariantChecker {
    seed: u64,
    n: usize,
    /// Max commit any node has ever certified — the global commit high-water.
    pub global_max_commit: u64,
    /// term -> the node that opened it as leader (invariant 1: at most one).
    leaders_by_term: BTreeMap<u32, NodeId>,
    /// Per-node max commit EVER certified (durable across restart) — the bound
    /// for invariant 4 (a node never truncates below its own committed bytes).
    pub committed_hw: Vec<u64>,
    /// Per-node last commit within the current run (reset on restart, which is
    /// an exempt-and-expected commit regression) — invariant 3 monotonicity.
    last_commit: Vec<u64>,
}

impl InvariantChecker {
    pub fn new(seed: u64, n: usize) -> Self {
        Self {
            seed,
            n,
            global_max_commit: 0,
            leaders_by_term: BTreeMap::new(),
            committed_hw: vec![0; n],
            last_commit: vec![0; n],
        }
    }

    fn viol(&self, invariant: &'static str, step: u64, detail: String) -> InvariantViolation {
        InvariantViolation { invariant, step, seed: self.seed, detail }
    }

    /// Restart re-borns the SM: its run-local commit resets to 0, so invariant-3
    /// monotonicity restarts from 0 (the committed high-water is untouched — it
    /// is durable ground truth).
    pub fn on_restart(&mut self, node: usize) {
        self.last_commit[node] = 0;
    }

    /// A node advanced its commit counter to `commit` (`Action::AdvanceCommit`).
    /// Invariant 3: the raw counter is monotone per node per run. The *global*
    /// commit high-water tracks the max raw commit — sound because only a
    /// leader's `AdvanceCommit` originates a value (quorum-certified over
    /// `matched`-gated reports), and followers merely echo it via gossip.
    pub fn on_advance_commit(
        &mut self,
        node: NodeId,
        commit: u64,
        step: u64,
    ) -> Result<(), InvariantViolation> {
        let i = node as usize;
        if commit < self.last_commit[i] {
            return Err(self.viol(
                "commit monotonicity (inv3)",
                step,
                format!("node {node} commit regressed {} -> {commit}", self.last_commit[i]),
            ));
        }
        self.last_commit[i] = commit;
        self.global_max_commit = self.global_max_commit.max(commit);
        Ok(())
    }

    /// Raise node `i`'s committed high-water to `held` — the frontier that is
    /// simultaneously committed, byte-consistent with the leader, and fsync'd
    /// (`min(commit, matched, durable)`; a leader's own log is authoritative, so
    /// `min(commit, durable)`). The SM's commit counter is raw (unclamped by
    /// local durability — clamping is an apply-time / M5 concern), so it would
    /// *overestimate* what a lagging follower actually holds; this `held`
    /// measure is the sound bound for invariant 4. Monotone, durable across
    /// restart (a `matched` reset never lowers a past high-water).
    pub fn record_held(&mut self, node: usize, held: u64) {
        self.committed_hw[node] = self.committed_hw[node].max(held);
    }

    /// A node opened `term` as leader (translated `Action::BecomeLeader`).
    /// `maps[i]` is node `i`'s current term map (the leader's already includes
    /// the freshly-opened `(term, base)`). Checks invariants 1 and 5.
    pub fn on_become_leader(
        &mut self,
        node: NodeId,
        term: u32,
        base: u64,
        maps: &[Vec<(u32, u64)>],
        step: u64,
    ) -> Result<(), InvariantViolation> {
        // Invariant 1 — election safety: at most one leader per term.
        match self.leaders_by_term.get(&term) {
            Some(&other) if other != node => {
                return Err(self.viol(
                    "election safety (inv1)",
                    step,
                    format!("two leaders in term {term}: node {other} and node {node}"),
                ));
            }
            _ => {
                self.leaders_by_term.insert(term, node);
            }
        }

        // Invariant 5 — leader completeness (global commit-safety at election):
        //   (a) the new term opens at or above the global commit high-water, and
        //   (b) the leader's map matches every other node's over committed
        //       positions (it carries the whole committed history).
        if base < self.global_max_commit {
            return Err(self.viol(
                "leader completeness (inv5)",
                step,
                format!(
                    "node {node} opened term {term} at base {base} < global_max_commit {}",
                    self.global_max_commit
                ),
            ));
        }
        let gmc = self.global_max_commit;
        let lead: Vec<_> = maps[node as usize].iter().filter(|(_, b)| *b < gmc).collect();
        for (other, m) in maps.iter().enumerate() {
            if other == node as usize {
                continue;
            }
            let om: Vec<_> = m.iter().filter(|(_, b)| *b < gmc).collect();
            for (k, (a, b)) in lead.iter().zip(om.iter()).enumerate() {
                if a != b {
                    return Err(self.viol(
                        "leader completeness (inv5)",
                        step,
                        format!(
                            "new leader {node} (term {term}) map disagrees with node {other} at \
                             committed slot {k}: {a:?} vs {b:?} (gmc={gmc})"
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    /// A node is about to truncate to `to` (translated `Action::Truncate`).
    /// `own_before` is the node's map before reconciliation; `leader` is the
    /// map the leader shipped. Checks invariant 4 (per-node, see module note)
    /// and the T2 carry-over (`to` is 0 or a real term-map base).
    pub fn on_truncate(
        &self,
        node: NodeId,
        to: u64,
        own_before: &[(u32, u64)],
        leader: &[(u32, u64)],
        step: u64,
    ) -> Result<(), InvariantViolation> {
        let i = node as usize;
        // Invariant 4 — committed-never-truncated (per node): never below what
        // this node itself has certified as committed.
        if to < self.committed_hw[i] {
            return Err(self.viol(
                "committed-never-truncated (inv4)",
                step,
                format!(
                    "node {node} truncate to {to} < its committed high-water {}",
                    self.committed_hw[i]
                ),
            ));
        }
        // T2 carry — the truncation point is 0 or a term-map base (which the
        // real archive maps to an archived-block start; it never lands strictly
        // inside a block's span). In the model, blocks don't exist, so the
        // equivalent assertion is that `to` is a base present in our own map or
        // the leader's shipped map, or 0.
        if to != 0
            && !own_before.iter().any(|(_, b)| *b == to)
            && !leader.iter().any(|(_, b)| *b == to)
        {
            return Err(self.viol(
                "truncate-bound (T2 carry)",
                step,
                format!(
                    "node {node} truncate to {to} is neither 0 nor a term-map base \
                     (own={own_before:?}, leader={leader:?})"
                ),
            ));
        }
        Ok(())
    }

    /// Invariant 2 — term-map prefix consistency over committed positions: for
    /// any two nodes, their maps restricted to entries below the global commit
    /// high-water must be identical prefixes. Run after every event.
    pub fn check_prefix_consistency(
        &self,
        maps: &[Vec<(u32, u64)>],
        step: u64,
    ) -> Result<(), InvariantViolation> {
        debug_assert_eq!(maps.len(), self.n);
        let gmc = self.global_max_commit;
        for a in 0..maps.len() {
            let fa: Vec<_> = maps[a].iter().filter(|(_, b)| *b < gmc).collect();
            for (bnode, mb) in maps.iter().enumerate().skip(a + 1) {
                let fb: Vec<_> = mb.iter().filter(|(_, b)| *b < gmc).collect();
                for (k, (ea, eb)) in fa.iter().zip(fb.iter()).enumerate() {
                    if ea != eb {
                        return Err(self.viol(
                            "term-map prefix consistency (inv2)",
                            step,
                            format!(
                                "nodes {a} and {bnode} diverge at committed slot {k}: \
                                 {ea:?} vs {eb:?} (gmc={gmc})"
                            ),
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}
