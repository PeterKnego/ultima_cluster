// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The five safety invariants (spec §8) plus the two carry-over obligations
//! from earlier Task-4 reviews (T2 truncate-bound, `Fatal`-is-unreachable).
//!
//! The checker owns the sim's *ground truth* — the values a real cluster would
//! only know from an oracle: which node opened which term, the **genuine**
//! (byte-content-quorum) commit high-water, the committed content lineage, and
//! each node's own committed high-water (durable across restart). Node-local
//! state lives in [`crate::world`]; this module never mutates a node, it only
//! observes and judges.
//!
//! ## Oracle independence (the F3 contract)
//!
//! The commit oracle is derived from **genuine byte-content quorum ground
//! truth**, never from the model's `matched` frontier. On every `AdvanceCommit`
//! the checker recomputes, for each member, the position up to which it durably
//! holds bytes byte-identical to the committing leader's content lineage
//! (`term_at` equality is content identity — spec §6), and rejects any commit
//! that outruns the quorum-ranked frontier. Because this reads only real
//! `(durable, term_map)` state, the oracle's *strength is the same whether the
//! data plane runs `Gated` or `RawM3`* (see [`crate::world::DataPlane`]). That
//! is what lets the `RawM3` regression tests prove the oracle would have caught
//! the shipped M3 receiver's phantom-commit bug.
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
//! literally by invariant 5 (`base >= global_max_commit` + leader completeness
//! against the committed lineage). We therefore check invariant 4 per-node and
//! invariant 5 globally; together they are exactly Raft's State-Machine-Safety.

use std::fmt;

use uc2_consensus::election::NodeId;

/// The term covering byte `pos` in an ascending `(term, base)` map — the term of
/// the greatest entry whose base is `<= pos`, or 0 below the first entry. Within
/// a term the bytes are identical cluster-wide (spec §6), so the term at a
/// position IS its content identity: `term_at(a, p) == term_at(b, p)` ⇔ nodes
/// `a` and `b` hold byte-identical content at `p`.
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

/// The lowest position `< bound` at which maps `a` and `b` describe **different
/// content**, or `bound` if they agree over `[0, bound)`. Sampling at the union
/// of both maps' bases is exact: `term_at` is piecewise-constant and only steps
/// at a base, so agreement at every union base below `bound` implies agreement
/// at every position below it (a missing entry in one map that the other holds
/// IS a content difference — this is the fix for the old zip-truncation, which
/// compared only the common entry prefix and silently tolerated a node that was
/// *missing* a committed entry another held).
fn first_content_divergence(a: &[(u32, u64)], b: &[(u32, u64)], bound: u64) -> u64 {
    // A small stack-friendly union of bases; the maps are tiny (<= wire cap).
    let mut bases: Vec<u64> = a
        .iter()
        .chain(b.iter())
        .map(|&(_, base)| base)
        .filter(|&x| x < bound)
        .collect();
    bases.push(0);
    // Deterministic: sort + dedup, never iterate a hash container.
    bases.sort_unstable();
    bases.dedup();
    for &p in &bases {
        if term_at(a, p) != term_at(b, p) {
            return p;
        }
    }
    bound
}

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
    /// The genuine commit high-water: the greatest position any leader has
    /// committed with real byte-content quorum support (see the F3 note). Every
    /// other invariant keys off this — a phantom commit never advances it,
    /// because the phantom is caught (and the run aborted) at the offending
    /// `AdvanceCommit` before `global_max_commit` moves.
    pub global_max_commit: u64,
    /// The authoritative committed content lineage: the committing leader's term
    /// map. Its entries with `base < global_max_commit` ARE the committed history
    /// (a leader that commits is complete over that prefix — invariant 5). Every
    /// node's committed content is judged against this ground truth, not against
    /// another (possibly divergent) node's map.
    committed_lineage: Vec<(u32, u64)>,
    /// term -> the node that opened it as leader (invariant 1: at most one).
    leaders_by_term: std::collections::BTreeMap<u32, NodeId>,
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
            committed_lineage: Vec::new(),
            leaders_by_term: std::collections::BTreeMap::new(),
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
    ///
    /// Checks invariant 3 (per-node monotonicity) always, and the genuine-quorum
    /// commit oracle (F3) for **every leader's** rank-based commit — the actual "I
    /// certify these bytes are quorum-fsync'd" assertion. (T1) This includes a
    /// STALE leader (still `Role::Leader` of a superseded term while a higher term
    /// has opened elsewhere): a delayed report can reach it and its rank-commit can
    /// cover divergent bytes no genuine quorum holds — unchecked, and
    /// applied-then-truncated in M5, that is SMR divergence — so a leader commit is
    /// phantom-checked regardless of whether its term is the current max.
    ///
    /// `AdvanceCommit` is otherwise a RAW counter (spec §6 / inv3 note): a
    /// follower echoes a leader's gossiped commit without holding the bytes. That
    /// raw echo is benign — a higher-term leader is (or will be) elected complete
    /// over the genuine committed history (invariant 5), and M5 clamps apply to
    /// held-durable — so it must NOT be judged against a current-state quorum (that
    /// would false-positive on every legitimate gossip echo), and returns early.
    /// For a leader commit, a quorum must genuinely hold bytes identical to its
    /// lineage (`durables[m]`/`maps[m]` are every member's real state; `maps[node]`
    /// is the committing leader's map). Because this reads only real
    /// `(durable, term_map)` state, its strength is identical under `Gated` and
    /// `RawM3` — the phantom-commit guard the `RawM3` tests exercise. The reviewer
    /// worked the safety argument that the stale-leader check does not
    /// false-positive under `Gated`: a follower there clamps its report to
    /// `matched`, so a stale leader can only rank commits a genuine quorum truly
    /// holds (report/vote quorum intersection forces the newer leader's lineage to
    /// cover any stale-quorum-committed position).
    ///
    /// The global commit high-water and committed lineage advance ONLY for a leader
    /// of the highest term ever elected: a stale leader's lineage must never become
    /// ground truth (it may certify over history a newer term already superseded).
    #[allow(clippy::too_many_arguments)]
    pub fn on_advance_commit(
        &mut self,
        node: NodeId,
        commit: u64,
        term: u32,
        is_leader: bool,
        durables: &[u64],
        maps: &[Vec<(u32, u64)>],
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

        // A raw FOLLOWER echo of gossiped commit is benign — it holds no bytes and
        // asserts no certification, so it must NOT be judged against a current-state
        // quorum. Only a LEADER's rank-commit is a genuine certification, and (T1)
        // it is phantom-checked even when the leader is STALE (its term is below the
        // current max), because a delayed report can make a stale leader rank a
        // commit over divergent bytes no genuine quorum holds.
        if !is_leader {
            return Ok(()); // raw gossip echo: benign, gmc untouched
        }

        // Genuine byte-content quorum: for each member, the frontier up to which
        // it durably holds bytes identical to the committing leader's lineage.
        let lineage = &maps[i];
        let mut frontiers: Vec<u64> = (0..self.n)
            .map(|m| durables[m].min(first_content_divergence(&maps[m], lineage, u64::MAX)))
            .collect();
        frontiers.sort_unstable_by(|a, b| b.cmp(a)); // descending
        let quorum = self.n / 2 + 1;
        let genuine = frontiers[quorum - 1];
        if commit > genuine {
            return Err(self.viol(
                "leader completeness (inv5): phantom commit — no genuine quorum",
                step,
                format!(
                    "node {node} (leader, term {term}) certified commit {commit} but only a \
                     genuine quorum-frontier of {genuine} holds that content (per-member \
                     frontiers {frontiers:?})"
                ),
            ));
        }

        // Ground truth advances ONLY for a leader of the highest term ever elected:
        // a stale leader's lineage must never become the committed history (a
        // higher-term leader is elected complete over genuine history — inv5).
        let max_term = self.leaders_by_term.keys().next_back().copied().unwrap_or(0);
        if term == max_term && max_term > 0 && commit >= self.global_max_commit {
            self.global_max_commit = commit;
            // Refresh the committed lineage to the committing leader's map (it is
            // authoritative and, by invariant 5, complete over the committed
            // prefix). Cloned so later node-map mutations can't alias ground truth.
            self.committed_lineage = lineage.clone();
        }
        Ok(())
    }

    /// Raise node `i`'s committed high-water using **genuine content ground
    /// truth**: the frontier up to which it durably holds bytes identical to the
    /// committed lineage, capped at the genuine global commit. Independent of the
    /// model's `matched` (F3), so `Gated`/`RawM3` see the same bound. Monotone and
    /// durable across restart (a `matched` reset never lowers a past high-water).
    pub fn record_held_content(&mut self, node: usize, durable: u64, map: &[(u32, u64)]) {
        let agree = first_content_divergence(map, &self.committed_lineage, u64::MAX);
        let held = durable.min(self.global_max_commit).min(agree);
        self.committed_hw[node] = self.committed_hw[node].max(held);
    }

    /// A node opened `term` as leader (translated `Action::BecomeLeader`).
    /// `leader_map` is the new leader's term map (already including the freshly
    /// opened `(term, base)`). Checks invariants 1 and 5.
    pub fn on_become_leader(
        &mut self,
        node: NodeId,
        term: u32,
        base: u64,
        leader_map: &[(u32, u64)],
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
        //   (b) the leader HOLDS the entire committed lineage — every committed
        //       entry, judged against ground truth (not another node's map, so a
        //       divergent peer can't excuse a gap). A leader *missing* a committed
        //       entry (content merged / truncated away) is caught here — the fix
        //       for the old zip-truncation that tolerated a shorter leader map.
        let gmc = self.global_max_commit;
        if base < gmc {
            return Err(self.viol(
                "leader completeness (inv5)",
                step,
                format!("node {node} opened term {term} at base {base} < global_max_commit {gmc}"),
            ));
        }
        for &(lt, lb) in self.committed_lineage.iter() {
            if lb >= gmc {
                break; // lineage sorted by base; beyond the committed prefix
            }
            if term_at(leader_map, lb) != lt {
                return Err(self.viol(
                    "leader completeness (inv5)",
                    step,
                    format!(
                        "new leader {node} (term {term}) is missing/conflicts committed lineage \
                         entry ({lt},{lb}): leader has term {} there (gmc={gmc})",
                        term_at(leader_map, lb)
                    ),
                ));
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

    /// Invariant 2 — term-map prefix consistency over committed positions. The
    /// list of term boundaries a node records BELOW the global commit high-water
    /// must equal a LEADING SLICE (prefix) of the ground-truth lineage's committed
    /// boundaries — same terms AND bases, in order, with no gap.
    ///
    /// (T2) A pure membership test — "every below-gmc boundary is *some* committed
    /// lineage boundary" — is too weak: a node map `[(1,0),(3,2000)]` against a
    /// lineage `[(1,0),(2,960),(3,2000)]` (gmc 3000) passes membership (both node
    /// boundaries are lineage members) yet is MISSING the middle `(2,960)` — a
    /// divergent committed prefix. The leading-slice comparison rejects it, because
    /// the node's list is not a prefix of the lineage's.
    ///
    /// This still tolerates a lagging node (it records FEWER boundaries → a shorter
    /// prefix) and a clean divergent durable tail (a follower's
    /// uncommitted-from-its-view stale bytes add no boundary below gmc — its
    /// below-gmc list stays a valid prefix and is truncated later). But it catches
    /// map CORRUPTION: a boundary at the wrong base (the F1b `RawM3`
    /// wrong-base-stamp bug), at the wrong term, or a missing-middle boundary. This
    /// prefix form is the fix for the earlier membership check (which itself
    /// replaced the pre-review zip that stopped at the shorter map's length and
    /// compared terms position-by-position).
    ///
    /// Judging each node against the single lineage as a prefix is
    /// equivalent-or-stronger than the spec's pairwise form: two nodes that are
    /// each a prefix of ground truth are prefixes of each other, and a node that
    /// deviates from the ground-truth prefix is caught even if some peer deviated
    /// identically. Run after every event.
    pub fn check_prefix_consistency(
        &self,
        maps: &[Vec<(u32, u64)>],
        step: u64,
    ) -> Result<(), InvariantViolation> {
        debug_assert_eq!(maps.len(), self.n);
        let gmc = self.global_max_commit;
        // The ground-truth committed prefix: lineage boundaries below gmc (the
        // lineage is sorted by base, so `take_while` yields the committed prefix).
        let lineage_prefix: Vec<(u32, u64)> =
            self.committed_lineage.iter().copied().take_while(|&(_, b)| b < gmc).collect();
        for (node, m) in maps.iter().enumerate() {
            // This node's recorded boundaries below the committed high-water.
            let node_prefix: Vec<(u32, u64)> =
                m.iter().copied().take_while(|&(_, b)| b < gmc).collect();
            // Must be a LEADING SLICE of the lineage's committed boundaries.
            let is_prefix = node_prefix.len() <= lineage_prefix.len()
                && node_prefix.iter().zip(lineage_prefix.iter()).all(|(a, b)| a == b);
            if !is_prefix {
                return Err(self.viol(
                    "term-map prefix consistency (inv2)",
                    step,
                    format!(
                        "node {node} committed-position boundaries {node_prefix:?} are not a \
                         leading slice of the committed lineage prefix {lineage_prefix:?} \
                         (gmc={gmc})"
                    ),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checker(lineage: Vec<(u32, u64)>, gmc: u64) -> InvariantChecker {
        let mut c = InvariantChecker::new(42, 3);
        c.committed_lineage = lineage;
        c.global_max_commit = gmc;
        c
    }

    #[test]
    fn inv2_catches_misplaced_boundary_same_term() {
        // Lineage: term 2 starts at 960. A node records term 2 at 3456 — the right
        // term at the wrong base (the F4 case the old zip / term-only compare
        // silently tolerated).
        let c = checker(vec![(1, 0), (2, 960)], 4032);
        let maps = vec![
            vec![(1, 0), (2, 960)],
            vec![(1, 0), (2, 3456)], // misplaced boundary, SAME term
            vec![(1, 0), (2, 960)],
        ];
        let err = c.check_prefix_consistency(&maps, 1).unwrap_err();
        assert!(err.invariant.contains("inv2"), "{err}");
    }

    #[test]
    fn inv2_catches_wrong_term_at_committed_position() {
        let c = checker(vec![(1, 0), (2, 960)], 4032);
        let maps = vec![
            vec![(1, 0), (2, 960)],
            vec![(1, 0), (3, 960)], // wrong term at the committed boundary
            vec![(1, 0), (2, 960)],
        ];
        assert!(c.check_prefix_consistency(&maps, 1).is_err());
    }

    #[test]
    fn inv2_catches_missing_middle_boundary() {
        // (T2) The reviewer's counterexample: node map [(1,0),(3,2000)] against
        // lineage [(1,0),(2,960),(3,2000)] with gmc 3000. BOTH node boundaries are
        // members of the lineage, so a membership test passes it — but the node is
        // missing the middle (2,960) committed boundary, so its below-gmc list is
        // NOT a leading slice of the lineage's. The prefix comparison catches it.
        let c = checker(vec![(1, 0), (2, 960), (3, 2000)], 3000);
        let maps = vec![
            vec![(1, 0), (2, 960), (3, 2000)],
            vec![(1, 0), (3, 2000)], // missing-middle: passes membership, fails prefix
            vec![(1, 0), (2, 960), (3, 2000)],
        ];
        let err = c.check_prefix_consistency(&maps, 1).unwrap_err();
        assert!(err.invariant.contains("inv2"), "{err}");
    }

    #[test]
    fn inv2_tolerates_lagging_prefix() {
        // A node that recorded FEWER committed boundaries (it is behind) is a valid
        // prefix — not a violation.
        let c = checker(vec![(1, 0), (2, 960), (3, 2000)], 3000);
        let maps = vec![
            vec![(1, 0), (2, 960), (3, 2000)],
            vec![(1, 0), (2, 960)], // lagging
            vec![(1, 0)],           // very behind
        ];
        assert!(c.check_prefix_consistency(&maps, 1).is_ok());
    }

    #[test]
    fn inv5_catches_leader_missing_committed_entry() {
        // The committed lineage holds (2,960); a would-be leader whose map lacks it
        // (all term 1 below its base) is incomplete. The old zip-truncation, seeing
        // the leader's shorter committed prefix, tolerated this.
        let mut c = checker(vec![(1, 0), (2, 960)], 2000);
        let leader_map = vec![(1, 0), (5, 2000)];
        let err = c.on_become_leader(0, 5, 2000, &leader_map, 1).unwrap_err();
        assert!(err.invariant.contains("inv5"), "{err}");
    }

    #[test]
    fn inv5_accepts_complete_leader() {
        let mut c = checker(vec![(1, 0), (2, 960)], 2000);
        let leader_map = vec![(1, 0), (2, 960), (5, 2000)];
        assert!(c.on_become_leader(0, 5, 2000, &leader_map, 1).is_ok());
    }

    #[test]
    fn inv5_catches_election_below_global_commit() {
        let mut c = checker(vec![(1, 0)], 2000);
        // Opening a term at base 1500 < gmc 2000 is a completeness breach.
        assert!(c.on_become_leader(0, 3, 1500, &[(1, 0), (3, 1500)], 1).is_err());
    }

    #[test]
    fn phantom_commit_flagged_for_current_max_term_leader() {
        let mut c = InvariantChecker::new(42, 3);
        // Establish term 2 as the highest elected term (its leader = node 0).
        c.on_become_leader(0, 2, 0, &[(2, 0)], 1).unwrap();
        // Node 0 (leader, term 2) certifies commit 3000, but only itself holds that
        // content (divergent peers) -> genuine quorum = 0 -> phantom caught.
        let durables = vec![3000, 3000, 3000];
        let maps = vec![vec![(2, 0)], vec![(9, 0)], vec![(9, 0)]];
        let err = c.on_advance_commit(0, 3000, 2, true, &durables, &maps, 2).unwrap_err();
        assert!(err.invariant.contains("phantom"), "{err}");
    }

    #[test]
    fn stale_leader_phantom_commit_is_caught() {
        // (T1) The blind spot: a delayed report reaches a STALE leader (node 0,
        // still Role::Leader of the superseded term 2) after term 3 opened
        // elsewhere. Its rank-commit covers bytes only it holds. Pre-T1 this was
        // skipped (term 2 != max_term 3); now every leader commit is phantom-checked.
        let mut c = InvariantChecker::new(42, 3);
        c.leaders_by_term.insert(2, 0);
        c.leaders_by_term.insert(3, 2); // term 3 (node 2) is the current max term
        let durables = vec![3000, 3000, 3000];
        let maps = vec![vec![(2, 0)], vec![(9, 0)], vec![(9, 0)]];
        let err = c.on_advance_commit(0, 3000, 2, true, &durables, &maps, 5).unwrap_err();
        assert!(err.invariant.contains("phantom"), "{err}");
        assert_eq!(c.global_max_commit, 0, "a stale leader's phantom must not advance gmc");
    }

    #[test]
    fn stale_leader_genuine_commit_passes_but_does_not_advance_ground_truth() {
        // (T1, the gating half) A stale term-2 leader whose commit a genuine quorum
        // DOES hold is not a false positive — the check passes — but ground truth
        // must NOT move: a stale leader's lineage never becomes committed history.
        let mut c = InvariantChecker::new(42, 3);
        c.leaders_by_term.insert(2, 0);
        c.leaders_by_term.insert(3, 2);
        let durables = vec![3000, 3000, 0];
        let maps = vec![vec![(2, 0)], vec![(2, 0)], vec![]];
        assert!(c.on_advance_commit(0, 3000, 2, true, &durables, &maps, 5).is_ok());
        assert_eq!(c.global_max_commit, 0, "stale leader must not advance gmc");
        assert!(c.committed_lineage.is_empty(), "stale leader must not set the lineage");
    }

    #[test]
    fn raw_gossip_echo_from_follower_is_not_a_phantom() {
        let mut c = InvariantChecker::new(42, 3);
        c.on_become_leader(0, 2, 0, &[(2, 0)], 1).unwrap();
        // A follower (is_leader = false) echoing a gossiped commit it does not hold
        // is the raw counter — benign, never flagged, and does not move gmc.
        let durables = vec![3000, 0, 0];
        let maps = vec![vec![(2, 0)], vec![], vec![]];
        assert!(c.on_advance_commit(1, 3000, 2, false, &durables, &maps, 2).is_ok());
        assert_eq!(c.global_max_commit, 0, "a raw echo must not advance the genuine commit");
    }
}
