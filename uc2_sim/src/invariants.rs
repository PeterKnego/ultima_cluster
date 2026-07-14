// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The five safety invariants (spec §8) plus the two carry-over obligations
//! from earlier Task-4 reviews (T2 truncate-bound, `Fatal`-is-unreachable),
//! plus the M7 config invariants inv6–9 (reconfig spec §8):
//!
//! - **inv6 — config determinism:** a node's adopted config equals the config
//!   its durable frontier implies (the world recomputes the implication from
//!   its config-frame ledger; the append frontier is also accepted, covering
//!   the leader's adopt-at-append window).
//! - **inv7 — quorum legality:** a leader's commit must be certified by a
//!   genuine content-quorum of the ADOPTING node's config voters (the M4/M5
//!   "inv5 phantom" oracle, made config-aware), AND ride a config that chains
//!   off the committed config (the chain half also guards elections — a winner
//!   operating under a config that neither matches nor directly succeeds the
//!   committed one is the disjoint-quorum smoking gun the serving gate
//!   prevents).
//! - **inv8 — revert correctness:** after a truncation settles, the adopted
//!   config re-equals the frontier-implied config.
//! - **inv9 — tombstone permanence:** no adopted config re-lists a tombstoned
//!   id (self-consistency and across the adoption edge), and tombstones never
//!   shrink.
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

use uc2_consensus::config::ClusterConfig;
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
    /// M7 (inv7 chain half): the config governing the committed lineage — set
    /// from the committing leader's own view (its adopted config if the commit
    /// covers the config frame's end, else its prev) whenever `global_max_commit`
    /// advances. `None` until the first genuine commit. A later leader commit or
    /// election whose (adopted, prev) pair contains neither this config nor a
    /// direct successor of it is operating on a divergent config history —
    /// exactly the disjoint-quorum world the serving gate forbids.
    committed_config: Option<ClusterConfig>,
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
            committed_config: None,
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
    /// M7 addendum: the check is CONFIG-AWARE (inv7 — quorum legality). The
    /// genuine content-quorum is ranked over the ADOPTING leader's config
    /// VOTERS (`voters`, quorum = |voters|/2 + 1) instead of a static world
    /// majority — a static count would be too strong for a legally shrunk
    /// config (a 3-voter config commits with 2, not with a majority of every
    /// process that ever existed) and too weak for a grown one. With the
    /// genesis all-voter config this is bit-identical to the historical inv5
    /// phantom oracle, which is why the `RawM3`/`Mechanism` pins keep working
    /// unchanged. Additionally the CHAIN half of inv7 rejects a commit riding
    /// a config that neither matches nor directly succeeds the committed
    /// config (see [`InvariantChecker::committed_config`]).
    #[allow(clippy::too_many_arguments)]
    pub fn on_advance_commit(
        &mut self,
        node: NodeId,
        commit: u64,
        term: u32,
        is_leader: bool,
        durables: &[u64],
        maps: &[Vec<(u32, u64)>],
        voters: &[NodeId],
        adopted: &ClusterConfig,
        prev: &ClusterConfig,
        config_position: u64,
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

        // inv7 (chain half) — a commit that ADVANCES ground truth must ride a
        // config chained off the committed config: equal to it, or a direct
        // successor (prev == it — the leader's own still-pending proposal).
        // Anything else means the new committed history is being certified
        // under a config lineage ground truth never committed. Scoped to
        // gmc-ADVANCING commits only: a STALE leader's benign rank-commit (the
        // T1 legal case — content-genuine, never becomes ground truth) may
        // carry a superseded uncommitted config after a newer config committed
        // elsewhere, and must not false-positive; its content is still fully
        // phantom-checked below. Elections check the chain UNCONDITIONALLY
        // (`on_become_leader`): a legal winner always holds the committed
        // config (term-lexicographic vote freshness + inv5 completeness).
        let max_term = self.leaders_by_term.keys().next_back().copied().unwrap_or(0);
        let advancing = term == max_term && max_term > 0 && commit >= self.global_max_commit;
        if advancing
            && let Some(cc) = &self.committed_config
            && adopted != cc
            && prev != cc
        {
            return Err(self.viol(
                "quorum legality (inv7): config chain divergence",
                step,
                format!(
                    "node {node} (leader, term {term}) certified commit {commit} under config \
                     v{} (prev v{}) but the committed config is v{} — neither matches nor \
                     directly succeeds it (disjoint-quorum hazard)",
                    adopted.version, prev.version, cc.version
                ),
            ));
        }

        // inv7 (quorum half) — genuine byte-content quorum over the ADOPTING
        // node's config voters: for each voter, the frontier up to which it
        // durably holds bytes identical to the committing leader's lineage. A
        // voter id with no world process behind it (possible in fuzz-injected
        // configs) holds nothing.
        //
        // MID-SELF-REMOVAL leader (spec §4): a leader that proposed its own
        // removal keeps serving until the entry commits, and its own durable
        // legitimately occupies the "+1" ranking slot even though it is no
        // longer a voter (`CommitTracker::new(n_followers, n_followers + 1)` —
        // the appender always holds everything it appended). This is SAFE: the
        // certified position is genuinely held by ceil(v/2) counted VOTERS,
        // and any C_new election quorum (v/2 + 1 of v) must intersect that
        // subset (pigeonhole: ceil(v/2) + v/2 + 1 > v). The oracle therefore
        // ranks voters ∪ {committing leader}, exactly the tracker's rule; when
        // the leader IS a voter this changes nothing.
        let lineage = &maps[i];
        let mut certifiers: Vec<NodeId> = voters.to_vec();
        if !certifiers.contains(&node) {
            certifiers.push(node);
        }
        let mut frontiers: Vec<u64> = certifiers
            .iter()
            .map(|&v| {
                let m = v as usize;
                if m >= self.n {
                    return 0;
                }
                durables[m].min(first_content_divergence(&maps[m], lineage, u64::MAX))
            })
            .collect();
        frontiers.sort_unstable_by(|a, b| b.cmp(a)); // descending
        let quorum = certifiers.len() / 2 + 1;
        let genuine = frontiers.get(quorum - 1).copied().unwrap_or(0);
        if commit > genuine {
            return Err(self.viol(
                "quorum legality (inv7): phantom commit — no genuine quorum of the adopting config",
                step,
                format!(
                    "node {node} (leader, term {term}) certified commit {commit} but only a \
                     genuine quorum-frontier of {genuine} among config-v{} voters {voters:?} \
                     holds that content (per-voter frontiers {frontiers:?})",
                    adopted.version
                ),
            ));
        }

        // Ground truth advances ONLY for a leader of the highest term ever elected:
        // a stale leader's lineage must never become the committed history (a
        // higher-term leader is elected complete over genuine history — inv5).
        if advancing {
            self.global_max_commit = commit;
            // Refresh the committed lineage to the committing leader's map (it is
            // authoritative and, by invariant 5, complete over the committed
            // prefix). Cloned so later node-map mutations can't alias ground truth.
            self.committed_lineage = lineage.clone();
            // The governing config of the committed lineage (inv7 chain ground
            // truth): the leader's adopted config if this commit covers its
            // config frame's END, else the frame is still uncommitted and prev
            // governs. Cloned for the same aliasing reason as the lineage.
            let gov = if commit >= config_position { adopted } else { prev };
            self.committed_config = Some(gov.clone());
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
    /// opened `(term, base)`). Checks invariants 1, 7 (chain half) and 5.
    ///
    /// The inv7 chain check runs BEFORE inv5's completeness checks and mirrors
    /// the commit-side rule: a legally elected leader holds every committed
    /// byte durable (inv5), hence has adopted every committed config frame, so
    /// its (adopted, prev) pair always contains the committed config. A winner
    /// for which neither matches was elected by a quorum of a config lineage
    /// ground truth never committed — the serving-gate counterfactual's
    /// disjoint-quorum election (Ongaro 2015), caught here at the election
    /// itself rather than at the committed-history destruction it goes on to
    /// cause.
    #[allow(clippy::too_many_arguments)]
    pub fn on_become_leader(
        &mut self,
        node: NodeId,
        term: u32,
        base: u64,
        leader_map: &[(u32, u64)],
        adopted: &ClusterConfig,
        prev: &ClusterConfig,
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

        // Invariant 7 (chain half) — election config legality (see above).
        if let Some(cc) = &self.committed_config
            && adopted != cc
            && prev != cc
        {
            return Err(self.viol(
                "quorum legality (inv7): config chain divergence",
                step,
                format!(
                    "node {node} won term {term} under config v{} (prev v{}) but the committed \
                     config is v{} — neither matches nor directly succeeds it (the election's \
                     quorum came from a config lineage that was never committed)",
                    adopted.version, prev.version, cc.version
                ),
            ));
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

    /// Invariant 6 — config determinism: a node's adopted config must equal the
    /// config its DURABLE frontier implies (the world derives `implied_durable`
    /// from its config-frame ledger: the highest-version frame whose end is
    /// durably held with matching content identity, else the node's position-0
    /// baseline). The APPEND-frontier implication is also accepted — the
    /// adopt-at-append window: a leader adopts its own frame the moment it
    /// appends it (and keeps that adoption if deposed before the frame turns
    /// durable; its own archive closes the window). An adoption backed by
    /// NEITHER frontier — a config whose frame the node's log does not carry —
    /// is the determinism breach.
    pub fn check_config_determinism(
        &self,
        node: NodeId,
        adopted: &ClusterConfig,
        implied_durable: &ClusterConfig,
        implied_append: &ClusterConfig,
        step: u64,
    ) -> Result<(), InvariantViolation> {
        if adopted != implied_durable && adopted != implied_append {
            return Err(self.viol(
                "config determinism (inv6)",
                step,
                format!(
                    "node {node} has adopted config v{} but its durable frontier implies v{} \
                     (append frontier v{}) — the adopted config is not backed by its log",
                    adopted.version, implied_durable.version, implied_append.version
                ),
            ));
        }
        Ok(())
    }

    /// Invariant 8 — revert correctness: once a truncation SETTLES (the
    /// matching-epoch ack landed: durable clamped, map adopted, config
    /// reverted/kept per spec §5), the adopted config must re-equal the
    /// frontier-implied config exactly (durable == append here, so there is no
    /// adopt-at-append window to allow). The same property inv6 sweeps
    /// continuously, pinned at the exact point the revert obligation falls due
    /// — this is the check the `revert_on_truncate_disabled` counterfactual
    /// turns red.
    pub fn check_revert_correctness(
        &self,
        node: NodeId,
        adopted: &ClusterConfig,
        implied: &ClusterConfig,
        step: u64,
    ) -> Result<(), InvariantViolation> {
        if adopted != implied {
            return Err(self.viol(
                "revert correctness (inv8)",
                step,
                format!(
                    "node {node} finished a truncation still adopting config v{} while its \
                     durable frontier implies v{} — the truncated-away config frame survived \
                     (revert-on-truncate failed)",
                    adopted.version, implied.version
                ),
            ));
        }
        Ok(())
    }

    /// Invariant 9 — tombstone permanence, judged on EVERY adoption (forward,
    /// revert, or wipe-fiat): (a) the adopted config must not list a member it
    /// itself tombstones; (b) it must not re-list an id its predecessor
    /// tombstoned; (c) tombstones never shrink across the adoption edge.
    pub fn on_config_adopted(
        &self,
        node: NodeId,
        config: &ClusterConfig,
        prev: &ClusterConfig,
        step: u64,
    ) -> Result<(), InvariantViolation> {
        let relisted = config
            .voters
            .iter()
            .chain(config.learners.iter())
            .map(|(id, _)| *id)
            .find(|id| config.tombstones.contains(id) || prev.tombstones.contains(id));
        if let Some(id) = relisted {
            return Err(self.viol(
                "tombstone permanence (inv9)",
                step,
                format!(
                    "node {node} adopted config v{} re-listing tombstoned id {id} \
                     (config tombstones {:?}, prev tombstones {:?})",
                    config.version, config.tombstones, prev.tombstones
                ),
            ));
        }
        if let Some(id) = prev.tombstones.iter().find(|id| !config.tombstones.contains(id)) {
            return Err(self.viol(
                "tombstone permanence (inv9)",
                step,
                format!(
                    "node {node} adopted config v{} DROPPING tombstone {id} carried by its \
                     predecessor v{} — tombstones are forever",
                    config.version, prev.version
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

    /// A genesis config with the given node ids as voters (synthetic addrs).
    fn gcfg(voters: &[NodeId]) -> ClusterConfig {
        ClusterConfig::genesis(voters.iter().map(|&v| (v, (v, 1))).collect(), Vec::new())
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
        let g = gcfg(&[0, 1, 2]);
        let err = c.on_become_leader(0, 5, 2000, &leader_map, &g, &g, 1).unwrap_err();
        assert!(err.invariant.contains("inv5"), "{err}");
    }

    #[test]
    fn inv5_accepts_complete_leader() {
        let mut c = checker(vec![(1, 0), (2, 960)], 2000);
        let leader_map = vec![(1, 0), (2, 960), (5, 2000)];
        let g = gcfg(&[0, 1, 2]);
        assert!(c.on_become_leader(0, 5, 2000, &leader_map, &g, &g, 1).is_ok());
    }

    #[test]
    fn inv5_catches_election_below_global_commit() {
        let mut c = checker(vec![(1, 0)], 2000);
        // Opening a term at base 1500 < gmc 2000 is a completeness breach.
        let g = gcfg(&[0, 1, 2]);
        assert!(c.on_become_leader(0, 3, 1500, &[(1, 0), (3, 1500)], &g, &g, 1).is_err());
    }

    #[test]
    fn phantom_commit_flagged_for_current_max_term_leader() {
        let mut c = InvariantChecker::new(42, 3);
        // Establish term 2 as the highest elected term (its leader = node 0).
        let g = gcfg(&[0, 1, 2]);
        c.on_become_leader(0, 2, 0, &[(2, 0)], &g, &g, 1).unwrap();
        // Node 0 (leader, term 2) certifies commit 3000, but only itself holds that
        // content (divergent peers) -> genuine quorum = 0 -> phantom caught.
        let durables = vec![3000, 3000, 3000];
        let maps = vec![vec![(2, 0)], vec![(9, 0)], vec![(9, 0)]];
        let err = c
            .on_advance_commit(0, 3000, 2, true, &durables, &maps, &[0, 1, 2], &g, &g, 0, 2)
            .unwrap_err();
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
        let g = gcfg(&[0, 1, 2]);
        let err = c
            .on_advance_commit(0, 3000, 2, true, &durables, &maps, &[0, 1, 2], &g, &g, 0, 5)
            .unwrap_err();
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
        let g = gcfg(&[0, 1, 2]);
        assert!(
            c.on_advance_commit(0, 3000, 2, true, &durables, &maps, &[0, 1, 2], &g, &g, 0, 5)
                .is_ok()
        );
        assert_eq!(c.global_max_commit, 0, "stale leader must not advance gmc");
        assert!(c.committed_lineage.is_empty(), "stale leader must not set the lineage");
    }

    #[test]
    fn raw_gossip_echo_from_follower_is_not_a_phantom() {
        let mut c = InvariantChecker::new(42, 3);
        let g = gcfg(&[0, 1, 2]);
        c.on_become_leader(0, 2, 0, &[(2, 0)], &g, &g, 1).unwrap();
        // A follower (is_leader = false) echoing a gossiped commit it does not hold
        // is the raw counter — benign, never flagged, and does not move gmc.
        let durables = vec![3000, 0, 0];
        let maps = vec![vec![(2, 0)], vec![], vec![]];
        assert!(
            c.on_advance_commit(1, 3000, 2, false, &durables, &maps, &[0, 1, 2], &g, &g, 0, 2)
                .is_ok()
        );
        assert_eq!(c.global_max_commit, 0, "a raw echo must not advance the genuine commit");
    }

    // ---- M7 config invariants (inv6-9), the `inv2_catches_*` pattern ----

    /// A config with `version` and one extra learner per version bump, so
    /// distinct versions are also distinct content.
    fn vcfg(version: u64) -> ClusterConfig {
        let mut c = gcfg(&[0, 1, 2]);
        for v in 0..version {
            c.learners.push((10 + v as NodeId, (10 + v as u32, 1)));
        }
        c.version = version;
        c
    }

    #[test]
    fn inv6_catches_config_not_backed_by_log() {
        // The node adopted v1 but NEITHER its durable nor its append frontier
        // implies it (the frame is gone / never was on its lineage).
        let c = checker(vec![(1, 0)], 0);
        let err = c
            .check_config_determinism(1, &vcfg(1), &vcfg(0), &vcfg(0), 1)
            .unwrap_err();
        assert!(err.invariant.contains("inv6"), "{err}");
    }

    #[test]
    fn inv6_tolerates_the_adopt_at_append_window() {
        // A leader adopts its own config frame at APPEND, before its durable
        // crosses it: implied-by-durable is still v0, implied-by-append is v1.
        let c = checker(vec![(1, 0)], 0);
        assert!(c.check_config_determinism(0, &vcfg(1), &vcfg(0), &vcfg(1), 1).is_ok());
        // And plain agreement at the durable frontier is of course fine.
        assert!(c.check_config_determinism(0, &vcfg(1), &vcfg(1), &vcfg(1), 2).is_ok());
    }

    #[test]
    fn inv7_catches_commit_without_quorum_of_adopting_config() {
        // The voter RESTRICTION is load-bearing: a 5-process world where the
        // adopting config's voters are [0,1,2]. The leader (0) commits content
        // that nodes 3 and 4 genuinely hold — a STATIC world-majority (3 of 5:
        // {0,3,4}) would bless it — but of the config's actual voters only the
        // leader itself holds it, so no config quorum (2 of [0,1,2]) exists.
        let mut c = InvariantChecker::new(42, 5);
        c.leaders_by_term.insert(2, 0);
        let durables = vec![3000, 3000, 3000, 3000, 3000];
        let maps = vec![
            vec![(2, 0)], // leader lineage
            vec![(9, 0)], // voter 1: divergent
            vec![(9, 0)], // voter 2: divergent
            vec![(2, 0)], // NON-voter 3 holds it — must not count
            vec![(2, 0)], // NON-voter 4 holds it — must not count
        ];
        let g = gcfg(&[0, 1, 2]);
        let err = c
            .on_advance_commit(0, 3000, 2, true, &durables, &maps, &[0, 1, 2], &g, &g, 0, 5)
            .unwrap_err();
        assert!(err.invariant.contains("inv7"), "{err}");
        assert!(err.invariant.contains("phantom"), "{err}");
    }

    #[test]
    fn inv7_catches_commit_off_the_committed_config_chain() {
        // The committed config is v2; a leader commits under v1 whose prev is
        // v0 — neither matches nor directly succeeds the committed config: its
        // certifying quorum comes from a config lineage ground truth never
        // committed (the serving-gate counterfactual's smoking gun).
        let mut c = InvariantChecker::new(42, 3);
        c.leaders_by_term.insert(3, 1);
        c.committed_config = Some(vcfg(2));
        let durables = vec![3000, 3000, 3000];
        let maps = vec![vec![(3, 0)], vec![(3, 0)], vec![(3, 0)]];
        let err = c
            .on_advance_commit(1, 3000, 3, true, &durables, &maps, &[0, 1, 2], &vcfg(1), &vcfg(0), 0, 5)
            .unwrap_err();
        assert!(err.invariant.contains("inv7"), "{err}");
        assert!(err.detail.contains("chain") || err.invariant.contains("chain"), "{err}");
    }

    #[test]
    fn inv7_accepts_a_direct_successor_of_the_committed_config() {
        // A leader committing its own still-pending proposal: adopted v3 with
        // prev == the committed v2 — the legal one-in-flight shape.
        let mut c = InvariantChecker::new(42, 3);
        c.leaders_by_term.insert(3, 1);
        c.committed_config = Some(vcfg(2));
        let durables = vec![3000, 3000, 3000];
        let maps = vec![vec![(3, 0)], vec![(3, 0)], vec![(3, 0)]];
        assert!(
            c.on_advance_commit(1, 3000, 3, true, &durables, &maps, &[0, 1, 2], &vcfg(3), &vcfg(2), 100, 5)
                .is_ok()
        );
        // The commit covered the frame end (100 <= 3000): v3 is now committed.
        assert_eq!(c.committed_config.as_ref().map(|g| g.version), Some(3));
    }

    #[test]
    fn inv7_catches_election_off_the_committed_config_chain() {
        // Same chain rule at the ELECTION: a winner whose (adopted, prev) pair
        // contains neither the committed config nor its predecessor was elected
        // by a quorum of a divergent config lineage — red BEFORE it can destroy
        // committed history (which inv5 would only catch afterwards).
        let mut c = InvariantChecker::new(42, 3);
        c.committed_config = Some(vcfg(2));
        let err = c
            .on_become_leader(0, 7, 5000, &[(7, 5000)], &vcfg(1), &vcfg(0), 5)
            .unwrap_err();
        assert!(err.invariant.contains("inv7"), "{err}");
    }

    #[test]
    fn inv8_catches_unreverted_config_after_truncation() {
        // The truncation settled with the node still adopting v1 while its
        // durable frontier implies v0 — revert-on-truncate failed (the
        // counterfactual's bug class).
        let c = checker(vec![(1, 0)], 0);
        let err = c.check_revert_correctness(1, &vcfg(1), &vcfg(0), 1).unwrap_err();
        assert!(err.invariant.contains("inv8"), "{err}");
        // The settled, reverted state passes.
        assert!(c.check_revert_correctness(1, &vcfg(0), &vcfg(0), 2).is_ok());
    }

    #[test]
    fn inv9_catches_tombstoned_id_relisted() {
        let c = checker(vec![(1, 0)], 0);
        // (a) self-inconsistent: the config lists voter 2 AND tombstones it.
        let mut bad = gcfg(&[0, 1, 2]);
        bad.version = 1;
        bad.tombstones.push(2);
        let err = c.on_config_adopted(0, &bad, &gcfg(&[0, 1, 2]), 1).unwrap_err();
        assert!(err.invariant.contains("inv9"), "{err}");
        // (b) across the edge: prev tombstoned 2, the new config re-lists it.
        let mut prev = gcfg(&[0, 1]);
        prev.tombstones.push(2);
        let mut relist = gcfg(&[0, 1, 2]); // 2 is back
        relist.version = 2;
        relist.tombstones.push(2); // even carrying the tombstone forward
        assert!(c.on_config_adopted(0, &relist, &prev, 2).is_err());
    }

    #[test]
    fn inv9_catches_dropped_tombstone() {
        let c = checker(vec![(1, 0)], 0);
        let mut prev = gcfg(&[0, 1]);
        prev.tombstones.push(2);
        let mut next = gcfg(&[0, 1]); // tombstone list silently emptied
        next.version = 2;
        let err = c.on_config_adopted(0, &next, &prev, 1).unwrap_err();
        assert!(err.invariant.contains("inv9"), "{err}");
        // Carrying it forward is fine.
        let mut ok = gcfg(&[0, 1]);
        ok.version = 2;
        ok.tombstones.push(2);
        assert!(c.on_config_adopted(0, &ok, &prev, 2).is_ok());
    }
}
