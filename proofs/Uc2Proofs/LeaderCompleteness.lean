import Uc2Proofs.ProtocolCommit

/-! LB2 — LEADER COMPLETENESS: findings #6a/#6b FIXED; the proof is the
re-run's deliverable.

The original FIXED LC-core contract (LB2 brief, decision 5) —

```
theorem leader_completeness {n : Nat} (w : World n) (hw : Reachable w)
    (p t v : Nat) (hc : (p, t, v) ∈ w.committed)
    (i : Fin n) (hi : (w.nodes i).pn.role = .leader)
    (ht : t < (w.nodes i).pn.currentTerm) :
    (w.nodes i).hist p = some (t, v)
```

— was refuted twice over by machine-checked countermodels that lived in
this file (commit `14cdcfc`; full traces and Rust evidence in
`.superpowers/sdd/task-LB2-report.md` and the lean gate doc):

- **Finding #6a (statement gap)**: `finding_stamp_keyed_lc_stale_leader`
  (23-step kernel trace, n = 3) + `lc_core_stamp_keyed_is_false` — the
  ghost recorded the data STAMP while Raft's Leader Completeness (§5.4.3)
  keys on the COMMIT term; a stamp-`t` entry committed at `T > t` owes
  nothing to an honest intermediate-term leader `u ∈ (t, T)`. FIXED by
  re-keying the ghost: `committed` now carries
  `(position, stamp, commitTerm, payload)` with `commitTerm` = the
  committing leader's `currentTerm` at the advance
  (`Uc2Proofs/ProtocolCommit.lean`, module doc item 1). Both theorems
  DELETED with the re-key (they reference the old stamp-keyed shape and
  must no longer build).

- **Finding #6b (PROTOCOL gap, Raft §5.4.2 / Figure 8 — a REAL v2.x
  acked-write-loss bug)**: `finding_fig8_old_term_commit_data_loss`
  (46-step kernel trace, n = 5) — an old-term-only range committed at the
  election base before the NewTerm frame was quorum-durable, then a
  divergent higher-lastTerm rival won the next term with a commit-quorum
  member's grant and truncated the committed byte cluster-wide. FIXED in
  Rust (`election.rs::rank_leader` — advance/store/gossip only once
  `ranked ≥ new_term_pos`; sim pin
  `old_term_range_must_not_commit_before_new_term_quorum`, unit pin
  `commit_clamped_to_new_term_base_never_certifies_old_term_only_range`)
  and mirrored as `leaderAdvanceCommit`'s `hbase` enabling
  (`Uc2Proofs/ProtocolCommit.lean`, module doc item 9). The trace's
  commit step is no longer enabled (`k = 1` does not cross the term-3
  base 1), so the theorem became unprovable and was DELETED in the same
  commit as the fix.

**LB2's re-keyed target statement** (the `≤` form — the T-leader itself is
complete over what it commits; proof architecture in the LB2 task report):

```
theorem leader_completeness {n : Nat} (w : World n) (hw : Reachable w)
    (p t T v : Nat) (hc : (p, t, T, v) ∈ w.committed)
    (i : Fin n) (hi : (w.nodes i).pn.role = .leader)
    (ht : T ≤ (w.nodes i).pn.currentTerm) :
    (w.nodes i).hist p = some (t, v)
```

Non-vacuity for the re-keyed hypotheses is
`nonvacuity_commit_completeness_trace` (`ProtocolCommit.lean`): a real
clamped commit `(0, 1, 1, 42)` followed by a term-2 election whose winner
holds the entry. -/

namespace Uc2.Cert

-- (No theorems yet: the leader-completeness proof against the amended
-- model is the LB2 re-run's deliverable.)

end Uc2.Cert
