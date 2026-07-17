import Uc2Proofs.ProtocolCommit

/-! LB2 — LEADER COMPLETENESS: **BLOCKED, two machine-checked findings.**

The FIXED LC-core contract (LB2 brief, decision 5) was:

```
theorem leader_completeness {n : Nat} (w : World n) (hw : Reachable w)
    (p t v : Nat) (hc : (p, t, v) ∈ w.committed)
    (i : Fin n) (hi : (w.nodes i).pn.role = .leader)
    (ht : t < (w.nodes i).pn.currentTerm) :
    (w.nodes i).hist p = some (t, v)
```

Both theorems below are reachable countermodels to it (post-Finding-#5-fix
semantics), found during the invariant design before any preservation proof
was attempted. They are DIFFERENT findings:

**Finding #6a — statement gap (stamp vs commit term), benign for the
protocol.** The ghost records the entry's data STAMP (`ghostEntries` reads
the leader's `hist`), but the property Raft actually provides keys on the
term the entry was COMMITTED in (Raft §5.4.3: "present in the logs of the
leaders for all higher-numbered terms" — higher than the COMMIT term). A
stamp-`t` entry can be committed at a term `T > t` (a new leader commits an
inherited old-stamp prefix — Raft Fig. 8's very subject), and a legitimately
elected leader at an intermediate term `u`, `t < u < T`, elected on voters
that had not yet received the entry, owes nothing about it. It can even
still be sitting there, undisturbed and undeposed, AFTER the commit —
`finding_stamp_keyed_lc_stale_leader` drives exactly that (23 steps, n = 3),
and `lc_core_stamp_keyed_is_false` packages it as a refutation of the ∀-form
at n = 3. No Rust change is indicated by #6a alone: UC never claims a
stamp-keyed guarantee (the cnc commit counter is a bare position). The fix
is a model/statement amendment — the ghost must also record the committing
leader's `currentTerm` and LC must key on it — which is controller
territory (the ledger shape and the statement are both FIXED artifacts).
NOTE: this trace's commit range [0,2) includes a current-term byte, so #6a
survives the #6b fix below — the two findings are independent.

**Finding #6b — PROTOCOL gap (Raft §5.4.2 / Fig. 8 class): a commit can
certify an old-term-only range and the certified bytes can then be
truncated cluster-wide.** `finding_fig8_old_term_commit_data_loss` (46
steps, n = 5) drives the classical Figure-8 interleaving end to end:

- term 1: node 0 leads, appends `(pos 0, stamp 1, payload 10)`, replicates
  it to node 1 (reconcile + accept — a genuine 2-node copy);
- term 2: node 4 wins on the empty-log voters {2, 3} and appends its own
  divergent `(pos 0, stamp 2, payload 20)`, which stays local;
- term 3: node 0 is re-elected (voters {1, 2}); nodes 1 and 2 reconcile
  against its map ([(1,0),(3,1)] — clean, no truncation), node 2 repairs
  pos 0 from the term-1 replicate frame still on the wire (in Rust: NAK
  repair off the leader's log buffer serves the same stamped bytes), and
  both REPORT durable 1 — truthfully, gate open, at term 3. The tracker
  ranks {own 1, 1, 1, 0, 0} → quorum-3rd = 1 > 0: **`advance` fires and
  commits [0,1) — a range containing ONLY the old term-1 byte.** The ghost
  records (0, 1, 10). Nothing in `CommitTracker.advance` (the REAL kernel,
  `commit.rs`) nor in the model requires the certified position to cover a
  current-term byte.
- term 4: node 4 — whose divergent pos-0 byte gives it `lastTerm 2` while
  commit-quorum member node 1 still has `lastTerm 1` (it never received a
  term-3-stamped byte) — wins the election WITH NODE 1'S GRANT
  (`logOk (1,1) < (2,1)` — lexicographic, honest). Its map
  [(2,0),(4,1)] then gossips: every holder of the committed byte
  reconciles at `commonPrefixLen = 0`, `validUpTo = 0` — **the committed
  entry is erased from every node in the cluster** (node 1 even re-accepts
  the divergent `(2, 20)` at the committed position). The final world has
  `(0, 1, 10) ∈ committed` and `∀ i, hist i 0 ≠ some (1, 10)`.

  This falsifies EVERY LC variant (the stamp form, the commit-term form —
  the commit term is 3, node 4's term is 4 — and any
  committed-never-truncated clause). It is data loss of a
  quorum-certified — applied, output-fired, ackable — range.

**Rust mapping of #6b** (why the model is honest here):
- `commit.rs::advance` ranks `{own} ∪ reported` and clamps by own durable
  only — no term-base clamp (C2 is the only bound).
- `election.rs::rank_leader` (1421–1430) pushes `Action::AdvanceCommit`
  UNCONDITIONALLY on a firing advance; `node.rs` ~2505 stores it in the cnc
  commit counter ("the ONLY commit store in the binary") and
  `Action::GossipCommit` fans it out to every follower/learner — the apply
  agents and position-keyed client acks and leader-only `on_committed`
  outputs all key off that counter.
- UC DOES have the Raft §5.4.2 machinery — `become_leader` appends the
  NewTerm no-op frame and `new_term_pos`/`serving` (election.rs 279–282,
  1432–1444) latch once the rank covers it — **but it gates only the
  linearizable-READ path (`can_serve`, node.rs read barrier) and M7
  `propose_config`, NOT the commit store**. The commit can advance to the
  election base off reports that arrive after clean reconcile (the 20 ms
  AppendPosition floor) but before the NewTerm frame is quorum-durable —
  exactly the model's window. The model omits the NewTerm append entirely,
  which WIDENS the window (node 1 keeps `lastTerm 1` indefinitely); in Rust
  the rival's RequestVote must beat the in-flight NewTerm byte to the
  voter — narrower, but a real race (vote datagrams vs. the data stream
  under loss/NAK repair), and the unsafe commit itself needs no race at
  all: it fires the moment two base-level reports land.
- Candidate Rust fix (escalated, NOT applied): clamp the commit exactly
  like `serving` — in `rank_leader`, advance/store commit only once
  `ranked ≥ new_term_pos` (Raft §5.4.2: never commit old-term entries by
  counting replicas; the current-term NewTerm commit carries the prefix).
  Model amendment: `leaderAdvanceCommit` gains the same enabling (the
  advance must cross the leader's current-term base), with `becomeLeader`
  optionally folding the NewTerm append so commits stay live.

Both countermodels are kernel-`decide`d end to end off `World.init` —
no axioms beyond `propext`/`Quot.sound`. Escalation record, candidate
amendments, and the post-fix proof architecture: task report
`.superpowers/sdd/task-LB2-report.md`. -/

namespace Uc2.Cert

/-- Local copy of LB1's trace-discharge helper (private in
`ProtocolCommit.lean`): the kernel cannot `decide`-reduce
`CommitTracker.advance` (`List.mergeSort` is well-founded recursion), so
`decide` pins the trace world's tracker/durable to literals and one `simp`
discharges the concrete advance. -/
private theorem advance_fires {t : CommitTracker} {own : Nat}
    (t' : CommitTracker) (own' k : Nat)
    (ht : t = t') (hown : own = own')
    (hk : (t'.advance own').2 = some k) :
    (t.advance own).2 = some k := by
  subst ht
  subst hown
  exact hk

/-- **Finding #6a (statement gap): the stamp-keyed LC-core is false for a
correct protocol run.** 23-step trace, n = 3, no crash, no staleness, no
old-term-only commit (the certified range [0,2) covers the committing
term's own byte at pos 1, so this trace SURVIVES the #6b fix):

1–5. node 0 wins term 1 (grant: node 1) and appends `(0, 1, 42)`;
6–10. node 2 — empty log — wins term 2 on empty-log voters (node 1 grants
  at term 2 BEFORE ever receiving the entry; `logOk (0,0) ≤ (0,0)`), and
  becomes the **stale intermediate leader**: term 2, `hist 0 = none`. It
  processes nothing further and is never deposed.
11–15. node 0 (rejecting term 2's empty-log candidate, then campaigning
  with credentials `(lastTerm 1, durable 1)`) wins term 3 with node 1's
  grant;
16–23. node 0 appends a term-3 byte, gossips its map, node 1 reconciles
  (gate reopens), replicates BOTH bytes, reports durable 2 at term 3, and
  the kernel advance fires: the ghost commits `(0, 1, 42)` (and
  `(1, 3, 99)`) — the stamp-1 entry is committed AT TERM 3.

Final world: `(0, 1, 42) ∈ committed`, node 2 is a leader with
`currentTerm = 2 > 1 = stamp`, and `hist 0 = none` — every LC-core
hypothesis holds and the conclusion fails. The commit term is 3 > 2: the
Raft property (keyed on the COMMIT term) is not violated; the stamp-keyed
statement is. -/
theorem finding_stamp_keyed_lc_stale_leader :
    ∃ w : World 3, Reachable w ∧
      (0, 1, 42) ∈ w.committed ∧
      (w.nodes 2).pn.role = .leader ∧
      1 < (w.nodes 2).pn.currentTerm ∧
      (w.nodes 2).hist 0 = none ∧
      (w.nodes 2).hist 0 ≠ some (1, 42) := by
  refine ⟨_,
    .tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail
      (.single (.startElection _ 0 (by decide)))
      (.deliverRequestVote _ 1 0 1 0 0 (by decide) (by decide)))
      (.deliverVote _ 0 1 1 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)))
      (.leaderAppend _ 0 42 (by decide)))
      (.deliverRequestVote _ 2 0 1 0 0 (by decide) (by decide)))
      (.startElection _ 2 (by decide)))
      (.deliverRequestVote _ 1 2 2 0 0 (by decide) (by decide)))
      (.deliverVote _ 2 1 2 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 2 (by decide) (by decide)))
      (.deliverRequestVote _ 0 2 2 0 0 (by decide) (by decide)))
      (.startElection _ 0 (by decide)))
      (.deliverRequestVote _ 1 0 3 1 1 (by decide) (by decide)))
      (.deliverVote _ 0 1 3 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)))
      (.leaderAppend _ 0 99 (by decide)))
      (.shipTermMap _ 0 (by decide)))
      (.deliverTermMap _ 1 3 [(1, 0), (3, 1)] (by decide) (by decide)))
      (.deliverReplicate _ 1 0 1 42 (by decide) (by decide) (by decide)
        (by decide)))
      (.deliverReplicate _ 1 1 3 99 (by decide) (by decide) (by decide)
        (by decide)))
      (.sendReport _ 1 (by decide) (by decide)))
      (.deliverReport _ 0 1 3 2 (by decide) (by decide) (by decide)
        (by decide)))
      (.leaderAdvanceCommit _ 0 2 (by decide)
        (advance_fires ⟨[2, 0], 2, 0⟩ 2 2 (by decide) (by decide)
          (by simp [CommitTracker.advance, CommitTracker.ranking,
                List.mergeSort]))),
    by decide, by decide, by decide, by decide, by decide⟩

/-- The FIXED LC-core statement, ∀-quantified at n = 3, is refuted by
Finding #6a's trace. -/
theorem lc_core_stamp_keyed_is_false :
    ¬ ∀ (w : World 3), Reachable w →
        ∀ p t v, (p, t, v) ∈ w.committed →
          ∀ i : Fin 3, (w.nodes i).pn.role = .leader →
            t < (w.nodes i).pn.currentTerm →
            (w.nodes i).hist p = some (t, v) := by
  intro h
  obtain ⟨w, hw, hc, hrole, ht, -, hne⟩ := finding_stamp_keyed_lc_stale_leader
  exact hne (h w hw 0 1 42 hc 2 hrole ht)

/-- **Finding #6b (PROTOCOL gap, Raft §5.4.2 / Figure 8): an
old-term-only commit, later truncated cluster-wide.** 46-step trace,
n = 5 (module doc for the narrative and the Rust mapping):

1–10. node 0 wins term 1 ({0,1,2}), appends `(0, 1, 10)`, node 1
  reconciles and accepts it (genuine copy, `lastTerm 1`);
11–18. node 4 wins term 2 on empty-log voters ({2,3,4}) and appends the
  divergent `(0, 2, 20)` locally (`lastTerm 2`, never replicated);
19–25. node 0 wins term 3 ({0,1,2}; node 4's t2 candidacy already
  rejected by node 0's fresher log);
26–33. node 0 gossips [(1,0),(3,1)]; nodes 1, 2 reconcile CLEAN (gates
  reopen), node 2 repairs pos 0 from the term-1 frame; both report
  durable 1 at term 3 — truthful, honest, at the election base;
34. **the kernel advance fires at k = 1**: quorum {own, node 1, node 2}
  certifies [0,1) — an old-term-only range; ghost commits `(0, 1, 10)`.
  Neither node 1 nor node 2 holds any term-3 byte: their `lastTerm` is
  still 1.
35–41. node 4 adopts term 3 (rejecting nothing it needs), campaigns at
  term 4 with credentials `(lastTerm 2, durable 1)`, and **commit-quorum
  member node 1 grants it** (`logOk`: 1 < 2) along with empty node 3 —
  node 4 becomes the term-4 leader holding `(0, 2, 20)`;
42–46. node 4's map [(2,0),(4,1)] gossips; nodes 1, 0, 2 reconcile at
  `commonPrefixLen 0`, `validUpTo 0`: every copy of the committed byte
  dies, and node 1 re-accepts the divergent `(2, 20)` at the committed
  position.

Final world: `(0, 1, 10) ∈ committed`; node 4 is a leader at term
4 > 3 (commit term) > 1 (stamp) holding `(2, 20)` at position 0; and NO
node in the cluster holds the committed `(1, 10)`. Falsifies the stamp
form, the commit-term form, and committed-never-truncated. -/
theorem finding_fig8_old_term_commit_data_loss :
    ∃ w : World 5, Reachable w ∧
      (0, 1, 10) ∈ w.committed ∧
      (w.nodes 4).pn.role = .leader ∧
      1 < (w.nodes 4).pn.currentTerm ∧
      (w.nodes 4).hist 0 = some (2, 20) ∧
      (∀ i : Fin 5, (w.nodes i).hist 0 ≠ some (1, 10)) := by
  refine ⟨_,
    .tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail (.tail (.tail (.tail
      (.single (.startElection _ 0 (by decide)))
      (.deliverRequestVote _ 1 0 1 0 0 (by decide) (by decide)))
      (.deliverRequestVote _ 2 0 1 0 0 (by decide) (by decide)))
      (.deliverVote _ 0 1 1 (by decide) (by decide) (by decide)))
      (.deliverVote _ 0 2 1 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)))
      (.leaderAppend _ 0 10 (by decide)))
      (.shipTermMap _ 0 (by decide)))
      (.deliverTermMap _ 1 1 [(1, 0)] (by decide) (by decide)))
      (.deliverReplicate _ 1 0 1 10 (by decide) (by decide) (by decide)
        (by decide)))
      (.deliverRequestVote _ 4 0 1 0 0 (by decide) (by decide)))
      (.startElection _ 4 (by decide)))
      (.deliverRequestVote _ 2 4 2 0 0 (by decide) (by decide)))
      (.deliverRequestVote _ 3 4 2 0 0 (by decide) (by decide)))
      (.deliverVote _ 4 2 2 (by decide) (by decide) (by decide)))
      (.deliverVote _ 4 3 2 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 4 (by decide) (by decide)))
      (.leaderAppend _ 4 20 (by decide)))
      (.deliverRequestVote _ 0 4 2 0 0 (by decide) (by decide)))
      (.startElection _ 0 (by decide)))
      (.deliverRequestVote _ 1 0 3 1 1 (by decide) (by decide)))
      (.deliverRequestVote _ 2 0 3 1 1 (by decide) (by decide)))
      (.deliverVote _ 0 1 3 (by decide) (by decide) (by decide)))
      (.deliverVote _ 0 2 3 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)))
      (.shipTermMap _ 0 (by decide)))
      (.deliverTermMap _ 1 3 [(1, 0), (3, 1)] (by decide) (by decide)))
      (.deliverTermMap _ 2 3 [(1, 0), (3, 1)] (by decide) (by decide)))
      (.deliverReplicate _ 2 0 1 10 (by decide) (by decide) (by decide)
        (by decide)))
      (.sendReport _ 1 (by decide) (by decide)))
      (.sendReport _ 2 (by decide) (by decide)))
      (.deliverReport _ 0 1 3 1 (by decide) (by decide) (by decide)
        (by decide)))
      (.deliverReport _ 0 2 3 1 (by decide) (by decide) (by decide)
        (by decide)))
      (.leaderAdvanceCommit _ 0 1 (by decide)
        (advance_fires ⟨[1, 1, 0, 0], 3, 0⟩ 1 1 (by decide) (by decide)
          (by simp [CommitTracker.advance, CommitTracker.ranking,
                List.mergeSort]))))
      (.deliverRequestVote _ 4 0 3 1 1 (by decide) (by decide)))
      (.startElection _ 4 (by decide)))
      (.deliverRequestVote _ 1 4 4 2 1 (by decide) (by decide)))
      (.deliverRequestVote _ 3 4 4 2 1 (by decide) (by decide)))
      (.deliverVote _ 4 1 4 (by decide) (by decide) (by decide)))
      (.deliverVote _ 4 3 4 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 4 (by decide) (by decide)))
      (.shipTermMap _ 4 (by decide)))
      (.deliverTermMap _ 1 4 [(2, 0), (4, 1)] (by decide) (by decide)))
      (.deliverReplicate _ 1 0 2 20 (by decide) (by decide) (by decide)
        (by decide)))
      (.deliverTermMap _ 0 4 [(2, 0), (4, 1)] (by decide) (by decide)))
      (.deliverTermMap _ 2 4 [(2, 0), (4, 1)] (by decide) (by decide)),
    by decide, by decide, by decide, by decide, by decide⟩

#print axioms finding_stamp_keyed_lc_stale_leader
#print axioms lc_core_stamp_keyed_is_false
#print axioms finding_fig8_old_term_commit_data_loss

end Uc2.Cert
