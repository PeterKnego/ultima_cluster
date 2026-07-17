import Uc2Proofs.ProtocolCommit

/-! LB2 (re-run) — LEADER COMPLETENESS: BLOCKED again, Finding #7
(model-fidelity gap: cross-stream stale-replicate replay).

## History

The original FIXED LC-core contract (LB2 brief, decision 5) was refuted
twice over by machine-checked countermodels that lived in this file
(commit `14cdcfc`; full traces and Rust evidence in
`.superpowers/sdd/task-LB2-report.md` and the lean gate doc):

- **Finding #6a (statement gap)**: the ghost recorded the data STAMP while
  Raft's Leader Completeness (§5.4.3) keys on the COMMIT term. FIXED by
  re-keying the ghost to `(position, stamp, commitTerm, payload)`
  (`Uc2Proofs/ProtocolCommit.lean`, module doc item 1).
- **Finding #6b (PROTOCOL gap, Raft §5.4.2 / Figure 8 — a REAL v2.x
  acked-write-loss bug)**: an old-term-only range could commit at the
  election base before the NewTerm frame was quorum-durable. FIXED in Rust
  (`election.rs::rank_leader` commit clamp) and mirrored as
  `leaderAdvanceCommit`'s `hbase` enabling (module doc item 9).

## Finding #7 — the re-keyed LC-core is STILL false in this model
(`finding_stale_replicate_replay_lc_violation`, 33-step kernel trace,
n = 3, + `lc_core_commit_term_keyed_is_false`)

**The re-keyed FIXED statement**

```
theorem leader_completeness {n : Nat} (w : World n) (hw : Reachable w)
    (p t T v : Nat) (hc : (p, t, T, v) ∈ w.committed)
    (i : Fin n) (hi : (w.nodes i).pn.role = .leader)
    (ht : T < (w.nodes i).pn.currentTerm) :
    (w.nodes i).hist p = some (t, v)
```

is refuted (in the `T < currentTerm` FIXED-contract form, hence a fortiori
in the `T ≤` strengthening) by a reachable trace whose pivot is
**cross-stream stale-frame replay**: `deliverReplicate`'s stamp guard
`hstamp : t ≤ currentTerm` (inherited verbatim from the data model, LA1
module doc item 6) lets a follower that has just reconciled CLEANLY against
the current T-leader's map re-accept a **stale lower-term replicate frame
from a dead leader's stream** at its truncated frontier, then accept a
genuine T-stamped byte on top. Its data-stamped map now ends in a `(T, b)`
entry, its durable covers the leader's commit range, and its intake gate
never closed — so its AppendPosition report at term T is truthful,
gate-open, and folded by the T-leader's tracker, certifying (through the
Finding-#6b clamp, which is satisfied: the range crosses the T base) a
commit range containing a position where the reporter's CONTENT diverges
from the leader's log. The reporter then wins term T+1 on its
`(lastTerm = T, durable ≥ k)` credentials — a leader above the commit term
that does not hold the committed entry.

**Trace shape** (n = 3; every enabling condition honest — no staleness
beyond ordinary in-flight frames, one crash used only to free node 0 from
its own stale leadership):

- t1: node 0 leads {0,1}, appends `(pos 0, stamp 1, payload 10)`; node 1
  reconciles (gate opens) and accepts it.
- t2: node 1 leads {1,2} and appends `(pos 1, stamp 2, payload 20)` — the
  future stale frame — reaching durable 2.
- t3: node 0 (crash-restarted to follower) wins {0,2} on credentials
  `(1, 1)`; `prunePush` opens its map `[(1,0),(3,1)]` (base₃ = 1); it
  appends `(pos 1, stamp 3, payload 30)` and `(pos 2, stamp 3, payload 31)`
  and gossips its map. Node 1 adopts term 3 via the RequestVote (gate
  closes), reconciles against the t3 map — its divergent t2 tail dies at
  `validUpTo = 1`, gate REOPENS — and then:
  - **re-accepts its own stale t2 frame** `(1, 2, 20)` at its truncated
    frontier 1 (`hstamp : 2 ≤ 3` — the over-approximation), then
  - accepts the leader's genuine `(2, 3, 31)` at frontier 2, so its map
    grows `[(1,0),(2,1),(3,2)]` — LAST ENTRY TERM 3 — durable 3.
- Node 1 reports `(term 3, durable 3)` (follower, gate open — truthful);
  the leader folds it; the kernel `advance` fires at k = 3; `hbase` holds
  (base₃ = 1 < 3); the ghost commits `(1, 3, 3, 30)` from the LEADER's
  hist — while node 1's hist at 1 is `(2, 20)`.
- t4: node 1 wins {1,2} on `(lastTerm 3, durable 3)`. Final world:
  `(1, 3, 3, 30) ∈ committed`, node 1 is leader, `3 < currentTerm 1 = 4`,
  `hist 1 1 = some (2, 20) ≠ some (3, 30)`.

**Classification: MODEL-FIDELITY gap, not a Rust bug.** Verified in source
this session: `uc2_net/src/receiver.rs:635-639` drops any DATA datagram
whose header `leadership_term_id` is not EXACTLY the adopted term
(`dropped_stale_term`) — record stamps ride inside the datagram BODY under
the CURRENT leader's header (catch-up/NAK repair re-serves old-stamped
records under the new leadership term), while adoption comes only from
consensus datagrams. So in Rust a follower's post-reconcile intake is
scoped to the live T-leader's stream and the Frankenstein log above is
structurally impossible. The model's `Frame.replicate pos term payload`
conflates the record STAMP with the datagram HEADER term, and the `≤`
guard (documented in LA1 as "the model's ≤ consequence of two Rust
guards") is sound for LOG MATCHING (which is per-(pos, stamp) and does not
care which stream delivered a byte) but UNSOUND for LEADER COMPLETENESS.

**Why no local repair exists inside this task's staging envelope**: the
faithful fix gives frames both a header term and a record stamp
(acceptance requires `header = currentTerm`; `observeTerm` keeps stamping
by the record stamp; a leader re-serves its hist under its own header via
a new serve-tail step) — a `Uc2Proofs/ProtocolData.lean` amendment that
re-opens the LA2 preservation proof, i.e. controller territory under the
LA1 rules, exactly like the #6a ghost re-key was. The alternatives
(equality-`hstamp`, or an enabling that names the live same-term leader's
content) under-approximate Rust — they erase the real catch-up-of-
old-stamped-bytes behavior the model exists to cover — and are recorded in
the task report as explicit controller trade-offs, not applied here.

The LC-core proof against the repaired model remains the next re-run's
deliverable; the invariant architecture (holders quorum + canonical-prefix
`SplitsAt` + Cert-at-T + the grant-time freshness chain) and the
establishment-order analysis this trace fell out of are in
`.superpowers/sdd/task-LB2-rerun-report.md`. -/

namespace Uc2.Cert

/-- Trace-discharge helper for `leaderAdvanceCommit`'s enabling (the LB1
pattern, re-proved locally — `ProtocolCommit.lean`'s copy is private): the
kernel cannot `decide`-reduce `advance` (`List.mergeSort` is well-founded
recursion), so the trace world's tracker/durable are pinned by kernel
`decide` and the concrete advance is discharged by `simp` once, here. -/
private theorem advance_fires {t : CommitTracker} {own : Nat}
    (t' : CommitTracker) (own' k : Nat)
    (ht : t = t') (hown : own = own')
    (hk : (t'.advance own').2 = some k) :
    (t.advance own).2 = some k := by
  subst ht
  subst hown
  exact hk

/-- **Finding #7.** The cross-stream stale-replicate replay trace (module
doc): a reachable world satisfying every hypothesis of the re-keyed FIXED
LC-core — a genuinely committed `(1, 3, 3, 30)` (through the kernel
tracker AND the #6b `hbase` clamp) and a leader strictly above the commit
term — whose leader holds DIFFERENT content at the committed position
(`(2, 20)`, its replayed stale byte). -/
theorem finding_stale_replicate_replay_lc_violation :
    ∃ w : World 3, Reachable w ∧
      (1, 3, 3, 30) ∈ w.committed ∧
      (w.nodes 1).pn.role = .leader ∧
      3 < (w.nodes 1).pn.currentTerm ∧
      (w.nodes 1).hist 1 = some (2, 20) ∧
      (w.nodes 1).hist 1 ≠ some (3, 30) := by
  refine ⟨_,
    .tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail
      -- t1: node 0 leads {0,1} and replicates (0, 1, 10) to node 1.
      (.single (.startElection _ 0 (by decide)))
      (.deliverRequestVote _ 1 0 1 0 0 (by decide) (by decide)))
      (.deliverVote _ 0 1 1 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)))
      (.leaderAppend _ 0 10 (by decide)))
      (.shipTermMap _ 0 (by decide)))
      (.deliverTermMap _ 1 1 [(1, 0)] (by decide) (by decide)))
      (.deliverReplicate _ 1 0 1 10 (by decide) (by decide) (by decide)
        (by decide)))
      -- t2: node 1 leads {1,2}; its (1, 2, 20) append is the stale frame.
      (.startElection _ 1 (by decide)))
      (.deliverRequestVote _ 2 1 2 1 1 (by decide) (by decide)))
      (.deliverVote _ 1 2 2 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 1 (by decide) (by decide)))
      (.leaderAppend _ 1 20 (by decide)))
      -- t3: node 0 (crash-restarted follower) wins {0,2} at term 3.
      (.crashRestart _ 0))
      (.startElection _ 0 (by decide)))
      (.startElection _ 0 (by decide)))
      (.deliverRequestVote _ 2 0 3 1 1 (by decide) (by decide)))
      (.deliverVote _ 0 2 3 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)))
      (.leaderAppend _ 0 30 (by decide)))
      (.leaderAppend _ 0 31 (by decide)))
      (.shipTermMap _ 0 (by decide)))
      -- node 1: adopt t3 (gate closes), reconcile clean (tail dies,
      -- gate reopens) ...
      (.deliverRequestVote _ 1 0 3 1 1 (by decide) (by decide)))
      (.deliverTermMap _ 1 3 [(1, 0), (3, 1)] (by decide) (by decide)))
      -- ... then REPLAY the stale t2 frame at the truncated frontier
      -- (hstamp: 2 ≤ 3 — the over-approximation) and a genuine t3 byte.
      (.deliverReplicate _ 1 1 2 20 (by decide) (by decide) (by decide)
        (by decide)))
      (.deliverReplicate _ 1 2 3 31 (by decide) (by decide) (by decide)
        (by decide)))
      -- the truthful gate-open report certifies the commit at k = 3
      -- (hbase: base₃ = 1 < 3 — the #6b clamp is satisfied).
      (.sendReport _ 1 (by decide) (by decide)))
      (.deliverReport _ 0 1 3 3 (by decide) (by decide) (by decide)
        (by decide)))
      (.leaderAdvanceCommit _ 0 3 (by decide) ⟨(3, 1), by decide⟩
        (advance_fires ⟨[3, 0], 2, 0⟩ 3 3 (by decide) (by decide)
          (by simp [CommitTracker.advance, CommitTracker.ranking,
                List.mergeSort]))))
      -- t4: the divergent reporter wins on (lastTerm 3, durable 3).
      (.startElection _ 1 (by decide)))
      (.deliverRequestVote _ 2 1 4 3 3 (by decide) (by decide)))
      (.deliverVote _ 1 2 4 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 1 (by decide) (by decide)),
    by decide, by decide, by decide, by decide, by decide⟩

/-- The re-keyed FIXED LC-core statement (`T < currentTerm` form, at
n = 3) is FALSE for the post-#6a/#6b model — hence so is the `T ≤`
strengthening (the countermodel satisfies the strictly stronger
hypothesis). -/
theorem lc_core_commit_term_keyed_is_false :
    ¬ ∀ (w : World 3), Reachable w →
        ∀ (p t T v : Nat), (p, t, T, v) ∈ w.committed →
        ∀ i : Fin 3, (w.nodes i).pn.role = .leader →
        T < (w.nodes i).pn.currentTerm →
        (w.nodes i).hist p = some (t, v) := by
  intro h
  obtain ⟨w, hw, hc, hrole, hterm, -, hne⟩ :=
    finding_stale_replicate_replay_lc_violation
  exact hne (h w hw 1 3 3 30 hc 1 hrole hterm)

#print axioms finding_stale_replicate_replay_lc_violation
#print axioms lc_core_commit_term_keyed_is_false

end Uc2.Cert
