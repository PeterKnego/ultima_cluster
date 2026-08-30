/-
Issue #7 — the durable dual-reader skew, as a machine-checked pair.

`Uc2Proofs/StageB.lean` builds the report-to-grant bridge (`GrantReport`) out of
two facts about ONE node `y` that reported `(T, d)` and then granted a vote to a
candidate advertising `(cLastTerm, cd)`:

  * `ReportEraFloor`  :  `d ≤ (w.nodes y).pn.durable`
  * `logOk`           :  `(w.nodes y).pn.durable ≤ cd`   (same term)

and joins them with an `omega` (`StageB.lean` ~1899-1912) to conclude `d ≤ cd`.

That join is only valid if the durable position a node REPORTS and the durable
position it JUDGES VOTES AGAINST are the same number. In `Uc2Proofs` they are:
`PNode.durable` is a single `Nat` serving both roles. **In the real node they are
two reads of one shared counter, on two different threads** — the receiver agent
reports it directly, while the consensus agent absorbs it into `ElectionSm` a
duty cycle later — and before `main` 26d4827 the vote path compared against the
absorbed copy. A voter could therefore grant while judging the candidate against
a self-view LOWER than the value it had already reported for commit ranking,
letting a candidate behind a committed position win and collapse below it. That
is an acked-write loss, and it is real: reproduced deterministically in
`uc_node`'s `a_vote_is_refused_against_a_fresh_read_of_our_own_log` and in
`uc_sim`'s `stale_vote_credential_opens_a_term_below_a_committed_position`.

This file pins both halves of that, so the corpus records the distinction
whether or not the full `PNode` split (issue #7's Lean half) has landed:

  * `grant_bridge_sound_on_counter` — with the vote comparand on the COUNTER,
    the join is valid. This is the SHIPPED system, and it is why `GrantReport`
    is still provable.
  * `grant_bridge_false_on_absorbed_copy` — with the vote comparand on a LAGGING
    absorbed copy, the same join is FALSE. Not unprovable: false, with a witness.

The gap between them is exactly the bug 26d4827 fixed.

NOTE on scope. These are statements about the *join*, over bare `Nat`s and the
real `logOk` kernel, deliberately independent of `PNode`.

They were written when `PNode.durable` was still ONE field, at which point no
world-level theorem could even *state* the distinction. **That is no longer
true**: `PNode` now carries `durable` (the counter, reported and compared) and
`smDurable` (the absorbed copy, advertised), and `SmLeDurable` relates them. The
model can therefore now REACH a state where the two disagree —
`nonvacuity_durable_skew_trace` below exhibits one.

**The world-level lift is done**, in `Uc2Proofs/DurableSkewWorld.lean`: the sound
half is the existing `Uc2.Cert.reachable_grant_report`, and the false half is
`report_dominates_credential_false_under_stale_rule`, which refutes the bridge
over a 13-step REACHABLE trace under a counterfactual grant rule. These two
statements remain useful as the arithmetic core the world-level pair rests on.
-/
import Uc2Proofs.Vote
import Uc2Proofs.ProtocolData

namespace Uc2.DurableSkew

open Uc2

/-- **The bridge, on the counter — SOUND.** If the position a voter reported is
at most the position it judges votes against (`ReportEraFloor`, which is
reflexivity when both are the counter), and `logOk` granted at a matching term,
then the candidate's advertised credential dominates what the voter reported.

This is the `omega` at `StageB.lean` ~1899-1912, isolated. Nothing here is
surprising — it is recorded so that its twin below has something to contrast
with, and so the hypothesis doing the work (`hrep`, on the SAME value `logOk`
reads) is named rather than implicit. -/
theorem grant_bridge_sound_on_counter
    (T counter reported cLastTerm cd : Nat)
    (hrep : reported ≤ counter)
    (hlog : logOk T counter cLastTerm cd = true)
    (hsame : cLastTerm = T) :
    reported ≤ cd := by
  rcases (logOk_iff T counter cLastTerm cd).mp hlog with hlt | ⟨_, hdur⟩
  · omega
  · omega

/-- **The same bridge, on a lagging absorbed copy — FALSE.**

The voter's counter is at 1000 and it has REPORTED 1000 (so a leader may already
have ranked that report into `commit` and acked the write). Its consensus agent
has not yet absorbed the advance, so `ElectionSm::durable` still holds 900. A
candidate that is genuinely 100 bytes behind advertises 900. `logOk` compares
`(T, 900)` against `(T, 900)` — a TIE, and `logOk` grants on `≤`. The vote is
granted, and the candidate can now win and open its term below a position the
cluster has already committed.

Note the absorbed copy is a perfectly legitimate value: `900 ≤ 1000`, i.e. it
satisfies every invariant relating it to the counter. The unsoundness is not
that the copy is corrupt — it is that the JOIN silently treats it as the same
number the report was stamped from. -/
theorem grant_bridge_false_on_absorbed_copy :
    ¬ ∀ (T counter absorbed reported cLastTerm cd : Nat),
        absorbed ≤ counter →                        -- the copy is a valid lag
        reported ≤ counter →                        -- ReportEraFloor, on the counter
        logOk T absorbed cLastTerm cd = true →      -- the PRE-FIX grant rule
        cLastTerm = T →
        reported ≤ cd := by
  intro h
  have hbad : (1000 : Nat) ≤ 900 :=
    h 2 1000 900 1000 2 900 (by decide) (by decide) (by decide) rfl
  omega

/-- The witness spelled out as data, so the numbers in the scenario are checked
rather than merely described in a comment: the absorbed copy is a valid lag, the
report rides the counter, and `logOk` grants on the tie. -/
example :
    (900 : Nat) ≤ 1000 ∧ (1000 : Nat) ≤ 1000 ∧
      logOk 2 900 2 900 = true ∧ ¬ (1000 : Nat) ≤ 900 := by
  refine ⟨by decide, by decide, by decide, by decide⟩

/-- What the fix bought, stated directly: re-reading the counter before the grant
decision (`refresh_durable` in `uc_node`, called from `feed_net`'s
`NetEvent::RequestVote` arm) makes the SAME scenario refuse the vote. -/
example : logOk 2 1000 2 900 = false := by decide

/-- Issue #7 NON-VACUITY: the split is not cosmetic — the model can actually
reach a state where the absorbed copy LAGS the counter, which is the whole
phenomenon. Node 0 wins term 1 and appends a byte, moving its counter to 1; its
consensus agent has not polled, so `smDurable` is still 0.

Before the split this state was not merely unreachable, it was inexpressible:
there was one field, so `smDurable < durable` had no meaning. -/
theorem nonvacuity_durable_skew_trace :
    ∃ w : Uc2.Data.World 3, Uc2.Data.Reachable w ∧
      (w.nodes 0).pn.smDurable < (w.nodes 0).pn.durable := by
  refine ⟨_,
    .tail (.tail (.tail (.tail
      (.single (.startElection _ 0 (by decide)))
      (.deliverRequestVote _ 1 0 1 0 0 (by decide) (by decide)))
      (.deliverVote _ 0 1 1 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)))
      (.leaderAppend _ 0 42 (by decide)),
    by decide⟩

#print axioms nonvacuity_durable_skew_trace
#print axioms grant_bridge_sound_on_counter
#print axioms grant_bridge_false_on_absorbed_copy

end Uc2.DurableSkew
