import Mathlib.Tactic
import Uc2Model.Vote

namespace Uc2

/-! **V3 (runtime assumption, not a theorem).** The model treats
grant-and-record as one atomic step. The Rust discharges this via
`Action::PersistAndSendVote` — the vote record is fsynced via `StableValue`
BEFORE the grant datagram is sent (uc_node's action executor), so a
crash-restart can never un-vote. Discharged by code inspection; Tier B's
crash-restart step preserves `votedFor` accordingly. -/

/-- A granted `handleRequestVote` always leaves `votedFor` pointing at the
winning candidate this term, whether via a fresh grant (record updated) or an
idempotent re-grant (state unchanged, but the existing `votedFor` already
names the candidate). `currentTerm` is untouched either way. -/
private theorem handleRequestVote_granted_votedFor (s : VoterState) (c t d : Nat)
    (h : (handleRequestVote s c t d).2 = .granted) :
    (handleRequestVote s c t d).1.votedFor = some (s.currentTerm, c) ∧
    (handleRequestVote s c t d).1.currentTerm = s.currentTerm := by
  rcases hv : s.votedFor with _ | ⟨vt, vid⟩
  · simp only [handleRequestVote, hv, handleRequestVote.grantIfFresh] at h ⊢
    split_ifs at h ⊢ with hlog
    · exact ⟨rfl, rfl⟩
  · simp only [handleRequestVote, hv, handleRequestVote.grantIfFresh] at h ⊢
    split_ifs at h ⊢ with hvt hvid hlog
    · exact ⟨by rw [hv, hvt, hvid], rfl⟩
    · exact ⟨rfl, rfl⟩

/-- **V1.** Single vote per term: from any state, two grants in the same
`currentTerm` go to the same candidate (re-grant is idempotent). -/
theorem vote_unique_per_term (s : VoterState) (c1 c2 : Nat)
    (t1 t2 d1 d2 : Nat)
    (h1 : (handleRequestVote s c1 t1 d1).2 = .granted)
    (h2 : (handleRequestVote (handleRequestVote s c1 t1 d1).1 c2 t2 d2).2
      = .granted) :
    c1 = c2 ∨ (handleRequestVote s c1 t1 d1).1.currentTerm ≠ s.currentTerm := by
  left
  obtain ⟨hvf, hct⟩ := handleRequestVote_granted_votedFor s c1 t1 d1 h1
  set s' := (handleRequestVote s c1 t1 d1).1 with hs'
  simp [handleRequestVote, hvf, hct] at h2
  split_ifs at h2 with hvid
  · exact hvid

/-- **V2.** A fresh grant implies the candidate's `(lastTerm, durable)`
dominates the voter's, lexicographically. (The idempotent re-grant case is
excluded by `votedFor = none`.) -/
theorem grant_implies_logOk (s : VoterState) (c t d : Nat)
    (hnone : s.votedFor = none)
    (h : (handleRequestVote s c t d).2 = .granted) :
    logOk s.lastTerm s.durable t d = true := by
  simp only [handleRequestVote, hnone, handleRequestVote.grantIfFresh] at h
  split_ifs at h with hlog
  · exact hlog

/-- **V2 (frontier form, the leader-completeness seed).** `logOk` is the
decidable form of the lexicographic order: it holds iff
`(ourTerm, ourDurable) ≤ (candTerm, candDurable)` in `Prod.Lex`-style order.
Stated concretely for Tier B's use. -/
theorem logOk_iff (ourT ourD cT cD : Nat) :
    logOk ourT ourD cT cD = true ↔
      (ourT < cT ∨ (ourT = cT ∧ ourD ≤ cD)) := by
  unfold logOk
  split_ifs with h1 h2 <;> simp_all

end Uc2
