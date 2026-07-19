import Uc2Proofs.MapWF
import Uc2Proofs.LeaderCompleteness

/-! LC3 — message-indexed report/frame provenance + the
`FramesCurrentAuthored` discharge.

The serveTail residual (LC2b): a lagging node keeps accepting OLD header-`D`
frames re-delivered from the append-only `dsent` AFTER the `D`-leader has
stepped down, when `gossip_pinned` is vacuous and no state-only invariant can
witness the (frozen-at-tenure) attribution the frame's stamp was certified
against. Fix (this file): carry the attribution ON THE MESSAGES — the
`Inv.grant_state` pattern (`ElectionSafety.lean`) over the data wire and the
commit wire. The bundle `ProvInv` below has four planes:

- **message plane** (`frame_gossip`/`frame_mono`/`frame_uniq`/`frame_cert`):
  every header-`hdr` replicate frame's stamp agrees with what ANY
  header-`hdr` gossip attributes to its position; same-header stamps are
  position-monotone and per-position unique; a header is never minted
  without a writer certificate. These survive step-down because both sides
  are append-only wire content.
- **live-leader plane** (`leader_frontier`/`frame_leader`): while the
  header's tenure IS live, frames agree with the leader's (tenure-frozen)
  map and sit below its durable frontier.
- **gate (regime) plane** (`gate_*`): a gate-open node's map/attribution is
  consistent with its regime's stream — the receiver-side half, keyed on
  the `dataTerm` handle; covers the LC2b/B1 admitted over-approx state
  (carried-open lagged candidate + truncating non-adopt reconcile) because
  the clauses are per-entry/below-frontier and truncation only shrinks.
- **report plane** (`report_*`): the LC3 deliverable proper — the
  `grant_state` mirror over `CMsg.report`: a term-`T` report in flight pins
  its sender's send-time regime facts (`termAt`-only content, LB2b
  narrowing) for as long as the sender's data plane remains in regime `T`
  (`dataTerm u = T`); every escape (strict adoption) strictly raises the
  monotone handle past `T`, which is the recorded "moved past" tag LC4's
  credential chain consumes.

`frames_current_authored` (the FIXED FCA statement, defined in
`LeaderCompleteness.lean`) is the bundle's `fca` clause read off
`reachable_provInv` — discharged unconditionally at the end of this file. -/

namespace Uc2

namespace TermMap

/-- Once the fold's accumulator sits at-or-below every remaining term, it
never decreases (ascending suffix). -/
theorem le_foldl_termAt {p : Nat} :
    ∀ (l : TermMap) (init : Nat), Ascending l →
      (∀ x ∈ l, init ≤ x.1) →
      init ≤ l.foldl (fun acc e => if e.2 ≤ p then e.1 else acc) init
  | [], _, _, _ => Nat.le_refl _
  | b :: s, init, hwf, h => by
    rw [List.foldl_cons]
    by_cases hb : b.2 ≤ p
    · rw [if_pos hb]
      refine Nat.le_trans (h b List.mem_cons_self)
        (le_foldl_termAt s b.1 hwf.tail ?_)
      intro x hx
      obtain ⟨j, hj⟩ := List.mem_iff_getElem?.mp hx
      exact Nat.le_of_lt (hwf.head_term_lt hj)
    · rw [if_neg hb]
      exact le_foldl_termAt s init hwf.tail
        fun x hx => h x (List.mem_cons_of_mem b hx)

/-- A member entry whose base covers `p` bounds `termAt` from below. -/
theorem mem_le_foldl_termAt {p : Nat} :
    ∀ (l : TermMap) (init : Nat), Ascending l → ∀ {e : Nat × Nat}, e ∈ l →
      e.2 ≤ p → e.1 ≤ l.foldl (fun acc e => if e.2 ≤ p then e.1 else acc) init
  | [], _, _, _, he, _ => absurd he List.not_mem_nil
  | b :: s, init, hwf, e, he, hep => by
    rw [List.foldl_cons]
    rcases List.mem_cons.mp he with rfl | hes
    · rw [if_pos hep]
      refine le_foldl_termAt s e.1 hwf.tail ?_
      intro x hx
      obtain ⟨j, hj⟩ := List.mem_iff_getElem?.mp hx
      exact Nat.le_of_lt (hwf.head_term_lt hj)
    · exact mem_le_foldl_termAt s _ hwf.tail hes hep

/-- **Lower bound**: any member entry with base ≤ `p` bounds `termAt m p`. -/
theorem le_termAt {m : TermMap} (hwf : Ascending m) {e : Nat × Nat}
    (he : e ∈ m) (hep : e.2 ≤ p) : e.1 ≤ termAt m p :=
  mem_le_foldl_termAt m 0 hwf he hep

/-- Fold upper bound — no ascent needed: the result is `init` or a term. -/
theorem foldl_termAt_le {p b : Nat} :
    ∀ (l : TermMap) (init : Nat), init ≤ b → (∀ x ∈ l, x.1 ≤ b) →
      l.foldl (fun acc e => if e.2 ≤ p then e.1 else acc) init ≤ b
  | [], _, hi, _ => hi
  | c :: s, init, hi, h => by
    rw [List.foldl_cons]
    refine foldl_termAt_le s _ ?_ fun x hx => h x (List.mem_cons_of_mem c hx)
    split
    · exact h c List.mem_cons_self
    · exact hi

/-- Every member's term is bounded by the frontier term. -/
theorem term_le_lastTermOf : ∀ {m : TermMap}, Ascending m →
    ∀ {e : Nat × Nat}, e ∈ m → e.1 ≤ Data.lastTermOf m
  | [], _, _, he => absurd he List.not_mem_nil
  | [a], _, e, he => by
    rcases List.mem_cons.mp he with rfl | h
    · simp [Data.lastTermOf]
    · cases h
  | a :: b :: t, hwf, e, he => by
    have hlast : Data.lastTermOf (a :: b :: t) = Data.lastTermOf (b :: t) := by
      simp [Data.lastTermOf, List.getLast?_cons_cons]
    rw [hlast]
    rcases List.mem_cons.mp he with rfl | het
    · exact Nat.le_trans (Nat.le_of_lt hwf.1)
        (term_le_lastTermOf hwf.2.2 List.mem_cons_self)
    · exact term_le_lastTermOf hwf.2.2 het

/-- `termAt` never exceeds the frontier term. -/
theorem termAt_le_lastTermOf {m : TermMap} (hwf : Ascending m) (p : Nat) :
    termAt m p ≤ Data.lastTermOf m := by
  cases m with
  | nil => exact Nat.le_refl 0
  | cons a t =>
    exact foldl_termAt_le (a :: t) 0 (Nat.zero_le _)
      fun x hx => term_le_lastTermOf hwf hx

/-- Past the last entry's base, `termAt` IS the frontier term. -/
theorem termAt_of_last_base_le {m : TermMap} (hwf : Ascending m)
    {l : Nat × Nat} (hl : m.getLast? = some l) {p : Nat} (hp : l.2 ≤ p) :
    termAt m p = l.1 := by
  refine Nat.le_antisymm ?_ (le_termAt hwf (List.mem_of_getLast? hl) hp)
  have := termAt_le_lastTermOf hwf p
  rwa [show Data.lastTermOf m = l.1 by simp [Data.lastTermOf, hl]] at this

/-- Paired fold monotonicity in the position. -/
theorem foldl_termAt_mono {p q : Nat} (hpq : p ≤ q) :
    ∀ (l : TermMap) (ip iq : Nat), Ascending l → ip ≤ iq →
      (∀ x ∈ l, ip ≤ x.1) →
      l.foldl (fun acc e => if e.2 ≤ p then e.1 else acc) ip ≤
        l.foldl (fun acc e => if e.2 ≤ q then e.1 else acc) iq
  | [], _, _, _, hle, _ => hle
  | b :: s, ip, iq, hwf, hle, h => by
    rw [List.foldl_cons, List.foldl_cons]
    have hs : ∀ x ∈ s, b.1 ≤ x.1 := by
      intro x hx
      obtain ⟨j, hj⟩ := List.mem_iff_getElem?.mp hx
      exact Nat.le_of_lt (hwf.head_term_lt hj)
    by_cases hbp : b.2 ≤ p
    · rw [if_pos hbp, if_pos (Nat.le_trans hbp hpq)]
      exact foldl_termAt_mono hpq s b.1 b.1 hwf.tail (Nat.le_refl _) hs
    · rw [if_neg hbp]
      by_cases hbq : b.2 ≤ q
      · rw [if_pos hbq]
        exact foldl_termAt_mono hpq s ip b.1 hwf.tail
          (h b List.mem_cons_self)
          fun x hx => h x (List.mem_cons_of_mem b hx)
      · rw [if_neg hbq]
        exact foldl_termAt_mono hpq s ip iq hwf.tail hle
          fun x hx => h x (List.mem_cons_of_mem b hx)

/-- **Monotonicity**: `termAt` is monotone in the position (ascending map). -/
theorem termAt_mono {m : TermMap} (hwf : Ascending m) {p q : Nat}
    (hpq : p ≤ q) : termAt m p ≤ termAt m q :=
  foldl_termAt_mono hpq m 0 0 hwf (Nat.le_refl 0) fun _ _ => Nat.zero_le _

end TermMap

/-- The leader-side boundary clamp: `validUpTo` never exceeds the leader
map's first beyond-prefix base (in BOTH duplicate arms — `min` when it
undercuts the durable, transitively through `v1 ≤ d` otherwise). -/
theorem reconcile_ok_le_leader_k {own : TermMap} {d : Nat} {l0 : Nat × Nat}
    {ls : TermMap} {o : Outcome}
    (h : reconcile own d (l0 :: ls) = .ok o) :
    ∀ f, (l0 :: ls)[commonPrefixLen own (l0 :: ls)]? = some f →
      o.validUpTo ≤ f.2 := by
  intro f hf
  have hc := reconcile_ok_clamped h
  obtain ⟨v, mm⟩ := o
  dsimp only [Outcome.validUpTo]
  rcases ho : own[commonPrefixLen own (l0 :: ls)]? with _ | e <;>
    simp only [reconcile.reconcileClamped, ho, hf, ReconcileResult.ok.injEq,
      Outcome.mk.injEq] at hc <;>
    obtain ⟨hv, hm⟩ := hc <;>
    subst hv <;>
    simp only [Nat.min_def] <;>
    split_ifs <;> omega

namespace Data

/-! ### Certificate transport, per data step

`Cert w t ℓ` is stable under EVERY data-plane step (the quorum rides the
append-only `sent`; the pin survives because every vote-relevant write either
strictly bumps the term or freezes the recorded current-term vote; a fresh
foreign grant at a pinned term is blocked by the vote discipline's
idempotency). `DInv.cert`'s preservation re-proves this inline per case;
here it is factored once, so the commit-layer bundle can carry certificates
through `step_project`. -/

theorem cert_dstep {n : Nat} {dw dw' : World n} {t : Nat} {ℓ : Fin n}
    (hs : Step dw dw') (hc : Cert dw t ℓ) : Cert dw' t ℓ := by
  cases hs with
  | startElection i hrole =>
    refine hc.transport (fun m hm => List.mem_append_left _ hm) ?_ ?_
    · intro c hcm
      rcases List.mem_append.mp hcm with h | h
      · exact .inl h
      · exact absurd h (by simp)
    · rcases eq_or_ne ℓ i with rfl | hne
      · simp only [Function.update_self]
        left
        show t < (dw.nodes ℓ).pn.currentTerm + 1
        rcases hc.pinned with hlt | ⟨heq, -, -⟩ <;> omega
      · simp only [Function.update_of_ne hne]
        exact hc.pinned
  | deliverRequestVote j c nt clt cd hmsg hterm =>
    refine hc.transport (fun m hm => List.mem_append_left _ hm) ?_ ?_
    · intro c' hcm
      rcases List.mem_append.mp hcm with h | h
      · exact .inl h
      · rw [List.mem_singleton, Uc2.Msg.vote.injEq] at h
        obtain ⟨rfl, rfl, rfl, hg⟩ := h
        -- a fresh ℓ-grant AT the pinned term: the pin forces the recorded
        -- self-vote, and idempotency pins the candidate to ℓ itself.
        rcases hc.pinned with hlt | ⟨heq, hvf, -⟩
        · omega
        · exact .inr ((recv_voted_current (dw.nodes ℓ).pn c' ℓ t clt cd heq
            (by rw [heq]; exact hvf)).2 hg.symm).symm
    · rcases eq_or_ne ℓ j with rfl | hne
      · simp only [Function.update_self]
        by_cases hadopt : (dw.nodes ℓ).pn.currentTerm < nt
        · left
          rw [show (((dw.nodes ℓ).pn.recvRequestVote c nt clt cd).1).currentTerm
              = nt from recv_term _ _ _ _ _ hterm]
          rcases hc.pinned with hlt | ⟨heq, -, -⟩ <;> omega
        · rcases hc.pinned with hlt | ⟨heq, hvf, hnc⟩
          · left
            rw [show (((dw.nodes ℓ).pn.recvRequestVote c nt clt cd).1).currentTerm
                = nt from recv_term _ _ _ _ _ hterm]
            omega
          · rw [(recv_voted_current (dw.nodes ℓ).pn c ℓ nt clt cd (by omega)
              (by rw [heq]; exact hvf)).1]
            exact .inr ⟨heq, hvf, hnc⟩
      · simp only [Function.update_of_ne hne]
        exact hc.pinned
  | rejectStaleRequestVote j c nt clt cd hmsg hstale =>
    refine hc.transport (fun m hm => List.mem_append_left _ hm) ?_ hc.pinned
    intro c' hcm
    rcases List.mem_append.mp hcm with h | h
    · exact .inl h
    · rw [List.mem_singleton, Uc2.Msg.vote.injEq] at h
      obtain ⟨-, -, -, hg⟩ := h
      cases hg
  | deliverVote i v tv hmsg hrole hterm =>
    refine hc.transport (fun m hm => hm) (fun c h => .inl h) ?_
    rcases eq_or_ne ℓ i with rfl | hne
    · simp only [Function.update_self]
      rcases hc.pinned with hlt | ⟨heq, hvf, hnc⟩
      · exact .inl hlt
      · exact .inr ⟨heq, hvf, hnc⟩
    · simp only [Function.update_of_ne hne]
      exact hc.pinned
  | deliverVoteHigherTerm i v tv g hmsg hterm =>
    refine hc.transport (fun m hm => hm) (fun c h => .inl h) ?_
    rcases eq_or_ne ℓ i with rfl | hne
    · simp only [Function.update_self]
      left
      show t < ((dw.nodes ℓ).pn.adoptTerm tv).currentTerm
      have : ((dw.nodes ℓ).pn.adoptTerm tv).currentTerm = tv := rfl
      rw [this]
      rcases hc.pinned with hlt | ⟨heq, -, -⟩ <;> omega
    · simp only [Function.update_of_ne hne]
      exact hc.pinned
  | becomeLeader i hrole hquorum =>
    refine hc.transport (fun m hm => hm) (fun c h => .inl h) ?_
    rcases eq_or_ne ℓ i with rfl | hne
    · -- the pre-state writer is a CANDIDATE here, so its pin is the left arm.
      rcases hc.pinned with hlt | ⟨-, -, hnc⟩
      · simp only [Function.update_self]
        exact .inl hlt
      · exact absurd hrole hnc
    · simp only [Function.update_of_ne hne]
      exact hc.pinned
  | crashRestart i =>
    refine hc.transport (fun m hm => hm) (fun c h => .inl h) ?_
    rcases eq_or_ne ℓ i with rfl | hne
    · simp only [Function.update_self]
      rcases hc.pinned with hlt | ⟨heq, hvf, -⟩
      · exact .inl hlt
      · exact .inr ⟨heq, hvf, by show Role.follower ≠ .candidate; decide⟩
    · simp only [Function.update_of_ne hne]
      exact hc.pinned
  | leaderAppend i v hrole =>
    refine hc.transport (fun m hm => hm) (fun c h => .inl h) ?_
    rcases eq_or_ne ℓ i with rfl | hne
    · simp only [Function.update_self]
      rcases hc.pinned with hlt | ⟨heq, hvf, hnc⟩
      · exact .inl hlt
      · exact .inr ⟨heq, hvf, hnc⟩
    · simp only [Function.update_of_ne hne]
      exact hc.pinned
  | deliverReplicate j pos hdr tv v hmsg hpos hhdr =>
    refine hc.transport (fun m hm => hm) (fun c h => .inl h) ?_
    rcases eq_or_ne ℓ j with rfl | hne
    · simp only [Function.update_self]
      obtain ⟨hro, hct, hvf, -⟩ := recvReplicate_pn (dw.nodes ℓ) pos tv v
      rcases hc.pinned with hlt | ⟨heq, hv, hnc⟩
      · exact .inl (by rw [hct]; exact hlt)
      · exact .inr ⟨by rw [hct]; exact heq, by rw [hvf]; exact hv,
          by rw [hro]; exact hnc⟩
    · simp only [Function.update_of_ne hne]
      exact hc.pinned
  | serveTail i p tv v hrole hhist hp =>
    exact hc.transport (fun m hm => hm) (fun c h => .inl h) hc.pinned
  | shipTermMap i hrole =>
    exact hc.transport (fun m hm => hm) (fun c h => .inl h) hc.pinned
  | deliverTermMap j tg entries hmsg hterm =>
    refine hc.transport (fun m hm => hm) (fun c h => .inl h) ?_
    rcases eq_or_ne ℓ j with rfl | hne
    · simp only [Function.update_self]
      by_cases hadopt : (dw.nodes ℓ).pn.currentTerm < tg
      · left
        rw [(applyGossip_adopt (dw.nodes ℓ) entries hadopt).2]
        rcases hc.pinned with hlt | ⟨heq, -, -⟩ <;> omega
      · obtain ⟨hro, hct, hvf⟩ := applyGossip_no_adopt (dw.nodes ℓ) entries hadopt
        rcases hc.pinned with hlt | ⟨heq, hv, hnc⟩
        · exact .inl (by rw [hct]; exact hlt)
        · exact .inr ⟨by rw [hct]; exact heq, by rw [hvf]; exact hv,
            by rw [hro]; exact hnc⟩
    · simp only [Function.update_of_ne hne]
      exact hc.pinned

/-- Certificate transport along a data-step chain. -/
theorem cert_drtg {n : Nat} {dw dw' : World n} {t : Nat} {ℓ : Fin n}
    (hs : Relation.ReflTransGen Step dw dw') (hc : Cert dw t ℓ) :
    Cert dw' t ℓ := by
  induction hs with
  | refl => exact hc
  | tail _ hstep ih => exact cert_dstep hstep ih

end Data

namespace Cert

/-- **LC3's message-indexed provenance bundle** (module doc for the four
planes). All clauses are over the COMMIT-layer world; certificates are over
its data-plane projection. `fca` is the folded-in FCA discharge; the
`report_*` clauses are the LC4 deliverable (the `grant_state` mirror over
`CMsg.report`, `termAt`-only content, with the monotone `dataTerm` handle as
the recorded "moved past" tag). -/
structure ProvInv {n : Nat} (w : World n) : Prop where
  /-- A header-`hdr` frame's stamp is what ANY header-`hdr` gossip
  attributes to its position (both sides append-only ⇒ survives step-down —
  the serveTail residual's carrier). -/
  frame_gossip : ∀ p hdr t v es, Data.Frame.replicate p hdr t v ∈ w.dsent →
      Data.Frame.gossip hdr es ∈ w.dsent → TermMap.termAt es p = t
  /-- Same-header stamps are position-monotone (one tenure = one frozen
  ascending attribution). -/
  frame_mono : ∀ p₁ p₂ hdr t₁ t₂ v₁ v₂,
      Data.Frame.replicate p₁ hdr t₁ v₁ ∈ w.dsent →
      Data.Frame.replicate p₂ hdr t₂ v₂ ∈ w.dsent → p₁ ≤ p₂ → t₁ ≤ t₂
  /-- One stamp per (header, position): the tenure map is frozen. -/
  frame_uniq : ∀ p hdr t₁ t₂ v₁ v₂,
      Data.Frame.replicate p hdr t₁ v₁ ∈ w.dsent →
      Data.Frame.replicate p hdr t₂ v₂ ∈ w.dsent → t₁ = t₂
  /-- No header without a writer certificate (blocks a later candidate from
  RE-winning a term that already has wire content). -/
  frame_cert : ∀ p hdr t v, Data.Frame.replicate p hdr t v ∈ w.dsent →
      ∃ ℓ, Data.Cert w.project hdr ℓ
  /-- Only a candidate's handle lags (`StartElection` stores nothing;
  every other `currentTerm` writer re-keys the handle in lockstep). -/
  role_dt : ∀ j : Fin n, (w.nodes j).pn.role ≠ .candidate →
      (w.nodes j).dataTerm = (w.nodes j).pn.currentTerm
  /-- Held bytes sit strictly below the durable frontier. -/
  hist_bound : ∀ j : Fin n, ∀ p tv, (w.nodes j).hist p = some tv →
      p < (w.nodes j).pn.durable
  /-- A closed gate strictly separates the map frontier from the regime —
  the gate closes only on strict adoptions, which outrun the map. -/
  closed_lag : ∀ j : Fin n, (w.nodes j).reconciled = false →
      Data.lastTermOf (w.nodes j).dn.termMap < (w.nodes j).dataTerm
  /-- The FCA discharge (statement FIXED in `LeaderCompleteness.lean`). -/
  fca : FramesCurrentAuthored w
  /-- A live tenure's frames sit below the leader's (monotone-in-tenure)
  durable frontier. -/
  leader_frontier : ∀ ℓ : Fin n, (w.nodes ℓ).pn.role = .leader →
      ∀ p t v,
      Data.Frame.replicate p ((w.nodes ℓ).pn.currentTerm) t v ∈ w.dsent →
      p < (w.nodes ℓ).pn.durable
  /-- A live tenure's frames agree with the leader's (tenure-frozen) map. -/
  frame_leader : ∀ p hdr t v, Data.Frame.replicate p hdr t v ∈ w.dsent →
      ∀ i : Fin n, (w.nodes i).pn.role = .leader →
      (w.nodes i).pn.currentTerm = hdr →
      TermMap.termAt (w.nodes i).dn.termMap p = t
  /-- A gate-open regime is a certified term (or genesis 0). -/
  gate_cert : ∀ j : Fin n, (w.nodes j).reconciled = true →
      1 ≤ (w.nodes j).dataTerm →
      ∃ ℓ, Data.Cert w.project ((w.nodes j).dataTerm) ℓ
  /-- A gate-open node's map entries never out-term the regime stream at
  covered positions — per-entry, so truncation (which only removes entries)
  preserves it through the LC2b/B1 admitted state. -/
  gate_map_frame : ∀ j : Fin n, (w.nodes j).reconciled = true →
      ∀ e ∈ (w.nodes j).dn.termMap, ∀ p t v,
      Data.Frame.replicate p ((w.nodes j).dataTerm) t v ∈ w.dsent →
      e.2 ≤ p → e.1 ≤ t
  /-- Entry-wise sync against the LIVE regime leader's map. -/
  gate_leader : ∀ j ℓ : Fin n, (w.nodes j).reconciled = true →
      (w.nodes ℓ).pn.role = .leader →
      (w.nodes ℓ).pn.currentTerm = (w.nodes j).dataTerm →
      ∀ e ∈ (w.nodes j).dn.termMap,
        e.1 ≤ TermMap.termAt (w.nodes ℓ).dn.termMap e.2
  /-- Below its frontier, a gate-open node ATTRIBUTES like the live regime
  leader. -/
  gate_leader_eq : ∀ j ℓ : Fin n, (w.nodes j).reconciled = true →
      (w.nodes ℓ).pn.role = .leader →
      (w.nodes ℓ).pn.currentTerm = (w.nodes j).dataTerm →
      ∀ p, p < (w.nodes j).pn.durable →
        TermMap.termAt (w.nodes j).dn.termMap p
          = TermMap.termAt (w.nodes ℓ).dn.termMap p
  /-- Below its frontier, a gate-open node attributes exactly the regime
  stream's stamps (message-indexed — the leader may be gone). -/
  gate_frames_eq : ∀ j : Fin n, (w.nodes j).reconciled = true →
      ∀ p t v,
      Data.Frame.replicate p ((w.nodes j).dataTerm) t v ∈ w.dsent →
      p < (w.nodes j).pn.durable →
      TermMap.termAt (w.nodes j).dn.termMap p = t
  /-- A gate-open node never outruns its live regime leader's frontier. -/
  gate_durable : ∀ j ℓ : Fin n, (w.nodes j).reconciled = true →
      (w.nodes ℓ).pn.role = .leader →
      (w.nodes ℓ).pn.currentTerm = (w.nodes j).dataTerm →
      (w.nodes j).pn.durable ≤ (w.nodes ℓ).pn.durable
  /-- A term-`T` report pins its sender's (monotone) handle at ≥ `T`: the
  strict-adoption escape is RECORDED in the handle — `grant_state`'s
  "moved strictly past" tag, data-plane edition. -/
  report_dt : ∀ (u : Fin n) (T d : Nat), CMsg.report u T d ∈ w.csent →
      T ≤ (w.nodes u).dataTerm
  /-- A report's term is certified (reports are sent gate-open). -/
  report_cert : ∀ (u : Fin n) (T d : Nat), CMsg.report u T d ∈ w.csent →
      1 ≤ T → ∃ ℓ, Data.Cert w.project T ℓ
  /-- THE LC3 clause, frame form: while a term-`T` reporter's data plane
  remains in regime `T`, its below-frontier attribution IS the `T`-stream's
  (send-time `termAt` facts, transported — survives crash/closed-gate
  windows that `gate_frames_eq` cannot see). -/
  report_frames : ∀ (u : Fin n) (T d : Nat), CMsg.report u T d ∈ w.csent →
      (w.nodes u).dataTerm = T →
      ∀ p t v, Data.Frame.replicate p T t v ∈ w.dsent →
      p < (w.nodes u).pn.durable →
      TermMap.termAt (w.nodes u).dn.termMap p = t
  /-- THE LC3 clause, live-leader form. -/
  report_leader_eq : ∀ (u : Fin n) (T d : Nat), CMsg.report u T d ∈ w.csent →
      (w.nodes u).dataTerm = T →
      ∀ ℓ : Fin n, (w.nodes ℓ).pn.role = .leader →
      (w.nodes ℓ).pn.currentTerm = T →
      ∀ p, p < (w.nodes u).pn.durable →
        TermMap.termAt (w.nodes u).dn.termMap p
          = TermMap.termAt (w.nodes ℓ).dn.termMap p
  /-- A still-in-regime reporter sits at or below the live `T`-leader's
  frontier. -/
  report_durable : ∀ (u : Fin n) (T d : Nat), CMsg.report u T d ∈ w.csent →
      (w.nodes u).dataTerm = T →
      ∀ ℓ : Fin n, (w.nodes ℓ).pn.role = .leader →
      (w.nodes ℓ).pn.currentTerm = T →
      (w.nodes u).pn.durable ≤ (w.nodes ℓ).pn.durable

/-- Certificate transport across one commit-layer step (via the data-plane
projection: commit-plane steps project to zero data steps). -/
private theorem cert_carry {n : Nat} {w w' : World n} (hs : Step w w')
    {t : Nat} {ℓ : Fin n} (hc : Data.Cert w.project t ℓ) :
    Data.Cert w'.project t ℓ :=
  Data.cert_drtg (step_project hs) hc

/-- `prunePush` never changes attribution strictly below the push point:
the dropped phantoms and the pushed entry all sit at base `d > p`. -/
private theorem termAt_prunePush {m : TermMap} {c d p : Nat} (hp : p < d) :
    TermMap.termAt (Data.prunePush m c d) p = TermMap.termAt m p := by
  have hsplit : m = (m.reverse.dropWhile (fun e => e.2 == d)).reverse
      ++ (m.reverse.takeWhile (fun e => e.2 == d)).reverse := by
    conv_lhs => rw [← List.reverse_reverse m,
      ← List.takeWhile_append_dropWhile (p := fun e => e.2 == d)
        (l := m.reverse)]
    rw [List.reverse_append]
  have htake : ∀ e ∈ (m.reverse.takeWhile (fun e => e.2 == d)).reverse,
      p < e.2 := by
    intro e he
    rw [List.mem_reverse] at he
    have h2 := List.mem_takeWhile_imp he
    have he2 : e.2 = d := by simpa using h2
    omega
  show TermMap.termAt
    ((m.reverse.dropWhile (fun e => e.2 == d)).reverse ++ [(c, d)]) p
    = TermMap.termAt m p
  rw [TermMap.termAt_append_high (by
    intro e he
    rw [List.mem_singleton] at he
    subst he
    exact hp)]
  conv_rhs => rw [hsplit]
  rw [TermMap.termAt_append_high htake]

private theorem provinv_init (n : Nat) : ProvInv (World.init n) where
  frame_gossip := by intro p hdr t v es hf; simp [World.init] at hf
  frame_mono := by intro p₁ p₂ hdr t₁ t₂ v₁ v₂ h1; simp [World.init] at h1
  frame_uniq := by intro p hdr t₁ t₂ v₁ v₂ h1; simp [World.init] at h1
  frame_cert := by intro p hdr t v hf; simp [World.init] at hf
  role_dt := by intro j _; rfl
  hist_bound := by intro j p tv hh; simp [World.init, Node.hist] at hh
  closed_lag := by intro j hrec; simp [World.init] at hrec
  fca := by intro j p t v hh; simp [World.init, Node.hist] at hh
  leader_frontier := by intro ℓ hrole; simp [World.init, Node.pn] at hrole
  frame_leader := by intro p hdr t v hf; simp [World.init] at hf
  gate_cert := by intro j _ hdt; simp [World.init, Node.dataTerm] at hdt
  gate_map_frame := by intro j _ e he; simp [World.init] at he
  gate_leader := by intro j ℓ _ hrole; simp [World.init, Node.pn] at hrole
  gate_leader_eq := by intro j ℓ _ hrole; simp [World.init, Node.pn] at hrole
  gate_frames_eq := by intro j _ p t v hf; simp [World.init] at hf
  gate_durable := by intro j ℓ _ hrole; simp [World.init, Node.pn] at hrole
  report_dt := by intro u T d hr; simp [World.init] at hr
  report_cert := by intro u T d hr; simp [World.init] at hr
  report_frames := by intro u T d hr; simp [World.init] at hr
  report_leader_eq := by intro u T d hr; simp [World.init] at hr
  report_durable := by intro u T d hr; simp [World.init] at hr

/-- Generic transport: a step that leaves every clause-supporting node field
and both relevant wires unchanged preserves the whole bundle (certificates
ride `cert_carry`). Covers the commit-plane stutters and the tally-only vote
steps. -/
private theorem provinv_transport {n : Nat} {w w' : World n}
    (hs : Step w w') (h : ProvInv w)
    (hmap : ∀ k, (w'.nodes k).dn.termMap = (w.nodes k).dn.termMap)
    (hhist : ∀ k, (w'.nodes k).hist = (w.nodes k).hist)
    (hdur : ∀ k, (w'.nodes k).pn.durable = (w.nodes k).pn.durable)
    (hdt : ∀ k, (w'.nodes k).dataTerm = (w.nodes k).dataTerm)
    (hrole : ∀ k, (w'.nodes k).pn.role = (w.nodes k).pn.role)
    (hct : ∀ k, (w'.nodes k).pn.currentTerm = (w.nodes k).pn.currentTerm)
    (hrec : ∀ k, (w'.nodes k).reconciled = (w.nodes k).reconciled)
    (hds : w'.dsent = w.dsent)
    (hcs : w'.csent = w.csent) : ProvInv w' := by
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_,
    ?_, ?_, ?_, ?_, ?_⟩
  · intro p hdr t v es hf hg
    rw [hds] at hf hg
    exact h.frame_gossip p hdr t v es hf hg
  · intro p₁ p₂ hdr t₁ t₂ v₁ v₂ h1 h2 hle
    rw [hds] at h1 h2
    exact h.frame_mono p₁ p₂ hdr t₁ t₂ v₁ v₂ h1 h2 hle
  · intro p hdr t₁ t₂ v₁ v₂ h1 h2
    rw [hds] at h1 h2
    exact h.frame_uniq p hdr t₁ t₂ v₁ v₂ h1 h2
  · intro p hdr t v hf
    rw [hds] at hf
    obtain ⟨ℓ, hc⟩ := h.frame_cert p hdr t v hf
    exact ⟨ℓ, cert_carry hs hc⟩
  · intro j hnc
    rw [hrole] at hnc
    rw [hdt, hct]
    exact h.role_dt j hnc
  · intro j p tv hh
    rw [hhist] at hh
    rw [hdur]
    exact h.hist_bound j p tv hh
  · intro j hr
    rw [hrec] at hr
    rw [hmap, hdt]
    exact h.closed_lag j hr
  · intro j p t v hh
    rw [hhist] at hh
    rw [hmap]
    exact h.fca j p t v hh
  · intro ℓ hrl p t v hf
    rw [hrole] at hrl
    rw [hds, hct] at hf
    rw [hdur]
    exact h.leader_frontier ℓ hrl p t v hf
  · intro p hdr t v hf i hrl hcti
    rw [hds] at hf
    rw [hrole] at hrl
    rw [hct] at hcti
    rw [hmap]
    exact h.frame_leader p hdr t v hf i hrl hcti
  · intro j hr hdt1
    rw [hrec] at hr
    rw [hdt] at hdt1 ⊢
    obtain ⟨ℓ, hc⟩ := h.gate_cert j hr hdt1
    exact ⟨ℓ, cert_carry hs hc⟩
  · intro j hr e he p t v hf hep
    rw [hrec] at hr
    rw [hmap] at he
    rw [hds, hdt] at hf
    exact h.gate_map_frame j hr e he p t v hf hep
  · intro j ℓ hr hrl hctl e he
    rw [hrec] at hr
    rw [hrole] at hrl
    rw [hct, hdt] at hctl
    rw [hmap j] at he
    rw [hmap ℓ]
    exact h.gate_leader j ℓ hr hrl hctl e he
  · intro j ℓ hr hrl hctl p hp
    rw [hrec] at hr
    rw [hrole] at hrl
    rw [hct, hdt] at hctl
    rw [hdur] at hp
    rw [hmap j, hmap ℓ]
    exact h.gate_leader_eq j ℓ hr hrl hctl p hp
  · intro j hr p t v hf hp
    rw [hrec] at hr
    rw [hds, hdt] at hf
    rw [hdur] at hp
    rw [hmap]
    exact h.gate_frames_eq j hr p t v hf hp
  · intro j ℓ hr hrl hctl
    rw [hrec] at hr
    rw [hrole] at hrl
    rw [hct, hdt] at hctl
    rw [hdur j, hdur ℓ]
    exact h.gate_durable j ℓ hr hrl hctl
  · intro u T d hrp
    rw [hcs] at hrp
    rw [hdt]
    exact h.report_dt u T d hrp
  · intro u T d hrp hT
    rw [hcs] at hrp
    obtain ⟨ℓ, hc⟩ := h.report_cert u T d hrp hT
    exact ⟨ℓ, cert_carry hs hc⟩
  · intro u T d hrp hdtu p t v hf hp
    rw [hcs] at hrp
    rw [hdt] at hdtu
    rw [hds] at hf
    rw [hdur] at hp
    rw [hmap]
    exact h.report_frames u T d hrp hdtu p t v hf hp
  · intro u T d hrp hdtu ℓ hrl hctl p hp
    rw [hcs] at hrp
    rw [hdt] at hdtu
    rw [hrole] at hrl
    rw [hct] at hctl
    rw [hdur] at hp
    rw [hmap u, hmap ℓ]
    exact h.report_leader_eq u T d hrp hdtu ℓ hrl hctl p hp
  · intro u T d hrp hdtu ℓ hrl hctl
    rw [hcs] at hrp
    rw [hdt] at hdtu
    rw [hrole] at hrl
    rw [hct] at hctl
    rw [hdur u, hdur ℓ]
    exact h.report_durable u T d hrp hdtu ℓ hrl hctl

/-- Election-slice transport: one node's ELECTION state changed (votes,
role, term, handle, gate), its DATA slice (map/hist/durable) untouched, and
the data/commit wires untouched. Covers `startElection`, both
`deliverRequestVote` arms, `deliverVoteHigherTerm`, and `crashRestart`:

- `hgate`: a still/newly-open gate means the regime did NOT move and the
  gate was ALREADY open (adoptions close; `crashRestart`'s boot-open pins
  the regime via `closed_lag` at the call site);
- `hclosed`: a closed gate lags the (possibly raised) handle;
- `hldr`: a node still LEADING was untouched (only the same-term non-adopt
  `deliverRequestVote` at a leader — frozen by the vote discipline). -/
private theorem provinv_election {n : Nat} {w w' : World n} (hs : Step w w')
    (h : ProvInv w) {i : Fin n} {C : Node n}
    (hn : w'.nodes = Function.update w.nodes i C)
    (hds : w'.dsent = w.dsent) (hcs : w'.csent = w.csent)
    (hmap : C.dn.termMap = (w.nodes i).dn.termMap)
    (hhist : C.dn.hist = (w.nodes i).dn.hist)
    (hdur : C.dn.pn.durable = (w.nodes i).pn.durable)
    (hrdt : C.dn.pn.role ≠ .candidate → C.dn.dataTerm = C.dn.pn.currentTerm)
    (hdtge : (w.nodes i).dataTerm ≤ C.dn.dataTerm)
    (hclosed : C.reconciled = false →
      Data.lastTermOf (w.nodes i).dn.termMap < C.dn.dataTerm)
    (hgate : C.reconciled = true →
      C.dn.dataTerm = (w.nodes i).dataTerm ∧ (w.nodes i).reconciled = true)
    (hldr : C.dn.pn.role = .leader →
      C.dn = (w.nodes i).dn ∧ C.reconciled = (w.nodes i).reconciled ∧
      (w.nodes i).pn.role = .leader) : ProvInv w' := by
  have hpn : ∀ k, (w'.nodes k).dn
      = if k = i then C.dn else (w.nodes k).dn := by
    intro k
    rw [hn]
    rcases eq_or_ne k i with rfl | hk
    · rw [Function.update_self, if_pos rfl]
    · rw [Function.update_of_ne hk, if_neg hk]
  have hrc : ∀ k, (w'.nodes k).reconciled
      = if k = i then C.reconciled else (w.nodes k).reconciled := by
    intro k
    rw [hn]
    rcases eq_or_ne k i with rfl | hk
    · rw [Function.update_self, if_pos rfl]
    · rw [Function.update_of_ne hk, if_neg hk]
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_,
    ?_, ?_, ?_, ?_, ?_⟩
  · intro p hdr t v es hf hg
    rw [hds] at hf hg
    exact h.frame_gossip p hdr t v es hf hg
  · intro p₁ p₂ hdr t₁ t₂ v₁ v₂ h1 h2 hle
    rw [hds] at h1 h2
    exact h.frame_mono p₁ p₂ hdr t₁ t₂ v₁ v₂ h1 h2 hle
  · intro p hdr t₁ t₂ v₁ v₂ h1 h2
    rw [hds] at h1 h2
    exact h.frame_uniq p hdr t₁ t₂ v₁ v₂ h1 h2
  · intro p hdr t v hf
    rw [hds] at hf
    obtain ⟨ℓ, hc⟩ := h.frame_cert p hdr t v hf
    exact ⟨ℓ, cert_carry hs hc⟩
  · intro k hnc
    have hnc' : (w'.nodes k).dn.pn.role ≠ .candidate := hnc
    show (w'.nodes k).dn.dataTerm = (w'.nodes k).dn.pn.currentTerm
    rw [hpn] at hnc' ⊢
    rcases eq_or_ne k i with rfl | hk
    · rw [if_pos rfl] at hnc' ⊢
      exact hrdt hnc'
    · rw [if_neg hk] at hnc' ⊢
      exact h.role_dt k hnc'
  · intro k p tv hh
    show p < (w'.nodes k).dn.pn.durable
    have hh' : (w'.nodes k).dn.hist p = some tv := hh
    rw [hpn] at hh' ⊢
    rcases eq_or_ne k i with rfl | hk
    · rw [if_pos rfl] at hh' ⊢
      rw [hhist] at hh'
      rw [hdur]
      exact h.hist_bound k p tv hh'
    · rw [if_neg hk] at hh' ⊢
      exact h.hist_bound k p tv hh'
  · intro k hr
    show Data.lastTermOf (w'.nodes k).dn.termMap < (w'.nodes k).dn.dataTerm
    rw [hrc] at hr
    rw [hpn]
    rcases eq_or_ne k i with rfl | hk
    · rw [if_pos rfl] at hr ⊢
      rw [hmap]
      exact hclosed hr
    · rw [if_neg hk] at hr ⊢
      exact h.closed_lag k hr
  · intro k p t v hh
    show TermMap.termAt (w'.nodes k).dn.termMap p = t
    have hh' : (w'.nodes k).dn.hist p = some (t, v) := hh
    rw [hpn] at hh' ⊢
    rcases eq_or_ne k i with rfl | hk
    · rw [if_pos rfl] at hh' ⊢
      rw [hhist] at hh'
      rw [hmap]
      exact h.fca k p t v hh'
    · rw [if_neg hk] at hh' ⊢
      exact h.fca k p t v hh'
  · intro ℓ hrl p t v hf
    have hrl' : (w'.nodes ℓ).dn.pn.role = .leader := hrl
    have hf' : Data.Frame.replicate p ((w'.nodes ℓ).dn.pn.currentTerm) t v
        ∈ w.dsent := by rw [← hds]; exact hf
    show p < (w'.nodes ℓ).dn.pn.durable
    rw [hpn] at hrl' hf' ⊢
    rcases eq_or_ne ℓ i with rfl | hk
    · rw [if_pos rfl] at hrl' hf' ⊢
      obtain ⟨hdn, -, hprl⟩ := hldr hrl'
      rw [hdn] at hf' ⊢
      exact h.leader_frontier ℓ hprl p t v hf'
    · rw [if_neg hk] at hrl' hf' ⊢
      exact h.leader_frontier ℓ hrl' p t v hf'
  · intro p hdr t v hf i' hrl hcti
    rw [hds] at hf
    have hrl' : (w'.nodes i').dn.pn.role = .leader := hrl
    have hcti' : (w'.nodes i').dn.pn.currentTerm = hdr := hcti
    show TermMap.termAt (w'.nodes i').dn.termMap p = t
    rw [hpn] at hrl' hcti' ⊢
    rcases eq_or_ne i' i with rfl | hk
    · rw [if_pos rfl] at hrl' hcti' ⊢
      obtain ⟨hdn, -, hprl⟩ := hldr hrl'
      rw [hdn] at hcti' ⊢
      exact h.frame_leader p hdr t v hf i' hprl hcti'
    · rw [if_neg hk] at hrl' hcti' ⊢
      exact h.frame_leader p hdr t v hf i' hrl' hcti'
  · intro k hr h1
    rw [hrc] at hr
    have h1' : 1 ≤ (w'.nodes k).dn.dataTerm := h1
    have hgoal : ∃ ℓ, Data.Cert w.project ((w'.nodes k).dn.dataTerm) ℓ := by
      rw [hpn] at h1' ⊢
      rcases eq_or_ne k i with rfl | hk
      · rw [if_pos rfl] at h1' hr ⊢
        obtain ⟨hdt, hpre⟩ := hgate hr
        rw [hdt] at h1' ⊢
        exact h.gate_cert k hpre h1'
      · rw [if_neg hk] at h1' hr ⊢
        exact h.gate_cert k hr h1'
    obtain ⟨ℓ, hc⟩ := hgoal
    exact ⟨ℓ, cert_carry hs hc⟩
  · intro k hr e he p t v hf hep
    rw [hrc] at hr
    have he' : e ∈ (w'.nodes k).dn.termMap := he
    have hf' : Data.Frame.replicate p ((w'.nodes k).dn.dataTerm) t v
        ∈ w.dsent := by rw [← hds]; exact hf
    rw [hpn] at he' hf'
    rcases eq_or_ne k i with rfl | hk
    · rw [if_pos rfl] at he' hf' hr
      obtain ⟨hdt, hpre⟩ := hgate hr
      rw [hmap] at he'
      rw [hdt] at hf'
      exact h.gate_map_frame k hpre e he' p t v hf' hep
    · rw [if_neg hk] at he' hf' hr
      exact h.gate_map_frame k hr e he' p t v hf' hep
  · intro k ℓ hr hrl hctl e he
    rw [hrc] at hr
    have hrl' : (w'.nodes ℓ).dn.pn.role = .leader := hrl
    have hctl' : (w'.nodes ℓ).dn.pn.currentTerm = (w'.nodes k).dn.dataTerm :=
      hctl
    have he' : e ∈ (w'.nodes k).dn.termMap := he
    show e.1 ≤ TermMap.termAt (w'.nodes ℓ).dn.termMap e.2
    rw [hpn ℓ] at hrl'
    rw [hpn ℓ, hpn k] at hctl'
    rw [hpn k] at he'
    rw [hpn ℓ]
    rcases eq_or_ne k i with rfl | hk
    · rw [if_pos rfl] at hr he' hctl'
      obtain ⟨hdt, hpre⟩ := hgate hr
      rw [hmap] at he'
      rw [hdt] at hctl'
      rcases eq_or_ne ℓ k with rfl | hkl
      · rw [if_pos rfl] at hrl' hctl' ⊢
        obtain ⟨hdn, -, hprl⟩ := hldr hrl'
        rw [hdn] at hctl' ⊢
        exact h.gate_leader ℓ ℓ hpre hprl hctl' e he'
      · rw [if_neg hkl] at hrl' hctl' ⊢
        exact h.gate_leader k ℓ hpre hrl' hctl' e he'
    · rw [if_neg hk] at hr he' hctl'
      rcases eq_or_ne ℓ i with rfl | hkl
      · rw [if_pos rfl] at hrl' hctl' ⊢
        obtain ⟨hdn, -, hprl⟩ := hldr hrl'
        rw [hdn] at hctl' ⊢
        exact h.gate_leader k ℓ hr hprl hctl' e he'
      · rw [if_neg hkl] at hrl' hctl' ⊢
        exact h.gate_leader k ℓ hr hrl' hctl' e he'
  · intro k ℓ hr hrl hctl p hp'
    rw [hrc] at hr
    have hrl' : (w'.nodes ℓ).dn.pn.role = .leader := hrl
    have hctl' : (w'.nodes ℓ).dn.pn.currentTerm = (w'.nodes k).dn.dataTerm :=
      hctl
    have hp'' : p < (w'.nodes k).dn.pn.durable := hp'
    show TermMap.termAt (w'.nodes k).dn.termMap p
      = TermMap.termAt (w'.nodes ℓ).dn.termMap p
    rw [hpn ℓ] at hrl'
    rw [hpn ℓ, hpn k] at hctl'
    rw [hpn k] at hp''
    rw [hpn k, hpn ℓ]
    rcases eq_or_ne k i with rfl | hk
    · rw [if_pos rfl] at hr hctl' hp'' ⊢
      obtain ⟨hdt, hpre⟩ := hgate hr
      rw [hdur] at hp''
      rw [hdt] at hctl'
      rw [hmap]
      rcases eq_or_ne ℓ k with rfl | hkl
      · rw [if_pos rfl] at hrl' ⊢
        obtain ⟨hdn, -, -⟩ := hldr hrl'
        rw [hdn]
      · rw [if_neg hkl] at hrl' hctl' ⊢
        exact h.gate_leader_eq k ℓ hpre hrl' hctl' p hp''
    · rw [if_neg hk] at hr hctl' hp'' ⊢
      rcases eq_or_ne ℓ i with rfl | hkl
      · rw [if_pos rfl] at hrl' hctl' ⊢
        obtain ⟨hdn, -, hprl⟩ := hldr hrl'
        rw [hdn] at hctl' ⊢
        exact h.gate_leader_eq k ℓ hr hprl hctl' p hp''
      · rw [if_neg hkl] at hrl' hctl' ⊢
        exact h.gate_leader_eq k ℓ hr hrl' hctl' p hp''
  · intro k hr p t v hf hp'
    rw [hrc] at hr
    have hf' : Data.Frame.replicate p ((w'.nodes k).dn.dataTerm) t v
        ∈ w.dsent := by rw [← hds]; exact hf
    have hp'' : p < (w'.nodes k).dn.pn.durable := hp'
    show TermMap.termAt (w'.nodes k).dn.termMap p = t
    rw [hpn] at hf' hp'' ⊢
    rcases eq_or_ne k i with rfl | hk
    · rw [if_pos rfl] at hr hf' hp'' ⊢
      obtain ⟨hdt, hpre⟩ := hgate hr
      rw [hdur] at hp''
      rw [hdt] at hf'
      rw [hmap]
      exact h.gate_frames_eq k hpre p t v hf' hp''
    · rw [if_neg hk] at hr hf' hp'' ⊢
      exact h.gate_frames_eq k hr p t v hf' hp''
  · intro k ℓ hr hrl hctl
    rw [hrc] at hr
    have hrl' : (w'.nodes ℓ).dn.pn.role = .leader := hrl
    have hctl' : (w'.nodes ℓ).dn.pn.currentTerm = (w'.nodes k).dn.dataTerm :=
      hctl
    show (w'.nodes k).dn.pn.durable ≤ (w'.nodes ℓ).dn.pn.durable
    rw [hpn ℓ] at hrl'
    rw [hpn ℓ, hpn k] at hctl'
    rw [hpn k, hpn ℓ]
    rcases eq_or_ne k i with rfl | hk
    · rw [if_pos rfl] at hr hctl' ⊢
      obtain ⟨hdt, hpre⟩ := hgate hr
      rw [hdt] at hctl'
      rw [hdur]
      rcases eq_or_ne ℓ k with rfl | hkl
      · rw [if_pos rfl] at hrl' hctl' ⊢
        obtain ⟨hdn, -, hprl⟩ := hldr hrl'
        rw [hdn] at hctl' ⊢
        exact h.gate_durable ℓ ℓ hpre hprl hctl'
      · rw [if_neg hkl] at hrl' hctl' ⊢
        exact h.gate_durable k ℓ hpre hrl' hctl'
    · rw [if_neg hk] at hr hctl' ⊢
      rcases eq_or_ne ℓ i with rfl | hkl
      · rw [if_pos rfl] at hrl' hctl' ⊢
        obtain ⟨hdn, -, hprl⟩ := hldr hrl'
        rw [hdn] at hctl' ⊢
        exact h.gate_durable k ℓ hr hprl hctl'
      · rw [if_neg hkl] at hrl' hctl' ⊢
        exact h.gate_durable k ℓ hr hrl' hctl'
  · intro u T d hrp
    rw [hcs] at hrp
    show T ≤ (w'.nodes u).dn.dataTerm
    rw [hpn]
    rcases eq_or_ne u i with rfl | hk
    · rw [if_pos rfl]
      exact Nat.le_trans (h.report_dt u T d hrp) hdtge
    · rw [if_neg hk]
      exact h.report_dt u T d hrp
  · intro u T d hrp hT
    rw [hcs] at hrp
    obtain ⟨ℓ, hc⟩ := h.report_cert u T d hrp hT
    exact ⟨ℓ, cert_carry hs hc⟩
  · intro u T d hrp hdtu p t v hf hp'
    rw [hcs] at hrp
    rw [hds] at hf
    have hdtu' : (w'.nodes u).dn.dataTerm = T := hdtu
    have hp'' : p < (w'.nodes u).dn.pn.durable := hp'
    show TermMap.termAt (w'.nodes u).dn.termMap p = t
    rw [hpn] at hdtu' hp'' ⊢
    rcases eq_or_ne u i with rfl | hk
    · rw [if_pos rfl] at hdtu' hp'' ⊢
      have hold : (w.nodes u).dataTerm = T :=
        Nat.le_antisymm (hdtu' ▸ hdtge) (h.report_dt u T d hrp)
      rw [hdur] at hp''
      rw [hmap]
      exact h.report_frames u T d hrp hold p t v hf hp''
    · rw [if_neg hk] at hdtu' hp'' ⊢
      exact h.report_frames u T d hrp hdtu' p t v hf hp''
  · intro u T d hrp hdtu ℓ hrl hctl p hp'
    rw [hcs] at hrp
    have hdtu' : (w'.nodes u).dn.dataTerm = T := hdtu
    have hrl' : (w'.nodes ℓ).dn.pn.role = .leader := hrl
    have hctl' : (w'.nodes ℓ).dn.pn.currentTerm = T := hctl
    have hp'' : p < (w'.nodes u).dn.pn.durable := hp'
    show TermMap.termAt (w'.nodes u).dn.termMap p
      = TermMap.termAt (w'.nodes ℓ).dn.termMap p
    rw [hpn u] at hdtu'
    rw [hpn ℓ] at hrl' hctl'
    rw [hpn u] at hp''
    rw [hpn u, hpn ℓ]
    rcases eq_or_ne u i with rfl | hk
    · rw [if_pos rfl] at hdtu' hp'' ⊢
      have hold : (w.nodes u).dataTerm = T :=
        Nat.le_antisymm (hdtu' ▸ hdtge) (h.report_dt u T d hrp)
      rw [hdur] at hp''
      rw [hmap]
      rcases eq_or_ne ℓ u with rfl | hkl
      · rw [if_pos rfl] at hrl' ⊢
        obtain ⟨hdn, -, -⟩ := hldr hrl'
        rw [hdn]
      · rw [if_neg hkl] at hrl' hctl' ⊢
        exact h.report_leader_eq u T d hrp hold ℓ hrl' hctl' p hp''
    · rw [if_neg hk] at hdtu' hp'' ⊢
      rcases eq_or_ne ℓ i with rfl | hkl
      · rw [if_pos rfl] at hrl' hctl' ⊢
        obtain ⟨hdn, -, hprl⟩ := hldr hrl'
        rw [hdn] at hctl' ⊢
        exact h.report_leader_eq u T d hrp hdtu' ℓ hprl hctl' p hp''
      · rw [if_neg hkl] at hrl' hctl' ⊢
        exact h.report_leader_eq u T d hrp hdtu' ℓ hrl' hctl' p hp''
  · intro u T d hrp hdtu ℓ hrl hctl
    rw [hcs] at hrp
    have hdtu' : (w'.nodes u).dn.dataTerm = T := hdtu
    have hrl' : (w'.nodes ℓ).dn.pn.role = .leader := hrl
    have hctl' : (w'.nodes ℓ).dn.pn.currentTerm = T := hctl
    show (w'.nodes u).dn.pn.durable ≤ (w'.nodes ℓ).dn.pn.durable
    rw [hpn u] at hdtu'
    rw [hpn ℓ] at hrl' hctl'
    rw [hpn u, hpn ℓ]
    rcases eq_or_ne u i with rfl | hk
    · rw [if_pos rfl] at hdtu' ⊢
      have hold : (w.nodes u).dataTerm = T :=
        Nat.le_antisymm (hdtu' ▸ hdtge) (h.report_dt u T d hrp)
      rw [hdur]
      rcases eq_or_ne ℓ u with rfl | hkl
      · rw [if_pos rfl] at hrl' ⊢
        obtain ⟨hdn, -, -⟩ := hldr hrl'
        rw [hdn]
        exact Nat.le_refl _
      · rw [if_neg hkl] at hrl' hctl' ⊢
        exact h.report_durable u T d hrp hold ℓ hrl' hctl'
    · rw [if_neg hk] at hdtu' ⊢
      rcases eq_or_ne ℓ i with rfl | hkl
      · rw [if_pos rfl] at hrl' hctl' ⊢
        obtain ⟨hdn, -, hprl⟩ := hldr hrl'
        rw [hdn] at hctl' ⊢
        exact h.report_durable u T d hrp hdtu' ℓ hprl hctl'
      · rw [if_neg hkl] at hrl' hctl' ⊢
        exact h.report_durable u T d hrp hdtu' ℓ hrl' hctl'

/-- **Preservation**: every commit-layer step preserves the bundle. -/
private theorem provinv_step {n : Nat} {w w' : World n} (hw : Reachable w)
    (h : ProvInv w) (hs : Step w w') : ProvInv w' := by
  have hw' : Reachable w' := Relation.ReflTransGen.tail hw hs
  cases hs with
  | startElection i hrole =>
    -- candidate bump: data slice, handle, and gate untouched; the handle
    -- now LAGS the bumped term (the whole point of `dataTerm`).
    exact provinv_election (Step.startElection w i hrole) h rfl rfl rfl
      rfl rfl rfl (fun hnc => absurd rfl hnc) (Nat.le_refl _)
      (fun hcl => h.closed_lag i hcl) (fun hg => ⟨rfl, hg⟩)
      (fun hl => nomatch hl)
  | deliverRequestVote j c nt clt cd hmsg hterm =>
    -- vote receive: strict adoption re-keys the handle and closes the gate;
    -- the same-term receive freezes everything except (possibly) a fresh
    -- vote record — and a LEADER's state is frozen outright by the vote
    -- discipline's idempotency (`recv_voted_current` + `self_vote`).
    have hdle := (Uc2.Data.reachable_stamp (reachable_project hw)).data_le j
    have hmldt := Uc2.Data.reachable_map_le_dataTerm (reachable_project hw) j
    have hpInv := Uc2.reachable_inv
      (Uc2.Data.reachable_project (reachable_project hw))
    refine provinv_election (Step.deliverRequestVote w j c nt clt cd hmsg
      hterm) h rfl rfl rfl rfl rfl
      (Data.recv_durable (w.nodes j).pn c nt clt cd) ?_ ?_ ?_ ?_ ?_
    · intro hnc
      show (if (w.nodes j).pn.currentTerm < nt then nt
          else (w.nodes j).dataTerm)
        = ((w.nodes j).pn.recvRequestVote c nt clt cd).1.currentTerm
      by_cases hadopt : (w.nodes j).pn.currentTerm < nt
      · rw [if_pos hadopt, Data.recv_term _ _ _ _ _ hterm]
      · have hold : (w.nodes j).dataTerm = (w.nodes j).pn.currentTerm :=
          h.role_dt j (by
            have hnc' : ((w.nodes j).pn.recvRequestVote c nt clt cd).1.role
                ≠ .candidate := hnc
            rwa [(Data.recv_frame (w.nodes j).pn c nt clt cd hadopt).1]
              at hnc')
        rw [if_neg hadopt,
          (Data.recv_frame (w.nodes j).pn c nt clt cd hadopt).2]
        exact hold
    · show (w.nodes j).dataTerm ≤ (if (w.nodes j).pn.currentTerm < nt
        then nt else (w.nodes j).dn.dataTerm)
      by_cases hadopt : (w.nodes j).pn.currentTerm < nt
      · rw [if_pos hadopt]
        exact Nat.le_trans hdle (Nat.le_of_lt hadopt)
      · rw [if_neg hadopt]
        exact Nat.le_refl _
    · intro hcl
      show Data.lastTermOf (w.nodes j).dn.termMap
        < (if (w.nodes j).pn.currentTerm < nt then nt
           else (w.nodes j).dn.dataTerm)
      have hcl' : (if (w.nodes j).pn.currentTerm < nt then false
          else (w.nodes j).reconciled) = false := hcl
      by_cases hadopt : (w.nodes j).pn.currentTerm < nt
      · rw [if_pos hadopt]
        have h1 : Data.lastTermOf (w.nodes j).dn.termMap
            ≤ (w.nodes j).dataTerm := hmldt
        have h2 : (w.nodes j).dataTerm ≤ (w.nodes j).pn.currentTerm := hdle
        omega
      · rw [if_neg hadopt]
        rw [if_neg hadopt] at hcl'
        exact h.closed_lag j hcl'
    · intro hg
      have hg' : (if (w.nodes j).pn.currentTerm < nt then false
          else (w.nodes j).reconciled) = true := hg
      by_cases hadopt : (w.nodes j).pn.currentTerm < nt
      · rw [if_pos hadopt] at hg'
        cases hg'
      · rw [if_neg hadopt] at hg'
        refine ⟨?_, hg'⟩
        show (if (w.nodes j).pn.currentTerm < nt then nt
          else (w.nodes j).dn.dataTerm) = (w.nodes j).dataTerm
        rw [if_neg hadopt]
        rfl
    · intro hl
      have hl' : ((w.nodes j).pn.recvRequestVote c nt clt cd).1.role
          = .leader := hl
      by_cases hadopt : (w.nodes j).pn.currentTerm < nt
      · rw [Data.recv_adopt_role _ _ _ _ _ hadopt] at hl'
        cases hl'
      · have hrl : (w.nodes j).pn.role = .leader := by
          rwa [(Data.recv_frame (w.nodes j).pn c nt clt cd hadopt).1] at hl'
        have hsv := hpInv.self_vote j (by
          show (w.nodes j).pn.role ≠ .follower
          rw [hrl]; decide)
        have heq : (w.nodes j).pn.currentTerm = nt := by omega
        have hunch := (Data.recv_voted_current (w.nodes j).pn c j nt clt cd
          heq hsv).1
        refine ⟨?_, ?_, hrl⟩
        · simp only [hunch, if_neg hadopt]
          rfl
        · show (if (w.nodes j).pn.currentTerm < nt then false
            else (w.nodes j).reconciled) = (w.nodes j).reconciled
          rw [if_neg hadopt]
  | rejectStaleRequestVote j c nt clt cd hmsg hstale =>
    exact provinv_transport
      (Step.rejectStaleRequestVote w j c nt clt cd hmsg hstale) h
      (fun _ => rfl) (fun _ => rfl) (fun _ => rfl) (fun _ => rfl)
      (fun _ => rfl) (fun _ => rfl) (fun _ => rfl) rfl rfl
  | deliverVote i v tv hmsg hrole hterm =>
    refine provinv_transport (Step.deliverVote w i v tv hmsg hrole hterm) h
      ?_ ?_ ?_ ?_ ?_ ?_ ?_ rfl rfl <;>
    · intro k
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self, Node.pn, Node.hist, Node.dataTerm]
      · simp only [Function.update_of_ne hne]
  | deliverVoteHigherTerm i v tv g hmsg hterm =>
    -- strict adoption: handle re-keys to the new term, gate CLOSES.
    have hdle := (Uc2.Data.reachable_stamp (reachable_project hw)).data_le i
    have hmldt := Uc2.Data.reachable_map_le_dataTerm (reachable_project hw) i
    exact provinv_election (Step.deliverVoteHigherTerm w i v tv g hmsg hterm)
      h rfl rfl rfl rfl rfl rfl (fun _ => rfl)
      (Nat.le_trans hdle (Nat.le_of_lt hterm))
      (fun _ => Nat.lt_of_le_of_lt (Nat.le_trans hmldt hdle) hterm)
      (fun hg => nomatch hg)
      (fun hl => nomatch hl)
  | becomeLeader i hrole hquorum =>
    -- election win: NOTHING at the won term predates it — any frame,
    -- gossip, gate-open regime, or report at `c` would carry a certificate
    -- that `cert_blocks_candidate` turns against the winning tally.
    have hs' := Step.becomeLeader w i hrole hquorum
    have hw2 : Reachable _ := Relation.ReflTransGen.tail hw hs'
    have hminv := Uc2.Data.reachable_mapInv (reachable_project hw)
    have hstamp := Uc2.Data.reachable_stamp (reachable_project hw)
    have hpInv := Uc2.reachable_inv
      (Uc2.Data.reachable_project (reachable_project hw))
    have hblock : ∀ {ℓ' : Fin n},
        Data.Cert w.project ((w.nodes i).pn.currentTerm) ℓ' → False :=
      fun hc => Uc2.Data.cert_blocks_candidate hpInv hrole rfl hquorum hc
    have hc1 : 1 ≤ (w.nodes i).pn.currentTerm :=
      (hminv.node i).role_term_pos (by
        show (w.nodes i).pn.role ≠ .follower
        rw [hrole]; decide)
    have hasc' : TermMap.Ascending (Data.prunePush (w.nodes i).dn.termMap
        ((w.nodes i).pn.currentTerm) ((w.nodes i).pn.durable)) := by
      have h0 := ((Uc2.Data.reachable_mapInv (reachable_project hw2)).node
        i).asc
      simp only [World.project, Function.update_self] at h0
      exact h0
    refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_,
      ?_, ?_, ?_, ?_, ?_⟩
    · intro p' hdr' t' v' es hf hg
      exact h.frame_gossip p' hdr' t' v' es hf hg
    · intro p₁ p₂ hdr' t₁ t₂ v₁ v₂ h1 h2 hle
      exact h.frame_mono p₁ p₂ hdr' t₁ t₂ v₁ v₂ h1 h2 hle
    · intro p' hdr' t₁ t₂ v₁ v₂ h1 h2
      exact h.frame_uniq p' hdr' t₁ t₂ v₁ v₂ h1 h2
    · intro p' hdr' t' v' hf
      obtain ⟨ℓ, hc⟩ := h.frame_cert p' hdr' t' v' hf
      exact ⟨ℓ, cert_carry hs' hc⟩
    · intro k hnc
      rcases eq_or_ne k i with rfl | hk
      · simp only [Function.update_self]
        rfl
      · simp only [Function.update_of_ne hk] at hnc ⊢
        exact h.role_dt k hnc
    · intro k p tv hh
      rcases eq_or_ne k i with rfl | hk
      · simp only [Function.update_self] at hh ⊢
        exact h.hist_bound k p tv hh
      · simp only [Function.update_of_ne hk] at hh ⊢
        exact h.hist_bound k p tv hh
    · intro k hr
      rcases eq_or_ne k i with rfl | hk
      · simp only [Function.update_self] at hr
        cases hr
      · simp only [Function.update_of_ne hk] at hr ⊢
        exact h.closed_lag k hr
    · intro k p t' v' hh
      rcases eq_or_ne k i with rfl | hk
      · simp only [Function.update_self] at hh ⊢
        have hh' : (w.nodes k).dn.hist p = some (t', v') := hh
        have hpb := h.hist_bound k p (t', v') hh'
        show TermMap.termAt (Data.prunePush (w.nodes k).dn.termMap
          ((w.nodes k).pn.currentTerm) ((w.nodes k).pn.durable)) p = t'
        rw [termAt_prunePush hpb]
        exact h.fca k p t' v' hh'
      · simp only [Function.update_of_ne hk] at hh ⊢
        exact h.fca k p t' v' hh
    · intro ℓ hrl p' t' v' hf
      rcases eq_or_ne ℓ i with rfl | hk
      · simp only [Function.update_self] at hrl hf ⊢
        have hf' : Data.Frame.replicate p' ((w.nodes ℓ).pn.currentTerm) t' v'
            ∈ w.dsent := hf
        obtain ⟨ℓ', hc⟩ := h.frame_cert p' _ t' v' hf'
        exact absurd hc hblock
      · simp only [Function.update_of_ne hk] at hrl hf ⊢
        exact h.leader_frontier ℓ hrl p' t' v' hf
    · intro p' hdr' t' v' hf i' hrl hcti
      rcases eq_or_ne i' i with rfl | hk
      · simp only [Function.update_self] at hrl hcti ⊢
        have hcti' : (w.nodes i').pn.currentTerm = hdr' := hcti
        rw [← hcti'] at hf
        obtain ⟨ℓ', hc⟩ := h.frame_cert p' _ t' v' hf
        exact absurd hc hblock
      · simp only [Function.update_of_ne hk] at hrl hcti ⊢
        exact h.frame_leader p' hdr' t' v' hf i' hrl hcti
    · intro k hr h1
      rcases eq_or_ne k i with rfl | hk
      · simp only [Function.update_self]
        have hpInv2 := Uc2.reachable_inv
          (Uc2.Data.reachable_project (reachable_project hw2))
        have hcl := Uc2.Data.cert_of_leader (i := k) hpInv2
          (by simp only [World.project, Function.update_self])
        simp only [World.project, Function.update_self] at hcl
        exact ⟨k, hcl⟩
      · simp only [Function.update_of_ne hk] at hr h1 ⊢
        obtain ⟨ℓ, hc⟩ := h.gate_cert k hr h1
        exact ⟨ℓ, cert_carry hs' hc⟩
    · intro k hr e he p' t' v' hf hep
      rcases eq_or_ne k i with rfl | hk
      · simp only [Function.update_self] at hr he hf ⊢
        have hf' : Data.Frame.replicate p' ((w.nodes k).pn.currentTerm) t' v'
            ∈ w.dsent := hf
        obtain ⟨ℓ', hc⟩ := h.frame_cert p' _ t' v' hf'
        exact absurd hc hblock
      · simp only [Function.update_of_ne hk] at hr he hf ⊢
        exact h.gate_map_frame k hr e he p' t' v' hf hep
    · intro k ℓ hr hrl hctl e he
      rcases eq_or_ne k i with rfl | hk
      · rcases eq_or_ne ℓ k with rfl | hkl
        · simp only [Function.update_self] at he ⊢
          have he' : e ∈ Data.prunePush (w.nodes ℓ).dn.termMap
              ((w.nodes ℓ).pn.currentTerm) ((w.nodes ℓ).pn.durable) := he
          show e.1 ≤ TermMap.termAt (Data.prunePush (w.nodes ℓ).dn.termMap
            ((w.nodes ℓ).pn.currentTerm) ((w.nodes ℓ).pn.durable)) e.2
          exact TermMap.le_termAt hasc' he' (Nat.le_refl _)
        · simp only [Function.update_self] at hctl
          simp only [Function.update_of_ne hkl] at hrl hctl
          have hctl' : (w.nodes ℓ).pn.currentTerm
              = (w.nodes k).pn.currentTerm := hctl
          have hrl' : (w.nodes ℓ).pn.role = .leader := hrl
          have hc := Uc2.Data.cert_of_leader hpInv hrl'
          rw [show ((w.project.nodes ℓ).pn.currentTerm)
            = (w.nodes k).pn.currentTerm from hctl'] at hc
          exact absurd hc hblock
      · rcases eq_or_ne ℓ i with rfl | hkl
        · simp only [Function.update_of_ne hk] at hr hctl he
          simp only [Function.update_self] at hrl hctl
          have hctl2 : (w.nodes ℓ).pn.currentTerm = (w.nodes k).dataTerm :=
            hctl
          obtain ⟨ℓ2, hc⟩ := h.gate_cert k hr (by rw [← hctl2]; exact hc1)
          rw [← hctl2] at hc
          exact absurd hc hblock
        · simp only [Function.update_of_ne hk] at hr hctl he
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          exact h.gate_leader k ℓ hr hrl hctl e he
    · intro k ℓ hr hrl hctl p hp'
      rcases eq_or_ne k i with rfl | hk
      · rcases eq_or_ne ℓ k with rfl | hkl
        · rfl
        · simp only [Function.update_self] at hctl
          simp only [Function.update_of_ne hkl] at hrl hctl
          have hctl' : (w.nodes ℓ).pn.currentTerm
              = (w.nodes k).pn.currentTerm := hctl
          have hrl' : (w.nodes ℓ).pn.role = .leader := hrl
          have hc := Uc2.Data.cert_of_leader hpInv hrl'
          rw [show ((w.project.nodes ℓ).pn.currentTerm)
            = (w.nodes k).pn.currentTerm from hctl'] at hc
          exact absurd hc hblock
      · rcases eq_or_ne ℓ i with rfl | hkl
        · simp only [Function.update_of_ne hk] at hr hctl hp'
          simp only [Function.update_self] at hrl hctl
          have hctl2 : (w.nodes ℓ).pn.currentTerm = (w.nodes k).dataTerm :=
            hctl
          obtain ⟨ℓ2, hc⟩ := h.gate_cert k hr (by rw [← hctl2]; exact hc1)
          rw [← hctl2] at hc
          exact absurd hc hblock
        · simp only [Function.update_of_ne hk] at hr hctl hp' ⊢
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          exact h.gate_leader_eq k ℓ hr hrl hctl p hp'
    · intro k hr p' t' v' hf hp'
      rcases eq_or_ne k i with rfl | hk
      · simp only [Function.update_self] at hr hf hp' ⊢
        have hf' : Data.Frame.replicate p' ((w.nodes k).pn.currentTerm) t' v'
            ∈ w.dsent := hf
        obtain ⟨ℓ', hc⟩ := h.frame_cert p' _ t' v' hf'
        exact absurd hc hblock
      · simp only [Function.update_of_ne hk] at hr hf hp' ⊢
        exact h.gate_frames_eq k hr p' t' v' hf hp'
    · intro k ℓ hr hrl hctl
      rcases eq_or_ne k i with rfl | hk
      · rcases eq_or_ne ℓ k with rfl | hkl
        · exact Nat.le_refl _
        · simp only [Function.update_self] at hctl
          simp only [Function.update_of_ne hkl] at hrl hctl
          have hctl' : (w.nodes ℓ).pn.currentTerm
              = (w.nodes k).pn.currentTerm := hctl
          have hrl' : (w.nodes ℓ).pn.role = .leader := hrl
          have hc := Uc2.Data.cert_of_leader hpInv hrl'
          rw [show ((w.project.nodes ℓ).pn.currentTerm)
            = (w.nodes k).pn.currentTerm from hctl'] at hc
          exact absurd hc hblock
      · rcases eq_or_ne ℓ i with rfl | hkl
        · simp only [Function.update_of_ne hk] at hr hctl
          simp only [Function.update_self] at hrl hctl
          have hctl2 : (w.nodes ℓ).pn.currentTerm = (w.nodes k).dataTerm :=
            hctl
          obtain ⟨ℓ2, hc⟩ := h.gate_cert k hr (by rw [← hctl2]; exact hc1)
          rw [← hctl2] at hc
          exact absurd hc hblock
        · simp only [Function.update_of_ne hk] at hr hctl ⊢
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          exact h.gate_durable k ℓ hr hrl hctl
    · intro u T d hrp
      rcases eq_or_ne u i with rfl | hk
      · simp only [Function.update_self]
        have h1 := h.report_dt u T d hrp
        have h2 : (w.nodes u).dataTerm ≤ (w.nodes u).pn.currentTerm :=
          hstamp.data_le u
        show T ≤ (w.nodes u).pn.currentTerm
        omega
      · simp only [Function.update_of_ne hk]
        exact h.report_dt u T d hrp
    · intro u T d hrp hT
      obtain ⟨ℓ, hc⟩ := h.report_cert u T d hrp hT
      exact ⟨ℓ, cert_carry hs' hc⟩
    · intro u T d hrp hdtu p' t' v' hf hp'
      rcases eq_or_ne u i with rfl | hk
      · simp only [Function.update_self] at hdtu
        have hdtu' : (w.nodes u).pn.currentTerm = T := hdtu
        obtain ⟨ℓ2, hc⟩ := h.report_cert u T d hrp (by omega)
        rw [← hdtu'] at hc
        exact absurd hc hblock
      · simp only [Function.update_of_ne hk] at hdtu hf hp' ⊢
        exact h.report_frames u T d hrp hdtu p' t' v' hf hp'
    · intro u T d hrp hdtu ℓ hrl hctl p' hp'
      rcases eq_or_ne u i with rfl | hk
      · simp only [Function.update_self] at hdtu
        have hdtu' : (w.nodes u).pn.currentTerm = T := hdtu
        obtain ⟨ℓ2, hc⟩ := h.report_cert u T d hrp (by omega)
        rw [← hdtu'] at hc
        exact absurd hc hblock
      · rcases eq_or_ne ℓ i with rfl | hkl
        · simp only [Function.update_of_ne hk] at hdtu hp' ⊢
          simp only [Function.update_self] at hrl hctl
          have hctl' : (w.nodes ℓ).pn.currentTerm = T := hctl
          obtain ⟨ℓ2, hc⟩ := h.report_cert u T d hrp (by omega)
          rw [← hctl'] at hc
          exact absurd hc hblock
        · simp only [Function.update_of_ne hk] at hdtu hp' ⊢
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          exact h.report_leader_eq u T d hrp hdtu ℓ hrl hctl p' hp'
    · intro u T d hrp hdtu ℓ hrl hctl
      rcases eq_or_ne u i with rfl | hk
      · simp only [Function.update_self] at hdtu
        have hdtu' : (w.nodes u).pn.currentTerm = T := hdtu
        obtain ⟨ℓ2, hc⟩ := h.report_cert u T d hrp (by omega)
        rw [← hdtu'] at hc
        exact absurd hc hblock
      · rcases eq_or_ne ℓ i with rfl | hkl
        · simp only [Function.update_of_ne hk] at hdtu ⊢
          simp only [Function.update_self] at hrl hctl
          have hctl' : (w.nodes ℓ).pn.currentTerm = T := hctl
          obtain ⟨ℓ2, hc⟩ := h.report_cert u T d hrp (by omega)
          rw [← hctl'] at hc
          exact absurd hc hblock
        · simp only [Function.update_of_ne hk] at hdtu ⊢
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          exact h.report_durable u T d hrp hdtu ℓ hrl hctl
  | crashRestart i =>
    -- reboot: handle re-keys to the recovered term; the Finding-#5 boot
    -- predicate opens the gate exactly when the map's frontier reaches the
    -- recovered term — which pins the regime UNCHANGED across the crash
    -- (`closed_lag` rules out a pre-closed gate in that configuration).
    have hdle := (Uc2.Data.reachable_stamp (reachable_project hw)).data_le i
    have hmldt := Uc2.Data.reachable_map_le_dataTerm (reachable_project hw) i
    refine provinv_election (Step.crashRestart w i) h rfl rfl rfl rfl rfl rfl
      (fun _ => rfl) hdle ?_ ?_ (fun hl => nomatch hl)
    · intro hcl
      have h1 := of_decide_eq_false hcl
      show Data.lastTermOf (w.nodes i).dn.termMap < (w.nodes i).pn.currentTerm
      omega
    · intro hg
      have h1 := of_decide_eq_true hg
      have hdle' : (w.nodes i).dataTerm ≤ (w.nodes i).pn.currentTerm := hdle
      have hmldt' : Data.lastTermOf (w.nodes i).dn.termMap
          ≤ (w.nodes i).dataTerm := hmldt
      have h2 : (w.nodes i).pn.currentTerm = (w.nodes i).dataTerm := by
        omega
      refine ⟨h2, ?_⟩
      cases hpre : (w.nodes i).reconciled with
      | true => rfl
      | false =>
        have h3 := h.closed_lag i hpre
        omega
  | leaderAppend i v hrole =>
    -- fresh append: the new frame is stamped AND headed `currentTerm i`,
    -- and lands exactly at the leader's frontier, which its (frozen) map
    -- attributes to `currentTerm i` (`map_pinned` + `last_base`).
    have hs' := Step.leaderAppend w i v hrole
    have hdinv := Uc2.Data.reachable_dinv (reachable_project hw)
    have hminv := Uc2.Data.reachable_mapInv (reachable_project hw)
    have hmldt := Uc2.Data.reachable_map_le_dataTerm (reachable_project hw)
    have hldt : (w.nodes i).dataTerm = (w.nodes i).pn.currentTerm :=
      Uc2.Data.reachable_leader_dataTerm (reachable_project hw) i hrole
    have hstamp := Uc2.Data.reachable_stamp (reachable_project hw)
    have hpInv := Uc2.reachable_inv
      (Uc2.Data.reachable_project (reachable_project hw))
    have hasc : TermMap.Ascending (w.nodes i).dn.termMap := (hminv.node i).asc
    have hne : (w.nodes i).dn.termMap ≠ [] := (hminv.node i).leader_map hrole
    obtain ⟨lst, hlst⟩ := Option.ne_none_iff_exists'.mp
      (mt List.getLast?_eq_none_iff.mp hne)
    have hlstD : lst.1 = (w.nodes i).pn.currentTerm := by
      have hmp : Data.lastTermOf (w.nodes i).dn.termMap
          = (w.nodes i).pn.currentTerm := hdinv.map_pinned i hrole
      rwa [Data.lastTermOf, hlst, Option.map_some, Option.getD_some] at hmp
    have hlb : lst.2 ≤ (w.nodes i).pn.durable :=
      (hminv.node i).last_base lst hlst
    have hterm_at : TermMap.termAt (w.nodes i).dn.termMap
        ((w.nodes i).pn.durable) = (w.nodes i).pn.currentTerm := by
      rw [TermMap.termAt_of_last_base_le hasc hlst hlb]
      exact hlstD
    refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_,
      ?_, ?_, ?_, ?_, ?_⟩
    · intro p' hdr' t' v' es hf hg
      simp only [List.mem_append, List.mem_singleton] at hf hg
      rcases hg with hg | hg
      · rcases hf with hf | hf
        · exact h.frame_gossip p' hdr' t' v' es hf hg
        · rw [Data.Frame.replicate.injEq] at hf
          obtain ⟨rfl, rfl, rfl, rfl⟩ := hf
          rw [hdinv.gossip_pinned i hrole es hg]
          exact hterm_at
      · exact absurd hg (by simp)
    · intro p₁ p₂ hdr' t₁ t₂ v₁ v₂ h1 h2 hle
      simp only [List.mem_append, List.mem_singleton] at h1 h2
      rcases h1 with h1 | h1 <;> rcases h2 with h2 | h2
      · exact h.frame_mono p₁ p₂ hdr' t₁ t₂ v₁ v₂ h1 h2 hle
      · rw [Data.Frame.replicate.injEq] at h2
        obtain ⟨rfl, rfl, rfl, rfl⟩ := h2
        exact hstamp.frame_le p₁ _ t₁ v₁ h1
      · rw [Data.Frame.replicate.injEq] at h1
        obtain ⟨rfl, rfl, rfl, rfl⟩ := h1
        have := h.leader_frontier i hrole p₂ t₂ v₂ h2
        omega
      · rw [Data.Frame.replicate.injEq] at h1
        rw [Data.Frame.replicate.injEq] at h2
        obtain ⟨rfl, rfl, rfl, rfl⟩ := h1
        obtain ⟨-, -, rfl, -⟩ := h2
        exact Nat.le_refl _
    · intro p' hdr' t₁ t₂ v₁ v₂ h1 h2
      simp only [List.mem_append, List.mem_singleton] at h1 h2
      rcases h1 with h1 | h1 <;> rcases h2 with h2 | h2
      · exact h.frame_uniq p' hdr' t₁ t₂ v₁ v₂ h1 h2
      · rw [Data.Frame.replicate.injEq] at h2
        obtain ⟨rfl, rfl, rfl, rfl⟩ := h2
        have := h.leader_frontier i hrole _ t₁ v₁ h1
        omega
      · rw [Data.Frame.replicate.injEq] at h1
        obtain ⟨rfl, rfl, rfl, rfl⟩ := h1
        have := h.leader_frontier i hrole _ t₂ v₂ h2
        omega
      · rw [Data.Frame.replicate.injEq] at h1
        rw [Data.Frame.replicate.injEq] at h2
        obtain ⟨rfl, rfl, rfl, rfl⟩ := h1
        obtain ⟨-, -, rfl, -⟩ := h2
        rfl
    · intro p' hdr' t' v' hf
      simp only [List.mem_append, List.mem_singleton] at hf
      rcases hf with hf | hf
      · obtain ⟨ℓ, hc⟩ := h.frame_cert p' hdr' t' v' hf
        exact ⟨ℓ, cert_carry hs' hc⟩
      · rw [Data.Frame.replicate.injEq] at hf
        obtain ⟨rfl, rfl, rfl, rfl⟩ := hf
        exact ⟨i, cert_carry hs' (Uc2.Data.cert_of_leader hpInv hrole)⟩
    · intro k hnc
      rcases eq_or_ne k i with rfl | hk
      · simp only [Function.update_self] at hnc ⊢
        exact h.role_dt k hnc
      · simp only [Function.update_of_ne hk] at hnc ⊢
        exact h.role_dt k hnc
    · intro k p tv hh
      rcases eq_or_ne k i with rfl | hk
      · simp only [Function.update_self] at hh ⊢
        have hh' : Function.update (w.nodes k).dn.hist
            ((w.nodes k).pn.durable)
            (some ((w.nodes k).pn.currentTerm, v)) p = some tv := hh
        show p < (w.nodes k).pn.durable + 1
        by_cases hp : p = (w.nodes k).pn.durable
        · omega
        · rw [Function.update_of_ne hp] at hh'
          have := h.hist_bound k p tv hh'
          omega
      · simp only [Function.update_of_ne hk] at hh ⊢
        exact h.hist_bound k p tv hh
    · intro k hr
      rcases eq_or_ne k i with rfl | hk
      · simp only [Function.update_self] at hr ⊢
        exact h.closed_lag k hr
      · simp only [Function.update_of_ne hk] at hr ⊢
        exact h.closed_lag k hr
    · intro k p t' v' hh
      rcases eq_or_ne k i with rfl | hk
      · simp only [Function.update_self] at hh ⊢
        have hh' : Function.update (w.nodes k).dn.hist
            ((w.nodes k).pn.durable)
            (some ((w.nodes k).pn.currentTerm, v)) p = some (t', v') := hh
        show TermMap.termAt (w.nodes k).dn.termMap p = t'
        by_cases hp : p = (w.nodes k).pn.durable
        · subst hp
          rw [Function.update_self, Option.some.injEq, Prod.mk.injEq] at hh'
          rw [← hh'.1]
          exact hterm_at
        · rw [Function.update_of_ne hp] at hh'
          exact h.fca k p t' v' hh'
      · simp only [Function.update_of_ne hk] at hh ⊢
        exact h.fca k p t' v' hh
    · intro ℓ hrl p' t' v' hf
      simp only [List.mem_append, List.mem_singleton] at hf
      rcases eq_or_ne ℓ i with rfl | hk
      · simp only [Function.update_self] at hrl hf ⊢
        show p' < (w.nodes ℓ).pn.durable + 1
        rcases hf with hf | hf
        · have := h.leader_frontier ℓ hrole p' t' v' hf
          omega
        · rw [Data.Frame.replicate.injEq] at hf
          obtain ⟨rfl, -, -, -⟩ := hf
          omega
      · simp only [Function.update_of_ne hk] at hrl hf ⊢
        rcases hf with hf | hf
        · exact h.leader_frontier ℓ hrl p' t' v' hf
        · rw [Data.Frame.replicate.injEq] at hf
          obtain ⟨rfl, hct, rfl, rfl⟩ := hf
          exact absurd (election_safety w hw ℓ i hrl hrole hct) hk
    · intro p' hdr' t' v' hf i' hrl hcti
      simp only [List.mem_append, List.mem_singleton] at hf
      rcases eq_or_ne i' i with rfl | hk
      · simp only [Function.update_self] at hrl hcti ⊢
        show TermMap.termAt (w.nodes i').dn.termMap p' = t'
        rcases hf with hf | hf
        · exact h.frame_leader p' hdr' t' v' hf i' hrole hcti
        · rw [Data.Frame.replicate.injEq] at hf
          obtain ⟨rfl, rfl, rfl, rfl⟩ := hf
          exact hterm_at
      · simp only [Function.update_of_ne hk] at hrl hcti ⊢
        rcases hf with hf | hf
        · exact h.frame_leader p' hdr' t' v' hf i' hrl hcti
        · rw [Data.Frame.replicate.injEq] at hf
          obtain ⟨rfl, rfl, rfl, rfl⟩ := hf
          exact absurd (election_safety w hw i' i hrl hrole hcti) hk
    · intro k hr h1
      rcases eq_or_ne k i with rfl | hk
      · simp only [Function.update_self] at hr h1 ⊢
        obtain ⟨ℓ, hc⟩ := h.gate_cert k hr h1
        exact ⟨ℓ, cert_carry hs' hc⟩
      · simp only [Function.update_of_ne hk] at hr h1 ⊢
        obtain ⟨ℓ, hc⟩ := h.gate_cert k hr h1
        exact ⟨ℓ, cert_carry hs' hc⟩
    · intro k hr e he p' t' v' hf hep
      simp only [List.mem_append, List.mem_singleton] at hf
      rcases eq_or_ne k i with rfl | hk
      · simp only [Function.update_self] at hr he hf ⊢
        rcases hf with hf | hf
        · exact h.gate_map_frame k hr e he p' t' v' hf hep
        · rw [Data.Frame.replicate.injEq] at hf
          obtain ⟨rfl, hdt', rfl, rfl⟩ := hf
          have h1 : e.1 ≤ Data.lastTermOf (w.nodes k).dn.termMap :=
            TermMap.term_le_lastTermOf hasc he
          have h2 : Data.lastTermOf (w.nodes k).dn.termMap
              = (w.nodes k).pn.currentTerm := hdinv.map_pinned k hrole
          omega
      · simp only [Function.update_of_ne hk] at hr he hf ⊢
        rcases hf with hf | hf
        · exact h.gate_map_frame k hr e he p' t' v' hf hep
        · rw [Data.Frame.replicate.injEq] at hf
          obtain ⟨rfl, hdt', rfl, rfl⟩ := hf
          have h1 : e.1 ≤ Data.lastTermOf (w.nodes k).dn.termMap :=
            TermMap.term_le_lastTermOf (hminv.node k).asc he
          have h2 : Data.lastTermOf (w.nodes k).dn.termMap
              ≤ (w.nodes k).dataTerm := hmldt k
          have h3 : (w.nodes k).dataTerm = (w.nodes i).pn.currentTerm := hdt'
          omega
    · intro k ℓ hr hrl hctl e he
      rcases eq_or_ne k i with rfl | hk
      · rcases eq_or_ne ℓ k with rfl | hkl
        · simp only [Function.update_self] at hr hrl hctl he ⊢
          exact h.gate_leader ℓ ℓ hr hrl hctl e he
        · simp only [Function.update_self] at hr hctl he
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          exact h.gate_leader k ℓ hr hrl hctl e he
      · rcases eq_or_ne ℓ i with rfl | hkl
        · simp only [Function.update_of_ne hk] at hr hctl he
          simp only [Function.update_self] at hrl hctl ⊢
          exact h.gate_leader k ℓ hr hrl hctl e he
        · simp only [Function.update_of_ne hk] at hr hctl he
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          exact h.gate_leader k ℓ hr hrl hctl e he
    · intro k ℓ hr hrl hctl p hp'
      rcases eq_or_ne k i with rfl | hk
      · rcases eq_or_ne ℓ k with rfl | hkl
        · rfl
        · simp only [Function.update_self] at hr hctl hp'
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          have hctI : (w.nodes ℓ).pn.currentTerm = (w.nodes k).pn.currentTerm := by
            have h1 : (w.nodes ℓ).pn.currentTerm = (w.nodes k).dataTerm := hctl
            rw [h1, hldt]
          exact absurd (election_safety w hw ℓ k hrl hrole hctI) hkl
      · rcases eq_or_ne ℓ i with rfl | hkl
        · simp only [Function.update_of_ne hk] at hr hctl hp' ⊢
          simp only [Function.update_self] at hrl hctl ⊢
          exact h.gate_leader_eq k ℓ hr hrl hctl p hp'
        · simp only [Function.update_of_ne hk] at hr hctl hp' ⊢
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          exact h.gate_leader_eq k ℓ hr hrl hctl p hp'
    · intro k hr p' t' v' hf hp'
      simp only [List.mem_append, List.mem_singleton] at hf
      rcases eq_or_ne k i with rfl | hk
      · simp only [Function.update_self] at hr hf hp' ⊢
        show TermMap.termAt (w.nodes k).dn.termMap p' = t'
        rcases hf with hf | hf
        · by_cases hp : p' < (w.nodes k).pn.durable
          · exact h.gate_frames_eq k hr p' t' v' hf hp
          · have hp'' : p' < (w.nodes k).pn.durable + 1 := hp'
            have hpd : p' = (w.nodes k).pn.durable := by omega
            have hf2 : Data.Frame.replicate p' ((w.nodes k).dataTerm) t' v'
                ∈ w.dsent := hf
            rw [hldt] at hf2
            have := h.leader_frontier k hrole p' t' v' hf2
            omega
        · rw [Data.Frame.replicate.injEq] at hf
          obtain ⟨rfl, -, rfl, rfl⟩ := hf
          exact hterm_at
      · simp only [Function.update_of_ne hk] at hr hf hp' ⊢
        rcases hf with hf | hf
        · exact h.gate_frames_eq k hr p' t' v' hf hp'
        · rw [Data.Frame.replicate.injEq] at hf
          obtain ⟨rfl, hdt', rfl, rfl⟩ := hf
          have := h.gate_durable k i hr hrole hdt'.symm
          have h2 : (w.nodes i).pn.durable < (w.nodes k).pn.durable := hp'
          omega
    · intro k ℓ hr hrl hctl
      rcases eq_or_ne k i with rfl | hk
      · rcases eq_or_ne ℓ k with rfl | hkl
        · exact Nat.le_refl _
        · simp only [Function.update_self] at hr hctl
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          have hctI : (w.nodes ℓ).pn.currentTerm
              = (w.nodes k).pn.currentTerm := by
            have h1 : (w.nodes ℓ).pn.currentTerm = (w.nodes k).dataTerm := hctl
            rw [h1, hldt]
          exact absurd (election_safety w hw ℓ k hrl hrole hctI) hkl
      · rcases eq_or_ne ℓ i with rfl | hkl
        · simp only [Function.update_of_ne hk] at hr hctl ⊢
          simp only [Function.update_self] at hrl hctl ⊢
          have := h.gate_durable k ℓ hr hrl hctl
          show (w.nodes k).pn.durable ≤ (w.nodes ℓ).pn.durable + 1
          omega
        · simp only [Function.update_of_ne hk] at hr hctl ⊢
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          exact h.gate_durable k ℓ hr hrl hctl
    · intro u T d hrp
      rcases eq_or_ne u i with rfl | hk
      · simp only [Function.update_self]
        exact h.report_dt u T d hrp
      · simp only [Function.update_of_ne hk]
        exact h.report_dt u T d hrp
    · intro u T d hrp hT
      obtain ⟨ℓ, hc⟩ := h.report_cert u T d hrp hT
      exact ⟨ℓ, cert_carry hs' hc⟩
    · intro u T d hrp hdtu p' t' v' hf hp'
      simp only [List.mem_append, List.mem_singleton] at hf
      rcases eq_or_ne u i with rfl | hk
      · simp only [Function.update_self] at hdtu hf hp' ⊢
        show TermMap.termAt (w.nodes u).dn.termMap p' = t'
        rcases hf with hf | hf
        · by_cases hp : p' < (w.nodes u).pn.durable
          · exact h.report_frames u T d hrp hdtu p' t' v' hf hp
          · have hp'' : p' < (w.nodes u).pn.durable + 1 := hp'
            have hdtu' : (w.nodes u).dataTerm = T := hdtu
            have hf2 : Data.Frame.replicate p' ((w.nodes u).dataTerm) t' v'
                ∈ w.dsent := by rw [hdtu']; exact hf
            rw [hldt] at hf2
            have := h.leader_frontier u hrole p' t' v' hf2
            omega
        · rw [Data.Frame.replicate.injEq] at hf
          obtain ⟨rfl, -, rfl, rfl⟩ := hf
          exact hterm_at
      · simp only [Function.update_of_ne hk] at hdtu hf hp' ⊢
        rcases hf with hf | hf
        · exact h.report_frames u T d hrp hdtu p' t' v' hf hp'
        · rw [Data.Frame.replicate.injEq] at hf
          obtain ⟨rfl, hct, rfl, rfl⟩ := hf
          have := h.report_durable u T d hrp hdtu i hrole hct.symm
          have h2 : (w.nodes i).pn.durable < (w.nodes u).pn.durable := hp'
          omega
    · intro u T d hrp hdtu ℓ hrl hctl p' hp'
      rcases eq_or_ne u i with rfl | hk
      · rcases eq_or_ne ℓ u with rfl | hkl
        · rfl
        · simp only [Function.update_self] at hdtu hp'
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          have hdtu' : (w.nodes u).dataTerm = T := hdtu
          have hctl' : (w.nodes ℓ).pn.currentTerm = T := hctl
          have hctI : (w.nodes ℓ).pn.currentTerm
              = (w.nodes u).pn.currentTerm := by
            rw [hctl', ← hdtu', hldt]
          exact absurd (election_safety w hw ℓ u hrl hrole hctI) hkl
      · rcases eq_or_ne ℓ i with rfl | hkl
        · simp only [Function.update_of_ne hk] at hdtu hp' ⊢
          simp only [Function.update_self] at hrl hctl ⊢
          exact h.report_leader_eq u T d hrp hdtu ℓ hrl hctl p' hp'
        · simp only [Function.update_of_ne hk] at hdtu hp' ⊢
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          exact h.report_leader_eq u T d hrp hdtu ℓ hrl hctl p' hp'
    · intro u T d hrp hdtu ℓ hrl hctl
      rcases eq_or_ne u i with rfl | hk
      · rcases eq_or_ne ℓ u with rfl | hkl
        · exact Nat.le_refl _
        · simp only [Function.update_self] at hdtu
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          have hdtu' : (w.nodes u).dataTerm = T := hdtu
          have hctl' : (w.nodes ℓ).pn.currentTerm = T := hctl
          have hctI : (w.nodes ℓ).pn.currentTerm
              = (w.nodes u).pn.currentTerm := by
            rw [hctl', ← hdtu', hldt]
          exact absurd (election_safety w hw ℓ u hrl hrole hctI) hkl
      · rcases eq_or_ne ℓ i with rfl | hkl
        · simp only [Function.update_of_ne hk] at hdtu ⊢
          simp only [Function.update_self] at hrl hctl ⊢
          have := h.report_durable u T d hrp hdtu ℓ hrl hctl
          show (w.nodes u).pn.durable ≤ (w.nodes ℓ).pn.durable + 1
          omega
        · simp only [Function.update_of_ne hk] at hdtu ⊢
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          exact h.report_durable u T d hrp hdtu ℓ hrl hctl
  | deliverReplicate j pos hdr t v hmsg hpos hhdr hgate =>
    -- THE accept case (and the serveTail residual's closure): the frame is
    -- header-matched to the regime and gate-open; its stamp is exactly what
    -- the regime attributes to the frontier — growth arm by construction,
    -- no-op arm by `gate_map_frame` (lower bound) + the arm itself (upper).
    have hs' := Step.deliverReplicate w j pos hdr t v hmsg hpos hhdr hgate
    have hdinv := Uc2.Data.reachable_dinv (reachable_project hw)
    have hminv := Uc2.Data.reachable_mapInv (reachable_project hw)
    have hminv' := Uc2.Data.reachable_mapInv
      (reachable_project (Relation.ReflTransGen.tail hw hs'))
    have hstamp := Uc2.Data.reachable_stamp (reachable_project hw)
    have hasc : TermMap.Ascending (w.nodes j).dn.termMap := (hminv.node j).asc
    have hasc' : TermMap.Ascending
        (Data.observeTerm (w.nodes j).dn.termMap t pos) := by
      have h0 := (hminv'.node j).asc
      simp only [World.project, Function.update_self,
        Data.Node.recvReplicate] at h0
      exact h0
    have hmsg' : Data.Frame.replicate pos ((w.nodes j).dataTerm) t v
        ∈ w.dsent := by rw [← hhdr]; exact hmsg
    have hfle : t ≤ (w.nodes j).dataTerm := by
      have := hstamp.frame_le pos hdr t v hmsg
      rwa [hhdr] at this
    have htpos : 1 ≤ t := hminv.stamp_pos pos hdr t v hmsg
    have haccept : TermMap.termAt
        (Data.observeTerm (w.nodes j).dn.termMap t pos) pos = t := by
      unfold Data.observeTerm
      by_cases hgrow : Data.lastTermOf (w.nodes j).dn.termMap < t
      · rw [if_pos hgrow]
        have hasc2 : TermMap.Ascending
            ((w.nodes j).dn.termMap ++ [(t, pos)]) := by
          have h0 := hasc'
          rwa [Data.observeTerm, if_pos hgrow] at h0
        rw [TermMap.termAt_of_last_base_le hasc2
          (Data.getLast?_append_singleton _ _) (Nat.le_refl pos)]
      · rw [if_neg hgrow]
        have hle : t ≤ Data.lastTermOf (w.nodes j).dn.termMap :=
          Nat.not_lt.mp hgrow
        have hne : (w.nodes j).dn.termMap ≠ [] := by
          intro hemp
          rw [hemp] at hle
          simp [Data.lastTermOf] at hle
          omega
        obtain ⟨lst, hlst⟩ := Option.ne_none_iff_exists'.mp
          (mt List.getLast?_eq_none_iff.mp hne)
        have hlst1 : Data.lastTermOf (w.nodes j).dn.termMap = lst.1 :=
          Data.lastTermOf_getLast hlst
        have hlb : lst.2 ≤ (w.nodes j).pn.durable :=
          (hminv.node j).last_base lst hlst
        have hup : lst.1 ≤ t :=
          h.gate_map_frame j hgate lst (List.mem_of_getLast? hlst) pos t v
            hmsg' (by omega)
        rw [TermMap.termAt_of_last_base_le hasc hlst (by omega)]
        omega
    have hpres : ∀ p', p' < pos →
        TermMap.termAt (Data.observeTerm (w.nodes j).dn.termMap t pos) p'
          = TermMap.termAt (w.nodes j).dn.termMap p' := by
      intro p' hp'
      unfold Data.observeTerm
      by_cases hgrow : Data.lastTermOf (w.nodes j).dn.termMap < t
      · rw [if_pos hgrow]
        refine TermMap.termAt_append_high ?_
        intro e he
        rw [List.mem_singleton] at he
        subst he
        exact hp'
      · rw [if_neg hgrow]
    have hjmap : (w.nodes j).pn.role = .leader →
        Data.observeTerm (w.nodes j).dn.termMap t pos
          = (w.nodes j).dn.termMap := by
      intro hrl
      refine Data.observeTerm_of_le ?_ pos
      have h1 : Data.lastTermOf (w.nodes j).dn.termMap
          = (w.nodes j).pn.currentTerm := hdinv.map_pinned j hrl
      have h2 : (w.nodes j).dataTerm = (w.nodes j).pn.currentTerm :=
        Uc2.Data.reachable_leader_dataTerm (reachable_project hw) j hrl
      omega
    refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_,
      ?_, ?_, ?_, ?_, ?_⟩
    · intro p' hdr' t' v' es hf hg
      exact h.frame_gossip p' hdr' t' v' es hf hg
    · intro p₁ p₂ hdr' t₁ t₂ v₁ v₂ h1 h2 hle
      exact h.frame_mono p₁ p₂ hdr' t₁ t₂ v₁ v₂ h1 h2 hle
    · intro p' hdr' t₁ t₂ v₁ v₂ h1 h2
      exact h.frame_uniq p' hdr' t₁ t₂ v₁ v₂ h1 h2
    · intro p' hdr' t' v' hf
      obtain ⟨ℓ, hc⟩ := h.frame_cert p' hdr' t' v' hf
      exact ⟨ℓ, cert_carry hs' hc⟩
    · intro k hnc
      rcases eq_or_ne k j with rfl | hk
      · simp only [Function.update_self] at hnc ⊢
        exact h.role_dt k hnc
      · simp only [Function.update_of_ne hk] at hnc ⊢
        exact h.role_dt k hnc
    · intro k p tv hh
      rcases eq_or_ne k j with rfl | hk
      · simp only [Function.update_self] at hh ⊢
        have hh' : Function.update (w.nodes k).dn.hist pos (some (t, v)) p
            = some tv := hh
        show p < pos + 1
        by_cases hp : p = pos
        · omega
        · rw [Function.update_of_ne hp] at hh'
          have := h.hist_bound k p tv hh'
          omega
      · simp only [Function.update_of_ne hk] at hh ⊢
        exact h.hist_bound k p tv hh
    · intro k hr
      rcases eq_or_ne k j with rfl | hk
      · simp only [Function.update_self] at hr
        have hcon : (w.nodes k).reconciled = false := hr
        rw [hgate] at hcon
        cases hcon
      · simp only [Function.update_of_ne hk] at hr ⊢
        exact h.closed_lag k hr
    · intro k p t' v' hh
      rcases eq_or_ne k j with rfl | hk
      · simp only [Function.update_self] at hh ⊢
        have hh' : Function.update (w.nodes k).dn.hist pos (some (t, v)) p
            = some (t', v') := hh
        show TermMap.termAt (Data.observeTerm (w.nodes k).dn.termMap t pos) p
          = t'
        by_cases hp : p = pos
        · subst hp
          rw [Function.update_self, Option.some.injEq, Prod.mk.injEq] at hh'
          rw [← hh'.1]
          exact haccept
        · rw [Function.update_of_ne hp] at hh'
          have hpb := h.hist_bound k p (t', v') hh'
          rw [hpres p (by omega)]
          exact h.fca k p t' v' hh'
      · simp only [Function.update_of_ne hk] at hh ⊢
        exact h.fca k p t' v' hh
    · intro ℓ hrl p' t' v' hf
      rcases eq_or_ne ℓ j with rfl | hk
      · simp only [Function.update_self] at hrl hf ⊢
        have hrl' : (w.nodes ℓ).pn.role = .leader := hrl
        have hf' : Data.Frame.replicate p' ((w.nodes ℓ).pn.currentTerm) t' v'
            ∈ w.dsent := hf
        show p' < pos + 1
        have := h.leader_frontier ℓ hrl' p' t' v' hf'
        omega
      · simp only [Function.update_of_ne hk] at hrl hf ⊢
        exact h.leader_frontier ℓ hrl p' t' v' hf
    · intro p' hdr' t' v' hf i' hrl hcti
      rcases eq_or_ne i' j with rfl | hk
      · simp only [Function.update_self] at hrl hcti ⊢
        have hrl' : (w.nodes i').pn.role = .leader := hrl
        have hcti' : (w.nodes i').pn.currentTerm = hdr' := hcti
        show TermMap.termAt
          (Data.observeTerm (w.nodes i').dn.termMap t pos) p' = t'
        rw [hjmap hrl']
        exact h.frame_leader p' hdr' t' v' hf i' hrl' hcti'
      · simp only [Function.update_of_ne hk] at hrl hcti ⊢
        exact h.frame_leader p' hdr' t' v' hf i' hrl hcti
    · intro k hr h1
      rcases eq_or_ne k j with rfl | hk
      · simp only [Function.update_self] at hr h1 ⊢
        obtain ⟨ℓ, hc⟩ := h.gate_cert k hr h1
        exact ⟨ℓ, cert_carry hs' hc⟩
      · simp only [Function.update_of_ne hk] at hr h1 ⊢
        obtain ⟨ℓ, hc⟩ := h.gate_cert k hr h1
        exact ⟨ℓ, cert_carry hs' hc⟩
    · intro k hr e he p' t' v' hf hep
      rcases eq_or_ne k j with rfl | hk
      · simp only [Function.update_self] at hr he hf ⊢
        have hf' : Data.Frame.replicate p' ((w.nodes k).dataTerm) t' v'
            ∈ w.dsent := hf
        have he' : e ∈ Data.observeTerm (w.nodes k).dn.termMap t pos := he
        revert he'
        unfold Data.observeTerm
        by_cases hgrow : Data.lastTermOf (w.nodes k).dn.termMap < t
        · rw [if_pos hgrow]
          intro he'
          rcases List.mem_append.mp he' with he2 | he2
          · exact h.gate_map_frame k hr e he2 p' t' v' hf' hep
          · rw [List.mem_singleton] at he2
            subst he2
            exact h.frame_mono pos p' _ t t' v v' hmsg' hf' hep
        · rw [if_neg hgrow]
          intro he'
          exact h.gate_map_frame k hr e he' p' t' v' hf' hep
      · simp only [Function.update_of_ne hk] at hr he hf ⊢
        exact h.gate_map_frame k hr e he p' t' v' hf hep
    · intro k ℓ hr hrl hctl e he
      rcases eq_or_ne k j with rfl | hk
      · rcases eq_or_ne ℓ k with rfl | hkl
        · simp only [Function.update_self] at he ⊢
          have he' : e ∈ Data.observeTerm (w.nodes ℓ).dn.termMap t pos := he
          show e.1 ≤ TermMap.termAt
            (Data.observeTerm (w.nodes ℓ).dn.termMap t pos) e.2
          exact TermMap.le_termAt hasc' he' (Nat.le_refl _)
        · simp only [Function.update_self] at hr hctl he
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          have hctl' : (w.nodes ℓ).pn.currentTerm = (w.nodes k).dataTerm :=
            hctl
          have hrl' : (w.nodes ℓ).pn.role = .leader := hrl
          have he' : e ∈ Data.observeTerm (w.nodes k).dn.termMap t pos := he
          revert he'
          unfold Data.observeTerm
          by_cases hgrow : Data.lastTermOf (w.nodes k).dn.termMap < t
          · rw [if_pos hgrow]
            intro he'
            rcases List.mem_append.mp he' with he2 | he2
            · exact h.gate_leader k ℓ hr hrl' hctl' e he2
            · rw [List.mem_singleton] at he2
              subst he2
              exact Nat.le_of_eq
                (h.frame_leader pos _ t v hmsg' ℓ hrl' hctl').symm
          · rw [if_neg hgrow]
            intro he'
            exact h.gate_leader k ℓ hr hrl' hctl' e he'
      · rcases eq_or_ne ℓ j with rfl | hkl
        · simp only [Function.update_of_ne hk] at hr hctl he
          simp only [Function.update_self] at hrl hctl ⊢
          have hrl' : (w.nodes ℓ).pn.role = .leader := hrl
          show e.1 ≤ TermMap.termAt
            (Data.observeTerm (w.nodes ℓ).dn.termMap t pos) e.2
          rw [hjmap hrl']
          exact h.gate_leader k ℓ hr hrl' hctl e he
        · simp only [Function.update_of_ne hk] at hr hctl he
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          exact h.gate_leader k ℓ hr hrl hctl e he
    · intro k ℓ hr hrl hctl p hp'
      rcases eq_or_ne k j with rfl | hk
      · rcases eq_or_ne ℓ k with rfl | hkl
        · rfl
        · simp only [Function.update_self] at hr hctl hp' ⊢
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          have hctl' : (w.nodes ℓ).pn.currentTerm = (w.nodes k).dataTerm :=
            hctl
          have hrl' : (w.nodes ℓ).pn.role = .leader := hrl
          have hp'' : p < pos + 1 := hp'
          show TermMap.termAt (Data.observeTerm (w.nodes k).dn.termMap t pos) p
            = TermMap.termAt (w.nodes ℓ).dn.termMap p
          by_cases hp : p = pos
          · subst hp
            rw [haccept]
            exact (h.frame_leader p _ t v hmsg' ℓ hrl' hctl').symm
          · rw [hpres p (by omega)]
            have hd := hpos
            exact h.gate_leader_eq k ℓ hr hrl' hctl' p (by omega)
      · rcases eq_or_ne ℓ j with rfl | hkl
        · simp only [Function.update_of_ne hk] at hr hctl hp' ⊢
          simp only [Function.update_self] at hrl hctl ⊢
          have hrl' : (w.nodes ℓ).pn.role = .leader := hrl
          show TermMap.termAt (w.nodes k).dn.termMap p
            = TermMap.termAt (Data.observeTerm (w.nodes ℓ).dn.termMap t pos) p
          rw [hjmap hrl']
          exact h.gate_leader_eq k ℓ hr hrl' hctl p hp'
        · simp only [Function.update_of_ne hk] at hr hctl hp' ⊢
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          exact h.gate_leader_eq k ℓ hr hrl hctl p hp'
    · intro k hr p t' v' hf hp'
      rcases eq_or_ne k j with rfl | hk
      · simp only [Function.update_self] at hr hf hp' ⊢
        have hf' : Data.Frame.replicate p ((w.nodes k).dataTerm) t' v'
            ∈ w.dsent := hf
        have hp'' : p < pos + 1 := hp'
        show TermMap.termAt (Data.observeTerm (w.nodes k).dn.termMap t pos) p
          = t'
        by_cases hp : p = pos
        · subst hp
          rw [haccept]
          exact h.frame_uniq p _ t t' v v' hmsg' hf'
        · rw [hpres p (by omega)]
          have hd := hpos
          exact h.gate_frames_eq k hr p t' v' hf' (by omega)
      · simp only [Function.update_of_ne hk] at hr hf hp' ⊢
        exact h.gate_frames_eq k hr p t' v' hf hp'
    · intro k ℓ hr hrl hctl
      rcases eq_or_ne k j with rfl | hk
      · rcases eq_or_ne ℓ k with rfl | hkl
        · exact Nat.le_refl _
        · simp only [Function.update_self] at hr hctl ⊢
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          have hctl' : (w.nodes ℓ).pn.currentTerm = (w.nodes k).dataTerm :=
            hctl
          have hrl' : (w.nodes ℓ).pn.role = .leader := hrl
          have := h.leader_frontier ℓ hrl' pos t v
            (by rw [hctl']; exact hmsg')
          show pos + 1 ≤ (w.nodes ℓ).pn.durable
          omega
      · rcases eq_or_ne ℓ j with rfl | hkl
        · simp only [Function.update_of_ne hk] at hr hctl ⊢
          simp only [Function.update_self] at hrl hctl ⊢
          have hrl' : (w.nodes ℓ).pn.role = .leader := hrl
          have := h.gate_durable k ℓ hr hrl' hctl
          show (w.nodes k).pn.durable ≤ pos + 1
          omega
        · simp only [Function.update_of_ne hk] at hr hctl ⊢
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          exact h.gate_durable k ℓ hr hrl hctl
    · intro u T d hrp
      rcases eq_or_ne u j with rfl | hk
      · simp only [Function.update_self]
        exact h.report_dt u T d hrp
      · simp only [Function.update_of_ne hk]
        exact h.report_dt u T d hrp
    · intro u T d hrp hT
      obtain ⟨ℓ, hc⟩ := h.report_cert u T d hrp hT
      exact ⟨ℓ, cert_carry hs' hc⟩
    · intro u T d hrp hdtu p t' v' hf hp'
      rcases eq_or_ne u j with rfl | hk
      · simp only [Function.update_self] at hdtu hf hp' ⊢
        have hdtu' : (w.nodes u).dataTerm = T := hdtu
        have hf' : Data.Frame.replicate p ((w.nodes u).dataTerm) t' v'
            ∈ w.dsent := by rw [hdtu']; exact hf
        have hp'' : p < pos + 1 := hp'
        show TermMap.termAt (Data.observeTerm (w.nodes u).dn.termMap t pos) p
          = t'
        by_cases hp : p = pos
        · subst hp
          rw [haccept]
          exact h.frame_uniq p _ t t' v v' hmsg' hf'
        · rw [hpres p (by omega)]
          have hd := hpos
          exact h.report_frames u T d hrp hdtu' p t' v' (by
            rw [← hdtu']; exact hf') (by omega)
      · simp only [Function.update_of_ne hk] at hdtu hf hp' ⊢
        exact h.report_frames u T d hrp hdtu p t' v' hf hp'
    · intro u T d hrp hdtu ℓ hrl hctl p hp'
      rcases eq_or_ne u j with rfl | hk
      · rcases eq_or_ne ℓ u with rfl | hkl
        · rfl
        · simp only [Function.update_self] at hdtu hp' ⊢
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          have hdtu' : (w.nodes u).dataTerm = T := hdtu
          have hctl' : (w.nodes ℓ).pn.currentTerm = T := hctl
          have hrl' : (w.nodes ℓ).pn.role = .leader := hrl
          have hp'' : p < pos + 1 := hp'
          show TermMap.termAt (Data.observeTerm (w.nodes u).dn.termMap t pos) p
            = TermMap.termAt (w.nodes ℓ).dn.termMap p
          by_cases hp : p = pos
          · subst hp
            rw [haccept]
            exact (h.frame_leader p _ t v hmsg' ℓ hrl'
              (hctl'.trans hdtu'.symm)).symm
          · rw [hpres p (by omega)]
            have hd := hpos
            exact h.report_leader_eq u T d hrp hdtu' ℓ hrl' hctl' p (by omega)
      · rcases eq_or_ne ℓ j with rfl | hkl
        · simp only [Function.update_of_ne hk] at hdtu hp' ⊢
          simp only [Function.update_self] at hrl hctl ⊢
          have hrl' : (w.nodes ℓ).pn.role = .leader := hrl
          show TermMap.termAt (w.nodes u).dn.termMap p
            = TermMap.termAt (Data.observeTerm (w.nodes ℓ).dn.termMap t pos) p
          rw [hjmap hrl']
          exact h.report_leader_eq u T d hrp hdtu ℓ hrl' hctl p hp'
        · simp only [Function.update_of_ne hk] at hdtu hp' ⊢
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          exact h.report_leader_eq u T d hrp hdtu ℓ hrl hctl p hp'
    · intro u T d hrp hdtu ℓ hrl hctl
      rcases eq_or_ne u j with rfl | hk
      · rcases eq_or_ne ℓ u with rfl | hkl
        · exact Nat.le_refl _
        · simp only [Function.update_self] at hdtu ⊢
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          have hdtu' : (w.nodes u).dataTerm = T := hdtu
          have hctl' : (w.nodes ℓ).pn.currentTerm = T := hctl
          have hrl' : (w.nodes ℓ).pn.role = .leader := hrl
          have := h.leader_frontier ℓ hrl' pos t v
            (by rw [hctl'.trans hdtu'.symm]; exact hmsg')
          show pos + 1 ≤ (w.nodes ℓ).pn.durable
          omega
      · rcases eq_or_ne ℓ j with rfl | hkl
        · simp only [Function.update_of_ne hk] at hdtu ⊢
          simp only [Function.update_self] at hrl hctl ⊢
          have hrl' : (w.nodes ℓ).pn.role = .leader := hrl
          have := h.report_durable u T d hrp hdtu ℓ hrl' hctl
          have hd := hpos
          show (w.nodes u).pn.durable ≤ pos + 1
          omega
        · simp only [Function.update_of_ne hk] at hdtu ⊢
          simp only [Function.update_of_ne hkl] at hrl hctl ⊢
          exact h.report_durable u T d hrp hdtu ℓ hrl hctl
  | serveTail i p t v hrole hhist hp =>
    -- The serveTail residual's home case: the new frame re-ships an OLD
    -- stamp under the CURRENT header; every clause that quantifies frames
    -- absorbs it via `fca` at the emitting leader + the sync clauses.
    have hs' := Step.serveTail w i p t v hrole hhist hp
    have hdinv := Uc2.Data.reachable_dinv (reachable_project hw)
    have hminv := Uc2.Data.reachable_mapInv (reachable_project hw)
    have hpInv := Uc2.reachable_inv
      (Uc2.Data.reachable_project (reachable_project hw))
    have hasc : TermMap.Ascending (w.nodes i).dn.termMap := (hminv.node i).asc
    have hfca : TermMap.termAt (w.nodes i).dn.termMap p = t :=
      h.fca i p t v hhist
    refine ⟨?_, ?_, ?_, ?_, h.role_dt, h.hist_bound, h.closed_lag, h.fca,
      ?_, ?_, ?_, ?_, h.gate_leader, h.gate_leader_eq, ?_, h.gate_durable,
      h.report_dt, ?_, ?_, h.report_leader_eq, h.report_durable⟩
    · intro p' hdr' t' v' es hf hg
      simp only [List.mem_append, List.mem_singleton] at hf hg
      rcases hg with hg | hg
      · rcases hf with hf | hf
        · exact h.frame_gossip p' hdr' t' v' es hf hg
        · rw [Data.Frame.replicate.injEq] at hf
          obtain ⟨rfl, rfl, rfl, rfl⟩ := hf
          rw [hdinv.gossip_pinned i hrole es hg]
          exact hfca
      · exact absurd hg (by simp)
    · intro p₁ p₂ hdr' t₁ t₂ v₁ v₂ h1 h2 hle
      simp only [List.mem_append, List.mem_singleton] at h1 h2
      rcases h1 with h1 | h1 <;> rcases h2 with h2 | h2
      · exact h.frame_mono p₁ p₂ hdr' t₁ t₂ v₁ v₂ h1 h2 hle
      · rw [Data.Frame.replicate.injEq] at h2
        obtain ⟨rfl, rfl, rfl, rfl⟩ := h2
        rw [← hfca, ← h.frame_leader p₁ _ t₁ v₁ h1 i hrole rfl]
        exact TermMap.termAt_mono hasc hle
      · rw [Data.Frame.replicate.injEq] at h1
        obtain ⟨rfl, rfl, rfl, rfl⟩ := h1
        rw [← hfca, ← h.frame_leader p₂ _ t₂ v₂ h2 i hrole rfl]
        exact TermMap.termAt_mono hasc hle
      · rw [Data.Frame.replicate.injEq] at h1
        rw [Data.Frame.replicate.injEq] at h2
        obtain ⟨rfl, rfl, rfl, rfl⟩ := h1
        obtain ⟨-, -, rfl, -⟩ := h2
        exact Nat.le_refl _
    · intro p' hdr' t₁ t₂ v₁ v₂ h1 h2
      simp only [List.mem_append, List.mem_singleton] at h1 h2
      rcases h1 with h1 | h1 <;> rcases h2 with h2 | h2
      · exact h.frame_uniq p' hdr' t₁ t₂ v₁ v₂ h1 h2
      · rw [Data.Frame.replicate.injEq] at h2
        obtain ⟨rfl, rfl, rfl, rfl⟩ := h2
        rw [← hfca]
        exact (h.frame_leader p' _ t₁ v₁ h1 i hrole rfl).symm
      · rw [Data.Frame.replicate.injEq] at h1
        obtain ⟨rfl, rfl, rfl, rfl⟩ := h1
        rw [← hfca]
        exact h.frame_leader p' _ t₂ v₂ h2 i hrole rfl
      · rw [Data.Frame.replicate.injEq] at h1
        rw [Data.Frame.replicate.injEq] at h2
        obtain ⟨rfl, rfl, rfl, rfl⟩ := h1
        obtain ⟨-, -, rfl, -⟩ := h2
        rfl
    · intro p' hdr' t' v' hf
      simp only [List.mem_append, List.mem_singleton] at hf
      rcases hf with hf | hf
      · obtain ⟨ℓ, hc⟩ := h.frame_cert p' hdr' t' v' hf
        exact ⟨ℓ, cert_carry hs' hc⟩
      · rw [Data.Frame.replicate.injEq] at hf
        obtain ⟨rfl, rfl, rfl, rfl⟩ := hf
        exact ⟨i, cert_carry hs' (Uc2.Data.cert_of_leader hpInv hrole)⟩
    · intro ℓ hrl p' t' v' hf
      simp only [List.mem_append, List.mem_singleton] at hf
      rcases hf with hf | hf
      · exact h.leader_frontier ℓ hrl p' t' v' hf
      · rw [Data.Frame.replicate.injEq] at hf
        obtain ⟨rfl, hct, rfl, rfl⟩ := hf
        have hli : ℓ = i := election_safety w hw ℓ i hrl hrole hct
        subst hli
        exact hp
    · intro p' hdr' t' v' hf i' hrl hcti
      simp only [List.mem_append, List.mem_singleton] at hf
      rcases hf with hf | hf
      · exact h.frame_leader p' hdr' t' v' hf i' hrl hcti
      · rw [Data.Frame.replicate.injEq] at hf
        obtain ⟨rfl, rfl, rfl, rfl⟩ := hf
        have hli : i' = i := election_safety w hw i' i hrl hrole hcti
        subst hli
        exact hfca
    · intro j hr h1
      obtain ⟨ℓ, hc⟩ := h.gate_cert j hr h1
      exact ⟨ℓ, cert_carry hs' hc⟩
    · intro j hr e he p' t' v' hf hep
      simp only [List.mem_append, List.mem_singleton] at hf
      rcases hf with hf | hf
      · exact h.gate_map_frame j hr e he p' t' v' hf hep
      · rw [Data.Frame.replicate.injEq] at hf
        obtain ⟨rfl, hdt', rfl, rfl⟩ := hf
        rw [← hfca]
        exact Nat.le_trans (h.gate_leader j i hr hrole hdt'.symm e he)
          (TermMap.termAt_mono hasc hep)
    · intro j hr p' t' v' hf hp'
      simp only [List.mem_append, List.mem_singleton] at hf
      rcases hf with hf | hf
      · exact h.gate_frames_eq j hr p' t' v' hf hp'
      · rw [Data.Frame.replicate.injEq] at hf
        obtain ⟨rfl, hdt', rfl, rfl⟩ := hf
        rw [h.gate_leader_eq j i hr hrole hdt'.symm p' hp']
        exact hfca
    · intro u T d hrp hT
      obtain ⟨ℓ, hc⟩ := h.report_cert u T d hrp hT
      exact ⟨ℓ, cert_carry hs' hc⟩
    · intro u T d hrp hdtu p' t' v' hf hp'
      simp only [List.mem_append, List.mem_singleton] at hf
      rcases hf with hf | hf
      · exact h.report_frames u T d hrp hdtu p' t' v' hf hp'
      · rw [Data.Frame.replicate.injEq] at hf
        obtain ⟨rfl, hct, rfl, rfl⟩ := hf
        rw [h.report_leader_eq u T d hrp hdtu i hrole hct.symm p' hp']
        exact hfca
  | shipTermMap i hrole =>
    -- New gossip (currentTerm i, termMap i): the only clause with a gossip
    -- on the hypothesis side is `frame_gossip`, closed by `frame_leader`
    -- (the shipped map IS the live leader's map). Frame hypotheses reject
    -- the appended gossip by constructor.
    have hs' := Step.shipTermMap w i hrole
    refine ⟨?_, ?_, ?_, ?_, h.role_dt, h.hist_bound, h.closed_lag, h.fca,
      ?_, ?_, ?_, ?_, h.gate_leader, h.gate_leader_eq, ?_, h.gate_durable,
      h.report_dt, ?_, ?_, h.report_leader_eq, h.report_durable⟩
    · intro p' hdr' t' v' es hf hg
      simp only [List.mem_append, List.mem_singleton] at hf hg
      rcases hf with hf | hf
      · rcases hg with hg | hg
        · exact h.frame_gossip p' hdr' t' v' es hf hg
        · rw [Data.Frame.gossip.injEq] at hg
          obtain ⟨rfl, rfl⟩ := hg
          exact h.frame_leader p' _ t' v' hf i hrole rfl
      · exact absurd hf (by simp)
    · intro p₁ p₂ hdr' t₁ t₂ v₁ v₂ h1 h2 hle
      simp only [List.mem_append, List.mem_singleton] at h1 h2
      rcases h1 with h1 | h1
      · rcases h2 with h2 | h2
        · exact h.frame_mono p₁ p₂ hdr' t₁ t₂ v₁ v₂ h1 h2 hle
        · exact absurd h2 (by simp)
      · exact absurd h1 (by simp)
    · intro p' hdr' t₁ t₂ v₁ v₂ h1 h2
      simp only [List.mem_append, List.mem_singleton] at h1 h2
      rcases h1 with h1 | h1
      · rcases h2 with h2 | h2
        · exact h.frame_uniq p' hdr' t₁ t₂ v₁ v₂ h1 h2
        · exact absurd h2 (by simp)
      · exact absurd h1 (by simp)
    · intro p' hdr' t' v' hf
      simp only [List.mem_append, List.mem_singleton] at hf
      rcases hf with hf | hf
      · obtain ⟨ℓ, hc⟩ := h.frame_cert p' hdr' t' v' hf
        exact ⟨ℓ, cert_carry hs' hc⟩
      · exact absurd hf (by simp)
    · intro ℓ hrl p' t' v' hf
      simp only [List.mem_append, List.mem_singleton] at hf
      rcases hf with hf | hf
      · exact h.leader_frontier ℓ hrl p' t' v' hf
      · exact absurd hf (by simp)
    · intro p' hdr' t' v' hf i' hrl hcti
      simp only [List.mem_append, List.mem_singleton] at hf
      rcases hf with hf | hf
      · exact h.frame_leader p' hdr' t' v' hf i' hrl hcti
      · exact absurd hf (by simp)
    · intro j hr h1
      obtain ⟨ℓ, hc⟩ := h.gate_cert j hr h1
      exact ⟨ℓ, cert_carry hs' hc⟩
    · intro j hr e he p' t' v' hf hep
      simp only [List.mem_append, List.mem_singleton] at hf
      rcases hf with hf | hf
      · exact h.gate_map_frame j hr e he p' t' v' hf hep
      · exact absurd hf (by simp)
    · intro j hr p' t' v' hf hp'
      simp only [List.mem_append, List.mem_singleton] at hf
      rcases hf with hf | hf
      · exact h.gate_frames_eq j hr p' t' v' hf hp'
      · exact absurd hf (by simp)
    · intro u T d hrp hT
      obtain ⟨ℓ, hc⟩ := h.report_cert u T d hrp hT
      exact ⟨ℓ, cert_carry hs' hc⟩
    · intro u T d hrp hdtu p' t' v' hf hp'
      simp only [List.mem_append, List.mem_singleton] at hf
      rcases hf with hf | hf
      · exact h.report_frames u T d hrp hdtu p' t' v' hf hp'
      · exact absurd hf (by simp)
  | deliverTermMap j t entries hmsg hterm =>
    -- reconcile-on-gossip: the receiver's map becomes a PREFIX of its old
    -- map that is simultaneously a prefix of the shipped map — so the
    -- carried gate clauses survive by entry-subset, and a (re)opened gate
    -- re-establishes them from the gossip itself (`frame_gossip`).
    have hs' := Step.deliverTermMap w j t entries hmsg hterm
    have hdinv := Uc2.Data.reachable_dinv (reachable_project hw)
    have hminv := Uc2.Data.reachable_mapInv (reachable_project hw)
    have hstamp := Uc2.Data.reachable_stamp (reachable_project hw)
    have hmldt : Data.lastTermOf (w.nodes j).dn.termMap ≤ (w.nodes j).dataTerm :=
      Uc2.Data.reachable_map_le_dataTerm (reachable_project hw) j
    have hdle : (w.nodes j).dataTerm ≤ (w.nodes j).pn.currentTerm :=
      hstamp.data_le j
    have hasc : TermMap.Ascending (w.nodes j).dn.termMap := (hminv.node j).asc
    have hgwf : TermMap.Ascending entries := hminv.gossip_wf t entries hmsg
    have hnil_noframe : entries = [] → ∀ p' t' v',
        Data.Frame.replicate p' t t' v' ∈ w.dsent → False := by
      intro hnil p' t' v' hf
      have h0 := h.frame_gossip p' t t' v' entries hf hmsg
      rw [hnil] at h0
      have h1 := hminv.stamp_pos p' t t' v' hf
      simp [TermMap.termAt] at h0
      omega
    have hroute_leader : ∀ ℓ : Fin n, (w.nodes ℓ).pn.role = .leader →
        (w.nodes ℓ).pn.currentTerm = t → entries = (w.nodes ℓ).dn.termMap := by
      intro ℓ hrl hct
      rw [← hct] at hmsg
      exact hdinv.gossip_pinned ℓ hrl entries hmsg
    cases hrec : Uc2.reconcile (w.nodes j).dn.termMap
        ((w.nodes j).dn.pn.durable) entries with
    | noCommonPrefix =>
      -- wipe-and-rejoin: empty map, zero frontier, erased history — the
      -- data-slice clauses are vacuous; regime bookkeeping as in `ok`.
      obtain ⟨hFm, hFd, hFh, hFr, hFc, hFdt⟩ :=
        Data.applyGossip_ncp (w.nodes j).dn t hrec
      have hpost_nl : ((w.nodes j).dn.applyGossip t entries).pn.role
          ≠ .leader := by
        intro hl
        rw [hFr] at hl
        by_cases hadopt : (w.nodes j).dn.pn.currentTerm < t
        · rw [if_pos hadopt] at hl
          cases hl
        · rw [if_neg hadopt] at hl
          have hterm' : (w.nodes j).dn.pn.currentTerm ≤ t := hterm
          have hteq : (w.nodes j).pn.currentTerm = t := by
            show (w.nodes j).dn.pn.currentTerm = t
            omega
          have hes := hroute_leader j hl hteq
          rw [hes, Data.reconcile_self] at hrec
          simp at hrec
      have hgate' : ((if (w.nodes j).pn.currentTerm < t then true
            else (w.nodes j).reconciled ||
              decide ((w.nodes j).dn.dataTerm = (w.nodes j).pn.currentTerm))
            = true) →
          ((w.nodes j).dn.applyGossip t entries).dataTerm = t ∨
          (((w.nodes j).dn.applyGossip t entries).dataTerm
              = (w.nodes j).dataTerm ∧ (w.nodes j).reconciled = true) := by
        intro hr
        rw [hFdt]
        by_cases hadopt : (w.nodes j).dn.pn.currentTerm < t
        · rw [if_pos hadopt]
          exact .inl rfl
        · rw [if_neg (show ¬ (w.nodes j).pn.currentTerm < t from hadopt)] at hr
          rw [if_neg hadopt]
          rw [Bool.or_eq_true] at hr
          rcases hr with hr | hr
          · exact .inr ⟨rfl, hr⟩
          · have hde : (w.nodes j).dn.dataTerm
                = (w.nodes j).dn.pn.currentTerm := of_decide_eq_true hr
            have hterm' : (w.nodes j).dn.pn.currentTerm ≤ t := hterm
            exact .inl (by omega)
      refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_,
        ?_, ?_, ?_, ?_, ?_, ?_⟩
      · intro p' hdr' t' v' es hf hg
        exact h.frame_gossip p' hdr' t' v' es hf hg
      · intro p₁ p₂ hdr' t₁ t₂ v₁ v₂ h1 h2 hle
        exact h.frame_mono p₁ p₂ hdr' t₁ t₂ v₁ v₂ h1 h2 hle
      · intro p' hdr' t₁ t₂ v₁ v₂ h1 h2
        exact h.frame_uniq p' hdr' t₁ t₂ v₁ v₂ h1 h2
      · intro p' hdr' t' v' hf
        obtain ⟨ℓ, hc⟩ := h.frame_cert p' hdr' t' v' hf
        exact ⟨ℓ, cert_carry hs' hc⟩
      · intro k hnc
        rcases eq_or_ne k j with rfl | hk
        · simp only [Function.update_self] at hnc ⊢
          show ((w.nodes k).dn.applyGossip t entries).dataTerm
            = ((w.nodes k).dn.applyGossip t entries).pn.currentTerm
          rw [hFdt, hFc]
          by_cases hadopt : (w.nodes k).dn.pn.currentTerm < t
          · rw [if_pos hadopt, if_pos hadopt]
          · rw [if_neg hadopt, if_neg hadopt]
            refine h.role_dt k ?_
            have hnc' : ((w.nodes k).dn.applyGossip t entries).pn.role
                ≠ .candidate := hnc
            rw [hFr, if_neg hadopt] at hnc'
            exact hnc'
        · simp only [Function.update_of_ne hk] at hnc ⊢
          exact h.role_dt k hnc
      · intro k p tv hh
        rcases eq_or_ne k j with rfl | hk
        · simp only [Function.update_self] at hh
          have hh' : ((w.nodes k).dn.applyGossip t entries).hist p
              = some tv := hh
          rw [hFh] at hh'
          cases hh'
        · simp only [Function.update_of_ne hk] at hh ⊢
          exact h.hist_bound k p tv hh
      · intro k hr
        rcases eq_or_ne k j with rfl | hk
        · simp only [Function.update_self] at hr ⊢
          show Data.lastTermOf
              ((w.nodes k).dn.applyGossip t entries).termMap
            < ((w.nodes k).dn.applyGossip t entries).dataTerm
          rw [hFm, hFdt]
          have hr' : (if (w.nodes k).pn.currentTerm < t then true
              else (w.nodes k).reconciled ||
                decide ((w.nodes k).dn.dataTerm
                  = (w.nodes k).pn.currentTerm)) = false := hr
          by_cases hadopt : (w.nodes k).dn.pn.currentTerm < t
          · rw [if_pos (show (w.nodes k).pn.currentTerm < t from hadopt)]
              at hr'
            cases hr'
          · rw [if_neg (show ¬ (w.nodes k).pn.currentTerm < t from hadopt)]
              at hr'
            rw [if_neg hadopt]
            have hcl : Data.lastTermOf (w.nodes k).dn.termMap
                < (w.nodes k).dn.dataTerm := h.closed_lag k (by
              rcases Bool.or_eq_false_iff.mp hr' with ⟨h1, -⟩
              exact h1)
            simp [Data.lastTermOf]
            omega
        · simp only [Function.update_of_ne hk] at hr ⊢
          exact h.closed_lag k hr
      · intro k p t' v' hh
        rcases eq_or_ne k j with rfl | hk
        · simp only [Function.update_self] at hh ⊢
          have hh' : ((w.nodes k).dn.applyGossip t entries).hist p
              = some (t', v') := hh
          rw [hFh] at hh'
          cases hh'
        · simp only [Function.update_of_ne hk] at hh ⊢
          exact h.fca k p t' v' hh
      · intro ℓ hrl p' t' v' hf
        rcases eq_or_ne ℓ j with rfl | hk
        · simp only [Function.update_self] at hrl
          exact absurd hrl hpost_nl
        · simp only [Function.update_of_ne hk] at hrl hf ⊢
          exact h.leader_frontier ℓ hrl p' t' v' hf
      · intro p' hdr' t' v' hf i' hrl hcti
        rcases eq_or_ne i' j with rfl | hk
        · simp only [Function.update_self] at hrl
          exact absurd hrl hpost_nl
        · simp only [Function.update_of_ne hk] at hrl hcti ⊢
          exact h.frame_leader p' hdr' t' v' hf i' hrl hcti
      · intro k hr h1
        rcases eq_or_ne k j with rfl | hk
        · simp only [Function.update_self] at hr h1 ⊢
          rcases hgate' hr with hreg | ⟨hreg, hpre⟩
          · obtain ⟨ℓ, hc⟩ := hdinv.cert t (.inr ⟨entries, hmsg⟩)
            refine ⟨ℓ, ?_⟩
            show Data.Cert _ (((w.nodes k).dn.applyGossip t entries).dataTerm) ℓ
            rw [hreg]
            exact cert_carry hs' hc
          · have h1' : 1 ≤ ((w.nodes k).dn.applyGossip t entries).dataTerm := h1
            rw [hreg] at h1'
            obtain ⟨ℓ, hc⟩ := h.gate_cert k hpre h1'
            refine ⟨ℓ, ?_⟩
            show Data.Cert _ (((w.nodes k).dn.applyGossip t entries).dataTerm) ℓ
            rw [hreg]
            exact cert_carry hs' hc
        · simp only [Function.update_of_ne hk] at hr h1 ⊢
          obtain ⟨ℓ, hc⟩ := h.gate_cert k hr h1
          exact ⟨ℓ, cert_carry hs' hc⟩
      · intro k hr e he p' t' v' hf hep
        rcases eq_or_ne k j with rfl | hk
        · simp only [Function.update_self] at he
          have he' : e ∈ ((w.nodes k).dn.applyGossip t entries).termMap := he
          rw [hFm] at he'
          exact absurd he' List.not_mem_nil
        · simp only [Function.update_of_ne hk] at hr he hf ⊢
          exact h.gate_map_frame k hr e he p' t' v' hf hep
      · intro k ℓ hr hrl hctl e he
        rcases eq_or_ne k j with rfl | hk
        · simp only [Function.update_self] at he
          have he' : e ∈ ((w.nodes k).dn.applyGossip t entries).termMap := he
          rw [hFm] at he'
          exact absurd he' List.not_mem_nil
        · rcases eq_or_ne ℓ j with rfl | hkl
          · simp only [Function.update_self] at hrl
            exact absurd hrl hpost_nl
          · simp only [Function.update_of_ne hk] at hr hctl he
            simp only [Function.update_of_ne hkl] at hrl hctl ⊢
            exact h.gate_leader k ℓ hr hrl hctl e he
      · intro k ℓ hr hrl hctl p hp'
        rcases eq_or_ne k j with rfl | hk
        · simp only [Function.update_self] at hp'
          have hp'' : p < ((w.nodes k).dn.applyGossip t entries).pn.durable :=
            hp'
          rw [hFd] at hp''
          omega
        · rcases eq_or_ne ℓ j with rfl | hkl
          · simp only [Function.update_self] at hrl
            exact absurd hrl hpost_nl
          · simp only [Function.update_of_ne hk] at hr hctl hp' ⊢
            simp only [Function.update_of_ne hkl] at hrl hctl ⊢
            exact h.gate_leader_eq k ℓ hr hrl hctl p hp'
      · intro k hr p' t' v' hf hp'
        rcases eq_or_ne k j with rfl | hk
        · simp only [Function.update_self] at hp'
          have hp'' : p' < ((w.nodes k).dn.applyGossip t entries).pn.durable :=
            hp'
          rw [hFd] at hp''
          omega
        · simp only [Function.update_of_ne hk] at hr hf hp' ⊢
          exact h.gate_frames_eq k hr p' t' v' hf hp'
      · intro k ℓ hr hrl hctl
        rcases eq_or_ne k j with rfl | hk
        · simp only [Function.update_self] at hr ⊢
          show ((w.nodes k).dn.applyGossip t entries).pn.durable ≤ _
          rw [hFd]
          exact Nat.zero_le _
        · rcases eq_or_ne ℓ j with rfl | hkl
          · simp only [Function.update_self] at hrl
            exact absurd hrl hpost_nl
          · simp only [Function.update_of_ne hk] at hr hctl ⊢
            simp only [Function.update_of_ne hkl] at hrl hctl ⊢
            exact h.gate_durable k ℓ hr hrl hctl
      · intro u T d hrp
        rcases eq_or_ne u j with rfl | hk
        · simp only [Function.update_self]
          show T ≤ ((w.nodes u).dn.applyGossip t entries).dataTerm
          rw [hFdt]
          have h1 : T ≤ (w.nodes u).dn.dataTerm := h.report_dt u T d hrp
          have h2 : (w.nodes u).dn.dataTerm ≤ (w.nodes u).dn.pn.currentTerm :=
            hdle
          by_cases hadopt : (w.nodes u).dn.pn.currentTerm < t
          · rw [if_pos hadopt]
            omega
          · rw [if_neg hadopt]
            exact h1
        · simp only [Function.update_of_ne hk]
          exact h.report_dt u T d hrp
      · intro u T d hrp hT
        obtain ⟨ℓ, hc⟩ := h.report_cert u T d hrp hT
        exact ⟨ℓ, cert_carry hs' hc⟩
      · intro u T d hrp hdtu p' t' v' hf hp'
        rcases eq_or_ne u j with rfl | hk
        · simp only [Function.update_self] at hdtu hp'
          have hdtu' : ((w.nodes u).dn.applyGossip t entries).dataTerm = T :=
            hdtu
          have hp'' : p' < ((w.nodes u).dn.applyGossip t entries).pn.durable :=
            hp'
          rw [hFdt] at hdtu'
          rw [hFd] at hp''
          by_cases hadopt : (w.nodes u).dn.pn.currentTerm < t
          · rw [if_pos hadopt] at hdtu'
            have h1 : T ≤ (w.nodes u).dn.dataTerm := h.report_dt u T d hrp
            have h2 : (w.nodes u).dn.dataTerm
                ≤ (w.nodes u).dn.pn.currentTerm := hdle
            omega
          · omega
        · simp only [Function.update_of_ne hk] at hdtu hf hp' ⊢
          exact h.report_frames u T d hrp hdtu p' t' v' hf hp'
      · intro u T d hrp hdtu ℓ hrl hctl p' hp'
        rcases eq_or_ne u j with rfl | hk
        · simp only [Function.update_self] at hdtu hp'
          have hdtu' : ((w.nodes u).dn.applyGossip t entries).dataTerm = T :=
            hdtu
          have hp'' : p' < ((w.nodes u).dn.applyGossip t entries).pn.durable :=
            hp'
          rw [hFdt] at hdtu'
          rw [hFd] at hp''
          by_cases hadopt : (w.nodes u).dn.pn.currentTerm < t
          · rw [if_pos hadopt] at hdtu'
            have h1 : T ≤ (w.nodes u).dn.dataTerm := h.report_dt u T d hrp
            have h2 : (w.nodes u).dn.dataTerm
                ≤ (w.nodes u).dn.pn.currentTerm := hdle
            omega
          · omega
        · rcases eq_or_ne ℓ j with rfl | hkl
          · simp only [Function.update_self] at hrl
            exact absurd hrl hpost_nl
          · simp only [Function.update_of_ne hk] at hdtu hp' ⊢
            simp only [Function.update_of_ne hkl] at hrl hctl ⊢
            exact h.report_leader_eq u T d hrp hdtu ℓ hrl hctl p' hp'
      · intro u T d hrp hdtu ℓ hrl hctl
        rcases eq_or_ne u j with rfl | hk
        · simp only [Function.update_self] at hdtu ⊢
          have hdtu' : ((w.nodes u).dn.applyGossip t entries).dataTerm = T :=
            hdtu
          rw [hFdt] at hdtu'
          by_cases hadopt : (w.nodes u).dn.pn.currentTerm < t
          · rw [if_pos hadopt] at hdtu'
            have h1 : T ≤ (w.nodes u).dn.dataTerm := h.report_dt u T d hrp
            have h2 : (w.nodes u).dn.dataTerm
                ≤ (w.nodes u).dn.pn.currentTerm := hdle
            omega
          · show ((w.nodes u).dn.applyGossip t entries).pn.durable ≤ _
            rw [hFd]
            exact Nat.zero_le _
        · rcases eq_or_ne ℓ j with rfl | hkl
          · simp only [Function.update_self] at hrl
            exact absurd hrl hpost_nl
          · simp only [Function.update_of_ne hk] at hdtu ⊢
            simp only [Function.update_of_ne hkl] at hrl hctl ⊢
            exact h.report_durable u T d hrp hdtu ℓ hrl hctl
    | ok o =>
      obtain ⟨hFm, hFd, hFh, hFr, hFc, hFdt⟩ :=
        Data.applyGossip_ok (w.nodes j).dn t hrec
      have hval_le : o.validUpTo ≤ (w.nodes j).dn.pn.durable :=
        Uc2.reconcile_validUpTo_le _ _ _ o hrec
      have hsub : ∀ e ∈ o.newMap, e ∈ (w.nodes j).dn.termMap := by
        cases entries with
        | nil =>
          rw [show Uc2.reconcile (w.nodes j).dn.termMap
              ((w.nodes j).dn.pn.durable) ([] : TermMap)
              = .ok ⟨(w.nodes j).dn.pn.durable, (w.nodes j).dn.termMap⟩
            from rfl] at hrec
          injection hrec with ho
          rw [← ho]
          exact fun e he => he
        | cons l0 ls =>
          intro e he
          rw [Uc2.reconcile_ok_newMap_take hasc hrec] at he
          exact List.mem_of_mem_take he
      have hesmem : ∀ e ∈ o.newMap, e ∈ entries ∨ entries = [] := by
        cases entries with
        | nil => exact fun e _ => .inr rfl
        | cons l0 ls =>
          intro e he
          rw [Uc2.reconcile_ok_newMap_take hasc hrec,
            Uc2.take_commonPrefixLen_eq] at he
          exact .inl (List.mem_of_mem_take he)
      have hpres : ∀ p, p < o.validUpTo →
          TermMap.termAt o.newMap p
            = TermMap.termAt (w.nodes j).dn.termMap p := by
        cases entries with
        | nil =>
          rw [show Uc2.reconcile (w.nodes j).dn.termMap
              ((w.nodes j).dn.pn.durable) ([] : TermMap)
              = .ok ⟨(w.nodes j).dn.pn.durable, (w.nodes j).dn.termMap⟩
            from rfl] at hrec
          injection hrec with ho
          rw [← ho]
          exact fun p _ => rfl
        | cons l0 ls =>
          intro p hp
          rw [Uc2.reconcile_ok_newMap_take hasc hrec]
          exact (hasc.termAt_take (fun e hke =>
            Nat.lt_of_lt_of_le hp
              (Uc2.reconcile_cuts_own_conflict _ _ _ o (by simp) hrec e
                hke))).symm
      have hes_eq : ∀ p, p < o.validUpTo → entries ≠ [] →
          TermMap.termAt o.newMap p = TermMap.termAt entries p := by
        cases entries with
        | nil => exact fun p _ hne => absurd rfl hne
        | cons l0 ls =>
          intro p hp _
          rw [Uc2.reconcile_ok_newMap_take hasc hrec,
            Uc2.take_commonPrefixLen_eq]
          exact (hgwf.termAt_take (fun f hkf =>
            Nat.lt_of_lt_of_le hp
              (Uc2.reconcile_ok_le_leader_k hrec f hkf))).symm
      have hasc2 : TermMap.Ascending o.newMap := by
        cases entries with
        | nil =>
          rw [show Uc2.reconcile (w.nodes j).dn.termMap
              ((w.nodes j).dn.pn.durable) ([] : TermMap)
              = .ok ⟨(w.nodes j).dn.pn.durable, (w.nodes j).dn.termMap⟩
            from rfl] at hrec
          injection hrec with ho
          rw [← ho]
          exact hasc
        | cons l0 ls =>
          rw [Uc2.reconcile_ok_newMap_take hasc hrec]
          exact hasc.take _
      have hlast_le : Data.lastTermOf o.newMap
          ≤ Data.lastTermOf (w.nodes j).dn.termMap := by
        cases entries with
        | nil =>
          rw [show Uc2.reconcile (w.nodes j).dn.termMap
              ((w.nodes j).dn.pn.durable) ([] : TermMap)
              = .ok ⟨(w.nodes j).dn.pn.durable, (w.nodes j).dn.termMap⟩
            from rfl] at hrec
          injection hrec with ho
          rw [← ho]
        | cons l0 ls =>
          rw [Uc2.reconcile_ok_newMap_take hasc hrec]
          exact Data.lastTermOf_take_le hasc _
      have hgossip_entry : ∀ e ∈ o.newMap, ∀ p' t' v',
          Data.Frame.replicate p' t t' v' ∈ w.dsent → e.2 ≤ p' →
          e.1 ≤ t' := by
        intro e he p' t' v' hf hep
        rcases hesmem e he with hees | hnil
        · rw [← h.frame_gossip p' t t' v' entries hf hmsg]
          exact TermMap.le_termAt hgwf hees hep
        · exact absurd hf fun hf => hnil_noframe hnil p' t' v' hf
      have hgossip_route : ∀ p' t' v',
          Data.Frame.replicate p' t t' v' ∈ w.dsent → p' < o.validUpTo →
          TermMap.termAt o.newMap p' = t' := by
        intro p' t' v' hf hp
        by_cases hnil : entries = []
        · exact absurd hf fun hf => hnil_noframe hnil p' t' v' hf
        · rw [hes_eq p' hp hnil]
          exact h.frame_gossip p' t t' v' entries hf hmsg
      have hldr_route : ∀ ℓ : Fin n, (w.nodes ℓ).pn.role = .leader →
          (w.nodes ℓ).pn.currentTerm = t →
          (∀ p, p < o.validUpTo → TermMap.termAt o.newMap p
              = TermMap.termAt (w.nodes ℓ).dn.termMap p) ∧
          (∀ e ∈ o.newMap, e.1 ≤ TermMap.termAt (w.nodes ℓ).dn.termMap e.2) ∧
          o.validUpTo ≤ (w.nodes ℓ).pn.durable := by
        intro ℓ hrl hct
        have hes := hroute_leader ℓ hrl hct
        have hnem : entries ≠ [] := by
          intro hnil
          rw [hnil] at hes
          exact absurd hes.symm ((hminv.node ℓ).leader_map hrl)
        refine ⟨?_, ?_, ?_⟩
        · intro p hp
          rw [hes_eq p hp hnem, hes]
        · intro e he
          rcases hesmem e he with hees | hnil
          · rw [hes] at hees
            exact TermMap.le_termAt (hminv.node ℓ).asc hees (Nat.le_refl _)
          · exact absurd hnil hnem
        · -- validUpTo ≤ the live t-leader's durable frontier
          cases hentries : entries with
          | nil => exact absurd hentries hnem
          | cons l0 ls =>
            subst hentries
            obtain ⟨lst, hlst⟩ := Option.ne_none_iff_exists'.mp
              (mt List.getLast?_eq_none_iff.mp (List.cons_ne_nil l0 ls))
            have hlstm : lst.2 ≤ (w.nodes ℓ).pn.durable := by
              refine (hminv.node ℓ).last_base lst ?_
              show List.getLast? (w.nodes ℓ).dn.termMap = some lst
              rw [← hes]
              exact hlst
            cases hkf : (l0 :: ls)[commonPrefixLen (w.nodes j).dn.termMap
                (l0 :: ls)]? with
            | some f =>
              have h1 := Uc2.reconcile_ok_le_leader_k hrec f hkf
              have h2 : f.2 ≤ lst.2 :=
                hgwf.base_le_getLast hlst f (List.mem_of_getElem? hkf)
              omega
            | none =>
              have hklen : (l0 :: ls).length
                  ≤ commonPrefixLen (w.nodes j).dn.termMap (l0 :: ls) :=
                List.getElem?_eq_none_iff.mp hkf
              have hestake : (w.nodes j).dn.termMap.take
                  (commonPrefixLen (w.nodes j).dn.termMap (l0 :: ls))
                  = l0 :: ls := by
                rw [Uc2.take_commonPrefixLen_eq]
                exact List.take_of_length_le hklen
              have hlastes : Data.lastTermOf (l0 :: ls) = t := by
                rw [hes]
                have := hdinv.map_pinned ℓ hrl
                rw [← hct]
                exact this
              cases hmk : (w.nodes j).dn.termMap[commonPrefixLen
                  (w.nodes j).dn.termMap (l0 :: ls)]? with
              | some e =>
                have hlt : lst.1 < e.1 := by
                  refine hasc.take_term_lt hmk lst ?_
                  rw [hestake]
                  exact List.mem_of_getLast? hlst
                have hle1 : e.1 ≤ (w.nodes j).pn.currentTerm :=
                  (hminv.node j).map_le e (List.mem_of_getElem? hmk)
                have hlst1 : lst.1 = t := by
                  rw [← hlastes, Data.lastTermOf, hlst]
                  rfl
                have hterm' : (w.nodes j).pn.currentTerm ≤ t := hterm
                omega
              | none =>
                have hmlen : (w.nodes j).dn.termMap.length
                    ≤ commonPrefixLen (w.nodes j).dn.termMap (l0 :: ls) :=
                  List.getElem?_eq_none_iff.mp hmk
                have hmeq : (w.nodes j).dn.termMap = l0 :: ls := by
                  rw [← hestake]
                  exact (List.take_of_length_le hmlen).symm
                have hlastm : Data.lastTermOf (w.nodes j).dn.termMap = t := by
                  rw [hmeq]
                  exact hlastes
                have hterm' : (w.nodes j).pn.currentTerm ≤ t := hterm
                have hdteq : (w.nodes j).dataTerm = t := by omega
                cases hpre : (w.nodes j).reconciled with
                | false =>
                  have := h.closed_lag j hpre
                  omega
                | true =>
                  have := h.gate_durable j ℓ hpre hrl (by omega)
                  have hval_le' : o.validUpTo ≤ (w.nodes j).pn.durable :=
                    hval_le
                  omega
      have hgate2 : ((if (w.nodes j).pn.currentTerm < t then true
            else (w.nodes j).reconciled ||
              decide ((w.nodes j).dn.dataTerm = (w.nodes j).pn.currentTerm))
            = true) →
          ((w.nodes j).dn.applyGossip t entries).dataTerm = t ∨
          (((w.nodes j).dn.applyGossip t entries).dataTerm
              = (w.nodes j).dataTerm ∧ (w.nodes j).reconciled = true) := by
        intro hr
        rw [hFdt]
        by_cases hadopt : (w.nodes j).dn.pn.currentTerm < t
        · rw [if_pos hadopt]
          exact .inl rfl
        · rw [if_neg (show ¬ (w.nodes j).pn.currentTerm < t from hadopt)] at hr
          rw [if_neg hadopt]
          rw [Bool.or_eq_true] at hr
          rcases hr with hr | hr
          · exact .inr ⟨rfl, hr⟩
          · have hde : (w.nodes j).dn.dataTerm
                = (w.nodes j).dn.pn.currentTerm := of_decide_eq_true hr
            have hterm' : (w.nodes j).dn.pn.currentTerm ≤ t := hterm
            exact .inl (by omega)
      have hjld : ((w.nodes j).dn.applyGossip t entries).pn.role = .leader →
          o.newMap = (w.nodes j).dn.termMap ∧
          o.validUpTo = (w.nodes j).dn.pn.durable ∧
          (w.nodes j).pn.role = .leader ∧
          ¬ (w.nodes j).dn.pn.currentTerm < t := by
        intro hl
        rw [hFr] at hl
        by_cases hadopt : (w.nodes j).dn.pn.currentTerm < t
        · rw [if_pos hadopt] at hl
          cases hl
        · rw [if_neg hadopt] at hl
          have hterm' : (w.nodes j).dn.pn.currentTerm ≤ t := hterm
          have hteq : (w.nodes j).pn.currentTerm = t := by
            show (w.nodes j).dn.pn.currentTerm = t
            omega
          have hes := hroute_leader j hl hteq
          rw [hes, Data.reconcile_self] at hrec
          injection hrec with ho
          exact ⟨by rw [← ho], by rw [← ho], hl, hadopt⟩
      refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_,
        ?_, ?_, ?_, ?_, ?_, ?_⟩
      · intro p' hdr' t' v' es hf hg
        exact h.frame_gossip p' hdr' t' v' es hf hg
      · intro p₁ p₂ hdr' t₁ t₂ v₁ v₂ h1 h2 hle
        exact h.frame_mono p₁ p₂ hdr' t₁ t₂ v₁ v₂ h1 h2 hle
      · intro p' hdr' t₁ t₂ v₁ v₂ h1 h2
        exact h.frame_uniq p' hdr' t₁ t₂ v₁ v₂ h1 h2
      · intro p' hdr' t' v' hf
        obtain ⟨ℓ, hc⟩ := h.frame_cert p' hdr' t' v' hf
        exact ⟨ℓ, cert_carry hs' hc⟩
      · intro k hnc
        rcases eq_or_ne k j with rfl | hk
        · simp only [Function.update_self] at hnc ⊢
          show ((w.nodes k).dn.applyGossip t entries).dataTerm
            = ((w.nodes k).dn.applyGossip t entries).pn.currentTerm
          rw [hFdt, hFc]
          by_cases hadopt : (w.nodes k).dn.pn.currentTerm < t
          · rw [if_pos hadopt, if_pos hadopt]
          · rw [if_neg hadopt, if_neg hadopt]
            refine h.role_dt k ?_
            have hnc' : ((w.nodes k).dn.applyGossip t entries).pn.role
                ≠ .candidate := hnc
            rw [hFr, if_neg hadopt] at hnc'
            exact hnc'
        · simp only [Function.update_of_ne hk] at hnc ⊢
          exact h.role_dt k hnc
      · intro k p tv hh
        rcases eq_or_ne k j with rfl | hk
        · simp only [Function.update_self] at hh ⊢
          have hh' : ((w.nodes k).dn.applyGossip t entries).hist p
              = some tv := hh
          rw [hFh] at hh'
          have hh2 : (if p < o.validUpTo then (w.nodes k).dn.hist p else none)
              = some tv := hh'
          show p < ((w.nodes k).dn.applyGossip t entries).pn.durable
          rw [hFd]
          by_cases hpv : p < o.validUpTo
          · exact hpv
          · rw [if_neg hpv] at hh2
            cases hh2
        · simp only [Function.update_of_ne hk] at hh ⊢
          exact h.hist_bound k p tv hh
      · intro k hr
        rcases eq_or_ne k j with rfl | hk
        · simp only [Function.update_self] at hr ⊢
          show Data.lastTermOf
              ((w.nodes k).dn.applyGossip t entries).termMap
            < ((w.nodes k).dn.applyGossip t entries).dataTerm
          rw [hFm, hFdt]
          have hr' : (if (w.nodes k).pn.currentTerm < t then true
              else (w.nodes k).reconciled ||
                decide ((w.nodes k).dn.dataTerm
                  = (w.nodes k).pn.currentTerm)) = false := hr
          by_cases hadopt : (w.nodes k).dn.pn.currentTerm < t
          · rw [if_pos (show (w.nodes k).pn.currentTerm < t from hadopt)]
              at hr'
            cases hr'
          · rw [if_neg (show ¬ (w.nodes k).pn.currentTerm < t from hadopt)]
              at hr'
            rw [if_neg hadopt]
            have hcl : Data.lastTermOf (w.nodes k).dn.termMap
                < (w.nodes k).dn.dataTerm := h.closed_lag k (by
              rcases Bool.or_eq_false_iff.mp hr' with ⟨h1, -⟩
              exact h1)
            omega
        · simp only [Function.update_of_ne hk] at hr ⊢
          exact h.closed_lag k hr
      · intro k p t' v' hh
        rcases eq_or_ne k j with rfl | hk
        · simp only [Function.update_self] at hh ⊢
          have hh' : ((w.nodes k).dn.applyGossip t entries).hist p
              = some (t', v') := hh
          rw [hFh] at hh'
          have hh2 : (if p < o.validUpTo then (w.nodes k).dn.hist p else none)
              = some (t', v') := hh'
          show TermMap.termAt
            ((w.nodes k).dn.applyGossip t entries).termMap p = t'
          rw [hFm]
          by_cases hpv : p < o.validUpTo
          · rw [if_pos hpv] at hh2
            rw [hpres p hpv]
            exact h.fca k p t' v' hh2
          · rw [if_neg hpv] at hh2
            cases hh2
        · simp only [Function.update_of_ne hk] at hh ⊢
          exact h.fca k p t' v' hh
      · intro ℓ hrl p' t' v' hf
        rcases eq_or_ne ℓ j with rfl | hk
        · simp only [Function.update_self] at hrl hf ⊢
          obtain ⟨hm0, hd0, hprl, hnadopt⟩ := hjld hrl
          have hf' : Data.Frame.replicate p'
              (((w.nodes ℓ).dn.applyGossip t entries).pn.currentTerm) t' v'
              ∈ w.dsent := hf
          rw [hFc, if_neg hnadopt] at hf'
          show p' < ((w.nodes ℓ).dn.applyGossip t entries).pn.durable
          rw [hFd, hd0]
          exact h.leader_frontier ℓ hprl p' t' v' hf'
        · simp only [Function.update_of_ne hk] at hrl hf ⊢
          exact h.leader_frontier ℓ hrl p' t' v' hf
      · intro p' hdr' t' v' hf i' hrl hcti
        rcases eq_or_ne i' j with rfl | hk
        · simp only [Function.update_self] at hrl hcti ⊢
          obtain ⟨hm0, hd0, hprl, hnadopt⟩ := hjld hrl
          have hcti' : ((w.nodes i').dn.applyGossip t entries).pn.currentTerm
              = hdr' := hcti
          rw [hFc, if_neg hnadopt] at hcti'
          show TermMap.termAt
            ((w.nodes i').dn.applyGossip t entries).termMap p' = t'
          rw [hFm, hm0]
          exact h.frame_leader p' hdr' t' v' hf i' hprl hcti'
        · simp only [Function.update_of_ne hk] at hrl hcti ⊢
          exact h.frame_leader p' hdr' t' v' hf i' hrl hcti
      · intro k hr h1
        rcases eq_or_ne k j with rfl | hk
        · simp only [Function.update_self] at hr h1 ⊢
          rcases hgate2 hr with hreg | ⟨hreg, hpre⟩
          · obtain ⟨ℓ, hc⟩ := hdinv.cert t (.inr ⟨entries, hmsg⟩)
            refine ⟨ℓ, ?_⟩
            show Data.Cert _
              (((w.nodes k).dn.applyGossip t entries).dataTerm) ℓ
            rw [hreg]
            exact cert_carry hs' hc
          · have h1' : 1 ≤ ((w.nodes k).dn.applyGossip t entries).dataTerm :=
              h1
            rw [hreg] at h1'
            obtain ⟨ℓ, hc⟩ := h.gate_cert k hpre h1'
            refine ⟨ℓ, ?_⟩
            show Data.Cert _
              (((w.nodes k).dn.applyGossip t entries).dataTerm) ℓ
            rw [hreg]
            exact cert_carry hs' hc
        · simp only [Function.update_of_ne hk] at hr h1 ⊢
          obtain ⟨ℓ, hc⟩ := h.gate_cert k hr h1
          exact ⟨ℓ, cert_carry hs' hc⟩
      · intro k hr e he p' t' v' hf hep
        rcases eq_or_ne k j with rfl | hk
        · simp only [Function.update_self] at hr he hf
          have he' : e ∈ ((w.nodes k).dn.applyGossip t entries).termMap := he
          rw [hFm] at he'
          have hf' : Data.Frame.replicate p'
              (((w.nodes k).dn.applyGossip t entries).dataTerm) t' v'
              ∈ w.dsent := hf
          rcases hgate2 hr with hreg | ⟨hreg, hpre⟩
          · rw [hreg] at hf'
            exact hgossip_entry e he' p' t' v' hf' hep
          · rw [hreg] at hf'
            exact h.gate_map_frame k hpre e (hsub e he') p' t' v' hf' hep
        · simp only [Function.update_of_ne hk] at hr he hf ⊢
          exact h.gate_map_frame k hr e he p' t' v' hf hep
      · intro k ℓ hr hrl hctl e he
        rcases eq_or_ne k j with rfl | hk
        · rcases eq_or_ne ℓ k with rfl | hkl
          · simp only [Function.update_self] at he ⊢
            have he' : e ∈ ((w.nodes ℓ).dn.applyGossip t entries).termMap := he
            rw [hFm] at he'
            show e.1 ≤ TermMap.termAt
              ((w.nodes ℓ).dn.applyGossip t entries).termMap e.2
            rw [hFm]
            exact TermMap.le_termAt hasc2 he' (Nat.le_refl _)
          · simp only [Function.update_self] at hr hctl he
            simp only [Function.update_of_ne hkl] at hrl hctl ⊢
            have hrl' : (w.nodes ℓ).pn.role = .leader := hrl
            have he' : e ∈ ((w.nodes k).dn.applyGossip t entries).termMap := he
            rw [hFm] at he'
            have hctl' : (w.nodes ℓ).pn.currentTerm
                = ((w.nodes k).dn.applyGossip t entries).dataTerm := hctl
            rcases hgate2 hr with hreg | ⟨hreg, hpre⟩
            · rw [hreg] at hctl'
              exact (hldr_route ℓ hrl' hctl').2.1 e he'
            · rw [hreg] at hctl'
              exact h.gate_leader k ℓ hpre hrl' hctl' e (hsub e he')
        · rcases eq_or_ne ℓ j with rfl | hkl
          · simp only [Function.update_of_ne hk] at hr hctl he
            simp only [Function.update_self] at hrl hctl ⊢
            obtain ⟨hm0, hd0, hprl, hnadopt⟩ := hjld hrl
            have hctl' : ((w.nodes ℓ).dn.applyGossip t entries).pn.currentTerm
                = (w.nodes k).dataTerm := hctl
            rw [hFc, if_neg hnadopt] at hctl'
            show e.1 ≤ TermMap.termAt
              ((w.nodes ℓ).dn.applyGossip t entries).termMap e.2
            rw [hFm, hm0]
            exact h.gate_leader k ℓ hr hprl hctl' e he
          · simp only [Function.update_of_ne hk] at hr hctl he
            simp only [Function.update_of_ne hkl] at hrl hctl ⊢
            exact h.gate_leader k ℓ hr hrl hctl e he
      · intro k ℓ hr hrl hctl p hp'
        rcases eq_or_ne k j with rfl | hk
        · rcases eq_or_ne ℓ k with rfl | hkl
          · rfl
          · simp only [Function.update_self] at hr hctl hp' ⊢
            simp only [Function.update_of_ne hkl] at hrl hctl ⊢
            have hrl' : (w.nodes ℓ).pn.role = .leader := hrl
            have hp'' : p < ((w.nodes k).dn.applyGossip t entries).pn.durable
              := hp'
            rw [hFd] at hp''
            have hctl' : (w.nodes ℓ).pn.currentTerm
                = ((w.nodes k).dn.applyGossip t entries).dataTerm := hctl
            show TermMap.termAt
              ((w.nodes k).dn.applyGossip t entries).termMap p
              = TermMap.termAt (w.nodes ℓ).dn.termMap p
            rw [hFm]
            rcases hgate2 hr with hreg | ⟨hreg, hpre⟩
            · rw [hreg] at hctl'
              exact (hldr_route ℓ hrl' hctl').1 p hp''
            · rw [hreg] at hctl'
              rw [hpres p hp'']
              have hval' : o.validUpTo ≤ (w.nodes k).pn.durable := hval_le
              exact h.gate_leader_eq k ℓ hpre hrl' hctl' p (by omega)
        · rcases eq_or_ne ℓ j with rfl | hkl
          · simp only [Function.update_of_ne hk] at hr hctl hp' ⊢
            simp only [Function.update_self] at hrl hctl ⊢
            obtain ⟨hm0, hd0, hprl, hnadopt⟩ := hjld hrl
            have hctl' : ((w.nodes ℓ).dn.applyGossip t entries).pn.currentTerm
                = (w.nodes k).dataTerm := hctl
            rw [hFc, if_neg hnadopt] at hctl'
            show TermMap.termAt (w.nodes k).dn.termMap p
              = TermMap.termAt
                ((w.nodes ℓ).dn.applyGossip t entries).termMap p
            rw [hFm, hm0]
            exact h.gate_leader_eq k ℓ hr hprl hctl' p hp'
          · simp only [Function.update_of_ne hk] at hr hctl hp' ⊢
            simp only [Function.update_of_ne hkl] at hrl hctl ⊢
            exact h.gate_leader_eq k ℓ hr hrl hctl p hp'
      · intro k hr p' t' v' hf hp'
        rcases eq_or_ne k j with rfl | hk
        · simp only [Function.update_self] at hr hf hp' ⊢
          have hf' : Data.Frame.replicate p'
              (((w.nodes k).dn.applyGossip t entries).dataTerm) t' v'
              ∈ w.dsent := hf
          have hp'' : p' < ((w.nodes k).dn.applyGossip t entries).pn.durable :=
            hp'
          rw [hFd] at hp''
          show TermMap.termAt
            ((w.nodes k).dn.applyGossip t entries).termMap p' = t'
          rw [hFm]
          rcases hgate2 hr with hreg | ⟨hreg, hpre⟩
          · rw [hreg] at hf'
            exact hgossip_route p' t' v' hf' hp''
          · rw [hreg] at hf'
            rw [hpres p' hp'']
            have hval' : o.validUpTo ≤ (w.nodes k).pn.durable := hval_le
            exact h.gate_frames_eq k hpre p' t' v' hf' (by omega)
        · simp only [Function.update_of_ne hk] at hr hf hp' ⊢
          exact h.gate_frames_eq k hr p' t' v' hf hp'
      · intro k ℓ hr hrl hctl
        rcases eq_or_ne k j with rfl | hk
        · rcases eq_or_ne ℓ k with rfl | hkl
          · exact Nat.le_refl _
          · simp only [Function.update_self] at hr hctl ⊢
            simp only [Function.update_of_ne hkl] at hrl hctl ⊢
            have hrl' : (w.nodes ℓ).pn.role = .leader := hrl
            have hctl' : (w.nodes ℓ).pn.currentTerm
                = ((w.nodes k).dn.applyGossip t entries).dataTerm := hctl
            show ((w.nodes k).dn.applyGossip t entries).pn.durable
              ≤ (w.nodes ℓ).pn.durable
            rw [hFd]
            rcases hgate2 hr with hreg | ⟨hreg, hpre⟩
            · rw [hreg] at hctl'
              exact (hldr_route ℓ hrl' hctl').2.2
            · rw [hreg] at hctl'
              have h2 := h.gate_durable k ℓ hpre hrl' hctl'
              have hval' : o.validUpTo ≤ (w.nodes k).pn.durable := hval_le
              omega
        · rcases eq_or_ne ℓ j with rfl | hkl
          · simp only [Function.update_of_ne hk] at hr hctl ⊢
            simp only [Function.update_self] at hrl hctl ⊢
            obtain ⟨hm0, hd0, hprl, hnadopt⟩ := hjld hrl
            have hctl' : ((w.nodes ℓ).dn.applyGossip t entries).pn.currentTerm
                = (w.nodes k).dataTerm := hctl
            rw [hFc, if_neg hnadopt] at hctl'
            show (w.nodes k).pn.durable
              ≤ ((w.nodes ℓ).dn.applyGossip t entries).pn.durable
            rw [hFd, hd0]
            exact h.gate_durable k ℓ hr hprl hctl'
          · simp only [Function.update_of_ne hk] at hr hctl ⊢
            simp only [Function.update_of_ne hkl] at hrl hctl ⊢
            exact h.gate_durable k ℓ hr hrl hctl
      · intro u T d hrp
        rcases eq_or_ne u j with rfl | hk
        · simp only [Function.update_self]
          show T ≤ ((w.nodes u).dn.applyGossip t entries).dataTerm
          rw [hFdt]
          have h1 : T ≤ (w.nodes u).dn.dataTerm := h.report_dt u T d hrp
          have h2 : (w.nodes u).dn.dataTerm ≤ (w.nodes u).dn.pn.currentTerm :=
            hdle
          by_cases hadopt : (w.nodes u).dn.pn.currentTerm < t
          · rw [if_pos hadopt]
            omega
          · rw [if_neg hadopt]
            exact h1
        · simp only [Function.update_of_ne hk]
          exact h.report_dt u T d hrp
      · intro u T d hrp hT
        obtain ⟨ℓ, hc⟩ := h.report_cert u T d hrp hT
        exact ⟨ℓ, cert_carry hs' hc⟩
      · intro u T d hrp hdtu p' t' v' hf hp'
        rcases eq_or_ne u j with rfl | hk
        · simp only [Function.update_self] at hdtu hp' ⊢
          have hdtu' : ((w.nodes u).dn.applyGossip t entries).dataTerm = T :=
            hdtu
          have hp'' : p' < ((w.nodes u).dn.applyGossip t entries).pn.durable :=
            hp'
          rw [hFdt] at hdtu'
          rw [hFd] at hp''
          show TermMap.termAt
            ((w.nodes u).dn.applyGossip t entries).termMap p' = t'
          rw [hFm]
          by_cases hadopt : (w.nodes u).dn.pn.currentTerm < t
          · rw [if_pos hadopt] at hdtu'
            have h1 : T ≤ (w.nodes u).dn.dataTerm := h.report_dt u T d hrp
            have h2 : (w.nodes u).dn.dataTerm
                ≤ (w.nodes u).dn.pn.currentTerm := hdle
            omega
          · rw [if_neg hadopt] at hdtu'
            rw [hpres p' hp'']
            have hval' : o.validUpTo ≤ (w.nodes u).pn.durable := hval_le
            exact h.report_frames u T d hrp
              (show (w.nodes u).dataTerm = T from hdtu') p' t' v' hf
              (by omega)
        · simp only [Function.update_of_ne hk] at hdtu hp' ⊢
          exact h.report_frames u T d hrp hdtu p' t' v' hf hp'
      · intro u T d hrp hdtu ℓ hrl hctl p' hp'
        rcases eq_or_ne u j with rfl | hk
        · rcases eq_or_ne ℓ u with rfl | hkl
          · rfl
          · simp only [Function.update_self] at hdtu hp' ⊢
            simp only [Function.update_of_ne hkl] at hrl hctl ⊢
            have hdtu' : ((w.nodes u).dn.applyGossip t entries).dataTerm = T :=
              hdtu
            have hp'' : p' < ((w.nodes u).dn.applyGossip t entries).pn.durable
              := hp'
            rw [hFdt] at hdtu'
            rw [hFd] at hp''
            show TermMap.termAt
              ((w.nodes u).dn.applyGossip t entries).termMap p'
              = TermMap.termAt (w.nodes ℓ).dn.termMap p'
            rw [hFm]
            by_cases hadopt : (w.nodes u).dn.pn.currentTerm < t
            · rw [if_pos hadopt] at hdtu'
              have h1 : T ≤ (w.nodes u).dn.dataTerm := h.report_dt u T d hrp
              have h2 : (w.nodes u).dn.dataTerm
                  ≤ (w.nodes u).dn.pn.currentTerm := hdle
              omega
            · rw [if_neg hadopt] at hdtu'
              rw [hpres p' hp'']
              have hval' : o.validUpTo ≤ (w.nodes u).pn.durable := hval_le
              exact h.report_leader_eq u T d hrp
                (show (w.nodes u).dataTerm = T from hdtu') ℓ hrl hctl p'
                (by omega)
        · rcases eq_or_ne ℓ j with rfl | hkl
          · simp only [Function.update_of_ne hk] at hdtu hp' ⊢
            simp only [Function.update_self] at hrl hctl ⊢
            obtain ⟨hm0, hd0, hprl, hnadopt⟩ := hjld hrl
            have hctl' : ((w.nodes ℓ).dn.applyGossip t entries).pn.currentTerm
                = T := hctl
            rw [hFc, if_neg hnadopt] at hctl'
            show TermMap.termAt (w.nodes u).dn.termMap p'
              = TermMap.termAt
                ((w.nodes ℓ).dn.applyGossip t entries).termMap p'
            rw [hFm, hm0]
            exact h.report_leader_eq u T d hrp hdtu ℓ hprl hctl' p' hp'
          · simp only [Function.update_of_ne hk] at hdtu hp' ⊢
            simp only [Function.update_of_ne hkl] at hrl hctl ⊢
            exact h.report_leader_eq u T d hrp hdtu ℓ hrl hctl p' hp'
      · intro u T d hrp hdtu ℓ hrl hctl
        rcases eq_or_ne u j with rfl | hk
        · rcases eq_or_ne ℓ u with rfl | hkl
          · exact Nat.le_refl _
          · simp only [Function.update_self] at hdtu ⊢
            simp only [Function.update_of_ne hkl] at hrl hctl ⊢
            have hdtu' : ((w.nodes u).dn.applyGossip t entries).dataTerm = T :=
              hdtu
            rw [hFdt] at hdtu'
            show ((w.nodes u).dn.applyGossip t entries).pn.durable
              ≤ (w.nodes ℓ).pn.durable
            rw [hFd]
            by_cases hadopt : (w.nodes u).dn.pn.currentTerm < t
            · rw [if_pos hadopt] at hdtu'
              have h1 : T ≤ (w.nodes u).dn.dataTerm := h.report_dt u T d hrp
              have h2 : (w.nodes u).dn.dataTerm
                  ≤ (w.nodes u).dn.pn.currentTerm := hdle
              omega
            · rw [if_neg hadopt] at hdtu'
              have h2 := h.report_durable u T d hrp
                (show (w.nodes u).dataTerm = T from hdtu') ℓ hrl hctl
              have hval' : o.validUpTo ≤ (w.nodes u).pn.durable := hval_le
              omega
        · rcases eq_or_ne ℓ j with rfl | hkl
          · simp only [Function.update_of_ne hk] at hdtu ⊢
            simp only [Function.update_self] at hrl hctl ⊢
            obtain ⟨hm0, hd0, hprl, hnadopt⟩ := hjld hrl
            have hctl' : ((w.nodes ℓ).dn.applyGossip t entries).pn.currentTerm
                = T := hctl
            rw [hFc, if_neg hnadopt] at hctl'
            show (w.nodes u).pn.durable
              ≤ ((w.nodes ℓ).dn.applyGossip t entries).pn.durable
            rw [hFd, hd0]
            exact h.report_durable u T d hrp hdtu ℓ hprl hctl'
          · simp only [Function.update_of_ne hk] at hdtu ⊢
            simp only [Function.update_of_ne hkl] at hrl hctl ⊢
            exact h.report_durable u T d hrp hdtu ℓ hrl hctl
  | sendReport j hrole hgate =>
    -- The report plane's ESTABLISHMENT case: the new report is truthful at
    -- send, and every report clause is seeded by its gate-plane twin (a
    -- follower's handle equals its term, `role_dt`).
    have hs' := Step.sendReport w j hrole hgate
    have hjdt : (w.nodes j).dataTerm = (w.nodes j).pn.currentTerm :=
      h.role_dt j (by rw [hrole]; decide)
    refine ⟨h.frame_gossip, h.frame_mono, h.frame_uniq, ?_, h.role_dt,
      h.hist_bound, h.closed_lag, h.fca, h.leader_frontier, h.frame_leader,
      ?_, h.gate_map_frame, h.gate_leader, h.gate_leader_eq,
      h.gate_frames_eq, h.gate_durable, ?_, ?_, ?_, ?_, ?_⟩
    · intro p' hdr' t' v' hf
      obtain ⟨ℓ, hc⟩ := h.frame_cert p' hdr' t' v' hf
      exact ⟨ℓ, cert_carry hs' hc⟩
    · intro j' hr h1
      obtain ⟨ℓ, hc⟩ := h.gate_cert j' hr h1
      exact ⟨ℓ, cert_carry hs' hc⟩
    · intro u T d hrp
      simp only [List.mem_append, List.mem_singleton] at hrp
      rcases hrp with hrp | hrp
      · exact h.report_dt u T d hrp
      · rw [CMsg.report.injEq] at hrp
        obtain ⟨rfl, rfl, rfl⟩ := hrp
        rw [hjdt]
    · intro u T d hrp hT
      simp only [List.mem_append, List.mem_singleton] at hrp
      rcases hrp with hrp | hrp
      · obtain ⟨ℓ, hc⟩ := h.report_cert u T d hrp hT
        exact ⟨ℓ, cert_carry hs' hc⟩
      · rw [CMsg.report.injEq] at hrp
        obtain ⟨rfl, rfl, rfl⟩ := hrp
        obtain ⟨ℓ, hc⟩ := h.gate_cert u hgate (by rw [hjdt]; exact hT)
        refine ⟨ℓ, cert_carry hs' ?_⟩
        rwa [hjdt] at hc
    · intro u T d hrp hdtu p' t' v' hf hp'
      simp only [List.mem_append, List.mem_singleton] at hrp
      rcases hrp with hrp | hrp
      · exact h.report_frames u T d hrp hdtu p' t' v' hf hp'
      · rw [CMsg.report.injEq] at hrp
        obtain ⟨rfl, rfl, rfl⟩ := hrp
        exact h.gate_frames_eq u hgate p' t' v' (by rwa [hdtu]) hp'
    · intro u T d hrp hdtu ℓ hrl hctl p' hp'
      simp only [List.mem_append, List.mem_singleton] at hrp
      rcases hrp with hrp | hrp
      · exact h.report_leader_eq u T d hrp hdtu ℓ hrl hctl p' hp'
      · rw [CMsg.report.injEq] at hrp
        obtain ⟨rfl, rfl, rfl⟩ := hrp
        exact h.gate_leader_eq u ℓ hgate hrl (hctl.trans hdtu.symm) p' hp'
    · intro u T d hrp hdtu ℓ hrl hctl
      simp only [List.mem_append, List.mem_singleton] at hrp
      rcases hrp with hrp | hrp
      · exact h.report_durable u T d hrp hdtu ℓ hrl hctl
      · rw [CMsg.report.injEq] at hrp
        obtain ⟨rfl, rfl, rfl⟩ := hrp
        exact h.gate_durable u ℓ hgate hrl (hctl.trans hdtu.symm)
  | deliverReport i src t d hmsg hrole hterm hsrc =>
    refine provinv_transport
      (Step.deliverReport w i src t d hmsg hrole hterm hsrc) h
      ?_ ?_ ?_ ?_ ?_ ?_ ?_ rfl rfl <;>
    · intro k
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self, Node.pn, Node.hist, Node.dataTerm]
      · simp only [Function.update_of_ne hne]
  | leaderAdvanceCommit i k hrole hbase hadv =>
    refine provinv_transport
      (Step.leaderAdvanceCommit w i k hrole hbase hadv) h
      ?_ ?_ ?_ ?_ ?_ ?_ ?_ rfl rfl <;>
    · intro k'
      rcases eq_or_ne k' i with rfl | hne
      · simp only [Function.update_self, Node.pn, Node.hist, Node.dataTerm]
      · simp only [Function.update_of_ne hne]

/-- The bundle holds in every reachable commit-layer world — LC4's consumer
surface (the report plane + `fca` + the frame/gate sync machinery). -/
theorem reachable_provInv {n : Nat} {w : World n} (hw : Reachable w) :
    ProvInv w := by
  have h : Relation.ReflTransGen Step (World.init n) w := hw
  clear hw
  induction h with
  | refl => exact provinv_init n
  | tail hsteps hstep ih => exact provinv_step hsteps ih hstep

/-- **`FramesCurrentAuthored`, DISCHARGED** (LC3): a node's held content
always agrees with what its OWN term map attributes to that position, in
every reachable world — no per-world hypothesis. The FIXED predicate lives
in `LeaderCompleteness.lean`; the LC endgame consumes this as a lemma. -/
theorem frames_current_authored {n : Nat} {w : World n} (hw : Reachable w) :
    FramesCurrentAuthored w :=
  (reachable_provInv hw).fca

#print axioms reachable_provInv
#print axioms frames_current_authored

end Cert

-- The new public toolkit surface (this file):
#print axioms TermMap.le_termAt
#print axioms TermMap.termAt_mono
#print axioms TermMap.termAt_le_lastTermOf
#print axioms TermMap.termAt_of_last_base_le
#print axioms TermMap.term_le_lastTermOf
#print axioms reconcile_ok_le_leader_k
#print axioms Data.cert_dstep
#print axioms Data.cert_drtg
-- De-privatized in LC3 (LogMatching.lean / MapWF.lean), re-checked here:
#print axioms Data.reconcile_self
#print axioms Data.recv_voted_current
#print axioms Data.applyGossip_ok
#print axioms Data.applyGossip_ncp
#print axioms Data.recvReplicate_fields
#print axioms Data.lastTermOf_take_le
#print axioms Data.lastTermOf_observeTerm
#print axioms Data.prunePush_prefix
#print axioms Data.getLast?_append_singleton

end Uc2
