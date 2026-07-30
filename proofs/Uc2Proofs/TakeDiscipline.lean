import Uc2Proofs.LcClosure

/-! LC4b Stage A — the take-discipline bundle.

Entry-LEVEL map structure, the machinery the LC4 blocker analysis showed is
forced by `reconcile`'s entry-mismatch clamp (review verdict 2:
CONFIRMED-NO-CHEAPER-CLOSE). Six mutually-inductive clauses over the
commit-layer world:

- `strict_node`/`strict_gossip` — consecutive entry bases STRICTLY
  increase (no mid-map phantoms; the attribution-invisible-yet-cut-relevant
  objects of the review's verdict 2 never form).
- `gate_take` — a gate-open node's map IS a take (entry prefix) of every
  gossip at its regime term, and its durable never crosses the first
  beyond-take entry's base.
- `open_leader` — the live-leader twin (vs the regime leader's map), which
  seeds `gate_take` for a tenure's FIRST gossip at `shipTermMap`.
- `report_take` — the reporter twin, keyed on `dataTerm u = T` (NOT the
  gate), so it survives crash-closed windows; carries BOTH the gossip and
  live-leader forms. Per F-LC4-1 (machine-checked:
  `bare_report_durable_stability_is_false`) it carries NO bare durable
  floor — the floor is commit/RepQuorum-conditioned and lives in Stage B.
- `gate_frontier_eq` — at a zero-width frontier entry (base = durable),
  regime-stream frames at that base carry exactly the frontier's term.
  The pin that closes strictness preservation at winner-self re-accepts
  (the LC4 report's "least-verified corner", resolved by this clause).

All clauses are BARE (no RepQuorum conditioning) — audited against the
F-LC4-1 zero-cut: a total wipe leaves every clause trivially true
(`[] = es.take 0`, `0 ≤ base`), and truncation output is always a prefix
of OWN (`reconcile_ok_newMap_take`), so take-facts degrade to shorter
takes, never to falsehood. -/

namespace Uc2.Cert

open Uc2.Data (Frame)

/-! ## Strict bases: the phantom-free map shape -/

/-- Consecutive entry bases strictly increase. Strictly stronger than
`Ascending`'s non-strict base ordering; rules out the zero-width phantom
pairs that are invisible to `termAt` but move `reconcile`'s cut. -/
def StrictBases (m : TermMap) : Prop :=
  m.IsChain (fun a b => a.2 < b.2)

theorem StrictBases.nil : StrictBases [] := List.IsChain.nil

/-- A prefix of a strict map is strict. -/
theorem StrictBases.prefix' {m m' : TermMap} (h : StrictBases m)
    (hp : m' <+: m) : StrictBases m' :=
  List.IsChain.prefix h hp

/-- Index-monotone bases (transitive closure of the chain). -/
theorem StrictBases.base_lt {m : TermMap} (h : StrictBases m) :
    ∀ {i j : Nat} (_ : i < j) (hj : j < m.length),
      (m[i]'(by omega)).2 < (m[j]'hj).2 := by
  intro i j hij hj
  induction j with
  | zero => omega
  | succ j' ih =>
    have hstep : ∀ (hj' : j' + 1 < m.length),
        (m[j']'(by omega)).2 < (m[j' + 1]'hj').2 := fun hj' =>
      List.IsChain.getElem h j' hj'
    rcases Nat.lt_or_ge i j' with hlt | hge
    · exact Nat.lt_trans (ih hlt (by omega)) (hstep hj)
    · have : i = j' := by omega
      subst this
      exact hstep hj

/-- Entries after a member entry (by index) have strictly larger bases. -/
theorem StrictBases.drop_lt {m : TermMap} (h : StrictBases m) {i : Nat}
    (hi : i < m.length) :
    ∀ e ∈ m.drop (i + 1), (m[i]'hi).2 < e.2 := by
  intro e he
  obtain ⟨j, hj, rfl⟩ := List.getElem_of_mem he
  rw [List.getElem_drop]
  have hlen : (m.drop (i + 1)).length = m.length - (i + 1) :=
    List.length_drop
  exact h.base_lt (by omega) (by omega)

/-! ## `termAt` over strict maps: entry-base pinning -/

/-- Over an ascending+strict map, `termAt` at a member entry's base IS that
entry's term (later entries sit strictly above the base; the entry itself
covers it). -/
theorem termAt_entry_base {m : TermMap} (hasc : TermMap.Ascending m)
    (hstr : StrictBases m) {i : Nat} {e : Nat × Nat} (he : m[i]? = some e) :
    TermMap.termAt m e.2 = e.1 := by
  have hi : i < m.length := by
    by_contra hc
    rw [List.getElem?_eq_none (by omega)] at he
    cases he
  have hie : m[i]'hi = e := by
    rw [List.getElem?_eq_getElem hi] at he
    exact Option.some.inj he
  have hlast : (m.take (i + 1)).getLast? = some e := by
    rw [List.take_add_one, he]
    exact Data.getLast?_append_singleton _ _
  have hasct : TermMap.Ascending (m.take (i + 1)) := hasc.take (i + 1)
  have hdrop : ∀ x ∈ m.drop (i + 1), e.2 < x.2 := by
    intro x hx
    have h := hstr.drop_lt hi x hx
    rwa [hie] at h
  conv_lhs => rw [show m = m.take (i + 1) ++ m.drop (i + 1) from
    (List.take_append_drop _ m).symm]
  rw [TermMap.termAt_append_high hdrop]
  exact TermMap.termAt_of_last_base_le hasct hlast (Nat.le_refl _)

/-- **The growth pin.** If a map is a take of an ascending+strict `es`
whose next entry (if any) sits at-or-above `pos`, and the stream attributes
`pos` to a term STRICTLY above the take's frontier, then `es`'s next entry
is exactly `(t, pos)`. -/
theorem take_growth_pin {es : TermMap} {len pos t : Nat}
    (hasc : TermMap.Ascending es) (hstr : StrictBases es)
    (hnext : ∀ f ∈ es[len]?, pos ≤ f.2)
    (hattr : TermMap.termAt es pos = t)
    (hgrow : Data.lastTermOf (es.take len) < t) :
    es[len]? = some (t, pos) := by
  cases hcase : es[len]? with
  | none =>
    exfalso
    have hle : es.length ≤ len := by
      by_contra hc
      rw [List.getElem?_eq_getElem (by omega)] at hcase
      cases hcase
    rw [List.take_of_length_le hle] at hgrow
    have h := TermMap.termAt_le_lastTermOf hasc pos
    omega
  | some f =>
    have hflen : len < es.length := by
      by_contra hc
      rw [List.getElem?_eq_none (by omega)] at hcase
      cases hcase
    have hfe : es[len]'hflen = f := by
      rw [List.getElem?_eq_getElem hflen] at hcase
      exact Option.some.inj hcase
    have hple : pos ≤ f.2 := hnext f hcase
    rcases Nat.lt_or_ge pos f.2 with hlt | hge
    · exfalso
      have hhigh : ∀ e ∈ es.drop len, pos < e.2 := by
        intro e he
        obtain ⟨j, hj, rfl⟩ := List.getElem_of_mem he
        rw [List.getElem_drop]
        have hjlen : (es.drop len).length = es.length - len :=
          List.length_drop
        rcases Nat.eq_zero_or_pos j with rfl | hjpos
        · simp only [Nat.add_zero]
          rw [hfe]
          omega
        · have hb := hstr.base_lt (i := len) (j := len + j) (by omega)
            (by omega)
          rw [hfe] at hb
          omega
      conv at hattr => rw [show es = es.take len ++ es.drop len from
        (List.take_append_drop _ es).symm]
      rw [TermMap.termAt_append_high hhigh] at hattr
      have h := TermMap.termAt_le_lastTermOf (hasc.take len) pos
      omega
    · have hpe : pos = f.2 := by omega
      have hattr' : TermMap.termAt es f.2 = f.1 :=
        termAt_entry_base hasc hstr hcase
      have hf : f = (t, pos) := by
        have h1 : f.1 = t := by rw [← hattr', ← hpe, hattr]
        have h2 : f.2 = pos := hpe.symm
        cases f
        simp_all
      rw [hf]

/-! ## Construction-site support: `prunePush` and `reconcile` keep strict/take -/

private theorem dropWhile_head?_not {p : Nat × Nat → Bool} :
    ∀ (l : TermMap) {x : Nat × Nat}, (l.dropWhile p).head? = some x →
      p x = false
  | [], x, h => by cases h
  | a :: l, x, h => by
    by_cases ha : p a
    · rw [List.dropWhile_cons_of_pos ha] at h
      exact dropWhile_head?_not l h
    · rw [List.dropWhile_cons_of_neg ha] at h
      cases h
      exact Bool.eq_false_iff.mpr ha

/-- `become_leader`'s pruned push preserves strict bases: the kept slice is
a prefix (strict), and its last surviving base is ≠ `d` (it survived the
same-base drop) yet ≤ `d` (bases are bounded by the frontier ≤ durable) —
so strictly below the pushed `(c, d)`. -/
private theorem strict_prunePush {m : TermMap} {c d : Nat}
    (hstr : StrictBases m) (hasc : TermMap.Ascending m)
    (hlast : ∀ e, m.getLast? = some e → e.2 ≤ d) :
    StrictBases (Data.prunePush m c d) := by
  show ((m.reverse.dropWhile (fun e => e.2 == d)).reverse ++ [(c, d)]).IsChain _
  rw [List.isChain_append]
  refine ⟨hstr.prefix' (Data.prunePush_prefix m d), ?_, ?_⟩
  · exact List.IsChain.nil.cons (by simp)
  · intro x hx y hy
    simp only [List.head?_cons, Option.mem_def, Option.some.injEq] at hy
    subst hy
    rw [Option.mem_def, List.getLast?_reverse] at hx
    have hne : (x.2 == d) = false := dropWhile_head?_not _ hx
    have hxin : x ∈ m.reverse.dropWhile (fun e => e.2 == d) :=
      List.mem_of_mem_head? hx
    have hxm : x ∈ m := by
      have hs := (List.dropWhile_suffix
        (fun e : Nat × Nat => e.2 == d) (l := m.reverse)).subset hxin
      rwa [List.mem_reverse] at hs
    have hled : x.2 ≤ d := by
      cases hml : m.getLast? with
      | none =>
        rw [List.getLast?_eq_none_iff] at hml
        subst hml
        cases hxm
      | some g =>
        exact Nat.le_trans (hasc.base_le_getLast hml x hxm) (hlast g hml)
    simp only [beq_eq_false_iff_ne, ne_eq] at hne
    omega

/-- Fresh take-facts straight off a clean reconcile: the surviving map is a
take of the DELIVERED map, and the new durable is clamped at the first
beyond-take entry. -/
private theorem reconcile_take_facts {own : TermMap} {d : Nat} {l0 : Nat × Nat}
    {ls : TermMap} {o : Outcome} (hasc : TermMap.Ascending own)
    (hrec : Uc2.reconcile own d (l0 :: ls) = .ok o) :
    o.newMap = (l0 :: ls).take o.newMap.length ∧
    ∀ f ∈ (l0 :: ls)[o.newMap.length]?, o.validUpTo ≤ f.2 := by
  have hmap : o.newMap = own.take (commonPrefixLen own (l0 :: ls)) :=
    Uc2.reconcile_ok_newMap_take hasc hrec
  have hcple : commonPrefixLen own (l0 :: ls) ≤ own.length :=
    Uc2.commonPrefixLen_le_left own (l0 :: ls)
  have hlen : o.newMap.length = commonPrefixLen own (l0 :: ls) := by
    rw [hmap, List.length_take]
    omega
  constructor
  · rw [hlen, hmap, Uc2.take_commonPrefixLen_eq]
  · intro f hf
    rw [hlen] at hf
    exact Uc2.reconcile_ok_le_leader_k hrec f hf

/-- Lagged transport: a reconcile against an UNRELATED map still leaves the
node a (shorter) take of any map it was already a take of, with the durable
clamp re-derived from the own-side `v1` clamp. -/
private theorem take_facts_shrink {own es : TermMap} {d : Nat}
    {l0 : Nat × Nat} {ls : TermMap} {o : Outcome}
    (hasc : TermMap.Ascending own)
    (hrec : Uc2.reconcile own d (l0 :: ls) = .ok o)
    (hown : own = es.take own.length)
    (hbound : ∀ f ∈ es[own.length]?, d ≤ f.2) :
    o.newMap = es.take o.newMap.length ∧
    ∀ f ∈ es[o.newMap.length]?, o.validUpTo ≤ f.2 := by
  obtain ⟨hvd, hoclamp, -, -⟩ :=
    Uc2.reconcileClamped_ok (Uc2.reconcile_ok_clamped hrec)
  have hmap : o.newMap = own.take (commonPrefixLen own (l0 :: ls)) :=
    Uc2.reconcile_ok_newMap_take hasc hrec
  set cp := commonPrefixLen own (l0 :: ls) with hcp
  have hcple : cp ≤ own.length := Uc2.commonPrefixLen_le_left own (l0 :: ls)
  have hlen : o.newMap.length = cp := by
    rw [hmap, List.length_take]
    omega
  constructor
  · rw [hlen, hmap]
    conv_lhs => rw [hown]
    rw [List.take_take]
    congr 1
    omega
  · intro f hf
    rw [hlen] at hf
    rcases Nat.lt_or_ge cp own.length with hlt | hge
    · refine hoclamp f ?_
      rw [hown, List.getElem?_take_of_lt hlt]
      exact hf
    · have hcpe : cp = own.length := by omega
      rw [hcpe] at hf
      exact Nat.le_trans hvd (hbound f hf)

/-- `getLast?` of a nonempty take is the take's boundary entry. -/
private theorem getLast?_take {es : TermMap} {len : Nat} (h1 : 1 ≤ len)
    (h2 : len ≤ es.length) : (es.take len).getLast? = es[len - 1]? := by
  rw [List.getLast?_eq_getElem?, List.length_take]
  rw [List.getElem?_take_of_lt (by omega)]
  congr 1
  omega

/-- Consecutive entry terms strictly ascend. -/
private theorem ascending_term_lt_succ {m : TermMap}
    (hasc : TermMap.Ascending m) {j : Nat} (hj : j + 1 < m.length) :
    (m[j]'(by omega)).1 < (m[j + 1]'hj).1 := by
  induction j generalizing m with
  | zero =>
    cases m with
    | nil => simp at hj
    | cons a l =>
      cases l with
      | nil => simp at hj
      | cons b l' => exact hasc.1
  | succ j' ih =>
    cases m with
    | nil => simp at hj
    | cons a l =>
      cases l with
      | nil => simp at hj
      | cons b l' =>
        exact ih hasc.2.2 (by simpa using hj)

/-- Gossips are never empty: every ship is a leader's map, and a leader's
map carries at least its own tenure entry (`NodeWF.leader_map`). -/
def GossipNe {n : Nat} (w : World n) : Prop :=
  ∀ t es, Frame.gossip t es ∈ w.dsent → es ≠ []

private theorem gne_step {n : Nat} {w w' : World n} (hw : Reachable w)
    (h : GossipNe w) (hs : Step w w') : GossipNe w' := by
  cases hs with
  | leaderAppend i v hrole =>
    intro t es hg
    rcases List.mem_append.mp hg with hg | hg
    · exact h t es hg
    · simp at hg
  | serveTail i p t v hrole hhist hp =>
    intro t' es hg
    rcases List.mem_append.mp hg with hg | hg
    · exact h t' es hg
    · simp at hg
  | shipTermMap i hrole =>
    intro t es hg
    rcases List.mem_append.mp hg with hg | hg
    · exact h t es hg
    · simp only [List.mem_singleton, Frame.gossip.injEq] at hg
      obtain ⟨rfl, rfl⟩ := hg
      exact ((Data.reachable_mapInv (reachable_project hw)).node i).leader_map
        hrole
  | startElection i hrole => exact h
  | deliverRequestVote j c nt clt cd hmsg hterm => exact h
  | rejectStaleRequestVote j c nt clt cd hmsg hstale => exact h
  | deliverVote i v t hmsg hrole hterm => exact h
  | deliverVoteHigherTerm i v t g hmsg hterm => exact h
  | becomeLeader i hrole hquorum => exact h
  | absorbDurable i => exact h
  | crashRestart i => exact h
  | deliverReplicate j pos hdr t v hmsg hpos hhdr hgate => exact h
  | deliverTermMap j t entries hmsg hterm => exact h
  | sendReport j hrole hgate => exact h
  | deliverReport i src t d hmsg hrole hterm hsrc => exact h
  | leaderAdvanceCommit i k hrole hbase hadv => exact h

/-- **Gossip nonemptiness** in every reachable world. -/
theorem reachable_gossip_ne {n : Nat} {w : World n} (hw : Reachable w) :
    GossipNe w := by
  induction hw with
  | refl => intro t es hg; simp [World.init] at hg
  | tail hprev hstep ih => exact gne_step hprev ih hstep

/-! ## The bundle -/

/-- LC4b Stage A: the six take-discipline clauses (module doc). -/
structure TkInv {n : Nat} (w : World n) : Prop where
  strict_node : ∀ j : Fin n, StrictBases (w.nodes j).dn.termMap
  strict_gossip : ∀ t es, Frame.gossip t es ∈ w.dsent → StrictBases es
  gate_take : ∀ j : Fin n, (w.nodes j).reconciled = true →
      ∀ es, Frame.gossip ((w.nodes j).dataTerm) es ∈ w.dsent →
      (w.nodes j).dn.termMap = es.take (w.nodes j).dn.termMap.length ∧
      ∀ f ∈ es[(w.nodes j).dn.termMap.length]?, (w.nodes j).pn.durable ≤ f.2
  open_leader : ∀ j ℓ : Fin n, (w.nodes j).reconciled = true →
      (w.nodes ℓ).pn.role = .leader →
      (w.nodes ℓ).pn.currentTerm = (w.nodes j).dataTerm →
      (w.nodes j).dn.termMap
        = (w.nodes ℓ).dn.termMap.take (w.nodes j).dn.termMap.length ∧
      ∀ f ∈ (w.nodes ℓ).dn.termMap[(w.nodes j).dn.termMap.length]?,
        (w.nodes j).pn.durable ≤ f.2
  report_take : ∀ (u : Fin n) (T d : Nat), CMsg.report u T d ∈ w.csent →
      (w.nodes u).dataTerm = T →
      (∀ es, Frame.gossip T es ∈ w.dsent →
        (w.nodes u).dn.termMap = es.take (w.nodes u).dn.termMap.length ∧
        ∀ f ∈ es[(w.nodes u).dn.termMap.length]?,
          (w.nodes u).pn.durable ≤ f.2) ∧
      (∀ ℓ : Fin n, (w.nodes ℓ).pn.role = .leader →
        (w.nodes ℓ).pn.currentTerm = T →
        (w.nodes u).dn.termMap
          = (w.nodes ℓ).dn.termMap.take (w.nodes u).dn.termMap.length ∧
        ∀ f ∈ (w.nodes ℓ).dn.termMap[(w.nodes u).dn.termMap.length]?,
          (w.nodes u).pn.durable ≤ f.2)
  gate_frontier_eq : ∀ j : Fin n, (w.nodes j).reconciled = true →
      ∀ tf bf, (w.nodes j).dn.termMap.getLast? = some (tf, bf) →
      bf = (w.nodes j).pn.durable →
      ∀ t' v', Frame.replicate bf ((w.nodes j).dataTerm) t' v' ∈ w.dsent →
      t' = tf

private theorem tk_init (n : Nat) : TkInv (World.init n) where
  strict_node := fun j => StrictBases.nil
  strict_gossip := by intro t es h; simp [World.init] at h
  gate_take := by intro j _ es h; simp [World.init] at h
  open_leader := by intro j ℓ _ hrole; simp [World.init, Node.pn] at hrole
  report_take := by intro u T d h; simp [World.init] at h
  gate_frontier_eq := by intro j _ tf bf h; simp [World.init] at h

/-- Transport: map and durable unchanged per node; an open gate implies it
was open at an unchanged handle; surviving leadership implies prior
leadership at an unchanged term; a report whose sender's (new) handle
matches its term was already in flight with the (old) handle matching;
data wire unchanged. Covers every constructor that touches only the
election/commit plane. -/
private theorem tk_transport {n : Nat} {w w' : World n} (h : TkInv w)
    (hmap : ∀ k, (w'.nodes k).dn.termMap = (w.nodes k).dn.termMap)
    (hdur : ∀ k, (w'.nodes k).pn.durable = (w.nodes k).pn.durable)
    (hgate : ∀ k, (w'.nodes k).reconciled = true →
      (w.nodes k).reconciled = true ∧
      (w'.nodes k).dataTerm = (w.nodes k).dataTerm)
    (hldr : ∀ k, (w'.nodes k).pn.role = .leader →
      (w.nodes k).pn.role = .leader ∧
      (w'.nodes k).pn.currentTerm = (w.nodes k).pn.currentTerm)
    (hrept : ∀ (u : Fin n) (T d : Nat), CMsg.report u T d ∈ w'.csent →
      CMsg.report u T d ∈ w.csent ∧
      ((w'.nodes u).dataTerm = T → (w.nodes u).dataTerm = T))
    (hds : w'.dsent = w.dsent) :
    TkInv w' := by
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro j
    rw [hmap]
    exact h.strict_node j
  · intro t es hg
    rw [hds] at hg
    exact h.strict_gossip t es hg
  · intro j hr es hg
    obtain ⟨hrpre, hdteq⟩ := hgate j hr
    rw [hds, hdteq] at hg
    rw [hmap, hdur]
    exact h.gate_take j hrpre es hg
  · intro j ℓ hr hrl hct
    obtain ⟨hrpre, hdteq⟩ := hgate j hr
    obtain ⟨hpre, hcteq⟩ := hldr ℓ hrl
    rw [hcteq, hdteq] at hct
    rw [hmap j, hmap ℓ, hdur]
    exact h.open_leader j ℓ hrpre hpre hct
  · intro u T d hrp hdtu
    obtain ⟨hmem, himp⟩ := hrept u T d hrp
    obtain ⟨hg, hl⟩ := h.report_take u T d hmem (himp hdtu)
    constructor
    · intro es hges
      rw [hds] at hges
      rw [hmap, hdur]
      exact hg es hges
    · intro ℓ hrl hct
      obtain ⟨hpre, hcteq⟩ := hldr ℓ hrl
      rw [hcteq] at hct
      rw [hmap u, hmap ℓ, hdur]
      exact hl ℓ hpre hct
  · intro j hr tf bf hlast hbf t' v' hf
    obtain ⟨hrpre, hdteq⟩ := hgate j hr
    rw [hds, hdteq] at hf
    rw [hmap] at hlast
    rw [hdur] at hbf
    exact h.gate_frontier_eq j hrpre tf bf hlast hbf t' v' hf

private theorem tk_step {n : Nat} {w w' : World n} (hw : Reachable w)
    (hw' : Reachable w') (h : TkInv w) (hs : Step w w') : TkInv w' := by
  have hstep := hs
  cases hs with
  | startElection i hrole =>
    refine tk_transport h (fun k => ?_) (fun k => ?_) (fun k hr => ?_)
      (fun k hrl => ?_) (fun u T d hm => ⟨hm, fun hdt => ?_⟩) rfl
    · rcases eq_or_ne k i with rfl | hne
      · simp [Function.update_self]
      · simp [Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp [Node.pn, Function.update_self]
      · simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · refine ⟨by simpa [Function.update_self] using hr, ?_⟩
        simp [Node.dataTerm, Function.update_self]
      · refine ⟨by simpa [Function.update_of_ne hne] using hr, ?_⟩
        simp [Node.dataTerm, Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp only [Node.pn, Function.update_self] at hrl
        exact absurd hrl (by decide)
      · refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hrl, ?_⟩
        simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne u i with rfl | hne
      · simpa [Node.dataTerm, Function.update_self] using hdt
      · simpa [Node.dataTerm, Function.update_of_ne hne] using hdt
  | deliverRequestVote j c nt clt cd hmsg hterm =>
    refine tk_transport h (fun k => ?_) (fun k => ?_) (fun k hr => ?_)
      (fun k hrl => ?_) (fun u T d hm => ⟨hm, fun hdt => ?_⟩) rfl
    · rcases eq_or_ne k j with rfl | hne
      · simp [Function.update_self]
      · simp [Function.update_of_ne hne]
    · rcases eq_or_ne k j with rfl | hne
      · simp only [Node.pn, Function.update_self]
        exact Data.recv_durable _ _ _ _ _
      · simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne k j with rfl | hne
      · simp only [Node.dataTerm, Function.update_self] at hr ⊢
        by_cases hadopt : (w.nodes k).pn.currentTerm < nt
        · rw [if_pos hadopt] at hr
          cases hr
        · rw [if_neg hadopt] at hr ⊢
          exact ⟨hr, rfl⟩
      · refine ⟨by simpa [Function.update_of_ne hne] using hr, ?_⟩
        simp [Node.dataTerm, Function.update_of_ne hne]
    · rcases eq_or_ne k j with rfl | hne
      · simp only [Node.pn, Function.update_self] at hrl ⊢
        by_cases hadopt : (w.nodes k).dn.pn.currentTerm < nt
        · rw [Data.recv_adopt_role _ _ _ _ _ hadopt] at hrl
          exact absurd hrl (by decide)
        · rw [(Data.recv_frame _ _ _ _ _ hadopt).1] at hrl
          exact ⟨hrl, (Data.recv_frame _ _ _ _ _ hadopt).2⟩
      · refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hrl, ?_⟩
        simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne u j with rfl | hne
      · simp only [Node.dataTerm, Function.update_self] at hdt ⊢
        by_cases hadopt : (w.nodes u).pn.currentTerm < nt
        · rw [if_pos hadopt] at hdt
          exfalso
          have h1 : T ≤ (w.nodes u).dn.dataTerm :=
            (reachable_provInv hw).report_dt u T d hm
          have h2 : (w.nodes u).dn.dataTerm ≤ (w.nodes u).dn.pn.currentTerm :=
            (Data.reachable_stamp (reachable_project hw)).data_le u
          have h3 : (w.nodes u).dn.pn.currentTerm < nt := hadopt
          omega
        · rwa [if_neg hadopt] at hdt
      · simpa [Node.dataTerm, Function.update_of_ne hne] using hdt
  | rejectStaleRequestVote j c nt clt cd hmsg hstale =>
    exact tk_transport h (fun k => rfl) (fun k => rfl)
      (fun k hr => ⟨hr, rfl⟩) (fun k hrl => ⟨hrl, rfl⟩)
      (fun u T d hm => ⟨hm, fun hdt => hdt⟩) rfl
  | deliverVote i v t hmsg hrole hterm =>
    refine tk_transport h (fun k => ?_) (fun k => ?_) (fun k hr => ?_)
      (fun k hrl => ?_) (fun u T d hm => ⟨hm, fun hdt => ?_⟩) rfl
    · rcases eq_or_ne k i with rfl | hne
      · simp [Function.update_self]
      · simp [Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp [Node.pn, Function.update_self]
      · simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · refine ⟨by simpa [Function.update_self] using hr, ?_⟩
        simp [Node.dataTerm, Function.update_self]
      · refine ⟨by simpa [Function.update_of_ne hne] using hr, ?_⟩
        simp [Node.dataTerm, Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · refine ⟨by simpa [Node.pn, Function.update_self] using hrl, ?_⟩
        simp [Node.pn, Function.update_self]
      · refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hrl, ?_⟩
        simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne u i with rfl | hne
      · simpa [Node.dataTerm, Function.update_self] using hdt
      · simpa [Node.dataTerm, Function.update_of_ne hne] using hdt
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    refine tk_transport h (fun k => ?_) (fun k => ?_) (fun k hr => ?_)
      (fun k hrl => ?_) (fun u T d hm => ⟨hm, fun hdt => ?_⟩) rfl
    · rcases eq_or_ne k i with rfl | hne
      · simp [Function.update_self]
      · simp [Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp [Node.pn, Function.update_self, PNode.adoptTerm]
      · simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hr
        cases hr
      · refine ⟨by simpa [Function.update_of_ne hne] using hr, ?_⟩
        simp [Node.dataTerm, Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp only [Node.pn, Function.update_self, PNode.adoptTerm] at hrl
        exact absurd hrl (by decide)
      · refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hrl, ?_⟩
        simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne u i with rfl | hne
      · simp only [Node.dataTerm, Function.update_self] at hdt
        exfalso
        have h1 : T ≤ (w.nodes u).dn.dataTerm :=
          (reachable_provInv hw).report_dt u T d hm
        have h2 : (w.nodes u).dn.dataTerm ≤ (w.nodes u).dn.pn.currentTerm :=
          (Data.reachable_stamp (reachable_project hw)).data_le u
        have h3 : (w.nodes u).dn.pn.currentTerm < t := hterm
        omega
      · simpa [Node.dataTerm, Function.update_of_ne hne] using hdt
  | absorbDurable i =>
    refine tk_transport h (fun k => ?_) (fun k => ?_) (fun k hr => ?_)
      (fun k hrl => ?_) (fun u T d hm => ⟨hm, fun hdt => ?_⟩) rfl
    · rcases eq_or_ne k i with rfl | hne
      · simp [Function.update_self]
      · simp [Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp [Node.pn, Function.update_self]
      · simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp only [Node.dataTerm, Function.update_self] at hr ⊢
        have hle : (w.nodes k).dn.pn.currentTerm ≤
            Data.lastTermOf (w.nodes k).dn.termMap := of_decide_eq_true hr
        have h2 : Data.lastTermOf (w.nodes k).dn.termMap ≤
            (w.nodes k).dn.dataTerm :=
          Data.reachable_map_le_dataTerm (reachable_project hw) k
        have h3 : (w.nodes k).dn.dataTerm ≤ (w.nodes k).dn.pn.currentTerm :=
          (Data.reachable_stamp (reachable_project hw)).data_le k
        refine ⟨?_, ?_⟩
        · by_contra hcl
          have h4 : Data.lastTermOf (w.nodes k).dn.termMap <
              (w.nodes k).dn.dataTerm :=
            (reachable_provInv hw).closed_lag k (Bool.eq_false_iff.mpr hcl)
          omega
        · show (w.nodes k).dn.pn.currentTerm = (w.nodes k).dn.dataTerm
          omega
      · refine ⟨by simpa [Function.update_of_ne hne] using hr, ?_⟩
        simp [Node.dataTerm, Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp only [Node.pn, Function.update_self] at hrl
        exact absurd hrl (by decide)
      · refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hrl, ?_⟩
        simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne u i with rfl | hne
      · simp only [Node.dataTerm, Function.update_self] at hdt
        have h1 : T ≤ (w.nodes u).dn.dataTerm :=
          (reachable_provInv hw).report_dt u T d hm
        have h2 : (w.nodes u).dn.dataTerm ≤ (w.nodes u).dn.pn.currentTerm :=
          (Data.reachable_stamp (reachable_project hw)).data_le u
        have hdt' : (w.nodes u).dn.pn.currentTerm = T := hdt
        show (w.nodes u).dn.dataTerm = T
        omega
      · simpa [Node.dataTerm, Function.update_of_ne hne] using hdt
  | crashRestart i =>
    refine tk_transport h (fun k => ?_) (fun k => ?_) (fun k hr => ?_)
      (fun k hrl => ?_) (fun u T d hm => ⟨hm, fun hdt => ?_⟩) rfl
    · rcases eq_or_ne k i with rfl | hne
      · simp [Function.update_self]
      · simp [Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp [Node.pn, Function.update_self]
      · simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp only [Node.dataTerm, Function.update_self] at hr ⊢
        have hle : (w.nodes k).dn.pn.currentTerm ≤
            Data.lastTermOf (w.nodes k).dn.termMap := of_decide_eq_true hr
        have h2 : Data.lastTermOf (w.nodes k).dn.termMap ≤
            (w.nodes k).dn.dataTerm :=
          Data.reachable_map_le_dataTerm (reachable_project hw) k
        have h3 : (w.nodes k).dn.dataTerm ≤ (w.nodes k).dn.pn.currentTerm :=
          (Data.reachable_stamp (reachable_project hw)).data_le k
        refine ⟨?_, ?_⟩
        · by_contra hcl
          have h4 : Data.lastTermOf (w.nodes k).dn.termMap <
              (w.nodes k).dn.dataTerm :=
            (reachable_provInv hw).closed_lag k (Bool.eq_false_iff.mpr hcl)
          omega
        · show (w.nodes k).dn.pn.currentTerm = (w.nodes k).dn.dataTerm
          omega
      · refine ⟨by simpa [Function.update_of_ne hne] using hr, ?_⟩
        simp [Node.dataTerm, Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp only [Node.pn, Function.update_self] at hrl
        exact absurd hrl (by decide)
      · refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hrl, ?_⟩
        simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne u i with rfl | hne
      · simp only [Node.dataTerm, Function.update_self] at hdt
        have h1 : T ≤ (w.nodes u).dn.dataTerm :=
          (reachable_provInv hw).report_dt u T d hm
        have h2 : (w.nodes u).dn.dataTerm ≤ (w.nodes u).dn.pn.currentTerm :=
          (Data.reachable_stamp (reachable_project hw)).data_le u
        have hdt' : (w.nodes u).dn.pn.currentTerm = T := hdt
        show (w.nodes u).dn.dataTerm = T
        omega
      · simpa [Node.dataTerm, Function.update_of_ne hne] using hdt
  | deliverReport i src t d hmsg hrole hterm hsrc =>
    refine tk_transport h (fun k => ?_) (fun k => ?_) (fun k hr => ?_)
      (fun k hrl => ?_) (fun u T d' hm => ⟨hm, fun hdt => ?_⟩) rfl
    · rcases eq_or_ne k i with rfl | hne
      · simp [Function.update_self]
      · simp [Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp [Node.pn, Function.update_self]
      · simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · refine ⟨by simpa [Function.update_self] using hr, ?_⟩
        simp [Node.dataTerm, Function.update_self]
      · refine ⟨by simpa [Function.update_of_ne hne] using hr, ?_⟩
        simp [Node.dataTerm, Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · refine ⟨by simpa [Node.pn, Function.update_self] using hrl, ?_⟩
        simp [Node.pn, Function.update_self]
      · refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hrl, ?_⟩
        simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne u i with rfl | hne
      · simpa [Node.dataTerm, Function.update_self] using hdt
      · simpa [Node.dataTerm, Function.update_of_ne hne] using hdt
  | leaderAdvanceCommit i k hrole hbase hadv =>
    refine tk_transport h (fun k' => ?_) (fun k' => ?_) (fun k' hr => ?_)
      (fun k' hrl => ?_) (fun u T d hm => ⟨hm, fun hdt => ?_⟩) rfl
    · rcases eq_or_ne k' i with rfl | hne
      · simp [Function.update_self]
      · simp [Function.update_of_ne hne]
    · rcases eq_or_ne k' i with rfl | hne
      · simp [Node.pn, Function.update_self]
      · simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne k' i with rfl | hne
      · refine ⟨by simpa [Function.update_self] using hr, ?_⟩
        simp [Node.dataTerm, Function.update_self]
      · refine ⟨by simpa [Function.update_of_ne hne] using hr, ?_⟩
        simp [Node.dataTerm, Function.update_of_ne hne]
    · rcases eq_or_ne k' i with rfl | hne
      · refine ⟨by simpa [Node.pn, Function.update_self] using hrl, ?_⟩
        simp [Node.pn, Function.update_self]
      · refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hrl, ?_⟩
        simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne u i with rfl | hne
      · simpa [Node.dataTerm, Function.update_self] using hdt
      · simpa [Node.dataTerm, Function.update_of_ne hne] using hdt
  | becomeLeader i hrole hquorum =>
    have hInv : Uc2.Inv w.project.project :=
      Uc2.reachable_inv (Data.reachable_project (reachable_project hw))
    have hblock : (∃ ℓ, Data.Cert w.project ((w.nodes i).pn.currentTerm) ℓ) →
        False := by
      rintro ⟨ℓ, hc⟩
      exact Data.cert_blocks_candidate hInv hrole rfl hquorum hc
    have hct1 : 1 ≤ (w.nodes i).pn.currentTerm :=
      ((Data.reachable_mapInv (reachable_project hw)).node i).role_term_pos
        (by rw [show (w.project.nodes i).pn.role = Role.candidate from hrole]
            decide)
    refine ⟨?_, ?_, ?_, ?_, ?_, ?_⟩
    · intro k
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self]
        exact strict_prunePush (h.strict_node k)
          ((Data.reachable_mapsWF (reachable_project hw)) k).1
          ((Data.reachable_mapInv (reachable_project hw)).node k).last_base
      · simp only [Function.update_of_ne hne]
        exact h.strict_node k
    · exact h.strict_gossip
    · intro k hr es hg
      rcases eq_or_ne k i with rfl | hne
      · simp only [Node.dataTerm, Function.update_self] at hg
        exfalso
        refine hblock ?_
        exact (Data.reachable_dinv (reachable_project hw)).cert _
          (.inr ⟨es, hg⟩)
      · simp only [Node.dataTerm, Function.update_of_ne hne] at hr hg ⊢
        exact h.gate_take k (by simpa [Function.update_of_ne hne] using hr)
          es hg
    · intro j ℓ hr hrl hct
      rcases eq_or_ne ℓ i with rfl | hneℓ
      · -- the NEW leader: any open regime at its term was already certified
        rcases eq_or_ne j ℓ with rfl | hnej
        · simp only [Node.pn, Node.dataTerm, Function.update_self]
          refine ⟨by rw [List.take_length], ?_⟩
          intro f hf
          rw [List.getElem?_eq_none (Nat.le_refl _)] at hf
          cases hf
        · simp only [Node.pn, Node.dataTerm, Function.update_self,
            Function.update_of_ne hnej] at hct ⊢
          exfalso
          have hr' : (w.nodes j).reconciled = true := by
            simpa [Function.update_of_ne hnej] using hr
          have hctj : (w.nodes ℓ).pn.currentTerm = (w.nodes j).dataTerm := hct
          have hdtpos : 1 ≤ (w.nodes j).dataTerm := by omega
          obtain ⟨ℓ', hc⟩ := (reachable_provInv hw).gate_cert j hr' hdtpos
          rw [← hctj] at hc
          exact hblock ⟨ℓ', hc⟩
      · rcases eq_or_ne j i with rfl | hnej
        · simp only [Node.pn, Node.dataTerm, Function.update_self,
            Function.update_of_ne hneℓ] at hrl hct ⊢
          exfalso
          have hc : Data.Cert w.project ((w.nodes ℓ).pn.currentTerm) ℓ :=
            Data.cert_of_leader hInv hrl
          have hct' : (w.nodes ℓ).pn.currentTerm = (w.nodes j).pn.currentTerm :=
            hct
          rw [hct'] at hc
          exact hblock ⟨ℓ, hc⟩
        · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hnej,
            Function.update_of_ne hneℓ] at hr hrl hct ⊢
          exact h.open_leader j ℓ hr hrl hct
    · intro u T d hrp hdtu
      rcases eq_or_ne u i with rfl | hne
      · simp only [Node.dataTerm, Function.update_self] at hdtu
        exfalso
        have hdtu' : (w.nodes u).pn.currentTerm = T := hdtu
        have h1T : 1 ≤ T := by omega
        obtain ⟨ℓ', hc⟩ := (reachable_provInv hw).report_cert u T d hrp h1T
        rw [← hdtu'] at hc
        exact hblock ⟨ℓ', hc⟩
      · simp only [Node.dataTerm, Function.update_of_ne hne] at hdtu
        obtain ⟨hg, hl⟩ := h.report_take u T d hrp hdtu
        constructor
        · intro es hges
          simp only [Node.dataTerm, Function.update_of_ne hne]
          exact hg es hges
        · intro ℓ hrl hct
          rcases eq_or_ne ℓ i with rfl | hneℓ
          · simp only [Node.pn, Function.update_self] at hct
            exfalso
            have hct' : (w.nodes ℓ).pn.currentTerm = T := hct
            have h1T : 1 ≤ T := by omega
            obtain ⟨ℓ', hc⟩ := (reachable_provInv hw).report_cert u T d hrp h1T
            rw [← hct'] at hc
            exact hblock ⟨ℓ', hc⟩
          · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hne,
              Function.update_of_ne hneℓ] at hrl hct ⊢
            exact hl ℓ hrl hct
    · intro j hr tf bf hlast hbf t' v' hf
      rcases eq_or_ne j i with rfl | hne
      · simp only [Node.dataTerm, Function.update_self] at hf
        exfalso
        refine hblock ?_
        exact (reachable_provInv hw).frame_cert bf _ t' v' hf
      · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hne]
          at hr hlast hbf hf ⊢
        exact h.gate_frontier_eq j (by simpa [Function.update_of_ne hne]
          using hr) tf bf hlast hbf t' v' hf
  | leaderAppend i v hrole =>
    have hdti : (w.nodes i).dn.dataTerm = (w.nodes i).dn.pn.currentTerm :=
      Data.reachable_leader_dataTerm (reachable_project hw) i hrole
    have hpin := (Data.reachable_dinv (reachable_project hw)).gossip_pinned i
      hrole
    have hlfront := (reachable_provInv hw).leader_frontier i hrole
    refine ⟨?_, ?_, ?_, ?_, ?_, ?_⟩
    · intro k
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self]
        exact h.strict_node k
      · simp only [Function.update_of_ne hne]
        exact h.strict_node k
    · intro t es hg
      rcases List.mem_append.mp hg with hg | hg
      · exact h.strict_gossip t es hg
      · simp at hg
    · intro k hr es hg
      rcases List.mem_append.mp hg with hg | hg
      · rcases eq_or_ne k i with rfl | hne
        · simp only [Node.dataTerm, Function.update_self] at hr hg ⊢
          have hes : es = (w.nodes k).dn.termMap := by
            refine hpin es ?_
            have hg' : Frame.gossip ((w.nodes k).dn.pn.currentTerm) es
                ∈ w.dsent := by rwa [hdti] at hg
            exact hg'
          subst hes
          refine ⟨by rw [List.take_length], ?_⟩
          intro f hf
          rw [List.getElem?_eq_none (Nat.le_refl _)] at hf
          cases hf
        · simp only [Node.dataTerm, Function.update_of_ne hne] at hr hg ⊢
          exact h.gate_take k (by simpa [Function.update_of_ne hne] using hr)
            es hg
      · simp at hg
    · intro j ℓ hr hrl hct
      rcases eq_or_ne ℓ i with rfl | hneℓ
      · rcases eq_or_ne j ℓ with rfl | hnej
        · simp only [Node.pn, Node.dataTerm, Function.update_self] at hct ⊢
          refine ⟨by rw [List.take_length], ?_⟩
          intro f hf
          rw [List.getElem?_eq_none (Nat.le_refl _)] at hf
          cases hf
        · simp only [Node.pn, Node.dataTerm, Function.update_self,
            Function.update_of_ne hnej] at hr hct ⊢
          exact h.open_leader j ℓ
            (by simpa [Function.update_of_ne hnej] using hr) hrole hct
      · rcases eq_or_ne j i with rfl | hnej
        · -- j = i as the open node, vs a DIFFERENT live leader at ct i:
          -- election safety collapses ℓ = i, contradiction with hneℓ.
          exfalso
          simp only [Node.pn, Node.dataTerm, Function.update_self,
            Function.update_of_ne hneℓ] at hrl hct
          have hcteq : (w.nodes ℓ).pn.currentTerm = (w.nodes j).pn.currentTerm := by
            have h2 : (w.nodes j).dn.dataTerm = (w.nodes j).dn.pn.currentTerm :=
              hdti
            have h3 : (w.nodes ℓ).dn.pn.currentTerm = (w.nodes j).dn.dataTerm :=
              hct
            show (w.nodes ℓ).dn.pn.currentTerm = (w.nodes j).dn.pn.currentTerm
            omega
          exact hneℓ (Uc2.Cert.election_safety w hw ℓ j hrl hrole hcteq)
        · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hnej,
            Function.update_of_ne hneℓ] at hr hrl hct ⊢
          exact h.open_leader j ℓ hr hrl hct
    · intro u T d hrp hdtu
      rcases eq_or_ne u i with rfl | hne
      · exfalso
        have hdtu' : (w.nodes u).dn.dataTerm = T := by
          simpa [Node.dataTerm, Function.update_self] using hdtu
        have hcteq : (w.nodes u).dn.pn.currentTerm = T := by omega
        have hmem : CMsg.report u ((w.nodes u).pn.currentTerm) d ∈ w.csent := by
          rw [show (w.nodes u).pn.currentTerm = T from hcteq]
          exact hrp
        exact reachable_no_self_report hw u (by rw [hrole]; decide) d hmem
      · simp only [Node.dataTerm, Function.update_of_ne hne] at hdtu
        obtain ⟨hg, hl⟩ := h.report_take u T d hrp hdtu
        constructor
        · intro es hges
          rcases List.mem_append.mp hges with hges | hges
          · simp only [Function.update_of_ne hne]
            exact hg es hges
          · simp at hges
        · intro ℓ hrl hct
          rcases eq_or_ne ℓ i with rfl | hneℓ
          · simp only [Node.pn, Function.update_self,
              Function.update_of_ne hne] at hrl hct ⊢
            obtain ⟨htake, hbound⟩ := hl ℓ hrole hct
            refine ⟨htake, ?_⟩
            intro f hf
            exact hbound f hf
          · simp only [Node.pn, Function.update_of_ne hne,
              Function.update_of_ne hneℓ] at hrl hct ⊢
            exact hl ℓ hrl hct
    · intro j hr tf bf hlast hbf t' v' hf
      rcases List.mem_append.mp hf with hf | hf
      · rcases eq_or_ne j i with rfl | hne
        · -- old frame at the leader's own (raised) frontier: leader_frontier
          -- bounds old ct-i frames strictly below the OLD durable.
          exfalso
          simp only [Node.pn, Node.dataTerm, Function.update_self] at hbf hf
          have hf' : Frame.replicate bf ((w.nodes j).dn.pn.currentTerm) t' v'
              ∈ w.dsent := by rwa [hdti] at hf
          have hlt : bf < (w.nodes j).dn.pn.durable := hlfront bf t' v' hf'
          omega
        · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hne]
            at hr hlast hbf hf ⊢
          exact h.gate_frontier_eq j
            (by simpa [Function.update_of_ne hne] using hr) tf bf hlast hbf
            t' v' hf
      · simp only [List.mem_singleton, Frame.replicate.injEq] at hf
        obtain ⟨hpbf, hhdr2, hts, hvs⟩ := hf
        rcases eq_or_ne j i with rfl | hne
        · -- the appender itself: new frame sits at the OLD durable, the
          -- frontier condition is at the NEW durable — off by one.
          exfalso
          simp only [Node.pn, Function.update_self] at hbf
          have hpbf' : bf = (w.nodes j).dn.pn.durable := hpbf
          omega
        · -- another open node at regime ct i whose zero-width frontier sits
          -- exactly at the leader's append point: the take structure forces
          -- it to BE the leader's own frontier entry, whose term is ct i.
          simp only [Node.pn, Node.dataTerm, Function.update_of_ne hne]
            at hr hlast hbf hpbf hhdr2 ⊢
          have hr' : (w.nodes j).reconciled = true := by
            simpa [Function.update_of_ne hne] using hr
          have hcti2 : (w.nodes i).pn.currentTerm = (w.nodes j).dataTerm :=
            hhdr2.symm
          obtain ⟨htake, hbound⟩ := h.open_leader j i hr' hrole hcti2
          set len := (w.nodes j).dn.termMap.length with hlendef
          have hlen1 : 1 ≤ len := by
            by_contra hc
            have hnil : (w.nodes j).dn.termMap = [] := by
              cases hm : (w.nodes j).dn.termMap with
              | nil => rfl
              | cons a l =>
                rw [hlendef, hm] at hc
                simp at hc
            rw [hnil] at hlast
            cases hlast
          have hlenle : len ≤ (w.nodes i).dn.termMap.length := by
            have hl2 : len = ((w.nodes i).dn.termMap.take len).length := by
              conv_lhs => rw [hlendef, htake]
            rw [List.length_take] at hl2
            omega
          have hidx : (w.nodes i).dn.termMap[len - 1]? = some (tf, bf) := by
            have hgl : (w.nodes j).dn.termMap.getLast?
                = (w.nodes j).dn.termMap[len - 1]? := by
              rw [List.getLast?_eq_getElem?]
            rw [hgl, htake, List.getElem?_take_of_lt (by omega)] at hlast
            exact hlast
          have hlastbase :=
            ((Data.reachable_mapInv (reachable_project hw)).node i).last_base
          cases hcase : (w.nodes i).dn.termMap[len]? with
          | some f =>
            exfalso
            have hdlef : (w.nodes j).dn.pn.durable ≤ f.2 := hbound f hcase
            have hpbf' : bf = (w.nodes i).dn.pn.durable := hpbf
            have hbf' : bf = (w.nodes j).dn.pn.durable := hbf
            have hfm : f ∈ (w.nodes i).dn.termMap := by
              have hflen : len < (w.nodes i).dn.termMap.length := by
                by_contra hc
                rw [List.getElem?_eq_none (by omega)] at hcase
                cases hcase
              rw [List.getElem?_eq_getElem (by omega)] at hcase
              rw [← Option.some.inj hcase]
              exact List.getElem_mem _
            have hfle : f.2 ≤ (w.nodes i).dn.pn.durable := by
              cases hml : (w.nodes i).dn.termMap.getLast? with
              | none =>
                rw [List.getLast?_eq_none_iff] at hml
                rw [hml] at hfm
                cases hfm
              | some g =>
                have hgd : g.2 ≤ (w.nodes i).dn.pn.durable := hlastbase g hml
                have := ((Data.reachable_mapsWF
                  (reachable_project hw)) i).1.base_le_getLast hml f hfm
                omega
            have hblt : bf < f.2 := by
              have hstr := h.strict_node i
              have hflen : len < (w.nodes i).dn.termMap.length := by
                by_contra hc
                rw [List.getElem?_eq_none (by omega)] at hcase
                cases hcase
              have hb := hstr.base_lt (i := len - 1) (j := len) (by omega)
                (by omega)
              rw [List.getElem?_eq_getElem (by omega)] at hidx hcase
              have h1 := Option.some.inj hidx
              have h2 := Option.some.inj hcase
              rw [h1] at hb
              rw [h2] at hb
              exact hb
            omega
          | none =>
            have hlenge : (w.nodes i).dn.termMap.length ≤ len := by
              by_contra hc
              rw [List.getElem?_eq_getElem (by omega)] at hcase
              cases hcase
            have hmapeq : (w.nodes j).dn.termMap = (w.nodes i).dn.termMap := by
              rw [htake, List.take_of_length_le (by omega)]
            have hlasti : (w.nodes i).dn.termMap.getLast? = some (tf, bf) := by
              rw [← hmapeq]
              exact hlast
            have hlt : Data.lastTermOf (w.nodes i).dn.termMap = tf :=
              Data.lastTermOf_getLast hlasti
            have hmp : Data.lastTermOf (w.nodes i).dn.termMap
                = (w.nodes i).pn.currentTerm :=
              (Data.reachable_dinv (reachable_project hw)).map_pinned i hrole
            rw [hts]
            show (w.nodes i).pn.currentTerm = tf
            rw [← hlt]
            exact hmp.symm
  | deliverReplicate j pos hdr t v hmsg hpos hhdr hgate =>
    have hstamp : t ≤ hdr :=
      (Data.reachable_stamp (reachable_project hw)).frame_le pos hdr t v hmsg
    have htpos : 1 ≤ t :=
      (Data.reachable_mapInv (reachable_project hw)).stamp_pos pos hdr t v hmsg
    have hascj : TermMap.Ascending (w.nodes j).dn.termMap :=
      ((Data.reachable_mapsWF (reachable_project hw)) j).1
    have hlastb :=
      ((Data.reachable_mapInv (reachable_project hw)).node j).last_base
    have hposd : pos = (w.nodes j).dn.pn.durable := hpos
    -- a leader never self-delivers: its own-tenure frames sit strictly
    -- below its durable
    have hnotldr : (w.nodes j).dn.pn.role = .leader → False := by
      intro hrl
      have hdtj : (w.nodes j).dn.dataTerm = (w.nodes j).dn.pn.currentTerm :=
        Data.reachable_leader_dataTerm (reachable_project hw) j hrl
      have hf' : Frame.replicate pos ((w.nodes j).pn.currentTerm) t v
          ∈ w.dsent := by
        have hh : hdr = (w.nodes j).dn.pn.currentTerm := by
          have hh2 : hdr = (w.nodes j).dn.dataTerm := hhdr
          omega
        rwa [hh] at hmsg
      have hlt : pos < (w.nodes j).dn.pn.durable :=
        (reachable_provInv hw).leader_frontier j hrl pos t v hf'
      omega
    -- zero-width frontiers cannot grow: the incoming stamp equals the
    -- frontier term (gate_frontier_eq)
    have hjunction : Data.lastTermOf (w.nodes j).dn.termMap < t →
        ∀ x, (w.nodes j).dn.termMap.getLast? = some x → x.2 < pos := by
      intro hgrow x hx
      have hxle : x.2 ≤ (w.nodes j).dn.pn.durable := hlastb x hx
      rcases Nat.lt_or_ge x.2 pos with hlt | hge
      · exact hlt
      · exfalso
        have hxeq : x.2 = pos := by omega
        have hfx : Frame.replicate x.2 ((w.nodes j).dataTerm) t v
            ∈ w.dsent := by
          rw [hxeq, ← hhdr]
          exact hmsg
        have hteq := h.gate_frontier_eq j hgate x.1 x.2
          (by rw [← Prod.mk.eta (p := x)] at hx; exact hx)
          (by rw [hxeq]; exact hpos) t v hfx
        have hlt2 : Data.lastTermOf (w.nodes j).dn.termMap = x.1 :=
          Data.lastTermOf_getLast hx
        omega
    -- the shared take-facts engine, parameterized by the target stream map
    have hcore : ∀ es, TermMap.Ascending es → StrictBases es →
        (w.nodes j).dn.termMap = es.take (w.nodes j).dn.termMap.length →
        (∀ f ∈ es[(w.nodes j).dn.termMap.length]?,
          (w.nodes j).pn.durable ≤ f.2) →
        TermMap.termAt es pos = t →
        (Data.observeTerm (w.nodes j).dn.termMap t pos
          = es.take (Data.observeTerm (w.nodes j).dn.termMap t pos).length) ∧
        ∀ f ∈ es[(Data.observeTerm (w.nodes j).dn.termMap t pos).length]?,
          pos + 1 ≤ f.2 := by
      intro es hase hstre htake hbound hattr
      by_cases hgrow : Data.lastTermOf (w.nodes j).dn.termMap < t
      · have hobs : Data.observeTerm (w.nodes j).dn.termMap t pos
            = (w.nodes j).dn.termMap ++ [(t, pos)] := by
          simp [Data.observeTerm, hgrow]
        have hnext : ∀ f ∈ es[(w.nodes j).dn.termMap.length]?, pos ≤ f.2 := by
          intro f hf
          have hb := hbound f hf
          have hb' : (w.nodes j).dn.pn.durable ≤ f.2 := hb
          omega
        have hgrow2 : Data.lastTermOf
            (es.take (w.nodes j).dn.termMap.length) < t := by
          rw [← htake]
          exact hgrow
        have hpin := take_growth_pin hase hstre hnext hattr hgrow2
        have hlen2 : ((w.nodes j).dn.termMap ++ [(t, pos)]).length
            = (w.nodes j).dn.termMap.length + 1 := by
          simp
        constructor
        · rw [hobs, hlen2, List.take_add_one, hpin, ← htake]
          rfl
        · intro f hf
          rw [hobs, hlen2] at hf
          have hlenlt : (w.nodes j).dn.termMap.length < es.length := by
            by_contra hc
            rw [List.getElem?_eq_none (by omega)] at hpin
            cases hpin
          have hlt2 : (w.nodes j).dn.termMap.length + 1 < es.length := by
            by_contra hc
            rw [List.getElem?_eq_none (by omega)] at hf
            cases hf
          have hb := hstre.base_lt
            (i := (w.nodes j).dn.termMap.length)
            (j := (w.nodes j).dn.termMap.length + 1) (by omega) hlt2
          rw [List.getElem?_eq_getElem (by omega)] at hpin hf
          have he1 := Option.some.inj hpin
          have he2 := Option.some.inj hf
          rw [he1] at hb
          rw [he2] at hb
          simpa using hb
      · have hobs : Data.observeTerm (w.nodes j).dn.termMap t pos
            = (w.nodes j).dn.termMap :=
          Data.observeTerm_of_le (Nat.not_lt.mp hgrow) pos
        rw [hobs]
        refine ⟨htake, ?_⟩
        intro f hf
        have hb : (w.nodes j).dn.pn.durable ≤ f.2 := hbound f hf
        rcases Nat.lt_or_ge pos f.2 with hlt | hge
        · omega
        · exfalso
          have hfeq : f.2 = pos := by omega
          have hpin := termAt_entry_base hase hstre hf
          rw [hfeq, hattr] at hpin
          have hlenlt : (w.nodes j).dn.termMap.length < es.length := by
            by_contra hc
            rw [List.getElem?_eq_none (by omega)] at hf
            cases hf
          rcases Nat.eq_zero_or_pos (w.nodes j).dn.termMap.length with h0 | h1
          · have hnil : (w.nodes j).dn.termMap = [] :=
              List.length_eq_zero_iff.mp h0
            rw [hnil] at hgrow
            simp [Data.lastTermOf] at hgrow
            omega
          · obtain ⟨m, hm⟩ : ∃ m, (w.nodes j).dn.termMap.length = m + 1 :=
              ⟨(w.nodes j).dn.termMap.length - 1, by omega⟩
            have hglt : (es.take (w.nodes j).dn.termMap.length).getLast?
                = es[(w.nodes j).dn.termMap.length - 1]? :=
              getLast?_take (by omega) (by omega)
            have hmidx : (w.nodes j).dn.termMap.length - 1 = m := by omega
            have hsucc := ascending_term_lt_succ hase (j := m) (by omega)
            have hg : es[m]? = some (es[m]'(by omega)) :=
              List.getElem?_eq_getElem (by omega)
            have hlt3 : Data.lastTermOf (w.nodes j).dn.termMap
                = (es[m]'(by omega)).1 := by
              conv_lhs => rw [htake]
              refine Data.lastTermOf_getLast ?_
              rw [hglt, hmidx]
              exact hg
            have hfm : es[m + 1]? = some f := by
              rw [← hm]
              exact hf
            rw [List.getElem?_eq_getElem (by omega)] at hfm
            have hfe := Option.some.inj hfm
            rw [hfe] at hsucc
            omega
    refine ⟨?_, h.strict_gossip, ?_, ?_, ?_, ?_⟩
    · intro k
      rcases eq_or_ne k j with rfl | hne
      · simp only [Function.update_self, Uc2.Data.Node.recvReplicate]
        by_cases hgrow : Data.lastTermOf (w.nodes k).dn.termMap < t
        · rw [show Data.observeTerm (w.nodes k).dn.termMap t pos
              = (w.nodes k).dn.termMap ++ [(t, pos)] by
            simp [Data.observeTerm, hgrow]]
          show ((w.nodes k).dn.termMap ++ [(t, pos)]).IsChain _
          rw [List.isChain_append]
          refine ⟨h.strict_node k, List.IsChain.nil.cons (by simp), ?_⟩
          intro x hx y hy
          simp only [List.head?_cons, Option.mem_def,
            Option.some.injEq] at hy
          subst hy
          exact hjunction hgrow x hx
        · rw [Data.observeTerm_of_le (Nat.not_lt.mp hgrow) pos]
          exact h.strict_node k
      · simp only [Function.update_of_ne hne]
        exact h.strict_node k
    · intro k hr es hg
      rcases eq_or_ne k j with rfl | hne
      · simp only [Node.pn, Node.dataTerm, Function.update_self,
          Uc2.Data.Node.recvReplicate] at hr hg ⊢
        have hattr : TermMap.termAt es pos = t :=
          (reachable_provInv hw).frame_gossip pos ((w.nodes k).dataTerm) t v
            es (by rw [← hhdr]; exact hmsg) hg
        obtain ⟨htake, hbound⟩ := h.gate_take k hgate es hg
        obtain ⟨h1, h2⟩ := hcore es
          ((Data.reachable_mapInv (reachable_project hw)).gossip_wf _ es hg)
          (h.strict_gossip _ es hg) htake hbound hattr
        exact ⟨h1, h2⟩
      · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hne]
          at hr hg ⊢
        exact h.gate_take k hr es hg
    · intro k ℓ hr hrl hct
      rcases eq_or_ne ℓ j with rfl | hneℓ
      · exfalso
        rcases eq_or_ne k ℓ with rfl | hnek
        · simp only [Node.pn, Function.update_self,
            Uc2.Data.Node.recvReplicate] at hrl
          exact hnotldr hrl
        · simp only [Node.pn, Function.update_self,
            Uc2.Data.Node.recvReplicate] at hrl
          exact hnotldr hrl
      · rcases eq_or_ne k j with rfl | hnek
        · simp only [Node.pn, Node.dataTerm, Function.update_self,
            Function.update_of_ne hneℓ, Uc2.Data.Node.recvReplicate]
            at hr hrl hct ⊢
          have hattr : TermMap.termAt (w.nodes ℓ).dn.termMap pos = t := by
            have hh := (reachable_provInv hw).frame_leader pos
              ((w.nodes k).dataTerm) t v (by rw [← hhdr]; exact hmsg) ℓ hrl
              hct
            exact hh
          obtain ⟨htake, hbound⟩ := h.open_leader k ℓ hgate hrl hct
          exact hcore (w.nodes ℓ).dn.termMap
            ((Data.reachable_mapsWF (reachable_project hw)) ℓ).1
            (h.strict_node ℓ) htake hbound hattr
        · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hnek,
            Function.update_of_ne hneℓ] at hr hrl hct ⊢
          exact h.open_leader k ℓ hr hrl hct
    · intro u T d hrp hdtu
      rcases eq_or_ne u j with rfl | hne
      · simp only [Node.pn, Node.dataTerm, Function.update_self,
          Uc2.Data.Node.recvReplicate] at hdtu
        obtain ⟨hgarm, hlarm⟩ := h.report_take u T d hrp hdtu
        constructor
        · intro es hges
          simp only [Node.pn, Node.dataTerm, Function.update_self,
            Uc2.Data.Node.recvReplicate]
          have hattr : TermMap.termAt es pos = t :=
            (reachable_provInv hw).frame_gossip pos ((w.nodes u).dataTerm) t
              v es (by rw [← hhdr]; exact hmsg)
              (by rw [show (w.nodes u).dataTerm = T from hdtu]; exact hges)
          obtain ⟨htake, hbound⟩ := hgarm es hges
          exact hcore es
            ((Data.reachable_mapInv (reachable_project hw)).gossip_wf _ es
              hges)
            (h.strict_gossip _ es hges) htake hbound hattr
        · intro ℓ hrl hct
          rcases eq_or_ne ℓ u with rfl | hneℓ
          · exfalso
            simp only [Node.pn, Function.update_self,
              Uc2.Data.Node.recvReplicate] at hrl
            exact hnotldr hrl
          · simp only [Node.pn, Node.dataTerm, Function.update_self,
              Function.update_of_ne hneℓ, Uc2.Data.Node.recvReplicate]
              at hrl hct ⊢
            have hattr : TermMap.termAt (w.nodes ℓ).dn.termMap pos = t := by
              have hcth : (w.nodes ℓ).pn.currentTerm = (w.nodes u).dataTerm := by
                have hh1 : (w.nodes ℓ).dn.pn.currentTerm = T := hct
                have hh2 : (w.nodes u).dn.dataTerm = T := hdtu
                show (w.nodes ℓ).dn.pn.currentTerm = (w.nodes u).dn.dataTerm
                omega
              exact (reachable_provInv hw).frame_leader pos
                ((w.nodes u).dataTerm) t v (by rw [← hhdr]; exact hmsg) ℓ hrl
                hcth
            obtain ⟨htake, hbound⟩ := hlarm ℓ hrl hct
            exact hcore (w.nodes ℓ).dn.termMap
              ((Data.reachable_mapsWF (reachable_project hw)) ℓ).1
              (h.strict_node ℓ) htake hbound hattr
      · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hne] at hdtu
        obtain ⟨hgarm, hlarm⟩ := h.report_take u T d hrp hdtu
        constructor
        · intro es hges
          simp only [Node.pn, Node.dataTerm, Function.update_of_ne hne]
          exact hgarm es hges
        · intro ℓ hrl hct
          rcases eq_or_ne ℓ j with rfl | hneℓ
          · exfalso
            simp only [Node.pn, Function.update_self,
              Uc2.Data.Node.recvReplicate] at hrl
            exact hnotldr hrl
          · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hne,
              Function.update_of_ne hneℓ] at hrl hct ⊢
            exact hlarm ℓ hrl hct
    · intro k hr tf bf hlast hbf t' v' hf
      rcases eq_or_ne k j with rfl | hne
      · exfalso
        simp only [Node.pn, Function.update_self,
          Uc2.Data.Node.recvReplicate] at hlast hbf
        by_cases hgrow : Data.lastTermOf (w.nodes k).dn.termMap < t
        · rw [show Data.observeTerm (w.nodes k).dn.termMap t pos
              = (w.nodes k).dn.termMap ++ [(t, pos)] by
            simp [Data.observeTerm, hgrow]] at hlast
          rw [Data.getLast?_append_singleton] at hlast
          have hbe := (Prod.mk.injEq _ _ _ _).mp (Option.some.inj hlast)
          omega
        · rw [Data.observeTerm_of_le (Nat.not_lt.mp hgrow) pos] at hlast
          have hxle : bf ≤ (w.nodes k).dn.pn.durable := hlastb (tf, bf) hlast
          omega
      · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hne]
          at hr hlast hbf hf ⊢
        exact h.gate_frontier_eq k hr tf bf hlast hbf t' v' hf
  | serveTail i p t v hrole hhist hp =>
    have hdti : (w.nodes i).dn.dataTerm = (w.nodes i).dn.pn.currentTerm :=
      Data.reachable_leader_dataTerm (reachable_project hw) i hrole
    refine ⟨fun j => h.strict_node j, ?_, ?_, h.open_leader, ?_, ?_⟩
    · intro t' es hg
      rcases List.mem_append.mp hg with hg | hg
      · exact h.strict_gossip t' es hg
      · simp at hg
    · intro j hr es hg
      rcases List.mem_append.mp hg with hg | hg
      · exact h.gate_take j hr es hg
      · simp at hg
    · intro u T d hrp hdtu
      obtain ⟨hg, hl⟩ := h.report_take u T d hrp hdtu
      refine ⟨?_, hl⟩
      intro es hges
      rcases List.mem_append.mp hges with hges | hges
      · exact hg es hges
      · simp at hges
    · intro j hr tf bf hlast hbf t' v' hf
      rcases List.mem_append.mp hf with hf | hf
      · exact h.gate_frontier_eq j hr tf bf hlast hbf t' v' hf
      · simp only [List.mem_singleton, Frame.replicate.injEq] at hf
        obtain ⟨hpbf, hcti, hts, hvs⟩ := hf
        -- the fresh serveTail frame at j's zero-width frontier: pin via the
        -- live leader's map (open_leader) + fca + the entry-base pin.
        obtain ⟨htake, hbound⟩ := h.open_leader j i hr hrole hcti.symm
        set len := (w.nodes j).dn.termMap.length with hlendef
        have hlen1 : 1 ≤ len := by
          by_contra hc
          have hnil : (w.nodes j).dn.termMap = [] := by
            cases hm : (w.nodes j).dn.termMap with
            | nil => rfl
            | cons a l =>
              rw [hlendef, hm] at hc
              simp at hc
          rw [hnil] at hlast
          cases hlast
        have hlenle : len ≤ (w.nodes i).dn.termMap.length := by
          have hl2 : len = ((w.nodes i).dn.termMap.take len).length := by
            conv_lhs => rw [hlendef, htake]
          rw [List.length_take] at hl2
          omega
        have hidx : (w.nodes i).dn.termMap[len - 1]? = some (tf, bf) := by
          have hgl : (w.nodes j).dn.termMap.getLast?
              = (w.nodes j).dn.termMap[len - 1]? := by
            rw [List.getLast?_eq_getElem?]
          rw [hgl, htake, List.getElem?_take_of_lt (by omega)] at hlast
          exact hlast
        have hattr : TermMap.termAt (w.nodes i).dn.termMap p = t :=
          (reachable_provInv hw).fca i p t v hhist
        have hpin : TermMap.termAt (w.nodes i).dn.termMap bf = tf :=
          termAt_entry_base
            ((Data.reachable_mapsWF (reachable_project hw)) i).1
            (h.strict_node i) hidx
        rw [hpbf] at hpin
        rw [hattr] at hpin
        rw [hts]
        exact hpin
  | shipTermMap i hrole =>
    have hdti : (w.nodes i).dn.dataTerm = (w.nodes i).dn.pn.currentTerm :=
      Data.reachable_leader_dataTerm (reachable_project hw) i hrole
    refine ⟨fun j => h.strict_node j, ?_, ?_, h.open_leader, ?_,
      ?_⟩
    · intro t es hg
      rcases List.mem_append.mp hg with hg | hg
      · exact h.strict_gossip t es hg
      · simp only [List.mem_singleton, Frame.gossip.injEq] at hg
        obtain ⟨rfl, rfl⟩ := hg
        exact h.strict_node i
    · intro j hr es hg
      rcases List.mem_append.mp hg with hg | hg
      · exact h.gate_take j hr es hg
      · simp only [List.mem_singleton, Frame.gossip.injEq] at hg
        obtain ⟨hct, rfl⟩ := hg
        exact h.open_leader j i hr hrole hct.symm
    · intro u T d hrp hdtu
      obtain ⟨hg, hl⟩ := h.report_take u T d hrp hdtu
      refine ⟨?_, hl⟩
      intro es hges
      rcases List.mem_append.mp hges with hges | hges
      · exact hg es hges
      · simp only [List.mem_singleton, Frame.gossip.injEq] at hges
        obtain ⟨hct, rfl⟩ := hges
        exact hl i hrole hct.symm
    · intro j hr tf bf hlast hbf t' v' hf
      rcases List.mem_append.mp hf with hf | hf
      · exact h.gate_frontier_eq j hr tf bf hlast hbf t' v' hf
      · simp at hf
  | deliverTermMap j t entries hmsg hterm =>
    have hene : entries ≠ [] := reachable_gossip_ne hw t entries hmsg
    obtain ⟨l0, ls, rfl⟩ : ∃ l0 ls, entries = l0 :: ls := by
      cases entries with
      | nil => exact absurd rfl hene
      | cons a l => exact ⟨a, l, rfl⟩
    have hterm' : (w.nodes j).dn.pn.currentTerm ≤ t := hterm
    have hascj : TermMap.Ascending (w.nodes j).dn.termMap :=
      ((Data.reachable_mapsWF (reachable_project hw)) j).1
    have hesasc : TermMap.Ascending (l0 :: ls) :=
      (Data.reachable_mapInv (reachable_project hw)).gossip_wf t _ hmsg
    have hesstr : StrictBases (l0 :: ls) := h.strict_gossip t _ hmsg
    have hlid : ∀ (k : Fin n), (w.nodes k).dn.pn.role = .leader →
        (w.nodes k).dn.pn.currentTerm = t →
        l0 :: ls = (w.nodes k).dn.termMap := by
      intro k hrl hteq
      exact (Data.reachable_dinv (reachable_project hw)).gossip_pinned k hrl
        (l0 :: ls)
        (show Frame.gossip ((w.nodes k).dn.pn.currentTerm) (l0 :: ls)
            ∈ w.dsent by rw [hteq]; exact hmsg)
    cases hrec : Uc2.reconcile (w.nodes j).dn.termMap
        (w.nodes j).dn.pn.durable (l0 :: ls) with
    | ok o =>
      obtain ⟨hmapE, hdurE, hhistE, hroleE, hctE, hdtE⟩ :=
        Data.applyGossip_ok (w.nodes j).dn t hrec
      have hfresh := reconcile_take_facts hascj hrec
      have hfreshu : ∀ es', Frame.gossip t es' ∈ w.dsent →
          o.newMap = es'.take o.newMap.length ∧
          ∀ f ∈ es'[o.newMap.length]?, o.validUpTo ≤ f.2 := by
        intro es' hes'
        have he : es' = l0 :: ls := reachable_gossip_uniq hw t es' _ hes' hmsg
        rw [he]
        exact hfresh
      have hnewtake : o.newMap
          = (w.nodes j).dn.termMap.take
            (commonPrefixLen (w.nodes j).dn.termMap (l0 :: ls)) :=
        Uc2.reconcile_ok_newMap_take hascj hrec
      have hstrictnew : StrictBases o.newMap := by
        rw [hnewtake]
        exact (h.strict_node j).prefix' (List.take_prefix _ _)
      have hlidok : (w.nodes j).dn.pn.role = .leader →
          ¬ (w.nodes j).dn.pn.currentTerm < t →
          o.newMap = (w.nodes j).dn.termMap ∧
          o.validUpTo = (w.nodes j).dn.pn.durable := by
        intro hrl hnad
        have hteq : (w.nodes j).dn.pn.currentTerm = t := by omega
        have hpin := hlid j hrl hteq
        rw [hpin, Data.reconcile_self] at hrec
        cases hrec
        exact ⟨rfl, rfl⟩
      refine ⟨?_, ?_, ?_, ?_, ?_, ?_⟩
      · intro k
        rcases eq_or_ne k j with rfl | hne
        · simp only [Function.update_self]
          rw [hmapE]
          exact hstrictnew
        · simp only [Function.update_of_ne hne]
          exact h.strict_node k
      · exact h.strict_gossip
      · intro k hr es hg
        rcases eq_or_ne k j with rfl | hne
        · simp only [Node.pn, Node.dataTerm, Function.update_self]
            at hr hg ⊢
          rw [hmapE, hdurE]
          rw [hdtE] at hg
          by_cases hadopt : (w.nodes k).dn.pn.currentTerm < t
          · rw [if_pos hadopt] at hg
            exact hfreshu es hg
          · rw [if_neg hadopt] at hg
            rw [if_neg hadopt] at hr
            simp only [Bool.or_eq_true] at hr
            have hteq : t = (w.nodes k).dn.pn.currentTerm := by omega
            by_cases hdteq : (w.nodes k).dn.dataTerm
                = (w.nodes k).dn.pn.currentTerm
            · rw [hdteq, ← hteq] at hg
              exact hfreshu es hg
            · have hrpre : (w.nodes k).reconciled = true := by
                rcases hr with hr | hdec
                · exact hr
                · exact absurd (of_decide_eq_true hdec) hdteq
              obtain ⟨hown, hbound⟩ := h.gate_take k hrpre es hg
              exact take_facts_shrink hascj hrec hown hbound
        · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hne]
            at hr hg ⊢
          exact h.gate_take k hr es hg
      · intro k ℓ hr hrl hct
        rcases eq_or_ne ℓ j with rfl | hneℓ
        · rcases eq_or_ne k ℓ with rfl | hnek
          · simp only [Node.pn, Node.dataTerm, Function.update_self]
              at hrl hct ⊢
            rw [hroleE] at hrl
            by_cases hadopt : (w.nodes k).dn.pn.currentTerm < t
            · rw [if_pos hadopt] at hrl
              exact absurd hrl (by decide)
            · rw [if_neg hadopt] at hrl
              rw [hmapE, hdurE]
              obtain ⟨hmeq, hveq⟩ := hlidok hrl hadopt
              refine ⟨by rw [List.take_length], ?_⟩
              intro f hf
              rw [List.getElem?_eq_none (Nat.le_refl _)] at hf
              cases hf
          · simp only [Node.pn, Node.dataTerm, Function.update_self,
              Function.update_of_ne hnek] at hr hrl hct ⊢
            rw [hroleE] at hrl
            rw [hctE] at hct
            by_cases hadopt : (w.nodes ℓ).dn.pn.currentTerm < t
            · rw [if_pos hadopt] at hrl
              exact absurd hrl (by decide)
            · rw [if_neg hadopt] at hrl hct
              obtain ⟨hmeq, hveq⟩ := hlidok hrl hadopt
              rw [hmapE, hmeq]
              exact h.open_leader k ℓ hr hrl hct
        · rcases eq_or_ne k j with rfl | hnek
          · simp only [Node.pn, Node.dataTerm, Function.update_self,
              Function.update_of_ne hneℓ] at hr hrl hct ⊢
            rw [hmapE, hdurE]
            rw [hdtE] at hct
            by_cases hadopt : (w.nodes k).dn.pn.currentTerm < t
            · rw [if_pos hadopt] at hct
              have hme := hlid ℓ hrl hct
              rw [← hme]
              exact hfresh
            · rw [if_neg hadopt] at hct
              rw [if_neg hadopt] at hr
              simp only [Bool.or_eq_true] at hr
              have hteq : t = (w.nodes k).dn.pn.currentTerm := by omega
              by_cases hdteq : (w.nodes k).dn.dataTerm
                  = (w.nodes k).dn.pn.currentTerm
              · have hctt : (w.nodes ℓ).dn.pn.currentTerm = t := by omega
                have hme := hlid ℓ hrl hctt
                rw [← hme]
                exact hfresh
              · have hrpre : (w.nodes k).reconciled = true := by
                  rcases hr with hr | hdec
                  · exact hr
                  · exact absurd (of_decide_eq_true hdec) hdteq
                obtain ⟨hown, hbound⟩ := h.open_leader k ℓ hrpre hrl hct
                exact take_facts_shrink hascj hrec hown hbound
          · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hnek,
              Function.update_of_ne hneℓ] at hr hrl hct ⊢
            exact h.open_leader k ℓ hr hrl hct
      · intro u T d hrp hdtu
        rcases eq_or_ne u j with rfl | hne
        · simp only [Node.pn, Node.dataTerm, Function.update_self] at hdtu
          rw [hdtE] at hdtu
          by_cases hadopt : (w.nodes u).dn.pn.currentTerm < t
          · rw [if_pos hadopt] at hdtu
            exfalso
            have h1 : T ≤ (w.nodes u).dn.dataTerm :=
              (reachable_provInv hw).report_dt u T d hrp
            have h2 : (w.nodes u).dn.dataTerm
                ≤ (w.nodes u).dn.pn.currentTerm :=
              (Data.reachable_stamp (reachable_project hw)).data_le u
            omega
          · rw [if_neg hadopt] at hdtu
            have hteq : t = (w.nodes u).dn.pn.currentTerm := by omega
            obtain ⟨hgarm, hlarm⟩ := h.report_take u T d hrp hdtu
            constructor
            · intro es hges
              simp only [Node.pn, Node.dataTerm, Function.update_self]
              rw [hmapE, hdurE]
              by_cases hdteq : (w.nodes u).dn.dataTerm
                  = (w.nodes u).dn.pn.currentTerm
              · have hTt : T = t := by
                  have hh : (w.nodes u).dn.dataTerm = T := hdtu
                  omega
                rw [hTt] at hges
                exact hfreshu es hges
              · obtain ⟨hown, hbound⟩ := hgarm es hges
                exact take_facts_shrink hascj hrec hown hbound
            · intro ℓ hrl hct
              rcases eq_or_ne ℓ u with rfl | hneℓ
              · exfalso
                simp only [Node.pn, Function.update_self] at hrl
                rw [hroleE] at hrl
                rw [if_neg hadopt] at hrl
                have hdtl : (w.nodes ℓ).dn.dataTerm
                    = (w.nodes ℓ).dn.pn.currentTerm :=
                  Data.reachable_leader_dataTerm (reachable_project hw) ℓ hrl
                have hTct : (w.nodes ℓ).pn.currentTerm = T := by
                  have hh : (w.nodes ℓ).dn.dataTerm = T := hdtu
                  show (w.nodes ℓ).dn.pn.currentTerm = T
                  omega
                exact reachable_no_self_report hw ℓ
                  (by rw [show (w.nodes ℓ).pn.role = Role.leader from hrl]
                      decide) d
                  (by rw [hTct]; exact hrp)
              · simp only [Node.pn, Node.dataTerm, Function.update_self,
                  Function.update_of_ne hneℓ] at hrl hct ⊢
                rw [hmapE, hdurE]
                by_cases hdteq : (w.nodes u).dn.dataTerm
                    = (w.nodes u).dn.pn.currentTerm
                · have hctt : (w.nodes ℓ).dn.pn.currentTerm = t := by
                    have hh : (w.nodes ℓ).dn.pn.currentTerm = T := hct
                    have hh1 : (w.nodes u).dn.dataTerm = T := hdtu
                    omega
                  have hme := hlid ℓ hrl hctt
                  rw [← hme]
                  exact hfresh
                · obtain ⟨hown, hbound⟩ := hlarm ℓ hrl hct
                  exact take_facts_shrink hascj hrec hown hbound
        · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hne]
            at hdtu
          obtain ⟨hgarm, hlarm⟩ := h.report_take u T d hrp hdtu
          constructor
          · intro es hges
            simp only [Node.pn, Node.dataTerm, Function.update_of_ne hne]
            exact hgarm es hges
          · intro ℓ hrl hct
            rcases eq_or_ne ℓ j with rfl | hneℓ
            · simp only [Node.pn, Node.dataTerm, Function.update_self,
                Function.update_of_ne hne] at hrl hct ⊢
              rw [hroleE] at hrl
              by_cases hadopt : (w.nodes ℓ).dn.pn.currentTerm < t
              · rw [if_pos hadopt] at hrl
                exact absurd hrl (by decide)
              · rw [if_neg hadopt] at hrl
                rw [hctE] at hct
                rw [if_neg hadopt] at hct
                obtain ⟨hmeq, hveq⟩ := hlidok hrl hadopt
                rw [hmapE, hmeq]
                exact hlarm ℓ hrl hct
            · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hne,
                Function.update_of_ne hneℓ] at hrl hct ⊢
              exact hlarm ℓ hrl hct
      · intro k hr tf bf hlast hbf t' v' hf
        rcases eq_or_ne k j with rfl | hne
        · simp only [Node.pn, Node.dataTerm, Function.update_self]
            at hr hlast hbf hf
          rw [hmapE] at hlast
          rw [hdurE] at hbf
          rw [hdtE] at hf
          have hlen1 : 1 ≤ o.newMap.length := by
            by_contra hc
            have hnil : o.newMap = [] := by
              cases hm : o.newMap with
              | nil => rfl
              | cons a l => rw [hm] at hc; simp at hc
            rw [hnil] at hlast
            cases hlast
          have hcple : commonPrefixLen (w.nodes k).dn.termMap (l0 :: ls)
              ≤ (w.nodes k).dn.termMap.length :=
            Uc2.commonPrefixLen_le_left _ _
          have hlenle : o.newMap.length ≤ (w.nodes k).dn.termMap.length := by
            rw [hnewtake, List.length_take]
            omega
          have hidxo : o.newMap[o.newMap.length - 1]? = some (tf, bf) := by
            rw [← List.getLast?_eq_getElem?]
            exact hlast
          have hlencp : o.newMap.length
              = commonPrefixLen (w.nodes k).dn.termMap (l0 :: ls) := by
            rw [hnewtake, List.length_take]
            omega
          have hidx : (w.nodes k).dn.termMap[o.newMap.length - 1]?
              = some (tf, bf) := by
            rw [← List.getElem?_take_of_lt
              (show o.newMap.length - 1 <
                commonPrefixLen (w.nodes k).dn.termMap (l0 :: ls) by omega),
              ← hnewtake]
            exact hidxo
          have hvle : o.validUpTo ≤ (w.nodes k).dn.pn.durable :=
            Uc2.reconcile_validUpTo_le _ _ _ _ hrec
          by_cases hadopt : (w.nodes k).dn.pn.currentTerm < t
          · rw [if_pos hadopt] at hf
            have hattr : TermMap.termAt (l0 :: ls) bf = t' :=
              (reachable_provInv hw).frame_gossip bf t t' v' (l0 :: ls) hf hmsg
            have hidx2 : (l0 :: ls)[o.newMap.length - 1]? = some (tf, bf) := by
              rw [← List.getElem?_take_of_lt
                  (show o.newMap.length - 1 <
                    commonPrefixLen (w.nodes k).dn.termMap (l0 :: ls) by
                      omega),
                ← Uc2.take_commonPrefixLen_eq, ← hnewtake]
              exact hidxo
            have hpin := termAt_entry_base hesasc hesstr hidx2
            rw [hattr] at hpin
            exact hpin
          · -- non-adopt: same-regime frames pin against the OLD state
            rw [if_neg hadopt] at hf
            rw [if_neg hadopt] at hr
            simp only [Bool.or_eq_true] at hr
            have hteq : t = (w.nodes k).dn.pn.currentTerm := by omega
            by_cases hdteq : (w.nodes k).dn.dataTerm
                = (w.nodes k).dn.pn.currentTerm
            · -- aligned: the regime IS t; pin against the delivered gossip
              rw [hdteq, ← hteq] at hf
              have hattr : TermMap.termAt (l0 :: ls) bf = t' :=
                (reachable_provInv hw).frame_gossip bf t t' v' (l0 :: ls) hf hmsg
              have hidx2 : (l0 :: ls)[o.newMap.length - 1]?
                  = some (tf, bf) := by
                rw [← List.getElem?_take_of_lt
                    (show o.newMap.length - 1 <
                      commonPrefixLen (w.nodes k).dn.termMap (l0 :: ls) by
                        omega),
                  ← Uc2.take_commonPrefixLen_eq, ← hnewtake]
                exact hidxo
              have hpin := termAt_entry_base hesasc hesstr hidx2
              rw [hattr] at hpin
              exact hpin
            · -- lagged: pre-state pin (gate_frames_eq below the old durable,
              -- gate_frontier_eq at it)
              have hrpre : (w.nodes k).reconciled = true := by
                rcases hr with hr | hdec
                · exact hr
                · exact absurd (of_decide_eq_true hdec) hdteq
              rcases Nat.lt_or_ge bf ((w.nodes k).dn.pn.durable) with hblt | hbge
              · have hfr : TermMap.termAt (w.nodes k).dn.termMap bf = t' :=
                  (reachable_provInv hw).gate_frames_eq k hrpre bf t' v' hf
                    hblt
                have hpin := termAt_entry_base hascj (h.strict_node k) hidx
                rw [hfr] at hpin
                exact hpin
              · have hbeq : bf = (w.nodes k).dn.pn.durable := by omega
                have hlastpre : (w.nodes k).dn.termMap.getLast?
                    = some (tf, bf) := by
                  rw [List.getLast?_eq_getElem?]
                  have hlenge : o.newMap.length
                      = (w.nodes k).dn.termMap.length := by
                    by_contra hc
                    have hlt : o.newMap.length
                        < (w.nodes k).dn.termMap.length := by omega
                    have hb := (h.strict_node k).base_lt
                      (i := o.newMap.length - 1) (j := o.newMap.length)
                      (by omega) hlt
                    have hmem : (w.nodes k).dn.termMap[o.newMap.length]'hlt
                        ∈ (w.nodes k).dn.termMap := List.getElem_mem _
                    have hlb :=
                      ((Data.reachable_mapInv
                        (reachable_project hw)).node k).last_base
                    cases hml : (w.nodes k).dn.termMap.getLast? with
                    | none =>
                      rw [List.getLast?_eq_none_iff] at hml
                      rw [hml] at hlt
                      simp at hlt
                    | some g =>
                      have hgd : g.2 ≤ (w.nodes k).dn.pn.durable := hlb g hml
                      have hble :=
                        ((Data.reachable_mapsWF
                          (reachable_project hw)) k).1.base_le_getLast hml
                          _ hmem
                      rw [List.getElem?_eq_getElem (by omega)] at hidx
                      have hie := Option.some.inj hidx
                      have : ((w.nodes k).dn.termMap[o.newMap.length - 1]'
                          (by omega)).2 = bf := by rw [hie]
                      omega
                  rw [← hlenge]
                  exact hidx
                exact h.gate_frontier_eq k hrpre tf bf hlastpre hbeq t' v' hf
        · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hne]
            at hr hlast hbf hf ⊢
          exact h.gate_frontier_eq k hr tf bf hlast hbf t' v' hf
    | noCommonPrefix =>
      obtain ⟨hmapE, hdurE, hhistE, hroleE, hctE, hdtE⟩ :=
        Data.applyGossip_ncp (w.nodes j).dn t hrec
      have hlidn : (w.nodes j).dn.pn.role = .leader →
          ¬ (w.nodes j).dn.pn.currentTerm < t → False := by
        intro hrl hnad
        have hteq : (w.nodes j).dn.pn.currentTerm = t := by omega
        have hpin := hlid j hrl hteq
        rw [hpin, Data.reconcile_self] at hrec
        cases hrec
      refine ⟨?_, ?_, ?_, ?_, ?_, ?_⟩
      · intro k
        rcases eq_or_ne k j with rfl | hne
        · simp only [Function.update_self]
          rw [hmapE]
          exact StrictBases.nil
        · simp only [Function.update_of_ne hne]
          exact h.strict_node k
      · exact h.strict_gossip
      · intro k hr es hg
        rcases eq_or_ne k j with rfl | hne
        · simp only [Node.pn, Node.dataTerm, Function.update_self] at hg ⊢
          rw [hmapE, hdurE]
          exact ⟨by simp, fun f hf => Nat.zero_le _⟩
        · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hne]
            at hr hg ⊢
          exact h.gate_take k hr es hg
      · intro k ℓ hr hrl hct
        rcases eq_or_ne ℓ j with rfl | hneℓ
        · exfalso
          rcases eq_or_ne k ℓ with rfl | hnek
          · simp only [Node.pn, Function.update_self] at hrl
            rw [hroleE] at hrl
            by_cases hadopt : (w.nodes k).dn.pn.currentTerm < t
            · rw [if_pos hadopt] at hrl
              exact absurd hrl (by decide)
            · rw [if_neg hadopt] at hrl
              exact hlidn hrl hadopt
          · simp only [Node.pn, Function.update_self,
              Function.update_of_ne hnek] at hrl
            rw [hroleE] at hrl
            by_cases hadopt : (w.nodes ℓ).dn.pn.currentTerm < t
            · rw [if_pos hadopt] at hrl
              exact absurd hrl (by decide)
            · rw [if_neg hadopt] at hrl
              exact hlidn hrl hadopt
        · rcases eq_or_ne k j with rfl | hnek
          · simp only [Node.pn, Node.dataTerm, Function.update_self,
              Function.update_of_ne hneℓ] at hct ⊢
            rw [hmapE, hdurE]
            exact ⟨by simp, fun f hf => Nat.zero_le _⟩
          · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hnek,
              Function.update_of_ne hneℓ] at hr hrl hct ⊢
            exact h.open_leader k ℓ hr hrl hct
      · intro u T d hrp hdtu
        rcases eq_or_ne u j with rfl | hne
        · simp only [Node.pn, Node.dataTerm, Function.update_self] at hdtu
          rw [hdtE] at hdtu
          by_cases hadopt : (w.nodes u).dn.pn.currentTerm < t
          · rw [if_pos hadopt] at hdtu
            exfalso
            have h1 : T ≤ (w.nodes u).dn.dataTerm :=
              (reachable_provInv hw).report_dt u T d hrp
            have h2 : (w.nodes u).dn.dataTerm
                ≤ (w.nodes u).dn.pn.currentTerm :=
              (Data.reachable_stamp (reachable_project hw)).data_le u
            omega
          · constructor
            · intro es hges
              simp only [Node.pn, Node.dataTerm, Function.update_self]
              rw [hmapE, hdurE]
              exact ⟨by simp, fun f hf => Nat.zero_le _⟩
            · intro ℓ hrl hct
              rcases eq_or_ne ℓ u with rfl | hneℓ
              · exfalso
                simp only [Node.pn, Function.update_self] at hrl
                rw [hroleE] at hrl
                rw [if_neg hadopt] at hrl
                exact hlidn hrl hadopt
              · simp only [Node.pn, Node.dataTerm, Function.update_self,
                  Function.update_of_ne hneℓ] at hrl hct ⊢
                rw [hmapE, hdurE]
                exact ⟨by simp, fun f hf => Nat.zero_le _⟩
        · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hne]
            at hdtu
          obtain ⟨hgarm, hlarm⟩ := h.report_take u T d hrp hdtu
          constructor
          · intro es hges
            simp only [Node.pn, Node.dataTerm, Function.update_of_ne hne]
            exact hgarm es hges
          · intro ℓ hrl hct
            rcases eq_or_ne ℓ j with rfl | hneℓ
            · exfalso
              simp only [Node.pn, Function.update_self] at hrl
              rw [hroleE] at hrl
              by_cases hadopt : (w.nodes ℓ).dn.pn.currentTerm < t
              · rw [if_pos hadopt] at hrl
                exact absurd hrl (by decide)
              · rw [if_neg hadopt] at hrl
                exact hlidn hrl hadopt
            · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hne,
                Function.update_of_ne hneℓ] at hrl hct ⊢
              exact hlarm ℓ hrl hct
      · intro k hr tf bf hlast hbf t' v' hf
        rcases eq_or_ne k j with rfl | hne
        · exfalso
          simp only [Function.update_self] at hlast
          rw [hmapE] at hlast
          cases hlast
        · simp only [Node.pn, Node.dataTerm, Function.update_of_ne hne]
            at hr hlast hbf hf ⊢
          exact h.gate_frontier_eq k hr tf bf hlast hbf t' v' hf
  | sendReport j hrole hgate =>
    refine ⟨fun k => h.strict_node k, h.strict_gossip, h.gate_take,
      h.open_leader, ?_, h.gate_frontier_eq⟩
    intro u T d hrp hdtu
    rcases List.mem_append.mp hrp with hrp | hrp
    · exact h.report_take u T d hrp hdtu
    · simp only [List.mem_singleton, CMsg.report.injEq] at hrp
      obtain ⟨rfl, rfl, rfl⟩ := hrp
      constructor
      · intro es hges
        rw [← hdtu] at hges
        exact h.gate_take u hgate es hges
      · intro ℓ hrl hct
        rw [← hdtu] at hct
        exact h.open_leader u ℓ hgate hrl hct

/-- The bundle holds in every reachable world. -/
theorem reachable_tkInv {n : Nat} {w : World n} (hw : Reachable w) :
    TkInv w := by
  induction hw with
  | refl => exact tk_init n
  | tail hprev hstep ih => exact tk_step hprev (hprev.tail hstep) ih hstep



#print axioms reachable_gossip_ne
#print axioms reachable_tkInv

/-! ## Within-regime stability, in its true regime-scoped form

The LC4b review's bonus (verdict 2): given the two take-facts, a
same-regime reconcile is provably CLEAN — the full common prefix survives
and `validUpTo` is exactly the durable. This is the regime-scoped repair
of the refuted amendment-2 statement (see
`bare_report_durable_stability_is_false`). -/

private theorem commonPrefixLen_take_self : ∀ (es : TermMap) (m : Nat),
    commonPrefixLen (es.take m) es = min m es.length
  | [], m => by simp [commonPrefixLen]
  | e :: es, 0 => by simp [commonPrefixLen]
  | e :: es, m + 1 => by
    simp only [List.take_succ_cons, commonPrefixLen, if_true]
    rw [commonPrefixLen_take_self es m]
    simp only [List.length_cons]
    omega

/-- **Take-disciplined reconciles are clean**: a node whose map is a take of
the delivered map, with its durable at-or-below the first beyond-take
entry's base, reconciles as the identity (`validUpTo = durable`, map
kept). -/
theorem take_reconcile_clean {own es : TermMap} {d : Nat}
    (hown : own = es.take own.length)
    (hbound : ∀ f ∈ es[own.length]?, d ≤ f.2) :
    Uc2.reconcile own d es = .ok ⟨d, own⟩ := by
  cases es with
  | nil =>
    simp [Uc2.reconcile]
  | cons l0 ls =>
    have hlen : own.length ≤ (l0 :: ls).length := by
      have h1 : own.length = ((l0 :: ls).take own.length).length := by
        conv_lhs => rw [hown]
      rw [List.length_take] at h1
      omega
    have hcp : commonPrefixLen own (l0 :: ls) = own.length := by
      conv_lhs => rw [hown]
      rw [commonPrefixLen_take_self]
      omega
    have hgate : own = [] ∨ commonPrefixLen own (l0 :: ls) ≠ 0 := by
      by_cases hoe : own = []
      · exact .inl hoe
      · right
        rw [hcp]
        intro hc
        exact hoe (List.length_eq_zero_iff.mp hc)
    rw [Uc2.reconcile_eq_clamped own d l0 ls hgate, hcp]
    have hnone : own[own.length]? = none :=
      List.getElem?_eq_none (Nat.le_refl _)
    simp only [Uc2.reconcile.reconcileClamped, hnone]
    cases hes : (l0 :: ls)[own.length]? with
    | none =>
      dsimp only
      simp [List.take_of_length_le (Nat.le_refl own.length),
        List.drop_of_length_le (Nat.le_refl own.length)]
    | some f =>
      have hd := hbound f hes
      have hnlt : ¬ f.2 < d := by omega
      dsimp only
      simp [hnlt, List.take_of_length_le (Nat.le_refl own.length),
        List.drop_of_length_le (Nat.le_refl own.length)]

#print axioms take_reconcile_clean

end Uc2.Cert
