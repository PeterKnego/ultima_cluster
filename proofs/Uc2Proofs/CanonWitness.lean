import Uc2Proofs.StageC

/-! LC4h(F-A) — a satisfiability witness for `Canon` at `k > 0`.

`Canon w T 0` is trivially true (`preK _ 0 = []`, and every floor clause is
`0 ≤ _`), so the canon bundle could in principle be vacuous — or false —
without any consumer noticing. This file exhibits a REACHABLE world, a term
`T = 1` and a position `k = 1 > 0` at which `RepQuorum w T k` AND
`Canon w T k` both hold: the positive evidence the LC4g review (F-A) asked
for before canon's 15-constructor induction is authorized.

The trace is the 9-step prefix of `nonvacuity_commit_completeness_trace`
(`ProtocolCommit.lean`): node 0 wins term 1, appends payload `42` at
position 0, gossips its map so node 1's intake gate reopens, replicates the
record to node 1, and node 1 reports its durable `1` on the commit wire.
That is the shortest configuration realizing `RepQuorum`'s conjuncts at
`k = 1` — a `k`-floored report needs a reporter durable through `k`, which
needs the data path, which needs the gossip that reopens the gate.

**Kernel-cost note (corpus lore, extended).** The known rule is "harvest
decidable facts in ONE `refine` while `w` is still the concrete unified
term". Two further costs showed up here and are worth recording:

1. `Cert` is a `structure`, so it has no `Decidable` instance — `by decide`
   on `Data.Cert w.project T ℓ` does not merely fail, it stalls the
   elaborator on the giant world term. Build it constructor-first
   (`⟨⟨Q, by decide, by decide⟩, by decide, by decide⟩`); each FIELD is
   decidable.
2. Any `rcases`/`subst` performed while the giant world sits in the goal is
   catastrophic (>150 s for a two-case `Finset.mem_insert` split, vs. 2.8 s
   for the whole trace). The fix is to move every case split into a lemma
   whose world is a VARIABLE (`forall_mem_pair`, `canon_of_flat` below) and
   feed it flat, decidable facts from the call site. Both helpers are
   world-generic and instantiate in constant time. -/

namespace Uc2.Cert

open Uc2.Data (Frame)

/-- Abstract pair-membership splitter: keeps the `Finset` case analysis away
from the (kernel-transparent, giant) world term. -/
private theorem forall_mem_pair {α : Type _} [DecidableEq α] {a b : α}
    {P : α → Prop} (ha : P a) (hb : P b) :
    ∀ u ∈ ({a, b} : Finset α), P u := by
  intro u hu
  rcases Finset.mem_insert.mp hu with rfl | hu'
  · exact ha
  · rw [Finset.mem_singleton.mp hu']; exact hb

/-- **Canon from flat facts.** Assembles `Canon w T k` for a world whose
data wire is exactly one below-`T`-or-`T` replicate plus one non-above-`T`
gossip, whose commit wire is exactly one term-`T` report, and each of whose
nodes either carries the canonical map `c` or fails every `Canonical` arm.
`w` is a VARIABLE here on purpose: all case analysis happens against small
terms, and the call site supplies only `decide`-discharged facts. -/
private theorem canon_of_flat {n : Nat} {w : World n} {T k : Nat}
    {c : TermMap} {p0 h0 t0 v0 u0 : Nat} {es0 : TermMap} {y : Fin n}
    {d0 : Nat}
    (hds : w.dsent = [Frame.replicate p0 h0 t0 v0, Frame.gossip u0 es0])
    (hcs : w.csent = [CMsg.report y T d0])
    (ht0 : t0 ≤ T) (hu0 : u0 ≤ T)
    (hpast : ∀ j : Fin n, T < Data.lastTermOf (w.nodes j).dn.termMap →
      k ≤ (w.nodes j).pn.durable)
    (habove : ∀ j : Fin n, T < Data.lastTermOf (w.nodes j).dn.termMap →
      ∀ e ∈ (w.nodes j).dn.termMap, T < e.1 → k ≤ e.2)
    (hrep : k ≤ d0 → k ≤ (w.nodes y).pn.durable)
    (hnode : ∀ j : Fin n, (w.nodes j).dn.termMap = c ∨
      (¬ (T < Data.lastTermOf (w.nodes j).dn.termMap) ∧
       ¬ (Data.lastTermOf (w.nodes j).dn.termMap = T ∧
          k ≤ (w.nodes j).pn.durable) ∧ j ≠ y)) :
    Canon w T k := by
  -- The two wires, as membership facts.
  have hgno : ∀ (u : Nat) (es : TermMap), Frame.gossip u es ∈ w.dsent →
      u = u0 := by
    intro u es hg
    rw [hds] at hg
    rcases List.mem_cons.mp hg with h | h
    · exact absurd h (by simp)
    · rcases List.mem_cons.mp h with h | h
      · exact (Frame.gossip.injEq .. ▸ h : u = u0 ∧ es = es0).1
      · exact absurd h (by simp)
  have hrno : ∀ (pos hdr t v : Nat), Frame.replicate pos hdr t v ∈ w.dsent →
      t = t0 := by
    intro pos hdr t v hf
    rw [hds] at hf
    rcases List.mem_cons.mp hf with h | h
    · exact (Frame.replicate.injEq .. ▸ h :
        pos = p0 ∧ hdr = h0 ∧ t = t0 ∧ v = v0).2.2.1
    · rcases List.mem_cons.mp h with h | h
      · exact absurd h (by simp)
      · exact absurd h (by simp)
  have hcno : ∀ (z : Fin n) (t d : Nat), CMsg.report z t d ∈ w.csent →
      z = y ∧ t = T ∧ d = d0 := by
    intro z t d hr
    rw [hcs] at hr
    rcases List.mem_cons.mp hr with h | h
    · exact CMsg.report.injEq .. ▸ h
    · exact absurd h (by simp)
  -- Every `Canonical` map is `c`.
  have hcanon : ∀ m : TermMap, Canonical w T k m → m = c := by
    intro m hm
    rcases hm with ⟨j, rfl, harm⟩ | ⟨u, hu, hg⟩
    · rcases hnode j with h | ⟨n1, n2, n3⟩
      · exact h
      · exfalso
        rcases harm with h | h | ⟨d, hkd, hr⟩
        · exact n1 h
        · exact n2 h
        · exact n3 (hcno j T d hr).1
    · exact absurd (hgno u m hg ▸ hu : T < u0) (by omega)
  refine ⟨fun m₁ m₂ h1 h2 => by rw [hcanon m₁ h1, hcanon m₂ h2], hpast,
    habove, ?_, ?_, ?_⟩
  · intro u es hg hu
    exact absurd (hgno u es hg ▸ hu : T < u0) (by omega)
  · intro pos hdr t v hf ht
    exact absurd (hrno pos hdr t v hf ▸ ht : T < t0) (by omega)
  · intro z d hr hkd
    obtain ⟨rfl, -, rfl⟩ := hcno z T d hr
    exact hrep hkd

/-- **Non-vacuity of `Canon` at `k > 0`.** In the 3-node world reached by
the trace below, `RepQuorum w 1 1` holds (writer `ℓ = 0`, quorum
`Q = {0, 1}`, base frame `replicate 0 1 1 42` at `bT = 0 < 1`, node 1's
`report 1 1 1` floored at `k = 1`) and `Canon w 1 1` holds NONTRIVIALLY:

* `agree` has content: nodes 0 and 1 are both `Canonical` — node 1 via two
  arms at once (`lastTermOf = T` and durable through `k`, AND the
  `k`-floored `T` reporter) — carrying the same map `[(1, 0)]`, whose
  `preK _ 1` is NONEMPTY. So this world also meets the `hne` side condition
  of `canon_reconcile_clean`: the witness feeds canon's CONSUMER, not just
  its statement. Node 2 (never contacted) fails every arm, which the proof
  must discharge rather than compute.
* `rep_floor` is discharged on a REAL wire report (`report 1 1 1`, `k ≤ d`),
  not vacuously: node 1's durable genuinely is `1`.
* `past_floor`/`above`/`gossip_above`/`wire` hold vacuously at `T = 1` here,
  because nothing in the trace has moved past term 1 — expected at a birth
  site, and exactly the regime canon's base cases live in.

So the canon bundle is satisfiable off `k = 0`: the LC4g obstruction
(Finding #11) is one of proof SCOPE, not of the statement being false. -/
theorem canon_sat_witness :
    ∃ (w : World 3) (T k : Nat), Reachable w ∧ 0 < k ∧
      RepQuorum w T k ∧ Canon w T k ∧
      -- The bundle is non-vacuous HERE, machine-checked rather than argued:
      -- two DISTINCT nodes are `Canonical` (node 0 by the `lastTermOf = T`
      -- durable arm, node 1 by the `k`-floored REPORT arm), so `agree` has
      -- something to say, and the prefix it pins is NONEMPTY.
      (∃ i j : Fin 3, i ≠ j ∧
        Canonical w T k (w.nodes i).dn.termMap ∧
        Canonical w T k (w.nodes j).dn.termMap ∧
        preK (w.nodes i).dn.termMap k ≠ []) := by
  refine ⟨_, 1, 1,
    .tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.single (.startElection _ 0 (by decide)))
      (.deliverRequestVote _ 1 0 1 0 0 (by decide) (by decide)))
      (.deliverVote _ 0 1 1 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)))
      (.leaderAppend _ 0 42 (by decide)))
      (.shipTermMap _ 0 (by decide)))
      (.deliverTermMap _ 1 1 [(1, 0)] (by decide) (by decide)))
      (.deliverReplicate _ 1 0 1 1 42 (by decide) (by decide) (by decide)
        (by decide)))
      (.sendReport _ 1 (by decide) (by decide)),
    by decide,
    -- `RepQuorum w 1 1`: writer 0, quorum {0, 1}, base frame at bT = 0.
    ⟨by decide, 0, {0, 1}, 0, 42,
      ⟨⟨{0, 1}, by decide, by decide⟩, by decide, by decide⟩,
      by decide, by decide, by decide,
      forall_mem_pair (P := fun u : Fin 3 =>
          u = 0 ∨ ∃ d, 1 ≤ d ∧ CMsg.report u 1 d ∈ _)
        (.inl rfl) (.inr ⟨1, le_refl 1, by decide⟩)⟩,
    -- `Canon w 1 1`.
    canon_of_flat (c := [(1, 0)]) (p0 := 0) (h0 := 1) (t0 := 1) (v0 := 42)
      (u0 := 1) (es0 := [(1, 0)]) (y := 1) (d0 := 1)
      (by decide) (by decide) (by decide) (by decide) (by decide)
      (by decide) (by decide) (by decide),
    -- Non-vacuity: node 0 (durable arm) and node 1 (report arm).
    0, 1, by decide,
    .inl ⟨0, rfl, .inr (.inl ⟨by decide, by decide⟩)⟩,
    .inl ⟨1, rfl, .inr (.inr ⟨1, by decide, by decide⟩)⟩,
    by decide⟩

#print axioms canon_sat_witness

end Uc2.Cert
