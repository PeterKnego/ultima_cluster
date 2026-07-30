/-
Issue #7 — the durable dual-reader skew, lifted to WORLDS.

`Uc2Proofs/DurableSkew.lean` pins the arithmetic join in isolation: over bare
`Nat`s, joining `d ≤ vote-comparand` with `vote-comparand ≤ cd` is sound when the
comparand is the counter and FALSE when it lags. Those are conditional
statements — they say what follows IF a voter is ever in that situation. They do
not say the protocol can put one there.

This file closes that gap. Both halves are now about REACHABLE WORLDS of the
actual model:

* **Sound half — already proved, and cited here.**
  `Uc2.Cert.reachable_grant_report : Reachable w → GrantReport w` (`StageB`).
  In the shipped system a voter that reported `(T, d)` and then granted at a
  later term forces the candidate's advertised credential to dominate `d`. That
  is the world-level form of `grant_bridge_sound_on_counter`, and it holds
  because `logOk` reads the COUNTER — the same value `sendReport` stamps.

* **False half — proved here.** Under the PRE-`26d4827` grant rule, where
  `logOk` reads the ABSORBED COPY, `GrantReport` is refuted by an explicit
  13-step reachable trace. Not "unprovable": false, with a witness.

## Why a separate relation rather than a new `Step` constructor

Adding the counterfactual rule to `Cert.Step` would demand a new case in every
induction over `Step` in the corpus — some thirty of them, none of which have
anything to say about it. Instead `StaleStep` wraps the real `Step` and adds one
rule beside it. Every existing theorem keeps talking about `Step` and is
untouched; only this file knows the counterfactual exists.

This is also the honest reading of the pre-fix system: it was the shipped
protocol PLUS a grant rule that consulted the wrong value.

## The witness, in words

Node 0 wins term 1 and appends one byte. Nodes 1 and 2 both replicate it, so
both counters reach 1 — but neither consensus agent has polled, so both absorbed
copies are still 0. Node 1 REPORTS its counter (`report 1 1 1`), which is what a
leader would rank into `commit`. Node 2 then stands for election at term 2,
advertising its absorbed copy: `(lastTerm 1, cd 0)`.

Under the shipped rule node 1 refuses — it compares `(1, 1)` against `(1, 0)`.
Under the stale rule it compares `(1, 0)` against `(1, 0)`, a TIE, and grants.
So node 1 reported 1 and then certified a candidate advertising 0, which is
exactly `GrantReport`'s conclusion `d ≤ cd`, i.e. `1 ≤ 0`, refuted.
-/
import Uc2Proofs.StageB

namespace Uc2.Cert.SkewWorld

open Uc2 Uc2.Data Uc2.Cert

/-- The PRE-`26d4827` fresh-grant arm: `logOk` against `smDurable`, the
consensus agent's ABSORBED COPY, instead of against `durable`, the counter.
Byte-for-byte `PNode.recvRequestVote` otherwise. -/
def recvRequestVoteStale {n : Nat} (s : PNode n) (c : Fin n)
    (newTerm cLastTerm cDurable : Nat) : PNode n × Bool :=
  let s := if s.currentTerm < newTerm then s.adoptTerm newTerm else s
  match s.votedFor with
  | some (vt, vid) =>
    if vt = s.currentTerm then
      if vid = c then (s, true) else (s, false)
    else staleGrantIfFresh s c cLastTerm cDurable
  | none => staleGrantIfFresh s c cLastTerm cDurable
where
  staleGrantIfFresh (s : PNode n) (c : Fin n) (cLastTerm cDurable : Nat) :
      PNode n × Bool :=
    -- THE ONE CHARACTER THAT MATTERS: `s.smDurable`, not `s.durable`.
    if logOk s.lastTerm s.smDurable cLastTerm cDurable then
      ({ s with votedFor := some (s.currentTerm, c) }, true)
    else (s, false)

/-- The shipped protocol PLUS the stale grant rule. `shipped` embeds every real
step verbatim, so anything reachable in the model is reachable here. -/
inductive StaleStep {n : Nat} : World n → World n → Prop
  | shipped {w w' : World n} (hs : Step w w') : StaleStep w w'
  | staleGrant (w : World n) (j c : Fin n) (newTerm cLastTerm cDurable : Nat)
      (hmsg : Uc2.Msg.requestVote c newTerm cLastTerm cDurable ∈ w.sent)
      (hterm : (w.nodes j).pn.currentTerm ≤ newTerm) :
      StaleStep w
        { nodes := Function.update w.nodes j
            { w.nodes j with
              dn := { (w.nodes j).dn with
                pn := (recvRequestVoteStale (w.nodes j).pn c newTerm cLastTerm
                  cDurable).1
                dataTerm :=
                  if (w.nodes j).pn.currentTerm < newTerm then newTerm
                  else (w.nodes j).dn.dataTerm }
              reconciled :=
                if (w.nodes j).pn.currentTerm < newTerm then false
                else (w.nodes j).reconciled }
          sent := w.sent ++
            [.vote j c newTerm
              (recvRequestVoteStale (w.nodes j).pn c newTerm cLastTerm cDurable).2]
          dsent := w.dsent
          csent := w.csent
          committed := w.committed }

def StaleReachable {n : Nat} (w : World n) : Prop :=
  Relation.ReflTransGen StaleStep (World.init n) w

/-- The payload `GrantReport` delivers, stated directly: a voter `y` that
REPORTED `(T, d)` and then GRANTED candidate `c` at a later term `u` forces every
credential `c` advertised at `u` (with a matching last-term) to dominate `d`.

This is `Uc2.Cert.GrantReport` with its existential and its gossip escape hatch
stripped — the inequality that `crux_become_leader` actually consumes, and the
one `StageB` derives by joining `ReportEraFloor` with `logOk`. Under the SHIPPED
rule that join is sound (see `Uc2.Cert.reachable_grant_report`, which establishes
`GrantReport` for every reachable world, modulo the `Era` conditioning its
gossip arm carries). -/
def ReportDominatesCredential {n : Nat} (w : World n) : Prop :=
  ∀ (y c : Fin n) (u T d clt cd : Nat), y ≠ c →
    CMsg.report y T d ∈ w.csent →
    Uc2.Msg.vote y c u true ∈ w.sent →
    Uc2.Msg.requestVote c u clt cd ∈ w.sent →
    T < u → clt = T →
    d ≤ cd

/-- **THE LIFT.** Under the pre-`26d4827` grant rule, the report-to-credential
bridge is FALSE of a REACHABLE world — not merely unprovable, and not merely
false as arithmetic. There is a 13-step trace of the actual protocol that
produces a voter which reported `1` and then certified a candidate advertising
`0`.

Contrast `Uc2.Cert.reachable_grant_report`, which establishes the bridge for
every world reachable by the shipped rule. The two systems differ in exactly one
thing: whether the voter's `logOk` reads the counter or a copy of it taken at the
consensus agent's last poll. -/
theorem report_dominates_credential_false_under_stale_rule :
    ¬ ∀ w : World 3, StaleReachable w → ReportDominatesCredential w := by
  intro hAll
  -- Node 0 leads term 1 and appends byte 0; nodes 1 and 2 both replicate it, so
  -- both counters reach 1 while both absorbed copies are still 0; node 1 reports
  -- its COUNTER; node 2 stands at term 2 advertising its COPY (0); node 1 grants
  -- under the stale rule.
  have hreach : StaleReachable (n := 3) _ :=
    .tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail
      (.single (.shipped (.startElection _ 0 (by decide))))
      (.shipped (.deliverRequestVote _ 1 0 1 0 0 (by decide) (by decide))))
      (.shipped (.deliverVote _ 0 1 1 (by decide) (by decide) (by decide))))
      (.shipped (.becomeLeader _ 0 (by decide) (by decide))))
      (.shipped (.leaderAppend _ 0 42 (by decide))))
      (.shipped (.shipTermMap _ 0 (by decide))))
      (.shipped (.deliverTermMap _ 1 1 [(1, 0)] (by decide) (by decide))))
      (.shipped (.deliverReplicate _ 1 0 1 1 42 (by decide) (by decide)
        (by decide) (by decide))))
      (.shipped (.deliverTermMap _ 2 1 [(1, 0)] (by decide) (by decide))))
      (.shipped (.deliverReplicate _ 2 0 1 1 42 (by decide) (by decide)
        (by decide) (by decide))))
      (.shipped (.sendReport _ 1 (by decide) (by decide))))
      (.shipped (.startElection _ 2 (by decide))))
      -- The stale grant. Node 1 compares its ABSORBED COPY (0) against node 2's
      -- advertised credential (0) — a tie, so it grants. Under the shipped rule
      -- it would compare its COUNTER (1) against 0 and refuse.
      (.staleGrant _ 1 2 2 1 0 (by decide) (by decide))
  -- y = 1 reported (T, d) = (1, 1); c = 2 advertised (clt, cd) = (1, 0) at u = 2
  -- and was granted. The bridge would give 1 ≤ 0.
  have hbad : (1 : Nat) ≤ 0 :=
    hAll _ hreach 1 2 2 1 1 1 0 (by decide) (by decide) (by decide) (by decide)
      (by decide) (by decide)
  exact absurd hbad (by decide)

/-- The disagreement, isolated from any trace: at a voter whose last term is 1,
whose COUNTER is 1 and whose ABSORBED COPY is still 0, a candidate advertising
`(1, 0)` is REFUSED by the shipped rule and GRANTED by the stale one.

Everything the two rules see is identical except which of the node's two durable
positions `logOk` is handed. -/
theorem rules_disagree_on_a_lagging_voter (s : PNode 3)
    (hct : s.currentTerm = 1) (hlt : s.lastTerm = 1)
    (hdur : s.durable = 1) (hsm : s.smDurable = 0) :
    (s.recvRequestVote 2 2 1 0).2 = false ∧
      (recvRequestVoteStale s 2 2 1 0).2 = true := by
  constructor <;>
    simp [PNode.recvRequestVote, PNode.recvRequestVote.grantIfFresh,
      recvRequestVoteStale, recvRequestVoteStale.staleGrantIfFresh,
      PNode.adoptTerm, logOk, hct, hlt, hdur, hsm]

#print axioms report_dominates_credential_false_under_stale_rule
#print axioms rules_disagree_on_a_lagging_voter

end Uc2.Cert.SkewWorld
