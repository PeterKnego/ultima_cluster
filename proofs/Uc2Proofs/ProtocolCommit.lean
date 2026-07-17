import Uc2Model.Commit
import Uc2Proofs.ProtocolData
import Uc2Proofs.LogMatching

/-! LB1 — commit certification over the data plane.

Layered extension of the Tier B(a) data-plane model
(`Uc2Proofs/ProtocolData.lean`): each node gains the commit-certification
state of `uc2_consensus` — the REAL `Uc2Model.CommitTracker` kernel
(consumed, not re-modeled; its C1–C5 theorems in `Uc2Proofs/Commit.lean` are
LB2's quorum extractor), a commit watermark, and the reconciliation intake
gate — plus the commit transitions: durable reports as messages, the leader's
tracker fold, and the tracker-advance commit event recorded in an append-only
ghost ledger on the world.

Modeling decisions (settled at plan time, brief decisions 1–4, plus the
designer calls documented inline):

1. **Commit is an event in a world-level ghost** (decision 1):
   `committed : List (Nat × Nat × Nat)` — (position, term-stamp, payload)
   triples appended by `leaderAdvanceCommit` from the leader's own `hist`.
   The ghost is a PROOF DEVICE mirrored by no Rust state (the closest analog
   is the sequence of `Action::AdvanceCommit` values crossing positions);
   append-only makes committed-stability trivial, and the content claims
   (quorum held it, never truncated, later leaders have it) are theorems.
2. **Reports are messages on their own wire** (decision 2): `CMsg.report
   (src) (term) (durable)` mirrors the AppendPosition datagram
   (`uc2_net/src/receiver.rs` 1052–1078; header carries
   `leadership_term_id`). A third sent-set `csent` keeps the projection onto
   the data model a strict field erasure, exactly like LA1's `dsent`.
3. **Reports are truthful at send, adversarially stale in flight**
   (decision 3): `sendReport`'s successor pins the message to the sender's
   ACTUAL `currentTerm` and `durable` at emission. Nothing guards staleness
   at delivery — a report can arrive after the sender truncated. UC survives
   the vote-path staleness via term-scoped reports (the leader drops
   `term < current_term`, `election.rs` 546–548, and `become_leader` resets
   the tracker — C4) plus the reconciliation intake gate (call 6 below).
4. **THE load-bearing guard: the reconciliation intake gate.** Rust
   suppresses AppendPosition ENTIRELY while the gate is closed
   (`receiver.rs` 1052–1061 "SUPPRESSED ENTIRELY... phantom-commit source"),
   the node closes the gate on adopting a strictly-new term
   (`node.rs::exec` `Action::BecomeFollower`, ~2474–2478:
   `close_gate(); awaiting_reconcile = true`), and reopens it only when
   reconciliation for the adopted term completes (`node.rs` ~2381–2391
   clean-reconcile arm; `on_truncated` ~2710–2722) or on `BecomeLeader`
   (~2456–2458: a leader is the source of truth). Modeled as
   `reconciled : Bool` on the node. WITHOUT this gate the model admits a
   genuine phantom commit with no crash at all: a follower that adopted the
   leader's term via the vote path but still holds a longer divergent tail
   would report a truthful `durable` covering bytes that do NOT match the
   leader's log, and the quorum rank would certify positions only the leader
   holds. Truthful-at-send is NOT enough — the durable is honest, the
   CONTENT under it is not; the gate is what ties a term-T report to a
   hist that reconciled against the T-leader's map.
5. **FINDING (candidate real gap): the gate does not survive reboot.**
   Rust boots the intake gate OPEN (`node.rs` 516,
   `AtomicBool::new(true)` "open until a term is adopted") with
   `awaiting_reconcile: false` (`node.rs` 801), while term recovery is
   `current_term = vote_term.max(map_term)` (`election.rs` 400–402) — so a
   voter that persisted a vote at term T (persist-before-send), crashed
   before reconciling, and rebooted, reports `(T, durable)` over its raw
   divergent tail; the receiver's AppendPosition floor re-send
   (`receiver.rs` 1061) races the leader's kHz term-map gossip. Modeled
   faithfully: `crashRestart` sets `reconciled := true`. Consequence,
   machine-checked below (`finding_boot_gate_stale_report_lc_violation`):
   the FIXED LC-core statement is FALSE for Rust-as-of-today semantics.
   The candidate fix (boot the gate closed when `vote_term > map_term`,
   i.e. model `reconciled := currentTerm ≤ lastTermOf termMap` at restart)
   is escalated with the trace; see the task report.
6. **Tracker state lives on every node, meaningful while leader** (mirrors
   Rust: `ElectionSm.tracker` exists in every role; only the leader feeds
   and ranks it, `election.rs` 566–571). `becomeLeader` resets the report
   slots (`election.rs` 1050 `tracker.reset_reports()` — C4);
   `crashRestart` rebuilds it fresh (`election.rs` 410: constructed anew at
   boot). The M7 carried-reports vector (`last_reports`, promote-learner
   precondition only) is out of scope — fixed membership, no reconfig.
7. **Dense follower slots via a fixed order-preserving map** (recommended
   shape): `slotOf i j` mirrors `election.rs::follower_slot` (1250–1262:
   members in id order, skipping self). Delivery requires `src ≠ i`
   (`follower_slot(self) = None` — a self-report is dropped, so the no-op
   needs no constructor).
8. **`commitIdx` is the leader's ghost-append cursor** — the analog of the
   cnc commit counter the node stores on `Action::AdvanceCommit`
   (`node.rs` "The ONLY commit store in the binary"). Monotone
   (`max`-folded), never reset on term transitions (UC's commit counter
   never regresses across terms), and — a designer call — preserved across
   `crashRestart` even though Rust's in-memory `commit_seen` reboots to 0:
   the cursor only selects which newly-crossed positions enter the
   append-only ghost, so preserving it merely avoids duplicate ghost
   entries; every append is still individually quorum-certified by the
   kernel `advance` that fired it. (Re-certification duplicates would be
   equally honest, just noisier for LB2.)
9. **Omissions, documented**: (a) commit gossip / follower `commit_seen`
   (decision 4 — YAGNI: leader completeness is about the leader's hist);
   (b) the Report higher-term-adoption arm (`election.rs` 549–551) — the
   data model has no bare-adoption step to project onto, and the same
   adoptions are reachable via vote/gossip carriers; omitting a transition
   only shrinks the reachable set (scope note for the re-gate); (c)
   candidate reports: Rust's node-level `term_handle` moves only on
   `Become{Leader,Follower}` (`Action::StartElection` touches neither term
   handle nor gate, `node.rs` 2418–2428), so a candidate's reports carry
   its PRE-BUMP term — deliverable only to the previous-term leader,
   equivalent to having reported before candidacy. `sendReport` is
   follower-only; the model has one term per node, so allowing candidates
   would stamp the bumped term, a behavior Rust cannot produce.

Projection: every commit-layer step is a stutter or a verbatim mirror on
the data plane, so `Data.Reachable` lifts by simulation and both
`election_safety` and `log_matching` are inherited for free (proved FIRST,
per the LA1 pattern). -/

namespace Uc2.Cert

/-- `election.rs::follower_slot` (1250–1262): dense `CommitTracker` slot for
member `j` at leader `i` — members in id order, skipping self. Meaningful
for `j ≠ i` (Rust returns `None` for self; the model's `deliverReport`
requires `src ≠ i`). -/
def slotOf {n : Nat} (i j : Fin n) : Nat :=
  if j.val < i.val then j.val else j.val - 1

/-- The slot map is injective away from self — LB2's quorum-counting needs
distinct reporters to occupy distinct tracker slots. -/
theorem slotOf_inj {n : Nat} (i j k : Fin n) (hj : j ≠ i) (hk : k ≠ i)
    (h : slotOf i j = slotOf i k) : j = k := by
  have hj' : j.val ≠ i.val := fun hc => hj (Fin.ext hc)
  have hk' : k.val ≠ i.val := fun hc => hk (Fin.ext hc)
  unfold slotOf at h
  apply Fin.ext
  split at h <;> split at h <;> omega

/-- Slots stay inside the tracker's `n - 1` follower band. -/
theorem slotOf_lt {n : Nat} (i j : Fin n) (hj : j ≠ i) :
    slotOf i j < n - 1 := by
  have hj' : j.val ≠ i.val := fun hc => hj (Fin.ext hc)
  have := j.isLt
  have := i.isLt
  unfold slotOf
  split <;> omega

/-- One node: the whole data-plane node (`dn`, untouched `Uc2.Data.Node`) +
the commit-certification state:

- `tracker` — the REAL `Uc2Model.CommitTracker` kernel
  (`uc2_consensus/src/commit.rs`, `ElectionSm.tracker`).
- `commitIdx` — the leader's commit watermark / ghost-append cursor (the
  cnc commit counter stored on `Action::AdvanceCommit`; module doc, item 8).
- `reconciled` — the reconciliation intake gate, `true` = open (the node's
  `intake_gate` ∧ ¬`awaiting_reconcile`; module doc, items 4–5). -/
structure Node (n : Nat) where
  dn         : Uc2.Data.Node n
  tracker    : CommitTracker
  commitIdx  : Nat
  reconciled : Bool

/-- Election-slice sugar so the LC-core statement reads on its brief shape. -/
def Node.pn {n : Nat} (c : Node n) : PNode n := c.dn.pn

/-- Data-plane history sugar (position ↦ (term-stamp, payload)). -/
def Node.hist {n : Nat} (c : Node n) : Nat → Option (Nat × Nat) := c.dn.hist

/-- The commit wire: the AppendPosition datagram (`receiver.rs` 1062–1073,
`DGRAM_KIND_APPEND_POSITION`; consumed as `Event::Report { from, term,
durable }`, `election.rs` 545–573). Term-scoped: the header's
`leadership_term_id` is what the leader's stale-drop compares. -/
inductive CMsg (n : Nat) where
  | report (src : Fin n) (term durable : Nat)
deriving DecidableEq

/-- The cluster: per-node state, the election wire and data wire (both
projected verbatim onto the Tier B(a) model), the commit wire (erased by
the projection), and the append-only ghost commit ledger (module doc,
item 1 — world-level, mirrored by no Rust state). -/
structure World (n : Nat) where
  nodes     : Fin n → Node n
  sent      : List (Uc2.Msg n)
  dsent     : List Uc2.Data.Frame
  csent     : List (CMsg n)
  committed : List (Nat × Nat × Nat)

/-- Boot: the data model's boot per node + a fresh tracker
(`CommitTracker::new(n_members - 1, n_members)`, `election.rs` 410), zero
watermark, gate open (`node.rs` 516 "open until a term is adopted"). -/
def World.init (n : Nat) : World n :=
  { nodes := fun _ =>
      { dn := { pn := { currentTerm := 0, votedFor := none, role := .follower,
                        votesReceived := ∅, lastTerm := 0, durable := 0 },
                termMap := [], hist := fun _ => none }
        tracker := CommitTracker.new (n - 1) n
        commitIdx := 0
        reconciled := true }
    sent := [], dsent := [], csent := [], committed := [] }

/-- The ghost entries a commit advance appends: every position in
`[lo, hi)` the leader's hist defines, as `(position, term-stamp, payload)`.
(`filterMap`: positions the leader does not hold are skipped — under the
LA2 contiguity invariant and C2's own-durable clamp there are none.) -/
def ghostEntries (h : Nat → Option (Nat × Nat)) (lo hi : Nat) :
    List (Nat × Nat × Nat) :=
  (List.range' lo (hi - lo)).filterMap fun p => (h p).map fun tv => (p, tv.1, tv.2)

/-- One cluster transition. Constructors 1–11 mirror the data model's steps
verbatim on the `dn` slice, adding only the gate/tracker bookkeeping each
Rust site performs; 12–14 are the commit plane. Docstrings on the mirrors
defer to `Uc2Proofs/ProtocolData.lean`'s originals and name only the
commit-relevant Rust delta. -/
inductive Step {n : Nat} : World n → World n → Prop
  /-- `Data.Step.startElection`. Commit delta: NONE — `Action::StartElection`
  touches neither the term handle nor the gate (`node.rs` 2418–2428); the
  candidate's tracker is untouched until `become_leader`. -/
  | startElection (w : World n) (i : Fin n)
      (hrole : (w.nodes i).pn.role ≠ .leader) :
      Step w
        { nodes := Function.update w.nodes i
            { w.nodes i with dn :=
              { (w.nodes i).dn with pn :=
                { (w.nodes i).pn with
                  currentTerm := (w.nodes i).pn.currentTerm + 1
                  votedFor := some ((w.nodes i).pn.currentTerm + 1, i)
                  role := .candidate
                  votesReceived := {i} } } }
          sent := w.sent ++
            [.requestVote i ((w.nodes i).pn.currentTerm + 1)
              (w.nodes i).pn.lastTerm (w.nodes i).pn.durable]
          dsent := w.dsent
          csent := w.csent
          committed := w.committed }
  /-- `Data.Step.deliverRequestVote`. Commit delta: adopting a strictly
  higher term closes the intake gate (`adopt_term` → `Action::BecomeFollower`
  → `node.rs` ~2474–2478 `close_gate(); awaiting_reconcile = true`); a
  same-term delivery leaves it as-is. Tracker untouched (only
  `become_leader` resets it). -/
  | deliverRequestVote (w : World n) (j c : Fin n)
      (newTerm cLastTerm cDurable : Nat)
      (hmsg : Uc2.Msg.requestVote c newTerm cLastTerm cDurable ∈ w.sent)
      (hterm : (w.nodes j).pn.currentTerm ≤ newTerm) :
      Step w
        { nodes := Function.update w.nodes j
            { w.nodes j with
              dn := { (w.nodes j).dn with pn :=
                ((w.nodes j).pn.recvRequestVote c newTerm cLastTerm cDurable).1 }
              reconciled :=
                if (w.nodes j).pn.currentTerm < newTerm then false
                else (w.nodes j).reconciled }
          sent := w.sent ++
            [.vote j c newTerm
              ((w.nodes j).pn.recvRequestVote c newTerm cLastTerm cDurable).2]
          dsent := w.dsent
          csent := w.csent
          committed := w.committed }
  /-- `Data.Step.rejectStaleRequestVote` — no state, no commit delta. -/
  | rejectStaleRequestVote (w : World n) (j c : Fin n)
      (newTerm cLastTerm cDurable : Nat)
      (hmsg : Uc2.Msg.requestVote c newTerm cLastTerm cDurable ∈ w.sent)
      (hstale : newTerm < (w.nodes j).pn.currentTerm) :
      Step w
        { nodes := w.nodes
          sent := w.sent ++ [.vote j c (w.nodes j).pn.currentTerm false]
          dsent := w.dsent
          csent := w.csent
          committed := w.committed }
  /-- `Data.Step.deliverVote` (counting arm) — same-term by enabling, no
  commit delta. -/
  | deliverVote (w : World n) (i v : Fin n) (t : Nat)
      (hmsg : Uc2.Msg.vote v i t true ∈ w.sent)
      (hrole : (w.nodes i).pn.role = .candidate)
      (hterm : (w.nodes i).pn.currentTerm = t) :
      Step w
        { nodes := Function.update w.nodes i
            { w.nodes i with dn :=
              { (w.nodes i).dn with pn :=
                { (w.nodes i).pn with
                  votesReceived := insert v (w.nodes i).pn.votesReceived } } }
          sent := w.sent
          dsent := w.dsent
          csent := w.csent
          committed := w.committed }
  /-- `Data.Step.deliverVoteHigherTerm`. Commit delta: strictly-higher-term
  adoption closes the gate (as in `deliverRequestVote`). -/
  | deliverVoteHigherTerm (w : World n) (i v : Fin n) (t : Nat) (g : Bool)
      (hmsg : Uc2.Msg.vote v i t g ∈ w.sent)
      (hterm : (w.nodes i).pn.currentTerm < t) :
      Step w
        { nodes := Function.update w.nodes i
            { w.nodes i with
              dn := { (w.nodes i).dn with pn := (w.nodes i).pn.adoptTerm t }
              reconciled := false }
          sent := w.sent
          dsent := w.dsent
          csent := w.csent
          committed := w.committed }
  /-- `Data.Step.becomeLeader`. Commit delta: fresh follower slots —
  `tracker.reset_reports()` (`election.rs` 1050, C4: stale-term reports must
  not certify bytes in the new term) — and the gate opens (`node.rs`
  `Action::BecomeLeader` arm: "A leader is the source of truth; no reconcile
  pending"). `commitIdx` is NOT reset: the commit counter is monotone across
  terms (module doc, item 8). -/
  | becomeLeader (w : World n) (i : Fin n)
      (hrole : (w.nodes i).pn.role = .candidate)
      (hquorum : n / 2 + 1 ≤ (w.nodes i).pn.votesReceived.card) :
      Step w
        { nodes := Function.update w.nodes i
            { dn := { pn := { (w.nodes i).pn with
                        role := .leader
                        lastTerm := (w.nodes i).pn.currentTerm }
                      termMap := Uc2.Data.prunePush (w.nodes i).dn.termMap
                        (w.nodes i).pn.currentTerm (w.nodes i).pn.durable
                      hist := (w.nodes i).dn.hist }
              tracker := (w.nodes i).tracker.resetReports
              commitIdx := (w.nodes i).commitIdx
              reconciled := true }
          sent := w.sent
          dsent := w.dsent
          csent := w.csent
          committed := w.committed }
  /-- `Data.Step.crashRestart`. Commit delta: the tracker is rebuilt fresh
  (`election.rs` 410 — in-memory, reconstructed at boot) and the intake gate
  boots OPEN (`node.rs` 516 + 801) even though the recovered term is
  `vote_term.max(map_term)` (`election.rs` 400–402) — THE FINDING (module
  doc, item 5): a pre-crash strictly-higher-term adoption's gate closure
  does not survive the reboot. `commitIdx` preserved (ghost cursor —
  designer call, module doc item 8). -/
  | crashRestart (w : World n) (i : Fin n) :
      Step w
        { nodes := Function.update w.nodes i
            { dn := { (w.nodes i).dn with pn :=
                { (w.nodes i).pn with role := .follower, votesReceived := ∅ } }
              tracker := CommitTracker.new (n - 1) n
              commitIdx := (w.nodes i).commitIdx
              reconciled := true }
          sent := w.sent
          dsent := w.dsent
          csent := w.csent
          committed := w.committed }
  /-- `Data.Step.leaderAppend` — no commit delta (the ranking picks the new
  durable up on the next `advance`). -/
  | leaderAppend (w : World n) (i : Fin n) (v : Nat)
      (hrole : (w.nodes i).pn.role = .leader) :
      Step w
        { nodes := Function.update w.nodes i
            { w.nodes i with dn :=
              { pn := { (w.nodes i).pn with
                  durable := (w.nodes i).pn.durable + 1 }
                termMap := (w.nodes i).dn.termMap
                hist := Function.update (w.nodes i).dn.hist
                  (w.nodes i).pn.durable
                  (some ((w.nodes i).pn.currentTerm, v)) } }
          sent := w.sent
          dsent := w.dsent ++
            [.replicate (w.nodes i).pn.durable (w.nodes i).pn.currentTerm v]
          csent := w.csent
          committed := w.committed }
  /-- `Data.Step.deliverReplicate` + the intake gate on the DATA arm: a
  closed gate DROPS inbound data (`receiver.rs` 659–665 `dropped_gated`) —
  reconcile-before-data, the ordering `become_leader`'s phantom-prune NOTE
  leans on. Enabling-only strengthening of the data model's step. -/
  | deliverReplicate (w : World n) (j : Fin n) (pos t v : Nat)
      (hmsg : Uc2.Data.Frame.replicate pos t v ∈ w.dsent)
      (hpos : pos = (w.nodes j).pn.durable)
      (hstamp : t ≤ (w.nodes j).pn.currentTerm)
      (hgate : (w.nodes j).reconciled = true) :
      Step w
        { nodes := Function.update w.nodes j
            { w.nodes j with dn := (w.nodes j).dn.recvReplicate pos t v }
          sent := w.sent
          dsent := w.dsent
          csent := w.csent
          committed := w.committed }
  /-- `Data.Step.shipTermMap` — no state, no commit delta. -/
  | shipTermMap (w : World n) (i : Fin n)
      (hrole : (w.nodes i).pn.role = .leader) :
      Step w
        { nodes := w.nodes
          sent := w.sent
          dsent := w.dsent ++
            [.gossip (w.nodes i).pn.currentTerm (w.nodes i).dn.termMap]
          csent := w.csent
          committed := w.committed }
  /-- `Data.Step.deliverTermMap`. Commit delta: reconciliation for the
  (possibly just-adopted) term completes atomically with the truncation
  pipeline (LA1 module doc, item 5), so the gate reopens — the
  clean-reconcile arm (`node.rs` ~2381–2391) and the matching-epoch
  `on_truncated` reopen (~2710–2722) collapsed into the same atomic step. -/
  | deliverTermMap (w : World n) (j : Fin n) (t : Nat) (entries : TermMap)
      (hmsg : Uc2.Data.Frame.gossip t entries ∈ w.dsent)
      (hterm : (w.nodes j).pn.currentTerm ≤ t) :
      Step w
        { nodes := Function.update w.nodes j
            { w.nodes j with
              dn := (w.nodes j).dn.applyGossip t entries
              reconciled := true }
          sent := w.sent
          dsent := w.dsent
          csent := w.csent
          committed := w.committed }
  /-- AppendPosition emission (`receiver.rs` 1049–1078): a FOLLOWER whose
  intake gate is open reports its durable, stamped with its current term
  (the datagram's `leadership_term_id` — node-level `term_handle`, in sync
  with `currentTerm` for a follower). Truthful at send (decision 3): both
  fields are the sender's actual state at emission; staleness happens in
  flight. Follower-only: a leader's report is self-dropped
  (`follower_slot(self) = None`), and a candidate's is stamped with its
  PRE-bump term in Rust (module doc, item 9c). -/
  | sendReport (w : World n) (j : Fin n)
      (hrole : (w.nodes j).pn.role = .follower)
      (hgate : (w.nodes j).reconciled = true) :
      Step w
        { nodes := w.nodes
          sent := w.sent
          dsent := w.dsent
          csent := w.csent ++
            [.report j (w.nodes j).pn.currentTerm (w.nodes j).pn.durable]
          committed := w.committed }
  /-- `Event::Report`, current-term leader arm (`election.rs` 545–573): the
  leader folds a member's report into its tracker via the PROVED kernel
  `onDurable` (per-slot monotone — stale reordered deliveries never
  regress), slot-mapped by `follower_slot`. The `term < current_term` stale
  drop is a no-op (no constructor); the `term > current_term` adoption arm
  is out of scope (module doc, item 9b). Data-plane stutter. -/
  | deliverReport (w : World n) (i src : Fin n) (t d : Nat)
      (hmsg : CMsg.report src t d ∈ w.csent)
      (hrole : (w.nodes i).pn.role = .leader)
      (hterm : t = (w.nodes i).pn.currentTerm)
      (hsrc : src ≠ i) :
      Step w
        { nodes := Function.update w.nodes i
            { w.nodes i with
              tracker := (w.nodes i).tracker.onDurable (slotOf i src) d }
          sent := w.sent
          dsent := w.dsent
          csent := w.csent
          committed := w.committed }
  /-- The commit event (`election.rs::rank_leader` → `tracker.advance(own)`
  → `Action::AdvanceCommit`, stored by `node.rs`'s single commit-store
  site): the REAL kernel `advance`, fed by the folded reports and clamped
  by the leader's own durable (C2's `min`), fires `some k`. The step stores
  the advanced tracker, raises the watermark, and appends every
  newly-crossed `(p, term-stamp, payload)` from the leader's OWN hist to
  the ghost ledger (decision 1). Data-plane stutter. -/
  | leaderAdvanceCommit (w : World n) (i : Fin n) (k : Nat)
      (hrole : (w.nodes i).pn.role = .leader)
      (hadv : ((w.nodes i).tracker.advance (w.nodes i).pn.durable).2 = some k) :
      Step w
        { nodes := Function.update w.nodes i
            { w.nodes i with
              tracker := ((w.nodes i).tracker.advance (w.nodes i).pn.durable).1
              commitIdx := max (w.nodes i).commitIdx k }
          sent := w.sent
          dsent := w.dsent
          csent := w.csent
          committed := w.committed ++
            ghostEntries (w.nodes i).dn.hist (w.nodes i).commitIdx k }

/-- Reachability from boot. -/
def Reachable {n : Nat} (w : World n) : Prop :=
  Relation.ReflTransGen Step (World.init n) w

theorem reachable_init (n : Nat) : Reachable (World.init n) :=
  Relation.ReflTransGen.refl

/-! ## Projection onto the data-plane model

Erase the commit plane: keep the `dn` slice, the election wire, and the
data wire. Every mirror maps to its data-model original; the three commit
steps are stutters. `Reachable` here implies `Data.Reachable` there, so
`election_safety` AND `log_matching` lift — proved FIRST (LA1 pattern). -/

/-- The data-model shadow of a commit world. -/
def World.project {n : Nat} (w : World n) : Uc2.Data.World n :=
  { nodes := fun i => (w.nodes i).dn, sent := w.sent, dsent := w.dsent }

private theorem project_update {n : Nat} (nodes : Fin n → Node n) (i : Fin n)
    (c : Node n) :
    (fun j => (Function.update nodes i c j).dn)
      = Function.update (fun j => (nodes j).dn) i c.dn := by
  funext j
  rcases eq_or_ne j i with rfl | hj
  · simp
  · simp [Function.update_of_ne hj]

/-- Projection of a one-node update: exactly the data model's
`Function.update` on the `dn` slice. -/
private theorem project_mk {n : Nat} (w : World n) (i : Fin n) (c : Node n)
    (s : List (Uc2.Msg n)) (ds : List Uc2.Data.Frame) (cs : List (CMsg n))
    (gm : List (Nat × Nat × Nat)) :
    World.project
        { nodes := Function.update w.nodes i c, sent := s, dsent := ds,
          csent := cs, committed := gm }
      = { nodes := Function.update w.project.nodes i c.dn, sent := s,
          dsent := ds } := by
  simp only [World.project, project_update]

/-- A node update that leaves the `dn` slice untouched projects to the
identity — the commit steps' stutter witness. -/
private theorem project_mk_self {n : Nat} (w : World n) (i : Fin n)
    (c : Node n) (hc : c.dn = (w.nodes i).dn) (cs : List (CMsg n))
    (gm : List (Nat × Nat × Nat)) :
    World.project
        { nodes := Function.update w.nodes i c, sent := w.sent,
          dsent := w.dsent, csent := cs, committed := gm }
      = w.project := by
  simp only [World.project, project_update, hc]
  congr 1
  exact Function.update_eq_self i _

/-- **Simulation**: every commit-layer step is zero or one data-model steps
on the projection. Mirrors map to their originals; the commit plane
(`sendReport`, `deliverReport`, `leaderAdvanceCommit`) is invisible. -/
theorem step_project {n : Nat} {w w' : World n} (h : Step w w') :
    Relation.ReflTransGen Uc2.Data.Step w.project w'.project := by
  cases h with
  | startElection i hrole =>
    rw [project_mk]
    exact .single (Uc2.Data.Step.startElection w.project i hrole)
  | deliverRequestVote j c newTerm cLastTerm cDurable hmsg hterm =>
    rw [project_mk]
    exact .single
      (Uc2.Data.Step.deliverRequestVote w.project j c newTerm cLastTerm
        cDurable hmsg hterm)
  | rejectStaleRequestVote j c newTerm cLastTerm cDurable hmsg hstale =>
    exact .single
      (Uc2.Data.Step.rejectStaleRequestVote w.project j c newTerm cLastTerm
        cDurable hmsg hstale)
  | deliverVote i v t hmsg hrole hterm =>
    rw [project_mk]
    exact .single (Uc2.Data.Step.deliverVote w.project i v t hmsg hrole hterm)
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    rw [project_mk]
    exact .single
      (Uc2.Data.Step.deliverVoteHigherTerm w.project i v t g hmsg hterm)
  | becomeLeader i hrole hquorum =>
    rw [project_mk]
    exact .single (Uc2.Data.Step.becomeLeader w.project i hrole hquorum)
  | crashRestart i =>
    rw [project_mk]
    exact .single (Uc2.Data.Step.crashRestart w.project i)
  | leaderAppend i v hrole =>
    rw [project_mk]
    exact .single (Uc2.Data.Step.leaderAppend w.project i v hrole)
  | deliverReplicate j pos t v hmsg hpos hstamp hgate =>
    rw [project_mk]
    exact .single
      (Uc2.Data.Step.deliverReplicate w.project j pos t v hmsg hpos hstamp)
  | shipTermMap i hrole =>
    exact .single (Uc2.Data.Step.shipTermMap w.project i hrole)
  | deliverTermMap j t entries hmsg hterm =>
    rw [project_mk]
    exact .single (Uc2.Data.Step.deliverTermMap w.project j t entries hmsg hterm)
  | sendReport j hrole hgate =>
    exact .refl
  | deliverReport i src t d hmsg hrole hterm hsrc =>
    rw [project_mk_self w i
      { w.nodes i with
        tracker := (w.nodes i).tracker.onDurable (slotOf i src) d }
      rfl w.csent w.committed]
  | leaderAdvanceCommit i k hrole hadv =>
    rw [project_mk_self w i
      { w.nodes i with
        tracker := ((w.nodes i).tracker.advance (w.nodes i).pn.durable).1
        commitIdx := max (w.nodes i).commitIdx k }
      rfl w.csent
      (w.committed ++ ghostEntries (w.nodes i).dn.hist (w.nodes i).commitIdx k)]

/-- Boot projects to boot. -/
theorem project_init (n : Nat) :
    (World.init n).project = Uc2.Data.World.init n := rfl

/-- **Projection lift**: a reachable commit world's shadow is reachable in
the data-plane model — LA1/LA2's whole invariant stack applies for free. -/
theorem reachable_project {n : Nat} {w : World n} (h : Reachable w) :
    Uc2.Data.Reachable w.project := by
  have h' : Relation.ReflTransGen Step (World.init n) w := h
  clear h
  induction h' with
  | refl => exact Relation.ReflTransGen.refl
  | tail _ hstep ih => exact Relation.ReflTransGen.trans ih (step_project hstep)

/-- **Election safety, lifted** (Global Constraints: green at LB1 commit
time): at most one leader per term in the commit-layer model. -/
theorem election_safety {n : Nat} (w : World n) (hw : Reachable w)
    (i j : Fin n)
    (hi : (w.nodes i).pn.role = .leader) (hj : (w.nodes j).pn.role = .leader)
    (ht : (w.nodes i).pn.currentTerm = (w.nodes j).pn.currentTerm) : i = j :=
  Uc2.Data.election_safety w.project (reachable_project hw) i j hi hj ht

/-- **Log matching, lifted** (Global Constraints: green at LB1 commit
time): one payload per `(position, term-stamp)` in the commit-layer model. -/
theorem log_matching {n : Nat} (w : World n) (hw : Reachable w)
    (i j : Fin n) (p : Nat) (t vi vj : Nat)
    (hi : (w.nodes i).hist p = some (t, vi))
    (hj : (w.nodes j).hist p = some (t, vj)) : vi = vj :=
  Uc2.Data.log_matching w.project (reachable_project hw) i j p t vi vj hi hj

/-! ## Non-vacuity

The LC-core hypotheses must be genuinely reachable, INCLUDING the term
change: a real commit event (quorum of reports through the kernel tracker)
followed by a later-term election whose winner holds the committed entry.
If this trace fails to typecheck, the MODEL is broken. -/

/-- Trace-discharge helper for `leaderAdvanceCommit`'s enabling: the kernel
cannot `decide`-reduce `advance` (`List.mergeSort` is well-founded
recursion), so the trace worlds' tracker/durable are pinned by kernel
`decide` and the concrete advance is discharged by `simp` once, here. -/
private theorem advance_fires {t : CommitTracker} {own : Nat}
    (t' : CommitTracker) (own' k : Nat)
    (ht : t = t') (hown : own = own')
    (hk : (t'.advance own').2 = some k) :
    (t.advance own).2 = some k := by
  subst ht
  subst hown
  exact hk

/-- **Non-vacuity.** In a 3-node cluster: node 0 is elected at term 1,
appends payload `42` at position 0, gossips its map so node 1 reconciles
(gate reopens), replicates the record to node 1, node 1 REPORTS its durable
(AppendPosition), the leader folds the report and the kernel `advance`
fires — committing `(0, 1, 42)` into the ghost ledger — and then node 1
(which durably holds the entry) wins the term-2 election. The final world
satisfies every LC-core hypothesis (`(p,t,v) ∈ committed`, a leader, a
strictly later term) AND its conclusion (`hist p = some (t, v)`).
Exercises all three commit-plane constructors. -/
theorem nonvacuity_commit_completeness_trace :
    ∃ w : World 3, Reachable w ∧
      (0, 1, 42) ∈ w.committed ∧
      (w.nodes 1).pn.role = .leader ∧
      1 < (w.nodes 1).pn.currentTerm ∧
      (w.nodes 1).hist 0 = some (1, 42) := by
  refine ⟨_,
    .tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail (.tail (.tail
      (.single (.startElection _ 0 (by decide)))
      (.deliverRequestVote _ 1 0 1 0 0 (by decide) (by decide)))
      (.deliverVote _ 0 1 1 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)))
      (.leaderAppend _ 0 42 (by decide)))
      (.shipTermMap _ 0 (by decide)))
      (.deliverTermMap _ 1 1 [(1, 0)] (by decide) (by decide)))
      (.deliverReplicate _ 1 0 1 42 (by decide) (by decide) (by decide)
        (by decide)))
      (.sendReport _ 1 (by decide) (by decide)))
      (.deliverReport _ 0 1 1 1 (by decide) (by decide) (by decide)
        (by decide)))
      (.leaderAdvanceCommit _ 0 1 (by decide)
        (advance_fires ⟨[1, 0], 2, 0⟩ 1 1 (by decide) (by decide)
          (by simp [CommitTracker.advance, CommitTracker.ranking,
                List.mergeSort]))))
      (.startElection _ 1 (by decide)))
      (.deliverRequestVote _ 2 1 2 1 1 (by decide) (by decide)))
      (.deliverVote _ 1 2 2 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 1 (by decide) (by decide)),
    by decide, by decide, by decide, by decide⟩

/-! ## FINDING — boot reopens the intake gate (candidate real gap)

Machine-checked witness for module-doc item 5: with Rust-as-of-today boot
semantics (`node.rs` 516/801 gate open + `election.rs` 400–402 recovering
`current_term = vote_term.max(map_term)`), the FIXED LC-core statement is
FALSE. Every step below maps to a real Rust behavior — in particular the
crashed node's higher term survives the reboot because it PERSISTED A VOTE
at that term (persist-before-send), not merely adopted it.

The trace: node 1 leads term 1 and appends two entries (durable 2, only
position 0 replicated to node 0). Node 0 wins term 2 (node 2's grant),
opening its map at base 1 with zero appends — a same-base phantom — then
crashes, restarts, and wins term 3 with NODE 1's OWN GRANT (`logOk`:
candidate `lastTerm` 2 > 1 — node 1 persists its vote at term 3 and its
gate closes). Node 0 appends `(1, term 3, 9)`. Node 1 crashes and reboots:
its persisted term 3 survives, but the gate reopens — it reports
`(term 3, durable 2)` over its stale term-1 tail. The kernel tracker
certifies position 1 with quorum {leader, node 1} and the ghost commits
`(1, 3, 9)` — held durably ONLY by the leader. Node 1 then wins term 4
(node 2's grant), leaving a leader at term 4 > 3 whose hist at the
committed position holds `(1, 8)`, not `(3, 9)`. -/
theorem finding_boot_gate_stale_report_lc_violation :
    ∃ w : World 3, Reachable w ∧
      (1, 3, 9) ∈ w.committed ∧
      (w.nodes 1).pn.role = .leader ∧
      3 < (w.nodes 1).pn.currentTerm ∧
      (w.nodes 1).hist 1 = some (1, 8) ∧
      (w.nodes 1).hist 1 ≠ some (3, 9) := by
  refine ⟨_,
    .tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail (.tail (.tail (.tail (.tail
      -- term 1: node 1 leads, appends (0,(1,7)) and (1,(1,8));
      -- node 0 reconciles + replicates position 0 only.
      (.single (.startElection _ 1 (by decide)))
      (.deliverRequestVote _ 0 1 1 0 0 (by decide) (by decide)))
      (.deliverVote _ 1 0 1 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 1 (by decide) (by decide)))
      (.leaderAppend _ 1 7 (by decide)))
      (.leaderAppend _ 1 8 (by decide)))
      (.shipTermMap _ 1 (by decide)))
      (.deliverTermMap _ 0 1 [(1, 0)] (by decide) (by decide)))
      (.deliverReplicate _ 0 0 1 7 (by decide) (by decide) (by decide)
        (by decide)))
      -- term 2: node 0 wins with node 2's grant, base 1, zero appends
      -- (a same-base phantom (2,1) it will prune at term 3).
      (.startElection _ 0 (by decide)))
      (.deliverRequestVote _ 2 0 2 1 1 (by decide) (by decide)))
      (.deliverVote _ 0 2 2 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)))
      -- term 3: node 0 crash-restarts (leaders don't campaign) and wins
      -- with NODE 1's grant (logOk: lastTerm 2 > 1) — node 1 persists its
      -- vote at term 3 and its intake gate closes.
      (.crashRestart _ 0))
      (.startElection _ 0 (by decide)))
      (.deliverRequestVote _ 1 0 3 2 1 (by decide) (by decide)))
      (.deliverVote _ 0 1 3 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)))
      (.leaderAppend _ 0 9 (by decide)))
      -- THE FINDING: node 1 reboots — persisted term 3 survives, the gate
      -- reopens — and reports (term 3, durable 2) over its term-1 tail.
      (.crashRestart _ 1))
      (.sendReport _ 1 (by decide) (by decide)))
      (.deliverReport _ 0 1 3 2 (by decide) (by decide) (by decide)
        (by decide)))
      (.leaderAdvanceCommit _ 0 2 (by decide)
        (advance_fires ⟨[2, 0], 2, 0⟩ 2 2 (by decide) (by decide)
          (by simp [CommitTracker.advance, CommitTracker.ranking,
                List.mergeSort]))))
      -- term 4: node 1 wins with node 2's grant — a later-term leader
      -- missing the committed entry.
      (.startElection _ 1 (by decide)))
      (.deliverRequestVote _ 2 1 4 1 2 (by decide) (by decide)))
      (.deliverVote _ 1 2 4 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 1 (by decide) (by decide)),
    by decide, by decide, by decide, by decide, by decide⟩

#print axioms nonvacuity_commit_completeness_trace
#print axioms finding_boot_gate_stale_report_lc_violation
#print axioms election_safety
#print axioms log_matching

end Uc2.Cert

/-! ## Executable pins (Rust unit behavior re-pinned, kernel-checked) -/

open Uc2 Uc2.Cert in
section
-- follower_slot (election.rs 1250–1262): members in id order, skipping self.
#guard slotOf (i := (0 : Fin 3)) (j := 1) == 0
#guard slotOf (i := (0 : Fin 3)) (j := 2) == 1
#guard slotOf (i := (1 : Fin 3)) (j := 0) == 0
#guard slotOf (i := (1 : Fin 3)) (j := 2) == 1
#guard slotOf (i := (2 : Fin 3)) (j := 0) == 0
#guard slotOf (i := (2 : Fin 3)) (j := 1) == 1
-- ghost append: every held position in [lo, hi) as (pos, stamp, payload).
#guard ghostEntries
    (fun p => if p == 0 then some (1, 7) else if p == 1 then some (3, 9) else none)
    0 2 == [(0, 1, 7), (1, 3, 9)]
#guard ghostEntries (fun _ => some (1, 7)) 2 2 == []
end
