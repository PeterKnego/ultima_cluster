import Uc2Model.Reconcile
import Uc2Proofs.Protocol
import Uc2Proofs.ElectionSafety

/-! LA1 — the data plane over the election model.

Layered extension of the spike's election protocol (`Uc2Proofs/Protocol.lean`):
each node gains the REAL data plane the spike havoc'd — a stamped payload
history, the data-stamped term map, and an append/durable frontier — plus the
data transitions: leader append, contiguous replication, term-map growth
(`become_leader` pruned push + the `DataTermObserved` analog), leader gossip,
and reconcile-on-gossip truncation via the PROVED `Uc2Model.reconcile` kernel.

Modeling decisions (settled at plan time, briefs 1–6, plus the designer calls
documented inline):

1. **Payloads are real** (decision 1): `hist : Nat → Option (Nat × Nat)` maps a
   byte position to `(term-stamp, payload)`; `durable` bounds the defined
   prefix as an *invariant* (LA2), not a type constraint.
2. **Layering, not rewriting** (designer's call): `Node` wraps the untouched
   `PNode`; the election constructors are restated verbatim over the `pn`
   projection, and every data constructor projects to the election model's
   `havocData` (data steps never touch a vote-relevant field — they only move
   `lastTerm`/`durable`, which havoc absorbs) or to `adoptHigherTerm` (the one
   constructor LA1 added to `Protocol.lean`). `election_safety` therefore
   lifts by simulation (`reachable_project`) instead of being re-proved.
3. **Derived credentials** (designer's call): the spike's havoc payload
   `pn.lastTerm`/`pn.durable` become REAL — every data constructor writes them
   in lockstep (`lastTerm = lastTermOf termMap`, `durable` = the frontier), so
   the election steps' `logOk` inputs read genuine data-plane values. The
   coherence equations are maintained by construction; LA2 states them as an
   invariant where its proofs need them.
4. **Fsync lag collapsed** (decision 3, designer's call): a leader append is
   immediately durable (`durable` IS the append frontier) and `crashRestart`
   preserves the whole data plane (journal + `StableValue` recovery). The real
   lost-unfsynced-suffix behavior is not modeled: losing a suffix can only be
   followed by re-appending under a STRICTLY higher term (crash demotes;
   `currentTerm` is monotone; a new tenure re-stamps), so it cannot manufacture
   two payloads under one `(position, term)` — the thing LM is about. Recorded
   as an honest scope note for the re-gate.
5. **Atomic truncation** (designer's call): `node.rs`'s
   persist-map → `Action::Truncate` → archive truncate → epoch'd `Truncated`
   ack → pending-map adoption pipeline (with the data-plane latch holding the
   window closed) is collapsed into one atomic `deliverTermMap` step. The latch
   admits no data event into the window in Rust, and a crash inside it
   self-heals to the same post-state (persist-before-truncate +
   `rederive_term_map`), so the model's atomic effect is the committed outcome.
6. **Header gate on replication** (load-bearing; LC1 amendment — the
   original LA1 form was a single-term `stamp ≤ currentTerm` collapse of two
   Rust guards, which Finding #7 proved unsound for leader completeness): a
   receiver accepts a record only when the frame's wire HEADER term exactly
   equals its adopted term (`receiver.rs:636-639` `dropped_stale_term`); in
   UC adoption comes only from consensus datagrams, and the
   `awaiting_reconcile` intake gate forces reconcile-before-data on new-term
   adoption — the `become_leader` phantom-prune comment (`election.rs`
   ~1039) leans on exactly that gate. The old stamp bound is now an
   emission-side INVARIANT (`LogMatching.lean`'s `StampInv`:
   `stamp ≤ hdr`, and held stamps ≤ the holder's term), which still gives
   LM what it needs: raising `currentTerm` (gossip, votes) must precede
   accepting new-term data — the Rust ordering. Old-stamped bytes travel
   only inside the CURRENT leader's stream via `serveTail` (catch-up).
7. **Full-map gossip** (simplification): `shipTermMap` ships the whole term
   map, not `term_map_wire_tail()`'s bounded window. The window is what makes
   `NoCommonPrefix` reachable in production (purged prefixes); reconcile's
   wipe arm is still modeled and exercised by the kernel's own theorems.
8. **Sent-set data wire**: data frames live in `dsent`, append-only, delivered
   any number of times in any order — same fault model as the election wire.
   The receiver-side contiguity guard (`pos = durable`, decision 4) turns
   reorder/duplication into no-ops, mirroring `uc2_log`'s writer rule
   ("the RECEIVER advances only to the contiguous frontier"; accept at exactly
   the frontier). Keeping `dsent` separate from `sent` is what makes the
   projection to the election model a strict field-erasure. -/

namespace Uc2.Data

/-- Rust `term_map.last().map(|(t, _)| *t).unwrap_or(0)` — the map's last
(frontier) term, 0 on the empty map (`election.rs` line 674). -/
def lastTermOf (m : TermMap) : Nat := (m.getLast?.map Prod.fst).getD 0

/-- `election.rs::become_leader`'s term-map open, POST-Finding-#3 form (lines
1013–1048): pop trailing same-base phantoms (`base == durable` — zero-byte
entries from a prior life that opened a term here and crashed before a byte
fsynced), then push `(term, durable)`. -/
def prunePush (m : TermMap) (t d : Nat) : TermMap :=
  (m.reverse.dropWhile (fun e => e.2 == d)).reverse ++ [(t, d)]

/-- `election.rs`'s `Event::DataTermObserved` arm (lines 666–679): the map
grows iff the delivered stamp strictly exceeds the map's last term (base = the
first byte position observed under the new term); idempotent otherwise. -/
def observeTerm (m : TermMap) (t pos : Nat) : TermMap :=
  if lastTermOf m < t then m ++ [(t, pos)] else m

/-- One node: the election-model node (`pn`, untouched `PNode`) + the real
data plane the spike havoc'd — the data-stamped term map
(`ElectionSm::term_map`) and the stamped payload history (position ↦
`(term-stamp, payload)`; the log buffer + journal content, decision 1).

Derived-credential discipline (module doc, item 3): every constructor keeps
`pn.lastTerm = lastTermOf termMap` and `pn.durable` = the append/accept
frontier bounding `hist`'s defined prefix.

`dataTerm` (LC1b, Finding-#8 fix) is the node-level **data-plane term
handle** (`node.rs` `term_handle`): the term the receiver's DATA-path
exact-match compares against (`receiver.rs:635`). Rust stores it ONLY at
`Action::BecomeLeader` (`node.rs:2462`) and `Action::BecomeFollower`
(`node.rs:2490` — fired by every strict term adoption), and boots it to the
recovered current term (`node.rs:513`); `Action::StartElection`
(`node.rs:2440-2450`) does NOT touch it, so a candidate's data plane keeps
running at its PRE-bump term — the lag `currentTerm` alone cannot express,
which Finding #8 exploited. Invariantly `dataTerm ≤ currentTerm`
(`LogMatching.lean`'s `StampInv.data_le`). -/
structure Node (n : Nat) where
  pn       : PNode n
  termMap  : TermMap
  hist     : Nat → Option (Nat × Nat)
  dataTerm : Nat

/-- The data wire frames (decision 8; the election wire keeps `Uc2.Msg`).

- `replicate` mirrors one stamped record of the leader's replication stream
  (`uc2_net` sender off the log buffer; NAK repair re-serves the same bytes,
  which the sent-set semantics covers as re-delivery). **LC1 amendment
  (Finding #7 fix)**: the frame carries BOTH terms Rust keeps separate —
  `hdr` is the datagram's wire header (`leadership_term_id`, checked for
  EXACT equality against the receiver's adopted term,
  `receiver.rs:636-639`), `stamp` is the record's own term stamp (the term
  the byte was originally written under, which can be OLDER than the header
  on a legitimate catch-up/NAK-repair re-serve — see `Step.serveTail`).
  Emission is truthful: `leaderAppend` emits `hdr = stamp = currentTerm`;
  `serveTail` re-ships an old-stamped byte under the CURRENT header.
- `gossip` mirrors `Action::ShipTermMap` (`DGRAM_KIND_TERM_MAP`): the shipping
  leader's `current_term` plus its map (full map — module doc, item 7).

No addressing: both are broadcast, and their semantics are content-keyed. -/
inductive Frame where
  | replicate (pos hdr stamp payload : Nat)
  | gossip (term : Nat) (entries : TermMap)
deriving DecidableEq

/-- The cluster: per-node state, the election wire (projected onto the spike
model verbatim), and the data wire (erased by the projection). -/
structure World (n : Nat) where
  nodes : Fin n → Node n
  sent  : List (Uc2.Msg n)
  dsent : List Frame

/-- Boot: the election model's boot on `pn`, empty map, empty history. -/
def World.init (n : Nat) : World n :=
  { nodes := fun _ =>
      { pn := { currentTerm := 0, votedFor := none, role := .follower,
                votesReceived := ∅, lastTerm := 0, durable := 0,
                smDurable := 0 },
        termMap := [], hist := fun _ => none, dataTerm := 0 },
    sent := [], dsent := [] }

/-- Receiver-side accept of one stamped record at exactly the frontier, with
the folded `DataTermObserved` (`election.rs` 666–679): stamp the byte, advance
the frontier, grow the map iff the stamp opens a new term. `pn.lastTerm` /
`pn.durable` re-derived in lockstep (module doc, item 3). -/
def Node.recvReplicate {n : Nat} (d : Node n) (pos t v : Nat) : Node n :=
  { pn := { d.pn with
      lastTerm := lastTermOf (observeTerm d.termMap t pos)
      durable := pos + 1 }
      -- Issue #7: `smDurable` is deliberately NOT touched. A counter advance is
      -- the archive fsyncing; the consensus agent learns of it only on its own
      -- duty cycle (`absorbDurable`). That lag IS the modelled phenomenon.
    termMap := observeTerm d.termMap t pos
    hist := Function.update d.hist pos (some (t, v))
    dataTerm := d.dataTerm }

/-- `Event::TermMapReceived`, non-stale arm (`election.rs` 656–664 +
`reconcile_term_map` 1079–1133, collapsed atomically with the node's
truncate/ack pipeline — module doc, item 5): adopt a strictly-higher term
first (adoption on the `pn` slice), then run the PROVED `Uc2Model.reconcile`
kernel against the shipped entries:

- `.ok o`: truncate `hist` and the frontier to `o.validUpTo`, adopt
  `o.newMap`. When `validUpTo = durable` this is Rust's no-truncate arm
  (possibly a pure phantom-dropping map shrink); one uniform formula covers
  both (R1: `validUpTo ≤ durable`).
- `.noCommonPrefix`: wipe-and-rejoin (`wipe_on_no_common_prefix`, the M6+
  production config): empty history, empty map, frontier 0. The `Fatal`
  fail-stop alternative is a non-transition (not modeled). -/
def Node.applyGossip {n : Nat} (d : Node n) (t : Nat) (entries : TermMap) :
    Node n :=
  match reconcile d.termMap d.pn.durable entries with
  | .ok o =>
    { pn := { (if d.pn.currentTerm < t then d.pn.adoptTerm t else d.pn) with
                lastTerm := lastTermOf o.newMap, durable := o.validUpTo,
                smDurable := min d.pn.smDurable o.validUpTo }
      termMap := o.newMap
      hist := fun p => if p < o.validUpTo then d.hist p else none
      dataTerm := if d.pn.currentTerm < t then t else d.dataTerm }
  | .noCommonPrefix =>
    { pn := { (if d.pn.currentTerm < t then d.pn.adoptTerm t else d.pn) with
                lastTerm := 0, durable := 0, smDurable := 0 }
      termMap := []
      hist := fun _ => none
      dataTerm := if d.pn.currentTerm < t then t else d.dataTerm }

/-- One cluster transition. Constructors 1–7 restate the election model's
steps verbatim over the `pn` projection (`havocData` is dropped — the real
data plane below replaces it); 8–11 are the data plane. Docstrings on the
election mirrors defer to `Uc2Proofs/Protocol.lean`'s originals. -/
inductive Step {n : Nat} : World n → World n → Prop
  /-- `Uc2.Step.startElection` on `pn` (`election.rs::start_election`
  974–1001). The broadcast credentials `(lastTerm, durable)` are now the REAL
  derived values, no longer havoc payload.

  Issue #7: the advertised credential is `smDurable`, the consensus agent's
  ABSORBED copy — `start_election` reads `ElectionSm::durable`, not the counter.
  Under-advertising is conservative (it loses elections, it cannot win one it
  should not), which is exactly why the Rust fix left this side alone and
  re-read the counter on the GRANT side instead. -/
  | startElection (w : World n) (i : Fin n)
      (hrole : (w.nodes i).pn.role ≠ .leader) :
      Step w
        { nodes := Function.update w.nodes i
            { w.nodes i with pn :=
              { (w.nodes i).pn with
                currentTerm := (w.nodes i).pn.currentTerm + 1
                votedFor := some ((w.nodes i).pn.currentTerm + 1, i)
                role := .candidate
                votesReceived := {i} } }
          sent := w.sent ++
            [.requestVote i ((w.nodes i).pn.currentTerm + 1)
              (w.nodes i).pn.lastTerm (w.nodes i).pn.smDurable]
          dsent := w.dsent }
  /-- `Uc2.Step.deliverRequestVote` on `pn` (`election.rs` 600–627,
  `handle_request_vote` 1202–1227). The voter's `logOk` inputs are real.
  LC1b: a strict adoption fires `Action::BecomeFollower`, which stores the
  data-plane term handle (`node.rs:2490`); same-term delivery leaves it. -/
  | deliverRequestVote (w : World n) (j c : Fin n)
      (newTerm cLastTerm cDurable : Nat)
      (hmsg : Uc2.Msg.requestVote c newTerm cLastTerm cDurable ∈ w.sent)
      (hterm : (w.nodes j).pn.currentTerm ≤ newTerm) :
      Step w
        { nodes := Function.update w.nodes j
            { w.nodes j with
              pn :=
                ((w.nodes j).pn.recvRequestVote c newTerm cLastTerm cDurable).1
              dataTerm :=
                if (w.nodes j).pn.currentTerm < newTerm then newTerm
                else (w.nodes j).dataTerm }
          sent := w.sent ++
            [.vote j c newTerm
              ((w.nodes j).pn.recvRequestVote c newTerm cLastTerm cDurable).2]
          dsent := w.dsent }
  /-- `Uc2.Step.rejectStaleRequestVote` (`election.rs` 618–622). -/
  | rejectStaleRequestVote (w : World n) (j c : Fin n)
      (newTerm cLastTerm cDurable : Nat)
      (hmsg : Uc2.Msg.requestVote c newTerm cLastTerm cDurable ∈ w.sent)
      (hstale : newTerm < (w.nodes j).pn.currentTerm) :
      Step w
        { nodes := w.nodes
          sent := w.sent ++ [.vote j c (w.nodes j).pn.currentTerm false]
          dsent := w.dsent }
  /-- `Uc2.Step.deliverVote` on `pn` (`election.rs` 646–653). -/
  | deliverVote (w : World n) (i v : Fin n) (t : Nat)
      (hmsg : Uc2.Msg.vote v i t true ∈ w.sent)
      (hrole : (w.nodes i).pn.role = .candidate)
      (hterm : (w.nodes i).pn.currentTerm = t) :
      Step w
        { nodes := Function.update w.nodes i
            { w.nodes i with pn :=
              { (w.nodes i).pn with
                votesReceived := insert v (w.nodes i).pn.votesReceived } }
          sent := w.sent
          dsent := w.dsent }
  /-- `Uc2.Step.deliverVoteHigherTerm` on `pn` (`election.rs` 633–636).
  LC1b: strict adoption → `BecomeFollower` stores the handle
  (`node.rs:2490`). -/
  | deliverVoteHigherTerm (w : World n) (i v : Fin n) (t : Nat) (g : Bool)
      (hmsg : Uc2.Msg.vote v i t g ∈ w.sent)
      (hterm : (w.nodes i).pn.currentTerm < t) :
      Step w
        { nodes := Function.update w.nodes i
            { w.nodes i with pn := (w.nodes i).pn.adoptTerm t, dataTerm := t }
          sent := w.sent
          dsent := w.dsent }
  /-- `election.rs::become_leader` (1003–1057), now with its data half
  (decision 6): the role flip PLUS the term-map open — the same-base phantom
  prune + push `(currentTerm, durable)` (`prunePush`, the post-Finding-#3
  form, lines 1015–1048). `lastTerm` re-derives to `currentTerm` (the pushed
  entry is the new last). The `Action::ShipTermMap` become_leader emits is the
  separate free-firing `shipTermMap` step (a superset of Rust's emit-here). -/
  | becomeLeader (w : World n) (i : Fin n)
      (hrole : (w.nodes i).pn.role = .candidate)
      (hquorum : n / 2 + 1 ≤ (w.nodes i).pn.votesReceived.card) :
      Step w
        { nodes := Function.update w.nodes i
            { pn := { (w.nodes i).pn with
                role := .leader
                lastTerm := (w.nodes i).pn.currentTerm }
              -- counter — `Action::BecomeLeader`'s `base` is `ElectionSm::durable`.
              termMap := prunePush (w.nodes i).termMap
                (w.nodes i).pn.currentTerm (w.nodes i).pn.durable
              hist := (w.nodes i).hist
              -- LC1b: `Action::BecomeLeader` stores the handle (node.rs:2462)
              dataTerm := (w.nodes i).pn.currentTerm }
          sent := w.sent
          dsent := w.dsent }
  /-- `Uc2.Step.crashRestart` (decision 3): the vote record persists
  (`StableValue`), volatile role/tally reset — and the data plane persists
  too (journal recovery; fsync lag collapsed, module doc item 4). LC1b: the
  handle boots to the recovered current term (`node.rs:513`) — the model's
  `currentTerm` IS that recovered `max(vote, map)` value. -/
  | crashRestart (w : World n) (i : Fin n) :
      Step w
        { nodes := Function.update w.nodes i
            { w.nodes i with
              pn :=
                { (w.nodes i).pn with role := .follower, votesReceived := ∅ }
              dataTerm := (w.nodes i).pn.currentTerm }
          sent := w.sent
          dsent := w.dsent }
  /-- Issue #7: the consensus agent's duty cycle absorbing the durable counter
  into `ElectionSm` (`Consensus::do_work` step 2 / `refresh_durable` in
  `uc2_node`). The ONLY way `smDurable` catches up with `durable`, and the
  reason it can lag at all: in the real node the counter is advanced by the
  archive agent, READ AND REPORTED by the receiver agent, and absorbed here by
  a third thread on its own schedule. Collapsing those into one value is what
  made the acked-write loss fixed in `main` 26d4827 inexpressible.
  Restricted to a NON-LEADER, which costs nothing: `smDurable` is only ever READ
  by `startElection` (the advertised credential), and that step already requires
  `role ≠ .leader`. A leader that later stands must step down first, and can
  absorb then — so no reachable credential is lost. The guard is what lets this
  step reuse `provinv_election`, whose leader arm demands the data-node be
  unchanged. -/
  | absorbDurable (w : World n) (i : Fin n)
      (hrole : (w.nodes i).pn.role ≠ .leader) :
      Step w
        { nodes := Function.update w.nodes i
            { w.nodes i with
              pn := { (w.nodes i).pn with
                        smDurable := (w.nodes i).pn.durable } }
          sent := w.sent
          dsent := w.dsent }
  /-- Leader append (decision 3): a `.leader` stamps `(currentTerm, v)` at its
  own append frontier for arbitrary `v`, advancing the frontier — the client
  submit → log-buffer append of `node.rs`'s leader path, with the emitted
  `replicate` frame standing for the sender agent's stream off the buffer.
  The append is immediately durable (module doc, item 4).

  Emission note (LC2 amendment 6): the model emits the frame header as
  `currentTerm`, while Rust stamps it from the node-level `term_handle`
  (`dataTerm`). For a LEADER these COINCIDE — `dataTerm = currentTerm` holds
  invariantly (`MapWF.lean`'s `reachable_leader_dataTerm`), since a leader's
  handle was stored at `becomeLeader` (`node.rs:2462`) and no candidate-style
  lag applies while it leads — so the model's `currentTerm` header is faithful
  at every replicate emission site (`leaderAppend` here, and `serveTail`). -/
  | leaderAppend (w : World n) (i : Fin n) (v : Nat)
      (hrole : (w.nodes i).pn.role = .leader) :
      Step w
        { nodes := Function.update w.nodes i
            { pn := { (w.nodes i).pn with
                durable := (w.nodes i).pn.durable + 1 }
              termMap := (w.nodes i).termMap
              hist := Function.update (w.nodes i).hist (w.nodes i).pn.durable
                (some ((w.nodes i).pn.currentTerm, v))
              dataTerm := (w.nodes i).dataTerm }
          sent := w.sent
          dsent := w.dsent ++
            [.replicate (w.nodes i).pn.durable (w.nodes i).pn.currentTerm
              (w.nodes i).pn.currentTerm v] }
  /-- Replication delivery (decision 4): accept a stamped record only at
  exactly the receiver's frontier (`uc2_log` writer rule — reordered and
  duplicated deliveries become no-ops, never corruption) and only when the
  wire HEADER term EXACTLY matches the receiver's data-plane term handle
  `dataTerm` (LC1 amendment, Finding #7 fix; LC1b, Finding #8 fix — keyed
  to the HANDLE, not `currentTerm`, because a candidate's handle lags its
  bumped term: `receiver.rs:636-639` `dropped_stale_term` compares
  `self.term`, the handle — adoption itself comes only from consensus
  datagrams, never from DATA).
  The old `stamp ≤ currentTerm` guard is gone: its job moved to the
  emission sites (`leaderAppend` stamps `hdr = stamp = currentTerm`;
  `serveTail` re-serves an existing hist stamp under the current header),
  where `LogMatching.lean`'s `StampInv` proves `stamp ≤ hdr` invariantly.
  Folds the `DataTermObserved` map growth (`Node.recvReplicate`) — still
  keyed on the record STAMP, unchanged semantics. -/
  | deliverReplicate (w : World n) (j : Fin n) (pos hdr t v : Nat)
      (hmsg : Frame.replicate pos hdr t v ∈ w.dsent)
      (hpos : pos = (w.nodes j).pn.durable)
      (hhdr : hdr = (w.nodes j).dataTerm) :
      Step w
        { nodes := Function.update w.nodes j
            ((w.nodes j).recvReplicate pos t v)
          sent := w.sent
          dsent := w.dsent }
  /-- Tail re-serve (LC1 amendment, decision 2 — truth at emission): a
  leader re-ships any byte it durably holds under its CURRENT term as the
  wire header, keeping the record's original stamp. This is the
  NAK-repair / deep-NAK / journal-replay path by which OLD-stamped bytes
  legitimately reach a reconciled follower inside the CURRENT leader's
  stream (`uc2_net` sender serving retransmits off the log buffer /
  journal under its own `leadership_term_id`) — what keeps
  inherited-prefix catch-up (the #6a/Fig-8 case) alive now that delivery
  requires an exact header match. -/
  | serveTail (w : World n) (i : Fin n) (p t v : Nat)
      (hrole : (w.nodes i).pn.role = .leader)
      (hhist : (w.nodes i).hist p = some (t, v))
      (hp : p < (w.nodes i).pn.durable) :
      Step w
        { nodes := w.nodes
          sent := w.sent
          dsent := w.dsent ++
            [.replicate p (w.nodes i).pn.currentTerm t v] }
  /-- `Action::ShipTermMap` (`election.rs` 1056 + the leader's idle re-gossip
  tick): a leader ships its current term + map on the data wire, any time. -/
  | shipTermMap (w : World n) (i : Fin n)
      (hrole : (w.nodes i).pn.role = .leader) :
      Step w
        { nodes := w.nodes
          sent := w.sent
          dsent := w.dsent ++
            [.gossip (w.nodes i).pn.currentTerm (w.nodes i).termMap] }
  /-- Reconcile-on-gossip (decision 5): `Event::TermMapReceived`, non-stale
  arm (`election.rs` 656–664; the `term < current_term` stale drop is a
  no-op, hence no constructor) — adopt-if-higher, then the `Uc2Model.reconcile`
  kernel + atomic truncation (`Node.applyGossip`; module doc, item 5). This is
  where divergent tails die, exactly as in UC. -/
  | deliverTermMap (w : World n) (j : Fin n) (t : Nat) (entries : TermMap)
      (hmsg : Frame.gossip t entries ∈ w.dsent)
      (hterm : (w.nodes j).pn.currentTerm ≤ t) :
      Step w
        { nodes := Function.update w.nodes j ((w.nodes j).applyGossip t entries)
          sent := w.sent
          dsent := w.dsent }

/-- Reachability from boot. -/
def Reachable {n : Nat} (w : World n) : Prop :=
  Relation.ReflTransGen Step (World.init n) w

theorem reachable_init (n : Nat) : Reachable (World.init n) :=
  Relation.ReflTransGen.refl

/-! ## Projection onto the election model

Erase the data plane: keep the `pn` slice and the election wire. Every data
`Step` maps to zero, one, or two election `Step`s (`havocData` absorbs the
derived-credential writes; `adoptHigherTerm` absorbs the gossip adoption), so
`Reachable` here implies `Uc2.Reachable` there and `election_safety` lifts. -/

/-- The election-model shadow of a data world. -/
def World.project {n : Nat} (w : World n) : Uc2.World n :=
  { nodes := fun i => (w.nodes i).pn, sent := w.sent }

private theorem project_update {n : Nat} (nodes : Fin n → Node n) (i : Fin n)
    (d : Node n) :
    (fun j => (Function.update nodes i d j).pn)
      = Function.update (fun j => (nodes j).pn) i d.pn := by
  funext j
  rcases eq_or_ne j i with rfl | hj
  · simp
  · simp [Function.update_of_ne hj]

/-- Projection of a one-node update: exactly the election model's
`Function.update` on the `pn` slice. -/
private theorem project_mk {n : Nat} (w : World n) (i : Fin n) (d : Node n)
    (s : List (Uc2.Msg n)) (ds : List Frame) :
    World.project { nodes := Function.update w.nodes i d, sent := s, dsent := ds }
      = { nodes := Function.update w.project.nodes i d.pn, sent := s } := by
  simp only [World.project, project_update]

/-- The `pn` slice of `applyGossip` is the adopt-or-not election node with
its derived credentials rewritten — the shape `havocData` can produce. -/
private theorem applyGossip_pn {n : Nat} (d : Node n) (t : Nat)
    (entries : TermMap) :
    (d.applyGossip t entries).pn
      = { (if d.pn.currentTerm < t then d.pn.adoptTerm t else d.pn) with
          lastTerm := (d.applyGossip t entries).pn.lastTerm,
          durable := (d.applyGossip t entries).pn.durable,
          smDurable := (d.applyGossip t entries).pn.smDurable } := by
  cases hrec : reconcile d.termMap d.pn.durable entries <;>
    simp [Node.applyGossip, hrec]

/-- **Simulation**: every data-plane step is zero, one, or two election-model
steps on the projection. Election mirrors map to their originals; pure-data
steps map to `havocData` (or nothing — `shipTermMap` is invisible);
`becomeLeader` and `deliverTermMap` compose two. -/
theorem step_project {n : Nat} {w w' : World n} (h : Step w w') :
    Relation.ReflTransGen Uc2.Step w.project w'.project := by
  cases h with
  | startElection i hrole =>
    rw [project_mk]
    exact .single (Uc2.Step.startElection w.project i hrole)
  | deliverRequestVote j c newTerm cLastTerm cDurable hmsg hterm =>
    rw [project_mk]
    exact .single
      (Uc2.Step.deliverRequestVote w.project j c newTerm cLastTerm cDurable
        hmsg hterm)
  | rejectStaleRequestVote j c newTerm cLastTerm cDurable hmsg hstale =>
    exact .single
      (Uc2.Step.rejectStaleRequestVote w.project j c newTerm cLastTerm cDurable
        hmsg hstale)
  | deliverVote i v t hmsg hrole hterm =>
    rw [project_mk]
    exact .single (Uc2.Step.deliverVote w.project i v t hmsg hrole hterm)
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    rw [project_mk]
    exact .single (Uc2.Step.deliverVoteHigherTerm w.project i v t g hmsg hterm)
  | becomeLeader i hrole hquorum =>
    rw [project_mk]
    refine .head (Uc2.Step.becomeLeader w.project i hrole hquorum) (.single ?_)
    have h2 := Uc2.Step.havocData
      ({ nodes := Function.update w.project.nodes i
          { w.project.nodes i with role := .leader },
         sent := w.project.sent } : Uc2.World n)
      i (w.nodes i).pn.currentTerm (w.nodes i).pn.durable
      (w.nodes i).pn.smDurable
    simp only [Function.update_idem, Function.update_self] at h2
    exact h2
  | absorbDurable i hrole =>
    rw [project_mk]
    exact .single (Uc2.Step.absorbDurable w.project i)
  | crashRestart i =>
    rw [project_mk]
    exact .single (Uc2.Step.crashRestart w.project i)
  | leaderAppend i v hrole =>
    rw [project_mk]
    exact .single (Uc2.Step.havocData w.project i (w.nodes i).pn.lastTerm
      ((w.nodes i).pn.durable + 1) (w.nodes i).pn.smDurable)
  | deliverReplicate j pos hdr t v hmsg hpos hhdr =>
    rw [project_mk]
    exact .single (Uc2.Step.havocData w.project j
      (lastTermOf (observeTerm (w.nodes j).termMap t pos)) (pos + 1)
      (w.nodes j).pn.smDurable)
  | serveTail i p t v hrole hhist hp =>
    exact .refl
  | shipTermMap i hrole =>
    exact .refl
  | deliverTermMap j t entries hmsg hterm =>
    rw [project_mk, applyGossip_pn]
    by_cases hadopt : (w.nodes j).pn.currentTerm < t
    · rw [if_pos hadopt]
      refine .head (Uc2.Step.adoptHigherTerm w.project j t hadopt) (.single ?_)
      have h2 := Uc2.Step.havocData
        ({ nodes := Function.update w.project.nodes j
            ((w.project.nodes j).adoptTerm t),
           sent := w.project.sent } : Uc2.World n)
        j ((w.nodes j).applyGossip t entries).pn.lastTerm
        ((w.nodes j).applyGossip t entries).pn.durable
        ((w.nodes j).applyGossip t entries).pn.smDurable
      simp only [Function.update_idem, Function.update_self] at h2
      exact h2
    · rw [if_neg hadopt]
      exact .single (Uc2.Step.havocData w.project j
        ((w.nodes j).applyGossip t entries).pn.lastTerm
        ((w.nodes j).applyGossip t entries).pn.durable
        ((w.nodes j).applyGossip t entries).pn.smDurable)

/-- Boot projects to boot. -/
theorem project_init (n : Nat) :
    (World.init n).project = Uc2.World.init n := rfl

/-- **Projection lift**: a reachable data world's shadow is reachable in the
election model — the spike's whole invariant stack applies here for free. -/
theorem reachable_project {n : Nat} {w : World n} (h : Reachable w) :
    Uc2.Reachable w.project := by
  have h' : Relation.ReflTransGen Step (World.init n) w := h
  clear h
  induction h' with
  | refl => exact Relation.ReflTransGen.refl
  | tail _ hstep ih => exact Relation.ReflTransGen.trans ih (step_project hstep)

/-- **Election safety, lifted** (Global Constraints: green at LA1 commit
time): at most one leader per term in the full data-plane model, by
projection onto the S2 theorem. -/
theorem election_safety {n : Nat} (w : World n) (hw : Reachable w)
    (i j : Fin n)
    (hi : (w.nodes i).pn.role = .leader) (hj : (w.nodes j).pn.role = .leader)
    (ht : (w.nodes i).pn.currentTerm = (w.nodes j).pn.currentTerm) : i = j :=
  Uc2.election_safety w.project (reachable_project hw) i j hi hj ht

/-! ## Non-vacuity

The LM hypotheses must be genuinely reachable: a leader appends, a follower
replicates the bytes, and both hold the same stamped payload. If this trace
fails to typecheck, the MODEL is broken (enabling conditions too strong). -/

/-- **Non-vacuity.** In a 3-node cluster: node 0 is elected at term 1 (the S1
trace shape), opens its term map (`prunePush [] 1 0 = [(1, 0)]`), appends
payload `42` at position 0 stamped term 1, node 1 accepts the replicated
record at its frontier (folding the `DataTermObserved` map growth), the
leader ships its term map, and node 1 reconciles it cleanly (`Uc2Model.reconcile`
returns `validUpTo = 1`, truncating nothing) — after all of which BOTH nodes
hold `hist 0 = some (1, 42)`: the same position, the same term stamp, the
same payload. Exercises every data-plane constructor. -/
theorem nonvacuity_log_matching_trace :
    ∃ w : World 3, Reachable w ∧
      (w.nodes 0).hist 0 = some (1, 42) ∧
      (w.nodes 1).hist 0 = some (1, 42) := by
  refine ⟨_,
    .tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.single (.startElection _ 0 (by decide)))
      (.deliverRequestVote _ 1 0 1 0 0 (by decide) (by decide)))
      (.deliverRequestVote _ 2 0 1 0 0 (by decide) (by decide)))
      (.deliverVote _ 0 1 1 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)))
      (.leaderAppend _ 0 42 (by decide)))
      (.deliverReplicate _ 1 0 1 1 42 (by decide) (by decide) (by decide)))
      (.shipTermMap _ 0 (by decide)))
      (.deliverTermMap _ 1 1 [(1, 0)] (by decide) (by decide)),
    by decide, by decide⟩

#print axioms nonvacuity_log_matching_trace
#print axioms election_safety

end Uc2.Data

/-! ## Executable pins (Rust unit tests re-pinned, kernel-checked) -/

open Uc2 Uc2.Data in
section
-- become_leader same-base phantom prune (election.rs unit tests):
-- crash_rewin_prunes_same_base_phantom_at_become_leader
#guard prunePush [(1, 0), (2, 4096)] 3 4096 == [(1, 0), (3, 4096)]
-- crash_rewin_collapses_multi_phantom_chain
#guard prunePush [(1, 0), (2, 4096), (3, 4096)] 4 4096 == [(1, 0), (4, 4096)]
-- become_leader_keeps_predecessor_when_durable_advanced
#guard prunePush [(1, 0), (2, 4096)] 3 8192 == [(1, 0), (2, 4096), (3, 8192)]
-- first term ever opened
#guard prunePush [] 1 0 == [(1, 0)]
-- DataTermObserved: grows on a strictly-new term, idempotent otherwise
#guard observeTerm [(1, 0)] 2 2000 == [(1, 0), (2, 2000)]
#guard observeTerm [(1, 0), (2, 2000)] 2 3000 == [(1, 0), (2, 2000)]
#guard observeTerm [(1, 0), (2, 2000)] 1 3000 == [(1, 0), (2, 2000)]
end
