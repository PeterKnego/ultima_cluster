# Brief: move the term map to the SM's side of the durable split (Lean)

**Status:** scoped, NOT started. Written 2026-07-31 after a measured probe.
**Context:** issue #7, role (d). Prerequisite for moving `becomeLeader`'s collapse
base from the durable counter to the consensus agent's absorbed copy.
**Gate doc:** `docs/benchmarks/uc2-lean-gate-2026-07-16.md`, Finding #12.

## Why this exists

Issue #7 split `PNode.durable` into `durable` (the counter — reported, and
compared by `logOk`) and `smDurable` (the consensus agent's absorbed copy —
advertised as the election credential). Role (d), moving `becomeLeader`'s
collapse base to the copy, was expected to cost only `SmLeDurable` threaded
through `NodeWF.last_base`. It does not.

`prunePush_wf` needs `∀ e, map.getLast? = some e → e.2 ≤ base`. With
`base := smDurable` that is `map.last.base ≤ smDurable`, and **in the model it is
false**: `Node.recvReplicate` grows the term map (via `observeTerm`) and the
counter together in one step, while `smDurable` stays put. Collapsing to the copy
can then push a base below an existing entry and leave the map non-ascending, so
`NodeWF` fails — correctly.

The model is refusing the change, not failing to prove it. The map is currently
modelled as data-plane state; in the real node it is the state machine's.

## What the real node does, and the dependency it rests on

In `Consensus::do_work`:

- step **1b** drains data-stamped term observations → `Event::DataTermObserved`,
  which grows `ElectionSm::term_map` and re-derives `last_term` from it;
- step **2** polls the durable counter → `Event::DurableAdvanced`, raising
  `ElectionSm::durable`;
- `become_leader` fires from the vote drain at step **1a**, i.e. it reads state as
  of the END of the previous cycle.

So any base observed at a cycle's 1b is covered by that same cycle's step 2
(an observed base is recorded, so the counter was at least that), and by the time
`become_leader` runs at the next cycle's 1a, `map.last.base ≤ ElectionSm::durable`.

**Nothing enforces this.** `election.rs`'s prune loop pops only entries with
`base == self.durable` and then pushes `(current_term, self.durable)` with no
monotonicity guard. Reorder those two drains — or freeze the durable poll while
observations keep arriving — and the map goes non-monotone. Issue #6's
`pending_leader_open` suppression does freeze step 2 while 1b keeps running;
`become_leader` cannot fire inside that window, so it is not a live hazard today,
but it is the interaction to re-check if that suppression is ever widened.

Making the model carry this dependency explicitly is the point of the work below.

## Design

Three coordinated changes to `Uc2Proofs/ProtocolData.lean` (mirrored in
`ProtocolCommit.lean`):

1. **`Node.recvReplicate` stops growing the map.** It writes `hist` and advances
   `durable` only — the data plane. `termMap` and `lastTerm` are untouched.

2. **New step `observeDataTerm`** — the consensus agent learning a term:

   ```lean
   | observeDataTerm (w : World n) (i : Fin n) (pos t v : Nat)
       (hhist : (w.nodes i).hist pos = some (t, v))
       (hpos  : pos < (w.nodes i).pn.smDurable) :
       Step w { … termMap := observeTerm (w.nodes i).termMap t pos
                  pn := { … with lastTerm := lastTermOf (observeTerm …) } … }
   ```

   `hpos` is the load-bearing hypothesis: it is the model-level counterpart of
   the 1b/2 ordering, and it makes `∀ e ∈ map, e.2 < smDurable` true **by
   construction** — which is exactly what role (d) needs, without any new
   inductive invariant.

3. **`Node.applyGossip` reconciles against `smDurable`, not `durable`.** Rust's
   `reconcile_term_map` runs on the SM's own durable, so the post-reconcile map
   is clamped by the SM's view. Without this, gossip is a second path that can
   lift `map.last.base` above `smDurable`.

Projections: `observeDataTerm` maps to the base layer's `havocData` (which
already havocs `lastTerm`), so `Uc2Proofs/Protocol.lean` needs no new
constructor and `election_safety` is unaffected.

## Measured cost

A probe (applied, measured, reverted) established:

- **Mechanical:** two new constructors — `Data.Step.observeDataTerm` and its
  `Cert.Step` mirror — each requiring a case in every induction over that
  relation. That is the ~30-case grind of the split, twice, ≈ 60 cases. Known
  quantity; the traps are recorded in the gate doc (the `crashRestart`-twin
  template is wrong wherever a case leans on the role changing, and
  indentation-blind insertion displaces the original case).
- **Substantive, and the real cost:** ~64 sites across eight files couple
  `hist` to `termMap` (`LogMatching`, `MapWF`, `ReportProvenance`,
  `TakeDiscipline`, `LcClosure`, `StageB`, `StageC`, `LeaderCompleteness`).
  Once the map LAGS `hist`, invariants of the form "this node's map attributes
  every byte it holds" stop being **true**, not merely unproven. They must be
  restated with the lag accounted for — typically by conditioning on
  `pos < smDurable` — and every consumer re-proved.
- Every non-vacuity trace gains an `absorbDurable` + `observeDataTerm` pair
  wherever it previously relied on replication to grow the map.

`log_matching` itself is safe: its statement quantifies over `hist` only. The
supporting `DInv`/`MapWF`/`ProvInv`/`TkInv` stack is what needs restating.

## Recommendation

Run it as its own arc, in the shape the veil commit-plane arc used: a driver
session per layer with a gate between. Do NOT attempt it inside a session that is
also doing other work — a half-done version is **less** faithful than the current
model, because it would have the map lagging `hist` while the invariants still
claim lockstep.

Sequencing that keeps the corpus green at every checkpoint:

0. **Extract the map-growth reasoning into a reusable lemma FIRST.** See
   "Correction" below — without this, step 1 is not mechanical.

1. Add `observeDataTerm` and the `Cert` mirror **without** changing
   `recvReplicate` (the map then grows both ways; every existing invariant still
   holds semantically). With step 0 done this is the ~60 mechanical cases and
   lands green; without it, it is not.
2. Restate the `hist`/`termMap` invariants to be lag-tolerant, still with
   `recvReplicate` growing the map (they hold trivially, so this is a pure
   statement change that can be landed and reviewed on its own).
3. Only then remove the map growth from `recvReplicate` and switch
   `applyGossip` to `smDurable`. By this point the invariants already tolerate
   the lag and the change should be close to mechanical.
4. Land role (d): `becomeLeader`'s base becomes `smDurable`, discharged by
   `∀ e ∈ map, e.2 < smDurable` from step 2's `hpos`.

Steps 1 and 2 are individually landable and individually useful; step 3 is the
one that cannot be half-done.

## What is NOT blocked on this

Role (d) is not needed for the grant-plane result issue #7 is about. A candidate
that is genuinely behind has a low counter too, so the acked-write loss is
expressible — and is exhibited, over a reachable trace — without it. See
`Uc2Proofs/DurableSkewWorld.lean`.


## Correction (2026-07-31, after attempting step 1)

**Step 1 as written above is NOT mechanical, and the estimate that it was is the
one thing in this brief that was wrong.** Attempted, measured, reverted.

Adding `observeDataTerm` beside `recvReplicate` leaves every existing invariant
semantically TRUE — the map still grows on replication, so nothing lags. But each
invariant must be **re-proved for the new constructor**, and for the map-sensitive
ones that is not a transfer. `MapWF`'s `NodeWF` case for `deliverReplicate` is
~90 lines across two arms (map grows / observation is idempotent), and it is
written against `recvReplicate_fields` and `hpos : pos = durable`. The
`observeDataTerm` analogue changes neither `hist` nor `durable`, so the
`nonempty` and `last_base` bullets change meaning entirely and must be rewritten,
not adapted.

Every downstream file with a map-sensitive invariant (`MapWF`, `TakeDiscipline`,
parts of `ReportProvenance`, `StageB`, `StageC`) has the same shape. The non-map
invariants really are transfers, and those went through fine — `ProtocolData`,
`ProtocolCommit` and `LogMatching` all reached green, the last needing one real
argument (a reconciled node's map already ends at its own term, so with
`hterm : t ≤ currentTerm` the observation is a no-op and `DInv`'s
`map_pinned`/`gossip_pinned` survive).

**Two constructor hypotheses were discovered by doing this, and both are keepers:**

- `hbase : ∀ e, map.getLast? = some e → e.2 ≤ pos` — observations arrive in
  position order (the archive scans recorded blocks front to back). Without it
  `observeTerm` can append a base below the map's last and break ascending.
  `deliverReplicate` gets this for free from `pos = durable` + `NodeWF.last_base`.
- `hterm : t ≤ currentTerm` — the archive only observes terms this node accepted,
  and the intake gate bounds those by the node's own term. Without it a
  gate-open node could observe a term above its own and break the `reconciled`
  pins.

**Revised step 0.** Extract the map-growth reasoning from `deliverReplicate`'s
`NodeWF` case into a lemma parameterised over the map change alone — something of
the shape

```lean
theorem NodeWF.observeTerm_step (hn : NodeWF nd)
    (hbase : ∀ e, nd.termMap.getLast? = some e → e.2 ≤ pos)
    (hposd : pos < nd.pn.durable) (ht : 1 ≤ t) (htc : t ≤ nd.pn.currentTerm)
    (hfields : …) : NodeWF nd'
```

so that `deliverReplicate` and `observeDataTerm` share it. That refactor is
bounded, improves the corpus on its own, and is what makes step 1 the mechanical
exercise this brief originally claimed it was. It should be landed and reviewed
separately, against the CURRENT model, before any new constructor appears.
