# Brief: move the term map to the SM's side of the durable split (Lean)

> **STATUS: PARKED 2026-08-01.** Not started, and deliberately so. Everything
> below is design and measurement, all of it produced by applied-then-reverted
> probes rather than estimates. `main` is green throughout — nothing here is
> half-landed.
>
> **Nothing in this brief is on a correctness critical path.** The acked-write
> loss that motivated it (issue #7) is fixed in Rust, caught by `uc_sim`, and
> refuted at world level in Lean. This is model fidelity.
>
> **If you pick it up, start at "Scoping the FCA weakening" (last section) —
> not at step 1.** The staging in the middle of this document was corrected
> twice by doing it; the last two sections supersede it.
>
> **Landed groundwork** (green, on `main`, useful whenever this resumes):
> `NodeWF.observeTerm_step` + `NodeWF.observeTerm_static` (`MapWF`),
> `Data.termAt_observeTerm_self`/`_below` and `Cert.observeTerm_of_held_is_noop`
> (`ReportProvenance`).

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
   **DONE 2026-07-31:** `NodeWF.observeTerm_step` in `Uc2Proofs/MapWF.lean`,
   with `NodeWF.observeTerm_static` as the validation that it is usable without
   `pos = durable`. `deliverReplicate`'s case is now an 18-line application.

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

**Revised step 0 — LANDED 2026-07-31.** Extract the map-growth reasoning from
`deliverReplicate`'s `NodeWF` case into a lemma parameterised over the map change
alone. Shipped as `NodeWF.observeTerm_step`; the final hypothesis set turned out
slightly different from the sketch below (`hposcur : pos ≤ nd.pn.durable` plus
`hdmono` replaced the strict `pos < nd'.pn.durable`, because the empty-map
`floor0` case needs the base to be within the CURRENT frontier to force
`pos = 0`). Sketch as originally written:

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


## Step 1 measured (2026-08-01) — four files, not one

With step 0 landed, step 1 was re-attempted and measured, then reverted. `main`
untouched.

**Green under the new constructors:** `ProtocolData`, `ProtocolCommit`,
`LogMatching`, `MapWF`. Step 0 did exactly what it was for — `MapWF`'s
`observeDataTerm` case is a **six-line application** of
`NodeWF.observeTerm_static`, against the ~110-line rewrite it would otherwise
have been. `LogMatching` needed one real argument (the reconciled-node no-op,
below). So the extraction thesis is confirmed, on the file it was built for.

**Still to do:** `ReportProvenance`, `TakeDiscipline`, `StageB`, `StageC` — each
needs its own step-0-style extraction, because each couples the term map to
something different and `deliverReplicate`'s reasoning for it is inline.

Sizes of the existing `deliverReplicate` cases, as an upper bound on what has to
be factored:

| file | `deliverReplicate` case | map-dependent invariant fields |
|---|---|---|
| `ReportProvenance` | **376 lines** | ~5 of 15 (`closed_lag`, `frame_leader`, `gate_map_frame`, `gate_leader`, `gate_leader_eq`) — **extraction DONE 2026-08-01** |
| `TakeDiscipline` | **274 lines** | 1 of 6 (`strict_node`) |
| `StageB` | 135 lines | a few |
| `LogMatching` | 82 lines | done |
| `MapWF` | 110 lines | done (step 0) |

Total inline `deliverReplicate` reasoning across the corpus: **~1036 lines**.
That is an UPPER bound — `deliverReplicate` moves `hist`, `durable` AND the map,
while `observeDataTerm` moves only the map, so each case's map-dependent subset
is what actually needs factoring. For `ProvInv` that subset is real work, not
transfer: a gate-open node that grows its map must re-establish `gate_leader`
(`e.1 ≤ termAt (leader's map) e.2`) for the new entry, which needs the observed
byte's attribution routed through `gate_leader_eq` — available, since
`pos < smDurable ≤ durable` puts the byte below the frontier, but it has to be
threaded.

**Revised recommendation.** Step 1 is four more file-level efforts of the same
shape as step 0, largest first (`ReportProvenance`), each landable and reviewable
on its own against the current model. It is a dedicated arc, not a session.

**Two facts worth keeping from the attempt**, both already validated:

* The `observeDataTerm` constructor needs FOUR hypotheses, each earning its keep:
  `hpos : pos < smDurable` (the point of the exercise), `hbase` (observations
  arrive in position order — otherwise `observeTerm` can append below the map's
  last), `hterm : t ≤ currentTerm` and `hdterm : t ≤ dataTerm` (the archive only
  observes terms this node accepted; `deliverReplicate` gets both from
  `hdr = dataTerm`).
* `LogMatching`'s `DInv` pins (`map_pinned`, `gossip_pinned`) survive because a
  reconciled node's map already ends at its own term, so with `hterm` the
  observation is a NO-OP there. That argument is three lines and reusable.


## ReportProvenance extraction — done (2026-08-01)

Two lemmas, `Data.termAt_observeTerm_self` and `Data.termAt_observeTerm_below`,
factored out of `provinv_step`'s `deliverReplicate` case. They are the map-growth
core that case was carrying inline:

* `_below` — attribution at any position strictly under `pos` is unchanged.
  Fully generic; no hypotheses at all.
* `_self` — after growing with `(t, pos)`, position `pos` attributes to `t`.
  Two side conditions, both supplied by the caller: `hlb` (the map's last base is
  at or below `pos`) and `hup` (its last entry does not out-term the
  observation). `deliverReplicate` supplies `hlb` from `pos = durable` and `hup`
  from `ProvInv.gate_map_frame` applied to the frame it just accepted.

They live in `ReportProvenance` rather than `MapWF` because they need
`TermMap.termAt_of_last_base_le`, which is defined there.

The call site's ~40 inline lines became ~14 of application. Net +32 lines on the
file, same as step 0 — the docstrings cost more than the proofs save, which is
the right trade when the point is reuse.

**What this makes precise.** The remaining question for `observeDataTerm`'s
`ProvInv` case is now exactly one thing: *can a consensus-side observation supply
`hup`?* `deliverReplicate` gets it from a frame on the wire; an observation has
only `hhist : hist pos = some (t, v)` — the byte it already holds — so it needs a
route from held-byte to frame (`DInv`'s occurrence machinery is the likely
source). That is a small, sharp question instead of a diffuse 376-line one, which
is what the extraction was for.


## The `hup` question, settled — and the sequencing is wrong (2026-08-01)

`Cert.observeTerm_of_held_is_noop` (`ReportProvenance`, sorry-free):

```lean
Reachable w → (w.nodes j).hist pos = some (t, v) →
  observeTerm (w.nodes j).dn.termMap t pos = (w.nodes j).dn.termMap
```

The answer to "can a consensus-side observation supply `hup`?" is stronger than
yes: **it never needs to.** `FramesCurrentAuthored` — proven unconditionally as
`ProvInv`'s `fca` clause — says a node's map ALREADY attributes every byte it
holds. So `termAt map pos = t`, hence `lastTermOf map ≥ t`, hence `observeTerm`
does not grow. The growth arm is unreachable, and `hup` is vacuous.

**Which means step 1 is degenerate.** In the current model `observeDataTerm`'s
effect on the map is provably the identity. A constructor that provably does
nothing cannot exercise any invariant — every case discharges by rewriting with
the no-op and transferring. That is cheap (no per-file extraction needed, and my
earlier "four extractions" estimate was wrong in the other direction), but it
buys **nothing** towards step 3: the moment `recvReplicate` stops growing the
map, `fca` is FALSE, this lemma goes with it, and every case written for step 1
must be proved again for real.

### Sequencing corrected

Steps 1 and 2 are not useful preparation. The work is step 3, and it starts at a
different place than this brief assumed:

1. **Weaken `FramesCurrentAuthored`.** It is a PROVEN theorem
   (`frames_current_authored`) with downstream consumers, and it is exactly the
   lockstep assumption the move breaks. The honest replacement is something like
   "a node's map attributes every byte it holds BELOW `smDurable`" — conditioned
   on the absorbed frontier rather than unconditional. Re-prove its consumers
   against the weakened form. This is the load-bearing step and should be scoped
   on its own.
2. Then `recvReplicate` stops growing the map, `applyGossip` reconciles against
   `smDurable`, and `observeDataTerm` appears — at which point it is no longer a
   no-op and the extractions (`NodeWF.observeTerm_step`,
   `termAt_observeTerm_self`/`_below`) finally earn their keep.
3. Then role (d).

The two extractions already landed remain the right groundwork — they are what
step 3's real cases will be built from. What changes is that there is no cheap
intermediate landing between here and there.


## Scoping the FCA weakening (2026-08-01) — it needs a third field

Read-only survey. `FramesCurrentAuthored` is
`∀ j p t v, hist p = some (t,v) → termAt termMap p = t`, defined in
`LeaderCompleteness` and discharged as `ProvInv.fca`.

### Consumer census: 15 preservation sites, 4 genuine consumers

**15 preservation sites** inside `provinv_step` — `exact h.fca k p t v hh` and
three bundle transfers. These are the proof OF fca, not uses of it; under a
weakened statement each needs the new side condition threaded, but each is a
one-liner.

**4 genuine consumers**, and they differ sharply:

| site | what it needs | verdict |
|---|---|---|
| `StageB:444` | attribution at `e.2` for an entry `e` of the node's OWN map | **improves** — under the move, map entries satisfy the frontier condition by construction (it is exactly `observeDataTerm`'s `hpos`) |
| `StageC:514` (`lc_of_ctl`) | attribution at a committed position on a leader | **low risk** — inside the unlanded `leader_completeness` assembly, whose hypothesis bundle (`CommittedTermAtLeaders`) is not yet fixed and can carry the condition |
| `StageB:1126` | attribution at a served-tail position on a leader | **needs a route** to `p < frontier` |
| `TakeDiscipline:1321` | attribution at `p` for a take-discipline pin | **needs a route** to `p < frontier` |

So the consumer side is small: two sites need real work, one improves, one is
deferred with the theorem it belongs to.

### The finding: conditioning on `smDurable` is UNSOUND

The obvious weakening — "attributes every byte held below `smDurable`" — is **not
preserved by `absorbDurable`**. That step sets `smDurable := durable` and does
NOT touch `termMap`, so every byte between the old and new `smDurable` becomes
in-scope while still unattributed. The invariant would break on the very step
that exists to model the consensus agent catching up.

### Consequence: a third field

The condition has to name what the MAP has actually absorbed, not what the
counter-view has. That means a `mapFrontier` on the node — the archive's scan
position — with

* `observeDataTerm` advancing it (it is the step that observes),
* `absorbDurable` leaving it alone,
* `recvReplicate` leaving it alone,
* an invariant chain `mapFrontier ≤ smDurable ≤ durable`,

and FCA weakened to `p < mapFrontier → termAt termMap p = t`.

This is a genuine addition to the model, not a restatement — and it is a THIRD
frontier alongside the two the split already introduced. Worth stating plainly
because it changes step 3's shape: the move is not "make the map lag", it is
"give the map its own frontier and make everything that reads the map read that
frontier too".

### Revised size

Bounded and now concrete, but larger than "weaken one theorem":

1. add `mapFrontier` + the `≤ smDurable` invariant (a third `SmLeDurable`-shaped
   induction, cheap — the existing one is the template);
2. weaken FCA and re-thread its 15 preservation sites;
3. fix the two consumers that need a frontier route (`StageB:1126`,
   `TakeDiscipline:1321`);
4. then the map-growth removal, `applyGossip` on `smDurable`, and role (d).

The two extractions already landed still serve step 4. Nothing before that is
worth landing on its own — see the step-1 degeneracy note above.
