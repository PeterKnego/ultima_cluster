# Inductive proof vs. explicit-state search, explained

**Audience:** developers reading the Veil work — why the Reconfig commit-plane
item chose option (a) (abstract-quorum reformulation + inductive proof) over
option (b) (a larger box for deeper bounded search). No model-checking
background assumed.
**Status:** explanatory. The normative record is the Veil gate doc
(`docs/benchmarks/uc2-veil-spike-2026-07-24.md`, §4/§6) and the dispatch brief
(`docs/superpowers/specs/2026-07-26-uc2-veil-reconfig-commit-plane-brief.md`);
if this note and those disagree, they win.

---

## The problem being solved

The Veil spike built a model of UC's membership changes and asked the checker:
*can a reconfiguration ever lose committed data?* The checker said yes — but it
was **crying wolf**. Its model let a node adopt a new config without holding
any of the history, so it "found" losses that real UC structurally cannot have:
in real UC a config change is a log entry, so you cannot have adopted it
without holding everything before it (the exact mechanism chain the §5 Q2
directed Rust trace verified, gate doc §5). That false alarm is finding F-M7-2.

To ask the question properly — *does an elected leader, across any sequence of
membership changes, always hold everything ever committed?* — the model needs
a "commit plane": some representation of who holds what. Adding one forces a
choice between two fundamentally different ways of checking.

## Option (b): search every state — and why it cannot work here

An explicit-state checker literally enumerates every reachable configuration of
the system and looks for a bad one. It is superb at *finding* bugs: when there
is a needle, breadth-first search hands you the shortest path to it — that is
how the spike rediscovered known shipped bugs in seconds.

But to conclude "**no** bad state exists," it must finish the haystack. Add a
commit/log plane — who holds which entries, at which terms, under which
configs — and the number of states explodes exponentially. The spike already
hit this wall: one reconfig property was unreachable after 700 s of search,
and one bound setting reached 12.1 GB RSS and nearly took the box down (the
`Fin 4` wall, gate doc §3c). A bigger machine just moves the wall: it buys
*deeper search for counterexamples*, but the "no bug exists" answer recedes
exponentially faster than hardware grows. Since the entire point of the item is
the assurance answer, (b) is structurally the wrong tool — which is why it was
rejected, not deferred.

## Option (a): prove it by induction instead

Instead of visiting states, you state an **invariant** — a property of any
single state, e.g. "every committed entry is held by a quorum of the config in
force" — and prove exactly two things:

1. It holds in the initial state.
2. **Every possible step preserves it**: if it holds before any transition, it
   holds after.

If both hold, the invariant holds forever — after any number of steps, in any
order, with **any number of nodes**. No enumeration; the state explosion never
happens because states are never visited. An SMT solver (cvc5, via Veil's
`#check_invariants`) checks step 2 symbolically, the same way the spike
certified election safety "all-n" in Bar-1.

The "**abstract quorums**" half is what makes the induction tractable: instead
of modeling quorums as concrete vote-counts over 3 or 4 enumerated nodes,
quorums become abstract sets carrying only the one fact the proofs ever use —
*any two quorums of the same config intersect* — plus, for reconfiguration
specifically, a **lemma** (not an axiom) that consecutive configs' quorums
intersect, proved from the ±1 single-server rule. Everything irrelevant to the
argument is abstracted away, which is exactly why the solver can handle all
cluster sizes at once.

## The trade being made

- **(b)** is push-button but can only ever say "no bug found *within what we
  searched*."
- **(a)** delivers "no bug exists in the model, for **any** size, **any** run
  length" — but the human work is finding the right invariant. The property you
  want is rarely inductive by itself; auxiliary clauses are discovered by
  watching proof attempts fail. That is the skill-intensive part, and it is why
  the dispatch brief anchors the cost at one LC-arc task and builds in a
  stop-and-re-gate checkpoint before the proof push (the LC arc's own cost
  history — 13.3 S2-equivalents against a 5–8 estimate — is the cautionary
  anchor).

## Why calibration comes before the proof

The brief's first bar deliberately runs the extended model *without* the
holds-the-prefix coupling and requires the checker to exhibit the data-loss
shape as a counterexample. A "safe" verdict from a model that cannot express
the loss would be worthless — the same non-vacuity discipline the spike applied
everywhere (canary properties whose *violation* is the passing outcome).

## Further reading

- Gate doc §4 (F-M7-2, why the false positive is instructive) and §5 (the
  discharged Rust traces the commit plane must mirror).
- The dispatch brief — scope, bars, guardrails for the (a) arc.
- `docs/notes/uc2-read-barrier-explained.md` — same explanatory register, for
  the read path.
