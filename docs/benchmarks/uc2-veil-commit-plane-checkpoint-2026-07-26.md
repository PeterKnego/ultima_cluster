# UC v2 — Veil reconfig commit plane: session-1 checkpoint (bars 1–2)

**Date:** 2026-07-26
**Brief:** `docs/superpowers/specs/2026-07-26-uc2-veil-reconfig-commit-plane-brief.md`
(option (a): abstract-quorum reformulation + inductive proof; this session = bars
1–2 ONLY — build + calibrate the plane, then STOP at the re-gate).
**Branch:** `uc2/veil-commit-plane` (worktree off main @ `59c1b60`).
**Parent:** the Veil spike gate doc `docs/benchmarks/uc2-veil-spike-2026-07-24.md`
(F-M7-2 = the finding under discharge; §5's discharged Q2 trace = the verified
Rust ground-truth map this plane mirrors).
**Guardrails held:** all work in `proofs-veil/` + the separate `veil-2.0-preview`
checkout; `proofs/` untouched; scratch — NEVER the record.

---

## 1. TL;DR

**Bar 1 PASSED; checkpoint reached; the proof push is NOT started (by design).**

- The commit/log plane is built (`ReconfigCommit.lean`) with the F-M7-2 coupling
  behind a knob, and **calibration succeeded**: with the coupling OFF the checker
  exhibits the F-M7-2-shaped loss as a **depth-13 counterexample matching the
  hand-derived trace** — a leader elected under a later (validly adjacent!)
  config missing a committed entry. With the coupling ON, the same bounded run
  comes back **clean at the calibration horizon** (bounded ≠ proof — see §4).
- Two **real UC mechanisms had to be added mid-session** to keep the calibration
  honest (config-commit quorum gating; membership-gated vote granting) — both
  found by the session's own discipline (one by adjudicating a checker CE against
  the Rust, one by hand-tracing the post-fix model), both Rust-anchored, both
  ledgered (§5). The second is **load-bearing for P2 even in the coupled model**.
- The SMT-route skeleton (`ReconfigCommitSMT.lean`) is written and elaborates:
  abstract quorums (C5 `member`+intersection idiom, VerticalPaxos-style), config
  evolution as an abstract `succCfg` with apply's ±1 shape, P2 stated, the
  adjacency lemma stated as a prominently-marked TO-BE-PROVED obligation, and a
  seed set of candidate invariant clauses — next session starts at
  invariant-hunting, not modeling.
- Election safety: no regression (§4, runs B/D).
- Honest re-estimate for the proof push vs the brief's one-LC-task anchor: §7.

## 2. What was built

### 2.1 `proofs-veil/models/ReconfigCommit.lean` — the explicit-state plane

Extends `Reconfig.lean`'s election+config planes (which stays untouched as the
archived spike artifact) with an **entry-level** commit/log plane — one tracked
entry E, LC-arc style, NOT byte streams:

- `holdsE N` (N's log contains E), `committed`, `committedTerm`.
- `commitEntry (i, q : nodeSet)`: quorum-witness `q` must be a **majority of the
  leader's CURRENT config** (adoption-at-append ⇒ cfg i is the config in force —
  Q1's re-derived round quorum) with **every member holding E** (Q2 links 1+2,
  by construction) and **the leader itself holding E** (link 3 — the spike's
  thrice-recurring `rank_leader` trap, honored from the start this time).
- `propAfterE I`: recorded at PROPOSE time — does the config entry sit after E
  in the proposer's stream? (Checking `holdsE i` at adopt time instead would
  over-require when E is appended between propose and adopt — a narrowing in the
  dangerous direction for the coupled model's clean verdict.)
- **THE KNOB** `prefixCoupling`: `adopt` additionally requires
  `¬ propAfterE i ∨ holdsE j` — a config entry that sits after E can only be
  adopted by an E-holder (in Rust: config frames are detected only in the
  archive's recorded-block walk over the contiguous fsynced prefix). OFF = the
  old `Reconfig.lean` adopt, the exact move Q2 confirmed has no Rust counterpart.
  The knob deliberately does NOT weaken `commitEntry`'s counting: that would
  admit a cheap phantom commit that BFS would return INSTEAD of the config-walk
  CE (the session-4 "Bar-2 passes on the wrong bug" trap).
- State-space diet vs the stalled `ReconfigLC.lean`: vote tally collapsed
  (BootGate obligation 2, adapted to concrete configs via quorum-witness
  parameters), `elecQuorum` history variable dropped (it served P1, which the
  brief forbids chasing), `crashRestart` behind a `crashEnabled` knob (OFF in
  bounded runs; free for the inductive path). This is what turned ReconfigLC's
  700-s-no-verdict wall into a ~26-min decisive CE run.
- Knobs: `adjacencyGuard`, `prefixCoupling`, `crashEnabled`, `vacuityCanary`,
  `p2On` (gates P2 so the election-safety regression run can be read in the
  uncoupled model without BFS stopping at the expected P2 CE).

### 2.2 Properties

- **P2 (the target), stated:** `leader_completeness` —
  `(committed ∧ leader L ∧ committedTerm ≤ curTerm L) → holdsE L`.
  Leader completeness across reconfiguration, where "committed" is
  quorum-durable under the config in force at commit time.
- `election_safety` — unchanged statement, re-verified (§4).
- `p2_antecedent_canary` — non-vacuity for the coupled clean run (session-5b
  rule: every constrained/clean verdict needs a canary): a REPORTED violation
  witnesses a commit followed by a later-term leader under a changed config,
  i.e. P2's antecedent realized non-trivially with the coupling on.
- **P1 (`quorum_overlap`) is deliberately absent** — expected false as stated
  (single-server change permits non-adjacent-config quorum disjointness); the
  repaired statement IS P2 (brief scope).

### 2.3 `proofs-veil/models/ReconfigCommitSMT.lean` — the inductive-route sketch

Definitions + property statements only (`#check_invariants` deliberately not
run — next session's work):

- Abstract types `cfgid`/`quorum`; immutable `cmember`/`qmember`/`quorumOf`;
  `succCfg` (apply's ±1 shape as an axiom); the C5 same-config intersection
  assumption; `quorum_member_sound`.
- **The adjacency obligation stated as a loudly-marked assumption that MUST be
  discharged as a theorem next session** (brief: adjacency as a theorem, not an
  axiom). The arithmetic is in the header (⌊n/2⌋ + ⌊(n-1)/2⌋ + 1 = n > n−1
  forces intersection); two discharge routes recorded: (r1) plain-Lean proof
  over a concrete majority interpretation cited as the assumption's
  instantiation obligation, or (r2) in-module counting theory (risk: reopens
  the TSet List-universe wall that killed ReconfigLC's SMT path).
- The full action set mirrored in abstract dress, with the coupling
  UN-knobbed (the proof model is the coupled one) and both session fidelity
  fixes carried (gated `commitCfg`, membership-gated granting).
- History variables for the induction: `commitCfgid`, `commitQuorum`.
- A seed set of candidate invariant clauses: the 4 portable election clauses
  from the proven Election.lean Inv, plus `commit_backed`,
  `commit_quorum_sound`, `commit_term_bound`, and the load-bearing candidate
  `electable_cfgs_contain_holder` (§8 — expected to be refined by the CTI loop,
  not survive as stated).

## 3. Bar 1 — the calibration counterexample (coupling OFF)

`#model_check { node := Fin 3, term := Fin 3, nodeSet := ExtTreeSet (Fin 3) }`,
`adjacencyGuard := true`, `prefixCoupling := false`, maxDepth 14. Verdict:
**❌ `leader_completeness` at DEPTH 13** (~26 min wall, lean ~4–5 GB), matching
the pre-run hand-derived trace step for step (the checker's symmetric
relabeling of the same shape):

```
 1 startElection(0, t1)          8 adopt(j=1, i=0)        ← THE KNOBBED MOVE
 2 grant(1→0, t1)                9 commitCfg(0, q={0,1})
 3 becomeLeader(0, q={0,1})     10 proposeRemove(0, x=0)  → cfg0={1}
 4 appendEntry(0)               11 adopt(j=1, i=0)        → cfg1={1}
 5 replicate(0→2)               12 startElection(1, t2)
 6 commitEntry(0, q={0,2})      13 becomeLeader(1, q={1}) → P2 VIOLATED
 7 proposeRemove(0, x=2) → cfg0={0,1}, propAfterE 0
```

E is committed under `{0,1,2}` with holders `{0,2}`; a **valid single-server
adjacent chain** `{0,1,2}→{0,1}→{1}` walks the config past every E-holder,
with non-holder node 1 adopting both entries (steps 8/11 — exactly the
adopt-without-prefix move); node 1 then self-elects at t2 under `{1}` and holds
nothing. Every step except 8/11 is real-UC-legal; the CE isolates precisely the
knobbed coupling. The two-hop chain is FORCED by the up-to-date restriction
(under `{0,1}` holder 0 refuses non-holder 1), which is why the shallowest CE
sits at 13 — the plane sees the class, and sees it through the guards that make
shallower variants impossible.

**This is the F-M7-2 shape made concrete at the data plane** — the evidence the
brief's bar 1 demands that the plane can SEE the class it will guard against.

## 4. Coupling ON — bounded clean + election-safety regression

| Run | Knobs | Bound | Verdict |
|---|---|---|---|
| A (calibration) | coupling OFF, p2 on | maxDepth 14 | ❌ P2 at depth 13 (§3) — **bar 1 PASS** |
| B | coupling ON, p2 on | maxDepth 13 | ✅ **no violation, 4,211,943 states** |
| C (canary) | coupling ON, canary on | (none needed — BFS stops at the CE) | ❌ `p2_antecedent_canary` at depth 10 — **non-vacuity witnessed** (the good outcome) |
| D (regression) | coupling OFF, p2 OFF | maxDepth 14 | ✅ **no violation, 9,160,143 states** |

- **Read B and D as BOUNDED, not safe** (the brief's own framing): `✅ No
  violation` renders identically for exhaustion and a depth bound. B is clean
  through the calibration horizon — the exact depth at which the uncoupled model
  exhibits the loss — which is the same-model pairing that makes a knob
  calibration meaningful (the checker finds the bug without the mechanism and
  loses it with the mechanism). The assurance result is the NEXT session's
  inductive proof.
- B re-verifies `election_safety` in the COUPLED (faithful) model through depth
  13; D re-verifies it in the UNCOUPLED model (the strictly larger behavior
  set) through depth 14 with P2 gated off so BFS cannot stop at the expected P2
  CE. Run A additionally guarantees election_safety clean at depth ≤ 12 in the
  uncoupled model (BFS returns the shallowest violation of ANY property, and
  its first was P2 at 13). **No regression.**
- C's witness doubles as a fidelity observation (§5 item 7): the canary fired on
  a STALE-t1-leader commit certified partly by a node whose `holdsE` came from
  its own t2 append — stream conflation the report-collapse obligation admits.
  Still a valid non-vacuity witness (the antecedent IS realizable; the
  hand-derived clean witness at depth ~11 is also in the space), and an
  over-approximation, i.e. the sound direction for B's clean verdict.
- **Run mechanics (recorded honestly):** a maxDepth-15 B attempt was abandoned
  at ~50 min (6.5 GB, memory margin judgment). Two d13 attempts then died at
  almost exactly ~60 min wall with no verdict, no OOM, and 7 GB free — the
  **harness background-task ~60-min ceiling**, a new operational gotcha for
  runs of this size on top of Lean's buffer-until-elaboration-ends behavior
  (attempt 1 was initially misread as an OOM; attempt 2 falsified that).
  Workaround: `setsid`-detach the build and poll the log. The detached build
  then ran B+C+D sequentially in ~80 min (an earlier edit had dropped the
  comment guard on C/D — a lucky accident that delivered all three verdicts;
  the resulting dangling-marker parse error sits AFTER the final verdict and
  voids nothing, per the zero-`error:`-lines-before-verdicts discipline).

## 5. Fidelity ledger entries (all appended to `proofs-veil/spike-ledger.md`)

Session total: **2 mechanism gaps fixed (both real UC mechanisms), 4 recorded
abstraction obligations, 1 recorded narrowing.** Both gaps biased toward
FALSE POSITIVES of P2 (present in both knob positions), i.e. they would have
broken the calibration architecture, not flattered it.

1. **Gap 1 — config commits need a C_new quorum of adopters** (found by the
   checker: attempt-1 returned a depth-11 CE riding an ungated `commitCfg` —
   a config chain to `{0}` with ZERO follower adoptions, then a solo commit
   invisible to a legitimately-elected t2 leader). Adjudicated UNREACHABLE in
   Rust: a config entry commits like any entry — C_new-quorum durable past it
   (Q1's "⌈n/2⌉ genuine C_new ackers", with the removed leader's self-ack a
   non-voter seed; `ChangePending` clears only at commit). The initial
   "ungated = sound superset" reasoning was WRONG for the calibration
   architecture: the artifact CE fires in BOTH knob positions and would have
   masked the class the knob isolates. Fix: `hasAdopted (J I)` evidence +
   quorum-witness-gated `commitCfg` (the leader self-adopts at append; `∩ cfg i`
   discards a removed leader's self-ack automatically).
2. **Gap 2 — vote granting is membership-gated on the voter's ADOPTED config**
   (Q2 link 5; found by hand-tracing the post-gap-1 model). Without it, even
   the COUPLED model loses E: a pre-E config walk shrinks the voter set
   legitimately (config entries preceding E carry no prefix obligation), E
   commits under the shrunken config, and a stale-config candidate assembles an
   old-config quorum from voters who have long since moved on — grants real UC
   refuses (M7's membership-gated solicitation/granting). Fix:
   `require nset.contains c (cfg j)` in the grant arm.
3. **Obligation — report plane collapsed** (counting toward E's commit = holding
   E, by construction): justified by Q2 links 1+2 being CONFIRMED-SAFE shipped
   mechanisms; the stale-report/divergent-tail class is banked in
   `BootGate.lean`/`Finding9.lean`. This plane checks the CONFIG coupling given
   sound reports; it cannot re-find #5/#9-class report bugs (by design).
4. **Obligation — below-floor/snapshot path (Q2 link 4) not modeled**: no
   purge/snapshot in this plane. The snapshot-carried-config jump is argued (not
   checked) equivalent-or-stronger under the coupling, since installation
   implies holding the committed state wholesale.
5. **Obligation — no leader step-down on self-removal** (carried from session
   3): a self-removed leader's flag lingers; benign for P2 (it holds E) and
   consistent with Q1's deliberate serve-until-commit window, but a P2-adjacent
   property over "current leaders" would over-count.
6. **Narrowing — the adopt window closes at `commitCfg`** (`hasProposal`
   cleared): real UC permits later adoption via journal replay/snapshot install.
   Benign under the coupling (late adopters hold the prefix a fortiori) but the
   coupled clean verdict is "clean within this restriction".
7. **Obligation — no per-term stream identity on the single tracked entry**
   (surfaced by run C's witness): a stale t1 leader can commit E counting a
   holder whose bytes are its own t2 append — real UC's handle-term-stamped
   reports reject exactly this (the machinery banked in BootGate/Finding9 and
   deliberately collapsed here, item 3). Over-approximation — sound for the P2
   verdicts. This is the brief's optional "run-2 narrowing lift" (`entryTerm`)
   surfacing in the commit plane; per the brief it was NOT forced this session,
   but it is flagged as a likely source of nuisance CTIs in the proof push.

## 6. The trusted base's open `canon` obligation — noted, not touched

`proofs/Uc2Proofs/LcClosure.lean` records the LC arc stuck exactly on the
ENTRY-level canonical-prefix bundle (canonical-prefix agreement below the
reported frontier, mutually inductive with within-regime durable stability).
Nothing here works on or weakens that obligation. But the session sharpens its
motivation from the reconfig side: this plane's coupling ("counting toward any
quorum at X ⟹ holding the committed prefix ≤ X") is precisely the canon-style
property ASSUMED at the abstraction boundary, and the calibration CE is a
machine-checked demonstration that **removing exactly that assumption yields
acked-write loss at depth 13 in the M7 setting** — i.e. canon is not proof
hygiene, it is the load-bearing fact whose absence is the bug class. If the
canon bundle ever lands in `proofs/`, this model's coupling is the shape of its
consumer on the reconfig side.

## 7. Re-estimate of the proof push (vs the brief's one-LC-task anchor)

**Estimate: 1–2 LC-task S2-equivalents; the anchor is plausible as a floor, not
a ceiling.** Reasoning:

*In favor of the anchor:*
- The C5 abstract-quorum idiom + `#check_invariants` all-n is proven on this
  exact stack (spike Bar-1: 43 ✅ via cvc5), and the election Inv clauses port
  verbatim in shape from already-proven material.
- The plane itself is now built and calibrated; next session starts at
  invariant-hunting with a seeded clause set — the modeling cost is paid.
- The Phase-2 spike history (election_safety over the N-node model in ~45 min
  vs a 1–2-week estimate) shows this fragment can go fast when the invariant
  structure is right.

*Against (the tail risks):*
- The **adjacency-lemma discharge** is a real separable sub-task (~0.5–1 unit):
  route r1 (plain-Lean majority-instantiation proof) is low-risk but is new
  proof text; route r2 risks the List-universe wall.
- The load-bearing invariant is NOT the seeded `electable_cfgs_contain_holder`
  as stated — the CE analysis (§8) shows the induction must thread **four**
  mechanisms (coupling, gated config-commit, one-in-flight, membership-gated
  grants) plus the adjacency lemma through the in-flight window. Expect the CTI
  loop to demand per-proposal bookkeeping clauses (`propAfterE`/`hasAdopted`
  soundness) and a stale-config-candidate case that is the likely hotspot.
- This session already found **two** mechanisms the model needed that the spike's
  model lacked; each new mechanism is more VC surface. If the CTI loop surfaces
  a THIRD missing mechanism, expect the 2× end.
- `succ_shape`'s ∃∀ alternation may stress the solver; mitigation: the proof
  likely needs only the adjacency lemma, not `succ_shape` directly — drop it
  from the VC set if it churns.

## 8. Early signal on invariant shape (for the next session)

The coupled model stays safe through a chain of mutually-supporting facts the
induction will have to state explicitly:

- **Post-E config commits certify holder-quorums:** if `propAfterE i`, every
  `hasAdopted`-witness holds E (coupling), so `commitCfg`'s C_new quorum is a
  quorum of E-holders — the "blocking set" of C_new. One-in-flight then
  guarantees the NEXT propose starts from a config whose quorums all intersect
  the holders. The invariant likely needs the committed/pending split:
  "`¬pending i` ∧ post-E ⟹ a quorum of `cfg i` holds E" plus an in-flight
  clause for the `pending` window (where the adjacency lemma does its work).
- **Pre-E config walks are harmless but not vacuous:** they shrink/grow the
  voter set with no prefix obligation, so the holder-blocking property must be
  stated relative to configs whose entries POSTDATE E (`propAfterE` threading),
  with E's own commit quorum (`commitQuorum`/`commitCfgid` history variables)
  anchoring the base case.
- **The stale-config-candidate case is the expected CTI hotspot:** a candidate
  electing under an old adopted config, with grants only from voters whose
  adopted config still contains it (gap 2's gate). The argument that its quorum
  contains a holder is where same-config intersection, the adjacency lemma, and
  the membership gate must meet. This is also exactly UC's tombstone/membership
  machinery's job — if the CTI loop cannot close this case, that is a question
  to take BACK to the Rust (any-hit-→-Rust rule), not a modeling failure to
  paper over.

## 9. Cost + box data

- ~1 session. Decisive runs: attempt-1 artifact CE ~6 min; calibration CE
  ~26 min (lean 4–5 GB); B+C+D in one detached build ~80 min (lean peak
  ~7.7 GB observed). Two ~60-min run attempts lost to the harness task ceiling
  (§4); ~4 GB of idle foreign rust-analyzer daemons reclaimed for headroom.
- Guardrails held throughout: memwatch active (kill <2.5 GB available), no OOM,
  no `Fin 4`, `proofs/` untouched, `Reconfig.lean` untouched.

## 10. Disposition

**STOP at the re-gate (per the brief).** The proof push (bar 3: `Inv` clauses +
P2 inductive, all-n, cvc5, CTI-adjudicated against the Q2 chain) awaits the
user's go/no-go with §7's re-estimate on the table. Gate doc §6 item 4 remains
open pending that decision; this checkpoint is its bar-1/bar-2 record.
