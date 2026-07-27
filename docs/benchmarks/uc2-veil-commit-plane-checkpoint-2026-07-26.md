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

---

# Session 2 (bar 3, part 1) — the inductive proof push, to the mid-arc gate

**Date:** 2026-07-26 (same day, second session). **Branch:** `uc2/veil-commit-plane`.
**Policy in force:** the brief's "Re-gate outcome + bar-3 execution policy" — Opus
drives the CTI loop; hard Rust-anchored ledger rule before any model edit;
any-hit-→-Rust; two Fable gates. **This session ends at gate 1 (mid-arc).**

## S2.1 TL;DR

- **The adjacency obligation is DISCHARGED** — route **r1**, `proofs-veil/models/
  QuorumAdjacency.lean`, sorry-free (`#print axioms` = `propext, Classical.choice,
  Quot.sound` only). It proves not just adjacency but **all four** assumptions of the
  abstract quorum bundle over the intended interpretation, so the bundle is
  **satisfiable** and no `#check_invariants` green in this arc is vacuous.
- **12 of 16 invariant clauses are CERTIFIED INDUCTIVE, all-n, via cvc5** (run 3:
  **170 ✅ / 6 ❌**), including the two new load-bearing clauses this session designed
  (`holder_grants_are_covered`, `commit_leader_evidence`). All the added state is
  **ghost** — it appears in no `require`, so the reachable behaviour set is unchanged
  and the session-6 calibration is untouched by construction.
- **Still open: `election_safety` (1 CTI), P2 `leader_completeness` (2), and the
  load-bearing `electable_cfgs_contain_holder` (3).** All four are blocked on the
  SAME two adjudicated model-fidelity gaps, which are **specified and Rust-anchored
  but deliberately NOT applied** (ledger items 14/15).
- **P2 is REACHABLY FALSE in the model as session 1 left it** — an n=5 Figure-8
  trace, every step legal, where a never-deposed stale t1 leader commits E by counting
  a node that acquired "E" from a t3 leader's own append (the one-tracked-entry plane
  conflates the two streams). That is the collapsed per-term-stream-identity
  obligation (session-6 item 7) turning from "nuisance CTI source" into a hard
  blocker. It is the class UC *fixed* (Finding #6b, `new_term_pos`).
- **Operational finding: the SMT route costs ~60–95 s per full verdict table** —
  ~25× cheaper per run than session 6's explicit-state runs, and no memory pressure
  (peak well under the watchdog). Plan bar-3 sessions as many small runs.

## S2.2 The adjacency route (checkpoint question 2, answered)

**r1 chosen.** The trade-off in the session-1 memo was framed as "low-risk new proof
text (r1) vs List-universe wall risk (r2)". The decisive argument turned out to be a
different one: **Veil `assumption`s are free, and an inconsistent bundle makes every
verdict vacuously green.** This arc's bundle is four assumptions deep. r1 discharges
adjacency *and* exhibits a model of the whole bundle; r2 would have discharged
adjacency alone. The proof is ~60 lines: one counting lemma (two subsets of a carrier
whose sizes sum past it must meet, via `card_union_add_card_inter` + `omega`), then
the add case against carrier `d` (|d| ≤ |c|+1) and the remove case against carrier `c`
(|c| ≤ |d|+1). Cost: well under an hour, no wall.

## S2.3 The CTI loop — what closed, and how

| CTI (run 1) | Verdict | Fix |
|---|---|---|
| `leader_quorum` ❌ `propose` | (a) weak clause | clause named `cfgOf I`, which `propose` moves; restate over ghost `elecCfg I` (config at election time) ⇒ inductive |
| `leader_completeness` ❌ `becomeLeader` (holder's grant in pre-state) | (a) missing clause | the up-to-date guard is a GRANT-TIME fact the state forgets; new clause `holder_grants_are_covered` with ghost `gotEAt` supplying the ordering ⇒ inductive |
| solver-invented multi-commit-leader pre-states | (a) ghost soundness | `commit_leader_unique`, `commit_leader_only_after_commit`, `gotE_bounded` ⇒ all inductive |
| `election_safety` ❌ `becomeLeader` | **(b) model infidelity** | MODEL-EDIT-2 (below) — tried (a) first and rejected it with a legal-chain n=5 trace |
| `leader_completeness` ❌ ×2, `electable_cfgs_contain_holder` ❌ ×3 | blocked | need MODEL-EDIT-1 **and** -2 |

## S2.4 The two gaps put to the gate (full text: ledger items 14/15)

1. **MODEL-EDIT-1 — `commitEntry` counts reports that are not the leader's.**
   Rust: `uc2_consensus/src/election.rs:545-552` (stale report dropped; higher-term
   report becomes `adopt_term`) so only own-term reports reach
   `tracker.on_durable` (`:566-570`); companion clamp `new_term_pos` (`:1451-1456`,
   Finding #6b). Proposed edit: `commitEntry` also requires
   `∀ V ∈ q, gotEAt V ≤ curTerm i` — strictly WEAKER than the Rust gate, i.e. an
   over-approximation (the sound direction), and it reuses a ghost variable already
   present rather than adding a message plane.
2. **MODEL-EDIT-2 — granting ignores CONFIG-entry currency.**
   Rust: `log_ok` = `(cand_last_term, cand_last_durable) >= (our_term, our_durable)`
   (`election.rs:342-350`, `:1240-1247`, call site `:1222`) over `durable`, the
   contiguous fsynced frontier that CONTAINS config frames (gate doc §5 Q2 link 1);
   plus Ongaro's errata precondition, `propose_config` → `NotServing` unless the
   leader committed an entry of its own term (`election.rs:876-878`). Proposed edit:
   an immutable strict chain order `cfgLt` plus `require ¬ cfgLt (cfgOf c) (cfgOf j)`
   in the grant arm — **the same `log_ok` abstraction the model already applies to E,
   extended to the other log content it tracks**; the session-1 asymmetry IS the gap.
   Recorded narrowing (for BOTH the new guard and the pre-existing E-guard, which has
   carried it unrecorded since `Reconfig.lean`): it is stronger than `log_ok`, which
   would grant to a divergent higher-`last_term` candidate; excluding those rests on
   the canonical-prefix property this plane already assumes at its boundary.

**Neither is applied.** Nothing in run 3 rests on an unaudited model change.

## S2.5 Why these two, and what they buy (the induction, sketched)

`electable_cfgs_contain_holder` becomes provable in exactly the shape the discharged
adjacency lemma was built for: a committed config C_{k+1} is held by a C_{k+1}-quorum
of adopters; **adjacency** makes that quorum meet every C_k-quorum; **MODEL-EDIT-2**
makes those adopters refuse a candidate stuck at C_k. Chain induction then says no
quorum of any adopted config is free of "current" nodes — and, once E commits, free of
an E-holder. P2 closes in two cases: `curTerm L > committedTerm` via the
already-inductive `holder_grants_are_covered` against a `commitQuorum` member (which
needs **MODEL-EDIT-1** to guarantee `gotEAt V ≤ committedTerm`), and
`curTerm L = committedTerm` via the already-inductive `commit_leader_evidence` +
`grant_uniq`. Both edits are load-bearing; neither is optional; the four mechanisms
§8 predicted are all present in that argument, with adjacency now a theorem.

## S2.6 Disposition

**STOP at Fable gate 1 (mid-arc), per the bar-3 policy** — trigger: two adjudicated
(b)-class edits, both load-bearing for every remaining clause. This is *not* a wall:
the route to P2 is mapped, and the adjacency lemma, the ghost apparatus and 12
inductive clauses are banked and independent of the gate's verdict. What the gate
should audit: ledger items 14 and 15 against the Q2 chain — in particular whether
MODEL-EDIT-2's `cfgLt` guard is the right abstraction of `log_ok` or whether the
divergent-branch narrowing it introduces is too much assurance to give away.
Next session, on a GO: apply both edits, re-run the session-6 calibration pair
(coupling OFF must still CE at depth 13; ON + canary must still witness), then
resume the CTI loop on `electable_cfgs_contain_holder` → P2.

---

# Session 2, part 2 — post-gate-1: edits applied, chain induction, ~5h checkpoint

Gate 1 ruled: both infidelity adjudications CONFIRMED against the Rust, the
(a)-rejection for EDIT-2 confirmed sound, the r1 witness verified non-vacuous.
EDIT-1 approved as specified; EDIT-2 approved with three required revisions. All
revisions implemented; work continued under the same policy to the time bound.

## S2.7 What gate 1 changed (all implemented)

- **CRITICAL (gate finding, verified):** with `cfgid ↦ Finset node` the `cfgLt`
  axioms are **unsatisfiable** (add-x-then-remove-x makes `succCfg` symmetric ⇒
  `cfgLt c c`), which by this arc's own anti-vacuity doctrine would void every
  subsequent green. `QuorumAdjacency.lean` now carries **two chain-indexed
  witnesses** and proves the full bundle over them, `#print axioms` clean:
  **W1** `ℕ × Finset V` (permits branching — proves the model does not secretly
  assume linearity) and **W2** `ℕ` over a fixed ±1 chain (adds successor
  functionality, order totality, immediacy, connectedness, least-genesis).
  Bookkeeping recorded: once `cfglt_total` is assumed, **W1 no longer witnesses the
  bundle — satisfiability now rests on W2**, and W1 is kept precisely to document
  which assumption buys linearity.
- **Narrowing (n2)** recorded (gate-supplied): `cfgOf` conflates holding a config
  entry with having adopted it; real UC can `log_ok`-grant to a candidate durable
  past a config frame whose adoption still lags the archive re-scan
  (`election.rs:889-899`).
- **Both narrowings + the conditionality are now in the MODEL HEADERS** of
  `ReconfigCommit.lean` and `ReconfigCommitSMT.lean`, verbatim in the required form:
  **any SAFE verdict from this plane is CONDITIONAL — on the canonical-prefix /
  contiguity discipline (Q2 chain, CONFIRMED-SAFE in Rust) and on the data-plane
  freshness / Finding-#6b `new_term_pos` clamp (proved at the Lean tier).**
- Ledger citation corrected: the E-guard entered with **`ReconfigLC.lean:108`**
  (session 3), not `Reconfig.lean` (which has no commit plane at all).
- `gotEAt` recorded as promoted from ghost to load-bearing by EDIT-1.

## S2.8 Calibration cross-check — BOTH PASS (21m10s, one detached twin build)

| run | knobs | verdict |
|---|---|---|
| **A2** | coupling OFF, EDIT-2 on, adds off | **❌ `leader_completeness` at depth 13** — the session-6 CE survives trace-for-trace. EDIT-2 did not break the plane's eyesight. |
| **C2** | coupling ON, canary | **❌ `p2_antecedent_canary` at depth 10** — non-vacuity still witnessed. |

C2 did *not* shift to the depth-11 witness the gate anticipated, and that is correct:
the shift was predicated on EDIT-1, which is deliberately absent from the twin (gate
direction — a `gotEAt` function is a ~x27 state multiplier at `Fin 3`). The twin now
over-approximates the proof model: strictly more behaviours, the sound direction for
a CE calibrator.

## S2.9 Three further model edits — and why they were unavoidable

Applied with ledger-first discipline (items 19-21); **all three are UNAUDITED and
gate 2 must review them:**

- **EDIT-2b `cfglt_total`** — config history is one chain, not a tree. Applied under
  gate 1's explicit chain-indexing sanction; a narrowing inside the (n1) boundary;
  witnessed by W2's already-proved `l_cfglt_total`.
- **EDIT-2c** — `adopt` may not move a node's config BACKWARD (the archive walks
  recorded blocks in log-position order; gate doc §5 Q2 link 1).
- **EDIT-3 `serving`** — `propose` requires the leader's own config to be committed.
  **Required: without it `election_safety` is reachably FALSE in the model.** The
  n=5 trace: `pending` is per-node, so a leader B that adopted A's uncommitted `C1`
  and won a term can propose `C2` with `C1` still uncommitted; two leaders then split
  the same term across disjoint `C0`- and `C2`-quorums. Real UC forbids it —
  `propose_config` → `NotServing` unless the leader committed an entry of its own
  term (`election.rs:876-878`, "the single-server-change precondition", Ongaro's
  errata), and commit is prefix-closed, so that own-term commit also commits `C1`.
  Session 1 modeled one-in-flight per NODE; the real mechanism is cluster-wide.

Plus a five-part chain-order assumption package (`cfglt_total`, `succ_immediate`,
`cfglt_connected`, `genesis_least`, W1→W2 witness migration) — every part proved of
W2, so the bundle stays satisfiable and no verdict is vacuous.

## S2.10 Where the induction stands (run 8, banked)

**290 ✅ / 7 ❌. 22 invariant clauses + `doesNotThrow` certified inductive, all-n**
(12 at gate 1). Nine new config-chain clauses joined the bundle. **P2's CTI count fell
from 2 to 1** — `commitEntry` no longer breaks it, i.e. EDIT-1 + `commitq_gotE` closed
the Figure-8-shaped half exactly as designed.

Open: `election_safety` (1), `leader_completeness` (1), `eleccfg_not_stale` (2),
`electable_cfgs_contain_holder` (3). **The entire residue is one argument** — the
stale-config election — and it now has a diagnosed hole rather than a mystery:
the adjacency intersection between a leader's electing quorum and a committed config's
adopter quorum can land on **the candidate itself**, which is legitimately at-or-past
the newer config (it may have advanced after its own election). The fix has a template
already in this model: freeze the grant-time config the way `gotEAt` freezes the
acquisition term (`cfgAt` ghost + the same grant-postdates-adoption ordering, since
`adopt` requires `curTerm j <= curTerm i`). **Ghost state plus clauses — not a further
model edit.**

## S2.11 Disposition

**~5-hour checkpoint per the bar-3 policy. Not certification, not a wall.** Next
session opens on the `cfgAt` hole with no new mechanism expected. Gate 2 must audit
ledger items 19-21 and the chain-order assumption package, and the final claim must
carry the conditionality of S2.7 verbatim.

---

# Session 2, part 3 — post-gate-1b: recording debts, the cfgAt step, close

Gate 1b: no stop-the-arc Rust finding; edits 19-21 APPROVED; run-8 greens STAND in
their declared conditional form. A **process breach was confirmed** — runs 6-8 were
built on three unaudited edits — and the rule is now a COUNT: any new `require` or
assumption stops the session for a gate before another run is banked. Adopted.

## S2.12 Recording debts — all five discharged

- **(n3) added to both model headers and ledger items 19/20**: MODEL-EDIT-2c makes
  adoption forward-only, but real UC moves the adopted config BACKWARD in exactly one
  place — the M7 truncation revert (`election.rs:703-748`). Those are the
  config-BRANCH states, which EDIT-2b's linearity also excludes. Item 19's claim is
  **sharpened**: UC linearizes config history only for the CANONICAL history — across
  branches two configs can share a `version`, which is exactly why the forward gate is
  a version COMPARISON, not a global order (so the gate is not independent evidence
  for linearity).
- **Item 20 primary anchor amended** to the version gate `election.rs:751-756` +
  `config.rs:133` (bump by one); archive position-order now supporting; snapshot fiat
  adoption verified forward via its `durable < floor` gate.
- **Item 21 primary reassigned to `config_pending()`** (`election.rs:854-858`,
  enforced `:879-881`) — the literal abstraction of the model's require, blocking the
  same-leader path; `serving` is complementary, blocking the new-leader path.
- **Axiom audit completed**: `#print axioms` now covers all seventeen witness
  theorems including the three newest; all clean; banked in
  `proofs-veil/logs/quorumadjacency-axioms.log`. The claim is now mechanically backed.
- **Twin divergence list completed**: FOUR SMT-only mechanisms, not one — (d1) EDIT-1,
  (d2) EDIT-2b linear history, (d3) EDIT-2c forward-only adoption, (d4) EDIT-3
  cluster-wide one-in-flight. All over-approximate in the twin.

## S2.13 The cfgAt step — applied, compliant, not yet closed

Ghost + clauses only; **mechanically verified: 35 `require`s and 11 assumptions in both
the banked run-8 model and this one.** `cfgAt N` mirrors `gotEAt`; `elecCfg` is now
frozen from CANDIDACY (sound: a candidate's config cannot move — `adopt` clears
candidacy, `propose` requires `leader`).

**The gate's correction to my diagnosis is confirmed**: both holes were real. The
granter-advanced-after-granting shape needed `cfgAt`; the `V = i` disjunct needed
`elecCfg` frozen from candidacy plus `cand_cfg_frozen`/`role_exclusive`.

Two findings from the attempt:

1. **`eleccfg_not_stale` was FALSE as stated — a defect in my invariant, not the
   model.** A stale leader is legal in UC (no check-quorum step-down), so a leader
   elected under an old config keeps its flag while later configs commit. The property
   is about TERMS; replaced by the term-conditioned `no_stale_election`.
2. **The ordering bound cannot ride on `committed_cfg_quorum`** — strengthening it
   with `cfgAt V ≤ cfgCommitTerm D` broke a previously inductive clause, because an
   adopter's `cfgAt` RISES when it later moves further along the chain. The fact the
   argument needs is per-(node, config) — "the term at which V FIRST reached D or
   later" — and needs its own ghost. Reverted.

**Run 10: 343 ✅ / 9 ❌ — 26 clauses inductive (up from 22), but P2 regressed from 1
CTI to 2** and the template clause `grant_cfg_covered` is not yet inductive. Net:
more machinery certified, the target not closer. Run 8 remains the reference for P2's
CTI count; run 10 is what the file carries. The regression's likely source —
`elecCfg` now written at `startElection` as well as `becomeLeader`, changing what
`commitElecCfg` snapshots in non-reachable pre-states — is UNDIAGNOSED.

## S2.14 Disposition

**Session close, past the ~5-hour bound. Not certification, not a wall.** The residue
is still the single stale-config-election argument, now with two named sub-obligations
(the per-(node,config) reach ghost; the run-10 P2 regression). Gate-2 scope as
directed: (n1)+(n2)+(n3) verbatim in the final conditionality, and the (d1)-(d4)
divergence list complete — both are now written into the model headers.

---

# Session 3 (bar 3, part 3) — the regression diagnosed, and the reach ghost

Fresh driver context by design. Binding frame unchanged: the count-based corrective
(any new `require`/`assumption` stops the session for a Fable gate before another
banked run), ledger-before-proceeding for every artifact adjudication, and the final
verdict CONDITIONAL on (n1)+(n2)+(n3).

**Inventory correction.** The gate-1b baseline was recorded as "35 `require`s and 11
assumptions". The mechanical count is **34 / 11** (`grep -cE '^\s*require '`). The model
at `ce4ab33` is unchanged; only the prior entry's number was wrong. 34/11 is the count
this session holds itself to, and it was re-verified before every banked run.

## S3.1 Task 1 — the run-8 → run-10 P2 regression is **(c)**, not (a)

The predecessor's suspicion (the new `elecCfg` write site at `startElection` changing
what `commitElecCfg` snapshots) is **refuted**. Three independent lines:

1. **The CTI cannot reach the new write site.** Run 10's `commitEntry` P2 counterexample
   has `candidate = []`. `startElection` writes `elecCfg` only for candidates and
   `becomeLeader` overwrites it for every leader, so on a candidate-free state the two
   write-site regimes are pointwise identical — and `commitEntry` writes no `elecCfg`.
2. **The pre-state violates run 8's `eleccfg_not_stale`.** `leader 1`,
   `cfgCommitted cfg2`, `elecCfg 1 = cfg1`, `cfgLt cfg1 cfg2`. Run 8's bundle excluded
   it outright. Run 10 replaced that clause with the strictly weaker, term-conditioned
   `no_stale_election` — and that replacement is the *only* weakening between the two
   bundles; everything else run 10 did was additive.
3. **Diagnostic run D1** (run-10 model + run-8's clause restored verbatim, nothing else
   changed): **353 ✅ / 10 ❌ with `leader_completeness` failing at `becomeLeader` only** —
   the `commitEntry` CTI disappears and P2 returns to exactly run 8's single CTI.
   Log: `proofs-veil/logs/smt-D1-diag-regression.log`.

**The consequence is the finding, not the diagnosis.** `eleccfg_not_stale` is FALSE in
reachable states — re-verified in the Rust this session: a leader leaves `Role::Leader`
only via `adopt_term` on a strictly higher term (`election.rs:1059-1061`) or the M7
self-removal/demotion latch (`:1505`/`:1539`); the two `step_down_to_follower` call
sites at `:541`/`:592` are candidate-only. There is no check-quorum step-down. So a
false clause was serving as a live hypothesis for every other clause in run 8's bundle,
and P2-at-`commitEntry` rested on it. **Run 8 is retracted as the reference state for
P2's CTI count**; run 10's 2 CTIs is the honest baseline, and run 8's "22 inductive
clauses" was inflated by the same hypothesis. Run 10's "regression" was the bundle
becoming honest.

**Generalized lesson (recorded in the ledger):** a non-inductive clause is not a
harmless open obligation — it is an assumed hypothesis for its neighbours. Any clause
with open CTIs must be re-argued for TRUTH before greens around it are quoted as
progress.

## S3.2 Task 2 — the per-(node, config) first-reach ghost works

`reachAt (N, C) : term` freezes the term at which N FIRST reached C-or-later
("reached C" := `¬ cfgLt (cfgOf N) C`, monotone because MODEL-EDIT-2c makes adoption
forward-only). Written at the two `cfgOf` writers for every config a move newly covers,
read in no `require` — **34/11, unchanged**.

**Run 11: 398 ✅ / 9 ❌.** All five new clauses inductive first try, and the
`committed_cfg_quorum` strengthening that broke under `cfgAt` in run 9 (`tot.le (reachAt
V D) (cfgCommitTerm D)`) is now inductive. **31 clauses + `doesNotThrow`** (run 10: 26),
same nine CTIs as run 10 clause-for-clause — no regression. Ledger item 24 is closed:
`cfgAt` rises as an adopter walks on, `reachAt` freezes.

Two things the CTI loop taught in passing: `grant_reach_covered` must conclude over
`cfgOf C`, not `elecCfg C` (a winner that has since PROPOSED can still take late grants
at the same term, and the guard then compares against its moved config) — `cand_cfg_frozen`
bridges back to `elecCfg` at `becomeLeader`, the only action that can newly create a
stale-config leader. And the same-term wrinkle is handled structurally by the `V = i`
clause, not by term-strictness.

## S3.3 A second false clause — and what it says about counting greens

`electable_cfgs_contain_holder` as written since session 1 is **false in reachable
states**. Countermodel (n=5, every step legal): genesis `C0={0..4}`, commit `C1=C0∖{4}`
(3-of-4 adopters) then `C2=C1∖{3}` (2-of-3), node 4 never adopts; commit E under
`commitCfgid=C2`; the `C0`-quorum `{2,3,4}` holds no E. Real UC is safe there because
the config-currency guard makes the advanced nodes refuse a `C0`-stale candidate — not
because stale quorums contain holders. The clause is restricted (run 12) to configs
at-or-above `commitCfgid`.

That makes **three** clauses in this arc that were defects in what I wrote rather than in
the model (`eleccfg_not_stale`, ledger 23; `electable_cfgs_contain_holder`, ledger 25;
plus the run-8 counting effect of the first). The rule this justifies is in the ledger:
**an open clause is an assumed hypothesis, so it must be argued TRUE, not merely
"not yet inductive", before greens around it are quoted as progress.** Run 11's 398 was
measured with the false clause still in the bundle; run 12's numbers supersede it.

## S3.4 Run 12, and the one thing the residue now needs

| run | content | verdict | P2 |
|---|---|---|---|
| 10 | cfgAt template (session 2 close) | 343 ✅ / 9 ❌ | 2 |
| D1 | run 10 + run-8's false clause restored (DIAGNOSTIC) | 353 ✅ / 10 ❌ | 1 |
| 11 | + the `reachAt` ghost and its 5 clauses | 398 ✅ / 9 ❌ | 2 |
| 12 | + `reach_quorum_below`, `electable_cfgs_contain_holder` corrected | **409 ✅ / 8 ❌** | **1** |

**32 clauses + `doesNotThrow` inductive, all-n, cvc5.** `no_stale_election` at
`becomeLeader` and `leader_completeness` at `commitEntry` both went green; P2 and
`no_stale_election` each fell from 2 CTIs to 1. Open: `reach_quorum_below` (1),
`electable_cfgs_contain_holder` (3), `grant_cfg_covered` (1), `election_safety` (1),
`leader_completeness` (1), `no_stale_election` (1).

**Those two new greens are conditional**, and the condition is the session's stop point.
`reach_quorum_below` — the clause that carries the reach bound DOWN the config chain, and
the one the whole stale-config argument now runs through — is not inductive, and its
single CTI is a precise diagnosis rather than a puzzle:

> `propose` CTI, chain `genesis → cfg2 → cfg0`, terms `t1 < t0`: `cfg2` is committed with
> `cfgCommitTerm cfg2 = t0`, and `leader 0` sits at `curTerm 0 = t1` with `cfgOf 0 = cfg2`.
> `propose(0, cfg0)` is legal in the model — a leader at the LOW term proposing past a
> config that committed at the HIGH term.

Real UC forbids exactly that. `propose_config` needs `!config_pending()`
(`config_position > commit_seen`, `election.rs:854-858`, enforced `:879-880`) and a
LEADER's `commit_seen` has one writer — `rank_leader` (`:1421-1457`), clamped to this
term's `new_term_pos`; the gossip intake at `:594-595` is explicitly non-leader. So a
leader proposes past a config only after certifying that config's commit **at its own
term**. The model's `cfgCommitted` is a global flag with no such link.

### The request (ledger item 26): MODEL-EDIT-4, one new `require`

```
require cfgOf i = genesisC ∨ (cfgCommitted (cfgOf i) ∧ tot.le (cfgCommitTerm (cfgOf i)) (curTerm i))
```

in `propose`. Still an OVER-approximation of the Rust gate (which forces the proposer's
own term, not merely at-or-below), so every real UC behaviour still satisfies it. It
promotes `cfgCommitTerm` from ghost to load-bearing — the same move MODEL-EDIT-1 made for
`gotEAt`. No new assumption, so no witness/anti-vacuity debt in `QuorumAdjacency.lean`.
A ghost-only alternative does not exist: the offending behaviour is REACHABLE in the
model, so no invariant can exclude it (the MODEL-EDIT-3 adjudication shape, ledger 21).

## S3.5 Disposition

**Stop for the Fable gate on MODEL-EDIT-4 (call it gate 1c).** Everything banked this
session is clause/ghost-only at **34 `require`s / 11 assumptions** — verified before each
run — so the count-based corrective was not breached. `QuorumAdjacency.lean` and the twin
`ReconfigCommit.lean` are untouched; the gate-1 calibration cross-check obligation
transfers unspent to whichever session applies MODEL-EDIT-4. Conditionality (n1)+(n2)+(n3)
unchanged and still in both model headers; divergences (d1)-(d4) complete.

After the gate, the residue is mapped end to end (ledger 27) and needs no further
mechanism: `reach_quorum_below` closes at `propose`, `no_stale_election` closes at
`commitCfg` by the same argument, and P2's last CTI is the same-term
commit-leader-self-vote hole, which is clause-only (`voteterm_bounded` +
`commit_leader_self_vote`).

---

# Session 4 (bar 3, part 4) — gate-1c ruled, MODEL-EDIT-4 applied, the residue map executed

**Date:** 2026-07-27. Worktree `.claude/worktrees/uc2-veil-commit-plane` (from `64b4acf`).
Runs in `/home/claude/veil-spike/veil-preview`, logs `/home/claude/veil-spike/runs/` and
`proofs-veil/logs/`. Full detail in `proofs-veil/spike-ledger.md` §SESSION 9.

## S4.1 The gate-1c ruling, recorded before any run

**MODEL-EDIT-4 APPROVED** with five binding amendments, all now in the ledger and in the
model (three inline at the edit site, two in the header):

* **(a)** `cfgCommitTerm` is the **proposer-stamped** term at `commitCfg`-fire time
  (`ReconfigCommitSMT.lean:439`), NOT the real certification term — the proposer's term
  drifts upward, so the stamp can exceed the term a quorum actually certified at.
* **(b)** The over-approximation holds via `commitCfg`'s scheduling freedom (it may fire at
  the earliest enabling point, before causally-independent term raises) PLUS the
  `election.rs:545-552` own-term report gate (a certifying quorum's adoption evidence is
  causally independent of any term above the certifying leader's). **Without this argument
  the edit is unproven** — it is recorded, not assumed.
* **(c)** PROHIBITION: the `≤` must never be strengthened to `=`. Item 26's "(in fact =)"
  is true only of the authorizing advance; `=` would under-approximate, since a leader's
  SECOND proposal compares against a config committed in an earlier term.
* **(d)** Corner, for the final claim verbatim: `commit_seen` is **not** reset at
  `become_leader` (`election.rs:1040-1056`), so a fresh leader carries follower-period
  commit state; that inherited value cannot satisfy the serving latch (`:522-527`) without
  a pre-existing completeness violation — same conditionality bucket as (n1).
* **(e)** Run 12's `propose` CTI is an **unreachable** pre-state (leader at `curTerm` zero);
  the reachability evidence is the gate's hand trace, recorded in the ledger.

Also ratified: the run-8 retraction; the `electable_cfgs_contain_holder` correction; the
true inventory **34/11 at all three commits** (session 7's "35" was a miscount both times);
and that a CTI-bearing run ending `exit code 1 / build failed` is the NORMAL shape.

**The open-clause truth rule** (gate 1c, binding, recorded verbatim in the ledger and the
model header): a clause with open CTIs is a live hypothesis for every verdict in its
bundle; no run's greens may be quoted as progress unless every clause still open in that
run carries either a written truth argument or an explicit conditional label; a clause
later found false VOIDS the quoted greens of every run that carried it.

**Session-labelling reconciliation:** the ledger numbers sessions globally across the whole
Veil spike, this memo numbers them within the commit-plane arc — ledger "SESSION 7" = memo
Session 2, ledger "SESSION 8" = memo Session 3, ledger "SESSION 9" = this Session 4; items
22–24 sit under the SESSION 7 heading because they were written in that session's
post-gate-1b continuation.

## S4.2 MODEL-EDIT-4 applied — new baseline 35 `require`s / 11 assumptions

Applied in `propose` as a SECOND `require`
(`cfgOf i = genesisC ∨ tot.le (cfgCommitTerm (cfgOf i)) (curTerm i)`) rather than as a
conjunct in the existing one: the forms are equivalent by distribution, and the split makes
the mechanical count honest at the mandated 35. `cfgCommitTerm` is moved OUT of the model
header's ghost list — load-bearing now, as `gotEAt` became at gate 1. `QuorumAdjacency.lean`
is untouched (no new assumption ⇒ no new witness/anti-vacuity debt).

## S4.3 The twin calibration cross-check — DISCHARGED, both pass

The debt transferred twice (sessions 2 and 3) is spent. One detached build,
`proofs-veil/logs/twin-runA2C2-gate1c.log`, exactly two `error: Examples/` lines — both the
expected violations:

| run | knobs | verdict |
|---|---|---|
| A2 | coupling OFF, adjacency ON, addEnabled OFF, maxDepth 14 | ❌ `leader_completeness` at **depth 13** — the calibration CE survives, trace-for-trace |
| C2 | coupling ON, canary | ❌ `p2_antecedent_canary` at **depth 10** — non-vacuity still witnessed |

**Divergence (d5) recorded** in the twin header: MODEL-EDIT-4 is deliberately NOT mirrored
(the twin has no `cfgCommitTerm` at all; a per-config commit-term function is a state
multiplier past the explicit-state envelope, the same reason (d1) was skipped). The twin
over-approximates on this axis too — the sound direction for a calibrator. The divergence
list is now **(d1)–(d5)**, complete.

## S4.4 The corrected residue map, executed

Truth arguments **T1–T11** were written into the ledger BEFORE each inductiveness hunt, as
the truth rule requires.

| run | content | verdict | open |
|---|---|---|---|
| 12 | (session 3 close, conditional on EDIT-4) | 409 ✅ / 8 ❌ | 6 clauses |
| 13 | **MODEL-EDIT-4 alone**, no clause change | **410 ✅ / 7 ❌** | 5 clauses |
| 14 | `elecQuorum` ghost + `elecq_witness` + `elecq_grant_covers_reach` + `cand_reach_strict` + `voteterm_bounded` + `commit_leader_self_vote` + `commit_leader_no_foreign_grant`; `grant_cfg_covered` and `electable_cfgs_contain_holder` RETIRED | **457 ✅ / 4 ❌** | 3 clauses |

**Run 13 confirms the gate's one VALID map item**: `reach_quorum_below` is INDUCTIVE, so
run 12's two conditionally-banked greens (`no_stale_election`@`becomeLeader`,
`leader_completeness`@`commitEntry`) are discharged from their conditional form.

**Run 14 closes both of the gate's identified GAPS plus its OMISSION:**
* `no_stale_election` — INDUCTIVE at both sites. The certifying-quorum ghost is exactly
  what was missing: `leader_quorum`'s ∃q can be witnessed by a LATE grant made against a
  MOVED config, whereas the frozen `elecQuorum` carries only grants that predate
  `becomeLeader`, i.e. were made while `cand_cfg_frozen` pins `elecCfg I = cfgOf I`.
* `election_safety` — INDUCTIVE (the gate's unmapped clause, mapped by T8 and proved).
  `cand_reach_strict` excludes the run-12 CTI (a role at the zero term); the non-adjacent
  case goes frozen-quorum → `reach_quorum_below` → adjacency → `elecq_grant_covers_reach`.
* P2's `becomeLeader` CTI in run 12 was exactly the same-term commit-leader-self-vote hole
  the gate described, and the persistent carrier
  (`isCommitLeader V → ∀C ≠ V, ¬voteMsg V C committedTerm`) is what excludes it —
  `grant_state`'s first disjunct absorbs the stray `voteMsg` without constraining
  `voteCand`, so the two supporting clauses alone are not enough. Confirmed against the
  log.

**The `electable_cfgs_contain_holder` retirement, measured rather than asserted.** P2
regained a `commitEntry` CTI the instant the clause left the bundle, so **P2's argument DID
consume it** and the "nothing consumes it" ruling is not earned as written. What the CTIs
then showed is sharper: what P2 consumes is not the clause's (false) content but the
**quorum-supply** it smuggled in — the `becomeLeader` CTI interprets the intermediate
configs of a 3-link chain as having NO quorums, which makes
`adjacent_cfg_quorum_intersection` vacuous and breaks the chain from the commit config
down. In a reachable state those quorums must exist (`propose` requires the predecessor
committed; `commitCfg` requires a quorum of it), and the clause that recovers it is
`commit_leader_at_commit_cfg` (T11) via `chain_committed_below` + `committed_cfg_quorum`.
This is a strictly better outcome than keeping the original clause: the supply is now
derived from mechanisms already inductive, instead of assumed by a clause no one could
prove.

## S4.5 The tractability wall, and a retracted certification

**Run 15 (run 14's set + `role_positive_term` + `commit_leader_at_commit_cfg`) ran 2h28m
wall / 3h10m CPU at 139% and was killed with NO verdict.** `veil.smt.timeout` is **60 s per
VC** (`Veil/Base.lean:140`), so a `#check_invariants` run is bounded — but at this bundle
size that bound is ~410 × 60 s ≈ **6.8 hours**, and Lean buffers everything until
elaboration ends, so a killed run yields nothing. Operational rule adopted: at 40+ clauses,
add ONE clause at a time and treat a run exceeding ~3× the previous run's wall as a signal
to kill and bisect.

**Run 16 (same file minus `role_positive_term`): 470 ✅ / 3 ❌ in 10 minutes** — a 15×
difference that isolates the blocker. `role_positive_term` is the first clause in the
bundle to put `tot.zero` into every VC's hypothesis set, forcing `zero_le` instantiation
across the term theory. It is TRUE (T10 stands); it is withdrawn for COST. **40 clauses +
`doesNotThrow` inductive** — up from 32 at run 12.

**A certification claim is retracted.** Run 16's `election_safety` CTI was hand-checked
against **run 14's** bundle, clause by clause, and satisfies it: the two clauses run 16
adds are vacuous in that state. Since adding invariants only strengthens each VC's
antecedent, run 14's ✅ was not sound, and **run 14's `election_safety` green is retracted**
under the truth rule. The conservative reading now in force: a `#check_invariants` ✅ means
"not refuted by this bundle at this solver configuration", and a later ❌ on a STRONGER
bundle voids it. *How* run 14 produced the ✅ is a tool-level question carried to gate 2.
No Rust adjudication changes; one certification claim does.

**The defect is precisely named.** `grant_reach_covered`'s ordering bound is STRICT
(`tlt (reachAt V D) T`), so it says nothing when a voter reached its config at the very
term it granted at — the intra-term order of the two events is what decides, and the state
does not record it. The `V = I` analogue was already handled structurally
(`eleccfg_covers_early_reach`); the `V ≠ I` analogue needs the same treatment: a
**grant-time config ghost**, the `gotEAt`/`cfgAt` pattern applied to the grant. Ghost +
clauses, not a model edit.

## S4.6 Disposition

**Stop at the ~5-hour checkpoint — NOT gate 2.** Gate 2's precondition (bundle closed,
truth arguments on file) is not met: `election_safety` has one CTI and P2 has two. Banked:
the gate-1c ruling and the truth rule; MODEL-EDIT-4 at a mechanically verified **35/11**
before every run; the twin cross-check discharged with **(d1)–(d5)** complete; **40 clauses
+ `doesNotThrow` inductive**; truth arguments T1–T11; two written retirements (one of which
measured NEGATIVE and is reported that way); one retraction; one tractability wall.
`QuorumAdjacency.lean` untouched — no new assumption, no new witness debt.

Next session, in order: (1) the grant-time config ghost for the same-term wrinkle;
(2) a cheaper encoding of "no role at the zero term"; (3) the cross-config HOLDER supply
for P2@`becomeLeader` — the one place a new mechanism may genuinely be needed, and hence
the likeliest next gate request; (4) gate 2 only when all three land.

---

# Session 5 (bar 3, part 5) — the ⏱️ protocol, and a cost wall that moved

**Date:** 2026-07-27. Worktree `.claude/worktrees/uc2-veil-commit-plane` (from `3bfb6f9`).
Runs in `/home/claude/veil-spike/veil-preview`, logs `/home/claude/veil-spike/runs/`.
Full detail in `proofs-veil/spike-ledger.md` §SESSION 10 (items 32–37).

## S5.1 The controller finding — item 29's "tool anomaly" is dissolved

Session 4 retracted run 14's `election_safety` ✅ and left "how did the tool produce an
unsound ✅" as a gate-2 question. **The answer is that it never did.** Veil prints a THIRD
verdict marker — **⏱️, one per timed-out VC** — which the headline tally does not surface:

```
smt-run14-elecq-carrier.log:408-411
  election_safety ... ⏱️
    Exceptions:
      becomeLeader_election_safety_0_WP, becomeLeader_election_safety_tr_0_TR
        unable to prove goal. Try providing more hints. Reason: TIMEOUT
```

A full audit of every banked log — re-verified mechanically this session with
`grep -c "⏱️"` — finds **exactly three ⏱️ VCs in the whole arc**: run 12
(`leader_completeness`), run 13 (`leader_completeness`), run 14 (`election_safety`). Runs 8,
11 and 16 are timeout-clean.

**Binding protocol from here: a ⏱️ VC is an OPEN verdict — not a green, not a red.** Every
run is grepped for ⏱️ before it is banked, the count is quoted alongside the others
(**"N ✅ / M ❌ / K ⏱️"**), and any clause carrying one is OPEN regardless of the tally.
Corrections that follow: runs 12/13's "P2 at 1 CTI" was **1 CTI + 1 ⏱️**; run 14's
`election_safety` was never green, so the session-4 retraction stands with a corrected
cause; **run 16 (470 ✅ / 3 ❌ / 0 ⏱️) is timeout-clean and remains the baseline.** No tool
distrust is warranted, and the gate-2 investigation item is dissolved.

## S5.2 Tasks 1 and 2 — one clause answers both, and it is not the clause the map predicted

The map called for a **grant-time config ghost**. Written analysis says that ghost does not
close the CTI: the model genuinely permits a voter to grant while at config X and then adopt
a higher config at the SAME term (`adopt` requires only `tot.le (curTerm j) (curTerm i)`), so
the solver satisfies any grant-time clause and keeps the counterexample. **The voter's side
of run 16's CTI is legal; the leader's side is not** — the incumbent has
`reachAt 1 (elecCfg 1) = curTerm 1`, i.e. it reached its own ELECTION config at the very term
it was elected in, which `startElection`'s strict bump forbids. The fix is therefore a
CLAUSE, not a ghost, and it adds no state:

> **T12 `leader_reach_strict`** — `((candidate I ∨ leader I) ∧ ¬ cfgLt (elecCfg I) C) →
> tlt (reachAt I C) (curTerm I)`

and **task 2 falls out of it for free**: at `C := genesisC` (antecedent free by
`genesis_least`, and `reachAt N genesisC` is never written by either writer, so it sits at
`tot.zero`) T12 plus `zero_le` IS `role_positive_term` — the ~7 h clause of run 15 — without
putting `tot.zero` into every VC's hypothesis set. That is the "restate over an existing
bounded ghost" option, and it costs no clause of its own.

## S5.3 What actually happened: three runs, no verdict

| run | change | outcome |
|---|---|---|
| 17 | T12, ∀C form (clause-only, 35/11 verified) | **KILLED 29m42s, RSS 7.58 GB, NO VERDICT** (3×-wall rule; run 16 = 10 min) |
| 18 | T12 bisected to its two GROUND instances (`C := elecCfg I`, `C := genesisC`) | **KILLED 30m47s, RSS 7.62 GB, NO VERDICT** |
| 19 | same file, `veil.smt.timeout` 60 s → **12 s** | (see S5.4) |

Run 18 is the informative one: removing the quantified `C` — and with it the solver's
instantiation search — did **not** measurably help. The cost is the clause's presence in
every VC's hypothesis set, not its shape.
