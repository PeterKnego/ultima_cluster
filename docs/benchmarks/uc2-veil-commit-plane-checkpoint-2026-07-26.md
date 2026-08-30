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
   Rust: `uc_consensus/src/election.rs:545-552` (stale report dropped; higher-term
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
`genesis_least`) T12 plus the theory's `zero_le` IS `role_positive_term` — the ~7 h clause of
run 15 — without putting `tot.zero` into every VC's hypothesis set. The derivation runs
through `zero_le`, *not* through "`reachAt N genesisC` is never written": that equation holds
in every reachable state but is not available to the solver, and run 16's own CTI invents a
pre-state where it fails. That is the "restate over an existing
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

## S5.4 The slice device — and the clause is machine-certified

The way past the wall is not a cheaper clause but a smaller **bundle**. `#check_invariants`
proves, per clause and per action, `Inv_bundle(s) ∧ action(s,s') → clause(s')`. Prove a
clause against a SUBSET of the invariant conjunction and the full-bundle VC is *implied*
(`Inv_full → Inv_slice` only weakens the antecedent). **A slice ✅ transfers to the full
bundle; a slice ❌ or ⏱️ transfers in neither direction.** The slice is a cost device, not a
weakening.

**Run 20** — the model unchanged (same 35 requires / 11 assumptions, same actions), only the
nine clauses T12's preservation consumes: **110 ✅ / 0 ❌ / 0 ⏱️ in 80 seconds**.
`leader_reach_strict` is **CERTIFIED INDUCTIVE, all-n, cvc5**, and by monotonicity that
certification holds in the full bundle. Tasks 1 and 2's clause is true *mechanically*, not
only by argument.

## S5.5 A toolchain finding that rewrites one of this session's own inferences

`set_option veil.smt.timeout N in <command>` **does not propagate to the solver calls.** The
option must be set at FILE SCOPE. Three runs on the same file prove it: run 19 (full bundle,
"12 s", inline) behaved exactly like the 60 s default and was killed at 96 min with no
verdict; run 22 (slice, "900 s", inline) finished in the same 3 minutes as run 21 at the
default; run 23 (same slice, 900 s at file scope) went from 3 minutes to 30.

**Consequence, recorded against this session's own earlier claim:** run 19 was never a 12 s
run, so it is not evidence about where the cost lives, and S5.3's inference that the cost sits
"upstream of the solver" is **withdrawn**. The flat 7.60 GB RSS with falling CPU is the
ordinary signature of long sequential solver calls. What survives unchanged: runs 17 and 18
were both killed at ~30 minutes with no verdict, and the ∀C → ground bisection did not help.

## S5.6 Task 1's real status, and task 3's map

**`election_safety`@`becomeLeader` is OPEN (⏱️), not refuted.** Run 21 (the 17-clause
election slice) gives **206 ✅ / 2 ❌ / 1 ⏱️** — ✅ at every action except `becomeLeader`; the
two ❌ are `reach_quorum_below` at `propose`/`adopt`, slice artifacts from deliberately
omitting the three clauses its preservation consumes. Run 23 re-ran that VC pair with 900 s
apiece and it is **still ⏱️** — 15 minutes of solver time each without a counterexample.
Run 16's ❌ at this VC is *superseded*: its pre-state has `reachAt 1 (elecCfg 1) = curTerm 1`,
which violates T12, so it is not a pre-state of any T12-bearing bundle. But "no longer
refuted" is not "proved": under the truth rule the clause is carried with option (a) — the
written argument T8 + T12 — and its mechanical verdict is quoted as OPEN. **The obstacle
moved from a missing invariant to solver search.**

**Task 3 (the cross-config holder supply) is mapped, not measured.** Written out in ledger 36:
the SAME-TERM half of P2@`becomeLeader` needs only a ghost (`commitElecQuorum`) plus T13/T14,
because the commit leader may have crash-restarted and the argument must run against frozen
evidence — count-exempt, no gate. Of the STRICT half's four sub-cases, three close on existing
machinery (`CL < commitCfgid` by chain contradiction or adjacency; `CL = commitCfgid` by
same-config intersection; `CL = succ commitCfgid` by adjacency — each finishing through
`holder_grants_are_covered`). The residue is `CL` two or more steps above `commitCfgid`, which
needs an ALL-holder quorum one level down, which propagates up the chain iff every config
above `commitCfgid` was proposed by an E-holder. That has exactly one hole — a stale leader
sitting EXACTLY at `commitCfgid` — and two candidate closures: **(i)** clause-only, via the
commit leader's frozen electing quorum sitting at terms `≥ committedTerm` against
`committed_cfg_quorum`'s reach bound (works abstractly whenever `commitElecCfg = commitCfgid`);
**(ii)** **MODEL-EDIT-5**, strengthening `commitEntry`'s deliberately-weak report gate to the
own-term form `∀ V ∈ q, tot.le (curTerm i) (curTerm V)` — Rust anchor `election.rs:546-551`
(stale dropped / higher adopts) with `tracker.on_durable` at `:569`. That is a new `require`
(36) and therefore a gate stop. **It is PREPARED, NOT REQUESTED:** the gate template demands a
checker-produced reachability trace, and the cost wall meant no run could produce one. Asking
for a `require` on a hand argument alone would invert the arc's discipline.

## S5.7 Disposition

**Stop at the ~5-hour checkpoint — NOT gate 2, and no gate request made.** Gate 2's
precondition is not met, and it now has an extra clause of its own: **the bundle must close
with zero ⏱️**, not merely with no ❌.

Run 24 — the full 41-clause bundle at a file-scope 5 s per VC, which should have bounded it
at ~41 minutes — was still buffering at 61 minutes and was killed. That yields the session's
last finding: **a per-VC solver budget bounds the solver, not the run**; the remainder is VC
generation and elaboration at this bundle size, which no timeout setting touches. The cost
model for this bundle is two-term, and only the slice device attacks the second term.
**Run 16 (470 ✅ / 3 ❌ / 0 ⏱️) therefore remains the banked baseline, and nothing quoted from
this session comes from a full-bundle run.**

Banked, all clause-only at an unchanged **35 requires / 11 assumptions**, `QuorumAdjacency.lean`
untouched: the ⏱️ protocol and the dissolution of the tool-soundness question; **T12 certified
inductive in 80 seconds**; the slice device with its monotonicity argument; two toolchain
findings; truth arguments T12/T13/T14 written before their runs; an honest re-label of
`election_safety` as OPEN rather than refuted; and task 3 mapped with MODEL-EDIT-5 prepared
but deliberately not requested.

**Next session, in order — and all three are now slice-shaped work, which is the change:**
(1) close `election_safety`@`becomeLeader` by cutting its chain into named lemma clauses,
each certified in its own slice, rather than asking one VC to find the whole instantiation
sequence; (2) apply the `commitElecQuorum` ghost + T13/T14 and certify them in a slice
(count-exempt, no gate); (3) try route (i) of the holder-supply map in a slice, and only if
it fails does MODEL-EDIT-5 become a gate request — carrying a checker-produced CTI, not a
hand trace; (4) gate 2 only when the bundle closes with zero ⏱️.

---

# Session 6 (bar 3, part 6) — 2026-07-27, opus: closure by ACTION PARTITION

*Ledger: `proofs-veil/spike-ledger.md` §SESSION 11, items 41–47. Model at session start
`c93a943`; baseline **35 `require`s / 11 `assumption`s**, verified mechanically before every
launch and unchanged at session end.*

## S6.1 The finding that reorganises the arc: `#check_action`

Veil exposes a per-action verification command, `#check_action <name>`
(`Veil/Frontend/DSL/Module/Syntax.lean:308`), elaborated through the same
`runFilteredInvariantCheck` as `#check_invariants` but with the filter
`isInductionForAction` rather than `VCMetadata.isInduction`
(`Veil/Frontend/DSL/Module/Elaborators.lean:421-465`). The filter is applied where
dischargers are *started* (`Veil/Core/Tools/Verifier/Server.lean:34,49-59`), so the run pays
for one action's verification conditions instead of the bundle's.

**Why this is stronger than the clause slice.** A clause slice needs the
antecedent-weakening transfer argument, because it proves a clause against a *subset* of the
invariant conjunction. `#check_action` changes nothing about any VC — same `Invariants`
hypothesis, same `Assumptions`, same goal. **A ✅ from `#check_action A` *is* the
full-bundle verdict for that (clause, action) pair.** Nothing to ratify, nothing to transfer.

The measurement that makes the point: run 24 (session 5) put the whole bundle at 5 s per VC
and was killed at 61 minutes with no verdict. **Run 25 — the same bundle, at
`becomeLeader`, 60 s per VC — finished in 393 seconds with 42 ✅ / 1 ❌ / 1 ⏱️**, and the ❌
was the first checker-produced `leader_completeness` counterexample since run 16.

## S6.2 Task 1 — the chain cut into lemma clauses, and what the cut can and cannot do

The cut is constrained by what `#check_invariants` proves: `Inv(s) ∧ action(s,s') →
clause(s')`. The POST-state instances of sibling clauses are **not** hypotheses. So the
obvious decomposition — an intermediate "two same-term leaders have equal election configs"
clause — buys nothing: at `becomeLeader` its VC is the same crux as `election_safety`'s,
because the new leader's quorum comes from the action's `require`, not from any invariant.
What *does* help is pre-composing the SUPPORT steps, which is what the two new lemmas do.

* **T15 `role_below_quorum_strict`** pre-composes `reach_quorum_below` with
  `leader_reach_strict` (T12): every config strictly below a role's election config is
  genesis or carries a quorum whose members are at-or-past it *and reached it strictly
  before that role's term*. Stated over `candidate ∨ leader` so that at `becomeLeader` the
  pre-state instance carries verbatim (`cand_cfg_frozen`).
  **RUN 26 — 130 ✅ / 2 ❌ / 0 ⏱️ in 83 s. CERTIFIED INDUCTIVE.**
* **T17 `role_below_meets_quorum`** pre-composes T15 with
  `adjacent_cfg_quorum_intersection`, so that no single VC has to find the
  `cfglt_connected` → `reach_quorum_below` → adjacency → grant-clause sequence.
  **RUN 27 — 141 ✅ / 2 ❌ / 0 ⏱️ in 91 s. CERTIFIED INDUCTIVE.**

In both runs the two ❌ are the known slice artifact (`reach_quorum_below` at
`propose`/`adopt`, from omitting the three clauses its preservation consumes); that clause is
inductive in the full bundle from run 13 on, and again in run 25. Neither ❌ touches the
target clause's verdict — each VC assumes the slice conjunction independently.

Truth arguments T15 and T17 were written into the ledger **before** either run, per the
gate-1c truth rule; so were T13/T14 (session 5) and T18 (this session).

## S6.3 Task 2 — the frozen commit evidence, and a prediction that held

The `commitElecQuorum` ghost (written only at `commitEntry`, read in **no** `require` —
count-exempt, no gate) plus `commitq_witness` (T18), `commitq_grant_covers_reach` (T13) and
`commit_leader_frozen_reach` (T14) went into the full bundle together with T15/T17: **46
invariants + 2 safeties, still 35 requires / 11 assumptions**, `QuorumAdjacency.lean`
untouched, so the seventeen-witness `#print axioms` audit still covers the bundle.

Run 25's `leader_completeness` counterexample was adjudicated *before* the clauses landed:
same-term (`committedTerm = curTerm i`), commit leader crash-restarted, electing quorum two
configs above `commitCfgid`, model-artifact (the pre-state is unreachable — node 0 could only
sit at the top config by adopting, and every such adoption postdates the commit, so `adopt`'s
coupling requires it to hold E, which the CTI denies). The prediction recorded was that T18
+ T13 exclude it.

**RUN 28 confirmed it: 47 ✅ / 1 ❌ / 1 ⏱️ in 614 s.** All five new clauses ✅ at
`becomeLeader`; the same-term CTI is gone; what remains is a **strict-half** CTI
(`committedTerm < curTerm i` in a three-term theory).

## S6.4 The closure-criterion amendment — DRAFTED, NOT SELF-RATIFIED (gate 2 rules)

Session 5 left the arc with an unreachable closure criterion: a single `#check_invariants`
run over the whole bundle, which run 24 showed this box cannot finish. Session 5's proposed
replacement was slice-certification-with-coverage. **`#check_action` makes a strictly
stronger criterion available, and the amendment below proposes it as primary, with slice
certification demoted to a documented fallback.** Nothing here is self-ratified; the greens
of this session are quoted under the labels the amendment defines, and gate 2 decides.

> **PROPOSED AMENDMENT (bar 3 closure criterion).** The bundle CLOSES when both hold:
>
> **(A) Action-partitioned full-bundle verification.** For every action `A` of the model,
> a `#check_action A` run over the FULL bundle reports **0 ❌ and 0 ⏱️**; and for every
> clause, the initialisation obligation is ✅ in some run. This is *logically identical* to a
> single `#check_invariants` run: `#check_action` applies a filter over which verification
> conditions are STARTED and changes no VC's statement — same `Assumptions`, same
> `Invariants` hypothesis, same goal — and the initialisation obligation is
> `after_init → clause` per clause, with no other clause in its antecedent, hence
> bundle-independent. **There is therefore no weakening to ratify in (A); what gate 2 is
> asked to ratify is the IDENTITY claim, against `Elaborators.lean:421-465` and
> `Server.lean:34,49-59`.**
>
> **(B) Slice certification, as a FALLBACK where (A) is unaffordable.** A clause may be
> certified in a slice — the model verbatim, the invariant conjunction cut to a recorded
> subset — and the certification transfers to the full bundle by antecedent weakening
> (`Inv_full → Inv_slice`). Each such clause must carry, in the dossier: its certifying run,
> the run's **explicit hypothesis set** (every member of which must be a clause of the full
> bundle), its log and line, and the ⏱️-inclusive quote of that run. **A slice ❌ or ⏱️
> transfers in neither direction**, and any ❌ appearing in a certifying slice must be
> named as a slice artifact with the run that certifies that clause in the full bundle.
>
> **(C) The ⏱️ protocol is part of the criterion**, not an addendum: every banked run is
> grepped (`grep -c "⏱️"`) and quoted as **"N ✅ / M ❌ / K ⏱️"**; a nonzero K anywhere in a
> certifying run leaves the clauses carrying it OPEN regardless of the tally.
>
> **(D) A THIRD transfer, which this session had to use and which gate 2 must rule on
> separately: GHOST EXTENSION.** Run 16's greens are quoted for clauses whose full-bundle
> re-measurement this session did not repeat. Between run 16's bundle and the current one
> there are two differences: six ADDED clauses (antecedent weakening — sound, same argument
> as (B)) and one ADDED GHOST state component (`commitElecQuorum`), written only at
> `commitEntry` and appearing in no run-16 clause. The claim is that a fresh state symbol
> occurring in neither the hypothesis nor the goal of a VC cannot invalidate that VC. **It is
> believed sound and is the standard ghost-extension argument this arc has relied on since
> session 2, but it has never been ratified, and until it is, every run-16-sourced green in
> the dossier below is labelled `transfer: run-16 + ghost-extension`.**

## S6.5 The slice-coverage dossier (clause → certifying run → hypothesis set → transfer)

Bundle at session end: **46 invariants + 2 safeties + `doesNotThrow`**, at **35 `require`s /
11 `assumption`s**. Actions: `startElection`, `deliverRequestVoteGrant`, `becomeLeader`,
`crashRestart`, `appendEntry`, `replicate`, `commitEntry`, `propose`, `adopt`, `commitCfg`
(ten), plus the initialisation obligation.

| clause group | certifying run | actions covered | init | transfer needed |
|---|---|---|---|---|
| the 40 clauses of run 16's bundle | **run 16** (470 ✅ / 3 ❌ / 0 ⏱️, `smt-run16-commitleadercfg.log`) | all ten, except the three ❌ below | ✅ | run-16 + ghost-extension (D) |
| `leader_reach_strict` (T12) | **run 20** (110 ✅ / 0 ❌ / 0 ⏱️, `smt-run20-slice-T12.log`) | all ten | ✅ | slice (B), 9-clause hypothesis set |
| `role_below_quorum_strict` (T15) | **run 26** (130 ✅ / 2 ❌ / 0 ⏱️, `smt-run26-T15slice.log`) | all ten | ✅ | slice (B), 10-clause hypothesis set |
| `role_below_meets_quorum` (T17) | **run 27** (141 ✅ / 2 ❌ / 0 ⏱️, `smt-run27-T17slice.log`) | all ten | ✅ | slice (B), 11-clause hypothesis set |
| all 46 invariants + `doesNotThrow` **at `becomeLeader`** | **run 28** (47 ✅ / 1 ❌ / 1 ⏱️, `smt-run28-actionBL-frozen.log`) | `becomeLeader` | n/a | **NONE** — criterion (A) |
| `commitq_witness` (T18), `commitq_grant_covers_reach` (T13), `commit_leader_frozen_reach` (T14) | run 28 at `becomeLeader`; **run 31** (slice S3) elsewhere | see S6.6 | see S6.6 | (A) at `becomeLeader`, (B) elsewhere |

**OPEN, and therefore the reason this is not gate 2's precondition:**
* `election_safety` @ `becomeLeader` — **⏱️** in every run that has ever measured it
  (14, 21, 23 at 900 s, 25, 28, and run 30 below). OPEN under the ⏱️ protocol; carried on
  the written truth argument T8 + T12 + the T17 chain.
* `leader_completeness` @ `becomeLeader` — **❌**, the strict half (S6.7).
* `leader_completeness` @ `commitEntry` — ❌ in run 16; expected to close on T12, which
  subsumes the withdrawn `role_positive_term` (session 5, item 34). **UNMEASURED since T12
  landed** — the `#check_action commitEntry` run was started and killed for box memory when
  it collided with run 30 (S6.6), so this is an honest hole, not a claim.

## S6.6 Coverage of the frozen-evidence clauses — three slices, and a CTI that named its own fix

Run 28 certified T18/T13/T14 at `becomeLeader` in the full bundle. The other nine actions and
the initialisation obligation were covered by slice, in three attempts, each of which is
worth recording because the failures were informative rather than wasteful:

* **Run 31** (`FrozenSlice`, 17 clauses, 104 s) — **195 ✅ / 3 ❌ / 0 ⏱️**. `commitq_witness`
  and `commit_leader_frozen_reach` ✅ at INIT and all ten actions: **CERTIFIED**.
  `commitq_grant_covers_reach` ❌ at `propose`/`adopt`.
* **Run 33** (`FrozenSlice2` = +`voteterm_bounded`, `commit_leader_self_vote`,
  `reach_quorum_below`, `cand_reach_strict`, 154 s) — **237 ✅ / 5 ❌ / 0 ⏱️**; T13 still ❌,
  **and its counterexample named the omission**: a pre-state with `committed = true` and
  `isCommitLeader = []`, which `commitEntry` cannot produce (it writes both in one step) and
  which makes every commit-leader clause vacuous, so nothing bounds `commitElecQuorum`
  members' terms.
* **Run 34** (`FrozenSlice3` = +`commit_leader_evidence`, 157 s) —
  **250 ✅ / 3 ❌ / 0 ⏱️**, and **`commitq_grant_covers_reach` is ✅ at INIT and all ten
  actions: CERTIFIED**. The three residual ❌ are the standing slice artifacts
  (`reach_quorum_below` at `propose`/`adopt`, `elecq_grant_covers_reach` at one action), all
  inductive in the full bundle.

## S6.7 What did NOT close, stated plainly

**`election_safety`@`becomeLeader` is still ⏱️, and it is no longer an invariant problem.**
The measurement grid is now {full bundle, 17-clause slice, 11-clause slice} × {60 s, 300 s,
900 s per VC}, and the VC pair `becomeLeader_election_safety_0_WP` / `_tr_0_TR` times out in
every cell. **Run 32** is the cleanest statement of it: an eleven-clause slice built around
T17, **11 ✅ / 0 ❌ / 1 ⏱️ in 639 s** — no artifacts at all, and the *only* undischarged VC in
the file is the crux. The clause work the last two sessions called for is done and
machine-certified (T12 in run 20, T15 in run 26, T17 in run 27); what remains is a
first-order instantiation search cvc5 does not complete. The next lever is a **manual
discharge** of that single VC through the `@[veil] theorem … := by unveil; …` stub Veil emits
for exactly this purpose, or a different solver configuration — not another invariant.

**`leader_completeness`@`becomeLeader`: the same-term half closed, the strict half did not.**
Run 28's CTI has `committedTerm < curTerm i` in a three-term theory, with the new leader's
electing quorum two configs above `commitCfgid`. It is again a model artifact with an
unreachable pre-state (`hasAdopted = []` while sitting two adoptions above genesis). Two
things follow, both recorded in ledger 45:
1. **MODEL-EDIT-5 would not exclude it.** The own-term report gate puts `commitQuorum`'s
   members at terms ≥ `committedTerm`; it creates no intersection between a genesis quorum
   and a quorum two configs above. So this CTI is **not** the reachability trace that would
   justify the gate request, and **the request is again NOT made**.
2. **What the CTI wants is a clause chain**, and the session's contribution is to have
   derived it: `T24 : (committed ∧ cfgLt commitCfgid (cfgOf N)) → holdsE N`, reduced through
   `adopt`'s coupling and `propose`'s `propAfterE` write to
   `T23 : (committed ∧ hasProposal I ∧ cfgLt commitCfgid (proposedC I)) → propAfterE I`,
   whose only open case is a STALE proposer sitting exactly at `commitCfgid` — item 36's
   named hole. Route (i) is now understood mechanically: the contradiction is not about the
   stale leader's own E-holding but about **who can adopt from it** (`adopt` requires
   `curTerm j ≤ curTerm i`, so every adopter is staler than `committedTerm`, whereas every
   member of `commitElecQuorum` is at a term ≥ `committedTerm`), and it closes by adjacency
   whenever `commitElecCfg = commitCfgid`. Its general form is
   `T27 : (committed ∧ cfgCommitted D ∧ cfgLt commitElecCfg D) → committedTerm ≤ cfgCommitTerm D`.
   **Written, not measured** — no run in this session carries T23/T24/T27.

## S6.8 Disposition

**Stop at the checkpoint. NOT gate 2, and no gate request made.** One ⏱️ and two ❌ remain, so
the precondition (bundle closed with zero ⏱️) is unmet.

Banked, all of it clause/ghost-only at an unchanged **35 `require`s / 11 `assumption`s**,
`QuorumAdjacency.lean` untouched:
* **`#check_action`** — an action-dimension partition of the bundle that needs no transfer
  argument, and the first full-bundle verdicts since run 16 (runs 25, 28, 30).
* **Five new clauses, every one certified**: T15 (run 26), T17 (run 27), T18 + T14 (run 31),
  T13 (run 34) — plus the `commitElecQuorum` ghost.
* **A predicted CTI death**: run 25's same-term P2 counterexample was adjudicated as a model
  artifact and predicted to fall to T18 + T13 *before* the clauses were written; run 28
  confirms it.
* **The closure-criterion amendment, drafted for gate 2** (S6.4), including the
  ghost-extension transfer that this session had to use and that has never been ratified.
* **Task 1's honest verdict**: the lemma cut is certified and the crux VC is still ⏱️; the
  obstacle is solver search, and the next lever is manual discharge, not more invariants.
* **Task 3's honest verdict**: the holder-supply chain is derived one level deeper than item
  36 had it, MODEL-EDIT-5 is shown *not* to answer the CTI in hand, and no `require` is
  requested.

**A box lesson, since it cost a 15-minute run:** two Lean verification processes in parallel
drove MemAvailable to 4 GB against memwatch's 2.5 GB floor. Run these ONE AT A TIME.

---

# SESSION 7 (2026-07-27, opus) — the per-action sweep, and the T24 refutation

## S7.1 Two source-level findings that shrink the closure criterion

**(i) The initialisation obligations are bundle-independent — mechanically, not by
argument.** Amendment clause (A) claimed it; the VC generator says it outright.
`DeclarationKind.assumesInvariantsForInductionVC`
(`Veil/Frontend/DSL/Module/VCGen/Induction.lean:132-134`) returns `false` for
`.procedure .initializer`, and `mkInductionPrecondition` (`:136-143`) then builds the VC
with the precondition `fun _ _ => True` rather than `@Invariants …`. An init VC is
therefore `Assumptions → wp(after_init)(clause)` — **no invariant of any bundle appears in
it**. Every init ✅ this arc has produced, in any slice, is a full-bundle certification of
that clause's init obligation, with nothing to transfer. Transfer question (B)/(D)
consequently shrinks to the ACTION obligations alone.

Corollary, recorded so nobody looks for it: init VCs carry `action = `initializer``
(`Veil/Core/UI/Verifier/VerificationResults.lean:352`) and `getCheckableAction?`
(`Elaborators.lean:425-430`) admits only `.action` procedures, so `#check_action
initializer` is REJECTED. The init obligations are covered by the runs that already
report them, not by the sweep.

**(ii) `Invariants` includes the safeties.** `Module.assembleInvariants`
(`Veil/Frontend/DSL/Module/Util/Assemble.lean:130`) assembles the `.invariantLike` set —
`invariant`, `safety` and `trusted invariant` clauses together. So `leader_completeness`
(P2) and `election_safety` are PRE-state hypotheses in every VC, and a clause may lean on
P2 for its current-term case as part of the same mutual induction. This is what makes the
holder-supply chain's "current leader" case free, and it is why the residue below is
exactly the STALE leader.

## S7.2 The T24 refutation — item 45's chain is false, not merely unproved

Session 6 left the strict half of P2 with a written, unmeasured chain whose first link was
`T24 : (committed ∧ cfgLt commitCfgid (cfgOf N)) → holdsE N`, and whose one open case was
"the STALE proposer sitting exactly at `commitCfgid`". **That case is a reachable model
behaviour.** Five legal actions produce a state violating T24 (full step-by-step
justification, with the `require` checked at each step, in ledger truth argument T19):
a leader elected at term 1 under genesis, a second leader elected at term 2 whose quorum
excludes the first (so the first keeps its `leader` flag — nothing else clears it), a
commit at term 2 under genesis, and then a `propose` by the term-1 leader, which passes
**both** config gates through their `cfgOf i = genesisC` disjunct. The proposer moves
strictly above `commitCfgid` holding nothing.

The obvious repair — term-guarding the clause with `tot.le committedTerm (curTerm N)` — is
false too, by the same trace plus an `adopt` and a vote grant that raise the adopter's term
above `committedTerm` without giving it the entry.

**P2 itself is NOT violated in that trace**, and the reason is the shape of the real
argument: the stale leader can create configs, but it cannot create ELECTABILITY. Every
quorum of the config it creates meets `commitQuorum` by
`adjacent_cfg_quorum_intersection`, so it contains an E-holder, and the up-to-date grant
guard forbids a holder from granting to a non-holder. So this is a clause refutation and a
correction to the map — **not** a possibly-real CTI, and not a stop-the-arc finding.

**The corrected chain (ledger T20).** The holder supply is indexed by COMMITTED CONFIG, not
by node, because `propose` requires `cfgOf i = genesisC ∨ cfgCommitted (cfgOf i)`: a config
two steps above genesis exists only if the one below it committed, and a commit is a quorum
fact. `T20 : (committed ∧ (D = commitCfgid ∨ (cfgCommitted D ∧ cfgLt commitCfgid D))) →
∃ q, quorumOf q D ∧ ∀ V ∈ q, holdsE V`, with the base from `commit_backed` +
`commit_quorum_sound` and the step running the proposer's election quorum against the
predecessor's holder quorum. It is WRITTEN, NOT MEASURED: its step needs a link from a
committed config back to its proposer, which the state does not currently name, so it
likely needs one more ghost — a next-session opening move, not an end-of-session bolt-on.

## S7.3 The closure-criterion amendment, as this session leaves it

Session 6 drafted (A)–(D) and did not self-ratify them. Two of the four are now settled by
the source, and one is materially reduced:

* **(A) Action-partitioned full-bundle verification — UNCHANGED, and now exercised.** The
  identity claim (a `#check_action A` ✅ *is* the full-bundle verdict for that
  (clause, action) pair) is what gate 2 is asked to ratify, against
  `Elaborators.lean:421-465` and `Server.lean:34,49-59`. This session ran it on nine of the
  ten actions.
* **(B) Slice certification — still needed, but only for the ACTION obligations** of the
  clauses whose action was not measured directly (see the per-action table).
* **(C) The ⏱️ protocol — unchanged, and it bit twice this session** (`leader_completeness`
  @ `commitEntry`, `election_safety` @ `becomeLeader`).
* **(D) Ghost extension — SHRUNK.** It was needed for run-16-sourced greens. After this
  session's sweep, run 16 is no longer the source for any action that the sweep covered, so
  (D) applies only to the actions the sweep could not measure. The INIT half of the
  transfer question is gone outright (S7.1(i)).

**One clarification the dossier must carry, because it changes what every ✅ in the arc
means.** Each clause is verified through two VCs — a WP-style primary and a TR-style
alternative — and `effectiveStatus`
(`Veil/Core/UI/Verifier/VerificationResults.lean:120-136`) reports the clause with the best
of the two ("conclusive outcomes win over sibling errors"). So **a ✅ means "the primary or
its TR alternative discharged", and a ⏱️ means neither did.** The `commitCfg` run made this
visible: two of its VCs returned solver-unsat but produced no Lean witness
(`Induction.lean:47-58`, most plausibly downstream of the `LocalRProp` typeclass-synthesis
heartbeat warning that every per-action run carries), Veil `throwError`s, and the build
exits 1 — on an action whose clause table is nonetheless 49 ✅ / 0 ❌ / 0 ⏱️.

## S7.4 The per-action sweep — the dossier's spine

Every row is `ReconfigCommitSMTAct<action>.lean`: **the model verbatim** — 35 `require`s /
11 `assumption`s / 46 invariants + 2 safeties, each file diffed against
`ReconfigCommitSMT.lean` before launch and identical modulo the module name, the file-scope
`veil.smt.timeout` and the `#check_action` line — run one Lean process at a time with
memwatch armed. A fully green action reports **49 ✅** (46 invariants + 2 safeties +
`doesNotThrow`).

| action | run / log | verdict | wall |
|---|---|---|---|
| `startElection` | `smt-act-startElection.log` | **49 ✅ / 0 ❌ / 0 ⏱️** | 212 s |
| `deliverRequestVoteGrant` | `smt-act-deliverRequestVoteGrant.log` | **49 ✅ / 0 ❌ / 0 ⏱️** | 502 s |
| `becomeLeader` | run 28 `smt-run28-actionBL-frozen.log` (and run 30 at 900 s) | 47 ✅ / **1 ❌** / **1 ⏱️** | 614 s |
| `crashRestart` | `smt-act-crashRestart.log` | **49 ✅ / 0 ❌ / 0 ⏱️** | 703 s |
| `appendEntry` | `smt-act-appendEntry.log` | **49 ✅ / 0 ❌ / 0 ⏱️** | 835 s |
| `replicate` | `smt-act-replicate.log` | **49 ✅ / 0 ❌ / 0 ⏱️** | 1037 s |
| `commitEntry` | run 35 `smt-run35-act-commitEntry.log` | 48 ✅ / 0 ❌ / **1 ⏱️** | 1535 s |
| `commitCfg` | `smt-act-commitCfg.log` | **49 ✅ / 0 ❌ / 0 ⏱️** (build exits 1, S7.3) | 1641 s |
| `adopt` | `smt-act-adopt-t20.log` (**20 s** per VC) | **49 ✅ / 0 ❌ / 0 ⏱️** (build exits 1, S7.3) | 1407 s |
| `propose` | `smt-act-propose.log` (60 s), `-t20.log` (20 s), `-t5.log` (5 s) | **KILLED, NO VERDICT** ×3 | 3300 + 1900 + 2100 s |

`adopt` is quoted at a **20 s** per-VC budget rather than 60 s: the 60 s run was cut at 36 s
to make room for it, and a proof found at 20 s is a proof (any VC that did not close would
have been reported ⏱️ = OPEN, and none was). Every other row is at 60 s.

**Clause-level reading of the nine measured actions: all 46 invariant clauses are ✅ at
every one of them.** The only non-greens in the whole sweep are the two properties:
* `election_safety` — ⏱️ at `becomeLeader`, ✅ at the other eight measured actions;
* `leader_completeness` — ❌ at `becomeLeader`, **⏱️ at `commitEntry`**, ✅ at the other seven.

**Session 6's hole 3 moved, and only half way.** `leader_completeness` @ `commitEntry` was a
❌ in run 16 and was predicted (session 5) to fall to T12. Run 35 shows **no counterexample
survives** — but the VC does not discharge at 60 s either, so under the ⏱️ protocol it is
**OPEN**, not green. The prediction is confirmed in kind and unconfirmed in verdict.

**`propose` is a cost wall of its own.** Killed at 60 s, at 20 s and at 5 s per VC — a 12×
span of solver budgets moved nothing. Since every
per-action file elaborates the same module (`startElection` finished end-to-end in 212 s),
the missing time is per-VC WP/TR generation and SMT translation for the model's heaviest
action: five `require`s plus the conditional `reachAt i Z := if … then … else …` update
that the WP must push through every clause mentioning `reachAt`.

**What covers `propose` in the dossier** (all of it pre-existing, none of it new):
run 16 for the 40 run-16-era clauses (transfer (D)), and the all-ten-action slice
certifications for the six later clauses — T12 (run 20), T15 (run 26), T17 (run 27),
T18 + T14 (run 31), T13 (run 34). So **every clause has a verdict at every action**; what
`propose` alone lacks is a verdict that needs no transfer argument.

## S7.5 The crux VC — one lever measured, one lever not attempted

Session 6 named two levers for `election_safety`@`becomeLeader`: a different solver
configuration, or a manual discharge through the `@[veil] theorem … := by unveil; …` stub.

**The configuration lever is measured and NEGATIVE.** `mkVeilSmtTactic`
(`Veil/Frontend/DSL/Tactic.lean:872-886`) passes cvc5 exactly three extra options, one of
which — `finite-model-find`, default TRUE — is a model-finding mode and a known drag on
hard unsat goals. Run 36 is run 32's eleven-clause slice verbatim with
`veil.smt.finiteModelFind false` at file scope, 300 s per VC: **11 ✅ / 0 ❌ / 1 ⏱️ in
650 s** — same tally, same wall, same single ⏱️. The measurement grid is now
{full bundle, 17-clause slice, 11-clause slice} × {60 s, 300 s, 900 s} × {fmf on, fmf off},
and the VC pair times out in every cell.

**The manual lever was NOT attempted, and that is an omission, not a result.** The box runs
one Lean process at a time and the per-action sweep — the session's stated first priority —
used it end to end; each manual iteration costs a full module elaboration. What the session
adds is the *shape* the attempt should take: `mkVeilSmtTactic` hands the solver every `Prop`
in context (`getPropsInContext`), so the productive move is the Ivy/mypyvy idiom — `unveil`,
`have` the two or three assumption INSTANCES the T8/T17 chain needs (`cfglt_connected` at
`(elecCfg L, elecCfg i)`, then `adjacent_cfg_quorum_intersection` at the succ-step it
returns, plus `same_cfg_quorum_intersection` in the equal case), then `veil_smt` with those
as ground hypotheses. That is the next session's first Lean command.

## S7.6 Disposition — STOP at the checkpoint, still NOT gate 2

Gate 2's precondition remains **bundle closed with zero ⏱️**, and it is not met: one ❌ and
two ⏱️ survive. Everything this session is measurement-only — **`ReconfigCommitSMT.lean`
was not edited**, so the bundle stands at 46 invariants + 2 safeties, 35 `require`s / 11
`assumption`s, `QuorumAdjacency.lean` untouched and its seventeen-witness `#print axioms`
audit still covering the bundle. No gate request is made; MODEL-EDIT-5 stays PREPARED, NOT
REQUESTED (session 6 showed it would not even exclude the CTI in hand, and this session's
T24 work does not resurrect it).

**Banked:**
* **The per-action sweep** (S7.4): **nine of ten actions** with a criterion-(A) full-bundle
  verdict, **all 46 invariant clauses ✅ at every measured action**, and the residue reduced
  to two properties at two actions plus ONE unmeasured action.
* **Two source-level findings** (S7.1) that shrink the closure criterion: init obligations
  are bundle-independent *in the VC generator*, and `Invariants` includes the safeties.
* **A third** (S7.3): what a ✅ means — best-of {WP primary, TR alternative}.
* **The T24 refutation** (S7.2): session 6's holder-supply chain is false in a reachable
  state, its obvious term-guarded repair is false too, and the corrected chain (T20,
  indexed by COMMITTED CONFIG) is written with the ghost it will need identified.
* **The crux VC's configuration lever, measured and negative** (S7.5).

**Open, and this is the complete list:**
1. `election_safety` @ `becomeLeader` — ⏱️ / OPEN in every cell of the grid.
2. `leader_completeness` @ `becomeLeader` — ❌ (the strict half; the clause chain that
   would close it is now T20, not T24).
3. `leader_completeness` @ `commitEntry` — ⏱️ / OPEN at 60 s (no counterexample survives).
4. `propose` — no criterion-(A) verdict (killed at 60 s, 20 s and 5 s per VC); covered by
   run 16 + the all-ten-action slices, i.e. by transfer.

# SESSION 8 (2026-07-27, opus) — THE GATE-2 DOSSIER: the T20 machinery, the crux VC's manual
# discharge, `propose` by slice

This is the closing driver session of bar 3. It is written as the **gate-2 dossier**: what is
CLOSED, what is CONDITIONAL and on exactly what, and what the SAFE verdict would quantify over.
Nothing here is self-ratified — the amendment below is what gate 2 is asked to rule on, and the
open items are stated as open.


## S8.0 TL;DR

* **The crux VC is DISCHARGED.** `election_safety` @ `becomeLeader` — ⏱️ in every cell of the
  measurement grid since run 14 — is now **fully green by a sorry-free hand proof** of the
  written truth argument (T8 + T12 + T17). The route sessions 11/12 named (`veil_smt` inside the
  interactive stub) is refuted by measurement; the route that works is Veil's own
  (`VerticalPaxosFirstOrder.lean`): `unveil`, `rcases`, term-level Lean.
* **T20's machinery is certified.** Four count-exempt ghosts (`cfgSeen`, `cfgPred`, `cfgQ`,
  `cfgBacked`) and seven clauses (T32–T37) — truth arguments written first — are inductive at
  INIT and at **all ten actions** (run 37, 110 ✅ / 0 ❌ / 0 ⏱️, 77 s), and green in the full
  bundle at `becomeLeader` (run 38) and `commitEntry` (run 41). Two further ghost-soundness
  clauses (T39/T40) were added in response to run 41's CTI and certified the same way (run 44),
  taking the bundle to **55 invariants + 2 safeties** at an unchanged 35/11.
* **P2's strict half narrowed, then REFUTED its own candidate clause — twice.** With T20's
  machinery in place the `becomeLeader` CTI survives only by denying that the intermediate
  config's proposal was authored by an E-holder, i.e. the residue looked like the single clause
  `T38 : (committed ∧ cfgCommitted D ∧ cfgLt commitCfgid D) → cfgBacked D`. Four probe runs later
  **T38 is REFUTED by a reachable trace** (the E-holder itself adopts a stale non-holder's
  proposal), and so is its corrected form `T43` (the all-holder conclusion stated directly on the
  frozen adopter quorum). Neither went into the bundle. The honest residue is therefore a
  cross-config holder-supply argument this plane does not yet carry — a WEAKER claim than
  "one clause away", and it is the one the evidence supports.
* **`propose` is certified 57 of 57 under amendment (B).** It has no criterion-(A) verdict and
  cannot get one on this box, but the wall is a BUNDLE-SIZE cliff between ~15 and ~17 clauses,
  not a solver-budget one — so nine ≤13-clause slices with recorded hypothesis sets cover every
  clause of the bundle at that action (union checked mechanically). The run-16 + ghost-extension
  transfer is no longer load-bearing there.
* **Bar 3 is not declared done.** This is gate 2's dossier, and the open list is complete.

## S8.1 What was added to the model, and why it is count-exempt

Session 7 left the strict half of P2 with a corrected but unmeasured chain (**T20**, the holder
supply indexed by COMMITTED CONFIG) and named its blocker: the state does not link a committed
config back to its proposer. This session added that link and the coupling's payload — **four
ghosts and seven clauses**, all read in NO `require` (mechanically checked:
`grep -nE '^\s*require .*(cfgSeen|cfgPred|cfgQ|cfgBacked)'` is empty), so the mechanical count
is **unchanged at 35 `require`s / 11 `assumption`s** and `QuorumAdjacency.lean` is untouched
(its seventeen-witness `#print axioms` audit still covers the bundle). Bundle:
46 → **55 invariants + 2 safeties** (seven for T20's machinery, two more — T39/T40 — for the ghost-soundness gap run 41's CTI named).

| ghost | written at | what it names |
|---|---|---|
| `cfgSeen : cfgid → Bool` | `propose` | this config has been proposed at least once |
| `cfgPred : cfgid → cfgid` | `propose` (before the `cfgOf i := d` move) | the config its proposer sat at — **the config→proposer link** |
| `cfgQ : cfgid → quorum` | `commitCfg` | the ADOPTER quorum that certified this config's commit |
| `cfgBacked : cfgid → Bool` | `commitCfg` | its proposal was authored by an E-HOLDER (`propAfterE` at the proposer) |

`cfgPred` is the load-bearing one and the reason a 12th `assumption` was NOT needed: the chain
axioms connect only UPWARD (`cfglt_connected` hands out `succ c`), so no VC can reach the
immediate PREDECESSOR of a config, which is exactly what P2's `becomeLeader` argument needs.
A downward-connectivity axiom would have been a model change requiring a gate and a new
witness; `propose`'s own `require succCfg (cfgOf i) d` already IS the fact, and the ghost
carries it.

Truth arguments **T32–T37** were written BEFORE the run that hunts their CTIs (gate-1c truth
rule); full text in the ledger.

## S8.2 The measurements — six runs, all quoted ⏱️-inclusive

| run | what | verdict | wall |
|---|---|---|---|
| **37** | `ReconfigCommitSMTT20Slice` — the T20 machinery, nine clauses, `#check_invariants` (all ten actions + init), 20 s/VC | **110 ✅ / 0 ❌ / 0 ⏱️** | 77 s |
| **38** | `ReconfigCommitSMTActbecomeLeaderS8` — the FULL enlarged bundle at `becomeLeader`, 60 s/VC | **54 ✅ / 1 ❌ / 1 ⏱️** | 770 s |
| **manual 1–3** | the interactive-stub route: `unveil` + `trace_state`; then `+ haves + veil_smt`; then the bare control `unveil; veil_smt` | `cannot translate Type` — **the named route is refuted** | 43 s each |
| **manual 4–5** | `ReconfigCommitSMTManual` — the eleven-clause election slice + a HAND proof of the crux VC | **12 ✅ / 0 ❌ / 0 ⏱️, EXIT=0, sorry-free** | 50 s |
| **42** | `ReconfigCommitSMTManualFull` — the same hand proof against the FULL bundle at `becomeLeader` | **55 ✅ / 1 ❌ / 0 ⏱️** — `election_safety` ✅ in the FULL bundle, criterion (A); the only non-green left at this action is `leader_completeness` | 709 s |
| **39/40** | `ReconfigCommitSMTPropSlice{A,B}` — `#check_action propose` by slice (criterion (B)) | KILLED at 1550 s, no verdict (30 clauses — over the cliff) / superseded by the ≤13-clause groups | — |
| **41** | `ReconfigCommitSMTActcommitEntryS8` — the full enlarged bundle at `commitEntry`, 300 s/VC | **55 ✅ / 1 ❌ / 0 ⏱️** (run 41, 300 s/VC) — the run-35 ⏱️ resolved to a CTI, which named two missing ghost-soundness clauses (T39/T40, now in the bundle); with them the CTI is gone and the VC is **⏱️ / OPEN** in a 35-clause slice at 60 s (run 45) and at 300 s (run 46) | 1912 s |

## S8.3 The closure criterion, as this session leaves it

The amendment stands as session 6 drafted it and sessions 7–8 exercised it, now reduced to
**(A) + (C) + (B) for `propose`**:

* **(A) Action-partitioned full-bundle verification — the primary criterion.** A
  `#check_action A` ✅ *is* the full-bundle verdict for that (clause, action) pair; the filter
  (`Elaborators.lean:421-465`, `Server.lean:34,49-59`) gates which VCs are STARTED and changes
  no VC's statement. **This identity claim is what gate 2 is asked to ratify.**
* **(B) Slice certification — the documented fallback**, now needed only for `propose` (and for
  the seven new clauses at the eight actions this session did not re-measure directly). Each
  slice-certified clause carries its run, its explicit hypothesis set (every member a clause of
  the full bundle), its log and its ⏱️-inclusive quote.
* **(C) The ⏱️ protocol.** Every banked run is grepped and quoted as "N ✅ / M ❌ / K ⏱️";
  a ⏱️ leaves its clause OPEN regardless of the tally.
* **(D) Ghost extension — still live, and now carrying one more instance.** Run 16's greens are
  quoted for clauses at actions the sweep did not re-measure; the bundle has since gained
  ghosts (`commitElecQuorum`, and this session's four). The claim is that a fresh state symbol
  occurring in neither the hypothesis nor the goal of a VC cannot invalidate that VC. It is
  believed sound, it has never been ratified, and every green resting on it is labelled
  `transfer: run-16 + ghost-extension`.
* **Init obligations need no transfer at all** (session 7, from the VC generator:
  `Induction.lean:132-143` gives an init VC the precondition `fun _ _ => True`, so no invariant
  of any bundle appears in it).
* **What a ✅ means** (session 7): each clause has two VCs (WP primary, TR alternative) and
  `effectiveStatus` reports the best of the two. A ✅ = "one of the two discharged".

## S8.4 THE GATE-2 DOSSIER — per-action coverage

Bundle at session end: **55 invariants + 2 safeties + `doesNotThrow`**, at **35 `require`s /
11 `assumption`s**. Ten actions plus the initialisation obligation. A fully green action
reports **58 ✅** (55 + 2 + `doesNotThrow`). (Runs 38/41/42 predate T39/T40 and were
measured on the 53 + 2 bundle — see S8.12; their tallies are quoted against that bundle.)

Two coverage layers, because the bundle grew this session:

1. **The 46 pre-session-8 invariants + the 2 safeties.** Their per-action verdicts are session
   7's sweep (nine actions under criterion (A); `propose` by transfer). Those greens SURVIVE
   the bundle extension: adding clauses only strengthens each VC's `Invariants` hypothesis
   (`Inv_new → Inv_old`), which is the same antecedent-weakening argument the arc has used
   throughout — plus ghost extension (D) for the four new ghosts, none of which occurs in any
   of those clauses.
2. **The 9 clauses added this session.** Certified at INIT and at **all ten actions** by run 37
   (criterion (B), hypothesis set recorded), and again at `becomeLeader` and `commitEntry` in
   the full bundle by runs 38/41 (criterion (A)).

| action | certifying run(s) | verdict | wall |
|---|---|---|---|
| `startElection` | `smt-act-startElection.log` (s7) | 49 ✅ / 0 ❌ / 0 ⏱️ | 212 s |
| `deliverRequestVoteGrant` | `smt-act-deliverRequestVoteGrant.log` (s7) | 49 ✅ / 0 ❌ / 0 ⏱️ | 502 s |
| `becomeLeader` | **run 38** then **run 42** (`smt-run38-actBL-T20.log`, `smt-run42-manualFull.log`; 53+2) | 54 ✅ / 1 ❌ / 1 ⏱️ → **55 ✅ / 1 ❌ / 0 ⏱️** (the ⏱️ closed by the hand proof) | 770 s / 709 s |
| `crashRestart` | `smt-act-crashRestart.log` (s7) | 49 ✅ / 0 ❌ / 0 ⏱️ | 703 s |
| `appendEntry` | `smt-act-appendEntry.log` (s7) | 49 ✅ / 0 ❌ / 0 ⏱️ | 835 s |
| `replicate` | `smt-act-replicate.log` (s7) | 49 ✅ / 0 ❌ / 0 ⏱️ | 1037 s |
| `commitEntry` | **run 41** (53+2, 300 s/VC) + runs 45/46 (slice) | **55 ✅ / 1 ❌ / 0 ⏱️** (run 41, 300 s/VC) — the run-35 ⏱️ resolved to a CTI, which named two missing ghost-soundness clauses (T39/T40, now in the bundle); with them the CTI is gone and the VC is **⏱️ / OPEN** in a 35-clause slice at 60 s (run 45) and at 300 s (run 46) | 1912 s |
| `commitCfg` | `smt-act-commitCfg.log` (s7) | 49 ✅ / 0 ❌ / 0 ⏱️ | 1641 s |
| `adopt` | `smt-act-adopt-t20.log` (s7, 20 s/VC) | 49 ✅ / 0 ❌ / 0 ⏱️ | 1407 s |
| `propose` | **runs 37/39/40** (slices, criterion (B)) + run 16 | **all 57 clauses ✅ under criterion (B)** — runs 37/44 + `PropG2`–`PropG9`, union checked 57/57 | — |
| INIT | runs 16/20/26/27/31/34/37 | ✅ for every clause; **no transfer needed** (`Induction.lean:132-143`) | — |

## S8.5 Truth-argument inventory (T1–T40)

Every clause in the bundle carries a written truth argument, per the gate-1c open-clause truth
rule. The inventory below is the complete list, with the run that machine-certified it.

| # | clause | status |
|---|---|---|
| T1 | `reach_quorum_below` | ✅ inductive, full bundle (run 13 on) |
| T2 | `elecq_witness` | ✅ (run 14 on) |
| T3 | `elecq_grant_covers_reach` | ✅ (run 14 on) |
| T4 | `cand_reach_strict` | ✅ (run 14 on) |
| T5 | `voteterm_bounded` | ✅ (run 14 on) |
| T6 | `commit_leader_self_vote` | ✅ (run 14 on) |
| T7 | `commit_leader_no_foreign_grant` | ✅ (run 14 on) |
| T8 | `election_safety` (the crux argument) | **FULLY GREEN — DISCHARGED.** The six-session ⏱️ at `becomeLeader` is closed by the sorry-free hand proof (`ReconfigCommitSMTManual.lean`, item 59), whose statement is **byte-identical to the Veil-emitted stub** and whose axiom profile is `[propext, Classical.choice, Quot.sound]` — kernel-checked, no `veil.smt.trust` (gate-2 R1). Run 42 (full bundle, criterion (A)): **55 ✅ / 1 ❌ / 0 ⏱️** |
| T9 | `leader_completeness` (P2) at `becomeLeader` | same-term half CLOSED (T18/T13/T14); **strict half OPEN** |
| T10 | `role_positive_term` | WITHDRAWN (tractability); subsumed by T12 |
| T11 | `commit_leader_at_commit_cfg` | ✅ (run 16 on) |
| T12 | `leader_reach_strict` | ✅ run 20 (110 ✅ / 0 ❌ / 0 ⏱️) |
| T13 | `commitq_grant_covers_reach` | ✅ run 34 (250 ✅ / 3 ❌ / 0 ⏱️, artifacts named) |
| T14 | `commit_leader_frozen_reach` | ✅ run 31 (195 ✅ / 3 ❌ / 0 ⏱️) |
| T15 | `role_below_quorum_strict` | ✅ run 26 (130 ✅ / 2 ❌ / 0 ⏱️) |
| T17 | `role_below_meets_quorum` | ✅ run 27 (141 ✅ / 2 ❌ / 0 ⏱️) |
| T18 | `commitq_witness` | ✅ run 31 |
| T19 | **refutation** of T24 | banked (session 7) — T24 is false in a reachable state |
| T20 | the corrected holder-supply chain | machinery CERTIFIED (T32–T37, run 37); **final step OPEN** (S8.7) |
| T23/T24/T27 | session 6's holder-supply map | SUPERSEDED by T19/T20 |
| T32 | `cfgpred_succ` | ✅ run 37 (110 ✅ / 0 ❌ / 0 ⏱️) |
| T33 | `proposal_seen` | ✅ run 37 |
| T34 | `cfg_seen_adopted` | ✅ run 37 |
| T35 | `cfg_seen_committed` | ✅ run 37 |
| T36 | `adopted_holds` | ✅ run 37 |
| T37 | `cfgq_witness` + `cfgq_holders` | ✅ run 37 |
| T39 | `propafter_holds` (`propAfterE I → holdsE I`) — *truth:* `propAfterE` is written ONLY at `propose i`, as `propAfterE i := holdsE i`, and `holdsE` is MONOTONE (no action clears it), so a node whose `propAfterE` is set has held E ever since (argument written in ledger item 64's adjudication) | ✅ run 44 (44 ✅ / 0 ❌ / 0 ⏱️, 51 s — all ten actions + init); ✅ in the full bundle at `commitEntry` by construction of the CTI it excludes |
| T40 | `cfgbacked_committed` (`cfgBacked D → cfgCommitted D`) — *truth:* `cfgBacked (proposedC i)` and `cfgCommitted (proposedC i)` are written in the SAME `commitCfg` step, and neither is ever cleared (argument written in ledger item 64's adjudication) | ✅ run 44 (same run/tally) |

*Not in the bundle, recorded so the inventory is complete:* **T38** (`… → cfgBacked D`) and
**T43** (`cfgq_holders_above`) are both **REFUTED by reachable traces** (ledger items 68 and 69) —
they were probe-slice experiments and never entered `ReconfigCommitSMT.lean`; **T41**
(`isCommitLeader ∧ hasProposal ∧ cfgLt commitCfgid (proposedC I) → propAfterE I`) is certified
inside the probe slice only (run 47) and is likewise not a bundle clause.

## S8.6 Conditionality — what a SAFE verdict from this model would and would not mean

Unchanged from gate 1 and carried verbatim in both model headers; it is part of any claim:

* **(n1)** the config-currency grant guard (like the pre-existing E-guard) is STRONGER than real
  `log_ok`, which would grant to a candidate that lacks the voter's entries but carries a higher
  `last_term` on a divergent branch. Deliberate: faithful `log_ok` in a one-tracked-entry plane
  makes the Figure-8 grant model-legal, and that class is banked in `Figure8.lean`/`Finding9.lean`.
* **(n2)** `cfgOf` conflates HOLDING a config entry with having ADOPTED it (real UC grants where
  this model refuses: `election.rs:889-899`). P2-benign under the contiguity boundary, but a
  distinct exclusion.
* **(n3)** truncation-revert / config-branch exclusion: MODEL-EDIT-2c makes adoption
  forward-only; real UC reverts at `election.rs:703-748`. Those are exactly the config-BRANCH
  states, which MODEL-EDIT-2b's linearity assumption also excludes.
* **gate amendment (d), the corner that belongs in the final claim verbatim:** `commit_seen` is
  not reset at `become_leader` (`election.rs:1040-1056`), so a fresh leader carries commit state
  inherited from its follower period; that inherited value cannot satisfy the serving latch
  (`:522-527`) without a pre-existing completeness violation — the same conditionality bucket
  as (n1).
* **The standing conditionality:** any SAFE verdict here is conditional on the
  canonical-prefix/contiguity discipline (the Q2 chain, CONFIRMED-SAFE in Rust) and on the
  data-plane freshness / Finding-#6b `new_term_pos` clamp (proved at the Lean tier). It is
  never an unconditional claim. (Precedent: the LC arc's `FramesCurrentAuthored`.)

**Twin divergences (d1)–(d5)** — the explicit-state twin (`ReconfigCommit.lean`) deliberately
does NOT mirror five SMT-only mechanisms: (d1) EDIT-1 own-term-stamped reports, (d2) EDIT-2b
linear config history, (d3) EDIT-2c forward-only adoption, (d4) EDIT-3 cluster-wide
one-in-flight, (d5) EDIT-4's own-term commit-view gate. The twin therefore over-approximates
and its calibration CE remains the coarser instrument. Complete and unchanged.

**What the verdict quantifies over.** With abstract quorums and the eleven assumptions (all
discharged against the two `QuorumAdjacency.lean` witnesses, seventeen `#print axioms` entries),
a fully green bundle would be an **all-n inductive invariance proof** of the model — not a
bounded search. It says nothing about UC beyond the model, and `proofs/` remains the sole
trusted base; nothing in `proofs-veil/` is the record.

## S8.7 THE OPEN-ITEM LIST — complete, each with its named condition

| # | item | status | carried on |
|---|---|---|---|
| 1 | `election_safety` @ `becomeLeader` | **CLOSED — FULLY GREEN.** No longer an open item: the ⏱️ that stood in every cell of {full bundle, 17-clause slice, 11-clause slice} × {60, 300, 900 s} × {fmf on, off} is **discharged by the sorry-free hand proof** (`ReconfigCommitSMTManual.lean`, item 59), statement **byte-identical to the Veil-emitted stub**, axiom profile `[propext, Classical.choice, Quot.sound]` — kernel-checked, carrying no `veil.smt.trust` (gate-2 R1). Run 42 in the FULL bundle, criterion (A): **55 ✅ / 1 ❌ / 0 ⏱️** | nothing conditional at this VC beyond the model's standing conditionality (S8.6): it is a proof, not a solver verdict |
| 2 | `leader_completeness` @ `becomeLeader` (strict half) | **OPEN (❌)**, CTI adjudicated as a model artifact with an unreachable pre-state (items 42/45/58) | **NO written model-level truth argument is on file.** T20's machinery is certified (T32–T37) but its last link is open; **item 62's semantic argument died with T38**, and both T38 and its corrected form T43 are **refuted by reachable traces** (ledger 68/69). What the item is carried on is therefore *not* an argument but: interpretation-level CTI adjudications (each `becomeLeader` CTI shown unreachable by hand) + the Rust-side Q2 contiguity chain. The next session starts from the refutations, not from a chain |
| 3 | `leader_completeness` @ `commitEntry` | **OPEN (⏱️)** — run 41 (full bundle, 300 s) gave a CTI; T39/T40 exclude it; runs 45/46 (35-clause slice, 60 s and 300 s) leave the VC ⏱️ with no counterexample | **no surviving counterexample at 300 s** — an absence, not a proof. The T9/T10 sketch is not a completed model-level argument for this VC |
| 4 | `propose` under criterion (A) | **OPEN** — VC-GENERATION-walled (killed at 60 s, 20 s, 5 s per VC; a 12× budget span moved nothing) | criterion **(B)**: slices with recorded hypothesis sets (runs 37/39/40) + run 16 for the run-16-era clauses |
| 5 | amendment clauses (A) and (D) | **UNRATIFIED** — gate 2's decision | (A) is an identity claim about `#check_action`, evidenced from the Veil source; (D) is the ghost-extension transfer |

Nothing else is open. Every other (clause, action) pair in the bundle has a ✅, and every ✅
quoted in this dossier comes from a run whose ⏱️ count is quoted with it.

**HONESTY NOTE on P2's strict half (gate-2 R3), which governs every statement of it above.**
Session 8 narrowed the strict half to one clause and then refuted that clause — twice. Item 62's
semantic argument for T38 was the last *written* model-level truth argument for the strict half,
and it **died with T38** (ledger 68); the corrected all-holder form T43 died with it (ledger 69).
So: **the strict half of `leader_completeness` has NO complete written model-level truth argument
on file.** It is carried on exactly three things, none of which is an argument about the model's
reachable states in the sense the truth rule requires — (i) the absence of any surviving
counterexample (300 s at `commitEntry`), (ii) interpretation-level adjudications of the CTIs that
were produced (each shown unreachable by hand, one CTI at a time), and (iii) the Rust-side Q2
contiguity chain, which is evidence about UC, not about this model. Any wording that suggests
the residue is "one clause away", or that a written chain merely awaits mechanisation, is
superseded by this note.

## S8.8 Disposition — STOP for gate 2

Bar 3 is NOT declared done. The dossier above is assembled to the standard the closing session
was asked for: every item is either fully closed or explicitly conditional with its condition
named. What gate 2 is asked to decide:

1. **Ratify or reject amendment (A)** (the `#check_action` identity claim) and **(D)** (ghost
   extension). Everything else in the criterion is already settled by the source.
2. **Rule on the residue**: whether an arc that closes 53 invariants + one safety at every
   action, with the second safety open at ONE action on a written argument and the first open
   at one action on a mapped clause chain, is banked as-is or pushed further.
3. If pushed: the named moves are (i) the HAND-PROOF route (item 59) applied to
   `leader_completeness` @ `commitEntry`, whose VC has no surviving counterexample at 300 s, and
   (ii) a fresh holder-supply argument for the strict half — NOT T38/T43, both refuted.
   **MODEL-EDIT-5 remains PREPARED, NOT REQUESTED**; nothing this session strengthens the case
   for it.

`ReconfigCommitSMT.lean` ends the session at **35 `require`s / 11 `assumption`s**,
`QuorumAdjacency.lean` untouched, its seventeen-witness `#print axioms` audit still covering
the bundle. Everything added is ghost-and-clause only. **Never `proofs/`; nothing here is the
record.**

## S8.9 Reproduction

```
cd /home/claude/veil-spike/veil-preview
# the bundle, per action (criterion A):
python3 /home/claude/veil-spike/runs/make_act.py becomeLeader 60 S8
bash /home/claude/veil-spike/runs/runmod.sh ReconfigCommitSMTActbecomeLeaderS8 <log>
# a slice (criterion B) — model verbatim, invariant conjunction cut, hypothesis set explicit:
python3 /home/claude/veil-spike/runs/make_slice.py <Module> <per-VC budget> {inv|<action>} <clause>...
bash /home/claude/veil-spike/runs/runmod.sh <Module> <log>
```
Both generators emit the model VERBATIM apart from the module name, the file-scope
`veil.smt.timeout` and the check command; each launch in this session was diffed against
`ReconfigCommitSMT.lean` and the 35/11 count re-verified mechanically before the run.
ONE Lean process at a time, `memwatch.sh` armed (2.5 GB floor).

## S8.10 `propose` — the (B) route, and the scaling law that makes it work

`propose` has no criterion-(A) verdict and cannot get one on this box: the full bundle was
killed at 60 s, 20 s AND 5 s per VC (session 7, item 53), so the SOLVER budget is not the lever.
This session measured the other axis — **bundle size** — and it is decisive:

| slice size | `#check_action propose` | wall |
|---|---|---|
| 9 clauses (run 37; it ran all TEN actions, not just `propose`) | **110 ✅ / 0 ❌ / 0 ⏱️** | **77 s** |
| 10–13 clauses (`PropG2`–`PropG7`, six runs) | 55 ✅ / 5 ❌ / 0 ⏱️ | 55–68 s each |
| 15 clauses (`PropG3b`) | 14 ✅ / 2 ❌ / 0 ⏱️ | 68 s |
| 17 clauses (`PropG4b`) | **KILLED, no verdict** | >380 s |
| 30 clauses (`PropSliceA`) | **KILLED, no verdict** | >1550 s |
| 55 clauses (full bundle) | **KILLED ×3** | >3300 / >1900 / >2100 s |

**There is a cliff between ~15 and ~17 clauses at this action, not a gradient.** The practical
rule for extending this bundle: at `propose`, certify in groups of ≤ 13.

**Coverage achieved.** Every slice is the model VERBATIM with the conjunction cut to a recorded
set, all of whose members are clauses of the full bundle, so each ✅ transfers by antecedent
weakening. Taking the union of the greens over runs 37 and `PropG2`–`PropG9`:

* ****all 57** of the 55 clauses have a criterion-(B) `propose` verdict with an explicit
  hypothesis set** — including BOTH safeties (`PropG6`: `election_safety` and
  `leader_completeness` are ✅ at `propose`).
* Nothing is left resting on the run-16 + ghost-extension transfer at this action: the union of slice greens was checked mechanically at **57 of 57**.
* The five ❌ in the first six groups are ordinary SLICE ARTIFACTS — each of those clauses is ✅
  at `propose` in ANOTHER group that carries its supports (`cand_cfg_frozen` in `PropG3`,
  `adopted_reach_bound` in `PropG2`), or in run 16's full bundle. **A slice ❌ transfers in
  neither direction** (amendment (B)), and none of them is quoted as a verdict.

Hypothesis sets, per run, are recorded in the ledger (item 66) and are reproducible from the
generator invocation: `make_slice.py <Module> 20 propose <clause>...`.


## S8.12 One bookkeeping note the gate must see

Runs 38/41/42 measured the bundle at **53 invariants + 2 safeties**. T39/T40 were added AFTER
them, in response to run 41's CTI, taking the bundle to **55 + 2**. Those three runs' greens
therefore carry the same antecedent-weakening transfer the rest of the arc uses (`Inv_new →
Inv_old`), and T39/T40 have their own verdicts at all ten actions + init from **run 44**
(44 ✅ / 0 ❌ / 0 ⏱️, 51 s, hypothesis set `propafter_holds`, `cfgbacked_committed`,
`pending_iff_proposal`). Nothing else in the dossier is affected, and the count is still
**35 `require`s / 11 `assumption`s**.

# GATE 2 VERDICT — the citable claim

Gate 2 convened 2026-07-27 on the dossier above (sections S8.0–S8.12) and on ledger session 13
(items 56–68). **Verdict: BANK.** The paragraph below is the gate's R6 output and is **the only
form in which this arc's result may be cited**; it is reproduced verbatim, and it is to be quoted
whole — the conditions are inseparable from the claim.

> **What is proved.** Over the abstract-quorum commit-plane model
> `proofs-veil/models/ReconfigCommitSMT.lean` (35 `require`s / 11 `assumption`s, all eleven
> assumptions discharged against the chain-indexed witnesses in `QuorumAdjacency.lean`, seventeen
> `#print axioms` entries clean), the bundle of 55 invariants is inductively invariant, all-n — a
> proof, not a bounded search — and `election_safety` holds across live single-server
> reconfiguration with a commit/log plane: inductive at every action, with the crux VC
> (`becomeLeader`) discharged by a sorry-free hand proof whose statement is byte-identical to
> Veil's generated obligation and whose axiom profile is `[propext, Classical.choice, Quot.sound]`
> (kernel-checked; solver verdicts elsewhere carry `veil.smt.trust`). Coverage is criterion (A)
> (full-bundle `#check_action`) at nine of ten actions and criterion (B) (verbatim-model slices
> with recorded hypothesis sets, union mechanically 57/57) at `propose`; init obligations are
> bundle-independent by the VC generator. **What is conditional.** `leader_completeness` (P2: a
> leader at a term ≥ the commit term holds the tracked entry) is proved preserved at eight actions
> + init + `propose`, and remains OPEN at `becomeLeader` (counterexamples adjudicated unreachable;
> the invariant route through proposal-backing is closed — T24, T38, T43 all refuted in reachable
> states) and at `commitEntry` (timeout with no surviving counterexample at 300 s). This residue is
> carried on the absence of any surviving counterexample and on the Rust-side Q2 contiguity chain —
> not on a completed model-level argument. **Conditions, verbatim and inseparable from the claim:**
> (n1) the config-currency grant guard, like the E-guard, is stronger than real `log_ok` (divergent
> higher-`last_term` grants excluded; the Figure-8 class is banked in
> `Figure8.lean`/`Finding9.lean`); (n2) `cfgOf` conflates holding a config entry with having
> adopted it (real UC grants where the model refuses, `election.rs:889-899`); (n3)
> truncation-revert/config-branch states are excluded (forward-only adoption + linear history; real
> UC reverts at `election.rs:703-748`); gate-1c corner (d): `commit_seen` is not reset at
> `become_leader` (`election.rs:1040-1056`), so a fresh leader carries follower-period commit state,
> which cannot satisfy the serving latch (`:522-527`) without a pre-existing completeness violation
> — the same conditionality bucket as (n1). Any SAFE verdict is conditional on the
> canonical-prefix/contiguity discipline (Q2 chain, CONFIRMED-SAFE in Rust, gate doc §5) and the
> data-plane freshness / Finding-#6b `new_term_pos` clamp (proved at the Lean tier); it is never
> unconditional. The explicit-state twin diverges by (d1) own-term-stamped reports, (d2) linear
> config history, (d3) forward-only adoption, (d4) cluster-wide one-in-flight, (d5) the own-term
> commit-view gate — it over-approximates, and its calibration CE is the coarser instrument.
> **Boundary:** the report plane is collapsed by construction (counting toward E's commit ⟹ holding
> E), so this arc can never re-find #5/#9-class report bugs — by design, that class stays banked in
> `BootGate.lean`/`Finding9.lean`; the below-floor/snapshot path is argued, not checked; the adopt
> window closes at `commitCfg`. This model says nothing about UC beyond it; `proofs-veil/` is never
> the record and `proofs/` remains the sole trusted base.

## Gate-2 rulings, in summary

| ruling | disposition |
|---|---|
| **(A)** action-partitioned full-bundle verification (the `#check_action` identity claim) | **RATIFIED** |
| **(B)** slice certification with recorded hypothesis sets | **RATIFIED** |
| **(C)** the ⏱️ protocol (a ⏱️ leaves its clause OPEN regardless of the tally) | **RATIFIED** |
| **(D)** ghost extension (a fresh state symbol in neither hypothesis nor goal cannot invalidate a VC) | **RATIFIED, CONDITIONALLY** — conditional on the two mechanical side-conditions being **re-verified on every future ghost addition** (the symbol occurs in no `require`, and in neither the hypothesis nor the goal of the transferred VC). Greens resting on it **keep their `transfer:` label**; the label is not to be dropped |
| **residue** (P2 open at `becomeLeader` and `commitEntry`) | **BANK.** The arc is not pushed further on the strict half |
| **optional coda** | the hand-proof route (item 59) applied to **P2 @ `commitEntry` ONLY**, in a **single hard-timeboxed session**. Nothing else. **The strict half is not to be reopened in this arc** |

**R1 strengthening, recorded.** The crux VC's discharge is stronger than the dossier originally
claimed: `#print axioms` on the hand proof reports `[propext, Classical.choice, Quot.sound]` — the
plain Lean axiom set, with **no `veil.smt.trust`**. `election_safety` @ `becomeLeader` is therefore
**kernel-checked**, not solver-trusted, and it is the one VC in the bundle with that status.

**Bookkeeping nit, recorded.** Item 59 cites five manual attempts (`smt-manual1-trace.log` …
`smt-manual5.log`); only **two** are banked in `proofs-veil/logs/` — `smt-manual3-control.log` (the
`unveil; veil_smt` control that produced `cannot translate Type`) and `smt-manual5.log` (the final
sorry-free run). The three intermediate attempt logs were not retained. The claims they support
(the tooling correction and the anti-vacuity signals) are reproducible from the two banked logs
plus the model files, so this is a completeness nit, not a defect in the result.

**Defects found by gate 2, all narrative-layer, all corrected in this document** (no model change,
no re-run): (1) S8.5/S8.7 still described the crux VC as OPEN/CONDITIONAL, contradicting S8.0,
S8.2, S8.4, S8.8 and run 42 — corrected; (2) S8.4's "56 ✅" was stale post-T39/T40 — corrected to
58; (3) the S8.5 inventory stopped at T37 and the T39/T40 truth arguments lived only inside ledger
item 64's adjudication text — lifted into the inventory. Plus the R3 honesty correction on P2's
strict half (S8.7) and the two ledger repairs (ledger items 68 and 69).

**Arc status: BANKED, pending the user's merge decision.** Nothing in `proofs-veil/` is the
record; `proofs/` was never touched by this arc and remains the sole trusted base.

# CODA (post-gate-2, 2026-07-28) — P2 @ `commitEntry`: **NOT CLOSED**, and the residue identified

This is the single hard-timeboxed coda session gate 2 sanctioned (ruling: "the hand-proof route
(item 59) applied to **P2 @ `commitEntry` ONLY**"). **The GATE 2 VERDICT above is UNCHANGED and
is still the only citable form of this arc's result**: nothing here closes a VC, nothing here
amends the claim, no clause and no ghost was added, `ReconfigCommitSMT.lean` is untouched at
`ae3c67b` (re-verified before the first run: **35 `require`s / 11 `assumption`s, 55 invariants +
2 safeties**). The strict half at `becomeLeader` was not reopened.

**Outcome: the proof did not land — and the reason is now a fact, not a timeout.** The hand proof
reduces `leader_completeness` @ `commitEntry` to **exactly one case**, and that case is the
**cross-config holder supply** — the same residue the dossier's S8.7 honesty note records for P2's
strict half at `becomeLeader`. The two open items are therefore **one item**, not two.

## C.1 What was done

| # | run | what | verdict | wall |
|---|---|---|---|---|
| coda-1 | `smt-coda1-stub.log` | `P2CEManual`, 12-clause slice, `#check_action commitEntry`, 60 s/VC | **12 ✅ / 1 ❌ / 0 ⏱️** — `leader_completeness` ❌ | 139 s |
| coda-2 | `smt-coda2-probe17.log` | `P2CEProbe2`, 17 clauses (+`committed_cfg_quorum`, `chain_committed_below`, `no_stale_election`, `cfgq_witness`, `leader_reach_strict`) | **17 ✅ / 0 ❌ / 1 ⏱️** — the coda-1 CTI is killed; the VC is ⏱️ and **the stub is emitted** | 167 s |
| coda-3 | `smt-coda3-stubCE.log` | the same 17-clause slice under the proof module name `ReconfigCommitSMTManualCE` — **STUB ELICITATION** | 17 ✅ / 0 ❌ / 1 ⏱️, stub emitted | 166 s |
| coda-4 | `smt-coda4-probe-trace.log` | the stub statement + `unveil; rcases; trace_state; sorry` — **the ANTI-VACUITY CONTROL** | **💥** `interactive proof ... contains 'sorry'` — the theorem IS consumed for this VC | 63 s |
| coda-5 | `smt-coda5-proof1.log` | the **partial hand proof**: same-term half + all three config cases except one | **💥** (one `sorry`), **zero tactic errors** — every other case elaborates | 65 s |
| coda-6 | `smt-coda6-probe24.log` | `P2CEProbe3`, 24 clauses (+ the whole T20 machinery: `cfgpred_succ`, `cfg_seen_adopted`, `cfg_seen_committed`, `adopted_holds`, `cfgq_holders`, `propafter_holds`, `cfgbacked_committed`) | **24 ✅ / 0 ❌ / 1 ⏱️** — still no counterexample, still no proof | 221 s |

Method as gate 2 required: the stub was **elicited** (coda-3), the sorry-version was **rejected
with 💥** (coda-4 — the anti-vacuity control, banked), and the theorem statement in
`proofs-veil/models/ReconfigCommitSMTManualCE.lean` is **byte-identical (whitespace-normalised)
to the Veil-emitted stub** (mechanically diffed). Every slice is `ReconfigCommitSMT.lean`
VERBATIM with the invariant conjunction cut (`make_slice.py`), 35/11 re-verified.

## C.2 The proof, and exactly where it sticks

After `unveil` the obligation is (verbatim from coda-4's `trace_state`):

```
st.leader i = true → st.committed = false → st.holdsE i = true →
  th.quorumOf q (st.cfgOf i) = true →
  (∀ V, th.qmember V q = true → st.holdsE V = true) →
  (∀ V, th.qmember V q = true → le (st.gotEAt V) (st.curTerm i)) →
  ∀ L, st.leader L = true → le (st.curTerm i) (st.curTerm L) → st.holdsE L = true
```

Note `st.committed = false` in the pre-state: **P2 itself is vacuous as a pre-state hypothesis at
this action**, and so is every `committed`- or `isCommitLeader`-gated clause in the bundle
(`commit_backed`, `commit_quorum_sound`, `commit_term_bound`, `commit_leader_evidence`,
`commitq_gotE`, `commitq_witness`, `commitq_grant_covers_reach`, `commit_leader_*`). The
frozen-commit-leadership machinery (T18/T13/T14) is therefore **unavailable at `commitEntry`** —
a structural difference from `becomeLeader` that the arc had not previously recorded.

**PROVED (sorry-free, zero tactic errors):**

* **Same-term half** (`curTerm L = curTerm i`) — pre-state `election_safety` collapses `L` to the
  committing leader, which holds E by `commitEntry`'s own `require holdsE i`. (This is T9's
  same-term case, but it closes here through `election_safety` rather than through
  `commit_leader_evidence`, which is vacuous.)
* **The finisher** — any E-holder `V` in `elecQuorum L` with `gotEAt V ≤ curTerm i` transports E
  to `L`: either `V = L`, or `V` granted to `L` at `curTerm L > curTerm i ≥ gotEAt V` and
  `holder_grants_are_covered` fires.
* **Case (a), `elecCfg L = cfgOf i`** — `same_cfg_quorum_intersection` on `q` and `elecQuorum L`.
* **Case (b), `elecCfg L` strictly BELOW `cfgOf i`** — impossible. `cfglt_connected` gives a
  succ-step; the adjacent sub-case meets the quorums directly, and for a gap,
  `reach_quorum_below` at `(E, cfgOf i, i)` supplies a quorum of `E` whose members reached `E` no
  later than `i` reached its own config, `reach_bound` bounds that by `curTerm i < curTerm L`,
  adjacency meets it against `elecQuorum L`, and `elecq_grant_covers_reach` then forces
  `¬ cfgLt (elecCfg L) E`, contradicting `succ_cfglt`.
* **Case (c) ADJACENT, `succCfg (cfgOf i) (elecCfg L)`** — `adjacent_cfg_quorum_intersection`
  bridges `q` to `elecQuorum L`.

**THE ONE OPEN CASE — case (c) with a GAP:** `cfgLt (cfgOf i) E` and `cfgLt E (elecCfg L)` where
`succCfg (cfgOf i) E`, i.e. **`elecCfg L` is two or more `succCfg` steps above `cfgOf i`**. The
single `sorry` in the file is the named `have`:

```
holder_supply : ∃ V, th.qmember V (st.elecQuorum L) = true ∧ st.holdsE V = true ∧
                     le (st.gotEAt V) (st.curTerm i)
```

## C.3 Why it does not close — stated as an argument, not as a timeout

1. `holdsE L` can be concluded in exactly two ways in this bundle: `L` is itself a member of the
   all-holder quorum `q`, or a **holder granted to `L`** (`holder_grants_are_covered`). Both need
   a holder inside `elecQuorum L`.
2. The only all-holder set the pre-state names is `q`, a quorum of `cfgOf i`.
   `adjacent_cfg_quorum_intersection` bridges **exactly one** `succCfg` step, so it reaches
   quorums of `succ (cfgOf i)` and no further.
3. The intermediate config `E` **is** committed (`chain_committed_below` via `eleccfg_not_ahead`,
   with `E = genesisC` excluded by `genesis_least`), and `committed_cfg_quorum` / `cfgq_witness`
   name a quorum of it — but adjacency puts only **ONE** holder in that quorum, and one holder
   does not license the next step. Repeating the step needs the **all-holder** form, which is
   `cfgq_holders` gated on `cfgBacked E` — i.e. **T38**, refuted in a reachable state (ledger 68)
   — or its direct form **T43**, likewise refuted (ledger 69). Coda-6 measured this: adding the
   entire T20 machinery changes nothing.
4. `no_stale_election` at `(i, E)` is the natural exclusion of the gap, and it **fails in the
   wrong direction**: since `elecCfg i ≤ cfgOf i < E`, the clause yields not a contradiction but
   `tot.le (curTerm i) (cfgCommitTerm E)`. No clause in the bundle bounds `cfgCommitTerm` from
   above.
5. Walking the chain from `cfgOf i` up to `elecCfg L` step by step is not available: `cfgLt` has
   **no well-foundedness axiom**, so there is no induction along the config chain in this
   fragment. Adding one would be a 12th `assumption` — a model change, a gate, and a new
   `QuorumAdjacency.lean` witness. **Not requested.**

**So the residue at `commitEntry` is the cross-config HOLDER SUPPLY — the same residue as P2's
strict half at `becomeLeader`.** Gate 2's open items 2 and 3 are one item.

## C.4 What this changes, and what it does not

* **It does not change the citable claim.** P2 remains OPEN at `becomeLeader` (❌) and at
  `commitEntry` (⏱️); the conditionality paragraph is unchanged and remains the only citable form.
* **It sharpens the honesty note (S8.7).** S8.7 recorded `commitEntry` as carried on "no surviving
  counterexample at 300 s — an absence, not a proof". That absence is now **explained**: the VC is
  not solver-hard by accident, it needs a fact the bundle does not contain. The next reader should
  not expect a bigger budget, a different solver configuration, or another hand-proof iteration to
  close it.
* **It retires one of gate 2's two "if pushed" moves.** Gate 2 named (i) the hand-proof route at
  `commitEntry` and (ii) a fresh holder-supply argument for the strict half. Move (i) is now
  **executed and exhausted**: it lands three of four cases and then arrives at move (ii). There is
  one piece of work left in this arc, not two.
* **Cost model, corrected.** Item 59 recorded "a ⏱️ is no longer a wall, it is a proof
  obligation". That is right, and the coda adds the other half: a hand proof can also **discharge
  the question without discharging the VC** — here at a cost of six runs and ~13 minutes of solver
  wall, which is what turned a six-month-shaped unknown into a named missing lemma.

## C.5 Artifacts

`proofs-veil/models/ReconfigCommitSMTManualCE.lean` (the partial proof — **one** `sorry`,
mechanically counted, at the named `holder_supply`), `proofs-veil/models/P2CEProbe3.lean` (the
24-clause probe), and the six logs `proofs-veil/logs/smt-coda{1..6}-*.log`. Reproduction:

```
cd /home/claude/veil-spike/veil-preview
python3 /home/claude/veil-spike/runs/make_slice.py ReconfigCommitSMTManualCE 60 commitEntry <17 clauses>
python3 /home/claude/veil-spike/runs/coda_insert.py ReconfigCommitSMTManualCE <proof-body>
bash /home/claude/veil-spike/runs/runmod.sh ReconfigCommitSMTManualCE <log>
```

**Never `proofs/`; nothing in `proofs-veil/` is the record. The GATE 2 VERDICT above stands
verbatim and unamended.**
