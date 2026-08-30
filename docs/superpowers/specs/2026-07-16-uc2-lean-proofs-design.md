# UC v2 Lean formal-verification design — the `uc_consensus` safety core

**Date:** 2026-07-16
**Status:** approved design (brainstormed; supersedes nothing — first formal-verification effort in this repo)
**Prerequisites:** the v2 design spec (`2026-07-09-uc-v2-aeron-shaped-smr-design.md`, esp. §6 commit and §8 invariants), the elle gate (`docs/benchmarks/uc2-elle-gate-2026-07-16.md`)

## 1. Purpose and stance

Add machine-checked proofs, in Lean 4, of the safety arguments at the heart of
`uc_consensus` — phased so the effort earns its keep twice:

1. **Bug-hunting first.** UC is not textbook Raft: byte positions instead of
   indices, `(term, base)` boundary maps instead of per-entry terms, commit as
   an order statistic over durables bounded by own, data-stamped term
   recording. No existing mechanized Raft proof covers this formulation. Model
   the novel mechanisms and push proofs through them; a stuck proof is a
   candidate spec gap or bug, which is the point.
2. **Assurance artifact second.** The surviving proofs become a permanent,
   CI-checked artifact alongside the sim, the lincheck capstones, the
   crashtest, and elle — a fourth correctness level.

Model↔code linkage is **executable model + conformance fuzz** (§5), upgraded
to **proved equivalence via Aeneas** for `reconcile` if the gated Phase 1.5
experiment (§6) lands.

## 2. Scope

### In scope (Phase 1 — committed)

The three pure kernels of `uc_consensus`, modeled as executable Lean 4
definitions, plus the semantic ground they stand on:

- **`Commit`** — `CommitTracker` (`src/commit.rs`) as a fold over
  `on_durable` / `reset_reports` / `advance` events.
- **`Reconcile`** — `reconcile(own, own_durable, leader)`
  (`src/reconcile.rs`) ported 1:1: common-prefix scan, the own-side and
  leader-side clamps, the phantom-dropping map filter.
- **`Vote`** — `log_ok` (lexicographic `(last_term, last_durable)`,
  `src/election.rs`) plus the `voted_for` single-vote-per-term discipline as a
  tiny per-node state machine.
- **`ByteHistory`** — a log as a function `pos → term-stamp`; the ascending
  `(term, base)` map is an encoding of it. "Content identity at `p`" is
  `term_at` equality — exactly the sim's `term_at` oracle (spec §6: within a
  term, bytes are cluster-identical). This is what lets the deep `Reconcile`
  theorems say "truncated bytes are genuinely divergent", and it is the
  vocabulary Tier B reuses.

### In scope (Phase 2 — spike committed, remainder gated)

An N-node protocol model and the distributed safety theorems (§7).

### Explicitly out of scope

- The full `ElectionSm::step()` wiring (in-flight event gating, action
  emission order, cnc counters) — covered by the sim, not the prover.
- Everything outside `uc_consensus`: persistence, transport, log buffer,
  read barrier, snapshots (`uc_node` / `uc_net` / `uc_log`).
- M7 reconfig (single-server change, tombstones, config chaining — the sim's
  inv6–9): **Tier C, deferred**; listed as future work, unscoped.
- Liveness (elections terminate, commits advance) — safety only.
- Byzantine faults. The fault model matches the sim: message loss,
  duplication, reordering; crash-restart preserving durable state only.

## 3. Repo layout and toolchain

```
proofs/                      # lake package `uc2-proofs` (NOT a cargo crate)
  lean-toolchain             # pinned Lean 4 version
  lakefile.toml              # mathlib dep, pinned rev; lake-manifest.json committed
  Uc2Model/                  # library 1: executable, ZERO mathlib imports
    TermMap.lean             #   ascending (term, base) maps, term_at
    ByteHistory.lean         #   logs as pos → term-stamp; encoding lemma-ready defs
    Commit.lean              #   CommitTracker event fold
    Reconcile.lean           #   reconcile, 1:1 port
    Vote.lean                #   log_ok + voted_for discipline
  Uc2Proofs/                 # library 2: imports Uc2Model + mathlib
    Quorum.lean              #   C5 quorum intersection (Finset pigeonhole)
    Commit.lean  Reconcile.lean  Vote.lean
  Aeneas/                    # Phase 1.5: vendored generated Lean + equivalence
    Generated/               #   charon+aeneas output, committed (like the elle jar)
    Equiv.lean               #   Uc2Model.reconcile = translated reconcile
    SOURCES.sha256           #   drift-guard hash of the Rust kernel sources
  Conform/Main.lean          # executable: JSONL conformance replay checker
```

- `Uc2Model` imports nothing outside core Lean → builds in seconds; the
  conformance loop never waits on mathlib.
- `Uc2Proofs` imports mathlib (wanted for `Finset` pigeonhole — quorum
  intersection; hand-rolling it is wasted effort). Pinned rev, bumped
  deliberately, `lake exe cache get` in CI.
- Rust side gains one dev-only piece: `uc_consensus/examples/conform_gen.rs`
  (vector generator). **No production code changes beyond the §6
  verifiability refactors; `uc_consensus` stays env-free and
  dependency-free.**

### Model fidelity rules (drift prevention)

Mirrors the cnc offset-pinning convention:

- Every `Uc2Model` definition carries a doc comment naming its Rust source
  (`uc_consensus/src/commit.rs::advance`).
- The nightly conformance job (§5) re-derives agreement mechanically.
- `Aeneas/SOURCES.sha256` records the kernel sources' hash; nightly fails if
  the Rust changed without regeneration + equivalence repair.
- Runbook gains a "changing a proved kernel" section: change Rust → update
  model → re-run conformance → (if Aeneas-covered) regenerate + repair
  `Equiv.lean`.

## 4. Theorem list (Phase 1 deliverable)

### `Commit` (over the event-fold model)

| # | Theorem | Statement (informal) |
|---|---------|----------------------|
| C1 | monotonicity | `commit` never decreases across any event sequence |
| C2 | bounded-by-own | after `advance(own)`, `commit ≤ own` — never certify what the leader cannot itself serve |
| C3 | no-phantom-commit | `commit = c > 0` ⟹ ≥ `quorum` members (own ∪ reporters) each reported durable ≥ `c` |
| C4 | reset soundness | after `reset_reports`, any advance is certified exclusively by post-reset reports (stale-term reports never certify new-term bytes) |
| C5 | quorum intersection | any two `⌊n/2⌋+1`-sized subsets of `n` members intersect (mathlib Finset pigeonhole; foundational, reused by Tier B) |

### `Reconcile` (stated over `ByteHistory` — the bug-hunting core)

| # | Theorem | Statement (informal) |
|---|---------|----------------------|
| R1 | bound | `valid_up_to ≤ own_durable` |
| R2 | shared-prefix preservation | positions covered by the common certified prefix always survive — "committed bytes at a healed follower survive reconcile", local form |
| R3 | divergence completeness | every provably-divergent position (own conflicting entry, overhang, or a leader term below durable that the data-stamped map lacks) is cut |
| R4 | **divergence soundness** | under the data-stamped-map contract, every *truncated* byte is genuinely divergent — reconcile never cuts a byte content-identical to the leader's certified lineage. R2+R4: truncation is *exactly right*. Would have caught the F4 ex-leader bug class |
| R5 | idempotence / phantom hygiene | reconcile against the same leader map is a fixed point; a clean outcome never leaves an entry that could cause a later spurious truncation (phantom-frontier class, generalized) |
| R6 | NoCommonPrefix characterization | surfaced iff the leader's shipped window truly slid past our history |

R4 is the long pole: the data-stamped-map contract ("an entry `(t, b)` appears
in our map iff we opened `t` as leader or durably wrote `t`-stamped bytes from
`b`") must be made inductive. If it will not go through as stated, that is a
finding about the spec, not a failure of the effort.

### `Vote` (local lemmas Tier B consumes)

| # | Theorem | Statement (informal) |
|---|---------|----------------------|
| V1 | single-vote-per-term | at most one grant per `(node, term)`; re-grant only idempotently to the same candidate |
| V2 | grant order | a grant implies candidate `(last_term, last_durable) ≥` voter's at grant time; with C5, a winner's frontier ≥ a full quorum's frontiers — the leader-completeness seed |
| V3 | persist-before-send | modeled as an atomic persist+send step; recorded as an **assumption on the runtime** (uc_node's `PersistAndSendVote` contract), discharged by code inspection, not proof |

## 5. Conformance rig (model ↔ Rust, mechanical)

One-directional, no FFI:

1. `uc_consensus/examples/conform_gen.rs` emits JSONL
   `{fn, input, rust_output}`:
   - random vectors from the same seeded-generator distribution the sim fuzz
     uses (deterministic seeds), **plus** every hand-written edge vector from
     the Rust unit tests (`commit.rs`, `reconcile.rs` test cases verbatim);
   - covered functions: the `advance`-fold (event sequences), `reconcile`,
     `log_ok`;
   - target ≥ 10⁵ vectors; written to **disk** (`$HOME/.cache/uc2-conform`),
     never `/tmp` (RAM tmpfs — see CLAUDE.md).
2. `Conform/Main.lean` (builds against `Uc2Model` only — no mathlib) replays
   each vector through the model and exits non-zero on first divergence,
   printing the offending vector.

Any drift between model and Rust fails the nightly loudly.

## 6. Phase 1.5 — Aeneas equivalence (time-boxed, gated)

**Goal:** upgrade the `reconcile` linkage from statistical (fuzz) to
definitional: Charon translates `uc_consensus`'s MIR to pure Lean; prove
`Uc2Model.reconcile = translated_reconcile` (extensional equivalence). The
remaining trust gap is the Charon/Aeneas translation semantics — peer-reviewed
and in production use (Microsoft SymCrypt port).

**Why feasible here:** the published Aeneas-friendliness guidance (standalone,
non-generic, dependency-free, monomorphic, safe, simple loops) describes
`uc_consensus` as it already is: zero deps, no generics, no unsafe.

**Verifiability refactors** (1–2 land in Phase 1 regardless — small,
harmless, unit tests binding; 3 is conditional):

1. `reconcile.rs`: replace the `.iter().chain(.filter()).collect()` map
   construction with a plain for-push loop (iterator chains are Aeneas's rough
   edge).
2. `election.rs`: extract `log_ok` as a free function over
   `(our_term, our_durable, cand_term, cand_durable)`; the method delegates.
3. `commit.rs` (stretch-goal enabler): optionally replace `sort_unstable_by`
   with a k-th-max selection loop (avoids axiomatizing the stdlib sort;
   arguably faster at cluster sizes ≤ 7). Only if the stretch goal is taken.

**Mechanics:** run charon+aeneas **locally**, vendor the generated `.lean`
under `proofs/Aeneas/Generated/` (the elle-jar pattern); nightly CI only
`lake build`s it. `SOURCES.sha256` guards staleness. No OCaml or rustc-nightly
in CI.

**Box and exit:** 3–5 sessions, `reconcile` only; `commit.rs` a stretch goal
only if `reconcile` goes smoothly. If Charon/Aeneas chokes or the equivalence
proof blows the box: keep the refactors, fall back to conformance-fuzz-only
linkage, record the attempt in the gate doc. Theorems are **always** stated
against the clean hand model; generated code appears only in `Equiv.lean`, so
a kernel change only ever requires repairing the thin equivalence layer.

## 7. Phase 2 — distributed safety (spike committed, remainder gated)

**Spike (fixed scope, before any go/no-go):**

- Model: N nodes, per-node state
  `(current_term, voted_for, role, term_map, durable, commit)`; a message
  multiset (`RequestVote`, `Vote`, data-append, commit-gossip,
  term-map-gossip); a small-step relation under the sim's fault model —
  message loss/duplication/reordering; crash-restart preserving durable state
  only (vote, term map, durable; volatile reports reset, cf. C4).
- Theorem: **election safety** — at most one leader per term (V1 + C5 + an
  inductive invariant over the step relation). The smallest theorem that
  exercises the full distributed machinery; it prices Tier B honestly.

**Go/no-go** after the spike, with real proof-cost data, on the remainder:

- log-matching analog (term-at content identity: term `t`'s bytes are
  determined by `t`'s unique leader),
- leader completeness (elected leader durable ≥ global commit; agrees with
  the committed lineage),
- state-machine safety — together the sim's inv4 (per-node
  committed-never-truncated) + inv5 (election above global commit + leader
  completeness), i.e. Raft State-Machine-Safety in UC's byte formulation.

**Tier C (deferred, unscoped):** M7 reconfig — config-dependent quorums,
single-server change safety, tombstone permanence (sim inv6–9).

## 8. CI

- New nightly job `lean-proofs` (same tier as elle; **not** on the PR path):
  1. elan bootstrap (pinned `lean-toolchain`), mathlib cache
     (`lake exe cache get`);
  2. `lake build` — model, proofs, vendored Aeneas output, conform checker;
  3. `cargo run -p uc_consensus --example conform_gen` → run the Lean
     checker over the vectors;
  4. `SOURCES.sha256` drift check.
- Build artifacts and vectors go to disk (`$HOME/.cache`), never `/tmp`;
  mathlib cold builds are RAM/disk-heavy (CLAUDE.md box rules apply).

## 9. Effort and phasing

| Phase | Scope | Estimate |
|-------|-------|----------|
| 1 | lake+CI setup; `Commit`+`Vote` models & proofs; `ByteHistory`+`Reconcile` models & proofs (R4 the long pole); conformance rig; verifiability refactors; gate doc | elle-sized SDD arc, ~9–12 tasks (~1–2 weeks of sessions) |
| 1.5 | Aeneas equivalence for `reconcile` (+ `commit.rs` stretch) | time-boxed 3–5 sessions, gated exit |
| 2 spike | N-node model + election safety | ~1–2 weeks of sessions |
| 2 full | leader completeness + state-machine safety | 1–3 months, **gated** on the spike |
| 3 (Tier C) | M7 reconfig safety | deferred, unscoped |

## 10. Risks

1. **Model drift** → conformance fuzz + doc-comment cross-refs + drift-guard
   hash + runbook procedure.
2. **R4 harder than expected** — the data-stamped contract may need
   strengthening to become inductive. A stuck proof is a *finding* (candidate
   spec gap), not a failure; record it either way.
3. **mathlib churn** → pinned rev, deliberate bumps only.
4. **Aeneas toolchain** (research-grade edges, rustc-nightly pin) → gated,
   local-only, vendored output, defined exit (§6).
5. **This box** — mathlib cold build and vector files are RAM/disk-heavy:
   everything to `/home/claude`-backed disk, never `/tmp` (tmpfs, no swap).

## 11. Success criteria

**Phase 1 done when:**

- All C1–C5, R1–R6, V1–V3 theorems are `sorry`-free under the pinned
  toolchain.
- Conformance replay green over ≥ 10⁵ seeded vectors + all unit-test edge
  vectors, for all three covered functions.
- Nightly `lean-proofs` job green.
- Gate doc `docs/benchmarks/uc2-lean-gate-<date>.md` records the theorem ↔
  sim-invariant mapping (C3↔inv7's quorum half, R2/R4↔inv4's local form,
  V2↔inv5's seed), any findings (stuck proofs, spec gaps), and the Phase 1.5
  outcome.

**Phase 2 spike done when:** election safety is `sorry`-free over the N-node
model, and the go/no-go memo (proof-cost data, remaining-theorem estimate) is
written.
