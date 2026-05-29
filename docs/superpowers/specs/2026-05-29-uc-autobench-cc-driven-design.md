# uc_autobench — Claude-Code-Driven Autoresearch Loop (rework)

**Status:** Design — approved through Sections 1–6 in brainstorm 2026-05-29. Awaiting final spec review before plan-writing.
**Supersedes:** `docs/superpowers/specs/2026-05-24-uc-autobench-design.md` (API-driven design, scrapped — required separately-billed `ANTHROPIC_API_KEY`).
**Author:** brainstormed 2026-05-29 with Claude Code.
**Reference:** karpathy/autoresearch (`/Users/peter/Projects/ml/autoresearch/`) — pattern adapted to ultima_cluster.

---

## 1. Approach

A karpathy-shape autoresearch loop where **Claude Code is the orchestrator**. The human starts a run with a one-line prompt that points Claude at `uc_autobench/program.md` (+ per-task overlay at `uc_autobench/tasks/<task>/program.md`). Claude reads the loop instructions, reads the current TSV/branch state, proposes an edit to the mutable target files, runs a single `cargo run -p uc_autobench --bin run-iter --release -- --task <task> --json` invocation, parses the consolidated JSON, commits or reverts via plain git, appends a row to the (committed) per-task TSV, and iterates indefinitely until the human interrupts.

**Why this shape (vs. the prior API-driven design):**

- Reuses the human's existing Claude Code subscription — no separate Anthropic API spend.
- Eliminates ~2000 lines of Rust loop-driver code (orchestrator state machine, Anthropic tool-use client, prompt rendering, patch apply/restore, sandbox subprocess driver, leaderboard, JSONL persistence, declarative TOML task spec, task-scaffolding skill). All of that is subsumed by Claude Code's built-in primitives (`Edit`, `Bash`, `Read`, plain git) plus a 50-line markdown loop spec.
- Aligns with karpathy/autoresearch's empirically validated minimalism: one markdown file as the orchestrator, branch advances on win, `git checkout --` on loss, TSV log.

**Approach B chosen** over pure karpathy port (A) or leaderboard discipline overlay (C): a tiny Rust `run-iter` helper consolidates build → ring_torture → shmem-microbench → shmem-e2e into one command emitting one JSON. The agent's decision logic stays a 2-state branch (`pass` → keep, anything else → discard) rather than parsing three separate command outputs. The conditional e2e gate (skipped when microbench is clearly not a winner) lives inside the helper, saving iteration wall-time.

**Scope of this rework:**

- Delete the API-driven Rust loop machinery (full file list in Section 6).
- Keep the fitness layer untouched: `shmem-microbench.rs`, `shmem-e2e.rs`, `ring_torture.rs`, and `uc_node::test_support::ClusterFixture`.
- Add `program.md` (generic loop spec), `tasks/shmem/program.md` (per-task overlay), `tasks/shmem/results.tsv` (empty header row), and `src/bin/run-iter.rs` (consolidation helper).
- Rewrite `uc_autobench/CLAUDE.md` and `docs/tasks/task07_uc_autobench.md` for the new flow.

**Out of scope:**

- No archive-to-branch of the old code. Git history preserves it.
- No new task-scaffolding skill. Defer until a second task actually exists.
- No actual autoresearch run on the shmem target — that's a separate human-kicked operation on the rebuilt loop.
- No changes to `shmem-microbench`, `shmem-e2e`, `ring_torture`, or `ClusterFixture` (frozen by design).

**Metrics (unchanged from prior design):**

- Primary: `spsc_p99_ns` (minimize), from `shmem-microbench`.
- Goodhart gate: `submit_to_resp_p99_ns` from `shmem-e2e`, 5% regression tolerance vs. baseline.
- Correctness floor: 6 behavioral conformance tests in `ring_torture` must pass.

---

## 2. Repo layout

```
ultima_cluster/
├── uc_protocol/                          TARGET — mutable ring files live here
├── uc_node/
│   └── src/test_support.rs               KEEP (ClusterFixture, reused by shmem-e2e)
├── uc_service/
├── uc_client/
└── uc_autobench/
    ├── Cargo.toml                        KEEP (slim dep list after deletes)
    ├── CLAUDE.md                         REWRITE (operator manual, CC-driven)
    ├── program.md                        NEW (generic loop instructions)
    ├── src/
    │   ├── lib.rs                        REWRITE (export only what bin/* needs)
    │   ├── bin/
    │   │   ├── run-iter.rs               NEW — Approach B consolidation helper
    │   │   ├── shmem-microbench.rs       KEEP (frozen, JSON out)
    │   │   └── shmem-e2e.rs              KEEP (frozen, JSON out)
    │   ├── leaderboard.rs                DELETE
    │   ├── llm.rs                        DELETE
    │   ├── orchestrator.rs               DELETE
    │   ├── outcome.rs                    DELETE
    │   ├── persist.rs                    DELETE
    │   ├── prompt.rs                     DELETE
    │   ├── proposal.rs                   DELETE
    │   ├── sandbox.rs                    DELETE
    │   ├── task.rs                       DELETE
    │   └── tasks/                        DELETE (dir + contents)
    ├── tests/
    │   └── ring_torture.rs               KEEP (frozen correctness floor)
    └── tasks/
        └── shmem/
            ├── program.md                NEW — per-task overlay
            ├── results.tsv               NEW — header-only at init; committed
            └── task.toml                 DELETE (declarative spec subsumed by program.md)

.claude/skills/create-autobench-task/     DELETE (entire dir)

docs/tasks/task07_uc_autobench.md         REWRITE
docs/superpowers/specs/
  ├── 2026-05-24-uc-autobench-design.md   KEEP (historical record of the API-driven design)
  └── 2026-05-29-uc-autobench-cc-driven-design.md   THIS FILE
docs/superpowers/plans/
  ├── 2026-05-24-uc-autobench.md          DELETE (per project workflow; the API-driven plan is dead)
  └── 2026-05-29-uc-autobench-cc-driven.md NEW (written next via writing-plans skill)
```

Run artifacts: the prior design's `auto-bench-runs/<task-id>/<run-id>/` directory is dropped. Branch + TSV carry all state.

`uc_autobench/Cargo.toml` deps shrink: drop `reqwest`, drop `tokio` (if no remaining use after the orchestrator goes), drop orchestrator-only `anyhow`/`bincode` plumbing. Keep what `shmem-microbench`, `shmem-e2e`, `ring_torture`, and `run-iter` actually need (`serde_json` for `run-iter`'s output, `wait-timeout` or hand-rolled equivalent for subprocess timeouts).

---

## 3. Loop semantics

### 3.1 Branch model

Operator (or Claude during setup) creates `autoresearch/<task>-<tag>` from `main` (e.g. `autoresearch/shmem-may29`). All iteration commits land on this branch. Pull request to `main` only happens after the human is satisfied with the winner.

### 3.2 TSV — committed, per-task

`uc_autobench/tasks/shmem/results.tsv`, tab-separated, six columns:

```
commit	spsc_p99_ns	e2e_p99_ns	memory_kb	status	description
a1b2c3d	    218	  38104	   832	keep	baseline
b2c3d4e	    195	  38240	   832	keep	cache-pad head/tail to separate lines
c3d4e5f	    260	      0	   832	discard	relax fence on push — torture failed
d4e5f6g	      0	      0	     0	crash	mpsc index overflow, panic in producer
```

- `status` ∈ {`keep`, `discard`, `crash`}.
- Use `0` for any metric that didn't run (microbench skipped, e2e gate skipped, etc.).
- `description` is the agent's one-line hypothesis label.
- TSV is committed on every iteration (kept + discarded + crashed), so the branch genuinely encodes the full research log.

### 3.3 Per-iteration sequence (the loop)

1. **Read state.** `cat uc_autobench/tasks/<task>/results.tsv` → identify current best by lowest `spsc_p99_ns` among `status=keep` rows.
2. **Propose.** Form a hypothesis; write a one-line description for the TSV.
3. **Edit.** Modify only files inside `mutable_paths` (declared in the per-task overlay).
4. **Run.** `cargo run -p uc_autobench --bin run-iter --release -- --task <task> --json --baseline-spsc-p99-ns <n> --baseline-e2e-p99-ns <n> > /tmp/run-iter.json 2>&1`. (Baselines absent on first iter.)
5. **Parse.** `jq '.status, .metrics, .gate, .stderr_tail' /tmp/run-iter.json`.
6. **Decide.**
   - `status == "pass"` AND `metrics.spsc_p99_ns < current_best` AND `gate.e2e_passed == true` → **KEEP**: append TSV row, `git add -A`, `git commit -m "<task>: <description>"`. Branch advances.
   - `status == "pass"` but no improvement OR `gate.e2e_passed == false` → **DISCARD**: append TSV row with `status=discard`, revert mutable paths via `git checkout -- <mutable_paths>`, then `git add` only the TSV and commit `discard: <description>`.
   - `status` ∈ {`build_failed`, `torture_failed`, `microbench_failed`, `e2e_failed`, `timeout`} → **CRASH**: same flow as DISCARD but `status=crash` and `description` includes the failing stage. If the failure looks trivial (typo, missing import), the agent may attempt up to 2 quick fixes before giving up.
7. **Loop.** Never stop voluntarily. The human will interrupt.

### 3.4 Win criteria

- **Primary:** `spsc_p99_ns` strictly less than current branch best.
- **Goodhart gate:** `shmem-e2e` `submit_to_resp_p99_ns` must not regress more than 5% over baseline. The e2e gate runs *inside* `run-iter`, conditional on the microbench showing plausibility (see Section 4).
- **Floor:** `ring_torture` must pass (6 tests, zero failures, zero timeouts).

### 3.5 Crash handling

OOM, panic, build error, timeout → log as `crash`, revert source. Trivial-fix budget: 2 quick attempts (typo, missing import) before moving on.

### 3.6 Simplicity rule (karpathy-derived)

Match the karpathy criterion: a small `spsc_p99_ns` win that adds 30 lines of arch-specific intrinsics may not be worth a 2% gain; a wash-or-improvement that *deletes* code is always a keep. The agent weighs complexity cost against improvement magnitude.

---

## 4. `run-iter` consolidation helper

### 4.1 Purpose

One command per iteration, one structured JSON output, one decision. The agent runs `cargo run -p uc_autobench --bin run-iter --release -- --task shmem --json` instead of orchestrating three cargo invocations and parsing three outputs.

### 4.2 Invocation

```
run-iter --task <name> --json
        [--baseline-spsc-p99-ns <n>]
        [--baseline-e2e-p99-ns <n>]
```

Baselines are optional. When supplied (agent pulls from the latest `keep` row of the TSV), `run-iter` uses them to conditionally skip the e2e gate. When absent (first iteration of a new run), the e2e gate always runs to establish baseline.

For v1, `--task <name>` accepts only `shmem`. The flag exists so future tasks can be added without breaking the agent-facing CLI.

### 4.3 Stages (executed in order, short-circuit on failure)

1. **Build.** `cargo build -p uc_protocol -p uc_autobench --release`. Failure → `{"status":"build_failed", "stage":"build", "stderr_tail":"..."}`, exit 0.
2. **Torture.** `cargo test -p uc_autobench --test ring_torture --release`. Failure → `{"status":"torture_failed", ...}`.
3. **Microbench.** Spawn `shmem-microbench --json` with hard wall-clock timeout 180s. Parse its JSON metrics. Timeout/crash → `{"status":"microbench_failed", ...}` or `{"status":"timeout", ...}`.
4. **E2E gate (conditional).** Runs only if `spsc_p99_ns ≤ baseline_spsc_p99_ns * 1.05` (i.e. plausibly a winner). Otherwise marks gate as `{"ran": false, "e2e_passed": null, "reason": "skipped_microbench_not_plausible"}`. If it runs and `submit_to_resp_p99_ns > baseline_e2e * 1.05` → `gate.e2e_passed = false`. Hard timeout 300s.
5. **Done.** `{"status":"pass", ...}`.

### 4.4 Output schema

Always JSON. Exit code 0 on tool-success (regardless of which stage failed); non-zero only on `run-iter`'s own internal bug (panic, malformed args). The agent reads the JSON `status` field, not the exit code.

```json
{
  "status": "pass | build_failed | torture_failed | microbench_failed | e2e_failed | timeout",
  "stage": "build | torture | microbench | e2e",
  "duration_s": { "build": 12.3, "torture": 1.4, "microbench": 22.0, "e2e": 41.2 },
  "metrics": {
    "spsc_p50_ns": 87, "spsc_p99_ns": 195, "spsc_throughput_msgs": 28000000,
    "mpsc_4p_p99_ns": 410, "mpsc_4p_throughput": 14000000,
    "broadcast_4sub_p99_ns": 380, "large_payload_p99_ns": 1100, "wrap_throughput": 9800000,
    "peak_rss_kb": 832
  },
  "gate": {
    "ran": true,
    "e2e_passed": true,
    "submit_to_resp_p99_ns": 38240,
    "baseline": 38104,
    "regress_pct": 0.36
  },
  "stderr_tail": null
}
```

`stderr_tail` is the last ~50 lines on any failure path, so the agent can diagnose without re-reading large logs. For `status == "pass"`, `stderr_tail` is `null`.

### 4.5 Implementation shape

- Sync code: plain `std::process::Command` for child spawning; no `tokio` required.
- Subprocess timeouts: `wait-timeout` crate (or hand-rolled with `Command::spawn` + a watchdog thread that calls `child.kill()`).
- Reuses existing JSON output schemas of `shmem-microbench` and `shmem-e2e` — no schema changes there.
- One file: `uc_autobench/src/bin/run-iter.rs`, target ~250 lines.

---

## 5. `program.md` content

### 5.1 `uc_autobench/program.md` (generic)

```markdown
# uc_autobench

Autoresearch loop: you (Claude Code) propose code changes to optimize a target,
the harness measures, and you advance the branch on wins / revert on losses.

## Setup (run once at the start of a new run)

1. Confirm task: read `uc_autobench/tasks/<TASK>/program.md` for task-specific
   constraints (mutable paths, metric, baselines, special instructions).
2. Confirm working tree is clean (`git status`). If not, stop and ask.
3. Propose a run tag based on today's date (e.g. `may29`). The branch
   `autoresearch/<TASK>-<tag>` must not already exist.
4. Create the branch: `git checkout -b autoresearch/<TASK>-<tag>` from `main`.
5. If `uc_autobench/tasks/<TASK>/results.tsv` does not exist, create it with
   the header row described in the task overlay.
6. Confirm setup with the human, then begin the loop.

## The loop

LOOP FOREVER:

1. Read state: `cat uc_autobench/tasks/<TASK>/results.tsv`. Find current best.
2. Form a hypothesis. Write a one-line description.
3. Edit ONLY files in `mutable_paths` (see task overlay). Never touch
   `frozen_paths`. Never edit the microbench, e2e, torture, or fixture code.
4. Run: `cargo run -p uc_autobench --bin run-iter --release -- \
     --task <TASK> --json \
     --baseline-spsc-p99-ns <best> --baseline-e2e-p99-ns <e2e_best> \
     > /tmp/run-iter.json 2>&1`
5. Parse: `jq '.status, .metrics, .gate, .stderr_tail' /tmp/run-iter.json`.
6. Decide:
   - status=="pass" AND primary metric improved AND gate passed → KEEP:
     append TSV row, `git add -A`, `git commit -m "<TASK>: <description>"`.
   - status=="pass" but no improvement OR gate failed → DISCARD:
     append TSV row with status=discard, revert mutable paths with
     `git checkout -- <mutable_paths>`, then `git add` the TSV and commit
     `discard: <description>`.
   - status starts with "*_failed" or "timeout" → CRASH:
     same as DISCARD but status=crash and description includes the failure
     stage. If the failure looks like a trivial fix (typo, missing import),
     attempt up to 2 quick fixes before giving up.
7. GOTO 1.

## Rules

- NEVER stop on your own. The human will interrupt. If you're stuck, think
  harder: re-read the in-scope files, look for combinations of prior
  near-misses, try more radical structural changes.
- NEVER touch frozen files. NEVER add a dependency to `Cargo.toml`. NEVER
  modify `run-iter`, microbench, e2e, or torture binaries.
- Simplicity wins: a small gain that adds ugly complexity is not worth it.
  A wash that deletes code is always a keep.
- Use `git checkout --` for reverts, not `git reset --hard` (which would
  drop the TSV row you just added).
```

### 5.2 `uc_autobench/tasks/shmem/program.md` (per-task overlay)

```markdown
# Task: shmem-rings

Optimize uc_protocol shared-memory ring buffers for latency and throughput.

## Mutable paths

- uc_protocol/src/ring/spsc.rs
- uc_protocol/src/ring/mpsc.rs
- uc_protocol/src/ring/broadcast.rs
- uc_protocol/src/ring/common.rs

## Frozen paths (never edit)

- uc_protocol/src/lib.rs
- uc_protocol/src/ring/mod.rs       (public API surface)
- uc_autobench/tests/ring_torture.rs
- uc_autobench/src/bin/shmem-microbench.rs
- uc_autobench/src/bin/shmem-e2e.rs
- uc_autobench/src/bin/run-iter.rs
- uc_node/src/test_support.rs

## Metric

- Primary: `spsc_p99_ns` (minimize)
- Goodhart gate: `submit_to_resp_p99_ns` from shmem-e2e
  (5% regression tolerance vs current branch best)
- Floor: ring_torture must pass (6 tests, zero failures)

## TSV schema

`uc_autobench/tasks/shmem/results.tsv`, tab-separated:

    commit	spsc_p99_ns	e2e_p99_ns	memory_kb	status	description

Statuses: keep, discard, crash. Use 0 for metrics that didn't run.

## Constraints specific to this task

- `uc_protocol` is `no_std`-friendly: `core` only. No `std::`, no `tokio`,
  no `serde`, no allocations on the hot path beyond what the existing API
  already does.
- Ring buffer correctness: SPSC, MPSC (N producers, 1 consumer), Broadcast
  (1 producer, N subscribers). All operations must be lock-free in the
  steady state (memory-fence reorderings are fair game; locks aren't).
- Variable-length records with atomic-after-write length prefix. Reader
  sees length=0 → spin/yield.
- Ideas to consider (non-exhaustive, no obligation): cache-line padding of
  head/tail indices, weakening unnecessary `SeqCst` to `AcqRel`/`Release`,
  batched MPSC reservation, broadcast slot reuse strategies, prefetch hints,
  exponential vs linear backoff in spin loops.

## Operator-supplied baselines (filled in by the human at start of run)

- baseline_spsc_p99_ns: <TBD on first run>
- baseline_e2e_p99_ns:  <TBD on first run>
```

---

## 6. Migration plan

Single working branch `auto_bench_shmem` (already checked out). Six commits, in order:

### Step 1 — Delete the API-driven loop machinery

Delete:

- `uc_autobench/src/{orchestrator.rs, llm.rs, prompt.rs, proposal.rs, sandbox.rs, leaderboard.rs, persist.rs, outcome.rs, task.rs}`
- `uc_autobench/src/tasks/` (entire dir)
- `uc_autobench/src/bin/auto-bench.rs`
- `uc_autobench/tasks/shmem/task.toml`
- `.claude/skills/create-autobench-task/` (entire dir)

Update:

- `uc_autobench/src/lib.rs` — re-export only what `run-iter` / microbench / e2e need (likely just utility constants and JSON helpers; possibly empty).
- `uc_autobench/Cargo.toml` — drop `reqwest`, drop `tokio` (if no remaining use after the orchestrator goes), drop orchestrator-only `anyhow`/`bincode` plumbing. Keep what microbench / e2e / torture / `run-iter` actually need.
- `uc_autobench/CLAUDE.md` — replace with operator manual for the new flow (describe `program.md`, branch model, `run-iter`, TSV).

Verify: `cargo build --release` + `cargo clippy --workspace -- -D warnings` pass.

Commit: `refactor(uc_autobench): remove API-driven loop, prep for CC-driven autoresearch`.

### Step 2 — Add `run-iter` consolidation binary

- Create `uc_autobench/src/bin/run-iter.rs` per Section 4.
- Add `wait-timeout` dep to `uc_autobench/Cargo.toml` (or hand-roll with `Command::spawn` + watchdog thread).
- Manual smoke test: `cargo run -p uc_autobench --bin run-iter --release -- --task shmem --json` — verify build → torture → microbench → e2e runs and emits valid JSON for the happy path.
- Manual failure test: temporarily break `spsc.rs` (e.g. add a `let x =`), verify `status:"build_failed"` JSON; revert.

Commit: `feat(uc_autobench): run-iter consolidation binary`.

### Step 3 — Add `program.md` (generic) and `tasks/shmem/program.md` (overlay)

Write both files per Section 5.

Commit: `docs(uc_autobench): program.md for CC-driven autoresearch loop`.

### Step 4 — Initialize empty TSV and update gitignore

- Create `uc_autobench/tasks/shmem/results.tsv` with just the header row.
- Remove `auto-bench-runs/` from `.gitignore` if present (no longer used).

Commit: `feat(uc_autobench): initialize shmem results.tsv`.

### Step 5 — Rewrite `docs/tasks/task07_uc_autobench.md`

Replace contents to describe the new CC-driven shape: roles of `program.md`, `run-iter`, per-task TSV, the branch model. Reference this design doc + the plan that will live under `docs/superpowers/plans/`. Per project workflow, after the rework verifies green, consolidate this design doc + plan into `task07_uc_autobench.md` and delete the matching `docs/superpowers/specs/2026-05-29-*.md` + `docs/superpowers/plans/2026-05-29-*.md`. The old `2026-05-24-uc-autobench.md` plan is deleted in this same commit; the old `2026-05-24-uc-autobench-design.md` spec is kept as historical record.

Commit: `docs(tasks): rewrite task07 for CC-driven uc_autobench`.

### Step 6 — Smoke-test the full loop manually

- Cut trial branch `autoresearch/shmem-smoke` from `auto_bench_shmem`.
- Operator (or Claude in a non-loop session) runs one manual iteration: make one no-op edit to `spsc.rs` (whitespace), run `run-iter`, verify it produces a `pass` JSON with metrics.
- Revert; delete the smoke branch.

No commit (smoke-only).

### Verification gates between steps

- After Step 1: `cargo build --release` + `cargo clippy --workspace -- -D warnings` pass.
- After Step 2: smoke runs of `run-iter` (happy path + induced build failure) succeed.
- After Step 6: full manual single-iter run on the smoke branch succeeds.

---

## 7. Open questions / explicit non-decisions

- **Race vs. macro-stability of `spsc_p99_ns`.** The prior design's retrospective noted run-to-run variance is sensitive to background load; that's still true here. Mitigation deferred to the future framework retrospective (no core-pinning in v1).
- **Multi-task support.** `run-iter --task <name>` accepts only `shmem` in v1. Adding a second task means: (a) write `tasks/<name>/program.md`, (b) write per-task fitness binaries, (c) extend `run-iter`'s `--task` match arm. No new framework primitives required.
- **Resume of a prior run.** Not needed: a "run" is identified by its branch, and the TSV + git log on that branch is its complete state. To resume, `git checkout autoresearch/<task>-<tag>` and prompt Claude with the same setup line — it reads the TSV and continues.
- **Parallel variant execution.** Out of scope for v1 (single-branch, sequential). Could be added later by cutting per-variant branches and merging winners, but that's a framework v2 question.