# uc_autobench — Auto-Optimization Loop for ultima_cluster / ultima_db

**Status:** Design — approved through Section 7 + post-review additions (framework CLAUDE.md, task-scaffolding skill, post-run retrospective). Awaiting final spec review.
**Author:** brainstormed 2026-05-24 with Claude Code.
**Scope:** Build a reusable Rust-based auto-optimization loop ("autoresearcher" in the spirit of Karpathy's `autoresearch`) and apply it as its first concrete task to the `uc_protocol` shmem ring buffers (`spsc.rs`, `mpsc.rs`, `broadcast.rs`, `common.rs`).

**Primary goal:** Ship a measurably faster shmem layer.
**Secondary goal:** Leave behind a minimal, reusable framework whose shape was informed by one real run (shmem) rather than imagined future requirements.

---

## 1. Approach

A **leaderboard + hypothesis** loop. Each iteration the LLM is shown the current best, two diverse leaders, recent rejections, and the search temperature. It returns a structured proposal `{hypothesis, rationale, expected_outcome, files}`. The orchestrator applies, builds, tests, microbenches, and (if the variant beats best) runs an end-to-end Goodhart-gate before promoting.

The loop is **sequential** (one variant at a time), with **full code-rewrite freedom** inside a per-task contract that declares which paths are mutable and which are frozen. The LLM provides full file contents, not diffs.

Search strategy: greedy-with-diversity. Temperature rises after a plateau, encouraging structurally different proposals.

Why this shape was chosen (alternatives considered and rejected during brainstorming):

- *Param-tuning only* — rejected; the user wants exploratory rewriting, not a knob-twiddler.
- *Minimal greedy single-log* (Karpathy-shape) — viable; rejected as too thin on artifacts and prone to local optima.
- *Population/island evolutionary* — rejected for v1; needs parallel execution to pay off, premature given sequential start.
- *Python orchestrator* — rejected in favor of Rust to match the project stack and avoid a polyglot dep.
- *Microbench-only fitness* — rejected; Goodhart's law makes a pure-microbench loop dangerous when the LLM has full rewrite freedom.
- *End-to-end-only fitness* — rejected; per-iteration cost too high for a 200-iter run.

---

## 2. Repo layout

A new top-level workspace crate `uc_autobench/`:

```
ultima_cluster/
├── uc_protocol/                        target of shmem task
├── uc_node/                            test scaffolding reused by e2e gate
├── uc_service/
├── uc_client/
└── uc_autobench/
    ├── Cargo.toml                      workspace member
    ├── CLAUDE.md                       framework doc (generic; see §12)
    ├── src/
    │   ├── lib.rs                      orchestrator core (reusable)
    │   ├── bin/
    │   │   ├── auto-bench.rs           CLI: --task shmem [--resume <run-id>]
    │   │   ├── shmem-microbench.rs     shmem-task fitness binary (JSON out)
    │   │   └── shmem-e2e.rs            shmem-task Goodhart gate (JSON out)
    │   ├── task.rs                     OptimizationTask trait + spec types
    │   ├── llm.rs                      Anthropic API client (reqwest)
    │   ├── leaderboard.rs              top-K state + diversity sampling
    │   ├── sandbox.rs                  subprocess driver, timeouts, cleanup
    │   ├── persist.rs                  JSONL events + per-variant dirs
    │   └── prompt.rs                   system prompt + per-iter user message
    ├── tests/
    │   └── ring_torture.rs             frozen behavioral conformance suite
    └── tasks/
        └── shmem/
            └── task.toml               declarative shmem-task spec

.claude/
└── skills/
    └── create-autobench-task/          new skill (see §13)
        └── SKILL.md
```

Run artifacts land under `auto-bench-runs/<task-id>/<run-id>/` (gitignored).

---

## 3. Task specification

Each optimization task = one TOML file + one Rust impl of `OptimizationTask`.

### 3.1 `tasks/shmem/task.toml`

```toml
[task]
id          = "shmem-rings"
description = "Optimize uc_protocol shmem ring buffers for latency and throughput"

[contract]
mode          = "rust_api_plus_wire"
mutable_paths = [
  "uc_protocol/src/ring/spsc.rs",
  "uc_protocol/src/ring/mpsc.rs",
  "uc_protocol/src/ring/broadcast.rs",
  "uc_protocol/src/ring/common.rs",
]
frozen_paths = [
  "uc_protocol/src/lib.rs",
  "uc_protocol/src/ring/mod.rs",       # public re-exports = API surface
]

[gates]
test_cmd        = "cargo test -p uc_protocol --release"
torture_cmd     = "cargo test -p uc_autobench --test ring_torture --release"
build_timeout_s = 120
test_timeout_s  = 180

[microbench]
cmd          = "cargo run -p uc_autobench --bin shmem-microbench --release -- --json"
metrics      = ["spsc_p50_ns", "spsc_p99_ns", "spsc_throughput_msgs",
                "mpsc_4p_p99_ns", "mpsc_4p_throughput",
                "broadcast_4sub_p99_ns", "large_payload_p99_ns",
                "wrap_throughput"]
primary      = "spsc_p99_ns"
primary_dir  = "minimize"

[e2e_gate]
cmd          = "cargo run -p uc_autobench --bin shmem-e2e --release -- --json"
primary      = "submit_to_resp_p99_ns"
primary_dir  = "minimize"
regress_pct  = 5.0

[budget]
max_iterations   = 200
plateau_window   = 30
wall_clock_hours = 12
```

### 3.2 `OptimizationTask` trait

```rust
pub trait OptimizationTask {
    fn id(&self) -> &str;
    /// Source files the LLM sees as "current state" each iteration.
    fn read_state(&self, root: &Path) -> Result<HashMap<PathBuf, String>>;
    /// Parse microbench JSON stdout into a `BenchResult`.
    fn parse_microbench(&self, stdout: &str) -> Result<BenchResult>;
    /// Parse e2e JSON stdout into a `BenchResult`.
    fn parse_e2e(&self, stdout: &str) -> Result<BenchResult>;
    /// Task-specific extra prompt context (e.g. the lock-free invariants).
    fn extra_prompt_context(&self) -> &str;
}
```

The shmem task is one impl. Future tasks (ultima_db B-tree node layout, journal group-commit batching, etc.) implement the trait + ship a `task.toml`. **No generalization beyond what shmem forces** — we will only widen the trait when a second task demands it.

---

## 4. Variant lifecycle

One iteration = one variant attempt, a strict state machine:

1. **`pick_context`** — assemble current best + 2 diverse leaders + 2 recent rejections + temperature.
2. **`propose_variant`** — Claude API call; receive structured `VariantProposal`.
3. **`static_checks`** — reject without building if frozen paths touched, fmt fails, or `cargo check` fails.
4. **`correctness_gate`** — `cargo test -p uc_protocol` then `ring_torture`. Hard subprocess timeout.
5. **`microbench`** — separate subprocess; 3 warmup + 9 measured; median + stddev.
6. **`promote?`** — if primary metric beats best by > 2σ of best's distribution:
   - **`e2e_gate`** — separate subprocess.
   - If e2e regression < `regress_pct`: **PROMOTE** (becomes new current best).
   - Else: **REJECT** as Goodhart; flagged explicitly in `recent_rejections`.
   - Else (no microbench improvement): **RECORD** (kept in archive, not promoted).
7. **`append_to_leaderboard`** + write events.

### 4.1 Outcomes that feed back into the LLM's next-iteration context

- `static_reject` — patch + reason; doesn't count toward plateau.
- `test_fail` — test name + truncated stderr.
- `bench_regression` — full metrics; kept in archive for diversity sampling.
- `promoted` — becomes current best; prior best demoted to leader.
- `goodhart_reject` — explicit, so the LLM learns to avoid the same shape.

### 4.2 Termination

Any of: `max_iterations` reached, `wall_clock_hours` exceeded, plateau persists past `2 * plateau_window` even after temperature warm-up, user `Ctrl-C`. On Ctrl-C, finish the in-flight iteration then write a clean summary.

### 4.3 Temperature schedule

- 0.4 default ("focus on principled small changes to current best").
- After `plateau_window` iters w/o improvement → 0.7 ("try a meaningfully different approach").
- After `2 * plateau_window` w/o improvement → 0.9 ("propose a structurally different design") and broaden diverse-leader sampling.
- API `temperature` mirrors the same values.

### 4.4 Resumability

Every iteration writes its event to `events.jsonl` **before** the work it represents starts. Resume = replay the JSONL into in-memory state and continue. A `variant_proposed` with no matching `outcome` is marked `RESUMED_ABORTED`.

---

## 5. LLM prompt design

### 5.1 System prompt (stable, prompt-cached)

Contains:
- Role: "Rust systems engineer optimizing lock-free shmem ring buffers."
- The **contract** in words (mutable paths, frozen paths, on-disk layout invariants, the lock-free invariants from `CLAUDE.md`).
- The task's `extra_prompt_context()` (shmem-specific: cache lines on x86/ARM, futex availability, NUMA, etc.).
- The **fitness function** in words.
- The output JSON schema with examples.
- Rules: do not touch frozen paths; do not add new public items; do not change on-disk byte layout (unless task contract permits); shared helpers go in `common.rs`.

### 5.2 Per-iteration user message

Includes:
- Current best (full files, metrics, hypothesis).
- 2 diverse leaders (summary + metrics + hypothesis + "why not best").
- Last ~5 rejections with reasons.
- Microbench JSON schema (so the model can read its own results).
- Current search temperature + a one-line explanation.
- The literal instruction "Propose ONE variant. Respond with JSON conforming to the schema in the system prompt."

### 5.3 Structured response (enforced via Anthropic tool use)

Anthropic's API does not have an OpenAI-style "JSON mode" but does support enforced structured output via [tool use](https://docs.anthropic.com/en/docs/build-with-claude/tool-use). The orchestrator declares a single `propose_variant` tool whose input schema matches the JSON below; the model is forced to call it. This is more reliable than prompt-only "respond in JSON".

```json
{
  "hypothesis": "single-line claim",
  "rationale": "2-5 sentences. Reference prior variants by # when relevant.",
  "expected_outcome": {"primary_metric": "spsc_p99_ns", "expected_value": 1050, "confidence": "medium"},
  "risk_notes": "what might break / which torture test should catch it",
  "files": {
    "uc_protocol/src/ring/spsc.rs": "<full file content>",
    "uc_protocol/src/ring/common.rs": "<full file content>"
  }
}
```

**Full files, not diffs.** Simpler to apply atomically, simpler to validate, Claude is reliable at producing them.

### 5.4 Cost budget

~20 KB input + ~10–15 KB output per iteration, ~$0.05–$0.15 with Opus 4.7. A 200-iter run is ~$10–30. Cheap.

---

## 6. Sandboxing & correctness gate

With full-rewrite freedom on `unsafe` lock-free code, things *will* segfault, deadlock, or busy-loop. The harness must contain blast radius.

### 6.1 Process model

The orchestrator never runs candidate code in-process. Every external invocation is a subprocess with a hard timeout: `cargo check` (fast pre-gate), `cargo test` (correctness), `shmem-microbench`/`shmem-e2e` (fitness). Hard kill via `SIGKILL` on timeout so the OS reclaims shmem mappings.

### 6.2 Cleanup between iterations

Pre- and post-run: `rm -rf /dev/shm/ultima-autobench-*`. The bench harness uses a per-iteration instance suffix `ultima-autobench-<run_id>-<iter>` so even concurrent stale processes can't collide.

### 6.3 Three-layer gate

1. `cargo test -p uc_protocol --release` — existing unit tests; catches API/type breakage.
2. `ring_torture` test suite (new; frozen — LLM cannot edit). Six tests:
   - SPSC FIFO + no-loss + no-torn-read (1M small + 100k large messages, CRC each payload).
   - MPSC per-producer FIFO + total-count + no-torn-read (4 producers, 1M total).
   - Broadcast all-subscribers-see (1 producer, N consumers).
   - Wrap stress (4KB ring, 10M messages, thousands of wraps).
   - Backpressure (full ring + reader pause; producer behaves per contract).
   - Shutdown race (drop producer mid-write; consumer sees clean end).
3. `loom` model checker — optional, behind feature flag, off by default. User opts in for a single "deep verify" pass on the final champion.

The torture suite is the **non-negotiable correctness floor**. If we can't write good torture tests, we can't trust the loop. This is the single largest investment in the framework.

### 6.4 Timeouts

Per-test wall-clock cap (default 180 s). On timeout: `SIGKILL`, mark `TEST_FAIL` with reason `"timeout after Ns in test X"`, feed back to LLM so it learns to add finite spin bounds or yields.

### 6.5 Bench reproducibility

`taskset -c 3` on Linux; on macOS no equivalent → 3 warmup + 9 measured, take median, require improvement > 2σ of current best.

### 6.6 Explicitly out of scope for v1

- Docker / VM isolation (subprocess + cleanup + dedicated shmem prefix is enough).
- Miri (can't model cross-process shmem).
- Fuzzers (cargo-fuzz / AFL) — worth revisiting later as an extra gate.

---

## 7. Persistence, leaderboard, observability

The run *is* the artifact. Everything important about a variant is on disk before the next iteration starts.

### 7.1 Directory layout (gitignored)

```
auto-bench-runs/
└── shmem-rings/
    └── 2026-05-24T16-30-00-abc12/
        ├── task.toml.snapshot
        ├── git.head
        ├── events.jsonl
        ├── leaderboard.jsonl
        ├── summary.md
        ├── best/                        symlink → variants/00NN-promoted/
        └── variants/
            ├── 0000-baseline/
            │   ├── proposal.json        LLM response
            │   ├── patch.diff           derived
            │   ├── files/               actual contents written
            │   ├── outcome.json         {status, microbench, e2e?, timings}
            │   └── logs/                {cargo-check, cargo-test, ring-torture, microbench, e2e}.log
            └── …
```

### 7.2 `events.jsonl` — canonical log

Append-only; orchestrator state is fully reconstructible by replay. Each event is written *before* the work it represents begins (so a crash is recoverable to the last completed step). Event kinds:

`run_started`, `variant_proposed`, `static_check`, `correctness_gate`, `microbench`, `e2e_gate`, `outcome`, `plateau_temperature`, `run_ended`.

### 7.3 `leaderboard.jsonl` — derived top-K

Default K=20. Sorted by primary metric. Each entry carries a diversity tag so `pick_context()` can sample two structurally different leaders. **For v1 the diversity tag is a hash of the file with comments/whitespace/identifier-renames normalized** (cheap: strip comments, collapse whitespace, hash); structurally similar variants hash the same. A future version may switch to a proper AST hash (`syn::parse_file` → strip spans → hash) if the cheap version causes too many duplicates. Rebuilt from `events.jsonl` on every resume.

### 7.4 `summary.md` — human-readable

Rewritten each iteration. Shows status, best-so-far + delta vs baseline, plateau countdown, top-5 leaderboard, recent rejections. This is what the user reads after wandering away for two hours.

### 7.5 Observability while running

- `tail -f events.jsonl | jq .` works out of the box.
- `tracing` to stderr; `RUST_LOG=uc_autobench=info` for normal use.
- Optional TUI (`ratatui`) — nice-to-have, not v1.

### 7.6 Resume command

`cargo run -p uc_autobench -- --task shmem --resume <run-id>`.

---

## 8. Shmem-task specifics

### 8.1 Contract (the prompt verbalizes this)

- Mutable: `spsc.rs`, `mpsc.rs`, `broadcast.rs`, `common.rs`.
- Frozen: `ring/mod.rs` (public re-exports = API).
- Wire-layout invariants: header fields (magic / version / capacity / producer_seq / consumer_seq) keep their meanings; per-record framing uses an atomic-after-write length prefix (reader sees `len=0` → spin/yield); FIFO; no torn reads; no loss for SPSC/MPSC; all-subscribers-see for Broadcast.
- Wire-layout *bytes* may be repacked during optimization runs (multi-language consumers aren't shipped yet).

### 8.2 `shmem-microbench` binary

Single binary, multiple sub-benchmarks, one JSON line out. Schema (values elided):

```text
{
  "spsc_p50_ns":           N,    // 1 producer + 1 consumer, 64B payload, N=1M
  "spsc_p99_ns":           N,    // PRIMARY
  "spsc_throughput_msgs":  N,    // saturated 2s
  "mpsc_4p_p99_ns":        N,    // 4 producers + 1 consumer, 64B payload
  "mpsc_4p_throughput":    N,
  "broadcast_4sub_p99_ns": N,    // 1 producer + 4 subscribers, 64B
  "large_payload_p99_ns":  N,    // SPSC, 4096B payload
  "wrap_throughput":       N,    // small ring (4KB), 64B, saturated 2s
}
```

3 warmup + 9 measured runs; report median + stddev. Primary `spsc_p99_ns` is the scalar minimized. Other metrics are visible in `outcome.json` so the LLM can reason about trade-offs.

### 8.3 `shmem-e2e` binary (Goodhart gate)

In-process node + service + 4 clients (based on the existing M4 test scaffolding). Drives 100k submit→response round-trips. Reports `submit_to_resp_p50_ns`, `submit_to_resp_p99_ns`, `submit_to_resp_throughput`. Runs only when a variant beats current best on microbench. Variant rejected if e2e regresses more than 5%.

The harness extracts the M4 test setup into a reusable helper at `uc_node/src/test_support.rs` (gated behind a `test-support` feature so it doesn't affect production builds). The shmem-e2e binary depends on `uc_node = { path = "...", features = ["test-support"] }`. The existing M4 tests migrate to use the same helper as part of this work — a modest, useful refactor.

### 8.4 `ring_torture` test suite

Six conformance tests (Section 6.3). Frozen, LLM never sees or edits. Uses only the public `uc_protocol::ring::{Spsc, Mpsc, Broadcast}` API so it works regardless of internal rewrite.

### 8.5 Baseline (iteration #0000)

The first variant is the unmodified current code, run through the full pipeline. The LLM never proposes #0000 — it's a synthetic freebie produced by the orchestrator. Establishes the baseline metrics every subsequent comparison uses.

### 8.6 Budget

`max_iterations=200`, `wall_clock_hours=12`, `plateau_window=30`. At ~$0.10/iter that is ≤$20 API cost. Per-iter walltime is dominated by build+test+microbench: realistic range 3–5 min/iter, so 200 iters is 10–17 h — `wall_clock_hours=12` may bite before `max_iterations`; that's intentional. Re-tune after the first real run.

### 8.7 "Done" criteria

A run produces a `best/` symlink → winning variant. User reads `summary.md` + `proposal.json` for the winner, decides whether to trust the change, and **manually** applies it via a normal commit + PR. The loop does not auto-merge.

---

## 9. Out of scope for v1

- Parallel variant execution (revisit once sequential loop has run end-to-end).
- Auto-merge / auto-PR of winning variant.
- Population/island evolutionary search.
- Loom run on every variant (opt-in only).
- Fuzzer integration.
- TUI dashboard.
- Multi-language wire-format preservation during optimization runs.
- Generalizing the `OptimizationTask` trait beyond what shmem demands.

---

## 10. Open risks and mitigations

| Risk | Mitigation |
|------|------------|
| Goodhart's law: LLM exploits microbench, regresses real use | E2E gate with 5% regression threshold; explicit `goodhart_reject` outcome fed back to LLM |
| Torture suite has a gap → loop ships a broken ring | Treat torture suite as the most important code in the framework; review carefully; consider loom on the final champion |
| Macos noise floor too high to detect small wins | 9 measured runs, median, 2σ threshold; consider running final shootout on Linux box if available |
| API costs spiral | Iteration count cap + plateau detection; cost-per-iter logged; user can Ctrl-C |
| Plateau detection misses local optima | Temperature schedule + diverse-leader sampling; final fallback is manual inspection of summary.md |
| Resume after crash loses an in-flight variant | Events written before work; `RESUMED_ABORTED` outcome marks it, loop continues |
| LLM proposes API-breaking change | `frozen_paths` hard-reject in `static_checks` before any build |

---

## 11. Out-of-the-box tasks beyond shmem

Tasks reasonable to add once the framework has run shmem end-to-end (each one is its own brainstorm/spec cycle):

- `ultima_db` B-tree node layout (key density, split heuristics, cache-line packing).
- `ultima_journal` group-commit batching parameters and fsync strategy.
- `uc_node` apply-pipeline batching (how many committed entries to coalesce per `ApplyFrame`).
- `uc_node` openraft replication tuning (network frame batching, pipeline depth).

These are listed only to verify the framework's reuse story makes sense — none are committed work.

---

## 12. Framework documentation — `uc_autobench/CLAUDE.md`

A generic doc that lives in the crate root and is loaded automatically by Claude Code when working under `uc_autobench/`. It is the *user manual* for the framework — not a re-statement of this spec, not shmem-specific. It should answer: how do I run a task, how do I add a task, how do I read a run's output, what failure modes mean what, when not to use the framework.

### 12.1 Required sections

1. **What this crate is** — one-paragraph overview: leaderboard-driven LLM optimization loop with subprocess sandboxing and Goodhart-resistant gates.
2. **Running an existing task** — `cargo run -p uc_autobench -- --task <id>`, env vars (`ANTHROPIC_API_KEY`), where output lands, how to resume (`--resume <run-id>`), how to Ctrl-C cleanly.
3. **Reading a run** — directory layout under `auto-bench-runs/`, what each file means, how to find the winning variant, what `summary.md` tells you, how to interpret `goodhart_reject`.
4. **Adding a new task** — points at the `create-autobench-task` skill (see §13) as the supported entry point, plus the underlying mechanics in case the skill isn't being used:
   - Author `tasks/<id>/task.toml` (link the schema).
   - Implement `OptimizationTask` in `tasks/<id>/mod.rs`.
   - Write `<id>-microbench.rs` + `<id>-e2e.rs` binaries — these are **task-author-owned, never LLM-edited**.
   - Write a torture/conformance suite under `tests/<id>_torture.rs`.
   - Register the task in `bin/auto-bench.rs`.
5. **The contract concept** — what `mutable_paths` / `frozen_paths` mean, the three `contract.mode` values, why the wire-layout one matters for tasks with multi-language consumers.
6. **The Goodhart gate** — why every task should have one; what regression threshold to pick (the shmem run will calibrate this); cost of skipping it.
7. **Cost & runtime expectations** — typical $/run, typical hours/run, how to lower both (smaller `max_iterations`, cheaper microbench).
8. **Failure modes & their fixes** — table mapping each event-log outcome (`static_reject`, `test_fail`, `bench_regression`, `goodhart_reject`, `RESUMED_ABORTED`) to "what went wrong" + "what to look at".
9. **When NOT to use the framework** — for problems where (a) you don't have a fast fitness function, (b) correctness is not crisply verifiable by tests, (c) the search space has 1 obvious answer, (d) you don't trust your own benchmarks. Be honest — autoresearch is not a hammer for every nail.
10. **Conventions** — naming (`<id>-microbench`, `<id>-e2e`, `<id>_torture`), JSON metric schema, run-id format.
11. **Pointers** — back to this spec (`docs/superpowers/specs/2026-05-24-uc-autobench-design.md`) for the rationale, and to the per-task `task.toml`s for the configured tasks.

### 12.2 What this doc must NOT do

- No prescriptive tuning advice (every task is different).
- No restating of section 1–10 of this spec — link instead.
- No shmem-specific details — those live in `tasks/shmem/`.

---

## 13. Skill: `create-autobench-task`

A project-local skill at `.claude/skills/create-autobench-task/SKILL.md` that scaffolds a new optimization task end-to-end so the user doesn't have to remember the seven steps.

### 13.1 Trigger

Frontmatter description targets phrases like "create a new auto-optimization task", "scaffold uc_autobench task for X", "add an autobench task for the ultima_db btree". Project-local — won't fire in unrelated repos.

### 13.2 Inputs (conversational)

The skill asks the user (one question at a time, brainstorming-style — but lighter than full `brainstorming`):

1. **Task id and short description** — e.g. `udb-btree`, "Optimize ultima_db B-tree node layout".
2. **Contract mode** — `rust_api` / `rust_api_plus_wire` / `behavior_only` (with explanations).
3. **Mutable + frozen paths** — the skill suggests sensible defaults by scanning the user-named target module.
4. **Primary microbench metric** — what scalar to minimize/maximize.
5. **Is there an e2e Goodhart gate?** — if yes, what binary + metric + regression threshold; if no, the skill explicitly notes the Goodhart risk in the generated task.
6. **Budget** — `max_iterations`, `wall_clock_hours`, `plateau_window` (offer defaults).

### 13.3 Outputs (files created)

- `uc_autobench/tasks/<id>/task.toml` — populated from inputs.
- `uc_autobench/tasks/<id>/mod.rs` — stub `OptimizationTask` impl with TODO bodies for `parse_microbench` and `parse_e2e`.
- `uc_autobench/src/bin/<id>-microbench.rs` — runnable stub printing zeros (so the harness wires through end-to-end even before the bench is written).
- `uc_autobench/src/bin/<id>-e2e.rs` — same, if e2e gate selected.
- `uc_autobench/tests/<id>_torture.rs` — stub with one trivial passing test and a `// TODO:` checklist of conformance tests the user must add.
- Edit `uc_autobench/src/bin/auto-bench.rs` to register the new task.
- Edit `uc_autobench/Cargo.toml` to add the new binaries.

### 13.4 Closing checklist the skill prints to the user

> Task `<id>` scaffolded. Before running:
> 1. Implement the conformance suite in `tests/<id>_torture.rs` — **this is the correctness floor; do not skip.**
> 2. Implement the microbench in `bin/<id>-microbench.rs` — make sure JSON keys match `task.toml`'s `metrics`.
> 3. (If gated) implement the e2e binary.
> 4. Run `cargo test -p uc_autobench --test <id>_torture` and `cargo run -p uc_autobench --bin <id>-microbench` once by hand to verify wiring.
> 5. Then: `cargo run -p uc_autobench -- --task <id>`.

### 13.5 Why this is a skill, not a `cargo generate` template

A skill lets Claude tailor each file to the user's actual target module (read it, propose sensible mutable/frozen path lists, generate idiomatic stubs). A static template can't do that.

---

## 14. Post-shmem framework retrospective

The framework was designed from imagined requirements. After the first real shmem run, **explicitly stop and re-evaluate** before adding more tasks. This is the "let one real run inform the framework" step that was implicit in the primary-goal choice (ship faster shmem first; generalize later from lessons learned).

### 14.1 When

After the shmem run terminates (whether by budget, plateau, or manual stop) **and** the winning variant has been reviewed and either merged or rejected by the user.

### 14.2 Workflow

Drives a new design doc `docs/superpowers/specs/<date>-uc-autobench-v1-retro.md` covering, in order:

1. **Did the framework actually produce a useful win?**
   - Primary metric: % improvement of best vs baseline on `spsc_p99_ns` (and other metrics).
   - Subjective: was the winning variant something a human would have proposed? Was its hypothesis insightful, or trivial?
   - Per-iteration cost in $ and wall-clock vs. the design's predictions (3–5 min/iter, ~$0.10/iter).

2. **Where did the gate machinery succeed / fail?**
   - How many variants were `static_reject` / `test_fail` / `bench_regression` / `goodhart_reject` / `promoted`? If `static_reject` rate >30%, the contract communication is unclear. If `goodhart_reject` is zero, either the e2e gate is too lenient or the LLM is well-aligned; both worth probing.
   - Did the torture suite catch anything that `cargo test -p uc_protocol` didn't? If not, was it a useless suite or an unusually well-behaved LLM?
   - Did any timeout-killed variant indicate a gap (e.g. a real deadlock pattern worth a regression test)?

3. **Where did the LLM proposal quality fall short?**
   - Read the top-20 leaderboard's `hypothesis` fields. Are they diverse? Did the diverse-leader sampling actually surface different approaches, or did the loop converge early?
   - Were the rationales used in subsequent prompts? Look for variants that explicitly built on prior ones.
   - Did temperature warm-up help, or did it just produce noise?

4. **What was *not* in the spec that turned out to matter?**
   - Examples of plausible misses: per-platform bench harness needed (Linux vs macOS), need for a "best-of-N" microbench within a single variant to fight noise, need for a separate "stability check" iteration where the best variant is re-benched 10× to confirm.

5. **What was *in* the spec that turned out to be wasted complexity?**
   - Candidates to demote: AST-shape hash if the cheap normalized hash worked fine; the `behavior_only` contract mode if nobody used it; the temperature schedule if it didn't move the needle.

6. **Proposed framework v1.1 changes**
   - Concrete deltas to the framework: trait additions, prompt revisions, gate ordering, default budgets, new event kinds.
   - Each delta tied back to a specific observation from §14.2.1–§14.2.5.

7. **Decision on next task**
   - Is the framework ready for a second task (e.g. ultima_db B-tree), or does v1.1 need to land first?

### 14.3 Tooling

The retrospective is driven by the **same** brainstorming + writing-plans flow as any other feature. There is no special "retro" skill — that would be over-engineering. The trigger is just the user opening this section of this spec and saying "do the retro".

The retrospective itself should produce its own follow-up plan (if any framework changes are made) and its own commit to `docs/tasks/`.

---

## 15. Next step

After spec approval: invoke `writing-plans` to break this design into an implementation plan with verifiable checkpoints. The plan must distinguish four work tranches with clear boundaries:

1. **Framework core** (sections 2–7): orchestrator, sandbox, persistence, prompt machinery, leaderboard.
2. **Shmem task instantiation** (section 8): the shmem-specific binaries, torture suite, task.toml.
3. **Documentation & tooling** (sections 12–13): `uc_autobench/CLAUDE.md`, the `create-autobench-task` skill.
4. **Retrospective** (section 14): scheduled, but only executed after the first real shmem run completes.

Tranches 1–3 are the v1 shipping scope. Tranche 4 is a deferred milestone gating v1.1.
