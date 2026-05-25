# uc_autobench

Leaderboard+hypothesis LLM optimization loop for ultima_cluster / ultima_db. Subprocess-sandboxed, Goodhart-resistant via two-tier (microbench + e2e) gating.

For design rationale, see `../docs/superpowers/specs/2026-05-24-uc-autobench-design.md`.

## Running an existing task

```bash
export ANTHROPIC_API_KEY=sk-ant-...
cargo run -p uc_autobench --bin auto-bench --release -- --task <id>
```

Optional: set `UC_AUTOBENCH_MODEL` to override the model (defaults to an Opus build).

Output lands under `auto-bench-runs/<task-id>/<run-id>/`. The run id is an ISO timestamp.

Resume a prior run (planned, not in v1 — the CLI currently rejects `--resume`):
```bash
cargo run -p uc_autobench --bin auto-bench -- --task <id> --resume <run-id>
```

## Reading a run

```
auto-bench-runs/
└── <task-id>/
    └── <run-id>/
        ├── events.jsonl        canonical log (tail -f | jq)
        ├── task.toml.snapshot  the task spec this run used
        ├── git.head            repo HEAD at run start
        ├── summary.md          human-readable status (rewritten each iteration)
        └── variants/
            └── NNNN-<slug>/
                ├── proposal.json  LLM's hypothesis + rationale + files
                ├── outcome.json   {status, microbench, e2e?}
                └── logs/          <name>.log per subprocess (cargo-test,
                                   ring-torture, microbench, e2e)
```

The log file names mirror the gate/bench step that produced them and therefore depend on the task's `task.toml`. Two planned artifacts are **not** written by v1: a derived `leaderboard.jsonl` and a `best/` symlink — read the leaderboard from `summary.md` and the winner from its `variants/NNNN-<slug>/` entry instead.

- **What won?** The current best is named in `summary.md`; open its `variants/NNNN-<slug>/proposal.json` and read `hypothesis` + `rationale`.
- **Why was variant X rejected?** `variants/X/outcome.json`.status + `logs/` for raw output.
- **What did the loop try?** `events.jsonl` is the truth; `summary.md` is friendlier.

## Adding a new task

Use the project skill:

```
/create-autobench-task
```

It scaffolds `tasks/<id>/`, `src/tasks/<id>.rs`, `src/bin/<id>-microbench.rs`, optional `src/bin/<id>-e2e.rs`, and `tests/<id>_torture.rs`. Then implement them — the skill prints a closing checklist.

Manual steps if you skip the skill:
1. `tasks/<id>/task.toml` — see `tasks/shmem/task.toml` as the reference schema.
2. `src/tasks/<id>.rs` — implement `OptimizationTask`.
3. `src/bin/<id>-microbench.rs` — emit one JSON line; keys must match `metrics` in `task.toml`.
4. (Optional) `src/bin/<id>-e2e.rs` — Goodhart gate.
5. `tests/<id>_torture.rs` — behavioral conformance suite. **Do not skip this.**
6. Register the task in `src/bin/auto-bench.rs`.

## The contract

`contract.mode` chooses the freedom level given to the LLM:

| Mode | LLM may change | LLM must preserve |
|------|---------------|-------------------|
| `rust_api` | Internal layout, on-disk format, framing | Public Rust API of mutable_paths |
| `rust_api_plus_wire` | Internal layout only | Public Rust API + on-disk byte layout (verified by torture suite) |
| `behavior_only` | Public Rust API too (orchestrator regenerates adapters) | Behavior (verified by torture suite) |

`frozen_paths` is a hard reject pre-build. `mutable_paths` is what the LLM is shown as "current state".

## The Goodhart gate

Every task should have an `e2e_gate`. Without it, the LLM will exploit any quirk of the microbench. The gate runs only on variants that beat current best, so its cost amortizes.

If your task genuinely cannot be e2e-benched, document why in the task's `extra_prompt_context()` so the LLM is at least asked to avoid known cheap-shots.

## Cost & runtime expectations

These are rough estimates — measure your own task.

- **API cost:** ~$0.05–$0.15 per iteration with Opus.
- **Walltime:** dominated by build+test+microbench. Estimate 3–5 min/iter for shmem; varies by task.
- **Total run:** 200 iterations × 3–5 min ≈ 10–17 h. The `wall_clock_hours` budget often bites first.

Lower cost by reducing `max_iterations` or by writing a faster microbench (the LLM doesn't need 1M-sample p99 to learn).

## Failure modes

The `status` field in `outcome.json` (and in the `outcome` of each `outcome_recorded` event) is one of:

| Outcome | Meaning | Where to look |
|---------|---------|---------------|
| `static_reject` | LLM touched a frozen path or path outside `mutable_paths` | `outcome.json`.reason; tighten/clarify the system prompt if rate >30% |
| `test_fail` | `cargo test` or torture suite failed; OR a subprocess timed out | `logs/cargo-test.log`, `logs/ring-torture.log` |
| `bench_regression` | Microbench ran but didn't beat current best by > noise threshold | Normal — most variants land here |
| `goodhart_reject` | Won microbench but e2e regressed > regress_pct | Microbench is being exploited; consider strengthening the gate or e2e bench |
| `promoted` | Beat current best (and passed e2e if configured) | The new winner; `proposal.json` has the hypothesis |
| `resumed_aborted` | Iteration was killed mid-flight by a crash/Ctrl-C; resume found the dangling state | Diagnostic only; loop continues |

## When NOT to use this framework

- You don't have a fast, deterministic fitness function.
- Correctness isn't crisply verifiable by tests.
- The search space has one obvious answer — write it by hand.
- You don't trust your own benchmarks. **The loop will optimize whatever you measure**, including measurement noise.

## Conventions

- Microbench binary name: `<task-id>-microbench`.
- E2E binary name: `<task-id>-e2e`.
- Torture test file: `tests/<task-id>_torture.rs`.
- All bench binaries emit one JSON line on stdout; everything else goes to stderr.
- Metric keys in JSON output must match `task.toml`'s `metrics` exactly (typo = silent NaN).

## Pointers

- **Design:** `docs/superpowers/specs/2026-05-24-uc-autobench-design.md`
- **First task config:** `tasks/shmem/task.toml`
- **First task impl:** `src/tasks/shmem.rs`
