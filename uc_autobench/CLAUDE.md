# uc_autobench

Claude-Code-driven autoresearch loop for ultima_cluster / ultima_db. Karpathy/autoresearch shape: Claude Code (you) edits a target file, runs a fixed harness (`run-iter`), commits or reverts via plain git, and iterates indefinitely.

For design rationale, see `../docs/superpowers/specs/2026-05-29-uc-autobench-cc-driven-design.md`.

## Running a task

Start a new run interactively from Claude Code:

```
Read uc_autobench/program.md and uc_autobench/tasks/shmem/program.md, then start a run.
```

That's it. There is no `auto-bench` binary, no API key, no orchestration daemon. The loop is the conversation: Claude Code reads `program.md`, executes the loop, the human watches and interrupts when satisfied.

## Reading a run

A run is one branch (`autoresearch/<task>-<tag>`) and one TSV (`tasks/<task>/results.tsv`).

- **What won?** The current best: the row in `results.tsv` with the lowest primary metric (`spsc_p99_ns` for shmem) and `status=keep`. The commit hash points to the source state.
- **Why was variant X rejected?** Read the row with `status=discard` or `status=crash` and the matching commit message in `git log`.
- **What did the loop try?** Tail the TSV: every iteration appended a row.

## Adding a new task

There is no scaffolding skill (yet). To add task `<id>`:

1. Write `tasks/<id>/program.md` — task overlay (see `tasks/shmem/program.md` as a reference).
2. Add a per-task fitness binary `src/bin/<id>-microbench.rs` (emits one JSON object on stdout, keys per the metric list in the overlay).
3. Add a per-task Goodhart gate `src/bin/<id>-e2e.rs` (also JSON-emitting). Skip if your task genuinely cannot be e2e-benched, but say so explicitly in the task overlay.
4. Add a frozen behavioral suite `tests/<id>_torture.rs`. **Do not skip this.**
5. Extend `src/bin/run-iter.rs`'s `--task` match arm to dispatch to the new bench binaries.
6. Initialize `tasks/<id>/results.tsv` with the header row matching the TSV schema declared in the overlay.

## The `run-iter` consolidation helper

```
cargo run -p uc_autobench --bin run-iter --release -- \
  --task shmem --json \
  --baseline-spsc-p99-ns <n> --baseline-e2e-p99-ns <n>
```

Drives build → ring_torture → shmem-microbench → conditional shmem-e2e. Emits one JSON object on stdout. The e2e gate is skipped if the microbench result is clearly not a winner (>5% over baseline). See the design spec §4 for the full output schema.

## The Goodhart gate

Every task should have an e2e gate. Without it, the LLM (you) will exploit any quirk of the microbench. The gate runs only on variants that microbench-plausibly win, so the cost amortizes.

If your task genuinely cannot be e2e-benched, document why in the task overlay so the loop is at least asked to avoid known cheap-shots.

## Cost & runtime expectations

- **API cost:** $0 — the loop uses your existing Claude Code subscription.
- **Walltime per iter:** dominated by build + test + microbench + e2e. ~2–4 min/iter for shmem, varies by task.

## Failure modes

The `status` field in the run-iter JSON output is one of:

| Status | Meaning | Action |
|--------|---------|--------|
| `pass` | All stages ran; check `gate.e2e_passed` and `metrics.spsc_p99_ns` to decide keep/discard |
| `build_failed` | `cargo build` failed | Read `stderr_tail`; usually a syntax error |
| `torture_failed` | `ring_torture` correctness suite failed | The proposal broke ring semantics; revert |
| `microbench_failed` | Microbench spawn/exit/JSON-parse failed | Read `stderr_tail`; if it's a panic in your edited code, that's a crash |
| `e2e_failed` | Same as above for the e2e gate | Usually a panic in the in-process node |
| `timeout` | Some stage exceeded its hard wall-clock budget | Stage is in `stage`; either your code spins forever or the timeout needs raising |

`build_failed` / `torture_failed` / `microbench_failed` / `e2e_failed` / `timeout` all map to TSV `status=crash`. `pass` with no improvement or failed gate maps to `status=discard`. `pass` with improvement + passed gate maps to `status=keep`.

## When NOT to use this framework

- You don't have a fast, deterministic fitness function.
- Correctness isn't crisply verifiable by tests.
- The search space has one obvious answer — write it by hand.
- You don't trust your own benchmarks. **The loop will optimize whatever you measure**, including measurement noise.

## Conventions

- Microbench binary name: `<task>-microbench`.
- E2E binary name: `<task>-e2e`.
- Torture test file: `tests/<task>_torture.rs`.
- All bench binaries emit one JSON object on stdout; everything else goes to stderr.
- Metric keys in JSON output must match what the task overlay declares (typo = silent miss).

## Pointers

- **Design:** `../docs/superpowers/specs/2026-05-29-uc-autobench-cc-driven-design.md`
- **Loop spec:** `program.md`
- **First task overlay:** `tasks/shmem/program.md`
