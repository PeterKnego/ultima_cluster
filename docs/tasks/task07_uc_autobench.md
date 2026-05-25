# Task 07 — uc_autobench: auto-optimization framework + shmem task

**Status:** Framework + shmem-task scaffolding shipped on branch `auto_bench_shmem`. The first real shmem optimization run has **not** yet been executed (it requires `ANTHROPIC_API_KEY` and incurs API cost; see "Running the first shmem run" below).

`uc_autobench` is a leaderboard+hypothesis LLM optimization loop ("autoresearcher" in the spirit of Karpathy's `autoresearch`): each iteration the LLM proposes a full rewrite of the target files, the harness builds + runs a frozen correctness suite + a microbench in sandboxed subprocesses, and a variant is promoted only if it beats the current best and passes an end-to-end Goodhart gate. Its first task optimizes the `uc_protocol` shmem ring buffers.

## Working artifacts (not yet consolidated here)

Per the project workflow (consolidate into `docs/tasks/` and delete the superpowers scaffolding once the feature ships), these will be folded into this file after the first real run + the framework retrospective land:

- Design spec: `docs/superpowers/specs/2026-05-24-uc-autobench-design.md`
- Implementation plan: `docs/superpowers/plans/2026-05-24-uc-autobench.md`

## Code shipped

- `uc_autobench/` crate:
  - Orchestrator state machine (`src/orchestrator.rs`), task spec + trait (`src/task.rs`), proposal apply/revert + static checks (`src/proposal.rs`), subprocess sandbox with hard timeout (`src/sandbox.rs`), append-only JSONL event log (`src/persist.rs`), top-K diversity-aware leaderboard (`src/leaderboard.rs`), prompt rendering (`src/prompt.rs`), Anthropic tool-use client + deterministic stub (`src/llm.rs`), outcome/event types (`src/outcome.rs`).
  - Shmem task: `src/tasks/shmem.rs` + `tasks/shmem/task.toml`.
  - Fitness binaries (human-owned, never LLM-edited): `src/bin/shmem-microbench.rs` (8 metrics, batched-sample sub-tick latency), `src/bin/shmem-e2e.rs` (Goodhart gate via in-process node+service+4 clients).
  - Correctness floor: `tests/ring_torture.rs` (6 behavioral conformance tests against the public ring API; frozen).
  - CLI: `src/bin/auto-bench.rs`.
  - Operator manual: `uc_autobench/CLAUDE.md`.
- `uc_node/src/test_support.rs` — `ClusterFixture` (behind the `test-support` feature), extracted from the M4 integration tests and reused by `shmem-e2e`.
- `.claude/skills/create-autobench-task/` — skill for scaffolding additional optimization tasks.

## Running the first shmem run

```bash
export ANTHROPIC_API_KEY=sk-ant-...
cargo run -p uc_autobench --bin auto-bench --release -- --task shmem
```

Artifacts land under `auto-bench-runs/shmem-rings/<run-id>/` (gitignored). Read `summary.md` for the leaderboard and `variants/<best>/proposal.json` for the winning hypothesis. The loop never auto-merges — a human reviews and applies the winner.

## Known limitations / retrospective inputs

These were surfaced during implementation and should be revisited in the framework retrospective (the deferred Tranche 4 / future task):

- **The e2e Goodhart gate is Raft-commit-dominated.** End-to-end submit→response latency (~38 ms) is dominated by the journal group-commit window, not by ns-scale shmem ring time. So the e2e gate functions as a *throughput-collapse / correctness guard*, not a sensitive latency-regression detector for shmem changes. The microbench is the real fitness signal for the ring; the e2e gate's default sample is small (2k round-trips) accordingly.
- **Microbench latency resolution.** The host monotonic clock is coarse (~42 ns), so latency sub-benches use batched-sample timing (per-batch mean over many samples) to get sub-tick resolution. Run-to-run `spsc_p99_ns` is sensitive to background machine load; for trustworthy comparisons the loop should run benches without concurrent compilation.
- **Not yet implemented (deferred to v1.1):** `--resume` of a prior run (the event log is replayable, the CLI surface is not wired), a `leaderboard.jsonl` snapshot file (the leaderboard is in memory + reflected in `summary.md`), a `best/` symlink, Ctrl-C clean shutdown, parallel variant execution, opt-in loom verification of the champion, and core-pinning for bench reproducibility.

## Out of scope here

- Tranche 4: post-shmem framework retrospective + framework v1.1. Its own future task, executed after the first real shmem run produces evidence.
