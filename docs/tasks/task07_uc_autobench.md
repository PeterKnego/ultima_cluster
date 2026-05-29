# Task 07 — uc_autobench: Claude-Code-driven autoresearch loop + shmem task

**Status:** Reworked on branch `auto_bench_shmem` (supersedes the prior API-driven design). The first real shmem autoresearch run has **not** yet been executed — it requires a human to kick off a Claude Code session, point it at `program.md`, and let it iterate.

`uc_autobench` is a karpathy/autoresearch-shape loop where Claude Code itself is the orchestrator: it reads `uc_autobench/program.md` + `uc_autobench/tasks/<task>/program.md`, edits the mutable target files, runs `cargo run -p uc_autobench --bin run-iter -- --task <task> --json`, parses the consolidated JSON, commits on win and `git checkout --` reverts on loss, appends a row to the committed per-task `results.tsv`, and loops indefinitely until the human interrupts. The first task optimizes the `uc_protocol` shmem ring buffers.

## Why the rework

The original design (`docs/superpowers/specs/2026-05-24-uc-autobench-design.md`) had a Rust orchestrator drive an Anthropic API client directly, which required a separately-billed `ANTHROPIC_API_KEY` outside the human's Claude Code subscription. The reworked design (`docs/superpowers/specs/2026-05-29-uc-autobench-cc-driven-design.md`) has Claude Code itself drive the loop, eliminating the separate API spend and ~2000 lines of orchestrator/LLM-client/proposal/sandbox/leaderboard/persist code in favor of a 50-line markdown loop spec plus a tiny `run-iter` consolidation helper.

## Working artifacts (consolidate, then delete, per project workflow)

Per CLAUDE.md's "Feature Development Workflow", these are ephemeral scaffolding. Once the rework lands and the first real run produces evidence, consolidate the design + plan into this file and delete the scaffolding:

- Design spec: `docs/superpowers/specs/2026-05-29-uc-autobench-cc-driven-design.md`
- Implementation plan: `docs/superpowers/plans/2026-05-29-uc-autobench-cc-driven.md`
- Historical record (do not delete): `docs/superpowers/specs/2026-05-24-uc-autobench-design.md` — the original API-driven design

## Code shipped (post-rework)

- `uc_autobench/` crate:
  - `program.md` — generic loop instructions (operator-facing).
  - `tasks/shmem/program.md` — per-task overlay (mutable paths, metric, TSV schema, task-specific constraints).
  - `tasks/shmem/results.tsv` — committed run log (header-only at init; grows row-per-iter).
  - `src/bin/run-iter.rs` — consolidation helper (build → ring_torture → microbench → conditional e2e gate → one JSON). Drains child stdout/stderr in background threads so verbose binaries (the e2e gate's tracing-subscriber output, in particular) don't block on pipe pressure.
  - `src/bin/shmem-microbench.rs` — frozen fitness binary (8 metrics, batched-sample sub-tick latency).
  - `src/bin/shmem-e2e.rs` — frozen Goodhart gate (in-process node+service+4 clients via the M4 fixture).
  - `tests/ring_torture.rs` — frozen 7-test behavioral conformance suite.
  - `CLAUDE.md` — operator manual.
- `uc_node/src/test_support.rs` — `ClusterFixture` (behind the `test-support` feature), unchanged from the M4 extract; reused by `shmem-e2e`.

## Running the first shmem run

In a Claude Code session at this repo:

```
Read uc_autobench/program.md and uc_autobench/tasks/shmem/program.md, then start a run.
```

Claude Code will propose a run tag, create `autoresearch/shmem-<tag>` from `main`, run a baseline iteration to populate `results.tsv`, and then iterate. The branch + TSV are the only state. Interrupt with Ctrl-C when satisfied; review the winning row's commit; PR to `main`.

## Known limitations / retrospective inputs

These were surfaced during the original implementation and remain true under the rework. They should be revisited after the first real run:

- **The e2e Goodhart gate is Raft-commit-dominated.** End-to-end submit→response latency (~38 ms) is dominated by the journal group-commit window, not by ns-scale shmem ring time. So the e2e gate functions as a *throughput-collapse / correctness guard*, not a sensitive latency-regression detector for shmem changes. The microbench is the real fitness signal for the ring; the e2e gate's default sample is small (2k round-trips) accordingly.
- **Microbench latency resolution.** The host monotonic clock is coarse (~42 ns), so latency sub-benches use batched-sample timing (per-batch mean over many samples) to get sub-tick resolution. Run-to-run `spsc_p99_ns` is sensitive to background machine load; for trustworthy comparisons the loop should run benches without concurrent compilation.
- **Microbench emits floats.** Percentile timings come out as fractional nanoseconds; `run-iter`'s `extract_u64` rounds them so the `u64`-typed `gate_decision` / `regress_pct` interface keeps working. Don't accidentally re-type the metrics as ints in the microbench — float resolution is real.
- **Not yet implemented (deferred):** parallel variant execution (would need per-variant branches), opt-in loom verification of the champion, core-pinning for bench reproducibility, and a task-scaffolding skill (defer until a second task actually exists).

## Out of scope here

- Tranche 4: post-shmem framework retrospective + framework v1.1. Its own future task, executed after the first real shmem run produces evidence.
