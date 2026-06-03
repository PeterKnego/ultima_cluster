# Task template

Every optimization task lives in `uc_autobench/tasks/<task>/` and is registered
by adding one `TaskSpec` row in `uc_autobench/src/task_spec.rs`.

## Files per task

- `program.md` — mutable paths, frozen paths, the primary/secondary metrics,
  the TSV schema, and task-specific constraints. Modeled on `tasks/shmem/program.md`.
- `results.tsv` — committed run log, tab-separated. First column `commit`, last
  two columns `status` (keep|discard|crash) and `description`. Metric columns in
  between. Integer nanoseconds only; values are median-of-N (note N in the
  description).

## Conventions (all tasks)

- Integer ns baselines; median-of-5 for latency, median-of-9 for throughput.
- Warmup + fixed iteration counts; never single-sample a noisy percentile.
- `current_thread` tokio runtime for any in-process fixture (multi_thread flakes
  the shmem handshake).
- No `Date`/wall-clock and no `rand` in bench logic; vary by index.
- The frame CRC is never removed to win a number (Goodhart trap).
- A change is KEEP only if it beats the champion beyond run-to-run noise on the
  primary metric without regressing the secondary or the Goodhart gate.

## Registering a task

Add to `task_spec()`:

    "journal" => Some(TaskSpec {
        task: "journal",
        microbench_bin: "journal-microbench",
        primary_metric: "fsync_p99_ns",
        gate_bin: Some("shmem-e2e"),
        gate_metric: Some("submit_to_resp_p99_ns"),
    }),
