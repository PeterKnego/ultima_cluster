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

- Primary: `spsc_p99_ns` (minimize).
- Secondary: `spsc_throughput_msgs` (maximize). A variant is a KEEP if it
  clearly beats the current best on EITHER `spsc_p99_ns` OR
  `spsc_throughput_msgs` (beyond run-to-run noise) **without regressing the
  other** beyond noise. Latency-only and throughput-only wins both count.
- Goodhart gate: `submit_to_resp_p99_ns` from shmem-e2e
  (5% regression tolerance vs current branch best).
- Floor: ring_torture must pass (6 tests, zero failures).
- Noise: `spsc_p99_ns` has large between-process variance (~14%); compare
  MEDIAN-of-5 (or more), not single samples.

## TSV schema

`uc_autobench/tasks/shmem/results.tsv`, tab-separated:

    commit	spsc_p99_ns	spsc_throughput_msgs	e2e_p99_ns	memory_kb	status	description

Statuses: keep, discard, crash. Use 0 for metrics that didn't run / weren't
measured. Metric values are median-of-N where noted in the description.

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
