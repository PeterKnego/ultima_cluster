# Task overlay: uc-throughput

Optimize UC's **3-node distributed throughput ceiling**, measured on a persistent
`bench_infra` cloud fleet (UC-only). See
`../../../docs/superpowers/specs/2026-06-16-uc-autobench-distributed-throughput-loop-design.md`.

## Prerequisite (human, once per session)
A UC-only cloud fleet must be UP before the loop runs:
`cd ../../bench-infra && make up-uc`  (and `make destroy` when done).

## The loop
Per iteration: edit a mutable path, then run the driver:
`bash uc_autobench/scripts/uc-throughput-iter.sh > /tmp/uc-iter.json`
Parse `jq '.status, .metrics, .gate' /tmp/uc-iter.json` and:
- status=="pass" AND `uc_throughput_msgs` beats current best beyond noise -> KEEP (commit, append TSV row).
- status=="pass" but no improvement -> DISCARD (revert mutable paths, append row).
- status in {build_failed, lincheck_failed} -> DISCARD (revert, append row). No cloud spend was incurred.

## Mutable paths (the throughput lever -- task13 section 6: submit/apply pipeline)
- uc_node/src/runtime/builder.rs        (openraft Config: api_batch_capacity, max_payload_entries, api_batch_linger_ms)
- uc_node/src/ipc/                       (client_dispatcher, apply-ring publish/enqueue, wakeup path)
- uc_node/src/raft/                      (apply pipeline, log_storage append batching)
Do NOT edit uc_protocol/src/ring/ (that is the `shmem` task's domain).

## Metrics
- Primary: `uc_throughput_msgs` (maximize) -- max achieved_rate across the ladder.
- Secondary/observability: `knee_rate`, `p99_at_knee_ms` (not gated).
- Correctness gate: lincheck capstone `linearizable_under_failover` MUST pass -- a
  throughput win that breaks linearizability is discarded before any cloud spend.

## Noise
Cloud `achieved_rate` has run-to-run variance; treat changes within ~5% as noise.

## TSV schema (results.tsv)
```
commit	uc_throughput_msgs	knee_rate	p99_at_knee_ms	lincheck_passed	status	description
```
Statuses: keep, discard, crash. Use 0 for metrics that didn't run.
