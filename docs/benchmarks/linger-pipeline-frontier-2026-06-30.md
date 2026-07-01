# Linger throughput/latency frontier — 2026-06-30

Follow-on to `throughput-knee-attribution-2026-06-30.md` (which found the 5k knee is
linger(2×) × replication(2×), NOT fsync). This maps the `api_batch_linger_ms` frontier to pick
the SLA-optimal default. **Result: `linger=2ms` is a Pareto win over the current `5ms` — 2×
throughput AND lower latency.** (Partial run — the deeper-pipeline cells were lost to an infra
hang; see Caveats.)

## Setup

3× AWS `c6id.2xlarge`, default RaftCore binary, 3-node + consistent (production config), rate
ladder [100..20000], inflight 128, 2 reps/cell. Cells were linger ∈ {5,2,1} at pipeline
depth=8; the depth-32 and linger=0 cells did not complete (hang). Per-rate p50/p99 from the
sweep CSVs (`frontier_results/`).

## Frontier (achieved throughput, p50/p99 in ms; rep1, rep2 consistent)

| offered | linger=5 (current default) | linger=2 | linger=1 |
|---|---|---|---|
| 5,000 | thr 4999, p50 10.0, p99 92 | thr 4999, **p50 4.7, p99 82** | thr 4999, p50 2.9, p99 83 |
| 10,000 | thr 7649, **p50 1398, p99 3070 (collapse)** | thr 9953, **p50 18.9, p99 134** ✓ | thr 9999, p50 11.6, p99 202 ✓ |
| 15,000 | collapsed | thr 10824, collapsed | thr 13325, collapsed |
| **knee** | **5,000** @ p99 92ms | **10,000** @ p99 134ms | **10,000** @ p99 202ms |

## Verdict — set `api_batch_linger_ms` default 5 → 2

`linger=2ms` **strictly dominates** the current `linger=5ms`:
- **2× throughput knee** (5k→10k). At 10k, linger=5 collapses (p99 3.07 s) while linger=2
  holds p99 134 ms.
- **Lower latency at every load**: at 5k, p50 4.7 ms vs 10.0 ms and p99 82 ms vs 92 ms. The
  5 ms linger is dead wait — each message sits up to 5 ms filling a batch.

It is a Pareto improvement (not a latency/throughput trade) because linger's only benefit is
fsync/proposal batching, and the attribution proved **fsync is not the bottleneck** — so the
batching the linger buys is worthless here while its latency cost is real.

`linger=1` gives no extra throughput over `=2` (both hit the 10k replication wall) and a worse
tail at the 10k knee (p99 202 vs 134 ms). So **`linger=2 ms` is the sweet spot** — the
recommended new default. (Latency-critical low-load deployments could go to 1 ms for the
slightly lower p50, at a worse high-load tail.)

Implementation: `uc_node/src/runtime/builder.rs` `API_BATCH_LINGER_MS_DEFAULT: 5 → 2`. Pure
timing; correctness-neutral (the lincheck/partition suites should be unaffected). The
`UC_API_BATCH_LINGER_MS` env override already exists for rollback.

## What's still open (lost to the hang)

- **Depth cells (linger=2/0 × pipeline_depth=32) did not run.** So whether deeper pipelining
  breaks the **10k replication wall** (the second 2× from the attribution) is untested. That
  is the next experiment: at linger=2, sweep `UC_PIPELINE_DEPTH` {8,16,32,64} to see if the
  knee climbs past 10k toward the ~20k structural ceiling.

## Infra lesson — fleet driver needs a hard timeout

The `linger=0` cell **hung** in ansible `wait_for_connection` (~40 min, no progress). The
driver's guaranteed-destroy trap fires on script *exit* — but a hung script never exits, so the
cost guard fell back to manual detection (the monitor timing out) + a force `make destroy`. No
leak occurred, but future fleet drivers must wrap the work in a hard overall `timeout` (e.g.
`timeout 2400 ...`) so a hang self-terminates into the teardown trap. Filed as a bench-infra
follow-up. (Separately: linger=0 *may* have destabilized the cluster — unconfirmed; another
reason `linger=2`, not `0`, is the right default.)

## Status

Fleet destroyed (0 resources, no leak). The linger recommendation (5→2) is solid on the
collected data; the pipeline-depth question is open.
