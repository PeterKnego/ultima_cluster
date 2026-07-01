# Pipeline-depth sweep — the 10k wall is structural, not depth-bound — 2026-06-30

Closes the open question from `linger-pipeline-frontier-2026-06-30.md`: does deeper
replication pipelining break the **10k replication wall** (the second 2× from the knee
attribution) toward the ~20k structural ceiling? **Answer: no.** Depth 8→64 leaves the knee
pinned at 10k. The second 2× is structural (replication latency), not a config knob.

## Setup

3× AWS `c6id.2xlarge`, default RaftCore binary, 3-node + consistent, **linger=2ms** (the new
default, set explicitly via `-e` since the bench group_var is still 5), rate ladder
[100..20000], inflight 128, 2 reps/cell. Swept `UC_PIPELINE_DEPTH` ∈ {8,16,32,64}. Driver was
the hang-hardened version (per-op `timeout` + watchdog) — see "Infra" below.

## Results

| depth | knee (msg/s) | p99@knee (ms), rep1/rep2 |
|---|---|---|
| 8  | **10,000** | 342 / 140 |
| 16 | — (intermittent relaunch hang, both reps; lost) | — |
| 32 | **10,000** | 222 / 218 |
| 64 | **10,000** | 316 / 133 |

## Verdict

- **Depth does not move the knee.** 8, 32, and 64 all cap at exactly 10k msg/s. Deeper
  pipelining buys zero throughput headroom.
- **It does not help the tail either** (p99@knee is noisy, ~130–340 ms, with no improving trend
  — if anything deeper depth adds queueing). So there is no reason to raise the default depth.
- **The 10k wall is structural — the replication *latency* itself**, not the number of
  in-flight AppendEntries. The throughput model is throughput ≈ inflight / commit-latency;
  the commit-latency's replication term is the cross-host RTT + the 3-process IPC hops, and
  adding pipeline depth does not shorten that term. Recovering the second 2× (10k→~20k)
  therefore requires **reducing replication latency**: co-location (collapse the 3-proc IPC
  boundaries), fewer hops, or a lower-RTT transport — an engineering effort, not a knob. (And
  not consensus — the 3c A/B already ruled that out.)

## The full throughput picture (now complete)

- Baseline knee 5k = linger(2×) × replication(2×), NOT fsync, NOT consensus.
- **First 2× — linger (5→2ms): cheap config win, LANDED** (`f262c2f`).
- **Second 2× — replication latency: structural** (co-location / transport), NOT recoverable by
  linger, pipeline depth, fsync, or consensus. The ~20k structural ceiling (1-node, all-off)
  is the upper bound.

## Infra — the hang-hardened driver worked

The `linger=0` cell hung the previous (frontier) run for ~40 min and needed manual rescue
because the destroy trap only fires on script *exit*. This run's driver wrapped every long op
in `timeout -k` + a 75-min watchdog backstop. When the **depth=16 cell hung (both reps)**, each
was killed at the 12-min cap and the run **continued and tore itself down cleanly with no
manual intervention** — the fix is validated. (The d16 hang is an intermittent
cluster-relaunch issue — d32, which is *deeper*, succeeded — not depth-specific; worth a
separate bench-infra look but it no longer threatens cost or completion.)

## Status

Fleet destroyed (0 resources, no leak). 5 session fleets total, all torn down clean.
