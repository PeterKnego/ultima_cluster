# SyncCore-3c vs RaftCore — fleet throughput A/B (real QUIC + fsync), 2026-06-30

The decision-metric measurement: **throughput at bounded tail latency**, on a real 3-host
fleet (dedicated cores per node — no oversubscription confound), after 3c. Verdict:
**3c is throughput-neutral.** The +35–45% inflight-1 latency win does NOT move the throughput
ceiling.

## TL;DR

- **Throughput ceiling (knee at bounded p99): IDENTICAL — 5,000 msg/s for both arms, all 6
  reps.** 3c buys zero throughput headroom.
- **p99 @ knee: SyncCore ~10% WORSE** (median 105 ms vs RaftCore 96 ms).
- The aggregate-throughput figure trends higher for SyncCore but is noise (counts the
  overload rungs above the knee, where latency is unbounded — not an SLA point).
- **Conclusion: on the metric that matters for this server, Model-B/3c is null** (slightly
  negative on tail). The ceiling is gated by group-commit/linger + fsync + 3-proc IPC — the
  structural floor the aeron investigation identified — NOT by the consensus/replication
  choreography that busy-spin removes. Same lesson the project keeps relearning: a clean
  latency microbench win that's null on throughput.
- Secondary positive: the **bench-infra ansible hardening held** — first fully clean denoised
  fleet run (provision + a feature-flip rebuild + 6 sweeps, zero UNREACHABLE/sudo-stall).

## Setup

- 3× AWS `c6id.2xlarge` (8 vCPU + local NVMe), us-east-1, cluster placement group. UC-only
  fleet (`make up-uc`). `durability: consistent` (real `fdatasync` per commit). Rate ladder
  [100, 500, 1000, 2000, 5000, 10000, 20000] msg/s, payload 64B, inflight 128, warmup 2s /
  measure 10s per rung.
- A/B by build feature: RaftCore (default) vs SyncCore-3c (`-e uc_sync_core=true` →
  `--features sync-core`), at 3c HEAD `a6a590ec`. **3 reps/arm, blocked** (RaftCore ×3, then
  one openraft rebuild under the feature, SyncCore ×3). Driver auto-destroyed the fleet on
  completion (11 resources, verified empty state).
- Metric per sweep (`scripts/uc_fitness.py`): `knee_rate` (highest rung holding the p99 SLA),
  `p99_at_knee_ms`, `uc_throughput_msgs` (aggregate).

## Results

| arm | rep | knee (msg/s) | p99@knee (ms) | aggregate |
|---|---|---|---|---|
| RaftCore | 1 | 5000 | 95.6 | 7624.8 |
| RaftCore | 2 | 5000 | 93.6 | 7638.9 |
| RaftCore | 3 | 5000 | 96.6 | 9905.2 |
| SyncCore | 1 | 5000 | 109.2 | 9884.0 |
| SyncCore | 2 | 5000 | 95.9 | 10305.9 |
| SyncCore | 3 | 5000 | 105.3 | 9773.3 |

**Medians:** knee 5000 vs 5000 (identical); p99@knee 95.6 ms vs 105.3 ms (SyncCore +10%);
aggregate 7638.9 vs 9884.0 (noisy — RaftCore's own reps span 7.6k–9.9k).

## Interpretation

- **The knee is the robust throughput metric** (throughput sustainable at bounded p99) and it
  is identical at 5k for every rep of both arms. There is no SyncCore throughput advantage at
  any rung where latency stays bounded.
- **p99@knee is slightly worse under SyncCore** (105 vs 96 ms median). Plausible cause: the
  busy-spin threads (consensus + per-peer network consumers + durability) compete with the
  command-processing/IPC work for the 8 vCPU under sustained load, adding scheduling jitter at
  the tail — the CPU-for-latency trade going the wrong way once the box is busy.
- **The aggregate-throughput edge is not real signal.** It aggregates the overload rungs
  (>knee) where latency has already blown past the SLA; RaftCore rep3 hit 9905 too. Throughput
  at unbounded latency is not a deployable operating point.
- **Why the inflight-1 win didn't translate:** inflight-1 measures the serial critical path
  where per-commit futex choreography is the whole cost. Under throughput load with
  batching/pipelining, that choreography is amortized across a batch and is no longer the
  bottleneck — the bottleneck is group-commit/linger + fsync bandwidth + the 3-process IPC
  hand-offs, none of which 3c touches. So the ceiling is unmoved. This is exactly the
  floor-decomposition prediction and the aeron-investigation conclusion ("p50 is linger-bound;
  real target is the ~10k/s ceiling").

## Implications

- **3c (and the SyncCore/Model-B line as a latency play) does not pay off on throughput.** The
  work is correct (180-suite + UC lincheck/partition green), latency-positive at inflight-1,
  and a clean foundation — but it does not raise the throughput ceiling, which is what a
  throughput-bound SMR server is judged on. The busy-spin CPU cost buys no ceiling and a
  slightly worse tail under load.
- **To move the throughput ceiling, target the actual bottleneck**, not the consensus
  choreography: group-commit/linger tuning, fsync batching/bandwidth, and the 3-process IPC
  hand-off count (e.g. co-location / fewer proc boundaries). These are orthogonal to Model-B.
- The 3c.2 per-append `MultiProducer::clone` mutex finding is moot for throughput here (the
  ceiling isn't ring-bound; it's linger/fsync/IPC-bound), so that optimization would not
  change the verdict.

## Status

- Fleet destroyed (11 resources, empty tf state, hosts.yml removed). No cost leak.
- Raw fitness JSONs + per-rep CSVs in the session scratchpad (`fleet_results/`).
- SyncCore branch `sync-core` (3c.1+3c.2) stays as-is on the fork — correct + merge-ready, but
  the throughput case for shipping it is not made by this data.
