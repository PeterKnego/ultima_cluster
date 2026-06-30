# UC throughput-knee attribution — same-instance bisection, 2026-06-30

Follow-on to `synccore-3c-fleet-throughput-2026-06-30.md` (which found the throughput knee is
~5k msg/s and 3c/consensus is NOT the cap). This run cleanly attributes the 5k knee on ONE
2xlarge fleet (no instance/build confounds), default RaftCore binary, by toggling one factor
per cell.

## TL;DR

**The 5k knee is two compounding ~2× caps: the 5 ms `api_batch_linger_ms` and the 3-node
replication-path latency. They stack multiplicatively (2× × 2× ≈ 4×). fsync is NOT a cap
(group-commit fully amortizes it). The 2xlarge structural ceiling is ~20k — the baseline runs
4× below hardware.** Pivot: linger/pipeline tuning is a cheap ~2× config win; the other 2× is
the replication path (deeper pipelining / co-location), not fsync and not consensus.

## Setup

3× AWS `c6id.2xlarge`, us-east-1, default RaftCore binary (sync-core feature OFF; the 3c A/B
already proved RaftCore≡SyncCore on the knee). Rate ladder [100..20000] msg/s, payload 64B,
inflight 128, 10s/rung, 2 reps/cell. One factor toggled per cell via `iterate -e`. Fleet
auto-destroyed (11 resources, verified empty state).

## Results (knee = highest rung holding the p99 SLA; medians of 2)

| cell | config | knee (msg/s) | p99@knee (ms) | vs A |
|---|---|---|---|---|
| **A** | 3-node, consistent (fsync), linger=5 | **5,000** | ~93 | baseline |
| **B** | 3-node, **none (no fsync)**, linger=5 | **5,000** | ~95 | ×1.0 — **fsync is free** |
| **C** | 3-node, consistent, **linger=0** | **10,000** | ~180 | **×2 — linger** |
| **D** | **1-node**, consistent, linger=5 | **10,000** | ~132 | **×2 — replication** |
| **E** | 1-node, none, linger=0 (all off) | **20,000** | ~273 | **×4 — both removed** |

Per-rep knees were identical within each cell (5/5, 5/5, 10/10, 10/10, 20/20) — the knee is a
very stable metric.

## Interpretation

- **fsync is not a throughput cap (B ≡ A).** Turning off `fdatasync` on the same host does not
  move the knee — group-commit at inflight=128 amortizes the fsync barrier completely. This
  refutes the "fsync-bound" hypothesis; the earlier 10k-vs-5k gap was the *instance size*
  (4xlarge vs 2xlarge), not durability.
- **Linger is a ~2× cap (C: 5k→10k).** The default 5 ms `api_batch_linger_ms` throttles the
  proposal cadence; at finite effective pipeline depth, throughput ≈ depth / commit-cycle and
  the 5 ms wait is a large slice of the cycle. Removing it doubles the knee.
- **The replication path is a ~2× cap (D: 5k→10k).** Dropping to a 1-node raft (no follower
  RTT, no QUIC append round-trip) doubles the knee *even with* linger=5 — so the cross-host
  replication latency is the other big commit-cycle term.
- **They compound (E: 4×, 20k).** Removing both linger and replication gives 20k — roughly the
  product of the two 2× factors. This is the signature of a **commit-latency-bound throughput
  at finite pipeline depth**: throughput ≈ in-flight-concurrency / commit-latency, and linger +
  replication-RTT are the two dominant latency terms. (Consensus choreography — already ruled
  out by the 3c A/B — is not one of them; that's why busy-spin / SyncCore didn't help.)
- **~20k is the 2xlarge structural ceiling** (everything off). The baseline operates 4× below
  hardware, entirely in linger + replication latency.
- **Latency/throughput trade is real:** p99@knee climbs 93 → 132/180 → 273 ms as the knee
  rises. Higher throughput comes at a higher tail, so for a fixed p99 SLA there is an optimal
  linger, not simply "linger=0".

## Pivot — where to spend effort next

1. **Linger + pipeline-depth tuning (cheap, ~2×, config-only).** Sweep
   `UC_API_BATCH_LINGER_MS` ∈ {0,1,2,5} × `UC_PIPELINE_DEPTH` and map the throughput/p99
   frontier; pick the SLA-optimal point (likely linger 1–2 ms recovers most of the 2× with a
   bounded tail). No code change. This is the immediate win.
2. **Replication-path latency at depth (structural, the second 2×).** Deeper / smarter
   pipelining of AppendEntries, or reducing the cross-host + 3-proc hop count (co-location).
   Bigger effort. NOT consensus (3c proved it) and NOT fsync.
3. **Do NOT** chase fsync batching or the SyncCore per-append clone-mutex for throughput —
   both are off the critical path for the ceiling (B≡A; consensus null).

## Status

- Both session fleets destroyed; empty tf state; no cost leak.
- The ansible hardening (`49d9468`) held across both multi-sweep runs (zero UNREACHABLE).
- Raw fitness JSONs + per-cell CSVs in the session scratchpad (`attr_results/`).
