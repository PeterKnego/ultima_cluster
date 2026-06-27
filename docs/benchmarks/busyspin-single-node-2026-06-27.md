# Busy-spin runtime — local single-node commit-latency A/B

**Date:** 2026-06-27 · **Branch:** `spike/openraft-hotpath-runtime`
**Bench:** `uc_autobench/src/bin/busyspin-commit-bench.rs`
**Relates to:** `docs/benchmarks/floor-decomposition-2026-06-25.md`,
`docs/openraft-busyspin-runtime-feasibility.md`

## What this measures (and what it does not)

The unloaded **single-node commit floor** at `inflight=1`: submit one command,
await the response, record latency, repeat. Single-node embedded (M1), **eventual
durability** (no fsync) — i.e. the `1node_eventual` arm of the floor
decomposition, which isolates the **base commit→apply handoff** (no replication,
no fsync, no wire). This is the only component the busy-spin lever can move
*locally*; replication choreography (the larger structural slice) and fsync need
the fleet.

**The runtime flavor is the whole point.** The futex cost the busy-spin engine
targets only appears when openraft's internal tasks scatter across worker
threads — a **multi_thread** runtime. So the bench runs `#[tokio::main(flavor =
"multi_thread")]`. (`commit-path-load` is `current_thread` by design and
therefore structurally cannot show this lever — which is exactly why the floor
decomposition was measured on the multi-thread fleet.)

Both arms are the *same* binary; only `TypeConfig::AsyncRuntime` differs
(`--features busyspin` → `uc_node/busyspin-runtime`). `UC_API_BATCH_LINGER_MS=0`
(matching the floor-decomp regime — at `inflight=1` the default 5 ms linger
otherwise dominates at ~6.5 ms and masks everything).

## Result (3 reps each, 15k iters, linger=0, this 4-core dev box)

| metric | tokio (avg of 3) | busy-spin (avg of 3) | Δ |
|---|---:|---:|---:|
| **min** (cleanest) | 58.8 µs | 28.5 µs | **−30.3 µs (−51%)** |
| p50 | 157.0 µs | 102.3 µs | −54.7 µs (−35%) |
| p99 | 505.4 µs | 248.5 µs | −256.9 µs (−51%) |

Per-rep p50 (µs): tokio `136 / 144 / 191` (noisy) vs busy-spin `97 / 111 / 99`
(tight). Per-rep min is rock-stable: tokio `~58`, busy-spin `~28`.

## Reading it

- **The lever moves the floor.** Busy-spin removes ~30 µs from the best-case
  single-node commit — consistent with eliminating the handful of cross-thread
  futex wakes (~8.8 µs each) on openraft's internal commit→apply path
  (RaftCore → sm_worker → apply). Only the openraft↔caller boundary hops remain
  on tokio.
- **Tail and consistency improve most.** p99 roughly halves and the whole
  distribution tightens — futex park latency has a heavy, variable tail; a
  never-park executor doesn't.
- **This is the *small* component.** Local base ≈ 135 µs here vs ≈ 860 µs on the
  fleet (different hardware + full real path). The bigger structural slice — the
  ~0.52 ms of openraft replication *choreography* (many more hops than a
  single-node commit) — is not exercised here. So the fleet A/B should show a
  larger absolute win.
- **It is a CPU-for-latency trade.** Busy-spin pegs a core; on a box with spare
  cores that buys lower, steadier latency (the premise). The throughput cost
  shows up under load — a fleet concern, not visible at `inflight=1`.

**Verdict:** a clear, reproducible *positive* local signal — the first
non-null result after the long string of µs-scale nulls, because this attacks the
structural lever (the scheduler) rather than the physical 27%. Strong motivation
for the fleet A/B (base + replication + fsync) against the floor decomposition.

## Reproduce

```bash
cargo build -p uc_autobench --release --bin busyspin-commit-bench               # tokio
cargo build -p uc_autobench --release --features busyspin --bin busyspin-commit-bench  # busy-spin
# run each (binary path is the workspace target dir):
UC_API_BATCH_LINGER_MS=0 UC_BENCH_ITERS=15000 <target>/release/busyspin-commit-bench
```

## Next

Fleet A/B reproducing the floor-decomposition 2×2 ladder (1/3-node ×
eventual/consistent) with the busy-spin build, to measure base + **replication
choreography** + fsync against the canonical numbers. Requires the `../openraft`
checkout rsync'd alongside `ultima_cluster` (path-dep) and the `busyspin-runtime`
feature wired through bench-infra's build.
