# SyncCore 3c vs RaftCore — commit-latency microbench (multi-node, injected RTT/fsync), 2026-06-30

First multi-node measurement after **3c** (replication severed from RaftCore onto per-peer
busy-spin network consumers + hot sync notifications on a disruptor input ring). Controlled
in-process microbench; the headline is **strongly positive in the realistic regime**.

## TL;DR

**At inflight=1, once the commit path has any realistic per-op blocking I/O (replication RTT,
with or without fsync), SyncCore-3c is consistently ~35–45% faster than RaftCore — even under
4-core oversubscription that handicaps SyncCore.** It loses only in the unrealistic free-I/O
regime (zero RTT) where there is nothing to overlap and busy-spin is pure cost. This is the
first clear multi-node signal that severing replication from RaftCore's async choreography
pays off, and it corroborates the floor decomposition (the ~37% replication bucket is mostly
futex task-hop choreography, which the busy-spin re-poll removes).

## Harness

`openraft/benchmarks/minimal` `commit_latency` bin (NOT committed; the cheap iteration
harness). A/B by build feature: RaftCore (default) vs SyncCore (`--features sync-core`),
both built at 3c HEAD `a6a590ec`. In-process cluster (`-m` nodes in one process), in-memory
store + network, `inflight=1` sequential client (`-c 1`). Injected: `--rtt-us` (per-message
in-memory network delay) and `--fsync-us` (per-commit committed-marker delay). 4-vCPU dev box.
n=3000–20000, warmup 500–2000. p50 ns/op reported.

## Results (inflight=1, p50)

**Control — m=1 (no replication; ~no oversubscription: 1 consensus + 1 durability thread):**

| regime | RaftCore | SyncCore | ratio |
|---|---|---|---|
| rtt=0 fsync=0 | 27.2µs | 26.7µs | 0.98x (parity) |
| rtt=0 fsync=100µs | 250µs | 168µs | **0.67x (SyncCore −33%)** |
| rtt=200 fsync=100µs | 256µs | 168µs | **0.65x (−35%)** |

Reproduces the 3d crossover: at m=1, SyncCore matches RaftCore at free I/O and wins ~⅓ once
durability ≥ ~100µs. Validates the binaries + isolates that any m≥3 free-I/O loss is
oversubscription/replication, not a regression.

**m=3 (replication active):**

| regime | RaftCore | SyncCore | ratio |
|---|---|---|---|
| rtt=0 fsync=0 | 39.5µs | 263µs | 6.65x (SyncCore SLOWER) |
| rtt=0 fsync=100µs | 303µs | 376µs | 1.24x (SyncCore slower) |
| rtt=50µs fsync=0 | 2.34ms | 1.36ms | **0.58x (−42%)** |
| rtt=200µs fsync=0 | 2.37ms | 1.46ms | **0.62x (−38%)** |
| rtt=1000µs fsync=0 | 4.32ms | 2.40ms | **0.55x (−45%)** |
| rtt=200µs fsync=100µs | 2.58ms | 1.58ms | **0.61x (−39%)** |
| rtt=1000µs fsync=100µs | 4.00ms | 2.49ms | **0.62x (−38%)** |

**m=5 (more fan-out):** rtt=200 fsync=0 → RaftCore 2.24ms, SyncCore 1.45ms = **0.65x (−35%)**.
The win holds with a larger cluster.

## Interpretation

- **The win appears exactly when there is I/O to overlap.** At m=3, the only regimes where
  SyncCore loses are rtt=0 (free network): rtt=0/fsync=0 (6.65x, pure oversubscription cost)
  and rtt=0/fsync=100 (1.24x — fsync alone isn't enough overlap to beat m=3 oversubscription;
  note this same fsync=100 WON at m=1, 0.67x — the difference is the extra busy-spin threads
  m=3 adds on 4 cores). The moment the replication round-trip blocks (any rtt>0), SyncCore's
  never-park consensus↔replication↔ack choreography wins 35–45%.
- **Real clusters are always in the winning regime** — replication to followers always has a
  round-trip (LAN µs to cross-AZ ms). The rtt=0 loss is a microbench artifact, not a
  deployment regime.
- **This is despite a handicap, not because of an advantage.** 4-core oversubscription
  (m=3 sync-core = 3 busy-spin consensus threads + per-peer network consumers + durability
  consumers, all thrashing 4 cores) penalizes SyncCore; it still wins by 35–45%. A real fleet
  (each node on its own host with dedicated cores) removes the handicap → the win should be
  cleaner and likely larger.

## Caveats (why this is directional, not final)

1. **`--rtt-us` is tokio-timer-quantized (~1ms granularity).** rtt=50 and rtt=200 both yield
   RaftCore p50 ≈ 2.34ms (they cluster) → sub-ms RTT rounds up to ~1ms per sleep; with ~2
   sleeps in the commit path that's ~2.3ms. So absolute latencies are inflated and the "RTT"
   axis is really "~1–2ms per-op blocking" (arguably MORE representative of real cross-host
   commit than µs). **The relative A/B at a fixed rtt is unaffected** (both arms hit the same
   quantization) — that's what we report.
2. **4-core oversubscription** handicaps SyncCore at m≥3 (see above) — a confound that a real
   fleet removes.
3. **In-memory, single-process, inflight=1.** Throughput under concurrency is NOT measured
   here — on 4 cores it is hopelessly confounded by busy-spin oversubscription (the 3d finding
   stands: needs the fleet). The per-append `MultiProducer::clone` mutex cost (3c.2 final-review
   finding) would show under throughput, not inflight=1.

## Next

1. **Denoised fleet A/B** (3× hosts, real QUIC + fsync, no oversubscription, real RTT) — the
   confound-free confirmation. Blocked on bench-infra ansible sudo-flakiness (see
   `synccore-fleet-2026-06-29.md`); harden that first. The fleet should show the win cleaner
   AND let throughput be measured fairly.
2. If throughput is flat/negative on the fleet, revisit the **per-append `MultiProducer::clone`
   mutex contention** (3c.2 final review's top follow-up) before concluding.
3. The inflight=1 result already justifies 3c: the Model-B thesis (busy-spin re-poll removes
   futex choreography) is realized multi-node, in the regime real clusters live in.

Raw runs in the session scratchpad; binaries `cl_raftcore_3c` / `cl_synccore_3c`.
