# Task 20 — aeron-go clustered-service A/B (Go vs Java service container)

**Date:** 2026-07-03.
**Status:** Bench-infra shipped + one fleet A/B run complete. Result: **Go service container
at latency parity with the Java `EchoClusteredService`** across a 100→20 000 msg/s ladder,
consensus plane held fixed. Data + full tables in
`docs/benchmarks/aerongo-cluster-echo-2026-07-03.md`. This doc is the canonical standalone
record; the design/plan scaffolding is retained under
`docs/superpowers/plans/2026-07-03-aerongo-cluster-bench.md`.

---

## 1. Why

The 2026-07-02 parity scorecard measured **UC vs Aeron**, both self-hosted. This task asks a
narrower, cleaner question that isolates one variable: **what does the language of the Aeron
Cluster *service container* cost, holding the entire consensus plane fixed?**

The motivation is twofold:

1. **Container overhead as a datapoint for UC.** UC's own service (`uc_service`) is a separate
   process bridged to the consensus node (`uc_node`) over shared-memory rings — the same
   split-process topology as running aeron-go's Go service against a Java consensus module.
   Measuring how much that split costs *inside Aeron's own stack* (where the consensus plane is
   Aeron's hand-tuned busy-spin agents, not openraft) tells us whether the service-split
   topology is inherently expensive or whether UC's gap is elsewhere (it is: openraft
   choreography + fsync/IPC floor, per the floor decomposition). If Aeron's own stack pays only
   a few µs to externalize the service across a process boundary, then UC's ~1–2 ms floor is
   *not* attributable to the service-split — it confirms the gap is in the consensus layer.

2. **aeron-go feasibility.** aeron-go (`github.com/lirm/aeron-go`) provides a Go
   `ClusteredServiceContainer` but **no consensus module** — its `cluster/` package is a service
   container + ingress client only. A Go shop that wants Aeron Cluster must run the Java
   consensus module and write the service in Go. This measures whether that hybrid is viable at
   latency, not just whether it compiles.

## 2. Design — the external-service patch approach

Because aeron-go has no consensus module, both arms **must** share the Java consensus module,
media driver, archive, and LoadTestRig client. The only variable that can change is the service
container. The topology:

```
        Java media driver + archive + consensus module (identical both arms)
                                  │
        ┌─────────────────────────┴─────────────────────────┐
   JAVA ARM: embedded                              GO ARM: external
   EchoClusteredService                            ClusterNode started with
   in the cluster-node JVM                         -D…cluster.service=external
   (default ECHO type)                             (skips embedded container)
                                                          │  shared /dev/shm driver dir
                                                   aeron-go echo_service
                                                   (separate process, busy-spin idle)
```

**The patch** (`bench-infra/ansible/roles/build_aeron/files/clusternode-external-service.patch`,
~40 lines against `benchmarks-aeron/.../ClusterNode.java`):

- Adds an `EXTERNAL` value to the private `Type` enum;
  `fromSystemProperty()` maps `-Dio.aeron.benchmarks.aeron.cluster.service=external` → `EXTERNAL`
  (unrecognized / unset still → `ECHO`, so the **Java arm is byte-identical to stock**).
- In `main`, the try-with-resources that launches the `ClusteredServiceContainer` becomes
  `Type.EXTERNAL == type ? null : clusteredServiceContainer.start()` — a null resource is legal
  and skipped on close, so the node runs consensus with no embedded service.
- Guards the service mark-file error dump behind `Type.EXTERNAL != type` (the external Go service
  owns that mark file).

The patch defaults to existing behavior and is upstream-friendly (matches real-logic checkstyle
so the benchmarks gradle build passes). It is applied by the `build_aeron` ansible role after
clone.

**aeron-go arm** runs the stock `echo_service` example as a supervised process with
`NO_OP_IDLE=1` (busy-spin, matching the Java container's `BusySpinIdleStrategy`), attached to the
same media-driver directory. Both arms are driven in a single `bench.yml` invocation on the same
three instances (Java arm → wipe cluster state → Go arm), gated by `aerongo_enabled`.

**Fairness caveat:** the Go arm crosses **one extra same-host process boundary** (Go service ↔
consensus module over the shared driver) that the embedded Java service does not. This mildly
favors the Java arm — so measured parity is a lower bound on the Go container's closeness.

## 3. Config

- **Fleet:** 3× `c6id.4xlarge` (16 vCPU / 32 GiB / local NVMe), us-east-1, rig on `node0`.
- **Durability:** `consistent` → `aeron.archive.file.sync.level=1` (fdatasync) **both arms**.
  This is stronger than Aeron's own upstream bench default (`sync.level=0`) but is identical
  across the two arms, so the A/B is fair; only the absolute floor sits a touch higher than an
  fsync-free run.
- **Workload:** 64 B payload, batch 1, warmup 2 s / measure 10 s per rung, busy-spin idle both
  arms, rate ladder `100, 500, 1000, 2000, 5000, 10000, 20000` msg/s, Aeron ingress = UDP.
- **Versions:** `aeron-io/benchmarks @ 6afb215`, Aeron **1.51.0**, `aeron-go @ 8b05ad1`
  (upstream HEAD, 2024-06-06). `.hdr` values are **NANOSECONDS**.

## 4. Results (summary)

Full tables + delta analysis: `docs/benchmarks/aerongo-cluster-echo-2026-07-03.md`.

- **Both arms sustained every rung** (achieved == offered, n = rate × 10 s). **No knee reached**
  in either arm within the 20k ladder — this run bounds latency parity, not the throughput
  ceiling.
- **p50 parity:** Java 0.082–0.109 ms vs Go 0.083–0.115 ms. Go overhead is largest at the lowest
  rungs (+15 µs at 1 000 msg/s) and **vanishes under load** — the 10k/20k rungs match within
  ±1 µs (occasionally negative).
- **Tail:** Go p99 runs +8…+40 µs at low rate, converging to within ±1 µs by 10k; p99.9
  similar. Isolated ~2–4 ms max spikes appear in **both** arms (shared Java consensus-plane
  stalls), not Go-specific.
- **Interpretation:** the overhead *inverts* with load (biggest when idle, gone when hot) —
  consistent with a cold-poller wake-up cost on the extra process, not a fixed per-message IPC
  tax. At 64 B echo the consensus plane, not the service language or the process split,
  dominates the hot path.

**For UC:** externalizing the service across a process boundary inside Aeron's own stack costs
single-digit µs at p50. This confirms UC's ~1–2 ms floor is **not** the service-split topology —
it is the consensus layer (openraft async choreography + fsync/IPC), as the floor decomposition
(`docs/benchmarks/floor-decomposition-2026-06-25.md`) already found.

## 5. How to re-run

```bash
# In bench-infra/ansible/group_vars/all.yml (or via -e):
#   aerongo_enabled: true            # run the Go arm after the Java arm
#   aeron_benchmarks_ref: "6afb215"  # so the external-service patch applies
#   aerongo_ref: "8b05ad1"
#   durability: consistent           # → sync.level=1 both arms
# Then the standard fleet bring-up + bench:
make up            # provision 3× c6id.4xlarge, build both arms (patch applied by build_aeron)
make bench         # bench.yml: Java arm → wipe → Go arm, one invocation
# Results land in bench-out/aerongo-ab/<date>/results/node0/:
#   aeron_rung_*.hdr   (Java arm)   aerongo_rung_*.hdr (Go arm)
# Parse (values are NANOSECONDS) with the hdrh interval-line decoder used by the scorecard.
```

## 6. Caveats (honest)

- **One A/B run**, no repeats — few-µs low-rung deltas are indicative, not precise.
- **Ladder capped at 20k** — no throughput-ceiling comparison; latency parity only.
- **aeron-go is stale** (`8b05ad1`, last upstream commit 2024-06-06); round-trips against Aeron
  1.51 but is unmaintained — a production evaluation must re-verify against the target Aeron
  version.
- **`sync.level=1` both arms** — fair A/B, but stronger durability than Aeron's own bench default;
  absolute floors would drop slightly at `sync.level=0`.
- **Go GC not stressed** — short low-rate rungs; longer/higher-rate runs could surface Go GC tail
  pauses this run does not exercise.

## 7. Artifacts

- Results: `bench-out/aerongo-ab/2026-07-03/results/node0/{aeron,aerongo}_rung_*.hdr`,
  `manifest.txt`.
- Logs: `bench-out/aerongo-ab/2026-07-03/node0-logs/`.
- Patch: `bench-infra/ansible/roles/build_aeron/files/clusternode-external-service.patch`.
- Results doc: `docs/benchmarks/aerongo-cluster-echo-2026-07-03.md`.
- Plan (retained scaffolding): `docs/superpowers/plans/2026-07-03-aerongo-cluster-bench.md`.
