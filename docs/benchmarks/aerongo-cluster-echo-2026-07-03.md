# aeron-go vs Java clustered-service — fleet A/B (2026-07-03)

**Question:** how much does swapping Aeron Cluster's *service container* from Java to Go
(aeron-go's `ClusteredServiceContainer`) cost, holding the consensus plane fixed? Both arms
run the **same Java media driver, archive, consensus module, and LoadTestRig client** — the
only variable is the echo service and the language it runs in.

**Answer (headline):** the Go container is at latency parity with the Java
`EchoClusteredService` across the whole 100→20 000 msg/s ladder. p50 tracks within
**single-digit µs** (worst case +15 µs at 1 000 msg/s), the tail within **~tens of µs**
(worst +40 µs p99 at 100 msg/s), and the gap **shrinks to noise under load** — the 10k and
20k rungs are within ±7 µs at every percentile. Both arms sustained every rung
(achieved == offered); **no throughput knee was reached** in either arm within the 20k
ladder, so this run bounds latency parity, not the ceiling.

The notable result is that the Go arm crosses **one extra process boundary** (the service is
a separate process; the Java service is embedded in the cluster-node JVM) and *still* matches
— the container swap and the extra IPC hop are effectively free at these rates.

---

## Fleet & config

- **Hardware:** 3× `c6id.4xlarge` (16 vCPU, 32 GiB, local NVMe), us-east-1, kernel
  6.17 (`node0` manifest). Rig/client on `node0`.
- **Durability:** `durability=consistent` → **`aeron.archive.file.sync.level=1`** (fdatasync
  per archive term) on **both arms**. Note this differs from Aeron's *own* upstream benchmark
  default of `sync.level=0` (no fsync) — but it is **identical across both arms here**, so the
  A/B is fair; the absolute numbers are simply a touch higher than an fsync-free run would be.
- **Workload:** 64 B payload, batch size 1 (per-message pacing), busy-spin idle on every
  agent/poller in both arms, warmup 2 s, measure 10 s per rung, rate ladder
  `100, 500, 1000, 2000, 5000, 10000, 20000` msg/s. Aeron ingress = UDP.
- **Versions:** `aeron-io/benchmarks @ 6afb215`, Aeron **1.51.0**, `aeron-go @ 8b05ad1`
  (upstream HEAD, last commit 2024-06-06; proven round-trip-compatible against Aeron 1.43 and
  1.51 in a local spike). `.hdr` values are **NANOSECONDS**.

---

## Method

Both arms run in a single `bench.yml` invocation on the same three instances (Java arm first,
cluster state wiped, then Go arm), so the hardware, driver, archive, consensus module and
LoadTestRig are byte-identical across arms. The only difference:

- **Java arm** — stock benchmarks `ClusterNode` JVM with its embedded
  `EchoClusteredService` container (default `ECHO` type).
- **Go arm** — the same `ClusterNode` JVM launched with
  `-Dio.aeron.benchmarks.aeron.cluster.service=external`, which **skips** starting the embedded
  container (a ~40-line upstream-friendly patch adding an `EXTERNAL` service type —
  `bench-infra/ansible/roles/build_aeron/files/clusternode-external-service.patch`), plus the
  **aeron-go `echo_service` as a separate process** on the same host, attached to the shared
  Aeron media-driver directory (`/dev/shm/node0-driver`). aeron-go has no consensus module —
  its `cluster/` package is a service container + ingress client only — which is exactly why
  the Java consensus module has to stay.

The Go service ran with the busy-spin idle strategy (`NO_OP_IDLE`) to match the Java
container's `BusySpinIdleStrategy`. Its log shows the expected per-rung session churn
(`OnSessionOpen`/`OnSessionClose` per LoadTestRig run), confirming it genuinely applied and
echoed every message rather than passing through.

**Fairness caveat:** the Go arm crosses one **extra same-host process boundary** (service ↔
consensus module over the shared driver) that the embedded Java service does not. This mildly
favors the Java arm — so the observed parity is a lower bound on how close the Go container is.

**HDR processing:** each `.hdr` is a headerless HdrHistogram interval-log
(`start,end,max,HISTFAA<base64>`), decoded with Python `hdrh` (all interval lines per rung
accumulated; percentiles ÷ 1e6 → ms; achieved = count ÷ 10 s). Same method as the
2026-07-02 parity scorecard (`aeron-parity-scorecard-2026-07-02.md`).

---

## Results

### Java arm — benchmarks `EchoClusteredService` (embedded)

| offered | achieved | p50 (ms) | p99 (ms) | p99.9 (ms) | max (ms) | n |
|--:|--:|--:|--:|--:|--:|--:|
| 100 | 100.0 | 0.109 | 0.306 | 0.463 | 0.472 | 1 000 |
| 500 | 500.0 | 0.092 | 0.235 | 0.255 | 4.121 | 5 000 |
| 1 000 | 1 000.0 | 0.087 | 0.230 | 0.243 | 0.251 | 10 000 |
| 2 000 | 2 000.0 | 0.087 | 0.219 | 0.227 | 0.244 | 20 000 |
| 5 000 | 5 000.0 | 0.082 | 0.222 | 0.232 | 2.275 | 50 000 |
| 10 000 | 10 000.0 | 0.083 | 0.223 | 0.231 | 1.741 | 100 000 |
| 20 000 | 20 000.0 | 0.087 | 0.230 | 0.247 | 0.963 | 200 000 |

### Go arm — aeron-go `echo_service` (external process, external-service ClusterNode)

| offered | achieved | p50 (ms) | p99 (ms) | p99.9 (ms) | max (ms) | n |
|--:|--:|--:|--:|--:|--:|--:|
| 100 | 100.0 | 0.115 | 0.346 | 0.478 | 0.511 | 1 000 |
| 500 | 500.0 | 0.096 | 0.274 | 0.305 | 4.301 | 5 000 |
| 1 000 | 1 000.0 | 0.102 | 0.254 | 0.275 | 0.303 | 10 000 |
| 2 000 | 2 000.0 | 0.085 | 0.234 | 0.252 | 0.302 | 20 000 |
| 5 000 | 5 000.0 | 0.084 | 0.230 | 0.256 | 2.660 | 50 000 |
| 10 000 | 10 000.0 | 0.083 | 0.224 | 0.236 | 1.901 | 100 000 |
| 20 000 | 20 000.0 | 0.086 | 0.229 | 0.240 | 1.185 | 200 000 |

### Delta (Go − Java, µs)

| offered | Δp50 | Δp99 | Δp99.9 |
|--:|--:|--:|--:|
| 100 | +6 | +40 | +15 |
| 500 | +4 | +39 | +50 |
| 1 000 | **+15** | +24 | +32 |
| 2 000 | −2 | +15 | +25 |
| 5 000 | +2 | +8 | +24 |
| 10 000 | 0 | +1 | +5 |
| 20 000 | −1 | −1 | −7 |

---

## Reading the data

- **No knee, either arm.** Achieved rate == offered at every rung for both (n = rate × 10 s
  exactly). The 20k ceiling of this ladder was chosen for a latency-parity comparison, not a
  saturation hunt — the scorecard already pushed the Java consensus plane to ≥ 800k. So this
  run says nothing about a Go-vs-Java *throughput* ceiling; it establishes latency parity up
  to 20k msg/s.

- **The Go overhead inverts with load.** It is largest at the *lowest* rungs (+6…+15 µs p50,
  +24…+40 µs p99 at ≤ 1 000 msg/s) and disappears by 5 000 (the 10k/20k rungs are within
  ±7 µs at every percentile, occasionally *negative*). This is the opposite of what a fixed
  per-message process-boundary tax would produce and is consistent with a **cold-poller
  wake-up** cost: at low rate the extra process's busy-spin loop and caches sit idle between
  messages; under sustained load both pipelines stay hot and the hop is amortized. The
  low-rung counts (n = 1 000…5 000) also sit near the measurement noise floor.

- **Tail is well-behaved.** Isolated max spikes (~2.3–4.3 ms) show up at 500 / 5 000 / 10 000
  in **both** arms — one-off scheduler/fsync/GC stalls in the shared Java consensus plane, not
  a Go-specific artifact. No sustained tail divergence in the Go arm across a 200 000-sample
  rung.

**Bottom line:** at 64 B echo, replacing the Java service container with aeron-go's Go
container — across an extra process boundary — costs at most a handful of µs at p50 and tens
of µs in the tail at low rates, and nothing measurable under load. The consensus plane, not
the service language, dominates this hot path.

---

## Caveats

- **One A/B run**, no repeats — treat few-µs low-rung deltas as indicative, not precise.
- **Ladder capped at 20k** — throughput ceilings not probed; latency parity only.
- **aeron-go is stale** (`8b05ad1`, last upstream commit 2024-06-06). It round-trips cleanly
  against Aeron 1.51 but is not actively maintained; a production evaluation should re-verify.
- **`sync.level=1` both arms** — stronger durability than Aeron's own bench default (0), but
  identical across arms, so the comparison is fair; absolute floors would drop a little at
  `sync.level=0`.
- **Go GC** was not stressed — short 10 s rungs at ≤ 20k msg/s; longer or higher-rate runs
  could surface Go GC pauses in the tail that this run does not exercise.

## Artifacts

- `bench-out/aerongo-ab/2026-07-03/results/node0/aeron_rung_*.hdr` (Java arm),
  `aerongo_rung_*.hdr` (Go arm), `manifest.txt`.
- Go-service / node logs: `bench-out/aerongo-ab/2026-07-03/node0-logs/`
  (`aerongo-service.out`, `node-go.out`, `md-go.out`).
- Patch: `bench-infra/ansible/roles/build_aeron/files/clusternode-external-service.patch`.
- Canonical record: `docs/tasks/task20_aerongo_cluster_bench.md`.
