# Aeron parity scorecard — matched durability, fair hardware (2026-07-02)

**The scorecard the 2026-06-21 parity sweep couldn't produce**: both systems on the same
fleet, **both durability levels on both systems** (the maintainer confirmed Aeron's own
benchmarks run `aeron.archive.file.sync.level=0` = no fsync; our harness maps
`durability=consistent` → `sync.level=1`), Aeron's ladder extended until (almost) finding
its knee, and UC in its best configuration (embedded, admission 256, purge slack, wedge
fix — the full day's stack).

**Fleet:** 3× c6id.4xlarge (16 vCPU — Aeron's dedicated busy-spin agents need the cores;
on 8-vCPU c6id.2xlarge Aeron's latency degrades ~15× from oversubscription, see method
notes), us-east-1 placement group, 64 B payloads, 3-node cluster, rig/client on node0.
UC: embedded, inflight 256, linger 2 (floor cells: inflight 1, linger 0).

## The scorecard

### Matched no-fsync (Aeron `sync.level=0` ↔ UC `eventual`)

| axis | Aeron | UC (embedded) | gap |
|---|---|---|---|
| latency floor (200 msg/s) | p50 **0.081 ms** / p99 0.133 | p50 **1.08 ms** / p99 1.57 | **~13×** |
| latency at 60k msg/s | p50 0.059 ms / p99 0.084 | p50 509 ms (saturated) | — |
| throughput ceiling | **≥ 300 k/s** (p50 72 µs at 300k; knee not reached) | **~54 k/s** plateau | **≥ 5.5×** |

### Matched fsync (Aeron `sync.level=1` ↔ UC `consistent`)

| axis | Aeron | UC (embedded) | gap |
|---|---|---|---|
| latency floor (200 msg/s) | p50 **0.117 ms** / p99 0.299 | p50 **1.48 ms** / p99 2.07 | **~13×** |
| throughput ceiling | **≥ 800 k/s** (p50 0.38 ms / p99 0.44 ms at 800k; knee NOT reached) | **~56 k/s** plateau | **≥ 14×** |

(Aeron fsync ≥ no-fsync in these runs — archive fsync simply isn't its constraint at
these payload sizes; the 800k rung moves ~50 MB/s to NVMe. Its real knee likely sits
beyond the single-threaded LoadTestRig's pacing. UC's fsync tax on *throughput* is also
≈nil — group commit — and ~0.4 ms on the floor.)

### Robustness (from this week's UC work; Aeron not stress-probed here)

UC now degrades gracefully at any offered load/concurrency (58k plateau at 8× overload,
2048-concurrency clients, no collapse) — this axis is fixed and no longer a gap.

## Honest verdict against the "on par or better" goal

- **Robustness / overload behavior: at parity.**
- **Durable-throughput per fsync semantics: UC's fsync is free throughput-wise, same as
  Aeron's** — durability is not the differentiator either way at KV payloads.
- **Raw latency: ~13× gap** (1.1–1.5 ms vs 0.08–0.12 ms floors). Composition is the known
  structural story (floor decomposition): ~73 % openraft async choreography + process
  hops, ~27 % physical. Closing it means finishing the Model-B vision: consensus +
  replication + apply on dedicated spin threads (SyncCore 3e + busy-spin executor +
  adaptive linger→0) — each piece exists in the fork; the composition is unmeasured.
- **Raw throughput: ≥14× gap** (56k vs ≥800k). UC's plateau is the single consensus
  thread's capacity (proven: same 54–56k on 8 and 16 vCPU). Aeron's log fan-out runs on
  dedicated Agents. Moving this needs pipeline parallelism in the node (consensus thread
  + parallel journal/replication/apply stages — partially what SyncCore 3b/3c already
  built) rather than any tuning.

UC went 10k → 56k (5.6×) this week by removing self-inflicted failures (wedge, purge
livelock, unbounded admission); what remains between 56k and 800k is architecture, not
bugs: the openraft choreography and the one-thread node. Both have existing fork
foundations (task19 SyncCore); neither is a config knob.

## Method notes

- **Aeron .hdr files record NANOSECONDS** (`outputTimeUnit=MICROSECONDS` applies only to
  its text output). Decoder: `parse_aeron.py` (hdrh, headerless interval lines). An
  earlier µs misread made Aeron look 1000× worse on this fleet and triggered a spurious
  harness-regression hunt (June-era aeron-benchmarks `c605027` rebuilt on-fleet: numbers
  identical to current master — the harness was never the issue; pin the ref anyway for
  reproducibility).
- **Aeron needs ≥16 vCPU hosts**: on c6id.2xlarge its floor p50 is ~1.2 ms and load p99
  15–40 ms (agent oversubscription) while still sustaining 100k+ achieved. Those runs
  (`score_*` labels, dist 191148Z..191725Z) are kept as the small-host datapoint. The
  3×4xlarge fleet now fits the account's raised vCPU limit (the June 32-vCPU cap is gone).
- Aeron sync level is CONFIG-time (`cluster.properties`): re-render via the new
  `configure.yml` playbook (`-e durability=...`) between arms.
- Aeron's achieved rate == offered at every rung (the rig is open-loop and lossless
  here); UC's plateau numbers are achieved-vs-offered saturation.
- Artifacts: `bench-out/dist/20260702T{19*,20*}Z` — labels `s4_{nofsync,fsync}_{knee,floor}`
  (current master harness), `s4j_*` (June-era harness confirmation), `s4_fsync_hiprobe`
  (400–800k), plus the 2xlarge `score_*` runs. UC rows in `uc_sweep.csv`, Aeron in
  `aeron_rung_*.hdr`.
