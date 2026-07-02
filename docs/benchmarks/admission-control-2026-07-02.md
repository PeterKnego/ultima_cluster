# Admission control — bounded in-pipeline writes, validated (2026-07-02)

**Change (uc_node `e0f6275`):** `RaftTuning::max_inflight_writes` (default **256**, `0` =
uncapped) — one write-permit semaphore per node, held for the full commit round-trip:

- **shmem**: the client dispatcher acquires a permit *before* spawning each write task;
  when saturated the dispatch loop stops draining `submit.ring`, the ring fills, and
  clients see ring backpressure. Excess load waits at the door instead of bloating
  cluster queues.
- **embedded**: `NodeHandle::submit` awaits a permit.

Env `UC_MAX_INFLIGHT_WRITES`; bench-infra var `uc_max_inflight_writes` (empty = default,
`0` = control arm). Default 256 = the measured throughput optimum on the reference fleet.

**Why:** with the wedge fixed and purge slack shipped, the remaining overload pathology
was self-inflicted queueing: in-cluster write concurrency was whatever clients offered,
and past ~256 the pipeline degrades (512 concurrent measured 2.6× lower goodput embedded;
2048 was near-dead). Admission pins the pipeline at its efficient depth regardless of
offered concurrency.

## Fleet A/B (3× c6id.2xlarge, consistent, linger=2, 64 B, client request-timeout 120 s;
## client inflight 2048 = the catastrophic case)

| arm | offered | uncapped (control) | cap=256 |
|---|---|---|---|
| embedded | 60k | 22.3k (p50 2.4 s) | **58.2k** (p50 138 ms) |
| embedded | 75k | 11.4k | **57.7k** |
| shmem | 60k | 16.8k (p50 3.2 s) | **29.0k** (stable) |
| shmem | 75k | 7.3k (declining) | **29.3k** (stable) |

- **Embedded at 2048-client concurrency now delivers the full ~58k plateau** — identical
  to the optimal 256-client operating point. Admission is worth **2.6–5×** goodput under
  catastrophic overload, and the uncapped arm's signature downward spiral (deeper offered
  → less served) is gone.
- **shmem stabilizes at ~29k** under 2048-client overload (vs 7–17k declining uncapped).
  Note this is below shmem's ~44k best (reached with 256-native clients): with 2048
  clients the submit-ring backpressure cycle (ring-full → client retry loop) costs real
  throughput. Bounding is right; the shmem door mechanism has room to improve (e.g.
  larger submit ring or smarter client retry) if shmem deep-overload matters.
- **Healthy-point regression check** (embedded, client 256 = cap): 45.0k @ p50 7.2 ms /
  55.3k / 54.9k plateau — within run variance of the pre-admission 45/58/58k; the
  semaphore costs nothing at or below the cap.

## The overload story, end to end (one day's arc)

| stage | embedded @ deep overload | failure mode |
|---|---|---|
| wedge present (morning) | 0 (dead forever) | silent replication wedge, 2/3 quorum |
| wedge fixed | 0–5k (visible thrash) | snapshot-thrash livelock |
| + purge slack | 5–22k (alive, degraded) | unbounded queueing |
| + admission control | **~58k (full plateau)** | none observed at 2048×75k |

## Notes

- p50 at deep overload includes door-wait by design (an admitted request is fast; a
  queued one waits its turn) — goodput and stability are the metrics, and clients get
  backpressure instead of silent multi-second in-cluster queueing.
- Reads are not capped (writes drove every measured pathology). Adaptive limits (AIMD on
  the latency knee) and a fast-reject mode (typed Busy instead of waiting) are possible
  refinements; not needed by current data.
- Artifacts: `bench-out/dist/` runs labeled `adm_{embed,shmem}_2048`,
  `noadm_{embed,shmem}_2048`, `adm_embed_256` (+ salvaged copies in the session
  scratchpad `adm-csv/`). Gates: uc_node 145/0, lin_register 3/3, lin_partition 4/4.
