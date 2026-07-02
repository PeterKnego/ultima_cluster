# Post-wedge-fix ceiling re-run — the ceiling moves ~31k → ~57k (embedded) (2026-07-02)

**Question:** with the replication-stream wedge fixed (openraft fork `8d535489`, see
`docs/openraft-known-issues.md` §5 and `embedded-mode-sweep-2026-07-02.md`), does the
previously-wedge-contaminated ≥256-inflight region move the throughput ceiling past ~30k?

**Answer: yes — dramatically, and it flips a prior conclusion.**

- **Embedded (co-located) arm @ inflight 256: 45.0k msg/s clean at p50 7 ms, saturating in a
  graceful plateau at ~57k** (60k and 75k offered both achieve ~57k; no collapse). That is
  **~1.85× the pre-fix ~31k ceiling**, at full `consistent` durability (per-commit fsync),
  3-node, linger=2, 64 B payloads, c6id.2xlarge.
- **Shmem arm @ inflight 256: unchanged ~31k**, and deep overload still collapses it
  (60k offered → ~1.1k, then 0). **Co-location is therefore NOT throughput-null after all** —
  the earlier "ceiling identical in both arms" verdict (embedded-mode-sweep doc) was measured
  with the wedge present in both arms; the wedge capped both at ~30k and masked the arm
  difference. This supersedes that conclusion.
- **The old silent wedge is gone**: `old-loop` (the infinite `violates the API contract`
  10 ms-heartbeat loop) = **0 occurrences across all nodes in all runs** (pre-fix: 283k in one
  run). The escalation warn (`range is unservable`) appears only in degraded runs and is
  bounded/visible.
- **inflight 512 still collapses in both arms**, and degraded runs show why: **snapshot-thrash
  livelock**. The leader's escalation cycle runs continuously (shmem_r2: 13,192 escalations
  ≈ 10/s for the collapsed phase) — a follower falls below the purge horizon, the engine
  correctly re-decides snapshot, the follower installs it, and by then it is below the (very
  aggressive, per-snapshot) purge horizon again. Visible and recoverable-in-principle, but
  zero goodput while it persists. This is the next structural problem, distinct from the
  (fixed) wedge.

## Data (achieved msg/s @ p50; 3-node consistent, linger=2, 64 B, RaftCore build)

### Full matrix reps

| inflight | offered | shmem r1 | shmem r2 | embed r2 |
|---|---|---|---|---|
| 128 | 20k | 17.2k | 17.2k | 18.9k |
| 128 | 30k | 20.3k | 16.6k | 30.0k (p50 6.3ms) |
| 128 | 45k | 17.3k | 17.2k | 30.8k |
| 128 | 60k | 25.3k | 17.3k | 31.1k |
| 256 | 20k | 20.0k (p50 5.2ms) | 20.0k (p50 5.5ms) | 20.0k (p50 10.5ms) |
| 256 | 30k | 30.0k (p50 6.7ms) | 30.0k (p50 6.6ms) | 30.0k (p50 8.3ms) |
| 256 | 45k | 31.0k | 1.8k → dead | **45.0k (p50 7.0ms)** |
| 256 | 60k | 31.0k | 0 | **58.2k (p50 157ms)** |
| 512 | 20k | 20.0k | 0 | 0.04k |
| 512 | 30k+ | 0.5k → 0 | 0 | 0 |

(embed r1's collect was lost to the ansible sudo-flake; its two salvaged cells match r2.
shmem r2 degraded early — a follower entered the snapshot-thrash cycle at 256/45k and the
run never recovered; its 128-row numbers are depressed by the degraded quorum.)

### Confirmation reps (inflight 256 only, ladder to 75k)

| offered | shmem | embedded |
|---|---|---|
| 30k | 30.0k (p50 6.6ms) | 30.0k (p50 9.2ms) |
| 45k | 31.0k (p50 1.56s, saturated) | **45.0k (p50 7.2ms)** |
| 60k | 1.1k (collapse) | **56.8k (p50 228ms)** |
| 75k | 0 | **57.1k (p50 1.19s, plateau)** |

## Interpretation

1. **The ~30k "ceiling is the latency floor" framing needs a correction**: ~30k was the
   ceiling of a *wedge-degraded* cluster (silent 2/3 quorum + one core burning warn-spam +
   collapse cliffs). A healthy embedded cluster does 45k at single-digit-ms p50 and saturates
   at ~57k gracefully. Throughput = concurrency ÷ latency still holds — 256 in flight ÷
   ~4.5 ms effective service time ≈ 57k — the fix simply let the cluster actually operate at
   that point.
2. **Co-location now matters for throughput.** Shmem tops at ~31k and collapses under deeper
   overload; embedded reaches ~57k gracefully. Plausible mechanism: the shmem topology's
   3-process pipeline (client rings → node → service rings, plus the output/replay machinery)
   destabilizes under deep overload — a follower or the service falls behind, crossing the
   purge horizon and entering snapshot-thrash — while the embedded in-process apply keeps
   pace. (Pre-fix, both arms hit the wedge first, hiding this.)
3. **Remaining blockers for the next step up:**
   - **Purge slack** — the purge horizon advances on every snapshot (~5/s under load), so any
     follower that lags a snapshot-interval's worth of entries can only recover via snapshot,
     and under sustained load re-lags before the install completes (the thrash livelock).
     `max_in_snapshot_log_to_keep` (openraft) is the direct knob — keeping O(100k) entries of
     log slack behind the snapshot would let lagging followers catch up via logs. Untested;
     top candidate for the next fleet session.
   - **inflight 512 collapse** — admission control / adaptive inflight remains open; the
     healthy operating band at 256 is wide but ends abruptly.
4. **Aeron-parity context:** the 2026-06-21 parity sweep measured Aeron at ~20k sustained
   (its ceiling unprobed) **non-durable**, UC then ~10k. UC embedded now sustains **45k clean
   / 57k saturated with per-commit fsync durability** — UC has likely crossed the measured
   Aeron rungs on throughput; the honest comparison needs Aeron's real knee (extend its
   ladder) and matched durability, per the parity-scorecard direction.

## Method notes

- Fleet: 3× c6id.2xlarge us-east-1 placement group, `make up-uc` (rsync ships the local
  `../openraft` fork tree with `8d535489`), RaftCore build (`uc_sync_core=false` default).
- Wedge forensics per run: `grep -c 'violates the API contract'` (old loop; 0 everywhere) and
  `grep -c 'range is unservable'` (escalations; 0 in healthy runs, ~6k/13k in degraded ones)
  on all nodes' `uc-node.out` before the next run's cleanup.
- The ansible sudo-flake (privilege-escalation timeout under load) hit two collects; one CSV
  lost (embed r1), one salvaged ad-hoc. Local gotcha: a hung ansible ad-hoc in the driver
  serializes ALL subsequent background commands on this box — kill the stuck `ansible` PID,
  everything flushes.
- Artifacts: `bench-out/dist/20260702T{112707,131949,133617,133813,134541}Z/` (labels
  `ceilfix_shmem_r{1,2}`, `ceilfix_embed_r2`, `confirm_{embed,shmem}_256`; embed r1 lost).

## Disposition

- Wedge fix validated at fleet scale: silent wedge eliminated, moderate overload stabilized,
  ceiling roughly doubled in the embedded arm.
- `embedded-mode-sweep-2026-07-02.md`'s "co-location ~null on the ceiling" conclusion is
  **superseded** by this run (it was measured under the wedge).
- Next levers, in order: purge slack (`max_in_snapshot_log_to_keep`) for the thrash livelock;
  admission control for the 512 cliff; then the Aeron parity scorecard rerun (probe Aeron's
  real knee, matched durability).
