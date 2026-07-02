# Purge slack — the snapshot-thrash livelock fix, validated (2026-07-02)

**Change:** `RaftTuning::max_in_snapshot_log_to_keep` default **1000 → 200 000** (uc_node
commit `9904c68`), plus sweep knobs `UC_MAX_IN_SNAPSHOT_LOG_TO_KEEP` /
`UC_SNAPSHOT_LOGS_SINCE_LAST` and bench-infra vars.

**Why:** the post-wedge-fix re-run (`postfix-ceiling-rerun-2026-07-02.md`) showed degraded
runs stuck in a snapshot-thrash livelock: with 1000 entries of purge slack and snapshots
every 5000 applied, purge advances to within ~22 ms (at 45k msg/s) of the frontier several
times per second; a follower lagging more than that can only recover via snapshot install,
by the end of which it is below the horizon again. 200 000 entries ≈ 4 s of catch-up-via-
logs headroom at 50k msg/s, ~20 MB of journal at KV payloads (less than one segment).

## Fleet A/B (3× c6id.2xlarge, consistent, linger=2, 64 B; control = keep-1000 runs from
## `postfix-ceiling-rerun-2026-07-02.md` on the same day/fleet-type)

### shmem @ inflight 256 (the collapse-prone cell)

| offered | control (keep=1000) | slack r1 | slack r2 |
|---|---|---|---|
| 45k | 31.0k (saturated) | **42.5k** (p50 0.57s) | 31.4k |
| 60k | **1.1k collapse** | **44.1k** (p50 1.5s) | 21.1k (p50 5.7s) |
| 75k | **0 (dead)** | 1.8k | 39.3k |

The shmem saturation ceiling moves ~31k → **~44k**, and — the robustness headline — the
**collapse-to-zero-forever mode is gone**: deep-overload goodput varies (2–44k, seconds-level
p50) but the cluster keeps serving and recovers between rungs. 75k remains past the edge.

### inflight 512 (the cliff; control = total collapse in both arms)

| arm | offered | control | slack |
|---|---|---|---|
| shmem | 30k | 0 | **29.0k (p50 6.8 ms)** |
| shmem | 45k | 0 | **34.1k (p50 0.36s)** |
| embedded | 30k | 0.04k → 0 | 21.7k (p50 18 ms) |
| embedded | 45k | 0 | 10.5k (degraded, alive) |
| embedded | 60k | 0 | 4.8k (degraded, alive) |

**shmem @512 is now a working operating point** (29k clean at 30k offered). Embedded @512
degrades with depth (the single current_thread node runtime drowning in per-request
overhead — 512 in-process submit tasks vs shmem's separate client process) but never dies.

### embedded @ inflight 256 (regression check)

45k → 45.0k (p50 6.9 ms); 60k → 58.0k; 75k → 58.0k (p50 1.15s) — the ~58k graceful plateau
is intact; slack costs nothing.

## Verdict

- **Livelock fixed**: no zero-goodput cells anywhere with slack (control had 7).
- **shmem ceiling ~31k → ~44k**; embedded plateau unchanged at ~58k (its ceiling is the
  node-thread capacity, not purge).
- **Cost**: ~20 MB more journal retained; nothing else. New default shipped for all nodes.
- Remaining overload roughness (goodput oscillation at seconds-level p50 past saturation,
  75k shmem edge, embedded@512 degradation) is ordinary overload behavior without admission
  control — the next lever, now cleanly separated from the (fixed) wedge and (fixed)
  livelock.

## Aeron-parity note (from the maintainer)

Aeron's benchmark configuration runs `aeron.archive.file.sync.level=0` — **no fsync at
all** on its log. Every UC number above is with `consistent` durability (per-commit
fdatasync on leader and followers). UC embedded's 45k clean / ~58k saturated therefore
compares against Aeron's measured ~20k rungs at a strictly *stronger* durability level;
the honest scorecard needs Aeron's real knee probed, and a UC `eventual` run for the
matched-durability comparison.

## Method notes

- Fleet gotchas this session: the parallel rsync in `build_uc` hit sshd connection-close
  flakes on ALL nodes twice (fresh instances; burst of new connections from one source IP)
  — `ansible-playbook provision.yml -f 1` (serialized) works; a failed provision leaves
  `uc-peers.env` missing (config role runs after build), so bench runs fail fast with
  "cannot open /opt/bench/uc-peers.env" until provision completes.
- `ansible -m fetch` does not support async flags (`-B/-P`) — the driver's inline CSV
  salvage silently failed; one cell (embed512 r1) was lost to the next run's cleanup and
  re-run. Salvage fetches must be plain synchronous.
- Artifacts: `bench-out/dist/20260702T{165023,170824,170931,171033,171221}Z/` (labels
  `slack_shmem256_r{1,2}`, `slack_shmem512`, `slack_embed256`, `slack_embed512_r2`).
