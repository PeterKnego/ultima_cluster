# Embedded-mode (co-location) sweep — floor & ceiling A/B vs shmem (2026-07-02)

> **⚠️ CEILING CONCLUSION SUPERSEDED same-day:** the "co-location ~null on the ceiling"
> verdict below was measured with the replication wedge present in BOTH arms, capping both
> at ~30k. After the wedge fix, the post-fix re-run
> (`postfix-ceiling-rerun-2026-07-02.md`) shows **embedded ~57k graceful plateau vs shmem
> ~31k + collapse** at inflight 256 — co-location IS a throughput lever once the cluster
> stays healthy. The floor conclusions (co-location ~40 µs; multi_thread +45%) stand.

**Question:** the 2026-07-02 ceiling correction says the ~25-30k msg/s ceiling is the
*latency floor* (fsync + 3-process shmem IPC + openraft async choreography) and that to move
past it you must "co-locate to remove IPC hops, cut async replication choreography, or batch
fsync harder". This sweep prices the **co-location lever**: run the node with the state
machine **in-process** (`IpcMode::Embedded` — no service process, no shmem surface, no
output_chan/replay) and A/B it against the historical shmem topology on the same fleet.

**Result in one line: co-location is ~null on both axes — the floor moves ~2% and the
ceiling doesn't move at all — but the sweep root-caused the "congestion collapse" as a
replication-stream wedge bug (details below), found that the collapsed region is really a
*cluster wedge*, and measured a large hidden cost of running the node on a multi-thread
tokio runtime (+0.68 ms floor, +45%).**

## Setup

- Fleet: 3× c6id.2xlarge (8 vCPU, local NVMe journal), us-east-1 placement group, QUIC,
  RaftCore build (`uc_sync_core=false` → `--no-default-features`), payload 64 B.
- **Shmem arm** (historical): `uc-node-launch` (node, current_thread runtime) + co-located
  `uc_service` (KvSm, NoopOutput) per host; load driven by `commit-path-load` on node0 via
  `uc_client` over cnc.dat/rings.
- **Embedded arm** (new): `uc-node-launch --ipc-mode embedded` — the SAME `KvSm` runs
  in-process via `AdaptedStateMachine`; no service process, no rings, no output dispatcher,
  no replay. Embedded mode has **no shmem client surface**, so the load driver lives in
  node0's process (`--load-*` flags; shared `uc_autobench::loadcore` sweep core behind a
  `Submitter` seam, so both arms share pacing/measurement/CSV code exactly). Commits
  `39e8c1b` + `932029b`; harness knob `-e uc_ipc_mode=embedded` (runtime, no rebuild).
- Floors: linger=0, inflight=1, rate=200, 10 s window. Ceilings: linger=2, consistent,
  3-node, inflight {128,256,512} × offered {20,30,45,60}k, request timeout 60 s, 2 reps
  per arm interleaved.

### Split runtimes — a required correction mid-experiment

The first embedded floor cell ran the whole process on one **multi_thread** tokio runtime
and came out **2.10 ms p50 vs shmem 1.45 ms** — the tokio flavor confounds the topology A/B:
on multi_thread, openraft's task hops become cross-worker futex wakeups. Fixed by splitting:
the **node** (RaftCore, journal, QUIC) runs on a dedicated **current_thread** runtime on its
own thread — same flavor as the shmem arm — and the in-process load driver runs on a separate
multi_thread runtime (932029b). The multi_thread numbers are kept below as a variant — the
delta is a finding in its own right.

## Floors (p50 / p99, inflight=1, rate=200, linger=0)

| cell | shmem | embedded (split rt) | Δ | embedded (multi_thread rt) |
|---|---|---|---|---|
| 3-node consistent | **1.45 / 2.02 ms** | **1.42 / 1.93 ms** | **−2%** | 2.10 / 2.62 ms (**+45%**) |
| 1-node eventual (base bucket) | 0.87 / 1.40 ms | 0.83 / 1.34 ms | −5% (~40 µs) | 1.03 / 1.54 ms (+18%) |

Two conclusions:

1. **Removing ALL shmem IPC is worth ~30–40 µs at the floor.** This matches the futex
   arithmetic exactly (≈4 cross-process hops × ~9 µs, task18/aeron-investigation): the
   "base" bucket of the floor decomposition (0.86 ms) is almost entirely **openraft
   commit→apply choreography + apply**, not IPC. The co-location lever, as a *latency* play,
   is **closed**.
2. **Putting the node on a multi_thread tokio runtime costs +0.68 ms (+45%) at the 3-node
   floor.** Same binary, same topology — only the runtime flavor differs. This is direct
   evidence for the hop-count×wakeup-cost model of the choreography bucket, and independent
   support for single-threaded consensus (the SyncCore/Model-B direction): the *scheduler
   placement* of the consensus hops is worth more than the entire IPC layer.

## Ceilings (achieved msg/s; linger=2, consistent, 3-node)

| inflight | offered | shmem r1 | shmem r2 | embed r1 | embed r2 |
|---|---|---|---|---|---|
| 128 | 20k | 17.7k (p50 413ms) | 19.8k (p50 368ms) | 18.5k (p50 333ms) | 19.3k (p50 9.7ms) |
| 128 | 30k | 17.1k | 20.0k | **30.0k (p50 7.3ms)** | **30.0k (p50 7.3ms)** |
| 128 | 45k | 27.1k | 23.3k | **30.3k** | **30.4k** |
| 128 | 60k | 17.2k | 21.2k | **30.2k** | **30.4k** |
| 256 | 20k | 20.0k (p50 5.3ms) | 20.0k (p50 5.7ms) | 20.0k (p50 10.5ms) | 20.0k (p50 11.1ms) |
| 256 | 30k | **30.0k (p50 6.6ms)** | 0.2k **collapse** | 1.8k **collapse** | 0.5k **collapse** |
| 256 | 45k | 30.4k | 0 (wedged) | 0 (wedged) | 0 (wedged) |
| 256 | 60k | 31.3k | 0 | 0 | 0 |
| 512 | 20k | 20.0k | 0 | 0 | 0 |
| 512 | 30k+ | 1.1k → 0 | 0 | 0 | 0 |

- **The ceiling does not move: ~30–31k in both arms.** Embedded peaks 30.2–30.4k, shmem
  31.3k (rep1). Removing the entire shmem layer — service process, client rings, output
  dispatcher, output replay — does not lift the ceiling. Together with the floor result,
  this closes the attribution: **the ceiling is openraft choreography + fsync (+ the wedge
  below), not IPC and not the apply/output pipeline.**
- **The optimal operating point shifts, reproducibly.** Embedded sustains a clean 30k at
  **inflight=128** (p50 7 ms!) where shmem needs 256 (at 128 shmem starves at 17–23k with
  ~0.4 s p50 — its higher effective per-op latency means 128 in-flight doesn't cover the
  bandwidth-delay product). Lower latency → less concurrency needed for the same
  throughput → a wider margin to the collapse cliff. That is a real operational benefit of
  embedded mode even with an unchanged peak.
- **Both arms collapse at 256/30k in 3 of 4 runs** (shmem r1 survived 256; both embed reps
  and shmem r2 did not) — and after collapse **every subsequent rung is 0**: the cluster is
  *wedged*, not congested.

## Root cause of the "collapse": a replication-stream wedge, not queueing

node0's log from the wedged embedded rep2 (identical signature in the wedged shmem rep):

```
WARN openraft::replication::stream_state: limited_get_log_entries(0, 300) returned empty;
     this violates the API contract but is handled gracefully as a heartbeat. Sleeping 10ms
```

- The first occurrence is at **06:50:40 — one minute into the run**, during the *first*
  rung. From then on it repeats every ~10 ms for the rest of the run (**283,096 lines /
  ~43 min ≈ 110/s**): one follower's replication stream got stuck requesting entries from
  **index 0**, getting an empty read (the prefix is long purged — openraft purges per
  snapshot), sleeping, and retrying — forever. It never escalates to snapshot install.
- The cluster then ran the entire "healthy" part of the ladder on a **silent 2/3 quorum**.
  The 30k rungs at inflight=128 were replicated by the leader + ONE follower.
- At 256/30k the *second* follower fell behind (node2 shows an `InstallSnapshot` completing
  at 07:16:56 followed by QUIC `accept_bi TimedOut`) → no quorum → zero goodput → every
  later rung reads 0. node1 was still spinning in the same `(0, 300)` warn loop 8 minutes
  after the sweep ended.
- The loop is in the fork's pipelined replication streaming (`openraft::replication::
  stream_state`, the stream_append path, cf. the `fix/clarify-try-get-log-entries-contract`
  branch): "returned empty → treat as heartbeat, sleep 10 ms" is correct for a transient
  race but is a **permanent wedge when the requested range is below the purge horizon** —
  the stream must transition to snapshot replication instead.

**Implication: the measured ~30k "ceiling with collapse beyond" is partly this bug.** A
follower that hiccups under load falls behind the (very aggressive, per-snapshot) purge
horizon and can wedge; the cluster degrades 3→2→1 replicas under exactly the load that
needs quorum most. Fixing the wedge (and/or throttling the purge horizon, e.g. keep-N-
segments slack) is now the **single most actionable throughput item** — it may not only
stabilize the 256+ region but raise the sustainable ceiling, since pre-wedge cells hit 30k
on a 2/3 quorum with one core's worth of warn-logging overhead.

## Disposition

- **Co-location (embedded mode): keep as a deployment option, close as a perf lever.**
  It buys ~40 µs latency, a cleaner concurrency profile (30k @ inflight 128), and removes a
  process — but neither the floor nor the ceiling moves materially. The Aeron-gap path is
  NOT through IPC removal; it is openraft choreography + fsync + linger (+ the wedge).
- **Node runtime flavor matters a lot**: never run the node on a multi_thread runtime
  (+45% floor). Worth a follow-up: does a *production* embedded config need work-splitting
  (the single current_thread node thread does raft+journal+QUIC+apply and is the plausible
  reason embedded didn't exceed shmem's peak)?
- **File/fix the replication wedge** (fork `stream_state`): empty `limited_get_log_entries`
  below the purge horizon must trigger snapshot install, not a 10 ms-heartbeat retry loop.
  Add a bench-side alert on the warn (it burned ~110 log lines/s for 43 min, silently).
  **→ FIXED 2026-07-02, fork commit `8d535489` (bounded retries → once-per-inflight `Err`
  escalation → engine re-decides snapshot; stale-response hardening). Regression test
  `t21_empty_reads_escalate_to_snapshot.rs`; openraft suites + UC lincheck/partition gates
  green. See `docs/openraft-known-issues.md` §5. A ceiling re-run to measure the unwedged
  256+ inflight region is the natural follow-up.**
- Infra: `uc_ipc_mode` knob + `build.yml` (build-only playbook) + `loadcore` Submitter seam
  shipped (39e8c1b, 932029b). One ansible sudo-flake hit the embed-r2 collect (CSV fetched
  ad-hoc; data complete). 8 bench.yml runs + 1 provision on this fleet; destroyed clean
  (11 resources).

## Artifacts

`bench-out/dist/20260702T{051053,051125,051208,051239,054124,054156,055424,062153,064930}Z/`
(+ `20260702T073300Z-embed-r2/` fetched ad-hoc) — per-run `node0/uc_sweep.csv`; config
labels `floor_{3n_cons,1n_evt}_{shmem,embed,embed2}` and `ceil_{shmem,embed}_r{1,2}`.
