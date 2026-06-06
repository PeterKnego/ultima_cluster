# Task 13 — Aeron IPC vs ultima_cluster commit-path benchmark

**Date:** 2026-05-30 (investigation); harness integrated to `main` 2026-06-06.
**Status:** Phase 1 (single-node, in-process) + Phase 2 (3-node loopback, multi-process) complete. Investigation closed; reusable harness merged.
**Spec/plan:** `docs/superpowers/specs/2026-05-30-aeron-vs-uc-commit-path-benchmark-design.md`, `docs/superpowers/plans/2026-05-30-aeron-vs-uc-commit-path-benchmark.md`

> **Integration note (2026-06-06).** This investigation originally lived on the
> abandoned `bench/aeron-vs-uc-commit-path` branch. Its decisive finding — that
> the commit floor is **poll-sleep IPC latency, not fsync** (see "Conclusion /
> real lever" below) — was already acted on and shipped as **task11
> (event-driven ring wakeups)**, which collapsed the commit-path floor ~4.6×.
> What is merged here is the **reusable harness only**: the `commit-path-load`
> open-loop driver and the `uc-node-launch` multi-process cluster launcher
> (`uc_autobench`), plus the run/plot scripts and this writeup.
>
> The Phase-0 `commit-profile` instrumentation described in the spec/plan was
> **deliberately dropped on integration**: its journal-side half
> (`ultima_journal::commit_profile`) was never committed to `ultima_journal` and
> is unrecoverable, and its verdict is already harvested. The fsync-vs-poll-sleep
> numbers below were produced by that now-removed instrumentation and are kept
> as the historical record, not as something re-runnable from this tree.

## Linux re-run (2026-06-06, post-task11)

The original investigation (everything below this section) ran on **Apple Silicon
/ macOS, pre-task11**. Re-running the single-node `commit-path-load` driver on
**Linux** with the current `main` (which includes task11 event-driven ring
wakeups) shows the predicted collapse of the latency floor — confirming the
corrected "poll-sleep, not fsync" verdict from the field.

- **Setup:** in-process `ClusterFixture` single-node, `--release`, `RUST_LOG=off`,
  64-byte payload, `KvSm` apply, `log_durability` left at the fixture default.
  Two journal targets: **ext** (real disk, `TMPDIR=/home/claude/...`) and
  **tmpfs** (`/dev/shm`). Rate ladder 100→20000/s × in-flight {1,8,32,128}.
  Reproduce: `RATES=... INFLIGHT=... TMPDIR=<ext-path> bash uc_autobench/scripts/run-uc-single-node.sh`
  (the script resolves the cargo target dir via `cargo metadata`, so it works
  under a relocated `CARGO_TARGET_DIR`).
  **Note:** on this host both `/tmp` and `/dev/shm` are tmpfs — the "disk" run
  must point `TMPDIR` at a real ext path or it silently measures tmpfs.

### Unloaded latency floor (inflight=1, below the knee)

| target rate | ext disk p50 | ext disk p99 | tmpfs p50 | tmpfs p99 |
|--:|--:|--:|--:|--:|
| 1000/s | 1.03 ms | 2.28 ms | 0.76 ms | 1.76 ms |
| 2000/s | 1.13 ms | 7.11 ms | 0.78 ms | 2.98 ms |
| 5000/s | *(saturated)* | — | 0.76 ms | 6.79 ms |

**Unloaded commit latency: ext disk ≈ 1.0 ms, tmpfs ≈ 0.76 ms.** fsync adds only
**~0.25 ms** here — not the ~7.5 ms seen on macOS HFS+.

### Throughput ceiling (achieved rate at/above the knee)

| target rate | ext disk achieved | tmpfs achieved (if=1 / if=8) |
|--:|--:|--:|
| 2000/s  | 2000/s (below knee) | 1999/s / 1999/s |
| 5000/s  | **~2460/s** (saturated) | 4998/s / 4996/s (sustained) |
| 10000/s | **~2500/s** (saturated) | **~6190/s** / **~8180/s** |

- **ext disk: hard ceiling ≈ 2500/s**, flat across inflight 1→128 (same
  single-serialized-writer signature as macOS).
- **tmpfs: ≈ 6000/s at inflight=1, ≈ 8000/s at inflight=8** — fsync caps
  throughput **~2.5–3×** on this host.

### What changed vs the macOS investigation

| metric | macOS (pre-task11) | **Linux (post-task11)** | delta |
|---|--:|--:|--:|
| Unloaded p50, disk | ~11 ms | **~1.0 ms** | ~10× lower |
| Unloaded p50, tmpfs | ~3.5 ms | **~0.76 ms** | ~4.6× lower |
| Throughput ceiling, disk | ~85–110/s | **~2500/s** | ~25× higher |
| fsync latency cost | ~7.5 ms | **~0.25 ms** | host-dependent |

The **~3.5 ms → ~0.76 ms tmpfs floor collapse** is the task11 poll-sleep removal,
landing exactly where this investigation said the real lever was (~4.6×, matching
task11's own measurement). The much smaller fsync *latency* cost is platform: this
host's ext filesystem fsyncs in sub-millisecond, versus macOS HFS+ at ~7.5 ms.
fsync still **caps throughput** (~2500 vs ~6000–8000/s), so journal group-commit
remains the next throughput lever — but it is no longer a latency wall.

> **Caveat:** the "disk" target is the container's ext2/3 on `/home/claude`
> (likely SSD/overlay-backed) — real *non-tmpfs* durability, but the absolute
> fsync cost is host-specific and not representative of a spinning disk or a
> networked block device. The relative disk-vs-tmpfs gap is the portable signal.

### Phase 2 — 3-node QUIC loopback (Linux, ext disk)

Three real `uc-node-launch` processes over QUIC on `127.0.0.1`, node1 the
bootstrap leader, journal on ext disk (`DATA_ROOT` on `/home/claude`), shmem
rings on `/dev/shm`; load via `commit-path-load --connect` against the leader.
Reproduce: `DATA_ROOT=<ext-path> RATES=... INFLIGHT=... bash uc_autobench/scripts/run-uc-3node.sh`.

| target rate | 3-node p50 | 3-node achieved | (single-node disk p50 / achieved) |
|--:|--:|--:|--:|
| 100/s  | 3.0 ms  | 100/s              | (~1.0 ms / 100/s) |
| 500/s  | 4.0 ms* | ~500/s (sustained) | (~1.0 ms / 500/s) |
| 1000/s | *(saturated)* | ~590/s       | (~1.0 ms / 1000/s) |
| 2000/s | *(saturated)* | ~600/s       | (~1.1 ms / 2000/s) |

\* inflight=8; at inflight=1 the 500/s step is right at the edge (p50 ~10 ms,
heavy p99 tail). Sustainable throughput is **~500–600/s** across inflight
{1,8,32}; above that, achieved pins ~600/s and p50 climbs into seconds
(coordinated-omission backlog).

- **Unloaded latency ≈ 3.0 ms** (vs ~1.0 ms single-node) — **replication adds
  ~2 ms**, one QUIC round-trip to a 2/3 quorum on loopback.
- **Throughput ceiling ≈ 600/s** (vs ~2500/s single-node) — replication cuts
  throughput **~4×** on this host.

Versus the macOS Phase 2 (p50 ~16 ms, ceiling ~48/s): Linux is ~5× lower latency
and ~12× higher throughput. Note the single→3-node *throughput* drop is steeper on
Linux (~4×) than macOS (~2×): with the fsync latency wall gone (task11), the QUIC
quorum round-trip + replica append is now the **dominant** commit cost, not a
second-order add-on. Same loopback caveat as the macOS run — a real NIC adds
tens-to-hundreds of µs per replication round-trip, so these are lower bounds.

---

*The remainder of this document is the original macOS investigation, preserved
as the historical record.*

## TL;DR

UC's single-node commit path delivers **p50 ≈ 11 ms / throughput-ceiling ≈ 85–110 commits/s** on real disk, versus Aeron same-host IPC at **p50 ≈ 0.17 µs**. The ~65,000× gap is **entirely consensus + journal durability — transport is a rounding error.** The decisive finding: running the journal on a RAM disk (tmpfs) drops p50 to **~3.5 ms** *and removes the throughput ceiling within the tested range* (sustains 250/s with no knee). **`fsync` durability is responsible for both ~7.5 ms of per-op latency and the entire ~100/s throughput wall.** Optimizing group-commit/fsync batching is the single highest-leverage UC perf lever; shmem rings (task08, ~15 ns) and transport are already far below the noise floor.

**Phase 2 (3-node QUIC loopback):** adds quorum replication on top — unloaded **p50 ≈ 16 ms** (+~5 ms vs single-node for the cross-node round-trip to a 2/3 quorum) and **throughput-ceiling ≈ 48–52/s** (roughly halved). Replication is real and durable (clearly not the ~3.5 ms tmpfs floor), but the same single-serialized-writer + fsync bottleneck still dominates — the replication round-trip is *second-order* to the fsync cost. Conclusion is unchanged: **fix group-commit/fsync first; it caps both the single-node and replicated configs.**

## Methodology

- **Open-loop load**, coordinated-omission-free: each request's latency is measured from its *intended* send time (`run_step` advances `next_send` by `1/rate` regardless of actual dispatch), so backlog past saturation is captured honestly rather than hidden.
- **HDR histograms** per ladder step; shared 13-column CSV schema across both systems.
- **Rate ladder** bracketing the knee: 20,40,60,80,100,150,250 msgs/s. **In-flight concurrency sweep:** 1,4,16,64. 5 s measurement window, 2 s warmup per step, 64-byte payload.
- **UC side (Phase 1):** in-process single-node `ClusterFixture` (one `uc_node` + service + 1 client), real Raft commit path, KV state machine (in-memory `HashMap` apply — real keyed write per commit). Two journal-durability targets: **real disk** (`TMPDIR=/tmp`) and **tmpfs** (macOS `hdiutil` RAM disk). Driver: `commit-path-load` (`uc_autobench`), `--release`.
- **UC side (Phase 2):** three real `uc-node-launch` processes over QUIC loopback (`BootstrapConfig::Peers`, `IpcMode::Shmem`, self-signed TLS), real quorum replication, journal on disk; the same `commit-path-load` attaches via `--connect` to the leader. Knee-bracketing ladder 10,20,30,40,48,60,80 (the 3-node ceiling is ~48/s). Reproduce: `bash uc_autobench/scripts/run-uc-3node.sh`.
- **Aeron side:** C `cping`/`cpong` over `aeron:ipc`, busy-spin driver (`AERON_THREADING_MODE=DEDICATED`, `*_IDLE_STRATEGY=spin`), HdrHistogram → shared CSV. **Bytes only — no state machine.** This is the transport floor, not an apples-to-apples system.
- **Platform:** Apple Silicon arm64, macOS.
- **Reproduce:**
  - UC: `RATES=20,40,60,80,100,150,250 INFLIGHT=1,4,16,64 PAYLOAD=64 bash uc_autobench/scripts/run-uc-single-node.sh`
  - Aeron: `bash ../aeron/scripts-scratch/run-aeron-ipc.sh`
  - 3-node: `bash uc_autobench/scripts/run-uc-3node.sh`
  - Plots: `/tmp/benchvenv/bin/python uc_autobench/scripts/plot_decomposition.py bench-out/uc_*.csv bench-out/aeron_ipc.csv --out-dir bench-out/plots`
  - (CSVs/plots live under the gitignored `bench-out/`; regenerate with the above.)

## Results

### Aeron IPC transport floor (bytes only)

| payload | p50 | p99 | p99.9 | max |
|--:|--:|--:|--:|--:|
| 8 B   | 167 ns | 375 ns | 3.0 µs | 3.98 ms* |
| 64 B  | 167 ns | 583 ns | 6.5 µs | 47 µs |
| 256 B | 208 ns | 2.08 µs | 8.7 µs | 4.90 ms* |

\* isolated max outliers over 1 M messages (OS jitter), not typical.

### UC single-node commit path — unloaded latency (below the knee)

Latency is flat with concurrency below saturation (open-loop, true serial round-trip):

| target | disk p50 | disk p99 | tmpfs p50 | tmpfs p99 |
|--:|--:|--:|--:|--:|
| 20/s | 11.3 ms | 13.4 ms | 3.8 ms | 5.1 ms |
| 40/s | 11.4 ms | 12.8 ms | 3.4 ms | 4.8 ms |
| 60/s | 11.4 ms | 13.4 ms | 3.4 ms | 5.0 ms |
| 80/s | 12.3 ms | 18.3 ms | 3.7 ms | 5.1 ms |

**Unloaded commit latency: disk ≈ 11 ms, tmpfs ≈ 3.5 ms. fsync ≈ 7.5 ms of every commit.**

### UC throughput ceiling (achieved rate at/above the knee)

| target rate | disk achieved (any inflight) | tmpfs achieved |
|--:|--:|--:|
| 80/s  | 80/s (below knee)            | 80/s |
| 100/s | **~86–100/s** (at knee)      | 100/s |
| 150/s | **~85–110/s** (saturated)    | 150/s |
| 250/s | **~84–109/s** (saturated)    | 250/s |

- **Disk: hard ceiling ~85/s at inflight=1, rising only to ~100–110/s at inflight=4–64.** 64× concurrency buys ~1.3× throughput. Past the knee, p50 climbs into the seconds (coordinated-omission backlog: e.g. inflight=64 @ 250/s → p50 3.8 s) — pure queueing, the system is throughput-bound.
- **tmpfs: NO ceiling in the tested range** — achieved == target through 250/s at every concurrency level, p50 staying ~3.5 ms. The serialized-writer/fsync wall that dominates the disk path does not bind until far higher load.

Plots: `bench-out/plots/latency_vs_throughput.png` (overlay + Aeron floor line), `bench-out/plots/decomposition.png` (unloaded p99 per layer, log scale).

### Phase 2 — 3-node QUIC loopback (real multi-process cluster)

Three real `uc-node-launch` processes over QUIC on loopback (`BootstrapConfig::Peers` + `IpcMode::Shmem`), node1 the bootstrap leader, journal on real disk; load via `commit-path-load --connect` against the leader's instance dir. Knee-bracketing ladder (10→80/s).

Unloaded latency (below the ~48/s knee), vs single-node disk:

| target | 3-node p50 | 3-node p99 | (single-node disk p50) |
|--:|--:|--:|--:|
| 10/s | 17.0 ms | 20.3 ms | (11 ms) |
| 20/s | 16.4 ms | 23.2 ms | (11 ms) |
| 30/s | 16.1 ms | 21.9 ms | (11 ms) |
| 40/s | 15.8 ms | 23.4 ms | (12 ms) |

Throughput ceiling: **~48–52/s** at every concurrency level (1/4/16/64). At rate 48 the system is right at the knee (p50 26–47 ms, achieved ~47.7/s); above it, achieved stays pinned ~46–52/s while p50 blows into the hundreds-of-ms to seconds (coordinated-omission backlog). Same single-serialized-writer signature as single-node, ceiling roughly halved by replication.

**Replication cost ≈ +5 ms unloaded latency** (16 vs 11 ms — one QUIC round-trip to a 2/3 quorum before commit) and **~½ the throughput** (~48 vs ~100/s). Clearly durable+replicated (p50 16 ms ≫ the 3.5 ms tmpfs non-durable floor).

## Gap decomposition

Per-commit latency, unloaded, attributed by layer (each row adds to the one above):

| Layer | Contribution | Evidence |
|---|--:|---|
| Same-host transport (Aeron IPC RT) | ~0.0002 ms | Aeron p50 167 ns |
| UC shmem ring RT (client↔node↔service) | ~0.00002 ms | task08, ~15 ns SPSC p99 |
| Consensus + apply + journal batching (non-durable) | **~3.5 ms** | UC tmpfs p50 |
| **+ fsync durability (disk write barrier)** | **+~7.5 ms** | UC single-node disk p50 − tmpfs (11 − 3.5) |
| = UC single-node commit (disk) | **~11 ms** | UC single-node disk p50 |
| **+ QUIC quorum replication (loopback)** | **+~5 ms** | UC 3-node p50 − single-node (16 − 11) |
| = UC 3-node replicated commit (disk) | **~16 ms** | UC 3-node loopback p50 |

Of the 65,000× UC-vs-Aeron gap: **transport < 0.002%**; **fsync ≈ 68%** of the single-node latency; consensus/apply/batching ~32% (~3.5 ms); replication adds ~5 ms (~31% on top) for the 3-node config. The **throughput ceiling is an fsync/serialized-writer artifact in both configs** — it disappears on tmpfs single-node, and replication only lowers it further (it does not introduce a *new* dominant bottleneck). Real-NIC replication would cost more than loopback's ~5 ms (see caveats).

## Prioritized optimization backlog

1. **Journal group-commit / fsync batching — by far the #1 lever.** fsync is ~7.5 ms/commit *and* the cause of the ~100/s ceiling. The current `Durability::Consistent` path fsyncs per batch behind a single writer thread; under the open-loop driver the batch rarely coalesces multiple commits because the client waits for each response. **Action:** widen the group-commit window so concurrent in-flight commits share one fsync (amortize the 7.5 ms across N requests); pipeline fsync with the next batch's serialization. Expected impact: throughput ceiling should rise toward the tmpfs curve (250/s+), and tail latency under load should collapse. **The `uc_autobench` autoresearch loop can drive this** (it already targets journal/commit code; add a commit-throughput fitness metric).
2. **fsync mechanism** — `fdatasync` vs `fsync` (skip inode-metadata flush), and **io_uring batched/async fsync** on Linux. **Platform-gated: io_uring is Linux-only — cannot be measured on this arm64 macOS host.** Measure in a Linux CI/bench run. Expected impact: shave a fraction of the 7.5 ms and enable deeper fsync pipelining.
3. **Consensus/apply path (~3.5 ms non-durable floor)** — second-order. Worth profiling *after* fsync is addressed to see what the openraft replicate→commit→apply→respond cycle costs once durability is amortized. Lower confidence; needs a flame graph, not just this black-box bench.
4. **Replication efficiency (Phase 2, ~5 ms + halved throughput on loopback)** — only relevant once fsync is fixed (today it is dwarfed by fsync). When the leader commits, it must fsync *and* gather a quorum ack; batching AppendEntries so one fsync + one replication round covers N client commits would lift the 3-node ceiling alongside the single-node one. Same group-commit lever (item 1), applied to the replication path. Measure real-NIC cost separately (loopback understates it).
5. **Shmem rings / transport — do NOT invest.** task08 already drove SPSC to ~15 ns; transport is < 0.002% of the commit budget. Any ring work is wasted effort against this workload.

## Caveats

- **Phase 1 is in-process single-node** (`ClusterFixture`, node+service+client in one process); **Phase 2 is real multi-process** (3 separate `uc-node-launch` processes + the load driver, over QUIC). The two use different harnesses but the same `commit-path-load` driver, KV state machine, ladder methodology, and CSV schema.
- **3-node is loopback, not real NIC.** QUIC over `127.0.0.1` has ~µs link latency; a real datacenter network adds tens-to-hundreds of µs per replication round-trip. So the measured **~5 ms replication cost is a lower bound** — real-NIC 3-node latency will be higher. The *throughput* ceiling (~48/s) is loopback-flattering too.
- **tmpfs ≠ production durability.** The tmpfs run measures the *non-durable* floor to isolate fsync cost; it is not a deployment recommendation (a RAM-disk journal loses data on crash).
- **macOS host:** no io_uring; the RAM disk is `hdiutil` HFS+, not Linux tmpfs. Linux numbers will differ (likely better fsync, and io_uring available).
- **Aeron is bytes-only.** It carries no state machine and no durability — it is the *transport floor*, never an end-to-end system comparison. Do not read "UC is 65,000× slower than Aeron" as a system verdict; almost all of that is durable consensus that Aeron simply does not do.
- **KV apply is in-memory** (`HashMap`), not the `ultima_db` `StoreStateMachine` (which isn't `Default`, required by the fixture). Apply cost here is minimal; a real `ultima_db`-backed apply would add to the ~3.5 ms non-durable layer.

## What this is NOT: no direct same-workload KV head-to-head

This study is a **layered gap decomposition**, not an apples-to-apples KV contest. Aeron here is the *transport floor* (bytes echoed, no state machine, no durability, no replication); UC runs the full durable replicated KV commit path. The overlay shares methodology (open-loop, HDR, CSV schema) so curves are comparable *per layer*, but there is **no run where Aeron and UC perform the same KV workload**. The "65,000×" figure is the distance from raw same-host transport to durable consensus — not a statement that Aeron does KV 65,000× faster (Aeron core has no KV store).

A genuine direct comparison was considered and **deliberately not built** — the decomposition already identifies the optimization lever (group-commit/fsync), so a contest would not change the conclusion. If ever wanted, the two fair framings are:
- **UC vs Aeron Cluster (SMR-level).** Aeron Cluster is also a Raft replicated state machine — the true architectural peer. Build an Aeron Cluster KV service+client (Java) and run the same KV ladder against UC 3-node. Only here is "X is N× faster at KV" a meaningful claim. Largest effort.
- **UC rings vs Aeron IPC (transport-level).** Add a raw-ring echo mode to `commit-path-load` (bypass Raft) and compare against the existing Aeron IPC data. Isolates UC's shmem path vs Aeron's; modest effort; expected to show UC rings are µs-competitive (task08: ~15 ns SPSC).

## Phase 0 commit-profile findings (2026-05-30)

> **Historical — not re-runnable from this tree.** The `commit-profile`
> instrumentation that produced this section was dropped on integration (see the
> integration note at the top). The findings stand as the record that named the
> real lever; the build flag below no longer exists.

Run with the `commit-profile` build of `commit-path-load` against the in-process
`ClusterFixture` single-node. One process per `inflight` (the global counters
reset between sweep points), offered `--rates 2000`, 3 s window / 1 s warmup,
64-byte payloads. Raw logs: `bench-out/phase0-inflight-{1,4,16,64}.log`.

**Environment caveat (read first).** This run was taken on a host where the
fixture's `TempDir` storage lands on **tmpfs (`/tmp` is RAM here)**, so
`fsync` is effectively free (`fsync_ms` mean 0.000). The driver and the
in-process node+service all share **one OS thread** (`current_thread` runtime,
required by the shmem handshake). Both facts confound the headline metric — see
the verdict.

| inflight | entries/append | batch/fsync | fsyncs/100 | fsync_ms (mean/max) | actual conc (mean/max) | achieved/s | p50 / p99 |
|---|---|---|---|---|---|---|---|
| 1  | 1.000 | 1.000 | 100 | 0.000 / 0.007 | 1.0 / 1   | 285 | 1.28 s / 2.55 s |
| 4  | 1.000 | 1.000 | 100 | 0.000 / 0.011 | 4.0 / 4   | 514 | 4.25 s / 8.58 s |
| 16 | 1.000 | 1.000 | 100 | 0.000 / 0.021 | 16.0 / 16 | 519 | 4.22 s / 8.48 s |
| 64 | 1.000 | 1.000 | 100 | 0.000 / 0.028 | 63.3 / 64 | 524 | 4.13 s / 8.36 s |

Supporting numbers (all inflight): `append_lock_wait_us` mean ~0.06,
`append_lock_hold_ms` mean 0.005, `writerstate_hold_us` mean ~3.5. None of these
is a bottleneck.

**Reading the numbers:**
- **`entries_per_append = 1.000` at every inflight.** openraft hands
  `RaftLogStorage::append` exactly one entry per call, even with 64 concurrent
  in-flight commits — it is not coalescing concurrent `client_write`s into
  batched appends. This is the single most diagnostic signal and it points at an
  **openraft** lever (a dependency the autobench loop cannot edit).
- **`batch_per_fsync = 1.000` everywhere — but on tmpfs that means nothing.**
  fsync costs ~0 ms here, so the journal writer's flush for append N completes
  before append N+1 arrives; there is never a batch to coalesce. We therefore
  **cannot test the group-commit/fsync lever this plan was built around** in this
  environment. On a real disk (fsync in the ms range) `batch_per_fsync` would be
  the number to watch; here it is uninformative.
- **Throughput is flat in concurrency.** Achieved rate moves only 285 → 524/s as
  inflight goes 1 → 64 (1.8×, not 64×), and stays far below the 2000/s offered.
  The concurrency gauge (now fixed — it previously read cap−1) confirms the
  in-flight set genuinely sits at the cap (63.3/64), so this is **not** the
  Little's-law "driver not actually concurrent" artifact. There is a real serial
  floor of ~2–3.5 ms per commit that is **not** fsync, append_lock, or the
  WriterState mutex (all µs). The most likely causes are openraft's commit/tick
  cadence and the single-thread co-scheduling of driver+node+service — neither of
  which is a journal/UC group-commit tunable.

**Lever owner:** openraft (append call shape `entries_per_append = 1`, and the
commit cadence behind the ~ms serial floor) — not a surface the autobench loop
can edit, and not the fsync/group-commit surface the plan assumed.

### Phase 1 route decision: **STOP — repair the measurement environment, then re-run**

Per the spec's decision matrix, this is the STOP case, but for a sharper reason
than the original "actual concurrency ≪ cap": the concurrency metric is now
trustworthy (gauge fixed, conc ≈ cap), yet **the throughput metric is not**, on
two counts:
1. **tmpfs** removes fsync cost, so the plan's central group-commit/fsync lever
   is unmeasurable and `batch_per_fsync` is meaningless here.
2. The **single-thread in-process fixture** co-schedules the load driver with the
   node and service, so the flat ~520/s ceiling cannot be attributed to the
   commit path versus one-thread saturation.

**Required before A-vs-C routing:** re-run Phase 0 on a host with the journal on
a **real disk** (so fsync is in the ms range and group-commit can actually
coalesce), driving a **multi-process** node via `commit-path-load --connect` to a
real `uc-node-launch` (so the driver does not share a thread with the node and
service). Only then are `batch_per_fsync` and achieved-throughput trustworthy
enough to choose Route A (rich journal-tunable space) vs Route C (one openraft
knob). The `entries_per_append = 1` finding already stands regardless of host and
is the leading Route-C candidate.

**What was fixed this round:** the concurrency gauge sampled at the top of the
loop (after `select!` drained one completion, before the launch block refilled),
so it reported cap−1 every iteration (cap=4 → 3.0, cap=1 → 0.0). It now samples
after the launch block; cap=4 reads 4.0/4. Commit `8c1afba`.

## CORRECTION (2026-05-30): the harness was aimed at the wrong layer

The Phase-0 STOP section above asked for a real-disk + multi-process re-run before
routing. Two follow-up experiments below make a stronger conclusion possible now:
**the commit-path bottleneck is poll-sleep IPC latency, not journal fsync, not
group-commit, and not the openraft append call shape.** The instrumentation this
plan built measures a layer responsible for <10 % of the per-commit cost.

### Experiment 1 — concurrent client dispatcher (does openraft batch?)

Hypothesis: `entries_per_append = 1` because `spawn_client_dispatcher`
(`uc_node/src/ipc/client_dispatcher.rs`) is a single task that `.await`s each
`raft.client_write` to completion before reading the next submit frame, so
openraft never sees >1 concurrent `client_write` to coalesce.

Prototyped: rewrote the dispatcher to read frames continuously and `tokio::spawn`
each `client_write` concurrently (bounded by a `Semaphore(256)`). Result, single
node, inflight=64, rate=2000, 64 B:

| dispatcher | storage | fsync_ms | entries/append | achieved/s |
|---|---|---|---|---|
| serial (baseline)     | tmpfs | 0.000 | 1.000 | 524 |
| concurrent (prototype) | tmpfs | 0.000 | 1.016 | 570 |
| concurrent (prototype) | ext4  | 0.319 | 1.025 | 671 |

**Hypothesis rejected.** Even with concurrent `client_write` *and* real fsync
latency (ext4, 0.32 ms mean / 2.66 ms max) *and* 64 in-flight requests,
`entries_per_append` stays ≈ 1. openraft does **not** coalesce here. Most likely
because on a **single-node** cluster there is no follower-replication wait to
open a batching window — the core commits each entry before the next is dequeued.
openraft's published 33k→912k→3.5M ops/s figures are a single in-process
benchmark with a no-I/O `BTreeMap` store; the high-concurrency numbers depend on
the multi-node replication window and/or the explicit `client_write_many` batch
API. They are **not** comparable to UC's multi-process durable pipeline. The
prototype was reverted (working tree unchanged).

### Experiment 2 — accounting for the ~3.5 ms single-client floor

At inflight=1 UC commits at ~285/s = **3.5 ms/commit**. ext4 fsync is **0.32 ms**.
So ~3.2 ms (>90 %) is **not** fsync. It is poll-sleep latency: every stage of the
commit round-trip wakes its consumer by *polling a ring on an idle backoff*, not
by an event signal. The hops, with the constant from each source file:

1. client writes submit frame → node `client_dispatcher` polls submit ring —
   `POLL_BACKOFF = 100 µs` (`client_dispatcher.rs:34`)
2. node `client_write` → append → fsync (0.32 ms) → commit → apply frame to service
3. service `apply_loop` polls apply ring — `IDLE_BACKOFF = 100 µs` via
   **`std::thread::sleep`** (`uc_service/src/runtime/apply_loop.rs:35,104`)
4. service apply response → node consumes
5. node broadcasts response → client `broadcast_reader` polls response ring —
   `sleep(100 µs)` (`uc_client/src/rings.rs:112`)

(Plus coarser tickers — `service.rs:510` 2 ms, various 100 ms liveness/stall
watchers — that add tail latency.) Under the single-thread fixture these sleeps
do not overlap; they serialize, and step 3 blocks the whole thread. A handful of
~100 µs–2 ms idle-backoff sleeps per commit, serialized, is the missing ~3 ms.

### Conclusion / real lever

The orders-of-magnitude gap vs openraft decomposes into, in priority order:
1. **Poll-sleep IPC latency (dominant).** Rings are drained by polling every
   100 µs–2 ms instead of event-driven wakeups (futex / eventfd / condvar). This
   sets the ~3 ms/commit single-client floor. **This is the real optimization
   lever** — and it is an architecture change, not a tunable knob, and not the
   journal/group-commit surface this whole plan instrumented.
2. **Single-node Raft cannot batch appends** (`entries_per_append ≈ 1`, proven in
   Exp 1) — caps any coalescing win until multi-node or `client_write_many`.
3. **Single-thread fixture** serializes driver + node + service, preventing even
   the poll sleeps from overlapping.

fsync / group-commit (the plan's target) contributes ~0.32 ms of 3.5 ms (~9 %).

**Revised verdict: neither Route A nor Route C.** Do not build the autobench loop
over journal knobs and do not hand-tune group-commit — both optimize a ~9 % layer.
The commit-profile instrumentation did its job: it ruled out the journal and named
the real lever (it has since been removed — see the integration note at the top).
The recommended next step — an **event-driven ring-wakeup** prototype starting with
the service `apply_loop` `std::thread::sleep` — was subsequently built and shipped
as **task11**, collapsing the commit-path floor ~4.6× and confirming this verdict.
