# Task 13 — Aeron IPC vs ultima_cluster commit-path benchmark

**Date:** 2026-05-30 (investigation) → 2026-06-06 (harness integrated + Linux re-run).
**Status:** Closed. Single-node + 3-node phases complete; reusable harness merged to `main`.

**Provenance.** The investigation originally lived on the abandoned
`bench/aeron-vs-uc-commit-path` branch. Its decisive finding — the commit floor
is **poll-sleep IPC latency, not fsync** — was acted on and shipped as **task11
(event-driven ring wakeups)**, which collapsed the commit-path floor ~4.6×. What
landed here is the **reusable harness** (`commit-path-load` driver,
`uc-node-launch` launcher, run/plot scripts) plus this writeup. The Phase-0
`commit-profile` instrumentation was **dropped on integration**: its journal-side
half (`uc_journal::commit_profile`) was never committed and is unrecoverable,
and its verdict is already harvested — the fsync-vs-poll-sleep numbers it produced
are kept below as historical record, not as something re-runnable from this tree.
Design scaffolding: `docs/superpowers/specs/2026-05-30-aeron-vs-uc-commit-path-benchmark-design.md`
+ `…/plans/2026-05-30-aeron-vs-uc-commit-path-benchmark.md`.

---

## 1. What this is

A **layered gap decomposition** of ultima_cluster's full durable commit path
against Aeron same-host IPC — not an apples-to-apples KV contest (Aeron carries
bytes only; see §7). Aeron sets the *transport floor* (what is physically
achievable same-host); UC's layers show where the time actually goes, so
"improve UC" means attacking the dominant layer first.

The headline: same-host **transport is a rounding error** in UC's commit budget
(Aeron ~167 ns; UC shmem rings ~15 ns per task08). The cost is **consensus +
journal durability + replication**, and — as the investigation found and task11
confirmed — the original latency floor was dominated by **poll-sleep ring wakeups**,
since fixed.

## 2. The harness

Two `uc_autobench` binaries plus scripts, all merged to `main`:

- **`commit-path-load`** — open-loop, **coordinated-omission-free** load driver:
  each request's latency is measured from its *intended* send time (`run_step`
  advances `next_send` by `1/rate` regardless of actual dispatch), so backlog past
  saturation is captured honestly. Sweeps a **rate ladder × in-flight concurrency**,
  records an **HDR histogram** per step, emits a shared **13-column CSV**. Two modes:
  in-process `ClusterFixture` single-node, or `--connect <instance-dir>` to attach
  to a running cluster. Uses a `current_thread` runtime for the in-process path
  (the shmem handshake intermittently times out under `multi_thread`).
- **`uc-node-launch`** — wraps `NodeBuilder` with `BootstrapConfig::Peers` +
  `IpcMode::Shmem` + self-signed TLS to launch a real multi-process N-node cluster
  over QUIC loopback. `log_durability = Consistent` (durable commit path under test).
- **Scripts:** `run-uc-single-node.sh` (disk + tmpfs targets), `run-uc-3node.sh`
  (spawns 3 nodes, probes for the leader, drives the ladder against it),
  `aeron_hdr_to_csv.py`, `plot_decomposition.py`. Both run scripts resolve the
  cargo target dir via `cargo metadata` (work under a relocated `CARGO_TARGET_DIR`);
  the 3-node script has a hardened teardown (SIGINT → bounded wait → SIGKILL → `wait`
  to reap, also on Ctrl-C) so no node processes leak.

**Methodology.** Both UC and Aeron run the same open-loop ladder and emit
HDR histograms so curves overlay. CSV schema:
`system, config, workload, payload_bytes, inflight, target_rate, achieved_rate, p50, p99, p99_9, p99_99, max, count`.
The headline per config is the **knee** — the offered load where p99 hockey-sticks
and achieved-rate falls below target. Fairness controls disclosed, not hidden:
identical payloads, same host, disclosed fsync target (real disk vs tmpfs),
disclosed + swept in-flight concurrency, `--release` builds. CSVs/plots live under
the gitignored `bench-out/`.

---

## 3. Results — Linux (current, post-task11)

In-process `ClusterFixture` single-node and real 3-node loopback, current `main`
(includes task11 event-driven wakeups), `--release`, `RUST_LOG=off`, 64-byte
payload, in-memory `KvSm` apply. **Disk = ext** (`/home/claude`, real non-tmpfs
durability); **tmpfs = `/dev/shm`**.
Reproduce: `RATES=… INFLIGHT=… TMPDIR=<ext-path> bash uc_autobench/scripts/run-uc-single-node.sh`
and `DATA_ROOT=<ext-path> RATES=… INFLIGHT=… bash uc_autobench/scripts/run-uc-3node.sh`.
**Note:** on this host both `/tmp` and `/dev/shm` are tmpfs — the "disk" run must
point `TMPDIR`/`DATA_ROOT` at a real ext path or it silently measures tmpfs.

### Single-node — unloaded latency floor (inflight=1, below the knee)

| target rate | ext disk p50 | ext disk p99 | tmpfs p50 | tmpfs p99 |
|--:|--:|--:|--:|--:|
| 1000/s | 1.03 ms | 2.28 ms | 0.76 ms | 1.76 ms |
| 2000/s | 1.13 ms | 7.11 ms | 0.78 ms | 2.98 ms |
| 5000/s | *(saturated)* | — | 0.76 ms | 6.79 ms |

**Unloaded commit latency: ext disk ≈ 1.0 ms, tmpfs ≈ 0.76 ms** — fsync adds only
**~0.25 ms** here.

### Single-node — throughput ceiling (achieved at/above the knee)

| target rate | ext disk achieved | tmpfs achieved (if=1 / if=8) |
|--:|--:|--:|
| 2000/s  | 2000/s (below knee) | 1999/s / 1999/s |
| 5000/s  | **~2460/s** (saturated) | 4998/s / 4996/s (sustained) |
| 10000/s | **~2500/s** (saturated) | **~6190/s** / **~8180/s** |

- **ext disk: hard ceiling ≈ 2500/s**, flat across inflight 1→128 (single
  serialized-writer signature).
- **tmpfs: ≈ 6000/s (if=1), ≈ 8000/s (if=8)** — fsync caps throughput ~2.5–3×.

### 3-node QUIC loopback (ext disk, durable quorum replication)

| target rate | 3-node p50 | 3-node achieved | (single-node disk) |
|--:|--:|--:|--:|
| 100/s  | 3.0 ms  | 100/s              | ~1.0 ms / 100/s |
| 500/s  | 4.0 ms* | ~500/s (sustained) | ~1.0 ms / 500/s |
| 1000/s | *(saturated)* | ~590/s       | ~1.0 ms / 1000/s |
| 2000/s | *(saturated)* | ~600/s       | ~1.1 ms / 2000/s |

\* inflight=8; at inflight=1 the 500/s step is at the edge (p50 ~10 ms, heavy p99
tail). Sustainable throughput **~500–600/s** across inflight {1,8,32}.

- **Unloaded latency ≈ 3.0 ms** — replication adds **~2 ms** (one QUIC round-trip
  to a 2/3 quorum on loopback).
- **Throughput ceiling ≈ 600/s** — replication cuts throughput **~4×** vs
  single-node.

### What changed vs the original macOS investigation

| metric | macOS (pre-task11) | **Linux (post-task11)** | delta |
|---|--:|--:|--:|
| Unloaded p50, single-node disk | ~11 ms | **~1.0 ms** | ~10× lower |
| Unloaded p50, single-node tmpfs | ~3.5 ms | **~0.76 ms** | ~4.6× lower |
| Throughput ceiling, single-node disk | ~85–110/s | **~2500/s** | ~25× higher |
| Unloaded p50, 3-node | ~16 ms | **~3.0 ms** | ~5× lower |
| Throughput ceiling, 3-node | ~48/s | **~600/s** | ~12× higher |
| fsync latency cost | ~7.5 ms | **~0.25 ms** | host-dependent |

The **tmpfs floor collapse (~3.5 ms → ~0.76 ms)** is the task11 poll-sleep
removal, landing exactly where this investigation said the lever was (~4.6×,
matching task11's own measurement). The much smaller fsync *latency* cost is
platform (this host's ext fsyncs sub-ms vs macOS HFS+ at ~7.5 ms). Two
consequences:

- fsync still **caps throughput** (single-node ~2500 vs ~6000–8000/s), so journal
  group-commit remains the next *throughput* lever — but it is no longer a
  *latency* wall.
- The single→3-node throughput drop is **steeper on Linux (~4×) than macOS (~2×)**:
  with the fsync latency wall gone, the QUIC quorum round-trip + replica append is
  now the **dominant** commit cost, not a second-order add-on.

---

## 4. Results — macOS (original investigation, pre-task11)

Apple Silicon arm64. Phase 1 in-process single-node `ClusterFixture`; Phase 2 real
3-node multi-process over QUIC loopback. tmpfs = `hdiutil` HFS+ RAM disk.

### Aeron IPC transport floor (bytes only)

| payload | p50 | p99 | p99.9 | max |
|--:|--:|--:|--:|--:|
| 8 B   | 167 ns | 375 ns | 3.0 µs | 3.98 ms* |
| 64 B  | 167 ns | 583 ns | 6.5 µs | 47 µs |
| 256 B | 208 ns | 2.08 µs | 8.7 µs | 4.90 ms* |

\* isolated max outliers over 1 M messages (OS jitter), not typical.

### UC single-node (disk vs tmpfs)

- **Unloaded latency:** disk ≈ 11 ms, tmpfs ≈ 3.5 ms (flat in concurrency below
  saturation). fsync ≈ 7.5 ms of every commit.
- **Throughput ceiling:** disk ~85/s at inflight=1, rising only to ~100–110/s at
  inflight 4–64 (64× concurrency buys ~1.3×); tmpfs had **no ceiling** in the
  tested range (sustained 250/s, p50 ~3.5 ms). Past the knee p50 climbs into
  seconds (coordinated-omission backlog) — throughput-bound.

### UC 3-node loopback

Unloaded p50 ≈ 16 ms (+~5 ms vs single-node = one QUIC quorum round-trip);
ceiling ~48–52/s (roughly halved by replication). Clearly durable+replicated
(16 ms ≫ the 3.5 ms non-durable tmpfs floor).

### Gap decomposition (unloaded, by layer)

| Layer | Contribution | Evidence |
|---|--:|---|
| Same-host transport (Aeron IPC RT) | ~0.0002 ms | Aeron p50 167 ns |
| UC shmem ring RT | ~0.00002 ms | task08, ~15 ns SPSC p99 |
| Consensus + apply + journal (non-durable) | ~3.5 ms | UC tmpfs p50 |
| + fsync durability | +~7.5 ms | disk − tmpfs (11 − 3.5) |
| = UC single-node commit (disk) | ~11 ms | UC disk p50 |
| + QUIC quorum replication (loopback) | +~5 ms | 3-node − single (16 − 11) |
| = UC 3-node replicated commit | ~16 ms | UC 3-node p50 |

> These macOS layer attributions predate task11. The "+7.5 ms fsync" line is a
> macOS-HFS+ artifact (Linux ext: ~0.25 ms), and the "~3.5 ms non-durable floor"
> was later shown to be dominated by poll-sleep IPC, not consensus/apply — see §5.
> The *structure* (transport ≪ durability ≪ replication) is what holds.

---

## 5. How the conclusion was reached (Phase-0, settled)

Phase-0 ran the (now-removed) `commit-profile` instrumentation against the
in-process single-node fixture. The single most diagnostic signal:
**`entries_per_append = 1.000` at every inflight (1/4/16/64)** — openraft hands
`RaftLogStorage::append` exactly one entry per call even with 64 concurrent
in-flight commits; it is **not** coalescing. Throughput was flat in concurrency
(285 → 524/s as inflight 1 → 64), with a real ~2–3.5 ms serial floor that
`append_lock` (~µs), the WriterState mutex (~µs), and fsync (~0 ms on the tmpfs
host) could not explain.

Two follow-up experiments closed it out:

1. **Concurrent client dispatcher.** Hypothesis: `entries_per_append = 1` because
   `spawn_client_dispatcher` `.await`s each `raft.client_write` before reading the
   next frame. Prototype (read continuously, `tokio::spawn` each write, `Semaphore(256)`):

   | dispatcher | storage | fsync_ms | entries/append | achieved/s |
   |---|---|---|---|---|
   | serial (baseline)     | tmpfs | 0.000 | 1.000 | 524 |
   | concurrent (prototype) | tmpfs | 0.000 | 1.016 | 570 |
   | concurrent (prototype) | ext4  | 0.319 | 1.025 | 671 |

   **Rejected:** even with concurrent writes + real fsync + 64 in-flight,
   `entries_per_append` stays ≈ 1 — a single-node cluster has no
   follower-replication wait to open a batching window. (Prototype reverted.)

2. **Accounting for the ~3.5 ms single-client floor.** At inflight=1, ~285/s =
   ~3.5 ms/commit; ext4 fsync = 0.32 ms. So **>90 % is not fsync** — it is
   poll-sleep latency: every ring hop wakes its consumer by polling on a
   100 µs–2 ms idle backoff rather than an event signal —
   `client_dispatcher` `POLL_BACKOFF=100 µs`, service `apply_loop`
   `IDLE_BACKOFF=100 µs` (a `std::thread::sleep`), client `broadcast_reader`
   `sleep(100 µs)`, plus coarser tickers. Serialized under the single-thread
   fixture, a handful of these per commit is the missing ~3 ms.

**Verdict (confirmed by task11):** the dominant lever was **poll-sleep IPC
latency** — an architecture change (event-driven wakeups), not the
journal/group-commit surface the plan instrumented (fsync was ~9 % of the floor).
The recommended next step (event-driven ring wakeups, starting with the
`apply_loop` `std::thread::sleep`) was built and shipped as **task11**,
collapsing the floor ~4.6× — exactly as predicted (see §3).

---

## 6. Remaining levers

1. **Journal group-commit / fsync batching — the #1 *throughput* lever.** fsync no
   longer dominates *latency* (task11), but it still caps *throughput* (single-node
   ~2500 vs ~6000–8000/s tmpfs). Widening the group-commit window so concurrent
   in-flight commits share one fsync should lift the disk ceiling toward the tmpfs
   curve. The `uc_autobench` autoresearch loop can drive this (add a
   commit-throughput fitness metric). **→ Implemented and measured (2026-06-13);
   see §9. The journal change is real (+275% on its own group-commit microbench
   here) but does NOT move the UC commit floor — that floor is gated by the
   `submit_to_node` poll-sleep stage, not journal durability.**
2. **Replication batching (3-node).** Now the dominant 3-node cost (~4× throughput
   drop, ~2 ms latency). Batching AppendEntries so one fsync + one quorum round
   covers N client commits lifts the 3-node ceiling — the same group-commit lever
   applied to replication. Measure real-NIC cost separately (loopback understates).
3. **fsync mechanism** — `fdatasync` vs `fsync`, io_uring batched/async fsync
   (Linux-only; now measurable on this host). Second-order; shaves fsync + enables
   deeper pipelining.
4. **Consensus/apply path** — the residual non-durable floor; profile with a flame
   graph once group-commit is addressed. Lower confidence.
5. **Shmem rings / transport — do NOT invest.** task08 drove SPSC to ~15 ns;
   transport is <0.002 % of the commit budget.

## 7. Caveats

- **Phase 1 in-process single-node** (`ClusterFixture`) vs **Phase 2 real
  multi-process** (3 `uc-node-launch` processes over QUIC) — different harnesses,
  same `commit-path-load` driver, KV state machine, ladder, and CSV schema.
- **3-node is loopback, not real NIC** — `127.0.0.1` adds ~µs; a real datacenter
  network adds tens-to-hundreds of µs per replication round-trip, so the measured
  replication cost and the 3-node ceiling are **lower bounds**.
- **tmpfs ≠ production durability** — it measures the non-durable floor to isolate
  fsync; a RAM-disk journal loses data on crash.
- **Disk fsync cost is host-specific** — the Linux "disk" target is the container's
  ext2/3 on `/home/claude` (likely SSD/overlay-backed); the absolute fsync number
  is not representative of a spinning disk or a networked block device. The
  *relative* disk-vs-tmpfs gap is the portable signal. macOS used `hdiutil` HFS+.
- **Aeron is bytes-only** — the transport floor, never an end-to-end system. Do not
  read the raw UC-vs-Aeron ratio as a system verdict; almost all of it is durable
  consensus Aeron does not do.
- **KV apply is in-memory** (`HashMap`), not the `ultima_db` `StoreStateMachine`
  (which isn't `Default`, required by the fixture). A real `ultima_db`-backed apply
  would add to the non-durable layer.

## 8. What this is NOT — and the fair head-to-heads

This is a **layered decomposition**, not an apples-to-apples KV contest: there is
**no run where Aeron and UC perform the same KV workload** (Aeron core has no KV
store). A direct comparison was considered and **deliberately not built** — the
decomposition already identifies the levers (§6). If ever wanted, the two fair
framings are:

- **UC vs Aeron Cluster (SMR-level).** Aeron Cluster is also a Raft replicated
  state machine — the true architectural peer. Build an Aeron Cluster KV
  service+client and run the same KV ladder against UC 3-node. Only here is "X is
  N× faster at KV" meaningful. Largest effort.
- **UC rings vs Aeron IPC (transport-level).** Add a raw-ring echo mode to
  `commit-path-load` (bypass Raft) and compare against the Aeron IPC data. Modest
  effort; expected to show UC rings are µs-competitive (task08: ~15 ns SPSC).

---

## 9. Follow-up (2026-06-13) — journal group-commit lever: implemented, measured, doesn't move the UC floor

`uc_journal` landed the §6 lever #1 work (autoresearch run, merged as
`ultima_db` `eabe345`). The champion commit `4fcb939` *"fsync dup'd fd, release
`state.lock` across `sync_all`"* (preceded by `fec3094`, fsync re-drain
coalescing, +21%) drops the journal's state lock while a writer thread is blocked
in `sync_all`, so concurrent in-flight appends coalesce into the same fsync
instead of serializing behind it. It touches only `segment.rs` + `writer.rs`.

I re-ran benchmarks as a **controlled A/B on this host** rather than comparing
against `bench-out/reference/` (that reference was captured on a different,
real-disk machine — a direct compare would be confounded). BEFORE = journal at
`2249b81` (pre-optimization); AFTER = `ultima_db` `main` (`eabe345`). Method:
roll just the two changed files back/forward, rebuild each side, run on **real
disk** (`/dev/sda1` ext4 — note `/tmp` *and* `/dev/shm` are both tmpfs on this
host, where fsync is a no-op and the lever is invisible by construction).

**Journal's own group-commit microbench** (`ultima-autobench journal-microbench`,
256-entry bursts, `Durability::Consistent`, ext4, median-of-5):

| | `group_commit_throughput` (entries/s, median) |
|---|---|
| BEFORE (`2249b81`) | **53,266** |
| AFTER  (`eabe345`) | **200,038** |

**≈ 3.8× / +275%** — even larger than the +213% the autoresearch loop recorded,
because this VM's fsync is slow enough to make the lock-release coalescing pay
off strongly. The win at the journal layer is unambiguous.

**ultima_cluster commit path** (`attribution-bench`, in-process single-node
fixture, real-disk ext4, `journal_durable` stage, ns p50 / p99):

| inflight | BEFORE p50 / p99 | AFTER p50 / p99 |
|---|---|---|
| 8  | 12983 / 59423 | 12791 / 114111 |
| 32 | 12647 / 43871 | 12687 / 164735 |
| 64 | 12903 / 41503 | 12871 / 157823 |

`journal_durable` p50 is **flat (~12.7 µs)** both ways; the p99 swings are noise
bleeding in from the saturated submit stage. The dominant stage is
**`submit_to_node`** (3.8–27 ms p99, growing with inflight = saturation),
outweighing `journal_durable` by ~100–1000×.

**Why the layer win doesn't propagate.** The microbench fires a 256-entry burst
and waits only the last notifier — maximal append concurrency, so fsync
coalescing is fully exercised. In the UC fixture the commit path is gated by the
`submit_to_node` poll-sleep stage, so entries reach the journal already batched;
openraft amortizes many entries per fsync (hence ~12.7 µs/entry attributed durable
cost despite a single ext4 fsync being ms-scale here). The +275% throughput lets
the journal *sustain* more load, but the journal is not on UC's critical latency
path — confirming the standing headline that **durability is a rounding error in
UC's commit budget; the floor is the IPC/poll-sleep + consensus path.** The next
real lever for UC commit latency remains the submit/apply enqueue stages, not the
journal.

A/B CSVs: `bench-out/ab/{before,after}_disk_if{8,32,64}.csv` (untracked working
artifacts). Reproduce by checking out the two journal files at each ref,
rebuilding `attribution-bench --features uc-bench-probes` and
`ultima-autobench --bin journal-microbench`, both pointed at a real-disk path.

---

## 10. Follow-up (2026-06-14) — `max_payload_entries` replication-batch lever: NULL result on 3-node

Tested §6 lever #2's first knob: does raising openraft's `max_payload_entries`
(entries packed per AppendEntries RPC) lift the 3-node throughput ceiling? The
knob was plumbed through `RaftTuning` → openraft `Config` (`builder.rs`),
env-overridable via `UC_MAX_PAYLOAD_ENTRIES` in `uc-node-launch` (commit
`4336e2d`). Sweep harness: `uc_autobench/scripts/sweep-max-payload.sh` — fresh
3-node QUIC-loopback cluster per value, **real-disk ext4 journal**
(`log_durability = Consistent`), open-loop ladder driven against the leader.

Swept `max_payload_entries ∈ {300 (default), 1024, 4096}` at `inflight {64, 512}`,
rate ladder 500→10000 msgs/s, 64 B payload:

| inflight | target msgs/s | achieved (300 / 1024 / 4096) | p99 ms (300 / 1024 / 4096) |
|---|---|---|---|
| 64  | 500   | 500 / 500 / 500 | 82.4 / 13.6 / **5.6** |
| 64  | 1000  | 563 / 578 / 599 | 3844 / 3607 / 3318 |
| 64  | 5000  | 598 / 567 / 592 | 36474 / 38722 / 36910 |
| 512 | 500   | 500 / 500 / 500 | 18.9 / 5.0 / **4.5** |
| 512 | 1000  | 604 / 582 / 594 | 3181 / 3486 / 3314 |
| 512 | 10000 | 582 / 581 / 585 | 80128 / 80262 / 79725 |

**Peak achieved per config: 604 / 590 / 600 msgs/s — within noise.** The 3-node
ceiling is ~600 msgs/s regardless of the knob; past the ~500–600/s knee, p99
hockey-sticks into seconds (open-loop backlog) identically across all three.

**Why it's inert.** `max_payload_entries` caps the replication batch, but the
actual batch is `min(entries_pending_replication, cap)`. On fast loopback the
consensus+apply+IPC pipeline gates entries *before* the replication stage, so
fewer than 300 entries are ever pending for a single AppendEntries — even at
`inflight = 512`. The default 300 never binds; raising it cannot help. The knob
would only matter if the leader could append far faster than the network
round-trips (slow / high-latency NIC), which loopback is not. The 3-node ceiling
is set by **per-commit pipeline latency**, the same `submit_to_node` poll-sleep +
apply floor as single-node — not replication payload size.

**Minor real effect:** below the knee (target = 500/s, unsaturated) a larger cap
trimmed the p99 tail (82 → 5.6 ms at inflight=64) — a brief burst draining in one
RPC. It does not move the ceiling.

**Conclusion.** Neither `max_payload_entries` (this §) nor `api_batch_capacity`
(already 4096, `linger = 0`, rarely fills — analyzed, not swept) is the throughput
limiter in the same-host/loopback regime. The knob stays exposed (`4336e2d`) as a
real lever for a future high-latency-network deployment, but is a no-op here. §6
lever #2's *latency* half (AppendEntries fan-out / pipelining) is untouched and
remains the open question for 3-node; replication **payload size** is settled-null.

Sweep CSVs: `bench-out/maxpayload/mpe_{300,1024,4096}.csv` (untracked). Reproduce:
`bash uc_autobench/scripts/sweep-max-payload.sh` (real-disk `DATA_ROOT`).

---

## 11. Follow-up (2026-06-14) — Aeron-Cluster head-to-head: pipeline runnable end-to-end, but this host can't produce valid numbers

First attempt at §8's "fair framing #1" (UC vs Aeron Cluster, SMR-level). The
`uc_autobench/bench-parity/` infra (config + `RUN-PARAMS.md` + the `commit-path-load`
`.hgrm` exporter) was driven against a real Aeron cluster.

**What was proven — the full pipeline is runnable.** On this host: cloned
`aeron-io/benchmarks`, built `./gradlew deployTar` (works on both JDK 26 and the
installed Temurin **JDK 21 LTS**), brought up a real **3-node Aeron cluster**, and
ran `LoadTestRig` producing HdrHistograms. Wiring issues cleared along the way:
`io.aeron.benchmarks.output.directory` must be `-D`-set; archive requires
`catalog.file.sync.level ≥ file.sync.level`; stale JVMs must be reaped between runs
(port `Address already in use`); a backgrounded cluster must outlive the launching
shell (`setsid`/persistent task — the harness reaps the foreground process group).

**Config bug found — the repo's IPC-ingress variant does not work multi-node.**
`bench-parity/aeron-cluster-ipc/` sets a global `aeron.cluster.ingress.channel=aeron:ipc`
while the members list still carries per-member ingress endpoints; at election
complete a **follower crashes** in `connectIngress`:
`InvalidChannelException: UdpChannel only supports UDP media: aeron:ipc?endpoint=localhost:21000`.
So the shmem client-edge config (the whole point of that variant) is not yet
viable on current aeron-benchmarks. Fell back to the canonical **UDP-ingress**
`cluster_localhost` (client edge = UDP loopback, not shmem — a known, small caveat
per §3: transport is a rounding error vs ms-scale consensus).

**Numbers obtained — and why they are INVALID here.** 3-node, 500 msgs/s, 64 B,
matched durability:

| System | durability | p50 | p99 | valid |
|---|---|---|---|---|
| UC (openraft), inflight 512 | Consistent (fsync) | 2.1 ms | 18.9 ms | ✅ achieved 500/s |
| UC, inflight 64 | Consistent | 2.4 ms | 82.4 ms | ✅ achieved 500/s |
| Aeron | fsync (sync.level=2) | 107 ms | 202 ms | ❌ `.FAIL` (0.04% loss) |
| Aeron | non-durable (sync.level=0) | 101 ms | 161 ms | ❌ `.FAIL` |

Aeron shows a flat **~100 ms latency floor proven independent of rate (500 vs 1000),
durability (fsync vs none), and heartbeat interval (200 ms vs 10 ms)**. Root cause:
**the host is 4 vCPUs and ran at load average ~28** (154 Java threads across 7 JVMs).
Aeron **busy-spins every agent thread** (driver sender/receiver/conductor +
consensus + archive + service, ×3 nodes +4 drivers); on 4 cores those spin threads
starve and every round-trip waits ~100 ms for a scheduler slice — that IS the floor.
UC's `tokio` async runtime parks rather than spins, so it tolerates oversubscription
(its 2 ms p50 is plausible), but UC's own 3-node also saturated at ~600/s here. **Both
sides are host-limited and the comparison is actively unfair to Aeron** (engineered
for single-digit-µs on a provisioned box). Reporting "UC 2 ms vs Aeron 100 ms" would
be misleading and is NOT a result.

**Verdict.** Runnability: ✅ proven end-to-end. Valid parity data: ❌ not obtainable
on a 4-vCPU shared VM — Aeron's busy-spin model needs **physical cores ≥ its spin-thread
count** on a quiet dedicated machine (as `RUN-PARAMS.md` already stipulates). Open
work before a real run: (1) fix the IPC-ingress follower crash if the shmem client
edge is wanted; (2) run on a host with enough cores; (3) confirm `achieved == target`
(no `.FAIL`) at every rung. Build/run recipe and the `.FAIL` histograms were scratch
under `/home/claude/` (aeron-benchmarks clone, deployTar, parity-run) — not committed.

---

## 12. Follow-up (2026-06-16) — FIRST VALID Aeron-vs-UC parity result (provisioned 3-node Hetzner)

§11's run was host-invalid (Aeron busy-spin starved on a shared 4-vCPU box → flat
~100 ms floor). This run used the `bench-infra/` Terraform+Ansible rig (see
`docs/superpowers/specs/2026-06-14-bench-infra-terraform-ansible-design.md`) to
provision **3 dedicated 8-vCPU Hetzner CCX33 nodes** (one cluster node per host,
client co-located with node0), build both systems, and drive the matched sweep.

**Config:** 3-node cross-host (real UDP for Aeron / QUIC for UC), 64 B payload,
**durable** (UC `Durability::Consistent`; Aeron `archive.file.sync.level=1`), rate
ladder 100→20000 msgs/s, 10 s measure + 2 s warmup per rung. UC `--inflight 128`;
Aeron `LoadTestRig` open-loop. Aeron client edge = UDP-loopback on node0 (the shmem
IPC-ingress variant is still gated behind §11's follower-crash bug); UC client edge =
shmem. kernel 6.8, ext4.

| target/s | UC achieved | UC p50 | UC p99 | Aeron p50 | Aeron p99 | Aeron sent |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 100 | 2.4 ms | 3.4 ms | 3.1 ms | 15.9 ms | 1 000 ✓ |
| 500 | 500 | 1.9 ms | 2.9 ms | 3.5 ms | 15.4 ms | 5 000 ✓ |
| 1 000 | 804 (sat) | 1 264 ms | 2 422 ms | 2.8 ms | 14.0 ms | 10 000 ✓ |
| 2 000 | 781 (sat) | 7.7 s | 15.4 s | 2.0 ms | 14.3 ms | 20 000 ✓ |
| 5 000 | 777 (sat) | 27 s | 54 s | 1.0 ms | 14.7 ms | 50 000 ✓ |
| 10 000 | 775 (sat) | 59 s | 118 s | 0.27 ms | 8.3 ms | 100 000 ✓ |
| 20 000 | 759 (sat) | 128 s | 251 s | 0.30 ms | 6.9 ms | 200 000 ✓ |

**Findings (valid):**
- **Throughput:** UC saturates at **~800 msgs/s** (achieved caps 760–804 regardless
  of offered load); **Aeron sustained all rungs through 20 000/s with zero loss** and
  never reached its knee → **Aeron ≳25× UC's throughput ceiling** on identical hardware.
- **Latency below UC's knee (≤500/s): UC is competitive / tighter** — UC p99 ~3 ms vs
  Aeron ~15 ms. Aeron's low-rate latency is its idle path; under load its pipeline
  warms and p50 drops to **0.27 ms** (vs UC's best 1.9 ms). Past ~800/s UC's numbers
  are saturation backlog (coordinated-omission), not steady state.
- Matches the architecture: Aeron is a mature batching/pipelining SMR; UC is early and
  throughput-gated by its submit/apply path (task13 floor). UC's ~800/s here vs ~600/s
  on the local 4-vCPU box shows it scales modestly with hardware but is pipeline-, not
  host-, limited.

**Aeron is NOT starved here** (p50 sub-ms, max ~20–43 ms, full counts) — the §11
host-artifact floor is gone, confirming the rig + dedicated cores produce trustworthy
numbers. This is the first apples-to-apples (matched durability, same hosts, one axis =
the system) UC-vs-Aeron data point.

**Rig bugs this live run surfaced and fixed** (now on `main`): non-active cloud
provider blocks demanded creds (dummy-cred fix); `build_uc` didn't ship the external
path-dep `../ultima_db`; `ansible.cfg` used the removed `community.general.yaml`
callback; toolchains provenance task missed `RUSTUP_HOME`; `run` role `pkill -f
'io.aeron'` self-killed its own shell (bracket-trick fix) + no pre-run cleanup; UC
`uc-peers.env` used `source` (bashism, dash-incompatible) with unquoted space-bearing
values; collect rsync lacked `--mkpath`. Also added `UC_DURABILITY` env to
`uc-node-launch` so the matched-durability knob reaches UC.

Results: `bench-out/dist/20260616T192626Z/` — `node0/uc_sweep.csv`, 7 Aeron `.hdr`,
`manifest.txt`, and `summary_uc_vs_aeron.csv` (combined). Reproduce: `cd bench-infra &&
make up && make bench` (needs the Hetzner dedicated-vCPU quota for 3×CCX33).

**Open:** find Aeron's actual knee (rungs >20 000/s); fix §11 IPC-ingress to match the
shmem client edge; sweep the non-durable posture; raise UC's throughput ceiling (its
submit/apply path remains the lever, per §6).

---

## 13. Follow-up (2026-06-17) — autoresearch loop raised UC 3-node throughput ~28×

Using the `uc_autobench` distributed-throughput loop (spec
`docs/superpowers/specs/2026-06-16-uc-autobench-distributed-throughput-loop-design.md`),
driven against a persistent UC-only Hetzner fleet via `bench-infra` `make up-uc` +
`make iterate` (each iteration: edit → local compile → lincheck capstone gate →
cloud 3-node sweep → fitness = `max(achieved_rate)`). UC-only, durable, 64 B,
inflight 128. **Every kept change stayed linearizable (lincheck gate green).**

This fleet's baseline was ~205 msgs/s — ~4× below §12's fleet (inter-node RTT
varies; Hetzner has no low-latency placement group), but **stable to ~2%** (a
re-run baseline gave 207), so single-run keep/discard signals were trustworthy.

| iter | change | throughput | knee | verdict |
|---|---|---:|---:|---|
| baseline | — | 205/s | 100/s | stable |
| 1 | `api_batch_linger_ms=2` (alone) | 153/s | 100/s | ❌ discard |
| 2 | **concurrent `client_write` dispatch** (`d0c7856`) | **1996/s** | 1000/s | ✅ 9.7× |
| 3 | + `api_batch_linger_ms=2` (`1ad22fd`) | **4468/s** | 2000/s | ✅ +2.24× |
| 4 | `api_batch_linger_ms=5` (`77c5d7d`) | **5790/s** | 5000/s | ✅ +30% |
| 5 | `api_batch_linger_ms=10` | 4641/s | 2000/s | ❌ discard (overshoot) |

**Net: 205 → 5790 msgs/s ≈ 28×; knee 100/s → 5000/s.**

**Root cause & fix.** `uc_node/src/ipc/client_dispatcher.rs` awaited
`raft.client_write()` **inline** in the submit read-loop, fully serializing
commits at ~1/commit-latency (≈205/s here). Spawning the commit+broadcast per
submit (`d0c7856`) keeps many `ClientWrite`s in flight → openraft batches them →
9.7×. This *also* explains why the config levers had failed in isolation:
`api_batch_linger_ms` (iter 1) and `max_payload_entries` (§10) had nothing to
batch when only one write was ever in flight. Once concurrency fills the pipeline,
a small RaftCore batch-linger (5 ms) coalesces writes into larger append/replicate
rounds, amortizing the quorum round-trip + follower fsync — another 2.9× on top.
`linger=10` overshoots (over-batches + adds latency), confirming 5 ms as the knee.

**Caveats.** Absolute numbers are this RTT-bound fleet's; the wins are
architectural (serial-commit removal + consensus-batch coalescing) and should
carry to a faster fleet, likely pushing it well past its §12 ~800/s too. The
linger trades low-load p99 up (~9 → 18 ms) for throughput — acceptable per the
throughput objective; revisit if a latency target is added. Both perf commits are
on `main`. The next ceiling (~5790/s here) is the apply/response pipeline or the
single RaftCore loop — a deeper change than config tuning (§6 lever territory).

Loop record: `uc_autobench/tasks/uc-throughput/results.tsv`.

---

## 14. Follow-up (2026-06-17) — pipelined apply: +35% (same-fleet A/B), cumulative ~38×

The apply pipeline (spec `docs/superpowers/specs/2026-06-17-pipelined-apply-design.md`)
removed the serial per-entry cross-process apply round-trip identified as the
~5790/s ceiling in §13: node `apply()` now publishes a run of committed entries to
the service then awaits the run (FIFO), instead of one publish→await round-trip per
entry. No wire-protocol change; per-entry frontier + `Reattach`-mid-run flush
preserved; lincheck capstone (×3 seeds) + hard-crash + the m1/m2/m3/partition/
ring_torture gates all green.

**Validated by a same-fleet A/B** (UC-only 3-node Hetzner CCX33, durable, 64 B,
inflight 128 — provision once, swap only the `uc_node` lib between runs):

| `uc_node` lib | throughput | knee | p99@knee |
|---|---:|---:|---:|
| pre-pipeline (`8ccb049`: concurrent dispatch + linger=5) | 5808/s | 5000/s | 68.6 ms |
| **+ pipelined apply** (`7ea3dad`) | **7820/s** | 5000/s | 75.9 ms |

**+34.6%** isolated to the apply-pipeline change (same hosts/params). The WITHOUT
number (~5808) matches §13's prior-fleet ~5790, confirming the fleets are comparable.

**Cumulative on this 3-node path: ~205 → ~7820 msgs/s ≈ 38×** (concurrent
`client_write` dispatch 9.7× → `api_batch_linger=5` → pipelined apply +35%), all
lincheck-green. Absolute numbers remain this RTT-bound fleet's; the wins are
architectural. The next ceiling (~7820/s here) is likely the single RaftCore loop or
QUIC replication — a deeper investigation than the apply/consensus batching done so far.

---

## 15. Follow-up (2026-06-17) — RaftCore loop profile: the loop is NOT the ceiling

Profiled the RaftCore commit pipeline under load using openraft 0.10's built-in
`runtime-stats` (enabled via the openraft `runtime-stats` cargo feature; exposed
through `RaftHandle::runtime_stats_display` + a periodic dump in `uc-node-launch` —
on the **`profile/raftcore-stats`** debug branch, NOT merged). Captured node0's
per-stage `log_stages(us)` + batch histograms during a UC-only 3-node sweep
(throughput ~9522/s on this fleet — a faster box than §14's; the **stage
proportions**, not the absolute, are the finding).

**Per-entry log-lifecycle (P50 / P99, µs):**

| stage | P50 | P99 | what it is |
|---|---:|---:|---|
| proposed→received | **6478** | 7153 | client_write waiting in the api channel = the **`api_batch_linger=5ms`** |
| received→submitted | 2 | 6 | engine step (trivial) |
| submitted→persisted | 976 | **171517** | leader journal append + fsync — **severe P99 tail** |
| persisted→committed | 2715 | 22009 | quorum replication round-trip |
| committed→applied | 1145 | 1489 | apply pipeline (our §14 win — fast) |
| proposed→applied | 10776 | 202485 | total |

**Batches** (append/apply/write): P50 12, **P90 ~145, P99 158** — batching is working
(openraft coalesces ~145 entries/round). **RaftCore-loop budget:** `raft_msg_per_run`
P50≈0–1 with `raft_msg_budget`≈960 and usage **≈0‰**, despite a notification flood
(50k ReplicationProgress + 51k HeartbeatProgress vs 11.7k ClientWrites).

**Finding: the RaftCore loop is NOT the next ceiling.** It has ~1000 msg/run of budget
and uses ~0‰ — it absorbs the 100k-notification flood with capacity to spare. The
residual costs are physical, not loop-bound:
- **Dominant latency = `api_batch_linger=5ms`** (proposed→received ~6.5ms), a
  throughput-for-latency trade we chose (§13); reduce it only to trade throughput back.
- **Leader journal fsync P99 tail = 171ms** (submitted→persisted) — the one clearly
  *actionable, in-our-code* lever: tighten the `uc_journal` group-commit / fsync
  tail (the P50 is ~1ms, so it's a tail problem, not steady-state).
- **Quorum replication round-trip ~2.7ms** (persisted→committed) — network /
  openraft-replication territory (alpha.21 has no in-flight pipelining knob).
- **apply ~1.1ms** — confirms §14's apply pipeline removed that stage as a cost.

So the next throughput lever is **not** the RaftCore loop (spare capacity) and **not**
apply (fixed) — it's the **leader fsync tail** (ours) and the **replication round-trip**
(openraft/network). The instrument (`runtime-stats` dump) is reusable from the
`profile/raftcore-stats` branch for any future commit-path profiling.

## 16. Follow-up (2026-06-17) — journal fdatasync (`sync_data`): NULL end-to-end result

Acted on §15's "leader fsync tail" lever: changed the journal's hot per-commit fsync
from `sync_all` (full fsync) to **`sync_data` (fdatasync)** in
`uc_journal/src/journal/writer.rs::fsync_active_segment` (full `sync_all` retained
on segment-create, where the new directory entry must be durable). fdatasync flushes
data + the `i_size` growth and skips only inode timestamps — the standard WAL commit
primitive; `Durability::Consistent`'s power-loss guarantee is preserved. One line, no
dep, no on-disk-format change. Shipped to `ultima_db` main (`3181393`), guarded by a new
`consistent_durability_survives_reopen` test + the full journal suite (99) + the cluster
lincheck capstone (3/3) + the hard-crash `kill -9` gate — all green. (Spec/plan:
`docs/superpowers/specs|plans/2026-06-17-journal-fdatasync*.md`.)

**Local microbench (real ext4, interleaved A/B ×10):** `group_commit_throughput`
before (sync_all) median 81077/s vs after (sync_data) 84064/s — median +3.7% / mean
−1.1%, fully overlapping (CV ~11–16%). **Inconclusive** — a throughput-median over
256-entry bursts amortizes the per-fsync metadata cost and can't isolate an fsync-*tail*
effect; dev-host disks (tmpfs/noisy ext4) don't expose it either.

**Same-fleet cloud A/B (fresh 3× ccx33 Hetzner, interleaved B,A,B,A; UC = `main`,
sole variable = `writer.rs`):**

| metric | A `sync_all` (median) | B `sync_data` (median) | B vs A |
|---|---:|---:|---|
| `uc_throughput_msgs` | 9266.9 | 9206.6 | **−0.7%** |
| `p99_at_knee_ms` (end-to-end, knee=5000) | 64.18 | 67.22 | **+4.7% (worse)** |

Per-run: A {9230.7, 9303.1} / {63.24, 65.11 ms}; B {9065.5, 9347.7} / {67.63, 66.81 ms}.

**Finding: no measurable end-to-end effect.** Both deltas are tiny and within (throughput)
or near (p99) run-to-run noise — the p99 even slightly favors `sync_all`. This is exactly
what §15 predicts: `submitted→persisted` is a P99 **tail** (P50 ~1 ms), while the
*end-to-end* commit latency is gated by `api_batch_linger=5ms` (proposed→received ~6.5 ms)
and the replication round-trip (~2.7 ms) — **not** the fsync stage's steady state. And per
[[unified-bench-harness-done]], fsync is only ~4% of the commit path (submit→node
poll-sleep ~81%). So even a large reduction in the journal-fsync metadata cost is invisible
at the throughput/knee-p99 level.

**Scope caveat (honest):** this measured the *end-to-end* sweep, not the `submitted→persisted`
P50/P99 decomposition the plan's Task 5 named. Capturing that on a fleet needs the
`profile/raftcore-stats` branch **plus** node0-stderr `RAFT_RUNTIME_STATS` log-scraping
during the sweep (the standard `run` role does not collect node logs) — extra plumbing +
a re-provision. Not run, because the conclusion wouldn't change: a tail that doesn't gate
steady-state throughput won't move the user-visible numbers regardless of what fdatasync
does to it internally.

**Decisions:**
- **Keep the change merged.** It is correct, zero-risk, zero-cost, and the right WAL
  primitive — a hygiene/correctness improvement even with no measurable perf win.
- **§5 segment-preallocation follow-up: NOT pursued (YAGNI).** It targets the same
  fsync-tail stage that this A/B shows doesn't gate end-to-end performance; it would add
  format/recovery surface for no user-visible gain.
- **Real next levers** (per §15 + the ~81% poll-sleep finding): the `api_batch_linger`
  latency/throughput trade, the replication round-trip (openraft/network), and the
  submit→node IPC poll-sleep (already partly addressed by event-driven ring wakeups,
  task11). Journal fsync is not where the commit-path budget is spent.

## 17. Follow-up (2026-06-20) — journal segment preallocation: real journal win, NULL end-to-end (again)

Pursued the §16 "leader fsync tail" lever further: **segment preallocation** (etcd
`filePipeline` — preallocate each journal segment so the per-commit `fdatasync` skips the
ext4 jbd2 metadata commit a size-extending append forces). Shipped to `uc_journal`
(task36); cluster reads `UC_JOURNAL_PREALLOC`. Note §13/this doc earlier called §5
preallocation "NOT pursued (YAGNI)" — that was reversed once a microbench showed the jbd2
commit is a real fraction of a *local-NVMe* fsync.

3-platform interleaved cloud A/B (Consistent, 200/s, inflight 8; `submitted→persisted` from
the `runtime-stats` instrument, prealloc OFF→ON):

| platform | storage | `submitted→persisted` P50 | P99 / end-to-end |
|---|---|---|---|
| Hetzner ccx13 | local NVMe | 1531→1095µs (**−28%**), body −~50% | flat / NULL |
| AWS c6id | local NVMe | 261→194µs (**−26%**), floor 155→75µs | flat / NULL |
| AWS c7i | EBS (network) | 2910→2866µs (−1.5%) | flat / NULL |

**Same verdict as §16 (fdatasync).** A real, reproducible journal-stage win on local NVMe
(the jbd2 commit is ~half a fast-NVMe fsync; on EBS the ~2.5ms network-flush floor swamps it)
— but it **does not reach end-to-end**: commit latency is unchanged because `api_batch_linger`
(~5ms) + replication dominate, exactly as §15/§16 found. The journal P99 tail is also flat
(batching/scheduling-bound at this rate). **Enabled by default anyway** (real, free,
correctness-proven, no regression on any storage; `UC_JOURNAL_PREALLOC=0` rollback). The
pattern is now firmly established across two journal optimizations: **leader-fsync wins are
real but masked by linger + replication on the 3-node Consistent path.** The end-to-end levers
remain `api_batch_linger`, the replication round-trip, and IPC poll-sleep — not journal fsync.
