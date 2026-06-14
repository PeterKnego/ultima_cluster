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
half (`ultima_journal::commit_profile`) was never committed and is unrecoverable,
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

`ultima_journal` landed the §6 lever #1 work (autoresearch run, merged as
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
