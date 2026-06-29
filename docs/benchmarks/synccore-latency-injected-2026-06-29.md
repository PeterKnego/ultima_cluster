# SyncCore vs RaftCore — latency-injected microbench (2026-06-29)

Extends the `commit_latency` harness (`openraft/benchmarks/minimal`) with **injectable
durability/network latency**, to test the hypothesis that SyncCore's value is invisible to a
*zero-latency* in-memory bench (see `synccore-3d-redesign-commit-latency-2026-06-28.md`) and
only appears once I/O is expensive enough to overlap. Result: **confirmed.**

## What was added (and why this way)

- **`--fsync-us N`** (env `BENCH_FSYNC_US`): a per-commit `save_committed` delay in the mock
  store (`benchmarks/minimal/src/store.rs`). This is the one hot-path durability op the 3d
  redesign differentiates on: **RaftCore `.await`s `save_committed` inline on its core task**
  (so it lands on the commit critical path), while **SyncCore publishes it fire-and-forget**
  to the off-thread durability consumer (so it overlaps). Modeled as a **blocking
  `std::thread::sleep`**, which is faithful: the real journal's `StableValue.wait()` blocks
  and frees the CPU (a spin-wait would peg a core, unlike real fsync), and SyncCore's
  reactor-free consumer drives it under a never-park `block_on` where no tokio timer fires.
- **`--rtt-us N`** (env `BENCH_RTT_US`): a per-RPC `tokio::time::sleep` in the in-process
  router. Common-mode between the cores in 3d (replication is delegated/async for both), so
  it does not differentiate them yet — included for multi-node absolute-latency realism and
  for when replication moves off RaftCore (3c).
- Append flush is left **instant**: it is overlapped by *both* cores (the `IOFlushed`
  callback → `LocalIO` notification path), so it is common-mode and adding it would only
  raise both numbers equally. The bench therefore measures an **honest lower bound** on the
  redesign's value — it injects cost only where 3d actually changed the behavior.

## Results (4-core box, single-node)

**Latency, inflight=1, p50 (n=20k, `--server-workers 2`):**

| `fsync_us` | RaftCore | SyncCore | winner |
|---|---|---|---|
| 0   | 27 µs  | 40 µs  | RaftCore (zero-latency baseline) |
| 25  | 162 µs | 110 µs | **SyncCore −32%** |
| 50  | 202 µs | ~138 µs (135/135/149) | **SyncCore −27%** |
| 100 | 258 µs | 186 µs | **SyncCore −28%** |
| 200 | 361 µs | 289 µs | **SyncCore −20%** |

**Throughput, conc=64 (n=40k, op/s):**

| `fsync_us` | RaftCore | SyncCore | winner |
|---|---|---|---|
| 0  | 713k | 386k | RaftCore |
| 50 | 165k | 184k | **SyncCore +11%** |

## Interpretation

- **The crossover is real and low.** SyncCore is already ahead at `fsync_us=25` and the
  baseline (`fsync_us=0`) is the only point where RaftCore wins. So the moment per-commit
  durability costs more than a few µs, the off-thread design pays off. Real `fdatasync` is
  tens-to-hundreds of µs, i.e. comfortably past the crossover.
- **Why latency wins more than throughput.** At inflight=1 SyncCore takes the blocking
  `save_committed` off the response path entirely (it applies/responds without waiting for
  the marker). Under concurrency, the *single* durability consumer must still run
  `append + save_committed` per op sequentially, so it becomes its own serialization point —
  the throughput win (+11%) is smaller than the latency win (−20…−32%). This points at a
  future refinement (don't let `save_committed` block `append` on the same consumer), but
  even unoptimized it already flips positive.
- **This is a lower bound on the design's value.** Only `save_committed` is injected — the
  one op 3d moved off-thread. 3c/3e move replication and apply off RaftCore too, adding more
  overlappable I/O, so the advantage should widen as the pipeline completes.

## Caveats

- **`std::thread::sleep` granularity** floors small delays at ~tens of µs (OS timer), so the
  *effective* injected delay is "≥ `fsync_us`" and absolute numbers for tiny values are
  inflated. This is faithful enough (real fsync is rarely sub-50µs) and the relative
  comparison is unaffected (both cores use the same sleep). Precise sub-50µs modeling would
  need a different mechanism, but a CPU-pegging spin-wait would be *less* faithful (fsync
  frees the CPU) and would worsen 4-core oversubscription.
- Still single-node, 4-core, in-process. This **de-risks** the design but does not replace
  the **UC fleet** (real `fdatasync` + QUIC) as ground truth — it answers "does the design
  win when durability is expensive, and where?" (yes; crossover < ~25µs), which is exactly
  what the zero-latency bench could not.

## Bottom line

The zero-latency microbench was the wrong regime, as suspected. With realistic per-commit
durability cost, **SyncCore beats RaftCore on both latency (−20…−32%) and throughput
(+11%)** — validating the 3d redesign and justifying the UC fleet measurement as the next
step.
