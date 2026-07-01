# Log entry cache — fleet A/B is NULL; the bottleneck was misattributed — 2026-07-01

The post-merge measurement of the log entry cache (`task19` §6a → the cache feature). **Result:
the cache delivers zero throughput win, because the profiled `read()`+`crc32` bottleneck is NOT
openraft's replication reads (which the cache serves) — it is the UC *shmem apply/output/replay*
path reading the journal *directly*, which the cache never sees.** The cache is correct
(lincheck/partition green) but aimed at the wrong read path.

## The A/B (same 2xlarge fleet, cache-enabled build, linger=2, 3-node consistent, inflight=256)

Direct profile of the leader at the knee, `UC_LOG_CACHE_BYTES=0` (OFF) vs `268435456` (ON):

| | busiest thread | crc32 | read() | achieved @15k | p99@knee |
|---|---|---|---|---|---|
| **cache OFF** | 87% | 23.2% | 19.8% | 15,000 | 268 ms |
| **cache ON** | 86% | 21.0% | 20.3% | 15,000 | 426 ms |

`read()` and `crc32` are **unchanged** with the cache on; the knee is **15k both ways** (the
inflight-256 sweep: OFF and ON both saturate ~15k, ON slightly worse tail). **Null — and the
cache hit counters could not even be read** (the `log_entry_cache` `debug!` telemetry is below
the node's INFO log level on the fleet, so hit-rate was invisible — an observability miss).

## Root cause — the cache is at the wrong layer for shmem mode

The bench runs **shmem mode** (`uc-node-launch --with-service`). Two things the profile exposed:

1. **599,000 `WARN … output_chan full; replay will catch this`** in ~40 s (~1 per commit). The
   M5 apply→output_dispatcher channel (buffer 1024) is *constantly* overflowing — the service
   (apply) cannot keep up at 15k, so the node continuously **replays committed entries from the
   journal into the service** to catch it up.
2. Those replay/apply reads go through **`journal.iter_range` directly**, NOT
   `JournalLogStorage::try_get_log_entries` where the cache lives:
   - `uc_node/src/raft/state_machine_shmem.rs:1153` — `journal.iter_range((from+1)..(to+1))` (the
     shmem SM replaying committed entries into the service)
   - `uc_node/src/runtime/output_replay.rs:132` — `journal.iter_range(range)` (output replay)
   - `state_machine_shmem.rs:1392` / `:1330` — more direct journal reads.

So the saturated thread's `read()`+`crc32` is the **shmem apply/replay path re-reading the
journal**, driven by the service falling behind. The cache only intercepts openraft's
`try_get_log_entries` (replication + the openraft-side apply read), which is **not** the hot path
in shmem mode. The cache cannot touch these reads.

## Correction to task19 / the throughput attribution

`task19` §6a (and `leader-profile-inflight-2026-06-30.md`) attributed the ~15k ceiling to "the
leader re-reading the log **for replication**." That profiling was *also* on the shmem bench, so
the attribution was **wrong**: the hot journal reads are the **shmem apply/output pipeline**
re-reading committed entries because the service can't keep up (the `output_chan`-full replay
loop), not openraft replication. The cache was built on that misattribution.

## What the real bottleneck is, and what to do

The shmem-mode ~15k ceiling is the **apply/output pipeline**: at 15k the service falls behind →
the node's apply/replay thread saturates re-reading the journal (`iter_range` + CRC) to catch it
up. The levers, in order of likely value:

1. **Why does the service fall behind at 15k?** — profile/instrument the *service* (apply) side
   and the `output_chan` (1024): is apply CPU-bound, is the `apply.ring` / `output.ring` the
   limit, is the 1024 buffer too small, is the output_dispatcher the serial point? This is the
   real throughput investigation now (the node-side journal replay is a *symptom*).
2. **Route the shmem apply/replay `iter_range` reads through the `EntryCache`** — a targeted
   extension that WOULD cut the replay thread's read+CRC CPU (the cache already exists + is
   correct). Treats the symptom, but may raise the replay ceiling if the service isn't the hard
   limit. Cheaper than #1 to try.
3. Consider whether embedded mode (no shmem service, no output replay) has the same ceiling —
   if the cache helps there (openraft apply reads go through `try_get_log_entries`), it validates
   the cache for that deployment even though shmem doesn't benefit.

## Disposition of the cache

The cache is **correct, reviewed, lincheck/partition-green, and merge-ready** — it just does not
help the shmem bench because that workload's hot reads bypass it. It is not wasted: it accelerates
openraft's `try_get_log_entries` (replication + openraft apply), which matters in embedded mode
and for any future path that reads through the storage trait. But **the throughput win it was
built to deliver is not realized in shmem mode**, and the honest next step is re-diagnosing the
apply/output pipeline. Also fix the observability: the hit/miss log must be at INFO (or a metric)
to be visible on the fleet.

## Status

Fleet destroyed (0 resources, no leak — the driver teardown completed after a transient
state-lock collision). 8 session fleets, all clean. Bench-infra now threads `UC_LOG_CACHE_BYTES`
(default 256MB, `-e uc_log_cache_bytes=0` to disable) for future A/Bs.

## Addendum — the NoopOutput bench fix is ALSO null (2026-07-01)

Wired `NoopOutput` in the bench (`uc-node-launch.rs`, commit 6c6dec2) so the service drains
`service/output.ring`, and re-measured (verified deployed: node0 source + binary rebuilt this
run). **Complete null — every axis identical to pre-fix:**

| | crc32 | read() | busiest thread | `output_chan full` warns | knee (i128 / i256) |
|---|---|---|---|---|---|
| pre-fix | 22% | 20% | 87% | 599k | 10k / 15k |
| **+ NoopOutput** | 21% | 20% | 86% | **581k** | 10k / 15k |

So the explorer's "missing output.ring consumer" model was wrong: draining `output.ring` did
**not** clear the `output_chan` backup. The `output_chan` (1024) stays full because the
`output_dispatcher`'s per-entry synchronous round-trip (publish to `output.ring` + await
`output_resp`) cannot drain 1024 slots at 15k/s regardless of the consumer — and even that does
not fully explain the persistent steady-state read/crc32 (output_replay is transition-only).
**The read/crc32 source remains unpinned, and the ~15k ceiling is unmoved by every lever.**

## Meta — stop the fleet-based throughput chase

This is the 4th consecutive null in the throughput chase (SyncCore, the log cache, and now the
output-handler fix). Across ~9 session fleets the ~15k knee (inflight 256) / ~10k (inflight 128)
has moved for **exactly one** lever — the `linger 5→2` config (the shipped 2× win). Every
structural/code hypothesis has failed, and the bottleneck resists diagnosis (read/crc32 +
output_chan-full persist through the obvious fixes). Recommendation: **stop the fleet A/B chase.**
Bank the linger win + the (correct, embedded-useful) cache + the diagnostic knowledge. If
throughput is genuinely critical later, switch to a LOCAL deep-instrumented single-node
flamegraph of the apply/output path (cheaper + more diagnostic than more cloud A/Bs) rather than
another fleet.
