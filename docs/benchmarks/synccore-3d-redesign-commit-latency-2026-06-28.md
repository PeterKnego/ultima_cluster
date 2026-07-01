# SyncCore 3d redesign (pass 1) re-measurement — commit_latency A/B (2026-06-28)

Follow-up to `synccore-3d-commit-latency-2026-06-28.md` (the minimal-3d spike measurement).
Measures the **completion-as-notification redesign pass 1** (openraft fork `sync-core`,
commit `3fdea52e`) vs async `RaftCore`, same harness (`benchmarks/minimal` `commit_latency`,
in-memory store+network, A/B via `--features sync-core`). 4-core box, n=50k (latency) /
100k (throughput), 5k warmup.

What the redesign changed (all suite-green, 180 integration + 496 lib, opus-reviewed):
`AppendEntries` and `save_committed` are now **fire-and-forget** (no per-write `block_on`
on the consensus loop); readability is preserved by a consumer watermark + `GatedLogReader`
on the replication path AND the sm-worker apply path (the wait moved OFF the consensus loop
into the reader's own task).

## Result: the redesign did NOT meet the success criterion

**Success criterion** (from the spike doc): redesigned-3d ≥ RaftCore at inflight=1 AND
under concurrency. **Neither holds.** The redesign is ~identical to the pre-redesign spike.

| single-node, inflight=1, p50 | value |
|---|---|
| RaftCore (async) | ~27 µs |
| minimal-3d spike (pre-redesign) | ~35 µs |
| **3d redesign (this pass)** | **~35 µs** (+30% vs RaftCore) |

(3 reps each, `--server-workers 2`: RaftCore 27.1/26.9/26.4; redesign 37.0/34.6/35.7 µs.)

| single-node throughput (op/s) | RaftCore | 3d redesign |
|---|---|---|
| conc=16 | ~392k | ~286k |
| conc=64 | ~1.05M | ~430k (≈2.4× worse) |
| conc=256 | ~1.85M | ~496k |

(conc=64 redesign across 4 reps: 0.40/0.44/0.46M — plus one 0.24M outlier — vs the spike's
~0.33–0.54M. No improvement over the spike; still well below RaftCore.)

## Why removing the loop waits didn't help

The redesign correctly removed the per-write busy-waits from the consensus loop, yet the
numbers didn't move. Two reasons, both now understood:

1. **inflight=1: the cross-thread round-trip is on the critical path regardless of where
   the wait lives.** With one outstanding op, the client cannot proceed until the entry is
   appended, committed, applied, and answered. The append→readable→apply hop to the
   off-thread durability consumer + sm worker is unavoidably serial on that single op.
   Moving the wait from the consensus loop into the sm-worker task (this redesign) does not
   shorten it when there is no concurrent work to overlap. (Predicted exactly by the Task-3
   review.) So inflight=1 stays at the spike's ~35 µs.

2. **Concurrency: the bottleneck is the off-thread coordination on fast I/O, not the loop
   waits.** For an *in-memory* store, an "append" is a few-ns map insert. Handing it to a
   separate consumer thread over a disruptor ring, advancing a watch watermark, and gating
   the apply read on that watch costs **far more** than the append itself. RaftCore does the
   append in-process and lets the async scheduler overlap cheaply; the cross-thread pipeline
   has nothing expensive to overlap, so its coordination overhead dominates — compounded by
   4-core oversubscription (consensus loop + durability consumer + sm worker + tokio workers
   contend; the busy-spin/yield consumers can't get a dedicated core).

## The real conclusion (strategic)

This is the same lesson the spike measurement pointed at, now confirmed against the
*correct* (busy-wait-free) implementation: **the off-thread-durability architecture (3b.2)
+ sync loop cannot show a win on an in-memory store** — the cross-thread coordination costs
more than it saves when the "I/O" is nanoseconds and there's nothing to overlap. The
in-memory commit→apply microbench is structurally unable to demonstrate this design's value,
and on a 4-core box it actively disfavors it.

Pass 1 is still worth keeping: it is the architecturally correct foundation (the consensus
loop no longer busy-waits on per-write I/O; suite-green; cleaner), and it is a *precondition*
for any real win — it just can't be **measured** here.

### What this redirects the next step toward (not done this pass)

- **Real-I/O measurement is now required, not optional.** The value of off-thread durability
  appears only when the I/O is expensive enough to overlap (real `fdatasync` + QUIC). That
  means wiring UC to build openraft with `--features sync-core` and running UC's bench/fleet
  against the floor decomposition. Prerequisite/risk (unchanged): UC's journal/quinn adapters
  must complete reactor-free under the durability consumer's `block_on` — unverified.
- **Core pinning / fewer threads.** The 4-core box oversubscribes the busy-spin consensus
  thread against the durability/apply/tokio threads. A real deployment is 1 node/host with
  cores to spare; the microbench's penalty is partly an artifact of the box. Pinning (and the
  eventual collapse of tokio out of the hot path via 3c/3e) is needed before a fair
  in-process number.
- **Do NOT chase further in-memory microbench tuning** — it measures coordination overhead
  on free I/O, which is the wrong target. The `commit_latency` harness stays useful only as a
  *regression* guard (don't get slower), not as the success oracle.

## Status

3d redesign pass 1: **complete and correct, perf-neutral on this harness.** Success criterion
deferred to a real-I/O setting. Suite 180/496 green; default path 494 unchanged; clippy clean.
