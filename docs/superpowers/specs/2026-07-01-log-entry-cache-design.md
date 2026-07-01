# Log entry cache — design

**Date:** 2026-07-01
**Status:** Proposed — awaiting review
**Motivation:** `docs/benchmarks/leader-profile-inflight-2026-06-30.md` (§5) +
`docs/tasks/task19_synccore_model_b.md` (§6a).

## 1. Why

Fleet profiling identified the throughput ceiling (~15k msg/s, 3-node, on `c6id.2xlarge`) as a
**single-threaded hot path on the leader**: at the knee one thread saturates while the box sits
~90% idle, and its perf call-graph is dominated by **`read()` of the journal's ext4 segment
files (~21% + page-copy + ~15% page-faults) and `crc32` recomputation (~22%)**. The leader is
**re-reading log entries back from disk that it just wrote to memory** — to build AppendEntries
for followers (and to feed apply) — through `JournalLogStorage::try_get_log_entries →
Journal::iter_range`.

Replication reads cluster at the **log tail** (followers lag by milliseconds; the same entries
are re-served to N followers and on retries). Serving those reads from an in-memory cache of
recent entries removes the read + CRC + page-copy + deserialize entirely for the common case.
This is the first change that attacks the *measured* bottleneck; it is expected to raise the
ceiling well past 15k. It is orthogonal to SyncCore (task19) and applies to the default
RaftCore path (which the profiling used).

## 2. Approach (chosen: A)

Add an in-memory cache of recent **deserialized `Entry<C>`** at the `JournalLogStorage` adapter
(`uc_node/src/raft/log_storage.rs`), populated on `append`, consulted by
`try_get_log_entries`, falling through to `Journal::iter_range` on a miss. `ultima_journal` is
untouched; the journal remains the source of truth and the cache is a pure read accelerator
over the tail.

Rejected alternatives: **B** — cache raw serialized bytes (still deserializes on every read,
leaves CPU on the table, needs its own framing). **C** — push the cache into `ultima_journal`
(bigger, riskier change to a shared crate; the measured hot path is at the UC adapter).

Caching *deserialized* entries means a hit skips **all four** costs — `read()`, CRC, page-copy,
and bincode-deserialize. Clones are cheap: `Entry<C>`'s payload is `AppCommand = Bytes`
(refcounted), so serving a cached entry is a refcount bump plus a small `log_id`, not a data
copy.

## 3. Data structure

A field on `JournalLogStorage`, a contiguous-tail structure (the log tail is a dense ascending
`seq` range):

```rust
struct EntryCache<C: RaftTypeConfig> {
    inner: parking_lot::RwLock<CacheInner<C>>,
    budget_bytes: usize,           // from env; 0 = disabled
}
struct CacheInner<C: RaftTypeConfig> {
    base_seq: u64,                 // seq of entries.front(); 0/None when empty
    entries: std::collections::VecDeque<Entry<C>>,  // dense seqs [base_seq, base_seq+len)
    bytes: usize,                  // approx cached size, for the budget
}
```

- **Contiguous `VecDeque` keyed by `base_seq`** (not a `BTreeMap`): dense tail → `seq →
  entries[seq - base_seq]` is O(1), append is `push_back`, evict-oldest is `pop_front`
  (advancing `base_seq`), range read is a slice.
- Holds `Entry<C>` (deserialized). Cloning on serve is cheap (refcounted `Bytes` payload).
- **`parking_lot::RwLock`**: single serialized writer (append/truncate/purge, all issued
  serially by the Raft core, and append already holds the adapter's append lock), few concurrent
  readers (per-follower replication + apply), read-mostly. An uncontended `parking_lot` read is
  a couple of atomics (adaptive spin, no park) — trivial next to the millisecond ext4 read it
  replaces. **The lock is not on the bottleneck; the disk I/O is.** A lock-free seqlock+epoch
  ring was considered and rejected: the payload is a refcounted `Bytes`, so a lock-free reader
  cloning it races the writer's eviction → use-after-free unless guarded by epoch/hazard-pointer
  reclamation — real complexity + a correctness hazard on a linearizability-critical path, for a
  lock cost that does not appear in the profile. Revisit only if a future profile shows lock
  contention (it will not at these read rates).

## 4. Operations & correctness

All three mutations run *inside* the adapter methods that already mutate the journal, holding
the cache write lock, so the cache is updated in lockstep with the journal — a concurrent reader
never sees them diverge.

- **`append(entries)`** (already serialized by the adapter append lock): write-lock; assert
  each `seq == base_seq + len` (contiguous); `push_back`; add its byte estimate; then `pop_front`
  (advancing `base_seq`) until `bytes ≤ budget_bytes`. Completes before `append` returns.
- **`try_get_log_entries(start..end)`**: read-lock; if `start ≥ base_seq && end ≤ base_seq +
  len` → clone that slice → return `Some(Vec<Entry<C>>)`. Otherwise drop the lock and call
  `Journal::iter_range` exactly as today. **All-or-nothing** — a range is served entirely from
  cache or entirely from the journal; no split reads, no seam hazard. (Partial-hit is out —
  YAGNI; near-tail replication reads are fully in-window.)
- **`truncate_after(seq)`** (follower conflict): write-lock; `pop_back` every cached entry with
  index `> seq`; adjust `bytes`. Same op as the journal truncate — the cache never retains a
  truncated entry.
- **`purge_before(seq)`**: write-lock; `pop_front` every entry with index `< seq`; adjust
  `bytes`.

**Invariant:** the cache is always a contiguous suffix of the journal's live log, mutated within
the same adapter operation as the journal. With all-or-nothing reads, a served cached entry is
**byte-identical** to what `Journal::iter_range` would return.

**Defensive edges:** `budget_bytes == 0` → cache stays empty, every read falls through (today's
exact behavior — the rollback). A non-contiguous append (should not happen on a Raft log) →
clear + rebase rather than risk inconsistency. If a supposedly-cached range is missing an entry
(should not happen) → fall through to the journal.

## 5. Config & observability

- **`UC_LOG_CACHE_BYTES`** env, per-node. `0` = disabled (rollback). Default **256 MB**. Follows
  the repo's env-tunable-optimization convention (`UC_JOURNAL_PREALLOC`,
  `UC_API_BATCH_LINGER_MS`). Ships **default-on** (the cache is correctness-neutral by design);
  the default is confirmed/flipped by the fleet validation (same ship-then-confirm pattern as
  journal prealloc / linger).
- **Byte accounting (approximate):** per entry ≈ `payload.len()` + a fixed per-entry overhead
  constant. It only has to *bound* the footprint, not be exact.
- **Observability:** an `AtomicU64` hit/miss pair, surfaced through the existing node metrics + a
  periodic debug log — confirms the cache is hitting in production and lets the fleet re-measure
  hit-rate alongside the CPU drop.

## 6. Testing & validation

**Unit tests:** append→get *hit* (byte-identical); get *miss* below `base_seq` / beyond tail /
partial-overlap → `None`; `truncate_after(k)` drops `>k`; `purge_before(k)` drops `<k`; budget
eviction (oldest evicted, `bytes ≤ budget`, `base_seq` advances); non-contiguous append →
clear+rebase; `budget=0` → empty, all reads fall through.

**Differential correctness test (key guard):** a randomized append/truncate/purge sequence, then
for random ranges assert **cache-served entries are byte-identical to `Journal::iter_range`** for
that range (or the cache misses → journal used). Pins the core invariant.

**Correctness-neutral, proven by the existing oracles:**
- Full workspace `cargo test` green with the cache on.
- **UC lincheck `lin_register` + partition `lin_partition` green with the cache ON** (256 MB) —
  the ultimate guard (the cache feeds replication/apply; a wrong entry would surface as a
  linearizability violation). Also run once with `UC_LOG_CACHE_BYTES=0` to confirm the fallback
  path is unchanged.

**The win (success criterion — measured):** re-run the leader profile + inflight sweep on the
fleet with the cache on (the harness from `leader-profile-inflight-2026-06-30.md`):
- the bottleneck thread's `read()`/`ext4`/`crc32` CPU collapses (reads hit RAM);
- the throughput knee rises past ~15k;
- the hit/miss counter shows a high tail hit-rate.

## 7. Scope / non-goals

- **In scope:** the adapter-level recent-entry cache (populate/serve/evict/truncate/purge),
  config + metrics, tests, and the fleet before/after.
- **Out of scope (YAGNI):** partial-range hits; caching in `ultima_journal`; a lock-free ring;
  the separate CRC-skip / zero-copy fixes (the cache subsumes them for the hot path); the
  `inflight ≥ 512` instability (a separate issue noted in the profiling doc).

## 8. Files

- Modify `uc_node/src/raft/log_storage.rs` — the `EntryCache` type + wiring into
  `append`/`truncate_after`/`purge_before`/`try_get_log_entries` on `JournalLogStorage`.
  (Split the cache into its own `uc_node/src/raft/entry_cache.rs` module if the adapter file
  grows unwieldy.)
- New env read (`UC_LOG_CACHE_BYTES`) alongside the existing journal env toggles.
- Metrics wiring for the hit/miss counters.
