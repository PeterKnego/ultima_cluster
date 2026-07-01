# Log Entry Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve the leader's replication/apply log reads from an in-memory cache of recent entries instead of re-reading them off the journal's ext4 segments — attacking the measured ~15k single-thread throughput bottleneck (`read()` + CRC + copy + deserialize).

**Architecture:** A byte-bounded contiguous-tail cache of deserialized `Entry<C>` at the `JournalLogStorage` adapter (`uc_node/src/raft/log_storage.rs`), populated on `append`, consulted by `try_get_log_entries` (all-or-nothing; else fall through to `Journal::iter_range`), evicted on budget/`truncate_after`/`purge`. The journal stays the source of truth; the cache is a pure read accelerator shared across the storage and every log reader it hands out.

**Tech Stack:** Rust, `openraft` (`RaftLogStorage`/`RaftLogReader` over `ultima_journal`), `parking_lot::RwLock`, `bincode`.

## Global Constraints

- **Correctness-neutral, byte-identical.** A cache-served entry MUST equal what `Journal::iter_range` would return for that range. The cache feeds replication/apply; a wrong entry is a linearizability violation. Guarded by a differential test + the UC lincheck/partition suites.
- **`UC_LOG_CACHE_BYTES`** env, per-node, default **`268435456` (256 MB)**, **`0` = disabled** (cache stays empty, every read falls through — today's exact behavior + the rollback). Follows the repo env-toggle convention (`UC_JOURNAL_PREALLOC`, `UC_API_BATCH_LINGER_MS`).
- **Shared cache:** the cache is `Arc<EntryCache>`; `get_log_reader` clones the `Arc` so every reader shares the one cache that `append` populates.
- **All-or-nothing reads:** a range is served entirely from cache or entirely from the journal — never split.
- Work on branch `spike/openraft-hotpath-runtime`. BASE: `99703c3`. Build/test: `cargo test -p uc_node` (+ `cargo build -p uc_node`, `cargo clippy -p uc_node -- -D warnings`). Spec: `docs/superpowers/specs/2026-07-01-log-entry-cache-design.md`.

## File structure

- **Create** `uc_node/src/raft/entry_cache.rs` — the `EntryCache` type (data structure + ops + hit/miss counters). One responsibility, unit-tested standalone.
- **Modify** `uc_node/src/raft/mod.rs` — declare `mod entry_cache;`.
- **Modify** `uc_node/src/raft/log_storage.rs` — add the `Arc<EntryCache>` field + env init, share it in `get_log_reader`, populate in `append`, consult in `try_get_log_entries`, evict in `truncate_after`/`purge`; the env parse fn; a periodic hit/miss log.

---

## Task 1: The `EntryCache` data structure (standalone, unit-tested)

**Files:**
- Create: `uc_node/src/raft/entry_cache.rs`
- Modify: `uc_node/src/raft/mod.rs` (add `mod entry_cache;`)
- Test: `uc_node/src/raft/entry_cache.rs` `#[cfg(test)] mod tests`

**Interfaces:**
- Produces:
  - `pub(crate) struct EntryCache` with `pub(crate) fn new(budget_bytes: usize) -> Self`.
  - `pub(crate) fn append_entry(&self, seq: u64, entry: Entry, payload_len: usize)`
  - `pub(crate) fn get_range(&self, start: u64, end: u64) -> Option<Vec<Entry>>` (end exclusive)
  - `pub(crate) fn truncate_after(&self, keep_seq: u64)` (drop entries with `seq > keep_seq`)
  - `pub(crate) fn purge_upto(&self, last_removed_seq: u64)` (drop entries with `seq <= last_removed_seq`)
  - `pub(crate) fn hits(&self) -> u64` / `pub(crate) fn misses(&self) -> u64`
  - where `type Entry = <crate::raft::TypeConfig as openraft::RaftTypeConfig>::Entry;`

- [ ] **Step 1: Write the failing unit tests**

Add to `entry_cache.rs`. Build test entries with the crate's real `TypeConfig::Entry`. Use a helper `fn ent(index: u64, term: u64) -> Entry` that constructs a blank/normal entry at that log id (mirror how existing `uc_node` tests build entries — check `uc_node/tests` or `log_storage.rs` tests for the constructor; a `Blank` entry needs only a `LogId`).

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // ent(index, term): a minimal Entry at (term, index). Adapt to the real
    // Entry constructor used elsewhere in uc_node (openraft::Entry::<TypeConfig>
    // with payload EntryPayload::Blank is enough for identity tests).
    fn ent(index: u64, term: u64) -> Entry { /* see Step 3 note */ crate::raft::test_blank_entry(term, index) }

    fn idx(e: &Entry) -> u64 { e.log_id.index }

    #[test]
    fn hit_returns_contiguous_range() {
        let c = EntryCache::new(1 << 20);
        for i in 1..=5 { c.append_entry(i, ent(i, 1), 10); }
        let got = c.get_range(2, 5).expect("cached"); // [2,5) = idx 2,3,4
        assert_eq!(got.iter().map(idx).collect::<Vec<_>>(), vec![2, 3, 4]);
        assert_eq!(c.hits(), 1);
    }

    #[test]
    fn miss_below_base_or_above_tail() {
        let c = EntryCache::new(1 << 20);
        for i in 3..=5 { c.append_entry(i, ent(i, 1), 10); } // base_seq=3
        assert!(c.get_range(1, 3).is_none());   // below base
        assert!(c.get_range(4, 7).is_none());   // beyond tail
        assert_eq!(c.misses(), 2);
    }

    #[test]
    fn empty_range_is_trivial_hit() {
        let c = EntryCache::new(1 << 20);
        c.append_entry(1, ent(1, 1), 10);
        assert_eq!(c.get_range(2, 2).map(|v| v.len()), Some(0));
    }

    #[test]
    fn budget_evicts_oldest() {
        let c = EntryCache::new(100); // budget 100 bytes
        for i in 1..=20 { c.append_entry(i, ent(i, 1), 10); } // ~10*(10+overhead) >> 100
        // oldest evicted: a low-index read misses, the tail hits.
        assert!(c.get_range(1, 2).is_none());
        assert!(c.get_range(19, 21).is_some());
    }

    #[test]
    fn truncate_drops_tail() {
        let c = EntryCache::new(1 << 20);
        for i in 1..=5 { c.append_entry(i, ent(i, 1), 10); }
        c.truncate_after(3); // keep <=3
        assert!(c.get_range(4, 6).is_none()); // 4,5 gone
        assert_eq!(c.get_range(1, 4).unwrap().iter().map(idx).collect::<Vec<_>>(), vec![1,2,3]);
    }

    #[test]
    fn purge_drops_front() {
        let c = EntryCache::new(1 << 20);
        for i in 1..=5 { c.append_entry(i, ent(i, 1), 10); }
        c.purge_upto(2); // remove seq <= 2
        assert!(c.get_range(2, 3).is_none());
        assert_eq!(c.get_range(3, 6).unwrap().iter().map(idx).collect::<Vec<_>>(), vec![3,4,5]);
    }

    #[test]
    fn noncontiguous_append_rebases() {
        let c = EntryCache::new(1 << 20);
        c.append_entry(1, ent(1, 1), 10);
        c.append_entry(9, ent(9, 1), 10); // gap -> clear + rebase to 9
        assert!(c.get_range(1, 2).is_none());
        assert!(c.get_range(9, 10).is_some());
    }

    #[test]
    fn disabled_never_caches() {
        let c = EntryCache::new(0);
        c.append_entry(1, ent(1, 1), 10);
        assert!(c.get_range(1, 2).is_none());
    }
}
```

- [ ] **Step 2: Run — verify it fails**

Run: `cargo test -p uc_node entry_cache 2>&1 | tail -15`
Expected: FAIL — module/`EntryCache` not found. (Add `#[cfg(feature=...)]`? No — unconditional.)

- [ ] **Step 3: Implement `entry_cache.rs`**

```rust
//! In-memory cache of recent log entries (the log tail), serving replication/apply
//! reads from RAM instead of re-reading the journal's ext4 segments. See
//! docs/superpowers/specs/2026-07-01-log-entry-cache-design.md.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

/// The concrete log entry type of this cluster's Raft config.
pub(crate) type Entry = <crate::raft::TypeConfig as openraft::RaftTypeConfig>::Entry;

/// Fixed per-entry overhead added to the payload length for budget accounting
/// (log_id + enum tag + VecDeque slot). Approximate — the budget only needs to be bounded.
const ENTRY_OVERHEAD_BYTES: usize = 64;

pub(crate) struct EntryCache {
    inner: RwLock<CacheInner>,
    budget_bytes: usize,
    hits: AtomicU64,
    misses: AtomicU64,
}

struct CacheInner {
    /// seq of `entries.front()`; meaningless when `entries` is empty.
    base_seq: u64,
    /// Dense ascending seqs [base_seq, base_seq + len). `.1` = approx bytes of that entry.
    entries: VecDeque<(Entry, usize)>,
    bytes: usize,
}

impl EntryCache {
    pub(crate) fn new(budget_bytes: usize) -> Self {
        EntryCache {
            inner: RwLock::new(CacheInner { base_seq: 0, entries: VecDeque::new(), bytes: 0 }),
            budget_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    #[inline]
    fn enabled(&self) -> bool { self.budget_bytes > 0 }

    /// Insert a just-appended entry at `seq`. Caller guarantees appends arrive in
    /// ascending contiguous seq order (the Raft log); a gap clears + rebases defensively.
    pub(crate) fn append_entry(&self, seq: u64, entry: Entry, payload_len: usize) {
        if !self.enabled() { return; }
        let sz = payload_len + ENTRY_OVERHEAD_BYTES;
        let mut g = self.inner.write();
        if g.entries.is_empty() {
            g.base_seq = seq;
        } else if seq != g.base_seq + g.entries.len() as u64 {
            // non-contiguous (unexpected on a Raft log) — clear + rebase.
            g.entries.clear();
            g.bytes = 0;
            g.base_seq = seq;
        }
        g.entries.push_back((entry, sz));
        g.bytes += sz;
        while g.bytes > self.budget_bytes {
            match g.entries.pop_front() {
                Some((_, s)) => { g.bytes -= s; g.base_seq += 1; }
                None => break,
            }
        }
    }

    /// Serve `[start, end)` if fully cached, else `None` (caller falls through to journal).
    pub(crate) fn get_range(&self, start: u64, end: u64) -> Option<Vec<Entry>> {
        if start >= end {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Some(Vec::new());
        }
        if !self.enabled() {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let g = self.inner.read();
        let len = g.entries.len() as u64;
        if g.entries.is_empty() || start < g.base_seq || end > g.base_seq + len {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let lo = (start - g.base_seq) as usize;
        let hi = (end - g.base_seq) as usize;
        let out: Vec<Entry> = g.entries.range(lo..hi).map(|(e, _)| e.clone()).collect();
        self.hits.fetch_add(1, Ordering::Relaxed);
        Some(out)
    }

    /// Drop entries with `seq > keep_seq` (follower conflict truncation).
    pub(crate) fn truncate_after(&self, keep_seq: u64) {
        if !self.enabled() { return; }
        let mut g = self.inner.write();
        while let Some((_, _)) = g.entries.back() {
            let last = g.base_seq + g.entries.len() as u64 - 1;
            if last > keep_seq {
                if let Some((_, s)) = g.entries.pop_back() { g.bytes -= s; }
            } else { break; }
        }
    }

    /// Drop entries with `seq <= last_removed_seq` (purge).
    pub(crate) fn purge_upto(&self, last_removed_seq: u64) {
        if !self.enabled() { return; }
        let mut g = self.inner.write();
        while !g.entries.is_empty() && g.base_seq <= last_removed_seq {
            if let Some((_, s)) = g.entries.pop_front() { g.bytes -= s; g.base_seq += 1; }
        }
    }

    pub(crate) fn hits(&self) -> u64 { self.hits.load(Ordering::Relaxed) }
    pub(crate) fn misses(&self) -> u64 { self.misses.load(Ordering::Relaxed) }
}
```

Add `mod entry_cache;` to `uc_node/src/raft/mod.rs`. For the test helper `ent`, add a small `pub(crate) fn test_blank_entry(term: u64, index: u64) -> Entry` next to the cache (or in `raft/mod.rs`) that builds `openraft::Entry { log_id: LogId::new(<committed leader id for term>, index), payload: EntryPayload::Blank }` — copy the exact `LogId`/leader-id constructor from an existing `uc_node` test (grep `EntryPayload::Blank` / `LogId::new` in `uc_node`). If the real constructor differs, match it; the identity tests only need `log_id.index` to round-trip.

- [ ] **Step 4: Run — verify pass**

Run: `cargo test -p uc_node entry_cache 2>&1 | tail -15`
Expected: all cache tests PASS. Then `cargo clippy -p uc_node -- -D warnings 2>&1 | tail -3` clean.

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/raft/entry_cache.rs uc_node/src/raft/mod.rs
git commit -m "feat(uc_node): EntryCache — byte-bounded recent-entry tail cache (data structure)"
```

---

## Task 2: Integrate the cache into `JournalLogStorage`

**Files:**
- Modify: `uc_node/src/raft/log_storage.rs` (struct field + env init + `get_log_reader` + `append` + `try_get_log_entries` + `truncate_after` + `purge`)
- Test: `uc_node/src/raft/log_storage.rs` `#[cfg(test)] mod tests` (differential test)

**Interfaces:**
- Consumes: `crate::raft::entry_cache::EntryCache` (`new`/`append_entry`/`get_range`/`truncate_after`/`purge_upto`).
- Produces: `JournalLogStorage.cache: Arc<EntryCache>` shared across readers.

- [ ] **Step 1: Write the failing differential test**

Add to the `log_storage.rs` test module. Build a `JournalLogStorage` over a `tempfile::TempDir` (mirror the setup in existing `uc_node` storage/integration tests — grep `JournalLogStorage::` in `uc_node/tests` for the constructor + how they append/read). The test appends N entries, then asserts `try_get_log_entries` returns the same entries **whether the cache is enabled or disabled**, across several ranges, and after a `purge`:

```rust
#[tokio::test]
async fn cache_reads_match_journal() {
    // helper: build storage with a given UC_LOG_CACHE_BYTES; append entries 1..=N;
    // read ranges via try_get_log_entries; assert indexes + payloads match the appended.
    for budget in [0usize, 64 * 1024 * 1024] {          // disabled vs enabled
        let (mut store, _dir) = new_test_storage(budget).await;
        let appended = append_n(&mut store, 1, 20).await; // Vec<Entry>, indexes 1..=20
        for (lo, hi) in [(1u64, 21u64), (5, 15), (18, 21), (1, 2)] {
            let got = store.try_get_log_entries(lo..hi).await.unwrap();
            assert_eq!(entry_indexes(&got), (lo..hi).collect::<Vec<_>>(),
                       "budget={budget} range={lo}..{hi}");
        }
        store.purge(log_id_at(5)).await.unwrap();          // remove <=5
        let got = store.try_get_log_entries(6..21).await.unwrap();
        assert_eq!(entry_indexes(&got), (6u64..21).collect::<Vec<_>>());
    }
}
```

Provide the small helpers (`new_test_storage`, `append_n`, `entry_indexes`, `log_id_at`) in the test module, adapted to the real constructors used by existing `uc_node` tests.

- [ ] **Step 2: Run — verify it fails**

Run: `cargo test -p uc_node cache_reads_match_journal 2>&1 | tail -20`
Expected: FAIL (no `cache` field / `new_test_storage` can't set the budget yet).

- [ ] **Step 3: Add the field + env init + reader sharing**

In `log_storage.rs`:
- Add `use std::sync::Arc;` (if not present) and `use crate::raft::entry_cache::EntryCache;`.
- Add the field to `struct JournalLogStorage`: `pub(crate) cache: Arc<EntryCache>,`.
- Add the env parser near the other env fns:
```rust
/// `UC_LOG_CACHE_BYTES` — recent-entry cache budget in bytes; 0 disables. Default 256 MiB.
const LOG_CACHE_BYTES_DEFAULT: usize = 256 * 1024 * 1024;
fn log_cache_bytes_from_env() -> usize {
    std::env::var("UC_LOG_CACHE_BYTES").ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(LOG_CACHE_BYTES_DEFAULT)
}
```
- In the constructor (where the struct literal is built, ~line 158), add:
  `cache: Arc::new(EntryCache::new(log_cache_bytes_from_env())),`
  (For tests, expose a way to set the budget — simplest: have `new_test_storage` set the env var before construction, or add a `pub(crate) fn _with_cache_budget(mut self, bytes: usize) -> Self` that replaces `self.cache`. Prefer the env approach in the test helper to exercise the real path.)
- In `get_log_reader`, add `cache: self.cache.clone(),` to the returned struct literal (shares the `Arc`).

- [ ] **Step 4: Populate on append**

In `append`, inside the entry loop, AFTER the successful `self.journal.append(...)` and the probe stamp, insert into the cache (move `entry` in — it is no longer used after encoding):
```rust
// ... let notifier = self.journal.append(seq, term, &payload).map_err(journal_io)?;
// last_notifier = Some(notifier);
// uc_protocol::probes::stamp_log(seq, ...JournalAppended);
// probe_last_seq = Some(seq);
self.cache.append_entry(seq, entry, payload.len());
```
(`payload` is the `Vec<u8>` from `encode_to_vec`; `payload.len()` is its byte size. `entry` was only borrowed by `encode_to_vec(&entry, ...)`, so it is still owned here and can be moved.)

- [ ] **Step 5: Consult the cache in `try_get_log_entries`**

Replace the body of `try_get_log_entries` to check the cache first, resolving the `RangeBounds` to concrete `[start, end)`:
```rust
async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
    &mut self,
    range: RB,
) -> Result<Vec<<TypeConfig as openraft::RaftTypeConfig>::Entry>, io::Error> {
    use std::ops::Bound;
    // Resolve to [start, end). Unbounded ends can't be cache-checked -> fall through.
    let start = match range.start_bound() {
        Bound::Included(&s) => Some(s),
        Bound::Excluded(&s) => Some(s + 1),
        Bound::Unbounded => None,
    };
    let end = match range.end_bound() {
        Bound::Included(&e) => Some(e + 1),
        Bound::Excluded(&e) => Some(e),
        Bound::Unbounded => None,
    };
    if let (Some(start), Some(end)) = (start, end) {
        if let Some(entries) = self.cache.get_range(start, end) {
            return Ok(entries);
        }
    }
    // Miss / unbounded -> journal (unchanged).
    let iter = self.journal.iter_range(range).map_err(journal_io)?;
    let mut entries = Vec::new();
    for record in iter {
        let (_seq, _meta, payload) = record.map_err(journal_io)?;
        let (entry, _) = bincode::serde::decode_from_slice::<
            <TypeConfig as openraft::RaftTypeConfig>::Entry, _,
        >(&payload, bincode::config::standard())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        entries.push(entry);
    }
    Ok(entries)
}
```

- [ ] **Step 6: Evict on `truncate_after` and `purge`**

In `truncate_after`, after `self.journal.truncate_after(keep_seq)...wait()...`, add `self.cache.truncate_after(keep_seq);`.
In `purge`, after `self.journal.purge_before(log_id.index)...` (and the `last_purged` store), add `self.cache.purge_upto(log_id.index);` (purge removes `index <= log_id.index`).

- [ ] **Step 7: Run the differential test + the existing suite**

Run:
```bash
cargo test -p uc_node cache_reads_match_journal 2>&1 | tail -8
cargo test -p uc_node 2>&1 | grep -E "test result|FAILED"
cargo clippy -p uc_node -- -D warnings 2>&1 | tail -3
```
Expected: the differential test PASSES for both `budget=0` and `budget=64MB`; the full `uc_node` unit/integration suite green; clippy clean. If a read returns wrong entries, the cache invariant is broken — debug the append/evict/range logic against the spec §4; do NOT weaken the test.

- [ ] **Step 8: Commit**

```bash
git add uc_node/src/raft/log_storage.rs
git commit -m "feat(uc_node): wire EntryCache into JournalLogStorage (populate/serve/evict); UC_LOG_CACHE_BYTES"
```

---

## Task 3: Hit/miss observability + lincheck/partition validation

**Files:**
- Modify: `uc_node/src/raft/log_storage.rs` (periodic hit/miss debug log) — and, if straightforward, the node metrics publisher.
- Validation only (no test file changes beyond confirming green).

- [ ] **Step 1: Surface hit/miss**

Add a lightweight periodic log of the cache hit/miss counters. Simplest: in an existing periodic node task (grep `metrics_publisher` / a tick loop in `uc_node/src`), every few seconds emit `tracing::debug!(hits = cache.hits(), misses = cache.misses(), "log_entry_cache")`. If the node has a metrics publisher struct that already exposes gauges/counters, register `log_cache_hits`/`log_cache_misses` there (read via `Arc<EntryCache>`); if wiring into the publisher is non-trivial, the debug log alone is sufficient for this task — note that in the report. Keep it cheap (relaxed atomic loads).

- [ ] **Step 2: Build + unit/integration green**

Run: `cargo test -p uc_node 2>&1 | grep -E "test result|FAILED"` and `cargo clippy -p uc_node -- -D warnings 2>&1 | tail -3`. Expected: green + clean.

- [ ] **Step 3: Lincheck + partition (the correctness guard), cache ON and OFF**

The cache feeds replication/apply, so linearizability is the ultimate guard. Run the capstone with the cache enabled (default) and disabled:
```bash
# cache ON (default 256MB)
cargo test -p uc_node --test lin_register 2>&1 | tail -15
cargo test -p uc_node --features fault-injection --test lin_partition -- --test-threads=1 2>&1 | tail -15
# cache OFF (rollback path unchanged)
UC_LOG_CACHE_BYTES=0 cargo test -p uc_node --test lin_register 2>&1 | tail -15
UC_LOG_CACHE_BYTES=0 cargo test -p uc_node --features fault-injection --test lin_partition -- --test-threads=1 2>&1 | tail -15
```
Expected: **Linearizable / green in all four**. A failure with the cache ON but not OFF means the cache diverges from the journal — debug against spec §4 (append/truncate/purge/range), do NOT patch the test. (Check the exact lincheck test names/features against `docs/tasks/task14`/`task15`; adjust the invocation if the crate/feature names differ.)

- [ ] **Step 4: Commit**

```bash
git add uc_node/src/raft/log_storage.rs
git commit -m "feat(uc_node): log entry cache hit/miss observability + lincheck/partition validated (cache on & off)"
```

---

## Post-merge validation (measured — not a code task)

The success criterion is the fleet before/after, using the harness from `docs/benchmarks/leader-profile-inflight-2026-06-30.md` (the `profile` hook + inflight sweep). With the cache on: (1) the leader profile at the knee should show the bottleneck thread's `read()`/`ext4`/`crc32` CPU collapse; (2) the throughput knee should rise past ~15k; (3) the hit/miss counter should show a high tail hit-rate. This is a fleet run (cloud), performed separately after merge — it validates the win the profiling predicted.

---

## Self-review notes

- **Spec coverage:** §2 approach A → Task 1+2; §3 data structure → Task 1 (`EntryCache`/`CacheInner`, contiguous `VecDeque`, `parking_lot::RwLock`); §4 operations + byte-identical invariant → Task 2 (append/serve/evict/truncate/purge) + the differential test; §5 config (`UC_LOG_CACHE_BYTES` default 256MB, 0=disabled) + hit/miss metrics → Task 2 (config) + Task 3 (metrics); §6 testing (unit + differential + lincheck/partition on&off) → Tasks 1–3; the fleet win-measurement → Post-merge section. §7 non-goals honored (no partial-hits, no journal-level, no lock-free, no separate CRC/zero-copy).
- **Placeholder check:** the two test-helper constructors (`test_blank_entry`, `new_test_storage`/`append_n`) are described as "adapt to the real `uc_node` constructor" with the exact grep to find it — this is a faithful pointer to existing code, not a vague TODO; the cache code + all integration edits are complete and concrete. The metrics-publisher wiring is bounded ("debug log suffices if publisher wiring is non-trivial") to avoid unbounded scope.
- **Type consistency:** `EntryCache::new/append_entry/get_range/truncate_after/purge_upto/hits/misses` used identically across tasks; `type Entry` alias consistent; `purge_upto(log_id.index)` matches the journal's `purge_before(log_id.index)` semantics (removes `index <= log_id.index`); `truncate_after(keep_seq)` matches `Journal::truncate_after(keep_seq)` (keeps `<= keep_seq`).
