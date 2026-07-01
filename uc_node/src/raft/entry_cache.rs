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
        while !g.entries.is_empty() {
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

    // Telemetry accessors. `hits` is used by the differential cache test and
    // by the periodic node-level `log_entry_cache` tracing emit (Task 3).
    // `misses` is used for the same tracing emit.  Both use relaxed loads —
    // the counters are approximate and require no cross-thread ordering.
    pub(crate) fn hits(&self) -> u64 { self.hits.load(Ordering::Relaxed) }
    pub(crate) fn misses(&self) -> u64 { self.misses.load(Ordering::Relaxed) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::vote::RaftLeaderId as _;
    use openraft::{EntryPayload, LogId};

    fn ent(index: u64, term: u64) -> Entry {
        Entry {
            log_id: LogId::new(crate::raft::LeaderId::new(term, 1), index),
            payload: EntryPayload::Blank,
        }
    }

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
        // Each entry: payload_len=10 + ENTRY_OVERHEAD_BYTES=64 = 74 bytes.
        // Budget 100 bytes → only 1 entry fits at a time.
        let c = EntryCache::new(100);
        for i in 1..=20 { c.append_entry(i, ent(i, 1), 10); }
        // oldest evicted: low-index read misses, only the tail (entry 20) survives.
        assert!(c.get_range(1, 2).is_none());
        assert!(c.get_range(20, 21).is_some());
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
