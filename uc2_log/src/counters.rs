// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Position counters (spec §4). One writer per counter, many readers, all
//! coordination is release/acquire on these — no locks, no wakeups.
//! `repr(C)` + fixed layout: these will be placed into the mmap'd cnc v2
//! page when protocol v2 IPC lands (M5); until then they live on the heap.

use std::sync::atomic::{AtomicU64, Ordering};

/// A cache-line-isolated atomic u64 (prevents false sharing between counters).
#[repr(C, align(64))]
pub struct PaddedAtomicU64 {
    v: AtomicU64,
    _pad: [u8; 56],
}

impl PaddedAtomicU64 {
    pub fn new(v: u64) -> Self {
        Self { v: AtomicU64::new(v), _pad: [0; 56] }
    }
    #[inline]
    pub fn load_acquire(&self) -> u64 {
        self.v.load(Ordering::Acquire)
    }
    #[inline]
    pub fn store_release(&self, v: u64) {
        self.v.store(v, Ordering::Release)
    }
}

/// The M1 counter set. append: written only by the appender, after the frame
/// commit word (so any position below `append` is a committed frame).
/// durable: written only by the archive, after write+fdatasync of the block.
#[repr(C)]
pub struct LogCounters {
    pub append: PaddedAtomicU64,
    pub durable: PaddedAtomicU64,
}

impl LogCounters {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self { append: PaddedAtomicU64::new(0), durable: PaddedAtomicU64::new(0) }
    }
    /// Prime both counters after archive recovery (append resumes at durable —
    /// bytes beyond durable are discarded on restart, spec §6).
    pub fn prime(&self, pos: u64) {
        self.durable.store_release(pos);
        self.append.store_release(pos);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_start_at_zero_and_prime() {
        let c = LogCounters::new();
        assert_eq!(c.append.load_acquire(), 0);
        assert_eq!(c.durable.load_acquire(), 0);
        c.prime(4096);
        assert_eq!(c.append.load_acquire(), 4096);
        assert_eq!(c.durable.load_acquire(), 4096);
    }

    #[test]
    fn padded_is_a_full_cache_line() {
        assert_eq!(std::mem::size_of::<PaddedAtomicU64>(), 64);
        assert_eq!(std::mem::align_of::<PaddedAtomicU64>(), 64);
    }
}
