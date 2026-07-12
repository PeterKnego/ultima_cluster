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
    /// Atomic fetch-add, `AcqRel` (used by the M5 cnc page: `service_epoch`
    /// bump at service attach and `next_client_id` allocation — both need
    /// the read-modify-write result, and everything the caller wrote before
    /// it, visible to any concurrent reader immediately, not just
    /// eventually). Returns the value *before* the add, matching
    /// `AtomicU64::fetch_add`.
    #[inline]
    pub fn fetch_add(&self, v: u64) -> u64 {
        self.v.fetch_add(v, Ordering::AcqRel)
    }
}

const _: () = assert!(std::mem::size_of::<PaddedAtomicU64>() == 64);

/// The M1+M2+M3 counter set. append: written only by the appender (leader) /
/// receiver (follower), after the frame commit word (so any position below
/// `append` is a committed frame). durable: written only by the archive,
/// after write+fdatasync of the block. sent: written only by the sender
/// agent, after the datagram send (leader only; follower leaves it 0).
/// commit: the cluster's quorum-fsync'd position (spec §6) — written only by
/// the sender-agent thread on the leader (quorum ranking) and only by the
/// receiver-agent thread on a follower (CommitPosition gossip, monotonic).
/// NOT primed on restart: locally-durable bytes are not necessarily
/// quorum-durable, so priming commit would manufacture a phantom commit; it
/// is re-derived live. (Commit persistence is revisited in M4/M5.)
#[repr(C)]
pub struct LogCounters {
    pub append: PaddedAtomicU64,
    pub durable: PaddedAtomicU64,
    pub sent: PaddedAtomicU64,
    pub commit: PaddedAtomicU64,
}

// Layout pinned to the M5 cnc v2 page (uc_protocol::v2::cnc): `CncPage`
// casts `LogCounters` directly at `CNC_OFF_APPEND`, so its field order and
// per-field 64-byte stride must never drift from the protocol constants.
// `uc2_log::cnc::tests::cnc_offsets_match_protocol_constants` cross-checks
// this against `uc_protocol` directly; these asserts pin the struct side.
const _: () = assert!(std::mem::size_of::<LogCounters>() == 256);
const _: () = assert!(std::mem::offset_of!(LogCounters, append) == 0);
const _: () = assert!(std::mem::offset_of!(LogCounters, durable) == 64);
const _: () = assert!(std::mem::offset_of!(LogCounters, sent) == 128);
const _: () = assert!(std::mem::offset_of!(LogCounters, commit) == 192);

impl LogCounters {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            append: PaddedAtomicU64::new(0),
            durable: PaddedAtomicU64::new(0),
            sent: PaddedAtomicU64::new(0),
            commit: PaddedAtomicU64::new(0),
        }
    }
    /// Prime the counters after archive recovery (append resumes at durable —
    /// bytes beyond durable are discarded on restart, spec §6).
    ///
    /// CONTRACT: after priming over a FRESH (zeroed/recreated) buffer file,
    /// positions below `pos` have no bytes in the buffer — validated reads
    /// return `Overrun` and the journal is the only source, until a prefill
    /// mechanism exists (spec §4 "node restart", sized in M4/M6).
    /// `commit` is deliberately not primed (see the struct doc).
    pub fn prime(&self, pos: u64) {
        self.durable.store_release(pos);
        self.append.store_release(pos);
        // A restart resends from durable; followers drop the duplicates.
        self.sent.store_release(pos);
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
        assert_eq!(c.sent.load_acquire(), 0);
        assert_eq!(c.commit.load_acquire(), 0);
        c.prime(4096);
        assert_eq!(c.append.load_acquire(), 4096);
        assert_eq!(c.durable.load_acquire(), 4096);
        assert_eq!(c.sent.load_acquire(), 4096);
        // commit is NOT primed: locally-durable bytes are not necessarily
        // quorum-durable — priming commit would be a phantom commit. It is
        // re-derived from quorum reports (leader) or gossip (follower).
        assert_eq!(c.commit.load_acquire(), 0);
    }

    #[test]
    fn padded_is_a_full_cache_line() {
        assert_eq!(std::mem::size_of::<PaddedAtomicU64>(), 64);
        assert_eq!(std::mem::align_of::<PaddedAtomicU64>(), 64);
    }

    #[test]
    fn fetch_add_returns_prior_value_and_advances() {
        let a = PaddedAtomicU64::new(1);
        assert_eq!(a.fetch_add(1), 1, "next_client_id-style allocation: old id returned");
        assert_eq!(a.load_acquire(), 2);
        assert_eq!(a.fetch_add(5), 2);
        assert_eq!(a.load_acquire(), 7);
    }
}
