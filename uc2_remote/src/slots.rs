// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Correlation slot table: generation-tagged, exactly-once completions.
//!
//! ADAPTED COPY of `uc2_client/src/slots.rs` (same invariants, same
//! single-CAS completion protocol) rather than a dependency: `uc2_remote`'s
//! tiny dependency set is an advertised property of the crate.
//!
//! # The invariants this file owns
//!
//! 1. A slot's `owner` word is `0` = FREE, `u64::MAX` = RESERVED (mid-claim,
//!    metadata not yet valid), else `seq + 1` — the generation tag.
//! 2. Claim is three-phase: CAS `FREE -> RESERVED`, write metadata, publish
//!    `owner = seq + 1` with `Release`.
//! 3. Exactly-once resolution: whoever CASes `owner: seq+1 -> FREE` (AcqRel)
//!    owns the completion. `resolve`, `abort`, `sweep` and `drain_abort` all
//!    race through that one CAS.
//! 4. The seq is assigned by the submitter (gap-free, from 1) and is a full
//!    `u64` on this wire, so a stale generation is caught by the exact
//!    `owner == seq + 1` test — no truncation argument needed.
//! 5. `extent`, `kind`, `sent`, `not_before_ns` and `attempts` are written by
//!    the submitter before publish and thereafter by the writer/reader
//!    threads; they are advisory (they steer the writer), never the
//!    completion protocol, so `Relaxed` is correct for them.
//!
//! Task 5 gave this table its first real callers — the link's deadline sweep,
//! its shutdown drain and its retransmit bookkeeping. The claim/resolve
//! protocol itself waits for task 6's `try_submit` and the reader's frame
//! handlers, and each still-unused item carries a narrow `allow` until then.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};

const FREE: u64 = 0;
const RESERVED: u64 = u64::MAX;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ReqKind {
    Submit = 0,
    Query = 1,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Resolve {
    Won { user_data: u64 },
    Miss,
}

struct Slot {
    owner: AtomicU64,
    user_data: AtomicU64,
    deadline_ns: AtomicU64,
    not_before_ns: AtomicU64,
    off: AtomicU64,
    len: AtomicU32,
    attempts: AtomicU32,
    kind: AtomicU8,
    sent: AtomicU8,
}

pub(crate) struct SlotTable {
    slots: Box<[Slot]>,
    mask: usize,
    inflight: AtomicU64,
    max_inflight: u64,
    next_seq: AtomicU64,
}

impl SlotTable {
    pub(crate) fn new(max_inflight: u32) -> SlotTable {
        assert!(max_inflight >= 1);
        // 2x headroom over the window, 64 floor — same sizing rule as
        // `uc2_client`: it keeps a stuck (deadline-pending) occupant off the
        // index a fresh seq lands on.
        let n = (max_inflight.next_power_of_two() as usize * 2).max(64);
        let slots = (0..n)
            .map(|_| Slot {
                owner: AtomicU64::new(FREE),
                user_data: AtomicU64::new(0),
                deadline_ns: AtomicU64::new(0),
                not_before_ns: AtomicU64::new(0),
                off: AtomicU64::new(0),
                len: AtomicU32::new(0),
                attempts: AtomicU32::new(0),
                kind: AtomicU8::new(0),
                sent: AtomicU8::new(0),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        SlotTable {
            slots,
            mask: n - 1,
            inflight: AtomicU64::new(0),
            max_inflight: max_inflight as u64,
            next_seq: AtomicU64::new(1),
        }
    }

    fn slot(&self, seq: u64) -> &Slot {
        &self.slots[(seq as usize) & self.mask]
    }

    /// SUBMITTER ONLY. `false` = the window is full or the slot's previous
    /// occupant is still live; either way the caller reports backpressure and
    /// does NOT consume the seq.
    #[allow(dead_code, reason = "task 6's `try_submit` claims the slot")]
    pub(crate) fn claim(
        &self,
        seq: u64,
        user_data: u64,
        kind: ReqKind,
        deadline_ns: u64,
        off: u64,
        len: u32,
    ) -> bool {
        if self.inflight.fetch_add(1, Ordering::AcqRel) >= self.max_inflight {
            self.inflight.fetch_sub(1, Ordering::AcqRel);
            return false;
        }
        let s = self.slot(seq);
        if s.owner
            .compare_exchange(FREE, RESERVED, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            self.inflight.fetch_sub(1, Ordering::AcqRel);
            return false;
        }
        s.user_data.store(user_data, Ordering::Relaxed);
        s.deadline_ns.store(deadline_ns, Ordering::Relaxed);
        s.not_before_ns.store(0, Ordering::Relaxed);
        s.off.store(off, Ordering::Relaxed);
        s.len.store(len, Ordering::Relaxed);
        s.attempts.store(0, Ordering::Relaxed);
        s.kind.store(kind as u8, Ordering::Relaxed);
        s.sent.store(0, Ordering::Relaxed);
        s.owner.store(seq + 1, Ordering::Release);
        true
    }

    fn take(&self, seq: u64) -> Resolve {
        let s = self.slot(seq);
        let owner = s.owner.load(Ordering::Acquire);
        if owner != seq + 1 {
            return Resolve::Miss;
        }
        let user_data = s.user_data.load(Ordering::Relaxed);
        if s.owner
            .compare_exchange(owner, FREE, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Resolve::Miss;
        }
        self.inflight.fetch_sub(1, Ordering::AcqRel);
        Resolve::Won { user_data }
    }

    #[allow(dead_code, reason = "task 6's RESPONSE handler resolves the slot")]
    pub(crate) fn resolve(&self, seq: u64) -> Resolve {
        self.take(seq)
    }

    #[allow(dead_code, reason = "task 8/9 abort a slot the edge answered finally")]
    pub(crate) fn abort(&self, seq: u64) -> Resolve {
        self.take(seq)
    }

    pub(crate) fn sweep(&self, now_ns: u64, mut cb: impl FnMut(u64)) -> usize {
        let mut n = 0;
        for s in self.slots.iter() {
            let owner = s.owner.load(Ordering::Acquire);
            if owner == FREE || owner == RESERVED {
                continue;
            }
            if s.deadline_ns.load(Ordering::Relaxed) > now_ns {
                continue;
            }
            let user_data = s.user_data.load(Ordering::Relaxed);
            if s.owner
                .compare_exchange(owner, FREE, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.inflight.fetch_sub(1, Ordering::AcqRel);
                cb(user_data);
                n += 1;
            }
        }
        n
    }

    pub(crate) fn drain_abort(&self, mut cb: impl FnMut(u64)) -> usize {
        let mut n = 0;
        for s in self.slots.iter() {
            let owner = s.owner.load(Ordering::Acquire);
            if owner == FREE || owner == RESERVED {
                continue;
            }
            let user_data = s.user_data.load(Ordering::Relaxed);
            if s.owner
                .compare_exchange(owner, FREE, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.inflight.fetch_sub(1, Ordering::AcqRel);
                cb(user_data);
                n += 1;
            }
        }
        n
    }

    #[allow(dead_code, reason = "task 8's ordered window re-send skips dead slots")]
    pub(crate) fn is_live(&self, seq: u64) -> bool {
        self.slot(seq).owner.load(Ordering::Acquire) == seq + 1
    }

    #[allow(dead_code, reason = "task 8's re-send copies the frame out of the ring by extent")]
    pub(crate) fn extent(&self, seq: u64) -> (u64, u32) {
        let s = self.slot(seq);
        (s.off.load(Ordering::Relaxed), s.len.load(Ordering::Relaxed))
    }

    #[allow(dead_code, reason = "task 8's re-send needs SUBMIT vs QUERY for the stats split")]
    pub(crate) fn kind(&self, seq: u64) -> ReqKind {
        if self.slot(seq).kind.load(Ordering::Relaxed) == ReqKind::Query as u8 {
            ReqKind::Query
        } else {
            ReqKind::Submit
        }
    }

    pub(crate) fn mark_sent(&self, seq: u64, sent: bool) {
        self.slot(seq).sent.store(u8::from(sent), Ordering::Relaxed);
    }

    #[allow(dead_code, reason = "task 8's re-send skips what is already on the wire")]
    pub(crate) fn is_sent(&self, seq: u64) -> bool {
        self.slot(seq).sent.load(Ordering::Relaxed) != 0
    }

    pub(crate) fn set_not_before(&self, seq: u64, ns: u64) {
        self.slot(seq).not_before_ns.store(ns, Ordering::Relaxed);
    }

    #[allow(dead_code, reason = "task 8 honours a RETRY backoff through this")]
    pub(crate) fn not_before(&self, seq: u64) -> u64 {
        self.slot(seq).not_before_ns.load(Ordering::Relaxed)
    }

    #[allow(dead_code, reason = "task 8 counts a request's re-sends")]
    pub(crate) fn bump_attempts(&self, seq: u64) -> u32 {
        self.slot(seq).attempts.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub(crate) fn inflight(&self) -> u64 {
        self.inflight.load(Ordering::Acquire)
    }

    /// The lowest seq never yet issued. SUBMITTER publishes, writer reads.
    #[allow(dead_code, reason = "task 8's re-send walks the submitter's published seq range")]
    pub(crate) fn next_seq(&self) -> u64 {
        self.next_seq.load(Ordering::Acquire)
    }

    #[allow(dead_code, reason = "task 6's `try_submit` publishes the cursor the writer reads")]
    pub(crate) fn publish_next_seq(&self, seq: u64) {
        self.next_seq.store(seq, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn slot_count(&self) -> usize {
        self.slots.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> SlotTable {
        SlotTable::new(8)
    }

    #[test]
    fn a_claimed_slot_resolves_exactly_once() {
        let t = table();
        assert!(t.claim(1, 0xAA, ReqKind::Submit, 1_000, 0, 88));
        assert_eq!(t.inflight(), 1);
        assert_eq!(t.resolve(1), Resolve::Won { user_data: 0xAA });
        assert_eq!(t.resolve(1), Resolve::Miss, "a second resolve must lose");
        assert_eq!(t.inflight(), 0);
    }

    #[test]
    fn a_stale_generation_never_resolves_a_live_slot() {
        let t = table();
        let n = 1 + (t.slot_count() as u64); // same index, older generation
        assert!(t.claim(n, 0xBB, ReqKind::Submit, 1_000, 0, 88));
        assert_eq!(t.resolve(1), Resolve::Miss, "seq 1 is a stale generation of that slot");
        assert_eq!(t.resolve(n), Resolve::Won { user_data: 0xBB });
    }

    #[test]
    fn the_window_is_capped_at_max_inflight() {
        let t = table();
        for seq in 1..=8u64 {
            assert!(t.claim(seq, seq, ReqKind::Submit, 1_000, 0, 88), "seq {seq}");
        }
        assert!(!t.claim(9, 9, ReqKind::Submit, 1_000, 0, 88), "the 9th must be refused");
        assert_eq!(t.inflight(), 8);
        assert_eq!(t.resolve(1), Resolve::Won { user_data: 1 });
        assert!(t.claim(9, 9, ReqKind::Submit, 1_000, 0, 88), "a freed slot admits the next");
    }

    #[test]
    fn sweep_fails_everything_past_its_deadline_and_nothing_before_it() {
        let t = table();
        assert!(t.claim(1, 0xA1, ReqKind::Submit, 100, 0, 88));
        assert!(t.claim(2, 0xA2, ReqKind::Submit, 900, 88, 88));
        let mut fired = Vec::new();
        assert_eq!(t.sweep(500, |ud| fired.push(ud)), 1);
        assert_eq!(fired, vec![0xA1]);
        assert_eq!(t.resolve(1), Resolve::Miss, "a swept slot is gone");
        assert_eq!(t.resolve(2), Resolve::Won { user_data: 0xA2 });
    }

    #[test]
    fn drain_abort_takes_every_live_slot_once() {
        let t = table();
        for seq in 1..=4u64 {
            assert!(t.claim(seq, seq, ReqKind::Submit, u64::MAX, 0, 88));
        }
        let mut fired = Vec::new();
        assert_eq!(t.drain_abort(|ud| fired.push(ud)), 4);
        fired.sort_unstable();
        assert_eq!(fired, vec![1, 2, 3, 4]);
        assert_eq!(t.drain_abort(|_| {}), 0);
        assert_eq!(t.inflight(), 0);
    }

    #[test]
    fn the_ring_extent_and_the_sent_flag_round_trip() {
        let t = table();
        assert!(t.claim(1, 0xAA, ReqKind::Query, 1_000, 4096, 120));
        assert_eq!(t.extent(1), (4096, 120));
        assert_eq!(t.kind(1), ReqKind::Query);
        assert!(!t.is_sent(1), "a fresh slot has not been written yet");
        t.mark_sent(1, true);
        assert!(t.is_sent(1));
        t.mark_sent(1, false);
        assert!(!t.is_sent(1), "a RETRY marks a slot unsent again");
        t.set_not_before(1, 12_345);
        assert_eq!(t.not_before(1), 12_345);
        assert_eq!(t.bump_attempts(1), 1);
        assert_eq!(t.bump_attempts(1), 2);
    }

    #[test]
    fn is_live_tracks_the_slot_and_next_seq_is_published() {
        let t = table();
        assert!(!t.is_live(1));
        assert!(t.claim(1, 1, ReqKind::Submit, 1_000, 0, 88));
        assert!(t.is_live(1));
        t.publish_next_seq(2);
        assert_eq!(t.next_seq(), 2);
        assert_eq!(t.abort(1), Resolve::Won { user_data: 1 });
        assert!(!t.is_live(1));
    }

    #[test]
    fn a_live_occupant_a_table_length_later_refuses_the_claim_without_corruption() {
        let t = table(); // max_inflight = 8, slot_count = 16
        assert!(t.claim(1, 0xC1, ReqKind::Query, u64::MAX, 4096, 64));
        let collide = 1 + t.slot_count() as u64; // same physical index, later generation
        let before = t.inflight();
        assert!(
            !t.claim(collide, 0xC2, ReqKind::Submit, u64::MAX, 0, 88),
            "a live occupant must refuse a same-index claim from a later generation"
        );
        assert_eq!(t.inflight(), before, "a refused claim must not change inflight");
        assert_eq!(
            t.kind(1),
            ReqKind::Query,
            "the occupant's metadata must be untouched by the failed claim"
        );
        assert_eq!(
            t.extent(1),
            (4096, 64),
            "the occupant's extent must be untouched by the failed claim"
        );
        assert_eq!(
            t.resolve(1),
            Resolve::Won { user_data: 0xC1 },
            "the original occupant resolves cleanly, unharmed by the refused collision"
        );
        assert!(
            t.claim(collide, 0xC2, ReqKind::Submit, u64::MAX, 0, 88),
            "the slot is free now and admits the next claim"
        );
    }
}
