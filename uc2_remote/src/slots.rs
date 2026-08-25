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
//! 5. `off`/`len`, `kind`, `sent_seq`, `not_before_ns` and `attempts` are
//!    written by the submitter before publish and thereafter by the
//!    writer/reader threads; they are advisory (they steer the writer), never
//!    the completion protocol, so `Relaxed` is correct for them.
//! 6. **Advisory does not mean reachable at any time, in either direction.**
//!    Those words belong to the generation that wrote them and the next
//!    occupant of the same INDEX reuses them, so a seq that has been resolved
//!    must neither be read from nor written to. Nobody outside this file
//!    "holds a seq live": a caller can only ever have *observed* it live a
//!    moment ago, and be preempted before it acts. So the accessors carry the
//!    check themselves — [`SlotTable::live_extent`] answers `None` rather than
//!    stale, and [`SlotTable::mark_sent_if`] / [`SlotTable::bump_attempts_if`]
//!    refuse rather than stamp. Both stamps additionally tag their VALUE with
//!    the generation, so even a store that slips through the gate's own
//!    window is read as "not mine" by the next occupant instead of being
//!    inherited.
//!
//! Task 5 gave this table its first real callers — the link's deadline sweep,
//! its shutdown drain and its retransmit bookkeeping. Task 6 gave it the
//! claim/resolve protocol itself: `RemoteSendHalf::try_submit` claims, the
//! reader's `RESPONSE` handler resolves, and the writer's frame cursor marks
//! what it has put on the wire. What is still unused is task 8's re-send
//! bookkeeping, which carries a narrow per-item `allow` until then.

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
    /// Generation-tagged attempt counter: `(seq + 1) << 32 | count`, `0` when
    /// it belongs to nobody. See the note on [`SlotTable::bump_attempts_if`]
    /// for why the tag is part of the value rather than a separate check.
    attempts: AtomicU64,
    kind: AtomicU8,
    /// `seq + 1` once this request's frame has been written, `0` otherwise —
    /// a generation tag rather than a flag, for the same reason.
    sent_seq: AtomicU64,
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
                attempts: AtomicU64::new(0),
                kind: AtomicU8::new(0),
                sent_seq: AtomicU64::new(0),
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
        s.sent_seq.store(0, Ordering::Relaxed);
        s.owner.store(seq + 1, Ordering::Release);
        true
    }

    /// SUBMITTER ONLY: is the index this seq lands on unoccupied? Checked
    /// before the outgoing ring is touched, so a refusal never consumes a seq
    /// and never leaves staged bytes behind a slot that was never claimed.
    /// Only the submitter claims and every other thread can only ever FREE a
    /// slot, so a `true` here cannot go stale under the caller.
    pub(crate) fn is_free(&self, seq: u64) -> bool {
        self.slot(seq).owner.load(Ordering::Acquire) == FREE
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

    pub(crate) fn resolve(&self, seq: u64) -> Resolve {
        self.take(seq)
    }

    #[allow(dead_code, reason = "task 9 aborts a slot the edge answered finally")]
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

    pub(crate) fn is_live(&self, seq: u64) -> bool {
        self.slot(seq).owner.load(Ordering::Acquire) == seq + 1
    }

    /// Where `seq`'s frame sits in the outgoing ring — **only while the slot
    /// still belongs to `seq`**. `None` once it has been resolved.
    ///
    /// The generation check is not a nicety. `off`/`len` are advisory words
    /// that a later occupant of the same INDEX overwrites, and an index is
    /// re-claimed every `slot_count()` seqs, so a caller that reads the extent
    /// of a resolved seq gets a **different frame's** offset — a much larger
    /// one. Both callers would be wrong in a way that is silent: the writer's
    /// frame cursor would never reach it and would stop advancing for good,
    /// and the submitter's reclaim would free ring bytes that still belong to
    /// live requests. So the extent is readable only through the generation
    /// that owns it, and "resolved" is `None` rather than a stale answer.
    ///
    /// Seqlock-shaped: check the generation, read the words, check again. The
    /// second check is what closes a resolve-and-re-claim that lands between
    /// the first check and the reads — the re-claim must pass through
    /// `RESERVED` and publish `seq' + 1 != seq + 1`, so a matching second read
    /// proves nothing moved.
    pub(crate) fn live_extent(&self, seq: u64) -> Option<(u64, u32)> {
        let s = self.slot(seq);
        if s.owner.load(Ordering::Acquire) != seq + 1 {
            return None;
        }
        let off = s.off.load(Ordering::Relaxed);
        let len = s.len.load(Ordering::Relaxed);
        // Keeps the two advisory loads above from being ordered after the
        // re-check below (an `Acquire` load orders only what FOLLOWS it).
        std::sync::atomic::fence(Ordering::Acquire);
        if s.owner.load(Ordering::Relaxed) != seq + 1 {
            return None;
        }
        Some((off, len))
    }

    /// What the slot recorded at `claim`. Asserted by this module's own tests;
    /// nothing on the hot path reads it, because task 8's re-send turned out to
    /// be **positional** — a frame is copied back out of the outgoing ring
    /// exactly as it was staged, so the re-send never has to know which of the
    /// two kinds it is carrying.
    #[allow(dead_code, reason = "recorded at claim; read only by this module's tests")]
    pub(crate) fn kind(&self, seq: u64) -> ReqKind {
        if self.slot(seq).kind.load(Ordering::Relaxed) == ReqKind::Query as u8 {
            ReqKind::Query
        } else {
            ReqKind::Submit
        }
    }

    /// Record whether `seq`'s frame is on the wire — **only while the slot
    /// still belongs to `seq`**. `false` = it does not any more, and nothing
    /// was written.
    ///
    /// Two layers, because one is not enough. The gate refuses the common
    /// case (the caller is stamping a seq that has since been answered), but
    /// a caller that observed the slot live a moment ago can still be
    /// preempted between the check and the store, and by the time it lands
    /// the index may belong to `seq + slot_count()`. So the VALUE carries the
    /// generation too: a stray store writes the OLD `seq + 1`, and
    /// [`SlotTable::is_sent`] compares against the CURRENT seq, so the new
    /// occupant reads `false`. The dangerous direction — a never-written
    /// frame that task 8's re-send skips as "already on the wire" — is
    /// therefore closed by construction, not by timing.
    pub(crate) fn mark_sent_if(&self, seq: u64, sent: bool) -> bool {
        let s = self.slot(seq);
        if s.owner.load(Ordering::Acquire) != seq + 1 {
            return false;
        }
        s.sent_seq.store(if sent { seq + 1 } else { 0 }, Ordering::Relaxed);
        true
    }

    pub(crate) fn is_sent(&self, seq: u64) -> bool {
        self.slot(seq).sent_seq.load(Ordering::Relaxed) == seq + 1
    }

    /// Hold `seq`'s frame back until `ns` — **only while the slot still
    /// belongs to `seq`**. `false` = it does not any more, and nothing was
    /// written.
    ///
    /// Gated for the same reason as [`SlotTable::mark_sent_if`], with a
    /// different failure mode: an ungated store lands on whichever request has
    /// since re-claimed the INDEX (every `slot_count()` seqs), and a backoff of
    /// up to `MAX_RETRY_SLEEP` inherited by a brand-new request is a latency
    /// defect nothing later corrects — the writer simply will not write it
    /// until a delay somebody else was told to take has expired.
    pub(crate) fn set_not_before_if(&self, seq: u64, ns: u64) -> bool {
        let s = self.slot(seq);
        if s.owner.load(Ordering::Acquire) != seq + 1 {
            return false;
        }
        s.not_before_ns.store(ns, Ordering::Relaxed);
        true
    }

    pub(crate) fn not_before(&self, seq: u64) -> u64 {
        self.slot(seq).not_before_ns.load(Ordering::Relaxed)
    }

    /// Count one write of `seq`'s frame and report the new total — **only
    /// while the slot still belongs to `seq`**. `None` = it does not any
    /// more, and nothing was counted.
    ///
    /// Same two layers as [`SlotTable::mark_sent_if`], and the counter is
    /// tagged for the same reason: `(seq + 1) << 32 | count`. A stray bump
    /// that lands after the index has been re-claimed writes the OLD tag, and
    /// the new occupant's first real bump sees a tag that is not its own and
    /// starts from 1 — so an inherited count can never make a first
    /// transmission look like a re-send.
    pub(crate) fn bump_attempts_if(&self, seq: u64) -> Option<u32> {
        let s = self.slot(seq);
        if s.owner.load(Ordering::Acquire) != seq + 1 {
            return None;
        }
        // The tag is the low 32 bits of the generation. Two seqs can only
        // share a tag AND an index if they are 2^32 apart, and the window a
        // stray bump has to survive is the few instructions between the gate
        // above and the store below — not 2^32 requests. The full-width check
        // is the `owner` gate; this is only what makes a LOST race harmless.
        let tag = ((seq + 1) & 0xFFFF_FFFF) << 32;
        let mut cur = s.attempts.load(Ordering::Relaxed);
        loop {
            // A value carrying anyone else's tag (or none) counts as zero for
            // this generation.
            let count = if cur & 0xFFFF_FFFF_0000_0000 == tag { cur & 0xFFFF_FFFF } else { 0 };
            let next = tag | (count + 1).min(0xFFFF_FFFF);
            match s.attempts.compare_exchange_weak(
                cur,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some((next & 0xFFFF_FFFF) as u32),
                Err(actual) => cur = actual,
            }
        }
    }

    pub(crate) fn inflight(&self) -> u64 {
        self.inflight.load(Ordering::Acquire)
    }

    /// The lowest seq never yet issued. SUBMITTER publishes, writer reads.
    pub(crate) fn next_seq(&self) -> u64 {
        self.next_seq.load(Ordering::Acquire)
    }

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
        assert_eq!(t.live_extent(1), Some((4096, 120)));
        assert_eq!(t.kind(1), ReqKind::Query);
        assert!(!t.is_sent(1), "a fresh slot has not been written yet");
        assert!(t.mark_sent_if(1, true), "a live slot takes the stamp");
        assert!(t.is_sent(1));
        assert!(t.mark_sent_if(1, false));
        assert!(!t.is_sent(1), "a RETRY marks a slot unsent again");
        assert!(t.set_not_before_if(1, 12_345), "a live slot takes the backoff");
        assert_eq!(t.not_before(1), 12_345);
        assert_eq!(t.bump_attempts_if(1), Some(1));
        assert_eq!(t.bump_attempts_if(1), Some(2));
    }

    /// The hazard `live_extent` closed for READS, closed for WRITES: the
    /// writer thread observes a slot live, is preempted, and by the time it
    /// stamps `sent`/`attempts` the request has been answered and the INDEX
    /// re-claimed by a seq a table-length later. Stamping the old seq must
    /// leave the new occupant completely alone — otherwise task 8's re-send
    /// skips a frame that was never written ("already on the wire") and the
    /// request is lost.
    #[test]
    fn a_stamp_for_a_resolved_seq_never_touches_the_slots_new_occupant() {
        let t = table();
        let old = 1u64;
        let new = old + t.slot_count() as u64; // same physical index

        assert!(t.claim(old, 0xA1, ReqKind::Submit, u64::MAX, 0, 64));
        assert!(t.mark_sent_if(old, true));
        assert_eq!(t.bump_attempts_if(old), Some(1));
        assert_eq!(t.resolve(old), Resolve::Won { user_data: 0xA1 });
        assert!(t.claim(new, 0xA2, ReqKind::Submit, u64::MAX, 4096, 64));

        // The gate: a stamp naming the resolved seq is refused outright.
        assert!(!t.mark_sent_if(old, true), "a resolved seq must not take a stamp");
        assert!(!t.mark_sent_if(old, false), "…in either direction");
        assert_eq!(t.bump_attempts_if(old), None, "a resolved seq must not be counted");

        // The new occupant is untouched: not sent, and its first real write is
        // attempt 1 — never an inherited count that would read as a re-send.
        assert!(!t.is_sent(new), "the new occupant must not inherit `sent`");
        assert_eq!(t.bump_attempts_if(new), Some(1), "the new occupant starts at 1");
        assert_eq!(t.live_extent(new), Some((4096, 64)), "and its extent is its own");
    }

    /// The residual race the gate alone cannot close: a stamp passes the gate
    /// while the slot is live, is preempted, and its store lands only after
    /// the request was answered and the index re-claimed. No API can express
    /// that ordering from outside, so the stray store is written directly —
    /// which is precisely the state such a thread leaves behind.
    ///
    /// The value is generation-tagged, so the new occupant reads "not mine":
    /// not sent, and its first real write is attempt 1.
    #[test]
    fn a_stamp_that_lands_after_a_re_claim_is_read_as_not_mine() {
        let t = table();
        let old = 1u64;
        let new = old + t.slot_count() as u64;

        assert!(t.claim(old, 0xB1, ReqKind::Submit, u64::MAX, 0, 64));
        assert_eq!(t.resolve(old), Resolve::Won { user_data: 0xB1 });
        assert!(t.claim(new, 0xB2, ReqKind::Submit, u64::MAX, 4096, 64));

        // A writer thread that observed `old` live, then stalled: its stores
        // land here, on a slot that now belongs to `new`.
        let s = t.slot(old);
        s.sent_seq.store(old + 1, Ordering::Relaxed);
        s.attempts.store((((old + 1) & 0xFFFF_FFFF) << 32) | 7, Ordering::Relaxed);

        assert!(!t.is_sent(new), "a stray stamp must not read as sent for the new occupant");
        assert_eq!(t.bump_attempts_if(new), Some(1), "nor may its count be inherited");
        // …and the new occupant's own stamps still work, over the top of it.
        assert!(t.mark_sent_if(new, true));
        assert!(t.is_sent(new));
        assert_eq!(t.bump_attempts_if(new), Some(2));
    }

    #[test]
    fn is_live_tracks_the_slot_and_next_seq_is_published() {
        let t = table();
        assert!(!t.is_live(1));
        assert!(t.is_free(1), "an unclaimed index is free");
        assert!(t.claim(1, 1, ReqKind::Submit, 1_000, 0, 88));
        assert!(t.is_live(1));
        assert!(!t.is_free(1), "a claimed index is not free");
        assert!(
            !t.is_free(1 + t.slot_count() as u64),
            "`is_free` is about the INDEX: a later generation of an occupied index is not free"
        );
        t.publish_next_seq(2);
        assert_eq!(t.next_seq(), 2);
        assert_eq!(t.abort(1), Resolve::Won { user_data: 1 });
        assert!(!t.is_live(1));
    }

    /// The table's first REAL concurrent exercise — task 6 gives it its two
    /// live callers (a submitter claiming, a reader resolving), and every
    /// earlier test in this file drives both from one thread.
    ///
    /// What it pins is the single-CAS protocol under a genuine race: every
    /// `user_data` is resolved EXACTLY once (no loss, no double completion),
    /// a losing `resolve` says `Miss`, and `inflight` returns to zero — the
    /// accounting that the credit window and `reclaim` both key off. The
    /// submitter's refusals (window full, or the index's previous occupant
    /// still live) are the same backpressure `try_submit` reports, so the
    /// retry loop here is the shape the engine's caller runs.
    ///
    /// This is also one of the three modules the Miri run covers: the CAS
    /// orderings are what make the claim's metadata writes visible to the
    /// resolver, and a race there would be UB, not a flake.
    #[test]
    fn a_submitter_and_a_resolver_race_without_losing_or_duplicating_a_completion() {
        use std::sync::Arc;
        use std::thread;

        // Keep it modest: Miri interprets every access, and 200 seqs through
        // the 64-slot floor is already three laps of generation reuse — every
        // index is re-claimed under a later seq while the resolver races it.
        const N: u64 = 200;
        let t = Arc::new(SlotTable::new(8));
        assert_eq!(t.slot_count(), 64, "the sizing floor, so N must exceed it to reuse an index");

        let resolver_t = Arc::clone(&t);
        let resolver = thread::spawn(move || {
            let mut won = Vec::with_capacity(N as usize);
            for seq in 1..=N {
                loop {
                    match resolver_t.resolve(seq) {
                        Resolve::Won { user_data } => {
                            // The loser of the race must say so, every time.
                            assert_eq!(
                                resolver_t.resolve(seq),
                                Resolve::Miss,
                                "seq {seq} resolved twice"
                            );
                            won.push(user_data);
                            break;
                        }
                        // Not claimed yet: the submitter is behind us.
                        Resolve::Miss => thread::yield_now(),
                    }
                }
            }
            won
        });

        for seq in 1..=N {
            // The submitter's own retry loop: `claim` refuses on a full window
            // or a still-live occupant of the index this seq lands on, and a
            // refusal must never consume the seq.
            while !t.claim(seq, seq * 7, ReqKind::Submit, u64::MAX, seq * 128, 64) {
                thread::yield_now();
            }
        }

        let won = resolver.join().unwrap();
        assert_eq!(
            won,
            (1..=N).map(|s| s * 7).collect::<Vec<u64>>(),
            "every claim must be resolved exactly once, in seq order"
        );
        assert_eq!(t.inflight(), 0, "inflight must return to zero");
        assert!(!t.is_live(N), "nothing is left live");
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
            t.live_extent(1),
            Some((4096, 64)),
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
