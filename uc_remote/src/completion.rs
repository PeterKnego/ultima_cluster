// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `CompletionQueue` — the bounded SPSC hand-off from the reader thread to
//! `RemotePollHalf::poll`.
//!
//! The reader is the producer; `poll` is the consumer. Response bodies are
//! copied **once**, into this queue's arena, so a completion callback can
//! borrow them without a per-request allocation and without pinning the
//! socket read buffer.
//!
//! **It never drops.** A full queue makes [`CompletionQueue::push`] return
//! `false`; the reader then publishes what it has and parks on
//! [`CompletionQueue::drained`] until `poll` frees space. Dropping a
//! completion would break the crate's central promise (every accepted request
//! ends in exactly one outcome), so backpressure is the only option.
//!
//! Sizing: the arena is at least `MAX_FRAME_LEN`, so any single body that
//! could arrive on this wire fits in an empty arena — which is what makes
//! "park until there is room" terminate rather than deadlock.
//!
//! Task 5 gave this queue its real ends: `link::Reader` (the reader thread's
//! role token, plus a `Link::close` that has already joined it) is the
//! producer, and `RemotePollHalf::poll` — through `link::drain_completions` —
//! is the consumer. Only the outcome tags no frame handler produces yet carry
//! a narrow `allow`.

use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::park::WaitCell;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum OutcomeTag {
    Response,
    Unknown,
    PayloadTooLarge,
    TimedOut,
    Closed,
}

#[derive(Clone, Copy)]
pub(crate) struct Record {
    pub user_data: u64,
    pub position: u64,
    pub has_position: bool,
    pub tag: OutcomeTag,
    pub replayed: bool,
    pub expired: bool,
    pub body_off: u64,
    pub body_len: u32,
}

impl Record {
    pub(crate) fn simple(user_data: u64, tag: OutcomeTag) -> Record {
        Record {
            user_data,
            position: 0,
            has_position: false,
            tag,
            replayed: false,
            expired: false,
            body_off: 0,
            body_len: 0,
        }
    }
}

pub(crate) struct CompletionQueue {
    /// The slot array, owned via a raw pointer rather than
    /// `UnsafeCell<Box<[Record]>>` — deliberately, and for the same reason
    /// [`crate::outgoing::OutRing`] does it: `Box`'s `Deref`/`DerefMut` (and
    /// any indexing through it) forms a reference over the WHOLE allocation,
    /// which retags every element (Stacked/Tree Borrows) and invalidates
    /// whatever the other thread is concurrently touching in a disjoint part
    /// of the same allocation. Every access below builds its pointer with
    /// `ptr::add` and reads/writes EXACTLY the one slot it owns.
    slots: *mut Record,
    slot_mask: usize,
    /// The body arena. Same ownership rule as `slots`, and the reason is
    /// sharper here: `drain` hands the callback a `&[u8]` INTO this
    /// allocation while the producer may be filling a disjoint range of it,
    /// so a whole-allocation reference on the producer side would invalidate
    /// the slice the callback is reading. Slices are built with
    /// `slice::from_raw_parts` over exactly the accessed range; writes are
    /// raw `ptr::copy_nonoverlapping` and form no reference at all.
    arena: *mut u8,
    arena_mask: usize,
    /// Producer-owned: slots filled.
    head: AtomicU64,
    /// Consumer-owned: slots released.
    tail: AtomicU64,
    /// Producer-owned: arena bytes written.
    arena_head: AtomicU64,
    /// Consumer-owned: arena bytes released.
    arena_tail: AtomicU64,
    ready: WaitCell,
    drained: WaitCell,
}

// SAFETY: single producer (the reader thread) and single consumer (`poll`),
// each owning its own cursors — the producer writes `head`/`arena_head` and
// only ever reads `tail`/`arena_tail`; the consumer writes `tail`/
// `arena_tail` and only ever reads `head`/`arena_head`. Storage follows the
// cursors: the producer touches only slot `head & slot_mask` and arena bytes
// `[arena_head, arena_tail + arena_capacity)`, the consumer only slots
// `[tail, head)` and arena bytes `[arena_tail, arena_head)`, and the
// admission checks in `push` keep those two ranges disjoint. No reference is
// ever formed over more than the exact range one thread owns (see the
// `slots`/`arena` field docs), so the two threads' accesses never alias.
unsafe impl Send for CompletionQueue {}
unsafe impl Sync for CompletionQueue {}

impl CompletionQueue {
    /// `arena_bytes` is raised to at least `MAX_FRAME_LEN` and both sizes are
    /// rounded up to a power of two. The arena floor is load-bearing, not
    /// hygiene: a body larger than the whole arena could never be pushed, and
    /// the reader's "park until there is room" loop would then never
    /// terminate. `MAX_FRAME_LEN` bounds every body that can arrive on this
    /// wire, so an empty arena always has room for the next one.
    pub(crate) fn new(entries: usize, arena_bytes: usize) -> CompletionQueue {
        Self::build(
            entries,
            arena_bytes.max(crate::frame::MAX_FRAME_LEN as usize),
        )
    }

    /// Test-only: build with an arena BELOW the `MAX_FRAME_LEN` floor, so the
    /// wrap and arena-full paths can be exercised in a few kilobytes (and, in
    /// the two-thread Miri test, at a size Miri can actually interpret).
    /// Bodies must be kept smaller than `arena_bytes` by the caller — the
    /// property `new` guarantees structurally.
    #[cfg(test)]
    pub(crate) fn with_small_arena(entries: usize, arena_bytes: usize) -> CompletionQueue {
        Self::build(entries, arena_bytes)
    }

    fn build(entries: usize, arena_bytes: usize) -> CompletionQueue {
        let n = entries.max(16).next_power_of_two();
        let a = arena_bytes.max(64).next_power_of_two();
        let blank = Record::simple(0, OutcomeTag::Closed);
        // SAFETY (both): `Box::into_raw` hands back exactly the pointer
        // `Drop` reconstructs the box from, once, with the same length (`n`
        // / `a`, recovered as `slot_mask + 1` / `arena_mask + 1`).
        let slots = Box::into_raw(vec![blank; n].into_boxed_slice()) as *mut Record;
        let arena = Box::into_raw(vec![0u8; a].into_boxed_slice()) as *mut u8;
        CompletionQueue {
            slots,
            slot_mask: n - 1,
            arena,
            arena_mask: a - 1,
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
            arena_head: AtomicU64::new(0),
            arena_tail: AtomicU64::new(0),
            ready: WaitCell::new(),
            drained: WaitCell::new(),
        }
    }

    pub(crate) fn entries(&self) -> usize {
        self.slot_mask + 1
    }

    pub(crate) fn arena_capacity(&self) -> usize {
        self.arena_mask + 1
    }

    /// PRODUCER ONLY. `false` = no room; the caller must retry the same
    /// record after `poll` has drained (it must never drop it).
    ///
    /// **Publish, then park.** On `false`, call [`Self::publish`] before
    /// parking on [`Self::drained`]: a producer that parks without publishing
    /// waits for a consumer it never woke, and sleeps to its park timeout.
    pub(crate) fn push(&self, mut r: Record, body: &[u8]) -> bool {
        // Producer-owned cursors are read `Relaxed` (this thread is their
        // only writer); the consumer's are read `Acquire`, which is what
        // makes the space the consumer released visible here.
        let h = self.head.load(Ordering::Relaxed);
        let t = self.tail.load(Ordering::Acquire);
        debug_assert!(h >= t, "push: head must never fall behind tail");
        debug_assert!(
            (h - t) as usize <= self.entries(),
            "push: occupancy must never exceed the slot count"
        );
        if (h - t) as usize > self.slot_mask {
            return false;
        }
        let ah = self.arena_head.load(Ordering::Relaxed);
        let at = self.arena_tail.load(Ordering::Acquire);
        debug_assert!(
            ah >= at,
            "push: arena_head must never fall behind arena_tail"
        );
        let arena_cap = self.arena_capacity();
        debug_assert!(
            (ah - at) as usize <= arena_cap,
            "push: arena occupancy must never exceed the arena"
        );
        if body.len() > arena_cap - (ah - at) as usize {
            return false;
        }
        let mut done = 0usize;
        while done < body.len() {
            let idx = ((ah + done as u64) as usize) & self.arena_mask;
            let n = (body.len() - done).min(arena_cap - idx);
            // SAFETY: `[arena_head, arena_tail + arena_capacity)` is
            // producer-owned — the checks above proved this body fits inside
            // it, so these `n` bytes are not bytes the consumer may read
            // (the consumer reads only `[arena_tail, arena_head)`, and it
            // only advances `arena_tail` after its callbacks have returned).
            // `idx + n <= arena_cap` by construction, so the write stays in
            // the allocation. This forms no reference at all — a raw
            // pointer copy — so it cannot invalidate the `&[u8]` the
            // consumer thread may be holding into a disjoint range of the
            // same allocation (see the `arena` field doc).
            unsafe {
                ptr::copy_nonoverlapping(body[done..done + n].as_ptr(), self.arena.add(idx), n);
            }
            done += n;
        }
        r.body_off = ah;
        debug_assert!(
            body.len() <= u32::MAX as usize,
            "push: a body length must fit Record::body_len"
        );
        r.body_len = body.len() as u32;
        let slot = (h as usize) & self.slot_mask;
        // SAFETY: slot `head & slot_mask` is producer-owned until `head` is
        // published below — the consumer only reads slots `[tail, head)`,
        // and `head - tail <= slot_mask` (checked above) keeps this index
        // distinct from every one of them. `slot <= slot_mask`, so the write
        // is in bounds. `Record` is `Copy` (no destructor), so overwriting
        // the previous lap's value with `ptr::write` leaks nothing, and no
        // reference is formed over the array.
        unsafe {
            ptr::write(self.slots.add(slot), r);
        }
        // Release both cursors: the consumer's `Acquire` load of `head`
        // synchronizes with the store below, which is ordered after every
        // write above, so a drained record's slot AND its arena bytes are
        // visible to the consumer.
        debug_assert!(
            ah + body.len() as u64 <= at + arena_cap as u64,
            "push: arena_head must never pass arena_tail + capacity"
        );
        debug_assert!(
            (h + 1 - t) as usize <= self.entries(),
            "push: head must never pass tail + the slot count"
        );
        self.arena_head
            .store(ah + body.len() as u64, Ordering::Release);
        self.head.store(h + 1, Ordering::Release);
        true
    }

    /// PRODUCER ONLY: one wake per read batch, not per frame.
    pub(crate) fn publish(&self) {
        self.ready.signal();
    }

    /// CONSUMER ONLY: hand at most `max` completions to `cb`, then release
    /// their arena bytes. A body that wrapped is copied into a scratch buffer
    /// so the callback always sees one contiguous slice.
    ///
    /// # Panics
    ///
    /// If `cb` panics the panic propagates — but the records whose callbacks
    /// already RETURNED are released first, by a drop guard, so the unwind
    /// cannot re-deliver them on the next `drain`. The record whose callback
    /// panicked is *not* released: it is retried on the next `drain`, and comes
    /// out first. A panicking callback therefore weakens exactly-once to
    /// at-least-once for the single record it panicked on, and for no other —
    /// the only direction that keeps the module's promise (every accepted
    /// request ends in exactly one outcome) free of duplicates.
    pub(crate) fn drain(&self, max: usize, mut cb: impl FnMut(Record, &[u8])) -> usize {
        let arena_cap = self.arena_capacity();
        let mut t = self.tail.load(Ordering::Relaxed);
        let h = self.head.load(Ordering::Acquire);
        debug_assert!(h >= t, "drain: head must never fall behind tail");
        let mut scratch: Vec<u8> = Vec::new();
        let arena_from = self.arena_tail.load(Ordering::Relaxed);
        let arena_head = self.arena_head.load(Ordering::Acquire);
        // Every cursor advance below is published by this guard's `Drop`: on
        // the normal path when it goes out of scope at the end of the batch
        // (one publish, one wake, exactly as before), and on an unwinding path
        // from wherever `cb` panicked. Its fields advance only AFTER a
        // callback returns, so an unwind frees exactly the records the user
        // actually received.
        let mut p = Progress {
            q: self,
            tail: t,
            arena_tail: arena_from,
            arena_head,
            n: 0,
        };
        while p.n < max && t < h {
            let slot = (t as usize) & self.slot_mask;
            // SAFETY: slots `[tail, head)` are consumer-owned — the producer
            // will not touch this one again until the `tail` store below
            // releases it. The producer published the slot with a `Release`
            // store to `head` that the `Acquire` load above synchronizes
            // with, so the bytes are visible and initialized. `slot <=
            // slot_mask`, so the read is in bounds; `Record` is `Copy`, so
            // `ptr::read` duplicates rather than moves, and no reference is
            // formed over the array.
            let rec = unsafe { ptr::read(self.slots.add(slot)) };
            let len = rec.body_len as usize;
            debug_assert!(
                rec.body_off >= arena_from && rec.body_off + len as u64 <= arena_head,
                "drain: a queued record's body must lie inside [arena_tail, arena_head)"
            );
            let idx = (rec.body_off as usize) & self.arena_mask;
            if idx + len <= arena_cap {
                // SAFETY: `[arena_tail, arena_head)` is consumer-readable —
                // the producer's admission check refuses to write into it,
                // and it is only freed by the `arena_tail` store below,
                // after every callback of this drain has returned. The
                // producer published these bytes before the `Release` store
                // to `head` this drain's `Acquire` load synchronizes with.
                // The slice covers EXACTLY `[idx, idx + len)` (`idx + len <=
                // arena_cap` on this branch), never the whole allocation, so
                // it cannot be invalidated by the producer's disjoint,
                // equally narrow raw-pointer writes elsewhere in the arena.
                let body = unsafe { slice::from_raw_parts(self.arena.add(idx), len) };
                cb(rec, body);
            } else {
                let head_n = arena_cap - idx;
                scratch.clear();
                scratch.reserve(len);
                // SAFETY: as above, for each of the two halves the wrapped
                // body occupies — `[idx, arena_cap)` and `[0, len -
                // head_n)`, both inside the allocation and both inside
                // `[arena_tail, arena_head)`.
                unsafe {
                    scratch.extend_from_slice(slice::from_raw_parts(self.arena.add(idx), head_n));
                    scratch.extend_from_slice(slice::from_raw_parts(self.arena, len - head_n));
                }
                cb(rec, &scratch);
            }
            // The callback returned, so this record is now genuinely consumed
            // — and only now may the guard release it.
            t += 1;
            p.tail = t;
            p.arena_tail = rec.body_off + len as u64;
            p.n += 1;
            debug_assert!(p.tail <= h, "drain: tail must never pass head");
        }
        let n = p.n;
        drop(p);
        n
    }

    /// Either side: the producer to decide whether to signal, the consumer to
    /// decide whether to park.
    pub(crate) fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire) == self.tail.load(Ordering::Acquire)
    }

    /// The consumer parks here; the producer signals it from [`Self::publish`].
    pub(crate) fn ready(&self) -> &WaitCell {
        &self.ready
    }

    /// The producer parks here when full; the consumer signals it as it frees.
    ///
    /// **Publish, then park**: call [`Self::publish`] before parking here, or
    /// the consumer may never have been woken to do the draining this park is
    /// waiting on, and the park runs to its timeout. Park with the
    /// seq-observed pattern — read [`WaitCell::seq`] BEFORE re-checking
    /// `push`, and pass that value — so a signal landing in between is not
    /// slept through.
    pub(crate) fn drained(&self) -> &WaitCell {
        &self.drained
    }

    #[cfg(test)]
    fn cursors(&self) -> (u64, u64, u64, u64) {
        (
            self.head.load(Ordering::Acquire),
            self.tail.load(Ordering::Acquire),
            self.arena_head.load(Ordering::Acquire),
            self.arena_tail.load(Ordering::Acquire),
        )
    }
}

/// CONSUMER ONLY: the cursor advance [`CompletionQueue::drain`] owes the
/// producer, published from `Drop` so it happens on the unwinding path too.
///
/// Without it, a callback that panics mid-batch would leave `tail` and
/// `arena_tail` where the batch started, and the next `drain` would hand the
/// user the records it already delivered — a duplicate completion, which is
/// precisely what this module promises never to produce. With it, an unwind
/// releases exactly the records whose callbacks returned.
struct Progress<'a> {
    q: &'a CompletionQueue,
    /// The slot cursor to publish: one past the last record `cb` returned on.
    tail: u64,
    /// The arena cursor to publish: the end of that record's body.
    arena_tail: u64,
    /// Snapshot taken before the batch, for the debug assertions only.
    arena_head: u64,
    /// Records consumed so far; `0` means there is nothing to publish.
    n: usize,
}

impl Drop for Progress<'_> {
    fn drop(&mut self) {
        if self.n == 0 {
            return;
        }
        debug_assert!(
            self.arena_tail <= self.arena_head,
            "drain: arena_tail moves forward, never past arena_head"
        );
        // Release, so the producer's `Acquire` loads of these two cursors
        // order its reuse writes after every read the callbacks made. Both
        // stores are sound on an unwind for the same reason they are on the
        // normal path: `n > 0` only counts callbacks that RETURNED, so no
        // slice handed out is still live.
        self.q.arena_tail.store(self.arena_tail, Ordering::Release);
        self.q.tail.store(self.tail, Ordering::Release);
        self.q.drained.signal();
    }
}

impl Drop for CompletionQueue {
    fn drop(&mut self) {
        // SAFETY: both pointers came from `Box::into_raw` in `build`, over
        // slices of exactly `self.entries()` / `self.arena_capacity()`
        // elements; `drop` takes `&mut self`, so both threads that could
        // have held a borrow into either allocation are gone, and each box
        // is reconstructed exactly once. `ptr::slice_from_raw_parts_mut`
        // builds the fat pointer directly rather than going through
        // `slice::from_raw_parts_mut`, which would form a whole-allocation
        // `&mut` just to hand it to `Box::from_raw` — the very thing the
        // field docs above forbid.
        unsafe {
            drop(Box::from_raw(ptr::slice_from_raw_parts_mut(
                self.slots,
                self.entries(),
            )));
            drop(Box::from_raw(ptr::slice_from_raw_parts_mut(
                self.arena,
                self.arena_capacity(),
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn a_pushed_record_and_its_body_come_back_out() {
        let q = CompletionQueue::new(8, 64 * 1024);
        let mut r = Record::simple(0xDEAD, OutcomeTag::Response);
        r.position = 640;
        r.has_position = true;
        r.replayed = true;
        assert!(q.push(r, b"cba"));
        q.publish();
        let mut seen = Vec::new();
        let n = q.drain(16, |rec, body| {
            seen.push((rec.user_data, rec.position, rec.replayed, body.to_vec()))
        });
        assert_eq!(n, 1);
        assert_eq!(seen, vec![(0xDEAD, 640, true, b"cba".to_vec())]);
        assert!(q.is_empty());
    }

    #[test]
    fn the_arena_floor_admits_any_body_that_can_arrive_on_this_wire() {
        // The property that makes the reader's "park until there is room"
        // loop terminate: an empty arena always fits the largest frame the
        // wire can carry.
        let q = CompletionQueue::new(8, 0);
        assert!(q.arena_capacity() >= crate::frame::MAX_FRAME_LEN as usize);
    }

    #[test]
    fn a_full_queue_refuses_rather_than_dropping_and_recovers_after_a_drain() {
        // `entries` is rounded up to the 16-slot floor, so this queue holds 16.
        let q = CompletionQueue::new(4, 64 * 1024);
        assert_eq!(q.entries(), 16);
        let mut pushed = 0u64;
        while q.push(Record::simple(pushed, OutcomeTag::Response), b"x") {
            pushed += 1;
            assert!(pushed < 64, "the queue never filled");
        }
        assert_eq!(pushed, 16, "capacity is the entry count, exactly");
        let before = q.drained().seq();
        let drained = q.drain(2, |_, _| {});
        assert_eq!(drained, 2);
        assert_ne!(
            q.drained().seq(),
            before,
            "a drain must wake a parked producer"
        );
        assert!(
            q.push(Record::simple(99, OutcomeTag::Response), b"x"),
            "a drained queue takes more"
        );
    }

    #[test]
    fn a_full_arena_refuses_even_when_slots_are_free() {
        // 4 KiB arena, 1024 entries: the arena is the binding limit.
        let q = CompletionQueue::with_small_arena(1024, 4096);
        let body = vec![0u8; 1000];
        let mut pushed = 0;
        while q.push(Record::simple(pushed, OutcomeTag::Response), &body) {
            pushed += 1;
            assert!(pushed < 64, "the arena never filled");
        }
        assert!(
            pushed <= 4,
            "a 4 KiB arena cannot hold five 1000-byte bodies"
        );
        let n = q.drain(1024, |_, b| assert_eq!(b.len(), 1000));
        assert_eq!(n as u64, pushed);
        assert!(
            q.push(Record::simple(1, OutcomeTag::Response), &body),
            "a drained arena takes more"
        );
    }

    #[test]
    fn a_body_that_wraps_the_arena_is_returned_contiguous() {
        let q = CompletionQueue::with_small_arena(64, 4096);
        let body: Vec<u8> = (0..600u32).map(|i| i as u8).collect();
        let mut wrapped = false;
        // Six 600-byte bodies push the seventh over the 4 KiB wrap.
        for round in 0..8 {
            let off = q.cursors().2;
            assert!(
                q.push(Record::simple(round, OutcomeTag::Response), &body),
                "round {round}"
            );
            if (off as usize & q.arena_mask) + body.len() > q.arena_capacity() {
                wrapped = true;
            }
            let n = q.drain(1, |rec, b| {
                assert_eq!(rec.user_data, round);
                assert_eq!(
                    b,
                    &body[..],
                    "round {round}: a wrapped body must read back whole"
                );
            });
            assert_eq!(n, 1);
        }
        assert!(
            wrapped,
            "no body actually straddled the wrap — the test proved nothing"
        );
    }

    #[test]
    fn drain_is_bounded_by_max() {
        let q = CompletionQueue::new(64, 64 * 1024);
        for i in 0..10 {
            assert!(q.push(Record::simple(i, OutcomeTag::TimedOut), b""));
        }
        assert_eq!(q.drain(3, |_, _| {}), 3);
        assert_eq!(q.drain(100, |_, _| {}), 7);
    }

    /// A panicking callback must not cost the queue its no-duplicates
    /// promise: the records it already handed over are released on the way
    /// out, and only the one it panicked on comes back.
    #[test]
    fn a_panicking_callback_retries_only_its_own_record_and_re_delivers_no_other() {
        let q = CompletionQueue::new(16, 64 * 1024);
        for i in 0..5u64 {
            let body = vec![i as u8; 4];
            assert!(q.push(Record::simple(i, OutcomeTag::Response), &body));
        }

        let first: Mutex<Vec<(u64, Vec<u8>)>> = Mutex::new(Vec::new());
        let before = q.drained().seq();
        // The panic message would otherwise be printed by the default hook and
        // read as a test failure to anyone watching the output.
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            q.drain(10, |rec, body| {
                first.lock().unwrap().push((rec.user_data, body.to_vec()));
                assert_ne!(rec.user_data, 2, "boom");
            })
        }));
        std::panic::set_hook(prev_hook);
        assert!(outcome.is_err(), "the callback's panic must propagate");
        assert_eq!(
            first.into_inner().unwrap(),
            vec![(0, vec![0u8; 4]), (1, vec![1u8; 4]), (2, vec![2u8; 4]),],
            "records 0 and 1 completed; 2 is the one that panicked"
        );
        // 0 and 1 returned, so they are gone; 2 panicked, so it is retried and
        // comes out first; 3 and 4 were never reached.
        let mut second = Vec::new();
        let n = q.drain(10, |rec, body| second.push((rec.user_data, body.to_vec())));
        assert_eq!(n, 3);
        assert_eq!(
            second,
            vec![(2, vec![2u8; 4]), (3, vec![3u8; 4]), (4, vec![4u8; 4])],
            "no record may be delivered twice, and none may be lost"
        );
        assert!(q.is_empty());
        assert_ne!(
            q.drained().seq(),
            before,
            "the unwind still had to wake a producer parked on a full queue"
        );
        let (h, t, ah, at) = q.cursors();
        assert_eq!(h, t, "the slot cursors reconverge");
        assert_eq!(ah, at, "the arena cursors reconverge");
    }

    #[test]
    fn publish_bumps_the_ready_cell_so_a_parked_poller_wakes() {
        let q = CompletionQueue::new(8, 4096);
        let before = q.ready().seq();
        q.publish();
        assert_ne!(q.ready().seq(), before);
    }

    /// The real two-thread exercise, and the one the Miri run in the report is
    /// over: a producer pushing while a consumer drains, concurrently,
    /// through the raw-pointer accesses in `push`/`drain`. Body sizes are
    /// irregular (a xorshift PRNG) so bodies straddle the arena's physical
    /// wrap over and over at unaligned offsets, and the arena is small enough
    /// that the producer really does hit "full" and park on `drained` — so
    /// the callback is reading a slice out of the arena while the producer
    /// writes a disjoint range of the SAME allocation, which is the aliasing
    /// question the raw-pointer storage exists to answer.
    ///
    /// Both parks use the seq-observed pattern (read `seq()` BEFORE the
    /// re-check, pass it to `park`), so a signal landing between the check
    /// and the park cannot be slept through.
    #[test]
    fn two_threads_agree_on_every_completion_under_concurrent_push_and_drain() {
        // Miri interprets every memory access, so keep this modest; it still
        // pushes ~90 KiB of bodies through a 1 KiB arena (~90 laps).
        const N: u64 = 700;

        let q = Arc::new(CompletionQueue::with_small_arena(32, 1024));
        let mut expected: Vec<(u64, u64, OutcomeTag, Vec<u8>)> = Vec::with_capacity(N as usize);
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        for i in 0..N {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let len = (state % 257) as usize; // 0..=256-byte bodies
            let body: Vec<u8> = (0..len).map(|k| ((i as usize + k) & 0xFF) as u8).collect();
            let tag = if len.is_multiple_of(3) {
                OutcomeTag::Response
            } else {
                OutcomeTag::Unknown
            };
            expected.push((i, i * 8, tag, body));
        }

        let done = Arc::new(AtomicBool::new(false));
        let cq = Arc::clone(&q);
        let cdone = Arc::clone(&done);
        let consumer = thread::spawn(move || {
            let mut seen: Vec<(u64, u64, OutcomeTag, Vec<u8>)> = Vec::new();
            loop {
                // Seq observed BEFORE the drain (the condition re-check).
                let observed = cq.ready().seq();
                let n = cq.drain(4, |rec, body| {
                    assert!(rec.has_position, "every pushed record set has_position");
                    seen.push((rec.user_data, rec.position, rec.tag, body.to_vec()));
                });
                if n == 0 {
                    if cdone.load(Ordering::Acquire) && cq.is_empty() {
                        break;
                    }
                    cq.ready().park(observed, Duration::from_millis(20));
                }
            }
            seen
        });

        let mut refusals = 0u64;
        for (user_data, position, tag, body) in &expected {
            let mut r = Record::simple(*user_data, *tag);
            r.position = *position;
            r.has_position = true;
            loop {
                // Seq observed BEFORE the push (the condition re-check).
                let observed = q.drained().seq();
                if q.push(r, body) {
                    break;
                }
                refusals += 1;
                q.publish(); // let the consumer see what is already queued
                q.drained().park(observed, Duration::from_millis(20));
            }
            q.publish();
        }
        done.store(true, Ordering::Release);
        q.publish();

        let seen = consumer.join().unwrap();
        assert_eq!(seen.len(), expected.len());
        assert!(
            seen == expected,
            "the consumer's drained stream must equal the exact push order, records and bodies"
        );
        assert!(
            refusals > 0,
            "the producer never hit a full queue — the backpressure path, and the \
             producer-writes-while-the-callback-reads overlap it creates, went untested"
        );
        let (h, t, ah, at) = q.cursors();
        assert_eq!(h, t, "every slot was released");
        assert_eq!(ah, at, "every arena byte was released");
        assert_eq!(h, N, "exactly N records made the round trip");
    }
}
