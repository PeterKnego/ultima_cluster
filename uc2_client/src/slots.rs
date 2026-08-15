// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Correlation slot table: generation-tagged, exactly-once completions.
//!
//! # The invariants this file owns
//!
//! 1. A slot's `owner` word is `0` = FREE, `u64::MAX` = RESERVED (mid-claim, metadata not yet valid — resolve/sweep must skip), else `seq + 1` (the FULL u64 sequence: the generation tag; the wire only carries `seq as u32`).
//!
//! 2. Claim is three-phase: CAS `FREE -> RESERVED` (so a failed claim never stomps a live occupant's metadata), write metadata (`user_data`, `deadline_ns`, `kind`), publish `owner = seq + 1` with `Release`.
//!
//! 3. Exactly-once resolution: whoever CASes `owner: seq+1 -> FREE` (AcqRel) owns the completion — resolve, sweep, drain_abort, and release all race through that single CAS, so a request completes exactly once.
//!
//! 4. Wrap safety: `resolve(wire_seq)` recomputes `idx = wire_seq as usize & mask` (valid because `mask < 2^32` so `seq & mask == (seq as u32) & mask`), then checks `(stored_seq as u32) == wire_seq`. A stale collision would need the same slot AND the same low 32 bits — a 2^32 outstanding gap, impossible under a bounded window.
//!
//! 5. A `SlotBusy` claim burns its seq (gaps in the wire sequence are harmless — correlation is by value, not continuity) and surfaces as backpressure; it means an old in-flight (a full table-length of seqs ago) still holds the slot, which the deadline sweep will clear.
//!
//! 6. `drain_abort` is exhaustive only after claims are quiesced — a claim racing the drain publishes after the scan passes and is backstopped by the deadline sweep, so pollers must keep sweeping unless claims are provably stopped.

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

const FREE: u64 = 0;
const RESERVED: u64 = u64::MAX;

#[allow(dead_code)] // consumed from Task 3 on
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ReqKind {
    Submit = 0,
    Query = 1,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ClaimError {
    WindowFull,
    SlotBusy,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Resolve {
    Won { user_data: u64 },
    KindMismatch,
    Miss,
}

struct Slot {
    owner: AtomicU64,       // FREE / RESERVED / seq+1
    user_data: AtomicU64,
    deadline_ns: AtomicU64, // nanos since the engine's t0
    kind: AtomicU8,         // ReqKind as u8
}

#[allow(dead_code)] // consumed from Task 3 on
pub(crate) struct SlotTable {
    slots: Box<[Slot]>,
    mask: usize,
    next_seq: AtomicU64,
    inflight: AtomicU64,
    max_inflight: u64,
}

#[allow(dead_code)] // consumed from Task 3 on
impl SlotTable {
    pub(crate) fn new(max_inflight: u32, start_seq: u64) -> SlotTable {
        assert!(max_inflight >= 1);
        // 2x headroom over the window halves the odds a fresh seq lands on a
        // stuck (deadline-pending) occupant's slot; 64 floor keeps tiny
        // windows from degenerate tables.
        let n = (max_inflight.next_power_of_two() as usize * 2).max(64);
        let slots = (0..n)
            .map(|_| Slot {
                owner: AtomicU64::new(FREE),
                user_data: AtomicU64::new(0),
                deadline_ns: AtomicU64::new(0),
                kind: AtomicU8::new(0),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        SlotTable {
            slots,
            mask: n - 1,
            next_seq: AtomicU64::new(start_seq),
            inflight: AtomicU64::new(0),
            max_inflight: max_inflight as u64,
        }
    }

    pub(crate) fn claim(
        &self,
        user_data: u64,
        kind: ReqKind,
        deadline_ns: u64,
    ) -> Result<u64, ClaimError> {
        if self.inflight.fetch_add(1, Ordering::AcqRel) >= self.max_inflight {
            self.inflight.fetch_sub(1, Ordering::AcqRel);
            return Err(ClaimError::WindowFull);
        }
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let slot = &self.slots[(seq as usize) & self.mask];
        // Phase 1: reserve. A failed reserve must NOT touch the occupant's
        // metadata — that is why metadata writes come after this CAS.
        if slot
            .owner
            .compare_exchange(FREE, RESERVED, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            self.inflight.fetch_sub(1, Ordering::AcqRel);
            return Err(ClaimError::SlotBusy); // seq burned; gaps are harmless
        }
        // Phase 2: metadata, invisible to readers while RESERVED.
        slot.user_data.store(user_data, Ordering::Relaxed);
        slot.deadline_ns.store(deadline_ns, Ordering::Relaxed);
        slot.kind.store(kind as u8, Ordering::Relaxed);
        // Phase 3: publish.
        slot.owner.store(seq + 1, Ordering::Release);
        Ok(seq)
    }

    pub(crate) fn release(&self, seq: u64) -> bool {
        let slot = &self.slots[(seq as usize) & self.mask];
        // Joins the single-CAS completion protocol (invariant 3): only the winner
        // of `seq+1 -> FREE` may decrement the window — release racing a
        // concurrent sweep/drain_abort must not double-complete.
        if slot
            .owner
            .compare_exchange(seq + 1, FREE, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            self.inflight.fetch_sub(1, Ordering::AcqRel);
            true
        } else {
            false
        }
    }

    pub(crate) fn resolve(&self, wire_seq: u32, expect_kind: Option<ReqKind>) -> Resolve {
        let slot = &self.slots[(wire_seq as usize) & self.mask];
        let owner = slot.owner.load(Ordering::Acquire);
        if owner == FREE || owner == RESERVED {
            return Resolve::Miss;
        }
        let seq = owner - 1;
        if seq as u32 != wire_seq {
            return Resolve::Miss; // stale generation
        }
        if let Some(expect) = expect_kind
            && slot.kind.load(Ordering::Relaxed) != expect as u8
        {
            return Resolve::KindMismatch; // leave the slot for the real answer
        }
        let user_data = slot.user_data.load(Ordering::Relaxed);
        if slot
            .owner
            .compare_exchange(owner, FREE, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Resolve::Miss; // lost the race to sweep/another delivery
        }
        self.inflight.fetch_sub(1, Ordering::AcqRel);
        Resolve::Won { user_data }
    }

    pub(crate) fn sweep(&self, now_ns: u64, mut cb: impl FnMut(u64)) {
        for slot in self.slots.iter() {
            let owner = slot.owner.load(Ordering::Acquire);
            if owner == FREE || owner == RESERVED {
                continue;
            }
            if slot.deadline_ns.load(Ordering::Relaxed) > now_ns {
                continue;
            }
            let user_data = slot.user_data.load(Ordering::Relaxed);
            if slot
                .owner
                .compare_exchange(owner, FREE, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.inflight.fetch_sub(1, Ordering::AcqRel);
                cb(user_data);
            }
        }
    }

    /// Drain all live slots on shutdown.
    ///
    /// drain_abort is exhaustive only if the caller has stopped issuing claims first;
    /// a claim racing the drain is backstopped by the deadline sweep, so pollers must
    /// keep sweeping after a drain unless claims are provably quiesced.
    pub(crate) fn drain_abort(&self, mut cb: impl FnMut(u64)) {
        for slot in self.slots.iter() {
            let owner = slot.owner.load(Ordering::Acquire);
            if owner == FREE || owner == RESERVED {
                continue;
            }
            let user_data = slot.user_data.load(Ordering::Relaxed);
            if slot
                .owner
                .compare_exchange(owner, FREE, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.inflight.fetch_sub(1, Ordering::AcqRel);
                cb(user_data);
            }
        }
    }

    pub(crate) fn inflight(&self) -> u64 {
        self.inflight.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn set_next_seq_for_tests(&self, v: u64) {
        self.next_seq.store(v, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn slot_count(&self) -> usize {
        self.slots.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claim_resolve_roundtrip_returns_user_data_and_decrements_inflight() {
        let t = SlotTable::new(8, 0);
        let seq = t.claim(0xAB, ReqKind::Submit, u64::MAX).unwrap();
        assert_eq!(t.inflight(), 1);
        match t.resolve(seq as u32, Some(ReqKind::Submit)) {
            Resolve::Won { user_data } => assert_eq!(user_data, 0xAB),
            other => panic!("expected Won, got {other:?}"),
        }
        assert_eq!(t.inflight(), 0);
    }

    #[test]
    fn second_resolve_is_a_miss_exactly_once() {
        let t = SlotTable::new(8, 0);
        let seq = t.claim(1, ReqKind::Submit, u64::MAX).unwrap();
        assert!(matches!(t.resolve(seq as u32, None), Resolve::Won { .. }));
        assert_eq!(t.resolve(seq as u32, None), Resolve::Miss, "duplicate must not double-complete");
    }

    #[test]
    fn kind_mismatch_leaves_the_slot_for_the_real_answer() {
        // T14 semantics moved down from matcher.rs: a query-flagged delivery
        // must not satisfy a Submit claim; the slot survives for the real one.
        let t = SlotTable::new(8, 0);
        let seq = t.claim(2, ReqKind::Submit, u64::MAX).unwrap();
        assert_eq!(t.resolve(seq as u32, Some(ReqKind::Query)), Resolve::KindMismatch);
        assert_eq!(t.inflight(), 1, "slot must survive a kind mismatch");
        assert!(matches!(t.resolve(seq as u32, Some(ReqKind::Submit)), Resolve::Won { .. }));
    }

    #[test]
    fn window_full_refuses_and_releases_cleanly() {
        let t = SlotTable::new(2, 0);
        let a = t.claim(1, ReqKind::Submit, u64::MAX).unwrap();
        let _b = t.claim(2, ReqKind::Submit, u64::MAX).unwrap();
        assert_eq!(t.claim(3, ReqKind::Submit, u64::MAX), Err(ClaimError::WindowFull));
        t.release(a); // failed ring write path
        assert_eq!(t.inflight(), 1);
        t.claim(4, ReqKind::Submit, u64::MAX).expect("window reopened by release");
    }

    #[test]
    fn stuck_slot_a_table_length_later_is_slot_busy_not_corruption() {
        let t = SlotTable::new(4, 0); // slot count = next_pow2(4)*2 = 8
        let stuck = t.claim(7, ReqKind::Submit, u64::MAX).unwrap();
        // Force the sequence to wrap the table back onto `stuck`'s slot.
        t.set_next_seq_for_tests(stuck + t.slot_count() as u64);
        assert_eq!(t.claim(8, ReqKind::Submit, u64::MAX), Err(ClaimError::SlotBusy));
        // The stuck occupant is untouched and still resolvable.
        assert!(matches!(t.resolve(stuck as u32, None), Resolve::Won { user_data: 7 }));
    }

    #[test]
    fn sweep_expires_only_past_deadline() {
        let t = SlotTable::new(8, 0);
        let _early = t.claim(1, ReqKind::Submit, 100).unwrap();
        let late = t.claim(2, ReqKind::Submit, 10_000).unwrap();
        let mut expired = Vec::new();
        t.sweep(5_000, |ud| expired.push(ud));
        assert_eq!(expired, vec![1]);
        assert_eq!(t.inflight(), 1);
        assert!(matches!(t.resolve(late as u32, None), Resolve::Won { user_data: 2 }));
    }

    #[test]
    fn drain_abort_hands_back_every_live_user_data() {
        let t = SlotTable::new(8, 0);
        t.claim(10, ReqKind::Submit, u64::MAX).unwrap();
        t.claim(11, ReqKind::Query, u64::MAX).unwrap();
        let mut got = Vec::new();
        t.drain_abort(|ud| got.push(ud));
        got.sort_unstable();
        assert_eq!(got, vec![10, 11]);
        assert_eq!(t.inflight(), 0);
    }

    #[test]
    fn wire_seq_wraps_u32_without_confusion() {
        // Start near the u32 boundary; run claims/resolves ACROSS the wrap.
        let t = SlotTable::new(8, u32::MAX as u64 - 4);
        for i in 0..16u64 {
            let seq = t.claim(i, ReqKind::Submit, u64::MAX).unwrap();
            match t.resolve(seq as u32, Some(ReqKind::Submit)) {
                Resolve::Won { user_data } => assert_eq!(user_data, i),
                other => panic!("wrap iteration {i}: {other:?}"),
            }
        }
        assert_eq!(t.inflight(), 0);
    }

    #[test]
    fn concurrent_exactly_once_stress() {
        use std::sync::atomic::{AtomicU32, AtomicU64 as StdAtomicU64, AtomicBool, Ordering as AtomicOrdering};
        use std::sync::Arc;

        const THREADS: u64 = 4;
        const ITERATIONS: u64 = 10_000;
        const TOTAL_CLAIMS: u64 = THREADS * ITERATIONS;

        // Small window to force contention and wrapping.
        let table = Arc::new(SlotTable::new(64, 0));

        // Track completions AND releases per user_data (one AtomicU32 per possible user_data).
        // Encoding: completions[ud] = resolve/sweep count, released[ud] = release success count.
        let completions: Arc<Vec<AtomicU32>> =
            Arc::new((0..TOTAL_CLAIMS as usize).map(|_| AtomicU32::new(0)).collect());
        let released: Arc<Vec<AtomicU32>> =
            Arc::new((0..TOTAL_CLAIMS as usize).map(|_| AtomicU32::new(0)).collect());
        // Track which user_data values were successfully claimed.
        let claimed: Arc<Vec<AtomicU32>> =
            Arc::new((0..TOTAL_CLAIMS as usize).map(|_| AtomicU32::new(0)).collect());

        // Count total claims accepted.
        let claims_accepted = Arc::new(StdAtomicU64::new(0));

        // Signal that all claimers are done.
        let claimers_done = Arc::new(AtomicBool::new(false));

        // Simple LCG for deterministic random without rand dep.
        let lcg = |seed: &mut u64| -> bool {
            *seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
            (*seed >> 32) & 1 == 0
        };

        std::thread::scope(|s| {
            // Spawn 4 claimer threads, each with disjoint user_data range.
            let mut claimer_handles = vec![];
            for thread_id in 0..THREADS {
                let table = Arc::clone(&table);
                let completions = Arc::clone(&completions);
                let released = Arc::clone(&released);
                let claimed = Arc::clone(&claimed);
                let claims_accepted = Arc::clone(&claims_accepted);

                let handle = s.spawn(move || {
                    let mut seed = (thread_id + 1) * 12345;
                    for iteration in 0..ITERATIONS {
                        // Each claim has a unique user_data: thread_id * ITERATIONS + iteration.
                        let user_data = thread_id * ITERATIONS + iteration;
                        match table.claim(user_data, ReqKind::Submit, u64::MAX) {
                            Ok(seq) => {
                                claims_accepted.fetch_add(1, AtomicOrdering::Release);
                                claimed[user_data as usize].store(1, AtomicOrdering::Release);
                                // Randomly resolve or release.
                                if lcg(&mut seed) {
                                    // Try to resolve it ourselves.
                                    if let Resolve::Won { user_data: ud } = table.resolve(seq as u32, None) {
                                        completions[ud as usize].fetch_add(1, AtomicOrdering::Relaxed);
                                    }
                                    // If resolve lost to sweep, sweep will count the completion.
                                } else {
                                    // Try to release. Track whether we won the CAS.
                                    if table.release(seq) {
                                        released[user_data as usize].fetch_add(1, AtomicOrdering::Relaxed);
                                    }
                                    // If release lost to sweep, sweep already counted the completion.
                                }
                            }
                            Err(_) => {
                                // Window full, skip this iteration.
                            }
                        }
                    }
                });
                claimer_handles.push(handle);
            }

            // Spawn 1 sweeper thread.
            let sweeper_handle = {
                let table = Arc::clone(&table);
                let completions = Arc::clone(&completions);
                let claimers_done = Arc::clone(&claimers_done);

                s.spawn(move || {
                    let mut sweeper_iterations = 0;
                    const MAX_SWEEPER_ITERS: u64 = 10_000_000;

                    // Keep sweeping until no more live slots.
                    loop {
                        let mut had_live = false;
                        table.sweep(u64::MAX, |ud| {
                            had_live = true;
                            completions[ud as usize].fetch_add(1, AtomicOrdering::Relaxed);
                        });

                        sweeper_iterations += 1;
                        if sweeper_iterations > MAX_SWEEPER_ITERS {
                            panic!("sweeper failed to quiesce — inflight accounting bug?");
                        }

                        if !had_live && table.inflight() == 0 && claimers_done.load(AtomicOrdering::Acquire) {
                            break;
                        }
                        // Yield to let other threads progress.
                        std::thread::yield_now();
                    }
                })
            };

            // Wait for all claimers.
            for handle in claimer_handles {
                handle.join().unwrap();
            }
            claimers_done.store(true, AtomicOrdering::Release);
            sweeper_handle.join().unwrap();
        });

        // Verify exactly-once semantics: every claimed user_data completed exactly once.
        let mut total_completions = 0u64;
        let mut total_released = 0u64;
        for ud in 0..TOTAL_CLAIMS as usize {
            if claimed[ud].load(AtomicOrdering::Acquire) == 1 {
                let comp_count = completions[ud].load(AtomicOrdering::Relaxed);
                let rel_count = released[ud].load(AtomicOrdering::Relaxed);
                total_completions += comp_count as u64;
                total_released += rel_count as u64;
                // For each claimed request, exactly one of the three paths won:
                // resolve (by claimer), sweep (by sweeper), or release (by claimer).
                assert_eq!(
                    comp_count + rel_count, 1,
                    "claimed user_data {}: completions={} + released={} must == 1 (exactly once)",
                    ud, comp_count, rel_count
                );
            }
        }

        let claims = claims_accepted.load(AtomicOrdering::Acquire);
        assert_eq!(
            claims,
            total_completions + total_released,
            "total accepted claims {} must equal total completions {} + releases {}",
            claims, total_completions, total_released
        );
        assert_eq!(table.inflight(), 0, "inflight must be 0 at end");
    }
}
