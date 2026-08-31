// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Loom model of the MPSC ring's per-record commit protocol (M13a, spec
//! §4.1/§4.2), over a `Vec` of loom atomics rather than an mmap.
//!
//! This is the "loom on the rings" item the M12d security package named
//! (`docs/security/self-assessment.md`), cashed in on the ring whose
//! publication protocol M13a rewrote.
//!
//! # What is modeled
//!
//! The PROTOCOL, not the mapping. Loom cannot see an mmap (the same wall Miri
//! hits — `docs/VERIFICATION.md`), so the model replaces the slot region with
//! a `Vec<AtomicU32>` of commit words plus a parallel `Vec<AtomicU32>` of
//! one-word record "bodies", and drives them through exactly the steps
//! `ring::mpsc` takes, in exactly the orderings it uses. The bit-packing and
//! the consumer's slot decision are NOT re-implemented here: the model calls
//! the production `encode_commit_word` / `classify_commit_word` / `lap_of`
//! directly, so a change to the word layout changes the model with it.
//!
//! **Kept in step with the M13a final review's Minor 1**: both
//! `compare_exchange` sites take `Relaxed` on FAILURE, not `Acquire`, because
//! neither reads anything on that path (the producer discards the returned
//! word and reports `Skipped`; the consumer `continue`s and re-Acquire-loads
//! the word at the top of its loop). Production made that change to avoid a
//! needless `ldaxr` on aarch64; the model follows it exactly, so the weaker
//! failure edge is what loom actually explores.
//!
//! Line-by-line correspondence to `uc_protocol/src/ring/mpsc.rs`:
//!
//! | model                         | production                              |
//! |-------------------------------|-----------------------------------------|
//! | `Ring::claim` head load       | `claim`, `claim_position.load(Acquire)`  |
//! | `Ring::claim` consumer load   | `claim`, `consumer_position.load(Acquire)` (the `cached_consumer_pos` fast path is a strictly more conservative LOWER bound, so the model takes the reload branch unconditionally) |
//! | `Ring::claim` CAS             | `claim`, `claim_position.compare_exchange_weak(AcqRel/Relaxed)` |
//! | `Ring::claim` claim stamp     | `claim`, `store_commit_word(.., CLAIMED\|LAP\|advance, Relaxed)` |
//! | `Ring::claim` body store      | `claim`, `write_record_body_at` (plain stores; modeled Relaxed) |
//! | `Ring::commit` CAS            | `commit`, `cas_commit_word(expected = own claim word, Release/Relaxed)` |
//! | `Ring::commit` count bump     | `commit`, `publish_position.fetch_add(1, Release)` (the futex wake word) |
//! | `Ring::try_read` pos load     | `try_read`, `consumer_position.load(Relaxed)` (single reader) |
//! | `Ring::try_read` word load    | `try_read`, `load_commit_word` (Acquire)  |
//! | `Ring::try_read` skip CAS     | `try_read`, `cas_commit_word(word -> CLAIMED\|LAP\|0, AcqRel/Relaxed)` |
//! | `Ring::try_read` advance      | `try_read`, `consumer_position.store(.., Release)` |
//!
//! Deliberately NOT modeled: the tail-straddle padding path (the model's
//! records tile the ring exactly), the futex park/wake (`signal` is a pure
//! wakeup hint — no reader's correctness depends on it), the record's crc32,
//! and the wall clock. `hole_timeout` is a modeled boolean `timed_out`
//! parameter, not an `Instant`, so the model stays deterministic: loom
//! explores interleavings, not schedules.
//!
//! **The padding omission had teeth, once.** Writing this model is what
//! surfaced it: the padding marker was a second publication path, and until
//! the fix that landed with this file it published with an unconditional
//! `store_commit_word` rather than a commit CAS — so it sat OUTSIDE P4's and
//! P5's exactly-one-winner guarantee, in exactly the shape mutation M3 below
//! proves unsafe (a producer preempted in its claim→publish window, its hole
//! skip-marked, resuming to stomp a later claimant with nothing reported).
//! `MpscProducer::commit_padding` now uses the same CAS as
//! `MpscProducer::commit`, and `mpsc.rs`'s
//! `a_skipped_padding_marker_is_refused_not_stomped` pins it. Modelling the
//! padding path in loom stays out of scope — it would need a ring whose
//! records do not tile it, doubling P1's already-678k state space — but the
//! two publication paths now share one discipline, so P4/P5 speak for both.
//!
//! # Properties
//!
//! * **P1** `every_committed_record_is_delivered_exactly_once_in_claim_order`
//!   — every committed record is delivered exactly once, at the position its
//!   producer claimed, with that producer's own bytes (the Release commit CAS
//!   → Acquire word load pair; a torn read shows up as a body of 0).
//! * **P2** `a_stalled_producer_never_blocks_another_producers_commit` — a
//!   producer preempted between claim and commit blocks nobody: with its
//!   commit deferred, two other producers contending on `claim_position`
//!   still complete both their claims AND their commits, the consumer is
//!   head-of-line (never a torn or out-of-order delivery), and claim order is
//!   restored the moment the stalled producer commits.
//! * **P3** `consumer_position <= claim_position` at all times, and a claim
//!   never lands on a slot the consumer has not finished with. Asserted
//!   inside the model on every claim and every consumer advance, so every
//!   test above and below carries it.
//! * **P4** `a_skip_and_a_commit_race_have_exactly_one_winner` — for every
//!   interleaving of the consumer's skip-marker CAS and the producer's commit
//!   CAS on the SAME claim, exactly one wins: either the record is delivered
//!   exactly once and the producer sees `Ok`, or the record is never
//!   delivered and the producer sees `Skipped`. Never both, never neither.
//! * **P5** `a_later_claimant_overwrites_the_marker_and_is_delivered_normally`
//!   — after a skip, a later claimant's stamp overwrites the skip marker and
//!   its record is delivered normally, while the resurrected producer's
//!   commit is refused in every interleaving.
//!
//! # Mutation checks (performed, not assumed)
//!
//! A model that passes proves nothing until it is shown to fail on a wrong
//! protocol. Five mutations and two coverage probes were run against exactly
//! this file; each was reverted afterwards.
//!
//! **1. Commit ordering.** Relaxing the commit CAS's success ordering
//! ([`COMMIT_SUCCESS`]) from `Release` to `Relaxed` makes P1 FAIL, at
//! `check_positions`:
//!
//! ```text
//! P3: consumer_position 8 passed claim_position 0
//! ```
//!
//! — without the release edge the consumer can observe the committed word
//! while a later Acquire load of `claim_position` still returns the
//! pre-claim value.
//!
//! **1b.** The same mutation with `check_positions` temporarily stubbed to
//! `return`, so the run reaches the body assertion, gives the direct
//! visibility failure:
//!
//! ```text
//! assertion `left == right` failed: a committed record must carry its
//! producer's bytes: position 0 delivered body 0x0
//!   left: 0
//!  right: 10
//! ```
//!
//! The model finds an interleaving where the consumer observes the commit
//! word but not the body written before it. The `Release` on
//! `MpscProducer::commit`'s `cas_commit_word` is load-bearing, not decoration.
//!
//! **2. The consumer's skip marker as an unconditional store** (the design
//! before Task 4's fix round 1): replacing the marker `compare_exchange` with
//! `store(marker, Release)` makes P4 FAIL with exactly the "neither" outcome
//! — the producer's commit returned `Ok` and its record was never delivered,
//! i.e. silent loss:
//!
//! ```text
//! assertion `left == right` failed: the commit won: its record is delivered
//! exactly once, with its bytes
//!   left: []
//!  right: [(0, 10)]
//! ```
//!
//! **3. The producer's commit as an unconditional store** (the design before
//! Task 4): replacing the commit `compare_exchange` with
//! `store(committed, Release)` makes BOTH P4 and P5 FAIL — P4 with the same
//! silent-loss shape above, P5 with
//!
//! ```text
//! assertion `left == right` failed: a resurrected producer whose hole was
//! marked is always refused
//!   left: Ok(())
//!  right: Err(Skipped)
//! ```
//!
//! — a resurrected producer stomps a later claimant's slot and is never told.
//!
//! **4. The pre-M13a publication order** — the one mutation that targets P2,
//! whose headline claim ("blocks nobody") is otherwise unfalsifiable in a
//! model containing no wait construct. Reinstate the serialization M13a
//! removed: make `Ring::commit` wait until every earlier claim has published
//! before publishing its own. As a plain spin
//! (`while commit_count.load(Acquire) != claim.pos / ADVANCE { yield_now() }`)
//! loom reports the starvation:
//!
//! ```text
//! Model exceeded maximum number of branches. This is often caused by an
//! algorithm requiring the processor to make progress, e.g. spin locks.
//! ```
//!
//! Modelled instead as a `loom::sync::Mutex` + `Condvar` turnstile that each
//! commit hands on (`*turn += 1; notify_all()`), loom reports the deadlock
//! unambiguously, with A stalled and B, C and the main thread all blocked:
//!
//! ```text
//! deadlock; threads = [(Id(0), Blocked(Location(None))),
//!                      (Id(1), Blocked(Location(None))),
//!                      (Id(2), Blocked(Location(None)))]
//! ```
//!
//! P5 deadlocks under M4 too (its record behind the skipped hole can never
//! take its turn); P1 and P4 still pass, since every claim in them does
//! eventually commit. This is the convoy
//! `docs/notes/uc2-m13-mpsc-publish-convoy-explained.md` measured on the
//! fleet, reproduced as a memory-model fact.
//!
//! **Branch coverage of P4.** A two-outcome property is vacuous if only one
//! outcome is ever reached, so both were probed: asserting
//! `commit_result.is_ok()` inside P4 FAILS, and so does asserting
//! `commit_result.is_err()`. Loom reaches both winners.
//!
//! # Cost
//!
//! Exhaustive, no preemption bound, on a 2026 dev box under `--release`:
//! P1 678 244 iterations, P2 324, P4 54, P5 36; ~31 s for the four together.
//! P1 dominates because two producers race the claim CAS (loom explores
//! `compare_exchange_weak`'s spurious failure as a branch) while the consumer
//! runs. Keep the rings this small — a third record in P1 is not affordable.
//!
//! Run: RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --test loom_mpsc --release
#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use loom::thread;

use uc_protocol::ring::{SlotState, classify_commit_word, encode_commit_word, lap_of};

/// Every modeled record claims exactly this much. The production `advance` is
/// `align_record_size(total)`, a multiple of `RECORD_ALIGN = 8`; 8 is the
/// smallest legal value and makes records tile the ring exactly (no
/// tail-straddle padding, which the model deliberately omits).
const ADVANCE: usize = 8;
/// Two slots: the smallest ring that holds two producers' records without
/// either waiting for the consumer, and small enough that the model laps
/// after three records.
const TWO_SLOTS: usize = 2 * ADVANCE;
/// Four slots — room for a stalled claim plus two contending producers behind
/// it (P2).
const FOUR_SLOTS: usize = 4 * ADVANCE;

/// The commit CAS's success ordering, hoisted to a constant so the mutation
/// check documented in the module doc is a one-line edit. Production:
/// `MpscProducer::commit`'s `cas_commit_word(.., Ordering::Release, ..)`.
const COMMIT_SUCCESS: Ordering = Ordering::Release;

/// The subset of `RingError` the model can produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelError {
    /// `RingError::Full`.
    Full,
    /// `RingError::Skipped` — the commit CAS lost.
    Skipped,
    /// `RingError::Corrupt` — a claim word whose `advance` fails the
    /// consumer's bounds check.
    Corrupt,
    /// `RingError::Wedged` — an unsized hole past the timeout.
    Wedged,
}

/// The model's `PendingClaim`: what `claim` hands to `commit`.
#[derive(Debug, Clone, Copy)]
struct Claim {
    pos: u64,
    lap: u32,
}

/// What one `try_read` returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Read {
    /// `Ok(None)`: empty, or head-of-line behind a claim.
    Nothing,
    /// `Ok(Some(record))`, carrying the position it was delivered at and the
    /// one-word body.
    Record { pos: u64, body: u32 },
}

struct Ring {
    /// Ring capacity in bytes; a power of two, as `MpscRing::create`
    /// requires. A field, not a constant, because production carries it in
    /// `RingHeader::capacity_bytes` and every wrap/lap computation reads it
    /// from there.
    capacity: usize,
    /// One commit word per slot — the first word of each production slot.
    words: Vec<AtomicU32>,
    /// One-word stand-in for the record body `write_record_body_at` writes
    /// between the claim stamp and the commit.
    bodies: Vec<AtomicU32>,
    claim_position: AtomicU64,
    consumer_position: AtomicU64,
    /// `publish_position` reinterpreted as the commit count (M13a module doc).
    commit_count: AtomicU64,
    /// `MpscConsumer::holes_skipped`. A plain field in production (single
    /// consumer); an atomic here only so a consumer running on a spawned
    /// thread can be inspected after the join.
    holes_skipped: AtomicU64,
}

impl Ring {
    fn new(capacity: usize) -> Ring {
        assert!(capacity.is_power_of_two() && capacity >= ADVANCE);
        let slots = capacity / ADVANCE;
        Ring {
            capacity,
            words: (0..slots).map(|_| AtomicU32::new(0)).collect(),
            bodies: (0..slots).map(|_| AtomicU32::new(0)).collect(),
            claim_position: AtomicU64::new(0),
            consumer_position: AtomicU64::new(0),
            commit_count: AtomicU64::new(0),
            holes_skipped: AtomicU64::new(0),
        }
    }

    fn slots(&self) -> usize {
        self.capacity / ADVANCE
    }

    /// `(pos as usize) & (capacity - 1)` then divided by the record size —
    /// production indexes bytes, the model indexes slots.
    fn slot_of(&self, pos: u64) -> usize {
        ((pos as usize) & (self.capacity - 1)) / ADVANCE
    }

    /// `lap_of(pos, capacity)`, the production helper.
    fn lap(&self, pos: u64) -> u32 {
        lap_of(pos, self.capacity)
    }

    /// **P3**, both halves. `consumer_position <= claim_position` always, and
    /// the claim frontier never runs more than one ring ahead of the consumer
    /// — i.e. a claim never lands on a slot holding a record the consumer has
    /// not finished with.
    ///
    /// Both loads are deliberately taken AFTER the event being checked: both
    /// counters only ever increase, so a freshly loaded `claim_position` is
    /// ≥ the value that mattered and a freshly loaded `consumer_position` is
    /// ≥ the value the claim was admitted against. Each assertion is
    /// therefore conservative in the direction that keeps it sound — a
    /// failure is a real violation, never an artifact of when the snapshot
    /// was taken.
    fn check_positions(&self) {
        let consumer = self.consumer_position.load(Ordering::Acquire);
        let claim = self.claim_position.load(Ordering::Acquire);
        assert!(
            consumer <= claim,
            "P3: consumer_position {consumer} passed claim_position {claim}"
        );
        assert!(
            claim - consumer <= self.capacity as u64,
            "P3: the claim frontier ({claim}) ran more than one ring ({}) ahead of \
             the consumer ({consumer}) — a claim overwrote an unconsumed slot",
            self.capacity
        );
    }

    /// `MpscProducer::claim`: CAS a slot off `claim_position` bounded by an
    /// Acquire load of `consumer_position`, stamp `CLAIMED | LAP | advance`
    /// (Relaxed), write the body, return without waiting for anyone.
    fn claim(&self, body: u32) -> Result<Claim, ModelError> {
        assert_ne!(body, 0, "0 is the model's 'body not yet visible' sentinel");
        loop {
            let claim_pos = self.claim_position.load(Ordering::Acquire);
            // Production reads its per-producer cache first and only reloads
            // when the cache says "full". The cache is a LOWER bound on the
            // real value, so it can only under-estimate free space; taking
            // the reload unconditionally models the same admission decision
            // with the same or more room, never less.
            let consumer_pos = self.consumer_position.load(Ordering::Acquire);
            let free = self
                .capacity
                .saturating_sub(claim_pos.saturating_sub(consumer_pos) as usize);
            if free < ADVANCE {
                return Err(ModelError::Full);
            }
            if self
                .claim_position
                .compare_exchange_weak(
                    claim_pos,
                    claim_pos + ADVANCE as u64,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                continue; // raced another producer (or a spurious weak failure)
            }
            // We own `[claim_pos, claim_pos + ADVANCE)`.
            self.check_positions();
            let lap = self.lap(claim_pos);
            let slot = self.slot_of(claim_pos);
            // The claim stamp lands BEFORE the body: it is what sizes a dead
            // producer's hole. Relaxed is enough — a consumer that observes
            // it only ever decides "not yet" (or, past the timeout, skips).
            self.words[slot].store(
                encode_commit_word(lap, ADVANCE as u32, true),
                Ordering::Relaxed,
            );
            // `write_record_body_at`: plain stores, ordered only by the
            // commit's Release below.
            self.bodies[slot].store(body, Ordering::Relaxed);
            return Ok(Claim {
                pos: claim_pos,
                lap,
            });
        }
    }

    /// `MpscProducer::commit`: `compare_exchange` the slot's word from our
    /// OWN claim word to the committed word. Release on success (this is the
    /// edge that publishes the body), `Relaxed` on failure — nothing is read
    /// on the failure path, so there is nothing for an `Acquire` to order
    /// (final review, Minor 1). A failure means the consumer marked us
    /// skipped, or a later claimant owns these bytes now — either way we
    /// touch nothing and report `Skipped`.
    fn commit(&self, claim: Claim) -> Result<(), ModelError> {
        let slot = self.slot_of(claim.pos);
        let expected = encode_commit_word(claim.lap, ADVANCE as u32, true);
        let committed = encode_commit_word(claim.lap, ADVANCE as u32, false);
        if self.words[slot]
            .compare_exchange(expected, committed, COMMIT_SUCCESS, Ordering::Relaxed)
            .is_err()
        {
            return Err(ModelError::Skipped);
        }
        // `publish_position` as a commit count: the futex wake word must
        // change on every commit. Nothing reads it as a position.
        self.commit_count.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// `MpscConsumer::try_read`. `timed_out` stands in for
    /// `hole_elapsed(consumer_pos)` — the modeled boolean replaces the clock
    /// so the model is deterministic.
    fn try_read(&self, timed_out: bool) -> Result<Read, ModelError> {
        loop {
            let consumer_pos = self.consumer_position.load(Ordering::Relaxed);
            let slot = self.slot_of(consumer_pos);
            let expected_lap = self.lap(consumer_pos);
            let word = self.words[slot].load(Ordering::Acquire);

            match classify_commit_word(word, expected_lap) {
                SlotState::Empty => {
                    if self.claim_position.load(Ordering::Acquire) <= consumer_pos {
                        return Ok(Read::Nothing);
                    }
                    // A claim landed but its word has not (or never will).
                    if timed_out {
                        return Err(ModelError::Wedged);
                    }
                    return Ok(Read::Nothing);
                }
                SlotState::Claimed { advance } => {
                    if !timed_out {
                        // Head-of-line behind exactly this producer.
                        return Ok(Read::Nothing);
                    }
                    // `advance` is data another process wrote; production
                    // bounds-checks it (aligned, non-zero, within the tail
                    // and `max_msg_size`) before acting on it. In the model
                    // every legal claim advances by exactly `ADVANCE`, and
                    // the skip marker's own `advance == 0` shape must never
                    // be seen AT `consumer_position`.
                    if advance as usize != ADVANCE {
                        return Err(ModelError::Corrupt);
                    }
                    // Mark the skip with a CAS from the EXACT word observed
                    // to `CLAIMED | LAP | 0` — the one write the otherwise
                    // read-only consumer makes into the slot region. If the
                    // producer committed in the window since the load, the
                    // CAS fails harmlessly: no skip, no count, re-classify.
                    let marker = encode_commit_word(expected_lap, 0, true);
                    assert_ne!(
                        marker,
                        encode_commit_word(expected_lap, ADVANCE as u32, true),
                        "the skip marker must never collide with a real claim word"
                    );
                    if self.words[slot]
                        .compare_exchange(word, marker, Ordering::AcqRel, Ordering::Relaxed)
                        .is_err()
                    {
                        continue;
                    }
                    self.holes_skipped.fetch_add(1, Ordering::Relaxed);
                    self.consumer_position
                        .store(consumer_pos + advance as u64, Ordering::Release);
                    self.check_positions();
                    continue;
                }
                SlotState::Committed { length } => {
                    assert_eq!(
                        length as usize, ADVANCE,
                        "commit word length out of range at position {consumer_pos}"
                    );
                    // The Acquire load of the commit word above
                    // synchronizes-with the producer's Release commit CAS,
                    // made after the body write.
                    let body = self.bodies[slot].load(Ordering::Relaxed);
                    self.consumer_position
                        .store(consumer_pos + ADVANCE as u64, Ordering::Release);
                    self.check_positions();
                    return Ok(Read::Record {
                        pos: consumer_pos,
                        body,
                    });
                }
            }
        }
    }

    /// One full `try_write`: claim then commit.
    fn try_write(&self, body: u32) -> Result<(), ModelError> {
        let claim = self.claim(body)?;
        self.commit(claim)
    }

    /// Read until the ring reports nothing, up to `budget` attempts (loom
    /// needs every loop bounded). Returns the records in delivery order.
    fn drain(&self, timed_out: bool, budget: usize) -> Vec<(u64, u32)> {
        let mut out = Vec::new();
        for _ in 0..budget {
            match self
                .try_read(timed_out)
                .expect("no wedge or corruption in this model")
            {
                Read::Nothing => break,
                Read::Record { pos, body } => out.push((pos, body)),
            }
        }
        out
    }
}

/// **P1** (and, via `check_positions`, **P3**). Two producers write
/// concurrently while the consumer runs. Every committed record is delivered
/// exactly once, at the position its producer claimed, carrying that
/// producer's bytes — a torn read would surface as body `0`.
///
/// Delivery is in claim order by construction (the consumer walks positions),
/// so what this pins is the harder half: the position→body binding survives
/// every interleaving.
#[test]
fn every_committed_record_is_delivered_exactly_once_in_claim_order() {
    loom::model(|| {
        let ring = Arc::new(Ring::new(TWO_SLOTS));

        let p1 = Arc::clone(&ring);
        let t1 = thread::spawn(move || {
            let claim = p1.claim(0xA).expect("producer 1 claims");
            p1.commit(claim).expect("producer 1 commits");
            claim.pos
        });
        let p2 = Arc::clone(&ring);
        let t2 = thread::spawn(move || {
            let claim = p2.claim(0xB).expect("producer 2 claims");
            p2.commit(claim).expect("producer 2 commits");
            claim.pos
        });

        // The consumer runs concurrently with both, bounded (loom needs
        // every loop bounded; a spin would not terminate). `timed_out =
        // false`: nothing here is a hole.
        let mut delivered = ring.drain(false, ring.slots());

        let pos_a = t1.join().unwrap();
        let pos_b = t2.join().unwrap();

        // Whatever the concurrent phase missed is drained now that both
        // records are committed.
        delivered.extend(ring.drain(false, ring.slots()));

        assert_ne!(
            pos_a, pos_b,
            "two producers must never claim the same position"
        );
        assert_eq!(
            ring.commit_count.load(Ordering::Acquire),
            2,
            "both commits bumped the wake word"
        );
        assert_eq!(
            ring.holes_skipped.load(Ordering::Relaxed),
            0,
            "no holes here"
        );

        // Exactly once, in claim order.
        let positions: Vec<u64> = delivered.iter().map(|(p, _)| *p).collect();
        let mut sorted = positions.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            positions, sorted,
            "delivery must follow claim order, no duplicates"
        );
        assert_eq!(
            delivered.len(),
            2,
            "both committed records delivered, exactly once"
        );

        // …with the claiming producer's own bytes.
        for (pos, body) in delivered {
            let want = if pos == pos_a { 0xA } else { 0xB };
            assert_eq!(
                body, want,
                "a committed record must carry its producer's bytes: position {pos} \
                 delivered body {body:#x}"
            );
        }
    });
}

/// **P2**. A producer preempted between claim and commit blocks nobody.
///
/// Producer A claims and stalls. Producers B and C — which genuinely contend
/// with each other on `claim_position`, so loom has real interleavings to
/// explore rather than two disjoint slots DPOR can collapse into one — must
/// each complete their claim AND their commit in every one of them. The joins
/// are the assertion: a thread that could not finish would leave this model
/// unable to make progress, which loom reports rather than hanging.
///
/// That the joins CAN fail is not taken on faith: mutation M4 in the module
/// doc reinstates the pre-M13a publication order and loom then reports
/// `deadlock; threads = [(Id(0), Blocked), (Id(1), Blocked), (Id(2),
/// Blocked)]` for this exact test. The model as committed contains no
/// wait/park construct anywhere, which is itself the point — the shipped
/// protocol has no step at which one producer waits for another.
///
/// Meanwhile the consumer is head-of-line behind A: it must report nothing at
/// all, however far B and C have got. When A finally commits, everything
/// drains in claim order, A first, each record carrying its own producer's
/// bytes.
#[test]
fn a_stalled_producer_never_blocks_another_producers_commit() {
    loom::model(|| {
        // Four slots: A's stalled claim plus room for both contenders.
        let ring = Arc::new(Ring::new(FOUR_SLOTS));

        // A claims and stalls — exactly the state a preemption (or a SIGKILL)
        // between the CAS and the commit leaves behind.
        let a = ring.claim(0xA).expect("A claims");
        assert_eq!(a.pos, 0);

        let pb = Arc::clone(&ring);
        let t_b = thread::spawn(move || {
            let claim = pb.claim(0xB).expect("B claims while A is stalled");
            pb.commit(claim).expect("B commits while A is stalled");
            claim.pos
        });
        let pc = Arc::clone(&ring);
        let t_c = thread::spawn(move || {
            let claim = pc.claim(0xC).expect("C claims while A is stalled");
            pc.commit(claim).expect("C commits while A is stalled");
            claim.pos
        });

        // Concurrently: the consumer must report nothing — head-of-line
        // behind A — no matter how far B and C have got.
        for _ in 0..2 {
            assert_eq!(
                ring.try_read(false).expect("no wedge"),
                Read::Nothing,
                "a claimed-but-uncommitted slot must never read as a record"
            );
        }

        let pos_b = t_b.join().unwrap();
        let pos_c = t_c.join().unwrap();
        assert_ne!(
            pos_b, pos_c,
            "two producers must never claim the same position"
        );
        assert_eq!(
            ring.holes_skipped.load(Ordering::Relaxed),
            0,
            "the timeout never elapsed"
        );
        assert_eq!(
            ring.commit_count.load(Ordering::Acquire),
            2,
            "B and C committed; A has not"
        );

        // Still head-of-line: both are committed, A is not.
        assert_eq!(ring.try_read(false).expect("no wedge"), Read::Nothing);

        // A commits. Now everything drains, A first.
        ring.commit(a).expect("A's commit is not refused");
        let delivered = ring.drain(false, ring.slots() + 1);
        let positions: Vec<u64> = delivered.iter().map(|(p, _)| *p).collect();
        assert_eq!(
            positions,
            vec![0, ADVANCE as u64, 2 * ADVANCE as u64],
            "claim order restored the moment the stalled producer commits"
        );
        for (pos, body) in delivered {
            let want = if pos == 0 {
                0xA
            } else if pos == pos_b {
                0xB
            } else {
                0xC
            };
            assert_eq!(
                body, want,
                "position {pos} must carry its own producer's bytes, saw {body:#x}"
            );
        }
    });
}

/// **P4**, the Task-4 skip/commit race. The consumer's skip-marker CAS and
/// the producer's commit CAS run concurrently on the same claim. For every
/// interleaving exactly one wins:
///
///   * producer `Ok`  → the record is delivered exactly once, no hole counted;
///   * producer `Skipped` → the record is never delivered, exactly one hole
///     counted, and the consumer has advanced past the claim.
///
/// Never both (a delivered record reported `Skipped` would let a caller retry
/// and double-apply it), never neither (a lost record with no `Skipped` would
/// be silent loss).
#[test]
fn a_skip_and_a_commit_race_have_exactly_one_winner() {
    loom::model(|| {
        let ring = Arc::new(Ring::new(TWO_SLOTS));
        let a = ring.claim(0xA).expect("A claims");
        assert_eq!(a.pos, 0);

        // The resurrected producer.
        let p = Arc::clone(&ring);
        let t_commit = thread::spawn(move || p.commit(a));
        // The consumer, past the hole timeout.
        let c = Arc::clone(&ring);
        let t_read = thread::spawn(move || c.try_read(true).expect("no wedge, no corruption"));

        let commit_result = t_commit.join().unwrap();
        let concurrent_read = t_read.join().unwrap();

        // Everything still readable after the race (the threads are joined,
        // so the single-consumer discipline holds).
        let mut delivered: Vec<(u64, u32)> = match concurrent_read {
            Read::Nothing => Vec::new(),
            Read::Record { pos, body } => vec![(pos, body)],
        };
        delivered.extend(ring.drain(true, ring.slots() + 1));

        let holes = ring.holes_skipped.load(Ordering::Relaxed);
        let consumer = ring.consumer_position.load(Ordering::Acquire);
        match commit_result {
            Ok(()) => {
                assert_eq!(
                    delivered,
                    vec![(0, 0xA)],
                    "the commit won: its record is delivered exactly once, with its bytes"
                );
                assert_eq!(holes, 0, "the commit won: nothing was skipped");
            }
            Err(ModelError::Skipped) => {
                assert!(
                    delivered.is_empty(),
                    "the skip won: the record must never be delivered, saw {delivered:?}"
                );
                assert_eq!(holes, 1, "the skip won: exactly one hole counted");
                assert_eq!(
                    consumer, ADVANCE as u64,
                    "the consumer advanced past the hole"
                );
            }
            Err(other) => panic!("commit can only return Ok or Skipped, got {other:?}"),
        }
        assert_eq!(
            ring.commit_count.load(Ordering::Acquire),
            u64::from(commit_result.is_ok()),
            "the wake word is bumped exactly when a commit lands"
        );
    });
}

/// **P5**. After a skip, a later claimant's stamp overwrites the marker and
/// its record is delivered normally — while the resurrected producer's commit
/// is refused in EVERY interleaving of the two (its expected claim word is
/// neither the marker nor the later claimant's word, so its CAS can never
/// succeed).
#[test]
fn a_later_claimant_overwrites_the_marker_and_is_delivered_normally() {
    loom::model(|| {
        let ring = Arc::new(Ring::new(TWO_SLOTS));

        // Sequential setup: A claims slot 0 and stalls; the consumer times
        // out and marks the skip; a second record fills slot 1 and is read,
        // leaving the consumer exactly one lap from A's slot.
        let a = ring.claim(0xA).expect("A claims");
        assert_eq!(
            ring.try_read(true).expect("no wedge"),
            Read::Nothing,
            "skips + marks the hole"
        );
        assert_eq!(ring.holes_skipped.load(Ordering::Relaxed), 1);
        assert_eq!(
            ring.words[0].load(Ordering::Acquire),
            encode_commit_word(0, 0, true),
            "the skip marker is what the consumer left in the slot"
        );
        ring.try_write(0xB).expect("a record behind the hole");
        assert_eq!(
            ring.drain(false, ring.slots()),
            vec![(ADVANCE as u64, 0xB)],
            "the record behind the hole is delivered"
        );
        assert_eq!(
            ring.consumer_position.load(Ordering::Acquire),
            TWO_SLOTS as u64
        );

        // Now race the resurrection against the later claimant that takes
        // A's exact slot on the next lap.
        let p = Arc::clone(&ring);
        let t_resurrect = thread::spawn(move || p.commit(a));
        let q = Arc::clone(&ring);
        let t_later = thread::spawn(move || q.try_write(0xC));

        assert_eq!(
            t_resurrect.join().unwrap(),
            Err(ModelError::Skipped),
            "a resurrected producer whose hole was marked is always refused"
        );
        t_later
            .join()
            .unwrap()
            .expect("the later claimant commits normally");

        assert_eq!(
            ring.drain(false, ring.slots()),
            vec![(TWO_SLOTS as u64, 0xC)],
            "the later claimant's record is delivered normally, with its own bytes"
        );
        assert_eq!(
            ring.holes_skipped.load(Ordering::Relaxed),
            1,
            "a refused resurrection is not a new hole"
        );
        assert_eq!(
            ring.commit_count.load(Ordering::Acquire),
            2,
            "B and C committed; A did not"
        );
    });
}
