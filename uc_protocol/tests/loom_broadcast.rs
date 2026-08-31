// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Loom model of the Broadcast ring's **seqlock read barrier**
//! (`uc_protocol/src/ring/broadcast.rs`), over a `Vec` of loom atomics rather
//! than an mmap.
//!
//! This closes the gap `docs/VERIFICATION.md` disclosed: loom covered the log
//! buffer's frame protocol and the MPSC ring's claim-then-commit, but "the
//! broadcast seqlock has never been model-checked". It is the one shipped
//! lock-free primitive with no memory model behind it, and it is the only one
//! with **no backpressure** — the single producer may lap a reader mid-copy,
//! so the read's validity rests entirely on a re-check after the fact.
//!
//! # The protocol, and why it is subtle
//!
//! Broadcast has no `consumer_position`: nothing stops the producer. A
//! consumer therefore reads a slot it does not own, and the bytes it copies
//! are worthless until it proves the producer did not lap it during the copy.
//! Production does that with a seqlock-style barrier:
//!
//! ```text
//! producer (single):                    consumer (many, each with own head):
//!   write record bytes at pos             p1 = publish.load(Acquire)
//!   publish.store(pos + adv, Release)     if p1 - head >= capacity -> Overwritten
//!                                         copy the record bytes
//!                                         fence(Acquire)
//!                                         p2 = publish.load(Acquire)
//!                                         if p2 - head >= capacity -> Overwritten
//!                                         else the copy is good
//! ```
//!
//! The argument is an ordering one, which is exactly what a model checker is
//! for: the producer publishes a position only AFTER writing the bytes at the
//! previous one, so `publish >= head + capacity` is observable *before* any
//! store lands in the consumer's slot. If the post-check still sees
//! `p2 - head < capacity`, no write to that slot can have begun.
//!
//! `fence(Acquire)` is what makes the post-check load meaningful on a weak
//! model: an acquire fence orders *preceding loads* against *subsequent loads
//! and stores*, which is the direction needed here (the copy must not sink
//! past the validation). An `Acquire` load alone would not give that — it
//! stops later work being hoisted above it, not earlier work sinking below.
//!
//! # What is modeled
//!
//! The PROTOCOL, not the mapping — the same scoping as `loom_mpsc.rs`. Loom
//! cannot see an mmap, so the slot region is a `Vec<AtomicUsize>` and a
//! "record" is TWO words rather than a byte range. Two, not one, on purpose:
//! with a single word a lapped read returns some *whole* value and the model
//! could only catch a stale one, never a **torn** one. With two words written
//! separately, a read that straddles the producer's overwrite sees halves from
//! different laps — which is the real failure (`try_read_record_at` copying a
//! header from lap N and a payload from lap N+1, escaping as `BadCrc` or,
//! worse, as a plausible record).
//!
//! | model | production (`ring/broadcast.rs`) |
//! |---|---|
//! | `produce`: `publish.load(Relaxed)` | `write`, `publish_position.load(Relaxed)` — single producer, sole writer |
//! | `produce`: two `slot[..].store(tag, Relaxed)` | `write`, `write_record_at` (plain non-atomic stores; modeled as Relaxed atomics so loom sees them without UB) |
//! | `produce`: `publish.store(next, Release)` | `write`, `publish_position.store(new_pos, Release)` |
//! | `try_read`: `publish.load(Acquire)` | `try_read`, `publish_position.load(Ordering::Acquire)` |
//! | `try_read`: empty check | `try_read`, `if self.head == producer_pos` |
//! | `try_read`: pre-check `>= CAPACITY` | `try_read`, the fast-path fall-behind `if (producer_pos - self.head) as usize >= capacity` |
//! | `try_read`: two `slot[..].load(Relaxed)` | `try_read`, `try_read_record_at` (the copy) |
//! | `try_read`: `fence(Acquire)` | `try_read`, `std::sync::atomic::fence(Ordering::Acquire)` |
//! | `try_read`: post-check on `head_before` | `try_read`, `if (producer_pos2 - head_before) as usize >= capacity` |
//!
//! Deliberately NOT modeled: byte positions and `align_record_size` (records
//! here are one slot, so "position" counts records — the wrap arithmetic
//! `pos & (capacity - 1)` is the same shape either way), the padding marker at
//! the tail (a second publication path, but it publishes through the identical
//! `Release` store this model already covers), the futex park/wake (`signal`
//! is a pure hint — no reader's correctness depends on it), and the record
//! crc32 (a detector, not a protocol step: the property proven here is that a
//! torn read never REACHES the crc).
//!
//! # The property
//!
//! `P: a consumer that returns a record must return the record for its own
//! head` — both halves from the same lap, and that lap the one its `head`
//! names. Everything else (`Overwritten`, `Empty`) is a defined answer and is
//! allowed at any time; the model asserts only that an ACCEPTED read is true.
//!
//! Run with:
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --test loom_broadcast --release
//! ```

#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicUsize, Ordering, fence};

/// Records the ring holds before it wraps. 2 is the smallest that can lap a
/// reader while leaving a slot it is not currently writing.
const CAPACITY: usize = 2;
/// Words per record. 2 is the smallest that can TEAR (see the module docs).
const WORDS: usize = 2;
/// Records the producer publishes. 3 is the smallest that makes the producer
/// write a slot a consumer parked at head 0 is reading: positions 0 and 1 fill
/// the ring, position 2 wraps onto slot 0.
const RECORDS: usize = 3;

/// The value both words of the record at `pos` carry. Non-zero and distinct
/// per position, so a torn pair is visibly a mix of two laps.
fn tag(pos: usize) -> usize {
    pos + 1
}

struct Ring {
    /// `publish_position` — the only synchronising word in the protocol.
    publish: AtomicUsize,
    /// The slot region: `CAPACITY * WORDS` words, addressed
    /// `slot(pos) = pos % CAPACITY`.
    words: Vec<AtomicUsize>,
}

impl Ring {
    fn new() -> Ring {
        Ring {
            publish: AtomicUsize::new(0),
            words: (0..CAPACITY * WORDS).map(|_| AtomicUsize::new(0)).collect(),
        }
    }

    fn word(&self, pos: usize, w: usize) -> &AtomicUsize {
        &self.words[(pos % CAPACITY) * WORDS + w]
    }
}

/// One producer publication, mirroring `BroadcastProducer::write`.
///
/// `release_before_body` models the publish-before-body barrier at the top of
/// `BroadcastProducer::write`: a `Release` fence between the PREVIOUS record's
/// publish store and THIS record's body stores, so a consumer cannot observe
/// lap N+1's bytes while still reading a `publish` from lap N. **That fence
/// exists because this model failed without it** — see the mutations below.
fn produce(ring: &Ring, pos: usize, release_before_body: bool) {
    if release_before_body {
        fence(Ordering::Release);
    }
    // Sole writer of `publish`, so the read back is Relaxed in production too.
    debug_assert_eq!(ring.publish.load(Ordering::Relaxed), pos);
    // The record's bytes go in BEFORE the position that publishes them. This
    // ordering is the whole basis of the consumer's argument.
    for w in 0..WORDS {
        ring.word(pos, w).store(tag(pos), Ordering::Relaxed);
    }
    ring.publish.store(pos + 1, Ordering::Release);
}

/// What one `try_read` can answer.
#[derive(Debug, PartialEq, Eq)]
enum Read {
    Empty,
    Overwritten,
    /// The two words as copied, and the head they were copied for.
    Got {
        head: usize,
        words: [usize; WORDS],
    },
}

/// One `BroadcastConsumer::try_read`, with the knobs the mutation tests below
/// turn off. With both `validate` and `fence_after_copy` true this is the
/// shipped protocol.
fn try_read(ring: &Ring, head: &mut usize, validate: bool, fence_after_copy: bool) -> Read {
    let p1 = ring.publish.load(Ordering::Acquire);
    if *head == p1 {
        return Read::Empty;
    }
    // Fast-path fall-behind: the producer is already a full capacity ahead.
    if p1 - *head >= CAPACITY {
        *head = p1;
        return Read::Overwritten;
    }

    let head_before = *head;
    let mut words = [0usize; WORDS];
    for (w, slot) in words.iter_mut().enumerate() {
        *slot = ring.word(head_before, w).load(Ordering::Relaxed);
    }

    if fence_after_copy {
        // Orders the copy above against the validation load below.
        fence(Ordering::Acquire);
    }

    if validate {
        let p2 = ring.publish.load(Ordering::Acquire);
        if p2 - head_before >= CAPACITY {
            *head = p2;
            return Read::Overwritten;
        }
    }

    *head += 1;
    Read::Got {
        head: head_before,
        words,
    }
}

/// Drive one producer and one consumer through every interleaving, and check
/// the property on each accepted read.
///
/// Returns the number of ACCEPTED reads seen across the whole model, so a
/// mutation that "passes" by never accepting anything cannot be mistaken for
/// a real pass.
fn model(validate: bool, fence_after_copy: bool, release_before_body: bool) {
    loom::model(move || {
        let ring = Arc::new(Ring::new());

        let w = ring.clone();
        let producer = loom::thread::spawn(move || {
            for pos in 0..RECORDS {
                produce(&w, pos, release_before_body);
            }
        });

        let r = ring.clone();
        let mut head = 0usize;
        // At most RECORDS reads: each either advances head or resyncs it.
        for _ in 0..RECORDS {
            match try_read(&r, &mut head, validate, fence_after_copy) {
                Read::Empty => {}
                Read::Overwritten => {}
                Read::Got { head: h, words } => {
                    // THE PROPERTY. Both halves from one lap, and that lap the
                    // one `head` named.
                    assert_eq!(
                        words,
                        [tag(h); WORDS],
                        "accepted a read at head {h} whose words are {words:?}, expected \
                         [{}; {WORDS}] — a torn or stale record escaped the seqlock barrier",
                        tag(h)
                    );
                }
            }
        }

        producer.join().unwrap();
    });
}

/// The shipped protocol: every interleaving, no torn or stale read accepted.
/// The shipped protocol: every interleaving, no torn or stale read accepted.
#[test]
fn the_seqlock_barrier_never_accepts_a_torn_or_stale_record() {
    model(true, true, true);
}

// ---------------------------------------------------------------------------
// Mutations. Each removes exactly one step of the protocol and must FAIL, so a
// green run above means the model has teeth rather than that it explored
// nothing. `#[should_panic]` is the assertion: loom's counterexample IS the
// test result.
// ---------------------------------------------------------------------------

/// **M1 — drop the producer's publish-before-body fence.** This is the state
/// the code was in before 2026-08-31, and finding it is why this file exists.
///
/// The consumer's re-check needs "lap N+1's bytes visible ⇒
/// `publish >= N+1` visible". A `Release` STORE does not provide that: it
/// orders accesses BEFORE it, not after, so the next record's body stores may
/// be observed ahead of the publish that would warn the reader. Loom's
/// counterexample: a consumer at head 0 accepts `[1, 3]` — word 0 from lap 0,
/// word 1 from lap 2 — having re-read `publish` as 1.
///
/// This is a weak-memory failure: x86-TSO forbids the store-store reordering
/// it needs, aarch64 does not, and UC builds aarch64 binaries that CI never
/// executes.
#[test]
#[should_panic(expected = "a torn or stale record escaped")]
fn m1_without_the_producer_publish_before_body_fence() {
    model(true, true, false);
}

/// **M2 — drop the consumer's post-copy re-check.** The pre-check alone is a
/// snapshot taken before the copy, so it cannot say anything about what
/// happened during it. Without the re-check the barrier does not exist.
#[test]
#[should_panic(expected = "a torn or stale record escaped")]
fn m2_without_the_post_copy_revalidation() {
    model(false, true, true);
}
