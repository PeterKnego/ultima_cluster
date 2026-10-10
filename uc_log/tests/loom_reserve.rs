// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Loom model of the follower write-reserve seqlock (#78, `counters.rs`
//! `AppendLine::raise_reserve` / `buffer.rs` `write_extent`):
//!   writer (receiver): reserve := end (Relaxed) -> fence(Release) -> overwrite bytes
//!   reader (validated): pre-check reserve -> copy bytes -> fence(Acquire)
//!                       -> re-check reserve -> accept the copy only if both pass
//!
//! Unlike the `append`-only overwrite race `loom_frame.rs` deliberately does not
//! model (no writer-side fence orders the data stores after the counter, so a
//! faithful model fails under C++ semantics), this protocol IS the textbook
//! seqlock writer, and the property holds under loom's full model: a reader
//! whose copy observed ANY byte of the next lap sees the raised reserve on its
//! re-check and refuses the copy.
//!
//! The ring bytes are modeled as Relaxed atomics (production uses plain
//! stores/loads, the same idealisation `loom_frame.rs` makes).
//!
//! Run: RUSTFLAGS="--cfg loom" cargo test -p uc_log --test loom_reserve --release
#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicU64, Ordering, fence};
use loom::thread;

/// Lap-0 bytes the reader is entitled to; lap-1 bytes the receiver writes.
const LAP0: u64 = 0x0A;
const LAP1: u64 = 0x1B;
/// The reader's limit: `from + capacity`. A reserve above it means the
/// receiver may have written over the reader's bytes.
const LIMIT: u64 = 100;
const RUN_END: u64 = 150; // > LIMIT: this run's ring image covers the reader

fn model(writer_fences: bool) {
    loom::model(move || {
        let bytes: Arc<[AtomicU64; 2]> = Arc::new([AtomicU64::new(LAP0), AtomicU64::new(LAP0)]);
        let reserve = Arc::new(AtomicU64::new(0));

        let (wb, wr) = (Arc::clone(&bytes), Arc::clone(&reserve));
        let writer = thread::spawn(move || {
            wr.store(RUN_END, Ordering::Relaxed);
            if writer_fences {
                fence(Ordering::Release);
            }
            wb[0].store(LAP1, Ordering::Relaxed);
            wb[1].store(LAP1, Ordering::Relaxed);
        });

        // Reader: one validated read.
        if reserve.load(Ordering::Acquire) <= LIMIT {
            let b0 = bytes[0].load(Ordering::Relaxed);
            let b1 = bytes[1].load(Ordering::Relaxed);
            fence(Ordering::Acquire);
            if reserve.load(Ordering::Acquire) <= LIMIT {
                assert_eq!(
                    (b0, b1),
                    (LAP0, LAP0),
                    "a copy that passed both checks contains next-lap bytes"
                );
            }
        }
        writer.join().unwrap();
    });
}

#[test]
fn a_reader_that_passes_both_checks_never_holds_next_lap_bytes() {
    model(true);
}

/// Teeth: without the writer's release fence the property is NOT guaranteed,
/// and loom must find the interleaving that breaks it.
#[test]
#[should_panic(expected = "next-lap bytes")]
fn without_the_writer_fence_the_model_fails() {
    model(false);
}
