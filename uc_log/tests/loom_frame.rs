// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Loom model of the frame commit protocol (buffer.rs):
//!   writer: plain payload writes -> Release store of length -> Release store of append
//!   reader: Acquire load of append -> bounded read -> payload fully visible
//!
//! Two properties are pinned separately here:
//!   1. `committed_frame_is_fully_visible_to_append_bounded_reader` pins the
//!      `append` counter's Release/Acquire pair: a reader that only ever
//!      bounds itself by an acquire-load of `append` sees fully-visible
//!      payload and commit-word bytes for every frame below that bound. This
//!      is the actual discipline `recordable_slice` / `read_frame_validated`
//!      use in production — the commit word's own Release is redundant on
//!      that path (verified by mutation: relaxing it there does not fail the
//!      model, because the reader never synchronizes on the commit word).
//!   2. `commit_word_release_is_sufficient_without_append` pins the commit
//!      word's Release/Acquire pair on its own, for a hypothetical reader
//!      that bounds itself by the commit word directly instead of `append`.
//!
//! Run: RUSTFLAGS="--cfg loom" cargo test -p uc_log --test loom_frame --release
//!
//! DELIBERATELY NOT MODELED — the seqlock overwrite-race: `read_frame_validated`
//! and `read_run_validated` copy with plain loads, then acquire-fence and
//! re-check the `append` margin. That discipline is sound on TSO (x86: stores
//! become visible in program order, so a stale post-check bound implies
//! unoverwritten bytes) but is formally racy under the C++ memory model —
//! like every mmap-ring design including Aeron's (M1 review adjudication,
//! 2026-07-09). A faithful loom model of it CORRECTLY FAILS under loom's full
//! C++ semantics (later plain stores may be observed before an earlier
//! release-store of `append`), so do not add one and conclude the code is
//! broken: the two visibility pairs above are the loom-modelable properties.
#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use loom::thread;

#[test]
fn committed_frame_is_fully_visible_to_append_bounded_reader() {
    loom::model(|| {
        // 2 "frames": [len_word, payload_word] each, plus an append counter.
        let cells: Arc<Vec<AtomicU32>> = Arc::new((0..4).map(|_| AtomicU32::new(0)).collect());
        let append = Arc::new(AtomicU64::new(0));

        let w_cells = Arc::clone(&cells);
        let w_append = Arc::clone(&append);
        let writer = thread::spawn(move || {
            for f in 0..2u64 {
                let base = (f * 2) as usize;
                // payload (plain-ish: modeled as relaxed — buffer.rs uses raw
                // non-atomic writes; the Release on the length word orders them)
                w_cells[base + 1].store(0xAB00 + f as u32, Ordering::Relaxed);
                // commit word, Release (mirrors commit_word().store(Release))
                w_cells[base].store(64, Ordering::Release);
                // append counter, Release (mirrors counters.append.store_release)
                w_append.store(f + 1, Ordering::Release);
            }
        });

        // reader bounded by an acquire of append (mirrors recordable_slice /
        // read_frame_validated pre-check)
        let bound = append.load(Ordering::Acquire);
        for f in 0..bound {
            let base = (f * 2) as usize;
            let len = cells[base].load(Ordering::Acquire);
            assert_eq!(len, 64, "commit word must be visible below append");
            let payload = cells[base + 1].load(Ordering::Relaxed);
            assert_eq!(payload, 0xAB00 + f as u32, "payload must be visible after acquire");
        }

        writer.join().unwrap();
    });
}

/// Pins the commit word's own Release/Acquire pair, independent of `append`.
/// A reader that bounds itself solely by an acquire-load of the commit word
/// (rather than the `append` counter) must still see the payload write that
/// preceded the commit word's Release. Observing 0 (not yet committed) is a
/// legal outcome and asserts nothing.
///
/// Mutation check: relaxing the commit-word store to `Ordering::Relaxed`
/// makes this test FAIL under loom (the model finds an interleaving where
/// the reader observes the committed length but not the payload write) —
/// confirming the Release here is load-bearing, not redundant.
#[test]
fn commit_word_release_is_sufficient_without_append() {
    loom::model(|| {
        let commit_word = Arc::new(AtomicU32::new(0));
        let payload = Arc::new(AtomicU32::new(0));

        let w_commit = Arc::clone(&commit_word);
        let w_payload = Arc::clone(&payload);
        let writer = thread::spawn(move || {
            // plain payload write (buffer.rs: raw non-atomic write)
            w_payload.store(0xCAFE, Ordering::Relaxed);
            // commit word, Release (mirrors commit_word().store(Release))
            w_commit.store(64, Ordering::Release);
        });

        // Reader bounds itself ONLY by the commit word, never touching
        // `append` at all.
        let len = commit_word.load(Ordering::Acquire);
        if len != 0 {
            assert_eq!(len, 64);
            assert_eq!(
                payload.load(Ordering::Relaxed),
                0xCAFE,
                "payload must be visible once the commit word is observed non-zero"
            );
        }
        // len == 0 (not yet committed) is a legal outcome: assert nothing.

        writer.join().unwrap();
    });
}
