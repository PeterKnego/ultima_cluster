// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Loom model of the frame commit protocol (buffer.rs):
//!   writer: plain payload writes -> Release store of length -> Release store of append
//!   reader: Acquire load of append -> bounded read -> payload fully visible
//! Run: RUSTFLAGS="--cfg loom" cargo test -p uc2_log --test loom_frame --release
#![allow(unexpected_cfgs)] // `loom` is a bespoke cfg set via RUSTFLAGS
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
