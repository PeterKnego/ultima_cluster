// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc_protocol::ring::common::{
    COMMIT_LAP_MASK, SlotState, classify_commit_word, decode_record_slice,
};

// The MPSC ingress ring is a writable mmap'd file that every client process
// on the host can write. Its consumer — the node's consensus agent, the
// single most safety-critical thread in the system — decides what to do with
// a slot from one 32-bit commit word and then decodes the bytes behind it.
// Both steps must be total on arbitrary input: a panic here is a node crash
// triggerable by a torn write or a hostile local process.
//
// Input layout: [0..4) commit word, [4..8) expected lap, [8..) slot bytes.
fuzz_target!(|data: &[u8]| {
    if data.len() < 8 {
        return;
    }
    let word = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let expected_lap = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) & COMMIT_LAP_MASK;
    let body = &data[8..];
    let mut buf = Vec::new();

    // 1. The classifier is total on every (word, lap) pair.
    match classify_commit_word(word, expected_lap) {
        SlotState::Committed { length } => {
            // The consumer decodes exactly `length` bytes. Clamp to what we
            // have so the committed path is exercised on EVERY input rather
            // than only when the fuzzer guesses a length that fits.
            let take = (length as usize).min(body.len());
            let _ = decode_record_slice(&body[..take], &mut buf);
        }
        SlotState::Claimed { .. } | SlotState::Empty => {}
    }

    // 2. The decoder is total on any slice, whatever the word claimed.
    let _ = decode_record_slice(body, &mut buf);
});
