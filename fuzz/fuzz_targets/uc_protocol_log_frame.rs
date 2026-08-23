// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc_protocol::v2::frame::*;

// `read_header` is deliberately caller-guarded (see its doc): the apply
// thread only calls it after an acquire load has shown a non-zero length on a
// buffer known to hold `HEADER_LEN` bytes. This target reproduces that guard
// rather than removing it, and asserts the field arithmetic behind it never
// panics for any 32-byte-or-longer content.
fuzz_target!(|data: &[u8]| {
    if data.len() < HEADER_LEN {
        return;
    }
    let h = read_header(data);
    let _ = (h.length, h.frame_type, h.flags, h.leadership_term_id, h.session_id, h.correlation_id);
    // The alignment helper the log buffer runs on every decoded length.
    let _ = align_frame_len(h.length as usize % (1 << 20));
});
