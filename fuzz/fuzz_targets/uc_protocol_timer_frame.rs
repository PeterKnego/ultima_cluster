// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc_protocol::v2::frame::*;

// The TIMER frame body the apply loop decodes from a committed frame: the
// header (behind the real caller's `len >= HEADER_LEN` guard, same as
// `uc_protocol_log_frame`) plus a `TimerBody`, which is total on any slice
// (`read_timer_body` returns `None` below `TIMER_BODY_LEN` rather than
// panicking). A round-trip on a decoded body must be exact, and the lateness
// comparison (`header.time_ns > body.deadline_ns`) must never panic.
fuzz_target!(|data: &[u8]| {
    if data.len() < HEADER_LEN {
        return;
    }
    let h = read_header(data);
    let _ = (h.client_id, h.seq, h.time_ns);
    let body = &data[HEADER_LEN..];
    if let Some(b) = read_timer_body(body) {
        let mut out = [0u8; TIMER_BODY_LEN];
        write_timer_body(&mut out, &b);
        assert_eq!(read_timer_body(&out), Some(b));
        let _ = h.time_ns > b.deadline_ns; // the lateness predicate is total
    }
});
