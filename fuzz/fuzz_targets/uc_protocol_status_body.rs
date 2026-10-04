// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]
use libfuzzer_sys::fuzz_target;
use uc_protocol::v2::datagram::{STATUS_BODY_LEN, read_status_body, write_status_body};
fuzz_target!(|data: &[u8]| {
    if let Some(b) = read_status_body(data) {
        let mut buf = [0u8; STATUS_BODY_LEN];
        write_status_body(&mut buf, &b);
        assert_eq!(read_status_body(&buf), Some(b), "re-encode must round-trip");
    }
});
