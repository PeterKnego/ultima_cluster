// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc_protocol::v2::datagram::{
    PROBE_ACK_BODY_LEN, read_probe_ack_body, read_probe_rung, write_probe_ack_body,
};

fuzz_target!(|data: &[u8]| {
    // A probe body is a rung plus padding: the decoder must never panic and
    // must agree with the first four bytes.
    if let Some(rung) = read_probe_rung(data) {
        assert_eq!(rung, u32::from_le_bytes(data[0..4].try_into().unwrap()));
    }
    // The ack body is exact-length and must round-trip.
    if let Some(b) = read_probe_ack_body(data) {
        let mut re = [0u8; PROBE_ACK_BODY_LEN];
        write_probe_ack_body(&mut re, &b);
        assert_eq!(read_probe_ack_body(&re), Some(b));
    }
});
