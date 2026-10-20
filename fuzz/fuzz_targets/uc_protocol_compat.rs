// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]
use libfuzzer_sys::fuzz_target;
use uc_protocol::WireRange;
use uc_protocol::v2::datagram::read_datagram_header;
use uc_protocol::v2::settings::{decode_settings_at, settings_version_for};

// Byte 0 is the committed level; the rest is an untrusted record. Two
// properties: no decoder panics, and nothing above the level is accepted.
fuzz_target!(|data: &[u8]| {
    let Some((&level, rest)) = data.split_first() else {
        return;
    };
    if let Some(s) = decode_settings_at(level, rest) {
        let version = u32::from_le_bytes(rest[0..4].try_into().unwrap());
        assert!(
            version <= settings_version_for(level),
            "accepted a v{version} record at level {level}"
        );
        let _ = s;
    }
    if let Some(h) = read_datagram_header(rest) {
        let range = WireRange {
            min: level.saturating_sub(1).max(1),
            max: level,
        };
        if range.accepts_peer(h.wire) {
            assert!(h.wire != 0 && h.wire <= level.saturating_add(1));
        }
    }
});
