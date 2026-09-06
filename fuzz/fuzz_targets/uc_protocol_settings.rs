// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc_protocol::v2::settings::{decode_settings, encode_settings};

fuzz_target!(|data: &[u8]| {
    if let Some(s) = decode_settings(data) {
        let mut re = Vec::new();
        encode_settings(&s, &mut re);
        assert_eq!(decode_settings(&re), Some(s));
    }
});
