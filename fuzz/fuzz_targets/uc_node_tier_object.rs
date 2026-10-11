// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]
use libfuzzer_sys::fuzz_target;
use std::io::Read;

fuzz_target!(|data: &[u8]| {
    if let Ok(mut r) = uc_node::tier::object::ObjectReader::open(std::io::Cursor::new(data)) {
        let mut sink = Vec::new();
        let _ = r.by_ref().take(64 << 20).read_to_end(&mut sink);
    }
    let _ = uc_node::tier::layout::SetLabel::parse(&String::from_utf8_lossy(data));
    let _ = uc_node::tier::layout::TierInfo::parse(&String::from_utf8_lossy(data));
});
