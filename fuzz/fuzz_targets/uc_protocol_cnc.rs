// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc_protocol::v2::cnc::*;

// The `cnc.dat` control page is a writable file on disk that every attaching
// process maps and parses before it trusts anything in it. Both readers must
// be total.
fuzz_target!(|data: &[u8]| {
    let _ = read_cnc_header(data);
    let _ = read_cnc_app_id(data);
});
