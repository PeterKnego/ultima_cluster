// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc_journal::fuzz_seams::{decode_header, decode_record};

// The on-disk segment parser: what recovery meets after a crash, a torn tail,
// a full disk, or bit rot. It must distinguish "torn tail" (`Ok(None)`) from
// "confirmed corruption" (`Err`) without ever panicking.
fuzz_target!(|data: &[u8]| {
    let _ = decode_header(data);
    let _ = decode_record(data, "fuzz-seg", 0);
});
