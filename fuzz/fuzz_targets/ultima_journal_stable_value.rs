// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use ultima_journal::stable_value::{decode_header, decode_slot};

// `StableValue` holds the vote, the term map, the snapshot floor and the
// cluster config record — the durable state a node's correctness rests on. A
// corrupt slot must read as absent, never panic the boot path.
fuzz_target!(|data: &[u8]| {
    let _ = decode_header(data);
    let _ = decode_slot(data);
});
