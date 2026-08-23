// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;

/// A fixed key. The point is not to break AES-GCM — it is that the framing
/// around it (header/counter/tag splitting, length arithmetic) must never
/// panic on bytes an attacker chose.
const KEY: [u8; 32] = [7u8; 32];

fuzz_target!(|data: &[u8]| {
    let mut v = data.to_vec();
    let _ = uc2_crypto::seal::open_in_place(&mut v, &KEY);

    let mut s = data.to_vec();
    let n = s.len();
    let _ = uc2_crypto::seal::open_detached(&mut s[..], &KEY);
    let _ = n;
});
