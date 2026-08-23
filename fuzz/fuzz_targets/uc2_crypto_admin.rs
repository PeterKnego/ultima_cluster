// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc2_crypto::admin::{ADMIN_TAG_LEN, AdminKey, AdminMessage, sign, verify};

// A PROPERTY target, not a parser: it guards the M12b signed-tag layout
// against silent drift. Whatever the field values, the canonical encoding must
// have the documented length, a tag over it must verify, and flipping any bit
// of that tag must break verification.
fuzz_target!(|data: &[u8]| {
    let parts = uc2_fuzz::split(data, 10);
    let app_id = String::from_utf8_lossy(parts[0]).into_owned();
    // `canonical_bytes` length-prefixes `app_id` as a `u16` and says so; an
    // app_id past 64 KiB is a config-time impossibility, not a wire input.
    if app_id.len() > u16::MAX as usize {
        return;
    }

    fn u64_of(b: &[u8]) -> u64 {
        let mut x = [0u8; 8];
        let n = b.len().min(8);
        x[..n].copy_from_slice(&b[..n]);
        u64::from_le_bytes(x)
    }
    fn u32_of(b: &[u8]) -> u32 {
        u64_of(b) as u32
    }

    let m = AdminMessage {
        app_id: &app_id,
        instance_id: ((u64_of(parts[1]) as u128) << 64) | u64_of(parts[2]) as u128,
        seq: u64_of(parts[3]),
        nonce: u64_of(parts[4]),
        op: u32_of(parts[5]),
        id: u32_of(parts[6]),
        ip: u32_of(parts[7]),
        port: u64_of(parts[8]) as u16,
        expiry_ns: u64_of(parts[9]),
    };

    // The layout the HMAC covers, pinned field by field.
    assert_eq!(
        m.canonical_bytes().len(),
        2 + app_id.len() + 16 + 8 + 8 + 4 + 4 + 4 + 2 + 8,
        "admin canonical_bytes layout drifted"
    );

    let key = AdminKey::new("fuzz", [9u8; 32]);
    let tag = sign(&key, &m);
    assert!(verify(&key, &m, &tag), "a freshly signed tag must verify");

    // Any single-bit change to the tag must break it.
    let bit = (u64_of(parts[3]) % (ADMIN_TAG_LEN as u64 * 8)) as usize;
    let mut bad = tag;
    bad[bit / 8] ^= 1 << (bit % 8);
    assert!(!verify(&key, &m, &bad), "a bit-flipped tag must NOT verify");

    // A different key must not verify the same message.
    let other = AdminKey::new("fuzz", [10u8; 32]);
    assert!(!verify(&other, &m, &tag), "a foreign key must NOT verify");
});
