// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc_service::snapshots::{
    SNAPSHOT_ENVELOPE_LEN, decode_snapshot_envelope, verify_snapshot_envelope,
};

// The 16-byte artifact ENVELOPE (coordinated-snapshot ruling P6): `ULTSNAP1`
// then the instant P the artifact was built at, LE. It is the FIRST thing read
// off any `snap-<pos>.ultsnap` — a file a joiner received over the wire, an
// operator restored from a backup, or anything else that happens to be sitting
// under `snapshots/<row>/` — and the decision it drives is "install this image
// or refuse it". So the decoder has to be total on any slice: every byte of it
// is attacker- or accident-chosen, and a panic here kills the apply agent.
//
// Two calls per input. `decode_snapshot_envelope` is the pure decoder; then
// `verify_snapshot_envelope` reads the same bytes through the `Read` path every
// install site actually uses, at the position the input itself names, so a
// well-formed envelope reaches the equality branch rather than always failing
// at the magic. The property is only ever "never panics" — refusing is a legal
// outcome for any input, and so is accepting one that happens to be valid.
fuzz_target!(|data: &[u8]| {
    let decoded = decode_snapshot_envelope(data);

    let mut src = data;
    let expected = *decoded.as_ref().unwrap_or(&0);
    let verified = verify_snapshot_envelope(&mut src, expected).is_ok();

    // The reader is left at the payload exactly when it verified.
    if verified && data.len() >= SNAPSHOT_ENVELOPE_LEN {
        assert_eq!(src.len(), data.len() - SNAPSHOT_ENVELOPE_LEN);
    }
});
