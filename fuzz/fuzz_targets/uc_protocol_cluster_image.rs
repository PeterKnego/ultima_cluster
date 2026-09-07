// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc_protocol::v2::cluster_image::{decode_cluster_image, encode_cluster_image};

// The cluster IMAGE codec (cluster-FSM spec §4.7, §4.8), moved to this leaf
// out of `uc_node::cluster_fsm` in plan 3 so it can be fuzzed directly: magic
// | version | applied | table_position | settings_position | membership |
// table | settings | crc32. A below-floor joiner installs this artifact BY
// FIAT off a snapshot session, and a restarted node reads it off disk, so
// the decoder has to be total on any slice — CRC32 is a public checksum, not
// a MAC, and every length prefix in a crc-consistent-but-crafted body is
// attacker-chosen.
//
// Property: decode is total (never panics) and idempotent through
// re-encoding — a decoded image re-encodes to bytes that decode back to the
// SAME parts.
fuzz_target!(|data: &[u8]| {
    if let Some(parts) = decode_cluster_image(data) {
        let mut re = Vec::new();
        encode_cluster_image(&parts, &mut re);
        assert_eq!(decode_cluster_image(&re), Some(parts));
    }
});
