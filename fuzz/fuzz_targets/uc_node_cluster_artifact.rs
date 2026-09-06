// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc_node::{ClusterFsm, ClusterState};
use uc_service::SnapshotStateMachine;

// The cluster IMAGE (cluster-FSM spec §4.7, §11): magic | version | applied |
// table_position | settings_position | membership | table | settings | crc32.
// A joiner installs this artifact BY FIAT off a snapshot session, and a
// restarted node reads it off disk, so the decoder has to be total on any
// slice: CRC32 is a public checksum, not a MAC, and every length prefix in a
// crc-consistent-but-crafted body is attacker-chosen.
//
// Two calls per input. The first pins the position at 0, the arm a
// wrong-position artifact takes; the second reads the image's OWN `applied`
// field (offset 12, little-endian) and passes that, so a mutated-but-valid
// image gets past the position gate and into the length-prefixed reads
// underneath. The property is only ever "never panics" — every outcome,
// installed or refused, is a legal one.
fuzz_target!(|data: &[u8]| {
    let mut fsm = ClusterFsm::new(ClusterState::genesis_empty(), vec![0xF5A0, 0xF5A1]);
    let _ = fsm.install_snapshot(0, &mut &data[..]);

    if let Some(applied) = data
        .get(12..20)
        .map(|b| u64::from_le_bytes(b.try_into().expect("8 bytes")))
    {
        let _ = fsm.install_snapshot(applied, &mut &data[..]);
    }
});
