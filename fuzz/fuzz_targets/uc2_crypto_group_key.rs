// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use std::sync::Mutex;
use uc2_crypto::group::GroupPlane;

// `DGRAM_KIND_HS_KEY` (20) carries two message shapes over one kind — a key
// delivery and an ack — demultiplexed by a leading tag byte. This is group
// key material arriving from the network; a malformed body must be refused,
// never panic and never install anything.
static PLANE: Mutex<Option<GroupPlane>> = Mutex::new(None);

fuzz_target!(|data: &[u8]| {
    let mut g = PLANE.lock().unwrap();
    let plane = g.get_or_insert_with(|| GroupPlane::new(uc2_fuzz::A_ID));
    let _ = plane.on_key_message(uc2_fuzz::B_ID, data);
});
