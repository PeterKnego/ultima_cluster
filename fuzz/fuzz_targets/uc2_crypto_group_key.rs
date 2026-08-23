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
    // byte 0 = the claimed sender id, so an ACK from a peer the pending epoch
    // never targeted, an ACK from ourselves, and an ACK from an unknown node
    // are all reachable — `on_ack` ranks acks per peer.
    let Some((&from, body)) = data.split_first() else {
        return;
    };
    let mut g = PLANE.lock().unwrap();
    // Built with a PENDING epoch already minted and outstanding, so the
    // `MSG_ACK` arm is not vacuous: with no pending epoch every ack folds
    // into a no-op and the whole branch is dead weight in the corpus.
    let plane = g.get_or_insert_with(uc2_fuzz::group_plane_with_pending);
    let _ = plane.on_key_message(from as u32, body);
});
