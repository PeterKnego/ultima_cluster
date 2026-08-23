// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use std::sync::Mutex;
use uc2_crypto::HandshakeAction;
use uc2_crypto::handshake::Peers;

// The pre-auth Noise `IK` surface: `Peers::on_message` is, in its own words,
// "the first thing in the process to see bytes from anyone who can reach the
// UDP port". The first fuzz byte picks the datagram kind (so HS_INIT, HS_RESP
// and every non-handshake kind are all reachable), the rest is the body.
static PEERS: Mutex<Option<Peers>> = Mutex::new(None);

fuzz_target!(|data: &[u8]| {
    // byte 0 = datagram kind, byte 1 = the claimed sender id, bytes 2..10 =
    // `now_ns`. `from` is a CLAIM the handshake has to check against the
    // pattern and the allowlist, so letting the fuzzer choose it reaches the
    // unknown-peer, self-id and not-in-allowlist branches; `now_ns` drives
    // the allowlist-reload rate limit and every expiry comparison.
    if data.len() < 10 {
        return;
    }
    let kind = data[0];
    let from = data[1] as u32;
    let now_ns = u64::from_le_bytes(data[2..10].try_into().unwrap());
    let body = &data[10..];

    let mut g = PEERS.lock().unwrap();
    let peers = g.get_or_insert_with(uc2_fuzz::responder_peers);

    let acts = peers.on_message(from, kind, body, now_ns);

    // A genuine message 1 (the seed corpus carries one) ESTABLISHES a session,
    // after which this `Peers` is no longer the never-seen-a-packet responder
    // the target is meant to model — snow's handshake state has advanced and
    // subsequent inputs would only exercise the transport path. Rebuild on
    // success so the pre-auth surface stays the thing being fuzzed. Rare, so
    // the per-iteration cost stays at one mutex lock.
    if acts.iter().any(|a| matches!(a, HandshakeAction::Established { .. })) {
        *g = None;
    }
});
