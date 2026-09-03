// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc_protocol::v2::datagram::*;

// Models the receiver's pre-auth path: a datagram of any length arrives; the
// header is read if >= 16 bytes; every body reader is then offered the rest.
// None of this may panic for ANY input.
fuzz_target!(|data: &[u8]| {
    let Some(h) = read_datagram_header(data) else {
        return;
    };
    let body = &data[DATAGRAM_HEADER_LEN..];
    let _ = h.kind; // dispatch by kind like the receiver does, but ALSO try every
    // reader on every body — a reader must be total regardless.
    let _ = read_append_position_body(body);
    let _ = read_read_probe_body(body);
    let _ = read_config_proposal_body(body);
    let _ = read_config_reply_body(body);
    let _ = read_snap_begin_body(body);
    let _ = read_snap_table_body(body);
    let _ = read_snap_nak_body(body);
    let _ = read_request_vote_body(body);
    let _ = read_vote_body(body);
    let _ = read_nak_body(body);
    let _ = read_status_body(body);
    let mut entries = [TermMapEntryWire { term: 0, base: 0 }; MAX_TERM_MAP_WIRE_ENTRIES];
    let _ = read_term_map_body(body, &mut entries);
});
