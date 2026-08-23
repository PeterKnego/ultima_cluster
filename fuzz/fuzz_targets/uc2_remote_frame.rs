// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc2_remote::frame::*;

// Models the gateway edge's read path: a TCP frame of any length arrives, the
// 24-byte header is decoded, and the body is handed to a typed decoder chosen
// by frame type. Every body decoder is offered every body regardless of type —
// a decoder must be total on `&[u8]` no matter which frame carried it.
fuzz_target!(|data: &[u8]| {
    let Ok((h, payload_len)) = decode_header(data) else {
        return;
    };
    // `decode_header` returns the PAYLOAD length, not bytes consumed: the
    // header itself is always `HEADER_LEN`. Bound the body by both what the
    // header claims and what actually arrived — exactly what `FramedConn`
    // does once it has read `len` bytes off the socket.
    let end = (HEADER_LEN + payload_len).min(data.len());
    let body = &data[HEADER_LEN..end];

    let _ = Hello::decode(body);
    let _ = HelloOk::decode(body);
    let _ = HelloRefused::decode(body);
    let _ = ResponseMeta::decode(body);
    let _ = Status::decode(body);
    let _ = Leader::decode(body);
    let _ = Retry::decode(body);
    let _ = (h.ty, h.flags, h.version, h.client_id, h.seq);
});
