// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use std::sync::OnceLock;
use uc_node::obs::ObsSources;
use uc_node::obs::http::route_raw;

static SRC: OnceLock<ObsSources> = OnceLock::new();

// The `/metrics` + `/healthz` + `/readyz` endpoint is the one TCP port a node
// opens to something other than a cluster peer or a local process, and it is
// entirely unauthenticated by design (M10). `route` parses whatever a scraper
// sent, so it must be total on `&[u8]`.
//
// The input is capped at 4607 bytes, which is the largest buffer
// `handle_conn` can actually hand the router: its read loop checks
// `buf.len() >= REQUEST_CAP` (4096) at the TOP of each iteration and then
// extends by up to one 512-byte chunk, so a buffer of 4095 becomes 4607
// before the cap fires. Clamping at a flat 4096 would have left the last 511
// bytes of the real input space unfuzzed; clamping higher would fuzz inputs
// the server cannot deliver.
const MAX_ROUTED: usize = 4095 + 512;

fuzz_target!(|data: &[u8]| {
    let src = SRC.get_or_init(ObsSources::for_tests);
    let (code, _ctype, _body) = route_raw(&data[..data.len().min(MAX_ROUTED)], src);
    // The router's REAL status set, pinned against `write_response`'s reason
    // table (the other half of the same contract): 200 OK, 404 Not Found,
    // 503 Service Unavailable. There is deliberately no 405 — a non-GET is a
    // 404 for a single-purpose scrape endpoint with no other verbs to
    // advertise — and no 400/413/414/500 anywhere in the module.
    assert!(matches!(code, 200 | 404 | 503), "router returned an undeclared status {code}");
});
