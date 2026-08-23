// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use std::sync::OnceLock;
use uc2_node::obs::ObsSources;
use uc2_node::obs::http::route_raw;

static SRC: OnceLock<ObsSources> = OnceLock::new();

// The `/metrics` + `/healthz` + `/readyz` endpoint is the one TCP port a node
// opens to something other than a cluster peer or a local process, and it is
// entirely unauthenticated by design (M10). `route` parses whatever a scraper
// sent, so it must be total on `&[u8]`.
//
// The input is capped at `REQUEST_CAP` (4096) exactly as `handle_conn` caps
// what it reads before calling the router — fuzzing past that would be
// fuzzing an input the server cannot actually deliver.
fuzz_target!(|data: &[u8]| {
    let src = SRC.get_or_init(ObsSources::for_tests);
    let (code, _ctype, _body) = route_raw(&data[..data.len().min(4096)], src);
    // The router's REAL status set, pinned against `write_response`'s reason
    // table (the other half of the same contract): 200 OK, 404 Not Found,
    // 503 Service Unavailable. There is deliberately no 405 — a non-GET is a
    // 404 for a single-purpose scrape endpoint with no other verbs to
    // advertise — and no 400/413/414/500 anywhere in the module.
    assert!(matches!(code, 200 | 404 | 503), "router returned an undeclared status {code}");
});
