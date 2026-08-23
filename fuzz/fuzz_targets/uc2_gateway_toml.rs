// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;

// `gateway.toml`'s loader is the WHOLE named-refusal path for `uc2-gateway`
// (it runs `EdgeConfig::validate` itself — there is no separate preflight
// step), so everything that can refuse a gateway start is behind this call.
fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = uc2_gateway::config_file::parse_str(s);
    }
});
