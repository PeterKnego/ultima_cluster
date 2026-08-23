// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;

// `node.toml` is operator-authored, not network input — but it is the file
// that decides whether a node starts, and every M12b/M11 named refusal is
// reached through this parser. A malformed or hostile config must produce a
// named `ConfigError`, never a panic: a panic here is an unnamed crash at
// startup, exactly the failure mode M9's "named startup refusals" exist to
// prevent.
fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = uc2_node::config_file::parse_str(s);
    }
});
