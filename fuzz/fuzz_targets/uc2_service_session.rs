// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc2_fuzz::EchoSm;
use uc2_service::RawStateMachine;
use uc2_service::{SessionConfig, Sessioned};

// `Sessioned` is the exactly-once envelope: the first thing a REMOTE client's
// bytes reach after the gateway relays them, and the owner of a snapshot
// artifact whose replicated-config check must refuse, not panic.
//
// The config is DERIVED from the fuzz input, and deliberately tiny. With the
// shipped defaults (window 4096, max_clients 65_536, max_bytes 256 MiB) the
// eviction machinery — `evict_clients_over_capacity`, `evict_bytes_over_budget`
// and the per-client window trim — is unreachable in a fuzz-sized run: you
// would need tens of thousands of distinct clients in one iteration. Shrinking
// the bounds to single digits makes all three fire constantly while testing
// exactly the same code.
//
// The inner SM is `EchoSm`, NOT `NoopSm`, and that choice is load-bearing:
// `Sessioned` accounts `total_bytes` from the length of each cached FRESH
// response, so a zero-length-response SM pins `total_bytes` at 0 and
// `evict_bytes_over_budget` is dead however small `max_bytes` gets. Echoing
// the command body makes the response length fuzzer-controlled, which is what
// actually reaches the budget path (Task 3 review: the first version of this
// target claimed all three eviction paths and only had two).
fuzz_target!(|data: &[u8]| {
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };

    // The first byte also selects the config: keep the shipped defaults on one
    // path, so the default-config shape stays covered and the snapshot seeds
    // (built under the defaults) still install cleanly rather than always
    // taking the config-mismatch branch.
    let default_cfg = sel & 1 == 0;
    let cfg = if default_cfg {
        SessionConfig::default()
    } else {
        let b = (sel >> 1) as usize;
        SessionConfig {
            window: 1 + b % 4,
            max_clients: 1 + b % 4,
            // Small enough that the byte budget evicts whole clients, but not
            // so small that every single response trips it.
            max_bytes: 16 + (b % 8) * 16,
        }
    };

    let parts = uc2_fuzz::split(rest, 9);
    let mut sm = Sessioned::new(EchoSm::default(), cfg);
    let mut out = Vec::new();

    // Eight applies at increasing positions: enough distinct clients and
    // repeated seqs, against a max_clients of 1..4, to drive eviction, the
    // window trim, and the byte-budget path many times per iteration.
    for (i, part) in parts[..8].iter().enumerate() {
        out.clear();
        sm.apply(i as u64 + 1, part, &mut out);
    }

    // install_snapshot over a Cursor — a mismatched replicated `SessionConfig`
    // must be REFUSED, and a truncated or hostile blob length must not panic
    // or over-allocate (M12d finding #2).
    let mut cur = std::io::Cursor::new(parts[8]);
    let _ = uc2_service::SnapshotStateMachine::install_snapshot(&mut sm, 9, &mut cur);
});
