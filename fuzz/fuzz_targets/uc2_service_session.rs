// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc2_fuzz::NoopSm;
use uc2_service::RawStateMachine;
use uc2_service::{SessionConfig, Sessioned};

// `Sessioned` is the exactly-once envelope: the first thing a REMOTE client's
// bytes reach after the gateway relays them, and the owner of a snapshot
// artifact whose replicated-config check must refuse, not panic.
fuzz_target!(|data: &[u8]| {
    let parts = uc2_fuzz::split(data, 3);
    let mut sm = Sessioned::new(NoopSm, SessionConfig::default());
    let mut out = Vec::new();

    sm.apply(1, parts[0], &mut out);
    out.clear();
    sm.apply(2, parts[1], &mut out);

    let mut cur = std::io::Cursor::new(parts[2]);
    let _ = uc2_service::SnapshotStateMachine::install_snapshot(&mut sm, 3, &mut cur);
});
