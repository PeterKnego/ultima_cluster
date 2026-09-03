// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc_protocol::v2::ipc::*;

// The 17-byte service->node schedule record (`SCHED_RECORD_LEN`) the
// consensus agent decodes from a shared-memory ring any local process can
// write. `read_sched_record` is total: `None` on a short slice or an op byte
// outside `1..=4`; a decoded record must round-trip through the encoder
// exactly.
fuzz_target!(|data: &[u8]| {
    if let Some(r) = read_sched_record(data) {
        assert_eq!(read_sched_record(&write_sched_record(&r)), Some(r));
    }
});
