// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![cfg(not(loom))]

use std::sync::Arc;
use uc2_log::buffer::{Appender, FrameRead, LogBuffer};
use uc2_log::counters::LogCounters;

#[test]
#[cfg_attr(miri, ignore)] // real mmap
fn file_backed_buffer_roundtrip_across_reopen_of_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.buf");
    let counters = Arc::new(LogCounters::new());

    let b = Arc::new(
        LogBuffer::create_file(&path, 1 << 16, Arc::clone(&counters), 1024).unwrap(),
    );
    let mut a = Appender::new(Arc::clone(&b), 1);
    let pos = a.append(5, 6, b"persisted?").unwrap();

    let mut out = Vec::new();
    assert!(matches!(b.read_frame_validated(pos, &mut out), FrameRead::Frame(_)));
    drop(a);
    drop(b);

    // Re-map the same file: bytes are there (same-host shared mapping is the
    // M5 IPC story; counters are NOT in the file yet — prime them by hand).
    let counters2 = Arc::new(LogCounters::new());
    counters2.prime(64); // one 42-byte frame -> aligned 64
    let b2 = LogBuffer::open_file(&path, counters2, 1024).unwrap();
    let mut out2 = Vec::new();
    match b2.read_frame_validated(0, &mut out2) {
        FrameRead::Frame(h) => {
            assert_eq!(h.session_id, 5);
            assert_eq!(&out2[32..], b"persisted?");
        }
        other => panic!("expected Frame, got {other:?}"),
    }
}
