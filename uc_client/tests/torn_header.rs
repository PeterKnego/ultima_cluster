// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M5 final review #2b: the client timeout path must never PANIC because a
//! node is recreating the cnc page in place. When the re-read observes a torn /
//! zeroed header (magic not yet rewritten), the client returns
//! `ClientError::InstanceRestart { current: 0 }` (the documented "header
//! unreadable / node mid-recreate" sentinel) rather than `expect`-panicking on
//! an invalid header.
//!
//! Own test binary so the `UC2_CLIENT_TIMEOUT_MS` process-wide override is
//! isolated (same discipline as `timeout_and_restart.rs`).

use std::path::Path;

use uc_client::{Client, ClientError};
use uc_log::cnc::{CncMeta, CncPage};
use uc_protocol::ring::{BroadcastRing, MpscRing};
use uc_protocol::v2::cnc::CNC_OFF_MAGIC;

const MIB: u64 = 1 << 20;

fn meta(app_id: &str, instance_id: u128) -> CncMeta {
    CncMeta {
        node_id: 0,
        instance_id,
        app_id: app_id.into(),
        buffer_bytes: MIB,
        max_payload: 256,
        services: [None; uc_protocol::v2::cnc::CNC_MAX_SERVICES],
    }
}

fn make_instance(dir: &Path, app_id: &str, instance_id: u128) {
    CncPage::create_file(&dir.join("cnc2.dat"), &meta(app_id, instance_id)).unwrap();
    MpscRing::create(&dir.join("ingress.ring"), MIB, 256).unwrap();
    MpscRing::create(&dir.join("query.ring"), MIB, 256).unwrap();
    BroadcastRing::create(&dir.join("egress_service.0.broadcast"), MIB, 256).unwrap();
    BroadcastRing::create(&dir.join("egress_node.broadcast"), MIB, 256).unwrap();
}

#[test]
fn timeout_path_does_not_panic_on_a_torn_cnc_header() {
    // SAFETY: sole test in this binary, so no other thread observes the env var.
    unsafe {
        std::env::set_var("UC2_CLIENT_TIMEOUT_MS", "150");
    }

    let dir = tempfile::tempdir().unwrap();
    make_instance(dir.path(), "torn-test", 0x1234_5678_9abc_def0);

    let client = Client::connect(dir.path(), "torn-test").unwrap();

    // Simulate the node-restart recreate window AFTER `set_len(4096)` but BEFORE
    // the header magic is rewritten: the page is full length (no SIGBUS) but the
    // magic bytes are zeroed. We keep the file at its full page length (NEVER
    // truncate here — a truncate is the accepted SIGBUS window, not what this
    // test exercises) and just clobber the 8 magic bytes in place. The client's
    // MAP_SHARED mmap observes the write (shared page cache, same inode).
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(dir.path().join("cnc2.dat"))
            .unwrap();
        f.seek(SeekFrom::Start(CNC_OFF_MAGIC as u64)).unwrap();
        f.write_all(&[0u8; 8]).unwrap();
        f.flush().unwrap();
    }

    // Nobody answers → the request times out → the timeout path re-reads the
    // header. A torn (zero-magic) header must yield InstanceRestart, NOT a panic.
    let result: Result<u8, ClientError> = client.submit(&1u8);
    match result {
        Err(ClientError::InstanceRestart { current, .. }) => {
            assert_eq!(current, 0, "torn-header sentinel is current == 0");
        }
        other => panic!("expected InstanceRestart on a torn header, got {other:?}"),
    }
}
