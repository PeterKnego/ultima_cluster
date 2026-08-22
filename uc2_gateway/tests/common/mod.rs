// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Shared rig for the gateway round-trip tests: a one-node cluster on
//! loopback UDP with its instance directory on **ext4** (never `/tmp`, which
//! is RAM-backed on the dev box — see CLAUDE.md "Local box").
//!
//! The `NodeConfig` shape is the one the lincheck harness uses
//! (`uc2_node/tests/lincheck_v2/mod.rs`), reduced to `n = 1`: a single member
//! elects itself immediately, so `can_serve()` goes true without any peer
//! traffic and the test never has to model failover (that is Task 9's job).

#![allow(dead_code)] // each test file uses a different subset

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};

pub const APP: &str = "gw-roundtrip";

/// A temp dir on real disk. `CARGO_TARGET_TMPDIR` lives under `target/`
/// (ext4), so journal segments never land on the RAM-backed `/tmp`.
pub fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("uc2-gw-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

/// Start a single-member node under `root/node0` and return it with its
/// instance directory.
pub fn start_single_node(root: &Path) -> (Node, PathBuf) {
    let dir = root.join("node0");
    std::fs::create_dir_all(&dir).expect("instance dir");
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let cfg = NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        bind,
        instance_dir: dir.clone(),
        app_id: APP.into(),
        buffer_bytes: 1 << 22, // 4 MiB
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: FaultConfig::default(),
        purge: uc2_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc2_node::CryptoConfig::Disabled,
    };
    let node = Node::start(cfg).expect("node start");
    (node, dir)
}

/// Poll until the node is a serving leader, or panic after `secs`.
pub fn await_serving(node: &Node, secs: u64) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !node.can_serve() {
        assert!(Instant::now() < deadline, "node never became a serving leader");
        std::thread::sleep(Duration::from_millis(2));
    }
}
