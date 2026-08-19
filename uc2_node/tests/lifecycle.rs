// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M9: daemon lifecycle — drain-on-stop and restart cost.
//!
//! Construction follows `failover.rs`: a pre-bound socket, an instance dir
//! under `CARGO_TARGET_TMPDIR` (ext4, never the RAM-backed `/tmp` — see
//! CLAUDE.md "Local box"), and a sole voter that elects itself.

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use uc2_log::archive::{Archive, ArchiveConfig};
use uc2_node::{DrainOutcome, Node, NodeConfig};

const PAYLOAD: usize = 96;
const RING: usize = 1 << 22;
/// Enough load that the archive is genuinely behind when the stop is issued.
/// See the drain test's comment on why a small load cannot discriminate.
const LOAD: u64 = 5_000;

fn config_for(addr: SocketAddr, instance_dir: PathBuf) -> NodeConfig {
    NodeConfig {
        id: 0,
        members: vec![(0, addr)],
        learners: Vec::new(),
        bind: addr,
        instance_dir,
        app_id: "lifecycle".into(),
        buffer_bytes: RING,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: 0xA1B2_C3D4_5566_7788,
        faults: Default::default(),
        purge: uc2_node::PurgePolicy::Disabled,
        journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc2_node::CryptoConfig::Disabled,
    }
}

/// A sole-voter node on a freshly bound loopback socket, already serving.
fn single_node(instance_dir: &Path) -> (Node, SocketAddr) {
    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind");
    let addr = sock.local_addr().unwrap();
    let node = Node::start_with_socket(config_for(addr, instance_dir.to_path_buf()), sock)
        .expect("start");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !node.can_serve() {
        assert!(Instant::now() < deadline, "sole voter never became leader");
        std::thread::yield_now();
    }
    (node, addr)
}

/// Submit `n` distinct payloads, retrying a full ingress (failover.rs pattern).
fn append_some_load(node: &Node, n: u64) {
    for i in 0..n {
        let mut p = vec![0u8; PAYLOAD];
        p[..8].copy_from_slice(&i.to_le_bytes());
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match node.submit(p.clone()) {
                Ok(()) => break,
                Err(uc2_node::SubmitError::Full) => {
                    assert!(Instant::now() < deadline, "ingress stayed full");
                    std::thread::yield_now();
                }
                Err(e) => panic!("submit to serving sole voter: {e:?}"),
            }
        }
    }
}

/// The drain's whole purpose: after it reports `Drained`, the JOURNAL itself
/// holds every appended byte.
///
/// Asserted by reopening the journal, not by restarting the node and reading
/// its counters. A restart cannot discriminate: `log.buf` is file-backed, so a
/// restarted archive picks the un-recorded tail straight back out of the buffer
/// and catches `durable` up within milliseconds whether or not the drain ever
/// ran. `Archive::recovered_position` is what the drain actually moves.
///
/// HONEST LIMIT — this pins the CONTRACT, not a behaviour difference. Mutation
/// (replacing the drain with a bare `stop()`) does NOT turn this test red, at
/// any load tried up to 5000 payloads. `Node::stop` tears the agents down in
/// order and the archive is LAST (`agents: [consensus, sender, receiver,
/// archive]`), so it keeps polling while the other three threads join and
/// clears a backlog of this size on its own. The drain's value is that the
/// catch-up becomes a bounded guarantee that REPORTS what it achieved, instead
/// of an accident of teardown order — which is what Task 8's restart-cost gate
/// measures under real load.
#[test]
fn stop_draining_leaves_durable_caught_up_with_append() {
    let dir = tempfile::Builder::new()
        .prefix("uc2-lifecycle-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let instance_dir = dir.path().join("n0");

    let (node, _addr) = single_node(&instance_dir);
    append_some_load(&node, LOAD);

    let append_before = node.counters().append.load_acquire();
    assert!(append_before > 0, "test must actually append something");

    match node.stop_draining(Duration::from_secs(10)) {
        DrainOutcome::Drained => {}
        other => panic!("expected Drained, got {other:?}"),
    }

    // The node is stopped, so its archive is dropped and the journal is quiet.
    let arch = Archive::open(ArchiveConfig::new(instance_dir.join("journal")))
        .expect("reopen journal");
    let recorded = arch.recovered_position();
    assert!(
        recorded >= append_before,
        "stop_draining reported Drained while the journal ends at {recorded}, short of the \
         {append_before} bytes appended — the drain did not actually drain"
    );
}

/// A drain that cannot finish must still stop the node. A shutdown that hangs
/// is worse than one that costs a replay.
///
/// This is a non-hang guard, not a proof that the deadline is honoured: when
/// the archive happens to be already caught up the call returns `Drained`
/// immediately and the bound is met trivially. It does catch an unbounded
/// wait, which is the failure that matters.
#[test]
fn stop_draining_honours_its_deadline() {
    let dir = tempfile::Builder::new()
        .prefix("uc2-lifecycle-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let (node, _addr) = single_node(&dir.path().join("n0"));
    append_some_load(&node, 512);

    let t0 = Instant::now();
    let outcome = node.stop_draining(Duration::from_nanos(1));
    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_secs(30),
        "a 1 ns deadline must not become an unbounded wait (took {elapsed:?}, got {outcome:?})"
    );
}
