// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 10 capstone: a real node + service (`CountSm`, same pattern as
//! `uc2_service/tests/apply.rs`) and TWO real `Client`s.
//!
//! 1. `client_a` alone submits 100 `Add(1)`s sequentially; since apply order
//!    is exactly submission order for a single client waiting on each
//!    response before issuing the next, the running total returned must be
//!    EXACTLY `1..=100` in order.
//! 2. `client_b` connects; its `client_id` differs from `client_a`'s.
//! 3. Both clients then submit another 100 `Add(1)`s each, concurrently, from
//!    separate threads. The shared running total means the two clients'
//!    sequences interleave arbitrarily — but because each client only issues
//!    its Nth submit after receiving its (N-1)th response, that client's OWN
//!    sequence of observed totals must still be strictly increasing
//!    (monotone) between successive submits.

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use uc2_client::Client;
use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};
use uc2_service::{ServiceBuilder, ServiceConfig, StateMachine};

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Cmd {
    Add(u64),
}

/// A running total; the response is the total AFTER applying, so apply order
/// is pinned by the returned values (same SM shape as `uc2_service`'s Task 8
/// capstone).
#[derive(Default)]
struct CountSm {
    total: u64,
    last_applied: Option<u64>,
}

impl StateMachine for CountSm {
    type Command = Cmd;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, position: u64, cmd: Cmd) -> u64 {
        let Cmd::Add(n) = cmd;
        self.total += n;
        self.last_applied = Some(position);
        self.total
    }

    fn query(&self, _q: ()) -> u64 {
        self.total
    }

    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

fn node_config(dir: &Path, app_id: &str) -> NodeConfig {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        bind,
        instance_dir: dir.to_path_buf(),
        app_id: app_id.into(),
        buffer_bytes: 1 << 20,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: FaultConfig::default(),
        purge: uc2_node::PurgePolicy::Disabled,
        journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
    }
}

fn wait_until(mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "condition never held");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn hundred_submits_in_order_then_two_concurrent_clients_stay_monotone_with_distinct_ids() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(node_config(dir.path(), "client-test")).unwrap();
    wait_until(|| node.can_serve());

    let svc = ServiceBuilder::new(ServiceConfig::new(dir.path(), "client-test"), CountSm::default())
        .start()
        .unwrap();

    // --- Step 1: one client, 100 sequential submits, exact totals 1..=100.
    let client_a = Client::connect(dir.path(), "client-test").unwrap();
    for expected in 1..=100u64 {
        let got: u64 = client_a.submit(&Cmd::Add(1)).unwrap();
        assert_eq!(got, expected, "apply order must match submission order for a solo client");
    }

    // --- Step 2: a second client gets a distinct client_id.
    let client_b = Client::connect(dir.path(), "client-test").unwrap();
    assert_ne!(client_a.client_id(), client_b.client_id(), "client_ids must differ");

    // --- Step 3: both clients submit 100 more, concurrently; each client's
    // OWN sequence of returned totals must be strictly increasing even
    // though the total itself is shared and the two clients' operations
    // interleave arbitrarily in the global commit order.
    let a_handle = std::thread::spawn(move || {
        let mut seen = Vec::with_capacity(100);
        for _ in 0..100u64 {
            seen.push(client_a.submit::<Cmd, u64>(&Cmd::Add(1)).unwrap());
        }
        (client_a, seen)
    });
    let b_handle = std::thread::spawn(move || {
        let mut seen = Vec::with_capacity(100);
        for _ in 0..100u64 {
            seen.push(client_b.submit::<Cmd, u64>(&Cmd::Add(1)).unwrap());
        }
        (client_b, seen)
    });

    let (client_a, seen_a) = a_handle.join().unwrap();
    let (client_b, seen_b) = b_handle.join().unwrap();

    for (name, seen) in [("a", &seen_a), ("b", &seen_b)] {
        assert_eq!(seen.len(), 100, "client {name} must see exactly 100 responses");
        assert!(
            seen.windows(2).all(|w| w[0] < w[1]),
            "client {name}'s own response sequence must be strictly increasing: {seen:?}"
        );
    }

    // NOTE: deliberately not exercising query_snapshot/query_linearizable
    // here — the node's query-answering path (read-index barrier, svc_query
    // wiring) lands in Task 11. Per the Task 10 brief: keep this capstone to
    // submits only.

    client_a.shutdown();
    client_b.shutdown();
    svc.stop();
    node.stop();
}
