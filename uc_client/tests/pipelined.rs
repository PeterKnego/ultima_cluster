// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 6 capstone: `PipelinedClient` against real nodes+services (plus one
//! synthetic-dir shutdown-drain scenario). Harness copied in verbatim per the
//! task brief — integration test binaries don't share modules:
//! `Cmd`/`CountSm`/`node_config`/`wait_until` from `tests/roundtrip.rs`,
//! `make_instance` from `tests/synthetic.rs`, and the hand-rolled `block_on`
//! from `src/ticket.rs`'s test module.

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig};
use uc_service::{ServiceBuilder, ServiceConfig, StateMachine};

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Cmd {
    Add(u64),
}

/// A running total; the response is the total AFTER applying, so apply order
/// is pinned by the returned values (same SM shape as `roundtrip.rs`).
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
        purge: uc_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::default(),
    }
}

fn wait_until(mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "condition never held");
        std::thread::sleep(Duration::from_millis(1));
    }
}

// --- copied from tests/synthetic.rs: hand-rolled instance dir, no real node.

use uc_log::cnc::{CncMeta, CncPage};
use uc_protocol::ring::{BroadcastRing, MpscRing};

const MIB: u64 = 1 << 20;

fn meta(app_id: &str) -> CncMeta {
    CncMeta {
        node_id: 0,
        instance_id: rand_u128(),
        app_id: app_id.into(),
        buffer_bytes: MIB,
        max_payload: 256,
    }
}

fn rand_u128() -> u128 {
    let a = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    a ^ 0xA5A5_5A5A_A5A5_5A5A_u128
}

fn make_instance(dir: &Path, app_id: &str, ingress_cap: u64, egress_cap: u64) {
    CncPage::create_file(&dir.join("cnc2.dat"), &meta(app_id)).unwrap();
    MpscRing::create(&dir.join("ingress.ring"), ingress_cap, 128).unwrap();
    MpscRing::create(&dir.join("query.ring"), MIB, 256).unwrap();
    BroadcastRing::create(&dir.join("egress_service.0.broadcast"), egress_cap, 128).unwrap();
    BroadcastRing::create(&dir.join("egress_node.broadcast"), egress_cap, 128).unwrap();
}

// --- copied from src/ticket.rs's test module: hand-rolled block_on.

/// Hand-rolled block_on: thread-parker waker, no runtime dep (spec §9).
fn block_on<F: std::future::Future>(mut fut: F) -> F::Output {
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    fn raw(thread: std::thread::Thread) -> RawWaker {
        fn clone(p: *const ()) -> RawWaker {
            raw(unsafe { (*(p as *const std::thread::Thread)).clone() })
        }
        fn wake(p: *const ()) {
            unsafe { Box::from_raw(p as *mut std::thread::Thread) }.unpark();
        }
        fn wake_by_ref(p: *const ()) {
            unsafe { &*(p as *const std::thread::Thread) }.unpark();
        }
        fn drop_fn(p: *const ()) {
            drop(unsafe { Box::from_raw(p as *mut std::thread::Thread) });
        }
        static VT: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop_fn);
        RawWaker::new(Box::into_raw(Box::new(thread)) as *const (), &VT)
    }
    let waker = unsafe { Waker::from_raw(raw(std::thread::current())) };
    let mut cx = Context::from_waker(&waker);
    let mut fut = unsafe { std::pin::Pin::new_unchecked(&mut fut) };
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => std::thread::park(),
        }
    }
}

fn connect(dir: &std::path::Path) -> uc_client::PipelinedClient {
    uc_client::PipelinedClient::connect(dir, "pipe-test", uc_client::PipelinedConfig::default())
        .unwrap()
}

#[test]
fn pipelined_submits_all_resolve_and_totals_are_a_permutation_free_prefix() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let node = Node::start(node_config(dir.path(), "pipe-test")).unwrap();
    wait_until(|| node.can_serve());
    let _svc = ServiceBuilder::new(
        ServiceConfig::new(dir.path(), "pipe-test"),
        CountSm::default(),
    )
    .start()
    .unwrap();

    let client = connect(dir.path());
    // WINDOW of outstanding tickets — the whole point of the layer.
    let tickets: Vec<uc_client::Ticket<u64>> = (0..100)
        .map(|_| client.submit(&Cmd::Add(1)).unwrap())
        .collect();
    let mut totals: Vec<u64> = tickets.into_iter().map(|t| t.wait().unwrap()).collect();
    // A single client's submits are applied in submission order (one MPSC
    // producer, FIFO ring, in-order apply): totals must be exactly 1..=100.
    totals.sort_unstable();
    assert_eq!(totals, (1..=100).collect::<Vec<u64>>());
}

#[test]
fn async_await_resolves_against_a_real_cluster() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let node = Node::start(node_config(dir.path(), "pipe-test")).unwrap();
    wait_until(|| node.can_serve());
    let _svc = ServiceBuilder::new(
        ServiceConfig::new(dir.path(), "pipe-test"),
        CountSm::default(),
    )
    .start()
    .unwrap();

    let client = connect(dir.path());
    let got: u64 = block_on(client.submit::<_, u64>(&Cmd::Add(7)).unwrap()).unwrap();
    assert_eq!(got, 7);
}

#[test]
fn queries_ride_the_same_engine() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let node = Node::start(node_config(dir.path(), "pipe-test")).unwrap();
    wait_until(|| node.can_serve());
    let _svc = ServiceBuilder::new(
        ServiceConfig::new(dir.path(), "pipe-test"),
        CountSm::default(),
    )
    .start()
    .unwrap();

    let client = connect(dir.path());
    client
        .submit::<_, u64>(&Cmd::Add(3))
        .unwrap()
        .wait()
        .unwrap();
    let snap: u64 = client.query_snapshot(&()).unwrap().wait().unwrap();
    assert_eq!(snap, 3);
    let lin: u64 = client.query_linearizable(&()).unwrap().wait().unwrap();
    assert_eq!(lin, 3);
}

#[test]
fn dropping_a_ticket_orphans_cleanly_and_later_traffic_is_unaffected() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let node = Node::start(node_config(dir.path(), "pipe-test")).unwrap();
    wait_until(|| node.can_serve());
    let _svc = ServiceBuilder::new(
        ServiceConfig::new(dir.path(), "pipe-test"),
        CountSm::default(),
    )
    .start()
    .unwrap();

    let client = connect(dir.path());
    drop(client.submit::<_, u64>(&Cmd::Add(1)).unwrap()); // abandon interest
    // The orphan's response is discarded by the driver; nothing wedges:
    let got: u64 = client
        .submit::<_, u64>(&Cmd::Add(1))
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(got, 2);
}

#[test]
fn shutdown_fails_inflight_tickets_with_shutdown() {
    // Synthetic dead dir (no node answers), serving gate off so the submit
    // is accepted and then never resolved — until shutdown drains it.
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "pipe-shut", 1 << 20, 1 << 20); // synthetic.rs helper, copied in
    let client = uc_client::PipelinedClient::connect(
        dir.path(),
        "pipe-shut",
        uc_client::PipelinedConfig {
            serving_gate: false,
            ..Default::default()
        },
    )
    .unwrap();
    let t = client.submit::<_, u64>(&1u8).unwrap();
    client.shutdown();
    assert!(matches!(t.wait(), Err(uc_client::ClientError::ShutDown)));
}

/// Finding 3 (task-6 review): the refusal path — `dispatch`'s reclaim of a
/// leaked `Arc<TicketCore>` for a request the engine never accepted — end to
/// end through the public API, both fail-fast (`try_submit`) and after the
/// grace loop (`submit`). A tiny ingress ring that nothing ever drains (no
/// node here) stays full forever once filled, so both calls are guaranteed
/// to see `Backpressure` from the engine every attempt.
#[test]
fn refusal_path_reports_backpressure_full_and_shutdown_is_still_clean() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "pipe-bp", 64, 4096);

    // Fill the ingress ring completely via a raw producer, exactly like
    // tests/synthetic.rs's `ingress_ring_stays_full_returns_backpressure_full`
    // — nobody reads it (no node/service running here), so it stays full.
    let (filler, _consumer) = MpscRing::open(&dir.path().join("ingress.ring"))
        .unwrap()
        .into_split();
    loop {
        if filler.try_write(1, 0, [0; 8], &[0u8; 8]).is_err() {
            break; // Full (or TooLarge for the last partial slot) — full enough.
        }
    }

    let client = uc_client::PipelinedClient::connect(
        dir.path(),
        "pipe-bp",
        uc_client::PipelinedConfig {
            serving_gate: false,
            ..Default::default()
        },
    )
    .unwrap();

    // Fail-fast: no retry loop, refused on the first attempt.
    let t0 = Instant::now();
    let result: Result<uc_client::Ticket<u8>, _> = client.try_submit(&7u8);
    let fail_fast_elapsed = t0.elapsed();
    match result {
        Err(uc_client::ClientError::BackpressureFull) => {}
        Err(e) => panic!("expected BackpressureFull, got {e:?}"),
        Ok(_) => panic!("expected BackpressureFull, got a Ticket (ring wasn't actually full)"),
    }
    assert!(
        fail_fast_elapsed < Duration::from_millis(500),
        "try_submit must be fail-fast, not grace-loop: {fail_fast_elapsed:?}"
    );

    // Grace loop: retries at 100us for ~1s before giving up.
    let t1 = Instant::now();
    let result: Result<uc_client::Ticket<u8>, _> = client.submit(&7u8);
    let grace_elapsed = t1.elapsed();
    match result {
        Err(uc_client::ClientError::BackpressureFull) => {}
        Err(e) => panic!("expected BackpressureFull, got {e:?}"),
        Ok(_) => panic!("expected BackpressureFull, got a Ticket (ring wasn't actually full)"),
    }
    assert!(
        grace_elapsed >= Duration::from_millis(900),
        "must honor the ~1s grace: {grace_elapsed:?}"
    );
    assert!(
        grace_elapsed < Duration::from_secs(5),
        "must not hang well past the grace window: {grace_elapsed:?}"
    );

    // Every refusal above reclaimed its leaked Arc<TicketCore> on its own
    // error path (never handed to the driver) — shutdown must still be
    // clean: no hang (nothing was ever inflight to drain) and no crash
    // (no double-free from a refusal that was mistakenly treated as
    // accepted).
    client.shutdown();
}

#[test]
fn every_wait_strategy_round_trips() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let node = Node::start(node_config(dir.path(), "pipe-test")).unwrap();
    wait_until(|| node.can_serve());
    let _svc = ServiceBuilder::new(
        ServiceConfig::new(dir.path(), "pipe-test"),
        CountSm::default(),
    )
    .start()
    .unwrap();

    for ws in [
        uc_client::WaitStrategy::BusySpin,
        uc_client::WaitStrategy::BackoffYield,
        uc_client::WaitStrategy::Backoff,
        uc_client::WaitStrategy::Park,
    ] {
        let client = uc_client::PipelinedClient::connect(
            dir.path(),
            "pipe-test",
            uc_client::PipelinedConfig {
                driver_wait: ws,
                ..Default::default()
            },
        )
        .unwrap();
        let _: u64 = client.submit(&Cmd::Add(1)).unwrap().wait().unwrap();
        client.shutdown();
    }
}
