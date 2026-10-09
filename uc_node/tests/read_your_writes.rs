// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Read-your-writes on a real 3-node loopback cluster (harness copied from
//! `query_barrier.rs`): a follower read at a writer's token sees the writer's
//! acknowledged writes; snapshot reads pass the serving gate; a caught-up-less
//! follower parks then RETRYs; a forged token is refused at once.

use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use uc_client::{Client, ClientError};
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig};
use uc_service::{ApplyCtx, ServiceBuilder, ServiceConfig, StateMachine};

const APP: &str = "ryw";

// ------------------------------------------------------------- state machine

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Cmd {
    Add(u64),
}

#[derive(Default)]
struct CountSm {
    total: u64,
    last_applied: Option<u64>,
}

impl StateMachine for CountSm {
    const NAME: &'static str = "count";

    type Command = Cmd;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Cmd) -> u64 {
        let Cmd::Add(n) = cmd;
        self.total += n;
        self.last_applied = Some(ctx.position);
        self.total
    }
    fn query(&self, _q: ()) -> u64 {
        self.total
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}
impl uc_service::WholeStateSnapshot for CountSm {
    fn encode_state(&self) -> Result<Vec<u8>, uc_service::SnapshotError> {
        bincode::serde::encode_to_vec((self.total, self.last_applied), bincode::config::standard())
            .map_err(|e| uc_service::SnapshotError::Codec(e.to_string()))
    }
    fn decode_state(&mut self, bytes: &[u8]) -> Result<(), uc_service::SnapshotError> {
        let ((total, last_applied), _): ((u64, Option<u64>), usize) =
            bincode::serde::decode_from_slice(bytes, bincode::config::standard())
                .map_err(|e| uc_service::SnapshotError::Codec(e.to_string()))?;
        self.total = total;
        self.last_applied = last_applied;
        Ok(())
    }
}

// ------------------------------------------------------------------ harness

fn make_config(
    id: u32,
    members: Vec<(u32, SocketAddr)>,
    instance_dir: PathBuf,
    seed: u64,
    addr: SocketAddr,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        bind: addr,
        instance_dir,
        app_id: APP.into(),
        buffer_bytes: 1 << 22, // 4 MiB: no wrap within the test
        max_payload: 256,
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
        force_jumbo_frames: false,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed,
        faults: FaultConfig::default(),
        purge: uc_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::single(CountSm::NAME),
    }
}

struct Cluster {
    _dir: tempfile::TempDir,
    dirs: Vec<PathBuf>,
    #[allow(dead_code)]
    members: Vec<(u32, SocketAddr)>,
    nodes: Vec<Node>,
}

fn spawn_cluster(n: usize) -> Cluster {
    let dir = tempfile::Builder::new()
        .prefix("uc2-ryw-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    // Bind every socket first so the full member map is known before any agent.
    let socks: Vec<UdpSocket> = (0..n)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind"))
        .collect();
    let members: Vec<(u32, SocketAddr)> = socks
        .iter()
        .enumerate()
        .map(|(i, s)| (i as u32, s.local_addr().unwrap()))
        .collect();
    let mut dirs = Vec::with_capacity(n);
    let mut nodes = Vec::with_capacity(n);
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = members[i].1;
        let instance_dir = dir.path().join(format!("n{i}"));
        dirs.push(instance_dir.clone());
        let seed = 0xA1B2_C3D4_5566_7788 ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let cfg = make_config(i as u32, members.clone(), instance_dir, seed, addr);
        nodes.push(Node::start_with_socket(cfg, sock).expect("start"));
    }
    Cluster {
        _dir: dir,
        dirs,
        members,
        nodes,
    }
}

/// Wait for exactly one serving leader; assert no split-brain throughout.
fn await_single_leader(nodes: &[Node], secs: u64) -> usize {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let serving: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].can_serve()).collect();
        assert!(
            serving.len() <= 1,
            "split-brain: nodes {serving:?} all serve"
        );
        if serving.len() == 1 {
            assert!(
                nodes[serving[0]].is_leader(),
                "serving node not flagged leader"
            );
            return serving[0];
        }
        assert!(Instant::now() < deadline, "no single leader elected");
        std::thread::yield_now();
    }
}

fn start_services(c: &Cluster) -> Vec<uc_service::Service<CountSm>> {
    c.dirs
        .iter()
        .map(|d| {
            ServiceBuilder::new(ServiceConfig::new(d, APP), CountSm::default())
                .start()
                .unwrap()
        })
        .collect()
}

/// RETRY is the documented answer of a node that is briefly behind; a caller
/// retries. Bounded so a real failure still fails.
fn ryw_read(client: &Client, within: Duration) -> Result<u64, ClientError> {
    let deadline = Instant::now() + within;
    loop {
        match client.query_read_your_writes::<(), u64>(&()) {
            Err(ClientError::Retry) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(2))
            }
            other => return other,
        }
    }
}

#[test]
fn a_follower_read_sees_the_writers_acknowledged_writes() {
    let mut c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 30);
    let svcs = start_services(&c);
    let follower = (leader + 1) % 3;
    let writer = Client::connect(&c.dirs[leader], APP).unwrap();
    let reader = Client::connect(&c.dirs[follower], APP).unwrap();
    let mut last = 0;
    for i in 1..=50u64 {
        let total: u64 = writer.submit(&Cmd::Add(1)).unwrap();
        assert_eq!(total, i);
        last = total;
        reader.observe(writer.read_token());
        let seen = ryw_read(&reader, Duration::from_secs(5)).unwrap();
        assert!(
            seen >= total,
            "follower read {seen} after write acknowledged at {total}"
        );
    }
    // Explicit-token API (ruling R3), same bounded RETRY retry.
    let token = writer.read_token();
    let deadline = Instant::now() + Duration::from_secs(5);
    let seen: u64 = loop {
        match reader.query_at_least_on::<(), u64>(0, &(), token) {
            Err(ClientError::Retry) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(2))
            }
            other => break other.unwrap(),
        }
    };
    assert!(seen >= last, "explicit-token read {seen} < {last}");
    writer.shutdown();
    reader.shutdown();
    for s in svcs {
        s.stop();
    }
    for n in c.nodes.drain(..) {
        n.stop();
    }
}

#[test]
fn a_default_client_on_a_follower_can_snapshot_read() {
    let mut c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 30);
    let svcs = start_services(&c);
    let follower = (leader + 1) % 3;
    let reader = Client::connect(&c.dirs[follower], APP).unwrap();
    let _: u64 = reader
        .query_snapshot(&())
        .expect("snapshot reads are any-node reads: the serving gate lets them through");
    reader.shutdown();
    for s in svcs {
        s.stop();
    }
    for n in c.nodes.drain(..) {
        n.stop();
    }
}

#[test]
fn a_follower_without_its_service_parks_then_retries_at_the_deadline() {
    let mut c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 30);
    let mut svcs = start_services(&c);
    let follower = (leader + 1) % 3;
    let writer = Client::connect(&c.dirs[leader], APP).unwrap();
    let reader = Client::connect(&c.dirs[follower], APP).unwrap();
    let _: u64 = writer.submit(&Cmd::Add(1)).unwrap();
    // Stop the follower's service: its node keeps receiving (durable moves),
    // its applied frontier does not.
    svcs.remove(follower).stop();
    let _: u64 = writer.submit(&Cmd::Add(1)).unwrap();
    let token = writer.read_token();
    // Wait until the follower holds the bytes, so the read parks rather than
    // being refused as "ahead".
    let cnc = uc_log::cnc::CncPage::open_file(&c.dirs[follower].join("cnc2.dat"), APP).unwrap();
    let t0 = Instant::now();
    while cnc.counters().durable.load_acquire() < token.as_u64() {
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "follower never received the write"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    reader.observe(token);
    let started = Instant::now();
    let res = reader.query_read_your_writes::<(), u64>(&());
    let waited = started.elapsed();
    assert!(matches!(res, Err(ClientError::Retry)), "got {res:?}");
    assert!(
        waited >= Duration::from_millis(800),
        "answered after {waited:?}: it did not park"
    );
    assert!(waited < Duration::from_secs(5));
    // Restart the service: the same token is now answered.
    let svc = ServiceBuilder::new(
        ServiceConfig::new(&c.dirs[follower], APP),
        CountSm::default(),
    )
    .start()
    .unwrap();
    let seen = ryw_read(&reader, Duration::from_secs(10)).unwrap();
    assert!(seen >= 2);
    writer.shutdown();
    reader.shutdown();
    svc.stop();
    for s in svcs {
        s.stop();
    }
    for n in c.nodes.drain(..) {
        n.stop();
    }
}

#[test]
fn a_forged_token_is_refused_at_once() {
    let mut c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 30);
    let svcs = start_services(&c);
    let follower = (leader + 1) % 3;
    let reader = Client::connect(&c.dirs[follower], APP).unwrap();
    reader.observe(uc_client::ReadToken::from_u64(u64::MAX));
    let started = Instant::now();
    let res = reader.query_read_your_writes::<(), u64>(&());
    assert!(matches!(res, Err(ClientError::Retry)), "got {res:?}");
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "a forged token must not park"
    );
    let stats = c.nodes[follower].observability().min_position;
    assert!(
        stats
            .refused_ahead
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 1
    );
    assert_eq!(stats.parked.load(std::sync::atomic::Ordering::Relaxed), 0);
    reader.shutdown();
    for s in svcs {
        s.stop();
    }
    for n in c.nodes.drain(..) {
        n.stop();
    }
}

/// Smoke, not a gate (dev box): a flood of forged (`u64::MAX`) and
/// at-`durable` tokens against a follower must not stall commit and must
/// never park more than the cap (spec 2026-10-08 §6.4).
#[test]
#[ignore = "smoke: run explicitly; a busy dev box can starve the writer for reasons unrelated to reads"]
fn token_floods_leave_commit_progress_and_the_parked_cap_intact() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use uc_protocol::ring::MpscRing;
    use uc_protocol::v2::ipc::{
        FLAG_V2_MIN_POSITION, MSG_V2_QUERY, extra_client, write_min_position_query_payload,
    };
    let mut c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 30);
    let mut svcs = start_services(&c);
    let follower = (leader + 1) % 3;
    let writer = Client::connect(&c.dirs[leader], APP).unwrap();
    let commits_in = |secs: u64| {
        let t0 = Instant::now();
        let mut n = 0u64;
        while t0.elapsed() < Duration::from_secs(secs) {
            let _: u64 = writer.submit(&Cmd::Add(1)).unwrap();
            n += 1;
        }
        n
    };
    let quiet = commits_in(3);
    // Freeze the follower's applied frontier (its durable keeps climbing), so
    // every at-durable token parks and the cap is actually exercised.
    svcs.remove(follower).stop();
    let _: u64 = writer.submit(&Cmd::Add(1)).unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let stats = c.nodes[follower].observability().min_position;
    let peak = Arc::new(AtomicU64::new(0));
    let sampler = {
        let (stop, stats, peak) = (stop.clone(), stats.clone(), peak.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                peak.fetch_max(stats.parked.load(Ordering::Relaxed), Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(10));
            }
        })
    };
    let flooders: Vec<_> = (0..8u32)
        .map(|k| {
            let dir = c.dirs[follower].clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let (p, _c) = MpscRing::open(&dir.join("query.ring"))
                    .unwrap()
                    .into_split();
                let cnc = uc_log::cnc::CncPage::open_file(&dir.join("cnc2.dat"), APP).unwrap();
                let q = bincode::serde::encode_to_vec((), bincode::config::standard()).unwrap();
                let mut payload = Vec::new();
                let (mut seq, mut forged, mut durable_sent) = (0u32, 0u64, 0u64);
                while !stop.load(Ordering::Relaxed) {
                    let token = if seq % 2 == 0 {
                        u64::MAX
                    } else {
                        cnc.counters().durable.load_acquire()
                    };
                    write_min_position_query_payload(0, token, &q, &mut payload);
                    let extra = extra_client(0x7000_0000 + k, seq);
                    if p.try_write(MSG_V2_QUERY, FLAG_V2_MIN_POSITION, extra, &payload)
                        .is_ok()
                    {
                        if token == u64::MAX {
                            forged += 1;
                        } else {
                            durable_sent += 1;
                        }
                        seq = seq.wrapping_add(1);
                    }
                }
                (forged, durable_sent)
            })
        })
        .collect();
    let loaded = commits_in(3);
    stop.store(true, Ordering::Relaxed);
    let (mut forged, mut durable_sent) = (0u64, 0u64);
    for h in flooders {
        let (f, d) = h.join().unwrap();
        forged += f;
        durable_sent += d;
    }
    sampler.join().unwrap();
    // Let the node drain the ring: poll until every forged token is counted.
    let t0 = Instant::now();
    while stats.refused_ahead.load(Ordering::Relaxed) < forged
        && t0.elapsed() < Duration::from_secs(5)
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    println!(
        "commits: quiet={quiet} under-flood={loaded}; forged sent={forged}; at-durable sent={durable_sent}; refused_cap={}; peak parked={}",
        stats.refused_cap.load(Ordering::Relaxed),
        peak.load(Ordering::Relaxed)
    );
    assert!(
        loaded * 2 >= quiet,
        "commit rate fell below half under the flood"
    );
    assert!(
        peak.load(Ordering::Relaxed) > 0,
        "parking was never exercised"
    );
    assert!(
        stats.refused_cap.load(Ordering::Relaxed) > 0,
        "the cap was never hit"
    );
    assert!(
        peak.load(Ordering::Relaxed) <= uc_node::min_position::MAX_PARKED_MIN_POSITION_READS as u64
    );
    assert!(
        stats.refused_ahead.load(Ordering::Relaxed) >= forged,
        "every forged token refused, none parked"
    );
    writer.shutdown();
    for s in svcs {
        s.stop();
    }
    for n in c.nodes.drain(..) {
        n.stop();
    }
}
