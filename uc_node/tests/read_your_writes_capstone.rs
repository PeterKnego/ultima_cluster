// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Read-your-writes capstone: workers write through the leader and read their
//! own writes from random nodes while the leader is repeatedly isolated. The
//! session checker must find no violation. `UC2_RYW_TOOTH` selects a mutation
//! tooth (see `scripts/ryw_mutation.sh`). Harness copied from
//! `read_your_writes.rs`.

use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use uc_client::Client;
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

/// Cut every link between nodes `a` and `b` (both send directions).
fn cut(nodes: &[Node], a: usize, b: usize, members: &[(u32, SocketAddr)]) {
    for h in nodes[a].partition_handles() {
        h.block(members[b].1);
    }
    for h in nodes[b].partition_handles() {
        h.block(members[a].1);
    }
}

fn heal(nodes: &[Node], a: usize, b: usize, members: &[(u32, SocketAddr)]) {
    for h in nodes[a].partition_handles() {
        h.unblock(members[b].1);
    }
    for h in nodes[b].partition_handles() {
        h.unblock(members[a].1);
    }
}

/// UC2_RYW_TOOTH: unset -> the checker must be clean; "T1" (node skips the
/// wait, client guard off) -> it must find a violation; "T2" (node skips the
/// wait, guard on) -> it must be clean AND the guard must have fired.
#[test]
fn ryw_capstone_under_leader_churn() {
    let tooth = std::env::var("UC2_RYW_TOOTH").ok();
    let workers_n: u64 = std::env::var("UC2_RYW_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    let secs: u64 = std::env::var("UC2_RYW_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(15);
    let mut c = spawn_cluster(3);
    let _ = await_single_leader(&c.nodes, 30);
    let svcs = start_services(&c);
    let checker = std::sync::Arc::new(std::sync::Mutex::new(
        uc_lincheck::session::SessionChecker::new(),
    ));
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stale = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let reads = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

    let workers: Vec<_> = (0..workers_n)
        .map(|w| {
            let dirs = c.dirs.clone();
            let (checker, stop, stale, reads) =
                (checker.clone(), stop.clone(), stale.clone(), reads.clone());
            std::thread::spawn(move || {
                let readers: Vec<Client> = dirs
                    .iter()
                    .map(|d| Client::connect(d, APP).unwrap())
                    .collect();
                let mut writer: Option<(usize, Client)> = None;
                let mut i = 0usize;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    // Find (or re-find after a failover) a client on the leader.
                    if writer.is_none() {
                        for (k, d) in dirs.iter().enumerate() {
                            if let Ok(cl) = Client::connect(d, APP) {
                                if cl.query_linearizable::<(), u64>(&()).is_ok() {
                                    writer = Some((k, cl));
                                    break;
                                }
                                cl.shutdown();
                            }
                        }
                        if writer.is_none() {
                            std::thread::sleep(Duration::from_millis(20));
                            continue;
                        }
                    }
                    let (_, wc) = writer.as_ref().unwrap();
                    let total: u64 = match wc.submit(&Cmd::Add(1)) {
                        Ok(t) => t,
                        Err(_) => {
                            if let Some((_, old)) = writer.take() {
                                old.shutdown();
                            }
                            continue;
                        }
                    };
                    checker.lock().unwrap().record_write_ack(w, total);
                    let token = wc.read_token();
                    let r = &readers[i % readers.len()];
                    i += 1;
                    r.observe(token);
                    let before = r.stats().stale_answers;
                    if let Ok(v) = r.query_read_your_writes::<(), u64>(&()) {
                        reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let _ = checker.lock().unwrap().record_read(w, v);
                    } // Err: RETRY / timeout: no answer, nothing to check
                    stale.fetch_add(
                        r.stats().stale_answers - before,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
                for r in readers {
                    r.shutdown();
                }
                if let Some((_, wc)) = writer {
                    wc.shutdown();
                }
            })
        })
        .collect();

    // Churn: isolate the current leader for 600 ms every 1.5 s.
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(secs) {
        std::thread::sleep(Duration::from_millis(1500));
        if let Some(l) = (0..3).find(|&i| c.nodes[i].can_serve()) {
            for f in (0..3).filter(|&i| i != l) {
                cut(&c.nodes, l, f, &c.members);
            }
            std::thread::sleep(Duration::from_millis(600));
            for f in (0..3).filter(|&i| i != l) {
                heal(&c.nodes, l, f, &c.members);
            }
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for w in workers {
        w.join().unwrap();
    }
    let violations = checker.lock().unwrap().violations().to_vec();
    let stale = stale.load(std::sync::atomic::Ordering::Relaxed);
    let reads = reads.load(std::sync::atomic::Ordering::Relaxed);
    println!(
        "ryw capstone: reads={reads} violations={} stale_answers={stale}",
        violations.len()
    );
    for s in svcs {
        s.stop();
    }
    for n in c.nodes.drain(..) {
        n.stop();
    }
    assert!(
        reads > 100,
        "too few answered reads ({reads}) to judge anything"
    );
    match tooth.as_deref() {
        None => assert!(violations.is_empty(), "RYW VIOLATION: {violations:?}"),
        Some("T1") => assert!(
            !violations.is_empty(),
            "tooth T1 not caught: the capstone has no teeth"
        ),
        Some("T2") => {
            assert!(violations.is_empty(), "guard on, yet: {violations:?}");
            assert!(stale > 0, "tooth T2: the guard never fired");
        }
        Some(other) => panic!("unknown UC2_RYW_TOOTH {other:?}"),
    }
}
