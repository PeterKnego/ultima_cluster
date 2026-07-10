// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! End-to-end replication over real loopback UDP: a leader (appender +
//! sender + control demux + archive) streams to two followers (receiver +
//! archive) under injected faults. Convergence asserts positions AND journal
//! content: REPLAYED FRAME STREAMS are compared, not raw journal bytes —
//! block boundaries legitimately differ between nodes (poll timing) and
//! padding spans carry node-local stale bytes (replay skips padding).
//!
//! Timing-sensitive by nature (real sockets, real threads): all assertions
//! are eventual-convergence with hard deadlines — a hang is a red test, not
//! a stuck CI job (M1 T8c lesson).

use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::Ordering;
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use uc2_log::agent::{AgentRunner, IdleStrategy};
use uc2_log::archive::{Archive, ArchiveConfig, ReplayFrame};
use uc2_log::buffer::{AppendError, Appender, LogBuffer};
use uc2_log::counters::{LogCounters, PaddedAtomicU64};
use uc2_log::region::Region;
use uc2_net::fault::{FaultConfig, FaultSocket};
use uc2_net::rebuild::NakConfig;
use uc2_net::receiver::{FollowerConfig, FollowerReceiver, LeaderReceiver};
use uc2_net::sender::{Sender, SenderConfig};

const TERM: u32 = 3;
const CAP: u64 = 1 << 20; // 1 MiB buffers, identical on every node
const MAX_PAYLOAD: usize = 256;

/// Small segments so parallel test journals fit the quota'd tmpfs (M1 lesson).
fn test_cfg(dir: &std::path::Path) -> ArchiveConfig {
    ArchiveConfig { segment_size_bytes: 4 * 1024 * 1024, ..ArchiveConfig::new(dir) }
}

fn buffer() -> Arc<LogBuffer> {
    let counters = Arc::new(LogCounters::new());
    Arc::new(LogBuffer::new(Region::heap_zeroed(CAP as usize), counters, MAX_PAYLOAD))
}

struct Node {
    buffer: Arc<LogBuffer>,
    dir: tempfile::TempDir,
    agents: Vec<AgentRunner>,
}

impl Node {
    /// Join all agents (dropping their `Archive`s — required before the
    /// journal dir can be reopened for replay) and hand back the dir.
    fn stop(self) -> tempfile::TempDir {
        for a in self.agents {
            a.stop();
        }
        self.dir
    }
}

fn spawn_archive(name: &str, buffer: &Arc<LogBuffer>, dir: &std::path::Path) -> AgentRunner {
    let mut archive = Archive::open(test_cfg(dir)).unwrap();
    let b = Arc::clone(buffer);
    AgentRunner::spawn(name, IdleStrategy::Yield, move || {
        archive.do_work(&b).expect("archive fail-stop")
    })
    .unwrap()
}

struct Follower {
    node: Node,
    stats: Arc<uc2_net::receiver::FollowerStats>,
    addr: SocketAddr,
}

fn spawn_follower(name: &str, leader: SocketAddr, faults: FaultConfig) -> Follower {
    let mut sock = FaultSocket::bind("127.0.0.1:0").unwrap();
    let addr = sock.local_addr().unwrap();
    sock.set_faults(faults);
    let buffer = buffer();
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = FollowerConfig::new(TERM, leader);
    cfg.seed = faults.seed.wrapping_add(addr.port() as u64);
    cfg.status_floor_ns = 5_000_000; // 5 ms: keep flow adverts fresh under test loads
    cfg.nak = NakConfig { delay_min_ns: 100_000, delay_max_ns: 500_000, backoff_ns: 2_000_000 };
    let mut rx = FollowerReceiver::new(Arc::clone(&buffer), sock, cfg);
    let stats = rx.stats();
    let rxa = AgentRunner::spawn(&format!("{name}-rx"), IdleStrategy::Yield, move || rx.do_work())
        .unwrap();
    let ara = spawn_archive(&format!("{name}-ar"), &buffer, dir.path());
    Follower { node: Node { buffer, dir, agents: vec![rxa, ara] }, stats, addr }
}

struct Leader {
    node: Node,
    stats: Arc<uc2_net::sender::SenderStats>,
}

/// The leader socket binds FIRST (followers need its address) — pass it in.
fn spawn_leader(raw: UdpSocket, followers: Vec<SocketAddr>, faults: FaultConfig) -> Leader {
    let buffer = buffer();
    let dir = tempfile::tempdir().unwrap();
    let recv = raw.try_clone().unwrap();
    let mut send = FaultSocket::from_socket(raw).unwrap();
    send.set_faults(faults);
    let (tx, rx) = mpsc::sync_channel(1024);
    let mut cfg = SenderConfig::new(TERM);
    cfg.heartbeat_ns = 2_000_000; // 2 ms: quick tail-loss detection in tests
    let mut sender = Sender::new(Arc::clone(&buffer), send, followers, 3, rx, cfg);
    let stats = sender.stats();
    let txa =
        AgentRunner::spawn("leader-tx", IdleStrategy::Yield, move || sender.do_work()).unwrap();
    let mut lr = LeaderReceiver::new(recv, tx, TERM).unwrap();
    let lra =
        AgentRunner::spawn("leader-ctrl", IdleStrategy::Yield, move || lr.do_work()).unwrap();
    let ara = spawn_archive("leader-ar", &buffer, dir.path());
    Leader { node: Node { buffer, dir, agents: vec![txa, lra, ara] }, stats }
}

/// Append `n_msgs` 64 B messages, pacing admission against the LIVE followers
/// (the M2 stand-in for admission control): the appender never gets more than
/// `CAP/2` ahead of the slowest live follower's DURABLE position.
///
/// Why durable-paced and not `sender.sent`-paced: a follower advertises a flow
/// window of `durable + CAP` (its whole ring), so `sent` alone can reach
/// exactly `follower.durable + CAP` — zero ring headroom below the follower's
/// frontier. And since `sent <= append` always, no slack measured against
/// `sent` can buy headroom back. Pacing `append` (hence `sent <= append`)
/// against the follower's durable instead keeps the leader from ever using the
/// full window, so at least `CAP/2` of the leader's ring always sits below
/// every follower's frontier. That headroom is what makes an OS-dropped
/// loopback datagram recoverable: the gap is still inside the leader's ring
/// when the NAK arrives (M2 has no journal replay session — that is M4 — so a
/// gap that scrolls out of the ring is an unrecoverable wedge). `live` excludes
/// a deliberately-dead follower so quorum pacing is preserved.
fn load(leader: &Arc<LogBuffer>, live: &[&Arc<LogBuffer>], n_msgs: u64) -> u64 {
    let mut a = Appender::new(Arc::clone(leader), TERM);
    let deadline = Instant::now() + Duration::from_secs(60);
    for i in 0..n_msgs {
        loop {
            assert!(Instant::now() < deadline, "load timed out at msg {i}");
            match a.append(1, i, &[i as u8; 64]) {
                Ok(_) => break,
                Err(AppendError::WouldOverrun) => std::thread::yield_now(),
                Err(e) => panic!("{e}"),
            }
        }
        loop {
            let slowest = live
                .iter()
                .map(|b| b.counters().durable.load_acquire())
                .min()
                .unwrap_or(u64::MAX);
            if a.position() <= slowest + CAP / 2 {
                break;
            }
            assert!(Instant::now() < deadline, "followers never caught up");
            std::thread::yield_now();
        }
    }
    a.position()
}

fn await_pos(c: &PaddedAtomicU64, target: u64, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let v = c.load_acquire();
        if v >= target {
            return;
        }
        assert!(Instant::now() < deadline, "{what} stuck at {v} < {target}");
        std::thread::yield_now();
    }
}

fn replayed(dir: &std::path::Path) -> Vec<ReplayFrame> {
    let arch = Archive::open(test_cfg(dir)).unwrap();
    let mut r = arch.replay_from(0).unwrap();
    let mut out = Vec::new();
    while let Some(f) = r.next().unwrap() {
        out.push(f);
    }
    out
}

/// Wait until every node has append+durable at `end`, stop everything, and
/// assert every follower's replayed frame stream equals the leader's.
fn converge_and_compare(leader: Leader, followers: Vec<Follower>, end: u64) {
    for f in &followers {
        await_pos(&f.node.buffer.counters().append, end, "follower append");
        await_pos(&f.node.buffer.counters().durable, end, "follower durable");
    }
    await_pos(&leader.node.buffer.counters().durable, end, "leader durable");
    let ldir = leader.node.stop();
    let golden = replayed(ldir.path());
    assert!(!golden.is_empty());
    for f in followers {
        let fdir = f.node.stop();
        assert_eq!(replayed(fdir.path()), golden, "follower journal diverged from leader");
    }
}

#[test]
fn clean_stream_converges_and_journals_match() {
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let clean = FaultConfig::default();
    let f1 = spawn_follower("c-f1", leader_addr, clean);
    let f2 = spawn_follower("c-f2", leader_addr, clean);
    let leader = spawn_leader(raw, vec![f1.addr, f2.addr], clean);
    let end = load(&leader.node.buffer, &[&f1.node.buffer, &f2.node.buffer], 5_000);
    converge_and_compare(leader, vec![f1, f2], end);
}

#[test]
fn one_percent_loss_recovers_via_nak() {
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let f1 = spawn_follower("l-f1", leader_addr, FaultConfig::default());
    let f2 = spawn_follower("l-f2", leader_addr, FaultConfig::default());
    let (s1, s2) = (Arc::clone(&f1.stats), Arc::clone(&f2.stats));
    // 1% loss on the leader's send side (data AND heartbeats drop; the NAK
    // delay + backoff recover both mid-stream gaps and tail loss)
    let faults = FaultConfig { seed: 20_260_710, drop_per_million: 10_000, ..Default::default() };
    let leader = spawn_leader(raw, vec![f1.addr, f2.addr], faults);
    let sstats = Arc::clone(&leader.stats);
    let end = load(&leader.node.buffer, &[&f1.node.buffer, &f2.node.buffer], 5_000);
    converge_and_compare(leader, vec![f1, f2], end);
    let naks = s1.naks_sent.load(Ordering::Relaxed) + s2.naks_sent.load(Ordering::Relaxed);
    assert!(naks > 0, "1% loss must exercise the NAK path");
    assert!(sstats.naks_served.load(Ordering::Relaxed) > 0);
    assert_eq!(sstats.overruns.load(Ordering::Relaxed), 0, "no replay-needed under 1% loss");
}

#[test]
fn dup_and_reorder_converge() {
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let f1 = spawn_follower("d-f1", leader_addr, FaultConfig::default());
    let f2 = spawn_follower("d-f2", leader_addr, FaultConfig::default());
    let faults = FaultConfig {
        seed: 7,
        dup_per_million: 20_000,
        reorder_per_million: 20_000,
        ..Default::default()
    };
    let leader = spawn_leader(raw, vec![f1.addr, f2.addr], faults);
    let end = load(&leader.node.buffer, &[&f1.node.buffer, &f2.node.buffer], 5_000);
    // dups are dropped by position, reordering is absorbed by Rebuilt —
    // convergence + identical replay IS the assertion
    converge_and_compare(leader, vec![f1, f2], end);
}

#[test]
fn stale_term_stream_is_ignored() {
    use uc_protocol::v2::datagram::{
        DATAGRAM_HEADER_LEN, DGRAM_KIND_DATA, DatagramHeader, write_datagram_header,
    };
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let f1 = spawn_follower("s-f1", leader_addr, FaultConfig::default());
    let stats = Arc::clone(&f1.stats);
    let fbuf = Arc::clone(&f1.node.buffer);
    let faddr = f1.addr;
    let leader = spawn_leader(raw, vec![f1.addr], FaultConfig::default());
    let end = load(&leader.node.buffer, &[&f1.node.buffer], 1_000);
    await_pos(&fbuf.counters().append, end, "follower append");

    // a "previous leader" blasts stale-term DATA at fresh positions
    let mut ghost = FaultSocket::bind("127.0.0.1:0").unwrap();
    for _ in 0..3 {
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN + 96];
        write_datagram_header(
            &mut d,
            &DatagramHeader {
                position: end,
                leadership_term_id: TERM - 1,
                kind: DGRAM_KIND_DATA,
                flags: 0,
            },
        );
        ghost.send_to(&d, faddr).unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while stats.dropped_stale_term.load(Ordering::Relaxed) < 3 {
        assert!(Instant::now() < deadline, "stale datagrams never observed");
        std::thread::yield_now();
    }
    assert_eq!(fbuf.counters().append.load_acquire(), end, "stale term advanced the log");
    converge_and_compare(leader, vec![f1], end);
}

#[test]
fn dead_follower_does_not_stall_the_quorum() {
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let f1 = spawn_follower("q-f1", leader_addr, FaultConfig::default());
    // Follower B is a bound socket with NO agents: silent — no statuses, no
    // NAKs. Its flow limit stays at initial_window (64 KiB) forever. Keep the
    // socket alive so sends don't turn into ICMP noise.
    let dead = FaultSocket::bind("127.0.0.1:0").unwrap();
    let leader =
        spawn_leader(raw, vec![f1.addr, dead.local_addr().unwrap()], FaultConfig::default());
    // ~3 MiB stream: several times BOTH the dead follower's window and CAP —
    // only quorum pacing (3 nodes -> the faster follower) lets this finish.
    let end = load(&leader.node.buffer, &[&f1.node.buffer], 32_768);
    assert!(end >= 3 * CAP);
    // The proof: the leader's stream reaches `end` despite the silent
    // follower — quorum pacing (leader + f1) never waits on the dead node.
    // Eventual (the sender is an async agent and `load` paces to within
    // CAP/2 of `end`), with a deadline like every other wait here.
    await_pos(&leader.node.buffer.counters().sent, end, "leader sent (dead follower stalled quorum)");
    converge_and_compare(leader, vec![f1], end);
    drop(dead);
}
