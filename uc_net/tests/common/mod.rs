// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Shared 3-node harness for uc_net integration tests. Each test binary
//! compiles this module separately, so items unused by one binary are
//! expected — hence the file-level allow.
#![allow(dead_code)]

use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use uc_journal::Journal;
use uc_log::agent::{AgentRunner, IdleStrategy};
use uc_log::archive::{Archive, ArchiveConfig, ReplayFrame};
use uc_log::buffer::{AppendError, Appender, LogBuffer};
use uc_log::cnc::{CncMeta, CncPage};
use uc_log::counters::PaddedAtomicU64;
use uc_log::region::Region;
use uc_net::fault::{FaultConfig, FaultSocket};
use uc_net::rebuild::NakConfig;
use uc_net::receiver::{FollowerConfig, FollowerReceiver, NetEvent};
use uc_net::sender::{Sender, SenderConfig};

/// A channel end for the consensus route that nothing drains (this harness has
/// no `ElectionSm` — the wire-level tests here don't exercise commit ranking,
/// which is entirely the consensus agent's job now, M4 carry #5). A dropped
/// receiver just makes `try_send` fail `Disconnected`, which the receiver
/// treats identically to a full channel (counted, never a hang or panic).
fn unrouted_consensus() -> mpsc::SyncSender<NetEvent> {
    let (tx, _rx) = mpsc::sync_channel(64);
    tx
}

pub const TERM: u32 = 3;
pub const CAP: u64 = 1 << 20; // 1 MiB buffers, identical on every node
pub const MAX_PAYLOAD: usize = 256;

/// Small segments so parallel test journals fit the quota'd tmpfs (M1 lesson).
pub fn test_cfg(dir: &std::path::Path) -> ArchiveConfig {
    ArchiveConfig {
        segment_size_bytes: 4 * 1024 * 1024,
        ..ArchiveConfig::new(dir)
    }
}

pub fn buffer() -> Arc<LogBuffer> {
    let cnc = CncPage::heap(&CncMeta {
        node_id: 0,
        instance_id: 0,
        app_id: "test".into(),
        buffer_bytes: CAP,
        max_payload: MAX_PAYLOAD as u32,
    });
    Arc::new(LogBuffer::new(
        Region::heap_zeroed(CAP as usize),
        cnc,
        MAX_PAYLOAD,
    ))
}

pub struct Node {
    pub buffer: Arc<LogBuffer>,
    pub dir: tempfile::TempDir,
    pub agents: Vec<AgentRunner>,
}

impl Node {
    /// Join all agents (dropping their `Archive`s — required before the
    /// journal dir can be reopened for replay) and hand back the dir.
    pub fn stop(self) -> tempfile::TempDir {
        for a in self.agents {
            a.stop();
        }
        self.dir
    }
}

/// Spawn the archive agent AND hand back a shared handle to its journal (the
/// sender's NAK replay source, M4). The journal is extracted BEFORE the archive
/// moves into the agent closure — a follower ignores the handle; the leader
/// wires it into its sender via `set_replay_source`.
pub fn spawn_archive(
    name: &str,
    buffer: &Arc<LogBuffer>,
    dir: &std::path::Path,
) -> (AgentRunner, Arc<Journal>) {
    let mut archive = Archive::open(test_cfg(dir)).unwrap();
    let journal = archive.journal_arc();
    let b = Arc::clone(buffer);
    let runner = AgentRunner::spawn(name, IdleStrategy::Yield, move || {
        archive.do_work(&b).expect("archive fail-stop")
    })
    .unwrap();
    (runner, journal)
}

pub struct Follower {
    pub node: Node,
    pub stats: Arc<uc_net::receiver::FollowerStats>,
    pub addr: SocketAddr,
}

pub fn spawn_follower(name: &str, leader: SocketAddr, faults: FaultConfig) -> Follower {
    let sock = FaultSocket::bind("127.0.0.1:0").unwrap();
    spawn_follower_on(name, sock, leader, faults)
}

/// Build a follower on a PRE-BOUND socket (the M4 paused-follower test binds
/// the socket first — quiet, no ICMP — then joins the cluster far behind).
/// `spawn_follower` is the two-line wrapper that binds a fresh socket.
pub fn spawn_follower_on(
    name: &str,
    mut sock: FaultSocket,
    leader: SocketAddr,
    faults: FaultConfig,
) -> Follower {
    let addr = sock.local_addr().unwrap();
    sock.set_faults(faults);
    let buffer = buffer();
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = FollowerConfig::new(leader);
    cfg.seed = faults.seed.wrapping_add(addr.port() as u64);
    cfg.status_floor_ns = 5_000_000; // 5 ms: keep flow adverts fresh under test loads
    cfg.nak = NakConfig {
        delay_min_ns: 100_000,
        delay_max_ns: 500_000,
        backoff_ns: 2_000_000,
    };
    let term = Arc::new(AtomicU32::new(TERM));
    let mut rx = FollowerReceiver::new(Arc::clone(&buffer), sock, cfg, term, unrouted_consensus());
    let stats = rx.stats();
    let rxa = AgentRunner::spawn(&format!("{name}-rx"), IdleStrategy::Yield, move || {
        rx.do_work()
    })
    .unwrap();
    let (ara, _journal) = spawn_archive(&format!("{name}-ar"), &buffer, dir.path());
    Follower {
        node: Node {
            buffer,
            dir,
            agents: vec![rxa, ara],
        },
        stats,
        addr,
    }
}

pub struct Leader {
    pub node: Node,
    pub stats: Arc<uc_net::sender::SenderStats>,
}

/// The leader socket binds FIRST (followers need its address) — pass it in.
///
/// Wired the way the real `uc_node::Node` composes leader duty (M4 carry #5
/// deleted the separate M2/M3 `LeaderReceiver` actor): ONE socket, cloned
/// twice, backs both the `Sender`'s outbound side and a unified
/// `FollowerReceiver` whose `sender_route` demuxes inbound NAK/STATUS to the
/// sender's control channel. This harness has no `ElectionSm`, so the
/// consensus route goes to a sink nobody drains (`unrouted_consensus`) —
/// harmless, since none of these wire-level tests assert on commit ranking
/// (that property now lives entirely in `uc_consensus`/`uc_node`).
pub fn spawn_leader(raw: UdpSocket, followers: Vec<SocketAddr>, faults: FaultConfig) -> Leader {
    let buffer = buffer();
    let dir = tempfile::tempdir().unwrap();
    let self_addr = raw.local_addr().unwrap();
    let mut send_sock = FaultSocket::from_socket(raw.try_clone().unwrap()).unwrap();
    let mut recv_sock = FaultSocket::from_socket(raw).unwrap();
    send_sock.set_faults(faults);
    recv_sock.set_faults(faults);
    let (ctrl_tx, ctrl_rx) = mpsc::sync_channel(1024);
    let mut cfg = SenderConfig::new(TERM);
    cfg.heartbeat_ns = 2_000_000; // 2 ms: quick tail-loss detection in tests
    // Open the archive first so the sender can take its journal as the deep-NAK
    // replay source (M4) before the sender agent spawns.
    let (ara, journal) = spawn_archive("leader-ar", &buffer, dir.path());
    let term = Arc::new(AtomicU32::new(TERM));
    let role = Arc::new(AtomicBool::new(true)); // this harness is always-leader
    let mut sender = Sender::new(
        Arc::clone(&buffer),
        send_sock,
        followers,
        3,
        ctrl_rx,
        cfg,
        Arc::clone(&term),
        role,
    );
    sender.set_replay_source(journal);
    let stats = sender.stats();
    let txa =
        AgentRunner::spawn("leader-tx", IdleStrategy::Yield, move || sender.do_work()).unwrap();

    let rcfg = FollowerConfig::new(self_addr); // never actually followed from
    let mut receiver = FollowerReceiver::new(
        Arc::clone(&buffer),
        recv_sock,
        rcfg,
        term,
        unrouted_consensus(),
    );
    receiver.set_sender_route(ctrl_tx);
    let rxa = AgentRunner::spawn("leader-ctrl", IdleStrategy::Yield, move || {
        receiver.do_work()
    })
    .unwrap();
    Leader {
        node: Node {
            buffer,
            dir,
            agents: vec![txa, rxa, ara],
        },
        stats,
    }
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
pub fn load(leader: &Arc<LogBuffer>, live: &[&Arc<LogBuffer>], n_msgs: u64) -> u64 {
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
            if a.position() <= slowest.saturating_add(CAP / 2) {
                break;
            }
            assert!(Instant::now() < deadline, "followers never caught up");
            std::thread::yield_now();
        }
    }
    a.position()
}

pub fn await_pos(c: &PaddedAtomicU64, target: u64, what: &str) {
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

pub fn replayed(dir: &std::path::Path) -> Vec<ReplayFrame> {
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
pub fn converge_and_compare(leader: Leader, followers: Vec<Follower>, end: u64) {
    for f in &followers {
        await_pos(&f.node.buffer.counters().append, end, "follower append");
        await_pos(&f.node.buffer.counters().durable, end, "follower durable");
    }
    await_pos(
        &leader.node.buffer.counters().durable,
        end,
        "leader durable",
    );
    let ldir = leader.node.stop();
    let golden = replayed(ldir.path());
    assert!(!golden.is_empty());
    for f in followers {
        let fdir = f.node.stop();
        assert_eq!(
            replayed(fdir.path()),
            golden,
            "follower journal diverged from leader"
        );
    }
}
