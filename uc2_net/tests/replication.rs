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

mod common;

use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::*;
use uc2_net::fault::{FaultConfig, FaultSocket};

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
