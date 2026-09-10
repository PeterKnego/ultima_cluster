// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Jumbo spec §10 (fault-layer tier): discovery lands on exactly the rung a
//! capped path carries, and on the top rung when nothing caps it.

mod common;

use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use uc_log::agent::{AgentRunner, IdleStrategy};
use uc_net::fault::{FaultConfig, FaultSocket};
use uc_net::probe::{ProbeCadence, ProbeTable};
use uc_net::receiver::{FollowerConfig, FollowerReceiver};
use uc_net::sender::{Sender, SenderConfig};
use uc_protocol::v2::datagram::MTU_BOUND;

/// One node: a sender (always-follower here, so it only probes) and a
/// receiver on the same socket, sharing one table.
struct Peer {
    addr: SocketAddr,
    table: Arc<ProbeTable>,
    _agents: Vec<AgentRunner>,
}

fn spawn_peer(name: &str, faults: FaultConfig) -> Peer {
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr = raw.local_addr().unwrap();
    let mut send_sock = FaultSocket::from_socket(raw.try_clone().unwrap()).unwrap();
    let mut recv_sock = FaultSocket::from_socket(raw).unwrap();
    send_sock.set_faults(faults);
    recv_sock.set_faults(faults);
    let table = ProbeTable::new(ProbeCadence {
        fast_ns: 20_000_000, // 20 ms
        fast_attempts: 5,
        slow_ns: 200_000_000,
    });
    let buffer = common::buffer();
    let (_ctrl_tx, ctrl_rx) = mpsc::sync_channel(64);
    let term = Arc::new(AtomicU32::new(common::TERM));
    let role = Arc::new(AtomicBool::new(false)); // never leads: probes only
    let mut sender = Sender::new(
        Arc::clone(&buffer),
        send_sock,
        Vec::new(),
        1,
        ctrl_rx,
        SenderConfig::new(common::TERM),
        Arc::clone(&term),
        role,
    );
    sender.set_probe_table(Arc::clone(&table));
    let mut receiver = FollowerReceiver::new(
        buffer,
        recv_sock,
        FollowerConfig::new(addr),
        term,
        common::unrouted_consensus(),
    );
    receiver.set_probe_table(Arc::clone(&table));
    let tx = AgentRunner::spawn(&format!("{name}-tx"), IdleStrategy::Yield, move || {
        sender.do_work()
    })
    .unwrap();
    let rx = AgentRunner::spawn(&format!("{name}-rx"), IdleStrategy::Yield, move || {
        receiver.do_work()
    })
    .unwrap();
    Peer {
        addr,
        table,
        _agents: vec![tx, rx],
    }
}

fn await_verified(p: &Peer, peer: SocketAddr, want: u32, secs: u64) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let got = p.table.get(peer).map(|e| e.verified).unwrap_or(0);
        if got == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "verified to {peer} = {got}, wanted {want}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn an_uncapped_loopback_path_verifies_the_top_rung_both_ways() {
    let a = spawn_peer("a", FaultConfig::default());
    let b = spawn_peer("b", FaultConfig::default());
    a.table.set_peers(&[b.addr]);
    b.table.set_peers(&[a.addr]);
    await_verified(&a, b.addr, MTU_BOUND as u32, 5);
    await_verified(&b, a.addr, MTU_BOUND as u32, 5);
    assert_eq!(a.table.own_min_rung(), MTU_BOUND as u32);
    // b's ack carried its own minimum, so a's table knows b's view too.
    let deadline = Instant::now() + Duration::from_secs(5);
    while a.table.get(b.addr).unwrap().advertised != MTU_BOUND as u32 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(a.table.table_min(&[b.addr]), Some(MTU_BOUND as u32));
}

#[test]
fn a_path_capped_at_8832_verifies_exactly_8832() {
    let cap = FaultConfig {
        max_datagram: 8832,
        ..FaultConfig::default()
    };
    let a = spawn_peer("a", cap);
    let b = spawn_peer("b", cap);
    a.table.set_peers(&[b.addr]);
    b.table.set_peers(&[a.addr]);
    await_verified(&a, b.addr, 8832, 5);
    await_verified(&b, a.addr, 8832, 5);
    // Give the ladder two more attempts: 8960 must never land.
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(a.table.get(b.addr).unwrap().verified, 8832);
    assert_eq!(a.table.own_min_rung(), 8832);
}

/// The cap applies to what THIS side sends. A narrow path in one direction
/// only still caps the pair's minimum: a's probes to b are dropped above
/// 1408, so b credits a at 1408 and advertises 1408 back.
#[test]
fn an_asymmetric_cap_shows_up_in_the_advertised_minimum() {
    let a = spawn_peer(
        "a",
        FaultConfig {
            max_datagram: 1408,
            ..FaultConfig::default()
        },
    );
    let b = spawn_peer("b", FaultConfig::default());
    a.table.set_peers(&[b.addr]);
    b.table.set_peers(&[a.addr]);
    await_verified(&b, a.addr, MTU_BOUND as u32, 5); // b → a is wide
    await_verified(&a, b.addr, 1408, 5); // a → b is narrow
    let deadline = Instant::now() + Duration::from_secs(5);
    while b.table.get(a.addr).unwrap().advertised != 1408 {
        assert!(Instant::now() < deadline, "b never learned a's minimum");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(b.table.table_min(&[a.addr]), Some(1408));
}
