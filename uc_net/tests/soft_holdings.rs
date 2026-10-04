// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Catalog spec §5 (Task 9): a follower's `STATUS` carries its `Holdings`,
//! and the leader's sender lands them in the shared soft table under the
//! follower's address.

mod common;

use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{spawn_follower_with_holdings, spawn_leader_with_soft};
use uc_net::SoftTableWire;
use uc_net::fault::{FaultConfig, FaultSocket};
use uc_protocol::v2::datagram::Holdings;

#[test]
fn a_followers_status_lands_its_holdings_in_the_leaders_soft_table() {
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let fsock = FaultSocket::bind("127.0.0.1:0").unwrap();
    let faddr = fsock.local_addr().unwrap();

    let holdings = Arc::new(Mutex::new(Holdings {
        journal_first: 7,
        sets_held: 0b10,
        ..Holdings::default()
    }));
    let soft: Arc<Mutex<SoftTableWire>> = Arc::new(Mutex::new(SoftTableWire::new()));

    let leader = spawn_leader_with_soft(
        raw,
        vec![faddr],
        FaultConfig::default(),
        Some(Arc::clone(&soft)),
    );
    let follower = spawn_follower_with_holdings(
        "f1",
        fsock,
        leader_addr,
        FaultConfig::default(),
        Some(Arc::clone(&holdings)),
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    let seen = loop {
        if let Some((h, at)) = soft.lock().unwrap().get(&faddr).copied() {
            break (h, at);
        }
        assert!(
            Instant::now() < deadline,
            "no STATUS from {faddr} reached the soft table"
        );
        std::thread::sleep(Duration::from_millis(5));
    };
    assert_eq!(seen.0.sets_held, 0b10);
    assert_eq!(seen.0.journal_first, 7);
    assert!(seen.1 > 0, "the entry is stamped with a receive time");
    assert_eq!(soft.lock().unwrap().len(), 1, "only the follower's address");

    // A later write to the cell rides the next STATUS.
    holdings.lock().unwrap().sets_held = 0b11;
    let deadline = Instant::now() + Duration::from_secs(10);
    while soft.lock().unwrap().get(&faddr).map(|e| e.0.sets_held) != Some(0b11) {
        assert!(Instant::now() < deadline, "the cell's update never shipped");
        std::thread::sleep(Duration::from_millis(5));
    }

    follower.node.stop();
    leader.node.stop();
}
