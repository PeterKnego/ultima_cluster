// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! End-to-end quorum commit semantics (spec §6) over real loopback UDP:
//! commit = quorum-fsync'd, bounded by the leader's durable, monotonic;
//! minority failure tolerated; quorum loss stalls cleanly (no phantom
//! commits); forged/stale reports are inert. Same eventual-with-deadline
//! discipline as replication.rs.

mod common;

use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use common::*;
use uc2_net::fault::{FaultConfig, FaultSocket};
use uc_protocol::v2::datagram::{
    DATAGRAM_HEADER_LEN, DGRAM_KIND_APPEND_POSITION, DatagramHeader, write_datagram_header,
};

#[test]
fn commit_reaches_end_and_followers_learn_it() {
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let clean = FaultConfig::default();
    let f1 = spawn_follower("cm-f1", leader_addr, clean);
    let f2 = spawn_follower("cm-f2", leader_addr, clean);
    let (b1, b2) = (Arc::clone(&f1.node.buffer), Arc::clone(&f2.node.buffer));
    let leader = spawn_leader(raw, vec![f1.addr, f2.addr], clean);
    let end = load(&leader.node.buffer, &[&b1, &b2], 5_000);
    // the leader's commit reaches the full stream (quorum-fsync'd)...
    await_pos(&leader.node.buffer.counters().commit, end, "leader commit");
    // ...and never exceeds its own durable (spot check at the converged point)
    assert!(
        leader.node.buffer.counters().commit.load_acquire()
            <= leader.node.buffer.counters().durable.load_acquire()
    );
    // ...and the followers learn it via gossip
    await_pos(&b1.counters().commit, end, "f1 commit");
    await_pos(&b2.counters().commit, end, "f2 commit");
    converge_and_compare(leader, vec![f1, f2], end);
}

#[test]
fn minority_failure_does_not_stall_commit() {
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let f1 = spawn_follower("cm2-f1", leader_addr, FaultConfig::default());
    let b1 = Arc::clone(&f1.node.buffer);
    // follower B: bound socket, no agents — silent minority
    let dead = FaultSocket::bind("127.0.0.1:0").unwrap();
    let leader =
        spawn_leader(raw, vec![f1.addr, dead.local_addr().unwrap()], FaultConfig::default());
    let end = load(&leader.node.buffer, &[&b1], 5_000);
    // quorum = leader + f1: commit must reach end despite the dead follower
    await_pos(&leader.node.buffer.counters().commit, end, "leader commit (minority down)");
    await_pos(&b1.counters().commit, end, "f1 commit (minority down)");
    converge_and_compare(leader, vec![f1], end);
    drop(dead);
}

#[test]
fn quorum_loss_stalls_commit_cleanly() {
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let _ = leader_addr;
    // BOTH followers silent: the leader can fsync locally but must never
    // commit on its own durable alone — no phantom commits under quorum loss.
    let dead1 = FaultSocket::bind("127.0.0.1:0").unwrap();
    let dead2 = FaultSocket::bind("127.0.0.1:0").unwrap();
    let leader = spawn_leader(
        raw,
        vec![dead1.local_addr().unwrap(), dead2.local_addr().unwrap()],
        FaultConfig::default(),
    );
    // small load, unpaced (500 x 96 B = 48 KB < the dead initial window and
    // far below CAP, so the appender/sender never block)
    let end = load(&leader.node.buffer, &[], 500);
    await_pos(&leader.node.buffer.counters().durable, end, "leader durable (quorum lost)");
    // generous settle: commit must STAY at zero
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        leader.node.buffer.counters().commit.load_acquire(),
        0,
        "phantom commit under quorum loss"
    );
    let ldir = leader.node.stop();
    let _ = ldir;
    drop((dead1, dead2));
}

#[test]
fn forged_and_stale_reports_cannot_move_commit() {
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let f1 = spawn_follower("cm4-f1", leader_addr, FaultConfig::default());
    let b1 = Arc::clone(&f1.node.buffer);
    let dead = FaultSocket::bind("127.0.0.1:0").unwrap();
    let leader =
        spawn_leader(raw, vec![f1.addr, dead.local_addr().unwrap()], FaultConfig::default());
    let end = load(&leader.node.buffer, &[&b1], 1_000);
    await_pos(&leader.node.buffer.counters().commit, end, "leader commit");

    // ghost reports far beyond the stream: (a) correct term from an UNKNOWN
    // address -> ignored (no tracker slot); (b) stale term from anywhere ->
    // dropped at the demux. Commit must not move past `end` (which equals
    // quorum durable here) in either case.
    let mut ghost = FaultSocket::bind("127.0.0.1:0").unwrap();
    for term in [TERM, TERM - 1] {
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(
            &mut d,
            &DatagramHeader {
                position: end + (1 << 30),
                leadership_term_id: term,
                kind: DGRAM_KIND_APPEND_POSITION,
                flags: 0,
            },
        );
        ghost.send_to(&d, leader_addr).unwrap();
    }

    // The `assert_eq!(commit, end)` below is only INSURANCE, not the proof: in
    // this topology a single forged report can never reach the quorum rank (2nd
    // of {own, f1, ghost} still needs f1) and the tracker's bounded-by-own cap
    // holds commit at the leader's own durable regardless — so that assertion
    // would pass even if both guards were deleted AND even if the datagrams
    // never arrived. The actual PROOF that the guards work is the two counters
    // below: each proves its datagram LANDED at the node and was REJECTED at its
    // specific guard.

    // (1) the stale-term ghost datagram reached the leader's demux and was
    // rejected there (never forwarded to the sender's tracker).
    let deadline = Instant::now() + Duration::from_secs(5);
    while leader.lr_stats.dropped_stale_term.load(Relaxed) < 1 {
        assert!(Instant::now() < deadline, "stale-term ghost never reached / rejected at the demux");
        std::thread::yield_now();
    }
    // (2) the correct-term ghost traversed the demux (term matched) and was
    // rejected at the sender's follower-set membership guard.
    let deadline = Instant::now() + Duration::from_secs(5);
    while leader.stats.append_pos_unknown_source.load(Relaxed) < 1 {
        assert!(Instant::now() < deadline, "unknown-source ghost never reached / rejected at the sender");
        std::thread::yield_now();
    }

    // With both guards proven to have fired, a short settle then confirms the
    // insurance: commit is still pinned at quorum durable, not the ghost's
    // 1 GiB overshoot.
    std::thread::sleep(Duration::from_millis(100));
    let commit = leader.node.buffer.counters().commit.load_acquire();
    assert_eq!(commit, end, "forged/stale report moved commit ({commit} != {end})");
    converge_and_compare(leader, vec![f1], end);
    drop(dead);
}
