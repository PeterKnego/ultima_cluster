// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M6 Task 6 — the snapshot session (datagram kinds 12–15).
//!
//! A below-floor NAK the leader cannot serve from ring OR journal upgrades to a
//! bounded, unicast, NAK-repaired file transfer instead of counting an
//! unrecoverable overrun. These tests drive a real sender + receiver pair over
//! loopback UDP with a synchronous pump so faults are deterministic:
//!
//! * LEADER = a `Sender` (ships SNAP_BEGIN/SNAP_CHUNK) + a `FollowerReceiver`
//!   whose `sender_route` demuxes the peer's inbound SNAP_NAK/SNAP_DONE back to
//!   the sender — exactly how `uc2_node` composes leader duty on one socket.
//! * FOLLOWER = a `FollowerReceiver` with snapshot intake enabled (writes the
//!   `.part`, NAKs gaps, renames on completion, acks SNAP_DONE).
//!
//! The session is triggered by injecting a deep NAK (below the leader's ring
//! floor, no journal replay source) into the sender's control channel.

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use uc2_log::buffer::LogBuffer;
use uc2_log::cnc::{CncMeta, CncPage};
use uc2_log::region::Region;
use uc2_net::fault::{FaultConfig, FaultSocket};
use uc2_net::rebuild::NakConfig;
use uc2_net::receiver::{FollowerConfig, FollowerReceiver, NetEvent};
use uc2_net::sender::{CtrlMsg, Sender, SenderConfig};

const TERM: u32 = 3;
const CAP: u64 = 1 << 20; // 1 MiB ring
const SNAP_POS: u64 = 64 * 1024;
const SNAP_LEN: usize = 300 * 1024;

fn heap_buffer() -> Arc<LogBuffer> {
    let cnc = CncPage::heap(&CncMeta {
        node_id: 0,
        instance_id: 0,
        app_id: "snap".into(),
        buffer_bytes: CAP,
        max_payload: 256,
    });
    Arc::new(LogBuffer::new(Region::heap_zeroed(CAP as usize), cnc, 256))
}

fn unrouted() -> mpsc::SyncSender<NetEvent> {
    let (tx, _rx) = mpsc::sync_channel(64);
    tx
}

/// Deterministic snapshot payload (so both ends can be byte-compared).
fn snapshot_bytes() -> Vec<u8> {
    (0..SNAP_LEN).map(|i| (i.wrapping_mul(31).wrapping_add(7)) as u8).collect()
}

fn write_snapshot_file(dir: &Path) -> PathBuf {
    let path = dir.join(format!("snap-{SNAP_POS}.ultsnap"));
    std::fs::write(&path, snapshot_bytes()).unwrap();
    path
}

struct Harness {
    leader_send: Sender,
    leader_recv: FollowerReceiver,
    follower: FollowerReceiver,
    ctrl_tx: mpsc::SyncSender<CtrlMsg>,
    follower_addr: SocketAddr,
    follower_snap_dir: PathBuf,
    _leader_dir: tempfile::TempDir,
    _follower_dir: tempfile::TempDir,
}

fn build(faults: FaultConfig) -> Harness {
    // Leader: one socket, cloned for send + recv (as uc2_node composes it).
    let leader_raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = leader_raw.local_addr().unwrap();
    let mut send_sock = FaultSocket::from_socket(leader_raw.try_clone().unwrap()).unwrap();
    let mut recv_sock = FaultSocket::from_socket(leader_raw).unwrap();
    send_sock.set_faults(faults);
    recv_sock.set_faults(faults);

    // Follower socket.
    let mut follower_sock = FaultSocket::bind("127.0.0.1:0").unwrap();
    let follower_addr = follower_sock.local_addr().unwrap();
    follower_sock.set_faults(faults);

    let term = Arc::new(AtomicU32::new(TERM));
    let role = Arc::new(AtomicBool::new(true));

    // The leader dir holds the source snapshot; the sender ships it on a
    // below-floor NAK. NO replay source is wired, so the NAK is unservable from
    // the journal and upgrades to a session.
    let leader_dir = tempfile::tempdir().unwrap();
    let snap_path = write_snapshot_file(leader_dir.path());
    let src_len = std::fs::metadata(&snap_path).unwrap().len();
    let snapshot_source: uc2_net::sender::SnapshotSource =
        Arc::new(move || Some((SNAP_POS, snap_path.clone(), src_len)));

    let leader_buf = heap_buffer();
    // Prime the ring far ahead so a NAK at position 0 is below the ring floor
    // (durable - capacity) → Overrun → (no journal) → snapshot session.
    leader_buf.counters().prime(2 * CAP);

    let (ctrl_tx, ctrl_rx) = mpsc::sync_channel(1024);
    let mut scfg = SenderConfig::new(TERM);
    // No live stream in this test — silence heartbeats so the follower's main
    // receiver never sees a (bogus, from the primed ring) leader_append and
    // NAKs a "gap"; those unservable main NAKs would confound the overrun count.
    // The snapshot session is driven purely by the injected deep NAK.
    scfg.heartbeat_ns = u64::MAX;
    let mut leader_send = Sender::new(
        Arc::clone(&leader_buf),
        send_sock,
        vec![follower_addr],
        3,
        ctrl_rx,
        scfg,
        Arc::clone(&term),
        role,
    );
    leader_send.set_snapshot_source(snapshot_source);

    let mut lrcfg = FollowerConfig::new(leader_addr);
    lrcfg.nak = NakConfig { delay_min_ns: 100_000, delay_max_ns: 500_000, backoff_ns: 2_000_000 };
    let mut leader_recv =
        FollowerReceiver::new(Arc::clone(&leader_buf), recv_sock, lrcfg, Arc::clone(&term), unrouted());
    leader_recv.set_sender_route(ctrl_tx.clone());

    // Follower: intake enabled.
    let follower_dir = tempfile::tempdir().unwrap();
    let follower_snap_dir = follower_dir.path().join("snapshots");
    std::fs::create_dir_all(&follower_snap_dir).unwrap();
    let follower_buf = heap_buffer();
    let mut fcfg = FollowerConfig::new(leader_addr);
    fcfg.nak = NakConfig { delay_min_ns: 100_000, delay_max_ns: 500_000, backoff_ns: 1_000_000 };
    let mut follower =
        FollowerReceiver::new(follower_buf, follower_sock, fcfg, term, unrouted());
    follower.set_snapshot_intake(follower_snap_dir.clone(), None);

    Harness {
        leader_send,
        leader_recv,
        follower,
        ctrl_tx,
        follower_addr,
        follower_snap_dir,
        _leader_dir: leader_dir,
        _follower_dir: follower_dir,
    }
}

impl Harness {
    /// Inject the deep NAK that triggers the session.
    fn trigger(&self) {
        self.ctrl_tx
            .send(CtrlMsg::Nak { from: self.follower_addr, position: 0, length: 96 })
            .unwrap();
    }

    /// Pump all three agents until `done`, or panic on timeout.
    fn pump_until(&mut self, what: &str, mut done: impl FnMut(&Harness) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            self.leader_send.do_work();
            self.leader_recv.do_work();
            self.follower.do_work();
            if done(self) {
                return;
            }
            assert!(Instant::now() < deadline, "timed out: {what}");
            std::thread::sleep(Duration::from_micros(50));
        }
    }

    fn final_path(&self) -> PathBuf {
        self.follower_snap_dir.join(format!("snap-{SNAP_POS}.ultsnap"))
    }
}

#[test]
fn below_floor_nak_upgrades_to_snapshot_session_and_file_transfers_exactly() {
    let mut h = build(FaultConfig::default());
    h.trigger();
    h.pump_until("file transferred", |h| h.final_path().exists());

    let got = std::fs::read(h.final_path()).unwrap();
    assert_eq!(got, snapshot_bytes(), "received file is byte-identical to the source");
    assert_eq!(
        h.leader_send.stats().snap_sessions.load(Ordering::Relaxed),
        1,
        "the below-floor NAK upgraded to a session, not an overrun"
    );
    assert_eq!(
        h.leader_send.stats().overruns.load(Ordering::Relaxed),
        0,
        "no unrecoverable overrun was counted"
    );
}

#[test]
fn snapshot_session_survives_chunk_loss_via_snap_nak() {
    // Drop 20% of datagrams in BOTH directions: chunks are lost → the follower
    // NAKs the gaps → repair chunks fill them. Completion is still reached.
    let faults = FaultConfig { drop_per_million: 200_000, seed: 42, ..FaultConfig::default() };
    let mut h = build(faults);
    h.trigger();
    h.pump_until("file transferred under loss", |h| h.final_path().exists());

    let got = std::fs::read(h.final_path()).unwrap();
    assert_eq!(got, snapshot_bytes(), "file is byte-identical despite chunk loss");
    assert!(
        h.leader_send.stats().snap_chunk_naks.load(Ordering::Relaxed) > 0,
        "the SNAP_NAK repair path must have run under 20% loss"
    );
}
