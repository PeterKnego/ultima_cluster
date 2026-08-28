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

/// Deterministic snapshot payload for `id` (so both ends can be byte-compared,
/// and the two artifacts of a stream are distinguishable).
fn snapshot_bytes(id: u8) -> Vec<u8> {
    (0..SNAP_LEN).map(|i| (i.wrapping_mul(31).wrapping_add(7 + id as usize)) as u8).collect()
}

/// The artifact position FSM `id` publishes in these tests.
fn snap_pos(id: u8) -> u64 {
    64 * 1024 + id as u64 * 4096
}

fn write_snapshot_file(dir: &Path, id: u8) -> PathBuf {
    let path = dir.join(format!("snap-{id}-{}.ultsnap", snap_pos(id)));
    std::fs::write(&path, snapshot_bytes(id)).unwrap();
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

fn build(faults: FaultConfig, ids: &[u8]) -> Harness {
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
    let leader_dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let mut declared = 0u64;
    let mut artifacts = Vec::new();
    for &id in ids {
        declared |= 1 << id;
        let path = write_snapshot_file(leader_dir.path(), id);
        let len = std::fs::metadata(&path).unwrap().len();
        artifacts.push(uc2_net::sender::SnapArtifact {
            service_id: id,
            snapshot_pos: snap_pos(id),
            path,
            len,
        });
    }
    let snapshot_source: uc2_net::sender::SnapshotSource = Arc::new(move || {
        Some(uc2_net::sender::SnapshotSet {
            services_declared: declared,
            config: Vec::new(),
            artifacts: artifacts.clone(),
        })
    });

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
    let follower_dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let follower_snap_dir = follower_dir.path().join("snapshots");
    std::fs::create_dir_all(&follower_snap_dir).unwrap();
    let follower_buf = heap_buffer();
    let mut fcfg = FollowerConfig::new(leader_addr);
    fcfg.nak = NakConfig { delay_min_ns: 100_000, delay_max_ns: 500_000, backoff_ns: 1_000_000 };
    let mut follower =
        FollowerReceiver::new(follower_buf, follower_sock, fcfg, term, unrouted());
    follower.set_snapshot_intake(follower_snap_dir.clone(), declared, None);

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

    fn final_path(&self, id: u8) -> PathBuf {
        self.follower_snap_dir.join(id.to_string()).join(format!("snap-{}.ultsnap", snap_pos(id)))
    }

    /// Send a hand-built SNAP_BEGIN straight at the follower — the only way to
    /// exercise a refusal, since our own sender never emits one.
    fn forge_begin(&self, layout: u8, services_declared: u64) {
        use uc_protocol::v2::datagram::{
            DATAGRAM_HEADER_LEN, DGRAM_KIND_SNAP_BEGIN, DatagramHeader, SNAP_BEGIN_FIXED_LEN,
            SnapBeginBody, write_datagram_header, write_snap_begin_body,
        };
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN + SNAP_BEGIN_FIXED_LEN];
        write_datagram_header(
            &mut d,
            &DatagramHeader {
                position: 0,
                leadership_term_id: TERM,
                kind: DGRAM_KIND_SNAP_BEGIN,
                flags: 0,
                key_epoch: 0,
            },
        );
        write_snap_begin_body(
            &mut d[DATAGRAM_HEADER_LEN..],
            &SnapBeginBody {
                session: 99,
                layout,
                service_id: 0,
                snapshot_pos: snap_pos(0),
                total_len: 64,
                services_declared,
                config: vec![],
            },
        );
        let s = UdpSocket::bind("127.0.0.1:0").unwrap();
        s.send_to(&d, self.follower_addr).unwrap();
    }
}

#[test]
fn below_floor_nak_upgrades_to_snapshot_session_and_file_transfers_exactly() {
    let mut h = build(FaultConfig::default(), &[0]);
    h.trigger();
    h.pump_until("file transferred", |h| h.final_path(0).exists());

    let got = std::fs::read(h.final_path(0)).unwrap();
    assert_eq!(got, snapshot_bytes(0), "received file is byte-identical to the source");
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
    let mut h = build(faults, &[0]);
    h.trigger();
    h.pump_until("file transferred under loss", |h| h.final_path(0).exists());

    let got = std::fs::read(h.final_path(0)).unwrap();
    assert_eq!(got, snapshot_bytes(0), "file is byte-identical despite chunk loss");
    assert!(
        h.leader_send.stats().snap_chunk_naks.load(Ordering::Relaxed) > 0,
        "the SNAP_NAK repair path must have run under 20% loss"
    );
}

#[test]
fn a_two_artifact_stream_lands_in_per_id_dirs_under_chunk_loss() {
    // Drop 20% of datagrams in BOTH directions: chunks from BOTH artifacts are
    // lost → the follower NAKs the stream-global gaps → repair chunks fill
    // them, wherever in the stream they fall. Both files complete.
    let faults = FaultConfig { drop_per_million: 200_000, seed: 42, ..FaultConfig::default() };
    let mut h = build(faults, &[0, 2]);
    h.trigger();
    h.pump_until("both artifacts transferred under loss", |h| {
        h.final_path(0).exists() && h.final_path(2).exists()
    });

    assert_eq!(std::fs::read(h.final_path(0)).unwrap(), snapshot_bytes(0), "FSM 0's artifact");
    assert_eq!(std::fs::read(h.final_path(2)).unwrap(), snapshot_bytes(2), "FSM 2's artifact");
    assert!(
        !h.follower_snap_dir.join("1").exists(),
        "an undeclared id gets no directory"
    );
    assert_eq!(
        h.leader_send.stats().snap_sessions.load(Ordering::Relaxed),
        1,
        "ONE session carries the whole set"
    );
    assert!(
        h.leader_send.stats().snap_chunk_naks.load(Ordering::Relaxed) > 0,
        "the SNAP_NAK repair path must have run under 20% loss"
    );
    // No `.part` survives a completed session.
    for id in [0u8, 2] {
        let dir = h.follower_snap_dir.join(id.to_string());
        let parts: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".part"))
            .collect();
        assert!(parts.is_empty(), "{dir:?} still holds a .part");
    }
}

#[test]
fn a_layout_zero_begin_is_refused_as_a_wire_050_peer() {
    let mut h = build(FaultConfig::default(), &[0]);
    let st = h.follower.stats();
    h.forge_begin(0, 0b1); // layout 0 = a 0.5.0-shaped body
    h.pump_until("the legacy-layout refusal is counted", |_| {
        st.snap_refused_legacy_peer.load(Ordering::Relaxed) > 0
    });
    assert_eq!(st.snap_refused_declared_mismatch.load(Ordering::Relaxed), 0, "not the other refusal");
    assert!(!h.follower_snap_dir.join("0").exists(), "no intake, no directory, no .part");
}

#[test]
fn a_mismatched_declared_set_refuses_the_session() {
    let mut h = build(FaultConfig::default(), &[0]); // the follower's own mask is 0b1
    let st = h.follower.stats();
    h.forge_begin(uc_protocol::v2::datagram::SNAP_BEGIN_LAYOUT_V2, 0b11);
    h.pump_until("the declared-set refusal is counted", |_| {
        st.snap_refused_declared_mismatch.load(Ordering::Relaxed) > 0
    });
    assert_eq!(st.snap_refused_legacy_peer.load(Ordering::Relaxed), 0, "not the other refusal");
    assert!(!h.follower_snap_dir.join("0").exists(), "no intake, no directory, no .part");
}
