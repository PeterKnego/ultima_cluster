// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `Sessioned<S>` — exactly-once envelope over a raw state machine. See
//! `uc2_service::session` and `docs/reference/state-machine-contract.md`.

use uc2_service::{
    RawStateMachine, SessionConfig, Sessioned, SnapshotStateMachine, SESSION_HEADER_LEN,
    TAG_EXPIRED, TAG_FRESH, TAG_REPLAYED,
};
use uc_lincheck::register::{Cmd, CmdResp, RegisterSm};

fn env(client: u64, seq: u64, cmd: &Cmd) -> Vec<u8> {
    let mut v = Vec::with_capacity(SESSION_HEADER_LEN + 16);
    v.extend_from_slice(&client.to_le_bytes());
    v.extend_from_slice(&seq.to_le_bytes());
    bincode::serde::encode_into_std_write(cmd, &mut v, bincode::config::standard()).unwrap();
    v
}
fn resp(out: &[u8]) -> (u8, Option<CmdResp>) {
    let tag = out[0];
    if tag == TAG_EXPIRED {
        return (tag, None);
    }
    let (r, _) = bincode::serde::decode_from_slice(&out[1..], bincode::config::standard()).unwrap();
    (tag, Some(r))
}
fn sm(window: usize, max_clients: usize) -> Sessioned<RegisterSm> {
    Sessioned::new(RegisterSm::default(), SessionConfig { window, max_clients })
}

#[test]
fn fresh_then_replayed_then_expired() {
    let mut s = sm(2, 16);
    let mut out = Vec::new();
    s.apply(100, &env(7, 1, &Cmd::Write(10)), &mut out);
    assert_eq!(resp(&out), (TAG_FRESH, Some(CmdResp::WriteAck)));
    out.clear();
    s.apply(200, &env(7, 2, &Cmd::Cas { old: 10, new: 11 }), &mut out);
    assert_eq!(resp(&out), (TAG_FRESH, Some(CmdResp::CasResult(true))));
    // retry of seq 2: replayed, NOT re-applied (a second CAS 10->11 would be false)
    out.clear();
    s.apply(300, &env(7, 2, &Cmd::Cas { old: 10, new: 11 }), &mut out);
    assert_eq!(resp(&out), (TAG_REPLAYED, Some(CmdResp::CasResult(true))));
    out.clear();
    s.apply(400, &env(7, 3, &Cmd::Write(1)), &mut out);
    // window = 2 holds seqs 2,3 now; seq 1 fell out
    out.clear();
    s.apply(500, &env(7, 1, &Cmd::Write(10)), &mut out);
    assert_eq!(resp(&out), (TAG_EXPIRED, None));
    assert_eq!(s.last_applied(), Some(500));
}

#[test]
fn a_gap_is_applied_fresh_and_lower_unseen_is_expired() {
    let mut s = sm(8, 16);
    let mut out = Vec::new();
    s.apply(1, &env(1, 5, &Cmd::Write(5)), &mut out);
    assert_eq!(resp(&out).0, TAG_FRESH);
    out.clear();
    s.apply(2, &env(1, 3, &Cmd::Write(3)), &mut out);
    assert_eq!(resp(&out).0, TAG_EXPIRED);
}

#[test]
fn clients_are_evicted_by_oldest_position_deterministically() {
    let mut s = sm(4, 2);
    let mut out = Vec::new();
    s.apply(10, &env(1, 1, &Cmd::Write(1)), &mut out);
    out.clear();
    s.apply(20, &env(2, 1, &Cmd::Write(2)), &mut out);
    out.clear();
    s.apply(30, &env(3, 1, &Cmd::Write(3)), &mut out); // evicts client 1 (seen at 10)
    out.clear();
    s.apply(40, &env(1, 1, &Cmd::Write(1)), &mut out);
    assert_eq!(resp(&out).0, TAG_FRESH, "evicted client starts over: its retry is applied fresh");
    out.clear();
    s.apply(50, &env(2, 1, &Cmd::Write(2)), &mut out);
    // client 2 (seen at 20) was evicted when client 1 came back at 40 (client 3 seen at 30 is newer)
    assert_eq!(resp(&out).0, TAG_FRESH);
}

#[test]
fn malformed_envelope_is_expired_not_a_panic() {
    let mut s = sm(4, 4);
    let mut out = Vec::new();
    s.apply(1, b"short", &mut out);
    assert_eq!(out, vec![TAG_EXPIRED]);
}

#[test]
fn snapshot_round_trip_carries_the_dedup_table() {
    let mut s = sm(4, 16);
    let mut out = Vec::new();
    s.apply(100, &env(9, 1, &Cmd::Write(42)), &mut out);
    out.clear();
    s.apply(200, &env(9, 2, &Cmd::Cas { old: 42, new: 43 }), &mut out);
    let (handle, pos) = s.freeze().unwrap();
    assert_eq!(pos, 200);
    let mut img = Vec::new();
    <Sessioned<RegisterSm> as SnapshotStateMachine>::stream_snapshot(handle, &mut img).unwrap();
    let mut fresh = sm(4, 16);
    let got = fresh.install_snapshot(200, &mut img.as_slice()).unwrap();
    assert_eq!(got, 200);
    out.clear();
    fresh.apply(300, &env(9, 2, &Cmd::Cas { old: 42, new: 43 }), &mut out);
    assert_eq!(resp(&out), (TAG_REPLAYED, Some(CmdResp::CasResult(true))), "dedup survived the snapshot");
    // RegisterSm's Query type is `()` (a plain Read), not a `Query::Read` enum
    // variant — confirm the query path still works post-restore.
    out.clear();
    fresh.query(&bincode::serde::encode_to_vec((), bincode::config::standard()).unwrap(), &mut out);
    let (v, _): (Option<u64>, usize) =
        bincode::serde::decode_from_slice(&out, bincode::config::standard()).unwrap();
    assert_eq!(v, Some(43));
}
