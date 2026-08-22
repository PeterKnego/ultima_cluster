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
    Sessioned::new(RegisterSm::default(), SessionConfig { window, max_clients, ..SessionConfig::default() })
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

/// A client's genuine first `seq` can legitimately be 0. `ClientState` used
/// to use a bare `u64` with `0` doubling as the "never seen" sentinel, so a
/// client whose first seq was 0 got re-classified as FRESH on every retry —
/// silently re-applying a non-idempotent command. Use a CAS as the seq-0
/// command so a double-apply is visible (the second CAS would fail).
#[test]
fn seq_zero_is_deduplicated() {
    let mut s = sm(4, 16);
    let mut out = Vec::new();
    // Seed the register via a different client so client 3's first-ever
    // command can be a CAS that is expected to succeed exactly once.
    s.apply(50, &env(99, 1, &Cmd::Write(10)), &mut out);
    out.clear();
    s.apply(100, &env(3, 0, &Cmd::Cas { old: 10, new: 11 }), &mut out);
    assert_eq!(resp(&out), (TAG_FRESH, Some(CmdResp::CasResult(true))));
    out.clear();
    // Retry of seq 0: must be REPLAYED. Under the old bug this hits FRESH
    // again, re-running CAS{old:10,new:11} against the now-11 register,
    // which would return `CasResult(false)` — visibly wrong.
    s.apply(200, &env(3, 0, &Cmd::Cas { old: 10, new: 11 }), &mut out);
    assert_eq!(resp(&out), (TAG_REPLAYED, Some(CmdResp::CasResult(true))));
}

/// `SessionConfig` is part of the replicated contract (see `session.rs`'s
/// module doc): a snapshot's embedded config must match the live node's
/// exactly, or `install_snapshot` refuses it before touching the inner SM.
#[test]
fn install_refuses_mismatched_session_config() {
    let mut s = sm(4, 16);
    let mut out = Vec::new();
    s.apply(100, &env(1, 1, &Cmd::Write(1)), &mut out);
    let (handle, pos) = s.freeze().unwrap();
    assert_eq!(pos, 100);
    let mut img = Vec::new();
    <Sessioned<RegisterSm> as SnapshotStateMachine>::stream_snapshot(handle, &mut img).unwrap();

    // A node running a different `window` than the snapshot's origin.
    let mut mismatched = Sessioned::new(
        RegisterSm::default(),
        SessionConfig { window: 8, max_clients: 16, ..SessionConfig::default() },
    );
    let err = mismatched.install_snapshot(100, &mut img.as_slice()).unwrap_err();
    assert!(format!("{err}").contains("session config mismatch"), "unexpected error: {err}");
    // The inner SM must be untouched — the check runs BEFORE the inner install.
    assert_eq!(mismatched.inner().last_applied(), None);
}

/// The `max_bytes` budget evicts whole clients (oldest `last_seen_pos`
/// first, the same deterministic order as `max_clients` eviction), and never
/// evicts the client whose frame just pushed the total over budget.
#[test]
fn max_bytes_evicts_oldest_clients_deterministically() {
    let resp_size =
        bincode::serde::encode_to_vec(CmdResp::WriteAck, bincode::config::standard()).unwrap().len();
    // Exactly one cached response fits under budget; a second tips it over.
    let mut s = Sessioned::new(
        RegisterSm::default(),
        SessionConfig { window: 100, max_clients: 100, max_bytes: resp_size },
    );
    let mut out = Vec::new();
    s.apply(10, &env(1, 1, &Cmd::Write(1)), &mut out);
    out.clear();
    // client 2's insert pushes total_bytes to 2*resp_size > max_bytes: client
    // 1 (older last_seen_pos) is evicted, never client 2 (just written).
    s.apply(20, &env(2, 1, &Cmd::Write(2)), &mut out);
    out.clear();
    // client 2 must have survived its own eviction pass.
    s.apply(30, &env(2, 1, &Cmd::Write(2)), &mut out);
    assert_eq!(resp(&out).0, TAG_REPLAYED, "the just-written client is never the byte-budget victim");
    out.clear();
    // client 1 was evicted by the byte budget; its retry starts over.
    s.apply(40, &env(1, 1, &Cmd::Write(1)), &mut out);
    assert_eq!(resp(&out).0, TAG_FRESH, "client 1 was evicted by the byte budget; its retry is applied fresh");
}

/// `freeze()` reports the INNER SM's position, which can sit strictly below
/// `Sessioned::last_applied()` when the most recent frames were
/// REPLAYED/EXPIRED. That is safe to round-trip through a snapshot: those
/// trailing frames only ever bump `last_seen_pos` (never evict, never touch
/// the window contents), so replaying the skew again after install
/// reproduces the identical table.
#[test]
fn freeze_with_trailing_replayed_frames_round_trips() {
    let mut s = sm(4, 16);
    let mut out = Vec::new();
    s.apply(100, &env(5, 1, &Cmd::Write(7)), &mut out);
    out.clear();
    s.apply(200, &env(5, 2, &Cmd::Cas { old: 7, new: 8 }), &mut out);
    out.clear();
    // A trailing REPLAYED frame at a position above the inner SM's last apply.
    s.apply(300, &env(5, 2, &Cmd::Cas { old: 7, new: 8 }), &mut out);
    assert_eq!(resp(&out).0, TAG_REPLAYED);

    let (handle, pos) = s.freeze().unwrap();
    assert_eq!(pos, 200, "freeze reports the inner SM's position, not Sessioned::last_applied()");
    assert_eq!(s.last_applied(), Some(300));

    let mut img = Vec::new();
    <Sessioned<RegisterSm> as SnapshotStateMachine>::stream_snapshot(handle, &mut img).unwrap();
    let mut fresh = sm(4, 16);
    let got = fresh.install_snapshot(200, &mut img.as_slice()).unwrap();
    assert_eq!(got, 200);

    out.clear();
    fresh.apply(400, &env(5, 2, &Cmd::Cas { old: 7, new: 8 }), &mut out);
    assert_eq!(
        resp(&out),
        (TAG_REPLAYED, Some(CmdResp::CasResult(true))),
        "the dedup table survived even though the pre-freeze snapshot carried a trailing replayed frame's last_seen_pos bump"
    );
}
