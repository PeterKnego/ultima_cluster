//! Invariants from docs/DESIGN.md § 6, exercised on the state machine alone —
//! no node, no process. `ApplyCtx::for_sm` is the contract page's own recipe
//! for driving `apply` in a unit test.

use bytes::Bytes;
use kv_store::KvSm;
use kv_store::wire::{self, DigestReply, GetReply, WriteReply};
use proptest::prelude::*;
use uc_service::{ApplyCtx, RawStateMachine, SnapshotStateMachine};

/// Positions are 32-aligned byte offsets and a user frame never sits at 0 (L11).
fn pos(i: u64) -> u64 {
    32 * (i + 1)
}

fn apply(sm: &mut KvSm, p: u64, cmd: &[u8]) -> Vec<u8> {
    let mut ctx = ApplyCtx::for_sm::<KvSm>(p);
    let mut out = Vec::new();
    sm.apply(&mut ctx, cmd, &mut out);
    out
}

fn query(sm: &KvSm, q: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    sm.query(q, &mut out);
    out
}

fn get(sm: &KvSm, key: &[u8]) -> GetReply {
    wire::decode_get_reply(&query(sm, &wire::encode_get(key))).unwrap()
}

fn digest(sm: &KvSm) -> DigestReply {
    wire::decode_digest_reply(&query(sm, &wire::encode_digest())).unwrap()
}

fn write(sm: &mut KvSm, p: u64, cmd: &[u8]) -> WriteReply {
    wire::decode_write_reply(&apply(sm, p, cmd)).unwrap()
}

// ---------------------------------------------------------------- invariant 2, 3

#[test]
fn put_returns_position_as_version_and_get_sees_it() {
    let mut sm = KvSm::default();
    let r = write(&mut sm, pos(0), &wire::encode_put(b"k", b"v1"));
    assert_eq!(r, WriteReply::Ok { version: pos(0) });
    assert_eq!(
        get(&sm, b"k"),
        GetReply::Found {
            version: pos(0),
            value: Bytes::from_static(b"v1")
        }
    );
    assert_eq!(sm.last_applied(), Some(pos(0)));
}

#[test]
fn get_missing_is_not_found() {
    let sm = KvSm::default();
    assert_eq!(get(&sm, b"nope"), GetReply::NotFound);
}

#[test]
fn delete_returns_old_version_then_not_found() {
    let mut sm = KvSm::default();
    write(&mut sm, pos(0), &wire::encode_put(b"k", b"v"));
    assert_eq!(
        write(&mut sm, pos(1), &wire::encode_delete(b"k")),
        WriteReply::Ok { version: pos(0) }
    );
    assert_eq!(get(&sm, b"k"), GetReply::NotFound);
    assert_eq!(
        write(&mut sm, pos(2), &wire::encode_delete(b"k")),
        WriteReply::NotFound
    );
    // A no-op frame still advances the cursor: the frame was consumed.
    assert_eq!(sm.last_applied(), Some(pos(2)));
}

#[test]
fn cas_semantics() {
    let mut sm = KvSm::default();
    // 0 = "must be absent": create succeeds, and a second create fails naming the current version.
    assert_eq!(
        write(&mut sm, pos(0), &wire::encode_cas(b"k", 0, b"a")),
        WriteReply::Ok { version: pos(0) }
    );
    assert_eq!(
        write(&mut sm, pos(1), &wire::encode_cas(b"k", 0, b"b")),
        WriteReply::VersionMismatch { current: pos(0) }
    );
    assert_eq!(
        get(&sm, b"k"),
        GetReply::Found {
            version: pos(0),
            value: Bytes::from_static(b"a")
        }
    );
    // Right version: applied, new version = this frame's position.
    assert_eq!(
        write(&mut sm, pos(2), &wire::encode_cas(b"k", pos(0), b"c")),
        WriteReply::Ok { version: pos(2) }
    );
    // Stale version: refused, unchanged.
    assert_eq!(
        write(&mut sm, pos(3), &wire::encode_cas(b"k", pos(0), b"d")),
        WriteReply::VersionMismatch { current: pos(2) }
    );
    assert_eq!(
        get(&sm, b"k"),
        GetReply::Found {
            version: pos(2),
            value: Bytes::from_static(b"c")
        }
    );
    // CAS on an absent key with a non-zero expectation: mismatch, current = 0.
    assert_eq!(
        write(&mut sm, pos(4), &wire::encode_cas(b"zz", 7, b"d")),
        WriteReply::VersionMismatch { current: 0 }
    );
}

// ---------------------------------------------------------------- limits

#[test]
fn limits_are_enforced_in_apply_not_just_in_the_client() {
    let mut sm = KvSm::default();
    let big_key = vec![b'k'; wire::MAX_KEY + 1];
    let ok_key = vec![b'k'; wire::MAX_KEY];
    let big_val = vec![b'v'; wire::MAX_VALUE + 1];
    let ok_val = vec![b'v'; wire::MAX_VALUE];
    // The encoders refuse out-of-range sizes so a well-behaved client never sends them...
    assert!(wire::try_encode_put(&big_key, &ok_val).is_err());
    assert!(wire::try_encode_put(&ok_key, &big_val).is_err());
    assert!(wire::try_encode_put(b"", &ok_val).is_err());
    // ...and the state machine refuses a hand-built oversize frame anyway.
    let mut frame = vec![wire::FORMAT_VERSION, wire::OP_PUT];
    frame.extend_from_slice(&(ok_key.len() as u16).to_le_bytes());
    frame.extend_from_slice(&ok_key);
    frame.extend_from_slice(&big_val);
    assert_eq!(
        write(&mut sm, pos(0), &frame),
        WriteReply::BadRequest(wire::BAD_VALUE_LEN)
    );
    assert_eq!(get(&sm, &ok_key), GetReply::NotFound);
    // The largest legal frames fit the derived budget (DESIGN.md § 5).
    let put = wire::encode_put(&ok_key, &ok_val);
    let cas = wire::encode_cas(&ok_key, 1, &ok_val);
    assert_eq!(put.len(), 4 + wire::MAX_KEY + wire::MAX_VALUE);
    assert_eq!(cas.len(), 12 + wire::MAX_KEY + wire::MAX_VALUE);
    assert!(cas.len() <= wire::COMMAND_BUDGET);
    assert_eq!(
        wire::COMMAND_BUDGET,
        uc_remote::engine::STANDARD_PAYLOAD - uc_service::SESSION_HEADER_LEN
    );
    assert_eq!(
        write(&mut sm, pos(1), &put),
        WriteReply::Ok { version: pos(1) }
    );
    assert_eq!(
        write(&mut sm, pos(2), &cas),
        WriteReply::VersionMismatch { current: pos(1) }
    );
}

#[test]
fn malformed_frames_are_bad_requests_not_panics() {
    let mut sm = KvSm::default();
    assert_eq!(
        write(&mut sm, pos(0), &[]),
        WriteReply::BadRequest(wire::BAD_TRUNCATED)
    );
    assert_eq!(
        write(&mut sm, pos(1), &[9, wire::OP_PUT, 1, 0, b'k']),
        WriteReply::BadRequest(wire::BAD_FORMAT_VERSION)
    );
    assert_eq!(
        write(&mut sm, pos(2), &[wire::FORMAT_VERSION, 200, 1, 0, b'k']),
        WriteReply::BadRequest(wire::BAD_UNKNOWN_OP)
    );
    assert_eq!(
        write(
            &mut sm,
            pos(3),
            &[wire::FORMAT_VERSION, wire::OP_PUT, 5, 0, b'k']
        ),
        WriteReply::BadRequest(wire::BAD_TRUNCATED)
    );
    assert_eq!(
        write(
            &mut sm,
            pos(4),
            &[wire::FORMAT_VERSION, wire::OP_DELETE, 1, 0, b'k', b'x']
        ),
        WriteReply::BadRequest(wire::BAD_TRAILING)
    );
    assert_eq!(
        write(
            &mut sm,
            pos(5),
            &[wire::FORMAT_VERSION, wire::OP_CAS, 1, 0, b'k', 1, 2, 3]
        ),
        WriteReply::BadRequest(wire::BAD_TRUNCATED)
    );
    // Queries too.
    let q = query(&sm, &[]);
    assert_eq!(
        wire::decode_get_reply(&q).unwrap(),
        GetReply::BadRequest(wire::BAD_TRUNCATED)
    );
    let q = query(&sm, &[wire::FORMAT_VERSION, 77]);
    assert_eq!(
        wire::decode_get_reply(&q).unwrap(),
        GetReply::BadRequest(wire::BAD_UNKNOWN_OP)
    );
    assert_eq!(sm.last_applied(), Some(pos(5)));
    assert_eq!(digest(&sm).count, 0);
}

// ---------------------------------------------------------------- invariant 1, 4: determinism, digest, golden replay

/// A fixed script of every op kind. Two fresh machines must agree, and the
/// digest is pinned so a future change to the codec or the hash is loud.
fn golden_script() -> Vec<Vec<u8>> {
    let mut v = Vec::new();
    for i in 0..50u32 {
        v.push(wire::encode_put(
            format!("key{i:03}").as_bytes(),
            &i.to_le_bytes(),
        ));
    }
    for i in (0..50u32).step_by(3) {
        v.push(wire::encode_delete(format!("key{i:03}").as_bytes()));
    }
    v.push(wire::encode_cas(b"key001", 0, b"never"));
    v.push(wire::encode_cas(b"key001", pos(1), b"cas-ok"));
    v.push(wire::encode_cas(b"fresh", 0, b"created"));
    v.push(vec![wire::FORMAT_VERSION, 250]); // a bad frame is part of the log too
    v.push(wire::encode_put(&[0u8; 256], &[0xffu8; 1024]));
    v
}

fn run_script(script: &[Vec<u8>]) -> KvSm {
    let mut sm = KvSm::default();
    for (i, cmd) in script.iter().enumerate() {
        apply(&mut sm, pos(i as u64), cmd);
    }
    sm
}

#[test]
fn golden_replay_pins_digest() {
    let a = run_script(&golden_script());
    let b = run_script(&golden_script());
    let da = digest(&a);
    let db = digest(&b);
    assert_eq!(da, db);
    assert_eq!(da.count, 50 - 17 + 2);
    assert_eq!(da.last_applied, pos(golden_script().len() as u64 - 1));
    assert_eq!(
        a.recompute_digest(),
        da.digest,
        "incremental digest drifted from a full recomputation"
    );
    // GOLDEN: recorded from the first green run; a change here is a wire or hash change.
    assert_eq!(
        da.digest, GOLDEN_DIGEST,
        "golden digest changed: 0x{:016x}",
        da.digest
    );
}
const GOLDEN_DIGEST: u64 = 0xe499_47cd_2f1a_8a58; // recorded 2026-09-15 from the first green run

// ---------------------------------------------------------------- invariant 5: snapshot round-trip

#[test]
fn snapshot_round_trip_reproduces_state_and_cursor() {
    let sm = run_script(&golden_script());
    let (handle, frozen_at) = sm.freeze().unwrap();
    assert_eq!(frozen_at, sm.last_applied().unwrap());
    let mut image = Vec::new();
    KvSm::stream_snapshot(handle, &mut image).unwrap();

    // The instant P is the frame-END of the SNAPSHOT frame: strictly above the cursor.
    let p = frozen_at + 64;
    let mut fresh = KvSm::default();
    let landed = fresh.install_snapshot(p, &mut &image[..]).unwrap();
    assert_eq!(landed, p);
    assert_eq!(
        fresh.last_applied(),
        sm.last_applied(),
        "cursor must come from the image, not the tag"
    );
    assert!(fresh.last_applied().unwrap() < p);
    assert_eq!(digest(&fresh), digest(&sm));
    assert_eq!(fresh.recompute_digest(), digest(&sm).digest);
    assert_eq!(get(&fresh, b"key001"), get(&sm, b"key001"));
    assert_eq!(get(&fresh, &[0u8; 256]), get(&sm, &[0u8; 256]));

    // Applying after install continues from the cursor, on identical state.
    let mut a = sm;
    let mut b = fresh;
    let next = wire::encode_put(b"after", b"snapshot");
    assert_eq!(write(&mut a, p, &next), write(&mut b, p, &next));
    assert_eq!(digest(&a), digest(&b));
}

#[test]
fn install_refuses_an_image_whose_cursor_is_not_below_the_tag() {
    let sm = run_script(&golden_script());
    let (handle, frozen_at) = sm.freeze().unwrap();
    let mut image = Vec::new();
    KvSm::stream_snapshot(handle, &mut image).unwrap();
    let mut fresh = KvSm::default();
    assert!(
        fresh.install_snapshot(frozen_at, &mut &image[..]).is_err(),
        "cursor == tag is a mis-tag"
    );
    assert!(
        fresh
            .install_snapshot(frozen_at - 32, &mut &image[..])
            .is_err()
    );
    assert_eq!(
        fresh.last_applied(),
        None,
        "a refused install must not half-apply"
    );
    assert_eq!(digest(&fresh).count, 0);
}

#[test]
fn install_refuses_a_corrupt_image() {
    let sm = run_script(&golden_script());
    let (handle, frozen_at) = sm.freeze().unwrap();
    let mut image = Vec::new();
    KvSm::stream_snapshot(handle, &mut image).unwrap();
    let mut truncated = image.clone();
    truncated.truncate(image.len() - 5);
    let mut fresh = KvSm::default();
    assert!(
        fresh
            .install_snapshot(frozen_at + 64, &mut &truncated[..])
            .is_err()
    );
    let mut flipped = image.clone();
    let last = flipped.len() - 1;
    flipped[last] ^= 0x01;
    assert!(
        fresh
            .install_snapshot(frozen_at + 64, &mut &flipped[..])
            .is_err(),
        "digest must catch a flipped byte"
    );
    let mut bad_version = image.clone();
    bad_version[0] = 99;
    assert!(
        fresh
            .install_snapshot(frozen_at + 64, &mut &bad_version[..])
            .is_err()
    );
}

#[test]
fn empty_state_snapshots_and_installs() {
    let sm = KvSm::default();
    let (handle, frozen_at) = sm.freeze().unwrap();
    assert_eq!(frozen_at, 0);
    let mut image = Vec::new();
    KvSm::stream_snapshot(handle, &mut image).unwrap();
    let mut fresh = KvSm::default();
    assert_eq!(fresh.install_snapshot(32, &mut &image[..]).unwrap(), 32);
    assert_eq!(fresh.last_applied(), None);
}

// ---------------------------------------------------------------- invariant 8: exactly-once through Sessioned

#[test]
fn sessioned_replays_a_duplicate_frame_without_reapplying() {
    use uc_service::{SESSION_HEADER_LEN, SessionConfig, Sessioned, TAG_FRESH, TAG_REPLAYED};
    let mut sm = Sessioned::new(KvSm::default(), SessionConfig::default());
    let envelope = |client: u64, seq: u64, inner: &[u8]| {
        let mut f = Vec::with_capacity(SESSION_HEADER_LEN + inner.len());
        f.extend_from_slice(&client.to_le_bytes());
        f.extend_from_slice(&seq.to_le_bytes());
        f.extend_from_slice(inner);
        f
    };
    let frame = envelope(42, 1, &wire::encode_put(b"k", b"v"));
    let mut out = Vec::new();
    sm.apply(&mut ApplyCtx::for_sm::<KvSm>(pos(0)), &frame, &mut out);
    assert_eq!(out[0], TAG_FRESH);
    let first = wire::decode_write_reply(&out[1..]).unwrap();
    assert_eq!(first, WriteReply::Ok { version: pos(0) });

    // The retry: same client, same seq, a later position. Not applied again —
    // the cached response comes back and the key's version is unchanged.
    let mut out2 = Vec::new();
    sm.apply(&mut ApplyCtx::for_sm::<KvSm>(pos(1)), &frame, &mut out2);
    assert_eq!(out2[0], TAG_REPLAYED);
    assert_eq!(wire::decode_write_reply(&out2[1..]).unwrap(), first);
    assert_eq!(
        get(sm.inner(), b"k"),
        GetReply::Found {
            version: pos(0),
            value: Bytes::from_static(b"v")
        }
    );
    assert_eq!(sm.inner().last_applied(), Some(pos(0)));
    assert_eq!(
        sm.last_applied(),
        Some(pos(1)),
        "Sessioned tracks the dedup-only frame as its resume point"
    );

    // A fresh seq from the same client is applied.
    let mut out3 = Vec::new();
    sm.apply(
        &mut ApplyCtx::for_sm::<KvSm>(pos(2)),
        &envelope(42, 2, &wire::encode_put(b"k", b"w")),
        &mut out3,
    );
    assert_eq!(out3[0], TAG_FRESH);
    assert_eq!(
        get(sm.inner(), b"k"),
        GetReply::Found {
            version: pos(2),
            value: Bytes::from_static(b"w")
        }
    );

    // Queries pass through the wrapper untouched (no envelope, no tag).
    let mut q = Vec::new();
    sm.query(&wire::encode_get(b"k"), &mut q);
    assert_eq!(
        wire::decode_get_reply(&q).unwrap(),
        GetReply::Found {
            version: pos(2),
            value: Bytes::from_static(b"w")
        }
    );
}

// ---------------------------------------------------------------- property tests (invariants 1, 4, 7)

#[derive(Debug, Clone)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Cas(Vec<u8>, u64, Vec<u8>),
    Raw(Vec<u8>),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    let key = prop::collection::vec(any::<u8>(), 0..=8).prop_map(|mut k| {
        // Bias toward a small key space so ops collide.
        k.truncate(3);
        if k.is_empty() { vec![0] } else { k }
    });
    let val = prop::collection::vec(any::<u8>(), 0..=40);
    prop_oneof![
        4 => (key.clone(), val.clone()).prop_map(|(k, v)| Op::Put(k, v)),
        2 => key.clone().prop_map(Op::Delete),
        2 => (key, any::<u64>(), val).prop_map(|(k, e, v)| Op::Cas(k, e, v)),
        1 => prop::collection::vec(any::<u8>(), 0..=32).prop_map(Op::Raw),
    ]
}

fn encode(op: &Op) -> Vec<u8> {
    match op {
        Op::Put(k, v) => wire::encode_put(k, v),
        Op::Delete(k) => wire::encode_delete(k),
        Op::Cas(k, e, v) => wire::encode_cas(k, *e, v),
        Op::Raw(b) => b.clone(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn replay_is_deterministic_and_digest_matches_recomputation(ops in prop::collection::vec(op_strategy(), 0..60)) {
        let mut a = KvSm::default();
        let mut b = KvSm::default();
        let mut versions: std::collections::BTreeMap<Vec<u8>, u64> = Default::default();
        for (i, op) in ops.iter().enumerate() {
            let p = pos(i as u64);
            let ra = apply(&mut a, p, &encode(op));
            let rb = apply(&mut b, p, &encode(op));
            prop_assert_eq!(&ra, &rb);
            // Invariant 2: a key's version is strictly increasing and equals the writing position.
            if let Ok(WriteReply::Ok { version }) = wire::decode_write_reply(&ra) {
                match op {
                    Op::Put(k, _) | Op::Cas(k, _, _) => {
                        prop_assert_eq!(version, p);
                        if let Some(prev) = versions.insert(k.clone(), p) { prop_assert!(prev < p); }
                    }
                    Op::Delete(k) => { versions.remove(k); }
                    Op::Raw(_) => {}
                }
            }
            prop_assert_eq!(a.last_applied(), Some(p));
        }
        prop_assert_eq!(a.recompute_digest(), digest(&a).digest);
        prop_assert_eq!(digest(&a), digest(&b));
        prop_assert_eq!(digest(&a).count as usize, versions.len());
        for (k, v) in &versions {
            match get(&a, k) { GetReply::Found { version, .. } => prop_assert_eq!(version, *v), other => prop_assert!(false, "{other:?}") }
        }
    }

    #[test]
    fn snapshot_round_trip_holds_for_any_history(ops in prop::collection::vec(op_strategy(), 0..40)) {
        let mut a = KvSm::default();
        for (i, op) in ops.iter().enumerate() { apply(&mut a, pos(i as u64), &encode(op)); }
        let (h, at) = a.freeze().unwrap();
        let mut img = Vec::new();
        KvSm::stream_snapshot(h, &mut img).unwrap();
        let mut b = KvSm::default();
        prop_assert_eq!(b.install_snapshot(at + 32, &mut &img[..]).unwrap(), at + 32);
        prop_assert_eq!(digest(&a), digest(&b));
        prop_assert_eq!(a.last_applied(), b.last_applied());
    }

    #[test]
    fn no_input_panics_apply_or_query(bytes in prop::collection::vec(any::<u8>(), 0..=1400)) {
        let mut sm = KvSm::default();
        apply(&mut sm, pos(0), &bytes);
        query(&sm, &bytes);
    }
}
