//! v2: Append / List-read, the two shapes, the caps, image v2, and the
//! old-image test (a real v1 image, written by the v1 code, installed into
//! this binary). docs/DESIGN.md § 9.

use bytes::Bytes;
use kv_store::wire::{self, AppendReply, GetReply, ListReply, WriteReply};
use kv_store::{KV_VERSION, KvSm};
use proptest::prelude::*;
use uc_service::{ApplyCtx, RawStateMachine, SnapshotStateMachine};

fn pos(i: u64) -> u64 {
    32 * (i + 1)
}
fn apply(sm: &mut KvSm, p: u64, cmd: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    sm.apply(&mut ApplyCtx::for_sm::<KvSm>(p), cmd, &mut out);
    out
}
fn query(sm: &KvSm, q: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    sm.query(q, &mut out);
    out
}
fn write(sm: &mut KvSm, p: u64, cmd: &[u8]) -> WriteReply {
    wire::decode_write_reply(&apply(sm, p, cmd)).unwrap()
}
fn append(sm: &mut KvSm, p: u64, key: &[u8], v: &[u8]) -> AppendReply {
    wire::decode_append_reply(&apply(sm, p, &wire::encode_append(key, v))).unwrap()
}
fn get(sm: &KvSm, key: &[u8]) -> GetReply {
    wire::decode_get_reply(&query(sm, &wire::encode_get(key))).unwrap()
}
fn list(sm: &KvSm, key: &[u8]) -> ListReply {
    wire::decode_list_reply(&query(sm, &wire::encode_list(key))).unwrap()
}
fn digest(sm: &KvSm) -> wire::DigestReply {
    wire::decode_digest_reply(&query(sm, &wire::encode_digest())).unwrap()
}
fn b(s: &[u8]) -> Bytes {
    Bytes::copy_from_slice(s)
}

#[test]
fn version_moved_to_2_0_0() {
    assert_eq!(
        KV_VERSION, 0x0200_0000,
        "pack_version(2,0,0): major:8 ‖ minor:8 ‖ patch:16"
    );
    assert_eq!(<KvSm as RawStateMachine>::VERSION, KV_VERSION);
    assert_eq!(
        <KvSm as RawStateMachine>::NAME,
        "kv",
        "the name must not move: it is the row's identity"
    );
}

// ---------------------------------------------------------------- append / list

#[test]
fn append_builds_an_ordered_list_and_list_reads_it() {
    let mut sm = KvSm::default();
    assert_eq!(list(&sm, b"l"), ListReply::NotFound);
    assert_eq!(
        append(&mut sm, pos(0), b"l", b"a"),
        AppendReply::Ok {
            version: pos(0),
            len: 1
        }
    );
    assert_eq!(
        append(&mut sm, pos(1), b"l", b""),
        AppendReply::Ok {
            version: pos(1),
            len: 2
        }
    );
    assert_eq!(
        append(&mut sm, pos(2), b"l", b"c"),
        AppendReply::Ok {
            version: pos(2),
            len: 3
        }
    );
    assert_eq!(
        list(&sm, b"l"),
        ListReply::Found {
            version: pos(2),
            items: vec![b(b"a"), b(b""), b(b"c")]
        }
    );
    // The version of a list is the position of its last append.
    assert_eq!(sm.last_applied(), Some(pos(2)));
}

#[test]
fn shapes_are_strict_except_delete() {
    let mut sm = KvSm::default();
    write(&mut sm, pos(0), &wire::encode_put(b"v", b"x"));
    append(&mut sm, pos(1), b"l", b"x");
    // Wrong-shape operations are refused and change nothing.
    assert_eq!(append(&mut sm, pos(2), b"v", b"y"), AppendReply::WrongShape);
    assert_eq!(
        get(&sm, b"v"),
        GetReply::Found {
            version: pos(0),
            value: b(b"x")
        }
    );
    assert_eq!(
        write(&mut sm, pos(3), &wire::encode_put(b"l", b"y")),
        WriteReply::WrongShape
    );
    assert_eq!(
        write(&mut sm, pos(4), &wire::encode_cas(b"l", pos(1), b"y")),
        WriteReply::WrongShape
    );
    assert_eq!(
        write(&mut sm, pos(5), &wire::encode_cas(b"l", 0, b"y")),
        WriteReply::WrongShape
    );
    assert_eq!(get(&sm, b"l"), GetReply::WrongShape);
    assert_eq!(list(&sm, b"v"), ListReply::WrongShape);
    assert_eq!(
        list(&sm, b"l"),
        ListReply::Found {
            version: pos(1),
            items: vec![b(b"x")]
        }
    );
    assert_eq!(digest(&sm).count, 2);
    // Delete removes either shape, reporting its version; the key can then take the other shape.
    assert_eq!(
        write(&mut sm, pos(6), &wire::encode_delete(b"l")),
        WriteReply::Ok { version: pos(1) }
    );
    assert_eq!(
        write(&mut sm, pos(7), &wire::encode_put(b"l", b"now-a-value")),
        WriteReply::Ok { version: pos(7) }
    );
    assert_eq!(
        write(&mut sm, pos(8), &wire::encode_delete(b"v")),
        WriteReply::Ok { version: pos(0) }
    );
    assert_eq!(
        append(&mut sm, pos(9), b"v", b"now-a-list"),
        AppendReply::Ok {
            version: pos(9),
            len: 1
        }
    );
    assert_eq!(sm.last_applied(), Some(pos(9)));
}

#[test]
fn list_caps_are_enforced() {
    let mut sm = KvSm::default();
    // Element count cap.
    for i in 0..wire::MAX_LIST_LEN as u64 {
        assert!(matches!(
            append(&mut sm, pos(i), b"n", b"x"),
            AppendReply::Ok { .. }
        ));
    }
    let p = pos(wire::MAX_LIST_LEN as u64);
    assert_eq!(
        append(&mut sm, p, b"n", b"x"),
        AppendReply::ListFull {
            len: wire::MAX_LIST_LEN as u32
        }
    );
    assert_eq!(list(&sm, b"n").len(), wire::MAX_LIST_LEN);
    // Byte cap: MAX_LIST_BYTES / MAX_VALUE full-size elements fit, one more does not.
    let full = vec![b'z'; wire::MAX_VALUE];
    let n = wire::MAX_LIST_BYTES / wire::MAX_VALUE;
    for i in 0..n as u64 {
        assert!(
            matches!(
                append(&mut sm, p + 32 * (i + 1), b"bytes", &full),
                AppendReply::Ok { .. }
            ),
            "element {i}"
        );
    }
    assert_eq!(
        append(&mut sm, p + 32 * (n as u64 + 1), b"bytes", b"1"),
        AppendReply::ListFull { len: n as u32 }
    );
    // Element size is the same as a value's.
    let mut frame = vec![wire::FORMAT_VERSION, wire::OP_APPEND, 1, 0, b'k'];
    frame.extend_from_slice(&vec![b'v'; wire::MAX_VALUE + 1]);
    assert_eq!(
        wire::decode_append_reply(&apply(&mut sm, p + 32 * 100, &frame)).unwrap(),
        AppendReply::BadRequest(wire::BAD_VALUE_LEN)
    );
    assert!(wire::try_encode_append(b"k", &vec![0u8; wire::MAX_VALUE + 1]).is_err());
    // The largest list reply fits well under the 1 MiB remote frame ceiling.
    let worst = 1 + 8 + 4 + wire::MAX_LIST_LEN * 4 + wire::MAX_LIST_BYTES;
    assert!(worst < 1 << 20, "{worst}");
    assert_eq!(
        wire::encode_append(b"k", &full).len(),
        4 + 1 + wire::MAX_VALUE,
        "same framing as PUT"
    );
}

#[test]
fn v1_commands_are_unchanged_on_the_wire() {
    // Byte-for-byte: v2 did not touch the v1 ops (the golden digest test in
    // sm_invariants.rs pins their semantics; this pins their encoding).
    assert_eq!(wire::encode_put(b"k", b"v"), [1, 1, 1, 0, b'k', b'v']);
    assert_eq!(wire::encode_delete(b"k"), [1, 2, 1, 0, b'k']);
    assert_eq!(
        wire::encode_cas(b"k", 7, b"v"),
        [1, 3, 1, 0, b'k', 7, 0, 0, 0, 0, 0, 0, 0, b'v']
    );
    assert_eq!(wire::encode_get(b"k"), [1, 1, 1, 0, b'k']);
    assert_eq!(wire::encode_digest(), [1, 2]);
    assert_eq!(wire::encode_append(b"k", b"v"), [1, 4, 1, 0, b'k', b'v']);
    assert_eq!(wire::encode_list(b"k"), [1, 3, 1, 0, b'k']);
}

// ---------------------------------------------------------------- images

#[test]
fn v2_image_round_trips_lists_and_is_version_2() {
    let mut sm = KvSm::default();
    write(&mut sm, pos(0), &wire::encode_put(b"v", b"value"));
    append(&mut sm, pos(1), b"l", b"one");
    append(&mut sm, pos(2), b"l", b"");
    append(&mut sm, pos(3), b"l", &[0xff; 1024]);
    let (h, at) = sm.freeze().unwrap();
    let mut img = Vec::new();
    KvSm::stream_snapshot(h, &mut img).unwrap();
    assert_eq!(&img[..4], &2u32.to_le_bytes(), "image_version 2");
    let mut fresh = KvSm::default();
    assert_eq!(
        fresh.install_snapshot(at + 64, &mut &img[..]).unwrap(),
        at + 64
    );
    assert_eq!(fresh.last_applied(), Some(at));
    assert_eq!(digest(&fresh), digest(&sm));
    assert_eq!(fresh.recompute_digest(), digest(&sm).digest);
    assert_eq!(list(&fresh, b"l"), list(&sm, b"l"));
    assert_eq!(get(&fresh, b"v"), get(&sm, b"v"));
    // Shape survives: appending after install continues the list.
    assert_eq!(
        append(&mut fresh, at + 64, b"l", b"four"),
        AppendReply::Ok {
            version: at + 64,
            len: 4
        }
    );
}

/// THE old-image test: `tests/fixtures/v1-golden.kvimage` was written by the
/// v1 binary's `stream_snapshot` (commit 9e82e84) from the golden script in
/// sm_invariants.rs; `v1-golden.meta` records what it froze.
#[test]
fn a_v1_image_installs_into_a_v2_binary_with_the_right_state() {
    let img = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/v1-golden.kvimage"
    ))
    .unwrap();
    assert_eq!(
        &img[..4],
        &1u32.to_le_bytes(),
        "the fixture really is a v1 image"
    );
    let meta = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/v1-golden.meta"
    ))
    .unwrap();
    let field = |k: &str| {
        meta.lines()
            .find_map(|l| l.strip_prefix(&format!("{k}=")))
            .unwrap()
            .to_string()
    };
    let frozen_at: u64 = field("frozen_at").parse().unwrap();
    let count: u64 = field("count").parse().unwrap();
    let want_digest = u64::from_str_radix(field("digest").trim_start_matches("0x"), 16).unwrap();

    let mut sm = KvSm::default();
    let p = frozen_at + 64;
    assert_eq!(sm.install_snapshot(p, &mut &img[..]).unwrap(), p);
    assert_eq!(
        sm.last_applied(),
        Some(frozen_at),
        "cursor from the v1 image"
    );
    let d = digest(&sm);
    assert_eq!(d.count, count);
    assert_eq!(
        d.digest, want_digest,
        "v2 must hash v1 value entries exactly as v1 did"
    );
    assert_eq!(sm.recompute_digest(), want_digest);
    // Spot-check contents against what the golden script did.
    assert_eq!(
        get(&sm, b"key001"),
        GetReply::Found {
            version: pos(68),
            value: b(b"cas-ok")
        }
    );
    assert_eq!(
        get(&sm, b"key000"),
        GetReply::NotFound,
        "deleted in the script"
    );
    assert_eq!(
        get(&sm, b"key002"),
        GetReply::Found {
            version: pos(2),
            value: b(&2u32.to_le_bytes())
        }
    );
    assert_eq!(
        get(&sm, b"fresh"),
        GetReply::Found {
            version: pos(69),
            value: b(b"created")
        }
    );
    assert_eq!(
        get(&sm, &[0u8; 256]),
        GetReply::Found {
            version: pos(71),
            value: b(&[0xffu8; 1024])
        }
    );
    // Every v1 entry is a value: list-read refuses it, and the store keeps working in v2 terms.
    assert_eq!(list(&sm, b"key001"), ListReply::WrongShape);
    assert_eq!(
        append(&mut sm, p, b"new-list", b"after-upgrade"),
        AppendReply::Ok { version: p, len: 1 }
    );
    assert_eq!(
        write(
            &mut sm,
            p + 32,
            &wire::encode_cas(b"key001", pos(68), b"cas-still-works")
        ),
        WriteReply::Ok { version: p + 32 }
    );
    // And what v2 writes back is a v2 image that reproduces this state.
    let (h, at) = sm.freeze().unwrap();
    let mut img2 = Vec::new();
    KvSm::stream_snapshot(h, &mut img2).unwrap();
    assert_eq!(&img2[..4], &2u32.to_le_bytes());
    let mut again = KvSm::default();
    again.install_snapshot(at + 32, &mut &img2[..]).unwrap();
    assert_eq!(digest(&again), digest(&sm));
}

#[test]
fn a_v1_image_with_a_bad_digest_is_still_refused() {
    let mut img = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/v1-golden.kvimage"
    ))
    .unwrap();
    let last = img.len() - 1;
    img[last] ^= 1;
    let mut sm = KvSm::default();
    assert!(sm.install_snapshot(10_000, &mut &img[..]).is_err());
    assert_eq!(sm.last_applied(), None);
}

// ---------------------------------------------------------------- properties with both shapes

#[derive(Debug, Clone)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Cas(Vec<u8>, u64, Vec<u8>),
    Append(Vec<u8>, Vec<u8>),
    Raw(Vec<u8>),
}
fn ops() -> impl Strategy<Value = Op> {
    let key = prop::collection::vec(any::<u8>(), 1..=2);
    let val = prop::collection::vec(any::<u8>(), 0..=16);
    prop_oneof![
        3 => (key.clone(), val.clone()).prop_map(|(k, v)| Op::Put(k, v)),
        2 => key.clone().prop_map(Op::Delete),
        1 => (key.clone(), any::<u64>(), val.clone()).prop_map(|(k, e, v)| Op::Cas(k, e, v)),
        4 => (key, val).prop_map(|(k, v)| Op::Append(k, v)),
        1 => prop::collection::vec(any::<u8>(), 0..=24).prop_map(Op::Raw),
    ]
}
fn enc(op: &Op) -> Vec<u8> {
    match op {
        Op::Put(k, v) => wire::encode_put(k, v),
        Op::Delete(k) => wire::encode_delete(k),
        Op::Cas(k, e, v) => wire::encode_cas(k, *e, v),
        Op::Append(k, v) => wire::encode_append(k, v),
        Op::Raw(b) => b.clone(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// A reference model (BTreeMap of enum) agrees with the store on every
    /// read, the digest recomputes, and a v2 image round-trips any history.
    #[test]
    fn model_digest_and_image_agree(script in prop::collection::vec(ops(), 0..80)) {
        use std::collections::BTreeMap;
        #[derive(Clone, Debug, PartialEq)]
        enum M { V(u64, Vec<u8>), L(u64, Vec<Vec<u8>>) }
        let mut model: BTreeMap<Vec<u8>, M> = BTreeMap::new();
        let mut sm = KvSm::default();
        for (i, op) in script.iter().enumerate() {
            let p = pos(i as u64);
            let out = apply(&mut sm, p, &enc(op));
            match op {
                Op::Put(k, v) => match model.get(k) {
                    Some(M::L(..)) => prop_assert_eq!(wire::decode_write_reply(&out).unwrap(), WriteReply::WrongShape),
                    _ => { model.insert(k.clone(), M::V(p, v.clone())); }
                },
                Op::Delete(k) => { model.remove(k); }
                Op::Cas(k, e, v) => match model.get(k) {
                    Some(M::L(..)) => prop_assert_eq!(wire::decode_write_reply(&out).unwrap(), WriteReply::WrongShape),
                    Some(M::V(cur, _)) if *cur == *e => { model.insert(k.clone(), M::V(p, v.clone())); }
                    None if *e == 0 => { model.insert(k.clone(), M::V(p, v.clone())); }
                    _ => { let r = wire::decode_write_reply(&out).unwrap(); prop_assert!(matches!(r, WriteReply::VersionMismatch { .. }), "{:?}", r); }
                },
                Op::Append(k, v) => match model.get_mut(k) {
                    Some(M::V(..)) => prop_assert_eq!(wire::decode_append_reply(&out).unwrap(), AppendReply::WrongShape),
                    Some(M::L(ver, items)) => { *ver = p; items.push(v.clone()); }
                    None => { model.insert(k.clone(), M::L(p, vec![v.clone()])); }
                },
                Op::Raw(_) => {}
            }
        }
        prop_assert_eq!(digest(&sm).count as usize, model.len());
        prop_assert_eq!(sm.recompute_digest(), digest(&sm).digest);
        for (k, m) in &model {
            match m {
                M::V(ver, v) => {
                    prop_assert_eq!(get(&sm, k), GetReply::Found { version: *ver, value: b(v) });
                    prop_assert_eq!(list(&sm, k), ListReply::WrongShape);
                }
                M::L(ver, items) => {
                    prop_assert_eq!(list(&sm, k), ListReply::Found { version: *ver, items: items.iter().map(|v| b(v)).collect() });
                    prop_assert_eq!(get(&sm, k), GetReply::WrongShape);
                }
            }
        }
        let (h, at) = sm.freeze().unwrap();
        let mut img = Vec::new();
        KvSm::stream_snapshot(h, &mut img).unwrap();
        let mut fresh = KvSm::default();
        prop_assert_eq!(fresh.install_snapshot(at + 32, &mut &img[..]).unwrap(), at + 32);
        prop_assert_eq!(digest(&fresh), digest(&sm));
        prop_assert_eq!(fresh.last_applied(), sm.last_applied());
    }
}
