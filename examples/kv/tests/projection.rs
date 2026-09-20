use kv_store::{KvSm, wire};
use uc_service::{ApplyCtx, RawStateMachine, SnapshotStateMachine};

fn apply(sm: &mut KvSm, pos: u64, frame: &[u8]) {
    let mut out = Vec::new();
    sm.apply(
        &mut ApplyCtx::new(pos, <KvSm as RawStateMachine>::IDENTITY),
        frame,
        &mut out,
    );
}

fn project(sm: &KvSm) -> String {
    let mut out = Vec::new();
    sm.project(&mut out).unwrap();
    String::from_utf8(out).unwrap()
}

#[test]
fn projection_is_unaffected_by_call_order_given_ordered_map_and_xor_digest() {
    // This guards the two structural properties the projection's
    // canonicality rests on: `OrdMap` iterates key-sorted regardless of
    // insertion order, and `digest` is an XOR fold, order-independent by
    // construction. It would fail if either were replaced (an
    // insertion-ordered map, or a non-commutative digest) — it does not by
    // itself prove canonicality across independently-derived histories;
    // that cross-history claim is `projection_survives_a_snapshot_roundtrip`'s.
    let mut a = KvSm::default();
    apply(&mut a, 32, &wire::encode_put(b"b", b"2"));
    apply(&mut a, 64, &wire::encode_put(b"a", b"1"));
    let mut b = KvSm::default();
    apply(&mut b, 64, &wire::encode_put(b"a", b"1"));
    apply(&mut b, 32, &wire::encode_put(b"b", b"2"));
    // Different call order → different cursor lines; compare the entry lines
    // (and everything else, which is order-independent).
    let strip = |s: String| {
        s.lines()
            .filter(|l| !l.starts_with("cursor="))
            .collect::<Vec<_>>()
            .join("\n")
    };
    assert_eq!(strip(project(&a)), strip(project(&b)));
    // key "a" = 0x61, value "1" = 0x31; written at position 64 in both
    // machines (Entry::version is the log position, not a per-key counter).
    assert!(project(&a).contains("key=61 version=64 shape=value bytes=31"));
}

#[test]
fn projection_survives_a_snapshot_roundtrip() {
    let mut a = KvSm::default();
    apply(&mut a, 32, &wire::encode_put(b"k", b"v"));
    apply(&mut a, 64, &wire::encode_append(b"l", b"x"));
    let (h, pos) = a.freeze().unwrap();
    let mut img = Vec::new();
    KvSm::stream_snapshot(h, &mut img).unwrap();
    let mut b = KvSm::default();
    b.install_snapshot(pos.max(96), &mut &img[..]).unwrap();
    assert_eq!(project(&a), project(&b));
}
