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
fn projection_is_canonical_regardless_of_insertion_order() {
    // Same two facts (position 32 writes "b", position 64 writes "a") applied
    // in opposite call order to `a` and `b`. `Entry::version` is the log
    // position that wrote it (lib.rs's `apply`), so swapping which KEY gets
    // which position (rather than just the call order) would give the two
    // machines genuinely different entries/digests — not what "regardless of
    // insertion order" means. Keeping the position<->key pairing fixed and
    // only reversing the order the two calls happen in is what actually
    // exercises OrdMap's insertion-order independence: same final entries and
    // digest, different `last_applied` (whichever call happened last).
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
