//! Wire-level spec §6.1: the bytes a level-12 binary emits, pinned. From
//! 0.13.0 on this file is what "encode at the committed level" means in
//! practice: a release that changes a level-12 encoding — even by one
//! byte — fails here, which is the regression class the 2.11.0 relayout
//! cost 27 %. Refresh the hex ONLY in the task that bumps the wire minor,
//! and only for the NEW level's file (`golden_level13.rs`), never this one.
use uc_protocol::v2::datagram::{
    DATAGRAM_HEADER_LEN, DGRAM_KIND_DATA, DatagramHeader, write_datagram_header,
};
use uc_protocol::v2::frame::{
    CLUSTER_BODY_PREFIX_LEN, ClusterKind, NEW_TERM_BODY_LEN, write_new_term_body,
};
use uc_protocol::v2::settings::{Settings, WIRE_RAISE_AUTO, encode_settings};

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[test]
fn settings_v7_genesis_at_level_12() {
    let mut s = Settings::genesis_default();
    s.wire_level = 12;
    s.wire_raise = WIRE_RAISE_AUTO;
    let mut b = Vec::new();
    encode_settings(&s, &mut b);
    assert_eq!(
        hex(&b),
        "07000000000000000000000000000000000000000000000000000000000000000001000100000000000000000000000c01"
    );
}

#[test]
fn datagram_header_data_at_level_12() {
    let h = DatagramHeader {
        position: 0x40,
        leadership_term_id: 1,
        kind: DGRAM_KIND_DATA,
        wire: 12,
        key_epoch: 0,
    };
    let mut b = [0u8; DATAGRAM_HEADER_LEN];
    write_datagram_header(&mut b, &h);
    assert_eq!(hex(&b), "400000000000000001000000010c0000");
}

#[test]
fn new_term_body_is_eight_zero_bytes() {
    let mut b = [0xffu8; NEW_TERM_BODY_LEN];
    write_new_term_body(&mut b);
    assert_eq!(hex(&b), "0000000000000000");
}

#[test]
fn cluster_kinds_known_at_level_12() {
    // 1..=7 decode; 8 does not. A kind added by a later level starts at 8.
    for k in 1..=7u8 {
        assert!(ClusterKind::from_u8(k).is_some(), "{k}");
    }
    assert!(ClusterKind::from_u8(8).is_none());
    assert_eq!(CLUSTER_BODY_PREFIX_LEN, 8);
}
