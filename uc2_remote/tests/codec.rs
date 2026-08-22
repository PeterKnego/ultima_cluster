use uc2_remote::frame::*;

#[test]
fn header_round_trip_and_length_prefix() {
    let h = Header { ty: FrameType::Submit, flags: 0, version: PROTOCOL_VERSION, client_id: 0xC11E, seq: 42 };
    let mut buf = Vec::new();
    encode_frame(&mut buf, h, b"payload");
    assert_eq!(buf.len(), HEADER_LEN + 7);
    let (got, plen) = decode_header(&buf).unwrap();
    assert_eq!(got, h);
    assert_eq!(plen, 7);
    assert_eq!(&buf[HEADER_LEN..], b"payload");
}

#[test]
fn short_and_oversized_and_bad_type_are_errors() {
    assert!(matches!(decode_header(&[0u8; 3]), Err(FrameError::Short { .. })));
    let mut buf = Vec::new();
    encode_frame(&mut buf, Header { ty: FrameType::Ping, flags: 0, version: PROTOCOL_VERSION, client_id: 1, seq: 1 }, &[]);
    buf[4] = 0xEE;
    assert!(matches!(decode_header(&buf), Err(FrameError::BadType(0xEE))));
    let mut big = buf.clone();
    big[0..4].copy_from_slice(&(MAX_FRAME_LEN + 1).to_le_bytes());
    assert!(matches!(decode_header(&big), Err(FrameError::TooLong(_))));
}

#[test]
fn typed_payloads_round_trip() {
    let mut out = Vec::new();
    HelloOk { credits: 512, leader: Some(2), leader_addr: "10.0.0.2:9100" }.encode(&mut out);
    let h = HelloOk::decode(&out).unwrap();
    assert_eq!((h.credits, h.leader, h.leader_addr), (512, Some(2), "10.0.0.2:9100"));
    out.clear();
    HelloOk { credits: 1, leader: None, leader_addr: "" }.encode(&mut out);
    assert_eq!(HelloOk::decode(&out).unwrap().leader, None);
    out.clear();
    ResponseMeta { credits: 7, acked_seq: 9, position: 4096 }.encode(&mut out);
    assert_eq!(out.len(), 20);
    let m = ResponseMeta::decode(&out).unwrap();
    assert_eq!((m.credits, m.acked_seq, m.position), (7, 9, 4096));
    out.clear();
    Retry { reason: RETRY_NOT_SERVING, retry_after_us: 250_000 }.encode(&mut out);
    assert_eq!(Retry::decode(&out).unwrap().retry_after_us, 250_000);
    out.clear();
    Leader { node_id: 3, addr: "h3:9100" }.encode(&mut out);
    assert_eq!(Leader::decode(&out).unwrap().addr, "h3:9100");
    assert!(matches!(Leader::decode(&out[..3]), Err(FrameError::Short { .. })));
}
