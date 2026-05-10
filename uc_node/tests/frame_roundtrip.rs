use bytes::Bytes;
use uc_node::network::frame::{Frame, MessageType};

#[test]
fn encode_decode_empty_body() {
    let frame = Frame::new_request(MessageType::VoteReq, 42, Bytes::new());
    let encoded = frame.encode();
    let mut bytes = encoded.freeze();
    let decoded = Frame::decode(&mut bytes).expect("decode");
    assert_eq!(decoded.msg_type, MessageType::VoteReq);
    assert_eq!(decoded.flags, 0);
    assert_eq!(decoded.request_id, 42);
    assert_eq!(decoded.body.len(), 0);
}

#[test]
fn encode_decode_with_body() {
    let body = Bytes::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
    let frame = Frame::new_response(MessageType::AppendEntriesResp, 0xdeadbeef, body.clone());
    let encoded = frame.encode();
    let mut bytes = encoded.freeze();
    let decoded = Frame::decode(&mut bytes).expect("decode");
    assert_eq!(decoded.msg_type, MessageType::AppendEntriesResp);
    assert_eq!(decoded.flags, 1);
    assert!(decoded.is_response());
    assert_eq!(decoded.request_id, 0xdeadbeef);
    assert_eq!(decoded.body, body);
}

#[test]
fn corrupted_crc_rejected() {
    let frame = Frame::new_request(MessageType::VoteReq, 1, Bytes::from(vec![42]));
    let mut encoded = frame.encode();
    // Flip a bit in the body.
    encoded[14] ^= 0xff;
    let mut bytes = encoded.freeze();
    let result = Frame::decode(&mut bytes);
    assert!(result.is_err(), "expected crc mismatch error");
}

#[test]
fn unknown_msg_type_rejected() {
    let mut encoded = Frame::new_request(MessageType::VoteReq, 1, Bytes::new()).encode();
    encoded[0] = 99;                            // unknown msg type
    let mut bytes = encoded.freeze();
    let result = Frame::decode(&mut bytes);
    assert!(result.is_err());
}

#[tokio::test]
async fn read_async_round_trip() {
    let body = Bytes::from(vec![10, 20, 30]);
    let frame = Frame::new_request(MessageType::InstallSnapshotReq, 7, body.clone());
    let encoded = frame.encode().freeze();
    let mut reader = std::io::Cursor::new(encoded.to_vec());
    let decoded = Frame::read_async(&mut reader).await.expect("read_async");
    assert_eq!(decoded.msg_type, MessageType::InstallSnapshotReq);
    assert_eq!(decoded.request_id, 7);
    assert_eq!(decoded.body, body);
}
