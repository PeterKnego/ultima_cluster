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
    encoded[0] = 99; // unknown msg type
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

#[test]
fn oversized_body_len_rejected() {
    // Manually construct a header claiming a 100 MiB body — exceeds the 16 MiB cap.
    use bytes::BufMut;
    let mut buf = bytes::BytesMut::with_capacity(14);
    buf.put_u8(MessageType::VoteReq as u8); // msg_type
    buf.put_u8(0); // flags
    buf.put_u64(1); // request_id
    buf.put_u32(100 * 1024 * 1024); // body_len = 100 MiB
    // (no body bytes — decode should reject before getting that far)
    let mut bytes = buf.freeze();
    let result = Frame::decode(&mut bytes);
    let err = match result {
        Ok(_) => panic!("oversized body_len should be rejected"),
        Err(e) => e,
    };
    let msg = format!("{err}");
    assert!(msg.contains("exceeds maximum"), "unexpected error: {msg}");
}

use openraft::Vote;
use openraft::raft::VoteRequest;
use uc_node::network::codec;

#[test]
fn vote_req_roundtrip() {
    let req: VoteRequest<u64> = VoteRequest::new(
        Vote::new(7, 3),
        Some(openraft::LogId::new(
            openraft::CommittedLeaderId::new(5, 0),
            42,
        )),
    );
    let bytes = codec::encode_vote_req(&req).expect("encode");
    let decoded = codec::decode_vote_req(&bytes).expect("decode");
    assert_eq!(decoded.vote, req.vote);
    assert_eq!(decoded.last_log_id, req.last_log_id);
}
