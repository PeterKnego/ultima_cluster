// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Read-your-writes through two edges: write via the leader's gateway, read
//! via a follower's gateway carrying the writer's token (spec 2026-10-08 §5.4).

use std::time::Duration;

use uc_gateway::{Edge, EdgeConfig, Member};
use uc_lincheck::register::{Cmd, CmdResp};
use uc_remote::{Consistency, RemoteClient, RemoteConfig};

use uc_remote::frame::{FLAG_LINEARIZABLE, FLAG_MIN_POSITION};

mod common;

fn enc(c: &Cmd) -> Vec<u8> {
    bincode::serde::encode_to_vec(c, bincode::config::standard()).unwrap()
}
fn dec(b: &[u8]) -> CmdResp {
    bincode::serde::decode_from_slice(b, bincode::config::standard())
        .unwrap()
        .0
}
fn read_query() -> Vec<u8> {
    bincode::serde::encode_to_vec((), bincode::config::standard()).unwrap()
}
fn edge_on(dir: &std::path::Path) -> Edge {
    Edge::start(EdgeConfig {
        instance_dir: dir.to_path_buf(),
        app_id: common::APP.into(),
        listen: "127.0.0.1:0".parse().unwrap(),
        members: vec![Member {
            node_id: 0,
            gateway: "127.0.0.1:0".into(),
        }],
        ..EdgeConfig::defaults()
    })
    .unwrap()
}
fn client_to(edge: &Edge) -> RemoteClient {
    RemoteClient::connect(RemoteConfig {
        app_id: common::APP.into(),
        members: vec![edge.local_addr().to_string()],
        request_timeout: Duration::from_secs(20),
        ..Default::default()
    })
    .unwrap()
}

#[test]
fn a_follower_gateway_read_sees_a_write_made_through_the_leaders_gateway() {
    let root = common::tempdir();
    let mut slots = common::start_cluster(root.path(), 3);
    let leader = common::await_single_leader(&slots, 30);
    let follower = (leader + 1) % 3;
    let le = edge_on(&slots[leader].instance_dir);
    let fe = edge_on(&slots[follower].instance_dir);
    let writer = client_to(&le);
    let reader = client_to(&fe);
    for v in 1..=20u64 {
        let r = writer.submit(&enc(&Cmd::Write(v))).unwrap().wait().unwrap();
        assert_eq!(dec(&r.bytes), CmdResp::WriteAck);
        reader.observe(writer.read_token());
        let r = reader
            .query(&read_query(), Consistency::ReadYourWrites)
            .unwrap()
            .wait()
            .unwrap();
        let got: Option<u64> =
            bincode::serde::decode_from_slice(&r.bytes, bincode::config::standard())
                .unwrap()
                .0;
        assert_eq!(got, Some(v), "the follower gateway's read missed write {v}");
        assert!(
            r.position >= writer.read_token().as_u64(),
            "answer below the token"
        );
    }
    writer.shutdown();
    reader.shutdown();
    le.stop();
    fe.stop();
    for s in &mut slots {
        s.stop();
    }
}

#[test]
fn a_protocol_v1_client_is_refused_by_name() {
    use uc_remote::frame::{FrameType, HELLO_REFUSED_VERSION, Header, Hello, HelloRefused};
    let root = common::tempdir();
    let mut slots = common::start_cluster(root.path(), 3);
    let leader = common::await_single_leader(&slots, 30);
    let edge = edge_on(&slots[leader].instance_dir);
    let mut c = common::dial_raw(edge.local_addr());
    let mut out = Vec::new();
    Hello {
        app_id: common::APP,
    }
    .encode(&mut out);
    c.write_frame(
        Header {
            ty: FrameType::Hello,
            flags: 0,
            version: 1,
            client_id: 7,
            seq: 0,
        },
        &out,
    )
    .unwrap();
    let (_, p) = common::read_until_frame(&mut c, FrameType::HelloRefused, Duration::from_secs(5))
        .expect("a v1 HELLO is refused");
    assert_eq!(
        HelloRefused::decode(&p).unwrap().reason,
        HELLO_REFUSED_VERSION
    );
    edge.stop();
    for s in &mut slots {
        s.stop();
    }
}

#[test]
fn a_query_with_both_min_position_and_linearizable_closes_the_connection() {
    use uc_remote::frame::{FrameType, Header};
    let root = common::tempdir();
    let mut slots = common::start_cluster(root.path(), 3);
    let leader = common::await_single_leader(&slots, 30);
    let edge = edge_on(&slots[leader].instance_dir);

    let query = |flags: u8, seq: u64| {
        let mut payload = 0u64.to_le_bytes().to_vec();
        payload.extend_from_slice(&read_query());
        (
            Header {
                ty: FrameType::Query,
                flags,
                version: uc_remote::frame::PROTOCOL_VERSION,
                client_id: 7,
                seq,
            },
            payload,
        )
    };

    // Control: the min-position flag alone is answered (not a close).
    let mut ok = common::dial_raw(edge.local_addr());
    common::send_hello(&mut ok, 7, common::APP);
    common::read_until(&mut ok, FrameType::HelloOk, Duration::from_secs(5)).expect("HELLO_OK");
    let (h, p) = query(FLAG_MIN_POSITION, 1);
    ok.write_frame(h, &p).unwrap();
    let got = common::read_until(&mut ok, FrameType::Response, Duration::from_secs(10));
    assert!(got.is_some(), "a plain min-position query is answered");

    // Both flags: the edge closes, and no RESPONSE ever arrives.
    let mut c = common::dial_raw(edge.local_addr());
    common::send_hello(&mut c, 8, common::APP);
    common::read_until(&mut c, FrameType::HelloOk, Duration::from_secs(5)).expect("HELLO_OK");
    let (h, p) = query(FLAG_MIN_POSITION | FLAG_LINEARIZABLE, 1);
    c.write_frame(h, &p).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut closed = false;
    while std::time::Instant::now() < deadline {
        match c.read_frame(common::READ_STALL) {
            Ok(Some((h, _))) => assert_ne!(h.ty, FrameType::Response, "no RESPONSE for both flags"),
            Ok(None) => {}
            Err(_) => {
                closed = true;
                break;
            }
        }
    }
    assert!(closed, "the edge closes a query carrying both flags");

    edge.stop();
    for s in &mut slots {
        s.stop();
    }
}
