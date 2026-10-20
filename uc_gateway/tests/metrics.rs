// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! #173: the gateway's `/metrics`, `/healthz` and `/readyz` over a live edge
//! — the listener `uc2-gateway` starts on `[metrics] bind`, driven against a
//! real single-node cluster with one handshaken client, so the counters and
//! the grant gauges read what the edge actually holds.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use uc_gateway::metrics::{now_unix_ns, serve};
use uc_gateway::{Edge, EdgeConfig, Member};
use uc_lincheck::register::RegisterSm;
use uc_remote::frame::FrameType;
use uc_service::{ServiceBuilder, ServiceConfig, SessionConfig, Sessioned};

mod common;

fn get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut s = TcpStream::connect(addr).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: gw\r\nConnection: close\r\n\r\n").as_bytes())
        .unwrap();
    let mut raw = String::new();
    s.read_to_string(&mut raw).unwrap();
    let (head, body) = raw.split_once("\r\n\r\n").expect("header terminator");
    let code = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (code, body.to_string())
}

#[test]
fn metrics_health_and_readiness_read_the_live_edge() {
    let root = common::tempdir();
    let (node, dir) = common::start_single_node(root.path());
    let svc = ServiceBuilder::new(
        ServiceConfig::new(&dir, common::APP),
        Sessioned::new(RegisterSm::default(), SessionConfig::default()),
    )
    .start()
    .unwrap();
    common::await_serving(&node, 10);
    let edge = Edge::start(EdgeConfig {
        instance_dir: dir.clone(),
        app_id: common::APP.into(),
        listen: "127.0.0.1:0".parse().unwrap(),
        members: vec![Member {
            node_id: 0,
            gateway: "127.0.0.1:0".into(),
        }],
        ..EdgeConfig::defaults()
    })
    .unwrap();

    let tick = Arc::new(AtomicU64::new(now_unix_ns()));
    let obs = serve(
        "127.0.0.1:0".parse().unwrap(),
        edge.obs(),
        Arc::clone(&tick),
    )
    .unwrap();
    let addr = obs.tcp_addr().unwrap();

    let mut c = common::dial_raw(edge.local_addr());
    common::send_hello(&mut c, 0xABCD, common::APP);
    assert!(common::read_until(&mut c, FrameType::HelloOk, Duration::from_secs(5)).is_some());

    let (code, body) = get(addr, "/metrics");
    assert_eq!(code, 200);
    assert!(
        body.contains("\nuc2_gateway_connections_total 1\n"),
        "{body}"
    );
    // One connection takes min(budget, per_conn_inflight) = 256 credits of
    // the 4096-window's 3584 budget.
    assert!(
        body.contains("\nuc2_gateway_grant_outstanding 256\n"),
        "{body}"
    );
    assert!(body.contains("\nuc2_gateway_grant_budget 3584\n"), "{body}");

    assert_eq!(get(addr, "/healthz").0, 200);
    let (code, body) = get(addr, "/readyz");
    assert_eq!(code, 200, "{body}");
    assert_eq!(get(addr, "/nope").0, 404);

    // A tick loop that stopped stamping is neither live nor ready.
    tick.store(0, Ordering::Release);
    assert_eq!(get(addr, "/healthz").0, 503);
    assert_eq!(get(addr, "/readyz").0, 503);

    obs.stop();
    drop(c);
    edge.stop();
    node.stop();
    svc.stop();
}
