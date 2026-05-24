//! 1 node + 1 service + 1 client (all in-process tokio tasks).
//! Client connects, submits two increments, queries via snapshot, shuts down.
//!
//! Setup (node + service + client spawn, leader-ready wait) is provided by
//! `uc_node::test_support::ClusterFixture`; this test keeps only its assertions.

use std::io::{Read, Write};

use serde::{Deserialize, Serialize};
use uc_node::test_support::ClusterFixture;
use uc_service::{SnapshotError, StateMachine};

#[derive(Default)]
struct Counter {
    value: u64,
    last_applied: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum Cmd {
    Inc(u64),
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Resp {
    value: u64,
}

impl StateMachine for Counter {
    type Command = Cmd;
    type Response = Resp;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, log_index: u64, cmd: Cmd) -> Resp {
        let Cmd::Inc(d) = cmd;
        self.value = self.value.wrapping_add(d);
        self.last_applied = Some(log_index);
        Resp { value: self.value }
    }
    fn query(&self, _: ()) -> u64 {
        self.value
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
    fn build_snapshot(&self, _: &mut dyn Write) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }
    fn install_snapshot(&mut self, _: &mut dyn Read) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }
}

#[tokio::test]
async fn m4_client_single_node() {
    let _ = tracing_subscriber::fmt::try_init();

    let fixture = ClusterFixture::<Counter>::single_node(1)
        .await
        .expect("spawn single-node cluster");

    let client = fixture.client(0);

    let r: Resp = client.submit(&Cmd::Inc(5)).await.expect("inc 5");
    assert_eq!(r, Resp { value: 5 });
    let r: Resp = client.submit(&Cmd::Inc(3)).await.expect("inc 3");
    assert_eq!(r, Resp { value: 8 });

    let v: u64 = client.query_snapshot(&()).await.expect("query");
    assert_eq!(v, 8);

    fixture.shutdown().await.expect("cluster shutdown");
}
