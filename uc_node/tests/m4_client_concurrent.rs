//! 1 node + 1 service, 4 concurrent uc_clients (M4 Task 5.3).
//!
//! Four clients each submit 50 `Inc` commands concurrently via `tokio::join!`.
//! All commands are additive, so the final state is deterministic regardless
//! of interleaving order: 50 × (1 + 2 + 3 + 4) = 500.
//!
//! Setup is provided by `uc_node::test_support::ClusterFixture::single_node(4)`.

use std::io::{Read, Write};

use serde::{Deserialize, Serialize};
use uc_client::Client;
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
    type SnapshotHandle = Vec<u8>;

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
    fn freeze(&self) -> Result<(Vec<u8>, u64), SnapshotError> {
        Ok((Vec::new(), self.last_applied.unwrap_or(0)))
    }
    fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn Write) -> Result<(), SnapshotError> {
        dst.write_all(&handle)?;
        Ok(())
    }
    fn install_snapshot(&mut self, _: &mut dyn Read) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }
}

async fn submit_many(c: &Client, delta: u64, count: usize) {
    for _ in 0..count {
        let _: Resp = c.submit(&Cmd::Inc(delta)).await.unwrap();
    }
}

#[tokio::test]
async fn m4_client_concurrent() {
    let _ = tracing_subscriber::fmt::try_init();

    let fixture = ClusterFixture::<Counter>::single_node(4)
        .await
        .expect("spawn single-node cluster with 4 clients");

    // 50 increments each: deltas 1, 2, 3, 4.
    // Total: 50 × (1 + 2 + 3 + 4) = 500.
    tokio::join!(
        submit_many(fixture.client(0), 1, 50),
        submit_many(fixture.client(1), 2, 50),
        submit_many(fixture.client(2), 3, 50),
        submit_many(fixture.client(3), 4, 50),
    );

    let v: u64 = fixture.client(0).query_snapshot(&()).await.unwrap();
    assert_eq!(v, 50 * (1 + 2 + 3 + 4));

    fixture.shutdown().await.expect("cluster shutdown");
}
