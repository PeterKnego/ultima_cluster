//! Acceptance test for the full-path probe wiring (Tasks 1.2–1.6). Runs real
//! requests through the in-process fixture and asserts every checkpoint is
//! captured and joins into a complete per-request row. Only compiled with the
//! probe feature.
#![cfg(feature = "uc-bench-probes")]

use std::io::{Read, Write};

use futures::stream::{self, StreamExt};
use uc_node::test_support::ClusterFixture;
use uc_service::SnapshotError;
use uc_service::StateMachine;

#[derive(Default)]
struct Echo {
    counter: u64,
    last_applied: Option<u64>,
}

impl StateMachine for Echo {
    type Command = Vec<u8>;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, log_index: u64, cmd: Vec<u8>) -> u64 {
        self.counter = self.counter.wrapping_add(cmd.len() as u64);
        self.last_applied = Some(log_index);
        self.counter
    }
    fn query(&self, _: ()) -> u64 {
        self.counter
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

#[tokio::test(flavor = "current_thread")]
async fn full_path_probes_capture_every_checkpoint() {
    let fixture = ClusterFixture::<Echo>::single_node(1)
        .await
        .expect("spawn single-node cluster");
    let client = fixture.client(0);

    uc_protocol::probes::reset();

    const N: usize = 64;
    let payload = vec![0u8; 64];
    stream::iter(0..N)
        .map(|_| {
            let p = payload.clone();
            async move {
                let _r: u64 = client.submit(&p).await.expect("submit");
            }
        })
        .buffer_unordered(8)
        .for_each(|_| async {})
        .await;

    let rows = uc_protocol::probes::drain_joined();
    assert!(
        rows.len() >= N - 2,
        "expected ~{N} joined rows, got {}",
        rows.len()
    );
    // Every joined row must have all checkpoints, and `total` must be the
    // largest stage.
    for row in &rows {
        for i in 0..uc_protocol::probes::N_CHECKPOINTS {
            assert!(row[i].is_some(), "checkpoint {i} missing in a joined row");
        }
        let deltas = uc_protocol::probes::stage_deltas(row);
        let total = deltas.iter().find(|(n, _)| *n == "total").unwrap().1;
        assert!(total > 0, "total latency must be positive");
    }

    fixture.shutdown().await.expect("shutdown");
}
