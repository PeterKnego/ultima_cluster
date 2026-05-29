//! 1 node + 1 service — dropped Client's session unlinked by GC (M4 Task 5.6).
//!
//! Connects one client, drops it without calling `shutdown()`, and waits for
//! the node-side `session_gc` task to unlink the session file. GC constants:
//!   - `STALE_AFTER` = 5 s (file is stale when heartbeat_seq stops advancing)
//!   - `GC_TICK`     = 2 s (GC runs every 2 s)
//!
//! When the `Client` is dropped, `Drop` stops the heartbeat ticker (sets the
//! stop flag) but does NOT remove the session file. The GC should unlink it
//! within `STALE_AFTER + GC_TICK` ≈ 7 s. We allow 10 s for slack.

use std::io::{Read, Write};
use std::time::Duration;

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
async fn m4_client_session_gc() {
    let _ = tracing_subscriber::fmt::try_init();

    // Bring up node + service via the shared fixture, but manage the client's
    // lifecycle in the test (this test deliberately *drops* a client without
    // calling shutdown(), so it must own it rather than let the fixture do so).
    let fixture = ClusterFixture::<Counter>::single_node(0)
        .await
        .expect("spawn single-node cluster");
    let instance_dir = fixture.instance_path().to_owned();

    // ── Connect and immediately drop ────────────────────────────────────────
    let client = Client::connect(&instance_dir, fixture.app_id())
        .await
        .unwrap();
    let cid = client.client_id();
    let session_path = instance_dir
        .join("clients")
        .join("sessions.dir")
        .join(format!("{cid}.session"));

    assert!(
        session_path.exists(),
        "session file should exist after connect"
    );

    // Drop without calling shutdown(). Client::Drop stops the heartbeat ticker
    // (so heartbeat_seq stops advancing) but does NOT remove the session file.
    // The node-side session_gc should unlink it once the heartbeat goes stale.
    drop(client);

    // Poll until the file disappears, with a generous deadline (STALE_AFTER=5s
    // + GC_TICK=2s + extra slack = 10s total).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while session_path.exists() {
        if std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert!(
        !session_path.exists(),
        "session_gc should have unlinked the file at {}",
        session_path.display()
    );

    fixture.shutdown().await.expect("cluster shutdown");
}
