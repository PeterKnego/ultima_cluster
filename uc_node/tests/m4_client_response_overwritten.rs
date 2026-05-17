//! Validates `ClientError::ResponseOverwritten` — a slow client's broadcast
//! reader gets lapped by the producer, so in-flight requests resolve to the
//! `Overwritten` variant (M4 Task 5.7).
//!
//! Strategy:
//!   1. Boot a 1-node + 1-service harness with 32 KiB broadcast rings.
//!   2. Connect a "slow" client (broadcast reader paused immediately after
//!      connect via the test-helper) and a "driver" client (normal).
//!   3. Warm up with a few driver submits so the cluster is fully up.
//!   4. Pause the slow client's broadcast reader.
//!   5. Slow client issues `submit(Inc(42))` — the dispatcher writes a
//!      `SubmitResponse` to the broadcast, but the paused reader doesn't
//!      consume it.  The `send_and_await` future stays blocked on the oneshot.
//!   6. Driver client floods the cluster with enough commands to lap the
//!      32 KiB broadcast ring at least once.
//!   7. Resume the slow reader.  Its next `try_read` returns
//!      `RingError::Overwritten`, which the reader translates to
//!      `RawResponse::Overwritten` on every pending oneshot.
//!   8. The slow client's `submit()` future resolves to
//!      `Err(ClientError::ResponseOverwritten)`.

use std::io::{Read, Write};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use uc_client::{Client, ClientError};
use uc_node::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeBuilder, NodeConfig, RaftTuning, TlsConfig,
};
use uc_service::runtime::ServiceConfig;
use uc_service::{ServiceBuilder, SnapshotError, StateMachine};

// ── Minimal counter state machine ──────────────────────────────────────────

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

// ── Helpers ─────────────────────────────────────────────────────────────────

async fn wait_for_path(p: &std::path::Path, t: Duration) {
    let deadline = std::time::Instant::now() + t;
    while !p.exists() {
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for {}", p.display());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ── Test ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn m4_client_response_overwritten() {
    let _ = tracing_subscriber::fmt::try_init();

    let inst = TempDir::new().unwrap();
    let node_data = TempDir::new().unwrap();
    let svc_data = TempDir::new().unwrap();
    let instance_dir = inst.path().to_owned();
    let app_id = "m4-overwritten".to_string();

    // Use tiny 4 KiB rings so the broadcast laps after ~128 responses
    // (~32 B each: 16 header + ~9 payload + 4 crc + 3 align = 32 B).
    // This lets the driver flood with only ~200 submits to lap the ring,
    // completing well within the slow client's 10 s timeout.
    let cfg = NodeConfig {
        node_id: 1,
        data_dir: node_data.path().to_owned(),
        raft_listen_addr: "127.0.0.1:0".parse().unwrap(),
        app_id: app_id.clone(),
        bootstrap: BootstrapConfig::SingleNode,
        raft: RaftTuning::default(),
        tls: TlsConfig::default(),
        ipc_mode: IpcMode::Shmem {
            instance_dir: instance_dir.clone(),
        },
        client_rings: ClientRingConfig {
            cap_bytes: 4 * 1024,
            max_msg: 512,
        },
    };

    let node_task = tokio::spawn(async move {
        NodeBuilder::new(cfg, Counter::default()).start().await
    });
    wait_for_path(&instance_dir.join("cnc.dat"), Duration::from_secs(5)).await;

    let svc_cfg = ServiceConfig {
        instance_dir: instance_dir.clone(),
        app_id: app_id.clone(),
        data_dir: svc_data.path().to_owned(),
        ..ServiceConfig::default()
    };
    let svc_task = tokio::spawn(async move {
        ServiceBuilder::new(svc_cfg, Counter::default()).run().await
    });

    let node = tokio::time::timeout(Duration::from_secs(15), node_task)
        .await
        .expect("node timeout")
        .expect("node panic")
        .expect("node start");
    let service = tokio::time::timeout(Duration::from_secs(15), svc_task)
        .await
        .expect("svc timeout")
        .expect("svc panic")
        .expect("svc start");

    // Wait for leader election.
    for _ in 0..50 {
        if node.current_leader().await == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(node.current_leader().await, Some(1), "no leader elected");

    // ── Connect clients ─────────────────────────────────────────────────────
    let slow = Client::connect(&instance_dir, &app_id).await.expect("slow");
    let driver = Client::connect(&instance_dir, &app_id).await.expect("driver");

    // Warm-up: a few driver submits to confirm the pipeline is live.
    for _ in 0..3 {
        let _: Resp = driver.submit(&Cmd::Inc(1)).await.expect("warm-up");
    }

    // ── Pause the slow client's broadcast reader ────────────────────────────
    slow._test_pause_broadcast_reader();

    // ── Slow client fires one submit (will stay pending) ────────────────────
    // Wrap in Arc so we can share it between the spawned task and the main
    // task without moving ownership prematurely.
    let slow_arc = Arc::new(slow);
    let slow_for_task = Arc::clone(&slow_arc);
    let pending_submit = tokio::spawn(async move {
        slow_for_task.submit::<Cmd, Resp>(&Cmd::Inc(42)).await
    });

    // Give the submit time to write its SubmitFrame and for the dispatcher to
    // publish the SubmitResponse into the broadcast ring.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // ── Driver floods the broadcast ring until it laps ──────────────────────
    // 4 KiB ring / ~32 B per framed SubmitResponse ≈ 128 responses per lap.
    // 300 sequential driver submits ≈ 2+ laps — well past the slow consumer's
    // stale head.  Sequential is intentional: we need ALL writes committed
    // before we resume the slow reader so that slow.head is definitely
    // > capacity behind publish_position.
    //
    // Each Raft round-trip is ≈ 1–5 ms on a single-node loopback cluster, so
    // 300 submits ≈ 0.3–1.5 s — well within the slow client's 10 s timeout.
    for i in 0u64..300 {
        let _: Resp = driver.submit(&Cmd::Inc(i)).await.expect("driver flood");
    }

    // ── Resume the slow reader ──────────────────────────────────────────────
    // Its next try_read will return RingError::Overwritten (producer_pos −
    // head > capacity); the reader drains all in-flight oneshotss with
    // RawResponse::Overwritten.
    slow_arc._test_resume_broadcast_reader();

    // ── Assert the pending submit resolves to ResponseOverwritten ───────────
    // Give the resumed reader up to 5 s to detect Overwritten + signal the
    // oneshot.  The pending_submit itself has a 10 s timeout, but that clock
    // started ticking when it was spawned; the reader is now unblocked so this
    // should resolve in milliseconds.
    let result = tokio::time::timeout(Duration::from_secs(5), pending_submit)
        .await
        .expect("pending_submit did not resolve within 5 s after reader resume")
        .expect("pending_submit task panicked");

    match result {
        Err(ClientError::ResponseOverwritten) => {
            // Expected path — test passes.
        }
        other => panic!("expected Err(ResponseOverwritten), got {other:?}"),
    }

    // ── Cleanup ─────────────────────────────────────────────────────────────
    let slow = Arc::try_unwrap(slow_arc)
        .map_err(|_| "slow Arc still has refs after pending_submit resolved")
        .unwrap();
    slow.shutdown().await.unwrap();
    driver.shutdown().await.unwrap();
    service.shutdown().await.unwrap();
    node.shutdown().await.unwrap();
}
