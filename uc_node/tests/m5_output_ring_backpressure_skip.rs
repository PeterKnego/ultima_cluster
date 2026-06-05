//! M5 Task 5.7 — verify the dispatcher's bounded-wait-then-skip behavior when
//! `service/output.ring` fills.
//!
//! Strategy: set `output_cap_bytes = 4 KiB` so the ring fills after a small
//! number of OutputFrames. Don't wire any OutputHandler service-side — the
//! `output_loop` isn't spawned and nothing drains `output.ring`. The node-side
//! dispatcher publishes the first frame (succeeds), waits up to 30 s for an
//! OutputResp that never comes (or is interrupted by stop), then for
//! subsequent frames hits the 1 s grace and skips.
//!
//! This matches the design intent: "If the service didn't wire up an
//! output_loop ... frames pile up on output.ring; the apply_dispatcher's 1 s
//! grace expires, then they're skipped" (spec §"Node-side awareness").
//!
//! Invariants tested:
//! * All 50 client submits succeed (apply path is decoupled from output path
//!   via the `try_send` mpsc channel).
//! * `output_progress` advances at most a couple steps (the very first frame
//!   may or may not have a published OutputFrame depending on timing; without
//!   a response, the dispatcher's await_output_resp times out and breaks the
//!   inner loop without advancing the marker).

use std::io::{Read, Write};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use uc_client::Client;
use uc_node::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeBuilder, NodeConfig, RaftTuning,
    ServiceRingConfig, TlsConfig,
};
use uc_service::runtime::ServiceConfig;
use uc_service::{ServiceBuilder, SnapshotError, StateMachine};

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

async fn wait_for_path(p: &std::path::Path, t: Duration) {
    let deadline = std::time::Instant::now() + t;
    while !p.exists() {
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for {}", p.display());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn m5_output_ring_backpressure_skip() {
    let _ = tracing_subscriber::fmt::try_init();

    let inst = TempDir::new().unwrap();
    let node_data = TempDir::new().unwrap();
    let svc_data = TempDir::new().unwrap();
    let instance_dir = inst.path().to_owned();
    let app_id = "m5-backpressure".to_string();

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
        client_rings: ClientRingConfig::default(),
        // Tiny output ring — the dispatcher hits Full quickly and skips.
        service_rings: ServiceRingConfig {
            output_cap_bytes: 4 * 1024,
            output_max_msg: 1024,
        },
        log_durability: ultima_journal::Durability::Eventual,
    };

    let node_task =
        tokio::spawn(async move { NodeBuilder::new(cfg, Counter::default()).start().await });
    wait_for_path(&instance_dir.join("cnc.dat"), Duration::from_secs(5)).await;

    let svc_cfg = ServiceConfig {
        instance_dir: instance_dir.clone(),
        app_id: app_id.clone(),
        data_dir: svc_data.path().to_owned(),
        ..ServiceConfig::default()
    };
    // No output_handler wired — service-side output_loop isn't spawned,
    // so nothing drains output.ring. The node-side dispatcher publishes
    // one frame, then either times out on OutputResp or sees the ring
    // Full and skips after the 1 s grace.
    let svc_task =
        tokio::spawn(async move { ServiceBuilder::new(svc_cfg, Counter::default()).run().await });

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

    for _ in 0..50 {
        if node.current_leader().await == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(node.current_leader().await, Some(1));

    let client = Client::connect(&instance_dir, &app_id)
        .await
        .expect("connect");

    // Submit 50 commands. The apply path is fully decoupled from output —
    // submits should complete in well under 5 s.
    let start = std::time::Instant::now();
    for i in 0..50u64 {
        let _: Resp = client.submit(&Cmd::Inc(i)).await.expect("submit");
    }
    let submit_elapsed = start.elapsed();
    assert!(
        submit_elapsed < Duration::from_secs(5),
        "50 submits should complete in <5s; took {submit_elapsed:?}"
    );

    // Give the dispatcher time to fire the skip path multiple times.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let progress = node._test_output_progress();
    // With no service-side reader, the dispatcher publishes the first
    // frame (output.ring is empty initially), waits for a response that
    // never comes, eventually times out (or is stopped on shutdown).
    // output_progress should NOT have advanced past a small number.
    assert!(
        progress < 10,
        "output_progress should be small (< 10) — no service consumer to ack; \
         got {progress}"
    );

    // Ordered shutdown: client → service → node. With the stop-flag-aware
    // await_output_resp (Task 3.4 fix), shutdown completes promptly.
    client.shutdown().await.expect("client shutdown");
    service.shutdown().await.expect("service shutdown");
    node.shutdown().await.expect("node shutdown");
}
