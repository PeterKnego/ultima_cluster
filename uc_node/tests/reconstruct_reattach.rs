//! Phase-1 proof: a NON-persisting in-memory state machine survives a
//! SERVICE-ONLY restart while the NODE stays up — reconstructed by the node
//! replaying committed entries.
//!
//! Scenario:
//!   1. Node (single-node shmem) + service with an in-memory `CounterSm`.
//!   2. Submit 1, 2, 3 → counter == 6.
//!   3. Crash ONLY the service (`Service::shutdown`); the node keeps running.
//!   4. Restart the service against the SAME instance_dir with a FRESH
//!      `CounterSm::default()` (starts at 0).
//!   5. Submit 10. This apply triggers the node's catch-up: it replays the
//!      committed history `(service_last_applied, up_to]` into the fresh service
//!      before/at applying 10.
//!   6. Assert the submit-10 response is 16 (== 1+2+3+10). Without
//!      reconstruction a fresh SM would return 10.
//!
//! How the feature triggers: on (re)attach the service publishes its SM's
//! `last_applied()` (0 for a fresh in-memory SM) and bumps `service_epoch`.
//! The node seeds `last_seen_epoch` at startup; on the NEXT apply after the
//! epoch changes it runs catch-up, replaying `(0, up_to]` into the service.

use std::io::{Read, Write};
use std::time::Duration;

use tempfile::TempDir;
use uc_node::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeBuilder, NodeConfig, RaftTuning,
    ServiceRingConfig, TlsConfig,
};
use uc_service::runtime::ServiceConfig;
use uc_service::{ServiceBuilder, SnapshotError, StateMachine};

/// Non-persisting in-memory counter. apply(n) adds n and records the highest
/// applied log_index. State is entirely in-memory: a fresh instance starts at
/// 0, so surviving a restart proves the node reconstructed it.
#[derive(Default)]
struct CounterSm {
    sum: u64,
    last_applied: Option<u64>,
}

impl StateMachine for CounterSm {
    type Command = u64;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, log_index: u64, cmd: u64) -> u64 {
        self.sum += cmd;
        self.last_applied = Some(log_index);
        self.sum
    }
    fn query(&self, _q: ()) -> u64 {
        self.sum
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
    fn build_snapshot(&self, dst: &mut dyn Write) -> Result<u64, SnapshotError> {
        let li = self.last_applied.unwrap_or(0);
        dst.write_all(&self.sum.to_le_bytes())?;
        dst.write_all(&li.to_le_bytes())?;
        Ok(li)
    }
    fn install_snapshot(&mut self, src: &mut dyn Read) -> Result<u64, SnapshotError> {
        let mut b = [0u8; 8];
        src.read_exact(&mut b)?;
        self.sum = u64::from_le_bytes(b);
        src.read_exact(&mut b)?;
        let li = u64::from_le_bytes(b);
        self.last_applied = (li != 0).then_some(li);
        Ok(li)
    }
}

/// Poll for the existence of `path` for up to `timeout`.
async fn wait_for_path(path: &std::path::Path, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    while !path.exists() {
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for {}", path.display());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn in_memory_sm_reconstructed_on_service_restart() {
    let instance_tempdir = TempDir::new().unwrap();
    let node_data_tempdir = TempDir::new().unwrap();
    let service_data_tempdir = TempDir::new().unwrap();
    let instance_dir = instance_tempdir.path().to_owned();

    let app_id = "reconstruct-reattach".to_string();

    // ── Node (single-node shmem) ────────────────────────────────────────
    let node_cfg = NodeConfig {
        node_id: 1,
        data_dir: node_data_tempdir.path().to_owned(),
        raft_listen_addr: "127.0.0.1:0".parse().unwrap(),
        app_id: app_id.clone(),
        bootstrap: BootstrapConfig::SingleNode,
        raft: RaftTuning::default(),
        tls: TlsConfig::default(),
        ipc_mode: IpcMode::Shmem {
            instance_dir: instance_dir.clone(),
        },
        client_rings: ClientRingConfig::default(),
        service_rings: ServiceRingConfig::default(),
        log_durability: ultima_journal::Durability::Eventual,
    };
    let node_task = tokio::spawn(async move {
        NodeBuilder::new(node_cfg, CounterSm::default())
            .start()
            .await
    });

    // Node start() blocks internally on wait_for_service_ready; spawn the
    // service once cnc.dat exists so it can attach.
    wait_for_path(&instance_dir.join("cnc.dat"), Duration::from_secs(5)).await;

    // ── Service #1 (fresh in-memory CounterSm) ──────────────────────────
    // ServiceConfig isn't Clone, so build a fresh one per service start
    // (both point at the SAME instance_dir / app_id / data_dir).
    let mk_svc_cfg = || ServiceConfig {
        instance_dir: instance_dir.clone(),
        app_id: app_id.clone(),
        data_dir: service_data_tempdir.path().to_owned(),
        ..ServiceConfig::default()
    };
    let svc_cfg1 = mk_svc_cfg();
    let svc_task = tokio::spawn(async move {
        ServiceBuilder::new(svc_cfg1, CounterSm::default())
            .run()
            .await
    });

    let node = tokio::time::timeout(Duration::from_secs(15), node_task)
        .await
        .expect("node start timed out")
        .expect("node task panic")
        .expect("node start error");
    let service = tokio::time::timeout(Duration::from_secs(15), svc_task)
        .await
        .expect("service start timed out")
        .expect("service task panic")
        .expect("service start error");

    // ── Wait for leader ─────────────────────────────────────────────────
    for _ in 0..50 {
        if node.current_leader().await == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        node.current_leader().await,
        Some(1),
        "leader did not converge"
    );

    // ── Step 2: submit 1, 2, 3 → counter == 6 ───────────────────────────
    assert_eq!(node.submit(1u64).await.expect("submit 1"), 1);
    assert_eq!(node.submit(2u64).await.expect("submit 2"), 3);
    assert_eq!(node.submit(3u64).await.expect("submit 3"), 6);

    // ── Step 3: crash ONLY the service; node keeps running ───────────────
    service.shutdown().await.expect("crash service");

    // ── Step 4: restart service with a FRESH (zeroed) in-memory SM ───────
    let svc_cfg2 = mk_svc_cfg();
    let service2 = tokio::time::timeout(
        Duration::from_secs(15),
        tokio::spawn(async move {
            ServiceBuilder::new(svc_cfg2, CounterSm::default())
                .run()
                .await
        }),
    )
    .await
    .expect("service restart timed out")
    .expect("service restart task panic")
    .expect("service restart error");

    // Give the node a moment to observe the new READY/epoch.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── Step 5: submit 10 → apply triggers node-driven catch-up ──────────
    // The fresh SM is at 0; the node replays committed (1,2,3) into it before
    // applying 10, so the response must be 16 — NOT 10.
    let resp = node.submit(10u64).await.expect("submit 10 post-restart");

    // ── Step 6: assert reconstruction happened ──────────────────────────
    assert_eq!(
        resp, 16,
        "expected reconstructed sum 1+2+3+10=16; got {resp} \
         (10 would mean the fresh SM was NOT reconstructed)"
    );

    // Cross-check via a query against the (now reconstructed) SM.
    let q = node.submit_query(()).await.expect("query post-restart");
    assert_eq!(q, 16, "query should observe the reconstructed sum");

    // ── Step 7: teardown — service first, then node (mirrors templates) ──
    service2.shutdown().await.expect("service2 shutdown");
    node.shutdown().await.expect("node shutdown");
}
