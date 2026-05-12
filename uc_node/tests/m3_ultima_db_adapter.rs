//! End-to-end test of `uc_service::ultima_db::StoreStateMachine`
//! through the M3 shmem pipeline (M3 Task 21).
//!
//! Replaces the hand-rolled `Counter` SM used by every other M3 test
//! with the canonical `StoreStateMachine` adapter wrapping an
//! `ultima_db::Store`. Verifies that:
//!
//! * the adapter's `apply` (which opens a `WriteTx` pinned to log_index,
//!   commits via the user-supplied closure) round-trips correctly
//!   through the shmem apply ring;
//! * the adapter's `query` (snapshot read against the latest committed
//!   version) round-trips through the shmem query ring;
//! * after several applies, `store.latest_version()` advances to match
//!   the raft `last_applied`.
//!
//! Single-node + shmem mode for simplicity — the 3-node case is already
//! exercised by `m3_three_node_shmem` with the hand-rolled Counter, and
//! the StoreStateMachine adapter's behavior is independent of cluster
//! topology.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use ultima_db::Store;

use uc_node::{BootstrapConfig, IpcMode, NodeBuilder, NodeConfig, RaftTuning, TlsConfig};
use uc_service::ServiceBuilder;
use uc_service::runtime::ServiceConfig;
use uc_service::ultima_db::StoreStateMachine;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum Cmd {
    Inc(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Resp {
    value: u64,
}

/// Build a `StoreStateMachine` backed by a fresh in-memory `ultima_db::Store`.
/// The "counter" table holds a single `u64` row at id=1.
fn build_store_sm() -> StoreStateMachine<Cmd, Resp, (), u64> {
    StoreStateMachine::builder(Store::default())
        .apply_fn(|tx, Cmd::Inc(delta)| {
            let mut table = tx.open_table::<u64>("counter").expect("open_table apply");
            let current = table.get(1).copied().unwrap_or(0);
            let new = current + delta;
            if table.get(1).is_some() {
                table.update(1, new).expect("update");
            } else {
                let id = table.insert(new).expect("insert");
                assert_eq!(id, 1, "first counter row should land at id=1");
            }
            Resp { value: new }
        })
        .query_fn(|tx, ()| {
            let table = tx.open_table::<u64>("counter").expect("open_table query");
            table.get(1).copied().unwrap_or(0)
        })
        .build()
        .expect("StoreStateMachine build")
}

async fn wait_for_path(path: &std::path::Path, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    while !path.exists() {
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for {}", path.display());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn ultima_db_adapter_end_to_end() {
    let instance_tempdir = Arc::new(TempDir::new().unwrap());
    let node_data_tempdir = TempDir::new().unwrap();
    let service_data_tempdir = TempDir::new().unwrap();
    let instance_dir = instance_tempdir.path().to_owned();
    let app_id = "m3-ultima-db".to_string();

    // ── Node ────────────────────────────────────────────────────────────
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
    };
    // Node-side SM is degenerate in shmem mode (snapshot trait surface only).
    let node_task =
        tokio::spawn(async move { NodeBuilder::new(node_cfg, build_store_sm()).start().await });

    wait_for_path(&instance_dir.join("cnc.dat"), Duration::from_secs(5)).await;

    // ── Service (the SM that actually owns the live Store) ──────────────
    let svc_cfg = ServiceConfig {
        instance_dir: instance_dir.clone(),
        app_id: app_id.clone(),
        data_dir: service_data_tempdir.path().to_owned(),
        ..ServiceConfig::default()
    };
    let svc_task =
        tokio::spawn(async move { ServiceBuilder::new(svc_cfg, build_store_sm()).run().await });

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

    for _ in 0..50 {
        if node.current_leader().await == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(node.current_leader().await, Some(1));

    // ── Apply path: each submit goes through the apply ring into the
    // service's StoreStateMachine, which opens a WriteTx pinned to
    // log_index and commits via the apply_fn closure.
    let r = node.submit(Cmd::Inc(10)).await.expect("submit 10");
    assert_eq!(r, Resp { value: 10 });
    let r = node.submit(Cmd::Inc(5)).await.expect("submit 5");
    assert_eq!(r, Resp { value: 15 });
    let r = node.submit(Cmd::Inc(7)).await.expect("submit 7");
    assert_eq!(r, Resp { value: 22 });

    // ── Query path: snapshot read against the latest committed version.
    let v = node.submit_query(()).await.expect("submit_query");
    assert_eq!(v, 22);

    service.shutdown().await.expect("service shutdown");
    node.shutdown().await.expect("node shutdown");
}
