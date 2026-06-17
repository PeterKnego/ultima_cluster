//! Pipelined-apply burst test.
//!
//! Brings up a single shmem node+service (same pattern as
//! `m3_shmem_single_node`) and fires N=500 concurrent `Inc(1)` submits so
//! that openraft commits them in batches. The pipelined apply must publish
//! the whole batch to `service/apply.ring`, await all responses in FIFO
//! order, and surface each response back to its caller without losing,
//! reordering, or duplicating any entry.
//!
//! **Assertion**: every submit succeeds and the final counter value equals N
//! (500 × Inc(1) → 500).
//!
//! # Why this matters
//!
//! The old code issued one ring round-trip per entry even when openraft batched
//! several entries into a single `apply()` call. The new pipelined path
//! publishes a whole run first, then awaits responses in FIFO order. This test
//! creates the batch pressure (500 concurrent submits from a single-node
//! cluster) that exercises the fast-path run in `ShmemAdaptedStateMachine::apply`.

use std::io::{Read, Write};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use uc_node::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeBuilder, NodeConfig, NodeHandle, RaftTuning,
    ServiceRingConfig, TlsConfig,
};
use uc_service::runtime::ServiceConfig;
use uc_service::{ServiceBuilder, SnapshotError, StateMachine};

// ── State machine ────────────────────────────────────────────────────────────

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
        let Cmd::Inc(delta) = cmd;
        self.value = self.value.wrapping_add(delta);
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
        let buf = bincode::serde::encode_to_vec(
            (self.value, self.last_applied),
            bincode::config::standard(),
        )
        .map_err(|e| SnapshotError::Codec(e.to_string()))?;
        Ok((buf, self.last_applied.unwrap_or(0)))
    }
    fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn Write) -> Result<(), SnapshotError> {
        dst.write_all(&handle)?;
        Ok(())
    }
    fn install_snapshot(&mut self, src: &mut dyn Read) -> Result<u64, SnapshotError> {
        let mut buf = Vec::new();
        src.read_to_end(&mut buf)?;
        let ((v, la), _) = bincode::serde::decode_from_slice::<(u64, Option<u64>), _>(
            &buf,
            bincode::config::standard(),
        )
        .map_err(|e| SnapshotError::Codec(e.to_string()))?;
        self.value = v;
        self.last_applied = la;
        Ok(la.unwrap_or(0))
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

async fn wait_for_path(path: &std::path::Path, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    while !path.exists() {
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for {}", path.display());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_leader(node: &NodeHandle<Counter>, id: u64, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if node.current_leader().await == Some(id) {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("leader {id} not elected within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ── Test ─────────────────────────────────────────────────────────────────────

/// Fire 500 concurrent `Inc(1)` submits through a single shmem node+service.
///
/// Openraft will batch many of them into each `apply()` call, exercising the
/// pipelined fast-path that publishes a whole run before awaiting responses.
/// After all 500 futures resolve we verify:
///   1. Every submit succeeded (no errors).
///   2. The counter value read back via `submit_query` is exactly 500.
///
/// The counter is additive so the final value is order-independent, but
/// *all* entries must have been applied exactly once — a lost or doubled entry
/// would produce a wrong sum.
#[tokio::test]
async fn apply_pipeline_burst() {
    const N: u64 = 500;

    let _ = tracing_subscriber::fmt::try_init();

    let instance_dir = Arc::new(TempDir::new().unwrap());
    let node_data_dir = TempDir::new().unwrap();
    let svc_data_dir = TempDir::new().unwrap();

    let app_id = "apply-pipeline-burst".to_string();

    // ── Node ─────────────────────────────────────────────────────────────
    let node_cfg = NodeConfig {
        node_id: 1,
        data_dir: node_data_dir.path().to_owned(),
        raft_listen_addr: "127.0.0.1:0".parse().unwrap(),
        app_id: app_id.clone(),
        bootstrap: BootstrapConfig::SingleNode,
        raft: RaftTuning::default(),
        tls: TlsConfig::default(),
        ipc_mode: IpcMode::Shmem {
            instance_dir: instance_dir.path().to_owned(),
        },
        client_rings: ClientRingConfig::default(),
        service_rings: ServiceRingConfig::default(),
        log_durability: ultima_journal::Durability::Eventual,
    };

    let instance_dir_clone = instance_dir.clone();
    let node_task =
        tokio::spawn(async move { NodeBuilder::new(node_cfg, Counter::default()).start().await });

    // Wait for cnc.dat before attaching the service.
    wait_for_path(
        &instance_dir_clone.path().join("cnc.dat"),
        Duration::from_secs(10),
    )
    .await;

    // ── Service ───────────────────────────────────────────────────────────
    let svc_cfg = ServiceConfig {
        instance_dir: instance_dir.path().to_owned(),
        app_id: app_id.clone(),
        data_dir: svc_data_dir.path().to_owned(),
        ..ServiceConfig::default()
    };
    let svc_task =
        tokio::spawn(async move { ServiceBuilder::new(svc_cfg, Counter::default()).run().await });

    // Await both handles (node::start() returns once the service handshake
    // completes, so the order matches m3_shmem_single_node).
    let node = tokio::time::timeout(Duration::from_secs(30), node_task)
        .await
        .expect("node start timed out")
        .expect("node task panicked")
        .expect("node start error");

    let service = tokio::time::timeout(Duration::from_secs(30), svc_task)
        .await
        .expect("service start timed out")
        .expect("service task panicked")
        .expect("service start error");

    // ── Wait for leader election ──────────────────────────────────────────
    wait_for_leader(&node, 1, Duration::from_secs(15)).await;

    // ── Burst: fire N concurrent Inc(1) submits ───────────────────────────
    //
    // Collect all submit futures into a vec and join_all so they all race
    // into raft's client_write concurrently. Openraft batches the committed
    // entries and calls our apply() with runs of Normal entries — exactly
    // what the pipelined path is built to handle.
    let mut futs = Vec::with_capacity(N as usize);
    for _ in 0..N {
        futs.push(node.submit(Cmd::Inc(1)));
    }
    let results: Vec<_> = futures::future::join_all(futs).await;

    // ── Assert: all N submits succeeded ──────────────────────────────────
    let mut errors = 0usize;
    for (i, r) in results.iter().enumerate() {
        if let Err(e) = r {
            eprintln!("submit[{i}] failed: {e:?}");
            errors += 1;
        }
    }
    assert_eq!(errors, 0, "{errors} out of {N} submits failed");

    // ── Assert: final counter value == N (all entries applied exactly once) ──
    //
    // Use submit_query (routes through the service's query ring) so we read
    // the service's live SM state, not any cached snapshot. This also
    // confirms the query path still works after a heavy apply burst.
    let final_value = node.submit_query(()).await.expect("final query");
    assert_eq!(
        final_value, N,
        "expected counter={N} after {N} Inc(1) submits, got {final_value}"
    );

    println!("apply_pipeline_burst: all {N} writes applied, counter={final_value} ✓");

    // ── Shutdown ──────────────────────────────────────────────────────────
    service.shutdown().await.expect("service shutdown");
    node.shutdown().await.expect("node shutdown");
}
