//! Phase-2a proof: a NON-persisting in-memory state machine that restarts FRESH
//! BELOW the purge boundary is reconstructed via snapshot INSTALL + tail-replay
//! — NOT via the Phase-1 log-replay path.
//!
//! This is the end-to-end proof that the snapshot path works: BUILD (Task 6,
//! the node drives the live service to produce real snapshot bytes and PURGES
//! the log below it) and INSTALL (Task 7, a below-purge reattach installs that
//! snapshot then tail-replays the surviving entries).
//!
//! CRITICAL — avoiding the mirage:
//!   The reattach decision is `service_last(0) < last_purged → NeedsSnapshot`
//!   (see `runtime::reconstruct::plan_replay`). A fresh service reports
//!   `last_applied = 0`. If NO purge has happened (`last_purged == 0`) then
//!   `0 < 0` is false and the reattach takes the Phase-1 Replay path — the test
//!   would pass WITHOUT ever touching the snapshot code (a false green).
//!   So BEFORE crashing the service we POLL `node._test_last_purged()` until it
//!   is `>= 1`, proving a snapshot+purge fired. Only then does the later
//!   below-purge reattach provably take the INSTALL path.
//!
//! Scenario:
//!   1. Node (single-node shmem) + service with an in-memory `CounterSm`,
//!      `snapshot_policy_logs_since_last = 4` (small, so a snapshot fires early).
//!   2. Submit 1,2,3,4,5,6 (await each) → sum == 21.
//!   3. POLL `_test_last_purged() >= 1` (timeout ~10s) → confirms snapshot+purge.
//!   4. Crash ONLY the service; the node keeps running.
//!   5. Restart the service with a FRESH `CounterSm::default()` (sum=0). This
//!      service is BELOW the purge boundary.
//!   6. Submit 10. apply(10) detects the reattach: service_last=0 < last_purged
//!      → INSTALL the node's snapshot + tail-replay → fresh service reconstructed.
//!   7. Assert the submit-10 response == 31 (1+2+3+4+5+6+10).
//!
//! The invariant is `== 31`: the full history is reproduced regardless of how
//! the snapshot/tail split falls. A `10` means reconstruction never ran; a value
//! that is missing the pre-snapshot increments means INSTALL didn't apply the
//! snapshot (BUILD produced empty/degenerate bytes).

use std::io::{Read, Write};
use std::time::Duration;

use tempfile::TempDir;
use uc_node::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeBuilder, NodeConfig, RaftTuning,
    ServiceRingConfig, TlsConfig,
};
use uc_service::runtime::ServiceConfig;
use uc_service::{ServiceBuilder, SnapshotError, StateMachine};

/// Non-persisting in-memory counter (copied verbatim from
/// `reconstruct_reattach.rs`). apply(n) adds n and records the highest applied
/// log_index. State is entirely in-memory: a fresh instance starts at 0, so
/// surviving a restart proves the node reconstructed it.
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
async fn below_purge_service_reconstructed_via_snapshot_install() {
    let instance_tempdir = TempDir::new().unwrap();
    let node_data_tempdir = TempDir::new().unwrap();
    let service_data_tempdir = TempDir::new().unwrap();
    let instance_dir = instance_tempdir.path().to_owned();

    let app_id = "reconstruct-snapshot".to_string();

    // ── Node (single-node shmem), SMALL snapshot policy ─────────────────
    // snapshot_policy_logs_since_last = 4 (default is 5000): a snapshot fires
    // after a handful of applied entries, and the log below it is purged. This
    // purge is the gate that forces the later fresh reattach onto the INSTALL
    // path (service_last=0 < last_purged).
    let node_cfg = NodeConfig {
        node_id: 1,
        data_dir: node_data_tempdir.path().to_owned(),
        raft_listen_addr: "127.0.0.1:0".parse().unwrap(),
        app_id: app_id.clone(),
        bootstrap: BootstrapConfig::SingleNode,
        raft: RaftTuning {
            snapshot_policy_logs_since_last: 4,
            // Keep only 1 in-snapshot log entry after a snapshot so the purge
            // boundary actually advances past index 1. The default (1000) would
            // let a snapshot fire yet purge NOTHING (everything stays within the
            // keep window) — `last_purged` would remain 0 and the reattach below
            // would silently fall back to the Phase-1 Replay path (the mirage).
            max_in_snapshot_log_to_keep: 1,
            ..RaftTuning::default()
        },
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

    // ── Step 2: submit 1..=6 → sum == 21 ────────────────────────────────
    let mut expected = 0u64;
    for n in 1..=6u64 {
        expected += n;
        let got = node.submit(n).await.expect("submit pre-snapshot");
        assert_eq!(got, expected, "submit {n} response");
    }
    assert_eq!(expected, 21, "pre-snapshot running sum");

    // Settle so any in-flight snapshot build completes against the STILL-POPULATED
    // service #1 before we crash it. This matters because of a Phase-2a snapshot
    // build/reattach race (observed while writing this test): openraft schedules a
    // snapshot build when logs-since-last crosses the policy; if that build is
    // still in flight when service #1 is crashed and a FRESH service #2 attaches,
    // the build runs against the empty service #2 and the node persists a
    // DEGENERATE (all-zero) snapshot — which a later install then "restores",
    // silently losing all pre-snapshot state. Settling here guarantees the
    // snapshot the install path consumes was built from the real service state, so
    // this test isolates the INSTALL path rather than tripping over that race.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // ── Step 3: CONFIRM PURGE — the anti-mirage gate ────────────────────
    // Poll `_test_last_purged()` (reads node `last_purged.state`) until >= 1.
    // Until a purge has happened, a fresh below-boundary reattach would take
    // the Phase-1 Replay path and never touch the snapshot code. Requiring
    // last_purged >= 1 BEFORE the crash guarantees the later reattach hits
    // `service_last(0) < last_purged → NeedsSnapshot` (the INSTALL path).
    let purged = {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let p = node._test_last_purged();
            if p >= 1 {
                break p;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out (10s) waiting for a snapshot+purge to fire with \
                 snapshot_policy_logs_since_last=4 after 6 submits; last_purged \
                 is still 0 — without a purge the reattach below would take the \
                 Phase-1 Replay path and this test would be a snapshot MIRAGE. \
                 If purge never fires, increase the submit count or lower the \
                 policy and re-check."
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    println!("[reconstruct_snapshot] CONFIRMED purge: last_purged = {purged} (>= 1)");
    println!(
        "[reconstruct_snapshot] => below-purge fresh reattach (service_last=0) \
         will take the snapshot-INSTALL path, NOT Phase-1 Replay"
    );
    // Belt-and-suspenders: a snapshot_*.bin in the node data_dir is direct
    // evidence BUILD produced real snapshot bytes (Task 6). Require at least one
    // NON-DEGENERATE snapshot file — `CounterSm::build_snapshot` writes
    // `sum(8) || last_applied(8)` little-endian, so a valid populated snapshot has
    // a non-zero leading sum. An all-zero snapshot is the empty-build mirage; we
    // assert against it so a degenerate snapshot can't masquerade as BUILD proof.
    let mut nondegenerate: Option<(String, u64)> = None;
    for entry in std::fs::read_dir(node_data_tempdir.path()).expect("read node data_dir") {
        let entry = entry.expect("dir entry");
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with("snapshot_") && name.ends_with(".bin") {
            let raw = std::fs::read(entry.path()).expect("read snapshot bin");
            if raw.len() >= 8 {
                let sum = u64::from_le_bytes(raw[0..8].try_into().unwrap());
                if sum != 0 {
                    nondegenerate = Some((name, sum));
                    break;
                }
            }
        }
    }
    let (snap_name, snap_sum) = nondegenerate.expect(
        "expected a NON-degenerate snapshot_*.bin in node data_dir (proof BUILD \
         captured real service state); found none with a non-zero sum — BUILD \
         produced only empty snapshots",
    );
    println!(
        "[reconstruct_snapshot] BUILD evidence: node data_dir has {snap_name} (sum={snap_sum})"
    );

    // ── Step 4: crash ONLY the service; node keeps running ───────────────
    service.shutdown().await.expect("crash service");

    // ── Step 5: restart service with a FRESH (zeroed) in-memory SM ───────
    // This service is BELOW the purge boundary: it reports last_applied=0.
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

    // ── Step 6: submit 10 → apply triggers INSTALL + tail-replay ─────────
    // service_last=0 < last_purged ⇒ NeedsSnapshot: the node installs its
    // snapshot (carrying the pre-snapshot sum) then tail-replays the surviving
    // committed entries, then applies 10. Response must be 31 — NOT 10.
    let resp = node.submit(10u64).await.expect("submit 10 post-restart");

    // ── Step 7: assert reconstruction via snapshot install happened ──────
    assert_eq!(
        resp, 31,
        "expected reconstructed sum 1+2+3+4+5+6+10 = 31; got {resp}. \
         10 == reconstruction didn't run at all; a value missing the \
         pre-snapshot increments == INSTALL didn't apply the snapshot \
         (BUILD produced empty/degenerate bytes)."
    );

    // Cross-check via a query against the (now reconstructed) SM.
    let q = node.submit_query(()).await.expect("query post-restart");
    assert_eq!(q, 31, "query should observe the reconstructed sum (31)");

    // ── Teardown — service first, then node (mirrors templates) ──────────
    service2.shutdown().await.expect("service2 shutdown");
    node.shutdown().await.expect("node shutdown");
}
