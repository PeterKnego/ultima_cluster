//! Targeted network-partition linearizability tests (cargo feature
//! `fault-injection`). Each isolates part of a 3-node cluster, drives ops, then
//! heals, and asserts WGL-linearizability plus a scenario-specific property:
//!   - minority partition: majority keeps committing; isolated node serves no
//!     stale read; catches up on heal.
//!   - leader isolation: the majority elects a NEW leader; old leader can't commit.
//!   - quorum loss: writes/reads fail cleanly (never a false Ok); recover on heal.
//!
//! Run: cargo test -p uc_node --features fault-injection --test lin_partition -- --test-threads=1
//!
//! Each test uses the default (current-thread) `#[tokio::test]` runtime, like the
//! `lin_register` capstone and the m3 cluster tests. Under a `multi_thread`
//! runtime the 3-node shmem boot flakes hard: openraft's apply-worker debug
//! assertion can fire on a runtime worker thread during initial membership apply
//! and wedge `Node::start()` (boot times out). current-thread boots reliably.
#![cfg(feature = "fault-injection")]

#[path = "lincheck/mod.rs"]
mod lincheck;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use lincheck::cluster::{LinCluster, ReadOutcome, SubmitOutcome};
use uc_lincheck::checker::{Verdict, check_register};
use uc_lincheck::history::{History, Outcome};
use uc_lincheck::model::{Op, RegResp};
use uc_lincheck::register::{Cmd, CmdResp};

/// Hold a partition long enough to guarantee an election under default tuning
/// (election_timeout_max = 2000 ms).
const PARTITION_HOLD: Duration = Duration::from_millis(3500);

/// Leader-routed worker: same op mix as the capstone, recording into `history`.
async fn worker(
    id: u32,
    cluster: Arc<LinCluster>,
    history: Arc<History>,
    stop: Arc<AtomicBool>,
    last_seen: Arc<AtomicU64>,
    mut rng: StdRng,
) {
    while !stop.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(15)).await;
        match rng.random_range(0..3u8) {
            0 => {
                let v = rng.random_range(1..1000u64);
                let inv = history.invoke();
                let outcome = match cluster.submit_cmd(&Cmd::Write(v)).await {
                    SubmitOutcome::Ok(_) => Outcome::Ok(RegResp::Ack),
                    SubmitOutcome::Indeterminate => Outcome::Indeterminate,
                    SubmitOutcome::Fatal(e) => panic!("fatal submit: {e}"),
                };
                history.record(id, Op::Write(v), inv, outcome);
            }
            1 => {
                let inv = history.invoke();
                let outcome = match cluster.read().await {
                    ReadOutcome::Ok(v) => {
                        if let Some(x) = v {
                            last_seen.store(x, Ordering::Relaxed);
                        }
                        Outcome::Ok(RegResp::Value(v))
                    }
                    ReadOutcome::Indeterminate => Outcome::Indeterminate,
                    ReadOutcome::Fatal(e) => panic!("fatal read: {e}"),
                };
                history.record(id, Op::Read, inv, outcome);
            }
            _ => {
                let old = if rng.random_bool(0.7) {
                    last_seen.load(Ordering::Relaxed)
                } else {
                    rng.random_range(1..1000u64)
                };
                let new = rng.random_range(1..1000u64);
                let inv = history.invoke();
                let outcome = match cluster.submit_cmd(&Cmd::Cas { old, new }).await {
                    SubmitOutcome::Ok(CmdResp::CasResult(b)) => {
                        if b {
                            last_seen.store(new, Ordering::Relaxed);
                        }
                        Outcome::Ok(RegResp::CasOk(b))
                    }
                    SubmitOutcome::Ok(other) => panic!("unexpected cas resp: {other:?}"),
                    SubmitOutcome::Indeterminate => Outcome::Indeterminate,
                    SubmitOutcome::Fatal(e) => panic!("fatal cas: {e}"),
                };
                history.record(id, Op::Cas { old, new }, inv, outcome);
            }
        }
    }
}

/// Spawn 3 leader-routed workers; return their join handles.
fn spawn_workers(
    cluster: &Arc<LinCluster>,
    history: &Arc<History>,
    stop: &Arc<AtomicBool>,
    last_seen: &Arc<AtomicU64>,
    seed: u64,
) -> Vec<tokio::task::JoinHandle<()>> {
    (0..3u32)
        .map(|w| {
            let rng = StdRng::seed_from_u64(seed ^ (w as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
            tokio::spawn(worker(
                w,
                cluster.clone(),
                history.clone(),
                stop.clone(),
                last_seen.clone(),
                rng,
            ))
        })
        .collect()
}

/// Run the WGL checker. A `Violation` is a real linearizability bug → panic NOW
/// (never retried). `Inconclusive` is tolerated (checker budget); `Linearizable`
/// is success.
fn assert_linearizable(entries: &[uc_lincheck::history::Entry], seed: u64, label: &str) {
    let ok = History::ok_count(entries);
    eprintln!("[lin_partition::{label}] seed={seed} ops={} ok={ok}", entries.len());
    match check_register(entries) {
        Verdict::Linearizable => {}
        Verdict::Inconclusive => {
            eprintln!("[lin_partition::{label}] seed={seed}: Inconclusive (checker budget)");
        }
        Verdict::Violation => panic!("[{label}] NOT linearizable (seed={seed})"),
    }
}

/// Cleanly shut down workers + cluster on EVERY exit path (success or transient
/// failure) so a retry doesn't leak QUIC ports / `/dev/shm` rings / a wedged
/// node. Returns the recorded history entries.
async fn teardown(
    cluster: Arc<LinCluster>,
    stop: &Arc<AtomicBool>,
    handles: Vec<tokio::task::JoinHandle<()>>,
    history: Arc<History>,
) -> Vec<uc_lincheck::history::Entry> {
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        // A worker only ever panics on an unexpected `Fatal` client error or a
        // wrong CAS response — both genuine bugs. Re-raise it here so a worker
        // `Fatal` fails the test (a panic propagates past the retry loop, so it
        // is NOT silently absorbed as a transient retry).
        if let Err(e) = h.await
            && e.is_panic()
        {
            std::panic::resume_unwind(e.into_panic());
        }
    }
    let cluster = Arc::try_unwrap(cluster).ok().expect("sole owner");
    cluster.shutdown().await;
    Arc::try_unwrap(history).ok().expect("sole owner").into_entries()
}

async fn run_minority(seed: u64) -> Result<(), String> {
    let cluster = Arc::new(LinCluster::start_3().await);
    let history = Arc::new(History::default());
    let stop = Arc::new(AtomicBool::new(false));
    let last_seen = Arc::new(AtomicU64::new(0));
    let handles = spawn_workers(&cluster, &history, &stop, &last_seen, seed);

    tokio::time::sleep(Duration::from_millis(800)).await;
    let before = History::ok_count(&history.snapshot());

    let isolated = cluster.partition_minority().await.expect("a follower");
    tokio::time::sleep(PARTITION_HOLD).await;
    let after = History::ok_count(&history.snapshot());
    // TRANSIENT: majority must keep committing while a follower is isolated.
    // Defer the verdict so teardown still runs.
    let majority_progressed = after > before;

    // Drive reads against the isolated node so the WGL checker sees them — the
    // checker (post-teardown) is the oracle for "no stale read". With this
    // minority partition the isolated node can't reach a quorum, so reads come
    // back Indeterminate; an `Ok` carrying a stale value would surface as a WGL
    // Violation. A `Fatal` is unexpected and panics immediately in `read_from`.
    for _ in 0..5 {
        let inv = history.invoke();
        let outcome = match cluster.read_from(isolated).await {
            ReadOutcome::Ok(v) => Outcome::Ok(RegResp::Value(v)),
            ReadOutcome::Indeterminate => Outcome::Indeterminate,
            ReadOutcome::Fatal(e) => panic!("fatal isolated read: {e}"),
        };
        history.record(100, Op::Read, inv, outcome);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    cluster.heal().await;
    cluster.wait_for_stable_leader(Duration::from_secs(15)).await;
    tokio::time::sleep(Duration::from_millis(800)).await;

    let entries = teardown(cluster, &stop, handles, history).await;

    if !majority_progressed {
        return Err(format!(
            "majority did not progress during minority partition ({before} -> {after})"
        ));
    }
    if History::ok_count(&entries) < 30 {
        return Err("too few Ok ops; run is vacuous".into());
    }
    assert_linearizable(&entries, seed, "minority");
    Ok(())
}

#[tokio::test]
async fn minority_partition_and_heal() {
    for attempt in 1..=3 {
        match run_minority(7).await {
            Ok(()) => return,
            Err(e) => eprintln!(
                "[lin_partition::minority] attempt {attempt}/3 transient: {e}; retrying"
            ),
        }
    }
    panic!("minority: failed after 3 transient attempts");
}

async fn run_leader_isolation(seed: u64) -> Result<(), String> {
    let cluster = Arc::new(LinCluster::start_3().await);
    let history = Arc::new(History::default());
    let stop = Arc::new(AtomicBool::new(false));
    let last_seen = Arc::new(AtomicU64::new(0));
    let handles = spawn_workers(&cluster, &history, &stop, &last_seen, seed);

    tokio::time::sleep(Duration::from_millis(800)).await;
    let old_leader = cluster.leader_id().await.expect("leader");

    let isolated = cluster.partition_leader().await.expect("leader");
    assert_eq!(isolated, old_leader);
    tokio::time::sleep(PARTITION_HOLD).await;

    // The majority must elect a NEW leader. We cannot rely on `leader_id()` /
    // worker routing here: the isolated old leader sees no higher term while
    // partitioned, so it keeps *claiming* leadership in its own view forever, and
    // `leader_id()` is first-wins across clients — so it (and the leader-routed
    // workers/`read()`) stay pinned to the stale old leader, which can't commit.
    // To observe the majority's choice we probe the two non-isolated nodes
    // directly: only the actual leader answers a linearizable read with `Ok`; a
    // follower returns `Indeterminate` (NotLeader). The node that serves `Ok` is
    // the new leader. This both confirms a NEW leader emerged AND that it can
    // commit (a linearizable read passes a ReadIndex barrier through a quorum).
    let majority: Vec<_> = cluster
        .node_ids()
        .await
        .into_iter()
        .filter(|&n| n != isolated)
        .collect();
    let mut new_leader: Option<uc_node::NodeId> = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        for &n in &majority {
            if let ReadOutcome::Ok(_) = cluster.read_from(n).await {
                new_leader = Some(n);
                break;
            }
        }
        if new_leader.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    // TRANSIENT: the majority may not have converged on a committing leader
    // within the probe window (the deferred openraft apply-worker flake manifests
    // here). Defer the verdict so the live SAFETY check + teardown still run.
    let elected_new = match new_leader {
        Some(n) => {
            // SAFETY: the elected leader must be a NEW one, never the old leader.
            assert_ne!(n, old_leader, "majority failed to elect a NEW leader");
            true
        }
        None => false,
    };

    // SAFETY (live): the old leader is isolated and must NOT be able to commit
    // (no split-brain): a linearizable read against it cannot pass its ReadIndex
    // barrier (it can't reach a quorum), so it returns Indeterminate, never Ok.
    // Probe while the cluster is still live and partitioned. Panic NOW on
    // violation — this is split-brain, never retried.
    assert!(
        !matches!(cluster.read_from(isolated).await, ReadOutcome::Ok(_)),
        "isolated old leader served a linearizable read — split-brain"
    );

    cluster.heal().await;
    cluster.wait_for_stable_leader(Duration::from_secs(15)).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let entries = teardown(cluster, &stop, handles, history).await;

    if !elected_new {
        return Err("no committing new leader found within the probe window".into());
    }
    if History::ok_count(&entries) < 30 {
        return Err("too few Ok ops; run is vacuous".into());
    }
    assert_linearizable(&entries, seed, "leader-isolation");
    Ok(())
}

#[tokio::test]
async fn leader_isolation_elects_new_leader() {
    for attempt in 1..=3 {
        match run_leader_isolation(42).await {
            Ok(()) => return,
            Err(e) => eprintln!(
                "[lin_partition::leader-isolation] attempt {attempt}/3 transient: {e}; retrying"
            ),
        }
    }
    panic!("leader-isolation: failed after 3 transient attempts");
}

async fn run_quorum_loss(seed: u64) -> Result<(), String> {
    let cluster = Arc::new(LinCluster::start_3().await);
    let history = Arc::new(History::default());
    let stop = Arc::new(AtomicBool::new(false));
    let last_seen = Arc::new(AtomicU64::new(0));
    let handles = spawn_workers(&cluster, &history, &stop, &last_seen, seed);

    tokio::time::sleep(Duration::from_millis(800)).await;

    cluster.partition_quorum_loss().await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let lo = History::ok_count(&history.snapshot());
    tokio::time::sleep(PARTITION_HOLD).await;
    let hi = History::ok_count(&history.snapshot());
    // SAFETY (live): no op may commit Ok during total quorum loss — an increase
    // is a false ack / split-brain. Panic NOW, never retried.
    assert_eq!(
        lo, hi,
        "ops committed Ok during total quorum loss ({lo} -> {hi}) — split-brain / false ack"
    );

    cluster.heal().await;
    cluster.wait_for_stable_leader(Duration::from_secs(15)).await;
    let recovered_from = History::ok_count(&history.snapshot());
    tokio::time::sleep(Duration::from_secs(2)).await;
    let recovered_to = History::ok_count(&history.snapshot());
    // TRANSIENT: the cluster must resume committing after heal. Defer so
    // teardown still runs.
    let resumed = recovered_to > recovered_from;

    let entries = teardown(cluster, &stop, handles, history).await;

    if !resumed {
        return Err(format!(
            "cluster did not resume after heal ({recovered_from} -> {recovered_to})"
        ));
    }
    if History::ok_count(&entries) < 30 {
        return Err("too few Ok ops; run is vacuous".into());
    }
    assert_linearizable(&entries, seed, "quorum-loss");
    Ok(())
}

#[tokio::test]
async fn total_quorum_loss_fails_clean_then_recovers() {
    for attempt in 1..=3 {
        match run_quorum_loss(88_888).await {
            Ok(()) => return,
            Err(e) => eprintln!(
                "[lin_partition::quorum-loss] attempt {attempt}/3 transient: {e}; retrying"
            ),
        }
    }
    panic!("quorum-loss: failed after 3 transient attempts");
}
