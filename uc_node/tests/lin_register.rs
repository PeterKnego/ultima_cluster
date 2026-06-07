#[path = "lincheck/mod.rs"]
mod lincheck;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use lincheck::checker::{Verdict, check_register};
use lincheck::cluster::{LinCluster, ReadOutcome, SubmitOutcome};
use lincheck::history::{History, Outcome};
use lincheck::model::{Op, RegResp};
use lincheck::register_sm::{Cmd, CmdResp};

/// One worker: until `stop`, pick a seeded op, submit/read via the leader,
/// classify the outcome, and record it. `last_seen` is shared across workers so
/// CAS picks a recently-observed value as `old` often enough that some succeed.
async fn worker(
    id: u32,
    cluster: Arc<LinCluster>,
    history: Arc<History>,
    stop: Arc<AtomicBool>,
    mut rng: StdRng,
    last_seen: Arc<AtomicU64>,
    throttle: std::time::Duration,
) {
    while !stop.load(Ordering::Relaxed) {
        if !throttle.is_zero() {
            tokio::time::sleep(throttle).await;
        }
        let choice = rng.random_range(0..3u8);
        match choice {
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
                // CAS using a recently-seen value as `old` (so some succeed),
                // sometimes a random old (so some fail).
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
                    SubmitOutcome::Ok(_) => panic!("cas returned non-cas response"),
                    SubmitOutcome::Indeterminate => Outcome::Indeterminate,
                    SubmitOutcome::Fatal(e) => panic!("fatal cas: {e}"),
                };
                history.record(id, Op::Cas { old, new }, inv, outcome);
            }
        }
    }
}

#[tokio::test]
async fn smoke_3node_submit_read() {
    let cluster = LinCluster::start_3().await;
    // A few sequential writes + reads through the leader, no faults.
    for v in 1..=5u64 {
        match cluster.submit_cmd(&Cmd::Write(v)).await {
            SubmitOutcome::Ok(_) => {}
            o => panic!("write {v} not Ok: {o:?}"),
        }
        match cluster.read().await {
            ReadOutcome::Ok(Some(got)) => assert_eq!(got, v, "read after write {v}"),
            o => panic!("read after write {v}: {o:?}"),
        }
    }
    cluster.shutdown().await;
}

#[tokio::test]
async fn fault_roundtrip_keeps_serving() {
    let cluster = LinCluster::start_3().await; // methods are &self; shutdown consumes self
    // Establish state.
    assert!(matches!(
        cluster.submit_cmd(&Cmd::Write(1)).await,
        SubmitOutcome::Ok(_)
    ));
    // Kill+restart the leader; cluster must keep serving.
    cluster.kill_and_restart_leader().await;
    match cluster.submit_cmd(&Cmd::Write(2)).await {
        SubmitOutcome::Ok(_) | SubmitOutcome::Indeterminate => {}
        o => panic!("post leader-restart submit: {o:?}"),
    }
    // Crash+restart the leader's service; cluster must keep serving.
    cluster.crash_and_restart_leader_service().await;
    match cluster.submit_cmd(&Cmd::Write(3)).await {
        SubmitOutcome::Ok(_) | SubmitOutcome::Indeterminate => {}
        o => panic!("post service-crash submit: {o:?}"),
    }
    // Reads still work and reflect a committed value.
    match cluster.read().await {
        ReadOutcome::Ok(Some(_)) | ReadOutcome::Indeterminate => {}
        o => panic!("post-fault read: {o:?}"),
    }
    cluster.shutdown().await;
}

/// Dump the full history to a file so a `Violation` is reproducible offline
/// (the checker is deterministic on a captured history even though the cluster
/// interleaving is not).
fn dump_history(entries: &[lincheck::history::Entry], seed: u64) {
    let path = format!("/tmp/lincheck_history_{seed}.txt");
    let mut s = String::new();
    for e in entries {
        s.push_str(&format!("{e:?}\n"));
    }
    let _ = std::fs::write(&path, s);
    eprintln!("history ({} entries) dumped to {path}", entries.len());
}

/// The capstone: a few seeded, throttled workers drive a concurrent CAS-register
/// workload while a seeded scheduler injects one quorum-preserving fault at a
/// time — leader node-kill+restart OR leader service-crash+restart — waiting for
/// recovery between faults. The recorded history must be linearizable. RegisterSm
/// is plain in-memory (register_sm.rs persists nothing): after a service crash it
/// restarts EMPTY and the node reconstructs it from the replicated log (mid-life
/// reattach replay, or snapshot-install + tail replay below the purge boundary).
/// That reconstruction is exactly what the capstone proves.
///
/// Multi-thread runtime (the default): the 3-node shmem boot deadlocks under the
/// current_thread runtime — see `smoke_3node_submit_read`.
#[tokio::test]
async fn linearizable_under_failover() {
    const DEFAULT_SEED: u64 = 0x1107;
    let seed: u64 = std::env::var("LIN_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_SEED);
    // Bounds tuned so the WGL checker reliably completes (no Inconclusive). The
    // search cost is dominated by the concurrency window and by indeterminate
    // mutations (each in-flight write/cas killed during a failover becomes an
    // optional op → a drop-branch), both of which scale with `n_workers`, so a
    // modest worker count is the key lever. ~800 ops across several failovers is
    // ample to surface a stale-read / lost-update / double-applied-cas bug.
    let target_ops: usize = 800;
    let n_workers: u32 = 3;
    // Per-op throttle: a single leader node-kill+restart takes ~5 s of recovery,
    // during which the workers freely drive the survivors. Unthrottled, 3-4
    // workers produce ~700 ops in one recovery window — so the op target is hit
    // after a single failover. Throttling each worker to ~1 op / 60 ms keeps
    // per-failover op counts modest so the run spans several failovers with a
    // bounded, checker-friendly history.
    let throttle = std::time::Duration::from_millis(60);
    // Spacing between faults (the fault's own ~5 s recovery dominates wall-clock).
    let fault_period = std::time::Duration::from_secs(1);

    let cluster = Arc::new(LinCluster::start_3().await);
    let history = Arc::new(History::default());
    let stop = Arc::new(AtomicBool::new(false));
    let last_seen = Arc::new(AtomicU64::new(0));

    // Workers.
    let mut handles = Vec::new();
    for w in 0..n_workers {
        let rng = StdRng::seed_from_u64(seed ^ (w as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        handles.push(tokio::spawn(worker(
            w,
            cluster.clone(),
            history.clone(),
            stop.clone(),
            rng,
            last_seen.clone(),
            throttle,
        )));
    }

    // Fault scheduler: inject one quorum-preserving fault at a time (the methods
    // are &self and lock internally), waiting for recovery between faults, until
    // enough ops have completed Ok. Workers keep running against the shared
    // Arc<LinCluster>. The seeded RNG picks between the two fault kinds:
    //   - leader node-kill+restart (full process down, rejoin via persisted
    //     data_dir), and
    //   - leader service-crash+restart (node stays up; the service watcher
    //     transfers leadership; a fresh, EMPTY service is reconstructed by the
    //     node from the replicated log).
    // Both are linearizable-safe because RegisterSm is plain in-memory
    // (register_sm.rs persists nothing): a restarted service comes back empty and
    // the node reconstructs it from the replicated log (mid-life reattach replay,
    // or snapshot-install + tail replay below the purge boundary).
    let mut fault_rng = StdRng::seed_from_u64(seed ^ 0xFA17);
    let mut faults = 0u32;
    while History::ok_count(&history.snapshot()) < target_ops {
        tokio::time::sleep(fault_period).await;
        if fault_rng.random_bool(0.5) {
            cluster.kill_and_restart_leader().await;
        } else {
            cluster.crash_and_restart_leader_service().await;
        }
        faults += 1;
    }

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }

    let cluster = Arc::try_unwrap(cluster)
        .ok()
        .expect("sole cluster owner at shutdown");
    cluster.shutdown().await;

    let entries = Arc::try_unwrap(history)
        .ok()
        .expect("sole history owner")
        .into_entries();

    // Liveness gate: most ops must have completed Ok, else the run is meaningless
    // (distinguishes a harness/cluster-progress failure from a linearizability bug).
    let ok = History::ok_count(&entries);
    eprintln!(
        "[lincheck] seed={seed} faults={faults} ops={} ok={ok} — checking linearizability",
        entries.len()
    );
    assert!(
        ok * 100 >= entries.len() * 80,
        "liveness: only {ok}/{} ops completed Ok (<80%) — cluster failed to progress",
        entries.len()
    );

    match check_register(&entries) {
        Verdict::Linearizable => {}
        Verdict::Violation => {
            dump_history(&entries, seed);
            panic!("LINEARIZABILITY VIOLATION (seed={seed}); history dumped");
        }
        Verdict::Inconclusive => {
            panic!("checker Inconclusive (seed={seed}); lower target_ops/n_workers");
        }
    }
}
