//! Hard-crash linearizability test: SIGKILL the SERVICE process mid-apply while
//! sustained `uc_client` load drives a concurrent CAS-register workload, restart
//! it, and assert the recorded op history is WGL-linearizable.
//!
//! Unlike the in-process capstone (`uc_node/tests/lin_register.rs`), which uses
//! graceful service shutdowns, this drops a `Reap` guard — a true `kill -9` +
//! reap + respawn — so it proves node-driven reconstruction + the
//! ReadIndex/seqlock read barrier survive a real hard crash. `RegisterSm` is
//! plain in-memory (persists nothing): a restarted service comes back EMPTY and
//! the node must reconstruct it from the replicated log (mid-life reattach replay
//! or snapshot-install + tail replay). That reconstruction is what we check.
//!
//! Gated behind `hard-crash-tests` because it spawns real OS processes.
#![cfg(feature = "hard-crash-tests")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::Mutex;

use uc_client::Client;
use uc_lincheck::checker::{Verdict, check_register};
use uc_lincheck::history::{History, Outcome};
use uc_lincheck::model::{Op, RegResp};
use uc_lincheck::register::{Cmd, CmdResp};

mod common;
use common::*;

/// One worker: until `stop`, pick a seeded op, fire it via the client, classify
/// the outcome, record it. A worker must NEVER panic on a client error — the
/// service WILL be dead mid-op during a fault — so every error (transient or not)
/// is classified `Indeterminate`, which the checker tolerates. We deliberately
/// swallow *all* errors rather than gating on `is_transient`: a hard crash may
/// surface error variants we don't want to enumerate, and a systemic failure
/// (nothing ever commits) is caught downstream by the `ok >= 50` liveness gate,
/// not by panicking a worker.
async fn worker(
    id: u32,
    client: Arc<Client>,
    history: Arc<History>,
    last_seen: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    mut rng: StdRng,
) {
    let throttle = Duration::from_millis(7);
    while !stop.load(Ordering::Relaxed) {
        tokio::time::sleep(throttle).await;
        match rng.random_range(0..3u8) {
            0 => {
                let v = rng.random_range(1..1000u64);
                let inv = history.invoke();
                let outcome = match client.submit::<Cmd, CmdResp>(&Cmd::Write(v)).await {
                    Ok(_) => Outcome::Ok(RegResp::Ack),
                    // A kill -9 mid-submit surfaces as a transient/closed error —
                    // never panic; the write may or may not have committed.
                    Err(_) => Outcome::Indeterminate,
                };
                history.record(id, Op::Write(v), inv, outcome);
            }
            1 => {
                let inv = history.invoke();
                let outcome = match client.query_linearizable::<(), Option<u64>>(&()).await {
                    Ok(v) => {
                        if let Some(x) = v {
                            last_seen.store(x, Ordering::Relaxed);
                        }
                        Outcome::Ok(RegResp::Value(v))
                    }
                    Err(_) => Outcome::Indeterminate,
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
                let outcome = match client.submit::<Cmd, CmdResp>(&Cmd::Cas { old, new }).await {
                    Ok(CmdResp::CasResult(b)) => {
                        if b {
                            last_seen.store(new, Ordering::Relaxed);
                        }
                        Outcome::Ok(RegResp::CasOk(b))
                    }
                    // A wrong-typed response to a CAS is a genuine framework/SM
                    // contract break, not crash fallout — surface the value.
                    Ok(other) => panic!("cas returned non-cas response: {other:?}"),
                    Err(_) => Outcome::Indeterminate,
                };
                history.record(id, Op::Cas { old, new }, inv, outcome);
            }
        }
    }
}

#[tokio::test]
async fn linearizable_under_hard_crash() {
    let seed: u64 = std::env::var("LIN_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let tmp = tempfile::tempdir().unwrap();
    let inst = tmp.path().join("inst");
    std::fs::create_dir_all(&inst).unwrap();
    let svcdata = tmp.path().join("svcdata");

    // Node held for the whole test; service held behind a Mutex so the fault loop
    // can SIGKILL + respawn it.
    let _node = spawn_node(&inst, &tmp.path().join("nodedata"));
    wait_for_path(&inst.join("cnc.dat"), Duration::from_secs(10)).await;
    let svc = Arc::new(Mutex::new(spawn_service(&inst, &svcdata)));

    let client = Arc::new(connect_with_retry(&inst, Duration::from_secs(10)).await);

    let history = Arc::new(History::default());
    let last_seen = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    // Warm-up: ensure write-readiness (leader's initial blank entry committed)
    // before the workers start. This MUST be recorded as the first history entry,
    // not discarded — the model's initial state is `None` (never written), so a
    // discarded warm-up write would leave a phantom value 1 that later reads
    // observe but the checker can't account for (a false Violation). Recording it
    // gives the history a clean, model-consistent start at value 1.
    let inv = history.invoke();
    submit_until_ok(&client, &Cmd::Write(1), Instant::now() + Duration::from_secs(15)).await;
    history.record(0, Op::Write(1), inv, Outcome::Ok(RegResp::Ack));
    last_seen.store(1, Ordering::Relaxed);

    // Workers.
    const N_WORKERS: u32 = 3;
    let mut handles = Vec::new();
    for w in 0..N_WORKERS {
        let rng = StdRng::seed_from_u64(seed ^ (w as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        handles.push(tokio::spawn(worker(
            w,
            client.clone(),
            history.clone(),
            last_seen.clone(),
            stop.clone(),
            rng,
        )));
    }

    // Fault loop: HARD-CRASH (SIGKILL) + restart the service several times. The
    // client stays connected to the NODE; submits during the down/reconstruct
    // window become Indeterminate. After respawn the node reconstructs the fresh
    // (empty) service from the replicated log.
    //
    // 700ms between crashes is comfortably longer than single-node reconstruction
    // (replay/snapshot-install of a tiny register) so the cluster fully recovers
    // and lands committed ops between faults; 5 iterations gives several distinct
    // crash/recover cycles within the test's runtime budget.
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(700)).await;
        let mut g = svc.lock().await;
        // Drop the old Reap (= SIGKILL + reap), then respawn.
        *g = spawn_service(&inst, &svcdata);
        drop(g);
    }

    // Let post-recovery ops land so the tail of the history reflects a healthy
    // reconstructed service.
    tokio::time::sleep(Duration::from_secs(1)).await;

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }

    let entries = Arc::try_unwrap(history)
        .ok()
        .expect("sole history owner")
        .into_entries();

    let ok = History::ok_count(&entries);
    eprintln!(
        "[hard_crash] seed={seed} ops={} ok={ok} — checking linearizability",
        entries.len()
    );
    // Liveness gate: enough ops must have completed Ok, else the run is vacuous.
    assert!(
        ok >= 50,
        "liveness: only {ok} ops completed Ok (<50) — cluster failed to progress under hard crash"
    );

    match check_register(&entries) {
        Verdict::Linearizable => {
            eprintln!("[hard_crash] seed={seed}: Linearizable");
        }
        Verdict::Inconclusive => {
            // WGL search hit its visited-state budget — acceptable (like the
            // capstone), not a correctness failure.
            eprintln!("[hard_crash] seed={seed}: Inconclusive (checker budget)");
        }
        Verdict::Violation => {
            // Dump the full history so a Violation is reproducible offline (the
            // checker is deterministic on a captured history even though the
            // crash interleaving is not).
            let path = format!("/tmp/hard_crash_history_{seed}.txt");
            let mut s = String::new();
            for e in &entries {
                s.push_str(&format!("{e:?}\n"));
            }
            let _ = std::fs::write(&path, s);
            eprintln!(
                "[hard_crash] history ({} entries) dumped to {path}",
                entries.len()
            );
            panic!("hard-crash history NOT linearizable (seed {seed})");
        }
    }

    // Workers are joined → their Arc clones are dropped → we are the sole owner,
    // so we can unwrap and consume the client for a graceful shutdown.
    if let Ok(c) = Arc::try_unwrap(client) {
        c.shutdown().await.ok();
    }
    // _node / svc Reaps dropped here → killed + reaped.
}
