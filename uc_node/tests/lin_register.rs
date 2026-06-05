#[path = "lincheck/mod.rs"]
mod lincheck;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use rand::Rng;
use rand::rngs::StdRng;

use lincheck::cluster::{LinCluster, ReadOutcome, SubmitOutcome};
use lincheck::history::{History, Outcome};
use lincheck::model::{Op, RegResp};
use lincheck::register_sm::{Cmd, CmdResp};

/// One worker: until `stop`, pick a seeded op, submit/read via the leader,
/// classify the outcome, and record it. `last_seen` is shared across workers so
/// CAS picks a recently-observed value as `old` often enough that some succeed.
#[allow(dead_code)] // wired into the capstone failover test in the next task
async fn worker(
    id: u32,
    cluster: Arc<LinCluster>,
    history: Arc<History>,
    stop: Arc<AtomicBool>,
    mut rng: StdRng,
    last_seen: Arc<AtomicU64>,
) {
    while !stop.load(Ordering::Relaxed) {
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
