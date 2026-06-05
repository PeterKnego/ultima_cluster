#[path = "lincheck/mod.rs"]
mod lincheck;

use lincheck::cluster::{LinCluster, ReadOutcome, SubmitOutcome};
use lincheck::register_sm::Cmd;

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
