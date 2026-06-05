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
