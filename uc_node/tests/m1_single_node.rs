//! M1 capstone: bootstrap_single_node → submit → query → restart → state preserved.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use uc_node::{
    BootstrapConfig, ClientRingConfig, NodeBuilder, NodeConfig, RaftTuning, ServiceRingConfig,
    TlsConfig,
};
use uc_service::{SnapshotError, StateMachine};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum CounterCmd {
    Increment(u64),
    Reset,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct CounterResponse {
    value: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct CounterQuery;

#[derive(Default)]
struct Counter {
    value: u64,
    last_applied: Option<u64>,
}

impl StateMachine for Counter {
    type Command = CounterCmd;
    type Response = CounterResponse;
    type Query = CounterQuery;
    type QueryResponse = u64;

    fn apply(&mut self, log_index: u64, cmd: Self::Command) -> Self::Response {
        match cmd {
            CounterCmd::Increment(n) => self.value += n,
            CounterCmd::Reset => self.value = 0,
        }
        self.last_applied = Some(log_index);
        CounterResponse { value: self.value }
    }

    fn query(&self, _: Self::Query) -> Self::QueryResponse {
        self.value
    }

    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }

    fn build_snapshot(&self, dst: &mut dyn Write) -> Result<u64, SnapshotError> {
        let bytes = bincode::serde::encode_to_vec(
            (self.value, self.last_applied),
            bincode::config::standard(),
        )
        .map_err(|e| SnapshotError::Codec(e.to_string()))?;
        dst.write_all(&bytes)?;
        Ok(self.last_applied.unwrap_or(0))
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

fn cfg(data_dir: PathBuf, bootstrap: BootstrapConfig) -> NodeConfig {
    NodeConfig {
        node_id: 1,
        data_dir,
        raft_listen_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        app_id: "counter-test".into(),
        bootstrap,
        raft: RaftTuning::default(),
        tls: TlsConfig::default(),
        ipc_mode: uc_node::IpcMode::default(),
        client_rings: ClientRingConfig::default(),
        service_rings: ServiceRingConfig::default(),
    }
}

async fn wait_for_leader<S: StateMachine>(node: &uc_node::NodeHandle<S>, expected: u64) {
    for _ in 0..50 {
        if node.current_leader().await == Some(expected) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("never became leader within 5 seconds");
}

#[tokio::test]
async fn submit_query_works() {
    let dir = TempDir::new().unwrap();
    let node = NodeBuilder::new(
        cfg(dir.path().to_owned(), BootstrapConfig::SingleNode),
        Counter::default(),
    )
    .start()
    .await
    .expect("start");

    wait_for_leader(&node, 1).await;

    let r1 = node
        .submit(CounterCmd::Increment(5))
        .await
        .expect("submit 1");
    assert_eq!(r1.value, 5);

    let r2 = node
        .submit(CounterCmd::Increment(3))
        .await
        .expect("submit 2");
    assert_eq!(r2.value, 8);

    let v = node.query_snapshot(|c: &Counter| c.value).await;
    assert_eq!(v, 8);

    node.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn state_survives_restart() {
    let dir = TempDir::new().unwrap();

    {
        let node = NodeBuilder::new(
            cfg(dir.path().to_owned(), BootstrapConfig::SingleNode),
            Counter::default(),
        )
        .start()
        .await
        .expect("start");
        wait_for_leader(&node, 1).await;
        node.submit(CounterCmd::Increment(42))
            .await
            .expect("submit 42");
        node.submit(CounterCmd::Increment(1))
            .await
            .expect("submit 1");
        node.shutdown().await.expect("shutdown");
    }

    // Restart with Resume — same data_dir.
    let node = NodeBuilder::new(
        cfg(dir.path().to_owned(), BootstrapConfig::Resume),
        Counter::default(),
    )
    .start()
    .await
    .expect("restart");
    wait_for_leader(&node, 1).await;

    let v = node.query_snapshot(|c: &Counter| c.value).await;
    assert_eq!(v, 43, "state must survive restart");

    node.shutdown().await.expect("shutdown");
}
