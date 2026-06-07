//! Verifies the startup cross-check between the user's `StateMachine::last_applied()`
//! and the framework's durable `last_applied.state`. A mismatch must be surfaced
//! as `ClusterError::DriftDetected`, never silently reconciled.

use std::io::{Read, Write};

use openraft::LogId;
use openraft::vote::RaftLeaderId as _;
type LeaderId = openraft::impls::leader_id_adv::LeaderId<u64, u64>;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use uc_node::ClusterError;
use uc_node::raft::log_storage::JournalLogStorage;
use uc_node::raft::state_machine::AdaptedStateMachine;
use uc_service::{SnapshotError, StateMachine};

/// Minimal `StateMachine` with a controllable `last_applied()` return value.
#[derive(Default)]
struct ControllableSM {
    /// What `last_applied()` should return.
    forced_last_applied: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct NoCmd;

impl StateMachine for ControllableSM {
    type Command = NoCmd;
    type Response = NoCmd;
    type Query = NoCmd;
    type QueryResponse = u64;
    type SnapshotHandle = Vec<u8>;

    fn apply(&mut self, _: u64, _: NoCmd) -> NoCmd {
        NoCmd
    }
    fn query(&self, _: NoCmd) -> u64 {
        0
    }
    fn last_applied(&self) -> Option<u64> {
        self.forced_last_applied
    }
    fn freeze(&self) -> Result<(Vec<u8>, u64), SnapshotError> {
        Ok((Vec::new(), 0))
    }
    fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn Write) -> Result<(), SnapshotError> {
        dst.write_all(&handle)?;
        Ok(())
    }
    fn install_snapshot(&mut self, _: &mut dyn Read) -> Result<u64, SnapshotError> {
        Ok(0)
    }
}

#[tokio::test]
async fn drift_detected_when_user_and_framework_disagree() {
    let dir = TempDir::new().unwrap();
    let storage = JournalLogStorage::open(dir.path()).expect("open");

    // Manually populate framework's durable last_applied with index = 100.
    let lid = LogId::new(LeaderId::new(1, 0), 100);
    storage
        ._testonly_last_applied()
        .store(&lid)
        .expect("store")
        .wait()
        .expect("wait");

    let handles = storage.handles(dir.path().to_owned());
    let sm = ControllableSM {
        forced_last_applied: Some(50),
    };

    // Constructing the adapter must drift-detect (50 != 100).
    let result = AdaptedStateMachine::new(sm, handles);
    match result {
        Err(ClusterError::DriftDetected { user, framework }) => {
            assert_eq!(user, Some(50));
            assert_eq!(framework, Some(100));
        }
        Ok(_) => panic!("expected DriftDetected, got Ok"),
        Err(e) => panic!("expected DriftDetected, got {e}"),
    }
}

#[tokio::test]
async fn drift_detected_when_user_some_framework_none() {
    // User says caught up, but framework has no record. This means the user
    // has applied entries the framework doesn't know about — must surface
    // as drift, not be silently accepted.
    let dir = TempDir::new().unwrap();
    let storage = JournalLogStorage::open(dir.path()).expect("open");

    // Sanity: framework's last_applied is None (we did NOT call store).
    assert!(
        storage
            ._testonly_last_applied()
            .load()
            .expect("load")
            .is_none()
    );

    let handles = storage.handles(dir.path().to_owned());
    let sm = ControllableSM {
        forced_last_applied: Some(50),
    };

    let result = AdaptedStateMachine::new(sm, handles);
    match result {
        Err(ClusterError::DriftDetected { user, framework }) => {
            assert_eq!(user, Some(50));
            assert_eq!(framework, None);
        }
        Ok(_) => panic!("expected DriftDetected, got Ok"),
        Err(e) => panic!("expected DriftDetected, got {e}"),
    }
}
