//! Publishes raft metrics (term, leader, role, last_applied, last_committed)
//! into the cnc `NodeStatus` block whenever the watch receiver changes.
//!
//! The heartbeat ticker in `liveness.rs` keeps `heartbeat_seq` / `_at_ns`
//! current; this task fills in the structural fields the clients (and
//! future operators) need to introspect the node's raft state.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use openraft::ServerState;
use openraft::rt::WatchReceiver as _;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use uc_protocol::cnc::{NodeStatus, node_role};
use uc_service::StateMachine;

use crate::runtime::node::RaftHandle;

pub struct MetricsPublisherHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
    /// M5: watch channel emitting whether this node currently sees itself as
    /// raft Leader. Subscribed by `output_dispatcher` / `output_replay`.
    pub leader_rx: watch::Receiver<bool>,
    /// Test-only: retained sender so tests can force a `false → true`
    /// transition to trigger output_replay without an actual leader flip.
    pub leader_tx: Arc<watch::Sender<bool>>,
}

/// Spawn a task that watches `raft.metrics()` and copies the latest
/// snapshot into the cnc `NodeStatus` block on every change.
///
/// # Safety
///
/// `status_ptr` must remain valid for the lifetime of the spawned task
/// (i.e., caller keeps the cnc mmap alive until the handle is joined).
pub(crate) unsafe fn spawn_metrics_publisher<S: StateMachine>(
    status_ptr: *const NodeStatus,
    raft: RaftHandle<S>,
) -> MetricsPublisherHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);

    // SAFETY: see function-level # Safety.
    let status: &'static NodeStatus = unsafe { &*status_ptr };

    // M5: watch channel that mirrors the node's leader state. Seeded false
    // until the first publish_once fires. `watch::send` is a no-op when the
    // value is identical, so steady-state overhead is minimal.
    let (leader_tx_raw, leader_rx) = watch::channel(false);
    let leader_tx = Arc::new(leader_tx_raw);
    let leader_tx_for_task = Arc::clone(&leader_tx);

    let join = tokio::spawn(async move {
        let mut rx = raft.metrics();
        // Publish once on entry (avoid waiting for the first .changed() if
        // metrics already settled).
        publish_once(status, &rx, &leader_tx_for_task);

        while !stop_for_task.load(Ordering::Relaxed) {
            // Wait for the next change. If the channel closes (raft
            // dropped) we exit the loop.
            if rx.changed().await.is_err() {
                break;
            }
            publish_once(status, &rx, &leader_tx_for_task);
        }
    });
    MetricsPublisherHandle {
        join,
        stop,
        leader_rx,
        leader_tx,
    }
}

fn publish_once(
    status: &NodeStatus,
    rx: &openraft::type_config::alias::WatchReceiverOf<
        crate::raft::TypeConfig,
        openraft::RaftMetrics<crate::raft::TypeConfig>,
    >,
    leader_tx: &Arc<watch::Sender<bool>>,
) {
    let m = rx.borrow_watched();

    // current_term: u64 in our TypeConfig.
    status.current_term.store(m.current_term, Ordering::Relaxed);

    // role: map ServerState to our node_role constants.
    let role = match m.state {
        ServerState::Learner => node_role::FOLLOWER, // closest analog
        ServerState::Follower => node_role::FOLLOWER,
        ServerState::Candidate => node_role::CANDIDATE,
        ServerState::Leader => node_role::LEADER,
        ServerState::Shutdown => node_role::SHUTTING,
    };
    status.role.store(role, Ordering::Relaxed);

    // current_leader: Option<NodeId> where NodeId = u64.
    let leader: u64 = m.current_leader.unwrap_or(u64::MAX);
    status.leader_node_id.store(leader, Ordering::Relaxed);

    // last_applied: Option<LogId<...>>; use .index() for the u64 log index.
    let last_applied = m.last_applied.as_ref().map(|l| l.index()).unwrap_or(0);
    status.last_applied.store(last_applied, Ordering::Relaxed);

    // committed: Option<LogId<...>>; the highest locally-committed index.
    let committed = m.committed.as_ref().map(|l| l.index()).unwrap_or(0);
    status.last_committed.store(committed, Ordering::Relaxed);

    // M5: notify output_dispatcher of leader state. `send` is a no-op when
    // the value is unchanged (watch channel semantics), so this is cheap in
    // steady state.
    let _ = leader_tx.send(role == node_role::LEADER);
}
