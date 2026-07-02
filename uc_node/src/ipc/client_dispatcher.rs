//! Submit-ring and query-ring dispatchers for the client-facing IPC plane.
//!
//! Both dispatchers share the response Broadcast producer through a
//! `parking_lot::Mutex` — `BroadcastProducer` is single-producer-by-design,
//! and the mutex enforces that across the two tasks. Each broadcast write
//! is one record; no awaits are held under the lock.
//!
//! These dispatchers are wired into the NodeBuilder shmem path (Task 3.4).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex as PlMutex;
use tokio::task::JoinHandle;

use uc_protocol::frames::client::{
    MSG_TYPE_CLIENT_QUERY, MSG_TYPE_CLIENT_QUERY_RESP, MSG_TYPE_NOT_LEADER_RESP, MSG_TYPE_SUBMIT,
    MSG_TYPE_SUBMIT_RESPONSE, decode_flags_client, encode_flags_client,
};
use uc_protocol::ring::RingError;
use uc_protocol::ring::broadcast::BroadcastProducer;
use uc_protocol::ring::mpsc::MpscConsumer;

use uc_service::StateMachine;

use crate::ipc::query_link::ShmemQueryLink;
use crate::raft::{AppCommand, NodeId};
use crate::runtime::node::RaftHandle;

pub(crate) type SharedResponseProducer = Arc<PlMutex<BroadcastProducer>>;

const POLL_BACKOFF: Duration = Duration::from_micros(100);
const BROADCAST_RETRY_BACKOFF: Duration = Duration::from_micros(100);

pub(crate) struct ClientDispatcherHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

/// Spawn the submit-ring dispatcher.
///
/// Consumes `MSG_TYPE_SUBMIT` frames from `submit_consumer`, validates
/// `service_id` via `decode_flags_client`, forwards to `raft.client_write`,
/// and broadcasts the response. On `ForwardToLeader` broadcasts a
/// `MSG_TYPE_NOT_LEADER_RESP` with the leader-id hint.
pub(crate) fn spawn_client_dispatcher<S>(
    submit_consumer: MpscConsumer,
    response_producer: SharedResponseProducer,
    raft: RaftHandle<S>,
    node_id: NodeId,
    admission: Option<Arc<tokio::sync::Semaphore>>,
) -> ClientDispatcherHandle
where
    S: StateMachine + Send + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);

    let join = tokio::spawn(async move {
        let mut consumer = submit_consumer;
        let submit_bridge =
            crate::ipc::ring_bridge::NotifyBridge::spawn(
                consumer.wait_handle(),
                "submit",
                crate::ipc::ring_bridge::bridge_spin_budget(),
            );
        let mut payload_buf: Vec<u8> = Vec::with_capacity(4096);
        while !stop_for_task.load(Ordering::Relaxed) {
            match consumer.try_read(&mut payload_buf) {
                Ok(Some(rec)) if rec.msg_type == MSG_TYPE_SUBMIT => {
                    let extra = rec.header_extra;
                    let (probe_cid, probe_seq) =
                        uc_protocol::frames::client::decode_extra_client(extra);
                    uc_protocol::probes::stamp_client(
                        probe_cid,
                        probe_seq,
                        uc_protocol::probes::Checkpoint::NodeDequeue,
                    );
                    let flags = rec.flags;
                    let payload = std::mem::take(&mut payload_buf);

                    // Validate service_id (v1: must be 0).
                    if let Err(e) = decode_flags_client(flags) {
                        broadcast_service_id_error(
                            &response_producer,
                            MSG_TYPE_SUBMIT_RESPONSE,
                            extra,
                            e.to_string(),
                        )
                        .await;
                        continue;
                    }

                    let leader = raft.current_leader().await;
                    if leader != Some(node_id) {
                        broadcast_not_leader(&response_producer, extra, leader).await;
                        continue;
                    }

                    let app_command = AppCommand::from(Bytes::from(payload));
                    // Dispatch the commit CONCURRENTLY: spawn so the read loop
                    // keeps pulling submits instead of awaiting each full Raft
                    // round-trip inline. Serial await capped throughput at
                    // ~1/commit-latency; spawning lets many ClientWrites be in
                    // flight so openraft batches them.
                    //
                    // Admission control: take a permit BEFORE spawning, so the
                    // number of in-pipeline writes is bounded cluster-side (not
                    // by whatever concurrency clients offer). When saturated,
                    // this loop parks here and stops draining submit.ring; the
                    // ring fills and clients see ring backpressure — excess
                    // load waits at the door instead of bloating cluster queues
                    // (deep queues measured as the overload collapse driver;
                    // see RaftTuning::max_inflight_writes).
                    let permit = match &admission {
                        Some(sem) => match Arc::clone(sem).acquire_owned().await {
                            Ok(p) => Some(p),
                            Err(_) => break, // semaphore closed = shutting down
                        },
                        None => None,
                    };
                    let raft_c = raft.clone();
                    let resp_c = Arc::clone(&response_producer);
                    tokio::spawn(async move {
                        let _permit = permit;
                        match raft_c.client_write(app_command).await {
                            Ok(resp) => {
                                uc_protocol::probes::bridge(
                                    probe_cid,
                                    probe_seq,
                                    resp.log_id.index,
                                );
                                broadcast_record(
                                    &resp_c,
                                    MSG_TYPE_SUBMIT_RESPONSE,
                                    encode_flags_client(0, None),
                                    extra,
                                    resp.data.as_ref(),
                                )
                                .await;
                                uc_protocol::probes::stamp_client(
                                    probe_cid,
                                    probe_seq,
                                    uc_protocol::probes::Checkpoint::Broadcast,
                                );
                            }
                            Err(e) => {
                                use openraft::error::{ClientWriteError, RaftError};
                                if let RaftError::APIError(ClientWriteError::ForwardToLeader(f)) =
                                    &e
                                {
                                    broadcast_not_leader(&resp_c, extra, f.leader_id).await;
                                } else {
                                    tracing::warn!(node_id, error = ?e, "client_write failed; dropping");
                                }
                            }
                        }
                    });
                }
                Ok(Some(rec)) => {
                    tracing::warn!(msg_type = rec.msg_type, "unexpected frame on submit.ring");
                }
                Ok(None) => submit_bridge.notified().await,
                Err(e) => {
                    tracing::error!(error = ?e, "submit.ring read error");
                    tokio::time::sleep(POLL_BACKOFF).await;
                }
            }
        }
    });

    ClientDispatcherHandle { join, stop }
}

/// Spawn the query-ring dispatcher.
///
/// Consumes `MSG_TYPE_CLIENT_QUERY` frames from `query_consumer`, validates
/// `service_id`, checks leader status for linearizable queries, then
/// forwards to `query_link.submit`. Broadcasts `MSG_TYPE_CLIENT_QUERY_RESP`
/// on success or `MSG_TYPE_NOT_LEADER_RESP` when not leader.
pub(crate) fn spawn_client_query_dispatcher<S>(
    query_consumer: MpscConsumer,
    response_producer: SharedResponseProducer,
    raft: RaftHandle<S>,
    query_link: Arc<ShmemQueryLink>,
    node_id: NodeId,
) -> ClientDispatcherHandle
where
    S: StateMachine + Send + 'static,
{
    use uc_protocol::frames::query::QueryKind;

    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);

    let join = tokio::spawn(async move {
        let mut consumer = query_consumer;
        let mut payload_buf: Vec<u8> = Vec::with_capacity(4096);
        while !stop_for_task.load(Ordering::Relaxed) {
            match consumer.try_read(&mut payload_buf) {
                Ok(Some(rec)) if rec.msg_type == MSG_TYPE_CLIENT_QUERY => {
                    let extra = rec.header_extra;
                    let flags = rec.flags;
                    let payload = std::mem::take(&mut payload_buf);

                    let (_service_id, kind) = match decode_flags_client(flags) {
                        Ok(x) => x,
                        Err(e) => {
                            broadcast_service_id_error(
                                &response_producer,
                                MSG_TYPE_CLIENT_QUERY_RESP,
                                extra,
                                e.to_string(),
                            )
                            .await;
                            continue;
                        }
                    };

                    // Linearizable reads go through the ReadIndex barrier: confirm
                    // leadership + get the read index (openraft applied up to it).
                    // The query link then waits for the service to catch up to that
                    // index before serving. Snapshot reads skip the barrier
                    // (read_index 0 = serve from the current incarnation as-is).
                    let read_index = if let QueryKind::Linearizable = kind {
                        match raft.ensure_linearizable().await {
                            Ok(idx) => idx,
                            Err(crate::ClusterError::NotLeader { leader_id }) => {
                                broadcast_not_leader(&response_producer, extra, leader_id).await;
                                continue;
                            }
                            Err(e) => {
                                tracing::warn!(node_id, error = ?e, "ensure_linearizable failed");
                                broadcast_not_leader(&response_producer, extra, None).await;
                                continue;
                            }
                        }
                    } else {
                        0
                    };

                    match query_link.submit(&payload, kind, read_index).await {
                        Ok(resp_bytes) => {
                            broadcast_record(
                                &response_producer,
                                MSG_TYPE_CLIENT_QUERY_RESP,
                                encode_flags_client(0, None),
                                extra,
                                resp_bytes.as_ref(),
                            )
                            .await;
                        }
                        Err(e) => {
                            tracing::warn!(node_id, error = ?e, "query_link submit failed");
                        }
                    }
                }
                Ok(Some(rec)) => {
                    tracing::warn!(msg_type = rec.msg_type, "unexpected frame on query.ring");
                }
                Ok(None) => tokio::time::sleep(POLL_BACKOFF).await,
                Err(e) => {
                    tracing::error!(error = ?e, "query.ring read error");
                    tokio::time::sleep(POLL_BACKOFF).await;
                }
            }
        }
    });

    ClientDispatcherHandle { join, stop }
}

// ---------------------------------------------------------------------------
// Broadcast helpers
// ---------------------------------------------------------------------------

async fn broadcast_not_leader(
    response_producer: &SharedResponseProducer,
    extra: [u8; 8],
    hint: Option<NodeId>,
) {
    let hint_bytes = match bincode::serde::encode_to_vec(hint, bincode::config::standard()) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = ?e, "encode NotLeader hint");
            return;
        }
    };
    broadcast_record(
        response_producer,
        MSG_TYPE_NOT_LEADER_RESP,
        encode_flags_client(0, None),
        extra,
        &hint_bytes,
    )
    .await;
}

/// Encode and broadcast a service-id validation error.
///
/// Payload is a bincode-encoded `String` describing the failure.
/// The client SDK surfaces this as `ClientError::Submission(..)`.
async fn broadcast_service_id_error(
    response_producer: &SharedResponseProducer,
    msg_type: u16,
    extra: [u8; 8],
    err_msg: String,
) {
    let payload = match bincode::serde::encode_to_vec(&err_msg, bincode::config::standard()) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = ?e, "encode service-id error payload");
            return;
        }
    };
    broadcast_record(
        response_producer,
        msg_type,
        encode_flags_client(0, None),
        extra,
        &payload,
    )
    .await;
}

/// Write one record to the broadcast ring.
///
/// `BroadcastProducer::write` overwrites old records rather than returning
/// `RingError::Full`, so the retry loop here only fires on other errors (e.g.
/// `TooLarge`). For `TooLarge` the record is silently dropped.
async fn broadcast_record(
    response_producer: &SharedResponseProducer,
    msg_type: u16,
    flags: u16,
    extra: [u8; 8],
    payload: &[u8],
) {
    loop {
        let result = {
            let mut g = response_producer.lock();
            g.write(msg_type, flags, extra, payload)
        };
        match result {
            Ok(()) => return,
            Err(RingError::Full) => {
                // Broadcast never returns Full, but handle defensively.
                tokio::time::sleep(BROADCAST_RETRY_BACKOFF).await;
            }
            Err(e) => {
                tracing::error!(?e, "response broadcast write failed; dropping");
                return;
            }
        }
    }
}
