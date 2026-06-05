//! Client-side ring opens + broadcast reader task.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use uc_protocol::frames::client::{
    MSG_TYPE_CLIENT_QUERY_RESP, MSG_TYPE_NOT_LEADER_RESP, MSG_TYPE_SUBMIT_RESPONSE,
    decode_extra_client,
};
use uc_protocol::ring::RingError;
use uc_protocol::ring::broadcast::{BroadcastConsumer, BroadcastRing};
use uc_protocol::ring::mpsc::{MpscProducer, MpscRing};

use crate::ClientError;

pub type ResponseSender = oneshot::Sender<RawResponse>;
#[allow(dead_code)]
pub type ResponseReceiver = oneshot::Receiver<RawResponse>;
pub type InFlight = Arc<DashMap<u32, ResponseSender>>;

/// Resolved response variant routed back to a single in-flight awaiter.
/// Distinct enum variants disambiguate Overwritten vs ShutDown vs
/// "we got a record back" — required by `m4_client_response_overwritten`.
#[derive(Debug)]
pub enum RawResponse {
    Record { msg_type: u16, payload: Bytes },
    Overwritten,
    ShutDown,
}

pub struct ClientRings {
    pub submit_producer: MpscProducer,
    pub query_producer: MpscProducer,
}

impl ClientRings {
    pub fn open(clients_dir: &Path) -> Result<(Self, BroadcastConsumer), ClientError> {
        let submit = MpscRing::open(&clients_dir.join("submit.ring"))
            .map_err(|e| ClientError::Decode(format!("open submit.ring: {e}")))?;
        let query = MpscRing::open(&clients_dir.join("query.ring"))
            .map_err(|e| ClientError::Decode(format!("open query.ring: {e}")))?;
        let response = BroadcastRing::open(&clients_dir.join("response.broadcast"))
            .map_err(|e| ClientError::Decode(format!("open response.broadcast: {e}")))?;
        let (submit_producer, _) = submit.into_split();
        let (query_producer, _) = query.into_split();
        let response_consumer = response.subscribe();
        Ok((
            ClientRings {
                submit_producer,
                query_producer,
            },
            response_consumer,
        ))
    }
}

pub struct BroadcastReaderHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
    /// Pause flag for test-only slow-consumer simulation.
    /// Read by the `_test_pause/resume_broadcast_reader` helpers on `Client`.
    #[cfg_attr(not(any(test, feature = "test-helpers")), allow(dead_code))]
    pub paused: Arc<AtomicBool>,
}

pub fn spawn_broadcast_reader(
    mut consumer: BroadcastConsumer,
    my_client_id: u32,
    in_flight: InFlight,
) -> BroadcastReaderHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let paused = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);
    let paused_for_task = Arc::clone(&paused);

    let join = tokio::spawn(async move {
        let bcast_bridge = crate::ring_bridge::NotifyBridge::spawn(consumer.wait_handle(), "broadcast");
        let mut buf: Vec<u8> = Vec::with_capacity(4096);
        while !stop_for_task.load(Ordering::Relaxed) {
            if paused_for_task.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            match consumer.try_read(&mut buf) {
                Ok(Some(rec)) => {
                    let (cid, local_seq) = decode_extra_client(rec.header_extra);
                    if cid != my_client_id {
                        continue;
                    }
                    uc_protocol::probes::stamp_client(
                        cid,
                        local_seq,
                        uc_protocol::probes::Checkpoint::ClientRecv,
                    );
                    // Recognized types only.
                    let msg_type = rec.msg_type;
                    let ok = matches!(
                        msg_type,
                        MSG_TYPE_SUBMIT_RESPONSE
                            | MSG_TYPE_CLIENT_QUERY_RESP
                            | MSG_TYPE_NOT_LEADER_RESP
                    );
                    if !ok {
                        continue;
                    }
                    let payload = Bytes::copy_from_slice(&buf);
                    if let Some((_, tx)) = in_flight.remove(&local_seq) {
                        let _ = tx.send(RawResponse::Record { msg_type, payload });
                    }
                }
                Ok(None) => bcast_bridge.notified().await,
                Err(RingError::Overwritten) => {
                    // Drain every in-flight with Overwritten.
                    let drained: Vec<u32> = in_flight.iter().map(|e| *e.key()).collect();
                    for k in drained {
                        if let Some((_, tx)) = in_flight.remove(&k) {
                            let _ = tx.send(RawResponse::Overwritten);
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(?e, "broadcast reader: error");
                    tokio::time::sleep(Duration::from_micros(100)).await;
                }
            }
        }
    });

    BroadcastReaderHandle { join, stop, paused }
}
