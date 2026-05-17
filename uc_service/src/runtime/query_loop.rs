//! Async query loop.
//!
//! Runs as a tokio task. Drains `service/query.ring`, takes the SM mutex
//! (cheap, parking_lot), calls `state_machine.query(q)`, publishes
//! `QueryRespFrame` into `service/query_resp.ring`. The lock is never held
//! across an `await`, so queries from many tokio callers can interleave
//! cooperatively.
//!
//! Shutdown: caller sets [`QueryLoopHandle::stop`] and `await`s the join.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bincode::config::standard as bincode_standard;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use uc_protocol::frames::query::{
    MSG_TYPE_QUERY, MSG_TYPE_QUERY_RESP, QueryKind, decode_extra_query, decode_flags_query,
    encode_extra_query, encode_flags_query,
};
use uc_protocol::ring::RingError;
use uc_protocol::ring::spsc::{SpscConsumer, SpscProducer};

use crate::StateMachine;

const IDLE_BACKOFF: Duration = Duration::from_micros(200);
const ERROR_BACKOFF: Duration = Duration::from_millis(10);

pub struct QueryLoopHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn_query_loop<S>(
    sm: Arc<Mutex<S>>,
    consumer: SpscConsumer,
    resp_producer: SpscProducer,
) -> QueryLoopHandle
where
    S: StateMachine,
{
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);
    let join = tokio::spawn(query_task_body::<S>(
        sm,
        consumer,
        resp_producer,
        stop_for_task,
    ));
    QueryLoopHandle { join, stop }
}

async fn query_task_body<S>(
    sm: Arc<Mutex<S>>,
    mut consumer: SpscConsumer,
    mut resp_producer: SpscProducer,
    stop: Arc<AtomicBool>,
) where
    S: StateMachine,
{
    let mut payload_buf: Vec<u8> = Vec::with_capacity(4096);
    while !stop.load(Ordering::Relaxed) {
        match consumer.try_read(&mut payload_buf) {
            Ok(Some(rec)) if rec.msg_type == MSG_TYPE_QUERY => {
                if let Err(e) = decode_flags_query(rec.flags) {
                    tracing::warn!("dropping query frame with bad flags: {e}");
                    continue;
                }
                let (request_id, kind) = match decode_extra_query(rec.header_extra) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(error = %e, "query header_extra decode");
                        continue;
                    }
                };
                let (q, _) = match bincode::serde::decode_from_slice::<S::Query, _>(
                    &payload_buf,
                    bincode_standard(),
                ) {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::error!(error = %e, request_id, "query payload decode failed");
                        panic!("query decode: {e}");
                    }
                };
                let resp = {
                    let guard = sm.lock();
                    guard.query(q)
                };
                let resp_bytes = bincode::serde::encode_to_vec(&resp, bincode_standard())
                    .expect("query response encode");
                publish_response(&mut resp_producer, request_id, kind, &resp_bytes, &stop).await;
            }
            Ok(Some(rec)) => {
                tracing::warn!(msg_type = rec.msg_type, "query ring: unexpected frame");
            }
            Ok(None) => tokio::time::sleep(IDLE_BACKOFF).await,
            Err(e) => {
                tracing::warn!(error = %e, "query ring read error");
                tokio::time::sleep(ERROR_BACKOFF).await;
            }
        }
    }
}

async fn publish_response(
    producer: &mut SpscProducer,
    request_id: u32,
    kind: QueryKind,
    resp_bytes: &[u8],
    stop: &AtomicBool,
) {
    loop {
        match producer.try_write(
            MSG_TYPE_QUERY_RESP,
            encode_flags_query(0),
            encode_extra_query(request_id, kind),
            resp_bytes,
        ) {
            Ok(()) => return,
            Err(RingError::Full) => {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                tokio::task::yield_now().await;
            }
            Err(e) => panic!("query_resp write request_id={request_id}: {e}"),
        }
    }
}
