//! Node-side query link.
//!
//! Wraps the `query.ring` producer and `query_resp.ring` consumer that
//! [`super::service_link::ServiceLink::create`] handed back. Exposed to the
//! public [`NodeHandle`](crate::NodeHandle) so callers in shmem mode can
//! submit a typed `S::Query`, have it bincode-encoded and pushed onto the
//! query ring, and await the matching `QueryRespFrame` from the service.
//!
//! Concurrency: a single `tokio::sync::Mutex` serializes the whole
//! `submit -> await` operation. SPSC rings require a single writer, and
//! response-routing is trivial when only one request is in flight at a
//! time. `request_id` is still allocated and round-tripped for sanity —
//! a mismatch indicates corruption (the service publishes responses in the
//! same order it consumes requests).
//!
//! M3 only emits `QueryKind::Snapshot`. `Linearizable` is reserved for the
//! M4+ raft-read path.

use std::time::Duration;

use bytes::Bytes;
use tokio::sync::Mutex as TokioMutex;

use uc_protocol::frames::query::{
    MSG_TYPE_QUERY, MSG_TYPE_QUERY_RESP, QueryKind, decode_extra_query, encode_extra_query,
};
use uc_protocol::ring::RingError;
use uc_protocol::ring::spsc::{SpscConsumer, SpscProducer};

use crate::ClusterError;

const FULL_BACKOFF: Duration = Duration::from_micros(100);
const EMPTY_BACKOFF: Duration = Duration::from_micros(100);

pub struct ShmemQueryLink {
    inner: TokioMutex<ShmemQueryInner>,
}

struct ShmemQueryInner {
    producer: SpscProducer,
    consumer: SpscConsumer,
    next_request_id: u32,
}

impl ShmemQueryLink {
    pub fn new(producer: SpscProducer, consumer: SpscConsumer) -> Self {
        Self {
            inner: TokioMutex::new(ShmemQueryInner {
                producer,
                consumer,
                next_request_id: 0,
            }),
        }
    }

    /// Publish `payload` onto `query.ring`, await the matching response
    /// frame on `query_resp.ring`, return its payload bytes. Caller is
    /// responsible for bincode-encoding the typed `Query` and decoding the
    /// returned `QueryResponse`.
    pub async fn submit(&self, payload: &[u8], kind: QueryKind) -> Result<Bytes, ClusterError> {
        let mut g = self.inner.lock().await;
        let request_id = g.next_request_id;
        g.next_request_id = g.next_request_id.wrapping_add(1);

        loop {
            match g.producer.try_write(
                MSG_TYPE_QUERY,
                0,
                encode_extra_query(request_id, kind),
                payload,
            ) {
                Ok(()) => break,
                Err(RingError::Full) => tokio::time::sleep(FULL_BACKOFF).await,
                Err(e) => {
                    return Err(ClusterError::Io(std::io::Error::other(format!(
                        "query ring write request_id={request_id}: {e}"
                    ))));
                }
            }
        }

        let mut payload_buf: Vec<u8> = Vec::with_capacity(1024);
        loop {
            match g.consumer.try_read(&mut payload_buf) {
                Ok(Some(rec)) if rec.msg_type == MSG_TYPE_QUERY_RESP => {
                    let (req, _kind) = decode_extra_query(rec.header_extra).map_err(|e| {
                        ClusterError::Io(std::io::Error::other(format!(
                            "query_resp decode header_extra: {e}"
                        )))
                    })?;
                    if req != request_id {
                        // Single-writer + single-reader + tokio serialization
                        // means responses arrive in publish order. A mismatch
                        // here is unrecoverable corruption.
                        return Err(ClusterError::Io(std::io::Error::other(format!(
                            "query_resp request_id mismatch: got {req}, expected {request_id}"
                        ))));
                    }
                    return Ok(Bytes::from(std::mem::take(&mut payload_buf)));
                }
                Ok(Some(rec)) => {
                    tracing::warn!(
                        msg_type = rec.msg_type,
                        "unexpected frame on query_resp ring"
                    );
                }
                Ok(None) => tokio::time::sleep(EMPTY_BACKOFF).await,
                Err(e) => {
                    return Err(ClusterError::Io(std::io::Error::other(format!(
                        "query_resp ring read: {e}"
                    ))));
                }
            }
        }
    }
}
