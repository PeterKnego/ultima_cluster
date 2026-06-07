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
    MSG_TYPE_QUERY, MSG_TYPE_QUERY_RESP, QueryKind, decode_extra_query, decode_flags_query,
    encode_extra_query, encode_flags_query,
};
use uc_protocol::ring::RingError;
use uc_protocol::ring::spsc::{SpscConsumer, SpscProducer};

use crate::ClusterError;

const FULL_BACKOFF: Duration = Duration::from_micros(100);
const EMPTY_BACKOFF: Duration = Duration::from_micros(100);

pub struct ShmemQueryLink {
    inner: TokioMutex<ShmemQueryInner>,
    /// Read-gate: reconstruct a reattached service to the node frontier before
    /// serving a query, so a freshly-restarted in-memory service can't answer
    /// with stale/empty state. Holds only shared signals (no adapter clone).
    /// `None` in unit tests that drive the rings directly.
    gate: Option<crate::raft::state_machine_shmem::ReconcileGate>,
}

struct ShmemQueryInner {
    producer: SpscProducer,
    consumer: SpscConsumer,
    next_request_id: u32,
}

impl ShmemQueryLink {
    pub fn new(producer: SpscProducer, consumer: SpscConsumer) -> Self {
        Self::with_gate(producer, consumer, None)
    }

    /// Construct with a reconciliation read-[`ReconcileGate`](crate::raft::state_machine_shmem::ReconcileGate).
    /// The production builder passes one so reads reconstruct a reattached service
    /// before being served; tests use [`Self::new`] (no gate).
    pub(crate) fn with_gate(
        producer: SpscProducer,
        consumer: SpscConsumer,
        gate: Option<crate::raft::state_machine_shmem::ReconcileGate>,
    ) -> Self {
        Self {
            inner: TokioMutex::new(ShmemQueryInner {
                producer,
                consumer,
                next_request_id: 0,
            }),
            gate,
        }
    }

    /// Publish `payload` onto `query.ring`, await the matching response
    /// frame on `query_resp.ring`, return its payload bytes. Caller is
    /// responsible for bincode-encoding the typed `Query` and decoding the
    /// returned `QueryResponse`.
    pub async fn submit(&self, payload: &[u8], kind: QueryKind) -> Result<Bytes, ClusterError> {
        // Read-gate: if the service reattached, wait until the reconcile driver has
        // rebuilt it to the node frontier before serving — else a fresh in-memory
        // SM would answer stale (a linearizability violation). Fast path returns
        // immediately when the current incarnation is already reconciled. Done
        // BEFORE taking the query-link lock so a wait doesn't serialize queries.
        if let Some(gate) = &self.gate {
            gate.wait_until_reconciled().await;
        }
        let mut g = self.inner.lock().await;
        let request_id = g.next_request_id;
        g.next_request_id = g.next_request_id.wrapping_add(1);

        loop {
            match g.producer.try_write(
                MSG_TYPE_QUERY,
                encode_flags_query(0),
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
                    if let Err(e) = decode_flags_query(rec.flags) {
                        tracing::warn!("dropping query_resp frame with bad flags: {e}");
                        continue;
                    }
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
