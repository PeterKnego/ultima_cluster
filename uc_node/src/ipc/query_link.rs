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
//! `QueryKind::Snapshot` (any node) and `QueryKind::Linearizable` are both wired:
//! linearizable reads go through the client dispatcher's `ensure_linearizable`
//! (ReadIndex barrier), and `submit` then waits via the [`ReconcileGate`] for the
//! service to catch up to the read index before serving.
//!
//! [`ReconcileGate`]: crate::raft::state_machine_shmem::ReconcileGate

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

    /// Publish `payload` onto `query.ring`, await the matching response frame on
    /// `query_resp.ring`, return its payload bytes. Caller bincode-encodes the
    /// typed `Query` and decodes the `QueryResponse`.
    ///
    /// Read barrier + seqlock: wait until the current service incarnation is
    /// reconstructed to `read_index` (`read_index == 0` = current incarnation only,
    /// for snapshot reads), serve, then ACCEPT only if the service stayed at that
    /// same reconciled incarnation throughout the query. If the service crashed and
    /// restarted between the gate check and the answer (a fresh/empty incarnation
    /// could have answered), retry. This closes the TOCTOU between checking
    /// readiness on the node and querying a separately-crashable service process.
    pub async fn submit(
        &self,
        payload: &[u8],
        kind: QueryKind,
        read_index: u64,
    ) -> Result<Bytes, ClusterError> {
        let Some(gate) = &self.gate else {
            // No gate (unit tests drive the rings directly): single-shot.
            return self.serve_once(payload, kind).await;
        };
        let mut retries: u32 = 0;
        loop {
            // Gate done BEFORE the query-link lock so a wait doesn't serialize
            // queries. Capturing the live `reconciled_epoch` here (rather than the
            // exact value the gate observed) is safe: `reconciled_epoch` is only ever
            // advanced AFTER reconstruction has caught the service up to the frontier
            // (>= any read_index), so any epoch we treat as reconciled is already
            // read-barrier-valid.
            gate.wait_until_caught_up(read_index).await;
            let reconciled = gate.reconciled_epoch();
            let resp = self.serve_once(payload, kind).await?;
            if gate.current_epoch() == reconciled {
                // Service did not restart during the query → the answer is from the
                // reconstructed incarnation the gate validated. Accept.
                return Ok(resp);
            }
            // Service restarted mid-query; the response may be from a fresh/empty
            // incarnation. Retry (the next gate wait blocks until the new incarnation
            // is reconstructed). Bounded in practice by the caller's deadline; a
            // restart storm would otherwise be silent, so make it observable.
            retries += 1;
            if retries.is_multiple_of(64) {
                tracing::warn!(
                    retries,
                    "query seqlock retrying: service restarting repeatedly"
                );
            }
        }
    }

    /// One publish → await round-trip on the query rings (no read barrier).
    async fn serve_once(&self, payload: &[u8], kind: QueryKind) -> Result<Bytes, ClusterError> {
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
