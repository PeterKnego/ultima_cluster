// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The service's egress publisher: response frames onto
//! `egress_service.broadcast`.
//!
//! **Pinned frame layout (Task 8 / Task 10 matcher must agree byte-for-byte):**
//! a submit response is `MSG_V2_RESPONSE` with
//! `header_extra = extra_client(session_id as u32, correlation_id as u32)` —
//! the SAME `(client_id, local_seq)` pair the client stamped on submit, so the
//! client can pick its own answer out of the shared broadcast — and payload
//! `position: u64 LE ++ bincode(response)`.

use uc_protocol::ring::BroadcastProducer;
use uc_protocol::v2::ipc::{FLAG_V2_IS_QUERY, MSG_V2_RESPONSE, MSG_V2_RETRY, extra_client};

pub(crate) struct Egress {
    producer: BroadcastProducer,
}

impl Egress {
    pub(crate) fn new(producer: BroadcastProducer) -> Self {
        Self { producer }
    }

    /// Publish a submit response. Best-effort: `BroadcastProducer` never blocks
    /// (a slow client simply misses records and re-submits). A deposed-leader
    /// race window can publish a duplicate response for an already-committed op
    /// — harmless: committed is committed, and the client matcher takes the
    /// first answer for a `(client_id, local_seq)` pair.
    pub(crate) fn publish<R: serde::Serialize>(
        &mut self,
        session_id: u64,
        correlation_id: u64,
        position: u64,
        resp: &R,
    ) {
        #[cfg(feature = "apply-profile")]
        let t0 = crate::apply::profile::now();
        let mut payload = Vec::with_capacity(8 + 32);
        payload.extend_from_slice(&position.to_le_bytes());
        // The response type is the user's; an encode failure here is a program
        // bug (a non-serializable Response is a compile-time-prevented shape in
        // practice), not a runtime condition to recover from — fail-stop.
        bincode::serde::encode_into_std_write(resp, &mut payload, bincode::config::standard())
            .expect("response bincode-encode (fail-stop)");
        #[cfg(feature = "apply-profile")]
        crate::apply::profile::ENCODE.fetch_add(
            crate::apply::profile::now() - t0,
            std::sync::atomic::Ordering::Relaxed,
        );
        let extra = extra_client(session_id as u32, correlation_id as u32);
        // flags = 0: this is a submit response, not a query answer (FLAG_V2_IS_QUERY).
        let _ = self.producer.write(MSG_V2_RESPONSE, 0, extra, &payload);
    }

    /// Publish a QUERY answer (Task 11): `MSG_V2_RESPONSE` with
    /// `FLAG_V2_IS_QUERY`, echoing the `svc_query` record's `header_extra` (the
    /// client identity the node stamped) so the client matcher routes it. Payload
    /// is `position: u64 LE ++ bincode(query response)` — the SAME shape as a
    /// submit response, so the client decodes both identically. The read barrier
    /// does not thread a position through to the service, so the prefix is `0`
    /// here (the client matcher skips those 8 bytes for query answers either way).
    pub(crate) fn publish_query_answer<QR: serde::Serialize>(
        &mut self,
        header_extra: [u8; 8],
        resp: &QR,
    ) {
        let mut payload = Vec::with_capacity(8 + 32);
        payload.extend_from_slice(&0u64.to_le_bytes());
        bincode::serde::encode_into_std_write(resp, &mut payload, bincode::config::standard())
            .expect("query response bincode-encode (fail-stop)");
        let _ = self.producer.write(MSG_V2_RESPONSE, FLAG_V2_IS_QUERY, header_extra, &payload);
    }

    /// Publish `MSG_V2_RETRY` for a query the service refuses (Task 11) — the
    /// only refusal today is a STALE service-epoch stamp (a read routed for a
    /// superseded incarnation). Emitted PRE-query and side-effect-free: the SM is
    /// never touched on the mismatch path, so the client may safely re-issue
    /// (the cross-task RETRY-is-side-effect-free invariant). `header_extra`
    /// echoes the client identity so the matcher routes the retry.
    pub(crate) fn publish_retry(&mut self, header_extra: [u8; 8]) {
        let _ = self.producer.write(MSG_V2_RETRY, 0, header_extra, &[]);
    }
}
