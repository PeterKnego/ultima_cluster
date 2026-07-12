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
use uc_protocol::v2::ipc::{MSG_V2_RESPONSE, extra_client};

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
        let mut payload = Vec::with_capacity(8 + 32);
        payload.extend_from_slice(&position.to_le_bytes());
        // The response type is the user's; an encode failure here is a program
        // bug (a non-serializable Response is a compile-time-prevented shape in
        // practice), not a runtime condition to recover from — fail-stop.
        bincode::serde::encode_into_std_write(resp, &mut payload, bincode::config::standard())
            .expect("response bincode-encode (fail-stop)");
        let extra = extra_client(session_id as u32, correlation_id as u32);
        // flags = 0: this is a submit response, not a query answer (FLAG_V2_IS_QUERY).
        let _ = self.producer.write(MSG_V2_RESPONSE, 0, extra, &payload);
    }
}
