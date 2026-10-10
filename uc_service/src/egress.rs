// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The service's egress publisher: response frames onto
//! `egress_service.broadcast`.
//!
//! **Pinned frame layout (Task 8 / Task 10 matcher must agree byte-for-byte):**
//! a submit response is `MSG_V2_RESPONSE` with
//! `header_extra = extra_client(client_id, seq)` —
//! the SAME `(client_id, local_seq)` pair the client stamped on submit, so the
//! client can pick its own answer out of the shared broadcast — and payload
//! `position: u64 LE ++ response bytes` (for a typed state machine those bytes
//! are exactly `bincode(response)`, produced inside the blanket
//! [`RawStateMachine`](crate::RawStateMachine) impl — byte-identical to
//! v2.5.0).

use uc_protocol::ring::BroadcastProducer;
use uc_protocol::v2::ipc::{FLAG_V2_IS_QUERY, MSG_V2_RESPONSE, MSG_V2_RETRY, extra_client};

pub(crate) struct Egress {
    producer: BroadcastProducer,
    /// Reused frame scratch: `position ++ response bytes`. Cleared and refilled
    /// per publish, so a steady-state response allocates nothing (the response
    /// bytes themselves already live in the apply thread's `resp_buf`).
    scratch: Vec<u8>,
}

impl Egress {
    pub(crate) fn new(producer: BroadcastProducer) -> Self {
        Self {
            producer,
            scratch: Vec::with_capacity(8 + 256),
        }
    }

    /// Publish a submit response: `position LE ++ resp` on the egress
    /// broadcast, keyed for the client matcher. `resp` is the state machine's
    /// response bytes (typed tier: bincode; raw tier: whatever it wrote). One
    /// memcpy into the reused scratch, then the ring write — no allocation in
    /// steady state.
    ///
    /// Best-effort: `BroadcastProducer` never blocks (a slow client simply
    /// misses records and re-submits). A deposed-leader race window can publish
    /// a duplicate response for an already-committed op — harmless: committed is
    /// committed, and the client matcher takes the first answer for a
    /// `(client_id, local_seq)` pair.
    // Forced inline: the apply loop calls this once per frame. It was inlined
    // at 17d5c6b and fell out of line (a GOT-indirect call per frame) once
    // `apply_cycle` outgrew the inliner's budget — see `FrameIter::next` in
    // `uc_log::reader` for the measurement (2026-09-07).
    #[inline(always)]
    pub(crate) fn publish(&mut self, client_id: u32, seq: u32, position: u64, resp: &[u8]) {
        self.scratch.clear();
        self.scratch.extend_from_slice(&position.to_le_bytes());
        self.scratch.extend_from_slice(resp);
        let extra = extra_client(client_id, seq);
        // flags = 0: this is a submit response, not a query answer (FLAG_V2_IS_QUERY).
        let _ = self
            .producer
            .write(MSG_V2_RESPONSE, 0, extra, &self.scratch);
    }

    /// Publish a QUERY answer: `MSG_V2_RESPONSE` with `FLAG_V2_IS_QUERY`,
    /// echoing the `svc_query` record's `header_extra`. The position prefix is
    /// the row's applied frontier when the query ran (an EXCLUSIVE end —
    /// read-your-writes spec 2026-10-08 §4.3), so a client can raise its token
    /// from it and check it against the token it sent.
    pub(crate) fn publish_query_answer(
        &mut self,
        header_extra: [u8; 8],
        applied: u64,
        resp: &[u8],
    ) {
        self.scratch.clear();
        self.scratch.extend_from_slice(&applied.to_le_bytes());
        self.scratch.extend_from_slice(resp);
        let _ = self.producer.write(
            MSG_V2_RESPONSE,
            FLAG_V2_IS_QUERY,
            header_extra,
            &self.scratch,
        );
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
