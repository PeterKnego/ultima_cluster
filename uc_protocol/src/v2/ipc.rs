// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! IPC ring message-type + flag constants (spec §7), and the client-identity
//! `header_extra` codec shared by every client-facing ring frame. Core-only,
//! like `v2::frame`/`v2::datagram`/`v2::cnc` — these are pure wire constants
//! and (de)serialization helpers, no I/O.
//!
//! Message routing by ring (spec §7):
//!   * `ingress.ring` (MPSC, clients → node): [`MSG_V2_SUBMIT`] — payload is
//!     the client's already-bincoded command bytes.
//!   * `query.ring` (MPSC, clients → node): [`MSG_V2_QUERY`] — payload is
//!     `service_id: u8` (M14b — which FSM answers) followed by the query
//!     bytes; `flags` bit 0 ([`FLAG_V2_LINEARIZABLE`]) selects linearizable
//!     vs. snapshot routing.
//!   * `svc_query.<id>.ring` (SPSC, node → service): [`MSG_V2_SVC_QUERY`] —
//!     payload is `expected_epoch: u64 LE` followed by the query bytes. M14a:
//!     one ring per declared FSM id (`<id>` = `0`..`7`).
//!   * `egress_service.<id>.broadcast` (SERVICE → clients): [`MSG_V2_RESPONSE`] —
//!     written by the service apply agent (`uc2_service::egress`), NOT the node
//!     (the node only creates the ring file; the attaching service owns the
//!     producer). Payload is `position: u64 LE` followed by the response bytes;
//!     `flags` bit 0 ([`FLAG_V2_IS_QUERY`]) distinguishes a query answer from a
//!     submit response. The service also emits [`MSG_V2_RETRY`] here for a
//!     stale-epoch query refusal.
//!   * `egress_node.broadcast` (node → clients): [`MSG_V2_NOT_LEADER`] —
//!     payload is `leader_hint: u64 LE` (`u64::MAX` = unknown); and
//!     [`MSG_V2_BAD_SERVICE`] — payload is `service_id: u8` (M14b).
//!   * Either broadcast ring may also carry [`MSG_V2_RETRY`] — a transient
//!     failure the client should retry, no payload contract beyond that.
//!
//! Every client-originated (and client-targeted) ring record carries the
//! originating client's identity in the record's `header_extra: [u8; 8]`
//! field (NOT the payload) via [`extra_client`]/[`client_from_extra`] — the
//! v1 `(client_id: u32 LE, local_seq: u32 LE)` pair layout, kept unchanged so
//! a client can filter the shared broadcast rings by its own identity (every
//! client sees every record; there is no ring-level per-client targeting).

/// `ingress.ring`: payload = command bytes (already bincoded by the client).
pub const MSG_V2_SUBMIT: u16 = 1;
/// `query.ring`: payload = service_id: u8 ++ query bytes; `flags` bit 0 =
/// [`FLAG_V2_LINEARIZABLE`].
pub const MSG_V2_QUERY: u16 = 2;
/// `svc_query.ring`: payload = `expected_epoch: u64 LE` ++ query bytes.
pub const MSG_V2_SVC_QUERY: u16 = 3;
/// `egress_service.broadcast`: payload = `position: u64 LE` ++ response bytes;
/// `flags` bit 0 = [`FLAG_V2_IS_QUERY`].
pub const MSG_V2_RESPONSE: u16 = 4;
/// `egress_node.broadcast`: payload = `leader_hint: u64 LE` (`u64::MAX` =
/// unknown).
pub const MSG_V2_NOT_LEADER: u16 = 5;
/// `egress_node` or `egress_service` broadcast: transient failure, client
/// retries.
pub const MSG_V2_RETRY: u16 = 6;
/// `egress_node.broadcast` (M14b): the query named a service id this node has
/// no ring for — undeclared, out of range (`>= CNC_MAX_SERVICES`), or a harness
/// node's non-zero id. Payload = `service_id: u8` (the offending id). Kind-
/// agnostic like [`MSG_V2_RETRY`]: no side effect happened, the slot resolves.
pub const MSG_V2_BAD_SERVICE: u16 = 7;

/// `query.ring` `flags` bit 0: linearizable (vs. snapshot) read.
pub const FLAG_V2_LINEARIZABLE: u16 = 1;
/// `egress_service.broadcast` `flags` bit 0: response answers a query (vs. a
/// submit).
pub const FLAG_V2_IS_QUERY: u16 = 1;

/// Encode the `(client_id, local_seq)` pair carried in a ring record's
/// `header_extra` — the v1 layout kept: bytes 0..4 = `client_id` (u32 LE),
/// bytes 4..8 = `local_seq` (u32 LE, per-client monotonic).
#[inline]
pub fn extra_client(client_id: u32, local_seq: u32) -> [u8; 8] {
    let mut out = [0u8; 8];
    out[0..4].copy_from_slice(&client_id.to_le_bytes());
    out[4..8].copy_from_slice(&local_seq.to_le_bytes());
    out
}

/// Decode the `(client_id, local_seq)` pair from a record's `header_extra`.
#[inline]
pub fn client_from_extra(extra: [u8; 8]) -> (u32, u32) {
    let client_id = u32::from_le_bytes(extra[0..4].try_into().unwrap());
    let local_seq = u32::from_le_bytes(extra[4..8].try_into().unwrap());
    (client_id, local_seq)
}

/// M14b: the `query.ring` payload is `service_id: u8 ++ query bytes`. Split it;
/// `None` for an empty payload (no id byte — a malformed record the node drops).
#[inline]
pub fn split_query_payload(payload: &[u8]) -> Option<(u8, &[u8])> {
    payload.split_first().map(|(id, rest)| (*id, rest))
}

/// M14b: build a `query.ring` payload into `out` (cleared first).
#[inline]
pub fn write_query_payload(service_id: u8, query: &[u8], out: &mut Vec<u8>) {
    out.clear();
    out.reserve(1 + query.len());
    out.push(service_id);
    out.extend_from_slice(query);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msg_type_and_flag_codes_are_stable() {
        assert_eq!(MSG_V2_SUBMIT, 1);
        assert_eq!(MSG_V2_QUERY, 2);
        assert_eq!(MSG_V2_SVC_QUERY, 3);
        assert_eq!(MSG_V2_RESPONSE, 4);
        assert_eq!(MSG_V2_NOT_LEADER, 5);
        assert_eq!(MSG_V2_RETRY, 6);
        assert_eq!(MSG_V2_BAD_SERVICE, 7);
        assert_eq!(FLAG_V2_LINEARIZABLE, 1);
        assert_eq!(FLAG_V2_IS_QUERY, 1);
    }

    #[test]
    fn query_payload_codec_round_trips_and_pins_the_prefix() {
        let mut out = Vec::new();
        write_query_payload(3, b"read", &mut out);
        assert_eq!(out, [3, b'r', b'e', b'a', b'd']);
        assert_eq!(split_query_payload(&out), Some((3, &b"read"[..])));
        // Reused buffer is cleared first.
        write_query_payload(0, b"", &mut out);
        assert_eq!(out, [0]);
        assert_eq!(split_query_payload(&out), Some((0, &b""[..])));
        // An empty payload has no id byte.
        assert_eq!(split_query_payload(&[]), None);
        // Any byte is a valid id at this layer (range/declared checks are the node's).
        assert_eq!(split_query_payload(&[255, 1]), Some((255, &[1][..])));
    }

    #[test]
    fn extra_client_round_trips() {
        for (cid, seq) in [
            (0u32, 0u32),
            (1, 0),
            (0, 1),
            (0xdead_beef, 0xcafe_babe),
            (u32::MAX, u32::MAX),
        ] {
            let extra = extra_client(cid, seq);
            assert_eq!(client_from_extra(extra), (cid, seq));
        }
    }

    #[test]
    fn extra_client_pins_literal_le_bytes() {
        // Absolute wire pin (not just internal round-trip) — a round trip
        // alone cannot catch a consistently-swapped field order. client_id =
        // 0x0102_0304, local_seq = 0x0506_0708.
        let extra = extra_client(0x0102_0304, 0x0506_0708);
        assert_eq!(extra, [0x04, 0x03, 0x02, 0x01, 0x08, 0x07, 0x06, 0x05]);
    }
}
