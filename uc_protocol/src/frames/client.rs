//! Client ↔ node ring frame types (M4).
//!
//! `header_extra` layout (8 bytes):
//!   * bytes 0..4 — `client_id` (u32 LE; identifies the originating
//!     client process; allocated via cnc.dat's `next_client_id` slot).
//!   * bytes 4..8 — `local_seq` (u32 LE; per-client monotonic).
//!
//! Client query frames pack `QueryKind` into bit 8 of the 16-bit `flags`
//! field; bits 0..7 carry the multi-service `service_id: u8` (always `0`
//! in v1). The service-side query frame (`frames::query`) keeps a
//! different `header_extra` shape — see that module — because it does
//! not carry `client_id`.
//!
//! `flags` layout (uniform across client frames and the M4-retrofit
//! service frames):
//!   * bits 0..7  — `service_id: u8` (always `0` in v1; decoders error
//!     on `!= 0` with `UnknownServiceId`).
//!   * bits 8..15 — type-specific. For `ClientQueryFrame`: bit 8 =
//!     `QueryKind` (0 = Linearizable, 1 = Snapshot). For all other
//!     frames: reserved (must be zero).
//!
//! `msg_type`:
//!   * `5` — `SubmitFrame` (clients → node, MPSC `clients/submit.ring`)
//!   * `6` — `SubmitResponse` (node → clients, Broadcast `clients/response.broadcast`)
//!   * `7` — `ClientQueryFrame` (clients → node, MPSC `clients/query.ring`)
//!   * `8` — `ClientQueryResp` (node → clients, Broadcast `clients/response.broadcast`)
//!   * `9` — `NotLeaderResp` (node → clients, Broadcast; payload is the
//!     bincode-encoded `Option<u64>` leader-id hint)

use crate::frames::query::QueryKind;

pub const MSG_TYPE_SUBMIT: u16 = 5;
pub const MSG_TYPE_SUBMIT_RESPONSE: u16 = 6;
pub const MSG_TYPE_CLIENT_QUERY: u16 = 7;
pub const MSG_TYPE_CLIENT_QUERY_RESP: u16 = 8;
pub const MSG_TYPE_NOT_LEADER_RESP: u16 = 9;

/// `flags` bit assignments shared across all M4+ frames.
pub const FLAGS_SERVICE_ID_MASK: u16 = 0x00FF;
/// `flags` bit assignments for client-query frames (bit 8 of `flags`).
pub const FLAG_QUERY_KIND_BIT: u16 = 0x0100;

#[derive(Debug, thiserror::Error)]
pub enum ClientFrameError {
    #[error("unknown service_id: {0}")]
    UnknownServiceId(u8),
}

#[inline]
pub fn encode_extra_client(client_id: u32, local_seq: u32) -> [u8; 8] {
    let mut out = [0u8; 8];
    out[0..4].copy_from_slice(&client_id.to_le_bytes());
    out[4..8].copy_from_slice(&local_seq.to_le_bytes());
    out
}

#[inline]
pub fn decode_extra_client(extra: [u8; 8]) -> (u32, u32) {
    let client_id = u32::from_le_bytes(extra[0..4].try_into().unwrap());
    let local_seq = u32::from_le_bytes(extra[4..8].try_into().unwrap());
    (client_id, local_seq)
}

/// Encode `flags` for a client frame.
///
/// `service_id` rides in the low byte; `kind` (only meaningful for
/// `ClientQueryFrame`) rides in bit 8. v1 always writes `service_id = 0`.
#[inline]
pub fn encode_flags_client(service_id: u8, kind: Option<QueryKind>) -> u16 {
    let mut flags = service_id as u16;
    if let Some(QueryKind::Snapshot) = kind {
        flags |= FLAG_QUERY_KIND_BIT;
    }
    flags
}

/// Decode the (service_id, kind) tuple from a client frame's `flags`.
///
/// Returns `UnknownServiceId(n)` on `service_id != 0` (v1 contract; M5+
/// loosens this).
#[inline]
pub fn decode_flags_client(flags: u16) -> Result<(u8, QueryKind), ClientFrameError> {
    let service_id = (flags & FLAGS_SERVICE_ID_MASK) as u8;
    if service_id != 0 {
        return Err(ClientFrameError::UnknownServiceId(service_id));
    }
    let kind = if flags & FLAG_QUERY_KIND_BIT == 0 {
        QueryKind::Linearizable
    } else {
        QueryKind::Snapshot
    };
    Ok((service_id, kind))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_extra_round_trip() {
        for (cid, seq) in [
            (0u32, 0u32),
            (1, 0),
            (0, 1),
            (0xdead_beef, 0xcafe_babe),
            (u32::MAX, u32::MAX),
        ] {
            let extra = encode_extra_client(cid, seq);
            let (got_cid, got_seq) = decode_extra_client(extra);
            assert_eq!(got_cid, cid);
            assert_eq!(got_seq, seq);
        }
    }

    #[test]
    fn flags_round_trip_v1_service_zero() {
        for k in [QueryKind::Linearizable, QueryKind::Snapshot] {
            let f = encode_flags_client(0, Some(k));
            let (sid, got_k) = decode_flags_client(f).expect("v1 decode");
            assert_eq!(sid, 0);
            assert_eq!(got_k, k);
        }
        // No kind specified: defaults to Linearizable on decode.
        let f = encode_flags_client(0, None);
        let (sid, got_k) = decode_flags_client(f).expect("v1 decode");
        assert_eq!(sid, 0);
        assert_eq!(got_k, QueryKind::Linearizable);
    }

    #[test]
    fn flags_rejects_nonzero_service_id() {
        let f = encode_flags_client(7, Some(QueryKind::Linearizable));
        assert!(matches!(
            decode_flags_client(f),
            Err(ClientFrameError::UnknownServiceId(7))
        ));
    }
}
