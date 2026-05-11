//! Query ring frame types.
//!
//! `header_extra` layout (8 bytes):
//!   * bytes 0..4 — `request_id` (u32 LE, allocated by node, lifetime-scoped
//!     to the ring; rolls over when it hits `u32::MAX`).
//!   * byte 4   — `kind` (`0 = Linearizable`, `1 = Snapshot`).
//!   * bytes 5..8 — reserved (must be zero).
//!
//! `msg_type`:
//!   * `3` — `QueryFrame` (node → service)
//!   * `4` — `QueryRespFrame` (service → node)

pub const MSG_TYPE_QUERY: u16 = 3;
pub const MSG_TYPE_QUERY_RESP: u16 = 4;

#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum QueryKind {
    Linearizable = 0,
    Snapshot = 1,
}

#[derive(Debug, thiserror::Error)]
pub enum QueryFrameError {
    #[error("unknown query kind byte: {0}")]
    UnknownKind(u8),
}

#[inline]
pub fn encode_extra_query(request_id: u32, kind: QueryKind) -> [u8; 8] {
    let mut out = [0u8; 8];
    out[0..4].copy_from_slice(&request_id.to_le_bytes());
    out[4] = kind as u8;
    out
}

#[inline]
pub fn decode_extra_query(extra: [u8; 8]) -> Result<(u32, QueryKind), QueryFrameError> {
    let request_id = u32::from_le_bytes(extra[0..4].try_into().unwrap());
    let kind = match extra[4] {
        0 => QueryKind::Linearizable,
        1 => QueryKind::Snapshot,
        n => return Err(QueryFrameError::UnknownKind(n)),
    };
    Ok((request_id, kind))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_linearizable() {
        let extra = encode_extra_query(0xdead_beef, QueryKind::Linearizable);
        let (req, kind) = decode_extra_query(extra).expect("decode");
        assert_eq!(req, 0xdead_beef);
        assert_eq!(kind, QueryKind::Linearizable);
    }

    #[test]
    fn round_trip_snapshot() {
        let extra = encode_extra_query(7, QueryKind::Snapshot);
        let (req, kind) = decode_extra_query(extra).expect("decode");
        assert_eq!(req, 7);
        assert_eq!(kind, QueryKind::Snapshot);
    }

    #[test]
    fn rejects_unknown_kind() {
        let mut extra = encode_extra_query(0, QueryKind::Linearizable);
        extra[4] = 99;
        let r = decode_extra_query(extra);
        assert!(matches!(r, Err(QueryFrameError::UnknownKind(99))));
    }

    #[test]
    fn reserved_bytes_remain_zero() {
        let extra = encode_extra_query(1, QueryKind::Linearizable);
        assert_eq!(&extra[5..], &[0, 0, 0]);
    }
}
