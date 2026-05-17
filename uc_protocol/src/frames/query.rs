//! Query ring frame types.
//!
//! `flags` field: bits 0..7 = `service_id: u8` (v1 always 0); bits 8..15
//! reserved (must be zero). Note: `QueryKind` remains in `header_extra` byte 4
//! for backwards compatibility — it is NOT moved to flags.
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
    #[error("unknown service_id: {0}")]
    UnknownServiceId(u8),
}

/// Mask for the low byte of `flags`, where `service_id` lives.
pub const FLAGS_SERVICE_ID_MASK: u16 = 0x00FF;

#[inline]
pub fn encode_flags_query(service_id: u8) -> u16 {
    service_id as u16
}

/// Decode `service_id` from a `QueryFrame`/`QueryRespFrame` `flags` field.
/// Returns `UnknownServiceId(n)` on `service_id != 0` (v1 contract).
#[inline]
pub fn decode_flags_query(flags: u16) -> Result<u8, QueryFrameError> {
    let service_id = (flags & FLAGS_SERVICE_ID_MASK) as u8;
    if service_id != 0 {
        return Err(QueryFrameError::UnknownServiceId(service_id));
    }
    Ok(service_id)
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

    #[test]
    fn flags_query_round_trip_v1() {
        let f = encode_flags_query(0);
        assert_eq!(decode_flags_query(f).unwrap(), 0);
    }

    #[test]
    fn flags_query_rejects_nonzero() {
        let f = encode_flags_query(3);
        assert!(matches!(
            decode_flags_query(f),
            Err(QueryFrameError::UnknownServiceId(3))
        ));
    }
}
