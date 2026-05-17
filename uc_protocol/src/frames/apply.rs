//! Apply ring frame types.
//!
//! `flags` field: bits 0..7 = `service_id: u8` (v1 always 0); bits 8..15
//! reserved (must be zero).
//!
//! `header_extra` layout (8 bytes): the raft `log_index` as a u64
//! little-endian. The payload bytes are the bincode-encoded user command
//! (request) or response.
//!
//! `msg_type`:
//!   * `1` — `ApplyFrame` (node → service)
//!   * `2` — `ApplyRespFrame` (service → node)

pub const MSG_TYPE_APPLY: u16 = 1;
pub const MSG_TYPE_APPLY_RESP: u16 = 2;

#[inline]
pub fn encode_extra_apply(log_index: u64) -> [u8; 8] {
    log_index.to_le_bytes()
}

#[inline]
pub fn decode_extra_apply(extra: [u8; 8]) -> u64 {
    u64::from_le_bytes(extra)
}

/// Mask for the low byte of `flags`, where `service_id` lives.
pub const FLAGS_SERVICE_ID_MASK: u16 = 0x00FF;

#[derive(Debug, thiserror::Error)]
pub enum ApplyFrameError {
    #[error("unknown service_id: {0}")]
    UnknownServiceId(u8),
}

#[inline]
pub fn encode_flags_apply(service_id: u8) -> u16 {
    service_id as u16
}

/// Decode `service_id` from an `ApplyFrame`/`ApplyRespFrame` `flags` field.
/// Returns `UnknownServiceId(n)` on `service_id != 0` (v1 contract).
#[inline]
pub fn decode_flags_apply(flags: u16) -> Result<u8, ApplyFrameError> {
    let service_id = (flags & FLAGS_SERVICE_ID_MASK) as u8;
    if service_id != 0 {
        return Err(ApplyFrameError::UnknownServiceId(service_id));
    }
    Ok(service_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        for li in [0u64, 1, 42, 1 << 40, u64::MAX] {
            assert_eq!(decode_extra_apply(encode_extra_apply(li)), li);
        }
    }

    #[test]
    fn flags_apply_round_trip_v1() {
        let f = encode_flags_apply(0);
        assert_eq!(decode_flags_apply(f).unwrap(), 0);
    }

    #[test]
    fn flags_apply_rejects_nonzero() {
        let f = encode_flags_apply(3);
        assert!(matches!(
            decode_flags_apply(f),
            Err(ApplyFrameError::UnknownServiceId(3))
        ));
    }
}
