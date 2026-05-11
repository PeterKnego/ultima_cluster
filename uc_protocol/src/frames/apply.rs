//! Apply ring frame types.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        for li in [0u64, 1, 42, 1 << 40, u64::MAX] {
            assert_eq!(decode_extra_apply(encode_extra_apply(li)), li);
        }
    }
}
