//! Output ring frame types (M5).
//!
//! `header_extra` layout (8 bytes): the raft `log_index` as a u64
//! little-endian.
//!
//! `flags` layout (uniform with M4 retrofits):
//!   * bits 0..7  — `service_id: u8` (always `0` in v1; decoders error
//!     on `!= 0` with `UnknownServiceId`).
//!   * bits 8..15 — reserved (must be zero).
//!
//! `msg_type`:
//!   * `10` — `OutputFrame` (node → service, SPSC `service/output.ring`).
//!     Payload: bincode-encoded `Command` from the journal record at
//!     `log_index`. Identical bytes to the original `ApplyFrame` payload.
//!   * `11` — `OutputResp` (service → node, SPSC `service/output_resp.ring`).
//!     Payload: bincode-encoded `Result<(), OutputError>`.

use serde::{Deserialize, Serialize};

pub const MSG_TYPE_OUTPUT: u16 = 10;
pub const MSG_TYPE_OUTPUT_RESP: u16 = 11;

pub const FLAGS_SERVICE_ID_MASK: u16 = 0x00FF;

#[derive(Debug, thiserror::Error)]
pub enum OutputFrameError {
    #[error("unknown service_id: {0}")]
    UnknownServiceId(u8),
}

/// User-returned outcome from `OutputHandler::on_committed`.
///
/// Wire-encoded by `uc_service` and decoded by `uc_node`'s
/// output_dispatcher to decide retry vs advance.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum OutputError {
    /// Retry while still leader. Backoff bounded by output_dispatcher.
    #[error("retryable: {0}")]
    Retryable(String),
    /// Log warn, advance output_progress anyway, move on.
    #[error("permanent: {0}")]
    Permanent(String),
}

impl OutputError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, OutputError::Retryable(_))
    }
}

#[inline]
pub fn encode_extra_output(log_index: u64) -> [u8; 8] {
    log_index.to_le_bytes()
}

#[inline]
pub fn decode_extra_output(extra: [u8; 8]) -> u64 {
    u64::from_le_bytes(extra)
}

#[inline]
pub fn encode_flags_output(service_id: u8) -> u16 {
    service_id as u16
}

#[inline]
pub fn decode_flags_output(flags: u16) -> Result<u8, OutputFrameError> {
    let service_id = (flags & FLAGS_SERVICE_ID_MASK) as u8;
    if service_id != 0 {
        return Err(OutputFrameError::UnknownServiceId(service_id));
    }
    Ok(service_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extra_round_trip() {
        for li in [0u64, 1, 42, 1 << 40, u64::MAX] {
            assert_eq!(decode_extra_output(encode_extra_output(li)), li);
        }
    }

    #[test]
    fn flags_round_trip_v1() {
        let f = encode_flags_output(0);
        assert_eq!(decode_flags_output(f).unwrap(), 0);
    }

    #[test]
    fn flags_rejects_nonzero_service_id() {
        let f = encode_flags_output(3);
        assert!(matches!(
            decode_flags_output(f),
            Err(OutputFrameError::UnknownServiceId(3))
        ));
    }

    #[test]
    fn output_error_serde_round_trip() {
        let cases = [
            OutputError::Retryable("upstream timeout".to_string()),
            OutputError::Permanent("invalid record".to_string()),
        ];
        for err in cases {
            let bytes = bincode::serde::encode_to_vec(&err, bincode::config::standard())
                .expect("encode");
            let (got, _) = bincode::serde::decode_from_slice::<OutputError, _>(
                &bytes,
                bincode::config::standard(),
            )
            .expect("decode");
            match (err.clone(), got) {
                (OutputError::Retryable(a), OutputError::Retryable(b)) => assert_eq!(a, b),
                (OutputError::Permanent(a), OutputError::Permanent(b)) => assert_eq!(a, b),
                (a, b) => panic!("variant mismatch: {a:?} vs {b:?}"),
            }
        }
    }

    #[test]
    fn msg_type_constants_stable() {
        assert_eq!(MSG_TYPE_OUTPUT, 10);
        assert_eq!(MSG_TYPE_OUTPUT_RESP, 11);
    }
}
