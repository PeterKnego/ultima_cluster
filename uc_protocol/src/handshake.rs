//! Handshake frame types — sent over cnc.dat's control rings.
//!
//! Service → node (over `control_to_node`):
//!   * `200` — `ServiceReady { last_applied }`
//!
//! Node → service (over `control_to_service`):
//!   * `201` — `RoleChanged { role }`

use crate::cnc::node_role;

pub const MSG_TYPE_SERVICE_READY: u16 = 200;
pub const MSG_TYPE_ROLE_CHANGED: u16 = 201;

/// `header_extra` for `ServiceReady`: bytes 0..8 = service's `last_applied`
/// (u64 LE). The empty payload distinguishes this frame from `ApplyFrame`.
#[inline]
pub fn encode_extra_service_ready(last_applied: u64) -> [u8; 8] {
    last_applied.to_le_bytes()
}

#[inline]
pub fn decode_extra_service_ready(extra: [u8; 8]) -> u64 {
    u64::from_le_bytes(extra)
}

#[derive(Debug, thiserror::Error)]
pub enum RoleChangedError {
    #[error("unknown node role byte: {0}")]
    UnknownRole(u8),
}

/// `header_extra` for `RoleChanged`: byte 0 = role (see
/// `cnc::node_role::*`). Bytes 1..8 reserved (zero).
#[inline]
pub fn encode_extra_role_changed(role: u32) -> [u8; 8] {
    let mut out = [0u8; 8];
    out[0] = role as u8;
    out
}

#[inline]
pub fn decode_extra_role_changed(extra: [u8; 8]) -> Result<u32, RoleChangedError> {
    match extra[0] as u32 {
        r @ (node_role::INIT
        | node_role::FOLLOWER
        | node_role::CANDIDATE
        | node_role::LEADER
        | node_role::SHUTTING) => Ok(r),
        _ => Err(RoleChangedError::UnknownRole(extra[0])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_ready_round_trip() {
        for la in [0u64, 1, 42, 1 << 40, u64::MAX] {
            assert_eq!(decode_extra_service_ready(encode_extra_service_ready(la)), la);
        }
    }

    #[test]
    fn role_changed_round_trip_each_role() {
        for role in [
            node_role::INIT,
            node_role::FOLLOWER,
            node_role::CANDIDATE,
            node_role::LEADER,
            node_role::SHUTTING,
        ] {
            let extra = encode_extra_role_changed(role);
            assert_eq!(decode_extra_role_changed(extra).unwrap(), role);
            assert_eq!(&extra[1..], &[0; 7]); // reserved bytes zero
        }
    }

    #[test]
    fn role_changed_rejects_unknown() {
        let mut extra = encode_extra_role_changed(node_role::FOLLOWER);
        extra[0] = 99;
        assert!(matches!(
            decode_extra_role_changed(extra),
            Err(RoleChangedError::UnknownRole(99))
        ));
    }
}
