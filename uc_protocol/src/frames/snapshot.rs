//! Snapshot control frames — flow over the cnc control rings, NOT the
//! service↔node SPSC rings.
//!
//! M3 uses these for the build/install handshake; the actual snapshot bytes
//! flow via the existing M2 path (`Cursor<Vec<u8>>` + openraft's
//! `InstallSnapshot` RPC over QUIC). M5 swaps to a `snapshot.region` mmap.

/// node → service: "please build a snapshot at your current `last_applied`."
pub const MSG_TYPE_BUILD_SNAPSHOT: u16 = 100;

/// service → node: "snapshot ready, encoded log_index in header_extra."
pub const MSG_TYPE_SNAPSHOT_BUILT: u16 = 101;

/// `header_extra` for `SnapshotBuilt`: bytes 0..8 = log_index (u64 LE).
#[inline]
pub fn encode_extra_snapshot_built(log_index: u64) -> [u8; 8] {
    log_index.to_le_bytes()
}

#[inline]
pub fn decode_extra_snapshot_built(extra: [u8; 8]) -> u64 {
    u64::from_le_bytes(extra)
}

/// node → service: "install the snapshot now in `snapshot.region`." `header_extra`
/// carries the snapshot's last_log_id index (the index the service will be at after).
pub const MSG_TYPE_INSTALL_SNAPSHOT: u16 = 102;
/// service → node: "snapshot installed." `header_extra` carries the new last_applied.
pub const MSG_TYPE_SNAPSHOT_INSTALLED: u16 = 103;

#[inline]
pub fn encode_extra_install_snapshot(snapshot_index: u64) -> [u8; 8] {
    snapshot_index.to_le_bytes()
}
#[inline]
pub fn decode_extra_install_snapshot(extra: [u8; 8]) -> u64 {
    u64::from_le_bytes(extra)
}
#[inline]
pub fn encode_extra_snapshot_installed(new_last_applied: u64) -> [u8; 8] {
    new_last_applied.to_le_bytes()
}
#[inline]
pub fn decode_extra_snapshot_installed(extra: [u8; 8]) -> u64 {
    u64::from_le_bytes(extra)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_extra_round_trip() {
        assert_eq!(
            decode_extra_install_snapshot(encode_extra_install_snapshot(42)),
            42
        );
        assert_eq!(
            decode_extra_snapshot_installed(encode_extra_snapshot_installed(99)),
            99
        );
    }

    #[test]
    fn msg_type_constants_are_stable() {
        // These discriminants are part of the wire protocol — bumping them
        // requires a protocol version bump and a coordinated SDK release.
        assert_eq!(MSG_TYPE_BUILD_SNAPSHOT, 100);
        assert_eq!(MSG_TYPE_SNAPSHOT_BUILT, 101);
        assert_eq!(MSG_TYPE_INSTALL_SNAPSHOT, 102);
        assert_eq!(MSG_TYPE_SNAPSHOT_INSTALLED, 103);
    }

    #[test]
    fn snapshot_built_round_trip() {
        for li in [0u64, 1, 42, 1 << 40, u64::MAX] {
            assert_eq!(
                decode_extra_snapshot_built(encode_extra_snapshot_built(li)),
                li
            );
        }
    }

    #[test]
    fn snapshot_built_is_little_endian() {
        // Pin the wire byte order so a cross-architecture/cross-language SDK
        // implementation can rely on it.
        assert_eq!(encode_extra_snapshot_built(0x01), [1, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(
            encode_extra_snapshot_built(0x0102_0304_0506_0708),
            [8, 7, 6, 5, 4, 3, 2, 1]
        );
    }
}
