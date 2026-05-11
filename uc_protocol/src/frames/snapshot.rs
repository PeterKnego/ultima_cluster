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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msg_type_constants_are_stable() {
        // These discriminants are part of the wire protocol — bumping them
        // requires a protocol version bump and a coordinated SDK release.
        assert_eq!(MSG_TYPE_BUILD_SNAPSHOT, 100);
        assert_eq!(MSG_TYPE_SNAPSHOT_BUILT, 101);
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
