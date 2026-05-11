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
