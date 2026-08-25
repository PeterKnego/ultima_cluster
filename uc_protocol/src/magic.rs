/// Magic byte sequences. Used to detect catastrophically wrong files.
pub const CNC_MAGIC: [u8; 8] = *b"ULTCNC\0\0";
pub const RING_MAGIC: [u8; 8] = *b"ULTRNG\0\0";
pub const FRAME_MAGIC: [u8; 4] = *b"ULTC";

/// MPSC ring files (M13a). Distinct from [`RING_MAGIC`] because the MPSC
/// per-record-commit protocol reinterprets the slot's first word and the
/// header's `publish_position` — an old-format file mapped by a new binary
/// (or the reverse) would misread every slot, so the attach is refused
/// instead. SPSC and Broadcast keep [`RING_MAGIC`].
pub const RING_MPSC_MAGIC: [u8; 8] = *b"ULTRNG2\0";
