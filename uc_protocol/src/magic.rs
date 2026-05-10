/// Magic byte sequences. Used to detect catastrophically wrong files.
pub const CNC_MAGIC: [u8; 8] = *b"ULTCNC\0\0";
pub const RING_MAGIC: [u8; 8] = *b"ULTRNG\0\0";
pub const FRAME_MAGIC: [u8; 4] = *b"ULTC";
