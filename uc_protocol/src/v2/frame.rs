// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Log-buffer frame layout (spec §4). Core-only: layout constants and
//! (de)serialization over byte slices. The `length` field at offset 0 is the
//! commit word: written LAST with a release store, read with an acquire load,
//! `0` = frame not yet committed. Those atomic ops live in the runtime crate
//! (`uc_log`) — this module never touches atomics so it stays `core`-only.

/// Every frame starts on a 32-byte boundary; frame slots are padded up to it.
pub const FRAME_ALIGNMENT: usize = 32;
/// Fixed header size; payload follows immediately.
pub const HEADER_LEN: usize = 32;

pub const OFF_LENGTH: usize = 0; // u32 LE — TOTAL frame length (header + payload); 0 = uncommitted
pub const OFF_TYPE: usize = 4; // u8
pub const OFF_FLAGS: usize = 5; // u8
pub const OFF_RESERVED0: usize = 6; // u16 — reserved, written as zero
pub const OFF_TERM_ID: usize = 8; // u32 LE — leadership_term_id
pub const OFF_RESERVED1: usize = 12; // u32 — reserved, written as zero
pub const OFF_SESSION_ID: usize = 16; // u64 LE
pub const OFF_CORRELATION_ID: usize = 24; // u64 LE

/// Application message; payload = user command bytes.
pub const FRAME_TYPE_MESSAGE: u8 = 1;
/// Wrap padding: `length` spans to the end of the buffer; ONLY the 32-byte
/// header is actually written — the rest of the padded region is stale bytes.
/// Readers and the archive skip it by `length`; replay drops it.
pub const FRAME_TYPE_PADDING: u8 = 2;
/// New-term no-op (spec §6, Raft §5.4.2): a zero-payload frame the new
/// leader appends immediately on opening a term and must see COMMIT before
/// serving. Replicated/archived/replayed like any message frame; the apply
/// layer (M5) skips every non-MESSAGE type.
pub const FRAME_TYPE_NEW_TERM: u8 = 3;
/// Cluster-config entry (M7, spec 2026-07-13): payload =
/// `v2::config::encode_config` bytes. Appended by a serving leader; adopted
/// at append (leader) / at durable recording (follower, archive scan).
/// Replicated/archived/replayed like any frame; the apply layer skips every
/// non-MESSAGE type, so services never see it.
pub const FRAME_TYPE_CONFIG: u8 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub length: u32,
    pub frame_type: u8,
    pub flags: u8,
    pub leadership_term_id: u32,
    pub session_id: u64,
    pub correlation_id: u64,
}

/// Round a total frame length up to the 32-byte slot size.
#[inline]
pub const fn align_frame_len(total: usize) -> usize {
    (total + FRAME_ALIGNMENT - 1) & !(FRAME_ALIGNMENT - 1)
}

/// Write every header field EXCEPT `length` (the commit word — the runtime
/// stores it atomically, last). `buf` must be at least `HEADER_LEN` bytes.
pub fn write_header_except_length(buf: &mut [u8], h: &FrameHeader) {
    buf[OFF_TYPE] = h.frame_type;
    buf[OFF_FLAGS] = h.flags;
    buf[OFF_RESERVED0..OFF_RESERVED0 + 2].copy_from_slice(&0u16.to_le_bytes());
    buf[OFF_TERM_ID..OFF_TERM_ID + 4].copy_from_slice(&h.leadership_term_id.to_le_bytes());
    buf[OFF_RESERVED1..OFF_RESERVED1 + 4].copy_from_slice(&0u32.to_le_bytes());
    buf[OFF_SESSION_ID..OFF_SESSION_ID + 8].copy_from_slice(&h.session_id.to_le_bytes());
    buf[OFF_CORRELATION_ID..OFF_CORRELATION_ID + 8].copy_from_slice(&h.correlation_id.to_le_bytes());
}

/// Parse a header from a committed frame. The caller must already have
/// observed `length != 0` via an acquire load (or hold the buffer's
/// single-writer/contiguity guarantees); this function does plain reads.
///
/// **Deliberately NOT total on `&[u8]`, unlike the `v2::datagram` readers**
/// (M12d ruling). This is the apply thread's innermost hot path, called once
/// per committed frame, and its input is never network bytes: the caller has
/// already observed a non-zero length through an acquire load on a buffer it
/// knows holds `HEADER_LEN` readable bytes, which is a stronger precondition
/// than a length compare here could re-establish. `buf` shorter than
/// [`HEADER_LEN`] is a caller bug, and panicking is the correct fail-stop.
/// The `uc_protocol_log_frame` fuzz target reproduces the real caller's
/// guard (`len >= HEADER_LEN`) rather than removing it.
pub fn read_header(buf: &[u8]) -> FrameHeader {
    FrameHeader {
        length: u32::from_le_bytes(buf[OFF_LENGTH..OFF_LENGTH + 4].try_into().unwrap()),
        frame_type: buf[OFF_TYPE],
        flags: buf[OFF_FLAGS],
        leadership_term_id: u32::from_le_bytes(buf[OFF_TERM_ID..OFF_TERM_ID + 4].try_into().unwrap()),
        session_id: u64::from_le_bytes(buf[OFF_SESSION_ID..OFF_SESSION_ID + 8].try_into().unwrap()),
        correlation_id: u64::from_le_bytes(
            buf[OFF_CORRELATION_ID..OFF_CORRELATION_ID + 8].try_into().unwrap(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alignment_math() {
        assert_eq!(align_frame_len(32), 32);
        assert_eq!(align_frame_len(33), 64);
        assert_eq!(align_frame_len(96), 96);
        assert_eq!(align_frame_len(97), 128);
        // 64 B payload + 32 B header = 96 B on the wire (spec §4 / anatomy doc)
        assert_eq!(align_frame_len(HEADER_LEN + 64), 96);
    }

    #[test]
    fn header_roundtrip_except_length() {
        let h = FrameHeader {
            length: 0, // not written by write_header_except_length
            frame_type: FRAME_TYPE_MESSAGE,
            flags: 0x5a,
            leadership_term_id: 7,
            session_id: 0x1122_3344_5566_7788,
            correlation_id: 42,
        };
        let mut buf = [0u8; HEADER_LEN];
        write_header_except_length(&mut buf, &h);
        // length bytes untouched (commit word is written atomically elsewhere, last)
        assert_eq!(&buf[OFF_LENGTH..OFF_LENGTH + 4], &[0, 0, 0, 0]);
        // simulate the runtime's commit-word store
        buf[OFF_LENGTH..OFF_LENGTH + 4].copy_from_slice(&(HEADER_LEN as u32 + 64).to_le_bytes());
        let out = read_header(&buf);
        assert_eq!(out.length, HEADER_LEN as u32 + 64);
        assert_eq!(out.frame_type, FRAME_TYPE_MESSAGE);
        assert_eq!(out.flags, 0x5a);
        assert_eq!(out.leadership_term_id, 7);
        assert_eq!(out.session_id, 0x1122_3344_5566_7788);
        assert_eq!(out.correlation_id, 42);
    }

    #[test]
    fn frame_type_codes_are_stable() {
        assert_eq!(FRAME_TYPE_MESSAGE, 1);
        assert_eq!(FRAME_TYPE_PADDING, 2);
        assert_eq!(FRAME_TYPE_NEW_TERM, 3);
        assert_eq!(FRAME_TYPE_CONFIG, 4);
    }

    #[test]
    fn field_offsets_do_not_overlap() {
        // layout: length(4) type(1) flags(1) rsvd(2) term(4) rsvd(4) session(8) correlation(8) = 32
        assert_eq!(OFF_LENGTH, 0);
        assert_eq!(OFF_TYPE, 4);
        assert_eq!(OFF_FLAGS, 5);
        assert_eq!(OFF_TERM_ID, 8);
        assert_eq!(OFF_SESSION_ID, 16);
        assert_eq!(OFF_CORRELATION_ID, 24);
        assert_eq!(HEADER_LEN, 32);
        assert_eq!(FRAME_ALIGNMENT, 32);
    }
}
