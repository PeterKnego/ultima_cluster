// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Replication datagram layout (spec §5). Core-only, like `v2::frame`.
//!
//! Every UDP datagram starts with this 16-byte header. DATA datagrams are
//! **self-locating**: `position` is the absolute stream position of the first
//! payload byte, and the payload is a run of complete, offset-contiguous
//! frames (a padding frame, if present, is last and sent header-only).
//! HEARTBEAT carries the leader's append position in `position` (liveness +
//! tail-loss detection) and has no payload. NAK and STATUS carry fixed-size
//! little-endian bodies. One UDP socket per node carries everything (control
//! rides the same socket, demuxed by `kind`).

/// Fixed datagram header size; payload (if any) follows immediately.
pub const DATAGRAM_HEADER_LEN: usize = 16;
/// Default datagram budget (spec §5); jumbo-frame deployments raise it.
pub const MTU_DEFAULT: usize = 1408;

pub const OFF_DGRAM_POSITION: usize = 0; // u64 LE — meaning depends on kind
pub const OFF_DGRAM_TERM_ID: usize = 8; // u32 LE — leadership_term_id
pub const OFF_DGRAM_KIND: usize = 12; // u8
pub const OFF_DGRAM_FLAGS: usize = 13; // u8
pub const OFF_DGRAM_RESERVED: usize = 14; // u16 — zero; future per-datagram PSK-MAC slot

/// Payload = run of complete frames starting at `position`.
pub const DGRAM_KIND_DATA: u8 = 1;
/// No payload; `position` = sender's append position.
pub const DGRAM_KIND_HEARTBEAT: u8 = 2;
/// Payload = `NakBody`.
pub const DGRAM_KIND_NAK: u8 = 3;
/// Payload = `StatusBody`.
pub const DGRAM_KIND_STATUS: u8 = 4;
// 5..=8 reserved: APPEND_POSITION, COMMIT_POSITION, REQUEST_VOTE, VOTE (M3/M4).

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatagramHeader {
    pub position: u64,
    pub leadership_term_id: u32,
    pub kind: u8,
    pub flags: u8,
}

/// `buf` must be at least `DATAGRAM_HEADER_LEN` bytes.
pub fn write_datagram_header(buf: &mut [u8], h: &DatagramHeader) {
    buf[OFF_DGRAM_POSITION..OFF_DGRAM_POSITION + 8].copy_from_slice(&h.position.to_le_bytes());
    buf[OFF_DGRAM_TERM_ID..OFF_DGRAM_TERM_ID + 4]
        .copy_from_slice(&h.leadership_term_id.to_le_bytes());
    buf[OFF_DGRAM_KIND] = h.kind;
    buf[OFF_DGRAM_FLAGS] = h.flags;
    buf[OFF_DGRAM_RESERVED..OFF_DGRAM_RESERVED + 2].copy_from_slice(&0u16.to_le_bytes());
}

/// `buf` must be at least `DATAGRAM_HEADER_LEN` bytes.
pub fn read_datagram_header(buf: &[u8]) -> DatagramHeader {
    DatagramHeader {
        position: u64::from_le_bytes(buf[OFF_DGRAM_POSITION..OFF_DGRAM_POSITION + 8].try_into().unwrap()),
        leadership_term_id: u32::from_le_bytes(
            buf[OFF_DGRAM_TERM_ID..OFF_DGRAM_TERM_ID + 4].try_into().unwrap(),
        ),
        kind: buf[OFF_DGRAM_KIND],
        flags: buf[OFF_DGRAM_FLAGS],
    }
}

/// NAK: "retransmit `length` bytes from `position`" (position is the
/// receiver's contiguous frontier — always a frame start).
pub const NAK_BODY_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NakBody {
    pub position: u64,
    pub length: u32,
}

pub fn write_nak_body(buf: &mut [u8], b: &NakBody) {
    buf[0..8].copy_from_slice(&b.position.to_le_bytes());
    buf[8..12].copy_from_slice(&b.length.to_le_bytes());
    buf[12..16].copy_from_slice(&0u32.to_le_bytes());
}

pub fn read_nak_body(buf: &[u8]) -> NakBody {
    NakBody {
        position: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
        length: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
    }
}

/// Status: flow-control advert (spec §5) — contiguous-rebuilt position +
/// receive window (bytes the receiver can still accept beyond it: its own
/// archive gate, `durable + capacity − contiguous`; capacity ≤ 2^31 so it
/// fits u32).
pub const STATUS_BODY_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusBody {
    pub contiguous_position: u64,
    pub receive_window: u32,
}

pub fn write_status_body(buf: &mut [u8], b: &StatusBody) {
    buf[0..8].copy_from_slice(&b.contiguous_position.to_le_bytes());
    buf[8..12].copy_from_slice(&b.receive_window.to_le_bytes());
    buf[12..16].copy_from_slice(&0u32.to_le_bytes());
}

pub fn read_status_body(buf: &[u8]) -> StatusBody {
    StatusBody {
        contiguous_position: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
        receive_window: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip_and_offsets() {
        let h = DatagramHeader {
            position: 0xDEAD_BEEF_0000_0040,
            leadership_term_id: 9,
            kind: DGRAM_KIND_DATA,
            flags: 0x5a,
        };
        let mut buf = [0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(&mut buf, &h);
        assert_eq!(read_datagram_header(&buf), h);
        // reserved slot stays zero (future per-datagram PSK-MAC home, spec §5)
        assert_eq!(&buf[OFF_DGRAM_RESERVED..OFF_DGRAM_RESERVED + 2], &[0, 0]);
        // layout: position(8) term(4) kind(1) flags(1) reserved(2) = 16
        assert_eq!(OFF_DGRAM_POSITION, 0);
        assert_eq!(OFF_DGRAM_TERM_ID, 8);
        assert_eq!(OFF_DGRAM_KIND, 12);
        assert_eq!(OFF_DGRAM_FLAGS, 13);
        assert_eq!(OFF_DGRAM_RESERVED, 14);
        assert_eq!(DATAGRAM_HEADER_LEN, 16);
    }

    #[test]
    fn control_bodies_roundtrip() {
        let n = NakBody { position: 4096, length: 65536 };
        let mut buf = [0u8; NAK_BODY_LEN];
        write_nak_body(&mut buf, &n);
        assert_eq!(read_nak_body(&buf), n);

        let s = StatusBody { contiguous_position: 1 << 33, receive_window: 1 << 28 };
        let mut buf = [0u8; STATUS_BODY_LEN];
        write_status_body(&mut buf, &s);
        assert_eq!(read_status_body(&buf), s);
    }

    #[test]
    fn kind_codes_are_stable() {
        assert_eq!(DGRAM_KIND_DATA, 1);
        assert_eq!(DGRAM_KIND_HEARTBEAT, 2);
        assert_eq!(DGRAM_KIND_NAK, 3);
        assert_eq!(DGRAM_KIND_STATUS, 4);
    }
}
