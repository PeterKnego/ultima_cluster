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
/// Header-only (spec §6): `position` = the sender's DURABLE position.
/// Follower → leader, on durable advance (block/fsync granularity) plus a
/// 100 ms floor. Feeds the leader's quorum commit ranking.
pub const DGRAM_KIND_APPEND_POSITION: u8 = 5;
/// Header-only (spec §6): `position` = the cluster COMMIT position (quorum-
/// fsync'd). Leader → followers, on commit advance plus the same floor.
pub const DGRAM_KIND_COMMIT_POSITION: u8 = 6;
/// Body = `RequestVoteBody` (spec §6): candidate solicits a vote for
/// `new_term` carrying its log position credentials. The header's
/// `leadership_term_id` also carries `new_term` (body is authoritative).
pub const DGRAM_KIND_REQUEST_VOTE: u8 = 7;
/// Body = `VoteBody`: the response. Granted votes are PERSISTED by the
/// granter before this datagram is sent (spec §6).
pub const DGRAM_KIND_VOTE: u8 = 8;
/// Body = term-map suffix (count + entries): the leader's term history for
/// follower reconciliation (spec §6). Ships at most
/// `MAX_TERM_MAP_WIRE_ENTRIES` most-recent entries; a follower whose common
/// prefix is older than the suffix falls back to full replay from 0.
pub const DGRAM_KIND_TERM_MAP: u8 = 9;

/// Body = `ReadProbeBody` (M5 §7 linearizable-read barrier): the leader's
/// nonce'd read-index confirmation solicitation. Leader → every follower. The
/// header's `leadership_term_id` carries the leader's current term — a follower
/// ACKs ONLY if that term still equals its own (a stale leader's probe dies
/// there; the no-stale-read theorem's teeth). `position` is unused (zero).
pub const DGRAM_KIND_READ_PROBE: u8 = 10;
/// Body = `ReadProbeBody`: the follower's confirmation. Follower → the probing
/// leader (addressed by the leader id carried in the probe body's `from`).
/// The leader counts DISTINCT ackers per nonce toward its read quorum.
pub const DGRAM_KIND_READ_PROBE_ACK: u8 = 11;

pub const READ_PROBE_BODY_LEN: usize = 16;

/// Read-barrier probe/ack body: a `nonce` scoping the round to one read, plus
/// the sender's own node id in `from` (so the receiver addresses its reply, and
/// the leader attributes each distinct acker). LE: nonce 0..8, from 8..12,
/// 12..16 zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadProbeBody {
    pub nonce: u64,
    pub from: u32,
}

pub fn write_read_probe_body(buf: &mut [u8], b: &ReadProbeBody) {
    buf[0..8].copy_from_slice(&b.nonce.to_le_bytes());
    buf[8..12].copy_from_slice(&b.from.to_le_bytes());
    buf[12..16].fill(0);
}

/// Decode a read-probe body, or `None` if the buffer is shorter than
/// [`READ_PROBE_BODY_LEN`] (the caller drops a malformed datagram). A longer
/// buffer decodes its 16-byte prefix (trailing bytes ignored).
pub fn read_read_probe_body(buf: &[u8]) -> Option<ReadProbeBody> {
    if buf.len() < READ_PROBE_BODY_LEN {
        return None;
    }
    Some(ReadProbeBody {
        nonce: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
        from: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
    })
}

// -- M6 Task 6: snapshot session (kinds 12–15) --------------------------------
// A bounded, strictly-lower-priority unicast file transfer: the leader ships a
// snapshot artifact to ONE peer whose NAK fell below the journal purge floor.
// MDC-free (single peer), NAK-repaired like the main stream, budget-bounded.

/// leader → peer: opens a session. Body = [`SnapBeginBody`]; header `position` = 0.
pub const DGRAM_KIND_SNAP_BEGIN: u8 = 12;
/// leader → peer: one file chunk. Header `position` = the snapshot-FILE offset;
/// payload = the raw bytes at that offset.
pub const DGRAM_KIND_SNAP_CHUNK: u8 = 13;
/// peer → leader: request a missing byte range. Body = [`SnapNakBody`].
pub const DGRAM_KIND_SNAP_NAK: u8 = 14;
/// peer → leader: the file is complete (echoes the [`SnapBeginBody`] as the ack).
pub const DGRAM_KIND_SNAP_DONE: u8 = 15;

pub const SNAP_BEGIN_BODY_LEN: usize = 24;

/// Opens (and, echoed back, acks) a snapshot session. `session` scopes chunk/NAK
/// traffic to one transfer; `snapshot_pos` is the artifact's tag `S`; `total_len`
/// is the file size the receiver pre-sizes its `.part` to. LE: session 0..4,
/// 4..8 zero (u64 alignment for `snapshot_pos`), snapshot_pos 8..16,
/// total_len 16..24.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapBeginBody {
    pub session: u32,
    pub snapshot_pos: u64,
    pub total_len: u64,
}

pub fn write_snap_begin_body(buf: &mut [u8], b: &SnapBeginBody) {
    buf[0..4].copy_from_slice(&b.session.to_le_bytes());
    buf[4..8].fill(0);
    buf[8..16].copy_from_slice(&b.snapshot_pos.to_le_bytes());
    buf[16..24].copy_from_slice(&b.total_len.to_le_bytes());
}

/// Decode a snap-begin body, or `None` if the buffer is shorter than
/// [`SNAP_BEGIN_BODY_LEN`] (the caller drops a malformed datagram).
pub fn read_snap_begin_body(buf: &[u8]) -> Option<SnapBeginBody> {
    if buf.len() < SNAP_BEGIN_BODY_LEN {
        return None;
    }
    Some(SnapBeginBody {
        session: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
        snapshot_pos: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
        total_len: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
    })
}

pub const SNAP_NAK_BODY_LEN: usize = 16;

/// Requests a missing chunk of the snapshot file: `[offset, offset+length)`.
/// `session` scopes it to the active transfer. LE: session 0..4, offset 4..12,
/// length 12..16.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapNakBody {
    pub session: u32,
    pub offset: u64,
    pub length: u32,
}

pub fn write_snap_nak_body(buf: &mut [u8], b: &SnapNakBody) {
    buf[0..4].copy_from_slice(&b.session.to_le_bytes());
    buf[4..12].copy_from_slice(&b.offset.to_le_bytes());
    buf[12..16].copy_from_slice(&b.length.to_le_bytes());
}

/// Decode a snap-NAK body, or `None` if shorter than [`SNAP_NAK_BODY_LEN`].
pub fn read_snap_nak_body(buf: &[u8]) -> Option<SnapNakBody> {
    if buf.len() < SNAP_NAK_BODY_LEN {
        return None;
    }
    Some(SnapNakBody {
        session: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
        offset: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
        length: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
    })
}

pub const REQUEST_VOTE_BODY_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestVoteBody {
    pub new_term: u32,
    pub last_term: u32,
    pub last_durable: u64,
}

pub fn write_request_vote_body(buf: &mut [u8], b: &RequestVoteBody) {
    buf[0..4].copy_from_slice(&b.new_term.to_le_bytes());
    buf[4..8].copy_from_slice(&b.last_term.to_le_bytes());
    buf[8..16].copy_from_slice(&b.last_durable.to_le_bytes());
}

pub fn read_request_vote_body(buf: &[u8]) -> RequestVoteBody {
    RequestVoteBody {
        new_term: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
        last_term: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
        last_durable: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
    }
}

pub const VOTE_BODY_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteBody {
    pub term: u32,
    pub granted: bool,
}

pub fn write_vote_body(buf: &mut [u8], b: &VoteBody) {
    buf[0..4].copy_from_slice(&b.term.to_le_bytes());
    buf[4] = b.granted as u8;
    buf[5..16].fill(0);
}

pub fn read_vote_body(buf: &[u8]) -> VoteBody {
    VoteBody {
        term: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
        granted: buf[4] != 0,
    }
}

pub const TERM_MAP_HEADER_LEN: usize = 8;
pub const TERM_MAP_ENTRY_LEN: usize = 16;
/// 64 × 16 + 8 = 1032 B — fits the 1392 B MTU body budget with room.
pub const MAX_TERM_MAP_WIRE_ENTRIES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TermMapEntryWire {
    pub term: u32,
    pub base: u64,
}

/// Writes header + entries; returns bytes written. `entries.len()` must be
/// ≤ `MAX_TERM_MAP_WIRE_ENTRIES` (caller ships a suffix).
pub fn write_term_map_body(buf: &mut [u8], entries: &[TermMapEntryWire]) -> usize {
    debug_assert!(entries.len() <= MAX_TERM_MAP_WIRE_ENTRIES);
    buf[0..4].copy_from_slice(&(entries.len() as u32).to_le_bytes());
    buf[4..8].copy_from_slice(&0u32.to_le_bytes());
    let mut o = TERM_MAP_HEADER_LEN;
    for e in entries {
        buf[o..o + 4].copy_from_slice(&e.term.to_le_bytes());
        buf[o + 4..o + 8].copy_from_slice(&0u32.to_le_bytes());
        buf[o + 8..o + 16].copy_from_slice(&e.base.to_le_bytes());
        o += TERM_MAP_ENTRY_LEN;
    }
    o
}

/// Returns the entry count read into `out`, or None if malformed (short
/// buffer, count over the cap, or trailing garbage length).
pub fn read_term_map_body(buf: &[u8], out: &mut [TermMapEntryWire]) -> Option<usize> {
    if buf.len() < TERM_MAP_HEADER_LEN {
        return None;
    }
    let count = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    if count > MAX_TERM_MAP_WIRE_ENTRIES || count > out.len() {
        return None;
    }
    if buf.len() != TERM_MAP_HEADER_LEN + count * TERM_MAP_ENTRY_LEN {
        return None;
    }
    let mut o = TERM_MAP_HEADER_LEN;
    for slot in out.iter_mut().take(count) {
        *slot = TermMapEntryWire {
            term: u32::from_le_bytes(buf[o..o + 4].try_into().unwrap()),
            base: u64::from_le_bytes(buf[o + 8..o + 16].try_into().unwrap()),
        };
        o += TERM_MAP_ENTRY_LEN;
    }
    Some(count)
}

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
        // Pin the ABSOLUTE wire layout with a literal LE byte array — this
        // module is frozen once M3 peers speak it, so a write/read round
        // trip alone isn't enough (both sides could agree on a wrong
        // layout). position=0xDEAD_BEEF_0000_0040 -> LE
        // [0x40,0,0,0,0xEF,0xBE,0xAD,0xDE]; term=9 -> [9,0,0,0]; kind=1;
        // flags=0x5a; reserved=[0,0].
        assert_eq!(
            buf,
            [0x40, 0x00, 0x00, 0x00, 0xEF, 0xBE, 0xAD, 0xDE, 9, 0, 0, 0, 1, 0x5a, 0, 0]
        );
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
        // Absolute wire pin, not just internal write/read consistency:
        // position=4096=0x1000 -> LE [0,16,0,0,0,0,0,0];
        // length=65536=0x10000 -> LE [0,0,1,0]; reserved=[0,0,0,0].
        assert_eq!(buf, [0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0]);

        let s = StatusBody { contiguous_position: 1 << 33, receive_window: 1 << 28 };
        let mut buf = [0u8; STATUS_BODY_LEN];
        write_status_body(&mut buf, &s);
        assert_eq!(read_status_body(&buf), s);
        // Absolute wire pin: contiguous_position=1<<33=0x2_0000_0000 -> LE
        // [0,0,0,0,2,0,0,0]; receive_window=1<<28=0x1000_0000 -> LE
        // [0,0,0,16]; reserved=[0,0,0,0].
        assert_eq!(buf, [0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 0]);
    }

    #[test]
    fn kind_codes_are_stable() {
        assert_eq!(DGRAM_KIND_DATA, 1);
        assert_eq!(DGRAM_KIND_HEARTBEAT, 2);
        assert_eq!(DGRAM_KIND_NAK, 3);
        assert_eq!(DGRAM_KIND_STATUS, 4);
        assert_eq!(DGRAM_KIND_APPEND_POSITION, 5);
        assert_eq!(DGRAM_KIND_COMMIT_POSITION, 6);
        assert_eq!(DGRAM_KIND_REQUEST_VOTE, 7);
        assert_eq!(DGRAM_KIND_VOTE, 8);
        assert_eq!(DGRAM_KIND_TERM_MAP, 9);
        assert_eq!(DGRAM_KIND_READ_PROBE, 10);
        assert_eq!(DGRAM_KIND_READ_PROBE_ACK, 11);
        assert_eq!(DGRAM_KIND_SNAP_BEGIN, 12);
        assert_eq!(DGRAM_KIND_SNAP_CHUNK, 13);
        assert_eq!(DGRAM_KIND_SNAP_NAK, 14);
        assert_eq!(DGRAM_KIND_SNAP_DONE, 15);
    }

    #[test]
    fn snap_begin_body_roundtrips_and_pins_layout() {
        let b = SnapBeginBody { session: 0x0A0B_0C0D, snapshot_pos: 0x1000, total_len: 300 * 1024 };
        let mut buf = [0u8; SNAP_BEGIN_BODY_LEN];
        write_snap_begin_body(&mut buf, &b);
        assert_eq!(read_snap_begin_body(&buf), Some(b));
        // session=0x0A0B0C0D -> LE [0x0D,0x0C,0x0B,0x0A]; pad [0,0,0,0];
        // snapshot_pos=0x1000 -> LE [0,0x10,0,0,0,0,0,0];
        // total_len=307200=0x0004_B000 -> LE [0x00,0xB0,0x04,0,0,0,0,0].
        assert_eq!(
            buf,
            [
                0x0D, 0x0C, 0x0B, 0x0A, 0, 0, 0, 0, 0x00, 0x10, 0, 0, 0, 0, 0, 0, 0x00, 0xB0,
                0x04, 0, 0, 0, 0, 0
            ]
        );
        // Short buffer is rejected (caller drops the datagram).
        assert_eq!(read_snap_begin_body(&buf[..SNAP_BEGIN_BODY_LEN - 1]), None);
    }

    #[test]
    fn snap_nak_body_roundtrips_and_pins_layout() {
        let b = SnapNakBody { session: 0x0102_0304, offset: 0x1_0000, length: 1408 };
        let mut buf = [0u8; SNAP_NAK_BODY_LEN];
        write_snap_nak_body(&mut buf, &b);
        assert_eq!(read_snap_nak_body(&buf), Some(b));
        // session=0x01020304 -> LE [0x04,0x03,0x02,0x01];
        // offset=0x10000 -> LE [0,0,1,0,0,0,0,0]; length=1408=0x0580 -> LE [0x80,0x05,0,0].
        assert_eq!(
            buf,
            [0x04, 0x03, 0x02, 0x01, 0, 0, 1, 0, 0, 0, 0, 0, 0x80, 0x05, 0, 0]
        );
        assert_eq!(read_snap_nak_body(&buf[..SNAP_NAK_BODY_LEN - 1]), None);
    }

    #[test]
    fn vote_bodies_roundtrip_and_pin_layout() {
        let rv = RequestVoteBody { new_term: 7, last_term: 6, last_durable: 0x0000_0001_0000_0040 };
        let mut buf = [0u8; REQUEST_VOTE_BODY_LEN];
        write_request_vote_body(&mut buf, &rv);
        assert_eq!(read_request_vote_body(&buf), rv);
        // literal LE pin: new_term 7, last_term 6, last_durable 2^32+64
        assert_eq!(buf, [7, 0, 0, 0, 6, 0, 0, 0, 0x40, 0, 0, 0, 1, 0, 0, 0]);

        let v = VoteBody { term: 7, granted: true };
        let mut buf = [0u8; VOTE_BODY_LEN];
        write_vote_body(&mut buf, &v);
        assert_eq!(read_vote_body(&buf), v);
        assert_eq!(buf, [7, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let v = VoteBody { term: 7, granted: false };
        write_vote_body(&mut buf, &v);
        assert_eq!(buf[4], 0);
    }

    #[test]
    fn read_probe_body_roundtrips_and_pins_layout() {
        let b = ReadProbeBody { nonce: 0x0102_0304_0506_0708, from: 0x0A0B_0C0D };
        let mut buf = [0u8; READ_PROBE_BODY_LEN];
        write_read_probe_body(&mut buf, &b);
        assert_eq!(read_read_probe_body(&buf), Some(b));
        // Absolute LE wire pin (not just a round trip — both sides could agree
        // on a swapped field order): nonce 0x0102030405060708 -> LE bytes
        // 0..8; from 0x0A0B0C0D -> LE bytes 8..12; bytes 12..16 zero.
        assert_eq!(
            buf,
            [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x0D, 0x0C, 0x0B, 0x0A, 0, 0, 0, 0]
        );
        // A longer buffer decodes the same 16-byte prefix (trailing bytes ignored).
        let mut long = [0xFFu8; READ_PROBE_BODY_LEN + 8];
        write_read_probe_body(&mut long[..READ_PROBE_BODY_LEN], &b);
        assert_eq!(read_read_probe_body(&long), Some(b));
        // Malformed: any buffer shorter than the 16-byte body -> None (must not
        // panic on slicing). Pin every boundary below the length.
        for short in 0..READ_PROBE_BODY_LEN {
            assert!(read_read_probe_body(&buf[..short]).is_none(), "len {short} must reject");
        }
        assert!(read_read_probe_body(&[]).is_none());
        // The two new kind codes are stable.
        assert_eq!(DGRAM_KIND_READ_PROBE, 10);
        assert_eq!(DGRAM_KIND_READ_PROBE_ACK, 11);
    }

    #[test]
    fn term_map_body_roundtrips_and_pins_layout() {
        let entries =
            [TermMapEntryWire { term: 1, base: 0 }, TermMapEntryWire { term: 3, base: 4096 }];
        let mut buf = [0u8; TERM_MAP_HEADER_LEN + 2 * TERM_MAP_ENTRY_LEN];
        let n = write_term_map_body(&mut buf, &entries);
        assert_eq!(n, 8 + 32);
        let mut out = [TermMapEntryWire { term: 0, base: 0 }; MAX_TERM_MAP_WIRE_ENTRIES];
        let m = read_term_map_body(&buf[..n], &mut out).expect("well-formed");
        assert_eq!(&out[..m], &entries);
        // literal pin: count 2, reserved 0, entry0 {1, rsvd, base 0}, entry1 {3, rsvd, base 4096}
        assert_eq!(
            &buf[..n],
            &[
                2, 0, 0, 0, 0, 0, 0, 0, // count + reserved
                1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // term 1, base 0
                3, 0, 0, 0, 0, 0, 0, 0, 0, 0x10, 0, 0, 0, 0, 0, 0, // term 3, base 4096
            ][..]
        );
        // malformed: truncated entry -> None
        assert!(read_term_map_body(&buf[..n - 1], &mut out).is_none());
        // malformed: count beyond the cap -> None
        let mut big = [0u8; TERM_MAP_HEADER_LEN];
        big[0..4].copy_from_slice(&(MAX_TERM_MAP_WIRE_ENTRIES as u32 + 1).to_le_bytes());
        assert!(read_term_map_body(&big, &mut out).is_none());
        // malformed: buffer shorter than the 8-byte header -> None (the
        // third documented failure mode; must not panic on slicing)
        assert!(read_term_map_body(&buf[..TERM_MAP_HEADER_LEN - 1], &mut out).is_none());
        assert!(read_term_map_body(&[], &mut out).is_none());
    }
}
