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
pub const OFF_CLIENT_ID: usize = 12; // u32 LE — the submitting client (0 for node-originated frames)
pub const OFF_SEQ: usize = 16; // u32 LE — the client's local sequence (0 for node-originated frames)
pub const OFF_RESERVED1: usize = 20; // u32 — reserved, written as zero
pub const OFF_TIME_NS: usize = 24; // u64 LE — leader-stamped ns since the Unix epoch; non-decreasing along the log

/// Application message; payload = user command bytes.
pub const FRAME_TYPE_MESSAGE: u8 = 1;
/// Wrap padding: `length` spans to the end of the buffer; ONLY the 32-byte
/// header is actually written — the rest of the padded region is stale bytes.
/// Readers and the archive skip it by `length`; replay drops it.
pub const FRAME_TYPE_PADDING: u8 = 2;
/// New-term no-op (spec §6, Raft §5.4.2): a zero-payload frame the new
/// leader appends immediately on opening a term and must see COMMIT before
/// serving. Replicated/archived/replayed like any message frame; the apply
/// layer (M5) applies only MESSAGE frames and TIMER frames addressed to the
/// row.
pub const FRAME_TYPE_NEW_TERM: u8 = 3;
/// Cluster-config entry (M7, spec 2026-07-13): payload =
/// `v2::config::encode_config` bytes. Appended by a serving leader; adopted
/// at append (leader) / at durable recording (follower, archive scan).
/// Replicated/archived/replayed like any frame; the apply layer applies only
/// MESSAGE frames and TIMER frames addressed to the row, so services never
/// see it.
pub const FRAME_TYPE_CONFIG: u8 = 4;
/// Scheduled timer fired by the leader (time-and-timers spec §4.2): a 24-byte
/// body ([`TimerBody`]); `client_id`/`seq` are 0; `time_ns` is the deadline
/// unless the frame is late (`time_ns > deadline_ns`). Delivered to exactly the
/// FSM whose identity hash it names; every other apply loop skips it.
pub const FRAME_TYPE_TIMER: u8 = 5;
/// `flags` bit 0 on a TIMER frame: fired from the replicated schedule table
/// (plan 2), not from a state machine's `schedule` call.
pub const FLAG_TIMER_TABLE: u8 = 0x01;
/// The replicated schedule table (time-and-timers spec §5, plan 2): payload =
/// `v2::schedule::encode_schedule_table` bytes; appended by a serving leader
/// on a verified `schedule_apply` admin request; adopted at append (leader) /
/// at durable recording (follower, archive scan); the apply layer skips it.
pub const FRAME_TYPE_SCHEDULE_TABLE: u8 = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub length: u32,
    pub frame_type: u8,
    pub flags: u8,
    pub leadership_term_id: u32,
    pub client_id: u32,
    pub seq: u32,
    pub time_ns: u64,
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
    buf[OFF_RESERVED0..OFF_RESERVED0 + 2].copy_from_slice(&[0, 0]);
    buf[OFF_TERM_ID..OFF_TERM_ID + 4].copy_from_slice(&h.leadership_term_id.to_le_bytes());
    buf[OFF_CLIENT_ID..OFF_CLIENT_ID + 4].copy_from_slice(&h.client_id.to_le_bytes());
    buf[OFF_SEQ..OFF_SEQ + 4].copy_from_slice(&h.seq.to_le_bytes());
    buf[OFF_RESERVED1..OFF_RESERVED1 + 4].copy_from_slice(&[0, 0, 0, 0]);
    buf[OFF_TIME_NS..OFF_TIME_NS + 8].copy_from_slice(&h.time_ns.to_le_bytes());
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
        leadership_term_id: u32::from_le_bytes(
            buf[OFF_TERM_ID..OFF_TERM_ID + 4].try_into().unwrap(),
        ),
        client_id: u32::from_le_bytes(buf[OFF_CLIENT_ID..OFF_CLIENT_ID + 4].try_into().unwrap()),
        seq: u32::from_le_bytes(buf[OFF_SEQ..OFF_SEQ + 4].try_into().unwrap()),
        time_ns: u64::from_le_bytes(buf[OFF_TIME_NS..OFF_TIME_NS + 8].try_into().unwrap()),
    }
}

/// The TIMER frame body: fixed, 24 bytes, three LE `u64`s.
pub const TIMER_BODY_LEN: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimerBody {
    /// `FsmIdentity::hash()` of the FSM this timer belongs to.
    pub identity_hash: u64,
    /// The FSM's own id for the timer.
    pub timer_id: u64,
    /// What was asked for; compare with the header's `time_ns` for lateness.
    pub deadline_ns: u64,
}

/// Write a [`TimerBody`]. `buf` must be at least [`TIMER_BODY_LEN`] bytes
/// (panics otherwise, like [`write_header_except_length`]).
pub fn write_timer_body(buf: &mut [u8], b: &TimerBody) {
    buf[0..8].copy_from_slice(&b.identity_hash.to_le_bytes());
    buf[8..16].copy_from_slice(&b.timer_id.to_le_bytes());
    buf[16..24].copy_from_slice(&b.deadline_ns.to_le_bytes());
}

/// Total on any input: `None` when shorter than [`TIMER_BODY_LEN`]; longer
/// input is accepted and the tail ignored (a committed frame is trusted; the
/// length check is what keeps this decoder safe on a fuzzed slice).
pub fn read_timer_body(buf: &[u8]) -> Option<TimerBody> {
    if buf.len() < TIMER_BODY_LEN {
        return None;
    }
    let u = |o: usize| u64::from_le_bytes(buf[o..o + 8].try_into().unwrap());
    Some(TimerBody {
        identity_hash: u(0),
        timer_id: u(8),
        deadline_ns: u(16),
    })
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

    /// FROZEN layout (spec §3.1). Never change these.
    #[test]
    fn field_offsets_are_the_relaid_layout() {
        // length(4) type(1) flags(1) rsvd(2) term(4) client_id(4) seq(4) rsvd(4) time_ns(8) = 32
        assert_eq!(OFF_LENGTH, 0);
        assert_eq!(OFF_TYPE, 4);
        assert_eq!(OFF_FLAGS, 5);
        assert_eq!(OFF_RESERVED0, 6);
        assert_eq!(OFF_TERM_ID, 8);
        assert_eq!(OFF_CLIENT_ID, 12);
        assert_eq!(OFF_SEQ, 16);
        assert_eq!(OFF_RESERVED1, 20);
        assert_eq!(OFF_TIME_NS, 24);
        assert_eq!(HEADER_LEN, 32);
        assert_eq!(FRAME_ALIGNMENT, 32);
    }

    #[test]
    fn header_roundtrip_except_length_pins_the_bytes() {
        let mut buf = [0xAAu8; HEADER_LEN];
        let h = FrameHeader {
            length: 0,
            frame_type: FRAME_TYPE_MESSAGE,
            flags: 0x5a,
            leadership_term_id: 7,
            client_id: 0x0102_0304,
            seq: 0x0506_0708,
            time_ns: 0x1122_3344_5566_7788,
        };
        write_header_except_length(&mut buf, &h);
        assert_eq!(
            &buf[0..4],
            &[0xAA; 4],
            "length is the commit word: untouched"
        );
        assert_eq!(&buf[6..8], &[0, 0], "reserved0 written as zero");
        assert_eq!(
            &buf[12..16],
            &[0x04, 0x03, 0x02, 0x01],
            "client_id LE at 12"
        );
        assert_eq!(&buf[16..20], &[0x08, 0x07, 0x06, 0x05], "seq LE at 16");
        assert_eq!(&buf[20..24], &[0, 0, 0, 0], "reserved1 written as zero");
        assert_eq!(
            &buf[24..32],
            &0x1122_3344_5566_7788u64.to_le_bytes(),
            "time_ns LE at 24"
        );
        let out = read_header(&buf);
        assert_eq!(out.frame_type, FRAME_TYPE_MESSAGE);
        assert_eq!(out.flags, 0x5a);
        assert_eq!(out.leadership_term_id, 7);
        assert_eq!(out.client_id, 0x0102_0304);
        assert_eq!(out.seq, 0x0506_0708);
        assert_eq!(out.time_ns, 0x1122_3344_5566_7788);
    }

    #[test]
    fn frame_type_codes_are_stable() {
        assert_eq!(FRAME_TYPE_MESSAGE, 1);
        assert_eq!(FRAME_TYPE_PADDING, 2);
        assert_eq!(FRAME_TYPE_NEW_TERM, 3);
        assert_eq!(FRAME_TYPE_CONFIG, 4);
        assert_eq!(FRAME_TYPE_TIMER, 5);
        assert_eq!(FLAG_TIMER_TABLE, 0x01);
        assert_eq!(FRAME_TYPE_SCHEDULE_TABLE, 6);
    }

    /// FROZEN: the 24-byte TIMER body (spec §4.2).
    #[test]
    fn timer_body_roundtrip_and_short_input_is_none() {
        let b = TimerBody {
            identity_hash: 0xdead_beef_cafe_f00d,
            timer_id: 42,
            deadline_ns: 1_700_000_000_000_000_000,
        };
        let mut buf = [0u8; TIMER_BODY_LEN];
        write_timer_body(&mut buf, &b);
        assert_eq!(&buf[0..8], &b.identity_hash.to_le_bytes());
        assert_eq!(&buf[8..16], &42u64.to_le_bytes());
        assert_eq!(&buf[16..24], &b.deadline_ns.to_le_bytes());
        assert_eq!(read_timer_body(&buf), Some(b));
        assert_eq!(read_timer_body(&buf[..23]), None);
        assert_eq!(read_timer_body(&[]), None);
        let mut longer = [7u8; 40];
        longer[..24].copy_from_slice(&buf);
        assert_eq!(
            read_timer_body(&longer),
            Some(b),
            "trailing bytes are ignored"
        );
    }
}
