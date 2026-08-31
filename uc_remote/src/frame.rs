// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The remote protocol v1 frame codec: a fixed 24-byte header followed by a
//! type-specific payload. All integers are little-endian; string fields are
//! a `u16` length prefix followed by UTF-8 bytes. See
//! `docs/reference/remote-protocol.md` and design spec §4.2.
//!
//! A handshake is refused with one of the `HELLO_REFUSED_*` reasons:
//! [`HELLO_REFUSED_APP_ID`] and [`HELLO_REFUSED_VERSION`] are the client's
//! problem (another member would answer the same way), while
//! [`HELLO_REFUSED_FAULTED`] and [`HELLO_REFUSED_BUSY`] are the *edge's* — it
//! has taken itself out of service, or is at its connection ceiling, and
//! another member is worth trying.

pub use crate::error::FrameError;

/// The wire protocol version this crate speaks.
pub const PROTOCOL_VERSION: u16 = 1;

/// `len u32 | ty u8 | flags u8 | version u16 | client_id u64 | seq u64`.
pub const HEADER_LEN: usize = 24;

/// Refuse anything larger than this at the door.
pub const MAX_FRAME_LEN: u32 = 1 << 20;

// ---- flags ------------------------------------------------------------

/// QUERY: linearizable (else snapshot read).
pub const FLAG_LINEARIZABLE: u8 = 0x01;
/// RESPONSE answers a QUERY.
pub const FLAG_IS_QUERY: u8 = 0x02;
/// RESPONSE: a Sessioned replay.
pub const FLAG_REPLAYED: u8 = 0x04;
/// RESPONSE: a Sessioned expiry (no payload).
pub const FLAG_EXPIRED: u8 = 0x08;
/// RESPONSE: a session_envelope tag was present.
pub const FLAG_ENVELOPED: u8 = 0x10;

// ---- RETRY reasons ------------------------------------------------------

pub const RETRY_NOT_SERVING: u8 = 1;
pub const RETRY_INSTANCE_RESTART: u8 = 2;
pub const RETRY_SERVICE_UNAVAILABLE: u8 = 3;
/// A SUBMIT payload exceeded the node's `max_payload`; the client must not
/// resend. An edge answers this with `retry_after_us: 0`.
pub const RETRY_PAYLOAD_TOO_LARGE: u8 = 4;

// ---- HELLO_REFUSED reasons ----------------------------------------------

pub const HELLO_REFUSED_APP_ID: u8 = 1;
pub const HELLO_REFUSED_VERSION: u8 = 2;
/// The edge is faulted and will not serve again until it is restarted (today:
/// the node's shmem instance restarted underneath it, so its `Engine` attach
/// is void). Unlike the other two reasons this says nothing about the client's
/// request — a *different* member's edge may well be healthy.
pub const HELLO_REFUSED_FAULTED: u8 = 3;
/// The edge is at its `max_connections` ceiling and will not accept another
/// client right now. Like [`HELLO_REFUSED_FAULTED`] this is about the edge,
/// not the client, so a client with more than one member should try the next
/// one; unlike it, the condition is transient — this edge may well accept the
/// same client a moment later, once a connection closes.
pub const HELLO_REFUSED_BUSY: u8 = 4;

/// The frame's type byte, one per row of the design spec's frame table.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FrameType {
    Hello = 1,
    HelloOk = 2,
    HelloRefused = 3,
    Submit = 4,
    Query = 5,
    Response = 6,
    Status = 7,
    Redirect = 8,
    Retry = 9,
    Unknown = 10,
    LeaderChanged = 11,
    Ping = 12,
    Pong = 13,
}

impl FrameType {
    fn from_u8(b: u8) -> Result<Self, FrameError> {
        Ok(match b {
            1 => FrameType::Hello,
            2 => FrameType::HelloOk,
            3 => FrameType::HelloRefused,
            4 => FrameType::Submit,
            5 => FrameType::Query,
            6 => FrameType::Response,
            7 => FrameType::Status,
            8 => FrameType::Redirect,
            9 => FrameType::Retry,
            10 => FrameType::Unknown,
            11 => FrameType::LeaderChanged,
            12 => FrameType::Ping,
            13 => FrameType::Pong,
            other => return Err(FrameError::BadType(other)),
        })
    }
}

/// The 24-byte frame header.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Header {
    pub ty: FrameType,
    pub flags: u8,
    pub version: u16,
    pub client_id: u64,
    pub seq: u64,
}

/// The 24 header bytes for a frame of `payload_len` bytes, as a stack array —
/// so a caller can copy the header and the payload into a ring (or an
/// `iovec`) without an intermediate `Vec`. `encode_frame` is this plus the
/// payload.
pub fn encode_header_into(h: Header, payload_len: usize) -> [u8; HEADER_LEN] {
    let len = (HEADER_LEN + payload_len) as u32;
    let mut out = [0u8; HEADER_LEN];
    out[0..4].copy_from_slice(&len.to_le_bytes());
    out[4] = h.ty as u8;
    out[5] = h.flags;
    out[6..8].copy_from_slice(&h.version.to_le_bytes());
    out[8..16].copy_from_slice(&h.client_id.to_le_bytes());
    out[16..24].copy_from_slice(&h.seq.to_le_bytes());
    out
}

/// Append one encoded frame (header + payload) to `out`.
///
/// Callers must reject an oversized payload (`HEADER_LEN + payload.len() >
/// MAX_FRAME_LEN`) before calling this — the edge answers oversized SUBMITs
/// with `RETRY_PAYLOAD_TOO_LARGE`, it does not truncate or split them here.
pub fn encode_frame(out: &mut Vec<u8>, h: Header, payload: &[u8]) {
    let len = HEADER_LEN + payload.len();
    debug_assert!(
        len <= MAX_FRAME_LEN as usize,
        "encode_frame: frame of {len} bytes exceeds MAX_FRAME_LEN ({MAX_FRAME_LEN}); caller must reject oversized payloads before calling encode_frame"
    );
    out.reserve(len);
    out.extend_from_slice(&encode_header_into(h, payload.len()));
    out.extend_from_slice(payload);
}

/// Decode the header from the front of `buf`. Returns the header and the
/// payload length. Requires `buf.len() >= HEADER_LEN`.
pub fn decode_header(buf: &[u8]) -> Result<(Header, usize), FrameError> {
    if buf.len() < HEADER_LEN {
        return Err(FrameError::Short {
            need: HEADER_LEN,
            have: buf.len(),
        });
    }
    let len = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    if len > MAX_FRAME_LEN {
        return Err(FrameError::TooLong(len));
    }
    if (len as usize) < HEADER_LEN {
        return Err(FrameError::Short {
            need: HEADER_LEN,
            have: len as usize,
        });
    }
    let ty = FrameType::from_u8(buf[4])?;
    let flags = buf[5];
    let version = u16::from_le_bytes(buf[6..8].try_into().unwrap());
    let client_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    let seq = u64::from_le_bytes(buf[16..24].try_into().unwrap());
    Ok((
        Header {
            ty,
            flags,
            version,
            client_id,
            seq,
        },
        len as usize - HEADER_LEN,
    ))
}

// ---- string helper --------------------------------------------------------

fn encode_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

/// Decode a `u16` length-prefixed UTF-8 string from the front of `buf`.
/// Returns the string and the number of bytes consumed.
fn decode_str(buf: &[u8]) -> Result<(&str, usize), FrameError> {
    if buf.len() < 2 {
        return Err(FrameError::Short {
            need: 2,
            have: buf.len(),
        });
    }
    let slen = u16::from_le_bytes(buf[0..2].try_into().unwrap()) as usize;
    let need = 2 + slen;
    if buf.len() < need {
        return Err(FrameError::Short {
            need,
            have: buf.len(),
        });
    }
    let s = std::str::from_utf8(&buf[2..need]).map_err(|_| FrameError::BadPayload("utf8"))?;
    Ok((s, need))
}

// ---- typed payloads ---------------------------------------------------

/// HELLO: `u16 len ++ bytes`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Hello<'a> {
    pub app_id: &'a str,
}

impl<'a> Hello<'a> {
    pub fn encode(&self, out: &mut Vec<u8>) {
        encode_str(out, self.app_id);
    }

    pub fn decode(buf: &'a [u8]) -> Result<Self, FrameError> {
        let (app_id, _) = decode_str(buf)?;
        Ok(Hello { app_id })
    }
}

/// HELLO_OK: `u32 credits ++ u32 leader (u32::MAX = None) ++ u16 len ++ bytes`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct HelloOk<'a> {
    pub credits: u32,
    pub leader: Option<u32>,
    pub leader_addr: &'a str,
}

impl<'a> HelloOk<'a> {
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.credits.to_le_bytes());
        out.extend_from_slice(&self.leader.unwrap_or(u32::MAX).to_le_bytes());
        encode_str(out, self.leader_addr);
    }

    pub fn decode(buf: &'a [u8]) -> Result<Self, FrameError> {
        if buf.len() < 8 {
            return Err(FrameError::Short {
                need: 8,
                have: buf.len(),
            });
        }
        let credits = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let leader_raw = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let leader = if leader_raw == u32::MAX {
            None
        } else {
            Some(leader_raw)
        };
        let (leader_addr, _) = decode_str(&buf[8..])?;
        Ok(HelloOk {
            credits,
            leader,
            leader_addr,
        })
    }
}

/// HELLO_REFUSED: `u8 reason ++ u16 len ++ bytes`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct HelloRefused<'a> {
    pub reason: u8,
    pub detail: &'a str,
}

impl<'a> HelloRefused<'a> {
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(self.reason);
        encode_str(out, self.detail);
    }

    pub fn decode(buf: &'a [u8]) -> Result<Self, FrameError> {
        if buf.is_empty() {
            return Err(FrameError::Short { need: 1, have: 0 });
        }
        let reason = buf[0];
        let (detail, _) = decode_str(&buf[1..])?;
        Ok(HelloRefused { reason, detail })
    }
}

/// RESPONSE header: `u32 credits ++ u64 acked_seq ++ u64 position` (20 bytes),
/// followed by the response bytes (not part of this struct).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ResponseMeta {
    pub credits: u32,
    pub acked_seq: u64,
    pub position: u64,
}

impl ResponseMeta {
    pub const LEN: usize = 20;

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.credits.to_le_bytes());
        out.extend_from_slice(&self.acked_seq.to_le_bytes());
        out.extend_from_slice(&self.position.to_le_bytes());
    }

    pub fn decode(buf: &[u8]) -> Result<Self, FrameError> {
        if buf.len() < Self::LEN {
            return Err(FrameError::Short {
                need: Self::LEN,
                have: buf.len(),
            });
        }
        let credits = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let acked_seq = u64::from_le_bytes(buf[4..12].try_into().unwrap());
        let position = u64::from_le_bytes(buf[12..20].try_into().unwrap());
        Ok(ResponseMeta {
            credits,
            acked_seq,
            position,
        })
    }
}

/// STATUS: `u64 acked_seq ++ u32 credits` (12 bytes).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Status {
    pub acked_seq: u64,
    pub credits: u32,
}

impl Status {
    pub const LEN: usize = 12;

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.acked_seq.to_le_bytes());
        out.extend_from_slice(&self.credits.to_le_bytes());
    }

    pub fn decode(buf: &[u8]) -> Result<Self, FrameError> {
        if buf.len() < Self::LEN {
            return Err(FrameError::Short {
                need: Self::LEN,
                have: buf.len(),
            });
        }
        let acked_seq = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let credits = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        Ok(Status { acked_seq, credits })
    }
}

/// REDIRECT / LEADER_CHANGED: `u32 node_id ++ u16 len ++ bytes`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Leader<'a> {
    pub node_id: u32,
    pub addr: &'a str,
}

impl<'a> Leader<'a> {
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.node_id.to_le_bytes());
        encode_str(out, self.addr);
    }

    pub fn decode(buf: &'a [u8]) -> Result<Self, FrameError> {
        if buf.len() < 4 {
            return Err(FrameError::Short {
                need: 4,
                have: buf.len(),
            });
        }
        let node_id = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let (addr, _) = decode_str(&buf[4..])?;
        Ok(Leader { node_id, addr })
    }
}

/// RETRY: `u8 reason ++ u32 retry_after_us` (5 bytes).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Retry {
    pub reason: u8,
    pub retry_after_us: u32,
}

impl Retry {
    pub const LEN: usize = 5;

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(self.reason);
        out.extend_from_slice(&self.retry_after_us.to_le_bytes());
    }

    pub fn decode(buf: &[u8]) -> Result<Self, FrameError> {
        if buf.len() < Self::LEN {
            return Err(FrameError::Short {
                need: Self::LEN,
                have: buf.len(),
            });
        }
        let reason = buf[0];
        let retry_after_us = u32::from_le_bytes(buf[1..5].try_into().unwrap());
        Ok(Retry {
            reason,
            retry_after_us,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_round_trips_and_rejects_short() {
        let mut out = Vec::new();
        Hello { app_id: "myapp" }.encode(&mut out);
        assert_eq!(Hello::decode(&out).unwrap().app_id, "myapp");
        assert!(matches!(
            Hello::decode(&out[..1]),
            Err(FrameError::Short { .. })
        ));
    }

    #[test]
    fn hello_empty_app_id_round_trips() {
        let mut out = Vec::new();
        Hello { app_id: "" }.encode(&mut out);
        assert_eq!(out.len(), 2);
        assert_eq!(Hello::decode(&out).unwrap().app_id, "");
    }

    #[test]
    fn hello_refused_round_trips_and_rejects_short() {
        let mut out = Vec::new();
        HelloRefused {
            reason: HELLO_REFUSED_APP_ID,
            detail: "wrong app_id",
        }
        .encode(&mut out);
        let hr = HelloRefused::decode(&out).unwrap();
        assert_eq!(
            (hr.reason, hr.detail),
            (HELLO_REFUSED_APP_ID, "wrong app_id")
        );
        assert!(matches!(
            HelloRefused::decode(&[]),
            Err(FrameError::Short { .. })
        ));
    }

    #[test]
    fn hello_ok_leader_max_u32_decodes_as_none() {
        let mut out = Vec::new();
        HelloOk {
            credits: 3,
            leader: Some(u32::MAX),
            leader_addr: "x",
        }
        .encode(&mut out);
        // Encoding Some(u32::MAX) is indistinguishable from None on the wire
        // by construction — assert that round trip explicitly.
        assert_eq!(HelloOk::decode(&out).unwrap().leader, None);
    }

    #[test]
    fn hello_ok_rejects_short_buffer() {
        assert!(matches!(
            HelloOk::decode(&[0u8; 4]),
            Err(FrameError::Short { .. })
        ));
    }

    #[test]
    fn status_round_trips_and_rejects_short() {
        let mut out = Vec::new();
        Status {
            acked_seq: 99,
            credits: 5,
        }
        .encode(&mut out);
        assert_eq!(out.len(), Status::LEN);
        let s = Status::decode(&out).unwrap();
        assert_eq!((s.acked_seq, s.credits), (99, 5));
        assert!(matches!(
            Status::decode(&out[..3]),
            Err(FrameError::Short { .. })
        ));
    }

    #[test]
    fn leader_empty_addr_round_trips() {
        let mut out = Vec::new();
        Leader {
            node_id: 7,
            addr: "",
        }
        .encode(&mut out);
        let l = Leader::decode(&out).unwrap();
        assert_eq!((l.node_id, l.addr), (7, ""));
    }

    #[test]
    fn the_four_hello_refusal_reasons_are_distinct_and_round_trip() {
        const REASONS: [u8; 4] = [
            HELLO_REFUSED_APP_ID,
            HELLO_REFUSED_VERSION,
            HELLO_REFUSED_FAULTED,
            HELLO_REFUSED_BUSY,
        ];
        for r in REASONS {
            let mut out = Vec::new();
            HelloRefused {
                reason: r,
                detail: "why",
            }
            .encode(&mut out);
            assert_eq!(HelloRefused::decode(&out).unwrap().reason, r);
        }
        assert_eq!(
            REASONS.len(),
            std::collections::BTreeSet::from(REASONS).len(),
            "reason codes must not collide"
        );
    }

    #[test]
    fn retry_payload_too_large_reason_encodes() {
        let mut out = Vec::new();
        Retry {
            reason: RETRY_PAYLOAD_TOO_LARGE,
            retry_after_us: 0,
        }
        .encode(&mut out);
        let r = Retry::decode(&out).unwrap();
        assert_eq!((r.reason, r.retry_after_us), (RETRY_PAYLOAD_TOO_LARGE, 0));
    }

    #[test]
    fn decode_header_rejects_len_below_header_len() {
        // len field claims fewer bytes than the header itself.
        let mut buf = vec![0u8; HEADER_LEN];
        buf[0..4].copy_from_slice(&(HEADER_LEN as u32 - 1).to_le_bytes());
        buf[4] = FrameType::Ping as u8;
        assert!(matches!(decode_header(&buf), Err(FrameError::Short { .. })));
    }

    #[test]
    fn decode_str_rejects_invalid_utf8() {
        // len=1, one invalid UTF-8 byte.
        let buf: [u8; 3] = [1, 0, 0xFF];
        assert!(matches!(
            Hello::decode(&buf),
            Err(FrameError::BadPayload("utf8"))
        ));
    }
}

#[cfg(test)]
mod header_tests {
    use super::*;

    #[test]
    fn encode_header_into_matches_encode_frame() {
        let h = Header {
            ty: FrameType::Submit,
            flags: 0,
            version: PROTOCOL_VERSION,
            client_id: 0x0102_0304_0506_0708,
            seq: 42,
        };
        let payload = b"hello";
        let mut whole = Vec::new();
        encode_frame(&mut whole, h, payload);
        let hdr = encode_header_into(h, payload.len());
        assert_eq!(
            &whole[..HEADER_LEN],
            &hdr[..],
            "the two encoders must agree byte for byte"
        );
        assert_eq!(&whole[HEADER_LEN..], payload);
    }
}
