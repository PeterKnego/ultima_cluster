//! UDP segment wire format. Little-endian header + optional payload + CRC32.
//!
//! Layout (28-byte header):
//! ```text
//!   version     u8
//!   seg_type    u8
//!   flags       u8
//!   _pad        u8
//!   session_id  u32  (le)
//!   seq         u64  (le)
//!   arg         u64  (le)
//!   payload_len u32  (le)
//!   payload     [u8; payload_len]
//!   crc32       u32  (le, over header[..24] + payload)
//! ```
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::network::NetworkError;

pub const WIRE_VERSION: u8 = 1;
pub const SEG_HEADER_LEN: usize = 28; // through payload_len
pub const FLAG_BEGIN: u8 = 0x01;
pub const FLAG_END: u8 = 0x02;

#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SegType {
    Data = 1,
    Nak = 2,
    Sm = 3,
    Heartbeat = 4,
    Hello = 5,
    HelloAck = 6,
}

impl SegType {
    fn from_u8(v: u8) -> Result<Self, NetworkError> {
        Ok(match v {
            1 => Self::Data,
            2 => Self::Nak,
            3 => Self::Sm,
            4 => Self::Heartbeat,
            5 => Self::Hello,
            6 => Self::HelloAck,
            other => return Err(NetworkError::Decode(format!("bad seg_type {other}"))),
        })
    }
}

pub struct Segment {
    pub version: u8,
    pub seg_type: SegType,
    pub flags: u8,
    pub session_id: u32,
    pub seq: u64,
    pub arg: u64,
    pub payload: Bytes,
}

impl Segment {
    pub fn encode(&self) -> BytesMut {
        let mut b = BytesMut::with_capacity(SEG_HEADER_LEN + self.payload.len() + 4);
        b.put_u8(self.version);
        b.put_u8(self.seg_type as u8);
        b.put_u8(self.flags);
        b.put_u8(0); // _pad
        b.put_u32_le(self.session_id);
        b.put_u64_le(self.seq);
        b.put_u64_le(self.arg);
        b.put_u32_le(self.payload.len() as u32);
        b.put_slice(&self.payload);
        let crc = crc32fast::hash(&b[..]); // covers header(24) + payload_len(4) + payload
        b.put_u32_le(crc);
        b
    }

    pub fn decode(buf: &[u8]) -> Result<Segment, NetworkError> {
        if buf.len() < SEG_HEADER_LEN + 4 {
            return Err(NetworkError::Decode(format!(
                "segment too short: {} bytes",
                buf.len()
            )));
        }
        let (body, crc_bytes) = buf.split_at(buf.len() - 4);
        let crc_expected = crc32fast::hash(body);
        let crc_actual = u32::from_le_bytes(crc_bytes.try_into().unwrap());
        if crc_actual != crc_expected {
            return Err(NetworkError::Decode("segment crc mismatch".into()));
        }
        let mut h = &body[..SEG_HEADER_LEN];
        let version = h.get_u8();
        let seg_type = SegType::from_u8(h.get_u8())?;
        let flags = h.get_u8();
        let _pad = h.get_u8();
        let session_id = h.get_u32_le();
        let seq = h.get_u64_le();
        let arg = h.get_u64_le();
        let payload_len = h.get_u32_le() as usize;
        let payload_region = &body[SEG_HEADER_LEN..];
        if payload_region.len() != payload_len {
            return Err(NetworkError::Decode(format!(
                "payload_len {payload_len} != actual {}",
                payload_region.len()
            )));
        }
        Ok(Segment {
            version,
            seg_type,
            flags,
            session_id,
            seq,
            arg,
            payload: Bytes::copy_from_slice(payload_region),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn data_round_trip() {
        let s = Segment {
            version: WIRE_VERSION,
            seg_type: SegType::Data,
            flags: FLAG_BEGIN | FLAG_END,
            session_id: 0xABCD,
            seq: 42,
            arg: 0,
            payload: Bytes::from_static(b"hello"),
        };
        let buf = s.encode();
        let d = Segment::decode(&buf).unwrap();
        assert_eq!(d.seg_type, SegType::Data);
        assert_eq!(d.session_id, 0xABCD);
        assert_eq!(d.seq, 42);
        assert_eq!(d.flags, FLAG_BEGIN | FLAG_END);
        assert_eq!(&d.payload[..], b"hello");
    }

    #[test]
    fn nak_round_trip() {
        let s = Segment {
            version: WIRE_VERSION,
            seg_type: SegType::Nak,
            flags: 0,
            session_id: 7,
            seq: 100,
            arg: 5,
            payload: Bytes::new(),
        };
        let d = Segment::decode(&s.encode()).unwrap();
        assert_eq!(d.seg_type, SegType::Nak);
        assert_eq!(d.seq, 100); // gap start
        assert_eq!(d.arg, 5); // gap count
    }

    #[test]
    fn rejects_short_buffer() {
        assert!(Segment::decode(&[0u8; 4]).is_err());
    }

    #[test]
    fn rejects_bad_crc() {
        let s = Segment {
            version: WIRE_VERSION,
            seg_type: SegType::Data,
            flags: FLAG_END,
            session_id: 1,
            seq: 1,
            arg: 0,
            payload: Bytes::from_static(b"xyz"),
        };
        let mut buf = s.encode();
        let last = buf.len() - 1;
        buf[last] ^= 0xFF; // corrupt CRC
        assert!(Segment::decode(&buf).is_err());
    }
}
