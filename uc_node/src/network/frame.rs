//! Wire framing on top of QUIC streams.
//!
//! Each stream carries a sequence of length-prefixed frames. The frame header
//! is fixed-size; the body is variable. CRC32 covers the body.
//!
//! Frame layout:
//!
//! ```text
//!     msg_type        u8     (MessageType enum)
//!     flags           u8     (bit 0: is_response)
//!     request_id      u64    (correlator for multiplexed in-flight requests)
//!     body_len        u32    (length of body in bytes)
//!     body            (variable)
//!     body_crc32      u32    (CRC over body)
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::NetworkError;

#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MessageType {
    AppendEntriesReq    = 1,
    AppendEntriesResp   = 2,
    VoteReq             = 3,
    VoteResp            = 4,
    InstallSnapshotReq  = 5,
    InstallSnapshotResp = 6,
    Handshake           = 10,
    HandshakeAck        = 11,
}

impl MessageType {
    #[allow(clippy::result_unit_err)]
    pub const fn from_u8(v: u8) -> Result<Self, ()> {
        match v {
            1 => Ok(Self::AppendEntriesReq),
            2 => Ok(Self::AppendEntriesResp),
            3 => Ok(Self::VoteReq),
            4 => Ok(Self::VoteResp),
            5 => Ok(Self::InstallSnapshotReq),
            6 => Ok(Self::InstallSnapshotResp),
            10 => Ok(Self::Handshake),
            11 => Ok(Self::HandshakeAck),
            _ => Err(()),
        }
    }
}

const HEADER_LEN: usize = 1 + 1 + 8 + 4;     // 14 bytes
const TRAILER_LEN: usize = 4;                 // body_crc32

pub struct Frame {
    pub msg_type: MessageType,
    pub flags: u8,
    pub request_id: u64,
    pub body: Bytes,
}

impl Frame {
    pub fn new_request(msg_type: MessageType, request_id: u64, body: Bytes) -> Self {
        Self { msg_type, flags: 0, request_id, body }
    }
    pub fn new_response(msg_type: MessageType, request_id: u64, body: Bytes) -> Self {
        Self { msg_type, flags: 1, request_id, body }
    }
    pub fn is_response(&self) -> bool { self.flags & 1 != 0 }

    /// Encode the frame as a `BytesMut`. Includes header + body + CRC32 trailer.
    pub fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(HEADER_LEN + self.body.len() + TRAILER_LEN);
        buf.put_u8(self.msg_type as u8);
        buf.put_u8(self.flags);
        buf.put_u64(self.request_id);
        buf.put_u32(self.body.len() as u32);
        buf.put_slice(&self.body);
        let crc = crc32fast::hash(&self.body);
        buf.put_u32(crc);
        buf
    }

    /// Decode a frame from a buffer that has at least `HEADER_LEN` bytes;
    /// returns the frame. Returns Err if there isn't enough data yet.
    pub fn decode(buf: &mut Bytes) -> Result<Frame, NetworkError> {
        if buf.len() < HEADER_LEN {
            return Err(NetworkError::Decode(format!(
                "need {HEADER_LEN} bytes for header, have {}", buf.len())));
        }
        let msg_type_byte = buf.get_u8();
        let msg_type = MessageType::from_u8(msg_type_byte)
            .map_err(|_| NetworkError::Decode(format!("unknown msg_type {msg_type_byte}")))?;
        let flags = buf.get_u8();
        let request_id = buf.get_u64();
        let body_len = buf.get_u32() as usize;
        if buf.len() < body_len + TRAILER_LEN {
            return Err(NetworkError::Decode(format!(
                "need {body_len}+{TRAILER_LEN} body bytes, have {}", buf.len())));
        }
        let body = buf.copy_to_bytes(body_len);
        let crc_actual = buf.get_u32();
        let crc_expected = crc32fast::hash(&body);
        if crc_actual != crc_expected {
            return Err(NetworkError::Decode(format!(
                "crc mismatch: expected {crc_expected}, got {crc_actual}")));
        }
        Ok(Frame { msg_type, flags, request_id, body })
    }

    /// Read a frame from an `AsyncRead` source (e.g., `quinn::RecvStream`).
    pub async fn read_async<R>(reader: &mut R) -> Result<Frame, NetworkError>
    where R: tokio::io::AsyncRead + Unpin
    {
        use tokio::io::AsyncReadExt;
        let mut header = [0u8; HEADER_LEN];
        reader.read_exact(&mut header).await?;
        let mut header_buf = Bytes::copy_from_slice(&header);
        let msg_type_byte = header_buf.get_u8();
        let msg_type = MessageType::from_u8(msg_type_byte)
            .map_err(|_| NetworkError::Decode(format!("unknown msg_type {msg_type_byte}")))?;
        let flags = header_buf.get_u8();
        let request_id = header_buf.get_u64();
        let body_len = header_buf.get_u32() as usize;

        let mut body_vec = vec![0u8; body_len];
        reader.read_exact(&mut body_vec).await?;
        let mut crc_buf = [0u8; TRAILER_LEN];
        reader.read_exact(&mut crc_buf).await?;
        let crc_actual = u32::from_be_bytes(crc_buf);
        let crc_expected = crc32fast::hash(&body_vec);
        if crc_actual != crc_expected {
            return Err(NetworkError::Decode(format!(
                "crc mismatch: expected {crc_expected}, got {crc_actual}")));
        }
        Ok(Frame { msg_type, flags, request_id, body: Bytes::from(body_vec) })
    }
}
