// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Framed reads and writes over a blocking [`TcpStream`].
//!
//! This is the thin I/O half of the remote protocol: it turns a byte stream
//! into whole `(Header, payload)` frames and back, and nothing else. It knows
//! nothing about credits, sequences, or reconnection — that is
//! [`crate::client`]'s job. The edge (`uc2_gateway`) speaks the same framing,
//! so the type is public.
//!
//! Timeouts are deliberate: a read timeout at a frame boundary reports
//! [`Ok(None)`] ("idle") so the caller can run periodic work; a timeout in the
//! middle of a frame just keeps reading, because a half-read frame cannot be
//! abandoned without desynchronising the stream. A *write* timeout, by
//! contrast, is always fatal to the connection — a partially written frame
//! leaves the peer's parser mid-frame, so the caller must discard the
//! connection (the client does: it reconnects and re-sends).

use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::time::Duration;

use bytes::Bytes;

use crate::error::{FrameError, RemoteError};
use crate::frame::{decode_header, encode_frame, Header, HEADER_LEN, MAX_FRAME_LEN};

/// A framed reader/writer over one TCP connection.
///
/// Clone the connection with [`FramedConn::try_clone`] to split reading and
/// writing across two threads: the two halves share one socket (and therefore
/// one set of socket timeouts), but each only ever touches its own direction.
pub struct FramedConn {
    stream: TcpStream,
    hdr: [u8; HEADER_LEN],
    out: Vec<u8>,
}

impl FramedConn {
    /// Wrap a stream, setting `TCP_NODELAY` (this protocol is latency-bound and
    /// writes whole frames, so Nagle only ever adds delay).
    pub fn new(stream: TcpStream) -> io::Result<Self> {
        stream.set_nodelay(true)?;
        Ok(FramedConn { stream, hdr: [0u8; HEADER_LEN], out: Vec::with_capacity(256) })
    }

    /// A second handle on the same socket — used to give the reader thread its
    /// own half while the writer stays under the client's lock.
    pub fn try_clone(&self) -> io::Result<Self> {
        FramedConn::new(self.stream.try_clone()?)
    }

    pub fn set_read_timeout(&self, d: Option<Duration>) -> io::Result<()> {
        self.stream.set_read_timeout(d)
    }

    pub fn set_write_timeout(&self, d: Option<Duration>) -> io::Result<()> {
        self.stream.set_write_timeout(d)
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.stream.peer_addr()
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.stream.local_addr()
    }

    /// Shut the socket down in both directions, waking a peer thread blocked in
    /// [`FramedConn::read_frame`]. Errors are ignored: the only reason to call
    /// this is that the connection is already being discarded.
    pub fn shutdown(&self) {
        let _ = self.stream.shutdown(Shutdown::Both);
    }

    /// Read one whole frame.
    ///
    /// - `Ok(Some(frame))` — a complete frame.
    /// - `Ok(None)` — the read timeout expired *at a frame boundary* (nothing
    ///   consumed). The stream is still in sync; the caller may do periodic
    ///   work and call again.
    /// - `Err(..)` — EOF, a socket error, or a malformed header. The connection
    ///   is unusable in every case.
    pub fn read_frame(&mut self) -> Result<Option<(Header, Bytes)>, RemoteError> {
        let mut got = 0usize;
        while got < HEADER_LEN {
            match self.stream.read(&mut self.hdr[got..]) {
                Ok(0) => return Err(RemoteError::Io(eof())),
                Ok(n) => got += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) if is_timeout(&e) => {
                    if got == 0 {
                        return Ok(None);
                    }
                }
                Err(e) => return Err(RemoteError::Io(e)),
            }
        }
        let (h, payload_len) = decode_header(&self.hdr)?;
        let mut payload = vec![0u8; payload_len];
        let mut got = 0usize;
        while got < payload_len {
            match self.stream.read(&mut payload[got..]) {
                Ok(0) => return Err(RemoteError::Io(eof())),
                Ok(n) => got += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) if is_timeout(&e) => {}
                Err(e) => return Err(RemoteError::Io(e)),
            }
        }
        Ok(Some((h, Bytes::from(payload))))
    }

    /// Encode and write one whole frame.
    ///
    /// A failure here (including a write timeout) may have left a partial frame
    /// on the wire, so the caller must not reuse the connection.
    pub fn write_frame(&mut self, h: Header, payload: &[u8]) -> Result<(), RemoteError> {
        let len = HEADER_LEN + payload.len();
        if len > MAX_FRAME_LEN as usize {
            return Err(RemoteError::Frame(FrameError::TooLong(len.min(u32::MAX as usize) as u32)));
        }
        self.out.clear();
        encode_frame(&mut self.out, h, payload);
        self.stream.write_all(&self.out).map_err(RemoteError::Io)
    }
}

fn eof() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, "peer closed the connection")
}

/// A socket timeout is reported as `WouldBlock` on Unix and `TimedOut` on
/// Windows; treat both the same.
fn is_timeout(e: &io::Error) -> bool {
    matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut)
}
