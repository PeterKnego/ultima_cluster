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
//! `Ok(None)` ("idle") so the caller can run periodic work; a timeout in the
//! middle of a frame does **not** end the read there, because a half-read
//! frame cannot be abandoned without desynchronising the stream — but it is
//! bounded by the caller's `max_stall`, after which the connection is failed
//! rather than waited on forever. A *write* timeout, by contrast, is always
//! fatal to the connection — a partially written frame leaves the peer's
//! parser mid-frame, so the caller must discard the connection (the client
//! does: it reconnects and re-sends).

use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::time::{Duration, Instant};

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

    /// Read one whole frame, giving a *partially* read one at most `max_stall`
    /// to finish.
    ///
    /// - `Ok(Some(frame))` — a complete frame.
    /// - `Ok(None)` — the read timeout expired *at a frame boundary* (nothing
    ///   consumed). The stream is still in sync; the caller may do periodic
    ///   work and call again. This is the contract every caller's tick,
    ///   sweep and liveness clock hangs off, and it is unchanged.
    /// - `Err(..)` — EOF, a socket error, a malformed header, **or a frame that
    ///   was still incomplete `max_stall` after its first byte arrived**
    ///   ([`io::ErrorKind::TimedOut`]). The connection is unusable in every
    ///   case, and both sides already discard it on `Err`.
    ///
    /// ## Why the mid-frame bound exists
    ///
    /// Without it a peer that vanishes *inside* a frame — half a header on the
    /// wire, then silence — parks this thread forever: the socket read timeout
    /// fires over and over and is simply re-issued, so `Ok(None)` is never
    /// returned and the caller's periodic work never runs again. On the client
    /// that means no tick, no sweep and no `dead_after`, so every outstanding
    /// `Ticket` blocks to eternity rather than failing over; on the edge it
    /// means a reader thread pinned until the process stops. The bound is
    /// measured from the moment the frame became partial and is **not** reset
    /// by trickled progress, so a byte-at-a-time drip is caught by the same
    /// clock as a total stall.
    ///
    /// Callers pass their own budget: `RemoteClient`'s reader uses
    /// `dead_after` (the same clock that already declares a silent-but-open
    /// connection dead), its dial uses `connect_timeout`, and the gateway edge
    /// uses `request_timeout` for a live connection and its handshake budget
    /// before that.
    pub fn read_frame(
        &mut self,
        max_stall: Duration,
    ) -> Result<Option<(Header, Bytes)>, RemoteError> {
        // Set on the first byte of a frame and never cleared until the frame
        // completes — `None` means "still at a frame boundary".
        let mut partial_since: Option<Instant> = None;
        let mut got = 0usize;
        while got < HEADER_LEN {
            match self.stream.read(&mut self.hdr[got..]) {
                Ok(0) => return Err(RemoteError::Io(eof())),
                Ok(n) => {
                    if got == 0 && n > 0 {
                        partial_since = Some(Instant::now());
                    }
                    got += n;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) if is_timeout(&e) => {
                    if got == 0 {
                        return Ok(None);
                    }
                    check_stall(partial_since, max_stall)?;
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
                Err(e) if is_timeout(&e) => check_stall(partial_since, max_stall)?,
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

    /// Write a buffer of one-or-more **already-encoded** frames in a single
    /// `write_all` (one syscall for the whole batch). The caller is responsible
    /// for the framing — each frame in `buf` must be a complete
    /// [`encode_frame`] product, so the peer's length-prefixed parser sees whole
    /// frames however the concatenation is split across TCP segments.
    ///
    /// Same failure contract as [`FramedConn::write_frame`]: a partial write may
    /// have left the peer's parser mid-frame, so the caller must discard the
    /// connection on `Err`.
    pub fn write_all_bytes(&mut self, buf: &[u8]) -> Result<(), RemoteError> {
        if buf.is_empty() {
            return Ok(());
        }
        self.stream.write_all(buf).map_err(RemoteError::Io)
    }
}

fn eof() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, "peer closed the connection")
}

/// Fail a frame that has been incomplete for longer than `max_stall`. A
/// `None` start means nothing has been consumed yet, which is a frame
/// boundary, not a stall.
fn check_stall(partial_since: Option<Instant>, max_stall: Duration) -> Result<(), RemoteError> {
    match partial_since {
        Some(t) if t.elapsed() >= max_stall => Err(RemoteError::Io(io::Error::new(
            io::ErrorKind::TimedOut,
            "peer stalled in the middle of a frame",
        ))),
        _ => Ok(()),
    }
}

/// A socket timeout is reported as `WouldBlock` on Unix and `TimedOut` on
/// Windows; treat both the same.
fn is_timeout(e: &io::Error) -> bool {
    matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::TcpListener;
    use std::time::Instant;

    /// A peer that writes HALF a header and then goes silent must fail the
    /// read, not park the caller forever.
    ///
    /// This is the regression the `max_stall` argument exists for: with the
    /// old signature the loop below re-issued its socket read timeout
    /// indefinitely, because a partially-read frame cannot be reported as
    /// `Ok(None)` without desynchronising the stream — so the caller's tick,
    /// sweep and liveness clock all stopped running.
    #[test]
    fn a_peer_that_stalls_inside_the_header_fails_the_read() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let mut client = FramedConn::new(TcpStream::connect(addr).unwrap()).unwrap();
        // Well under the stall budget, so the budget — not the socket timeout
        // — is what ends the read.
        client.set_read_timeout(Some(Duration::from_millis(20))).unwrap();
        let (mut peer, _) = l.accept().unwrap();
        peer.write_all(&[0u8; HEADER_LEN / 2]).unwrap();

        let budget = Duration::from_millis(200);
        let started = Instant::now();
        let err = client.read_frame(budget).expect_err("a stalled half-header must fail the read");
        let took = started.elapsed();
        match err {
            RemoteError::Io(e) => assert_eq!(e.kind(), io::ErrorKind::TimedOut, "{e:?}"),
            other => panic!("expected a timeout, got {other:?}"),
        }
        assert!(took >= budget, "gave up early, after {took:?}");
        assert!(took < budget * 2, "took {took:?}, want under 2x the {budget:?} budget");
        // The peer is still there and still silent — this was the read's own
        // verdict, not an EOF.
        drop(peer);
    }

    /// A peer that stalls inside the PAYLOAD is the same failure, and gets the
    /// same verdict — the bound spans the whole frame, not just its header.
    #[test]
    fn a_peer_that_stalls_inside_the_payload_fails_the_read() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let mut client = FramedConn::new(TcpStream::connect(addr).unwrap()).unwrap();
        client.set_read_timeout(Some(Duration::from_millis(20))).unwrap();
        let (mut peer, _) = l.accept().unwrap();

        let mut frame = Vec::new();
        crate::frame::encode_frame(
            &mut frame,
            Header {
                ty: crate::frame::FrameType::Response,
                flags: 0,
                version: crate::frame::PROTOCOL_VERSION,
                client_id: 7,
                seq: 1,
            },
            &[0u8; 32],
        );
        peer.write_all(&frame[..HEADER_LEN + 4]).unwrap();

        let budget = Duration::from_millis(200);
        let started = Instant::now();
        let err = client.read_frame(budget).expect_err("a stalled payload must fail the read");
        assert!(matches!(err, RemoteError::Io(ref e) if e.kind() == io::ErrorKind::TimedOut));
        assert!(started.elapsed() < budget * 2, "took {:?}", started.elapsed());
        drop(peer);
    }

    /// The `Ok(None)` contract is unchanged: a timeout AT a frame boundary is
    /// still "idle", however small the stall budget. Every caller's periodic
    /// work hangs off this.
    #[test]
    fn a_timeout_at_a_frame_boundary_is_still_idle() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let mut client = FramedConn::new(TcpStream::connect(addr).unwrap()).unwrap();
        client.set_read_timeout(Some(Duration::from_millis(10))).unwrap();
        let (peer, _) = l.accept().unwrap();
        for _ in 0..3 {
            assert!(
                client.read_frame(Duration::from_millis(1)).unwrap().is_none(),
                "an idle connection must keep reporting Ok(None), not time out"
            );
        }
        drop(peer);
    }
}
