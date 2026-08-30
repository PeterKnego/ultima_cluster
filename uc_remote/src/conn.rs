// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Framed reads and writes over a blocking [`TcpStream`].
//!
//! This is the thin I/O half of the remote protocol: it turns a byte stream
//! into whole `(Header, payload)` frames and back, and nothing else. It knows
//! nothing about credits, sequences, or reconnection — that is
//! [`crate::client`]'s job. The edge (`uc_gateway`) speaks the same framing,
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
    /// Bytes read from the socket but not yet parsed into a returned frame — a
    /// coalesced read may pull several frames' worth at once, and the tail of
    /// one read may be a partial frame carried to the next. Used ONLY by
    /// [`FramedConn::read_frame_buffered`] / [`FramedConn::next_buffered`]; the
    /// one-frame-at-a-time [`FramedConn::read_frame`] never touches it, and the
    /// two readers must not be mixed on one connection (the buffered readers own
    /// the byte stream once they start).
    inbuf: Vec<u8>,
    /// Parse cursor into `inbuf`: `inbuf[inpos..]` is still to be parsed.
    inpos: usize,
    /// When the bytes currently in `inbuf` first became a *partial* frame (some
    /// bytes present, not yet a whole frame). `None` at a frame boundary. Bounds
    /// a mid-frame stall exactly as `read_frame`'s local `partial_since` does,
    /// but across calls, so a peer that dribbles a frame is caught on the same
    /// clock whether or not the drip straddles a `read_frame_buffered` return.
    partial_since: Option<Instant>,
}

/// One coalesced socket read pulls up to this many bytes, so a burst of small
/// frames costs one `recvfrom` instead of two per frame. Sized to hold many
/// framed responses (24-byte header + 20-byte meta + payload) without being a
/// large stack or allocation.
const READ_CHUNK: usize = 64 * 1024;

impl FramedConn {
    /// Wrap a stream, setting `TCP_NODELAY` (this protocol is latency-bound and
    /// writes whole frames, so Nagle only ever adds delay).
    pub fn new(stream: TcpStream) -> io::Result<Self> {
        stream.set_nodelay(true)?;
        Ok(FramedConn {
            stream,
            hdr: [0u8; HEADER_LEN],
            out: Vec::with_capacity(256),
            inbuf: Vec::new(),
            inpos: 0,
            partial_since: None,
        })
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

    /// Parse the next whole frame already sitting in `inbuf`, WITHOUT touching
    /// the socket.
    ///
    /// - `Ok(Some(frame))` — a complete frame was buffered; the cursor advances
    ///   past it.
    /// - `Ok(None)` — no complete frame is buffered right now (empty, or only a
    ///   partial). The caller may block for more via
    ///   [`FramedConn::read_frame_buffered`], or come back later.
    /// - `Err(..)` — a malformed header. The connection is unusable, same as
    ///   `read_frame`.
    ///
    /// This is the non-blocking companion to `read_frame_buffered`: a reader
    /// can drain every frame a single coalesced read delivered — processing the
    /// whole burst — before it blocks again, which is what lets one wake do one
    /// batch of work (and, on the client, one admission notify).
    pub fn next_buffered(&mut self) -> Result<Option<(Header, Bytes)>, RemoteError> {
        let avail = &self.inbuf[self.inpos..];
        if avail.len() < HEADER_LEN {
            return Ok(None);
        }
        let (h, payload_len) = decode_header(avail)?;
        let total = HEADER_LEN + payload_len;
        if avail.len() < total {
            return Ok(None);
        }
        let payload = Bytes::copy_from_slice(&avail[HEADER_LEN..total]);
        self.inpos += total;
        // A whole frame was consumed: we are back at a frame boundary, so the
        // mid-frame clock resets. If bytes remain they are the START of the
        // next frame; `read_frame_buffered` restarts the clock for it before it
        // blocks (a conservative upper bound — those bytes arrived at or before
        // then).
        self.partial_since = None;
        if self.inpos == self.inbuf.len() {
            self.inbuf.clear();
            self.inpos = 0;
        }
        Ok(Some((h, payload)))
    }

    /// Like [`FramedConn::read_frame`], but reads through the coalescing buffer:
    /// one `recvfrom` can pull many frames, which are then handed out one at a
    /// time by this call and [`FramedConn::next_buffered`] with no further
    /// syscall until the buffer drains.
    ///
    /// The deadline contract is **identical** to `read_frame`, and for the same
    /// reasons (see its doc): `Ok(None)` only at a frame boundary (nothing
    /// buffered) when the socket read timed out; a mid-frame stall is bounded by
    /// `max_stall` measured from the frame's first byte; `Err(..)` on EOF, a
    /// socket error, a malformed header, or a stall past `max_stall`. A caller
    /// that already honoured `read_frame`'s timeout / `is_closed` re-check keeps
    /// exactly the same behaviour here.
    ///
    /// A connection is owned by ONE reader style: once this (or `next_buffered`)
    /// is used, `read_frame` must not be — it would read straight from the
    /// socket past bytes already sitting in `inbuf`.
    pub fn read_frame_buffered(
        &mut self,
        max_stall: Duration,
    ) -> Result<Option<(Header, Bytes)>, RemoteError> {
        loop {
            if let Some(frame) = self.next_buffered()? {
                return Ok(Some(frame));
            }
            // No whole frame buffered. Drop the consumed prefix so the reader
            // fills contiguous space, then note whether we are mid-frame.
            if self.inpos > 0 {
                self.inbuf.drain(..self.inpos);
                self.inpos = 0;
            }
            if !self.inbuf.is_empty() && self.partial_since.is_none() {
                self.partial_since = Some(Instant::now());
            }
            let old = self.inbuf.len();
            self.inbuf.resize(old + READ_CHUNK, 0);
            let r = self.stream.read(&mut self.inbuf[old..]);
            match r {
                Ok(0) => {
                    self.inbuf.truncate(old);
                    return Err(RemoteError::Io(eof()));
                }
                Ok(n) => {
                    self.inbuf.truncate(old + n);
                    if self.partial_since.is_none() && !self.inbuf.is_empty() {
                        self.partial_since = Some(Instant::now());
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {
                    self.inbuf.truncate(old);
                }
                Err(e) if is_timeout(&e) => {
                    self.inbuf.truncate(old);
                    // A timeout with nothing buffered is the idle frame boundary
                    // `Ok(None)` every caller's tick hangs off. With a partial
                    // frame buffered it is a mid-frame stall, bounded exactly as
                    // in `read_frame`.
                    if self.inbuf.is_empty() {
                        return Ok(None);
                    }
                    check_stall(self.partial_since, max_stall)?;
                }
                Err(e) => {
                    self.inbuf.truncate(old);
                    return Err(RemoteError::Io(e));
                }
            }
        }
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

    fn a_frame(seq: u64, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        crate::frame::encode_frame(
            &mut out,
            Header {
                ty: crate::frame::FrameType::Response,
                flags: 0,
                version: crate::frame::PROTOCOL_VERSION,
                client_id: 7,
                seq,
            },
            payload,
        );
        out
    }

    /// The whole point of the buffered reader: several frames arriving in one
    /// TCP segment are handed out one at a time, and only the FIRST needed a
    /// blocking read — the rest come from the buffer via `next_buffered` with no
    /// syscall.
    #[test]
    fn many_frames_in_one_read_are_parsed_one_at_a_time() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let mut client = FramedConn::new(TcpStream::connect(addr).unwrap()).unwrap();
        client.set_read_timeout(Some(Duration::from_millis(50))).unwrap();
        let (mut peer, _) = l.accept().unwrap();

        let mut wire = Vec::new();
        for s in 1..=5u64 {
            wire.extend_from_slice(&a_frame(s, &[s as u8; 8]));
        }
        peer.write_all(&wire).unwrap();

        // First frame blocks for the read; the rest are already buffered.
        let (h0, p0) = client.read_frame_buffered(Duration::from_secs(1)).unwrap().unwrap();
        assert_eq!((h0.seq, &p0[..]), (1, &[1u8; 8][..]));
        for s in 2..=5u64 {
            let (h, p) = client.next_buffered().unwrap().expect("frame already buffered");
            assert_eq!((h.seq, &p[..]), (s, &[s as u8; 8][..]));
        }
        // Buffer drained: nothing more without another read.
        assert!(client.next_buffered().unwrap().is_none());
        drop(peer);
    }

    /// A frame split across two reads is reassembled, and the half-frame does
    /// NOT surface as `Ok(None)` — `Ok(None)` is reserved for a clean boundary.
    #[test]
    fn a_frame_split_across_reads_is_reassembled() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let mut client = FramedConn::new(TcpStream::connect(addr).unwrap()).unwrap();
        client.set_read_timeout(Some(Duration::from_millis(10))).unwrap();
        let (mut peer, _) = l.accept().unwrap();

        let frame = a_frame(9, &[1, 2, 3, 4, 5, 6]);
        let (head, tail) = frame.split_at(HEADER_LEN + 2);
        peer.write_all(head).unwrap();
        // Give the reader a moment, then send the tail: the buffered reader must
        // keep waiting on the partial frame (not return None) and then complete
        // it once the tail lands.
        let mut peer2 = peer.try_clone().unwrap();
        let tail = tail.to_vec();
        let h = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            peer2.write_all(&tail).unwrap();
        });
        let (hdr, p) = client.read_frame_buffered(Duration::from_secs(1)).unwrap().unwrap();
        assert_eq!((hdr.seq, &p[..]), (9, &[1u8, 2, 3, 4, 5, 6][..]));
        h.join().unwrap();
        drop(peer);
    }

    /// The mid-frame stall bound holds for the buffered reader too: a half
    /// header then silence fails the read within `[budget, 2*budget)`, exactly
    /// like `read_frame`. This is the T3b guarantee the buffered path must keep.
    #[test]
    fn buffered_reader_bounds_a_mid_frame_stall() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let mut client = FramedConn::new(TcpStream::connect(addr).unwrap()).unwrap();
        client.set_read_timeout(Some(Duration::from_millis(20))).unwrap();
        let (mut peer, _) = l.accept().unwrap();
        peer.write_all(&[0u8; HEADER_LEN / 2]).unwrap();

        let budget = Duration::from_millis(200);
        let started = Instant::now();
        let err = client
            .read_frame_buffered(budget)
            .expect_err("a stalled half-header must fail the buffered read");
        let took = started.elapsed();
        match err {
            RemoteError::Io(e) => assert_eq!(e.kind(), io::ErrorKind::TimedOut, "{e:?}"),
            other => panic!("expected a timeout, got {other:?}"),
        }
        assert!(took >= budget, "gave up early, after {took:?}");
        assert!(took < budget * 3, "took {took:?}");
        drop(peer);
    }

    /// And the idle boundary contract: no bytes buffered + a socket timeout is
    /// `Ok(None)`, repeatedly, however small the stall budget.
    #[test]
    fn buffered_reader_reports_idle_at_a_boundary() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let mut client = FramedConn::new(TcpStream::connect(addr).unwrap()).unwrap();
        client.set_read_timeout(Some(Duration::from_millis(10))).unwrap();
        let (peer, _) = l.accept().unwrap();
        for _ in 0..3 {
            assert!(
                client.read_frame_buffered(Duration::from_millis(1)).unwrap().is_none(),
                "an idle connection must keep reporting Ok(None)"
            );
        }
        drop(peer);
    }
}
