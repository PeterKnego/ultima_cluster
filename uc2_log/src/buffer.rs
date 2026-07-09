// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The log buffer (spec §4): one mmap'd/heap ring per node, addressed by
//! absolute u64 positions, exactly one writer (role-determined), readers
//! coordinated by position counters.
//!
//! Commit protocol: payload + header fields are plain writes; the u32 length
//! word at the frame's offset is stored LAST with `Release`. The `append`
//! counter is stored `Release` after the commit word, so any reader that
//! bounds itself by an acquire-load of `append` sees only whole committed
//! frames. Padding frames write ONLY their 32-byte header.
//!
//! Overrun rule: the appender never claims past `durable + capacity` — the
//! single hard gate (the archive is the only reader the ring can never drop).

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use uc_protocol::v2::frame::{
    self, FRAME_TYPE_MESSAGE, FRAME_TYPE_PADDING, FrameHeader, HEADER_LEN, align_frame_len,
};

use crate::counters::LogCounters;
use crate::region::Region;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AppendError {
    /// Claim would overwrite bytes the archive hasn't recorded. Retry after
    /// the durable counter advances (surfaced as admission backpressure).
    #[error("append would overrun unrecorded bytes")]
    WouldOverrun,
    #[error("payload exceeds max_payload")]
    PayloadTooLarge,
}

pub struct LogBuffer {
    region: Region,
    capacity: u64,
    mask: u64,
    max_payload: usize,
    counters: Arc<LogCounters>,
}

impl LogBuffer {
    pub fn new(region: Region, counters: Arc<LogCounters>, max_payload: usize) -> Self {
        let capacity = region.len() as u64;
        assert!(capacity.is_power_of_two(), "capacity must be a power of two");
        assert!(capacity <= 1 << 31, "length commit word is u32");
        let max_claim = 2 * align_frame_len(HEADER_LEN + max_payload) as u64;
        assert!(
            capacity >= 4 * max_claim,
            "capacity too small for max_payload (need >= 4x max claim)"
        );
        Self { region, capacity, mask: capacity - 1, max_payload, counters }
    }

    #[inline]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    #[inline]
    pub fn counters(&self) -> &Arc<LogCounters> {
        &self.counters
    }

    #[inline]
    pub fn max_payload(&self) -> usize {
        self.max_payload
    }

    /// Worst-case single-append write footprint span: padding (< one aligned
    /// max frame, since padding is only emitted when the frame doesn't fit
    /// the space before the wrap) + the frame itself. Used by validated
    /// readers (Task 4) as their safety margin.
    #[allow(dead_code)] // consumed by the validated reader in Task 4
    #[inline]
    pub(crate) fn max_claim(&self) -> u64 {
        2 * align_frame_len(HEADER_LEN + self.max_payload) as u64
    }

    #[inline]
    fn offset(&self, pos: u64) -> usize {
        (pos & self.mask) as usize
    }

    /// The u32 commit word at a frame offset. Offsets are 32-aligned, which
    /// satisfies AtomicU32's alignment.
    #[inline]
    pub(crate) fn commit_word(&self, off: usize) -> &AtomicU32 {
        debug_assert_eq!(off % frame::FRAME_ALIGNMENT, 0);
        // SAFETY: off < capacity (masked), 4-byte aligned, points into the
        // region for its whole lifetime; concurrent access only via atomics.
        unsafe { AtomicU32::from_ptr(self.region.ptr_at(off).cast::<u32>()) }
    }

    /// Contiguous committed whole frames starting at `from`, bounded by the
    /// append counter, the wrap point, and (softly) `max_bytes` — the result
    /// contains at least one whole frame if any is available, and never cuts
    /// a frame in half. CONTRACT: only the archive (the durability gate
    /// holder) may call this; the returned slice is protected from overwrite
    /// by the appender's gate against `durable`.
    pub fn recordable_slice(&self, from: u64, max_bytes: usize) -> &[u8] {
        let append = self.counters.append.load_acquire();
        if append <= from {
            return &[];
        }
        let off = self.offset(from);
        let hard = (append - from).min(self.capacity - off as u64);
        // Frame-walk to trim to whole frames within max_bytes (>= 1 frame).
        // Everything in [from, append) is committed, so plain length reads
        // are safe (the acquire on `append` ordered them).
        let mut end = 0u64;
        while end < hard {
            let len = u32::from_le_bytes(
                // SAFETY: off+end within capacity (end < hard <= capacity-off).
                unsafe {
                    std::slice::from_raw_parts(self.region.ptr_at(off + end as usize), 4)
                }
                .try_into()
                .unwrap(),
            );
            let aligned = align_frame_len(len as usize) as u64;
            debug_assert!(aligned > 0 && end + aligned <= hard);
            if end > 0 && end + aligned > max_bytes as u64 {
                break;
            }
            end += aligned;
        }
        // SAFETY: [from, from+end) is committed, contiguous in the region,
        // and gate-protected from overwrite while the archive holds it.
        unsafe { std::slice::from_raw_parts(self.region.ptr_at(off), end as usize) }
    }
}

/// The single writer. On the leader this is driven by the consensus agent;
/// M1 drives it directly. NOT Sync — exactly one appender per buffer.
pub struct Appender {
    buffer: Arc<LogBuffer>,
    pos: u64,
    cached_durable: u64,
    leadership_term_id: u32,
}

impl Appender {
    pub fn new(buffer: Arc<LogBuffer>, leadership_term_id: u32) -> Self {
        let pos = buffer.counters.append.load_acquire();
        let cached_durable = buffer.counters.durable.load_acquire();
        Self { buffer, pos, cached_durable, leadership_term_id }
    }

    #[inline]
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// Append one message frame; returns its position. `WouldOverrun` is
    /// retryable (backpressure), `PayloadTooLarge` is not.
    pub fn append(
        &mut self,
        session_id: u64,
        correlation_id: u64,
        payload: &[u8],
    ) -> Result<u64, AppendError> {
        if payload.len() > self.buffer.max_payload {
            return Err(AppendError::PayloadTooLarge);
        }
        let total = HEADER_LEN + payload.len();
        let aligned = align_frame_len(total) as u64;
        let b = &self.buffer;

        let off = b.offset(self.pos);
        let to_end = b.capacity - off as u64;
        let pad = if aligned > to_end { to_end } else { 0 };
        let end = self.pos + pad + aligned;

        // The one hard gate: never claim past durable + capacity.
        if end > self.cached_durable + b.capacity {
            self.cached_durable = b.counters.durable.load_acquire();
            if end > self.cached_durable + b.capacity {
                return Err(AppendError::WouldOverrun);
            }
        }

        let frame_pos = if pad > 0 {
            self.write_padding(off, pad as u32);
            self.pos + pad
        } else {
            self.pos
        };
        let foff = b.offset(frame_pos);

        // SAFETY (all raw writes): within capacity; bytes in [append,
        // durable+capacity) are writer-owned per the gate; ordering via the
        // commit word + append counter release stores below.
        unsafe {
            std::ptr::copy_nonoverlapping(
                payload.as_ptr(),
                b.region.ptr_at(foff + HEADER_LEN),
                payload.len(),
            );
            let hdr = std::slice::from_raw_parts_mut(b.region.ptr_at(foff), HEADER_LEN);
            frame::write_header_except_length(
                hdr,
                &FrameHeader {
                    length: 0,
                    frame_type: FRAME_TYPE_MESSAGE,
                    flags: 0,
                    leadership_term_id: self.leadership_term_id,
                    session_id,
                    correlation_id,
                },
            );
        }
        b.commit_word(foff).store(total as u32, Ordering::Release);

        self.pos = end;
        b.counters.append.store_release(self.pos);
        Ok(frame_pos)
    }

    /// Padding frame: header only; `length` spans to the buffer end.
    fn write_padding(&self, off: usize, pad_len: u32) {
        let b = &self.buffer;
        // SAFETY: as in append().
        unsafe {
            let hdr = std::slice::from_raw_parts_mut(b.region.ptr_at(off), HEADER_LEN);
            frame::write_header_except_length(
                hdr,
                &FrameHeader {
                    length: 0,
                    frame_type: FRAME_TYPE_PADDING,
                    flags: 0,
                    leadership_term_id: self.leadership_term_id,
                    session_id: 0,
                    correlation_id: 0,
                },
            );
        }
        b.commit_word(off).store(pad_len, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::counters::LogCounters;
    use crate::region::Region;
    use std::sync::Arc;
    use uc_protocol::v2::frame::{
        FRAME_TYPE_MESSAGE, FRAME_TYPE_PADDING, HEADER_LEN, read_header,
    };

    const CAP: u64 = 4096;

    fn buf() -> (Arc<LogBuffer>, Arc<LogCounters>) {
        let counters = Arc::new(LogCounters::new());
        let b = Arc::new(LogBuffer::new(
            Region::heap_zeroed(CAP as usize),
            Arc::clone(&counters),
            256, // max_payload for tests
        ));
        (b, counters)
    }

    #[test]
    fn append_then_recordable_roundtrip() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 3);
        let pos = a.append(11, 42, b"hello world!").unwrap();
        assert_eq!(pos, 0);
        // 32 header + 12 payload = 44 -> aligned 64
        assert_eq!(a.position(), 64);
        assert_eq!(c.append.load_acquire(), 64);

        let s = b.recordable_slice(0, 1 << 20);
        assert_eq!(s.len(), 64);
        let h = read_header(s);
        assert_eq!(h.length, (HEADER_LEN + 12) as u32);
        assert_eq!(h.frame_type, FRAME_TYPE_MESSAGE);
        assert_eq!(h.leadership_term_id, 3);
        assert_eq!(h.session_id, 11);
        assert_eq!(h.correlation_id, 42);
        assert_eq!(&s[HEADER_LEN..HEADER_LEN + 12], b"hello world!");
    }

    #[test]
    fn recordable_slice_is_bounded_and_frame_aligned() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1);
        for i in 0..4 {
            a.append(1, i, &[0u8; 64]).unwrap(); // 96 B frames
        }
        // max_bytes cuts mid-frame at 200 -> trimmed to 2 whole frames (192)
        let s = b.recordable_slice(0, 200);
        assert_eq!(s.len(), 192);
        // always returns at least one whole frame even if max_bytes is tiny
        let s = b.recordable_slice(0, 8);
        assert_eq!(s.len(), 96);
        // empty when caught up
        assert_eq!(b.recordable_slice(4 * 96, 1 << 20).len(), 0);
    }

    #[test]
    fn wrap_emits_padding_and_slice_stops_at_wrap() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1);
        // fill to 4032: 42 frames of 96 B
        for i in 0..42 {
            a.append(1, i, &[0u8; 64]).unwrap();
        }
        assert_eq!(a.position(), 4032);
        // next 96 B frame doesn't fit in the remaining 64 -> 64 B padding + frame at 4096
        c.durable.store_release(4032); // let the gate breathe
        let pos = a.append(1, 99, &[0u8; 64]).unwrap();
        assert_eq!(pos, 4096);
        assert_eq!(a.position(), 4192);

        // slice from 4032 stops at the wrap: just the 64 B padding frame
        c.durable.store_release(4032);
        let s = b.recordable_slice(4032, 1 << 20);
        assert_eq!(s.len(), 64);
        let h = read_header(s);
        assert_eq!(h.frame_type, FRAME_TYPE_PADDING);
        assert_eq!(h.length, 64);
        // and the next slice (post-wrap) starts with the message frame
        let s = b.recordable_slice(4096, 1 << 20);
        assert_eq!(s.len(), 96);
        assert_eq!(read_header(s).correlation_id, 99);
    }

    #[test]
    fn overrun_gate_blocks_until_durable_advances() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1);
        // durable stays 0: we can fill exactly one capacity, no more
        for i in 0..42 {
            a.append(1, i, &[0u8; 64]).unwrap();
        }
        // 4032 used; next append needs padding(64) + frame(96) -> end 4192 > 0 + 4096
        assert_eq!(a.append(1, 500, &[0u8; 64]).unwrap_err(), AppendError::WouldOverrun);
        // archive "records" one frame -> gate opens exactly enough
        c.durable.store_release(96);
        assert_eq!(a.append(1, 500, &[0u8; 64]).unwrap(), 4096);
        // and closes again
        assert_eq!(a.append(1, 501, &[0u8; 64]).unwrap_err(), AppendError::WouldOverrun);
    }

    #[test]
    fn payload_too_large_is_rejected() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1);
        assert_eq!(a.append(1, 1, &[0u8; 257]).unwrap_err(), AppendError::PayloadTooLarge);
    }
}
