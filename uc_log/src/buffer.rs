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

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use uc_protocol::v2::frame::{
    self, FRAME_TYPE_CONFIG, FRAME_TYPE_MESSAGE, FRAME_TYPE_NEW_TERM, FRAME_TYPE_PADDING,
    FRAME_TYPE_SCHEDULE_TABLE, FRAME_TYPE_TIMER, FrameHeader, HEADER_LEN, TIMER_BODY_LEN,
    TimerBody, align_frame_len,
};

use crate::cnc::CncPage;
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

#[derive(Debug)]
pub enum FrameRead {
    /// Frame copied into `out` (header + payload, unaligned length).
    Frame(FrameHeader),
    /// `pos` is at or beyond the append counter.
    NotCommitted,
    /// The frame's bytes may have been overwritten (reader lagged more than
    /// capacity − max_claim behind). Fall back to journal replay.
    Overrun,
}

/// Result payload of a successful `read_run_validated`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunRead {
    /// Bytes copied into `out`.
    pub bytes: usize,
    /// Stream positions consumed (≥ `bytes`; strictly greater when the run
    /// ends in a padding frame whose span exceeds its 32-byte header — a
    /// 32-byte padding span gives advance == bytes).
    pub advance: u64,
}

#[derive(Debug)]
pub enum SliceRead {
    Run(RunRead),
    /// `from` is at or beyond the append counter.
    NotCommitted,
    /// The run's bytes may have been overwritten (reader lagged more than
    /// capacity − max_claim behind), or `from` predates a restart prime.
    /// Fall back to journal replay.
    Overrun,
}

pub struct LogBuffer {
    region: Region,
    capacity: u64,
    mask: u64,
    max_payload: usize,
    /// The shared cnc v2 page: the buffer's position counters
    /// ([`LogCounters`]) live cast onto it (`cnc.counters()`), so every
    /// process mapping the page coordinates over the same atomics (M5).
    cnc: Arc<CncPage>,
}

/// Post-M7 follow-up: an impossible length word inside the committed region
/// `[from, append)` during `recordable_slice`'s frame walk. The committed
/// region is immutable until recorded, so this is a recorder-side invariant
/// break (torn write, mis-primed `from`, or memory-ordering bug) — the
/// archive must fail-stop loudly rather than record a malformed block
/// (previously a `debug_assert!`, i.e. silent garbage in release builds —
/// the leading upstream suspect for the once-seen `Replay::next` panic).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordableCorrupt {
    pub from: u64,
    pub append: u64,
    pub end: u64,
    pub claimed_len: u32,
}

impl LogBuffer {
    pub fn new(region: Region, cnc: Arc<CncPage>, max_payload: usize) -> Self {
        let capacity = region.len() as u64;
        assert!(
            capacity.is_power_of_two(),
            "capacity must be a power of two"
        );
        assert!(capacity <= 1 << 31, "length commit word is u32");
        let max_claim = 2 * align_frame_len(HEADER_LEN + max_payload) as u64;
        assert!(
            capacity >= 4 * max_claim,
            "capacity too small for max_payload (need >= 4x max claim)"
        );
        Self {
            region,
            capacity,
            mask: capacity - 1,
            max_payload,
            cnc,
        }
    }

    #[inline]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// The position counters (cast onto the shared cnc page). Returns a
    /// borrowed reference — call-sites read/write the atomics through it.
    #[inline]
    pub fn counters(&self) -> &LogCounters {
        self.cnc.counters()
    }

    /// The shared cnc v2 page this buffer's counters live on. Cloned by agents
    /// that publish other page fields (consensus status, service progress).
    #[inline]
    pub fn cnc(&self) -> &Arc<CncPage> {
        &self.cnc
    }

    #[inline]
    pub fn max_payload(&self) -> usize {
        self.max_payload
    }

    /// Create (or truncate) the buffer file at `capacity` bytes and map it.
    pub fn create_file(
        path: &Path,
        capacity: u64,
        cnc: Arc<CncPage>,
        max_payload: usize,
    ) -> Result<Self, std::io::Error> {
        // In-place recreate safe (never shrinks, zeros via fallocate
        // ZERO_RANGE, which also RESERVES the blocks so no later page fault
        // can hit ENOSPC) — see uc_protocol's `create_shared_backing_file`
        // for both SIGBUS contracts. Cold in practice (restarts OPEN a
        // matching buffer), hardened regardless.
        let file = uc_protocol::ring::create_shared_backing_file(path, capacity)?;
        // SAFETY: exclusive logical ownership per the instance-dir contract
        // (one node per instance dir; instance.lock arrives with uc_node).
        let m = unsafe { memmap2::MmapMut::map_mut(&file)? };
        Ok(Self::new(Region::from_mmap(m), cnc, max_payload))
    }

    /// Map an existing buffer file; capacity = file length. Reuse preserves the
    /// ring bytes below `durable` across a node restart (free NAK-serving
    /// prefill) — the node only opens (vs. creates) when the file already
    /// matches the configured capacity.
    pub fn open_file(
        path: &Path,
        cnc: Arc<CncPage>,
        max_payload: usize,
    ) -> Result<Self, std::io::Error> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        // SAFETY: see create_file.
        let m = unsafe { memmap2::MmapMut::map_mut(&file)? };
        Ok(Self::new(Region::from_mmap(m), cnc, max_payload))
    }

    /// Worst-case single-append write footprint span: padding (< one aligned
    /// max frame, since padding is only emitted when the frame doesn't fit
    /// the space before the wrap) + the frame itself. Used by validated
    /// readers (Task 4) as their safety margin.
    #[inline]
    pub(crate) fn max_claim(&self) -> u64 {
        2 * align_frame_len(HEADER_LEN + self.max_payload) as u64
    }

    #[inline]
    pub(crate) fn region(&self) -> &Region {
        &self.region
    }

    #[inline]
    pub(crate) fn offset(&self, pos: u64) -> usize {
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

    /// DIAGNOSTIC (investigation of the nightly `elle_partition` archive
    /// fail-stop): a human-readable post-mortem for a [`RecordableCorrupt`] —
    /// the four counters, the region offset, a hexdump around `from`, and the
    /// decisive question: is `from` a frame START, or does a frame beginning
    /// BELOW it walk over it (i.e. the archive's cursor is mid-frame)?
    pub fn corrupt_report(&self, c: &RecordableCorrupt) -> String {
        use std::fmt::Write as _;
        let ctr = self.cnc.counters();
        let off = self.offset(c.from);
        let mut s = String::new();
        let _ = writeln!(
            s,
            "recordable_slice corrupt: from={} append={} end={} claimed_len={} \
             (off={off}, capacity={}, wrapped={})",
            c.from,
            c.append,
            c.end,
            c.claimed_len,
            self.capacity,
            c.from >= self.capacity,
        );
        let _ = writeln!(
            s,
            "  counters: append={} durable={} sent={} commit={}",
            ctr.append.load_acquire(),
            ctr.durable.load_acquire(),
            ctr.sent.load_acquire(),
            ctr.commit.load_acquire(),
        );
        // Which frame, if any, covers `from`? Scan 32-aligned candidates below
        // it: a candidate whose own length walks PAST `from` proves the cursor
        // is mid-frame (the read word was payload, not a length).
        let mut straddler = None;
        let mut k = 1u64;
        while k <= 64 && c.from >= k * frame::FRAME_ALIGNMENT as u64 {
            let cand = c.from - k * frame::FRAME_ALIGNMENT as u64;
            let coff = self.offset(cand);
            let len = self.commit_word(coff).load(Ordering::Acquire);
            if (len as usize) >= HEADER_LEN {
                let end = cand + align_frame_len(len as usize) as u64;
                if end > c.from {
                    straddler = Some((cand, len, end));
                }
                // A frame that ends at or before `from` bounds the scan: any
                // candidate below it is that frame's payload, not a header.
                if end <= c.from {
                    break;
                }
            }
            k += 1;
        }
        match straddler {
            Some((pos, len, end)) => {
                let _ = writeln!(
                    s,
                    "  MID-FRAME: a frame at {pos} (len={len}) spans past `from` \
                     and ends at {end}"
                );
            }
            None => {
                let _ = writeln!(
                    s,
                    "  no straddling frame found below `from` in 64 slots \
                     (bytes look unwritten or the prefix is garbage too)"
                );
            }
        }
        let lo = off.saturating_sub(64);
        let hi = (off + 128).min(self.region.len());
        let _ = writeln!(s, "  hexdump [{lo}, {hi}) (`from` at off={off}):");
        // SAFETY: [lo, hi) is inside the mapped region.
        let bytes = unsafe { std::slice::from_raw_parts(self.region.ptr_at(lo), hi - lo) };
        for (i, chunk) in bytes.chunks(32).enumerate() {
            let hex: Vec<String> = chunk.iter().map(|b| format!("{b:02x}")).collect();
            let _ = writeln!(s, "    {:>8}: {}", lo + i * 32, hex.join(" "));
        }
        let fpos = c.from + c.end;
        let foff = self.offset(fpos);
        let flo = foff.saturating_sub(64);
        let fhi = (foff + 96).min(self.region.len());
        let _ = writeln!(
            s,
            "  FAILING FRAME at pos={fpos} (off={foff}); hexdump [{flo}, {fhi}):"
        );
        // SAFETY: [flo, fhi) is inside the mapped region.
        let fbytes = unsafe { std::slice::from_raw_parts(self.region.ptr_at(flo), fhi - flo) };
        for (i, chunk) in fbytes.chunks(32).enumerate() {
            let hex: Vec<String> = chunk.iter().map(|b| format!("{b:02x}")).collect();
            let _ = writeln!(s, "    {:>8}: {}", flo + i * 32, hex.join(" "));
        }
        // ZERO-COST FORENSICS (the hot path carries no instrumentation — any
        // in-process tracing there hid the race: 0 hits in 36 traced runs
        // against 3 in 14 untraced). Every frame header carries its term, so
        // the buffer itself records which term wrote each region.
        let _ = writeln!(s, "  frame walk from `from` (pos len type term):");
        let mut w = 0u64;
        let mut walked = Vec::new();
        while w < c.end {
            let o = self.offset(c.from + w);
            let len = self.commit_word(o).load(Ordering::Acquire);
            if (len as usize) < HEADER_LEN {
                break;
            }
            let h = frame::read_header(unsafe {
                std::slice::from_raw_parts(self.region.ptr_at(o), HEADER_LEN)
            });
            walked.push(format!(
                "    {} len={} type={} term={}",
                c.from + w,
                len,
                h.frame_type,
                h.leadership_term_id
            ));
            w += align_frame_len(len as usize) as u64;
        }
        let _ = writeln!(
            s,
            "    ({} frames tile [from, from+{}))",
            walked.len(),
            c.end
        );
        for line in walked.iter().take(3) {
            let _ = writeln!(s, "{line}");
        }
        if walked.len() > 6 {
            let _ = writeln!(s, "    ...");
        }
        for line in walked.iter().skip(walked.len().saturating_sub(3)) {
            let _ = writeln!(s, "{line}");
        }
        // Where does the NEXT parseable frame start, and whose term is it? If
        // the over-claimed region is old-term leftovers, the terms differ.
        let _ = writeln!(s, "  forward scan from the failure for a parseable frame:");
        let mut probe = 0u64;
        let mut found = 0;
        while probe < 4096 && found < 4 {
            let pos = c.from + c.end + probe;
            if pos >= c.append {
                let _ = writeln!(s, "    (reached append={} after {probe} B)", c.append);
                break;
            }
            let o = self.offset(pos);
            let len = self.commit_word(o).load(Ordering::Acquire);
            if (len as usize) >= HEADER_LEN
                && pos + align_frame_len(len as usize) as u64 <= c.append
            {
                let h = frame::read_header(unsafe {
                    std::slice::from_raw_parts(self.region.ptr_at(o), HEADER_LEN)
                });
                let _ = writeln!(
                    s,
                    "    +{probe} B: pos={pos} len={len} type={} term={}",
                    h.frame_type, h.leadership_term_id
                );
                found += 1;
            }
            probe += frame::FRAME_ALIGNMENT as u64;
        }
        s
    }

    /// Contiguous committed whole frames starting at `from`, bounded by the
    /// append counter, the wrap point, and (softly) `max_bytes` — the result
    /// contains at least one whole frame if any is available, and never cuts
    /// a frame in half. CONTRACT: only the archive (the durability gate
    /// holder) may call this; the returned slice is protected from overwrite
    /// by the appender's gate against `durable`.
    pub fn recordable_slice(
        &self,
        from: u64,
        max_bytes: usize,
    ) -> Result<&[u8], RecordableCorrupt> {
        let append = self.cnc.counters().append.load_acquire();
        if append <= from {
            return Ok(&[]);
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
                unsafe { std::slice::from_raw_parts(self.region.ptr_at(off + end as usize), 4) }
                    .try_into()
                    .unwrap(),
            );
            let aligned = align_frame_len(len as usize) as u64;
            // Frames never span the wrap and padding fills exactly to it, so
            // `end + aligned > hard` — whether `hard` is the wrap clamp or the
            // append clamp — is genuinely impossible for intact committed
            // frames; a hit here means the length word itself is torn/corrupt.
            // A NONZERO length below `HEADER_LEN` (e.g. a torn `1`) aligns up to
            // 32 and would otherwise slip past BOTH disjuncts as a "valid"
            // sub-header frame — every legitimately committed word is a message/
            // config/new-term/padding frame of length >= HEADER_LEN, so guard it
            // exactly as `Replay::next`/`observe_terms` do (Task 3 H1 audit).
            if (len as usize) < HEADER_LEN || end + aligned > hard {
                return Err(RecordableCorrupt {
                    from,
                    append,
                    end,
                    claimed_len: len,
                });
            }
            if end > 0 && end + aligned > max_bytes as u64 {
                break;
            }
            end += aligned;
        }
        // SAFETY: [from, from+end) is committed, contiguous in the region,
        // and gate-protected from overwrite while the archive holds it.
        Ok(unsafe { std::slice::from_raw_parts(self.region.ptr_at(off), end as usize) })
    }

    /// Read one frame at `pos` with overwrite validation, for lagging /
    /// position-addressed readers (M2 NAK retransmit, M5 service). Copies the
    /// frame into `out` then re-checks the append counter: if the appender
    /// could have advanced into (or near) this frame's bytes, returns
    /// `Overrun` (caller falls back to journal replay). The margin is
    /// `max_claim()` because an in-flight append's writes (padding header +
    /// frame) are not yet reflected in the counter.
    ///
    /// CONTRACT: `pos` must be a frame start (positions come from append
    /// results / frame walks / archive block bases); a mid-frame `pos`
    /// misreads a payload byte as the length word (bounded by the safety
    /// analysis above, but garbage).
    pub fn read_frame_validated(&self, pos: u64, out: &mut Vec<u8>) -> FrameRead {
        let append = self.cnc.counters().append.load_acquire();
        if pos >= append {
            return FrameRead::NotCommitted;
        }
        if append + self.max_claim() > pos + self.capacity {
            return FrameRead::Overrun;
        }
        let off = self.offset(pos);
        let len = self.commit_word(off).load(Ordering::Acquire) as usize;
        if len == 0 {
            // A zero commit word below `append` means these bytes were never
            // written to THIS buffer file: the counters were primed past them
            // after a restart and the frames live only in the journal
            // (LogCounters::prime contract). Same remedy as a lap overrun.
            return FrameRead::Overrun;
        }
        debug_assert!(len >= 4 && align_frame_len(len) as u64 <= self.capacity - off as u64);
        out.clear();
        // SAFETY: [off, off+len) within capacity (frames never span the wrap).
        out.extend_from_slice(unsafe { std::slice::from_raw_parts(self.region.ptr_at(off), len) });
        // Seqlock discipline: an acquire fence orders the preceding
        // non-atomic copy before the re-check below. Without it the check is
        // only sound on TSO archs (e.g. x86) that happen not to reorder
        // loads past later loads; on weaker memory models the re-load of
        // `append` could be hoisted above (or racing) the copy above.
        std::sync::atomic::fence(Ordering::Acquire);
        // Re-validate: did the appender advance into our margin during the copy?
        let append_after = self.cnc.counters().append.load_acquire();
        if append_after + self.max_claim() > pos + self.capacity {
            return FrameRead::Overrun;
        }
        FrameRead::Frame(frame::read_header(out))
    }

    /// Batch validated read for the sender (M2): copy a run of contiguous
    /// committed whole frames starting at `from` (a frame start) into `out`.
    /// The run never crosses the wrap; a padding frame is copied header-only
    /// (32 B) but advances its full aligned span and ends the run. Always
    /// returns at least one frame if one is available (a frame larger than
    /// `max_bytes` is returned alone — the sender's MTU config assert makes
    /// that impossible in practice). Seqlock discipline as in
    /// `read_frame_validated`: pre/post overwrite-margin checks around the
    /// copy with an acquire fence between.
    pub fn read_run_validated(&self, from: u64, max_bytes: usize, out: &mut Vec<u8>) -> SliceRead {
        let append = self.cnc.counters().append.load_acquire();
        if from >= append {
            return SliceRead::NotCommitted;
        }
        if append + self.max_claim() > from + self.capacity {
            return SliceRead::Overrun;
        }
        let off = self.offset(from);
        let hard = (append - from).min(self.capacity - off as u64);
        out.clear();
        let mut walked = 0u64; // stream advance
        let mut copied = 0usize; // bytes in out
        while walked < hard {
            let o = off + walked as usize;
            let len = self.commit_word(o).load(Ordering::Acquire) as usize;
            if len == 0 {
                break; // restart-primed tail: no bytes in this buffer
            }
            let aligned = align_frame_len(len) as u64;
            if aligned == 0 || walked + aligned > hard {
                break; // torn/overwritten length — post-check will decide
            }
            // SAFETY: o + 5 within capacity (aligned span checked above).
            let ftype = unsafe { *self.region.ptr_at(o + frame::OFF_TYPE) };
            let copy_len = if ftype == FRAME_TYPE_PADDING {
                HEADER_LEN
            } else {
                aligned as usize
            };
            if copied > 0 && copied + copy_len > max_bytes {
                break;
            }
            // SAFETY: [o, o+copy_len) within capacity; validated below.
            out.extend_from_slice(unsafe {
                std::slice::from_raw_parts(self.region.ptr_at(o), copy_len)
            });
            copied += copy_len;
            walked += aligned;
            if ftype == FRAME_TYPE_PADDING || copied >= max_bytes {
                break; // padding ends at the wrap
            }
        }
        // Seqlock re-check (see read_frame_validated for the fence rationale).
        std::sync::atomic::fence(Ordering::Acquire);
        let append_after = self.cnc.counters().append.load_acquire();
        if append_after + self.max_claim() > from + self.capacity {
            return SliceRead::Overrun;
        }
        if walked == 0 {
            // len == 0 at a committed position: primed-over-fresh-buffer.
            return SliceRead::Overrun;
        }
        SliceRead::Run(RunRead {
            bytes: copied,
            advance: walked,
        })
    }
}

/// The single writer. On the leader this is driven by the consensus agent;
/// M1 drives it directly. `append` takes `&mut self` and the type is not
/// Clone; constructing more than one Appender per buffer is a
/// caller-contract violation (single-writer principle).
pub struct Appender {
    buffer: Arc<LogBuffer>,
    pos: u64,
    cached_durable: u64,
    leadership_term_id: u32,
    /// The pass's clock reading (`set_now`); a stamp is `max(now_ns, last_stamp)`.
    now_ns: u64,
    /// The last stamp written — the log's clock never goes backwards.
    last_stamp: u64,
}

impl Appender {
    /// `seed_stamp`: the highest stamp already on the log (the cnc `log_time_ns`
    /// word at leader open; `0` on a fresh cluster). Every stamp this appender
    /// writes is `>= seed_stamp`.
    pub fn new(buffer: Arc<LogBuffer>, leadership_term_id: u32, seed_stamp: u64) -> Self {
        let pos = buffer.cnc.counters().append.load_acquire();
        let cached_durable = buffer.cnc.counters().durable.load_acquire();
        Self {
            buffer,
            pos,
            cached_durable,
            leadership_term_id,
            now_ns: seed_stamp,
            last_stamp: seed_stamp,
        }
    }

    /// Once per leader pass (time-and-timers spec §3.2).
    pub fn set_now(&mut self, now_ns: u64) {
        self.now_ns = now_ns;
    }

    pub fn last_stamp(&self) -> u64 {
        self.last_stamp
    }

    #[inline]
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// Append one message frame; returns its position. `WouldOverrun` is
    /// retryable (backpressure), `PayloadTooLarge` is not.
    pub fn append(&mut self, client_id: u32, seq: u32, payload: &[u8]) -> Result<u64, AppendError> {
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
            self.cached_durable = b.cnc.counters().durable.load_acquire();
            if end > self.cached_durable + b.capacity {
                return Err(AppendError::WouldOverrun);
            }
        }

        // The clamp, inline (final-review I1): a `&mut self` method call here
        // would conflict with the shared borrow of `self.buffer`; writing the
        // field directly is a disjoint borrow and costs no Arc refcount traffic
        // per append. Non-strict — equal stamps are allowed (spec §3.2).
        let time_ns = self.now_ns.max(self.last_stamp);
        self.last_stamp = time_ns;

        let frame_pos = if pad > 0 {
            self.write_padding(off, pad as u32, time_ns);
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
                    client_id,
                    seq,
                    time_ns,
                },
            );
        }
        b.commit_word(foff).store(total as u32, Ordering::Release);

        self.pos = end;
        b.cnc.counters().append.store_release(self.pos);
        Ok(frame_pos)
    }

    /// Append the header-only `FRAME_TYPE_NEW_TERM` no-op frame that opens a
    /// leadership term (spec §6, M4): 32 bytes, stamped with this appender's
    /// leadership term, zero payload. The data plane streams it like any frame,
    /// the archive's term-observation walk records `(term, base)` at its
    /// position, and its quorum commit is what gates the new leader's serving
    /// (Raft §5.4.2). Returns its position. Same wrap/overrun discipline as
    /// `append`.
    pub fn append_new_term(&mut self) -> Result<u64, AppendError> {
        let total = HEADER_LEN; // header-only no-op frame
        let aligned = align_frame_len(total) as u64;
        let b = &self.buffer;

        let off = b.offset(self.pos);
        let to_end = b.capacity - off as u64;
        let pad = if aligned > to_end { to_end } else { 0 };
        let end = self.pos + pad + aligned;

        if end > self.cached_durable + b.capacity {
            self.cached_durable = b.cnc.counters().durable.load_acquire();
            if end > self.cached_durable + b.capacity {
                return Err(AppendError::WouldOverrun);
            }
        }

        // The clamp, inline (final-review I1): a `&mut self` method call here
        // would conflict with the shared borrow of `self.buffer`; writing the
        // field directly is a disjoint borrow and costs no Arc refcount traffic
        // per append. Non-strict — equal stamps are allowed (spec §3.2).
        let time_ns = self.now_ns.max(self.last_stamp);
        self.last_stamp = time_ns;

        let frame_pos = if pad > 0 {
            self.write_padding(off, pad as u32, time_ns);
            self.pos + pad
        } else {
            self.pos
        };
        let foff = b.offset(frame_pos);

        // SAFETY: as in `append` — within capacity, writer-owned by the gate.
        unsafe {
            let hdr = std::slice::from_raw_parts_mut(b.region.ptr_at(foff), HEADER_LEN);
            frame::write_header_except_length(
                hdr,
                &FrameHeader {
                    length: 0,
                    frame_type: FRAME_TYPE_NEW_TERM,
                    flags: 0,
                    leadership_term_id: self.leadership_term_id,
                    client_id: 0,
                    seq: 0,
                    time_ns,
                },
            );
        }
        b.commit_word(foff).store(total as u32, Ordering::Release);

        self.pos = end;
        b.cnc.counters().append.store_release(self.pos);
        Ok(frame_pos)
    }

    /// Append a `FRAME_TYPE_CONFIG` entry (M7, spec 2026-07-13): payload =
    /// `v2::config::encode_config` bytes, stamped with `term` — the caller's
    /// current leadership term, passed explicitly (rather than read off
    /// `self.leadership_term_id`) so the signature matches the config-append
    /// contract shared with the sim's model (`uc_sim::world`), which has no
    /// live `Appender` to carry it. In practice the caller always passes its
    /// own current term, so this is not observably different from the
    /// internal field. Returns the frame-END position — the adoption effect
    /// point (`ConfigRecord.position` semantics), UNLIKE `append`/
    /// `append_new_term` which return the frame START. Same wrap/overrun
    /// discipline as `append`.
    pub fn append_config(&mut self, term: u32, payload: &[u8]) -> Result<u64, AppendError> {
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
            self.cached_durable = b.cnc.counters().durable.load_acquire();
            if end > self.cached_durable + b.capacity {
                return Err(AppendError::WouldOverrun);
            }
        }

        // The clamp, inline (final-review I1): a `&mut self` method call here
        // would conflict with the shared borrow of `self.buffer`; writing the
        // field directly is a disjoint borrow and costs no Arc refcount traffic
        // per append. Non-strict — equal stamps are allowed (spec §3.2).
        let time_ns = self.now_ns.max(self.last_stamp);
        self.last_stamp = time_ns;

        let frame_pos = if pad > 0 {
            self.write_padding(off, pad as u32, time_ns);
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
                    frame_type: FRAME_TYPE_CONFIG,
                    flags: 0,
                    leadership_term_id: term,
                    client_id: 0,
                    seq: 0,
                    time_ns,
                },
            );
        }
        b.commit_word(foff).store(total as u32, Ordering::Release);

        self.pos = end;
        b.cnc.counters().append.store_release(self.pos);
        Ok(end)
    }

    /// Append a `FRAME_TYPE_SCHEDULE_TABLE` entry (time-and-timers plan 2):
    /// payload = the replicated schedule table's own encoding, stamped with
    /// `term` — the caller's current leadership term, passed explicitly for
    /// the same reason as `append_config`'s `term` parameter. `client_id =
    /// seq = 0`, stamped like every frame (`max(now, last)`). Returns the
    /// frame-END position, matching `append_config`'s convention (the
    /// adoption effect point), NOT the frame start. Same wrap/overrun/commit
    /// discipline as `append_config` — this is that method's body with the
    /// frame type swapped.
    pub fn append_schedule_table(&mut self, term: u32, payload: &[u8]) -> Result<u64, AppendError> {
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
            self.cached_durable = b.cnc.counters().durable.load_acquire();
            if end > self.cached_durable + b.capacity {
                return Err(AppendError::WouldOverrun);
            }
        }

        // The clamp, inline (final-review I1) — see `append_config`.
        let time_ns = self.now_ns.max(self.last_stamp);
        self.last_stamp = time_ns;

        let frame_pos = if pad > 0 {
            self.write_padding(off, pad as u32, time_ns);
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
                    frame_type: FRAME_TYPE_SCHEDULE_TABLE,
                    flags: 0,
                    leadership_term_id: term,
                    client_id: 0,
                    seq: 0,
                    time_ns,
                },
            );
        }
        b.commit_word(foff).store(total as u32, Ordering::Release);

        self.pos = end;
        b.cnc.counters().append.store_release(self.pos);
        Ok(end)
    }

    /// Append a TIMER frame (time-and-timers spec §4.2/§4.3). Stamped with the
    /// deadline, clamped to `last_stamp` — so `stamp > body.deadline_ns` means
    /// the timer is late. Returns `(frame_start, stamp)`.
    pub fn append_timer(&mut self, body: &TimerBody, flags: u8) -> Result<(u64, u64), AppendError> {
        let total = HEADER_LEN + TIMER_BODY_LEN;
        let aligned = align_frame_len(total) as u64;
        let b = &self.buffer;

        let off = b.offset(self.pos);
        let to_end = b.capacity - off as u64;
        let pad = if aligned > to_end { to_end } else { 0 };
        let end = self.pos + pad + aligned;

        // The one hard gate: never claim past durable + capacity.
        if end > self.cached_durable + b.capacity {
            self.cached_durable = b.cnc.counters().durable.load_acquire();
            if end > self.cached_durable + b.capacity {
                return Err(AppendError::WouldOverrun);
            }
        }

        // The clamp, inline (final-review I1) — see `append`. A TIMER frame is
        // stamped with its DEADLINE, clamped up to `last_stamp` when a previous
        // leader already stamped past it (spec §4.3: that is the `late` case).
        let time_ns = body.deadline_ns.max(self.last_stamp);
        self.last_stamp = time_ns;

        let frame_pos = if pad > 0 {
            self.write_padding(off, pad as u32, time_ns);
            self.pos + pad
        } else {
            self.pos
        };
        let foff = b.offset(frame_pos);

        // SAFETY (all raw writes): within capacity; bytes in [append,
        // durable+capacity) are writer-owned per the gate; ordering via the
        // commit word + append counter release stores below.
        unsafe {
            let dst = std::slice::from_raw_parts_mut(b.region.ptr_at(foff), aligned as usize);
            frame::write_header_except_length(
                dst,
                &FrameHeader {
                    length: 0,
                    frame_type: FRAME_TYPE_TIMER,
                    flags,
                    leadership_term_id: self.leadership_term_id,
                    client_id: 0,
                    seq: 0,
                    time_ns,
                },
            );
            frame::write_timer_body(&mut dst[HEADER_LEN..], body);
        }
        b.commit_word(foff).store(total as u32, Ordering::Release);

        self.pos = end;
        b.cnc.counters().append.store_release(self.pos);
        Ok((frame_pos, time_ns))
    }

    /// Padding frame: header only; `length` spans to the buffer end.
    fn write_padding(&self, off: usize, pad_len: u32, time_ns: u64) {
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
                    client_id: 0,
                    seq: 0,
                    time_ns,
                },
            );
        }
        b.commit_word(off).store(pad_len, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cnc::{CncMeta, CncPage};
    use crate::region::Region;
    use std::sync::Arc;
    use uc_protocol::v2::frame::{
        FLAG_TIMER_TABLE, FRAME_TYPE_CONFIG, FRAME_TYPE_MESSAGE, FRAME_TYPE_NEW_TERM,
        FRAME_TYPE_PADDING, FRAME_TYPE_SCHEDULE_TABLE, FRAME_TYPE_TIMER, HEADER_LEN,
        TIMER_BODY_LEN, TimerBody, read_header, read_timer_body,
    };

    const CAP: u64 = 4096;

    fn test_cnc() -> Arc<CncPage> {
        CncPage::heap(&CncMeta {
            node_id: 0,
            instance_id: 0,
            app_id: "test".into(),
            buffer_bytes: CAP,
            max_payload: 256,
            services: [None; uc_protocol::v2::cnc::CNC_MAX_SERVICES],
        })
    }

    fn buf() -> (Arc<LogBuffer>, Arc<CncPage>) {
        let cnc = test_cnc();
        let b = Arc::new(LogBuffer::new(
            Region::heap_zeroed(CAP as usize),
            Arc::clone(&cnc),
            256, // max_payload for tests
        ));
        (b, cnc)
    }

    #[test]
    fn append_then_recordable_roundtrip() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 3, 0);
        let pos = a.append(11, 42, b"hello world!").unwrap();
        assert_eq!(pos, 0);
        // 32 header + 12 payload = 44 -> aligned 64
        assert_eq!(a.position(), 64);
        assert_eq!(c.counters().append.load_acquire(), 64);

        let s = b.recordable_slice(0, 1 << 20).unwrap();
        assert_eq!(s.len(), 64);
        let h = read_header(s);
        assert_eq!(h.length, (HEADER_LEN + 12) as u32);
        assert_eq!(h.frame_type, FRAME_TYPE_MESSAGE);
        assert_eq!(h.leadership_term_id, 3);
        assert_eq!(h.client_id, 11);
        assert_eq!(h.seq, 42);
        assert_eq!(&s[HEADER_LEN..HEADER_LEN + 12], b"hello world!");
    }

    #[test]
    fn append_new_term_is_header_only_32_bytes() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 7, 0);
        let pos = a.append_new_term().unwrap();
        assert_eq!(pos, 0);
        assert_eq!(a.position(), 32, "the NewTerm frame is header-only (32 B)");
        assert_eq!(c.counters().append.load_acquire(), 32);
        let s = b.recordable_slice(0, 1 << 20).unwrap();
        assert_eq!(s.len(), 32);
        let h = read_header(s);
        assert_eq!(h.length, HEADER_LEN as u32);
        assert_eq!(h.frame_type, FRAME_TYPE_NEW_TERM);
        assert_eq!(h.leadership_term_id, 7);
        // a data frame after it opens exactly at base + 32
        let dpos = a.append(1, 0, &[0u8; 64]).unwrap();
        assert_eq!(dpos, 32);
    }

    #[test]
    fn append_config_records_type_term_and_payload_returns_frame_end() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 7, 0);
        let payload = b"cfg-bytes-v1";
        // Stamped with the PASSED term (9), not the appender's own (7) —
        // pins the explicit-term signature.
        let end = a.append_config(9, payload).unwrap();
        // 32 header + 12 payload = 44 -> aligned 64
        assert_eq!(
            end, 64,
            "returns the frame-END position, unlike append/append_new_term"
        );
        assert_eq!(a.position(), 64);
        assert_eq!(c.counters().append.load_acquire(), 64);

        let s = b.recordable_slice(0, 1 << 20).unwrap();
        assert_eq!(s.len(), 64);
        let h = read_header(s);
        assert_eq!(h.length, (HEADER_LEN + payload.len()) as u32);
        assert_eq!(h.frame_type, FRAME_TYPE_CONFIG);
        assert_eq!(h.leadership_term_id, 9);
        assert_eq!(&s[HEADER_LEN..HEADER_LEN + payload.len()], payload);

        // a data frame after it opens exactly at the returned frame-end
        let dpos = a.append(1, 0, &[0u8; 64]).unwrap();
        assert_eq!(dpos, 64);
    }

    #[test]
    fn append_schedule_table_is_a_stamped_type_6_frame_returning_the_end() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 4, 0);
        a.set_now(1_000);
        let end = a.append_schedule_table(4, b"table-bytes").unwrap();
        assert_eq!(
            end, 64,
            "32 header + 11 payload -> aligned 64; END returned"
        );
        let s = b.recordable_slice(0, 64).unwrap();
        let h = read_header(s);
        assert_eq!(h.frame_type, FRAME_TYPE_SCHEDULE_TABLE);
        assert_eq!((h.client_id, h.seq, h.flags), (0, 0, 0));
        assert_eq!(h.time_ns, 1_000);
        assert_eq!(&s[HEADER_LEN..HEADER_LEN + 11], b"table-bytes");
        assert_eq!(a.last_stamp(), 1_000);
    }

    #[test]
    fn recordable_slice_is_bounded_and_frame_aligned() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1, 0);
        for i in 0..4 {
            a.append(1, i, &[0u8; 64]).unwrap(); // 96 B frames
        }
        // max_bytes cuts mid-frame at 200 -> trimmed to 2 whole frames (192)
        let s = b.recordable_slice(0, 200).unwrap();
        assert_eq!(s.len(), 192);
        // always returns at least one whole frame even if max_bytes is tiny
        let s = b.recordable_slice(0, 8).unwrap();
        assert_eq!(s.len(), 96);
        // empty when caught up
        assert_eq!(b.recordable_slice(4 * 96, 1 << 20).unwrap().len(), 0);
    }

    /// Post-M7 follow-up: a torn/corrupt length word inside the committed
    /// region must surface as `Err(RecordableCorrupt)`, not a debug-only
    /// assert (silent garbage in release builds) or an OOB slice. Poisons the
    /// second frame's length word through the same `commit_word` atomic the
    /// appender itself uses (an absurd claimed length far past the committed
    /// span), mirroring a torn write / mis-primed `from` / ordering bug.
    #[test]
    fn recordable_slice_surfaces_torn_length_word() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1, 0);
        a.append(1, 0, &[0u8; 64]).unwrap(); // frame 1: 96 B, at offset 0
        a.append(1, 1, &[0u8; 64]).unwrap(); // frame 2: 96 B, at offset 96
        let second_frame_off = 96usize;
        b.commit_word(second_frame_off)
            .store(1 << 20, Ordering::Release); // absurd len
        let err = b.recordable_slice(0, 1 << 20).unwrap_err();
        assert_eq!(err.claimed_len, 1 << 20);
        assert_eq!(err.from, 0);
        assert_eq!(err.append, 192);
        assert!(
            err.end > 0,
            "the first, intact frame was walked before the tear"
        );
    }

    /// Post-M7 follow-up (Task 3 H1 audit): a torn length word whose value is
    /// NONZERO but below `HEADER_LEN` (e.g. 1) must ALSO surface as
    /// `Err(RecordableCorrupt)`. `align_frame_len(1) == 32 != 0`, so it passes
    /// the `aligned == 0` disjunct, and `end + 32 <= hard` when it is the last
    /// word in the committed region, so it passes the overrun disjunct too — it
    /// would otherwise be walked as a "valid" sub-header frame and recorded into
    /// a malformed block (the downstream `Replay::next` OOB the whole task is
    /// about). Mirrors `Replay::next`/`observe_terms`'s `total < HEADER_LEN`
    /// guard, closing the one walk that lacked it.
    #[test]
    fn recordable_slice_surfaces_subheader_length_word() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1, 0);
        a.append(1, 0, &[0u8; 64]).unwrap(); // frame 1: 96 B at offset 0
        a.append_new_term().unwrap(); // frame 2: 32 B header-only at offset 96
        assert_eq!(a.position(), 128, "committed region is exactly [0, 128)");
        // Poison frame 2's length word to a nonzero sub-HEADER_LEN value. It is
        // the LAST word in [0, append), so end(96) + align(1)=32 == hard(128)
        // does NOT trip the overrun disjunct.
        b.commit_word(96).store(1, Ordering::Release);
        let err = b.recordable_slice(0, 1 << 20).unwrap_err();
        assert_eq!(err.claimed_len, 1);
        assert_eq!(
            err.end, 96,
            "the first, intact frame was walked before the tear"
        );
        assert_eq!(err.from, 0);
        assert_eq!(err.append, 128);
    }

    #[test]
    fn wrap_emits_padding_and_slice_stops_at_wrap() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1, 0);
        // fill to 4032: 42 frames of 96 B
        for i in 0..42 {
            a.append(1, i, &[0u8; 64]).unwrap();
        }
        assert_eq!(a.position(), 4032);
        // next 96 B frame doesn't fit in the remaining 64 -> 64 B padding + frame at 4096
        c.counters().durable.store_release(4032); // let the gate breathe
        let pos = a.append(1, 99, &[0u8; 64]).unwrap();
        assert_eq!(pos, 4096);
        assert_eq!(a.position(), 4192);

        // slice from 4032 stops at the wrap: just the 64 B padding frame
        c.counters().durable.store_release(4032);
        let s = b.recordable_slice(4032, 1 << 20).unwrap();
        assert_eq!(s.len(), 64);
        let h = read_header(s);
        assert_eq!(h.frame_type, FRAME_TYPE_PADDING);
        assert_eq!(h.length, 64);
        // and the next slice (post-wrap) starts with the message frame
        let s = b.recordable_slice(4096, 1 << 20).unwrap();
        assert_eq!(s.len(), 96);
        assert_eq!(read_header(s).seq, 99);
    }

    #[test]
    fn overrun_gate_blocks_until_durable_advances() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1, 0);
        // durable stays 0: we can fill exactly one capacity, no more
        for i in 0..42 {
            a.append(1, i, &[0u8; 64]).unwrap();
        }
        // 4032 used; next append needs padding(64) + frame(96) -> end 4192 > 0 + 4096
        assert_eq!(
            a.append(1, 500, &[0u8; 64]).unwrap_err(),
            AppendError::WouldOverrun
        );
        // archive "records" one frame -> gate opens exactly enough
        c.counters().durable.store_release(96);
        assert_eq!(a.append(1, 500, &[0u8; 64]).unwrap(), 4096);
        // and closes again
        assert_eq!(
            a.append(1, 501, &[0u8; 64]).unwrap_err(),
            AppendError::WouldOverrun
        );
    }

    /// M7 Task 7 (uc_node's admin path, mandatory review carry): a
    /// `WouldOverrun` from `append_config` must leave EXACTLY the pre-call
    /// state behind — no partial write, no frontier advance, no stray padding
    /// frame — so `uc_node::Consensus::propose_and_append`'s retry-whole
    /// contract (reply `status=2` and let `uc2ctl`/the follower's forward try
    /// again) is sound: the SAME config bytes re-appended on retry must land
    /// as the FIRST thing after the gate reopens, not after some already-
    /// written-but-unlinked debris. Pins the exact code path this task's
    /// review relies on: `append_config`'s overrun check (buffer.rs) runs
    /// strictly before it touches `self.pos`/writes any header/commit word.
    #[test]
    fn append_config_would_overrun_leaves_no_partial_state() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1, 0);
        // Fill to exactly capacity (durable stays 0): 42 frames of 96 B = 4032,
        // 64 B of headroom left — too little for a config frame (32 header +
        // 12 payload = 44 -> aligned 64, but the overrun gate compares against
        // durable + capacity = 4096, and 4032 + 64 == 4096 is NOT an overrun,
        // so pad by one more small append to land exactly on the boundary).
        for i in 0..42 {
            a.append(1, i, &[0u8; 64]).unwrap();
        }
        assert_eq!(a.position(), 4032);
        let pos_before = a.position();
        let append_before = c.counters().append.load_acquire();
        let slice_before = b.recordable_slice(0, 1 << 20).unwrap().to_vec();

        // A config payload needing padding + frame > the 64 B headroom: pad(64)
        // would land the frame at 4096, well past durable(0) + capacity(4096).
        let big_payload = vec![0u8; 200];
        assert_eq!(
            a.append_config(9, &big_payload).unwrap_err(),
            AppendError::WouldOverrun,
            "expected the config append to be gated by the overrun check"
        );

        // No partial state: position, the shared append counter, and every
        // recorded byte are BIT-FOR-BIT what they were before the failed call.
        assert_eq!(
            a.position(),
            pos_before,
            "WouldOverrun must not advance the appender's position"
        );
        assert_eq!(
            c.counters().append.load_acquire(),
            append_before,
            "WouldOverrun must not advance the shared append counter"
        );
        assert_eq!(
            b.recordable_slice(0, 1 << 20).unwrap().to_vec(),
            slice_before,
            "WouldOverrun must not have written any bytes (no stray padding/header)"
        );

        // The retry, once durable advances enough to open the gate, appends
        // cleanly at exactly the pre-failure position — proving the failed
        // attempt left nothing behind for the retry to trip over.
        c.counters().durable.store_release(4032);
        let end = a.append_config(9, &big_payload).unwrap();
        assert_eq!(
            end,
            pos_before + 64 /* pad */ + align_frame_len(HEADER_LEN + big_payload.len()) as u64
        );
    }

    #[test]
    fn payload_too_large_is_rejected() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1, 0);
        assert_eq!(
            a.append(1, 1, &[0u8; 257]).unwrap_err(),
            AppendError::PayloadTooLarge
        );
    }

    #[test]
    fn validated_read_roundtrip_and_not_committed() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 2, 0);
        a.append(9, 77, b"abc").unwrap();
        let mut out = Vec::new();
        match b.read_frame_validated(0, &mut out) {
            FrameRead::Frame(h) => {
                assert_eq!(h.seq, 77);
                assert_eq!(h.length as usize, HEADER_LEN + 3);
                assert_eq!(&out[HEADER_LEN..], b"abc");
            }
            other => panic!("expected Frame, got {other:?}"),
        }
        // beyond append -> NotCommitted
        assert!(matches!(
            b.read_frame_validated(64, &mut out),
            FrameRead::NotCommitted
        ));
    }

    #[test]
    fn primed_fresh_buffer_reads_overrun_not_garbage() {
        // Node restart: journal recovered to 2*CAP, buffer file recreated
        // (all zeros). Positions below the primed point exist only in the
        // journal — validated reads must degrade to Overrun (replay is the
        // fallback), not parse zeroed/stale bytes.
        let (b, c) = buf();
        c.counters().prime(2 * CAP);
        let mut out = Vec::new();
        // Both positions pass the lap-overrun margin check (>= append +
        // max_claim - capacity = 8192 + 576 - 4096 = 4672) and previously
        // fell through to the zero commit word.
        assert!(matches!(
            b.read_frame_validated(2 * CAP - 64, &mut out),
            FrameRead::Overrun
        ));
        assert!(matches!(
            b.read_frame_validated(4672, &mut out),
            FrameRead::Overrun
        ));
        // Post-restart appends still read fine.
        let mut a = Appender::new(Arc::clone(&b), 5, 0);
        a.append(1, 7, b"post-restart").unwrap();
        assert!(matches!(
            b.read_frame_validated(2 * CAP, &mut out),
            FrameRead::Frame(_)
        ));
    }

    #[test]
    fn validated_read_detects_overrun_after_wrap() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1, 0);
        // write ~3 capacities worth, letting the gate breathe by keeping
        // durable glued to append (as a healthy archive would)
        let mut n = 0u32;
        while a.position() < 3 * CAP {
            a.append(1, n, &[0u8; 64]).unwrap();
            c.counters().durable.store_release(a.position());
            n += 1;
        }
        // position 0 was overwritten laps ago
        let mut out = Vec::new();
        assert!(matches!(
            b.read_frame_validated(0, &mut out),
            FrameRead::Overrun
        ));
        // a recent frame still reads fine (within capacity minus margin)
        let recent = a.position() - 96;
        assert!(matches!(
            b.read_frame_validated(recent, &mut out),
            FrameRead::Frame(_)
        ));
    }

    #[test]
    fn run_read_packs_whole_frames_up_to_max_bytes() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1, 0);
        for i in 0..4 {
            a.append(1, i, &[i as u8; 64]).unwrap(); // 4 x 96 B frames
        }
        let mut out = Vec::new();
        // 200-byte budget -> 2 whole frames (192)
        match b.read_run_validated(0, 200, &mut out) {
            SliceRead::Run(r) => {
                assert_eq!((r.bytes, r.advance), (192, 192));
                assert_eq!(out.len(), 192);
                assert_eq!(read_header(&out).seq, 0);
                assert_eq!(read_header(&out[96..]).seq, 1);
            }
            other => panic!("expected Run, got {other:?}"),
        }
        // continuing from the advance point picks up frame 2
        match b.read_run_validated(192, 4096, &mut out) {
            SliceRead::Run(r) => {
                assert_eq!((r.bytes, r.advance), (192, 192)); // frames 2,3
                assert_eq!(read_header(&out).seq, 2);
            }
            other => panic!("expected Run, got {other:?}"),
        }
        // at least one frame even under a tiny budget
        match b.read_run_validated(0, 8, &mut out) {
            SliceRead::Run(r) => assert_eq!((r.bytes, r.advance), (96, 96)),
            other => panic!("expected Run, got {other:?}"),
        }
        // caught up
        assert!(matches!(
            b.read_run_validated(4 * 96, 4096, &mut out),
            SliceRead::NotCommitted
        ));
    }

    #[test]
    fn run_read_padding_is_header_only_with_full_advance() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1, 0);
        for i in 0..42 {
            a.append(1, i, &[0u8; 64]).unwrap(); // fill to 4032
        }
        c.counters().durable.store_release(4032);
        a.append(1, 99, &[0u8; 64]).unwrap(); // 64 B padding at 4032, frame at 4096
        let mut out = Vec::new();
        // run starting at the padding: 32 bytes copied, 64 positions advanced,
        // run ends (padding ends at the wrap)
        match b.read_run_validated(4032, 1392, &mut out) {
            SliceRead::Run(r) => {
                assert_eq!((r.bytes, r.advance), (HEADER_LEN, 64));
                assert_eq!(read_header(&out).frame_type, FRAME_TYPE_PADDING);
                assert_eq!(read_header(&out).length, 64);
            }
            other => panic!("expected Run, got {other:?}"),
        }
        // and the post-wrap frame comes as its own run
        match b.read_run_validated(4096, 1392, &mut out) {
            SliceRead::Run(r) => {
                assert_eq!((r.bytes, r.advance), (96, 96));
                assert_eq!(read_header(&out).seq, 99);
            }
            other => panic!("expected Run, got {other:?}"),
        }
    }

    #[test]
    fn run_read_detects_overrun_and_primed_fresh_buffer() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1, 0);
        let mut n = 0u32;
        while a.position() < 3 * CAP {
            a.append(1, n, &[0u8; 64]).unwrap();
            c.counters().durable.store_release(a.position());
            n += 1;
        }
        let mut out = Vec::new();
        assert!(matches!(
            b.read_run_validated(0, 1392, &mut out),
            SliceRead::Overrun
        ));
        // primed-over-fresh-buffer (Task 1 semantics, run variant)
        let (b2, c2) = buf();
        c2.counters().prime(2 * CAP);
        assert!(matches!(
            b2.read_run_validated(2 * CAP - 64, 1392, &mut out),
            SliceRead::Overrun
        ));
    }

    // ------------------------------------------ time-and-timers (Task 3)

    fn headers(b: &LogBuffer, end: u64) -> Vec<FrameHeader> {
        let s = b.recordable_slice(0, end as usize).unwrap();
        let mut out = Vec::new();
        let mut off = 0usize;
        while off + HEADER_LEN <= s.len() {
            let h = read_header(&s[off..]);
            if h.length == 0 {
                break;
            }
            out.push(h);
            off += align_frame_len(h.length as usize);
        }
        out
    }

    #[test]
    fn every_frame_type_is_stamped_and_the_clamp_never_goes_backwards() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 3, 1_000);
        a.append_new_term().unwrap(); // stamped with the seed
        a.set_now(5_000);
        a.append(1, 1, b"x").unwrap();
        a.set_now(4_000); // the clock stepped back: the stamp must hold at 5_000
        a.append(1, 2, b"y").unwrap();
        a.append_config(3, b"cfg").unwrap();
        a.set_now(6_000);
        a.append(1, 3, b"z").unwrap();
        let hs = headers(&b, a.position());
        let stamps: Vec<u64> = hs.iter().map(|h| h.time_ns).collect();
        assert_eq!(stamps, vec![1_000, 5_000, 5_000, 5_000, 6_000]);
        assert_eq!(a.last_stamp(), 6_000);
        assert!(
            hs.iter()
                .all(|h| h.frame_type != FRAME_TYPE_PADDING || h.time_ns > 0)
        );
    }

    #[test]
    fn append_timer_stamps_the_deadline_and_marks_late_by_clamp() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 3, 0);
        a.set_now(100);
        a.append(1, 1, b"a").unwrap();
        let body = TimerBody {
            identity_hash: 9,
            timer_id: 42,
            deadline_ns: 150,
        };
        let (pos, stamp) = a.append_timer(&body, 0).unwrap();
        assert_eq!(stamp, 150, "on time: stamped with the deadline");
        let s = b.recordable_slice(pos, 64).unwrap();
        let h = read_header(s);
        assert_eq!(h.frame_type, FRAME_TYPE_TIMER);
        assert_eq!((h.client_id, h.seq, h.flags), (0, 0, 0));
        assert_eq!(h.length as usize, HEADER_LEN + TIMER_BODY_LEN);
        assert_eq!(read_timer_body(&s[HEADER_LEN..]), Some(body));
        // late: the log is already past the deadline
        a.set_now(1_000);
        a.append(1, 2, b"b").unwrap();
        let late = TimerBody {
            identity_hash: 9,
            timer_id: 43,
            deadline_ns: 500,
        };
        let (_, stamp) = a.append_timer(&late, FLAG_TIMER_TABLE).unwrap();
        assert_eq!(
            stamp, 1_000,
            "late: clamped to last_stamp, deadline kept in the body"
        );
        let hs = headers(&b, a.position());
        assert_eq!(hs.last().unwrap().flags, FLAG_TIMER_TABLE);
        let stamps: Vec<u64> = hs.iter().map(|h| h.time_ns).collect();
        assert!(stamps.windows(2).all(|w| w[0] <= w[1]), "{stamps:?}");
    }

    /// Final-review I2, unit half: `fire_due_timers`'s `WouldOverrun` exit
    /// holds the clients and retries the SAME instance next pass. That is only
    /// sound because the stamp is computed AFTER the overrun gate — a failed
    /// `append_timer` must leave `last_stamp` (and the frontier) untouched, or
    /// the retry's clamp would already have moved and the timer would report
    /// itself late for a frame that was never written.
    #[test]
    fn append_timer_would_overrun_leaves_last_stamp_untouched() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1, 0);
        a.set_now(100);
        for i in 0..42 {
            a.append(1, i, &[0u8; 64]).unwrap(); // 4032 used, durable still 0
        }
        // One more 32 B frame: 4064 used, 32 B of headroom — too little for a
        // 64 B timer frame, and padding to the wrap would land it past
        // durable(0) + capacity(4096).
        a.append(1, 99, &[]).unwrap();
        let (pos_before, stamp_before) = (a.position(), a.last_stamp());
        let append_before = c.counters().append.load_acquire();
        let body = TimerBody {
            identity_hash: 9,
            timer_id: 42,
            deadline_ns: 9_000, // far above last_stamp: would move the clamp
        };
        assert_eq!(
            a.append_timer(&body, 0).unwrap_err(),
            AppendError::WouldOverrun
        );
        assert_eq!(
            a.last_stamp(),
            stamp_before,
            "a gated timer append must not move the log clock"
        );
        assert_eq!(a.position(), pos_before);
        assert_eq!(c.counters().append.load_acquire(), append_before);
        // The retry, once the gate reopens, stamps with the deadline as if the
        // first attempt had never happened.
        c.counters().durable.store_release(4096);
        let (_, stamp) = a.append_timer(&body, 0).unwrap();
        assert_eq!(stamp, 9_000);
    }

    /// Spec §4.3, pinned at the appender: drive random passes of
    /// (set_now, due timers first, then client frames) and assert no frame
    /// before a TIMER carries a stamp above its deadline unless the TIMER is
    /// late.
    #[test]
    fn pass_order_property_no_earlier_frame_exceeds_an_on_time_deadline() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1, 0);
        let mut rng = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut now = 1_000u64;
        let mut pending: Vec<(u64, u64)> = Vec::new(); // (id, deadline)
        let mut id = 0u64;
        'passes: for _ in 0..40 {
            now += next() % 300;
            a.set_now(now);
            // schedule a few timers for the near future or the past
            for _ in 0..(next() % 3) {
                id += 1;
                pending.push((id, now.saturating_sub(200) + next() % 600));
            }
            pending.sort_by_key(|p| p.1);
            while let Some(&(tid, dl)) = pending.first() {
                if dl > now {
                    break;
                }
                pending.remove(0);
                if a.append_timer(
                    &TimerBody {
                        identity_hash: 1,
                        timer_id: tid,
                        deadline_ns: dl,
                    },
                    0,
                )
                .is_err()
                {
                    break 'passes;
                }
            }
            for _ in 0..(next() % 4) {
                if a.append(1, 1, b"c").is_err() {
                    break 'passes;
                }
            }
        }
        let hs = headers(&b, a.position());
        assert!(
            hs.len() > 20,
            "the buffer filled too early to mean anything: {}",
            hs.len()
        );
        for (i, h) in hs.iter().enumerate() {
            if h.frame_type != FRAME_TYPE_TIMER {
                continue;
            }
            let body = read_timer_body(
                &b.recordable_slice(
                    hs[..i]
                        .iter()
                        .map(|x| align_frame_len(x.length as usize) as u64)
                        .sum(),
                    64,
                )
                .unwrap()[HEADER_LEN..],
            )
            .unwrap();
            let late = h.time_ns > body.deadline_ns;
            if !late {
                assert!(
                    hs[..i].iter().all(|e| e.time_ns <= body.deadline_ns),
                    "frame before an on-time timer stamped past its deadline: {:?} vs {body:?}",
                    &hs[..i]
                );
            }
            assert!(
                hs[i..].iter().all(|e| e.time_ns >= h.time_ns),
                "a later frame stamped below the timer"
            );
        }
    }
}
