//! Receiver-side: order DATA fragments by seq, reassemble BEGIN..END runs into
//! complete messages, and expose gaps for NAK + the highest-contiguous seq for
//! flow-control SMs.
use std::collections::BTreeMap;

use bytes::{Bytes, BytesMut};

use super::wire::{FLAG_BEGIN, FLAG_END};

pub struct Reassembler {
    /// Buffered segments not yet consumed, keyed by seq.
    pending: BTreeMap<u64, (u8, Bytes)>,
    /// Next seq we expect to consume contiguously.
    next: u64,
    /// Highest seq ever observed (for gap computation).
    highest_seen: Option<u64>,
    /// Whether at least one segment has been delivered/consumed.
    consumed_any: bool,
}

impl Reassembler {
    pub fn new() -> Self {
        Self {
            pending: BTreeMap::new(),
            next: 0,
            highest_seen: None,
            consumed_any: false,
        }
    }

    pub fn accept(&mut self, seq: u64, flags: u8, payload: Bytes) -> Vec<Bytes> {
        if seq < self.next {
            return Vec::new(); // already consumed; duplicate/retransmit
        }
        self.highest_seen = Some(self.highest_seen.map_or(seq, |h| h.max(seq)));
        self.pending.entry(seq).or_insert((flags, payload));
        self.drain()
    }

    /// Consume contiguous segments from `next`, emitting complete messages.
    fn drain(&mut self) -> Vec<Bytes> {
        let mut out = Vec::new();
        loop {
            // Is the next expected seq present?
            if !self.pending.contains_key(&self.next) {
                break;
            }
            // Try to assemble one message starting at `next`: it must begin with
            // a BEGIN segment and run contiguously to an END segment.
            let (flags0, _) = self.pending.get(&self.next).cloned().unwrap();
            if flags0 & FLAG_BEGIN == 0 {
                // Shouldn't happen for a well-formed stream; drop to avoid stall.
                self.pending.remove(&self.next);
                self.next += 1;
                self.consumed_any = true;
                continue;
            }
            // Scan forward for END, requiring contiguity.
            let mut end_seq = None;
            let mut s = self.next;
            while let Some((f, _)) = self.pending.get(&s) {
                if *f & FLAG_END != 0 {
                    end_seq = Some(s);
                    break;
                }
                s += 1;
            }
            let Some(end_seq) = end_seq else { break }; // incomplete; wait for more
            // Assemble [next ..= end_seq].
            let mut msg = BytesMut::new();
            for k in self.next..=end_seq {
                let (_, p) = self.pending.remove(&k).unwrap();
                msg.extend_from_slice(&p);
            }
            out.push(msg.freeze());
            self.next = end_seq + 1;
            self.consumed_any = true;
        }
        out
    }

    /// Highest seq received with no gap below it. `None` until first consume.
    pub fn highest_contiguous(&self) -> Option<u64> {
        if !self.consumed_any {
            return None;
        }
        Some(self.next - 1)
    }

    /// Missing `(start, count)` ranges below `highest_seen`.
    pub fn gaps(&self) -> Vec<(u64, u64)> {
        let Some(hi) = self.highest_seen else {
            return Vec::new();
        };
        let mut gaps = Vec::new();
        let mut expect = self.next;
        while expect <= hi {
            if self.pending.contains_key(&expect) {
                expect += 1;
                continue;
            }
            let start = expect;
            while expect <= hi && !self.pending.contains_key(&expect) {
                expect += 1;
            }
            gaps.push((start, expect - start));
        }
        gaps
    }
}

impl Default for Reassembler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::udp::wire::{FLAG_BEGIN, FLAG_END};
    use bytes::Bytes;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn single_segment_message_delivered() {
        let mut r = Reassembler::new();
        let out = r.accept(0, FLAG_BEGIN | FLAG_END, b("hi"));
        assert_eq!(out.len(), 1);
        assert_eq!(&out[0][..], b"hi");
        assert_eq!(r.highest_contiguous(), Some(0));
        assert!(r.gaps().is_empty());
    }

    #[test]
    fn multi_fragment_message_reassembled_in_order() {
        let mut r = Reassembler::new();
        assert!(r.accept(0, FLAG_BEGIN, b("foo")).is_empty());
        let out = r.accept(1, FLAG_END, b("bar"));
        assert_eq!(out.len(), 1);
        assert_eq!(&out[0][..], b"foobar");
    }

    #[test]
    fn out_of_order_buffers_then_delivers() {
        let mut r = Reassembler::new();
        // seq 1 arrives before seq 0 → gap at 0
        assert!(r.accept(1, FLAG_END, b("bar")).is_empty());
        assert_eq!(r.gaps(), vec![(0, 1)]);
        let out = r.accept(0, FLAG_BEGIN, b("foo"));
        assert_eq!(out.len(), 1);
        assert_eq!(&out[0][..], b"foobar");
        assert!(r.gaps().is_empty());
        assert_eq!(r.highest_contiguous(), Some(1));
    }

    #[test]
    fn duplicate_segments_ignored() {
        let mut r = Reassembler::new();
        let _ = r.accept(0, FLAG_BEGIN | FLAG_END, b("a"));
        let out = r.accept(0, FLAG_BEGIN | FLAG_END, b("a"));
        assert!(out.is_empty()); // already delivered, no double-deliver
    }
}
