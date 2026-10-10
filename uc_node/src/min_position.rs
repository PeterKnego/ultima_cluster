// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Read-your-writes (spec 2026-10-08 §5.1): min-position reads parked on the
//! consensus agent until the row's applied frontier reaches their token.
//!
//! Per-pass cost must not grow with the number of parked reads, because a
//! client chooses the token and so chooses how long a read waits. Each row has
//! a min-heap keyed by token (the lowest surfaces first, so a high token never
//! hold back a lower one), and one FIFO orders every read by deadline (all
//! reads get the same timeout, so admission order is deadline order). Entries
//! are tagged with a generation so a released read's leftover heap/FIFO entry
//! is recognised as stale, and both are compacted when stale entries pile up.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::sync::atomic::AtomicU64;

use uc_protocol::v2::cnc::CNC_MAX_SERVICES;

/// Hard cap on parked min-position reads per node (spec §5.1 step 5).
pub const MAX_PARKED_MIN_POSITION_READS: usize = 4096;

/// Counters shared with `/metrics` (`uc2_read_min_position_refused_total`
/// by `reason`, and the `uc2_read_min_position_parked` gauge).
#[derive(Debug, Default)]
pub struct MinPositionReadStats {
    /// Tokens above this node's `durable` position, refused at once.
    pub refused_ahead: AtomicU64,
    /// Reads refused because the parked set was full.
    pub refused_cap: AtomicU64,
    /// Reads parked right now (gauge).
    pub parked: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParkedRead {
    pub client_id: u32,
    pub local_seq: u32,
    pub service_id: u8,
    pub query: Vec<u8>,
    pub token: u64,
    pub deadline_ns: u64,
}

pub(crate) struct ParkedReads {
    /// `(generation, read)`; `None` = free.
    slab: Vec<Option<(u64, ParkedRead)>>,
    free: Vec<usize>,
    /// Per row: `Reverse((token, generation, slab id))`.
    heaps: Vec<BinaryHeap<Reverse<(u64, u64, usize)>>>,
    /// `(deadline_ns, generation, slab id)` in admission order.
    expiry: VecDeque<(u64, u64, usize)>,
    next_gen: u64,
    len: usize,
    cap: usize,
}

impl ParkedReads {
    pub(crate) fn new(cap: usize) -> ParkedReads {
        ParkedReads {
            slab: Vec::new(),
            free: Vec::new(),
            heaps: (0..CNC_MAX_SERVICES).map(|_| BinaryHeap::new()).collect(),
            expiry: VecDeque::new(),
            next_gen: 1,
            len: 0,
            cap,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn live(&self, generation: u64, id: usize) -> bool {
        matches!(self.slab.get(id), Some(Some((g, _))) if *g == generation)
    }

    fn take(&mut self, id: usize) -> ParkedRead {
        let (_, r) = self.slab[id].take().expect("live slab entry");
        self.free.push(id);
        self.len -= 1;
        r
    }

    /// Park `read`, or hand it back when the set is full.
    pub(crate) fn park(&mut self, read: ParkedRead) -> Result<(), ParkedRead> {
        if self.len >= self.cap {
            return Err(read);
        }
        let generation = self.next_gen;
        self.next_gen += 1;
        let row = read.service_id as usize;
        let (token, deadline) = (read.token, read.deadline_ns);
        let id = match self.free.pop() {
            Some(id) => {
                self.slab[id] = Some((generation, read));
                id
            }
            None => {
                self.slab.push(Some((generation, read)));
                self.slab.len() - 1
            }
        };
        self.heaps[row].push(Reverse((token, generation, id)));
        self.expiry.push_back((deadline, generation, id));
        self.len += 1;
        self.compact();
        Ok(())
    }

    /// The lowest parked token on `row` (stale heap entries are dropped).
    pub(crate) fn peek_token(&mut self, row: u8) -> Option<u64> {
        loop {
            let Reverse((token, g, id)) = *self.heaps[row as usize].peek()?;
            if self.live(g, id) {
                return Some(token);
            }
            self.heaps[row as usize].pop();
        }
    }

    /// Remove and return the lowest-token read on `row`.
    pub(crate) fn pop(&mut self, row: u8) -> Option<ParkedRead> {
        while let Some(Reverse((_, g, id))) = self.heaps[row as usize].pop() {
            if self.live(g, id) {
                return Some(self.take(id));
            }
        }
        None
    }

    /// Remove every read whose deadline is `<= now_ns`, oldest first.
    pub(crate) fn expire(&mut self, now_ns: u64, mut f: impl FnMut(ParkedRead)) {
        while let Some(&(deadline, g, id)) = self.expiry.front() {
            if !self.live(g, id) {
                self.expiry.pop_front();
                continue;
            }
            if deadline > now_ns {
                break;
            }
            self.expiry.pop_front();
            f(self.take(id));
        }
    }

    /// Drop stale entries once they outnumber live ones 4:1, so churn (park
    /// then release, millions of times) cannot grow memory. Amortised O(1).
    fn compact(&mut self) {
        let bound = 4 * self.cap.max(1);
        if self.expiry.len() > bound {
            let slab = &self.slab;
            self.expiry
                .retain(|&(_, g, id)| matches!(slab.get(id), Some(Some((lg, _))) if *lg == g));
        }
        for h in 0..self.heaps.len() {
            if self.heaps[h].len() > bound {
                let slab = &self.slab;
                self.heaps[h].retain(
                    |Reverse((_, g, id))| matches!(slab.get(*id), Some(Some((lg, _))) if lg == g),
                );
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn bookkeeping_len(&self) -> usize {
        self.expiry.len() + self.heaps.iter().map(|h| h.len()).sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(row: u8, token: u64, deadline_ns: u64, seq: u32) -> ParkedRead {
        ParkedRead {
            client_id: 9,
            local_seq: seq,
            service_id: row,
            query: vec![seq as u8],
            token,
            deadline_ns,
        }
    }

    #[test]
    fn the_lowest_token_surfaces_first_whatever_the_park_order() {
        let mut p = ParkedReads::new(16);
        p.park(read(0, 900, 10, 1)).unwrap();
        p.park(read(0, 100, 10, 2)).unwrap();
        p.park(read(0, 500, 10, 3)).unwrap();
        assert_eq!(p.peek_token(0), Some(100));
        assert_eq!(p.pop(0).unwrap().local_seq, 2);
        assert_eq!(p.pop(0).unwrap().local_seq, 3);
        assert_eq!(p.pop(0).unwrap().local_seq, 1);
        assert_eq!(p.pop(0), None);
        assert!(p.is_empty());
    }

    #[test]
    fn rows_are_independent() {
        let mut p = ParkedReads::new(16);
        p.park(read(1, 50, 10, 1)).unwrap();
        assert_eq!(p.peek_token(0), None);
        assert_eq!(p.peek_token(1), Some(50));
    }

    #[test]
    fn the_cap_refuses_and_returns_the_read() {
        let mut p = ParkedReads::new(2);
        p.park(read(0, 1, 10, 1)).unwrap();
        p.park(read(0, 2, 10, 2)).unwrap();
        let back = p.park(read(0, 3, 10, 3)).unwrap_err();
        assert_eq!(back.local_seq, 3);
        assert_eq!(p.len(), 2);
    }

    #[test]
    fn expiry_takes_exactly_the_reads_past_their_deadline() {
        let mut p = ParkedReads::new(16);
        p.park(read(0, 1, 10, 1)).unwrap();
        p.park(read(0, 2, 20, 2)).unwrap();
        p.park(read(1, 3, 30, 3)).unwrap();
        let mut got = Vec::new();
        p.expire(20, |r| got.push(r.local_seq));
        assert_eq!(got, vec![1, 2]);
        assert_eq!(p.len(), 1);
        assert_eq!(p.peek_token(0), None, "expired reads leave the heap too");
        assert_eq!(p.peek_token(1), Some(3));
    }

    #[test]
    fn a_released_read_is_never_expired() {
        let mut p = ParkedReads::new(16);
        p.park(read(0, 1, 10, 1)).unwrap();
        assert_eq!(p.pop(0).unwrap().local_seq, 1);
        let mut got = Vec::new();
        p.expire(u64::MAX, |r| got.push(r.local_seq));
        assert!(got.is_empty());
    }

    #[test]
    fn a_reused_slab_slot_is_not_confused_with_its_old_occupant() {
        let mut p = ParkedReads::new(16);
        p.park(read(0, 1, 10, 1)).unwrap();
        p.pop(0).unwrap(); // frees slot 0, its expiry entry is now stale
        p.park(read(0, 2, 99, 2)).unwrap(); // reuses slot 0
        let mut got = Vec::new();
        p.expire(10, |r| got.push(r.local_seq));
        assert!(
            got.is_empty(),
            "the stale entry must not expire the new read"
        );
        assert_eq!(p.len(), 1);
    }

    #[test]
    fn a_repark_keeps_the_read_and_its_deadline() {
        let mut p = ParkedReads::new(16);
        p.park(read(0, 5, 10, 1)).unwrap();
        let r = p.pop(0).unwrap();
        p.park(r).unwrap(); // the node does this when svc_query is full
        assert_eq!(p.peek_token(0), Some(5));
        let mut got = Vec::new();
        p.expire(10, |r| got.push((r.local_seq, r.deadline_ns)));
        assert_eq!(got, vec![(1, 10)]);
    }

    #[test]
    fn churn_never_grows_the_bookkeeping_without_bound() {
        let mut p = ParkedReads::new(8);
        for i in 0..100_000u32 {
            p.park(read(0, i as u64, u64::MAX, i)).unwrap();
            p.pop(0).unwrap();
        }
        assert!(p.bookkeeping_len() <= 4 * 8 + 8, "{}", p.bookkeeping_len());
    }
}
