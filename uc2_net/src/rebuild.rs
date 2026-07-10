// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Follower-side loss recovery state (spec §5): gap tracking over absolute
//! positions (no reliance on buffer contents — stale bytes from a previous
//! lap can hold nonzero length words, so contiguity must be tracked here,
//! not scanned) + the randomized-delay NAK timer (~1 RTT; a short delay
//! absorbs benign reordering before asking for a retransmit).

use std::collections::BTreeMap;

use crate::fault::XorShift64;

/// Tracks which byte ranges of the stream have landed in the buffer and
/// where the contiguous frontier is. In-order traffic never touches the map.
pub struct Rebuilt {
    contiguous: u64,
    /// Out-of-order runs strictly above `contiguous`: start -> end.
    ooo: BTreeMap<u64, u64>,
}

impl Rebuilt {
    pub fn new(start: u64) -> Self {
        Self { contiguous: start, ooo: BTreeMap::new() }
    }

    #[inline]
    pub fn contiguous(&self) -> u64 {
        self.contiguous
    }

    /// Record [start, end). Returns true iff the contiguous frontier advanced.
    pub fn insert(&mut self, start: u64, end: u64) -> bool {
        debug_assert!(start <= end);
        if end <= self.contiguous {
            return false; // stale duplicate
        }
        if start <= self.contiguous {
            self.contiguous = end;
            // absorb ooo runs that are now contiguous
            while let Some((&s, &e)) = self.ooo.first_key_value() {
                if s > self.contiguous {
                    break;
                }
                self.contiguous = self.contiguous.max(e);
                self.ooo.remove(&s);
            }
            true
        } else {
            // coalesce with overlapping/adjacent neighbors
            let (mut s, mut e) = (start, end);
            if let Some((&ps, &pe)) = self.ooo.range(..=s).next_back()
                && pe >= s
            {
                s = ps;
                e = e.max(pe);
                self.ooo.remove(&ps);
            }
            while let Some((&ns, &ne)) = self.ooo.range(s..).next() {
                if ns > e {
                    break;
                }
                e = e.max(ne);
                self.ooo.remove(&ns);
            }
            self.ooo.insert(s, e);
            false
        }
    }

    /// The first missing range, if any out-of-order data is waiting behind it.
    /// (Tail loss — nothing waiting — is detected against the leader's
    /// heartbeat position by the receiver, not here.)
    pub fn first_gap(&self) -> Option<(u64, u64)> {
        self.ooo.first_key_value().map(|(&s, _)| (self.contiguous, s))
    }

    /// Highest position received (contiguous or not).
    pub fn highest(&self) -> u64 {
        self.ooo.last_key_value().map(|(_, &e)| e).unwrap_or(self.contiguous)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NakConfig {
    pub delay_min_ns: u64,
    pub delay_max_ns: u64,
    /// Re-NAK interval while the same gap start persists (covers a lost NAK
    /// or a lost retransmission).
    pub backoff_ns: u64,
}

impl Default for NakConfig {
    fn default() -> Self {
        Self { delay_min_ns: 200_000, delay_max_ns: 1_000_000, backoff_ns: 5_000_000 }
    }
}

pub struct NakTimer {
    cfg: NakConfig,
    rng: XorShift64,
    armed: Option<Armed>,
}

struct Armed {
    start: u64,
    deadline_ns: u64,
}

impl NakTimer {
    pub fn new(cfg: NakConfig, seed: u64) -> Self {
        Self { cfg, rng: XorShift64::new(seed), armed: None }
    }

    fn delay(&mut self) -> u64 {
        let span = self.cfg.delay_max_ns - self.cfg.delay_min_ns;
        self.cfg.delay_min_ns + if span == 0 { 0 } else { self.rng.next_u64() % span }
    }

    /// Drive with the current first gap and the current time. Returns the
    /// `(start, end)` range to NAK when the timer fires.
    pub fn poll(&mut self, gap: Option<(u64, u64)>, now_ns: u64) -> Option<(u64, u64)> {
        let Some((start, end)) = gap else {
            self.armed = None;
            return None;
        };
        match &mut self.armed {
            Some(a) if a.start == start => {
                if now_ns >= a.deadline_ns {
                    a.deadline_ns = now_ns + self.cfg.backoff_ns;
                    Some((start, end))
                } else {
                    None
                }
            }
            _ => {
                let d = self.delay();
                self.armed = Some(Armed { start, deadline_ns: now_ns + d });
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_order_advances_and_dups_do_not() {
        let mut r = Rebuilt::new(1000);
        assert_eq!(r.contiguous(), 1000);
        assert!(r.insert(1000, 1096));
        assert!(r.insert(1096, 1192));
        assert_eq!(r.contiguous(), 1192);
        assert!(!r.insert(1000, 1096)); // stale dup
        assert_eq!(r.contiguous(), 1192);
        assert_eq!(r.first_gap(), None);
        assert_eq!(r.highest(), 1192);
    }

    #[test]
    fn gap_then_fill_merges_and_reports() {
        let mut r = Rebuilt::new(0);
        assert!(!r.insert(96, 192)); // arrives ahead: gap [0, 96)
        assert!(!r.insert(288, 384)); // second ooo run
        assert_eq!(r.contiguous(), 0);
        assert_eq!(r.first_gap(), Some((0, 96)));
        assert_eq!(r.highest(), 384);
        assert!(r.insert(0, 96)); // fills the first gap, absorbs [96,192)
        assert_eq!(r.contiguous(), 192);
        assert_eq!(r.first_gap(), Some((192, 288)));
        assert!(r.insert(192, 288)); // fills the rest
        assert_eq!(r.contiguous(), 384);
        assert_eq!(r.first_gap(), None);
    }

    #[test]
    fn overlapping_ooo_runs_coalesce() {
        let mut r = Rebuilt::new(0);
        assert!(!r.insert(96, 288));
        assert!(!r.insert(192, 384)); // overlaps previous
        assert!(!r.insert(384, 480)); // adjacent
        assert!(r.insert(0, 96));
        assert_eq!(r.contiguous(), 480);
    }

    #[test]
    fn insert_absorbs_multiple_ooo_runs() {
        let mut r = Rebuilt::new(0);
        // three disjoint ooo runs
        assert!(!r.insert(100, 110));
        assert!(!r.insert(120, 130));
        assert!(!r.insert(140, 150));
        // one run overlapping all three: 95 is below every key, so the
        // predecessor branch never fires — the successor loop alone absorbs
        // [100,110), [120,130) and [140,150) (3 iterations)
        assert!(!r.insert(95, 145));
        assert_eq!(r.first_gap(), Some((0, 95)));
        assert_eq!(r.highest(), 150);
        // filling [0,95) must absorb the single coalesced run in one step
        assert!(r.insert(0, 95));
        assert_eq!(r.contiguous(), 150);
        assert_eq!(r.first_gap(), None);
    }

    #[test]
    fn insert_merges_forward_with_no_predecessor() {
        let mut r = Rebuilt::new(0);
        assert!(!r.insert(200, 300));
        // new run strictly before the existing one, nothing precedes it:
        // successor loop alone must absorb [200,300)
        assert!(!r.insert(100, 250));
        assert_eq!(r.first_gap(), Some((0, 100)));
        assert_eq!(r.highest(), 300);
        assert!(r.insert(0, 100));
        assert_eq!(r.contiguous(), 300);
    }

    #[test]
    fn insert_merges_backward_then_forward_combined() {
        // predecessor merge widens `s` back to `ps`, THEN the successor loop
        // iterates over the widened range — the one branch combination the
        // other coalescing tests don't reach
        let mut r = Rebuilt::new(0);
        assert!(!r.insert(100, 110));
        assert!(!r.insert(200, 210));
        assert!(!r.insert(105, 205)); // overlaps [100,110) backward, [200,210) forward
        assert_eq!(r.first_gap(), Some((0, 100)));
        assert_eq!(r.highest(), 210);
        assert!(r.insert(0, 100));
        assert_eq!(r.contiguous(), 210);
        assert_eq!(r.first_gap(), None);
    }

    #[test]
    fn nak_timer_arms_randomized_fires_and_backs_off() {
        let cfg = NakConfig { delay_min_ns: 200_000, delay_max_ns: 1_000_000, backoff_ns: 5_000_000 };
        let mut t = NakTimer::new(cfg, 42);
        // new gap arms; nothing fires before the deadline
        assert_eq!(t.poll(Some((0, 96)), 0), None);
        assert_eq!(t.poll(Some((0, 96)), 199_999), None);
        // by delay_max it must have fired exactly once
        let fired = t.poll(Some((0, 96)), 1_000_000);
        assert_eq!(fired, Some((0, 96)));
        // same gap: re-fires only after backoff
        assert_eq!(t.poll(Some((0, 96)), 1_000_001), None);
        assert_eq!(t.poll(Some((0, 96)), 1_000_000 + 5_000_000), Some((0, 96)));
        // gap cleared: disarm; new gap re-arms fresh
        assert_eq!(t.poll(None, 7_000_000), None);
        assert_eq!(t.poll(Some((96, 192)), 7_000_000), None); // arming, not firing
        assert!(t.poll(Some((96, 192)), 7_000_000 + 1_000_000).is_some());
    }

    #[test]
    fn nak_timer_tracks_growing_gap_end() {
        let mut t = NakTimer::new(NakConfig::default(), 7);
        assert_eq!(t.poll(Some((0, 96)), 0), None);
        // gap END grew while armed (more ooo arrived); same start = same gap
        let fired = t.poll(Some((0, 480)), 1_000_000).unwrap();
        assert_eq!(fired, (0, 480));
    }
}
