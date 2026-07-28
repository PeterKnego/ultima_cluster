//! The per-(sender, epoch) anti-replay sliding window: the standard
//! IPsec/RFC 6479 bitmap.
//!
//! # Why this exists
//!
//! AEAD authentication (T5) stops forgery, not replay: a captured, valid
//! datagram still verifies if resent later. In this system a replayed
//! `VOTE` or admin datagram is not inert, so the receive path must reject a
//! counter it has already accepted.
//!
//! # Why it keys on the counter, never the log position
//!
//! UC's transport reorders and retransmits *by design* — that is ordinary
//! operation, not an attack. A window that rejected out-of-order arrival
//! would turn routine reordering into packet loss. And a genuine NAK
//! retransmit re-seals the same log position under a fresh counter (see
//! `schedule.rs`'s per-sender-per-boot derivation), so a repeated *position*
//! is expected and must not collide with a repeated *counter* — the two are
//! deliberately decoupled, and this window only ever looks at the counter.
//!
//! # Why `highest == 0` doubles as "nothing accepted yet"
//!
//! Counters are assigned by the sender starting at 1 and never reused
//! (0 is reserved and always invalid off the wire), so a fresh window can
//! safely use `highest: 0` as the "no counter accepted yet" sentinel without
//! ambiguity — no separate `initialized` flag, no allocation, cheap enough
//! to embed one per (sender, epoch) in the session state T6 builds.
//!
//! # The algorithm
//!
//! `bitmap` is a 1024-bit window, stored as 16 `u64` words, LSB-first: bit
//! `i` means "counter == highest - i has been seen." Accepting a new
//! highest counter left-shifts the whole bitmap by the jump distance
//! (dropping bits that fall outside the window) and sets bit 0. Accepting a
//! counter at or below the current highest sets the bit at its distance
//! from highest, after checking it isn't already set (replay) — a distance
//! at or beyond the window width is unverifiable and is refused rather than
//! guessed at.

/// Width of the anti-replay window, in counters (and in bits of the
/// tracking bitmap). Fixed at the standard IPsec/RFC 6479 size.
pub const REPLAY_WINDOW_BITS: u64 = 1024;

/// `REPLAY_WINDOW_BITS` in 64-bit words. `REPLAY_WINDOW_BITS` is a multiple
/// of 64, so this is exact.
const WORDS: usize = (REPLAY_WINDOW_BITS / 64) as usize;

/// Per-(sender, epoch) anti-replay state: the highest counter accepted so
/// far, plus a bitmap of which of the `REPLAY_WINDOW_BITS` counters at or
/// below it have already been seen. `Copy`-free but allocation-free — safe
/// to embed by value in per-peer session state.
#[derive(Debug, Clone)]
pub struct ReplayWindow {
    highest: u64,
    bitmap: [u64; WORDS],
}

impl ReplayWindow {
    /// A fresh window: nothing accepted yet (`highest == 0`, which is also
    /// why counter `0` is never valid — see the module doc).
    pub fn new() -> Self {
        ReplayWindow {
            highest: 0,
            bitmap: [0; WORDS],
        }
    }

    /// Checks whether `counter` is acceptable (in order, or a reorder still
    /// inside the window, and not previously seen) and, if so, marks it
    /// seen. Returns `false` — never panics — for a replay, a counter too
    /// far in the past to verify, or the reserved value `0`. Every branch
    /// here runs on attacker-controlled bytes off the wire, so arithmetic is
    /// ordered to never underflow: the subtraction in each branch only runs
    /// after the branch's own comparison has established which side is
    /// larger.
    pub fn check_and_set(&mut self, counter: u64) -> bool {
        if counter == 0 {
            return false;
        }

        if counter > self.highest {
            // New high-water mark: slide the window forward and record it.
            let jump = counter - self.highest;
            if jump >= REPLAY_WINDOW_BITS {
                // The jump is wider than the whole window — nothing old
                // survives it, so start from a clean bitmap rather than
                // shift-by-a-huge-amount.
                self.bitmap = [0; WORDS];
            } else {
                shift_left(&mut self.bitmap, jump as u32);
            }
            self.highest = counter;
            set_bit(&mut self.bitmap, 0);
            true
        } else {
            // At or behind the high-water mark: legitimate only if it's
            // both inside the window and not already marked seen.
            let distance = self.highest - counter;
            if distance >= REPLAY_WINDOW_BITS {
                return false;
            }
            let idx = distance as u32;
            if bit_is_set(&self.bitmap, idx) {
                false
            } else {
                set_bit(&mut self.bitmap, idx);
                true
            }
        }
    }
}

impl Default for ReplayWindow {
    fn default() -> Self {
        ReplayWindow::new()
    }
}

/// Left-shifts the `REPLAY_WINDOW_BITS`-wide value held across `bitmap`'s
/// words by `n` bits (`n < REPLAY_WINDOW_BITS`, enforced by the caller),
/// dropping bits shifted past the top word and filling with zero from the
/// bottom. Iterates high-to-low so each word is computed from
/// not-yet-overwritten source words.
fn shift_left(bitmap: &mut [u64; WORDS], n: u32) {
    if n == 0 {
        return;
    }
    let word_shift = (n / 64) as usize;
    let bit_shift = n % 64;
    for i in (0..WORDS).rev() {
        let mut word = 0u64;
        if i >= word_shift {
            word = bitmap[i - word_shift] << bit_shift;
            if bit_shift > 0 && i > word_shift {
                word |= bitmap[i - word_shift - 1] >> (64 - bit_shift);
            }
        }
        bitmap[i] = word;
    }
}

/// Reads bit `index` (`index < REPLAY_WINDOW_BITS`, enforced by the caller).
fn bit_is_set(bitmap: &[u64; WORDS], index: u32) -> bool {
    let word = (index / 64) as usize;
    let bit = index % 64;
    (bitmap[word] >> bit) & 1 == 1
}

/// Sets bit `index` (`index < REPLAY_WINDOW_BITS`, enforced by the caller).
fn set_bit(bitmap: &mut [u64; WORDS], index: u32) {
    let word = (index / 64) as usize;
    let bit = index % 64;
    bitmap[word] |= 1u64 << bit;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_in_order_and_rejects_exact_repeats() {
        let mut w = ReplayWindow::new();
        assert!(w.check_and_set(1));
        assert!(w.check_and_set(2));
        assert!(!w.check_and_set(2), "a captured-and-resent datagram is refused");
        assert!(!w.check_and_set(1));
    }

    #[test]
    fn accepts_reordering_inside_the_window() {
        // UC's transport reorders freely; the window must not turn that into loss.
        let mut w = ReplayWindow::new();
        assert!(w.check_and_set(10));
        assert!(w.check_and_set(5), "late arrival within the window is legitimate");
        assert!(!w.check_and_set(5), "but only once");
    }

    #[test]
    fn rejects_anything_older_than_the_window() {
        let mut w = ReplayWindow::new();
        assert!(w.check_and_set(REPLAY_WINDOW_BITS + 100));
        assert!(!w.check_and_set(1), "far-past counter is unverifiable, so refuse");
    }

    #[test]
    fn a_large_forward_jump_resets_cleanly() {
        let mut w = ReplayWindow::new();
        assert!(w.check_and_set(1));
        assert!(w.check_and_set(u64::MAX / 2));
        assert!(!w.check_and_set(u64::MAX / 2), "still tracked after the jump");
        assert!(!w.check_and_set(1), "the old counter fell out of the window");
    }

    // The four tests above never move a tracked run across a `u64` word
    // boundary via the shift path (test 1's only shift is by 1 bit inside
    // word 0; tests 3 and 4 both land in the `jump >= REPLAY_WINDOW_BITS`
    // wipe path). A wrong `word_shift`/`bit_shift` computation in
    // `shift_left` would ship green against them, and the failure mode is
    // silent and asymmetric: a lost bit wrongly ACCEPTS a replay, a
    // misplaced bit wrongly REJECTS fresh traffic (a liveness bug — the
    // node quietly drops valid input). This test forces the shift path to
    // actually carry bits across two different word boundaries and checks
    // both failure directions explicitly.
    #[test]
    fn shift_preserves_and_places_bits_correctly_across_word_boundaries() {
        let mut w = ReplayWindow::new();
        // Accept 1..=70 in order: relative to highest = 70, the tracked
        // run occupies distances 0..=69, which spans word 0 (bits 0..63)
        // into word 1 (bits 64..69).
        for c in 1..=70u64 {
            assert!(w.check_and_set(c), "counter {c} is fresh and in order");
        }
        // Advance by 100 (word_shift = 1, bit_shift = 36: a genuine
        // cross-word carry, not a whole-word-multiple shift) — this is
        // still the shift path (100 < REPLAY_WINDOW_BITS), and it lands
        // the old run at distances 100..169, crossing the word1/word2
        // boundary this time.
        assert!(w.check_and_set(170));

        // Direction A — a bit lost in the shift wrongly ACCEPTS a replay:
        // every previously-accepted counter must still read as seen.
        assert!(!w.check_and_set(70), "near edge of the old run must survive the cross-word shift");
        assert!(!w.check_and_set(35), "middle of the old run must survive the cross-word shift");
        assert!(!w.check_and_set(1), "far edge of the old run must survive the cross-word shift");

        // Direction B — a bit misplaced by the shift wrongly REJECTS
        // fresh traffic: distance 20 (counter 150) was never set by
        // either the original run (distances 0..69, now 100..169) or by
        // accepting 170 itself (distance 0), so it must be accepted.
        assert!(w.check_and_set(150), "never-before-seen counter must not be corrupted into a false replay");
    }

    // Pins the two off-by-one boundaries the mandated tests never hit:
    // the window's own edge (distance == REPLAY_WINDOW_BITS - 1 is the
    // oldest counter still verifiable; REPLAY_WINDOW_BITS itself is one
    // step too old).
    #[test]
    fn distance_edge_is_inclusive_one_past_is_not() {
        let mut w = ReplayWindow::new();
        assert!(w.check_and_set(REPLAY_WINDOW_BITS + 1)); // highest = 1025
        assert!(
            w.check_and_set(2),
            "distance == REPLAY_WINDOW_BITS - 1 (1023) is still inside the window"
        );
        assert!(
            !w.check_and_set(1),
            "distance == REPLAY_WINDOW_BITS (1024) is one step too old to verify"
        );
    }

    // Pins the shift-vs-wipe boundary in the *advance* branch: a jump of
    // REPLAY_WINDOW_BITS - 1 must take the real shift path (the old
    // counter is still just inside the window afterwards, at the far
    // edge) — only observable if the implementation actually shifts
    // rather than wiping. A jump of exactly REPLAY_WINDOW_BITS takes the
    // wipe path; the outcome for the old counter is the same reject
    // either way, so this only pins that the wipe boundary itself doesn't
    // panic or misbehave.
    #[test]
    fn jump_boundary_shifts_below_window_width_and_wipes_at_it() {
        let mut w = ReplayWindow::new();
        assert!(w.check_and_set(1));
        assert!(w.check_and_set(1 + REPLAY_WINDOW_BITS - 1)); // jump = 1023, shift path
        assert!(
            !w.check_and_set(1),
            "jump == REPLAY_WINDOW_BITS - 1 must shift (not wipe): the old counter is still barely in-window"
        );

        let mut w2 = ReplayWindow::new();
        assert!(w2.check_and_set(1));
        assert!(w2.check_and_set(1 + REPLAY_WINDOW_BITS)); // jump = 1024, wipe path
        assert!(
            !w2.check_and_set(1),
            "jump == REPLAY_WINDOW_BITS wipes; the old counter is unverifiable either way"
        );
    }
}
