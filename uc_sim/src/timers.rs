// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The leader-pass model (spec §4.3), as a pure, dependency-free reference.
//!
//! `uc_sim`'s [`world`](crate::world) has no frames — it drives the election
//! state machine over `(term, position)` pairs, not timer/client frame
//! streams — so the §4.3 ordering invariant cannot be checked as a
//! world-level invariant. Instead this module models the leader pass exactly
//! as the real node performs it (spec §4.3):
//!
//! 1. read the clock → `now`;
//! 2. while the earliest **pending** timer has `deadline <= now`, append a
//!    `Timer` frame stamped `max(deadline, last_stamp)` and pop it, in global
//!    deadline order (not schedule order, not id order);
//! 3. then append the pass's `k` client frames, each stamped
//!    `max(now, last_stamp)`.
//!
//! `last_stamp` is the archived high-water mark of every stamp ever issued.
//! [`PassModel::leader_change`] models the clamp seed a fresh leader inherits
//! on takeover: `last_stamp = max(last_stamp, new_seed)` — a new leader may
//! inherit a stamp from a leader whose *clock* ran ahead of this one's `now`,
//! so the model must never let a frame's stamp regress relative to it.
//!
//! Only *due* timers fire in a pass — a timer scheduled with a deadline
//! already in the past relative to the pass's `now` still waits for the next
//! `pass()` call, where it fires late (`stamp > deadline`). This mirrors the
//! real node: a pass never looks back in time for timers it has already
//! stepped past.
//!
//! [`PassModel::check`] is the reference predicate for the §4.3 ordering
//! invariant, five rules:
//!
//! 1. stamps never decrease across the frame sequence;
//! 2. an on-time timer frame (`stamp == deadline`) is never preceded by a
//!    frame stamped past that deadline;
//! 3. every frame after a timer frame is stamped no earlier than that
//!    timer's own stamp;
//! 4. no timer frame is stamped earlier than its own deadline (never early);
//! 5. **lateness must pre-date the pass.** A timer frame stamped *past* its
//!    deadline (late) is legitimate only when that deadline was already
//!    behind the clock *before this pass began* — a previous leader's clock
//!    ran ahead (`leader_change`'s seed), or the deadline was already past
//!    when scheduled. Rules 1-4 alone cannot tell a genuinely late timer
//!    from the clients-before-timers bug: the clamp (`stamp =
//!    max(natural, last_stamp)`) makes *any* consistently-clamped append
//!    order monotone and clamp-safe by construction, whichever kind runs
//!    first — reordering the two loops with the clamp intact trips none of
//!    rules 1-4 (proven, and confirmed empirically: see the report for
//!    Task 12). What it DOES do is convert what should have been an
//!    on-time firing into a late one, because an earlier same-pass frame
//!    (the misplaced clients) advanced `last_stamp` past the timer's
//!    deadline before the timer's own turn. Rule 5 catches exactly that: it
//!    records, on every frame, the `last_stamp` value at the start of the
//!    pass that produced it (`pass_start_stamp`), and requires that any
//!    late timer's deadline already precede that value — i.e. the timer was
//!    *already* overdue when the pass began, not made overdue by a
//!    same-pass predecessor.

/// A tiny crate-local xorshift64 RNG, matching the shape of
/// [`crate::world`]'s private copy (kept separate — that one is private to
/// `world.rs` — so this module stays a leaf with no dependency on it and no
/// `rand` crate dependency anywhere in `uc_sim`).
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        })
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// Uniform in `[lo, hi)`. Panics if the range is empty or inverted.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(hi > lo, "Rng::range: empty or inverted range [{lo}, {hi})");
        lo + self.next_u64() % (hi - lo)
    }

    /// Uniform in `[lo, hi)` over signed values (the leader-change clock
    /// skew, which can go negative). Panics if the range is empty or
    /// inverted.
    pub fn range_i64(&mut self, lo: i64, hi: i64) -> i64 {
        assert!(
            hi > lo,
            "Rng::range_i64: empty or inverted range [{lo}, {hi})"
        );
        let span = (hi - lo) as u64;
        lo + (self.next_u64() % span) as i64
    }

    /// `true` with probability `1 / one_in_n`. Panics if `one_in_n == 0`.
    pub fn chance(&mut self, one_in_n: u64) -> bool {
        assert!(one_in_n > 0, "Rng::chance: one_in_n must be nonzero");
        self.next_u64().is_multiple_of(one_in_n)
    }
}

/// A frame kind: a fired timer, or an ordinary client-submitted frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Client,
    Timer,
}

/// One appended frame. `deadline` is `Some` only for `Kind::Timer`.
/// `pass_start_stamp` is `last_stamp` as it stood when the pass that
/// produced this frame began — every frame from the same `pass()` call
/// carries the same value, which is what rule 5 keys on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Frame {
    pub kind: Kind,
    pub stamp: u64,
    pub deadline: Option<u64>,
    pub pass_start_stamp: u64,
}

/// A pending, not-yet-fired timer: `(id, deadline)`.
type Pending = (u64, u64);

/// Pure model of the §4.3 leader pass: `now`, the pending-timer set, the
/// archived `last_stamp` high-water mark, and the frame sequence appended so
/// far.
pub struct PassModel {
    now: u64,
    last_stamp: u64,
    pending: Vec<Pending>,
    frames: Vec<Frame>,
}

impl PassModel {
    /// `seed_stamp` is the initial `last_stamp` — the archived high-water
    /// mark this model starts from (0 for a cluster's very first pass).
    pub fn new(seed_stamp: u64) -> Self {
        Self {
            now: 0,
            last_stamp: seed_stamp,
            pending: Vec::new(),
            frames: Vec::new(),
        }
    }

    /// Step 1 of §4.3: read the clock.
    pub fn set_now(&mut self, now: u64) {
        self.now = now;
    }

    /// Schedule a timer with the given `id` and `deadline`. It fires on the
    /// first `pass()` whose `now >= deadline`.
    pub fn schedule(&mut self, id: u64, deadline: u64) {
        self.pending.push((id, deadline));
    }

    /// Models the clamp seed a fresh leader inherits: `last_stamp =
    /// max(last_stamp, new_seed)`. Never regresses `last_stamp`.
    pub fn leader_change(&mut self, new_seed: u64) {
        self.last_stamp = self.last_stamp.max(new_seed);
    }

    pub fn last_stamp(&self) -> u64 {
        self.last_stamp
    }

    pub fn frames(&self) -> &[Frame] {
        &self.frames
    }

    /// Runs one leader pass: fires every **due** pending timer in global
    /// deadline order (steps 2), each stamped `max(deadline, last_stamp)`,
    /// then appends `k` client frames (step 3), each stamped `max(now,
    /// last_stamp)`. Timers strictly before clients, every pass — that
    /// ordering is the invariant under test.
    pub fn pass(&mut self, k: usize) {
        let now = self.now;
        // Captured once, before any frame of this pass is appended — every
        // frame this call produces carries this same value (rule 5).
        let pass_start_stamp = self.last_stamp;

        // Step 2: fire due timers in global deadline order. Re-scan each
        // iteration rather than pre-sorting once — no timer becomes newly
        // due mid-pass (now is fixed for the whole pass), so this is
        // equivalent to sorting the due subset by deadline and popping in
        // order, and it keeps the model a direct transcription of "while the
        // earliest pending due timer exists, pop it."
        loop {
            let due_earliest = self
                .pending
                .iter()
                .enumerate()
                .filter(|&(_, &(_, deadline))| deadline <= now)
                .min_by_key(|&(_, &(_, deadline))| deadline)
                .map(|(idx, _)| idx);
            let Some(idx) = due_earliest else {
                break;
            };
            let (_id, deadline) = self.pending.remove(idx);
            let stamp = self.last_stamp.max(deadline);
            self.last_stamp = stamp;
            self.frames.push(Frame {
                kind: Kind::Timer,
                stamp,
                deadline: Some(deadline),
                pass_start_stamp,
            });
        }

        // Step 3: append the pass's k client frames.
        for _ in 0..k {
            let stamp = self.last_stamp.max(now);
            self.last_stamp = stamp;
            self.frames.push(Frame {
                kind: Kind::Client,
                stamp,
                deadline: None,
                pass_start_stamp,
            });
        }
    }

    /// The §4.3 ordering + monotonicity predicate, checked against the frame
    /// sequence appended so far. Returns the first violation found, naming
    /// the offending frame indices.
    pub fn check(&self) -> Result<(), String> {
        // (a) stamps non-decreasing over the whole sequence.
        for i in 1..self.frames.len() {
            if self.frames[i].stamp < self.frames[i - 1].stamp {
                return Err(format!(
                    "stamp decreased: frame {} (stamp {}) < frame {} (stamp {})",
                    i,
                    self.frames[i].stamp,
                    i - 1,
                    self.frames[i - 1].stamp
                ));
            }
        }

        // (b) an on-time timer frame (stamp == deadline) is never preceded
        // by a frame stamped past its deadline.
        for (i, f) in self.frames.iter().enumerate() {
            if f.kind != Kind::Timer {
                continue;
            }
            let deadline = f.deadline.expect("timer frame missing deadline");
            if f.stamp != deadline {
                continue;
            }
            for (j, g) in self.frames[..i].iter().enumerate() {
                if g.stamp > deadline {
                    return Err(format!(
                        "frame {j} (stamp {}) precedes on-time timer frame {i} (deadline {deadline}) but exceeds the deadline",
                        g.stamp
                    ));
                }
            }
        }

        // (c) every frame after a timer frame is stamped no earlier than
        // that timer's own stamp.
        for (i, f) in self.frames.iter().enumerate() {
            if f.kind != Kind::Timer {
                continue;
            }
            for (j, g) in self.frames[i + 1..].iter().enumerate() {
                let j = i + 1 + j;
                if g.stamp < f.stamp {
                    return Err(format!(
                        "frame {j} (stamp {}) follows timer frame {i} (stamp {}) but has a smaller stamp",
                        g.stamp, f.stamp
                    ));
                }
            }
        }

        // (d) no timer frame fires early: stamp >= deadline, always.
        for (i, f) in self.frames.iter().enumerate() {
            if f.kind != Kind::Timer {
                continue;
            }
            let deadline = f.deadline.expect("timer frame missing deadline");
            if f.stamp < deadline {
                return Err(format!(
                    "timer frame {i} fired early: stamp {} < deadline {deadline}",
                    f.stamp
                ));
            }
        }

        // (e) rule 5: lateness must pre-date the pass. A TIMER frame stamped
        // past its own deadline (late) is legitimate only if that deadline
        // was already behind the clock before this pass began
        // (deadline < pass_start_stamp). A late timer whose deadline was
        // NOT yet behind the pass's own start stamp can only have become
        // late because some earlier frame in the SAME pass (same
        // pass_start_stamp) already pushed the clock past its deadline
        // before this timer's turn — the clients-before-timers bug. Name
        // both the late timer's index and, when found, the same-pass
        // predecessor's index that is responsible.
        for (i, f) in self.frames.iter().enumerate() {
            if f.kind != Kind::Timer {
                continue;
            }
            let deadline = f.deadline.expect("timer frame missing deadline");
            if f.stamp <= deadline {
                continue; // on-time or early (already ruled out by (d)) — not late
            }
            if deadline < f.pass_start_stamp {
                continue; // legitimately late: already overdue before this pass began
            }
            let culprit = self.frames[..i]
                .iter()
                .enumerate()
                .find(|(_, g)| g.pass_start_stamp == f.pass_start_stamp && g.stamp > deadline);
            return Err(match culprit {
                Some((j, g)) => format!(
                    "timer frame {i} (deadline {deadline}) fired late (stamp {}) though its \
                     deadline was not yet due when its pass began (pass_start_stamp {}); frame \
                     {j} (stamp {}), appended earlier in the SAME pass, already moved the clock \
                     past the deadline before this timer's turn — a same-pass frame ran ahead of \
                     a due timer",
                    f.stamp, f.pass_start_stamp, g.stamp
                ),
                None => format!(
                    "timer frame {i} (deadline {deadline}) fired late (stamp {}) though its \
                     deadline was not yet due when its pass began (pass_start_stamp {}), and no \
                     same-pass predecessor frame was found to blame — the model itself is \
                     inconsistent, investigate",
                    f.stamp, f.pass_start_stamp
                ),
            });
        }

        Ok(())
    }
}
