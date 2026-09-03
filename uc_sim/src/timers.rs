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
//! so the model must never let a frame's stamp regress relative to it. A
//! lower seed than the current `last_stamp` must leave it unchanged; see
//! `tests::leader_change_clamps_up_never_down_and_the_next_frame_reflects_it`
//! below, which pins both directions with a real assertion (not just by
//! reading the `max` in this doc comment).
//!
//! Only *due* timers fire in a pass — a timer scheduled with a deadline
//! already in the past relative to the pass's `now` still waits for the next
//! `pass()` call, where it fires late (`stamp > deadline`). This mirrors the
//! real node: a pass never looks back in time for timers it has already
//! stepped past.
//!
//! [`PassModel::check`] is the reference predicate for the §4.3 ordering
//! invariant, five rules. **Rules 1, 4 and 5 are load-bearing — rules 2 and
//! 3 are logical consequences of rule 1 (stamps non-decreasing ⇒
//! transitively implies both) and exist only for diagnosis, because their
//! error messages name the specific timer and its deadline rather than just
//! two arbitrary out-of-order frames** — `check()` runs 2 and 3 before 1 so
//! that, when both would fire, the more specific message wins.
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
    ///
    /// Rules 2 and 3 run BEFORE rule 1: they are logical consequences of
    /// rule 1 (non-decreasing stamps transitively implies both), so if rule
    /// 1 ran first its `Err` would always fire first and rules 2/3's more
    /// specific, timer-and-deadline-named messages would be unreachable
    /// dead code. Rules 1, 4 and 5 are the load-bearing ones; 2 and 3 exist
    /// purely for diagnosis.
    pub fn check(&self) -> Result<(), String> {
        // Rule 2: an on-time timer frame (stamp == deadline) is never
        // preceded by a frame stamped past its deadline. Diagnostic only —
        // implied by rule 1 — run first so its message wins when it names
        // the same violation.
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

        // Rule 3: every frame after a timer frame is stamped no earlier
        // than that timer's own stamp. Diagnostic only — implied by rule 1
        // — run before it for the same reason as rule 2.
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

        // Rule 1 (load-bearing): stamps non-decreasing over the whole
        // sequence.
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

        // Rule 4 (load-bearing): no timer frame fires early: stamp >=
        // deadline, always.
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

        // Rule 5 (load-bearing): lateness must pre-date the pass. A TIMER
        // frame stamped past its own deadline (late) is legitimate only if
        // that deadline was already behind the clock before this pass began
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
                continue; // on-time or early (already ruled out by rule 4) — not late
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins `leader_change`'s clamp directly: a lower seed must leave
    /// `last_stamp` unchanged (never regress it), a higher seed must raise
    /// it, and the very next frame appended must be stamped at the raised
    /// value even when `now` is below it — the clamp, not just `now`,
    /// governs the stamp. The seeded property test
    /// (`leader_pass_model_keeps_timers_in_order_across_leader_changes`)
    /// exercises `leader_change` with real up/down seeds too (Task 12 fix
    /// 2), but this unit test pins the exact mechanism with a single,
    /// readable trace, independent of any RNG draw.
    #[test]
    fn leader_change_clamps_up_never_down_and_the_next_frame_reflects_it() {
        let mut m = PassModel::new(1_000);

        // A lower seed must leave last_stamp unchanged (never regress it).
        m.leader_change(500);
        assert_eq!(
            m.last_stamp(),
            1_000,
            "a lower seed must not lower last_stamp"
        );

        // A higher seed must raise last_stamp.
        m.leader_change(5_000);
        assert_eq!(m.last_stamp(), 5_000, "a higher seed must raise last_stamp");

        // Even with `now` well below the raised last_stamp, the next client
        // frame must be stamped at the raised value, never at `now`.
        m.set_now(100);
        m.pass(1);
        let frame = m.frames().last().expect("pass(1) should append one frame");
        assert_eq!(frame.kind, Kind::Client);
        assert_eq!(
            frame.stamp, 5_000,
            "the client frame must be clamped to last_stamp, not now"
        );
    }
}
