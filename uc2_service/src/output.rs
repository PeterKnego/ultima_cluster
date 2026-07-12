// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The output agent duty cycle (spec §7, Task 12): leader-only, at-least-once
//! side effects. A single polling thread, spawned ONLY when the builder is
//! given a non-[`NoopOutput`](crate::traits::NoopOutput) handler
//! ([`crate::ServiceBuilder::start`]), owning its OWN `current_thread` tokio
//! runtime and its OWN [`LogFollower`] cursor (independent of the apply
//! agent's) over the same log buffer.
//!
//! **Leader gating, both directions (v1 contract verbatim — position is the
//! idempotency key):**
//! * Deposed (not leader): idle. The in-memory cursor is left wherever it is —
//!   it is never the source of truth for what has been delivered.
//! * (Re)gained leadership: the cursor is reset to
//!   `cnc.status().output_progress.load_acquire()` — the node's DURABLY
//!   persisted marker — and the agent replays `(marker, commit]` forward. This
//!   happens uniformly on first attach AND on every become-leader transition
//!   (including this same process regaining a leadership it briefly lost), so
//!   one rule covers both. Because the node's persist has a 100 ms floor (Task
//!   12 / `uc2_node::node`), `output_progress` can legitimately lag the
//!   in-page `output_completed` high-water mark — replaying that gap is
//!   redundant but always safe (at-least-once); it can never cause a SKIP.
//!
//! **Bounds:** never runs ahead of apply — the target is
//! `min(commit, durable, service_applied)`, so `state` passed to
//! `on_committed` always already reflects `cmd`'s effect.
//!
//! **Retry:** `Ok`/`Permanent` (after a warn log) advance
//! `service().output_completed`; `Retryable` retries with a bounded backoff
//! (10 ms → 500 ms, doubling) for as long as this node stays leader. Losing
//! leadership mid-retry abandons the frame WITHOUT advancing the marker — the
//! next become-leader transition resets the cursor to the (unaffected, still
//! durable) marker and redelivers it, per the at-least-once contract above.
//!
//! **Journal degrade:** reads through [`LogFollower`], same as the apply
//! agent; an `Overrun` (the live buffer scrolled past the cursor) degrades to
//! walking the archived journal via [`ultima_journal::TailReader`] — the same
//! primitive `crate::replay` uses for apply reconstruction — delivering every
//! `MESSAGE` frame at or after the resume cursor instead of applying it to the
//! SM. This is a SEPARATE small walker (not a reuse of `replay::replay_into`,
//! which is apply-specific: it calls `sm.apply`, dispatches by
//! `sm.last_applied()`, and never touches the output side); it shares only the
//! frame-walking shape, and both the live-batch path and the degrade path
//! funnel each frame through the SAME [`deliver`] helper so the leader-gated
//! retry/persist logic is written exactly once.
//!
//! **Borrow-checker note:** every helper below takes DECOMPOSED field
//! references (`&Arc<Mutex<S>>`, `&CncPage`, `&O`, `&Runtime`, ...) rather
//! than `&OutputState`. `output_cycle`'s live-batch loop holds a live `&mut`
//! borrow of `st.follower.cursor` (via the `FrameIter` `next_batch` returns)
//! for the loop's duration; passing `&st.sm` etc. as separate arguments keeps
//! those borrows visibly disjoint from `st.follower` to the borrow checker,
//! the same discipline `apply.rs`'s module doc calls out for its own loop.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use uc2_log::cnc::CncPage;
use uc2_log::reader::{Batch, LogFollower};
use uc_protocol::v2::cnc::NODE_FLAG_LEADER;
use uc_protocol::v2::frame::{self, FRAME_TYPE_MESSAGE, HEADER_LEN, align_frame_len};
use ultima_journal::TailReader;

use crate::traits::{OutputError, OutputHandler, StateMachine};

/// Retry backoff bounds for a `Retryable` `on_committed` (spec §7 / Task 12):
/// start at 10 ms, double each attempt, cap at 500 ms.
const BACKOFF_MIN: Duration = Duration::from_millis(10);
const BACKOFF_MAX: Duration = Duration::from_millis(500);

/// Everything the output thread owns. Mirrors `ApplyState`'s shape (spec §7):
/// its own `LogFollower` cursor, a SHARED handle to the apply thread's state
/// machine (so `state: &S` reflects whatever apply has applied so far — never
/// less than `cmd`'s own effect, by the `target` bound below), and its own
/// `current_thread` tokio runtime for `rt.block_on(handler.on_committed(..))`.
pub(crate) struct OutputState<S: StateMachine, O: OutputHandler<S>> {
    pub(crate) follower: LogFollower,
    pub(crate) sm: Arc<Mutex<S>>,
    pub(crate) cnc: Arc<CncPage>,
    pub(crate) handler: O,
    pub(crate) rt: tokio::runtime::Runtime,
    pub(crate) journal_dir: std::path::PathBuf,
    /// Edge-detector for the become-leader transition (see the module doc):
    /// `false` whenever the last cycle observed NOT-leader (or this is the
    /// first cycle), so the NEXT leader cycle resets the cursor from the
    /// node's durable marker before doing any work.
    pub(crate) was_leader: bool,
    /// The node `instance_id` this incarnation attached to (M5 final review
    /// #2c) — a change means the node restarted; this thread must fail-stop
    /// rather than keep writing `output_completed` onto the new generation's
    /// page. Checked with the same two-consecutive-cycle derace as the apply
    /// loop, via [`crate::apply::check_node_instance`].
    pub(crate) instance_id: u128,
    pub(crate) instance_mismatch_streak: u8,
}

impl<S: StateMachine, O: OutputHandler<S>> OutputState<S, O> {
    pub(crate) fn new(
        follower: LogFollower,
        sm: Arc<Mutex<S>>,
        cnc: Arc<CncPage>,
        handler: O,
        journal_dir: std::path::PathBuf,
        instance_id: u128,
    ) -> std::io::Result<Self> {
        // `rt` (thread scheduling) + `time` (enabled for a future handler that
        // wants `tokio::time` inside `on_committed`; the backoff sleeps here go
        // through plain `std::thread::sleep`) — "rt + time features only" per
        // the plan.
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().build()?;
        Ok(Self {
            follower,
            sm,
            cnc,
            handler,
            rt,
            journal_dir,
            was_leader: false,
            instance_id,
            instance_mismatch_streak: 0,
        })
    }
}

#[inline]
fn is_leader(cnc: &CncPage) -> bool {
    cnc.status().flags.load_acquire() & NODE_FLAG_LEADER != 0
}

/// One output duty cycle. Returns `true` iff it made progress (drove the idle
/// strategy). See the module doc for the full contract.
pub(crate) fn output_cycle<S: StateMachine, O: OutputHandler<S>>(
    st: &mut OutputState<S, O>,
) -> bool {
    // Node-restart fail-stop (M5 final review #2c), checked BEFORE the leader
    // gate so a deposed output thread on a restarted node still fail-stops rather
    // than lingering as a zombie writer of `output_completed`.
    crate::apply::check_node_instance(&st.cnc, st.instance_id, &mut st.instance_mismatch_streak);

    if !is_leader(&st.cnc) {
        // Deposed (or never yet leader): idle. Leave the cursor exactly where
        // it is — it is NOT the source of truth (`output_progress` is); the
        // next become-leader edge resets it.
        st.was_leader = false;
        return false;
    }
    if !st.was_leader {
        // Just (re)gained leadership: reset the cursor to the node's durable
        // marker and replay forward from there (module doc).
        let marker = st.cnc.status().output_progress.load_acquire();
        st.follower.cursor = marker;
        st.was_leader = true;
    }

    let counters = st.cnc.counters();
    let service_applied = st.cnc.service().service_applied.load_acquire();
    // Never run ahead of apply: `state` must already reflect `cmd`'s effect.
    let target =
        counters.commit.load_acquire().min(counters.durable.load_acquire()).min(service_applied);

    let mut progressed = false;
    'cycle: loop {
        if !is_leader(&st.cnc) {
            break;
        }
        let cursor_before = st.follower.cursor;
        let overrun = match st.follower.next_batch(target) {
            Batch::CaughtUp => break,
            Batch::Overrun => true,
            Batch::Frames(frames) => {
                for (pos, hdr, payload) in frames {
                    if hdr.frame_type != FRAME_TYPE_MESSAGE {
                        continue;
                    }
                    // Computed independently of `st.follower.cursor` (which the
                    // iterator already advanced past this frame the instant it
                    // was yielded) — see the module doc on why the in-memory
                    // cursor is allowed to run ahead of what's actually been
                    // delivered.
                    let frame_end = pos + align_frame_len(hdr.length as usize) as u64;
                    let (cmd, _) = bincode::serde::decode_from_slice::<S::Command, _>(
                        payload,
                        bincode::config::standard(),
                    )
                    .expect("corrupt committed frame (fail-stop)");
                    // Decomposed field args (not `&*st`): `frames` still holds a
                    // live `&mut` borrow of `st.follower.cursor` for the rest of
                    // this loop body, so touching only the OTHER fields keeps
                    // the borrows disjoint (module doc).
                    if !deliver(&st.sm, &st.cnc, &st.handler, &st.rt, pos, frame_end, &cmd) {
                        // Lost leadership mid-frame: abandon it (unpersisted —
                        // the next become-leader edge redelivers it) and stop
                        // touching the log this cycle.
                        st.was_leader = false;
                        break 'cycle;
                    }
                    progressed = true;
                }
                if st.follower.cursor == cursor_before {
                    break;
                }
                false
            }
        };
        if overrun {
            let outcome = output_replay_degrade(
                &st.sm,
                &st.cnc,
                &st.handler,
                &st.rt,
                &st.journal_dir,
                st.follower.cursor,
            );
            match outcome {
                ReplayOutcome::Cursor(c) => {
                    st.follower.cursor = c;
                    progressed = true;
                    // Re-loop: read live from the rejoin point.
                }
                ReplayOutcome::LostLeadership(c) => {
                    st.follower.cursor = c;
                    st.was_leader = false;
                    break;
                }
            }
        }
    }
    progressed
}

/// Deliver one committed `MESSAGE` frame to the handler: retry `Retryable`
/// with the bounded doubling backoff for as long as this node stays leader;
/// `Ok`/`Permanent` (after a warn log) persist `output_completed` and return
/// `true`. Returns `false` (frame NOT delivered, marker NOT advanced) iff
/// leadership was lost before the frame resolved.
///
/// **SM lock across the handler call:** `on_committed` takes `&S`, so the
/// `Mutex` guard is held for the duration of `rt.block_on(..)` — v1-verbatim
/// semantics (the pre-M5 design held an `RwLock` read the same way). This
/// blocks the APPLY thread's next `sm.lock()` for as long as the handler
/// takes — and with it QUERIES too: the query-ring drain (`apply.rs`'s
/// `drain_queries`, running on the apply thread to answer client reads) and
/// the embedded `Service::query` path (`lib.rs`) take this SAME `Mutex`, so a
/// slow handler stalls reads as well as applies. A real degradation for a
/// slow/blocking handler, flagged in the task report as a concern for the
/// controller rather than silently accepted.
fn deliver<S: StateMachine, O: OutputHandler<S>>(
    sm: &Arc<Mutex<S>>,
    cnc: &CncPage,
    handler: &O,
    rt: &tokio::runtime::Runtime,
    pos: u64,
    frame_end: u64,
    cmd: &S::Command,
) -> bool {
    let mut backoff = BACKOFF_MIN;
    loop {
        if !is_leader(cnc) {
            return false;
        }
        let result = {
            let guard = sm.lock().unwrap();
            rt.block_on(handler.on_committed(pos, cmd, &guard))
        };
        match result {
            Ok(()) => {
                store_output_completed(cnc, frame_end);
                return true;
            }
            Err(OutputError::Permanent(msg)) => {
                eprintln!(
                    "uc2_service: on_committed PERMANENT failure at position {pos}: {msg} \
                     (advancing output_progress anyway per the OutputError::Permanent contract)"
                );
                store_output_completed(cnc, frame_end);
                return true;
            }
            Err(OutputError::Retryable(_)) => {
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(BACKOFF_MAX);
                // Loop: re-check leadership, retry.
            }
        }
    }
}

/// `output_completed.store_release(frame_end)`, taking the max with whatever
/// is already there. Plain single-writer `frame_end` progress can only ever
/// increase in the live-batch path, but the journal-degrade replay path can
/// legitimately redeliver a frame BELOW an already-advanced `output_completed`
/// (the reset-to-marker rule on a become-leader edge intentionally floors the
/// cursor below the in-page high-water mark — module doc) — the `max` keeps
/// this shared, cross-process-visible counter monotonic even though a
/// redelivery pass would otherwise overwrite it backwards.
fn store_output_completed(cnc: &CncPage, frame_end: u64) {
    let cell = &cnc.service().output_completed;
    let current = cell.load_acquire();
    if frame_end > current {
        cell.store_release(frame_end);
    }
}

enum ReplayOutcome {
    /// Replay reached the live rejoin point (or the current `target`); resume
    /// the live follower from here.
    Cursor(u64),
    /// Leadership was lost mid-replay; resume from here on the NEXT
    /// become-leader edge (which resets to the durable marker again anyway —
    /// this cursor value is never read back beyond that reset).
    LostLeadership(u64),
}

/// Walk the archived journal (via `TailReader`, exactly as
/// `crate::replay::replay_into` does for apply) from `resume_from`, delivering
/// every `MESSAGE` frame at or after it whose END is `<= target` (target
/// re-read per block, since both counters can advance while this runs) to the
/// SAME [`deliver`] leader-gated retry/persist path the live batch loop uses.
/// Returns the byte cursor the live follower should rejoin from.
fn output_replay_degrade<S: StateMachine, O: OutputHandler<S>>(
    sm: &Arc<Mutex<S>>,
    cnc: &CncPage,
    handler: &O,
    rt: &tokio::runtime::Runtime,
    journal_dir: &Path,
    resume_from: u64,
) -> ReplayOutcome {
    let reader = TailReader::open(journal_dir)
        .expect("journal replay (output) open fail-stop: cannot reconstruct output progress");
    let mut cursor = resume_from;
    let mut lost_leadership = false;

    reader
        .scan(|_seq, base, payload| {
            let counters = cnc.counters();
            let service_applied = cnc.service().service_applied.load_acquire();
            let target = counters
                .commit
                .load_acquire()
                .min(counters.durable.load_acquire())
                .min(service_applied);

            let mut off = 0usize;
            while off + HEADER_LEN <= payload.len() {
                let hdr = frame::read_header(&payload[off..]);
                let total = hdr.length as usize;
                let aligned = align_frame_len(total);
                if total < HEADER_LEN || off + aligned > payload.len() {
                    break;
                }
                let pos = base + off as u64;
                let end = pos + aligned as u64;
                if end > target {
                    return false; // stop the whole scan at this frame boundary
                }
                if hdr.frame_type == FRAME_TYPE_MESSAGE && pos >= resume_from {
                    if !is_leader(cnc) {
                        lost_leadership = true;
                        return false;
                    }
                    let (cmd, _) = bincode::serde::decode_from_slice::<S::Command, _>(
                        &payload[off + HEADER_LEN..off + total],
                        bincode::config::standard(),
                    )
                    .expect("corrupt archived MESSAGE frame (fail-stop)");
                    if !deliver(sm, cnc, handler, rt, pos, end, &cmd) {
                        lost_leadership = true;
                        return false;
                    }
                }
                cursor = end;
                off += aligned;
            }
            true
        })
        .expect("journal replay (output) scan fail-stop");

    if lost_leadership { ReplayOutcome::LostLeadership(cursor) } else { ReplayOutcome::Cursor(cursor) }
}
