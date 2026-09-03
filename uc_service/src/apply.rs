// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The apply agent duty cycle (spec §7). A single polling thread that follows
//! the committed log, applies each `MESSAGE` frame to the user's state machine,
//! and (while leader) publishes the response onto the egress broadcast. On an
//! `Overrun` (the live buffer scrolled past the cursor) it degrades to journal
//! replay (Task 9) and rejoins the live buffer at the byte position replay
//! reached.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{SystemTime, UNIX_EPOCH};

use uc_log::cnc::CncPage;
use uc_log::reader::{Batch, LogFollower};
use uc_protocol::ring::{RingError, SpscConsumer, SpscProducer};
use uc_protocol::v2::cnc::NODE_FLAG_LEADER;
use uc_protocol::v2::frame::{
    FLAG_TIMER_TABLE, FRAME_TYPE_MESSAGE, FRAME_TYPE_TIMER, read_timer_body,
};
use uc_protocol::v2::ipc::{MSG_V2_SCHED, SchedOp, SchedRecord, write_sched_record};

use crate::builder_agent::BuildJob;
use crate::config::SnapshotError;
use crate::egress::Egress;
use crate::replay::{Replay, replay_into};
use crate::traits::{ApplyCtx, RawStateMachine, TimerEvent};

/// Time-and-timers §4.8: how many spins `write_sched` has taken waiting on a
/// full `svc_sched` ring, process-wide. Not yet exported through a metrics
/// surface (uc_service has no `uc_obs` dependency) — a later task may wire
/// it up; for now it exists so a spin storm leaves a countable trace.
pub(crate) static SCHED_RING_FULL_SPINS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Spike-only apply-budget probes (feature `apply-profile`). Counters are
/// process-global; printed every `PRINT_EVERY` frames and at drop. Since M12a
/// the codec lives INSIDE the state machine call (the blanket
/// [`RawStateMachine`](crate::RawStateMachine) impl decodes the command and
/// encodes the response), so `sm_apply` is "apply incl. codec" — there is no
/// separate decode/encode column to report any more.
#[cfg(feature = "apply-profile")]
pub(crate) mod profile {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    use std::time::Instant;

    pub static FRAMES: AtomicU64 = AtomicU64::new(0);
    /// Whole `RawStateMachine::apply` call — for a typed SM that includes the
    /// bincode decode of the command and the encode of the response.
    pub static SM_APPLY: AtomicU64 = AtomicU64::new(0);
    pub static PUBLISH: AtomicU64 = AtomicU64::new(0);
    pub static BATCH: AtomicU64 = AtomicU64::new(0);
    pub static CYCLE: AtomicU64 = AtomicU64::new(0);
    pub static CYCLES_CALLS: AtomicU64 = AtomicU64::new(0);
    pub static PAYLOAD_BYTES: AtomicU64 = AtomicU64::new(0);
    const PRINT_EVERY: u64 = 1_000_000;

    #[inline(always)]
    pub fn now() -> u64 {
        #[cfg(target_arch = "x86_64")]
        // SAFETY: rdtsc has no preconditions.
        unsafe {
            core::arch::x86_64::_rdtsc()
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
            START.get_or_init(Instant::now).elapsed().as_nanos() as u64
        }
    }

    /// Cycles per nanosecond, calibrated once against the wall clock.
    fn cyc_per_ns() -> f64 {
        static CAL: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
        *CAL.get_or_init(|| {
            let t0 = Instant::now();
            let c0 = now();
            while t0.elapsed().as_millis() < 50 {}
            let c1 = now();
            (c1 - c0) as f64 / t0.elapsed().as_nanos() as f64
        })
    }

    pub fn add(frames: u64, sm_apply: u64, publish: u64, bytes: u64) {
        SM_APPLY.fetch_add(sm_apply, Relaxed);
        PUBLISH.fetch_add(publish, Relaxed);
        PAYLOAD_BYTES.fetch_add(bytes, Relaxed);
        let before = FRAMES.fetch_add(frames, Relaxed);
        if before / PRINT_EVERY != (before + frames) / PRINT_EVERY {
            report("periodic");
        }
    }

    pub fn report(tag: &str) {
        let k = cyc_per_ns();
        let f = FRAMES.load(Relaxed).max(1) as f64;
        let app = SM_APPLY.load(Relaxed) as f64;
        let pubc = PUBLISH.load(Relaxed) as f64;
        let batch = BATCH.load(Relaxed) as f64;
        let cycle = CYCLE.load(Relaxed) as f64;
        let bytes = PAYLOAD_BYTES.load(Relaxed) as f64;
        eprintln!(
            "apply-profile[{tag}] frames={} avg_payload={:.0}B \
             per-frame: sm_apply={:.0}ns publish={:.0}ns batch_arm={:.0}ns \
             | sm_apply/batch_arm={:.1}% sm_apply/apply_cycle_total={:.1}% \
             batch_arm/apply_cycle_total={:.1}% apply_cycle_calls={}",
            f as u64,
            bytes / f,
            app / f / k,
            pubc / f / k,
            batch / f / k,
            100.0 * app / batch.max(1.0),
            100.0 * app / cycle.max(1.0),
            100.0 * batch / cycle.max(1.0),
            CYCLES_CALLS.load(Relaxed),
        );
    }
}

/// Boxed "freeze the current state and produce a streaming job" closure. Built
/// once, in [`crate::ServiceBuilder::start_with_snapshots`], where the
/// `S: SnapshotStateMachine` bound is available; stored here behind a plain
/// `S: RawStateMachine`-bounded type so [`ApplyState`] itself needs no such
/// bound.
pub(crate) type FreezeFn<S> = Box<dyn Fn(&S) -> Result<(BuildJob, u64), SnapshotError> + Send>;

/// Boxed "install this snapshot stream into the SM" closure (M6 Task 5). Same
/// type-erasure trick as [`FreezeFn`]: built in `start_with_snapshots` where
/// `S: SnapshotStateMachine`, called by the reconstruction path on the apply
/// thread with the SM lock held (install IS state mutation). Returns the
/// post-install position `S` (== the artifact's tag).
pub(crate) type InstallFn<S> =
    Box<dyn Fn(&mut S, u64, &mut dyn std::io::Read) -> Result<u64, SnapshotError> + Send>;

/// The apply thread's below-the-floor reconstruction capability (M6 Task 5).
/// Present only for a snapshot-capable service (`start_with_snapshots`); its
/// absence is what turns a below-floor gap into [`ServiceError::SnapshotRequired`]
/// fail-stop instead of a covering install.
pub(crate) struct SnapshotRestore<S: RawStateMachine> {
    pub(crate) store: crate::snapshots::SnapshotStore,
    pub(crate) install: InstallFn<S>,
}

/// M6 Task 3: the apply thread's half of the snapshot-builder handoff. Present
/// only when the service was started via `start_with_snapshots`; `None` for a
/// plain `start()` (or an SM that never opted in) means [`maybe_build_snapshot`]
/// is a no-op every cycle.
pub(crate) struct SnapshotTrigger<S: RawStateMachine> {
    pub(crate) policy: crate::config::SnapshotPolicy,
    /// The position basis for the next interval check: updated to the
    /// attempted position whenever a freeze is attempted (success OR failure)
    /// — see [`maybe_build_snapshot`]'s doc for why a failure still advances
    /// this rather than hot-looping a retry every cycle.
    pub(crate) last_snapshot_pos: u64,
    /// Shared with the builder thread's `BuilderState`. Gates BOTH directions
    /// of "one in-flight build max": checked here before even calling
    /// `freeze()`, held by the builder for the full stream+publish duration.
    pub(crate) busy: Arc<AtomicBool>,
    pub(crate) tx: mpsc::SyncSender<(u64, BuildJob)>,
    pub(crate) freeze: FreezeFn<S>,
}

/// Everything the apply thread owns. Fields are accessed directly (not through
/// `&self` methods) inside the frames loop so the borrow checker sees the
/// per-field disjoint borrows (`follower` iterated while `sm`/`egress`/`cnc`
/// are touched).
#[cfg(feature = "apply-profile")]
impl<S: RawStateMachine> Drop for ApplyState<S> {
    fn drop(&mut self) {
        profile::report("final");
    }
}

pub(crate) struct ApplyState<S: RawStateMachine> {
    /// **Poisoned incarnation** (2026-08-16 log-rewind contract). Set when the
    /// node truncates the log BENEATH what this SM already applied: our state
    /// belongs to a timeline that no longer exists. Once set, this incarnation
    /// applies nothing further and answers every query with RETRY — it must
    /// never serve dead-timeline state, nor resume applying on top of it (that
    /// merge is what elle sees as `incompatible-order`). Recovery is a FRESH
    /// incarnation, which reconstructs from the journal; `Service::is_alive`
    /// reports poisoned so a supervisor respawns it.
    ///
    /// Poisoning rather than panicking is deliberate: a panic kills the apply
    /// thread, which in-process leaves the node silently serving nothing and
    /// re-raises at teardown, and out-of-process still needs the supervisor to
    /// notice. A flag degrades safely in both worlds.
    pub(crate) poisoned: Arc<AtomicBool>,
    pub(crate) follower: LogFollower,
    /// The user state machine, behind `Arc<Mutex<S>>`. `Arc` (shared, not owned)
    /// so the `Service` handle can reach it for direct queries (the test/embedded
    /// query path until the client query ring lands in Task 10/11); `Mutex`
    /// (not `RwLock`) so sharing needs only `S: Send` — the `RawStateMachine`
    /// bound is `Send + 'static`, with no `Sync`. Task 11's `drain_queries` runs on
    /// THIS same apply thread right after the applies, taking the lock the same
    /// way the apply path does (single-threaded, so the read/write distinction a
    /// `RwLock` would give buys nothing here).
    pub(crate) sm: Arc<Mutex<S>>,
    pub(crate) cnc: Arc<CncPage>,
    pub(crate) egress: Egress,
    /// Reused response scratch for `RawStateMachine::apply` / `query`. Cleared
    /// before every call, so a steady-state response allocates nothing.
    pub(crate) resp_buf: Vec<u8>,
    /// The node's journal directory — the archived-log source the replay path
    /// reconstructs from on `Overrun`.
    pub(crate) journal_dir: PathBuf,
    /// The node→service query ring consumer half. Drained by Task 11's
    /// `drain_queries`; held here so the apply thread owns it (single reader).
    pub(crate) svc_query: SpscConsumer,
    /// Time-and-timers §4.4: the service→node schedule ring producer half —
    /// this process is the producer, the node's consensus agent the consumer.
    pub(crate) svc_sched: SpscProducer,
    /// Time-and-timers §4.8: re-announce this incarnation's pending timers
    /// (`sm.pending_timers()`) and, plan 2, its delivered table ticks
    /// (`sm.table_delivered()`) to the node on the FIRST cycle after attach,
    /// and again after every replay pass — a fresh incarnation's in-memory
    /// wrapper state (e.g. `Timed`'s pending set and `table_last`) is
    /// otherwise invisible to the node's scheduler until something
    /// re-declares it.
    pub(crate) announce_pending: bool,
    /// Observability: set while a batch has surfaced `Overrun` and the replay
    /// reconstruction is degrading the follower back onto the live buffer.
    /// Cleared once replay rejoins.
    pub(crate) needs_replay: bool,
    /// The artifact position a below-floor replay is waiting on
    /// (`Replay::AwaitArtifact`), so the wait is reported once per episode
    /// rather than once per cycle; `None` when not waiting.
    pub(crate) replay_wait: Option<u64>,
    /// The node `instance_id` this incarnation attached to (M5 final review
    /// #2c). A change means the node restarted and recreated the cnc page in
    /// place — this attachment is invalidated and this thread must fail-stop
    /// rather than keep writing `applied`/`heartbeat_ns` onto our slot on the
    /// NEW generation's page (a single-writer violation, and the enabler of the
    /// epoch-0 barrier collision guarded node-side in #1).
    pub(crate) instance_id: u128,
    /// Consecutive-cycle counter for the instance-mismatch derace (#2c): a node
    /// recreate is truncate → set_len → rewrite-in-place, so a single cycle can
    /// catch a torn/stale header. Only TWO consecutive confirmed mismatches
    /// fail-stop; any match or torn (`None`) read resets it.
    pub(crate) instance_mismatch_streak: u8,
    /// This incarnation's service epoch, captured at attach (M5 final review
    /// #5). Fixed for the life of this service incarnation — a newer incarnation
    /// would bump our slot's `epoch` on the shared page, but THIS thread must
    /// keep comparing forwarded reads against ITS OWN epoch, not whatever the
    /// slot now holds (re-reading live would make an old incarnation answer
    /// reads stamped for a newer one). #2c fail-stops on the node-restart
    /// case; this closes the same-node service-restart case.
    pub(crate) my_epoch: u64,
    /// M14a: which declared FSM slot this incarnation writes (`cfg.service_id`).
    pub(crate) service_id: u8,
    /// M14a Task 7: the lag barrier mode this incarnation runs under, fixed at
    /// attach (the page's lag config is boot-once, like `service_id`).
    pub(crate) lag_mode: crate::lag::LagMode,
    /// M14a Task 7: the effective declared-set bitmask (page `0` folded to
    /// `1`, the harness-node case) — `lag::floor`'s min ranges over this.
    pub(crate) declared: u64,
    /// M14a Task 7: true while this incarnation is mid wait-episode — so
    /// `lag_waits` counts EPISODES (the `false -> true` edge), not cycles.
    ///
    /// M14c2 ruling K: the episode ends when a FRAME MOVES, not when the plan
    /// stops saying `Wait`. A bounded cap that sits MID-FRAME is above the
    /// cursor, so `lag::plan` reports `Apply` with a target no frame can
    /// clear — a barrier stall the old "reset on any `Apply`" reset both
    /// missed (it never counted) and would have re-armed every cycle (it would
    /// have counted one episode per cycle). Set by [`note_lag_wait`], cleared
    /// only where the cursor advances.
    ///
    /// Two corners of that rule, both deliberate. A torn/`NotCommitted`
    /// interlude (`Batch::CaughtUp` under a capped target) is the LOG's doing,
    /// not the barrier's, and is never counted — the flag simply stays as it
    /// was. And in lockstep, a ladder that opens without a frame actually
    /// applying (the plan resolves, `next_batch` yields nothing) leaves the
    /// flag set, so the next park folds into the SAME episode rather than
    /// opening a new one.
    pub(crate) lag_waiting: bool,
    /// M6 Task 3: `Some` only for a service started via `start_with_snapshots`.
    pub(crate) snapshot_trigger: Option<SnapshotTrigger<S>>,
    /// M6 Task 5: below-floor reconstruction (snapshot install + tail replay).
    /// `Some` only for a snapshot-capable service; `None` makes a below-floor
    /// gap fail-stop with [`ServiceError::SnapshotRequired`].
    pub(crate) snapshot_restore: Option<SnapshotRestore<S>>,
}

/// Write schedule records to the node; a full ring is transient (the node
/// drains every pass), so spin like the egress path does, and count it.
fn write_sched(prod: &mut SpscProducer, recs: &[SchedRecord]) {
    for r in recs {
        let bytes = write_sched_record(r);
        loop {
            match prod.try_write(MSG_V2_SCHED, 0, [0; 8], &bytes) {
                Ok(()) => break,
                Err(RingError::Full) => {
                    SCHED_RING_FULL_SPINS.fetch_add(1, Ordering::Relaxed);
                    std::thread::yield_now();
                }
                Err(e) => panic!("svc_sched ring fail-stop: {e}"),
            }
        }
    }
}

/// One apply duty cycle. Returns `true` iff it made progress (drove the idle
/// strategy). Follows the plan skeleton exactly:
/// target = `min(commit, durable)`; apply every committed `MESSAGE` up to it;
/// skip already-applied positions (idempotent re-entry) and non-`MESSAGE`
/// frames; publish responses only while leader. On `Overrun`, reconstruct via
/// journal replay and rejoin the live buffer.
pub(crate) fn apply_cycle<S: RawStateMachine>(st: &mut ApplyState<S>) -> bool {
    #[cfg(feature = "apply-profile")]
    let _cycle_guard = {
        struct G(u64);
        impl Drop for G {
            fn drop(&mut self) {
                profile::CYCLE.fetch_add(
                    profile::now() - self.0,
                    std::sync::atomic::Ordering::Relaxed,
                );
                profile::CYCLES_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        G(profile::now())
    };
    // Node-restart fail-stop (M5 final review #2c, plan decision #9): a node
    // restart recreates the cnc page in place with a fresh random `instance_id`,
    // invalidating every attachment. Detect it before doing any work so this
    // apply thread stops being a zombie writer of `service_applied`/heartbeats on
    // the NEW generation's page. Run once per duty cycle.
    check_node_instance(&st.cnc, st.instance_id, &mut st.instance_mismatch_streak);

    let c = st.cnc.counters();
    // Apply frontier = the lesser of quorum-commit and local durability. Both
    // acquire-loaded from the shared cnc page.
    let durable = c.durable.load_acquire();
    // Log-rewind tripwire (2026-08-16 acked-write-loss hunt): `durable` below
    // our applied cursor means the node truncated/primed the log BENEATH state
    // this SM already applied — our state is from a dead timeline. Idling here
    // would serve stale answers for the whole refill and then MERGE two
    // timelines once the log regrows past the cursor (the elle
    // `incompatible-order` divergence). Poison the incarnation instead: stop
    // applying, refuse every query, and let a supervisor respawn a fresh
    // service that reconstructs from the journal. Gated on a matching instance
    // id so a node-restart's zeroed page stays `check_node_instance`'s case.
    if durable < st.follower.cursor
        && st.cnc.try_instance_id() == Some(st.instance_id)
        && !st.poisoned.swap(true, Ordering::Release)
    {
        eprintln!(
            "uc_service: log rewound beneath the applied frontier (durable {durable} < \
             applied cursor {}) — this incarnation's state is from a truncated timeline. \
             Poisoned: applying nothing further and refusing reads until respawned.",
            st.follower.cursor,
        );
    }
    if st.poisoned.load(Ordering::Acquire) {
        // Refuse reads (RETRY, side-effect-free) so no client can observe the
        // dead timeline; keep the heartbeat so the node sees a live-but-
        // poisoned service rather than a hung one.
        refuse_queries(st);
        crate::attach::slot(&st.cnc, st.service_id)
            .heartbeat_ns
            .store_release(unix_ns());
        return false;
    }
    if st.announce_pending {
        st.announce_pending = false;
        let (pending, table_delivered) = {
            let sm = st.sm.lock().unwrap();
            (sm.pending_timers(), sm.table_delivered())
        };
        let mut recs: Vec<SchedRecord> = pending
            .into_iter()
            .map(|(id, dl)| SchedRecord {
                op: SchedOp::Schedule,
                timer_id: id,
                deadline_ns: dl,
            })
            .collect();
        recs.extend(table_delivered.into_iter().map(|(id, dl)| SchedRecord {
            op: SchedOp::TableConsumed,
            timer_id: id,
            deadline_ns: dl,
        }));
        write_sched(&mut st.svc_sched, &recs);
    }
    let commit = c.commit.load_acquire();
    // The log's own frontier, for ruling K's "who set this target?" test below.
    let head = commit.min(durable);
    let mut progressed = false;
    loop {
        // M14a: the lag barrier — re-planned every iteration so a floor that
        // moved mid-cycle is honoured (`floor` only increases; a stale sample
        // is conservative).
        let floor = crate::lag::floor(&st.cnc, st.declared);
        let (target, one_frame) =
            match crate::lag::plan(st.lag_mode, floor, st.follower.cursor, commit, durable) {
                crate::lag::Plan::Wait => {
                    // Lockstep waits out of line (see `lockstep_wait`); a
                    // bounded wait is `fsm_lag` bytes ahead of the slowest FSM
                    // and goes straight to the agent's sleep.
                    let opened = if matches!(st.lag_mode, crate::lag::LagMode::Lockstep) {
                        lockstep_wait(st, commit, durable)
                    } else {
                        None
                    };
                    match opened {
                        Some(plan) => plan,
                        None => {
                            note_lag_wait(&st.cnc, st.service_id, &mut st.lag_waiting);
                            break;
                        }
                    }
                }
                crate::lag::Plan::Apply { target, one_frame } => (target, one_frame),
            };
        // is_leader read inline (a direct field access, not a `&self` method)
        // so it does not conflict with the `follower` borrow the batch holds.
        let is_leader = st.cnc.status().flags.load_acquire() & NODE_FLAG_LEADER != 0;
        let cursor_before = st.follower.cursor;
        // Resolve the batch to a plain enum before touching other fields, so the
        // mutable borrow of `st.follower` the batch holds ends before the
        // replay/publish arms mutate `st.follower.cursor`.
        let overrun = match st.follower.next_batch(target) {
            Batch::CaughtUp => break,
            // The live buffer scrolled past the cursor (or these bytes live only
            // in the journal after a restart prime) — degrade to replay below.
            Batch::Overrun => true,
            Batch::Frames(frames) => {
                #[cfg(feature = "apply-profile")]
                let batch_t0 = profile::now();
                #[cfg(feature = "apply-profile")]
                let (mut pf_frames, mut pf_sm, mut pf_pub, mut pf_bytes) = (0u64, 0u64, 0u64, 0u64);
                let mut sm = st.sm.lock().unwrap();
                for (pos, hdr, payload) in frames {
                    // PADDING, NEW_TERM, CONFIG (and any future type that is
                    // neither MESSAGE nor a TIMER for THIS row), and anything
                    // already applied (idempotent re-entry: a restart replays
                    // from `last_applied`), are simply not applied/published —
                    // the `one_frame` break below still fires after THIS
                    // yielded frame regardless of its type, so lockstep's
                    // "one frame per next_batch" counts every yielded frame,
                    // not only ones that were actually applied.
                    //
                    // The cheap frame-type test comes FIRST in each arm
                    // (final-review M1): `last_applied()` is a trait call and
                    // must not run for a frame the arm is going to skip anyway.
                    if hdr.frame_type == FRAME_TYPE_MESSAGE && Some(pos) > sm.last_applied() {
                        #[cfg(feature = "apply-profile")]
                        let t0 = profile::now();
                        // Bytes straight from the frame to the state machine. Typed
                        // SMs decode (and encode the response) inside their blanket
                        // `RawStateMachine` impl; raw SMs see the slice. Committed
                        // bytes are trusted; a decode failure there is
                        // unrecoverable corruption and fail-stops.
                        st.resp_buf.clear();
                        let mut ctx = ApplyCtx::new(pos, S::IDENTITY)
                            .with_time(hdr.time_ns)
                            .with_term(hdr.leadership_term_id);
                        sm.apply(&mut ctx, payload, &mut st.resp_buf);
                        #[cfg(feature = "apply-profile")]
                        let t1 = profile::now();
                        if is_leader {
                            st.egress.publish(hdr.client_id, hdr.seq, pos, &st.resp_buf);
                        }
                        #[cfg(feature = "apply-profile")]
                        {
                            let t2 = profile::now();
                            pf_frames += 1;
                            pf_sm += t1 - t0; // apply incl. codec
                            pf_pub += t2 - t1;
                            pf_bytes += payload.len() as u64;
                        }
                        let recs = ctx.take_sched_records();
                        if !recs.is_empty() {
                            write_sched(&mut st.svc_sched, &recs);
                        }
                    } else if hdr.frame_type == FRAME_TYPE_TIMER
                        && Some(pos) > sm.last_applied()
                        && let Some(body) = read_timer_body(payload)
                        && body.identity_hash == S::IDENTITY.hash()
                    {
                        let mut ctx = ApplyCtx::new(pos, S::IDENTITY)
                            .with_time(hdr.time_ns)
                            .with_term(hdr.leadership_term_id);
                        sm.on_timer(
                            &mut ctx,
                            TimerEvent {
                                id: body.timer_id,
                                deadline_ns: body.deadline_ns,
                                table: hdr.flags & FLAG_TIMER_TABLE != 0,
                            },
                        );
                        let recs = ctx.take_sched_records();
                        if !recs.is_empty() {
                            write_sched(&mut st.svc_sched, &recs);
                        }
                    }
                    if one_frame {
                        break; // lockstep: exactly one frame past the floor
                    }
                }
                drop(sm);
                #[cfg(feature = "apply-profile")]
                {
                    profile::BATCH.fetch_add(
                        profile::now() - batch_t0,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    profile::add(pf_frames, pf_sm, pf_pub, pf_bytes);
                }
                // Publish the new applied frontier for barrier readers / clients.
                crate::attach::slot(&st.cnc, st.service_id)
                    .applied
                    .store_release(st.follower.cursor);
                if st.follower.cursor == cursor_before {
                    // No frame cleared the target guard (target between the
                    // cursor and the next frame's end). Nothing more to do this
                    // cycle; avoids spinning on a partially-committed frame.
                    //
                    // M14c2 ruling K: `target < head` means the BARRIER set
                    // this target, not the log — a bounded cap that sits
                    // mid-frame. `plan` never said `Wait` (the cap is above the
                    // cursor), yet this FSM is parked at the barrier exactly
                    // the same, so count the episode on the same edge. A
                    // target the LOG set (`== head`) is plain idleness and is
                    // never a wait.
                    if target < head {
                        note_lag_wait(&st.cnc, st.service_id, &mut st.lag_waiting);
                    }
                    break;
                }
                progressed = true;
                // A frame moved: the wait episode (if any) is over.
                st.lag_waiting = false;
                false
            }
        };
        if overrun {
            // Task 9: reconstruct from the journal, then rejoin the live buffer
            // at the byte position replay reached. Livelock-free: each replay
            // pass strictly ADVANCES the cursor toward the archived frontier
            // captured at that pass's start (a monotonic byte position), so the
            // inner loop cannot spin in place — it either catches up (next
            // `next_batch` is `CaughtUp`/`Frames`) or, if the ring lapped the new
            // cursor while replay ran, degrades once more from a strictly higher
            // cursor. Forward progress every time.
            st.needs_replay = true;
            let cursor = match replay_into(
                &st.sm,
                &st.cnc,
                &st.journal_dir,
                st.snapshot_restore.as_ref(),
            ) {
                Ok(Replay::Rejoin(cursor)) => cursor,
                // The covering artifact is above `min(commit, durable)`: the
                // counters are still climbing toward it. Leave the cursor
                // where it is and let the agent idle; the next cycle overruns
                // again and re-checks. Reported once per wait episode.
                Ok(Replay::AwaitArtifact { artifact, target }) => {
                    if st.replay_wait != Some(artifact) {
                        eprintln!(
                            "uc_service: service {} replay waits for snapshot artifact at {artifact} \
                             (apply target {target}); the tail is still arriving",
                            st.service_id
                        );
                        st.replay_wait = Some(artifact);
                    }
                    break;
                }
                // Fail-stop with the contract named (Display carries it). The
                // SnapshotRequired case is the deliberate below-floor-without-
                // -snapshot outcome; any other Err is genuine journal I/O.
                Err(e) => panic!("service journal replay fail-stop: {e}"),
            };
            st.replay_wait = None;
            st.follower.cursor = cursor;
            crate::attach::slot(&st.cnc, st.service_id)
                .applied
                .store_release(cursor);
            st.needs_replay = false;
            progressed = true;
            // Replay jumped the cursor: any wait episode is over.
            st.lag_waiting = false;
            // Time-and-timers §4.8: replay dropped any `on_timer` requests it
            // saw (`replay_into` discards them) — re-announce this
            // incarnation's pending timers on the next cycle so the node's
            // scheduler sees them again.
            st.announce_pending = true;
            // Re-loop: read live from the rejoin point (may CaughtUp, apply
            // more, or Overrun again if the ring lapped us during replay).
        }
    }
    // Liveness: a wall-clock heartbeat the node compares against its own clock.
    crate::attach::slot(&st.cnc, st.service_id)
        .heartbeat_ns
        .store_release(unix_ns());
    drain_queries(st);
    // M6 Task 3: the freeze hook, last in the cycle (module doc / brief) —
    // strictly after the batch loop above has published this cycle's
    // `service_applied`, so a freeze taken here always sees the freshest
    // position this cycle produced.
    maybe_build_snapshot(st);
    progressed
}

/// Check the snapshot policy's interval and, if tripped, `freeze()` the SM
/// (taking its lock briefly — NOT the whole cycle's lock span) and hand the
/// resulting streaming job to the builder thread over the 1-slot channel.
///
/// No-op whenever: there is no trigger (plain `start()`, or an SM that never
/// opted into `start_with_snapshots`); the policy is `interval_bytes: 0`
/// ("never" — the default); a build is already in flight (`busy`); or the
/// threshold hasn't tripped yet.
///
/// **Both freeze failure and a full/disconnected handoff still advance
/// `last_snapshot_pos`** to this cycle's `service_applied` — deliberately: the
/// alternative (leaving the basis unchanged) would re-attempt `freeze()` every
/// single apply cycle for as long as the failure persists, turning "next
/// interval retries" into a hot loop of freeze calls + log lines. Advancing the
/// basis means a genuinely broken `freeze()` is retried once per
/// `interval_bytes` worth of progress, same cadence as the happy path — a
/// failed build looks, from the trigger's point of view, just like a
/// successful one that produced nothing durable.
fn maybe_build_snapshot<S: RawStateMachine>(st: &mut ApplyState<S>) {
    let Some(trigger) = st.snapshot_trigger.as_mut() else {
        return;
    };
    if trigger.policy.interval_bytes == 0 {
        return; // "never" (default policy)
    }
    if trigger.busy.load(Ordering::Acquire) {
        return; // one in-flight build max
    }
    let applied = crate::attach::slot(&st.cnc, st.service_id)
        .applied
        .load_acquire();
    if applied.saturating_sub(trigger.last_snapshot_pos) < trigger.policy.interval_bytes {
        return;
    }

    let frozen = {
        // The SM lock, taken briefly for `freeze()` ONLY — never held across
        // the (off-thread) stream/publish work, per the brief's lock-rule
        // split.
        let sm = st.sm.lock().unwrap();
        (trigger.freeze)(&sm)
    };
    match frozen {
        Ok((job, pos)) => {
            trigger.last_snapshot_pos = pos;
            trigger.busy.store(true, Ordering::Release);
            if trigger.tx.try_send((pos, job)).is_err() {
                // Defensive only: under normal operation `busy` already
                // prevents this (the builder can't be mid-cycle AND have an
                // empty channel slot occupied by us at the same time). Revert
                // `busy` so a torn/disconnected builder can't wedge future
                // attempts forever.
                trigger.busy.store(false, Ordering::Release);
            }
        }
        Err(e) => {
            eprintln!(
                "uc_service: snapshot freeze failed at applied={applied}: {e} \
                 (dropped; the next policy interval retries)"
            );
            trigger.last_snapshot_pos = applied;
        }
    }
}

/// Query bounded drain per apply cycle — the read-side analog of the apply
/// batch cap, so a burst of queries can never starve the apply loop.
const QUERY_DRAIN_PER_CYCLE: usize = 64;

/// Lockstep wait ladder (`lockstep_wait`): spins re-planning, then yields.
/// Measured in `uc_node/examples/apply_bench`
/// (docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md).
const LAG_WAIT_SPINS: u32 = 256;
// M14c2 T8: do NOT retune this expecting a win under CPU oversubscription —
// ×4 and ×16 both measured 1.00× (docs/benchmarks/uc2-m14c2-lockstep-oversubscription-2026-08-30.md).
const LAG_WAIT_YIELDS: u32 = 2048;
/// While yielding, refresh the heartbeat this often so a long ladder is
/// never mistaken for a dead FSM.
const LAG_WAIT_HEARTBEAT_EVERY: u32 = 256;

/// Drain `svc_query.ring` (bounded): read each request's `expected_epoch` prefix,
/// REFUSE any stamped for a superseded incarnation with `MSG_V2_RETRY`, and
/// answer the rest by querying the SM and publishing `MSG_V2_RESPONSE`
/// (`FLAG_V2_IS_QUERY`) onto the egress broadcast. Runs on the apply thread
/// right after the batch loop, taking the SM lock the same way `apply` does
/// (single-threaded — the read/write distinction a `RwLock` would give buys
/// nothing here).
///
/// Payload contract (`ipc.rs`): `expected_epoch: u64 LE ++ query bytes`.
/// `expected_epoch == 0` means "skip the check" — a snapshot read the node
/// forwarded unconditionally. Both this RETRY site and the barrier's are
/// PRE-query / side-effect-free: a query never mutates the SM (the
/// cross-task RETRY-is-side-effect-free invariant, Task 10 review).
/// Poisoned-incarnation read path: drain the query ring and answer every
/// request with RETRY. Side-effect-free (the SM is never touched), so the
/// cross-task "RETRY is side-effect-free" invariant holds here too. The client
/// rotates to another node or retries after the supervisor respawns us.
fn refuse_queries<S: RawStateMachine>(st: &mut ApplyState<S>) {
    let mut buf = Vec::new();
    for _ in 0..QUERY_DRAIN_PER_CYCLE {
        match st.svc_query.try_read(&mut buf) {
            Ok(Some(rec)) => st.egress.publish_retry(rec.header_extra),
            _ => break,
        }
    }
}

fn drain_queries<S: RawStateMachine>(st: &mut ApplyState<S>) {
    // This incarnation's epoch, CAPTURED AT ATTACH (M5 final review #5) — fixed
    // for this incarnation's life, NOT re-read live from the page. A newer
    // incarnation attaching to the same slot bumps its `epoch`; if we
    // re-read it live, an old (still-running) incarnation would start answering
    // reads stamped for the NEW epoch with ITS OWN (stale) state — a
    // linearizability hole. Comparing against the fixed attach-time epoch makes
    // any read not stamped for THIS incarnation fall through to RETRY. (#2c
    // fail-stops the node-restart case; this is the same-node service-restart
    // case, where `instance_id` is unchanged so #2c does not fire.)
    let my_epoch = st.my_epoch;
    let mut buf = Vec::new();
    for _ in 0..QUERY_DRAIN_PER_CYCLE {
        match st.svc_query.try_read(&mut buf) {
            Ok(Some(rec)) => {
                // Payload = expected_epoch u64 LE ++ query bytes. A record too
                // short to hold the prefix is a protocol violation — drop it (a
                // query has no recovery contract; the client times out/retries).
                if buf.len() < 8 {
                    continue;
                }
                let expected_epoch = u64::from_le_bytes(buf[..8].try_into().unwrap());
                if expected_epoch != 0 && expected_epoch != my_epoch {
                    // Stale incarnation (task14 TOCTOU close): the read was routed
                    // for a superseded service epoch. Refuse with RETRY rather
                    // than answer with THIS incarnation's (different) state. The
                    // SM is NOT touched — side-effect-free.
                    st.egress.publish_retry(rec.header_extra);
                    continue;
                }
                // Bytes through: a typed SM decodes the query and encodes the
                // answer inside its blanket `RawStateMachine` impl.
                st.resp_buf.clear();
                st.sm.lock().unwrap().query(&buf[8..], &mut st.resp_buf);
                st.egress
                    .publish_query_answer(rec.header_extra, &st.resp_buf);
            }
            Ok(None) => break,
            // Corrupt record (bad crc/magic): stop this cycle; the next retries
            // at the same unread position.
            Err(_) => break,
        }
    }
}

fn unix_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Fail-stop this service thread if the node it attached to has restarted (M5
/// final review #2c; shared by the apply and output loops).
///
/// A node restart recreates the cnc page IN PLACE with a fresh random
/// `instance_id` (the v2.0 contract: a restart invalidates every attachment;
/// there is no live re-attach — an external supervisor respawns the service).
/// The recreate is truncate → `set_len` → rewrite-in-place, so a probe can catch
/// a torn/zeroed header ([`CncPage::try_instance_id`] → `None`); we therefore
/// require TWO CONSECUTIVE confirmed mismatches before fail-stopping so a single
/// torn read cannot false-trip a live cluster. On confirmation we `panic!` — the
/// documented fail-stop: it unwinds this agent thread (stopping the zombie
/// writer), and the crashtest harness / a real process supervisor, which already
/// respawns the service on node restart, takes the process the rest of the way
/// down. A matching id (or a torn `None` read) resets the streak.
/// The lockstep barrier wait. Under lockstep the sibling is at most one frame
/// away, so the barrier opens within a frame's apply time plus a cache-line
/// round trip: spin re-planning for that, then yield. The yield budget is
/// generous on purpose — a lockstep FSM must never SLEEP on a live sibling:
/// one FSM in the agent's 50 µs sleep stalls every other FSM's next frame,
/// their ladders exhaust too, and the whole set falls into sleeping in
/// lockstep (measured ~18 k frames/s; the yield-only experiment ran 33×
/// faster). The heartbeat is refreshed while yielding. `None` after the
/// budget means a genuinely stalled or dead sibling: the caller counts the
/// episode and hands the cycle back to the agent.
///
/// Out of line: inlining the ladder into `apply_cycle`'s loop cost 9 % at
/// N=1 on a path N=1 never executes (codegen of the hot body).
///
/// M14c2 T8 — what this ladder does NOT fix: under CPU oversubscription (the
/// runnable set exceeding the CPUs) the ladder **never exhausts**
/// (`lag_waits = 0` on every collapsed run), so the sleep path above is never
/// reached and the M14a cascade is not what is happening; the yields
/// themselves are the collapse, at ~1.41 ms per frame. Lengthening the budget
/// ×4 and ×16 both measured **1.00×**, as did an unbounded yield-until-the-
/// sibling-looks-dead ladder. That collapse is a recorded operating-envelope
/// fact (lockstep needs a free CPU per declared FSM), not a defect:
/// `docs/benchmarks/uc2-m14c2-lockstep-oversubscription-2026-08-30.md`.
#[inline(never)]
fn lockstep_wait<S: RawStateMachine>(
    st: &mut ApplyState<S>,
    commit: u64,
    durable: u64,
) -> Option<(u64, bool)> {
    for i in 0..(LAG_WAIT_SPINS + LAG_WAIT_YIELDS) {
        if i < LAG_WAIT_SPINS {
            std::hint::spin_loop();
        } else {
            std::thread::yield_now();
            if (i - LAG_WAIT_SPINS).is_multiple_of(LAG_WAIT_HEARTBEAT_EVERY) {
                crate::attach::slot(&st.cnc, st.service_id)
                    .heartbeat_ns
                    .store_release(unix_ns());
            }
        }
        let floor = crate::lag::floor(&st.cnc, st.declared);
        if let crate::lag::Plan::Apply { target, one_frame } =
            crate::lag::plan(st.lag_mode, floor, st.follower.cursor, commit, durable)
        {
            return Some((target, one_frame));
        }
    }
    None
}

/// The barrier's wait-episode edge (`lag_waits`, M14a Task 7; M14c2 ruling K).
/// Counts EPISODES, not cycles: the `false -> true` edge of `lag_waiting`,
/// which the apply loop clears again only where the cursor advances.
///
/// Two callers, both "this cycle is parked at the barrier": the `Wait` plan
/// (the cap is at or below the cursor), and a batch that moved nothing under a
/// barrier-capped target (the mid-frame cap `plan` reports as `Apply` —
/// ruling K, the case that used to read 0 waits on a paced FSM).
///
/// Out of line, like [`lockstep_wait`], and for the same measured reason: code
/// in the apply loop's body costs even on paths that never run (M14a, −9 % at
/// N=1 for an inlined ladder).
///
/// **Not A/B'd.** This edit was NOT measured on `uc_node/examples/apply_bench`
/// at N=1. It is out of line and its only new hot-body cost is one predictable
/// compare on an already-cold exit branch, but that is an argument, not a
/// measurement — the only evidence behind it is that the workspace suite is
/// green. If the apply hop is ever re-benched, this is a place to look.
#[inline(never)]
fn note_lag_wait(cnc: &CncPage, service_id: u8, lag_waiting: &mut bool) {
    if !*lag_waiting {
        *lag_waiting = true;
        crate::attach::slot(cnc, service_id).lag_waits.fetch_add(1);
    }
}

pub(crate) fn check_node_instance(cnc: &CncPage, attached: u128, streak: &mut u8) {
    match cnc.try_instance_id() {
        Some(id) if id != attached => {
            *streak += 1;
            if *streak >= 2 {
                panic!(
                    "uc_service: node instance_id changed ({attached:#x} -> {id:#x}) — this \
                     attachment is invalidated by a node restart (v2.0 contract, plan decision \
                     #9). Fail-stop; the supervisor respawns the service."
                );
            }
        }
        // Matching id, or a torn/None read (node mid-recreate — re-probed next
        // cycle): not a confirmed change, so reset the consecutive-mismatch run.
        _ => *streak = 0,
    }
}

#[cfg(test)]
mod tests {
    use super::check_node_instance;
    use crate::traits::ApplyCtx;
    use std::sync::Arc;
    use uc_log::cnc::{CncMeta, CncPage};

    fn page(instance_id: u128) -> Arc<CncPage> {
        CncPage::heap(&CncMeta {
            node_id: 1,
            instance_id,
            app_id: "svc-test".into(),
            buffer_bytes: 1 << 20,
            max_payload: 256,
            services: [None; uc_protocol::v2::cnc::CNC_MAX_SERVICES],
        })
    }

    // A matching instance_id is the steady state: it resets any transient
    // mismatch streak and never fail-stops.
    #[test]
    fn matching_instance_resets_streak_and_does_not_fail_stop() {
        let p = page(0xAAAA);
        let mut streak = 1; // a prior torn/transient bump
        check_node_instance(&p, 0xAAAA, &mut streak);
        assert_eq!(streak, 0, "a matching instance resets the streak");
    }

    // The derace: ONE mismatching cycle only arms the streak — a single torn
    // read during a node recreate must not fail-stop a live cluster.
    #[test]
    fn a_single_mismatch_arms_but_does_not_fire() {
        let p = page(0xBBBB); // page instance != attached
        let mut streak = 0;
        check_node_instance(&p, 0xAAAA, &mut streak);
        assert_eq!(streak, 1, "one mismatch arms but does not fire");
    }

    // TWO consecutive confirmed mismatches fail-stop (the node genuinely
    // restarted with a fresh instance_id) — the documented panic.
    #[test]
    #[should_panic(expected = "node instance_id changed")]
    fn two_consecutive_mismatches_fail_stop() {
        let p = page(0xBBBB);
        let mut streak = 0;
        check_node_instance(&p, 0xAAAA, &mut streak); // streak -> 1
        check_node_instance(&p, 0xAAAA, &mut streak); // streak -> 2 -> panic
    }

    // ------------------------------------------------- M14c2 ruling K: lag_waits

    /// 32 B header + 64 B payload, 32-B aligned: every appended frame is 96 B,
    /// so a 128 B bound cannot divide the frame stream — the case ruling K is
    /// about.
    const FRAME: u64 = 96;
    const CAP: u64 = 1 << 16;
    const BOUND: u64 = 128;

    #[derive(Default)]
    struct CountSm {
        applies: u64,
        last: Option<u64>,
    }

    impl crate::traits::RawStateMachine for CountSm {
        const NAME: &'static str = "count";
        fn apply(&mut self, ctx: &mut ApplyCtx, _cmd: &[u8], _out: &mut Vec<u8>) {
            self.applies += 1;
            self.last = Some(ctx.position);
        }
        fn query(&self, _q: &[u8], _out: &mut Vec<u8>) {}
        fn last_applied(&self) -> Option<u64> {
            self.last
        }
    }

    /// The two ring files `ApplyState` needs, on REAL DISK under the cargo
    /// target tree (never `/tmp` — CLAUDE.md's scratch rule); removed with the
    /// returned `TempDir`.
    fn scratch() -> tempfile::TempDir {
        let base = std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        tempfile::Builder::new()
            .prefix("uc2-apply-lagk")
            .tempdir_in(base)
            .unwrap()
    }

    // ------------------- nightly 33488022809: replay must WAIT for a covering artifact

    /// Nightly 33488022809 (2026-09-01, the two-FSM learner capstone). A
    /// learner adopts a two-row snapshot set at the `min` of the rows'
    /// positions, so the row whose artifact sits ABOVE that floor has an
    /// artifact the gap guard cannot use yet: `newest(target)` with `target =
    /// min(commit, durable)` below the artifact finds nothing, and the guard
    /// treated "not durable enough yet" as "unbridgeable" — a
    /// `SnapshotRequired` fail-stop of the apply agent (`uc2-apply` panicked
    /// at apply.rs:470 on 502878c). The counters were still climbing; a later
    /// cycle would have installed it. The guard must fail-stop only when NO
    /// artifact at or above the purge floor exists at all; an artifact above
    /// the target means wait for the target to reach it.
    #[test]
    fn a_gap_with_an_artifact_above_the_target_waits_then_installs() {
        let dir = scratch();
        let cnc = page(0x5151);
        cnc.store_services_declared(0b1);
        let buffer = std::sync::Arc::new(uc_log::buffer::LogBuffer::new(
            uc_log::region::Region::heap_zeroed(CAP as usize),
            std::sync::Arc::clone(&cnc),
            256,
        ));
        // Lap the ring: 1400 frames of 96 B through a 64 KiB ring, recorded
        // into a real journal by the real archive as we go (the appender
        // never overwrites unrecorded bytes). Cursor 0 is then far below
        // what the ring retains. Frame positions come from the appender (it
        // pads at each wrap).
        let journal_dir = dir.path().join("journal");
        let mut archive = uc_log::archive::Archive::open(uc_log::archive::ArchiveConfig {
            segment_size_bytes: 16 * 1024,
            preallocate_segments: false,
            ..uc_log::archive::ArchiveConfig::new(&journal_dir)
        })
        .unwrap();
        let mut appender = uc_log::buffer::Appender::new(std::sync::Arc::clone(&buffer), 1, 0);
        const N: usize = 1400;
        let mut pos = Vec::with_capacity(N);
        for i in 0..N {
            pos.push(appender.append(1, i as u32, &[1u8; 64]).unwrap());
            if i % 100 == 99 {
                while archive.do_work(&buffer).unwrap() {}
            }
        }
        while archive.do_work(&buffer).unwrap() {}
        let head = cnc.counters().append.load_acquire();
        assert_eq!(cnc.counters().durable.load_acquire(), head, "all recorded");
        // The journal is purged below frame 300 (segment-granular, so the
        // floor F lands on a block base at or below it — above 0 is what
        // matters); the one artifact sits at P = frame 1300, inside the
        // ring's retained window.
        let p_pos = pos[1300];
        let f_base = archive.purge_below(pos[300]).unwrap();
        assert!(
            f_base > 0 && f_base <= pos[300],
            "purged below frame 300: F = {f_base}"
        );
        drop(archive);
        let store = crate::snapshots::SnapshotStore::open(dir.path(), 0).unwrap();
        store
            .publish(p_pos, |w| w.write_all(b"snap").map_err(Into::into))
            .unwrap();
        let restore = super::SnapshotRestore::<CountSm> {
            store,
            install: Box::new(|sm, pos, _r| {
                sm.last = Some(pos);
                Ok(pos)
            }),
        };
        // Commit lags the artifact: target = min(commit, durable) < P.
        cnc.counters().commit.store_release(pos[1200]);

        let egress_ring =
            uc_protocol::ring::BroadcastRing::create(&dir.path().join("egress.bc"), 1 << 16, 1024)
                .unwrap();
        let (_qp, svc_query) =
            uc_protocol::ring::SpscRing::create(&dir.path().join("svc_query.ring"), 1 << 16, 1024)
                .unwrap()
                .into_split();
        let (svc_sched, _sp) =
            uc_protocol::ring::SpscRing::create(&dir.path().join("svc_sched.ring"), 1 << 16, 1024)
                .unwrap()
                .into_split();
        let mut st = super::ApplyState {
            poisoned: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            follower: uc_log::reader::LogFollower::new(std::sync::Arc::clone(&buffer), 0),
            sm: Arc::new(std::sync::Mutex::new(CountSm::default())),
            cnc: Arc::clone(&cnc),
            egress: crate::egress::Egress::new(egress_ring.producer()),
            resp_buf: Vec::new(),
            journal_dir,
            svc_query,
            svc_sched,
            announce_pending: false,
            needs_replay: false,
            replay_wait: None,
            instance_id: 0x5151,
            instance_mismatch_streak: 0,
            my_epoch: 1,
            service_id: 0,
            lag_mode: crate::lag::LagMode::Off,
            declared: 0b1,
            lag_waiting: false,
            snapshot_trigger: None,
            snapshot_restore: Some(restore),
        };

        // Cycle 1: overrun -> replay -> the artifact is above the target.
        // Not a fail-stop: wait, cursor untouched, nothing applied.
        assert!(
            !super::apply_cycle(&mut st),
            "no progress while the artifact is above the target"
        );
        assert_eq!(st.follower.cursor, 0, "cursor untouched while waiting");
        assert_eq!(st.sm.lock().unwrap().last, None);
        // Still waiting on the next cycle (no panic, no progress).
        assert!(!super::apply_cycle(&mut st));

        // The target reaches the artifact: install at P, rejoin the live ring
        // at P, apply the tail P..head.
        cnc.counters().commit.store_release(head);
        assert!(super::apply_cycle(&mut st), "install + tail");
        let sm = st.sm.lock().unwrap();
        assert_eq!(
            sm.applies,
            (N - 1301) as u64,
            "exactly the frames above P (P itself is in the snapshot)"
        );
        assert_eq!(sm.last, Some(pos[N - 1]));
        assert_eq!(st.follower.cursor, head);
    }

    /// M14c2 ruling K (`docs/benchmarks/uc2-m14c-*`): `uc_service_lag_waits_total`
    /// read 0 while a BOUNDED FSM sat parked at the barrier, because the cap
    /// landed MID-FRAME. `lag::plan` only says `Wait` when the cap is at or
    /// below the cursor; a cap 32 B into a 96 B frame is above it, so the plan
    /// is `Apply` with a target no frame can clear and the batch moves nothing
    /// — a barrier stall the counter never saw. It must count, once per
    /// EPISODE (the same `false -> true` edge the `Wait` arm uses), not once
    /// per cycle.
    #[test]
    fn a_bounded_cap_mid_frame_counts_one_lag_wait_per_episode() {
        let dir = scratch();
        let cnc = page(0x1234);
        // Two declared FSMs and a 128 B bound over a 96 B frame stream.
        cnc.store_services_declared(0b11);
        cnc.store_fsm_lag_bytes(BOUND);
        let buffer = std::sync::Arc::new(uc_log::buffer::LogBuffer::new(
            uc_log::region::Region::heap_zeroed(CAP as usize),
            std::sync::Arc::clone(&cnc),
            256,
        ));
        let mut appender = uc_log::buffer::Appender::new(std::sync::Arc::clone(&buffer), 1, 0);
        for i in 0..5u32 {
            appender.append(1, i, &[i as u8; 64]).unwrap();
        }
        let head = 5 * FRAME;
        cnc.counters().durable.store_release(head);
        cnc.counters().commit.store_release(head);
        // We are FSM 0, one frame ahead of the floor: FSM 1 is still at 0, so
        // floor = 0, cap = 128 — 32 bytes into the frame at 96.
        cnc.service_slot(0).applied.store_release(FRAME);
        cnc.service_slot(1).applied.store_release(0);

        let egress_ring =
            uc_protocol::ring::BroadcastRing::create(&dir.path().join("egress.bc"), 1 << 16, 1024)
                .unwrap();
        let (_qp, svc_query) =
            uc_protocol::ring::SpscRing::create(&dir.path().join("svc_query.ring"), 1 << 16, 1024)
                .unwrap()
                .into_split();
        let (svc_sched, _sp) =
            uc_protocol::ring::SpscRing::create(&dir.path().join("svc_sched.ring"), 1 << 16, 1024)
                .unwrap()
                .into_split();
        let mut st = super::ApplyState {
            poisoned: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            follower: uc_log::reader::LogFollower::new(std::sync::Arc::clone(&buffer), FRAME),
            sm: Arc::new(std::sync::Mutex::new(CountSm::default())),
            cnc: Arc::clone(&cnc),
            egress: crate::egress::Egress::new(egress_ring.producer()),
            resp_buf: Vec::new(),
            journal_dir: dir.path().join("journal"),
            svc_query,
            svc_sched,
            announce_pending: false,
            needs_replay: false,
            replay_wait: None,
            instance_id: 0x1234,
            instance_mismatch_streak: 0,
            my_epoch: 1,
            service_id: 0,
            lag_mode: crate::lag::LagMode::Bounded(BOUND),
            declared: 0b11,
            lag_waiting: false,
            snapshot_trigger: None,
            snapshot_restore: None,
        };
        let waits = |c: &CncPage| c.service_slot(0).lag_waits.load_acquire();

        assert_eq!(waits(&cnc), 0, "nothing counted before the first cycle");
        // Cycle 1: parked at a cap that sits mid-frame — one episode.
        assert!(
            !super::apply_cycle(&mut st),
            "no progress: the cap blocks the frame"
        );
        assert_eq!(st.follower.cursor, FRAME, "no frame moved");
        assert_eq!(
            waits(&cnc),
            1,
            "ruling K: the mid-frame cap counts a wait episode"
        );
        // Cycle 2: still the SAME episode — episodes, not cycles.
        assert!(!super::apply_cycle(&mut st));
        assert_eq!(waits(&cnc), 1, "one episode, not one per cycle");

        // The sibling catches up: floor = 96, cap = 224, so the frame at 96
        // (ending 192) clears — the episode resolves and a new one opens at
        // the new cap, 32 bytes into the frame at 192.
        cnc.service_slot(1).applied.store_release(FRAME);
        assert!(
            super::apply_cycle(&mut st),
            "the floor moved: a frame applies"
        );
        assert_eq!(
            st.follower.cursor,
            2 * FRAME,
            "exactly one frame cleared the new cap"
        );
        assert_eq!(st.sm.lock().unwrap().applies, 1);
        assert_eq!(
            waits(&cnc),
            2,
            "the resolve opened a SECOND episode at the new cap"
        );
        // ... and that second episode, too, counts once however long it lasts.
        assert!(!super::apply_cycle(&mut st));
        assert_eq!(waits(&cnc), 2, "still one episode");
    }
}
