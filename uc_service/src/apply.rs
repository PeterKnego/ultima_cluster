// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The apply agent duty cycle (spec §7). A single polling thread that follows
//! the committed log, applies each `MESSAGE` frame to the user's state machine,
//! and (while leader) publishes the response onto the egress broadcast. On an
//! `Overrun` (the live buffer scrolled past the cursor) it degrades to journal
//! replay (Task 9) and rejoins the live buffer at the byte position replay
//! reached.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use uc_log::cnc::CncPage;
use uc_log::reader::{Batch, LogFollower};
use uc_protocol::ring::{RingError, SpscConsumer, SpscProducer};
use uc_protocol::v2::cnc::{NODE_FLAG_LEADER, NODE_FLAG_LEARNER};
use uc_protocol::v2::frame::{
    FLAG_SNAPSHOT_STANDBY, FLAG_TIMER_TABLE, FRAME_TYPE_MESSAGE, FRAME_TYPE_SNAPSHOT,
    FRAME_TYPE_TIMER, FrameHeader, align_frame_len, read_timer_body,
};
use uc_protocol::v2::ipc::{MSG_V2_SCHED, SchedOp, SchedRecord, write_sched_record};

use crate::builder_agent::BuildJob;
use crate::config::SnapshotError;
use crate::egress::Egress;
use crate::replay::{Replay, ReplayInstant, replay_into};
use crate::traits::{ApplyCtx, RawStateMachine, TimerEvent};

/// Time-and-timers §4.8: how many spins `write_sched` has taken waiting on a
/// full `svc_sched` ring, process-wide. Not yet exported through a metrics
/// surface (uc_service has no `uc_obs` dependency) — a later task may wire
/// it up; for now it exists so a spin storm leaves a countable trace.
pub(crate) static SCHED_RING_FULL_SPINS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Coordinated-snapshot spec §10: instants this row declined because a build
/// was still in flight. Each one leaves this node's set at that instant
/// incomplete — the honest outcome, not an error. Process-global and `pub`
/// (re-exported at the crate root) so the observability task can read it.
pub static SNAPSHOT_SKIPPED_BUSY: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Coordinated-snapshot spec §10: instants whose `freeze()` returned an error.
/// Same consequence as [`SNAPSHOT_SKIPPED_BUSY`] — an incomplete set — but a
/// service-side defect rather than a timing one, so it is counted apart.
pub static SNAPSHOT_FREEZE_FAILED: std::sync::atomic::AtomicU64 =
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
/// plain `start()` (or an SM that never opted in) means the row is not
/// snapshot-capable and [`on_snapshot_frame`] ignores every instant.
///
/// Coordinated-snapshot spec §5.2: the trigger is now the LOG — a
/// `FRAME_TYPE_SNAPSHOT` frame — so this type carries no cadence of its own.
/// The M6 byte-interval policy and the `last_snapshot_pos` basis it needed are
/// deleted; the leader decides when every row freezes, and it decides once for
/// the whole cluster.
pub(crate) struct SnapshotTrigger<S: RawStateMachine> {
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
    /// Cluster-FSM spec §4.9: the leader flag as of the PREVIOUS cycle — the
    /// edge (`false -> true`) is what sets `announce_pending`, race-free by
    /// ordering: `publish_status` sets the node's flag before the node could
    /// fire anything, so every record applied before this incarnation sees
    /// the edge is in the pending set the edge flushes, and every record
    /// applied after is written directly under a now-true gate.
    pub(crate) was_leader: bool,
    /// Cluster-FSM spec §4.9: the in-loop mirror of every SM's pending
    /// timers, maintained from `take_sched_records()` and from delivered
    /// `TIMER` frames — parity with `Timed<S>::pending_timers()` for a BARE
    /// state machine, which has no pending set of its own to re-announce.
    /// The edge-announce flushes the SM's own hook when it overrides one
    /// (`Timed`) and this map otherwise.
    pub(crate) pending: HashMap<u64, u64>,
    /// The same for the schedule table's delivered ticks (parity with
    /// `Timed<S>::table_delivered()`).
    pub(crate) table_last: HashMap<u64, u64>,
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

/// Cluster-FSM spec §4.9: maintain the loop's own mirror of every SM's
/// pending timers and delivered table ticks from a batch of
/// `take_sched_records()` — parity with `Timed<S>::pending_timers()`/
/// `table_delivered()` for a BARE state machine, which has neither.
fn track_sched(
    pending: &mut HashMap<u64, u64>,
    table_last: &mut HashMap<u64, u64>,
    recs: &[SchedRecord],
) {
    for r in recs {
        match r.op {
            SchedOp::Schedule => {
                pending.insert(r.timer_id, r.deadline_ns);
            }
            SchedOp::Cancel | SchedOp::Consumed => {
                pending.remove(&r.timer_id);
            }
            SchedOp::TableConsumed => {
                table_last.insert(r.timer_id, r.deadline_ns);
            }
        }
    }
}

/// Cluster-FSM spec §4.9: the ring must not be written on a follower — a
/// full `svc_sched` ring nobody drains would spin `write_sched` forever and
/// wedge this apply thread for good. Every record is still tracked in the
/// loop's own maps regardless of role (a follower's timers are not lost,
/// just not announced yet), and written to the node only while leader.
///
/// Takes the three fields it touches individually rather than
/// `&mut ApplyState<S>` as a whole: both call sites run while a batch's
/// `FrameIter` still holds a live borrow of `st.follower` (its `cursor` and
/// `buf`), so a helper taking the whole struct would conflict with it —
/// these three are, like `st.egress`/`st.resp_buf` at the same call sites,
/// statically disjoint fields.
fn write_sched_if_leader(
    svc_sched: &mut SpscProducer,
    pending: &mut HashMap<u64, u64>,
    table_last: &mut HashMap<u64, u64>,
    recs: &[SchedRecord],
    is_leader: bool,
) {
    track_sched(pending, table_last, recs);
    if is_leader {
        write_sched(svc_sched, recs);
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
    // Cluster-FSM spec §4.9: read the node-written status flags word ONCE, at
    // the top of the cycle — before the announce flush below, so the rising
    // edge is detected before anything can gate on `is_leader`, and reused for
    // the whole cycle's batch (a direct field access, not a `&self` method, so
    // it does not conflict with the `follower` borrow the batch holds).
    // Coordinated-snapshot spec §5.7 rides the same single read:
    // `NODE_FLAG_LEARNER` decides whether a standby-flagged instant is ours.
    let node_flags = st.cnc.status().flags.load_acquire();
    let is_leader = node_flags & NODE_FLAG_LEADER != 0;
    if is_leader && !st.was_leader {
        st.announce_pending = true; // spec §4.9: announce on the rising edge
    }
    st.was_leader = is_leader;

    if st.announce_pending && is_leader {
        st.announce_pending = false;
        let (mut pending, mut table_delivered) = {
            let sm = st.sm.lock().unwrap();
            (sm.pending_timers(), sm.table_delivered())
        };
        // A bare SM's hooks are provided no-ops (`Timed` is the only
        // override) — fall back to the loop's own maps, which track every
        // SM's pending set regardless of whether it wraps in `Timed`.
        if pending.is_empty() && table_delivered.is_empty() {
            pending = st.pending.iter().map(|(&id, &dl)| (id, dl)).collect();
            table_delivered = st.table_last.iter().map(|(&id, &dl)| (id, dl)).collect();
        }
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
    // `!is_leader` leaves `announce_pending` set (a follower attaching or
    // finishing replay set it true above/at attach) — a follower must never
    // write the ring (`write_sched` spins forever on a full one), so the
    // flush waits for this incarnation's first promotion.
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
        // `is_leader` was read once at the top of this cycle (cluster-FSM
        // spec §4.9: the edge-detection needs it before the announce flush)
        // and is reused here, unchanged, for the whole batch.
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
                            write_sched_if_leader(
                                &mut st.svc_sched,
                                &mut st.pending,
                                &mut st.table_last,
                                &recs,
                                is_leader,
                            );
                        }
                    } else if hdr.frame_type == FRAME_TYPE_TIMER
                        && Some(pos) > sm.last_applied()
                        && let Some(body) = read_timer_body(payload)
                        && body.identity_hash == S::IDENTITY.hash()
                    {
                        // Delivery bookkeeping BEFORE `on_timer`: this instance is
                        // no longer pending regardless of what the SM does with
                        // it, and a table tick's delivery raises `table_last` —
                        // parity with `Timed<S>`'s own bookkeeping, for the bare
                        // SM the loop's maps serve.
                        st.pending.remove(&body.timer_id);
                        if hdr.flags & FLAG_TIMER_TABLE != 0 {
                            st.table_last.insert(body.timer_id, body.deadline_ns);
                        }
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
                            write_sched_if_leader(
                                &mut st.svc_sched,
                                &mut st.pending,
                                &mut st.table_last,
                                &recs,
                                is_leader,
                            );
                        }
                    } else if hdr.frame_type == FRAME_TYPE_SNAPSHOT {
                        // M14a: the arm is the type test and this call, nothing
                        // else — plan 2's T8 resolved the cnc slot INLINE here
                        // and the codegen alone cost 2.7 % at N=1 on the apply
                        // hop (apply_ab.sh, 9b7bcc4 → a64a6ed, 2026-09-07).
                        on_snapshot_frame(
                            &mut st.snapshot_trigger,
                            &sm,
                            pos,
                            &hdr,
                            node_flags,
                            &st.cnc,
                            st.service_id,
                        );
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
                // Ruling P10: the span this replay walks may hold the very
                // instant the leader is waiting on. Same trigger, same flags
                // word as the live arm above — one decision, two paths.
                ReplayInstant {
                    trigger: &mut st.snapshot_trigger,
                    node_flags,
                    service_id: st.service_id,
                },
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
    progressed
}

/// Coordinated-snapshot spec §5.2/§5.7: act on a `FRAME_TYPE_SNAPSHOT` frame.
///
/// **Out of line on purpose** (M14a's hot-loop rule): the arm in the frames
/// loop is one frame-type test and one call to this. A wait ladder written
/// inline into that loop body cost 9 % at N=1 through codegen alone — on a
/// path N=1 never executes (`docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md`).
///
/// The freeze runs INSIDE the batch's SM lock span (this is called from the
/// frames loop, which holds the guard), not in a short span of its own at the
/// end of the cycle as the M6 byte-trigger did — so this cycle's `applied`
/// publication is delayed by one freeze. That is the cost spec §5.7/§10 already
/// state for a non-standby instant: on a quorum it is what stalls commit at
/// `P + fsm_lag`, and standby instants exist to avoid paying it on voters. Only
/// the STREAMING must stay off-lock, and it still does (`builder_agent`).
///
/// The frame is never *applied*: it carries no user bytes, publishes no
/// response and does not move `last_applied`. It is still a YIELDED frame —
/// the caller's `one_frame` break fires after it exactly as it does after a
/// `TIMER` frame, and the cursor advances over it either way, so lockstep and
/// the lag barrier count it like any other frame.
///
/// Three silent declines, in order (spec §10 — an incomplete set is the honest
/// outcome of every one of them, never a fail-stop):
///
/// 1. **No trigger.** A row started with plain `start()` is not
///    snapshot-capable and never sets `CNC_SVC_STATUS_SNAPSHOT_CAPABLE`; the
///    leader refuses to command an instant on a cluster holding one
///    (`48 snapshot_unsupported`, spec §5.5), so reaching here means the frame
///    predates the refusal or the row is a harness. Ignore it.
/// 2. **A standby instant on a node that is not a learner** (spec §5.7). The
///    whole point of the flag is that voters do not pay the freeze, whose cost
///    is O(state) on the apply thread and would cap a quorum's durable reports
///    at `P + fsm_lag` simultaneously.
/// 3. **A build already in flight.** One in flight max, the rule `busy` has
///    enforced since M6; this row is simply incomplete for THIS instant, which
///    [`SNAPSHOT_SKIPPED_BUSY`] counts.
/// 4. **An instant at or below what the SM has already applied** (final wave
///    M4). Unreachable on the live walk today — post-install the cursor is set
///    to the artifact's tag and post-replay to the replay end, and in both
///    cases `last_applied()` is strictly below the cursor — but the reason it
///    is unreachable is a GLOBAL argument about every cursor-setting path,
///    whereas the replay twin (`replay.rs`) makes the same invariant LOCAL
///    with one comparison. Freezing at such a frame would tag state above P
///    with P, a divergence the artifact envelope cannot catch (the tag IS P),
///    and fix round 3 exists because that class of bug is invisible. One
///    compare, on a rare arm.
///
/// Takes the `ApplyState` fields it touches individually rather than
/// `&mut ApplyState<S>`: the call site runs with the SM's `MutexGuard` alive,
/// which borrows `st.sm`, so a helper taking the whole struct would conflict
/// with it — exactly the reason `write_sched_if_leader` above is shaped the
/// same way. `slot` is this row's cnc slot (`crate::attach::slot(&st.cnc,
/// st.service_id)`), needed only to publish the freeze duration — spec §9's
/// `uc2_snapshot_freeze_seconds_max/_sum/_count{row}`.
#[inline(never)]
pub(crate) fn on_snapshot_frame<S: RawStateMachine>(
    trigger: &mut Option<SnapshotTrigger<S>>,
    sm: &S,
    pos: u64,
    hdr: &FrameHeader,
    node_flags: u64,
    cnc: &uc_log::cnc::CncPage,
    service_id: u8,
) {
    let Some(trig) = trigger.as_mut() else {
        return; // 1. not snapshot-capable
    };
    // Resolved HERE, out of line, never in the frame loop's body (M14a).
    let slot = crate::attach::slot(cnc, service_id);
    if hdr.flags & FLAG_SNAPSHOT_STANDBY != 0 && node_flags & NODE_FLAG_LEARNER == 0 {
        return; // 2. a voter on a standby instant
    }
    if trig.busy.load(Ordering::Acquire) {
        SNAPSHOT_SKIPPED_BUSY.fetch_add(1, Ordering::Relaxed);
        return; // 3. one in-flight build max
    }
    // 4. belt and braces, the replay twin's guard made local here too. Note
    //    the comparison is against the frame's START (`pos`), matching
    //    `replay.rs`'s `Some(pos) > guard.last_applied()`: `last_applied()` is
    //    the SM's own last applied MESSAGE, which is strictly below the
    //    instant's frame, so an instant this row is legitimately at has
    //    `pos > last_applied`. A trait call, but on the SNAPSHOT arm only.
    if Some(pos) <= sm.last_applied() {
        return;
    }
    // **P**, the instant: this frame's END position, not its start. Everything
    // below P has applied (this frame is the last one yielded before the
    // freeze), so the artifact is a function of the log below P — the same
    // function on every node, which is what makes the set position-aligned.
    // The tag is P and NOT the position `freeze()` reports (the SM's own last
    // applied MESSAGE, strictly below P): every row's artifact for one instant
    // must carry the SAME tag or the node can never detect a complete set.
    let p = pos + align_frame_len(hdr.length as usize) as u64;
    // The SM lock is already held by the frames loop; `freeze()` is expected to
    // pin state cheaply and hand the streaming off — the builder thread does
    // the writing, off-lock (`builder_agent`'s module doc). The `Instant` pair
    // brackets exactly this call (spec §9): it is the synchronous cost that
    // can stall the apply thread (§5.7's commit-stall argument), not the
    // builder's async write, which never touches this thread.
    let freeze_t0 = Instant::now();
    let result = (trig.freeze)(sm);
    let freeze_ns = freeze_t0.elapsed().as_nanos() as u64;
    slot.identity.store_freeze_ns(freeze_ns);
    match result {
        Ok((job, _sm_pos)) => {
            trig.busy.store(true, Ordering::Release);
            if trig.tx.try_send((p, job)).is_err() {
                // Defensive only: `busy` already prevents this (the builder
                // cannot be idle AND be holding the one channel slot). Revert
                // it so a torn/disconnected builder cannot wedge every later
                // instant, and count the row incomplete for this one.
                trig.busy.store(false, Ordering::Release);
                SNAPSHOT_SKIPPED_BUSY.fetch_add(1, Ordering::Relaxed);
            }
        }
        Err(e) => {
            SNAPSHOT_FREEZE_FAILED.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "uc_service: snapshot freeze at instant {p} failed: {e} \
                 (this row is incomplete for that instant; the next one retries)"
            );
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
    use super::{
        FLAG_SNAPSHOT_STANDBY, MSG_V2_SCHED, NODE_FLAG_LEADER, NODE_FLAG_LEARNER, SchedOp,
        SchedRecord, check_node_instance,
    };
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

    // ------------------- Cluster-FSM spec §4.9 Task 7: leader-only timer heap

    /// A tiny test SM whose `apply` parses `"schedule <id> @ <ns>"` and calls
    /// `ctx.schedule`. Deliberately does NOT override `pending_timers`/
    /// `table_delivered` — the bare-SM case `ApplyState::pending`/
    /// `table_last` exist for.
    #[derive(Default)]
    struct TimerySm {
        last: Option<u64>,
    }

    impl crate::traits::RawStateMachine for TimerySm {
        const NAME: &'static str = "timery";
        fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], _out: &mut Vec<u8>) {
            self.last = Some(ctx.position);
            let s = std::str::from_utf8(cmd).expect("test payload is ASCII");
            let rest = s
                .strip_prefix("schedule ")
                .expect("test payload starts with `schedule `");
            let (id_str, ns_str) = rest
                .split_once(" @ ")
                .expect("test payload has the form `schedule <id> @ <ns>`");
            ctx.schedule(
                id_str.trim().parse().unwrap(),
                ns_str.trim().parse().unwrap(),
            );
        }
        fn query(&self, _q: &[u8], _out: &mut Vec<u8>) {}
        fn last_applied(&self) -> Option<u64> {
            self.last
        }
    }

    /// Build an `ApplyState<S>` over a heap-backed log buffer plus real ring
    /// files on disk (never `/tmp`), the same shape the `write_sched` tests
    /// above use. Returns the state, the cnc page (to flip
    /// `NODE_FLAG_LEADER`), the `svc_sched` consumer half (the node's side),
    /// and an `Appender` for `append_and_commit`. The `TempDir` must be kept
    /// alive for the ring files' lifetime.
    fn apply_state_for_test<S: crate::traits::RawStateMachine>(
        sm: S,
    ) -> (
        super::ApplyState<S>,
        Arc<CncPage>,
        uc_protocol::ring::SpscConsumer,
        uc_log::buffer::Appender,
        tempfile::TempDir,
    ) {
        let dir = scratch();
        let cnc = page(0x7777);
        cnc.store_services_declared(0b1);
        let buffer = std::sync::Arc::new(uc_log::buffer::LogBuffer::new(
            uc_log::region::Region::heap_zeroed(CAP as usize),
            std::sync::Arc::clone(&cnc),
            256,
        ));
        let appender = uc_log::buffer::Appender::new(std::sync::Arc::clone(&buffer), 1, 0);
        let egress_ring =
            uc_protocol::ring::BroadcastRing::create(&dir.path().join("egress.bc"), 1 << 16, 1024)
                .unwrap();
        let (_qp, svc_query) =
            uc_protocol::ring::SpscRing::create(&dir.path().join("svc_query.ring"), 1 << 16, 1024)
                .unwrap()
                .into_split();
        let (svc_sched, sched_consumer) =
            uc_protocol::ring::SpscRing::create(&dir.path().join("svc_sched.ring"), 1 << 16, 1024)
                .unwrap()
                .into_split();
        let st = super::ApplyState {
            poisoned: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            follower: uc_log::reader::LogFollower::new(std::sync::Arc::clone(&buffer), 0),
            sm: Arc::new(std::sync::Mutex::new(sm)),
            cnc: Arc::clone(&cnc),
            egress: crate::egress::Egress::new(egress_ring.producer()),
            resp_buf: Vec::new(),
            journal_dir: dir.path().join("journal"),
            svc_query,
            svc_sched,
            announce_pending: false,
            was_leader: false,
            pending: std::collections::HashMap::new(),
            table_last: std::collections::HashMap::new(),
            needs_replay: false,
            replay_wait: None,
            instance_id: 0x7777,
            instance_mismatch_streak: 0,
            my_epoch: 1,
            service_id: 0,
            lag_mode: crate::lag::LagMode::Off,
            declared: 0b1,
            lag_waiting: false,
            snapshot_trigger: None,
            snapshot_restore: None,
        };
        (st, cnc, sched_consumer, appender, dir)
    }

    /// Append `payloads` as `MESSAGE` frames and advance `durable`/`commit`
    /// to the new head — the same "no real archive needed" shortcut
    /// `a_bounded_cap_mid_frame_counts_one_lag_wait_per_episode` (below)
    /// uses.
    fn append_and_commit(
        appender: &mut uc_log::buffer::Appender,
        cnc: &CncPage,
        payloads: &[&[u8]],
    ) {
        for p in payloads {
            appender.append(1, 0, p).unwrap();
        }
        let head = appender.position();
        cnc.counters().durable.store_release(head);
        cnc.counters().commit.store_release(head);
    }

    /// Drain every `MSG_V2_SCHED` record currently on the ring.
    fn drain_all(consumer: &mut uc_protocol::ring::SpscConsumer) -> Vec<SchedRecord> {
        let mut out = Vec::new();
        let mut buf = Vec::new();
        while let Some(hdr) = consumer.try_read(&mut buf).unwrap() {
            assert_eq!(
                hdr.msg_type, MSG_V2_SCHED,
                "only MSG_V2_SCHED belongs on this ring"
            );
            out.push(uc_protocol::v2::ipc::read_sched_record(&buf).expect("a well-formed record"));
        }
        out
    }

    /// Cluster-FSM spec §4.9: a follower must never write the `svc_sched`
    /// ring — `write_sched` spins forever on a full one, the load-bearing
    /// reason the service must gate every write on its own leader flag.
    /// Records are still tracked in the loop's `pending` map regardless of
    /// role, and flushed to the node on the rising edge to leader.
    #[test]
    fn a_follower_never_writes_the_sched_ring_and_announces_on_the_leader_edge() {
        let (mut st, cnc, mut sched_consumer, mut appender, _dir) =
            apply_state_for_test(TimerySm::default());
        cnc.status().flags.store_release(0); // follower
        append_and_commit(&mut appender, &cnc, &[b"schedule 1 @ 500"]);
        super::apply_cycle(&mut st);
        assert!(
            sched_consumer.try_read(&mut Vec::new()).unwrap().is_none(),
            "no record on a follower"
        );
        assert_eq!(
            st.pending.get(&1),
            Some(&500),
            "tracked in the loop regardless"
        );

        cnc.status().flags.store_release(NODE_FLAG_LEADER);
        super::apply_cycle(&mut st); // the rising edge
        let recs = drain_all(&mut sched_consumer);
        assert!(
            recs.iter()
                .any(|r| r.op == SchedOp::Schedule && r.timer_id == 1 && r.deadline_ns == 500),
            "announced on the edge: {recs:?}"
        );

        append_and_commit(&mut appender, &cnc, &[b"schedule 2 @ 600"]);
        super::apply_cycle(&mut st);
        let recs = drain_all(&mut sched_consumer);
        assert!(
            recs.iter().any(|r| r.timer_id == 2),
            "written directly once leader"
        );
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
            was_leader: false,
            pending: std::collections::HashMap::new(),
            table_last: std::collections::HashMap::new(),
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
            was_leader: false,
            pending: std::collections::HashMap::new(),
            table_last: std::collections::HashMap::new(),
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

    // ------------------- coordinated snapshot instants (spec §5.2, §5.7)

    /// The `FreezeFn` shape `start_with_snapshots` builds, for `CountSm`: the
    /// job writes the apply count as 8 LE bytes, so a test can read back
    /// exactly how much of the log the frozen image covers.
    fn count_freeze() -> super::FreezeFn<CountSm> {
        Box::new(|sm: &CountSm| {
            let n = sm.applies;
            let job: crate::builder_agent::BuildJob =
                Box::new(move |w: &mut dyn std::io::Write| {
                    w.write_all(&n.to_le_bytes()).map_err(Into::into)
                });
            Ok((job, sm.last.unwrap_or(0)))
        })
    }

    /// Install the snapshot trigger `start_with_snapshots` installs, minus the
    /// builder thread: the receiver half comes back so a test inspects the
    /// handoff directly, and `busy` so it can simulate a build in flight.
    fn with_snapshot_trigger<S: crate::traits::RawStateMachine>(
        st: &mut super::ApplyState<S>,
        freeze: super::FreezeFn<S>,
    ) -> (
        std::sync::mpsc::Receiver<(u64, crate::builder_agent::BuildJob)>,
        Arc<std::sync::atomic::AtomicBool>,
    ) {
        let busy = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        st.snapshot_trigger = Some(super::SnapshotTrigger {
            busy: Arc::clone(&busy),
            tx,
            freeze,
        });
        (rx, busy)
    }

    /// Append a `SNAPSHOT` frame with `flags` and advance `durable`/`commit`
    /// to the new head. Returns **P**, the frame-end position.
    fn append_snapshot_and_commit(
        appender: &mut uc_log::buffer::Appender,
        cnc: &CncPage,
        flags: u8,
    ) -> u64 {
        let (end, _stamp) = appender.append_snapshot(1, flags).unwrap();
        cnc.counters().durable.store_release(end);
        cnc.counters().commit.store_release(end);
        end
    }

    /// Spec §5.2: the trigger is the log, not a byte counter. The row freezes
    /// at the SNAPSHOT frame's frame-end P — after everything below P has
    /// applied, and tagged P (not the SM's own last-applied position).
    #[test]
    fn a_snapshot_frame_freezes_at_its_frame_end_after_everything_below_it() {
        let (mut st, cnc, _sched, mut appender, _dir) = apply_state_for_test(CountSm::default());
        let (rx, _busy) = with_snapshot_trigger(&mut st, count_freeze());
        cnc.status().flags.store_release(NODE_FLAG_LEADER);
        append_and_commit(&mut appender, &cnc, &[b"inc", b"inc"]);
        let end_c = appender.position();
        let end_s = append_snapshot_and_commit(&mut appender, &cnc, 0);
        super::apply_cycle(&mut st);
        let (pos, job) = rx.try_recv().expect("a build job at P");
        assert_eq!(pos, end_s, "the artifact is tagged with the frame-end P");
        let mut img = Vec::new();
        job(&mut img).unwrap();
        assert_eq!(
            u64::from_le_bytes(img.as_slice().try_into().unwrap()),
            2,
            "frozen AFTER the two incs below P"
        );
        assert!(end_c < end_s);
    }

    /// Spec §5.7: a standby-flagged instant is a learner's work. A voter
    /// yields the frame like any other node-only frame and pays no freeze.
    #[test]
    fn a_standby_instant_is_ignored_by_a_voter_and_taken_by_a_learner() {
        let (mut st, cnc, _sched, mut appender, _dir) = apply_state_for_test(CountSm::default());
        let (rx, _busy) = with_snapshot_trigger(&mut st, count_freeze());
        cnc.status().flags.store_release(0); // voter (follower)
        append_snapshot_and_commit(&mut appender, &cnc, FLAG_SNAPSHOT_STANDBY);
        super::apply_cycle(&mut st);
        assert!(rx.try_recv().is_err(), "a voter ignores a standby instant");
        cnc.status().flags.store_release(NODE_FLAG_LEARNER);
        let p = append_snapshot_and_commit(&mut appender, &cnc, FLAG_SNAPSHOT_STANDBY);
        super::apply_cycle(&mut st);
        assert_eq!(
            rx.try_recv().map(|(pos, _)| pos).ok(),
            Some(p),
            "a learner takes it"
        );
    }

    /// Spec §5.2 / §10: a row started with plain `start()` has no capability
    /// and ignores the frame (the set is simply incomplete); a row whose
    /// builder is still busy skips this instant and counts the skip.
    #[test]
    fn a_non_capable_service_ignores_the_frame_and_a_busy_one_counts_the_skip() {
        let (mut st, cnc, _sched, mut appender, _dir) = apply_state_for_test(CountSm::default());
        cnc.status().flags.store_release(NODE_FLAG_LEADER);
        append_snapshot_and_commit(&mut appender, &cnc, 0);
        super::apply_cycle(&mut st); // must not panic, must not build
        assert!(st.snapshot_trigger.is_none());

        let (mut st, cnc, _sched, mut appender, _dir) = apply_state_for_test(CountSm::default());
        let (rx, busy) = with_snapshot_trigger(&mut st, count_freeze());
        cnc.status().flags.store_release(NODE_FLAG_LEADER);
        busy.store(true, std::sync::atomic::Ordering::Release);
        let before = super::SNAPSHOT_SKIPPED_BUSY.load(std::sync::atomic::Ordering::Relaxed);
        append_snapshot_and_commit(&mut appender, &cnc, 0);
        super::apply_cycle(&mut st);
        assert!(rx.try_recv().is_err(), "a busy builder gets no new job");
        assert_eq!(
            super::SNAPSHOT_SKIPPED_BUSY.load(std::sync::atomic::Ordering::Relaxed),
            before + 1
        );
    }
}
