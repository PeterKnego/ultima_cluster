// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The timer state machine the time-and-timers capstones run, plus the shared
//! command wire a heterogeneous two-FSM cluster needs.
//!
//! ## Why there is a shared command wire
//!
//! UC's log is a **broadcast**: `submit_to(id, ..)` selects whose *response*
//! the client awaits, but every declared FSM applies every committed `MESSAGE`
//! frame (M14 "one log → N FSMs" — `uc_service::apply`'s loop has no per-row
//! filter; only `TIMER` frames are filtered, by identity hash). So two FSMs of
//! DIFFERENT types on one node cannot each have their own command type: each
//! row would be handed the other row's bytes and either fail-stop on the
//! decode or — worse — decode them into a valid-but-wrong command of its own
//! (bincode ignores trailing bytes, so `TimerCmd::Schedule { id, in_ns }` and
//! `register::Cmd::Write(id)` share a prefix).
//!
//! [`MixedCmd`] is therefore the ONE command type both rows decode:
//! [`MixedRegisterSm`] (row 0) runs the untouched [`RegisterSm`] transition for
//! `Reg(..)` and ignores `Timer(..)`; [`TimerSm`] (row 1) schedules/cancels for
//! `Timer(..)` and, for `Reg(..)`, only records the frame's stamp. The register
//! history the WGL checker adjudicates is therefore exactly `RegisterSm`'s, and
//! the sibling's traffic is a provable no-op on it.
//!
//! ## What `TimerSm` records
//!
//! Everything the §4.3 ordering property needs, and nothing else:
//!
//! * `fired` — one [`FiredRec`] per delivered `on_timer`, so the capstone can
//!   compare the vectors across replicas (replication equivalence) and check
//!   that no `(id, deadline_ns)` was delivered twice (exactly-once through
//!   [`uc_service::Timed`]).
//! * `stamps` — `(position, time_ns)` for EVERY apply and every `on_timer`, so
//!   the §4.3 partition check ("every frame before the timer is stamped at or
//!   before its deadline, every frame after it at or after the fire's stamp")
//!   has the whole series to work over.
//!
//! Like [`RegisterSm`], this SM persists NOTHING beyond the snapshot capability
//! below: a crashed service comes back empty and the node reconstructs it from
//! the replicated log — which is exactly how the capstones exercise journal
//! replay of `TIMER` frames through `Timed`.

use serde::{Deserialize, Serialize};

use crate::register::{Cmd, CmdResp, RegisterSm};

/// A timer operation (spec §4.4). `in_ns` is RELATIVE to the frame's own
/// stamp: `apply` schedules at `ctx.time_ns + in_ns`, so the deadline is a
/// function of the committed log alone and every replica computes the same one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimerCmd {
    Schedule { id: u64, in_ns: u64 },
    Cancel { id: u64 },
}

/// The shared command wire (see the module doc): whatever a client submits,
/// BOTH rows of a mixed register/timer cluster decode it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MixedCmd {
    Reg(Cmd),
    Timer(TimerCmd),
}

impl From<Cmd> for MixedCmd {
    fn from(c: Cmd) -> MixedCmd {
        MixedCmd::Reg(c)
    }
}
impl From<TimerCmd> for MixedCmd {
    fn from(t: TimerCmd) -> MixedCmd {
        MixedCmd::Timer(t)
    }
}

/// [`TimerSm`]'s response: the leader stamp the command was applied at — the
/// value `ctx.time_ns` carried, echoed back so a client can compute a deadline
/// in the same clock the log uses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimerResp {
    Stamp(u64),
}

/// One delivered timer, as `on_timer` saw it. `time_ns` is the FIRING frame's
/// stamp: equal to `deadline_ns` on an on-time fire, strictly greater on a late
/// one (spec §4.3's post-failover case).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FiredRec {
    pub position: u64,
    pub id: u64,
    pub deadline_ns: u64,
    pub time_ns: u64,
}

impl FiredRec {
    /// The leader could not place this instance at its deadline.
    pub fn late(&self) -> bool {
        self.time_ns > self.deadline_ns
    }
}

/// [`TimerSm`]'s query answer — the whole record, for the capstone oracle.
///
/// `scheduled`/`cancelled` are what makes the oracle a two-sided one: without
/// them "exactly once" is only *at most* once (nothing would catch a timer
/// that never fired, or one that fired after being cancelled). Every vector
/// here is a pure function of the applied MESSAGE/TIMER frames, so two
/// replicas at the same applied position hold identical ones.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerReport {
    pub fired: Vec<FiredRec>,
    /// `(position, time_ns)` for every applied frame AND every delivered timer,
    /// in apply order.
    pub stamps: Vec<(u64, u64)>,
    /// `(position, id, deadline_ns)` per applied [`TimerCmd::Schedule`], in
    /// apply order. `deadline_ns` is the ABSOLUTE deadline the SM asked for
    /// (`ctx.time_ns + in_ns`), i.e. exactly what a matching fire must carry.
    pub scheduled: Vec<(u64, u64, u64)>,
    /// `(position, id)` per applied [`TimerCmd::Cancel`], in apply order.
    pub cancelled: Vec<(u64, u64)>,
}

/// The timer state machine: schedules on command, records what fired.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TimerSm {
    fired: Vec<FiredRec>,
    stamps: Vec<(u64, u64)>,
    scheduled: Vec<(u64, u64, u64)>,
    cancelled: Vec<(u64, u64)>,
    last: Option<u64>,
}

impl TimerSm {
    pub fn report(&self) -> TimerReport {
        TimerReport {
            fired: self.fired.clone(),
            stamps: self.stamps.clone(),
            scheduled: self.scheduled.clone(),
            cancelled: self.cancelled.clone(),
        }
    }
}

// ------------------------------------------------------------------ v2 SDK

#[cfg(feature = "v2")]
impl uc_service::StateMachine for TimerSm {
    const NAME: &'static str = "timer";

    type Command = MixedCmd;
    type Response = TimerResp;
    type Query = ();
    type QueryResponse = TimerReport;

    fn apply(&mut self, ctx: &mut uc_service::ApplyCtx, cmd: MixedCmd) -> TimerResp {
        match cmd {
            // Relative to the frame's own stamp — deterministic on every replica.
            MixedCmd::Timer(TimerCmd::Schedule { id, in_ns }) => {
                let at_ns = ctx.time_ns + in_ns;
                ctx.schedule(id, at_ns);
                self.scheduled.push((ctx.position, id, at_ns));
            }
            MixedCmd::Timer(TimerCmd::Cancel { id }) => {
                ctx.cancel(id);
                self.cancelled.push((ctx.position, id));
            }
            // The sibling row's command on the shared log: no timer transition,
            // but the frame still gets a stamp (the §4.3 series must be dense).
            MixedCmd::Reg(_) => {}
        }
        self.stamps.push((ctx.position, ctx.time_ns));
        self.last = Some(ctx.position);
        TimerResp::Stamp(ctx.time_ns)
    }

    fn query(&self, _q: ()) -> TimerReport {
        self.report()
    }

    fn last_applied(&self) -> Option<u64> {
        self.last
    }

    /// A timer this FSM asked for has reached its position on the log. Record
    /// it and advance the frontier from `ctx.position`, exactly as `apply`
    /// does — `Timed<TimerSm>` has already decided this instance is still
    /// pending, so every delivery seen here is a first (and only) delivery.
    /// Deliberately asks for NOTHING: a re-schedule from inside `on_timer`
    /// would be dropped on the replay path (spec §4.8 re-announces instead),
    /// and the capstone's exactly-once oracle wants a fixed instance set.
    fn on_timer(&mut self, ctx: &mut uc_service::ApplyCtx, ev: uc_service::TimerEvent) {
        self.fired.push(FiredRec {
            position: ctx.position,
            id: ev.id,
            deadline_ns: ev.deadline_ns,
            time_ns: ctx.time_ns,
        });
        self.stamps.push((ctx.position, ctx.time_ns));
        self.last = Some(ctx.position);
    }
}

/// bincode of the whole struct, like [`RegisterSm`]'s M6 impl; the artifact is
/// tagged with `last_applied` and a mis-tagged install is refused.
#[cfg(feature = "v2")]
impl uc_service::SnapshotStateMachine for TimerSm {
    type SnapshotHandle = Vec<u8>;

    fn freeze(&self) -> Result<(Vec<u8>, u64), uc_service::SnapshotError> {
        let buf = bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| uc_service::SnapshotError::Codec(e.to_string()))?;
        Ok((buf, self.last.unwrap_or(0)))
    }

    fn stream_snapshot(
        handle: Vec<u8>,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc_service::SnapshotError> {
        std::io::Write::write_all(dst, &handle)?;
        Ok(())
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc_service::SnapshotError> {
        let mut buf = Vec::new();
        std::io::Read::read_to_end(src, &mut buf)?;
        let (sm, _) =
            bincode::serde::decode_from_slice::<TimerSm, _>(&buf, bincode::config::standard())
                .map_err(|e| uc_service::SnapshotError::Codec(e.to_string()))?;
        if sm.last.unwrap_or(0) != position {
            return Err(uc_service::SnapshotError::Codec(format!(
                "snapshot payload position {} != requested {position}",
                sm.last.unwrap_or(0)
            )));
        }
        *self = sm;
        self.last = Some(position);
        Ok(position)
    }
}

// ------------------------------------------------- the register on that wire

/// Row 0 of a mixed register/timer cluster: the untouched [`RegisterSm`]
/// transition, reached through [`MixedCmd`] so the row can decode the sibling's
/// frames instead of fail-stopping on them. `Timer(..)` is a no-op — no state
/// change and no client awaits the answer (the timer workers submit with
/// `submit_to(1, ..)`), so the WGL history recorded for this row is exactly the
/// register history `check_register` has always adjudicated.
#[derive(Default)]
pub struct MixedRegisterSm(RegisterSm);

#[cfg(feature = "v2")]
impl uc_service::StateMachine for MixedRegisterSm {
    // The same name the plain register attaches under: this IS the register
    // row, only reading a wider command wire.
    const NAME: &'static str = <RegisterSm as uc_service::StateMachine>::NAME;

    type Command = MixedCmd;
    type Response = CmdResp;
    type Query = ();
    type QueryResponse = Option<u64>;

    fn apply(&mut self, ctx: &mut uc_service::ApplyCtx, cmd: MixedCmd) -> CmdResp {
        match cmd {
            MixedCmd::Reg(c) => uc_service::StateMachine::apply(&mut self.0, ctx, c),
            // A sibling FSM's command: no register transition. The answer is
            // published but never awaited.
            MixedCmd::Timer(_) => CmdResp::CasResult(false),
        }
    }

    fn query(&self, q: ()) -> Option<u64> {
        uc_service::StateMachine::query(&self.0, q)
    }

    /// Deliberately the INNER register's frontier: an ignored sibling frame
    /// advances nothing, and under-reporting `last_applied` is safe (the apply
    /// loop's idempotent-skip re-applies nothing already seen).
    fn last_applied(&self) -> Option<u64> {
        uc_service::StateMachine::last_applied(&self.0)
    }
}

#[cfg(feature = "v2")]
impl uc_service::SnapshotStateMachine for MixedRegisterSm {
    type SnapshotHandle = <RegisterSm as uc_service::SnapshotStateMachine>::SnapshotHandle;

    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), uc_service::SnapshotError> {
        uc_service::SnapshotStateMachine::freeze(&self.0)
    }

    fn stream_snapshot(
        handle: Self::SnapshotHandle,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc_service::SnapshotError> {
        <RegisterSm as uc_service::SnapshotStateMachine>::stream_snapshot(handle, dst)
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc_service::SnapshotError> {
        uc_service::SnapshotStateMachine::install_snapshot(&mut self.0, position, src)
    }
}

// ------------------------------------------------------------- the oracle

/// How close to the end of the record the no-loss clause stops demanding a
/// fire. A timer whose deadline falls inside this window of the LAST stamp may
/// legitimately still be in flight when the report is read: the schedule
/// travels service -> node over the sched ring after the apply that made it, so
/// the node's heap can learn about an instance a hair after a frame stamped
/// past its deadline was already appended, and the fire then lands (late) in a
/// later pass. Everything older than the window has had a whole margin of log
/// time to fire in.
pub const COMPLETENESS_MARGIN_NS: u64 = 250_000_000;

/// What [`assert_timer_report`] measured, for the caller's summary line and
/// its own non-vacuity bars.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TimerStats {
    pub fires: usize,
    /// Fires the leader could not place at their deadline (`time_ns >
    /// deadline_ns`) — spec §4.3's post-failover case.
    pub late: usize,
    pub scheduled: usize,
    pub cancelled: usize,
    /// Schedules the no-loss clause actually DEMANDED a fire for (live,
    /// un-superseded, and older than [`COMPLETENESS_MARGIN_NS`]). A run whose
    /// `completeness_checked` is 0 proved nothing about loss.
    pub completeness_checked: usize,
}

/// The timer oracle, shared by the in-process capstone
/// (`uc_node/tests/lin_v2.rs`) and the multi-process SIGKILL scenario
/// (`examples/uc_crashtest/tests/hard_crash.rs`) so neither can drift from the
/// other. Panics with a located message on the first violation; the
/// cross-replica identity check lives in the capstone, which is the only
/// caller holding several reports.
///
/// Over ONE node's record:
///
/// 1. **Well-formed series.** `stamps` is strictly ascending in `position`
///    (one entry per applied frame — a repeat would mean a frame was applied
///    twice) and non-decreasing in `time_ns` (log time never goes backwards).
/// 2. **Never early.** `time_ns >= deadline_ns` for every fire. This is the
///    load-bearing half of §4.3 and the one an implementation could most
///    plausibly get wrong: a node that delivered every timer the instant it
///    was armed would satisfy every ordering clause below and still be broken.
/// 3. **At most once.** No `(id, deadline_ns)` delivered twice — what
///    `Timed<S>` exists to guarantee across a re-arm.
/// 4. **Ordering, independent of the fire's own stamp.** Both windows EXCLUDE
///    the firing frame itself (`position` strictly less / strictly greater),
///    so neither clause is a restatement of the monotonicity in (1): for an
///    on-time fire every earlier frame is stamped at or before the deadline,
///    and for any fire every later frame is stamped at or after the fire's own
///    stamp.
/// 5. **Every fire matches a live instance.** For a fire at `f` carrying
///    `(id, d)` there is a `Schedule (id, d)` at some `p < f` with no
///    `Cancel(id)` and no `Schedule(id, d')`, `d' != d`, anywhere in `(p, f)`.
///    A fire for a cancelled or superseded instance fails here — this is the
///    cancel-honoured clause.
/// 6. **No loss.** Every `Schedule (id, d)` at `p` that no later frame
///    cancelled or superseded, and whose `d` is older than
///    [`COMPLETENESS_MARGIN_NS`] before the last stamp, HAS a fire `(id, d)`
///    at a position after `p`. Without this, (3) alone is only *at most* once:
///    a node that armed nothing would pass.
pub fn assert_timer_report(tag: &str, report: &TimerReport) -> TimerStats {
    use std::collections::HashMap;
    use std::collections::HashSet;

    let st = &report.stamps;

    // (1) well-formed series.
    assert!(
        st.windows(2).all(|w| w[0].0 < w[1].0),
        "[{tag}] the stamp series is not strictly ascending in position — a frame was \
         applied twice, or the series is out of order"
    );
    assert!(
        st.windows(2).all(|w| w[0].1 <= w[1].1),
        "[{tag}] log time went backwards in the stamp series"
    );

    // Prefix-max / suffix-min over the stamp series, so (4) is
    // O(stamps + fires log stamps) rather than O(fires x stamps).
    let mut prefix_max = vec![0u64; st.len() + 1];
    for i in 0..st.len() {
        prefix_max[i + 1] = prefix_max[i].max(st[i].1);
    }
    let mut suffix_min = vec![u64::MAX; st.len() + 1];
    for i in (0..st.len()).rev() {
        suffix_min[i] = suffix_min[i + 1].min(st[i].1);
    }

    // Index the schedules/cancels by id; both vectors are already in apply
    // order, so each per-id list is ascending in position.
    let mut sched_by_id: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
    for &(p, id, d) in &report.scheduled {
        sched_by_id.entry(id).or_default().push((p, d));
    }
    let mut cancel_by_id: HashMap<u64, Vec<u64>> = HashMap::new();
    for &(p, id) in &report.cancelled {
        cancel_by_id.entry(id).or_default().push(p);
    }
    // Fire positions keyed by the instance they delivered, so the no-loss
    // clause can demand a fire AFTER the schedule it is checking (not merely
    // somewhere in the record).
    let mut fired_at: HashMap<(u64, u64), Vec<u64>> = HashMap::new();
    for r in &report.fired {
        fired_at
            .entry((r.id, r.deadline_ns))
            .or_default()
            .push(r.position);
    }

    let mut seen = HashSet::new();
    let mut late = 0usize;
    for rec in &report.fired {
        // (2) never early — the property the whole feature rests on.
        assert!(
            rec.time_ns >= rec.deadline_ns,
            "[{tag}] timer delivered EARLY: stamped {} but its deadline is {} — {rec:?}",
            rec.time_ns,
            rec.deadline_ns
        );
        // (3) at most once.
        assert!(
            seen.insert((rec.id, rec.deadline_ns)),
            "[{tag}] timer ({}, deadline {}) delivered TWICE — {rec:?}",
            rec.id,
            rec.deadline_ns
        );
        // (4) ordering, both windows excluding the firing frame itself.
        let lo = st.partition_point(|&(p, _)| p < rec.position);
        let hi = st.partition_point(|&(p, _)| p <= rec.position);
        if rec.time_ns > rec.deadline_ns {
            late += 1;
        } else {
            assert!(
                prefix_max[lo] <= rec.deadline_ns,
                "[{tag}] a frame BEFORE the on-time timer at {} is stamped {} > its deadline \
                 {} — {rec:?}",
                rec.position,
                prefix_max[lo],
                rec.deadline_ns
            );
        }
        assert!(
            suffix_min[hi] >= rec.time_ns,
            "[{tag}] a frame AFTER the timer at {} is stamped {} < the fire's own stamp {} \
             — {rec:?}",
            rec.position,
            suffix_min[hi],
            rec.time_ns
        );
        // (5) the fire matches a live, un-cancelled, un-superseded instance.
        let list = sched_by_id.get(&rec.id).unwrap_or_else(|| {
            panic!(
                "[{tag}] timer id {} fired but was never scheduled — {rec:?}",
                rec.id
            )
        });
        let p = list
            .iter()
            .rev()
            .find(|&&(p, d)| p < rec.position && d == rec.deadline_ns)
            .map(|&(p, _)| p)
            .unwrap_or_else(|| {
                panic!(
                    "[{tag}] fire {rec:?} has no earlier Schedule with that exact deadline; \
                     this id's schedules: {list:?}"
                )
            });
        if let Some(cs) = cancel_by_id.get(&rec.id) {
            assert!(
                !cs.iter().any(|&c| c > p && c < rec.position),
                "[{tag}] CANCELLED timer fired: scheduled at {p}, cancelled at {:?}, \
                 delivered — {rec:?}",
                cs.iter().find(|&&c| c > p && c < rec.position)
            );
        }
        assert!(
            !list
                .iter()
                .any(|&(q, d)| q > p && q < rec.position && d != rec.deadline_ns),
            "[{tag}] SUPERSEDED timer fired: scheduled at {p} for deadline {}, re-scheduled \
             before it fired — {rec:?} (this id's schedules: {list:?})",
            rec.deadline_ns
        );
    }

    // (6) no loss. `last_stamp` is the newest log time this node has applied;
    // by (1) it is the last entry.
    let last_stamp = st.last().map(|&(_, t)| t).unwrap_or(0);
    let mut completeness_checked = 0usize;
    for &(p, id, d) in &report.scheduled {
        let superseded = sched_by_id[&id].iter().any(|&(q, _)| q > p)
            || cancel_by_id
                .get(&id)
                .is_some_and(|cs| cs.iter().any(|&c| c > p));
        if superseded {
            continue;
        }
        if d.saturating_add(COMPLETENESS_MARGIN_NS) > last_stamp {
            continue; // still legitimately in flight — see COMPLETENESS_MARGIN_NS
        }
        assert!(
            fired_at
                .get(&(id, d))
                .is_some_and(|ps| ps.iter().any(|&f| f > p)),
            "[{tag}] LOST timer: scheduled ({id}, deadline {d}) at position {p}, never \
             cancelled or re-scheduled, deadline passed {} ns before the last applied frame \
             ({last_stamp}) — and it never fired",
            last_stamp - d
        );
        completeness_checked += 1;
    }

    TimerStats {
        fires: report.fired.len(),
        late,
        scheduled: report.scheduled.len(),
        cancelled: report.cancelled.len(),
        completeness_checked,
    }
}

#[cfg(all(test, feature = "v2"))]
mod v2_tests {
    use super::{FiredRec, MixedCmd, MixedRegisterSm, TimerCmd, TimerResp, TimerSm};
    use crate::register::{Cmd, CmdResp};
    use uc_service::{ApplyCtx, StateMachine, TimerEvent, TimerReq};

    /// `apply` schedules relative to the frame's own stamp, `on_timer` records
    /// the fire, and `last_applied` advances from BOTH.
    #[test]
    fn schedule_from_apply_then_fire_is_recorded() {
        let mut sm = TimerSm::default();
        assert_eq!(sm.last_applied(), None);

        // Schedule +50 ms off a frame stamped at 1_000.
        let mut ctx = ApplyCtx::for_sm::<TimerSm>(128).with_time(1_000);
        let resp = sm.apply(
            &mut ctx,
            MixedCmd::Timer(TimerCmd::Schedule {
                id: 7,
                in_ns: 50_000_000,
            }),
        );
        assert_eq!(resp, TimerResp::Stamp(1_000));
        assert_eq!(
            ctx.timers(),
            [TimerReq::Schedule {
                id: 7,
                at_ns: 50_001_000
            }]
        );
        assert_eq!(sm.last_applied(), Some(128));

        // A cancel reaches the framework the same way.
        let mut ctx = ApplyCtx::for_sm::<TimerSm>(160).with_time(2_000);
        sm.apply(&mut ctx, MixedCmd::Timer(TimerCmd::Cancel { id: 9 }));
        assert_eq!(ctx.timers(), [TimerReq::Cancel { id: 9 }]);

        // The sibling row's command is a stamp-only no-op here.
        let mut ctx = ApplyCtx::for_sm::<TimerSm>(192).with_time(3_000);
        sm.apply(&mut ctx, MixedCmd::Reg(Cmd::Write(4)));
        assert!(ctx.timers().is_empty());

        // The fire lands in `fired`, stamps the series, and advances the frontier.
        let mut ctx = ApplyCtx::for_sm::<TimerSm>(224).with_time(50_001_000);
        sm.on_timer(
            &mut ctx,
            TimerEvent {
                id: 7,
                deadline_ns: 50_001_000,
                table: false,
            },
        );
        let report = sm.query(());
        assert_eq!(
            report.fired,
            [FiredRec {
                position: 224,
                id: 7,
                deadline_ns: 50_001_000,
                time_ns: 50_001_000,
            }]
        );
        assert!(!report.fired[0].late());
        assert_eq!(
            report.stamps,
            [(128, 1_000), (160, 2_000), (192, 3_000), (224, 50_001_000)]
        );
        // The two-sided oracle's inputs: the schedule and the cancel are
        // recorded with the ABSOLUTE deadline the SM asked for.
        assert_eq!(report.scheduled, [(128, 7, 50_001_000)]);
        assert_eq!(report.cancelled, [(160, 9)]);
        assert_eq!(sm.last_applied(), Some(224));
    }

    /// A late fire is stamped past its deadline and says so.
    #[test]
    fn a_late_fire_reports_late() {
        let mut sm = TimerSm::default();
        let mut ctx = ApplyCtx::for_sm::<TimerSm>(64).with_time(9_000);
        sm.on_timer(
            &mut ctx,
            TimerEvent {
                id: 1,
                deadline_ns: 5_000,
                table: false,
            },
        );
        assert!(sm.query(()).fired[0].late());
    }

    /// The snapshot capability round-trips the whole record, keyed on position.
    #[test]
    fn snapshot_roundtrip_via_v2_capability() {
        use uc_service::SnapshotStateMachine;

        let mut sm = TimerSm::default();
        sm.apply(
            &mut ApplyCtx::for_sm::<TimerSm>(4096).with_time(11),
            MixedCmd::Timer(TimerCmd::Schedule { id: 3, in_ns: 5 }),
        );
        let (handle, s) = sm.freeze().unwrap();
        assert_eq!(s, 4096);
        let mut bytes = Vec::new();
        TimerSm::stream_snapshot(handle, &mut bytes).unwrap();

        let mut restored = TimerSm::default();
        assert_eq!(
            restored
                .install_snapshot(4096, &mut bytes.as_slice())
                .unwrap(),
            4096
        );
        assert_eq!(restored.query(()).stamps, [(4096, 11)]);
        assert_eq!(restored.last_applied(), Some(4096));
        // A mis-tagged install (wrong artifact position) is refused.
        assert!(
            restored
                .install_snapshot(99, &mut bytes.as_slice())
                .is_err()
        );
    }

    /// Row 0 runs the register transition for `Reg(..)` and is inert for the
    /// sibling's `Timer(..)` — the property the WGL history rests on.
    #[test]
    fn the_register_row_ignores_the_siblings_commands() {
        let mut sm = MixedRegisterSm::default();
        assert_eq!(
            sm.apply(
                &mut ApplyCtx::for_sm::<MixedRegisterSm>(128),
                MixedCmd::Reg(Cmd::Write(7))
            ),
            CmdResp::WriteAck
        );
        // A timer command changes nothing and asks for no timer.
        let mut ctx = ApplyCtx::for_sm::<MixedRegisterSm>(160);
        assert_eq!(
            sm.apply(
                &mut ctx,
                MixedCmd::Timer(TimerCmd::Schedule {
                    id: 1,
                    in_ns: 1_000
                })
            ),
            CmdResp::CasResult(false)
        );
        assert!(ctx.timers().is_empty());
        assert_eq!(sm.query(()), Some(7));
        assert_eq!(sm.last_applied(), Some(128));
        // ...and the register keeps working afterwards.
        assert_eq!(
            sm.apply(
                &mut ApplyCtx::for_sm::<MixedRegisterSm>(192),
                MixedCmd::Reg(Cmd::Cas { old: 7, new: 9 })
            ),
            CmdResp::CasResult(true)
        );
        assert_eq!(sm.query(()), Some(9));
    }
}

/// The oracle must BITE. Each test injects exactly one defect into an
/// otherwise-clean record and asserts [`assert_timer_report`] convicts it — if
/// a clause is ever weakened into a tautology, its test stops panicking.
#[cfg(test)]
mod oracle_tests {
    use super::{FiredRec, TimerReport, assert_timer_report};

    /// Two timers, both scheduled, both fired on time, both deadlines well
    /// past the last stamp's completeness margin.
    fn clean() -> TimerReport {
        TimerReport {
            //          pos    id  deadline    stamp
            scheduled: vec![(100, 1, 1_000), (200, 2, 2_000)],
            cancelled: vec![],
            fired: vec![
                FiredRec {
                    position: 300,
                    id: 1,
                    deadline_ns: 1_000,
                    time_ns: 1_000,
                },
                FiredRec {
                    position: 400,
                    id: 2,
                    deadline_ns: 2_000,
                    time_ns: 2_000,
                },
            ],
            stamps: vec![
                (100, 500),
                (200, 900),
                (300, 1_000),
                (400, 2_000),
                // A last frame far past both deadlines, so the no-loss clause
                // is armed for both (COMPLETENESS_MARGIN_NS = 250 ms).
                (500, 2_000 + super::COMPLETENESS_MARGIN_NS),
            ],
        }
    }

    #[test]
    fn a_clean_record_passes_and_the_no_loss_clause_is_armed() {
        let stats = assert_timer_report("clean", &clean());
        assert_eq!(stats.fires, 2);
        assert_eq!(stats.late, 0);
        assert_eq!(stats.scheduled, 2);
        // Both schedules were live, un-superseded and past the margin.
        assert_eq!(stats.completeness_checked, 2);
    }

    #[test]
    #[should_panic(expected = "delivered EARLY")]
    fn an_early_delivery_is_convicted() {
        let mut r = clean();
        // Delivered 400 ns before its deadline, with the whole series kept
        // monotone and every ordering clause still satisfied — only the
        // never-early check catches this.
        r.stamps[1].1 = 500;
        r.fired[0].time_ns = 600;
        r.stamps[2].1 = 600;
        assert_timer_report("early", &r);
    }

    #[test]
    #[should_panic(expected = "delivered TWICE")]
    fn a_duplicate_delivery_is_convicted() {
        let mut r = clean();
        r.fired.push(FiredRec {
            position: 450,
            id: 1,
            deadline_ns: 1_000,
            time_ns: 2_000,
        });
        r.stamps.insert(4, (450, 2_000));
        assert_timer_report("dup", &r);
    }

    #[test]
    #[should_panic(expected = "CANCELLED timer fired")]
    fn a_cancelled_timer_that_fires_is_convicted() {
        let mut r = clean();
        r.cancelled.push((250, 1));
        assert_timer_report("cancelled", &r);
    }

    #[test]
    #[should_panic(expected = "SUPERSEDED timer fired")]
    fn a_superseded_instance_that_fires_is_convicted() {
        let mut r = clean();
        // id 1 re-scheduled to a different deadline before the old instance
        // fired; the old instance must never be delivered.
        r.scheduled.push((250, 1, 9_000));
        assert_timer_report("superseded", &r);
    }

    #[test]
    #[should_panic(expected = "LOST timer")]
    fn a_lost_timer_is_convicted() {
        let mut r = clean();
        r.fired.remove(0); // id 1 armed, never cancelled, never delivered
        assert_timer_report("lost", &r);
    }

    #[test]
    #[should_panic(expected = "never scheduled")]
    fn a_fire_with_no_schedule_is_convicted() {
        let mut r = clean();
        r.scheduled.remove(0);
        assert_timer_report("unscheduled", &r);
    }

    #[test]
    #[should_panic(expected = "log time went backwards")]
    fn a_non_monotone_stamp_series_is_convicted() {
        let mut r = clean();
        r.stamps[3].1 = 700;
        assert_timer_report("non-monotone", &r);
    }

    /// The no-loss clause must not fire on an instance still legitimately in
    /// flight: same record, but the run ends right at the deadline.
    #[test]
    fn a_timer_inside_the_completeness_margin_is_not_demanded() {
        let mut r = clean();
        r.fired.remove(0);
        r.stamps.pop(); // last stamp is now 2_000, inside id 1's margin
        let stats = assert_timer_report("in-flight", &r);
        // id 2's fire is at the last stamp, so it too is inside the margin.
        assert_eq!(stats.completeness_checked, 0);
    }
}
