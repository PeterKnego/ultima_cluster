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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerReport {
    pub fired: Vec<FiredRec>,
    /// `(position, time_ns)` for every applied frame AND every delivered timer,
    /// in apply order.
    pub stamps: Vec<(u64, u64)>,
}

/// The timer state machine: schedules on command, records what fired.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TimerSm {
    fired: Vec<FiredRec>,
    stamps: Vec<(u64, u64)>,
    last: Option<u64>,
}

impl TimerSm {
    pub fn report(&self) -> TimerReport {
        TimerReport {
            fired: self.fired.clone(),
            stamps: self.stamps.clone(),
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
                ctx.schedule(id, ctx.time_ns + in_ns)
            }
            MixedCmd::Timer(TimerCmd::Cancel { id }) => ctx.cancel(id),
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
