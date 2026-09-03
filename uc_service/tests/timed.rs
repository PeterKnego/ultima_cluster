// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use uc_protocol::v2::ipc::SchedOp;
use uc_service::{
    ApplyCtx, RawStateMachine, SnapshotError, SnapshotStateMachine, Timed, TimerEvent,
};

#[derive(Default)]
struct Rec {
    fired: Vec<(u64, u64, u64)>, // (position, id, deadline)
    last: Option<u64>,
}
impl RawStateMachine for Rec {
    const NAME: &'static str = "rec";
    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], _out: &mut Vec<u8>) {
        // cmd: b"s<id>@<at>" schedules, b"c<id>" cancels
        let s = std::str::from_utf8(cmd).unwrap();
        if let Some(rest) = s.strip_prefix('s') {
            let (id, at) = rest.split_once('@').unwrap();
            ctx.schedule(id.parse().unwrap(), at.parse().unwrap());
        } else if let Some(id) = s.strip_prefix('c') {
            ctx.cancel(id.parse().unwrap());
        }
        self.last = Some(ctx.position);
    }
    fn query(&self, _q: &[u8], _out: &mut Vec<u8>) {}
    fn last_applied(&self) -> Option<u64> {
        self.last
    }
    fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {
        self.fired.push((ctx.position, ev.id, ev.deadline_ns));
        self.last = Some(ctx.position);
    }
}

fn ctx(pos: u64, t: u64) -> ApplyCtx {
    ApplyCtx::for_sm::<Timed<Rec>>(pos).with_time(t)
}
fn ev(id: u64, dl: u64) -> TimerEvent {
    TimerEvent {
        id,
        deadline_ns: dl,
        table: false,
    }
}
fn tev(id: u64, dl: u64) -> TimerEvent {
    TimerEvent {
        id,
        deadline_ns: dl,
        table: true,
    }
}

#[test]
fn delivers_a_pending_instance_exactly_once_and_reports_consumed() {
    let mut t = Timed::new(Rec::default());
    let mut c = ctx(64, 100);
    t.apply(&mut c, b"s7@500", &mut Vec::new());
    assert_eq!(t.pending(), vec![(7, 500)]);
    assert_eq!(t.pending_timers(), vec![(7, 500)]);
    // the schedule request is still in ctx for the apply loop to forward
    assert_eq!(c.timers().len(), 1);
    let mut c = ctx(128, 500);
    t.on_timer(&mut c, ev(7, 500));
    assert_eq!(t.inner().fired, vec![(128, 7, 500)]);
    assert!(t.pending().is_empty());
    assert_eq!(t.last_applied(), Some(128));
    // duplicate (a re-fire after leadership loss): dropped, still consumed, still advances
    let mut c = ctx(192, 500);
    t.on_timer(&mut c, ev(7, 500));
    assert_eq!(t.inner().fired.len(), 1, "dropped");
    assert_eq!(t.last_applied(), Some(192));
}

#[test]
fn reschedule_replaces_and_cancel_wins_over_a_fire_already_on_the_log() {
    let mut t = Timed::new(Rec::default());
    t.apply(&mut ctx(64, 100), b"s7@500", &mut Vec::new());
    t.apply(&mut ctx(96, 100), b"s7@900", &mut Vec::new());
    assert_eq!(t.pending(), vec![(7, 900)], "replaced");
    t.on_timer(&mut ctx(128, 500), ev(7, 500));
    assert!(
        t.inner().fired.is_empty(),
        "the stale instance (7, 500) is not pending"
    );
    t.apply(&mut ctx(160, 600), b"c7", &mut Vec::new());
    assert!(t.pending().is_empty());
    t.on_timer(&mut ctx(224, 900), ev(7, 900));
    assert!(t.inner().fired.is_empty(), "cancel wins");
}

#[test]
fn a_bare_state_machine_gets_every_frame_but_timed_filters() {
    let mut bare = Rec::default();
    bare.on_timer(&mut ApplyCtx::for_sm::<Rec>(1).with_time(1), ev(1, 1));
    bare.on_timer(&mut ApplyCtx::for_sm::<Rec>(2).with_time(1), ev(1, 1));
    assert_eq!(bare.fired.len(), 2, "at-least-once without the wrapper");
}

#[test]
fn table_ticks_deliver_strictly_increasing_deadlines_and_report_table_consumed() {
    let mut t = Timed::new(Rec::default());
    let mut c = ctx(64, 1_000);
    t.on_timer(&mut c, tev(5, 1_000));
    assert_eq!(t.inner().fired, vec![(64, 5, 1_000)]);
    let recs = c.take_sched_records_for_test(); // see note
    assert_eq!(recs.len(), 1);
    assert_eq!(
        (recs[0].op, recs[0].timer_id, recs[0].deadline_ns),
        (SchedOp::TableConsumed, 5, 1_000)
    );
    let mut c = ctx(128, 1_000);
    t.on_timer(&mut c, tev(5, 1_000)); // duplicate (a re-fire): dropped, still reported
    assert_eq!(t.inner().fired.len(), 1);
    assert_eq!(
        c.take_sched_records_for_test()[0].op,
        SchedOp::TableConsumed
    );
    let mut c = ctx(192, 900);
    t.on_timer(&mut c, tev(5, 900)); // an OLDER tick after a newer one: dropped
    assert_eq!(t.inner().fired.len(), 1);
    t.on_timer(&mut ctx(256, 2_000), tev(5, 2_000));
    assert_eq!(t.inner().fired.len(), 2);
    assert_eq!(t.table_delivered(), vec![(5, 2_000)]);
    // programmatic and table ids do not interfere
    t.apply(&mut ctx(300, 2_000), b"s5@3000", &mut Vec::new());
    assert_eq!(t.pending(), vec![(5, 3_000)]);
    assert_eq!(t.table_delivered(), vec![(5, 2_000)]);
}

// --- snapshot round trip ---

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct RecImage {
    fired: Vec<(u64, u64, u64)>,
    last: Option<u64>,
}

impl SnapshotStateMachine for Rec {
    type SnapshotHandle = Vec<u8>;

    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), SnapshotError> {
        let img = RecImage {
            fired: self.fired.clone(),
            last: self.last,
        };
        let blob = bincode::serde::encode_to_vec(&img, bincode::config::standard())
            .map_err(|e| SnapshotError::Codec(format!("rec encode: {e}")))?;
        Ok((blob, self.last.unwrap_or(0)))
    }

    fn stream_snapshot(
        handle: Self::SnapshotHandle,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), SnapshotError> {
        dst.write_all(&handle)?;
        Ok(())
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, SnapshotError> {
        let mut blob = Vec::new();
        std::io::Read::read_to_end(src, &mut blob)?;
        let (img, _): (RecImage, _) =
            bincode::serde::decode_from_slice(&blob, bincode::config::standard())
                .map_err(|e| SnapshotError::Codec(format!("rec decode: {e}")))?;
        self.fired = img.fired;
        self.last = img.last;
        Ok(position)
    }
}

#[test]
fn snapshot_round_trip_preserves_pending_and_delivery_decisions() {
    let mut t = Timed::new(Rec::default());
    t.apply(&mut ctx(64, 100), b"s7@500", &mut Vec::new());
    t.apply(&mut ctx(96, 100), b"s9@700", &mut Vec::new());
    // deliver id 7, leave id 9 pending
    t.on_timer(&mut ctx(128, 500), ev(7, 500));
    assert_eq!(t.pending(), vec![(9, 700)]);
    // a table tick too, so table_delivered() round-trips alongside pending
    t.on_timer(&mut ctx(144, 1_200), tev(11, 1_200));
    assert_eq!(t.table_delivered(), vec![(11, 1_200)]);

    let (handle, pos) = t.freeze().unwrap();
    let mut bytes = Vec::new();
    Timed::<Rec>::stream_snapshot(handle, &mut bytes).unwrap();

    let mut t2 = Timed::new(Rec::default());
    let installed = t2.install_snapshot(pos, &mut &bytes[..]).unwrap();
    assert_eq!(installed, pos);
    assert_eq!(t2.pending(), t.pending());
    assert_eq!(t2.last_applied(), Some(pos));
    assert_eq!(t2.table_delivered(), t.table_delivered());

    // the still-pending instance (9, 700) is delivered
    t2.on_timer(&mut ctx(160, 700), ev(9, 700));
    assert!(t2.inner().fired.contains(&(160, 9, 700)));

    // the already-consumed instance (7, 500) is dropped: no NEW fired entry
    // for it (the pre-snapshot delivery at position 128 is still in the
    // carried-over inner state — this checks no *second* delivery happened).
    let before = t2.inner().fired.len();
    t2.on_timer(&mut ctx(192, 500), ev(7, 500));
    assert_eq!(
        t2.inner().fired.len(),
        before,
        "consumed instance dropped after snapshot install"
    );
}
