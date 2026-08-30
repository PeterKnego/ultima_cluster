//! M14a: the FSM lag barrier as a TARGET CAP (spec §4.2, plan deviation 1).
//! `LogFollower::next_batch(target)` yields only frames whose END is
//! `<= target`, so "frame [p, p+len) may apply iff p + len - floor <= lag"
//! is exactly `target = min(head, floor + lag)`, and lockstep ("floor >= p")
//! is "apply one frame, only while cursor == floor". A capped batch simply
//! reads as `CaughtUp`; the agent's idle strategy is the wait.

use uc_log::cnc::CncPage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LagMode {
    /// The page declares no FSMs (a harness node): today's behaviour.
    Off,
    Lockstep,
    Bounded(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Plan {
    /// No frame may apply this cycle; count a wait episode and idle.
    Wait,
    /// Call `next_batch(target)`; if `one_frame`, stop after the first frame.
    Apply { target: u64, one_frame: bool },
}

pub(crate) fn mode_from_page(declared: u64, fsm_lag_bytes: u64) -> LagMode {
    match (declared, fsm_lag_bytes) {
        (0, _) => LagMode::Off,
        (_, 0) => LagMode::Lockstep,
        (_, b) => LagMode::Bounded(b),
    }
}

/// `min(slot.applied)` over the declared bits — N acquire loads.
pub(crate) fn floor(cnc: &CncPage, declared: u64) -> u64 {
    let mut f = u64::MAX;
    for id in 0..uc_protocol::v2::cnc::CNC_MAX_SERVICES {
        if declared & (1 << id) != 0 {
            f = f.min(cnc.service_slot(id).applied.load_acquire());
        }
    }
    f
}

pub(crate) fn plan(mode: LagMode, floor: u64, cursor: u64, commit: u64, durable: u64) -> Plan {
    let head = commit.min(durable);
    if cursor >= head {
        // Nothing new anyway — never report a wait for the log's own idleness.
        return Plan::Apply { target: head, one_frame: false };
    }
    match mode {
        LagMode::Off => Plan::Apply { target: head, one_frame: false },
        LagMode::Bounded(lag) => {
            let cap = floor.saturating_add(lag);
            if cap <= cursor {
                Plan::Wait
            } else {
                Plan::Apply { target: head.min(cap), one_frame: false }
            }
        }
        LagMode::Lockstep => {
            if cursor > floor {
                Plan::Wait
            } else {
                Plan::Apply { target: head, one_frame: true }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEAD: u64 = 10_000; // commit == durable == 10_000 unless stated

    #[test]
    fn off_is_todays_behaviour() {
        assert_eq!(plan(LagMode::Off, 0, 1_000, HEAD, HEAD), Plan::Apply { target: HEAD, one_frame: false });
        assert_eq!(plan(LagMode::Off, 0, HEAD, HEAD, HEAD), Plan::Apply { target: HEAD, one_frame: false });
    }

    #[test]
    fn head_is_min_commit_durable_in_every_mode() {
        assert_eq!(plan(LagMode::Off, 0, 0, 5_000, 7_000), Plan::Apply { target: 5_000, one_frame: false });
        assert_eq!(plan(LagMode::Bounded(1 << 20), 0, 0, 7_000, 5_000), Plan::Apply { target: 5_000, one_frame: false });
    }

    #[test]
    fn bounded_caps_the_target_at_floor_plus_lag() {
        // I am ahead of the floor by 1000 with a 4096 bound: 3096 more bytes may apply.
        assert_eq!(plan(LagMode::Bounded(4096), 2_000, 3_000, HEAD, HEAD), Plan::Apply { target: 6_096, one_frame: false });
        // Cap above head: head wins.
        assert_eq!(plan(LagMode::Bounded(1 << 20), 2_000, 3_000, HEAD, HEAD), Plan::Apply { target: HEAD, one_frame: false });
        // Exactly at the bound: nothing can fit → wait.
        assert_eq!(plan(LagMode::Bounded(4096), 2_000, 6_096, HEAD, HEAD), Plan::Wait);
        // I AM the floor: the bound is measured from me.
        assert_eq!(plan(LagMode::Bounded(4096), 3_000, 3_000, HEAD, HEAD), Plan::Apply { target: 7_096, one_frame: false });
        // Caught up with head: never Wait, always the plain CaughtUp path.
        assert_eq!(plan(LagMode::Bounded(4096), 2_000, HEAD, HEAD, HEAD), Plan::Apply { target: HEAD, one_frame: false });
    }

    #[test]
    fn lockstep_applies_one_frame_only_at_the_floor() {
        assert_eq!(plan(LagMode::Lockstep, 3_000, 3_000, HEAD, HEAD), Plan::Apply { target: HEAD, one_frame: true });
        assert_eq!(plan(LagMode::Lockstep, 3_000, 3_128, HEAD, HEAD), Plan::Wait);
        assert_eq!(plan(LagMode::Lockstep, 3_000, HEAD, HEAD, HEAD), Plan::Apply { target: HEAD, one_frame: false });
    }

    #[test]
    fn mode_from_page_table() {
        assert_eq!(mode_from_page(0, 0), LagMode::Off);
        assert_eq!(mode_from_page(0, 4096), LagMode::Off);
        assert_eq!(mode_from_page(0b1, 0), LagMode::Lockstep);
        assert_eq!(mode_from_page(0b11, 65_536), LagMode::Bounded(65_536));
    }

    #[test]
    fn floor_is_the_min_over_declared_slots() {
        let page = uc_log::cnc::CncPage::heap(&uc_log::cnc::CncMeta {
            node_id: 1, instance_id: 7, app_id: "t".into(), buffer_bytes: 1 << 20, max_payload: 256,
        });
        page.service_slot(0).applied.store_release(900);
        page.service_slot(1).applied.store_release(300);
        page.service_slot(2).applied.store_release(100); // undeclared
        assert_eq!(floor(&page, 0b11), 300);
        assert_eq!(floor(&page, 0b1), 900);
    }
}
