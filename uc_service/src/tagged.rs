// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `Tagged<ROW, S>`: run one state-machine type at several rows (harnesses
//! only — spec §3.3 "one type, one row"). Zero-cost forwarding newtype whose
//! `NAME` is `fsm<ROW>`; `ServicesConfig::tagged(n)` declares the rows.

use crate::config::SnapshotError;
use crate::{ApplyCtx, SnapshotStateMachine, StateMachine, TimerEvent};

pub const TAGGED_NAMES: [&str; 8] = [
    "fsm0", "fsm1", "fsm2", "fsm3", "fsm4", "fsm5", "fsm6", "fsm7",
];

#[derive(Default)]
pub struct Tagged<const ROW: u8, S>(pub S);

impl<const ROW: u8, S: StateMachine> StateMachine for Tagged<ROW, S> {
    const NAME: &'static str = TAGGED_NAMES[ROW as usize];
    const VERSION: u32 = S::VERSION;
    type Command = S::Command;
    type Response = S::Response;
    type Query = S::Query;
    type QueryResponse = S::QueryResponse;
    #[inline]
    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: S::Command) -> S::Response {
        self.0.apply(ctx, cmd)
    }
    #[inline]
    fn query(&self, q: S::Query) -> S::QueryResponse {
        self.0.query(q)
    }
    #[inline]
    fn last_applied(&self) -> Option<u64> {
        self.0.last_applied()
    }
    #[inline]
    fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {
        self.0.on_timer(ctx, ev)
    }
}

impl<const ROW: u8, S: StateMachine + SnapshotStateMachine> SnapshotStateMachine
    for Tagged<ROW, S>
{
    type SnapshotHandle = S::SnapshotHandle;
    fn freeze(&self) -> Result<(S::SnapshotHandle, u64), SnapshotError> {
        self.0.freeze()
    }
    fn stream_snapshot(
        h: S::SnapshotHandle,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), SnapshotError> {
        S::stream_snapshot(h, dst)
    }
    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, SnapshotError> {
        self.0.install_snapshot(position, src)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RawStateMachine;
    #[derive(Default)]
    struct Inner(u64);
    impl crate::StateMachine for Inner {
        const NAME: &'static str = "inner";
        const VERSION: u32 = 7;
        type Command = u64;
        type Response = u64;
        type Query = ();
        type QueryResponse = u64;
        fn apply(&mut self, _c: &mut crate::ApplyCtx, cmd: u64) -> u64 {
            self.0 += cmd;
            self.0
        }
        fn query(&self, _q: ()) -> u64 {
            self.0
        }
        fn last_applied(&self) -> Option<u64> {
            None
        }
    }
    #[test]
    fn tagged_renames_and_forwards_version_and_logic() {
        assert_eq!(<Tagged<3, Inner> as RawStateMachine>::NAME, "fsm3");
        assert_eq!(<Tagged<3, Inner> as RawStateMachine>::VERSION, 7);
        let mut t = Tagged::<3, Inner>::default();
        assert_eq!(
            crate::StateMachine::apply(
                &mut t,
                &mut crate::ApplyCtx::new(1, <Tagged<3, Inner> as RawStateMachine>::IDENTITY),
                5
            ),
            5
        );
    }
}
