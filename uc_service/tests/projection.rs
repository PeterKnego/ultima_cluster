// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `SnapshotStateMachine::project()` — the default named refusal, and the
//! `Tagged` forward (diff replay harness, task 4).

use uc_service::{
    ApplyCtx, RawStateMachine, SnapshotError, SnapshotStateMachine, StateMachine, Tagged,
    TimerEvent,
};

#[derive(Default)]
struct Plain(u64);
impl StateMachine for Plain {
    const NAME: &'static str = "plain";
    type Command = u64;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;
    fn apply(&mut self, _c: &mut ApplyCtx, cmd: u64) -> u64 {
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
impl SnapshotStateMachine for Plain {
    type SnapshotHandle = u64;
    fn freeze(&self) -> Result<(u64, u64), SnapshotError> {
        Ok((self.0, 0))
    }
    fn stream_snapshot(h: u64, dst: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        dst.write_all(&h.to_le_bytes())?;
        Ok(())
    }
    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, SnapshotError> {
        let mut b = [0u8; 8];
        src.read_exact(&mut b)?;
        self.0 = u64::from_le_bytes(b);
        Ok(position)
    }
}

#[derive(Default)]
struct Projecting(Plain);
impl StateMachine for Projecting {
    const NAME: &'static str = "projecting";
    type Command = u64;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;
    fn apply(&mut self, c: &mut ApplyCtx, cmd: u64) -> u64 {
        StateMachine::apply(&mut self.0, c, cmd)
    }
    fn query(&self, q: ()) -> u64 {
        StateMachine::query(&self.0, q)
    }
    fn last_applied(&self) -> Option<u64> {
        None
    }
}
impl SnapshotStateMachine for Projecting {
    type SnapshotHandle = u64;
    fn freeze(&self) -> Result<(u64, u64), SnapshotError> {
        self.0.freeze()
    }
    fn stream_snapshot(h: u64, dst: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        Plain::stream_snapshot(h, dst)
    }
    fn install_snapshot(
        &mut self,
        p: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, SnapshotError> {
        self.0.install_snapshot(p, src)
    }
    fn project(&self, out: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        writeln!(out, "total={}", self.0.0)?;
        Ok(())
    }
}

#[test]
fn default_project_is_a_named_refusal() {
    let mut out = Vec::new();
    let err = Plain::default().project(&mut out).unwrap_err();
    assert!(
        err.to_string().contains("project() not implemented"),
        "{err}"
    );
}

#[test]
fn an_implemented_project_renders_and_tagged_forwards_it() {
    let mut sm = Tagged::<2, Projecting>::default();
    RawStateMachine::apply(
        &mut sm,
        &mut ApplyCtx::new(64, <Tagged<2, Projecting> as RawStateMachine>::IDENTITY),
        &bincode::serde::encode_to_vec(5u64, bincode::config::standard()).unwrap(),
        &mut Vec::new(),
    );
    let mut out = Vec::new();
    sm.project(&mut out).unwrap();
    assert_eq!(String::from_utf8(out).unwrap(), "total=5\n");
}

#[test]
fn timer_event_new_is_public() {
    let ev = TimerEvent::new(7, 100, true);
    assert_eq!(ev.id, 7);
    assert_eq!(ev.deadline_ns, 100);
    assert!(ev.table);
}
