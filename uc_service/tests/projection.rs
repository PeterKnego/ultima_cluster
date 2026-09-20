// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `SnapshotStateMachine::project()` — the default named refusal, and the
//! `Tagged` forward (diff replay harness, task 4).

use uc_service::{
    ApplyCtx, RawStateMachine, SessionConfig, Sessioned, SnapshotError, SnapshotStateMachine,
    StateMachine, Tagged, Timed, TimerEvent,
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

/// A raw-tier SM that schedules a timer when its command asks (`s<id>@<at>`),
/// for exercising `Timed<S>::project`. Command format mirrors the `Rec` type
/// in `tests/timed.rs`.
#[derive(Default)]
struct Scheduling {
    last: Option<u64>,
}
impl RawStateMachine for Scheduling {
    const NAME: &'static str = "scheduling";
    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], _out: &mut Vec<u8>) {
        let s = std::str::from_utf8(cmd).unwrap();
        if let Some(rest) = s.strip_prefix('s') {
            let (id, at) = rest.split_once('@').unwrap();
            ctx.schedule(id.parse().unwrap(), at.parse().unwrap());
        }
        self.last = Some(ctx.position);
    }
    fn query(&self, _q: &[u8], _out: &mut Vec<u8>) {}
    fn last_applied(&self) -> Option<u64> {
        self.last
    }
}
impl SnapshotStateMachine for Scheduling {
    type SnapshotHandle = Option<u64>;
    fn freeze(&self) -> Result<(Option<u64>, u64), SnapshotError> {
        Ok((self.last, self.last.unwrap_or(0)))
    }
    fn stream_snapshot(h: Option<u64>, dst: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        dst.write_all(&h.unwrap_or(0).to_le_bytes())?;
        Ok(())
    }
    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, SnapshotError> {
        let mut b = [0u8; 8];
        src.read_exact(&mut b)?;
        self.last = Some(u64::from_le_bytes(b));
        Ok(position)
    }
    fn project(&self, out: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        writeln!(out, "last={:?}", self.last)?;
        Ok(())
    }
}

#[test]
fn timed_projection_lists_pending_timers_sorted() {
    let mut t = Timed::new(Scheduling::default());

    // Nothing pending: projects exactly the inner SM's line(s), no trailer.
    let mut out = Vec::new();
    t.project(&mut out).unwrap();
    assert_eq!(String::from_utf8(out).unwrap(), "last=None\n");

    // Schedule id 9 then id 3 (descending) — the projection must still come
    // out sorted by id, inner line first.
    t.apply(
        &mut ApplyCtx::for_sm::<Timed<Scheduling>>(64).with_time(10),
        b"s9@500",
        &mut Vec::new(),
    );
    t.apply(
        &mut ApplyCtx::for_sm::<Timed<Scheduling>>(96).with_time(10),
        b"s3@700",
        &mut Vec::new(),
    );

    let mut out = Vec::new();
    t.project(&mut out).unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "last=Some(96)\ntimer id=3 deadline_ns=700\ntimer id=9 deadline_ns=500\n"
    );
}

fn session_env(client: u64, seq: u64, cmd: u64) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(&client.to_le_bytes());
    v.extend_from_slice(&seq.to_le_bytes());
    bincode::serde::encode_into_std_write(cmd, &mut v, bincode::config::standard()).unwrap();
    v
}

#[test]
fn sessioned_projection_lists_sessions_sorted() {
    let mut s = Sessioned::new(Projecting::default(), SessionConfig::default());

    // Client 7 then client 2 (descending) — the projection must still come
    // out sorted by client id, inner line first.
    s.apply(
        &mut ApplyCtx::new(100, <Sessioned<Projecting> as RawStateMachine>::IDENTITY),
        &session_env(7, 1, 3),
        &mut Vec::new(),
    );
    s.apply(
        &mut ApplyCtx::new(200, <Sessioned<Projecting> as RawStateMachine>::IDENTITY),
        &session_env(2, 1, 4),
        &mut Vec::new(),
    );

    let mut out = Vec::new();
    s.project(&mut out).unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "total=7\nsession client=2 seq=Some(1)\nsession client=7 seq=Some(1)\n"
    );
}

#[test]
fn timer_event_new_is_public() {
    let ev = TimerEvent::new(7, 100, true);
    assert_eq!(ev.id, 7);
    assert_eq!(ev.deadline_ns, 100);
    assert!(ev.table);
}
