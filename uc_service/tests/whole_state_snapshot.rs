// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! #67: the helper's blanket impl coexists with hand-written impls and with
//! the SDK's generic snapshot wrappers.

use uc_service::{
    ApplyCtx, RawStateMachine, SessionConfig, Sessioned, SnapshotError, SnapshotStateMachine,
    Timed, WholeStateSnapshot,
};

#[derive(Default)]
struct Helper {
    v: u64,
    last: Option<u64>,
}
impl RawStateMachine for Helper {
    const NAME: &'static str = "helper";
    fn apply(&mut self, ctx: &mut ApplyCtx, _c: &[u8], out: &mut Vec<u8>) {
        self.v += 1;
        self.last = Some(ctx.position);
        out.clear();
    }
    fn query(&self, _q: &[u8], out: &mut Vec<u8>) {
        out.clear();
    }
    fn last_applied(&self) -> Option<u64> {
        self.last
    }
}
impl WholeStateSnapshot for Helper {
    fn encode_state(&self) -> Result<Vec<u8>, SnapshotError> {
        let mut b = self.v.to_le_bytes().to_vec();
        b.push(self.last.is_some() as u8);
        b.extend_from_slice(&self.last.unwrap_or(0).to_le_bytes());
        Ok(b)
    }
    fn decode_state(&mut self, b: &[u8]) -> Result<(), SnapshotError> {
        self.v = u64::from_le_bytes(b[0..8].try_into().unwrap());
        self.last = (b[8] == 1).then(|| u64::from_le_bytes(b[9..17].try_into().unwrap()));
        Ok(())
    }
}
impl Helper {
    fn state(&self) -> (u64, Option<u64>) {
        (self.v, self.last)
    }
}

/// A hand-written impl in the same crate as a helper-based one (D4: opt-in).
#[derive(Default)]
struct Manual {
    last: Option<u64>,
}
impl RawStateMachine for Manual {
    const NAME: &'static str = "manual";
    fn apply(&mut self, ctx: &mut ApplyCtx, _c: &[u8], out: &mut Vec<u8>) {
        self.last = Some(ctx.position);
        out.clear();
    }
    fn query(&self, _q: &[u8], out: &mut Vec<u8>) {
        out.clear();
    }
    fn last_applied(&self) -> Option<u64> {
        self.last
    }
}
impl SnapshotStateMachine for Manual {
    type SnapshotHandle = ();
    fn freeze(&self) -> Result<((), u64), SnapshotError> {
        Ok(((), self.last.unwrap_or(0)))
    }
    fn stream_snapshot(_h: (), _d: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        Ok(())
    }
    fn install_snapshot(
        &mut self,
        p: u64,
        _s: &mut dyn std::io::Read,
    ) -> Result<u64, SnapshotError> {
        Ok(p)
    }
}

fn assert_snapshot_capable<S: SnapshotStateMachine>() {}

#[test]
fn both_kinds_are_snapshot_capable() {
    assert_snapshot_capable::<Helper>();
    assert_snapshot_capable::<Manual>();
}

#[test]
fn wrappers_compose_with_the_helper() {
    assert_snapshot_capable::<Timed<Helper>>();
    assert_snapshot_capable::<Sessioned<Helper>>();
    assert_snapshot_capable::<Timed<Manual>>();
}

/// Review Focus 3: `Sessioned<Helper>` (the wrapper composed with the
/// helper-based blanket impl) round-trips through freeze -> stream -> install
/// just like a hand-written `SnapshotStateMachine` does.
#[test]
fn sessioned_helper_round_trips_through_freeze_stream_install() {
    let mut s = Sessioned::new(Helper::default(), SessionConfig::default());
    let mut ctx = ApplyCtx::for_sm::<Sessioned<Helper>>(4096);
    let mut out = Vec::new();
    // A fresh client frame (16-byte session header + empty body) so the
    // inner `Helper` actually applies and its cursor moves.
    let mut cmd = vec![0u8; 16];
    cmd[0..8].copy_from_slice(&1u64.to_le_bytes()); // client_id = 1
    cmd[8..16].copy_from_slice(&0u64.to_le_bytes()); // seq = 0
    s.apply(&mut ctx, &cmd, &mut out);
    assert_eq!(s.last_applied(), Some(4096));

    let (handle, pos) = s.freeze().unwrap();
    assert_eq!(pos, 4096);
    let mut bytes = Vec::new();
    Sessioned::<Helper>::stream_snapshot(handle, &mut bytes).unwrap();

    let mut t = Sessioned::new(Helper::default(), SessionConfig::default());
    let got = t.install_snapshot(4160, &mut &bytes[..]).unwrap();
    assert_eq!(got, 4160);
    assert_eq!(t.last_applied(), s.last_applied());
    assert_eq!(t.inner().state(), s.inner().state());
}
