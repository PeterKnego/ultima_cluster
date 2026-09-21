//! The replicated CAS-register state machine the lincheck capstone runs. Mirrors
//! the `Counter` test SM shape in m2/m3. `Read` is a Query; `Write`/`Cas` are
//! Commands.
//!
//! This SM is **plain in-memory** — it persists NOTHING. That is deliberate: it
//! is the proof object for service-state reconstruction. When the service crashes
//! and restarts, it comes back empty (value=None); the node reconstructs it from
//! the replicated log (mid-life reattach replay, or snapshot-install + tail replay
//! when the gap is below the purge boundary). The lincheck capstone exercises both
//! node-kill and service-crash faults against this non-persisting SM and asserts
//! linearizability — see docs/tasks/task14_service_state_reconstruction.md.
//!
//! ## The v2 SDK target
//!
//! `RegisterSm` implements `uc_service::StateMachine` (behind the `v2` Cargo
//! feature — now the only one; the v1 target was retired with the v1 stack) so
//! the checker/history/model above stay a single source of truth. The v2 trait
//! has no snapshot methods (M5 reconstruction replays the log) and keys apply on
//! the absolute byte `position`, the v2 log-index analog; the optional
//! `SnapshotStateMachine` capability (M6) drives the purge path.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Cmd {
    Write(u64),
    Cas { old: u64, new: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CmdResp {
    WriteAck,
    CasResult(bool),
}

#[derive(Default)]
pub struct RegisterSm {
    value: Option<u64>,
    last_applied: Option<u64>,
}

// ------------------------------------------------------------------ v2 SDK

// The v2 trait has no snapshot methods (M5 reconstruction replays the log) and
// keys `apply` on the absolute byte `position` (the v2 log-index analog). Full
// path on the trait keeps its name out of this module's namespace.
#[cfg(feature = "v2")]
impl uc_service::StateMachine for RegisterSm {
    const NAME: &'static str = "register";

    type Command = Cmd;
    type Response = CmdResp;
    type Query = (); // Read
    type QueryResponse = Option<u64>;

    fn apply(&mut self, ctx: &mut uc_service::ApplyCtx, cmd: Cmd) -> CmdResp {
        let resp = apply_cmd(&mut self.value, cmd);
        self.last_applied = Some(ctx.position);
        resp
    }
    fn query(&self, _q: ()) -> Option<u64> {
        self.value
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

// The optional snapshot capability (M6): what lets the L3 harness drive the
// REAL purge path. `SnapshotHandle = Vec<u8>` (bincode of `(value,
// last_applied)`). `install_snapshot` takes the target `position` (the artifact
// tag) and asserts the payload's recorded position does not EXCEED it
// (belt-and-suspenders against a mis-tagged artifact; the tag is an exclusive
// frontier since coordinated instants, so equality is no longer the rule).
#[cfg(feature = "v2")]
impl uc_service::SnapshotStateMachine for RegisterSm {
    type SnapshotHandle = Vec<u8>;

    fn freeze(&self) -> Result<(Vec<u8>, u64), uc_service::SnapshotError> {
        let buf = bincode::serde::encode_to_vec(
            (self.value, self.last_applied),
            bincode::config::standard(),
        )
        .map_err(|e| uc_service::SnapshotError::Codec(e.to_string()))?;
        Ok((buf, self.last_applied.unwrap_or(0)))
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
        let ((v, la), _) = bincode::serde::decode_from_slice::<(Option<u64>, Option<u64>), _>(
            &buf,
            bincode::config::standard(),
        )
        .map_err(|e| uc_service::SnapshotError::Codec(e.to_string()))?;
        // Coordinated-snapshot spec §5.2: the tag is the INSTANT **P** — the
        // frame-END of the `SNAPSHOT` frame, an EXCLUSIVE frontier: the image
        // covers every frame BELOW P, and a user frame usually starts exactly
        // AT P. So the payload's own position may be below the tag (never
        // above — that would be a genuinely mis-tagged artifact), and the
        // restored `last_applied` must be the cursor the artifact RECORDED,
        // not the tag: the framework's `pos > last_applied` idempotency guard
        // would otherwise swallow the frame at P.
        if la.unwrap_or(0) > position {
            return Err(uc_service::SnapshotError::Codec(format!(
                "snapshot payload position {} is above the artifact tag {position}",
                la.unwrap_or(0)
            )));
        }
        self.value = v;
        self.last_applied = la;
        Ok(position)
    }

    fn project(&self, out: &mut dyn std::io::Write) -> Result<(), uc_service::SnapshotError> {
        writeln!(out, "value={:?}", self.value)?;
        writeln!(out, "last_applied={:?}", self.last_applied)?;
        Ok(())
    }
}

// ------------------------------------------------------------ diff-replay fixture

/// A diff-replay test fixture (`uc_diffreplay`'s own end-to-end proof, spec
/// §2.3's counterfactual), **not a pattern for a user state machine** — see
/// `examples/kv` for that. Same `NAME` as [`RegisterSm`] (the two builds must
/// name the same FSM row for the harness to compare them) but `VERSION = 2`
/// and changed semantics: `Write(v)` stores `2·v` instead of `v`, giving the
/// harness a build whose `apply` genuinely differs from `RegisterSm` for the
/// same recorded command.
#[cfg(feature = "v2")]
#[derive(Default)]
pub struct DoublingRegisterSm(pub RegisterSm);

#[cfg(feature = "v2")]
impl uc_service::StateMachine for DoublingRegisterSm {
    const NAME: &'static str = <RegisterSm as uc_service::StateMachine>::NAME;
    const VERSION: u32 = 2;

    type Command = Cmd;
    type Response = CmdResp;
    type Query = ();
    type QueryResponse = Option<u64>;

    fn apply(&mut self, ctx: &mut uc_service::ApplyCtx, cmd: Cmd) -> CmdResp {
        let cmd = match cmd {
            Cmd::Write(v) => Cmd::Write(v * 2),
            other => other,
        };
        self.0.apply(ctx, cmd)
    }
    fn query(&self, q: ()) -> Option<u64> {
        self.0.query(q)
    }
    fn last_applied(&self) -> Option<u64> {
        self.0.last_applied()
    }
}

#[cfg(feature = "v2")]
impl uc_service::SnapshotStateMachine for DoublingRegisterSm {
    type SnapshotHandle = <RegisterSm as uc_service::SnapshotStateMachine>::SnapshotHandle;

    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), uc_service::SnapshotError> {
        self.0.freeze()
    }
    fn stream_snapshot(
        h: Self::SnapshotHandle,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc_service::SnapshotError> {
        RegisterSm::stream_snapshot(h, dst)
    }
    fn install_snapshot(
        &mut self,
        p: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc_service::SnapshotError> {
        self.0.install_snapshot(p, src)
    }
    fn project(&self, out: &mut dyn std::io::Write) -> Result<(), uc_service::SnapshotError> {
        self.0.project(out)
    }
}

/// The pure CAS-register transition shared by both SDK `apply` impls (the only
/// difference between v1/v2 is the index name and the trait surface, never the
/// business logic — keeping it in one place is what makes the model a single
/// source of truth across the ports).
fn apply_cmd(value: &mut Option<u64>, cmd: Cmd) -> CmdResp {
    match cmd {
        Cmd::Write(v) => {
            *value = Some(v);
            CmdResp::WriteAck
        }
        Cmd::Cas { old, new } => {
            if *value == Some(old) {
                *value = Some(new);
                CmdResp::CasResult(true)
            } else {
                CmdResp::CasResult(false)
            }
        }
    }
}

// ------------------------------------------------------------ durable fixture

/// A diff-replay / pin-verify **test fixture** — the "durable state machine"
/// shape spec §2.3 says decided Q2: one whose `last_applied()` is non-`None`
/// on attach because it persisted. Wraps [`RegisterSm`] or
/// [`DoublingRegisterSm`] and writes their snapshot image (`(value,
/// last_applied)`) to `<instance_dir>/register.state` after every `apply`
/// and every `install_snapshot`, restoring it at [`Durable::open`]. The
/// write is `rename`-atomic so a killed process leaves the previous image,
/// never a torn one. **Not a pattern for a user state machine**: it does
/// file I/O inside `apply`, which is only acceptable because the I/O IS the
/// durability under test and it is deterministic (same inputs, same file).
#[cfg(feature = "v2")]
pub struct Durable<S> {
    inner: S,
    path: std::path::PathBuf,
}

#[cfg(feature = "v2")]
impl<S> Durable<S>
where
    S: uc_service::SnapshotStateMachine<SnapshotHandle = Vec<u8>>,
{
    pub const STATE_FILE: &'static str = "register.state";

    /// Wrap `inner`, restoring the persisted image if `<instance_dir>/register.state`
    /// exists. The restore goes through `inner.install_snapshot(la, image)` with
    /// `la` = the image's own recorded cursor, which the register's install
    /// accepts (payload position ≤ tag).
    pub fn open(mut inner: S, instance_dir: &std::path::Path) -> std::io::Result<Durable<S>> {
        let path = instance_dir.join(Self::STATE_FILE);
        if path.is_file() {
            let image = std::fs::read(&path)?;
            let ((_, la), _) = bincode::serde::decode_from_slice::<(Option<u64>, Option<u64>), _>(
                &image,
                bincode::config::standard(),
            )
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            inner
                .install_snapshot(la.unwrap_or(0), &mut &image[..])
                .map_err(|e| std::io::Error::other(e.to_string()))?;
        }
        Ok(Durable { inner, path })
    }

    fn persist(&self) {
        // Failure here is a fixture defect, not a state-machine outcome:
        // panic, so the apply thread fail-stops and the test names it.
        let (image, _) = self.inner.freeze().expect("durable fixture: freeze");
        let tmp = self.path.with_extension("state.tmp");
        std::fs::write(&tmp, &image).expect("durable fixture: write");
        std::fs::rename(&tmp, &self.path).expect("durable fixture: rename");
    }
}

#[cfg(feature = "v2")]
impl<S> uc_service::StateMachine for Durable<S>
where
    S: uc_service::StateMachine + uc_service::SnapshotStateMachine<SnapshotHandle = Vec<u8>>,
{
    const NAME: &'static str = <S as uc_service::StateMachine>::NAME;
    const VERSION: u32 = <S as uc_service::StateMachine>::VERSION;
    type Command = S::Command;
    type Response = S::Response;
    type Query = S::Query;
    type QueryResponse = S::QueryResponse;

    fn apply(&mut self, ctx: &mut uc_service::ApplyCtx, cmd: S::Command) -> S::Response {
        let r = uc_service::StateMachine::apply(&mut self.inner, ctx, cmd);
        self.persist();
        r
    }
    fn query(&self, q: S::Query) -> S::QueryResponse {
        uc_service::StateMachine::query(&self.inner, q)
    }
    fn last_applied(&self) -> Option<u64> {
        uc_service::StateMachine::last_applied(&self.inner)
    }
    // `on_timer` is a provided method on `StateMachine` (default: ignore).
    // A timer can change state (it is delivered through the same `apply`
    // discipline — spec §4.7), so the wrapper is not transparent unless this
    // is forwarded and persisted exactly like `apply` above.
    fn on_timer(&mut self, ctx: &mut uc_service::ApplyCtx, ev: uc_service::TimerEvent) {
        uc_service::StateMachine::on_timer(&mut self.inner, ctx, ev);
        self.persist();
    }
}

#[cfg(feature = "v2")]
impl<S> uc_service::SnapshotStateMachine for Durable<S>
where
    S: uc_service::StateMachine + uc_service::SnapshotStateMachine<SnapshotHandle = Vec<u8>>,
{
    type SnapshotHandle = Vec<u8>;

    fn freeze(&self) -> Result<(Vec<u8>, u64), uc_service::SnapshotError> {
        self.inner.freeze()
    }
    fn stream_snapshot(
        handle: Vec<u8>,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc_service::SnapshotError> {
        S::stream_snapshot(handle, dst)
    }
    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc_service::SnapshotError> {
        let got = self.inner.install_snapshot(position, src)?;
        self.persist();
        Ok(got)
    }
    fn project(&self, out: &mut dyn std::io::Write) -> Result<(), uc_service::SnapshotError> {
        self.inner.project(out)
    }
}

// The v2 impl exercised through its own trait surface. `position` is the
// idempotency key; `query` returns the current value.
#[cfg(all(test, feature = "v2"))]
mod v2_tests {
    use super::{Cmd, CmdResp, RegisterSm};
    use uc_service::{ApplyCtx, StateMachine};

    #[test]
    fn apply_query_roundtrip_via_v2_trait() {
        let mut sm = RegisterSm::default();
        // Fresh SM: nothing applied, empty value.
        assert_eq!(sm.last_applied(), None);
        assert_eq!(sm.query(()), None);
        // Write, then a matching CAS, keyed on ascending byte positions.
        assert_eq!(
            sm.apply(&mut ApplyCtx::for_sm::<RegisterSm>(128), Cmd::Write(7)),
            CmdResp::WriteAck
        );
        assert_eq!(
            sm.apply(
                &mut ApplyCtx::for_sm::<RegisterSm>(256),
                Cmd::Cas { old: 7, new: 9 }
            ),
            CmdResp::CasResult(true)
        );
        // A non-matching CAS is a no-op with a `false` result.
        assert_eq!(
            sm.apply(
                &mut ApplyCtx::for_sm::<RegisterSm>(384),
                Cmd::Cas { old: 7, new: 1 }
            ),
            CmdResp::CasResult(false)
        );
        assert_eq!(sm.query(()), Some(9));
        assert_eq!(sm.last_applied(), Some(384));
    }

    /// The M6 snapshot capability roundtrips through the v2 trait, keyed on the
    /// artifact position `S`.
    #[test]
    fn snapshot_roundtrip_via_v2_capability() {
        use uc_service::SnapshotStateMachine;

        let mut sm = RegisterSm::default();
        sm.apply(&mut ApplyCtx::for_sm::<RegisterSm>(4096), Cmd::Write(42));
        let (handle, s) = sm.freeze().unwrap();
        assert_eq!(s, 4096);
        let mut bytes = Vec::new();
        RegisterSm::stream_snapshot(handle, &mut bytes).unwrap();

        let mut restored = RegisterSm::default();
        assert_eq!(
            restored
                .install_snapshot(4096, &mut bytes.as_slice())
                .unwrap(),
            4096
        );
        assert_eq!(restored.query(()), Some(42));
        assert_eq!(restored.last_applied(), Some(4096));

        // A mis-tagged install (wrong artifact position) is refused.
        assert!(
            restored
                .install_snapshot(99, &mut bytes.as_slice())
                .is_err()
        );
    }
}

#[cfg(all(test, feature = "v2"))]
mod durable_tests {
    use super::*;
    use uc_service::{SnapshotStateMachine, StateMachine};

    fn ctx(pos: u64) -> uc_service::ApplyCtx {
        uc_service::ApplyCtx::new(pos, <RegisterSm as uc_service::RawStateMachine>::IDENTITY)
    }

    /// Real disk under the cargo target tree (CLAUDE.md's scratch rule),
    /// never `/tmp`. `CARGO_TARGET_TMPDIR` is only defined for integration
    /// tests/benches, not a unit test compiled into `--lib` (see
    /// `uc_service/src/attach.rs`'s identical `scratch()`), so a lib-level
    /// test derives real disk from its own test binary's location instead.
    fn scratch_base() -> std::path::PathBuf {
        std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf()
    }

    #[test]
    fn a_durable_register_restores_value_and_last_applied_from_its_file() {
        let dir = tempfile::Builder::new()
            .prefix("durable-")
            .tempdir_in(scratch_base())
            .unwrap();
        {
            let mut d = Durable::open(RegisterSm::default(), dir.path()).unwrap();
            assert_eq!(
                StateMachine::last_applied(&d),
                None,
                "fresh: nothing persisted yet"
            );
            let _ = StateMachine::apply(&mut d, &mut ctx(64), Cmd::Write(7));
            let _ = StateMachine::apply(&mut d, &mut ctx(128), Cmd::Write(9));
        }
        let d = Durable::open(RegisterSm::default(), dir.path()).unwrap();
        assert_eq!(StateMachine::query(&d, ()), Some(9));
        assert_eq!(StateMachine::last_applied(&d), Some(128));
        assert!(dir.path().join(Durable::<RegisterSm>::STATE_FILE).is_file());
    }

    #[test]
    fn a_durable_register_persists_an_install_too() {
        let dir = tempfile::Builder::new()
            .prefix("durable-")
            .tempdir_in(scratch_base())
            .unwrap();
        let (image, _) = {
            let mut src = RegisterSm::default();
            let _ = StateMachine::apply(&mut src, &mut ctx(32), Cmd::Write(5));
            SnapshotStateMachine::freeze(&src).unwrap()
        };
        {
            let mut d = Durable::open(RegisterSm::default(), dir.path()).unwrap();
            let got = SnapshotStateMachine::install_snapshot(&mut d, 64, &mut &image[..]).unwrap();
            assert_eq!(got, 64);
        }
        let d = Durable::open(RegisterSm::default(), dir.path()).unwrap();
        assert_eq!(StateMachine::query(&d, ()), Some(5));
        assert_eq!(
            StateMachine::last_applied(&d),
            Some(32),
            "the image's recorded cursor, not the tag"
        );
    }
}
