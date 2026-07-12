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
//! ## Two SDK targets, one model
//!
//! `RegisterSm` implements the state-machine trait of BOTH SDK generations, each
//! behind its own Cargo feature so the checker/history/model above stay a single
//! source of truth. `v1` (default) targets `uc_service::StateMachine` (with
//! snapshot in/out); `v2` targets `uc2_service::StateMachine` (no snapshot
//! methods — M5 reconstruction replays the log — and keys apply on the absolute
//! byte `position`, the v2 log-index analog).
//!
//! The `Cmd`/`CmdResp` command/response types and the in-memory value are shared
//! by both impls, so the WGL capstone's op vocabulary is identical across the
//! v1 `uc_node` and v2 `uc2_node` cluster ports.

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

// ------------------------------------------------------------------ v1 SDK

#[cfg(feature = "v1")]
mod v1 {
    use std::io::{Read as IoRead, Write as IoWrite};

    use uc_service::{SnapshotError, StateMachine};

    use super::{Cmd, CmdResp, RegisterSm};

    impl StateMachine for RegisterSm {
        type Command = Cmd;
        type Response = CmdResp;
        type Query = (); // Read
        type QueryResponse = Option<u64>;
        type SnapshotHandle = Vec<u8>;

        fn apply(&mut self, log_index: u64, cmd: Cmd) -> CmdResp {
            let resp = super::apply_cmd(&mut self.value, cmd);
            self.last_applied = Some(log_index);
            resp
        }
        fn query(&self, _q: ()) -> Option<u64> {
            self.value
        }
        fn last_applied(&self) -> Option<u64> {
            self.last_applied
        }
        fn freeze(&self) -> Result<(Vec<u8>, u64), SnapshotError> {
            let buf = bincode::serde::encode_to_vec(
                (self.value, self.last_applied),
                bincode::config::standard(),
            )
            .map_err(|e| SnapshotError::Codec(e.to_string()))?;
            Ok((buf, self.last_applied.unwrap_or(0)))
        }
        fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn IoWrite) -> Result<(), SnapshotError> {
            dst.write_all(&handle)?;
            Ok(())
        }
        fn install_snapshot(&mut self, src: &mut dyn IoRead) -> Result<u64, SnapshotError> {
            let mut buf = Vec::new();
            src.read_to_end(&mut buf)?;
            let ((v, la), _) = bincode::serde::decode_from_slice::<(Option<u64>, Option<u64>), _>(
                &buf,
                bincode::config::standard(),
            )
            .map_err(|e| SnapshotError::Codec(e.to_string()))?;
            self.value = v;
            self.last_applied = la;
            Ok(la.unwrap_or(0))
        }
    }
}

// ------------------------------------------------------------------ v2 SDK

// The v2 trait has no snapshot methods (M5 reconstruction replays the log) and
// keys `apply` on the absolute byte `position` (the v2 log-index analog). Full
// path on the trait keeps its name out of this module's namespace, so the v1
// glob-importing tests below never see two `apply`-bearing traits at once.
#[cfg(feature = "v2")]
impl uc2_service::StateMachine for RegisterSm {
    type Command = Cmd;
    type Response = CmdResp;
    type Query = (); // Read
    type QueryResponse = Option<u64>;

    fn apply(&mut self, position: u64, cmd: Cmd) -> CmdResp {
        let resp = apply_cmd(&mut self.value, cmd);
        self.last_applied = Some(position);
        resp
    }
    fn query(&self, _q: ()) -> Option<u64> {
        self.value
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
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

#[cfg(all(test, feature = "v1"))]
mod v1_tests {
    use super::*;
    use uc_service::StateMachine;

    #[test]
    fn write_then_cas_in_memory() {
        let mut sm = RegisterSm::default();
        assert_eq!(sm.apply(1, Cmd::Write(7)), CmdResp::WriteAck);
        assert_eq!(sm.apply(2, Cmd::Cas { old: 7, new: 9 }), CmdResp::CasResult(true));
        assert_eq!(sm.apply(3, Cmd::Cas { old: 7, new: 1 }), CmdResp::CasResult(false));
        assert_eq!(sm.query(()), Some(9));
        assert_eq!(sm.last_applied(), Some(3));
    }

    #[test]
    fn freeze_install_roundtrip() {
        let mut sm = RegisterSm::default();
        sm.apply(1, Cmd::Write(42));
        let (handle, idx) = sm.freeze().unwrap();
        assert_eq!(idx, 1);
        let mut bytes = Vec::new();
        RegisterSm::stream_snapshot(handle, &mut bytes).unwrap();
        let mut restored = RegisterSm::default();
        assert_eq!(restored.install_snapshot(&mut std::io::Cursor::new(bytes)).unwrap(), 1);
        assert_eq!(restored.query(()), Some(42));
        assert_eq!(restored.last_applied(), Some(1));
    }
}

// The v2 impl exercised through its own trait surface (no glob of `super`, so the
// v1 `StateMachine` name is not in scope even when both features are enabled —
// `sm.apply` resolves unambiguously to `uc2_service::StateMachine`). `position`
// is the idempotency key; `query` returns the current value.
#[cfg(all(test, feature = "v2"))]
mod v2_tests {
    use super::{Cmd, CmdResp, RegisterSm};
    use uc2_service::StateMachine;

    #[test]
    fn apply_query_roundtrip_via_v2_trait() {
        let mut sm = RegisterSm::default();
        // Fresh SM: nothing applied, empty value.
        assert_eq!(sm.last_applied(), None);
        assert_eq!(sm.query(()), None);
        // Write, then a matching CAS, keyed on ascending byte positions.
        assert_eq!(sm.apply(128, Cmd::Write(7)), CmdResp::WriteAck);
        assert_eq!(sm.apply(256, Cmd::Cas { old: 7, new: 9 }), CmdResp::CasResult(true));
        // A non-matching CAS is a no-op with a `false` result.
        assert_eq!(sm.apply(384, Cmd::Cas { old: 7, new: 1 }), CmdResp::CasResult(false));
        assert_eq!(sm.query(()), Some(9));
        assert_eq!(sm.last_applied(), Some(384));
    }
}
