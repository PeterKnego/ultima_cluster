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
//! `RegisterSm` implements `uc2_service::StateMachine` (behind the `v2` Cargo
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

// The optional snapshot capability (M6): what lets the L3 harness drive the
// REAL purge path. `SnapshotHandle = Vec<u8>` (bincode of `(value,
// last_applied)`). `install_snapshot` takes the target `position` (the artifact
// tag) and asserts the payload's recorded position matches (belt-and-suspenders
// against a mis-tagged artifact).
#[cfg(feature = "v2")]
impl uc2_service::SnapshotStateMachine for RegisterSm {
    type SnapshotHandle = Vec<u8>;

    fn freeze(&self) -> Result<(Vec<u8>, u64), uc2_service::SnapshotError> {
        let buf = bincode::serde::encode_to_vec(
            (self.value, self.last_applied),
            bincode::config::standard(),
        )
        .map_err(|e| uc2_service::SnapshotError::Codec(e.to_string()))?;
        Ok((buf, self.last_applied.unwrap_or(0)))
    }

    fn stream_snapshot(
        handle: Vec<u8>,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc2_service::SnapshotError> {
        std::io::Write::write_all(dst, &handle)?;
        Ok(())
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc2_service::SnapshotError> {
        let mut buf = Vec::new();
        std::io::Read::read_to_end(src, &mut buf)?;
        let ((v, la), _) = bincode::serde::decode_from_slice::<(Option<u64>, Option<u64>), _>(
            &buf,
            bincode::config::standard(),
        )
        .map_err(|e| uc2_service::SnapshotError::Codec(e.to_string()))?;
        // The payload's recorded position must match the artifact tag we were
        // asked to land at.
        if la.unwrap_or(0) != position {
            return Err(uc2_service::SnapshotError::Codec(format!(
                "snapshot payload position {} != requested {position}",
                la.unwrap_or(0)
            )));
        }
        self.value = v;
        self.last_applied = Some(position);
        Ok(position)
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

// The v2 impl exercised through its own trait surface. `position` is the
// idempotency key; `query` returns the current value.
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

    /// The M6 snapshot capability roundtrips through the v2 trait, keyed on the
    /// artifact position `S`.
    #[test]
    fn snapshot_roundtrip_via_v2_capability() {
        use uc2_service::SnapshotStateMachine;

        let mut sm = RegisterSm::default();
        sm.apply(4096, Cmd::Write(42));
        let (handle, s) = sm.freeze().unwrap();
        assert_eq!(s, 4096);
        let mut bytes = Vec::new();
        RegisterSm::stream_snapshot(handle, &mut bytes).unwrap();

        let mut restored = RegisterSm::default();
        assert_eq!(
            restored.install_snapshot(4096, &mut bytes.as_slice()).unwrap(),
            4096
        );
        assert_eq!(restored.query(()), Some(42));
        assert_eq!(restored.last_applied(), Some(4096));

        // A mis-tagged install (wrong artifact position) is refused.
        assert!(restored.install_snapshot(99, &mut bytes.as_slice()).is_err());
    }
}
