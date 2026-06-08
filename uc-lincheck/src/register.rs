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

use std::io::{Read as IoRead, Write as IoWrite};

use serde::{Deserialize, Serialize};
use uc_service::{SnapshotError, StateMachine};

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

impl StateMachine for RegisterSm {
    type Command = Cmd;
    type Response = CmdResp;
    type Query = (); // Read
    type QueryResponse = Option<u64>;
    type SnapshotHandle = Vec<u8>;

    fn apply(&mut self, log_index: u64, cmd: Cmd) -> CmdResp {
        let resp = match cmd {
            Cmd::Write(v) => {
                self.value = Some(v);
                CmdResp::WriteAck
            }
            Cmd::Cas { old, new } => {
                if self.value == Some(old) {
                    self.value = Some(new);
                    CmdResp::CasResult(true)
                } else {
                    CmdResp::CasResult(false)
                }
            }
        };
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

#[cfg(test)]
mod tests {
    use super::*;

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
