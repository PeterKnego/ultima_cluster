//! The replicated CAS-register state machine the cluster runs. Mirrors the
//! `Counter` test SM shape in m2/m3. `Read` is a Query; `Write`/`Cas` are
//! Commands.

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

    fn apply(&mut self, log_index: u64, cmd: Cmd) -> CmdResp {
        self.last_applied = Some(log_index);
        match cmd {
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
        }
    }
    fn query(&self, _q: ()) -> Option<u64> {
        self.value
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
    fn build_snapshot(&self, dst: &mut dyn IoWrite) -> Result<u64, SnapshotError> {
        let bytes = bincode::serde::encode_to_vec(
            (self.value, self.last_applied),
            bincode::config::standard(),
        )
        .map_err(|e| SnapshotError::Codec(e.to_string()))?;
        dst.write_all(&bytes)?;
        Ok(self.last_applied.unwrap_or(0))
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
