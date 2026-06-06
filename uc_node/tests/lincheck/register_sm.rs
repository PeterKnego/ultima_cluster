//! The replicated CAS-register state machine the cluster runs. Mirrors the
//! `Counter` test SM shape in m2/m3. `Read` is a Query; `Write`/`Cas` are
//! Commands.
//!
//! This SM is **self-persisting**: it writes `(value, last_applied)` durably to
//! its `data_dir` on every apply (fsync before returning, so the framework only
//! ever sees a response for state that is already durable). On startup it
//! reloads that state. This mirrors the production `StoreStateMachine` contract
//! (which persists to `ultima_db`) and is what lets a *service-only* restart
//! recover correctly: uc does not replay history into a reconnecting service, so
//! a purely in-memory SM would come back empty. See
//! docs/tasks/task12_linearizability_harness.md (Finding #2).
//!
//! A `RegisterSm` built via [`RegisterSm::default`] (used for the degenerate
//! node-side SM in shmem mode, which never applies app data) has no `data_dir`
//! and does not persist. Only the service-side SM, built via
//! [`RegisterSm::new`], persists.

use std::io::{Read as IoRead, Write as IoWrite};
use std::path::PathBuf;

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
    /// `Some` for the service-side SM (persists here); `None` for the degenerate
    /// node-side `default()` SM (never persists).
    data_dir: Option<PathBuf>,
}

const STATE_FILE: &str = "register_sm.state";

impl RegisterSm {
    /// Build a persisting SM rooted at `data_dir`, reloading any prior state.
    pub fn new(data_dir: PathBuf) -> Self {
        let (value, last_applied) = match std::fs::read(data_dir.join(STATE_FILE)) {
            Ok(bytes) => {
                let ((v, la), _) =
                    bincode::serde::decode_from_slice::<(Option<u64>, Option<u64>), _>(
                        &bytes,
                        bincode::config::standard(),
                    )
                    .expect("decode persisted register state");
                (v, la)
            }
            Err(_) => (None, None), // no prior state
        };
        Self {
            value,
            last_applied,
            data_dir: Some(data_dir),
        }
    }

    /// Durably write `(value, last_applied)`: write a temp file, fsync, rename.
    /// No-op when there is no `data_dir` (degenerate node-side SM).
    fn persist(&self) {
        let Some(dir) = &self.data_dir else {
            return;
        };
        let bytes = bincode::serde::encode_to_vec(
            (self.value, self.last_applied),
            bincode::config::standard(),
        )
        .expect("encode register state");
        let tmp = dir.join("register_sm.state.tmp");
        let final_path = dir.join(STATE_FILE);
        {
            let mut f = std::fs::File::create(&tmp).expect("create state tmp");
            f.write_all(&bytes).expect("write state");
            f.sync_all().expect("fsync state"); // durable before the rename
        }
        std::fs::rename(&tmp, &final_path).expect("rename state into place");
    }
}

impl StateMachine for RegisterSm {
    type Command = Cmd;
    type Response = CmdResp;
    type Query = (); // Read
    type QueryResponse = Option<u64>;

    fn apply(&mut self, log_index: u64, cmd: Cmd) -> CmdResp {
        // Idempotency guard: on recovery the framework re-applies already-applied
        // entries (a node restart replays the committed log from its durable
        // last_applied, which lags). We've persisted those already; re-applying a
        // CAS against advanced state would corrupt it. Skip them. The response is
        // discarded — no client waits on a replayed entry during recovery.
        if let Some(la) = self.last_applied
            && log_index <= la
        {
            return match cmd {
                Cmd::Write(_) => CmdResp::WriteAck,
                Cmd::Cas { .. } => CmdResp::CasResult(false),
            };
        }
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
        self.persist(); // durable before we return (i.e. before the ack)
        resp
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
        self.persist(); // keep the durable file consistent with installed state
        Ok(la.unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn persists_and_reloads_state() {
        let dir = TempDir::new().unwrap();
        {
            let mut sm = RegisterSm::new(dir.path().to_owned());
            assert_eq!(sm.apply(1, Cmd::Write(7)), CmdResp::WriteAck);
            assert_eq!(
                sm.apply(2, Cmd::Cas { old: 7, new: 9 }),
                CmdResp::CasResult(true)
            );
            assert_eq!(
                sm.apply(3, Cmd::Cas { old: 7, new: 1 }),
                CmdResp::CasResult(false)
            );
        }
        // A fresh SM over the same dir reloads value=9, last_applied=3.
        let reloaded = RegisterSm::new(dir.path().to_owned());
        assert_eq!(reloaded.query(()), Some(9));
        assert_eq!(reloaded.last_applied(), Some(3));
    }

    #[test]
    fn replay_below_last_applied_is_skipped() {
        let dir = TempDir::new().unwrap();
        let mut sm = RegisterSm::new(dir.path().to_owned());
        sm.apply(1, Cmd::Write(5));
        sm.apply(2, Cmd::Write(8)); // value=8, last_applied=2
        // Replaying index 1 (a Write) must NOT clobber the newer value 8...
        sm.apply(1, Cmd::Write(5));
        assert_eq!(sm.query(()), Some(8));
        // ...nor must a replayed CAS mutate, even if its `old` matches the current
        // value (it was already accounted for at its real log position).
        sm.apply(2, Cmd::Cas { old: 8, new: 100 });
        assert_eq!(sm.query(()), Some(8));
        assert_eq!(sm.last_applied(), Some(2));
        // A genuinely new index applies normally.
        assert_eq!(
            sm.apply(3, Cmd::Cas { old: 8, new: 100 }),
            CmdResp::CasResult(true)
        );
        assert_eq!(sm.query(()), Some(100));
    }

    #[test]
    fn default_sm_does_not_persist() {
        // The degenerate node-side SM has no data_dir; apply must not panic.
        let mut sm = RegisterSm::default();
        assert_eq!(sm.apply(1, Cmd::Write(3)), CmdResp::WriteAck);
        assert_eq!(sm.query(()), Some(3));
    }
}
