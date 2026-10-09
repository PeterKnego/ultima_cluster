// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Snapshot-lifecycle spec §5: the START RULE. At attach and in overrun
//! recovery, a row installs the start set its node published on the cnc
//! slot (`+264/+272`) when that moves the row forward, then replays only the
//! tail. A pinned row takes its pin path instead (rule 1); every other case
//! is today's behaviour (rule 3), and a set that cannot be read falls back
//! to it by name (rule 4).

use std::sync::atomic::{AtomicU64, Ordering};

use uc_log::cnc::{RowRead, ServiceSlot};

use crate::apply::InstallFn;
use crate::config::ServiceError;
use crate::snapshots::{SnapshotStore, verify_snapshot_envelope};
use crate::traits::RawStateMachine;

/// Spec §5 rule 1 and plan ruling P5: may this row take a start set at all?
/// Never when it attached under a pin (the pin path has priority). In
/// overrun recovery also never when the row's LIVE view now names a pin, or
/// a version record whose frame END is above `decided_to` (jumping over it
/// would skip the #33 exact stop), or the view is mid-publish. At attach the
/// caller passes the view it just read and its own `record_pos`.
pub(crate) fn start_set_permitted(
    attach_pin: Option<(u64, u32, u32)>,
    live: &RowRead,
    decided_to: u64,
) -> bool {
    if attach_pin.is_some() {
        return false;
    }
    matches!(live, RowRead::View { pin: None, record_pos, .. } if *record_pos <= decided_to)
}

/// Spec §5 rule 4 ("log once by name", controller ruling PF15): the last
/// start-set position each row logged a line about. Overrun recovery can run
/// the rule many times against the same unusable set; it says so once per
/// position. Process-wide on purpose — one service process owns one row, and
/// a later incarnation of the same row in the same process has nothing new
/// to say about the same set. `0` is never a start-set position (the
/// zero-first pair reads `0` as "none"), so it doubles as "nothing logged".
static LOGGED: [AtomicU64; 256] = [const { AtomicU64::new(0) }; 256];

/// `true` the first time `(row, pos)` is seen in a row — the caller logs.
fn first_log_for(row: u8, pos: u64) -> bool {
    LOGGED[row as usize].swap(pos, Ordering::Relaxed) != pos
}

/// Spec §5 rule 2: install the row's start set when it is strictly ahead of
/// `resume` (where the row would otherwise resume), at or below `frontier`
/// (`min(commit, durable)`), and built on this binary's LINE (plan ruling
/// P3). `Ok(Some(p))`: installed — resume the follower AT `p` (the tag is an
/// exclusive frontier). `Ok(None)`: today's behaviour. A missing or
/// unverifiable artifact is `Ok(None)` with one line naming why (rule 4); an
/// error from the state machine's own install is a fail-stop (plan ruling
/// P4) — it may have half-mutated the state.
pub(crate) fn install_start_set<S: RawStateMachine>(
    sm: &mut S,
    slot: &ServiceSlot,
    row: u8,
    resume: u64,
    frontier: u64,
    store: &SnapshotStore,
    install: &InstallFn<S>,
) -> Result<Option<u64>, ServiceError> {
    let Some((pos, version)) = slot.snapshot_pos.start_set() else {
        return Ok(None);
    };
    if pos <= resume || pos > frontier {
        return Ok(None);
    }
    if !uc_protocol::identity::same_line(version, S::VERSION) {
        if first_log_for(row, pos) {
            eprintln!(
                "uc_service: row {row} start set snap-{pos} was built by {version:#010x}, \
                 this binary is {:#010x}; replaying instead",
                S::VERSION
            );
        }
        return Ok(None);
    }
    let path = store.path_for(pos);
    let mut file = match std::fs::File::open(&path) {
        Ok(f) => f,
        Err(e) => {
            if first_log_for(row, pos) {
                eprintln!(
                    "uc_service: row {row} start set {} unreadable ({e}); replaying instead",
                    path.display()
                );
            }
            return Ok(None);
        }
    };
    if let Err(e) = verify_snapshot_envelope(&mut file, pos, Some(S::VERSION)) {
        if first_log_for(row, pos) {
            eprintln!(
                "uc_service: row {row} start set {} refused ({e}); replaying instead",
                path.display()
            );
        }
        return Ok(None);
    }
    let installed = (install)(sm, pos, &mut file)
        .map_err(|e| ServiceError::Replay(format!("start-set install at {pos}: {e}")))?;
    // The same two trait-contract checks the pinned install makes: the tag
    // is an EXCLUSIVE frontier, so a cursor left AT or above `pos` swallows
    // the frame starting at `pos`, and a cursor left at `None` would restart
    // the replay from genesis on top of the installed image.
    let cursor = sm.last_applied();
    if installed != pos || cursor.is_none() || cursor >= Some(pos) {
        return Err(ServiceError::Replay(format!(
            "start-set install at {pos} left the state machine at {cursor:?} \
             (returned {installed}); install_snapshot must land at the tag \
             with its cursor strictly below it"
        )));
    }
    // Always logged: a successful install moves `resume` past `pos`, so it
    // cannot repeat for one position — and it must not be swallowed by an
    // earlier fallback line about the same set (an artifact fetched since).
    eprintln!("uc_service: row {row} started from snap-{pos} (start set; resume was {resume})");
    Ok(Some(pos))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use uc_log::cnc::{CncMeta, CncPage};

    struct Sum {
        total: u64,
        last: Option<u64>,
    }
    impl crate::traits::RawStateMachine for Sum {
        const NAME: &'static str = "sum";
        fn apply(&mut self, ctx: &mut crate::ApplyCtx, cmd: &[u8], _out: &mut Vec<u8>) {
            self.total += cmd.len() as u64;
            self.last = Some(ctx.position);
        }
        fn query(&self, _q: &[u8], _out: &mut Vec<u8>) {}
        fn last_applied(&self) -> Option<u64> {
            self.last
        }
    }

    fn page() -> Arc<CncPage> {
        CncPage::heap(&CncMeta {
            node_id: 1,
            instance_id: 1,
            app_id: "start-set".into(),
            buffer_bytes: 1 << 20,
            max_payload: 256,
            services: [None; uc_protocol::v2::cnc::CNC_MAX_SERVICES],
        })
    }

    fn scratch() -> tempfile::TempDir {
        let base = std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        tempfile::tempdir_in(base).unwrap()
    }

    /// `total ‖ cursor` payload; `install` restores both (cursor strictly
    /// below the tag, the exclusive-frontier contract).
    fn install() -> InstallFn<Sum> {
        Box::new(|sm: &mut Sum, pos: u64, src: &mut dyn std::io::Read| {
            let mut b = [0u8; 16];
            src.read_exact(&mut b)?;
            sm.total = u64::from_le_bytes(b[..8].try_into().unwrap());
            sm.last = Some(u64::from_le_bytes(b[8..].try_into().unwrap()));
            Ok(pos)
        })
    }

    fn publish(store: &SnapshotStore, pos: u64, version: u32, total: u64) {
        store
            .publish(pos, version, |w| {
                w.write_all(&total.to_le_bytes())?;
                w.write_all(&(pos - 32).to_le_bytes())?;
                Ok(())
            })
            .unwrap();
    }

    fn fresh() -> Sum {
        Sum {
            total: 0,
            last: None,
        }
    }

    /// Spec §5 rule 2: ahead → install, resume at the set.
    #[test]
    fn a_start_set_ahead_of_resume_is_installed() {
        let (p, dir) = (page(), scratch());
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        publish(&store, 4096, 0, 77);
        let slot = p.service_slot(0);
        slot.snapshot_pos.store_start_set(4096, 0);
        let mut sm = fresh();
        assert_eq!(
            install_start_set(&mut sm, slot, 0, 0, 8192, &store, &install()).unwrap(),
            Some(4096)
        );
        assert_eq!((sm.total, sm.last), (77, Some(4064)));
    }

    /// Spec §11 "overrun recovery jumping forward": the overrun path passes
    /// `resume = max(state machine position, follower cursor)`. A start set
    /// ahead of both is installed (the row jumps forward); the set a row
    /// already installed at attach — the follower sits AT it while the state
    /// machine's own cursor is strictly below it — is never installed twice.
    #[test]
    fn overrun_recovery_jumps_forward_and_never_reinstalls_the_attach_set() {
        let (p, dir) = (page(), scratch());
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        publish(&store, 4096, 0, 77);
        publish(&store, 8192, 0, 99);
        let slot = p.service_slot(0);
        // Attached at 4096: SM cursor 4064, follower cursor 4096.
        slot.snapshot_pos.store_start_set(4096, 0);
        let mut sm = Sum {
            total: 77,
            last: Some(4064),
        };
        let (sm_cursor, follower_cursor) = (sm.last.unwrap(), 4096u64);
        let resume = sm_cursor.max(follower_cursor);
        assert_eq!(
            install_start_set(&mut sm, slot, 0, resume, 16384, &store, &install()).unwrap(),
            None
        );
        // A newer start set published since: the overrun jumps to it.
        slot.snapshot_pos.store_start_set(8192, 0);
        assert_eq!(
            install_start_set(&mut sm, slot, 0, resume, 16384, &store, &install()).unwrap(),
            Some(8192)
        );
        assert_eq!((sm.total, sm.last), (99, Some(8160)));
    }

    /// Spec §5: behind or equal → no install (strict `>`: never rewind).
    #[test]
    fn a_start_set_at_or_behind_resume_is_not_installed() {
        let (p, dir) = (page(), scratch());
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        publish(&store, 4096, 0, 77);
        let slot = p.service_slot(0);
        slot.snapshot_pos.store_start_set(4096, 0);
        for resume in [4096u64, 5000] {
            let mut sm = Sum {
                total: 5,
                last: Some(resume),
            };
            assert_eq!(
                install_start_set(&mut sm, slot, 0, resume, 8192, &store, &install()).unwrap(),
                None
            );
            assert_eq!(sm.total, 5, "untouched");
        }
    }

    /// Plan ruling P3: another LINE → replay; a patch build of the same line
    /// installs (the envelope check is by line too).
    #[test]
    fn a_start_set_built_by_another_line_is_not_installed() {
        use uc_protocol::identity::pack_version;
        let (p, dir) = (page(), scratch());
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        publish(&store, 4096, pack_version(1, 0, 0), 77);
        let slot = p.service_slot(0);
        slot.snapshot_pos
            .store_start_set(4096, pack_version(1, 0, 0));
        let mut sm = fresh();
        // `Sum::VERSION` is 0 — line 0.0, not 1.0.
        assert_eq!(
            install_start_set(&mut sm, slot, 0, 0, 8192, &store, &install()).unwrap(),
            None
        );
        assert_eq!(sm.total, 0);
    }

    /// Review focus 1 + spec §10 row 1: the artifact is gone (pruned between
    /// publish and install) → replay, by name, never an error.
    #[test]
    fn a_start_set_whose_artifact_is_missing_falls_back_to_replay() {
        let (p, dir) = (page(), scratch());
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        let slot = p.service_slot(0);
        slot.snapshot_pos.store_start_set(4096, 0);
        let mut sm = fresh();
        assert_eq!(
            install_start_set(&mut sm, slot, 0, 0, 8192, &store, &install()).unwrap(),
            None
        );
    }

    /// Review focus 1: a file at the name whose envelope does not verify (a
    /// torn or mis-tagged artifact) → replay; the state machine is untouched.
    #[test]
    fn a_start_set_whose_envelope_is_bad_falls_back_to_replay() {
        let (p, dir) = (page(), scratch());
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        std::fs::write(store.path_for(4096), b"not an envelope").unwrap();
        let slot = p.service_slot(0);
        slot.snapshot_pos.store_start_set(4096, 0);
        let mut sm = fresh();
        assert_eq!(
            install_start_set(&mut sm, slot, 0, 0, 8192, &store, &install()).unwrap(),
            None
        );
        assert_eq!((sm.total, sm.last), (0, None));
    }

    /// Spec §4.1(4) belt and braces: never above `min(commit, durable)`.
    #[test]
    fn a_start_set_above_the_frontier_is_not_installed() {
        let (p, dir) = (page(), scratch());
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        publish(&store, 4096, 0, 77);
        let slot = p.service_slot(0);
        slot.snapshot_pos.store_start_set(4096, 0);
        let mut sm = fresh();
        assert_eq!(
            install_start_set(&mut sm, slot, 0, 0, 4000, &store, &install()).unwrap(),
            None
        );
    }

    /// Plan ruling P4: the state machine's OWN install failing after the
    /// envelope verified is a fail-stop — it may be half-mutated.
    #[test]
    fn a_state_machine_install_error_is_a_fail_stop() {
        let (p, dir) = (page(), scratch());
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        publish(&store, 4096, 0, 77);
        let slot = p.service_slot(0);
        slot.snapshot_pos.store_start_set(4096, 0);
        let failing: InstallFn<Sum> =
            Box::new(|_, _, _| Err(std::io::Error::other("injected").into()));
        let mut sm = fresh();
        assert!(matches!(
            install_start_set(&mut sm, slot, 0, 0, 8192, &store, &failing),
            Err(ServiceError::Replay(_))
        ));
    }

    /// Review focus 2 + spec §5 rule 1: a pinned row never takes the start
    /// set, whatever the catalog's newest agreed set is.
    #[test]
    fn a_pinned_row_never_takes_the_start_set() {
        let unpinned = RowRead::View {
            pin: None,
            running: Some(0),
            record_pos: 100,
        };
        assert!(start_set_permitted(None, &unpinned, 100));
        assert!(
            !start_set_permitted(Some((4096, 0, 0)), &unpinned, 100),
            "pinned at attach"
        );
    }

    /// Plan ruling P5: in overrun recovery the jump is refused when the row's
    /// live view now carries a pin, a version record committed above what the
    /// walk has decided, or the view is mid-publish.
    #[test]
    fn the_overrun_jump_is_refused_on_a_pinned_row_or_after_a_new_version_record() {
        let pinned_now = RowRead::View {
            pin: Some((8192, 0, 0)),
            running: Some(0),
            record_pos: 100,
        };
        assert!(!start_set_permitted(None, &pinned_now, 100));
        let newer_record = RowRead::View {
            pin: None,
            running: Some(0),
            record_pos: 9000,
        };
        assert!(!start_set_permitted(None, &newer_record, 8000));
        assert!(!start_set_permitted(None, &RowRead::Contended, 8000));
    }

    /// PF15 / spec §5 rule 4: "log once" — the fallback line is written once
    /// per (row, position), not once per call (overrun recovery can call the
    /// rule many times against the same unusable set).
    #[test]
    fn the_fallback_line_is_logged_once_per_position() {
        // A row number no other test uses: the memory is process-wide.
        assert!(first_log_for(250, 4096));
        assert!(!first_log_for(250, 4096), "same position: silent");
        assert!(first_log_for(250, 8192), "a new position logs again");
        assert!(first_log_for(251, 8192), "per row");
    }
}
