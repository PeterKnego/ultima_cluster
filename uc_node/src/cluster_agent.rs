// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The `uc2-cluster` agent (cluster-FSM spec §4.1): the cluster FSM's own
//! apply loop, in-node, no cnc slot, outside the lag policy. It walks the
//! node's `LogBuffer` with a `LogFollower`, acts on `CLUSTER` frames only,
//! and publishes [`ClusterView`] after every batch that applied something.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};

use uc_journal::Journal;
use uc_log::archive::replay_journal_from;
use uc_log::buffer::LogBuffer;
use uc_log::cnc::CncPage;
use uc_log::reader::{Batch, LogFollower};
use uc_protocol::v2::cnc::CNC_MAX_SERVICES;
use uc_protocol::v2::frame::{FRAME_TYPE_CLUSTER, align_frame_len};
use uc_protocol::v2::schedule::ScheduleTable;
use uc_service::{ApplyCtx, RawStateMachine, SnapshotStateMachine};

use crate::cluster_fsm::{ClusterFsm, ClusterState, ClusterView};

pub fn artifact_path(dir: &Path, position: u64) -> PathBuf {
    dir.join(format!("snap-{position}.ultcluster"))
}

/// Where the cluster artifacts live under an instance directory — the same
/// path [`crate::ipc::InstanceDir::cluster_snapshot_dir`] builds (pinned equal
/// by that module's own test), for a reader that must NOT take the instance
/// flock an `InstanceDir` holds.
pub fn snapshot_dir_of(instance_dir: &Path) -> PathBuf {
    instance_dir.join("snapshots").join("cluster")
}

/// `uc2ctl schedule show`/`status`'s reader: the schedule table this instance
/// directory's newest CLUSTER ARTIFACT holds, as `(table_position, table)`.
/// `(0, no entries)` when there is no artifact yet.
///
/// **It reads the artifact, not the live view** — a plain file read beside a
/// running node, taking no lock. The consequence is that it lags: the artifact
/// is written by the `uc2-cluster` agent's bridging trigger, which fires once
/// every declared row has snapshotted, so on a node whose rows have not
/// snapshotted yet this answers `(0, [])` even though the live view holds a
/// table. Plan 1 task 8 does NOT give `uc2ctl` a live, in-process reading
/// (spec §13 phase 2 is what would) — this stays the honest offline one, and
/// `schedule show`/`status` print `(0, [])`/`0` as "no cluster artifact yet"
/// rather than implying an empty table was adopted.
pub fn read_committed_table(instance_dir: &Path) -> io::Result<(u64, ScheduleTable)> {
    let genesis = ClusterState::genesis_empty();
    // No declared hashes: this reader never APPLIES a command, and
    // `install_snapshot` does not consult them.
    let (fsm, _) = recover(&snapshot_dir_of(instance_dir), genesis, Vec::new())?;
    let st = fsm.state();
    Ok((st.table_position, st.table.clone()))
}

/// `uc2ctl settings show`'s reader (plan 1 task 8): the settings this
/// instance directory's newest CLUSTER ARTIFACT holds, as `Some((position,
/// settings))` — `position` is [`ClusterState::applied`], "the view's
/// position tag and the artifact's position" (there is no separate
/// per-settings position field the way `table_position` tracks the schedule
/// table specifically). `None` when there is no artifact yet — the SAME
/// staleness caveat as [`read_committed_table`] applies: this reads the
/// artifact, not the live view, so it lags a freshly-applied settings record
/// until every declared row has snapshotted.
pub fn read_committed_settings(
    instance_dir: &Path,
) -> io::Result<Option<(u64, uc_protocol::v2::settings::Settings)>> {
    let genesis = ClusterState::genesis_empty();
    // No declared hashes: this reader never APPLIES a command, and
    // `install_snapshot` does not consult them.
    let (fsm, start) = recover(&snapshot_dir_of(instance_dir), genesis, Vec::new())?;
    if start == 0 {
        // No artifact under `dir` (`recover` returns position 0 for
        // genesis) — a real committed CLUSTER frame never lands at position
        // 0, so this is an unambiguous "nothing yet" rather than a
        // legitimate reading colliding with the sentinel.
        return Ok(None);
    }
    let st = fsm.state();
    Ok(Some((st.applied, st.settings)))
}

/// Recovery (spec §4.7): the newest `snap-*.ultcluster` under `dir`, or
/// genesis; returns `(fsm, start position)`.
pub fn recover(
    dir: &Path,
    genesis: ClusterState,
    declared_hashes: Vec<u64>,
) -> io::Result<(ClusterFsm, u64)> {
    let mut fsm = ClusterFsm::new(genesis, declared_hashes);
    let mut newest: Option<(u64, PathBuf)> = None;
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name();
            let name = name.to_string_lossy();
            if let Some(p) = name
                .strip_prefix("snap-")
                .and_then(|s| s.strip_suffix(".ultcluster"))
                .and_then(|s| s.parse::<u64>().ok())
                && newest.as_ref().is_none_or(|(n, _)| p > *n)
            {
                newest = Some((p, e.path()));
            }
        }
    }
    match newest {
        None => Ok((fsm, 0)),
        Some((pos, path)) => {
            let mut f = File::open(&path)?;
            let got = fsm.install_snapshot(pos, &mut f).map_err(|e| {
                crate::obs_event!(
                    Warn,
                    "cluster_artifact_corrupt",
                    path = path.display().to_string().as_str(),
                    err = e.to_string().as_str()
                );
                io::Error::other(e.to_string())
            })?;
            Ok((fsm, got))
        }
    }
}

/// The `uc2-cluster` agent: the fifth polling agent, driving the cluster
/// FSM's own apply loop over `CLUSTER` frames. Has no cnc slot and sits
/// outside the lag policy.
pub struct ClusterAgent {
    follower: LogFollower,
    cnc: Arc<CncPage>,
    fsm: ClusterFsm,
    view: Arc<ClusterView>,
    snapshot_dir: PathBuf,
    snapshot_pos: u64,
    /// Mirrors `snapshot_pos` for the consensus agent (task 5's floor
    /// computation) — Ruling R2.
    cluster_snapshot_pos: Arc<AtomicU64>,
    declared_rows: Vec<usize>,
    out: Vec<u8>,
    /// Ruling R12: the SAME journal handle the archive agent records into
    /// (`Archive::journal_arc`). The appender never overwrites bytes the
    /// archive has not yet recorded, so a live overrun (no prime explains
    /// it) always has its missing frames retained here — `do_work`'s Overrun
    /// arm replays from it instead of fail-stopping the node.
    journal: Arc<Journal>,
    /// Ruling R18: the "journal purged below this cursor" warning has already
    /// been emitted for the CURRENT episode. That condition is re-checked
    /// every duty cycle and the agent now IDLES on it (rather than skipping
    /// forward), so an unlatched line would be written thousands of times a
    /// second. Cleared by a replay that reaches frames, and by an install.
    replay_gap_logged: bool,
    /// Spec §5.6: the snapshot session's cluster-artifact route. The
    /// `uc_net` receiver agent sends `(position, path)` here the moment a
    /// session completes, BEFORE it publishes the session's floor — the
    /// cluster FSM lives on THIS thread, so an install has to travel rather
    /// than happen at the receiver.
    install: mpsc::Receiver<(u64, PathBuf)>,
    /// Spec §5.6: the ack for the above — the position of the newest cluster
    /// artifact this agent has installed (`Release`). The consensus agent's
    /// install handler waits for this to reach the session's cluster position
    /// before it adopts the floor, so a joiner never serves or leads off a
    /// floor whose cluster row it has not installed.
    installed: Arc<AtomicU64>,
}

impl ClusterAgent {
    /// `start` = the frame-START to begin at (the recovered artifact's
    /// position, or 0). `cluster_snapshot_pos` (Ruling R2) is seeded here from
    /// the recovered artifact's position and updated (Release) by
    /// [`Self::take_snapshot`]; the consensus agent (task 5) reads it.
    /// `journal` (Rulings R12/R18) is the archive's own journal handle
    /// (`Archive::journal_arc`) — the source EVERY overrun replays from.
    /// `install` (spec §5.6) is the receive end of the snapshot session's
    /// cluster-artifact route — the receiver agent hands `(position, path)`
    /// over it on a completed session and `do_work` drains it FIRST, before
    /// anything else it does that pass; `installed` is the ack the consensus
    /// agent's install handler waits on before it adopts the floor.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        buffer: Arc<LogBuffer>,
        cnc: Arc<CncPage>,
        fsm: ClusterFsm,
        view: Arc<ClusterView>,
        snapshot_dir: PathBuf,
        start: u64,
        cluster_snapshot_pos: Arc<AtomicU64>,
        journal: Arc<Journal>,
        install: mpsc::Receiver<(u64, PathBuf)>,
        installed: Arc<AtomicU64>,
    ) -> ClusterAgent {
        let snapshot_pos = fsm.last_applied().filter(|_| start > 0).unwrap_or(0);
        cluster_snapshot_pos.store(snapshot_pos, Ordering::Release);
        let declared_rows = (0..CNC_MAX_SERVICES)
            .filter(|r| cnc.service_slot(*r).identity.hash() != 0)
            .collect();
        ClusterAgent {
            follower: LogFollower::new(buffer, start),
            cnc,
            fsm,
            view,
            snapshot_dir,
            snapshot_pos,
            cluster_snapshot_pos,
            declared_rows,
            out: Vec::new(),
            journal,
            replay_gap_logged: false,
            install,
            installed,
        }
    }

    #[cfg(test)]
    pub fn set_declared_rows_for_test(&mut self, rows: Vec<usize>) {
        self.declared_rows = rows;
    }

    pub fn applied(&self) -> u64 {
        self.fsm.state().applied
    }

    /// The newest complete artifact's position on disk, 0 = none. Plan 2's
    /// set completeness reads this; not read in production yet (task 4
    /// exposes it, `cluster_snapshot_pos` is the value task 5 actually
    /// reads).
    #[allow(dead_code)]
    pub fn snapshot_pos(&self) -> u64 {
        self.snapshot_pos
    }

    /// One duty cycle: apply every committed CLUSTER frame up to
    /// `min(commit, durable)`; publish the view if anything applied; run the
    /// bridging trigger. Returns whether it did work.
    pub fn do_work(&mut self) -> bool {
        // Spec §5.6: the snapshot session's cluster artifact, FIRST. A
        // below-floor joiner's log has nothing this agent can apply — the
        // CLUSTER frames that built the image are below the leader's purge
        // floor — so the install must land before the walk below reads
        // counters that `AdoptFloor` is about to move. Drained in a loop (a
        // burst is possible in principle; each install is idempotent by
        // position) and FAIL-STOP on error: a cluster image that does not
        // parse would leave this node serving off whatever cluster row it had
        // before the join, which is the divergence this path exists to
        // prevent — the same posture the retired config-carry install had.
        let mut installed_any = false;
        while let Ok((position, path)) = self.install.try_recv() {
            if let Err(e) = self.install_from(position, &path) {
                panic!(
                    "uc2-cluster: cannot install the snapshot session's cluster artifact {} at \
                     position {position}: {e} (the session completed, so this node's cluster row \
                     -- membership, schedule table, settings -- would otherwise stay whatever it \
                     was before the join)",
                    path.display()
                );
            }
            installed_any = true;
        }
        let c = self.cnc.counters();
        let head = c.commit.load_acquire().min(c.durable.load_acquire());
        let mut applied_any = false;
        loop {
            // Invariant: one duty cycle never loops on a target it cannot
            // reach. `next_batch(head)` returns an empty `Batch::Frames`
            // (rather than `CaughtUp`) whenever `head` lands strictly inside
            // a frame -- `FrameIter`'s target guard refuses to yield a frame
            // whose END exceeds `head` and leaves the cursor put. That is the
            // normal state whenever this node's commit is paced by the
            // FSM-lag report ceiling (`services::report_ceiling`'s raw byte
            // cap need not land on a frame end), so without the no-progress
            // check below this loop never returns, starving
            // `AgentRunner`'s stop flag and hanging `Node::stop()` joining
            // `uc2-cluster` (services-hang investigation, Ruling R21).
            let before = self.follower.cursor;
            match self.follower.next_batch(head) {
                Batch::CaughtUp => break,
                Batch::Overrun => {
                    // Ruling R18: EVERY overrun replays from the journal.
                    // There is no case in which skipping forward is sound,
                    // and R17 (the FSM's position is the CURSOR) made the old
                    // "a prime explains it, resync to head" arm actively
                    // unsafe: the skipped span would be recorded as consumed,
                    // `take_snapshot` would tag an artifact at a position
                    // whose CLUSTER frames this node never applied, and a
                    // joiner installing that artifact would set its cursor to
                    // the tag and never be able to recover them.
                    //
                    // Nothing is lost by dropping the distinction, because
                    // every frame in `[cursor, buffer base)` is in the journal
                    // BY CONSTRUCTION: the appender never overwrites bytes the
                    // archive has not yet recorded. That covers all three
                    // primes as well as a plain live overrun — a truncation
                    // cuts only ABOVE commit, which is at or above this
                    // cursor; a leader-open collapse touches nothing durable;
                    // and a below-floor join never reaches here at all, since
                    // `install_from` runs at the top of this same `do_work`
                    // and resets the cursor to the installed position.
                    //
                    // This converges: if the buffer's base is still above the
                    // cursor afterwards, the next `next_batch` overruns and
                    // replays again, because the journal is always ahead of
                    // the buffer's base.
                    if self.replay_from_journal(head) {
                        applied_any = true;
                    }
                    break;
                }
                Batch::Frames(iter) => {
                    for (pos, hdr, payload) in iter {
                        if hdr.frame_type != FRAME_TYPE_CLUSTER {
                            continue; // yielded, not applied: the mirror image of the user loop
                        }
                        let end = pos + align_frame_len(hdr.length as usize) as u64;
                        let mut ctx = ApplyCtx::new(end, ClusterFsm::IDENTITY)
                            .with_time(hdr.time_ns)
                            .with_term(hdr.leadership_term_id);
                        self.fsm.apply(&mut ctx, payload, &mut self.out);
                        let accepted = self.out.first() == Some(&0);
                        crate::obs_event!(
                            Info,
                            "cluster_command_applied",
                            position = end,
                            kind = payload.first().copied().unwrap_or(0) as u64,
                            accepted = accepted as u64,
                            reason = self.out.first().copied().unwrap_or(0) as u64
                        );
                        applied_any = true;
                    }
                }
            }
            // A `Frames` batch that consumed nothing is `CaughtUp` in all but
            // name (see the invariant comment above the match): `head` is
            // mid-frame and will not move until the leader advances commit
            // past this frame's end. Ending the duty cycle here — rather than
            // spinning until it does — is what keeps this loop bounded.
            if self.follower.cursor == before {
                break;
            }
        }
        // Task 4's brief: "the follower's cursor after a batch is also a
        // frame-end; `applied` is that cursor." The FSM's position is what it
        // has CONSUMED, not just the last CLUSTER frame it acted on — a frame
        // it yielded is accounted for exactly as the user apply loop accounts
        // for one it skipped. Without this the artifact's tag can only ever be
        // the last operator action's position, and since the node's purge
        // floor is bounded by that tag (`maybe_persist_snapshot_floor`), a
        // cluster with no reconfiguration for a day would purge nothing and
        // ship no set a joiner's floor could sit above — the bridging trigger
        // below would never fire, because the rows' floor climbs with ordinary
        // traffic and this position would not.
        //
        // Deliberately NOT a view publish: nothing about the cluster's STATE
        // changed, and `refresh_from_view`'s per-pass mutex is taken exactly
        // when the position it sees moves (a hot path — M14a's lesson). The
        // view's tag catches up on the next pass that really applies something.
        self.fsm.set_consumed(self.follower.cursor);
        if applied_any {
            self.view.publish(self.fsm.state());
        }
        self.bridging_trigger();
        applied_any || installed_any
    }

    /// Spec §5.6: install the cluster artifact a snapshot session carried, by
    /// FIAT — this node is below the leader's purge floor, so the CLUSTER
    /// frames that built the image are unreadable here and there is nothing
    /// local to reconcile against.
    ///
    /// Publishes the view, seeds both snapshot-position words, and RESETS the
    /// follower cursor to the installed position: the log below it is purged
    /// on this node and `AdoptFloor` is about to prime the counters straight
    /// to the session's floor, so a cursor left where it was would read bytes
    /// that exist neither in the ring nor in this node's journal. This is the
    /// ONLY thing that moves a below-floor node's cursor forward — Ruling R18
    /// removed the overrun skip that used to paper over the same situation.
    ///
    /// An artifact that is empty, truncated or fails its CRC is an
    /// `io::Error` and the caller fail-stops. `ClusterFsm::install_snapshot`
    /// bounds-checks every read, so a corrupt image is refused by name rather
    /// than panicking inside the decoder.
    ///
    /// IDEMPOTENT, and it has to be: a joiner can complete more than one
    /// session for the same floor (a leader whose `SNAP_DONE` was lost opens a
    /// fresh session on the next below-floor NAK), and a node that is NOT
    /// below the floor can be shipped a set it does not need. Installing an
    /// image this FSM has already consumed past would rewind the cursor over
    /// CLUSTER frames it has since applied and replay them — so an artifact at
    /// or below the consumed position is acknowledged and ignored.
    pub fn install_from(&mut self, position: u64, path: &Path) -> io::Result<()> {
        if position <= self.fsm.state().applied {
            self.installed.fetch_max(position, Ordering::Release);
            return Ok(());
        }
        let mut f = File::open(path)?;
        let got = self
            .fsm
            .install_snapshot(position, &mut f)
            .map_err(|e| io::Error::other(e.to_string()))?;
        self.view.publish(self.fsm.state());
        self.snapshot_pos = got;
        self.cluster_snapshot_pos.store(got, Ordering::Release);
        // The artifact IS every CLUSTER frame up to `got`, so the next frame
        // to read is the one starting there.
        self.follower.cursor = got;
        self.replay_gap_logged = false;
        crate::obs_event!(
            Info,
            "cluster_artifact_installed",
            position = got,
            path = path.display().to_string().as_str()
        );
        // The ack — LAST, and `Release`, so a handler that sees it also sees
        // the view and both position words above. `fetch_max`, not `store`:
        // the ack is a high-water mark the consensus agent compares against,
        // and must never go backwards.
        self.installed.fetch_max(got, Ordering::Release);
        Ok(())
    }

    /// Rulings R12/R18: an overrun — any overrun — degrades to journal
    /// replay, exactly as the service's apply loop does below the floor.
    /// Walks `uc_log::archive::replay_journal_from` from
    /// `self.follower.cursor`, applying every `FRAME_TYPE_CLUSTER` frame
    /// through the FSM with the same frame-END `ApplyCtx` the live path
    /// uses, and stops at the first frame whose END exceeds `head` — never
    /// applying an uncommitted frame. Advances `self.follower.cursor` to
    /// wherever the replay actually reached (which then resumes reading
    /// from the live buffer on the next cycle). Returns whether anything
    /// was applied. Panics only if the journal replay itself errors (an
    /// `ArchiveError`) — a fail-stop in the archive's own class.
    fn replay_from_journal(&mut self, head: u64) -> bool {
        let from = self.follower.cursor;
        let mut replay = match replay_journal_from(&self.journal, from) {
            Ok(Some(r)) => r,
            Ok(None) => {
                // Below the journal's own retained floor: the frames are gone
                // from this node entirely. Ruling R18: IDLE — do not skip
                // forward. Skipping would silently drop CLUSTER commands and
                // then let `take_snapshot` tag an artifact claiming them,
                // which a joiner would install and never recover from.
                //
                // On a healthy node this is unreachable: R17 makes the FSM's
                // position the cursor, `take_snapshot` tags the artifact at
                // that position, and `maybe_persist_snapshot_floor` bounds the
                // purge floor by `cluster_snapshot_pos` — so a purge below
                // THIS cursor cannot happen unless the node is itself below
                // the floor. A node that IS below the floor is served a
                // snapshot session, whose id-255 part lands in `install_from`
                // and moves the cursor with real state behind it. So the
                // correct behaviour here is to wait for that.
                //
                // Latched: one line per episode, cleared by any successful
                // replay or install, because this is polled every duty cycle
                // and a per-cycle line would bury the log.
                if !self.replay_gap_logged {
                    self.replay_gap_logged = true;
                    crate::obs_event!(
                        Warn,
                        "cluster_agent_journal_replay_gap_purged",
                        from = from,
                        head = head
                    );
                }
                return false;
            }
            Err(e) => {
                panic!("uc2-cluster: log buffer overrun at {from} (journal replay: {e})")
            }
        };
        let mut cursor = from;
        let mut frames = 0u64;
        let mut applied_any = false;
        loop {
            match replay.next() {
                Ok(Some(rf)) => {
                    let end = rf.position + align_frame_len(rf.header.length as usize) as u64;
                    if end > head {
                        break; // never apply an uncommitted frame
                    }
                    cursor = end;
                    if rf.header.frame_type == FRAME_TYPE_CLUSTER {
                        let mut ctx = ApplyCtx::new(end, ClusterFsm::IDENTITY)
                            .with_time(rf.header.time_ns)
                            .with_term(rf.header.leadership_term_id);
                        self.fsm.apply(&mut ctx, &rf.payload, &mut self.out);
                        applied_any = true;
                        frames += 1;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    panic!("uc2-cluster: log buffer overrun at {cursor} (journal replay: {e})")
                }
            }
        }
        self.follower.cursor = cursor;
        self.replay_gap_logged = false;
        crate::obs_event!(
            Warn,
            "cluster_agent_journal_replay",
            from = from,
            to = cursor,
            frames = frames
        );
        applied_any
    }

    /// The bridging trigger (plan-1 only, deleted by plan 2): after each
    /// cycle, read `candidate_floor = min over declared rows of
    /// slot.snapshot_pos` (0 if any is 0); if `candidate_floor > 0 &&
    /// self.snapshot_pos() < candidate_floor && self.applied() >=
    /// candidate_floor`, call `take_snapshot()`. This keeps the cluster
    /// artifact at or above the user rows' minimum, so the node's floor
    /// (Task 5 includes the cluster artifact in the floor computation) is
    /// never held down by a stale cluster artifact, and a shipped set's min
    /// position is always one the leader still holds frames for.
    fn bridging_trigger(&mut self) {
        let mut floor = u64::MAX;
        for r in &self.declared_rows {
            let p = self.cnc.service_slot(*r).snapshot_pos.load_acquire();
            if p == 0 {
                return;
            }
            floor = floor.min(p);
        }
        if floor != u64::MAX
            && self.snapshot_pos < floor
            && self.applied() >= floor
            && let Err(e) = self.take_snapshot()
        {
            crate::obs_event!(
                Warn,
                "cluster_snapshot_failed",
                err = e.to_string().as_str()
            );
        }
    }

    /// Freeze at the current applied position and write
    /// `snapshots/cluster/snap-{applied}.ultcluster` (fsync, rename).
    /// Returns the position.
    pub fn take_snapshot(&mut self) -> io::Result<u64> {
        let (img, pos) = self
            .fsm
            .freeze()
            .map_err(|e| io::Error::other(e.to_string()))?;
        fs::create_dir_all(&self.snapshot_dir)?;
        let final_path = artifact_path(&self.snapshot_dir, pos);
        let tmp = final_path.with_extension("ultcluster.part");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&img)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &final_path)?;
        match File::open(&self.snapshot_dir) {
            Ok(d) => {
                if let Err(e) = d.sync_all() {
                    crate::obs_event!(
                        Warn,
                        "cluster_snapshot_dir_fsync_failed",
                        err = e.to_string().as_str()
                    );
                }
            }
            Err(e) => {
                crate::obs_event!(
                    Warn,
                    "cluster_snapshot_dir_fsync_failed",
                    err = e.to_string().as_str()
                );
            }
        }
        self.snapshot_pos = pos;
        self.cluster_snapshot_pos.store(pos, Ordering::Release);
        Ok(pos)
    }
}

#[cfg(test)]
mod tests {
    use uc_consensus::config::{Addr, ClusterConfig};
    use uc_log::archive::{Archive, ArchiveConfig};
    use uc_log::buffer::LogBuffer;
    use uc_log::cnc::{CncMeta, CncPage};
    use uc_log::region::Region;
    use uc_protocol::v2::cnc::CNC_MAX_SERVICES;
    use uc_protocol::v2::frame::ClusterKind;
    use uc_protocol::v2::settings::{Settings, encode_settings};

    use super::*;

    /// A journal with nothing recorded — for tests that need a valid
    /// `Arc<Journal>` but never exercise `replay_from_journal` (the Overrun
    /// path is either never hit, or explained by a prime). Built the same
    /// way `uc_log::archive`'s own tests build one (`Archive::open` +
    /// `journal_arc`), per Ruling R12.
    fn empty_journal(dir: &std::path::Path) -> Arc<Journal> {
        Archive::open(ArchiveConfig::new(dir.join("journal")))
            .unwrap()
            .journal_arc()
    }

    /// A scratch directory on REAL DISK, never `/tmp` (RAM-backed tmpfs with
    /// no swap on the dev box — CLAUDE.md). `CARGO_TARGET_TMPDIR` is set only
    /// for integration-test binaries and these are inline unit tests in the
    /// lib target, so this falls back to a package-relative `target/`
    /// directory — the same helper shape `audit.rs`'s tests use.
    fn tempdir() -> tempfile::TempDir {
        let root = std::env::var("CARGO_TARGET_TMPDIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc_node_tests")
            });
        std::fs::create_dir_all(&root).expect("scratch root");
        tempfile::Builder::new()
            .prefix("uc2-cluster-agent-")
            .tempdir_in(&root)
            .expect("tempdir")
    }

    /// The cluster-artifact install route for a test that never installs one
    /// (spec §5.6): the sending half is dropped immediately, so the drain at
    /// the top of `do_work` sees `Disconnected` and does nothing.
    fn no_install_route() -> mpsc::Receiver<(u64, PathBuf)> {
        let (_tx, rx) = mpsc::sync_channel(1);
        rx
    }

    fn addr(i: u32) -> Addr {
        (u32::from_be_bytes([127, 0, 0, i as u8]), 9100 + i as u16)
    }

    fn genesis_state() -> ClusterState {
        ClusterState {
            membership: ClusterConfig::genesis(
                vec![(0, addr(0)), (1, addr(1)), (2, addr(2))],
                vec![],
            ),
            table: ScheduleTable { entries: vec![] },
            table_position: 0,
            settings: Settings::genesis_default(),
            settings_position: 0,
            applied: 0,
        }
    }

    fn world() -> (Arc<LogBuffer>, Arc<CncPage>, tempfile::TempDir) {
        let dir = tempdir();
        let cnc = CncPage::heap(&CncMeta {
            node_id: 1,
            instance_id: 0,
            app_id: "t".into(),
            buffer_bytes: 1 << 16,
            max_payload: 4096,
            services: [None; CNC_MAX_SERVICES],
        });
        let buffer = Arc::new(LogBuffer::new(
            Region::heap_zeroed(1 << 16),
            Arc::clone(&cnc),
            4096,
        ));
        cnc.counters().prime(0);
        (buffer, cnc, dir)
    }

    fn settings_cmd(interval: u64) -> Vec<u8> {
        let mut p = Vec::new();
        encode_settings(
            &Settings {
                snapshot_interval_bytes: interval,
                ..Settings::genesis_default()
            },
            &mut p,
        );
        p
    }

    #[test]
    fn applies_only_committed_cluster_frames_and_publishes_the_view() {
        let (buffer, cnc, dir) = world();
        let mut app = buffer.appender_for_test(0); // the same helper node.rs's harness uses
        app.set_now(1);
        let e1 = app
            .append_cluster(1, ClusterKind::Settings, &settings_cmd(7))
            .unwrap();
        let _e2 = app.append(1, 1, b"client frame").unwrap(); // MESSAGE: must be yielded, not applied
        let e3 = app
            .append_cluster(1, ClusterKind::Settings, &settings_cmd(9))
            .unwrap();
        let (fsm, start) = recover(dir.path(), genesis_state(), vec![]).unwrap();
        let view = Arc::new(ClusterView::new(fsm.state()));
        let mut agent = ClusterAgent::new(
            Arc::clone(&buffer),
            Arc::clone(&cnc),
            fsm,
            Arc::clone(&view),
            dir.path().join("snapshots/cluster"),
            start,
            Arc::new(AtomicU64::new(0)),
            empty_journal(dir.path()),
            no_install_route(),
            Arc::new(AtomicU64::new(0)),
        );
        cnc.counters().durable.store_release(e3);
        cnc.counters().commit.store_release(e1); // only the first command is committed
        assert!(agent.do_work());
        assert_eq!(view.position.load(Ordering::Acquire), e1);
        assert_eq!(view.snapshot_interval_bytes.load(Ordering::Acquire), 7);
        cnc.counters().commit.store_release(e3);
        assert!(agent.do_work());
        assert_eq!(view.position.load(Ordering::Acquire), e3);
        assert_eq!(view.snapshot_interval_bytes.load(Ordering::Acquire), 9);
        assert!(!agent.do_work(), "caught up: no work");
    }

    /// Regression for the services-hang investigation
    /// (`.superpowers/sdd/2026-09-06-uc2-cluster-fsm-plan1/services-hang-investigation.md`):
    /// `head` (`min(commit, durable)`) landing STRICTLY INSIDE a frame is
    /// exactly what the FSM-lag report ceiling produces in production
    /// (`services::report_ceiling`'s raw byte cap need not land on a frame
    /// end). `LogFollower::next_batch` then hands back an empty
    /// `Batch::Frames` forever — `FrameIter`'s target guard refuses to yield
    /// a frame whose END exceeds `target` and leaves the cursor put — so the
    /// pre-fix drain loop (`loop { match next_batch(head) { .. } }`, breaking
    /// only on `CaughtUp`/`Overrun`) never returns. That starves
    /// `AgentRunner`'s stop flag and hangs `Node::stop()` joining
    /// `uc2-cluster`.
    ///
    /// One 96-byte frame at `[0, 96)`; `commit`/`durable` at `48`, the
    /// investigation's own example. Run off-thread with a bounded join: on
    /// the unfixed code this deadline fires (`do_work` spins forever); after
    /// Ruling R21 it returns promptly, having done no work.
    #[test]
    fn a_target_inside_a_frame_ends_the_duty_cycle_instead_of_spinning() {
        let (buffer, cnc, dir) = world();
        let mut app = buffer.appender_for_test(0);
        app.set_now(1);
        // 32 B header + 8 B cluster prefix + 56 B payload = 96, already
        // aligned — one frame spanning [0, 96).
        let end = app
            .append_cluster(1, ClusterKind::Settings, &[0u8; 56])
            .unwrap();
        assert_eq!(end, 96, "sanity: one 96-byte frame at [0, 96)");

        let (fsm, start) = recover(dir.path(), genesis_state(), vec![]).unwrap();
        let view = Arc::new(ClusterView::new(fsm.state()));
        let mut agent = ClusterAgent::new(
            Arc::clone(&buffer),
            Arc::clone(&cnc),
            fsm,
            view,
            dir.path().join("snapshots/cluster"),
            start,
            Arc::new(AtomicU64::new(0)),
            empty_journal(dir.path()),
            no_install_route(),
            Arc::new(AtomicU64::new(0)),
        );
        // Strictly inside the only frame: not 0 (genesis), not 96 (its end).
        cnc.counters().durable.store_release(48);
        cnc.counters().commit.store_release(48);

        let (tx, rx) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            let did_work = agent.do_work();
            let applied = agent.applied();
            let cursor = agent.follower.cursor;
            // Ignore a closed receiver: the assertion below already failed
            // by the time this send could fail.
            let _ = tx.send((did_work, applied, cursor));
        });
        match rx.recv_timeout(std::time::Duration::from_secs(2)) {
            Ok((did_work, applied, cursor)) => {
                handle.join().expect("agent thread panicked");
                assert!(
                    !did_work,
                    "a target that lands mid-frame is no work this cycle"
                );
                assert_eq!(
                    applied, 0,
                    "the FSM must not consume a frame it cannot see the end of"
                );
                assert_eq!(cursor, 0, "the cursor must stay at the frame's start");
            }
            Err(_) => panic!(
                "do_work() did not return within the 2s deadline: the drain loop is \
                 spinning on a target (commit=durable=48) that lands inside the only \
                 frame ([0, 96)) instead of yielding the duty cycle -- see \
                 .superpowers/sdd/2026-09-06-uc2-cluster-fsm-plan1/services-hang-investigation.md"
            ),
        }
    }

    /// Ruling R17: the position the view and the artifact carry is the apply
    /// loop's CURSOR after the batch — everything committed this FSM has
    /// consumed — not merely the last CLUSTER command's own frame-end.
    ///
    /// Staged so the two are DIFFERENT: the batch's last frame is a plain
    /// MESSAGE, appended after the command. Under the pre-R17 reading both
    /// assertions below would read `e_cmd`; the cursor reading is `e_msg`, and
    /// that difference is exactly what lets the artifact (and with it the
    /// node's purge floor) advance on a cluster whose CLUSTER frames are rare.
    #[test]
    fn the_view_and_artifact_position_is_the_cursor_not_the_last_command() {
        let (buffer, cnc, dir) = world();
        let mut app = buffer.appender_for_test(0);
        app.set_now(1);
        let e_cmd = app
            .append_cluster(1, ClusterKind::Settings, &settings_cmd(7))
            .unwrap();
        // Two ordinary frames AFTER it: yielded by this loop, but walked.
        let _ = app.append(1, 1, b"client frame").unwrap();
        let e_msg = app.append(1, 1, b"another client frame").unwrap();
        assert!(e_msg > e_cmd);

        let (fsm, start) = recover(dir.path(), genesis_state(), vec![]).unwrap();
        let view = Arc::new(ClusterView::new(fsm.state()));
        let cluster_pos = Arc::new(AtomicU64::new(0));
        let mut agent = ClusterAgent::new(
            Arc::clone(&buffer),
            Arc::clone(&cnc),
            fsm,
            Arc::clone(&view),
            dir.path().join("snapshots/cluster"),
            start,
            Arc::clone(&cluster_pos),
            empty_journal(dir.path()),
            no_install_route(),
            Arc::new(AtomicU64::new(0)),
        );
        cnc.counters().durable.store_release(e_msg);
        cnc.counters().commit.store_release(e_msg);
        assert!(agent.do_work());

        assert_eq!(
            agent.applied(),
            e_msg,
            "the FSM's position is the cursor — the END of the last frame WALKED"
        );
        assert_eq!(
            view.position.load(Ordering::Acquire),
            e_msg,
            "…and so is the view's position tag, published on the pass that applied"
        );
        assert_eq!(
            view.snapshot_interval_bytes.load(Ordering::Acquire),
            7,
            "sanity: the command itself did apply"
        );

        // The artifact carries the same position, which is what the purge
        // floor is bounded by, and it recovers at it.
        let pos = agent.take_snapshot().expect("freeze");
        assert_eq!(pos, e_msg);
        assert_eq!(cluster_pos.load(Ordering::Acquire), e_msg);
        let (_, start2) = recover(
            &dir.path().join("snapshots/cluster"),
            genesis_state(),
            vec![],
        )
        .unwrap();
        assert_eq!(start2, e_msg, "recovery resumes at the consumed position");
    }

    #[test]
    fn take_snapshot_writes_a_recoverable_artifact_and_recovery_resumes_after_it() {
        let (buffer, cnc, dir) = world();
        let mut app = buffer.appender_for_test(0);
        app.set_now(1);
        let e1 = app
            .append_cluster(1, ClusterKind::Settings, &settings_cmd(7))
            .unwrap();
        let (fsm, start) = recover(dir.path(), genesis_state(), vec![]).unwrap();
        let view = Arc::new(ClusterView::new(fsm.state()));
        let mut agent = ClusterAgent::new(
            Arc::clone(&buffer),
            Arc::clone(&cnc),
            fsm,
            view,
            dir.path().join("snapshots/cluster"),
            start,
            Arc::new(AtomicU64::new(0)),
            empty_journal(dir.path()),
            no_install_route(),
            Arc::new(AtomicU64::new(0)),
        );
        cnc.counters().durable.store_release(e1);
        cnc.counters().commit.store_release(e1);
        agent.do_work();
        assert_eq!(agent.take_snapshot().unwrap(), e1);
        assert!(artifact_path(&dir.path().join("snapshots/cluster"), e1).is_file());
        assert_eq!(agent.snapshot_pos(), e1);
        let (fsm2, start2) = recover(
            &dir.path().join("snapshots/cluster"),
            genesis_state(),
            vec![],
        )
        .unwrap();
        assert_eq!(start2, e1, "recovery resumes at the artifact's position");
        assert_eq!(fsm2.state().settings.snapshot_interval_bytes, 7);
    }

    #[test]
    fn bridging_trigger_snapshots_when_the_user_rows_floor_passes_the_artifact() {
        let (buffer, cnc, dir) = world();
        let mut app = buffer.appender_for_test(0);
        app.set_now(1);
        let e1 = app
            .append_cluster(1, ClusterKind::Settings, &settings_cmd(7))
            .unwrap();
        let (fsm, start) = recover(dir.path(), genesis_state(), vec![0xF5A0]).unwrap();
        let view = Arc::new(ClusterView::new(fsm.state()));
        let mut agent = ClusterAgent::new(
            Arc::clone(&buffer),
            Arc::clone(&cnc),
            fsm,
            view,
            dir.path().join("snapshots/cluster"),
            start,
            Arc::new(AtomicU64::new(0)),
            empty_journal(dir.path()),
            no_install_route(),
            Arc::new(AtomicU64::new(0)),
        );
        cnc.counters().durable.store_release(e1);
        cnc.counters().commit.store_release(e1);
        agent.set_declared_rows_for_test(vec![0]);
        agent.do_work();
        assert_eq!(agent.snapshot_pos(), 0, "no user row has snapshotted yet");
        cnc.service_slot(0).snapshot_pos.store_release(e1); // row 0 snapshotted at e1
        agent.do_work();
        assert_eq!(
            agent.snapshot_pos(),
            e1,
            "the cluster artifact caught up to the rows' floor"
        );
    }

    /// Stages an `Overrun` the same way `uc_log::reader`'s own
    /// `overrun_surfaces_after_a_prime_over_fresh_region` does: prime the
    /// counters straight to `2 * capacity` over a follower cursor still at 0
    /// (a fresh instance dir's genesis start) — the same shape as a
    /// below-floor joiner's `AdoptFloor`. `commit` is set alongside `durable`
    /// since `prime` deliberately leaves it alone (`LogCounters::prime`'s
    /// doc) and `do_work` heads on `min(commit, durable)`.
    fn stage_overrun(buffer: &Arc<LogBuffer>, cnc: &Arc<CncPage>) -> u64 {
        let head = 2 * buffer.capacity();
        cnc.counters().prime(head);
        cnc.counters().commit.store_release(head);
        head
    }

    /// Ruling R18: an overrun that a PRIME explains must still replay the
    /// journal — it must never skip the cursor forward.
    ///
    /// The regression this pins is a real data loss, and only R17 made it
    /// visible: the old arm set `follower.cursor = head`, R17's
    /// `set_consumed(cursor)` then recorded the skipped span as consumed, and
    /// `take_snapshot` tags the artifact at that position — so the node would
    /// ship an artifact CLAIMING CLUSTER commands it never applied, and a
    /// joiner installing it would set its own cursor to the tag and never be
    /// able to recover them. There is no sound case for the skip: every frame
    /// in the gap is in the journal by construction (the appender never
    /// overwrites bytes the archive has not recorded).
    ///
    /// Deliberately the same staging as the no-prime test below, plus the
    /// generation bump the archive does right after `LogCounters::prime` —
    /// which is exactly what used to select the losing branch.
    #[test]
    fn an_overrun_after_a_prime_replays_the_journal_rather_than_skipping() {
        let (buffer, cnc, dir) = world();
        let mut archive = Archive::open(ArchiveConfig::new(dir.path().join("journal"))).unwrap();
        let mut app = buffer.appender_for_test(0);
        app.set_now(1);
        let _e1 = app
            .append_cluster(1, ClusterKind::Settings, &settings_cmd(7))
            .unwrap();
        let _e2 = app.append(1, 1, b"client frame").unwrap(); // yielded, not applied
        let e3 = app
            .append_cluster(1, ClusterKind::Settings, &settings_cmd(9))
            .unwrap();
        while archive.do_work(&buffer).unwrap() {}
        let journal = archive.journal_arc();

        let (fsm, start) = recover(dir.path(), genesis_state(), vec![]).unwrap();
        let view = Arc::new(ClusterView::new(fsm.state()));
        let prime_gen = Arc::new(AtomicU64::new(0));
        let mut agent = ClusterAgent::new(
            Arc::clone(&buffer),
            Arc::clone(&cnc),
            fsm,
            Arc::clone(&view),
            dir.path().join("snapshots/cluster"),
            start,
            Arc::new(AtomicU64::new(0)),
            journal,
            no_install_route(),
            Arc::new(AtomicU64::new(0)),
        );
        // The archive bumps the generation right after `LogCounters::prime` —
        // do the same, THEN stage the overrun, matching how `AdoptFloor`
        // orders the two writes in `node.rs`. Under the old arm this is what
        // made the agent skip.
        prime_gen.fetch_add(1, Ordering::Release);
        let head = stage_overrun(&buffer, &cnc);
        assert!(head > e3, "the prime moves the head well past the frames");

        assert!(agent.do_work(), "the gap is replayed, not skipped");
        assert_eq!(
            view.snapshot_interval_bytes.load(Ordering::Acquire),
            9,
            "every CLUSTER frame in the overrun span was applied — the LAST \
             command's value, so nothing in between was skipped either"
        );
        assert_eq!(
            agent.applied(),
            e3,
            "the position is where the replay actually REACHED, not the head \
             the prime moved to — an artifact tagged at `head` would claim \
             frames this node never applied"
        );
        assert_eq!(agent.follower.cursor, e3);
        assert!(
            agent.applied() < head,
            "and it is strictly below the head, which is the whole point"
        );
    }

    /// Ruling R12: a live overrun — the ring simply outran this agent, with
    /// no prime anywhere — must not lose data either. Since R18 this takes
    /// the identical path as the test above (there is one arm now), and the
    /// pair is kept deliberately: together they say that WHY the overrun
    /// happened no longer selects a behaviour.
    #[test]
    fn an_overrun_without_a_prime_replays_the_gap_from_the_journal() {
        let (buffer, cnc, dir) = world();
        let mut archive = Archive::open(ArchiveConfig::new(dir.path().join("journal"))).unwrap();
        let mut app = buffer.appender_for_test(0);
        app.set_now(1);
        let _e1 = app
            .append_cluster(1, ClusterKind::Settings, &settings_cmd(7))
            .unwrap();
        let _e2 = app
            .append_cluster(1, ClusterKind::Settings, &settings_cmd(8))
            .unwrap();
        let e3 = app
            .append_cluster(1, ClusterKind::Settings, &settings_cmd(9))
            .unwrap();
        // Record all three into a REAL journal, driving the archive the way
        // `uc_log::archive`'s own tests do — this also advances `durable` to
        // `e3`, matching what the archive would really do before any of
        // this test's synthetic priming below.
        while archive.do_work(&buffer).unwrap() {}
        let journal = archive.journal_arc();

        let (fsm, start) = recover(dir.path(), genesis_state(), vec![]).unwrap();
        let view = Arc::new(ClusterView::new(fsm.state()));
        let mut agent = ClusterAgent::new(
            Arc::clone(&buffer),
            Arc::clone(&cnc),
            fsm,
            Arc::clone(&view),
            dir.path().join("snapshots/cluster"),
            start,
            Arc::new(AtomicU64::new(0)),
            journal,
            no_install_route(),
            Arc::new(AtomicU64::new(0)),
        );

        // Stage a LIVE overrun exactly like `stage_overrun` does — the ring
        // physically moved on past the follower's cursor (still 0) with NO
        // prime to explain it — but the journal above genuinely holds every
        // frame the live buffer no longer does.
        let head = stage_overrun(&buffer, &cnc);

        assert!(agent.do_work(), "replayed real work from the journal");
        assert_eq!(
            view.position.load(Ordering::Acquire),
            e3,
            "nothing was skipped — the view is at the LAST command's frame-end"
        );
        assert_eq!(
            view.snapshot_interval_bytes.load(Ordering::Acquire),
            9,
            "the view holds the LAST command's value, not an earlier one"
        );
        assert_eq!(agent.applied(), e3);
        // The journal only ever held these 3 real frames (nothing beyond
        // `e3` was ever appended) — the replay correctly stops there, NOT
        // at the synthetic `head` the staged overrun claims durable up to;
        // the next `do_work` cycle will overrun again from this cursor
        // (still below `head`) and replay again, converging once the
        // journal catches up in a real system.
        assert_eq!(
            agent.follower.cursor, e3,
            "the replay stops at the journal's real content, not at the staged head"
        );
        assert!(e3 < head, "sanity: the staged head is beyond real content");
    }

    // A `should_panic` test for the journal-error fail-stop path (a
    // corrupted/truncated journal below the cursor) is deliberately NOT
    // included: staging one cheaply would require reaching into
    // `uc_journal`'s on-disk segment format (record framing + checksums) to
    // corrupt exactly the archived payload bytes without tripping the
    // journal's OWN corruption handling differently (e.g. failing
    // `Journal::open`'s recovery scan instead of `Replay::next`), which is
    // an implementation detail of a different crate — getting it subtly
    // wrong risks a flaky or falsely-passing test rather than a real one.
    // See the task-4 report's "Fix round 2" section.
}
