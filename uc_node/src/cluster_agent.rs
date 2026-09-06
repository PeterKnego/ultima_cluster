// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The `uc2-cluster` agent (cluster-FSM spec §4.1): the cluster FSM's own
//! apply loop, in-node, no cnc slot, outside the lag policy. It walks the
//! node's `LogBuffer` with a `LogFollower`, acts on `CLUSTER` frames only,
//! and publishes [`ClusterView`] after every batch that applied something.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
/// table. Plan 1 task 8 gives `uc2ctl` the live reading; until then this is
/// the honest offline one.
pub fn read_committed_table(instance_dir: &Path) -> io::Result<(u64, ScheduleTable)> {
    let genesis = ClusterState {
        membership: uc_consensus::config::ClusterConfig::genesis(Vec::new(), Vec::new()),
        table: ScheduleTable {
            entries: Vec::new(),
        },
        table_position: 0,
        settings: uc_protocol::v2::settings::Settings::genesis_default(),
        applied: 0,
    };
    // No declared hashes: this reader never APPLIES a command, and
    // `install_snapshot` does not consult them.
    let (fsm, _) = recover(&snapshot_dir_of(instance_dir), genesis, Vec::new())?;
    let st = fsm.state();
    Ok((st.table_position, st.table.clone()))
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
    /// Ruling R11: the SAME node-internal generation counter the archive
    /// agent bumps immediately after every `LogCounters::prime` (truncate,
    /// AdoptFloor, leader-open collapse) — `uc_net::receiver`'s
    /// `prime_generation` field, shared here for the identical purpose:
    /// telling a benign forward re-prime apart from a genuine live overrun.
    prime_generation: Arc<AtomicU64>,
    /// The generation as of the last time we actually EXPLAINED an Overrun
    /// with it (not merely "as of the top of the last cycle" — a prime's
    /// generation bump and its effect on `commit`/`durable` becoming visible
    /// to THIS thread are two independent atomics with no ordering between
    /// them, so the two can be observed on different cycles; refreshing this
    /// unconditionally on every idle cycle would spend the "explained"
    /// credit before an Overrun ever needed it).
    last_prime_gen: u64,
    /// Ruling R12: the SAME journal handle the archive agent records into
    /// (`Archive::journal_arc`). The appender never overwrites bytes the
    /// archive has not yet recorded, so a live overrun (no prime explains
    /// it) always has its missing frames retained here — `do_work`'s Overrun
    /// arm replays from it instead of fail-stopping the node.
    journal: Arc<Journal>,
}

impl ClusterAgent {
    /// `start` = the frame-START to begin at (the recovered artifact's
    /// position, or 0). `cluster_snapshot_pos` (Ruling R2) is seeded here from
    /// the recovered artifact's position and updated (Release) by
    /// [`Self::take_snapshot`]; the consensus agent (task 5) reads it.
    /// `prime_generation` (Ruling R11) is the node-wide re-prime generation
    /// counter, shared with `uc_net::receiver`. `journal` (Ruling R12) is
    /// the archive's own journal handle (`Archive::journal_arc`), the
    /// fallback source for a live overrun with no explaining prime.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        buffer: Arc<LogBuffer>,
        cnc: Arc<CncPage>,
        fsm: ClusterFsm,
        view: Arc<ClusterView>,
        snapshot_dir: PathBuf,
        start: u64,
        cluster_snapshot_pos: Arc<AtomicU64>,
        prime_generation: Arc<AtomicU64>,
        journal: Arc<Journal>,
    ) -> ClusterAgent {
        let snapshot_pos = fsm.last_applied().filter(|_| start > 0).unwrap_or(0);
        cluster_snapshot_pos.store(snapshot_pos, Ordering::Release);
        let declared_rows = (0..CNC_MAX_SERVICES)
            .filter(|r| cnc.service_slot(*r).identity.hash() != 0)
            .collect();
        let last_prime_gen = prime_generation.load(Ordering::Acquire);
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
            prime_generation,
            last_prime_gen,
            journal,
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
        let c = self.cnc.counters();
        let head = c.commit.load_acquire().min(c.durable.load_acquire());
        let mut applied_any = false;
        loop {
            // Ruling R11: sample the prime generation before calling
            // `next_batch`, mirroring `uc_net::receiver`'s DATA-arm `gen0`
            // (its per-read straddle guard, ~line 1745). Compared against
            // `last_prime_gen` — the generation we last actually ACCOUNTED
            // for, not merely "as of the top of this cycle": a prime
            // (AdoptFloor, truncate, leader-open collapse) can complete, and
            // its generation bump land, long before this agent is next
            // scheduled to notice `head` has moved (the archive bumps the
            // generation right after `LogCounters::prime`, but `commit` is a
            // separate counter written by a different agent on a different
            // cadence — so the two are not guaranteed to become visible to
            // this thread on the same cycle).
            let gen0 = self.prime_generation.load(Ordering::Acquire);
            match self.follower.next_batch(head) {
                Batch::CaughtUp => break,
                Batch::Overrun => {
                    // Recheck (the same belt-and-suspenders the receiver's
                    // DATA arm uses at its own Overrun-adjacent site): a
                    // prime racing concurrently with THIS call's own
                    // execution may not yet have been visible in `gen0`.
                    let gen1 = self.prime_generation.load(Ordering::Acquire);
                    let primed = gen0 != self.last_prime_gen || gen1 != self.last_prime_gen;
                    if primed {
                        self.last_prime_gen = gen1;
                        // Below the buffer: a below-floor joiner (a fresh
                        // learner, a wipe-and-rejoin) has its counters primed
                        // straight to the installed snapshot's position by
                        // `AdoptFloor`, well past this follower's cursor —
                        // the same shape as the service apply loop's
                        // below-floor case, but plan 1 has not yet given the
                        // snapshot session a cluster artifact to install
                        // from (task 9 does: `ClusterAgent::install_from`
                        // resets the cursor to the installed position).
                        // Until then, resync forward rather than
                        // fail-stopping the whole node over a component
                        // nothing reads yet — any CLUSTER frames in the
                        // skipped span are missed on THIS node until task 9
                        // lands.
                        crate::obs_event!(
                            Warn,
                            "cluster_agent_resynced_over_overrun",
                            cursor = self.follower.cursor,
                            head = head
                        );
                        self.follower.cursor = head;
                    } else {
                        // Ruling R12: no prime explains this. Fix round 2
                        // found that fail-stopping here anyway punishes a
                        // perfectly healthy node — this agent has no
                        // admission door (spec §4.1, "outside the lag
                        // policy") and the shared ring holds every frame
                        // type, so ordinary heavy write throughput on a
                        // small ring can outrun it with no prime in sight.
                        // But the appender never overwrites bytes the
                        // archive has not recorded, so every frame this
                        // agent missed is still in the journal — replay it
                        // from there instead of resyncing blind or
                        // panicking. This converges: if the buffer's base is
                        // still above the cursor afterward, the next
                        // `next_batch` overruns again and replays again,
                        // because the journal is always ahead of the
                        // buffer's base.
                        if self.replay_from_journal(head) {
                            applied_any = true;
                        }
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
        }
        if applied_any {
            self.view.publish(self.fsm.state());
        }
        self.bridging_trigger();
        applied_any
    }

    /// Ruling R12: a live overrun with no explaining prime degrades to
    /// journal replay, exactly as the service's apply loop does below the
    /// floor. Walks `uc_log::archive::replay_journal_from` from
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
                // Below the journal's own retained floor (purged) — R12
                // doesn't cover this edge (it assumes the journal always
                // has what the live buffer no longer does, which holds
                // absent purge). Task 9's artifact-carrying snapshot
                // install is the real fix for a below-floor node; until
                // then there is nothing left to replay, so resync forward
                // (the same interim posture as a prime) rather than wedge
                // the node forever on an unreadable gap.
                crate::obs_event!(
                    Warn,
                    "cluster_agent_journal_replay_gap_purged",
                    from = from,
                    head = head
                );
                self.follower.cursor = head;
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
            Arc::new(AtomicU64::new(0)),
            empty_journal(dir.path()),
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
            Arc::new(AtomicU64::new(0)),
            empty_journal(dir.path()),
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
            Arc::new(AtomicU64::new(0)),
            empty_journal(dir.path()),
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

    #[test]
    fn an_overrun_after_a_prime_resyncs_and_warns() {
        let (buffer, cnc, dir) = world();
        let (fsm, start) = recover(dir.path(), genesis_state(), vec![]).unwrap();
        let view = Arc::new(ClusterView::new(fsm.state()));
        let prime_gen = Arc::new(AtomicU64::new(0));
        let mut agent = ClusterAgent::new(
            Arc::clone(&buffer),
            Arc::clone(&cnc),
            fsm,
            view,
            dir.path().join("snapshots/cluster"),
            start,
            Arc::new(AtomicU64::new(0)),
            Arc::clone(&prime_gen),
            empty_journal(dir.path()),
        );
        // The archive bumps the generation right after `LogCounters::prime` —
        // do the same here, THEN stage the overrun, matching how `AdoptFloor`
        // orders the two writes in `node.rs`.
        prime_gen.fetch_add(1, Ordering::Release);
        let head = stage_overrun(&buffer, &cnc);
        assert!(
            !agent.do_work(),
            "resynced, not panicked; nothing was actually applied"
        );
        assert_eq!(
            agent.follower.cursor, head,
            "the follower resyncs to the new head"
        );
    }

    /// Ruling R12: a live overrun with no explaining prime must not lose
    /// data — the appender never overwrites bytes the archive has not
    /// recorded, so every frame this agent missed is still in the journal.
    /// Stages the SAME shape of overrun as `an_overrun_after_a_prime_...`,
    /// but this time the frames it must recover are REAL, recorded ones
    /// (not the resync test's "nothing was actually applied"), and the
    /// generation is never bumped — a live overrun, not a prime.
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
            Arc::new(AtomicU64::new(0)), // prime_generation: never bumped
            journal,
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
