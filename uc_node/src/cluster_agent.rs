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

use uc_log::buffer::LogBuffer;
use uc_log::cnc::CncPage;
use uc_log::reader::{Batch, LogFollower};
use uc_protocol::v2::cnc::CNC_MAX_SERVICES;
use uc_protocol::v2::frame::{FRAME_TYPE_CLUSTER, align_frame_len};
use uc_service::{ApplyCtx, RawStateMachine, SnapshotStateMachine};

use crate::cluster_fsm::{ClusterFsm, ClusterState, ClusterView};

pub fn artifact_path(dir: &Path, position: u64) -> PathBuf {
    dir.join(format!("snap-{position}.ultcluster"))
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
    /// Set the first time this follower EVER makes forward progress (a
    /// resync, a `CaughtUp`, or a successful `Frames` batch) since
    /// construction; never cleared again. See `do_work`'s Overrun arm for
    /// why the fail-stop panic is gated on this rather than firing on every
    /// Overrun a fresh generation doesn't explain (Ruling R11, fix round 2).
    made_progress: bool,
}

impl ClusterAgent {
    /// `start` = the frame-START to begin at (the recovered artifact's
    /// position, or 0). `cluster_snapshot_pos` (Ruling R2) is seeded here from
    /// the recovered artifact's position and updated (Release) by
    /// [`Self::take_snapshot`]; the consensus agent (task 5) reads it.
    /// `prime_generation` (Ruling R11) is the node-wide re-prime generation
    /// counter, shared with `uc_net::receiver`.
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
            made_progress: false,
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
                Batch::CaughtUp => {
                    self.made_progress = true;
                    break;
                }
                Batch::Overrun => {
                    // Recheck (the same belt-and-suspenders the receiver's
                    // DATA arm uses at its own Overrun-adjacent site): a
                    // prime racing concurrently with THIS call's own
                    // execution may not yet have been visible in `gen0`.
                    let gen1 = self.prime_generation.load(Ordering::Acquire);
                    let primed = gen0 != self.last_prime_gen || gen1 != self.last_prime_gen;
                    // Fix round 2 (Ruling R11 follow-up): a fleet-scale
                    // finding, not a synthetic corner — this agent has no
                    // admission door (spec §4.1, "outside the lag policy"),
                    // and the shared ring holds EVERY frame type, not just
                    // CLUSTER ones. A write-heavy workload on a small ring
                    // (`uc_node/tests/learner.rs`'s 24k-submit setup, 256 KiB
                    // buffer) reliably overruns a live, healthy leader's OWN
                    // cluster agent with NO prime anywhere in sight — plain
                    // scheduling arithmetic, not a stuck thread. Gating the
                    // panic on `primed` ALONE fail-stops perfectly healthy
                    // nodes under ordinary load, which is worse than the
                    // resync it replaces. `made_progress` narrows the panic
                    // to what it can actually still prove: a reader that has
                    // NEVER once caught up or read a frame, overrunning with
                    // no prime to explain it — i.e. broken from birth (a
                    // misconfigured buffer/cnc pairing), not merely slow.
                    // Once any progress has been observed, an unexplained
                    // Overrun is resynced like every other one; task 9's
                    // artifact-carrying snapshot install removes the need
                    // for any of this by replacing replay with an install.
                    if primed || self.made_progress {
                        self.last_prime_gen = gen1;
                        self.made_progress = true;
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
                        break;
                    }
                    // Never once made progress, and no prime explains this:
                    // broken from birth, the fail-stop the brief specified.
                    panic!(
                        "uc2-cluster: log buffer overrun at {}",
                        self.follower.cursor
                    );
                }
                Batch::Frames(iter) => {
                    self.made_progress = true;
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
    use uc_log::buffer::LogBuffer;
    use uc_log::cnc::{CncMeta, CncPage};
    use uc_log::region::Region;
    use uc_protocol::v2::cnc::CNC_MAX_SERVICES;
    use uc_protocol::v2::frame::ClusterKind;
    use uc_protocol::v2::schedule::ScheduleTable;
    use uc_protocol::v2::settings::{Settings, encode_settings};

    use super::*;

    /// A scratch directory on REAL DISK, never `/tmp` (RAM-backed tmpfs with
    /// no swap on the dev box — CLAUDE.md). `CARGO_TARGET_TMPDIR` is set only
    /// for integration-test binaries and these are inline unit tests in the
    /// lib target, so this falls back to a package-relative `target/`
    /// directory — the same helper shape `schedule_state.rs`'s tests use.
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

    #[test]
    #[should_panic(expected = "uc2-cluster: log buffer overrun")]
    fn an_overrun_without_a_prime_is_a_fail_stop() {
        let (buffer, cnc, dir) = world();
        let (fsm, start) = recover(dir.path(), genesis_state(), vec![]).unwrap();
        let view = Arc::new(ClusterView::new(fsm.state()));
        // Same staging as the resync test, but the generation is NEVER
        // bumped: nothing explains the gap, so this must fail-stop exactly
        // as the brief specified.
        let mut agent = ClusterAgent::new(
            Arc::clone(&buffer),
            Arc::clone(&cnc),
            fsm,
            view,
            dir.path().join("snapshots/cluster"),
            start,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        );
        stage_overrun(&buffer, &cnc);
        agent.do_work();
    }
}
