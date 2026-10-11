// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Backup-tier capstone (spec §11.6, plan Task 14, Review Focus 5): a
//! whole-cluster restore from the tier is linearizable up to the restore
//! point Q and shows nothing after it.
//!
//! Three voters and one LEARNER (the spec's recommended writer placement)
//! run the CAS-register workload of `lin_v2` while the learner's
//! `uc2-backup` writer copies one coordinated snapshot set S and the journal
//! after it into a filesystem tier. One leader is killed and restarted under
//! the load. Q is chosen as the frame START of a sentinel write — which is
//! the frame END of everything before it, so a legal restore point (ruling
//! R11a) — placed at least 4 × `fsm_lag` (64 KiB) past S, so a cold restore
//! must replay more than the lag bound before it can commit (Review Focus 5,
//! the #95 shape). The load runs on past Q, so there is acknowledged history
//! the restore must NOT show.
//!
//! Everything stops; every node is rebuilt from the tier with
//! `materialize(Recovery, Some(Q))` into a fresh directory; the cluster boots
//! on those directories. Then:
//!
//! 1. the pre-stop history TRUNCATED to what lies below Q — mutations whose
//!    frame position is `< Q`, reads that observed only such a prefix,
//!    indeterminate ops left in as optional — followed by post-restore reads
//!    and a post-restore write, is WGL-linearizable;
//! 2. nothing at or after Q is visible: no post-restore read returns the
//!    sentinel, the restored register's last applied frame is below Q, and it
//!    is at or above every acknowledged mutation below Q;
//! 3. a new write commits after the restore and is read back.
//!
//! Positions come from the state machine itself: [`PosRegister`] wraps
//! `RegisterSm` and answers every command with `ctx.position` (the frame's
//! START — included in a restore at Q iff `< Q`) and every query with its
//! `last_applied`.

#[path = "lincheck_v2/mod.rs"]
mod lincheck_v2;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use lincheck_v2::{
    ClusterCfg, LinClusterV2, ReadOutcome, SubmitOutcome, WorkerConn, read_leader, serialize,
    submit_cmd,
};
use uc_lincheck::checker::{Verdict, check_register};
use uc_lincheck::history::{Entry, History, Outcome};
use uc_lincheck::model::{Op, RegResp};
use uc_lincheck::register::{Cmd, CmdResp, RegisterSm};
use uc_net::fault::FaultConfig;
use uc_node::tier::chain::{list_tier, watermark_of};
use uc_node::tier::materialize::{Mode, materialize, restore_point};
use uc_node::tier::target::FsTarget;
use uc_node::tier::writer::BackupConfig;
use uc_service::{ApplyCtx, SnapshotError, SnapshotStateMachine, StateMachine};

const FSM_LAG: u64 = 64 * 1024;
/// Review Focus 5: Q − S must exceed the lag bound by a wide margin.
const MIN_REPLAY: u64 = 4 * FSM_LAG;
/// Outside every value the workers write (`1..1000`). Each sentinel
/// attempt writes `SENTINEL + attempt`, so the acknowledged one is told
/// apart from an indeterminate earlier attempt (which may have committed
/// BELOW Q and may legitimately be read after the restore).
const SENTINEL: u64 = 1_000_000;
/// The sentinel's client id in the history.
const SENTINEL_CLIENT: u32 = 99;
/// How many acknowledged non-sentinel mutations must land past Q before the
/// load stops — the non-vacuity of assertion 1's truncation.
const MIN_POST_Q: usize = 5;
/// The post-restore write (assertion 3).
const AFTER: u64 = 2_000_000;
const VOTERS: usize = 3;
const WRITER: usize = 3; // the learner's slot

// ------------------------------------------------------------ the register

/// `RegisterSm` that reports positions: a command answers `(resp,
/// ctx.position)`, a query `(value, last_applied)`. State, snapshot and
/// semantics are `RegisterSm`'s, untouched.
#[derive(Default)]
struct PosRegister(RegisterSm);

impl StateMachine for PosRegister {
    const NAME: &'static str = "pos-register";

    type Command = Cmd;
    type Response = (CmdResp, u64);
    type Query = ();
    type QueryResponse = (Option<u64>, Option<u64>);

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Cmd) -> (CmdResp, u64) {
        let pos = ctx.position;
        (StateMachine::apply(&mut self.0, ctx, cmd), pos)
    }
    fn query(&self, q: ()) -> (Option<u64>, Option<u64>) {
        (
            StateMachine::query(&self.0, q),
            StateMachine::last_applied(&self.0),
        )
    }
    fn last_applied(&self) -> Option<u64> {
        StateMachine::last_applied(&self.0)
    }
}

impl SnapshotStateMachine for PosRegister {
    type SnapshotHandle = Vec<u8>;
    fn freeze(&self) -> Result<(Vec<u8>, u64), SnapshotError> {
        self.0.freeze()
    }
    fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        RegisterSm::stream_snapshot(handle, dst)
    }
    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, SnapshotError> {
        self.0.install_snapshot(position, src)
    }
}

// --------------------------------------------------------------- the load

/// What an `Ok` op's position says about it: a mutation's frame START, or
/// the `last_applied` a read observed.
#[derive(Clone, Copy, Debug)]
enum Pos {
    Mut(u64),
    Read(Option<u64>),
}

/// History invoke stamp → position, for every `Ok` op.
type Positions = Arc<Mutex<HashMap<u64, Pos>>>;

/// One recorded op through `conn`: the shared discipline of `lin_v2`'s
/// worker (route via `submit_cmd` / `read_leader`, indeterminate never
/// retried), plus the position side channel.
fn do_op(
    conn: &mut WorkerConn,
    client: u32,
    op: Op,
    history: &History,
    positions: &Positions,
    deadline: Instant,
) -> (Outcome, Option<Pos>) {
    let inv = history.invoke();
    let (outcome, pos) = match op {
        Op::Read => match read_leader::<(), (Option<u64>, Option<u64>)>(conn, &(), deadline) {
            ReadOutcome::Ok((v, la)) => (Outcome::Ok(RegResp::Value(v)), Some(Pos::Read(la))),
            ReadOutcome::Indeterminate => (Outcome::Indeterminate, None),
            ReadOutcome::Fatal(e) => panic!("fatal read: {e}"),
        },
        Op::Write(v) => match submit_cmd::<_, (CmdResp, u64)>(conn, &Cmd::Write(v), deadline) {
            SubmitOutcome::Ok((CmdResp::WriteAck, p)) => {
                (Outcome::Ok(RegResp::Ack), Some(Pos::Mut(p)))
            }
            SubmitOutcome::Ok(other) => panic!("write answered {other:?}"),
            SubmitOutcome::Indeterminate => (Outcome::Indeterminate, None),
            SubmitOutcome::Fatal(e) => panic!("fatal write: {e}"),
        },
        Op::Cas { old, new } => {
            match submit_cmd::<_, (CmdResp, u64)>(conn, &Cmd::Cas { old, new }, deadline) {
                SubmitOutcome::Ok((CmdResp::CasResult(b), p)) => {
                    (Outcome::Ok(RegResp::CasOk(b)), Some(Pos::Mut(p)))
                }
                SubmitOutcome::Ok(other) => panic!("cas answered {other:?}"),
                SubmitOutcome::Indeterminate => (Outcome::Indeterminate, None),
                SubmitOutcome::Fatal(e) => panic!("fatal cas: {e}"),
            }
        }
    };
    history.record(client, op, inv, outcome.clone());
    if let Some(p) = pos {
        positions.lock().unwrap().insert(inv, p);
    }
    (outcome, pos)
}

/// `lin_v2`'s workload: a seeded mix of Write / Read / CAS, throttled, CAS
/// biased toward the last value seen so some succeed.
fn spawn_workers(
    dirs: &Arc<Vec<PathBuf>>,
    history: &Arc<History>,
    positions: &Positions,
    stop: &Arc<AtomicBool>,
    seed: u64,
) -> Vec<JoinHandle<()>> {
    const N_WORKERS: u32 = 3;
    const THROTTLE: Duration = Duration::from_millis(20);
    let last_seen = Arc::new(AtomicU64::new(0));
    (0..N_WORKERS)
        .map(|w| {
            let (dirs, history, positions, stop, last_seen) = (
                dirs.clone(),
                history.clone(),
                positions.clone(),
                stop.clone(),
                last_seen.clone(),
            );
            let mut rng = StdRng::seed_from_u64(seed ^ (w as u64).wrapping_mul(0x9E37_79B9));
            std::thread::spawn(move || {
                let mut conn = WorkerConn::new(dirs, w as usize);
                while !stop.load(Ordering::Relaxed) {
                    std::thread::sleep(THROTTLE);
                    let op = match rng.random_range(0..3u8) {
                        0 => Op::Write(rng.random_range(1..1000u64)),
                        1 => Op::Read,
                        _ => Op::Cas {
                            old: if rng.random_bool(0.7) {
                                last_seen.load(Ordering::Relaxed)
                            } else {
                                rng.random_range(1..1000u64)
                            },
                            new: rng.random_range(1..1000u64),
                        },
                    };
                    let deadline = Instant::now() + Duration::from_secs(15);
                    let (outcome, _) =
                        do_op(&mut conn, w, op.clone(), &history, &positions, deadline);
                    match (&op, outcome) {
                        (Op::Read, Outcome::Ok(RegResp::Value(Some(x)))) => {
                            last_seen.store(x, Ordering::Relaxed)
                        }
                        (Op::Cas { new, .. }, Outcome::Ok(RegResp::CasOk(true))) => {
                            last_seen.store(*new, Ordering::Relaxed)
                        }
                        _ => {}
                    }
                }
                conn.drop_client();
            })
        })
        .collect()
}

/// Log volume without history: `Cas { old: 0, .. }` never matches (the
/// register only ever holds values `>= 1`), so each is a state no-op — a
/// failed CAS is a read, and leaving a read out of the history is sound.
/// It pushes the log past S by more than `MIN_REPLAY` in seconds rather
/// than the minutes the throttled workers alone would take.
fn spawn_filler(dirs: &Arc<Vec<PathBuf>>, stop: &Arc<AtomicBool>) -> JoinHandle<u64> {
    let (dirs, stop) = (dirs.clone(), stop.clone());
    std::thread::spawn(move || {
        let mut conn = WorkerConn::new(dirs, 0);
        let mut n = 0u64;
        while !stop.load(Ordering::Relaxed) {
            let deadline = Instant::now() + Duration::from_secs(5);
            if let SubmitOutcome::Ok(_) =
                submit_cmd::<_, (CmdResp, u64)>(&mut conn, &Cmd::Cas { old: 0, new: 0 }, deadline)
            {
                n += 1;
            }
            std::thread::sleep(Duration::from_micros(200));
        }
        conn.drop_client();
        n
    })
}

fn tempdir(prefix: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

fn await_until<T>(secs: u64, what: &str, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if let Some(t) = f() {
            return t;
        }
        assert!(Instant::now() < deadline, "{what} within {secs}s");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn dump_history(entries: &[Entry], positions: &HashMap<u64, Pos>, seed: u64) {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
    let path = dir.join(format!("lin_backup_tier_history_{seed}.txt"));
    let mut s = String::new();
    for e in entries {
        s.push_str(&format!("{e:?} pos={:?}\n", positions.get(&e.invoke)));
    }
    let _ = std::fs::write(&path, s);
    eprintln!(
        "history ({} entries) dumped to {}",
        entries.len(),
        path.display()
    );
}

// ------------------------------------------------------------- the capstone

#[test]
fn a_whole_cluster_restore_is_linearizable_up_to_q_and_shows_nothing_after() {
    const DEFAULT_SEED: u64 = 0x7134;
    const LOAD: Duration = Duration::from_secs(8);
    let seed: u64 = std::env::var("LIN_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_SEED);

    let _g = serialize();
    let wall = Instant::now();
    let root = tempdir("uc2-lin-tier-");
    let tier = tempdir("uc2-lin-tier-target-");
    let backup = BackupConfig {
        target: tier.path().to_path_buf(),
        quiet_interval: Duration::from_millis(500),
        ..BackupConfig::default()
    };
    let ccfg = ClusterCfg {
        learners: 1,
        fsm_lag_bytes: FSM_LAG,
        ..ClusterCfg::default()
    };
    let mut cluster: LinClusterV2<PosRegister> = LinClusterV2::start_cfg_backup(
        root.path(),
        VOTERS,
        FaultConfig::default(),
        ccfg,
        Some((WRITER, backup)),
    );
    cluster.await_single_serving(30);
    assert!(
        cluster.node(WRITER).unwrap().backup_writer().is_some(),
        "the learner runs the writer"
    );

    let voter_dirs = Arc::new(cluster.dirs()[..VOTERS].to_vec());
    let history = Arc::new(History::default());
    let positions: Positions = Arc::default();
    let stop_workers = Arc::new(AtomicBool::new(false));
    let stop_filler = Arc::new(AtomicBool::new(false));
    let workers = spawn_workers(&voter_dirs, &history, &positions, &stop_workers, seed);
    let filler = spawn_filler(&voter_dirs, &stop_filler);
    let load_start = Instant::now();

    // The one set S, commanded early and under load — but only once the
    // register has applied something: a `RegisterSm` frozen before its first
    // command records no cursor (`last_applied == None`), and the start-set
    // install refuses an image that leaves the cursor at `None` (see the
    // task report — a finding outside the tier).
    await_until(30, "the register applied its first mutations", || {
        let n = positions
            .lock()
            .unwrap()
            .values()
            .filter(|p| matches!(p, Pos::Mut(_)))
            .count();
        (n >= 20).then_some(())
    });
    let s_set = cluster.instant_until_complete(30);
    let t = FsTarget::new(tier.path());
    await_until(60, "the writer copied set S to the tier", || {
        let l = list_tier(&t).ok()?;
        l.sets.iter().any(|s| s.position == s_set).then_some(())
    });

    // One leader kill in the middle of the load.
    std::thread::sleep(LOAD / 2);
    cluster.kill_and_restart_leader();
    let mut respawned = cluster.supervise_services();

    // Run the load out, and far enough past S that a restore at the
    // sentinel replays more than 4 × fsm_lag. Throughput-bound, so the
    // budget is a hang guard sized for nightly's DEBUG build on a 4-vCPU
    // runner: a debug run on a 32-thread dev box spent ~17 s here (the
    // whole load phase 26.5 s against `LOAD` = 8 s).
    await_until(180, "the log passed S + 4 x fsm_lag", || {
        respawned += cluster.supervise_services();
        let li = cluster.leader()?;
        let commit = cluster.node(li)?.counters().commit.load_acquire();
        (load_start.elapsed() >= LOAD && commit > s_set + MIN_REPLAY + 16 * 1024).then_some(())
    });

    // The sentinel: its frame start is Q. Retried until it is acknowledged
    // (an indeterminate attempt stays in the history as optional).
    let mut conn = WorkerConn::new(voter_dirs.clone(), 0);
    let mut attempt = 0u64;
    let (q, sentinel) = await_until(60, "the sentinel write acknowledged", || {
        attempt += 1;
        let v = SENTINEL + attempt;
        match do_op(
            &mut conn,
            SENTINEL_CLIENT,
            Op::Write(v),
            &history,
            &positions,
            Instant::now() + Duration::from_secs(10),
        ) {
            (_, Some(Pos::Mut(p))) => Some((p, v)),
            _ => None,
        }
    });
    conn.drop_client();

    // History the restore must not show: keep loading until the workers
    // have acknowledged MIN_POST_Q mutations at or past Q (awaited, not a
    // fixed sleep), then let the tier's chain reach past it.
    await_until(60, "workers acknowledged mutations past Q", || {
        let n = positions
            .lock()
            .unwrap()
            .values()
            .filter(|p| matches!(p, Pos::Mut(m) if *m > q))
            .count();
        (n >= MIN_POST_Q).then_some(())
    });
    stop_workers.store(true, Ordering::Relaxed);
    for h in workers {
        h.join().expect("worker");
    }
    let through = await_until(60, "the tier's through passed Q", || {
        let (_, _, through) = watermark_of(&list_tier(&t).ok()?)?;
        (through > q).then_some(through)
    });
    stop_filler.store(true, Ordering::Relaxed);
    let fillers = filler.join().expect("filler");
    cluster.stop_all();
    let load_secs = load_start.elapsed().as_secs_f64();

    // Rebuild every node from the tier as of Q.
    let listing = list_tier(&t).expect("list tier");
    let (s, q_checked) = restore_point(&listing, Some(q)).expect("Q is restorable");
    assert_eq!(q_checked, q);
    assert_eq!(s, s_set, "the one set commanded is the restore's start");
    assert!(
        q - s >= MIN_REPLAY,
        "Review Focus 5: Q - S = {} < 4 x fsm_lag",
        q - s
    );
    let restored = tempdir("uc2-lin-tier-restored-");
    let dirs: Vec<PathBuf> = (0..VOTERS + 1)
        .map(|i| {
            let d = restored.path().join(format!("r{i}"));
            let m = materialize(&t, Some(q), &d, Mode::Recovery, 1).expect("materialize");
            assert_eq!((m.start_set, m.q), (s, q));
            d
        })
        .collect();
    let restore_mark = history.invoke();
    let boot = Instant::now();
    cluster.restart_on_dirs(&dirs, None);
    cluster.await_single_serving(60);
    let elect_secs = boot.elapsed().as_secs_f64();

    // Post-restore reads (assertion 2), then a new write (assertion 3).
    let new_dirs = Arc::new(cluster.dirs()[..VOTERS].to_vec());
    let mut conn = WorkerConn::new(new_dirs, 0);
    let mut seen = Vec::new();
    for _ in 0..5 {
        match do_op(
            &mut conn,
            100,
            Op::Read,
            &history,
            &positions,
            Instant::now() + Duration::from_secs(30),
        ) {
            (Outcome::Ok(RegResp::Value(v)), Some(Pos::Read(la))) => seen.push((v, la)),
            other => panic!("post-restore read did not answer: {other:?}"),
        }
    }
    let after = match do_op(
        &mut conn,
        100,
        Op::Write(AFTER),
        &history,
        &positions,
        Instant::now() + Duration::from_secs(30),
    ) {
        (_, Some(Pos::Mut(p))) => p,
        other => panic!("assertion 3: no write committed after the restore: {other:?}"),
    };
    let (readback, _) = do_op(
        &mut conn,
        100,
        Op::Read,
        &history,
        &positions,
        Instant::now() + Duration::from_secs(30),
    );
    conn.drop_client();
    cluster.stop();

    // ---- the verdicts
    let entries = Arc::try_unwrap(history)
        .ok()
        .expect("sole history owner")
        .into_entries();
    let positions = positions.lock().unwrap().clone();

    // Assertion 2: nothing at or after Q.
    let max_kept_mut = entries
        .iter()
        .filter(|e| e.invoke < restore_mark)
        .filter_map(|e| match positions.get(&e.invoke) {
            Some(Pos::Mut(p)) if *p < q => Some(*p),
            _ => None,
        })
        .max()
        .expect("acknowledged mutations below Q");
    for &(v, la) in &seen {
        assert_ne!(
            v,
            Some(sentinel),
            "the acknowledged write AT Q is visible after restore"
        );
        let la = la.expect("the restored register applied something");
        assert!(la < q, "the restored register applied {la} >= Q {q}");
        assert!(
            la >= max_kept_mut,
            "the restore lost an acknowledged mutation: last applied {la} < {max_kept_mut}"
        );
    }
    // Assertion 3.
    assert!(after >= q, "the new write sits past Q ({after} < {q})");
    assert!(
        matches!(readback, Outcome::Ok(RegResp::Value(Some(AFTER)))),
        "the new write is not read back: {readback:?}"
    );

    // Assertion 1: truncate the pre-stop history to what lies below Q.
    // `dropped` counts acknowledged NON-sentinel ops at or past Q: the
    // sentinel itself always lies at Q, so counting it made the
    // non-vacuity check below vacuous.
    let (mut kept_ok, mut kept_ind, mut dropped) = (0usize, 0usize, 0usize);
    let truncated: Vec<Entry> = entries
        .iter()
        .filter(|e| {
            if e.invoke >= restore_mark {
                return true;
            }
            let keep = match (&e.outcome, positions.get(&e.invoke)) {
                (Outcome::Indeterminate, _) => {
                    kept_ind += 1;
                    return true;
                }
                (_, Some(Pos::Mut(p))) => *p < q,
                (_, Some(Pos::Read(la))) => la.is_none_or(|la| la < q),
                (_, None) => unreachable!("an Ok op without a position"),
            };
            if keep {
                kept_ok += 1;
            } else if e.client != SENTINEL_CLIENT {
                dropped += 1;
            }
            keep
        })
        .cloned()
        .collect();
    let post = entries.len() - entries.iter().filter(|e| e.invoke < restore_mark).count();
    eprintln!(
        "[lin_backup_tier] seed={seed} S={s} Q={q} Q-S={} ({:.1} x fsm_lag) through={through} \
         kept_ok={kept_ok} kept_indeterminate={kept_ind} dropped={dropped} post={post} \
         fillers={fillers} respawned={respawned} load={load_secs:.1}s restart_to_leader={elect_secs:.1}s \
         wall={:.1}s",
        q - s,
        (q - s) as f64 / FSM_LAG as f64,
        wall.elapsed().as_secs_f64()
    );
    assert!(
        dropped >= MIN_POST_Q,
        "non-vacuity: only {dropped} acknowledged non-sentinel ops lay at or after Q"
    );
    match check_register(&truncated) {
        Verdict::Linearizable => {}
        Verdict::Violation => {
            dump_history(&entries, &positions, seed);
            panic!("LINEARIZABILITY VIOLATION across the restore (seed={seed})");
        }
        Verdict::Inconclusive => {
            dump_history(&entries, &positions, seed);
            panic!("checker Inconclusive (seed={seed})");
        }
    }
}
