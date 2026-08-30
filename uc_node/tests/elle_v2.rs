// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Elle consistency-harness driver (design spec 2026-07-15). Each `#[ignore]`d
//! test is one PASS: it boots a LinClusterV2 running `ListAppendSm`, drives
//! UNTHROTTLED seeded workers through the singleton-txn list-append workload,
//! runs the pass's nemesis arms, and writes `$ELLE_DIR/<pass>/history.edn`
//! for `scripts/elle_check.sh` to adjudicate (serializable + strict model,
//! anomaly set must be empty). Never in the default `cargo test`:
//!
//! ```bash
//! cargo test -p uc_node --release --test elle_v2 -- --ignored --exact elle_quiet
//! ```
//!
//! Elle semantics (vs the WGL harness): `:fail` = guaranteed-not-committed
//! only; maybe-committed appends are `:info` and RETIRE the worker's process
//! id (a Jepsen process may not act after an indeterminate outcome). Failed
//! reads are `:fail` (no side effect). Append values come from one global
//! AtomicU64 — unique across all workers and retries.

#[path = "lincheck_v2/mod.rs"]
mod lincheck_v2;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use lincheck_v2::{
    ClusterCfg, FsmSet, LinClusterV2, ReadOutcome, SubmitOutcome, WorkerConn, read_leader,
    read_leader_on, serialize, submit_all_cmd, submit_cmd,
};
#[cfg(feature = "mutation-testing")]
use lincheck_v2::CommittedTruncationWitness;
use uc_net::fault::FaultConfig;
use uc_lincheck::edn::{EdnOp, EdnRecorder, EdnType};
use uc_lincheck::list_append::{LaCmd, LaRead, LaResp, ListAppendSm};

// ------------------------------------------------------------------ env knobs

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

#[cfg_attr(not(feature = "mutation-testing"), allow(dead_code))]
fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

/// M8 Task 15: `UC2_CRYPTO=1` re-runs the clean elle tier with wire crypto
/// enabled on every node (see `lincheck_v2::ClusterCfg::crypto`) — honored
/// uniformly by every pass through [`run_pass`], mirroring `scripts/
/// elle_check.sh`'s own env-switch convention.
fn crypto_from_env() -> bool {
    std::env::var("UC2_CRYPTO").ok().as_deref() == Some("1")
}

fn elle_dir() -> PathBuf {
    // Default to DISK, never /tmp: /tmp is RAM-backed tmpfs with no swap on this
    // box and a 50k-event history OOM-kills the run (see CLAUDE.md). The
    // elle_check.sh / elle_mutation.sh wrappers set ELLE_DIR to $HOME/.cache too.
    std::env::var("ELLE_DIR").map(PathBuf::from).unwrap_or_else(|_| {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
        PathBuf::from(home).join(".cache/uc2-elle")
    })
}

/// Instance dirs on ext4 (journal segments blow the tmpfs /tmp quota).
fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("uc2-elle-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

// ------------------------------------------------------------------- worker

/// One unthrottled worker: `read_frac` reads / `1 - read_frac` appends over
/// `keys` hot keys. Clean-tier passes call with `read_frac = 0.5`,
/// `fatal_tolerant = false` (a `Fatal` outcome then panics — a real client bug
/// on a healthy cluster). The mutation passes pass `fatal_tolerant = true`: an
/// injected safety bug can crash a node (e.g. UC's truncation-below-commit
/// assertion under `skip-vote-order-check`), surfacing as a `ClientError::ShutDown`
/// on the pinned node; treat that like Indeterminate so the history is still
/// written and elle can adjudicate what the client observed.
#[allow(clippy::too_many_arguments)]
fn elle_worker(
    id: u32,
    dirs: Arc<Vec<PathBuf>>,
    rec: Arc<EdnRecorder>,
    stop: Arc<AtomicBool>,
    mut rng: StdRng,
    keys: u32,
    values: Arc<AtomicU64>,
    read_frac: f64,
    fatal_tolerant: bool,
    op_deadline: Duration,
) {
    let mut conn = WorkerConn::new(dirs, id as usize);
    // Initial process ids 0..n_workers are pre-allocated by EdnRecorder::new.
    let mut process = id as u64;
    while !stop.load(Ordering::Relaxed) {
        let deadline = Instant::now() + op_deadline;
        let key = rng.random_range(0..keys);
        if !rng.random_bool(read_frac) {
            let val = values.fetch_add(1, Ordering::Relaxed);
            let op = EdnOp::Append { key, val };
            rec.record(EdnType::Invoke, process, &op);
            match submit_cmd::<_, LaResp>(&mut conn, &LaCmd::Append { key, val }, deadline) {
                SubmitOutcome::Ok(LaResp::AppendAck) => rec.record(EdnType::Ok, process, &op),
                // Maybe-committed: :info, then this process id never acts again.
                SubmitOutcome::Indeterminate => {
                    rec.record(EdnType::Info, process, &op);
                    process = rec.retire();
                }
                // A crashed node is maybe-committed from the client's view: :info + retire.
                SubmitOutcome::Fatal(_) if fatal_tolerant => {
                    rec.record(EdnType::Info, process, &op);
                    process = rec.retire();
                    conn.drop_client();
                }
                SubmitOutcome::Fatal(e) => panic!("fatal submit: {e}"),
            }
        } else {
            let op = EdnOp::Read { key, result: None };
            rec.record(EdnType::Invoke, process, &op);
            match read_leader::<LaRead, Vec<u64>>(&mut conn, &LaRead { key }, deadline) {
                ReadOutcome::Ok(list) => {
                    rec.record(EdnType::Ok, process, &EdnOp::Read { key, result: Some(list) });
                }
                // Reads have no side effect: a lost read definitely didn't
                // happen — :fail, and the process may continue.
                ReadOutcome::Indeterminate => rec.record(EdnType::Fail, process, &op),
                ReadOutcome::Fatal(_) if fatal_tolerant => {
                    rec.record(EdnType::Fail, process, &op);
                    conn.drop_client();
                }
                ReadOutcome::Fatal(e) => panic!("fatal read: {e}"),
            }
        }
    }
    conn.drop_client();
}

/// M14c2: [`elle_worker`]'s two-FSM twin. Appends fan in via `submit_all_cmd`
/// and are recorded into BOTH per-FSM recorders from the ids/values `Ok`
/// actually returned: `LaResp` has one variant, so "equal" collapses to "both
/// present, ids exactly {0, 1}, ascending" — but a missing id or a non-ascending
/// pair is exactly the shape a future multi-variant response would also need
/// caught, so the general vector check stays even though this SM can't
/// currently produce an unequal pair. An unequal (or malformed) `Ok` — or an
/// `Indeterminate` — is `:info` in both and retires BOTH recorders (each keeps
/// its own process id; they retire independently). Reads alternate FSM: even
/// iterations go through [`read_leader`] (FSM 0, `rec0`), odd through
/// [`read_leader_on`] (FSM 1, `rec1`). `Fatal` panics — the quiet pass is not
/// fatal-tolerant.
#[allow(clippy::too_many_arguments)]
fn elle_worker2(
    id: u32,
    dirs: Arc<Vec<PathBuf>>,
    rec0: Arc<EdnRecorder>,
    rec1: Arc<EdnRecorder>,
    equiv: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    mut rng: StdRng,
    keys: u32,
    values: Arc<AtomicU64>,
    read_frac: f64,
    op_deadline: Duration,
) {
    let mut conn = WorkerConn::new(dirs, id as usize);
    // Initial process ids 0..n_workers are pre-allocated by EdnRecorder::new,
    // identically on both recorders; they retire independently afterwards.
    let mut process0 = id as u64;
    let mut process1 = id as u64;
    let mut iter: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        let deadline = Instant::now() + op_deadline;
        let key = rng.random_range(0..keys);
        if !rng.random_bool(read_frac) {
            let val = values.fetch_add(1, Ordering::Relaxed);
            let op = EdnOp::Append { key, val };
            rec0.record(EdnType::Invoke, process0, &op);
            rec1.record(EdnType::Invoke, process1, &op);
            match submit_all_cmd::<_, LaResp>(&mut conn, &LaCmd::Append { key, val }, deadline) {
                SubmitOutcome::Ok(answers) => {
                    let equal = answers.len() == 2
                        && answers[0].0 == 0
                        && answers[1].0 == 1
                        && answers[0].1 == answers[1].1;
                    if equal {
                        rec0.record(EdnType::Ok, process0, &op);
                        rec1.record(EdnType::Ok, process1, &op);
                    } else {
                        rec0.record(EdnType::Info, process0, &op);
                        rec1.record(EdnType::Info, process1, &op);
                        equiv.fetch_add(1, Ordering::Relaxed);
                        process0 = rec0.retire();
                        process1 = rec1.retire();
                    }
                }
                // Maybe-committed on one or both FSMs: :info on both, retire both.
                SubmitOutcome::Indeterminate => {
                    rec0.record(EdnType::Info, process0, &op);
                    rec1.record(EdnType::Info, process1, &op);
                    process0 = rec0.retire();
                    process1 = rec1.retire();
                }
                SubmitOutcome::Fatal(e) => panic!("fatal submit_all: {e}"),
            }
        } else {
            let op = EdnOp::Read { key, result: None };
            if iter.is_multiple_of(2) {
                rec0.record(EdnType::Invoke, process0, &op);
                match read_leader::<LaRead, Vec<u64>>(&mut conn, &LaRead { key }, deadline) {
                    ReadOutcome::Ok(list) => rec0.record(
                        EdnType::Ok,
                        process0,
                        &EdnOp::Read { key, result: Some(list) },
                    ),
                    ReadOutcome::Indeterminate => rec0.record(EdnType::Fail, process0, &op),
                    ReadOutcome::Fatal(e) => panic!("fatal read fsm0: {e}"),
                }
            } else {
                rec1.record(EdnType::Invoke, process1, &op);
                match read_leader_on::<LaRead, Vec<u64>>(&mut conn, 1, &LaRead { key }, deadline) {
                    ReadOutcome::Ok(list) => rec1.record(
                        EdnType::Ok,
                        process1,
                        &EdnOp::Read { key, result: Some(list) },
                    ),
                    ReadOutcome::Indeterminate => rec1.record(EdnType::Fail, process1, &op),
                    ReadOutcome::Fatal(e) => panic!("fatal read fsm1: {e}"),
                }
            }
            iter = iter.wrapping_add(1);
        }
    }
    conn.drop_client();
}

// ------------------------------------------------------------------ run_pass

/// Drive one pass: boot, spawn workers, tick the nemesis every `fault_period`
/// until the op target AND the pass's non-vacuity hold (or the budget runs
/// out), then write `$ELLE_DIR/<name>/history.edn` (+ a `seed` sidecar) and
/// assert the liveness/non-vacuity gates. The `default_workers` may be overridden
/// by `$ELLE_WORKERS`.
#[allow(clippy::too_many_arguments)]
fn run_pass<F, V>(
    name: &str,
    mut ccfg: ClusterCfg,
    default_target_ops: u64,
    default_workers: u64,
    min_ok_pct: u64,
    fault_period: Duration,
    mut nemesis_tick: F,
    non_vacuous: V,
    vacuity_label: &str,
) where
    F: FnMut(&mut LinClusterV2<ListAppendSm>, &mut StdRng, u32),
    V: Fn(&LinClusterV2<ListAppendSm>, u32) -> bool,
{
    let seed = env_u64("ELLE_SEED", 0x1107);
    let n_workers = env_u64("ELLE_WORKERS", default_workers) as u32;
    let keys = env_u64("ELLE_KEYS", 8) as u32;
    let target = env_u64("ELLE_TARGET_OPS", default_target_ops);
    let budget = Duration::from_secs(env_u64("ELLE_BUDGET_SECS", 120));
    // M8 Task 15: honor UC2_CRYPTO=1 uniformly across every pass.
    ccfg.crypto = crypto_from_env();

    let _g = serialize();
    let dir = tempdir();
    let mut cluster =
        LinClusterV2::<ListAppendSm>::start_cfg(dir.path(), 3, FaultConfig::default(), ccfg);
    let leader0 = cluster.await_single_serving(30);
    // Anti-vacuity (M8 Task 15): if crypto was supposed to be on, the elected
    // leader must have actually MINTED a group epoch — proof the switch did
    // something real, not just that the cluster formed (which it would do
    // even if `crypto` silently had no effect).
    if ccfg.crypto {
        assert!(
            cluster.crypto_epoch_of(leader0).is_some(),
            "[elle {name}] UC2_CRYPTO=1 but the elected leader never minted a crypto group \
             epoch — wire crypto did not actually engage"
        );
    }

    let dirs = Arc::new(cluster.dirs());
    let rec = Arc::new(EdnRecorder::new(n_workers as u64));
    let stop = Arc::new(AtomicBool::new(false));
    let values = Arc::new(AtomicU64::new(1));

    let handles: Vec<_> = (0..n_workers)
        .map(|w| {
            let rng = StdRng::seed_from_u64(seed ^ (w as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let (dirs, rec, stop, values) =
                (Arc::clone(&dirs), Arc::clone(&rec), Arc::clone(&stop), Arc::clone(&values));
            std::thread::spawn(move || {
                elle_worker(w, dirs, rec, stop, rng, keys, values, 0.5, false, Duration::from_secs(15))
            })
        })
        .collect();

    let mut frng = StdRng::seed_from_u64(seed ^ 0xFA17);
    let mut faults = 0u32;
    let start = Instant::now();
    let mut respawns = 0usize;
    while rec.ok_count() < target || !non_vacuous(&cluster, faults) {
        std::thread::sleep(fault_period);
        // Stand in for the production supervisor BEFORE the next fault, so a
        // fail-stopped service is reconstructed rather than silently absent
        // for the rest of the pass (and re-raised at teardown).
        respawns += cluster.supervise_services();
        nemesis_tick(&mut cluster, &mut frng, faults);
        faults += 1;
        if start.elapsed() > budget {
            break;
        }
    }
    let elapsed = start.elapsed();
    let vacuity_ok = non_vacuous(&cluster, faults);

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        if let Err(e) = h.join() {
            std::panic::resume_unwind(e);
        }
    }
    cluster.stop();

    let out = elle_dir().join(name);
    rec.write_to(&out.join("history.edn")).expect("write history");
    std::fs::write(out.join("seed"), format!("{seed}\n")).expect("write seed");
    // M8 Task 15 review fix: record the posture this history was ACTUALLY
    // generated under, beside the existing `seed` sidecar. `scripts/
    // elle_check.sh`'s cache-reuse check (`[ ! -f "$hist" ]`) is otherwise
    // blind to a stale ELLE_DIR crossing the crypto boundary — a directory
    // of crypto histories re-adjudicated under `UC2_CRYPTO=0` (or the more
    // dangerous mirror: `UC2_CRYPTO=1` against cleartext histories) would
    // print a posture it never actually ran. The sidecar lets the script
    // refuse that mismatch instead of silently trusting the caller's ask.
    std::fs::write(out.join("crypto"), if ccfg.crypto { "1\n" } else { "0\n" }).expect("write crypto sidecar");

    let (ok, completed) = (rec.ok_count(), rec.completed_count());
    eprintln!(
        "[elle {name}] seed={seed} faults={faults} respawns={respawns} \
         completed={completed} ok={ok} elapsed={:.1}s -> {}",
        elapsed.as_secs_f64(),
        out.join("history.edn").display()
    );
    assert!(vacuity_ok, "vacuous {name} pass: {vacuity_label} (faults={faults})");
    assert!(
        ok * 100 >= completed * min_ok_pct,
        "liveness: only {ok}/{completed} ops Ok (<{min_ok_pct}%) in the {name} pass"
    );
}

// ------------------------------------------------------------------- passes

/// Quiet pass: no faults — the baseline history and the biggest cycle-search
/// load for elle-cli (largest event count).
#[test]
#[ignore]
fn elle_quiet() {
    run_pass(
        "quiet",
        ClusterCfg::default(),
        50_000,
        4,
        90,
        Duration::from_millis(100),
        |_cluster, _rng, _faults| {},
        |_cluster, _faults| true,
        "unreachable",
    );
}

/// M14c2: [`run_pass`]'s twin for the two-FSM quiet pass — deliberately NOT
/// folded back into `run_pass` (that would need a generic
/// one-or-two-recorder shape for every other pass too, for no present
/// benefit). No nemesis, no crash-vacuity gate — same posture as
/// [`elle_quiet`], just fanned out over `FsmSet::Two`. Writes ONE
/// `history.edn` per FSM under `$ELLE_DIR/quiet_two_fsm/fsm{0,1}/`, with the
/// `seed`/`crypto` sidecars at the PASS level (`$ELLE_DIR/quiet_two_fsm/`) —
/// that is where `scripts/elle_check.sh` looks for them.
fn run_pass2() {
    let name = "quiet_two_fsm";
    let default_target_ops = 50_000u64;
    let default_workers = 4u64;
    let min_ok_pct = 90u64;
    let fault_period = Duration::from_millis(100);

    let seed = env_u64("ELLE_SEED", 0x1107);
    let n_workers = env_u64("ELLE_WORKERS", default_workers) as u32;
    let keys = env_u64("ELLE_KEYS", 8) as u32;
    let target = env_u64("ELLE_TARGET_OPS", default_target_ops);
    let budget = Duration::from_secs(env_u64("ELLE_BUDGET_SECS", 120));
    let ccfg = ClusterCfg {
        services: FsmSet::Two { lag: uc_node::FsmLag::Bounded(64 * 1024) },
        crypto: crypto_from_env(),
        ..ClusterCfg::default()
    };

    let _g = serialize();
    let dir = tempdir();
    let cluster =
        LinClusterV2::<ListAppendSm>::start_cfg(dir.path(), 3, FaultConfig::default(), ccfg);
    let leader0 = cluster.await_single_serving(30);
    if ccfg.crypto {
        assert!(
            cluster.crypto_epoch_of(leader0).is_some(),
            "[elle {name}] UC2_CRYPTO=1 but the elected leader never minted a crypto group \
             epoch — wire crypto did not actually engage"
        );
    }

    let dirs = Arc::new(cluster.dirs());
    let rec0 = Arc::new(EdnRecorder::new(n_workers as u64));
    let rec1 = Arc::new(EdnRecorder::new(n_workers as u64));
    let equiv = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let values = Arc::new(AtomicU64::new(1));

    let handles: Vec<_> = (0..n_workers)
        .map(|w| {
            let rng = StdRng::seed_from_u64(seed ^ (w as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let (dirs, rec0, rec1, equiv, stop, values) = (
                Arc::clone(&dirs),
                Arc::clone(&rec0),
                Arc::clone(&rec1),
                Arc::clone(&equiv),
                Arc::clone(&stop),
                Arc::clone(&values),
            );
            std::thread::spawn(move || {
                elle_worker2(
                    w,
                    dirs,
                    rec0,
                    rec1,
                    equiv,
                    stop,
                    rng,
                    keys,
                    values,
                    0.5,
                    Duration::from_secs(15),
                )
            })
        })
        .collect();

    let start = Instant::now();
    while rec0.ok_count() < target || rec1.ok_count() < target {
        std::thread::sleep(fault_period);
        if start.elapsed() > budget {
            break;
        }
    }
    let elapsed = start.elapsed();

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        if let Err(e) = h.join() {
            std::panic::resume_unwind(e);
        }
    }
    cluster.stop();

    let out = elle_dir().join(name);
    let out0 = out.join("fsm0");
    let out1 = out.join("fsm1");
    std::fs::create_dir_all(&out0).expect("mkdir fsm0");
    std::fs::create_dir_all(&out1).expect("mkdir fsm1");
    rec0.write_to(&out0.join("history.edn")).expect("write history fsm0");
    rec1.write_to(&out1.join("history.edn")).expect("write history fsm1");
    std::fs::write(out.join("seed"), format!("{seed}\n")).expect("write seed");
    // Same crypto-posture sidecar convention as `run_pass` (see its comment).
    std::fs::write(out.join("crypto"), if ccfg.crypto { "1\n" } else { "0\n" })
        .expect("write crypto sidecar");

    let (ok0, completed0) = (rec0.ok_count(), rec0.completed_count());
    let (ok1, completed1) = (rec1.ok_count(), rec1.completed_count());
    let equiv_count = equiv.load(Ordering::Relaxed);
    eprintln!(
        "[elle {name}] seed={seed} completed0={completed0} ok0={ok0} completed1={completed1} \
         ok1={ok1} equiv={equiv_count} elapsed={:.1}s -> {} , {}",
        elapsed.as_secs_f64(),
        out0.join("history.edn").display(),
        out1.join("history.edn").display()
    );
    assert_eq!(
        equiv_count, 0,
        "{name}: {equiv_count} submit_all_cmd answers disagreed across FSMs"
    );
    assert!(
        ok0 * 100 >= completed0 * min_ok_pct,
        "liveness: only {ok0}/{completed0} ops Ok (<{min_ok_pct}%) on fsm0 in the {name} pass"
    );
    assert!(
        ok1 * 100 >= completed1 * min_ok_pct,
        "liveness: only {ok1}/{completed1} ops Ok (<{min_ok_pct}%) on fsm1 in the {name} pass"
    );
}

/// Two-FSM quiet pass (M14c2): identical posture to [`elle_quiet`] — no
/// faults — but every op fans in across BOTH declared FSMs (`FsmSet::Two`,
/// bounded 64 KiB lag), writing one history per FSM for `elle_check.sh` to
/// adjudicate independently.
#[test]
#[ignore]
fn elle_quiet_two_fsm() {
    run_pass2();
}

/// Failover pass: the lin_v2 failover capstone's fault mix — leader node
/// kill+restart vs leader service crash+restart, 50/50, one quorum-preserving
/// fault at a time. Also the catch vehicle for the `commit-quorum-minus-one`
/// and `skip-vote-order-check` mutations (elle_mutation.sh).
#[test]
#[ignore]
fn elle_failover() {
    run_pass(
        "failover",
        ClusterCfg::default(),
        20_000,
        4,
        70,
        Duration::from_secs(1),
        |cluster, rng, _faults| {
            if rng.random_bool(0.5) {
                cluster.kill_and_restart_leader();
            } else {
                cluster.crash_and_restart_leader_service();
            }
        },
        |_cluster, faults| faults >= 3,
        "fewer than 3 faults landed",
    );
}

/// Partition pass (spec deviation, approved): isolate-then-heal cycles — 2/3
/// leader isolation (a deposed-but-alive leader is the stale-read window the
/// `skip-read-barrier` mutation needs), 1/3 minority isolation. Clean runs
/// must stay anomaly-free under the strict model: the barrier is exactly what
/// makes a partitioned leader refuse stale answers.
#[test]
#[ignore]
fn elle_partition() {
    run_pass(
        "partition",
        ClusterCfg::default(),
        20_000,
        4,
        60,
        Duration::from_millis(1200),
        |cluster, rng, _faults| {
            if rng.random_bool(2.0 / 3.0) {
                let _ = cluster.partition_leader();
            } else {
                let _ = cluster.partition_minority();
            }
            std::thread::sleep(Duration::from_millis(800));
            cluster.heal();
            cluster.await_reconverged(20);
        },
        |_cluster, faults| faults >= 3,
        "fewer than 3 partition cycles landed",
    );
}

/// Purge pass: the M6 purge-churn capstone's posture — aggressive
/// snapshot-backed purge (16 KiB segments, 32 KiB snapshot cadence, zero
/// slack) with the follower-service-crash arm forcing below-floor
/// snapshot-install reconstruction. Non-vacuity: the archive floor advanced.
#[test]
#[ignore]
fn elle_purge() {
    let ccfg = ClusterCfg {
        purge: uc_node::PurgePolicy::BelowSnapshot { slack_bytes: 0 },
        journal_segment_bytes: 16 * 1024,
        snapshot_interval_bytes: 32 * 1024,
        spare_node: false,
        crypto: false,
        ..ClusterCfg::default()
    };
    run_pass(
        "purge",
        ccfg,
        20_000,
        4,
        70,
        Duration::from_millis(1200),
        |cluster, rng, _faults| match rng.random_range(0..3u8) {
            0 => cluster.kill_and_restart_leader(),
            1 => cluster.crash_and_restart_leader_service(),
            _ => cluster.crash_and_restart_random_follower_service(rng),
        },
        |cluster, _faults| cluster.max_archive_first_base() > 0,
        "purge never advanced the archive floor",
    );
}

/// Reconfig pass: the M7 reconfig-churn capstone's four arms — leader kill
/// (gated off while the spare is a voter), follower service crash, a short
/// minority partition (same gate), and one step of the spare's
/// add/promote/demote/remove cycle. Non-vacuity: >= 3 accepted config ops.
#[test]
#[ignore]
fn elle_reconfig() {
    let ccfg = ClusterCfg { spare_node: true, ..ClusterCfg::default() };
    // Reconfig's history size is driven by time-to-config-non-vacuity × worker
    // throughput (not the op target); at 4 workers the ~195k-event history stalls
    // elle-cli's strong-serializable cycle search — 1 worker keeps it within
    // checker capacity (~44k events after earlier passes, clean verdicts).
    run_pass(
        "reconfig",
        ccfg,
        20_000,
        1,
        70,
        Duration::from_millis(1200),
        |cluster, rng, _faults| match rng.random_range(0..4u8) {
            0 if !cluster.spare_is_voting() => cluster.kill_and_restart_leader(),
            0 => {}
            1 => cluster.crash_and_restart_random_follower_service(rng),
            2 => {
                if !cluster.spare_is_voting() {
                    cluster.partition_minority();
                    std::thread::sleep(Duration::from_millis(800));
                    cluster.heal();
                    cluster.await_reconverged(20);
                }
            }
            _ => {
                cluster.random_config_op(rng);
            }
        },
        |cluster, _faults| cluster.config_ops_accepted >= 3,
        "reconfig churn never actually reconfigured (config_ops_accepted < 3)",
    );
}

// ------------------------------------------------------- mutation-testing passes
//
// These passes exist ONLY to give `scripts/elle_mutation.sh` a deterministic
// adversary that surfaces each injected consensus bug (design spec §4.7). They
// are compiled only under `--features mutation-testing` and are NEVER part of
// the clean tier (`scripts/elle_check.sh` runs the five passes above, none of
// these). A `UC2_MUTATION`-off run of any of them must stay clean — that is the
// control assertion at the top of `elle_mutation.sh`.
//
// Why dedicated passes instead of reusing failover/partition (the design spec's
// original mapping): the clean-tier faults are kill-AND-RESTART-same-disk and
// short symmetric-timeout partitions. Empirically neither exposes the vote-order
// or read-barrier teeth — a restarted leader recovers its own durable tail, and
// UC has NO check-quorum step-down (election.rs on_tick), so the stale window is
// real but a worker pinned to the isolated leader wastes its ~15 s submit
// deadline stuck on an unreachable-quorum append. These passes tune the workload
// (read-heavy, longer holds, fatal-tolerant workers) to land each anomaly.

/// Like [`run_pass`] but for the mutation adversary: `read_frac`/fatal-tolerant
/// workers, no minimum-liveness gate (an injected bug is *expected* to tank the
/// Ok ratio or crash a node), history always written for elle to adjudicate.
#[cfg(feature = "mutation-testing")]
#[allow(clippy::too_many_arguments)]
fn run_mutation_pass<F, V>(
    name: &str,
    ccfg: ClusterCfg,
    default_target_ops: u64,
    default_workers: u64,
    default_read_frac: f64,
    fault_period: Duration,
    mut nemesis_tick: F,
    non_vacuous: V,
    vacuity_label: &str,
) where
    F: FnMut(&mut LinClusterV2<ListAppendSm>, &mut StdRng, u32),
    V: Fn(&LinClusterV2<ListAppendSm>, u32) -> bool,
{
    let seed = env_u64("ELLE_SEED", 0x1107);
    let n_workers = env_u64("ELLE_WORKERS", default_workers) as u32;
    let keys = env_u64("ELLE_KEYS", 8) as u32;
    let target = env_u64("ELLE_TARGET_OPS", default_target_ops);
    let budget = Duration::from_secs(env_u64("ELLE_BUDGET_SECS", 120));
    let read_frac = env_f64("ELLE_READ_FRAC", default_read_frac);
    // On the isolated side an append can't reach quorum; a short deadline lets
    // the worker give up and cycle back to reads (the read-barrier tooth needs
    // reads to keep hitting the stale isolated leader, not stall 15 s per append).
    let op_deadline = Duration::from_millis(env_u64("ELLE_OP_DEADLINE_MS", 1500));
    // Minimum nemesis cycles before the pass may stop (a rare tooth needs many
    // isolation windows to land once). Independent of the op target so history
    // size stays bounded by throughput, not cycle count.
    let min_faults = env_u64("ELLE_MIN_FAULTS", 6) as u32;

    let _g = serialize();
    let dir = tempdir();
    let mut cluster =
        LinClusterV2::<ListAppendSm>::start_cfg(dir.path(), 3, FaultConfig::default(), ccfg);
    cluster.await_single_serving(30);

    let dirs = Arc::new(cluster.dirs());
    let rec = Arc::new(EdnRecorder::new(n_workers as u64));
    let stop = Arc::new(AtomicBool::new(false));
    let values = Arc::new(AtomicU64::new(1));

    let handles: Vec<_> = (0..n_workers)
        .map(|w| {
            let rng = StdRng::seed_from_u64(seed ^ (w as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let (dirs, rec, stop, values) =
                (Arc::clone(&dirs), Arc::clone(&rec), Arc::clone(&stop), Arc::clone(&values));
            std::thread::spawn(move || {
                elle_worker(w, dirs, rec, stop, rng, keys, values, read_frac, true, op_deadline)
            })
        })
        .collect();

    let mut frng = StdRng::seed_from_u64(seed ^ 0xFA17);
    let mut faults = 0u32;
    let start = Instant::now();
    let mut respawns = 0usize;
    while rec.ok_count() < target || faults < min_faults || !non_vacuous(&cluster, faults) {
        std::thread::sleep(fault_period);
        respawns += cluster.supervise_services();
        nemesis_tick(&mut cluster, &mut frng, faults);
        faults += 1;
        if start.elapsed() > budget {
            break;
        }
    }
    let elapsed = start.elapsed();
    let vacuity_ok = faults >= min_faults && non_vacuous(&cluster, faults);

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        if let Err(e) = h.join() {
            std::panic::resume_unwind(e);
        }
    }
    cluster.stop();

    let out = elle_dir().join(name);
    rec.write_to(&out.join("history.edn")).expect("write history");
    std::fs::write(out.join("seed"), format!("{seed}\n")).expect("write seed");

    let (ok, completed) = (rec.ok_count(), rec.completed_count());
    eprintln!(
        "[elle {name}] seed={seed} faults={faults} respawns={respawns} \
         completed={completed} ok={ok} read_frac={read_frac} elapsed={:.1}s -> {}",
        elapsed.as_secs_f64(),
        out.join("history.edn").display()
    );
    // The ONLY gate a mutation pass enforces is non-vacuity (the adversary
    // actually ran). Liveness is deliberately NOT gated — the injected bug may
    // legitimately crash a node or strand writes; the elle verdict is the oracle.
    assert!(vacuity_ok, "vacuous {name} pass: {vacuity_label} (faults={faults})");
}

/// `commit-quorum-minus-one` tooth. Leader-isolation cycles: with commit quorum
/// degraded to 1 the ISOLATED old leader keeps committing + acking to itself
/// while the majority elects a second leader and also commits — split brain.
/// Healing exposes two divergent committed histories (elle: incompatible-order /
/// strong-PL-1-cycle). Caught even at a few cycles; needs no read-heavy mix.
#[test]
#[ignore]
#[cfg(feature = "mutation-testing")]
fn elle_mut_commit_quorum() {
    run_mutation_pass(
        "mut_commit_quorum",
        ClusterCfg::default(),
        10_000,
        4,
        0.5,
        Duration::from_millis(1200),
        |cluster, _rng, _faults| {
            let _ = cluster.partition_leader();
            std::thread::sleep(Duration::from_millis(800));
            cluster.heal();
            cluster.await_reconverged(20);
        },
        |_cluster, faults| faults >= 3,
        "fewer than 3 leader-isolation cycles landed",
    );
}

/// `skip-read-barrier` tooth (DIRECTED 2-process probe — the strict-model-only
/// catch). UC has no check-quorum step-down (election.rs on_tick), so an isolated
/// old leader keeps believing it is leader; with the READ_PROBE barrier skipped
/// AND the leadership gate bypassed (the strengthened tooth in node.rs) it answers
/// linearizable reads from its own FROZEN local state. Natural workers can't
/// exploit this reliably — a worker pinned to the isolated leader reroutes away on
/// its first NotLeader'd submit, and the fragile 2-node majority rarely outruns
/// the most-advanced isolated node under churn. So the probe is fully directed,
/// two logical processes, per isolation cycle and per key:
///
/// 1. append process — a rerouting `WorkerConn` commits an append on the LIVE
///    majority leader (generous deadline to outlast the re-election);
/// 2. read process — a client pinned DIRECTLY to the isolated old leader reads the
///    same key immediately after, no reroute.
///
/// The append completes (real-time) before the read starts, yet the isolated
/// leader's frozen state is missing it: a pure real-time anomaly — INVALID under
/// strong-serializable, VALID under plain serializable. That gap is exactly what
/// the READ_PROBE barrier exists to prevent, and what the strict model catches.
#[test]
#[ignore]
#[cfg(feature = "mutation-testing")]
fn elle_mut_read_barrier() {
    let seed = env_u64("ELLE_SEED", 0x1107);
    let keys = env_u64("ELLE_KEYS", 8) as u32;
    let min_faults = env_u64("ELLE_MIN_FAULTS", 6) as u32;
    let budget = Duration::from_secs(env_u64("ELLE_BUDGET_SECS", 120));
    // A directed append must outlast the majority's re-election after the leader is
    // isolated (150-300 ms timeout + NewTerm commit); the settle lets the majority
    // elect before we start so the append never routes back to the isolated node.
    let app_deadline = Duration::from_millis(env_u64("ELLE_OP_DEADLINE_MS", 4000));
    let settle = Duration::from_millis(env_u64("ELLE_SETTLE_MS", 800));

    let _g = serialize();
    let dir = tempdir();
    let cluster = LinClusterV2::<ListAppendSm>::start_cfg(
        dir.path(),
        3,
        FaultConfig::default(),
        ClusterCfg::default(),
    );
    cluster.await_single_serving(30);
    let dirs = Arc::new(cluster.dirs());

    // Two logical processes: 0 = appender (majority), 1 = reader (isolated leader).
    let rec = EdnRecorder::new(2);
    let mut appender_pid = 0u64;
    let reader_pid = 1u64;
    let mut val = 1u64;

    let mut faults = 0u32;
    let start = Instant::now();
    while faults < min_faults {
        // `partition_leader` re-derives the current leader itself and returns the
        // index it isolated — use that directly (no redundant lookup, no TOCTOU
        // against a separate `await_single_serving`).
        let l = cluster.partition_leader(); // isolate the leader; its state is now frozen
        let stale = cluster.client(l); // pin DIRECTLY to the isolated leader (local shmem)
        let mut maj = WorkerConn::new(Arc::clone(&dirs), (l + 1) % 3); // a majority node
        std::thread::sleep(settle); // majority elects a new leader
        for key in 0..keys {
            // 1. advance `key` on the MAJORITY (submit reroutes to the new leader).
            let v = val;
            val += 1;
            let aop = EdnOp::Append { key, val: v };
            rec.record(EdnType::Invoke, appender_pid, &aop);
            match submit_cmd::<_, LaResp>(
                &mut maj,
                &LaCmd::Append { key, val: v },
                Instant::now() + app_deadline,
            ) {
                SubmitOutcome::Ok(LaResp::AppendAck) => {
                    rec.record(EdnType::Ok, appender_pid, &aop)
                }
                // Couldn't advance the key (majority not serving yet) → :info, retire,
                // and skip the read: nothing committed, so nothing to be stale about.
                SubmitOutcome::Indeterminate => {
                    rec.record(EdnType::Info, appender_pid, &aop);
                    appender_pid = rec.retire();
                    continue;
                }
                SubmitOutcome::Fatal(e) => panic!("fatal directed append: {e}"),
            }
            // 2. read `key` DIRECTLY from the isolated old leader (no reroute). Its
            //    frozen state is missing the just-acked append → a stale read.
            let rop = EdnOp::Read { key, result: None };
            rec.record(EdnType::Invoke, reader_pid, &rop);
            match stale.query_linearizable::<LaRead, Vec<u64>>(&LaRead { key }) {
                Ok(list) => {
                    rec.record(EdnType::Ok, reader_pid, &EdnOp::Read { key, result: Some(list) })
                }
                Err(_) => rec.record(EdnType::Fail, reader_pid, &rop),
            }
        }
        maj.drop_client();
        stale.shutdown();
        cluster.heal();
        cluster.await_reconverged(20);
        faults += 1;
        if start.elapsed() > budget {
            break;
        }
    }
    cluster.stop();

    let out = elle_dir().join("mut_read_barrier");
    rec.write_to(&out.join("history.edn")).expect("write history");
    std::fs::write(out.join("seed"), format!("{seed}\n")).expect("write seed");
    let (ok, completed) = (rec.ok_count(), rec.completed_count());
    eprintln!(
        "[elle mut_read_barrier] seed={seed} faults={faults} completed={completed} ok={ok} \
         elapsed={:.1}s -> {}",
        start.elapsed().as_secs_f64(),
        out.join("history.edn").display()
    );
    assert!(faults >= min_faults, "fewer than {min_faults} leader-isolation cycles landed");
}

/// `skip-vote-order-check` tooth. Minority-isolate a follower long enough that
/// its election timer climbs its term well above the cluster's; on heal, with
/// the lexicographic `(last_term, last_durable)` vote check skipped, the
/// up-to-date majority grants it their votes despite its STALER log — a stale
/// leader that then forces truncation of committed entries.
///
/// **The oracle is [`CommittedTruncationWitness`], not a crash.** Until
/// 2026-07-30 this tooth was scored on the driver merely exiting non-zero, and
/// the non-zero exit came from an `uc2-archive` fail-stop — issue #6, a real UC
/// defect, fixed by `2fd845e`. The injected bug still truncates committed bytes
/// afterwards; it just no longer crashes anything, so the tooth went silent
/// (weekly run 30736463470: 0/5 tries, while the two preceding weeklies caught
/// it via that panic). The witness convicts on the safety property itself —
/// `durable` stepping backward below a position the node had already committed
/// — which is what the injected bug destroys and what no unrelated fix can take
/// away.
#[test]
#[ignore]
#[cfg(feature = "mutation-testing")]
fn elle_mut_vote_order() {
    // Armed on the first tick and dropped with the closure. The pass never kills
    // a node, so every backward step of `durable` here is a truncation.
    let mut witness: Option<CommittedTruncationWitness> = None;
    // Record the hit, do NOT panic here: panicking inside the nemesis aborts
    // the pass before `run_mutation_pass` writes its history, and the history
    // is the INDEPENDENT oracle. If committed data really was lost, acked
    // appends have vanished and elle will call the history invalid without any
    // reference to this witness's own predicate; if elle calls it valid, the
    // witness is the thing under suspicion. Fail after the write instead.
    let hit: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));
    let hit_sink = Arc::clone(&hit);
    run_mutation_pass(
        "mut_vote_order",
        ClusterCfg::default(),
        8_000,
        4,
        0.5,
        Duration::from_millis(1500),
        move |cluster, _rng, _faults| {
            let witness =
                witness.get_or_insert_with(|| CommittedTruncationWitness::start(&cluster.dirs()));
            // Long isolation so the minority follower's election term climbs well
            // above the cluster's: on heal it campaigns, and with the vote-order
            // check skipped the up-to-date majority grants it despite its STALER
            // log → it wins and truncates committed entries cluster-wide.
            let hold = env_u64("ELLE_HOLD_MS", 3000);
            let _ = cluster.partition_minority();
            std::thread::sleep(Duration::from_millis(hold));
            cluster.heal();
            cluster.await_reconverged(20);
            // `await_reconverged` returns the instant one node serves — and the
            // majority's leader never stopped serving, so it returns before the
            // healed node has even campaigned. The stale win lands in the window
            // AFTER it, which is why the witness gets its own budget here.
            if let Some(found) = witness.check_within(Duration::from_millis(1500)) {
                let mut slot = hit_sink.lock().unwrap();
                if slot.is_none() {
                    eprintln!("[witness] COMMITTED-TRUNCATION — {found}");
                    *slot = Some(found);
                }
            }
        },
        |_cluster, faults| faults >= 3,
        "fewer than 3 minority-isolation cycles landed",
    );
    if let Some(found) = hit.lock().unwrap().as_ref() {
        panic!("COMMITTED-TRUNCATION — {found}");
    }
}
