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
//! cargo test -p uc2_node --release --test elle_v2 -- --ignored --exact elle_quiet
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
    ClusterCfg, LinClusterV2, ReadOutcome, SubmitOutcome, WorkerConn, read_leader, serialize,
    submit_cmd,
};
use uc2_net::fault::FaultConfig;
use uc_lincheck::edn::{EdnOp, EdnRecorder, EdnType};
use uc_lincheck::list_append::{LaCmd, LaRead, LaResp, ListAppendSm};

// ------------------------------------------------------------------ env knobs

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn elle_dir() -> PathBuf {
    PathBuf::from(std::env::var("ELLE_DIR").unwrap_or_else(|_| "/tmp/uc2-elle".into()))
}

/// Instance dirs on ext4 (journal segments blow the tmpfs /tmp quota).
fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("uc2-elle-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

// ------------------------------------------------------------------- worker

/// One unthrottled worker: 50/50 append/read over `keys` hot keys.
fn elle_worker(
    id: u32,
    dirs: Arc<Vec<PathBuf>>,
    rec: Arc<EdnRecorder>,
    stop: Arc<AtomicBool>,
    mut rng: StdRng,
    keys: u32,
    values: Arc<AtomicU64>,
) {
    let mut conn = WorkerConn::new(dirs, id as usize);
    // Initial process ids 0..n_workers are pre-allocated by EdnRecorder::new.
    let mut process = id as u64;
    while !stop.load(Ordering::Relaxed) {
        let deadline = Instant::now() + Duration::from_secs(15);
        let key = rng.random_range(0..keys);
        if rng.random_bool(0.5) {
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
                ReadOutcome::Fatal(e) => panic!("fatal read: {e}"),
            }
        }
    }
    conn.drop_client();
}

// ------------------------------------------------------------------ run_pass

/// Drive one pass: boot, spawn workers, tick the nemesis every `fault_period`
/// until the op target AND the pass's non-vacuity hold (or the budget runs
/// out), then write `$ELLE_DIR/<name>/history.edn` (+ a `seed` sidecar) and
/// assert the liveness/non-vacuity gates.
#[allow(clippy::too_many_arguments)]
fn run_pass<F, V>(
    name: &str,
    ccfg: ClusterCfg,
    default_target_ops: u64,
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
    let n_workers = env_u64("ELLE_WORKERS", 4) as u32;
    let keys = env_u64("ELLE_KEYS", 8) as u32;
    let target = env_u64("ELLE_TARGET_OPS", default_target_ops);
    let budget = Duration::from_secs(env_u64("ELLE_BUDGET_SECS", 120));

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
            std::thread::spawn(move || elle_worker(w, dirs, rec, stop, rng, keys, values))
        })
        .collect();

    let mut frng = StdRng::seed_from_u64(seed ^ 0xFA17);
    let mut faults = 0u32;
    let start = Instant::now();
    while rec.ok_count() < target || !non_vacuous(&cluster, faults) {
        std::thread::sleep(fault_period);
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

    let (ok, completed) = (rec.ok_count(), rec.completed_count());
    eprintln!(
        "[elle {name}] seed={seed} faults={faults} completed={completed} ok={ok} \
         elapsed={:.1}s -> {}",
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
        90,
        Duration::from_millis(100),
        |_cluster, _rng, _faults| {},
        |_cluster, _faults| true,
        "unreachable",
    );
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
