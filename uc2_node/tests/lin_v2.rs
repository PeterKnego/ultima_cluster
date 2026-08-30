// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! L3 capstone for the v2 SDK (M5 Task 13, spec §8 — the single biggest
//! de-risker). A few seeded, throttled workers drive a concurrent CAS-register
//! workload over the FULL v2 stack (real nodes + per-node services + cross-
//! process clients) while a seeded scheduler injects one quorum-preserving fault
//! at a time — leader node-kill+restart OR leader service-crash+restart — until
//! enough ops complete. The recorded history must be WGL-linearizable.
//!
//! `RegisterSm` persists NOTHING (see `uc-lincheck`): after a service crash it
//! restarts EMPTY and the node reconstructs it from the replicated log (journal
//! replay, Task 9). That reconstruction — surviving both a service-only crash
//! and a full node kill under churn — is exactly what the capstone proves, and
//! the unchanged WGL checker is the oracle.
//!
//! Runs in the default `cargo test` (mirrors the v1 `lin_register` capstone —
//! not `#[ignore]`d). Budget ≤ 120 s/seed; the whole binary is serialized.

#[path = "lincheck_v2/mod.rs"]
mod lincheck_v2;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use lincheck_v2::{ClusterCfg, LinClusterV2, join_workers, serialize, spawn_workers};
use uc2_net::fault::FaultConfig;
use uc2_node::PurgePolicy;
use uc_lincheck::checker::{Verdict, check_register};
use uc_lincheck::history::{Entry, History};
use uc_lincheck::register::{Cmd, CmdResp};

/// A tempdir on the ext4 target volume (64 MiB journal segments would blow the
/// tmpfs `/tmp` quota — see the harness module docs).
fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("uc2-linv2-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

/// Dump the full history so a `Violation` reproduces offline (the checker is
/// deterministic on a captured history even though the cluster interleaving is
/// not).
fn dump_history(entries: &[Entry], seed: u64) {
    let path = format!("/tmp/lin_v2_history_{seed}.txt");
    let mut s = String::new();
    for e in entries {
        s.push_str(&format!("{e:?}\n"));
    }
    let _ = std::fs::write(&path, s);
    eprintln!("history ({} entries) dumped to {path}", entries.len());
}

// ------------------------------------------------------------------- smoke

/// Step 2 smoke: 3 nodes + 3 services + 1 client, a handful of sequential
/// write-then-read pairs through the leader must each read back the value just
/// written, then a clean node-first-then-service teardown (must not hang).
#[test]
fn smoke_3node_write_then_read() {
    let _g = serialize();
    let dir = tempdir();
    let cluster: LinClusterV2 = LinClusterV2::start(dir.path(), 3, FaultConfig::default());
    let leader = cluster.await_single_serving(30);

    let client = cluster.client(leader);
    for v in 1..=5u64 {
        let resp: CmdResp = client.submit(&Cmd::Write(v)).expect("submit write");
        assert_eq!(resp, CmdResp::WriteAck);
        let got: Option<u64> = client.query_linearizable(&()).expect("linearizable read");
        assert_eq!(got, Some(v), "read after write {v}");
    }
    // A CAS round-trips through the same path.
    let cas: CmdResp = client.submit(&Cmd::Cas { old: 5, new: 9 }).expect("submit cas");
    assert_eq!(cas, CmdResp::CasResult(true));
    assert_eq!(client.query_linearizable::<(), Option<u64>>(&()).unwrap(), Some(9));

    client.shutdown();
    cluster.stop(); // node-first-then-service per slot — must return, not hang.
}

// ----------------------------------------------------------------- capstone

/// The capstone. Seeded workers drive the workload; a seeded scheduler injects a
/// fault every `FAULT_PERIOD` (50/50 leader node-kill vs. leader service-crash)
/// until `TARGET_OPS` ops have completed `Ok`. Liveness gate ≥ 80% `Ok`; the WGL
/// verdict must be `Linearizable`.
#[test]
fn linearizable_under_failover_v2() {
    const DEFAULT_SEED: u64 = 0x1107;
    const TARGET_OPS: usize = 800;
    const N_WORKERS: u32 = 3;
    // 20 ms/op paces each worker so per-failover op counts stay modest and the
    // WGL search (exponential in the concurrency window + indeterminate
    // mutations) reliably completes rather than going Inconclusive.
    const THROTTLE: Duration = Duration::from_millis(20);
    // One fault per second; the fault's own recovery (election + node restart +
    // rejoin, ~1–3 s) dominates the spacing.
    const FAULT_PERIOD: Duration = Duration::from_secs(1);
    // Hard wall-clock guard so a wedged run fails inside the 120 s budget rather
    // than hanging the suite.
    const BUDGET: Duration = Duration::from_secs(115);

    let seed: u64 =
        std::env::var("LIN_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_SEED);

    let _g = serialize();
    let dir = tempdir();
    let mut cluster: LinClusterV2 = LinClusterV2::start(dir.path(), 3, FaultConfig::default());
    cluster.await_single_serving(30);

    let dirs = Arc::new(cluster.dirs());
    let history = Arc::new(History::default());
    let stop = Arc::new(AtomicBool::new(false));
    let last_seen = Arc::new(AtomicU64::new(0));

    let handles = spawn_workers(&dirs, &history, &stop, &last_seen, seed, THROTTLE, N_WORKERS);

    // Fault scheduler (this thread owns `&mut cluster`): one quorum-preserving
    // fault at a time, waiting for recovery between faults, until enough ops
    // have completed Ok. The seeded RNG picks the fault kind.
    let mut frng = StdRng::seed_from_u64(seed ^ 0xFA17);
    let mut faults = 0u32;
    let start = Instant::now();
    while History::ok_count(&history.snapshot()) < TARGET_OPS {
        std::thread::sleep(FAULT_PERIOD);
        if frng.random_bool(0.5) {
            cluster.kill_and_restart_leader();
        } else {
            cluster.crash_and_restart_leader_service();
        }
        faults += 1;
        if start.elapsed() > BUDGET {
            break;
        }
    }
    let elapsed = start.elapsed();

    stop.store(true, Ordering::Relaxed);
    join_workers(handles);
    cluster.stop();

    let entries = Arc::try_unwrap(history).ok().expect("sole history owner").into_entries();
    let ok = History::ok_count(&entries);
    eprintln!(
        "[lin_v2] seed={seed} faults={faults} ops={} ok={ok} elapsed={:.1}s — checking",
        entries.len(),
        elapsed.as_secs_f64()
    );

    // Liveness gate: distinguishes a real linearizability bug from a
    // cluster-progress failure.
    assert!(
        ok * 100 >= entries.len() * 80,
        "liveness: only {ok}/{} ops Ok (<80%) — cluster failed to progress",
        entries.len()
    );
    assert!(
        elapsed < Duration::from_secs(120),
        "capstone took {elapsed:?} — exceeded the 120 s/seed budget"
    );

    match check_register(&entries) {
        Verdict::Linearizable => {}
        Verdict::Violation => {
            dump_history(&entries, seed);
            panic!("LINEARIZABILITY VIOLATION (seed={seed}); history dumped");
        }
        Verdict::Inconclusive => {
            panic!("checker Inconclusive (seed={seed}); raise THROTTLE / lower TARGET_OPS")
        }
    }
}

// -------------------------------------------------- capstone under purge (M6)

/// The M6 milestone's heart: the SAME capstone workload + WGL oracle, but every
/// node runs snapshot-backed **purge** (tiny journal segments + a 64 KiB snapshot
/// cadence + `BelowSnapshot { slack_bytes: 0 }`), and the fault mix gains a third
/// arm — crash a random **follower's service**. Under purge that follower's fresh
/// empty service can no longer tail-replay from the journal (the leader purged the
/// prefix it needs), so the node reconstructs it via a **snapshot install** +
/// tail-replay (Task 5). "Purge is safe": committed history stays linearizable
/// while the log underneath is continuously snapshotted, purged, and the state is
/// rebuilt from snapshots across churn.
///
/// Same bars as the failover capstone: ≥ 80 % `Ok`, `Linearizable`, ≤ 120 s, run
/// across seeds 0x1107 / 7 / 99 (the default + `LIN_SEED`).
#[test]
fn linearizable_under_purge_and_snapshot_churn() {
    const DEFAULT_SEED: u64 = 0x1107;
    const TARGET_OPS: usize = 700;
    const N_WORKERS: u32 = 3;
    const THROTTLE: Duration = Duration::from_millis(20);
    // A little more spacing than the failover capstone: a below-floor follower
    // reconstruction installs a snapshot + tail-replays, which is slower than a
    // plain restart, so give recovery room while staying inside the budget.
    const FAULT_PERIOD: Duration = Duration::from_millis(1200);
    // Env-tunable like the reconfig capstone (nightly CI sets UC2_LIN_BUDGET_SECS
    // = 240 for the whole capstones job): a slow 4-vCPU hosted runner pushes
    // bytes slowly enough that 115 s can elapse before a 16 KiB segment ever
    // falls below the snapshot floor — the non-vacuity assert then fires as
    // "vacuous" even though nothing is wrong. Same correctness bar, more room.
    let budget = Duration::from_secs(
        std::env::var("UC2_LIN_BUDGET_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(115),
    );

    // Purge posture: 16 KiB journal segments (smaller than the snapshot interval,
    // so whole segments fall below the snapshot floor and get dropped even in the
    // low-volume test workload), a snapshot every 32 KiB of applied progress, and
    // purge everything below the snapshot with zero slack — the most aggressive
    // purge, so below-floor reconstruction fires reliably within a short run.
    let ccfg = ClusterCfg {
        purge: PurgePolicy::BelowSnapshot { slack_bytes: 0 },
        journal_segment_bytes: 16 * 1024,
        snapshot_interval_bytes: 32 * 1024,
        spare_node: false,
        crypto: false,
        ..ClusterCfg::default()
    };

    let seed: u64 =
        std::env::var("LIN_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_SEED);

    let _g = serialize();
    let dir = tempdir();
    let mut cluster: LinClusterV2 = LinClusterV2::start_cfg(dir.path(), 3, FaultConfig::default(), ccfg);
    cluster.await_single_serving(30);

    let dirs = Arc::new(cluster.dirs());
    let history = Arc::new(History::default());
    let stop = Arc::new(AtomicBool::new(false));
    let last_seen = Arc::new(AtomicU64::new(0));

    let handles = spawn_workers(&dirs, &history, &stop, &last_seen, seed, THROTTLE, N_WORKERS);

    // Fault scheduler: one quorum-preserving fault at a time, 1-in-3 —
    //   0: leader node kill+restart (fresh empty service, log-replay reconstruct),
    //   1: leader service crash+restart,
    //   2: RANDOM FOLLOWER service crash+restart (below-floor snapshot-install
    //      reconstruct — the purge-safety path this capstone adds).
    let mut frng = StdRng::seed_from_u64(seed ^ 0xFA17);
    let mut faults = 0u32;
    let mut follower_svc_faults = 0u32;
    let start = Instant::now();
    // Churn until BOTH bars are met: enough ok ops AND the purge floor actually
    // advanced (the non-vacuity condition). On a fast box the floor advances
    // long before TARGET_OPS; on a slow CI runner the op bar can be met while
    // the byte volume still hasn't filled a segment — keep the fault mix live
    // until it does, bounded by the (env-tunable) budget.
    while History::ok_count(&history.snapshot()) < TARGET_OPS
        || cluster.max_archive_first_base() == 0
    {
        std::thread::sleep(FAULT_PERIOD);
        match frng.random_range(0..3u8) {
            0 => cluster.kill_and_restart_leader(),
            1 => cluster.crash_and_restart_leader_service(),
            _ => {
                cluster.crash_and_restart_random_follower_service(&mut frng);
                follower_svc_faults += 1;
            }
        }
        faults += 1;
        if start.elapsed() > budget {
            break;
        }
    }
    let elapsed = start.elapsed();

    // Non-vacuity: purge must have actually dropped a journal prefix, else the
    // "below-floor reconstruction" the follower-service fault claims to exercise
    // never happened (the fresh service would just tail-replay). Capture before
    // stopping the cluster.
    let purge_floor = cluster.max_archive_first_base();

    stop.store(true, Ordering::Relaxed);
    join_workers(handles);
    cluster.stop();

    let entries = Arc::try_unwrap(history).ok().expect("sole history owner").into_entries();
    let ok = History::ok_count(&entries);
    eprintln!(
        "[lin_v2 purge] seed={seed} faults={faults} (follower-svc={follower_svc_faults}) \
         ops={} ok={ok} purge_floor={purge_floor} elapsed={:.1}s — checking",
        entries.len(),
        elapsed.as_secs_f64()
    );

    assert!(
        purge_floor > 0,
        "purge never advanced the archive floor (max first_base = 0) — the capstone \
         did not exercise snapshot-backed purge; raise op volume / shrink segments"
    );

    assert!(
        ok * 100 >= entries.len() * 80,
        "liveness: only {ok}/{} ops Ok (<80%) — cluster failed to progress under purge",
        entries.len()
    );
    assert!(
        elapsed < Duration::from_secs(120),
        "purge capstone took {elapsed:?} — exceeded the 120 s/seed budget"
    );

    match check_register(&entries) {
        Verdict::Linearizable => {}
        Verdict::Violation => {
            dump_history(&entries, seed);
            panic!("LINEARIZABILITY VIOLATION under purge (seed={seed}); history dumped");
        }
        Verdict::Inconclusive => {
            panic!("checker Inconclusive under purge (seed={seed}); raise THROTTLE / lower TARGET_OPS")
        }
    }
}

// ------------------------------------------------- capstone under reconfig (M7)

/// M7 Task 10 — the milestone's strongest correctness proof: the SAME
/// failover-capstone workload + WGL oracle, but the fault mix now ALSO
/// churns live membership. A `ClusterCfg::spare_node` reserves an extra
/// address the scheduler cycles through `LinClusterV2::random_config_op`
/// (add-learner -> promote -> demote -> remove-learner, one step per pick,
/// each gated on the previous step's `config_pending` actually clearing —
/// see that method's doc for why a bare admin `status == 0` is NOT by itself
/// a durable-commit guarantee). The other three arms are: kill-leader
/// (node kill+restart), a follower-service crash (mirroring the purge
/// capstone's third arm), and a short isolate-a-follower-then-heal
/// partition (mirroring `lin_partition_v2.rs`'s minority scenario) — so this
/// capstone proves linearizability holds with ALL FOUR fault classes
/// interleaved with live reconfiguration, not just reconfiguration in
/// isolation.
///
/// Same bars as the other capstones: ≥ 80 % `Ok`, `Linearizable`, ≤ 120 s
/// (this capstone's budget is env-tunable via `UC2_LIN_BUDGET_SECS`, default
/// 120 — see the `budget_secs` local below), across seeds 0x1107 / 7 / 99
/// (the default + `LIN_SEED`). NON-VACUITY: `config_ops_accepted >= 3` —
/// proof that the reconfig arm didn't just spin on `NotCaughtUp`/pending
/// no-ops the whole run.
#[test]
fn linearizable_under_reconfig_churn() {
    const DEFAULT_SEED: u64 = 0x1107;
    const TARGET_OPS: usize = 600;
    // A hard ceiling on top of `TARGET_OPS`: the WGL checker (`uc-lincheck`,
    // untouched) has only ever been exercised up to ~1.7k entries in this
    // workspace (the crashtest's `hard_crash` test) — an UNDISTURBED run of
    // this capstone (few faults land, e.g. the config-op cycle completes in
    // its first few picks) can otherwise let the workers run wild for the
    // rest of `BUDGET` while only the `config_ops_accepted` condition below
    // is still pending, and was observed to reach 4000+ entries and blow the
    // checker's stack. The scheduler loop bails out once ops cross this line
    // even if `MIN_CONFIG_OPS` isn't met yet (the assert below still catches
    // genuine vacuity; this only guards against an accidental OVER-run).
    const MAX_OPS: usize = 1500;
    const N_WORKERS: u32 = 3;
    // Heavier than the other capstones' 15-20 ms: this capstone's fault
    // schedule (see `FAULT_PERIOD` below) gives the cluster long undisturbed
    // stretches, and light throttling let raw worker throughput alone push
    // the history size into `MAX_OPS` territory well before the fault
    // schedule had a chance to matter.
    const THROTTLE: Duration = Duration::from_millis(150);
    // More spacing than the plain failover capstone's 1 s (a config-op
    // round-trip — propose + replicate + commit, sometimes a catch-up wait —
    // is slower than a bare kill+restart), but NOT so much that the total
    // tick count over `BUDGET` gets small enough for the arm picker's
    // per-seed variance to matter: at 2.5 s this capstone was observed
    // (seed 99) to draw the config arm only twice in the entire budget by
    // sheer bad luck in that seed's RNG stream, reaching `config_ops_accepted
    // == 2` and failing non-vacuity even though nothing was actually stuck.
    // 1.2 s roughly doubles the tick budget, which the widened election
    // timeout + the spare-voting fault gates below already make safe to
    // sustain.
    const FAULT_PERIOD: Duration = Duration::from_millis(1200);
    // Wall-clock budget for this capstone only (the other three capstones
    // keep their hard-coded 120 s bar). This capstone runs a 4th busy-spin
    // node (`spare_node: true`) plus widened election timeouts, which can
    // run tight on a shrunk-vCPU hosted CI runner even though it comfortably
    // clears 120 s on the dev fleet — env-tunable so CI can widen it
    // (`UC2_LIN_BUDGET_SECS`, default 120, matching the other capstones'
    // fixed bar) without touching the correctness bars below.
    let budget_secs: u64 = std::env::var("UC2_LIN_BUDGET_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);
    // Soft cutoff for the scheduler loop, 5 s inside the hard budget below —
    // preserved from the original fixed 115-vs-120 split so teardown (worker
    // join + cluster stop) has room before the hard assert fires.
    let budget = Duration::from_secs(budget_secs.saturating_sub(5));

    let ccfg = ClusterCfg { spare_node: true, ..ClusterCfg::default() };

    let seed: u64 =
        std::env::var("LIN_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_SEED);

    let _g = serialize();
    let dir = tempdir();
    let mut cluster: LinClusterV2 = LinClusterV2::start_cfg(dir.path(), 3, FaultConfig::default(), ccfg);
    cluster.await_single_serving(30);

    let dirs = Arc::new(cluster.dirs());
    let history = Arc::new(History::default());
    let stop = Arc::new(AtomicBool::new(false));
    let last_seen = Arc::new(AtomicU64::new(0));

    let handles = spawn_workers(&dirs, &history, &stop, &last_seen, seed, THROTTLE, N_WORKERS);

    // Fault scheduler: one quorum-preserving fault at a time, 1-in-4 —
    //   0: leader node kill+restart,
    //   1: random follower service crash+restart,
    //   2: isolate a random follower, hold briefly, then heal
    //      (`lin_partition_v2.rs`'s minority scenario, condensed),
    //   3: random_config_op — one step of the spare's add/promote/demote/
    //      remove cycle (a legitimate no-op when a change is already
    //      pending / the learner isn't caught up yet).
    // Non-vacuity floor (the capstone asserts `>= 3` at the end; target one
    // higher here so scheduling jitter doesn't shave the margin to zero).
    // At full worker throughput (no purge slowing anything down, unlike the
    // purge-churn capstone) `TARGET_OPS` alone is reached in a few seconds —
    // WAY before enough fault-scheduler ticks have fired to cycle the spare a
    // handful of times, so the loop must keep running on the config-accepted
    // condition too, not stop the instant the op-count bar is cleared.
    const MIN_CONFIG_OPS: u32 = 4;
    let mut frng = StdRng::seed_from_u64(seed ^ 0xFA17);
    let mut faults = 0u32;
    let mut config_arm_picks = 0u32;
    let start = Instant::now();
    loop {
        let snap = history.snapshot();
        if snap.len() >= MAX_OPS {
            break;
        }
        if History::ok_count(&snap) >= TARGET_OPS && cluster.config_ops_accepted >= MIN_CONFIG_OPS {
            break;
        }
        std::thread::sleep(FAULT_PERIOD);
        match frng.random_range(0..4u8) {
            // While the spare is a full VOTER (`Promoted`, between
            // PromoteLearner and DemoteVoter committing) the cluster's
            // quorum is a razor-thin 3-of-4 with zero slack: killing one of
            // the original 3 leaves EXACTLY 3 live members, so the pending
            // DemoteVoter proposal needs every one of them healthy and
            // caught up to commit. Observed empirically (seed 99): repeated
            // kills during this window can keep the SAME pending change
            // perpetually unsettled (`ChangePending` on every subsequent
            // admin attempt) for the rest of the budget. Skip this arm
            // during that (short) window, same rationale as the partition
            // arm below.
            0 if !cluster.spare_is_voting() => cluster.kill_and_restart_leader(),
            0 => {}
            1 => cluster.crash_and_restart_random_follower_service(&mut frng),
            2 => {
                // `partition_minority`'s `cut()` plumbing only knows about
                // the original `n` nodes; while the spare is a full VOTER
                // (`Promoted`, between PromoteLearner and DemoteVoter
                // committing) it is a real 4th quorum member that isolating
                // "the other two" would not actually cut off from — skip
                // this arm during that (short) window rather than teaching
                // the shared partition helpers about a member that joins and
                // leaves quorum mid-run.
                if !cluster.spare_is_voting() {
                    cluster.partition_minority();
                    std::thread::sleep(Duration::from_millis(800));
                    cluster.heal();
                    cluster.await_reconverged(20);
                }
            }
            _ => {
                config_arm_picks += 1;
                cluster.random_config_op(&mut frng);
            }
        }
        faults += 1;
        if start.elapsed() > budget {
            break;
        }
    }
    let elapsed = start.elapsed();

    // Non-vacuity: captured before `stop()` consumes the cluster.
    let config_ops_accepted = cluster.config_ops_accepted;

    stop.store(true, Ordering::Relaxed);
    join_workers(handles);
    cluster.stop();

    let entries = Arc::try_unwrap(history).ok().expect("sole history owner").into_entries();
    let ok = History::ok_count(&entries);
    eprintln!(
        "[lin_v2 reconfig] seed={seed} faults={faults} (config-arm picks={config_arm_picks}) \
         ops={} ok={ok} config_ops_accepted={config_ops_accepted} elapsed={:.1}s — checking",
        entries.len(),
        elapsed.as_secs_f64()
    );

    assert!(
        config_ops_accepted >= 3,
        "vacuous: reconfig churn never actually reconfigured (config_ops_accepted={config_ops_accepted})"
    );

    assert!(
        ok * 100 >= entries.len() * 80,
        "liveness: only {ok}/{} ops Ok (<80%) — cluster failed to progress under reconfig churn",
        entries.len()
    );
    assert!(
        elapsed < Duration::from_secs(budget_secs),
        "reconfig-churn capstone took {elapsed:?} — exceeded the {budget_secs} s/seed budget \
         (override via UC2_LIN_BUDGET_SECS)"
    );

    match check_register(&entries) {
        Verdict::Linearizable => {}
        Verdict::Violation => {
            dump_history(&entries, seed);
            panic!("LINEARIZABILITY VIOLATION under reconfig churn (seed={seed}); history dumped");
        }
        Verdict::Inconclusive => {
            panic!("checker Inconclusive under reconfig churn (seed={seed}); raise THROTTLE / lower TARGET_OPS")
        }
    }
}

// ======================================================= M8 Task 15: crypto ON
//
// The same three capstones above, byte-for-byte, except every node boots
// with wire crypto `Enabled` (`ClusterCfg::crypto = true` — see
// `lincheck_v2`'s M8 Task 15 fixture). The WGL checker is untouched: if
// sealing, the replay window, or key rotation perturbed ordering, durability,
// or recovery under failover/purge/reconfig churn, this is what would show
// it. Each test also asserts the elected leader genuinely MINTED a crypto
// group epoch right after boot — proof the switch actually engaged, not just
// that the cluster (harmlessly) still formed with it silently doing nothing.

/// `linearizable_under_failover_v2`, crypto ON.
#[test]
fn linearizable_under_failover_with_crypto() {
    const DEFAULT_SEED: u64 = 0x1107;
    const TARGET_OPS: usize = 800;
    const N_WORKERS: u32 = 3;
    const THROTTLE: Duration = Duration::from_millis(20);
    const FAULT_PERIOD: Duration = Duration::from_secs(1);
    const BUDGET: Duration = Duration::from_secs(115);

    let seed: u64 =
        std::env::var("LIN_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_SEED);

    let ccfg = ClusterCfg { crypto: true, ..ClusterCfg::default() };

    let _g = serialize();
    let dir = tempdir();
    let mut cluster: LinClusterV2 =
        LinClusterV2::start_cfg(dir.path(), 3, FaultConfig::default(), ccfg);
    let leader0 = cluster.await_single_serving(30);
    assert!(
        cluster.crypto_epoch_of(leader0).is_some(),
        "crypto was configured but the elected leader never minted a group epoch — \
         wire crypto did not actually engage"
    );

    let dirs = Arc::new(cluster.dirs());
    let history = Arc::new(History::default());
    let stop = Arc::new(AtomicBool::new(false));
    let last_seen = Arc::new(AtomicU64::new(0));

    let handles = spawn_workers(&dirs, &history, &stop, &last_seen, seed, THROTTLE, N_WORKERS);

    let mut frng = StdRng::seed_from_u64(seed ^ 0xFA17);
    let mut faults = 0u32;
    let start = Instant::now();
    while History::ok_count(&history.snapshot()) < TARGET_OPS {
        std::thread::sleep(FAULT_PERIOD);
        if frng.random_bool(0.5) {
            cluster.kill_and_restart_leader();
        } else {
            cluster.crash_and_restart_leader_service();
        }
        faults += 1;
        if start.elapsed() > BUDGET {
            break;
        }
    }
    let elapsed = start.elapsed();

    stop.store(true, Ordering::Relaxed);
    join_workers(handles);
    cluster.stop();

    let entries = Arc::try_unwrap(history).ok().expect("sole history owner").into_entries();
    let ok = History::ok_count(&entries);
    eprintln!(
        "[lin_v2 crypto] seed={seed} faults={faults} ops={} ok={ok} elapsed={:.1}s — checking",
        entries.len(),
        elapsed.as_secs_f64()
    );

    assert!(
        ok * 100 >= entries.len() * 80,
        "liveness: only {ok}/{} ops Ok (<80%) — crypto-enabled cluster failed to progress",
        entries.len()
    );
    assert!(
        elapsed < Duration::from_secs(120),
        "crypto-enabled capstone took {elapsed:?} — exceeded the 120 s/seed budget"
    );

    match check_register(&entries) {
        Verdict::Linearizable => {}
        Verdict::Violation => {
            dump_history(&entries, seed);
            panic!("LINEARIZABILITY VIOLATION under crypto+failover (seed={seed}); history dumped");
        }
        Verdict::Inconclusive => {
            panic!("checker Inconclusive under crypto+failover (seed={seed}); raise THROTTLE / lower TARGET_OPS")
        }
    }
}

/// `linearizable_under_purge_and_snapshot_churn`, crypto ON.
#[test]
fn linearizable_under_purge_and_snapshot_churn_with_crypto() {
    const DEFAULT_SEED: u64 = 0x1107;
    const TARGET_OPS: usize = 700;
    const N_WORKERS: u32 = 3;
    const THROTTLE: Duration = Duration::from_millis(20);
    const FAULT_PERIOD: Duration = Duration::from_millis(1200);
    let budget = Duration::from_secs(
        std::env::var("UC2_LIN_BUDGET_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(115),
    );

    let ccfg = ClusterCfg {
        purge: PurgePolicy::BelowSnapshot { slack_bytes: 0 },
        journal_segment_bytes: 16 * 1024,
        snapshot_interval_bytes: 32 * 1024,
        spare_node: false,
        crypto: true,
        ..ClusterCfg::default()
    };

    let seed: u64 =
        std::env::var("LIN_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_SEED);

    let _g = serialize();
    let dir = tempdir();
    let mut cluster: LinClusterV2 = LinClusterV2::start_cfg(dir.path(), 3, FaultConfig::default(), ccfg);
    let leader0 = cluster.await_single_serving(30);
    assert!(
        cluster.crypto_epoch_of(leader0).is_some(),
        "crypto was configured but the elected leader never minted a group epoch — \
         wire crypto did not actually engage"
    );

    let dirs = Arc::new(cluster.dirs());
    let history = Arc::new(History::default());
    let stop = Arc::new(AtomicBool::new(false));
    let last_seen = Arc::new(AtomicU64::new(0));

    let handles = spawn_workers(&dirs, &history, &stop, &last_seen, seed, THROTTLE, N_WORKERS);

    let mut frng = StdRng::seed_from_u64(seed ^ 0xFA17);
    let mut faults = 0u32;
    let mut follower_svc_faults = 0u32;
    let start = Instant::now();
    while History::ok_count(&history.snapshot()) < TARGET_OPS
        || cluster.max_archive_first_base() == 0
    {
        std::thread::sleep(FAULT_PERIOD);
        match frng.random_range(0..3u8) {
            0 => cluster.kill_and_restart_leader(),
            1 => cluster.crash_and_restart_leader_service(),
            _ => {
                cluster.crash_and_restart_random_follower_service(&mut frng);
                follower_svc_faults += 1;
            }
        }
        faults += 1;
        if start.elapsed() > budget {
            break;
        }
    }
    let elapsed = start.elapsed();

    let purge_floor = cluster.max_archive_first_base();

    stop.store(true, Ordering::Relaxed);
    join_workers(handles);
    cluster.stop();

    let entries = Arc::try_unwrap(history).ok().expect("sole history owner").into_entries();
    let ok = History::ok_count(&entries);
    eprintln!(
        "[lin_v2 crypto+purge] seed={seed} faults={faults} (follower-svc={follower_svc_faults}) \
         ops={} ok={ok} purge_floor={purge_floor} elapsed={:.1}s — checking",
        entries.len(),
        elapsed.as_secs_f64()
    );

    assert!(
        purge_floor > 0,
        "purge never advanced the archive floor (max first_base = 0) — the crypto+purge \
         capstone did not exercise snapshot-backed purge; raise op volume / shrink segments"
    );

    assert!(
        ok * 100 >= entries.len() * 80,
        "liveness: only {ok}/{} ops Ok (<80%) — crypto-enabled cluster failed to progress under purge",
        entries.len()
    );
    assert!(
        elapsed < Duration::from_secs(120),
        "crypto+purge capstone took {elapsed:?} — exceeded the 120 s/seed budget"
    );

    match check_register(&entries) {
        Verdict::Linearizable => {}
        Verdict::Violation => {
            dump_history(&entries, seed);
            panic!("LINEARIZABILITY VIOLATION under crypto+purge (seed={seed}); history dumped");
        }
        Verdict::Inconclusive => {
            panic!("checker Inconclusive under crypto+purge (seed={seed}); raise THROTTLE / lower TARGET_OPS")
        }
    }
}

/// `linearizable_under_reconfig_churn`, crypto ON. The spare's freshly
/// allocated ids (100, 101, ...) are pre-provisioned crypto material too
/// (`lincheck_v2::crypto_ids_for`), so every add/promote/demote/remove cycle
/// boots the joining node sealed from the start.
#[test]
fn linearizable_under_reconfig_churn_with_crypto() {
    const DEFAULT_SEED: u64 = 0x1107;
    const TARGET_OPS: usize = 600;
    const MAX_OPS: usize = 1500;
    const N_WORKERS: u32 = 3;
    const THROTTLE: Duration = Duration::from_millis(150);
    const FAULT_PERIOD: Duration = Duration::from_millis(1200);
    let budget_secs: u64 = std::env::var("UC2_LIN_BUDGET_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);
    let budget = Duration::from_secs(budget_secs.saturating_sub(5));

    let ccfg = ClusterCfg { spare_node: true, crypto: true, ..ClusterCfg::default() };

    let seed: u64 =
        std::env::var("LIN_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_SEED);

    let _g = serialize();
    let dir = tempdir();
    let mut cluster: LinClusterV2 = LinClusterV2::start_cfg(dir.path(), 3, FaultConfig::default(), ccfg);
    let leader0 = cluster.await_single_serving(30);
    assert!(
        cluster.crypto_epoch_of(leader0).is_some(),
        "crypto was configured but the elected leader never minted a group epoch — \
         wire crypto did not actually engage"
    );

    let dirs = Arc::new(cluster.dirs());
    let history = Arc::new(History::default());
    let stop = Arc::new(AtomicBool::new(false));
    let last_seen = Arc::new(AtomicU64::new(0));

    let handles = spawn_workers(&dirs, &history, &stop, &last_seen, seed, THROTTLE, N_WORKERS);

    const MIN_CONFIG_OPS: u32 = 4;
    let mut frng = StdRng::seed_from_u64(seed ^ 0xFA17);
    let mut faults = 0u32;
    let mut config_arm_picks = 0u32;
    let start = Instant::now();
    loop {
        let snap = history.snapshot();
        if snap.len() >= MAX_OPS {
            break;
        }
        if History::ok_count(&snap) >= TARGET_OPS && cluster.config_ops_accepted >= MIN_CONFIG_OPS {
            break;
        }
        std::thread::sleep(FAULT_PERIOD);
        match frng.random_range(0..4u8) {
            0 if !cluster.spare_is_voting() => cluster.kill_and_restart_leader(),
            0 => {}
            1 => cluster.crash_and_restart_random_follower_service(&mut frng),
            2 => {
                if !cluster.spare_is_voting() {
                    cluster.partition_minority();
                    std::thread::sleep(Duration::from_millis(800));
                    cluster.heal();
                    cluster.await_reconverged(20);
                }
            }
            _ => {
                config_arm_picks += 1;
                cluster.random_config_op(&mut frng);
            }
        }
        faults += 1;
        if start.elapsed() > budget {
            break;
        }
    }
    let elapsed = start.elapsed();

    let config_ops_accepted = cluster.config_ops_accepted;

    stop.store(true, Ordering::Relaxed);
    join_workers(handles);
    cluster.stop();

    let entries = Arc::try_unwrap(history).ok().expect("sole history owner").into_entries();
    let ok = History::ok_count(&entries);
    eprintln!(
        "[lin_v2 crypto+reconfig] seed={seed} faults={faults} (config-arm picks={config_arm_picks}) \
         ops={} ok={ok} config_ops_accepted={config_ops_accepted} elapsed={:.1}s — checking",
        entries.len(),
        elapsed.as_secs_f64()
    );

    assert!(
        config_ops_accepted >= 3,
        "vacuous: crypto+reconfig churn never actually reconfigured (config_ops_accepted={config_ops_accepted})"
    );

    assert!(
        ok * 100 >= entries.len() * 80,
        "liveness: only {ok}/{} ops Ok (<80%) — crypto-enabled cluster failed to progress under reconfig churn",
        entries.len()
    );
    assert!(
        elapsed < Duration::from_secs(budget_secs),
        "crypto+reconfig-churn capstone took {elapsed:?} — exceeded the {budget_secs} s/seed budget \
         (override via UC2_LIN_BUDGET_SECS)"
    );

    match check_register(&entries) {
        Verdict::Linearizable => {}
        Verdict::Violation => {
            dump_history(&entries, seed);
            panic!("LINEARIZABILITY VIOLATION under crypto+reconfig churn (seed={seed}); history dumped");
        }
        Verdict::Inconclusive => {
            panic!("checker Inconclusive under crypto+reconfig churn (seed={seed}); raise THROTTLE / lower TARGET_OPS")
        }
    }
}

// ------------------------------------------------------------ M14c2 two FSMs

/// M14c2 T2 smoke: two FSMs boot, one `submit_all` answers from both with
/// equal responses, and the cnc slots show both attached and applied.
#[test]
fn two_fsm_smoke() {
    let _g = serialize();
    let dir = tempdir();
    let ccfg = ClusterCfg {
        services: lincheck_v2::FsmSet::Two { lag: uc2_node::FsmLag::Bounded(64 * 1024) },
        ..ClusterCfg::default()
    };
    let cluster: LinClusterV2 = LinClusterV2::start_cfg(dir.path(), 3, FaultConfig::default(), ccfg);
    let leader = cluster.await_single_serving(30);
    let client = cluster.client(leader);
    let resps: Vec<(u8, CmdResp)> = client.submit_all(&Cmd::Write(7)).expect("submit_all");
    assert_eq!(resps.len(), 2, "{resps:?}");
    assert_eq!(resps[0].1, resps[1].1, "replication-equivalence: {resps:?}");
    let r2: Vec<(u8, CmdResp)> =
        client.submit_all(&Cmd::Cas { old: 7, new: 8 }).expect("submit_all cas");
    assert!(r2.iter().all(|(_, r)| *r == CmdResp::CasResult(true)), "{r2:?}");
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline
        && (cluster.service_applied(leader, 0) == 0 || cluster.service_applied(leader, 1) == 0)
    {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(cluster.service_applied(leader, 0) > 0 && cluster.service_applied(leader, 1) > 0);
    cluster.stop();
}

/// M14c2 T3: two-FSM WGL capstone. Every write/CAS goes through `submit_all`
/// (fanning in to both FSMs), and any unequal pair between FSM 0 and FSM 1
/// increments `equiv_failures` — the replication-equivalence oracle. Each
/// FSM's own history must ALSO be WGL-linearizable on its own.
fn run_two_fsm(label: &str, lag: uc2_node::FsmLag, seed: u64) {
    const TARGET_OPS: usize = 600;
    const N_WORKERS: u32 = 3;
    const THROTTLE: Duration = Duration::from_millis(20);
    const FAULT_PERIOD: Duration = Duration::from_millis(1200);
    let budget = Duration::from_secs(
        std::env::var("UC2_LIN_BUDGET_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(115),
    );
    let ccfg = ClusterCfg {
        purge: PurgePolicy::BelowSnapshot { slack_bytes: 0 },
        journal_segment_bytes: 16 * 1024,
        snapshot_interval_bytes: 32 * 1024,
        services: lincheck_v2::FsmSet::Two { lag },
        ..ClusterCfg::default()
    };
    let _g = serialize();
    let dir = tempdir();
    let mut cluster: LinClusterV2 = LinClusterV2::start_cfg(dir.path(), 3, FaultConfig::default(), ccfg);
    cluster.await_single_serving(30);
    let dirs = Arc::new(cluster.dirs());
    let (h0, h1) = (Arc::new(History::default()), Arc::new(History::default()));
    let equiv_failures = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let last_seen = Arc::new(AtomicU64::new(0));
    let handles = lincheck_v2::spawn_workers2(
        &dirs,
        &h0,
        &h1,
        &equiv_failures,
        &stop,
        &last_seen,
        seed,
        THROTTLE,
        N_WORKERS,
    );
    let mut frng = StdRng::seed_from_u64(seed ^ 0xFA17);
    let mut faults = 0u32;
    let mut follower_svc_faults = 0u32;
    let start = Instant::now();
    while History::ok_count(&h0.snapshot()) < TARGET_OPS
        || History::ok_count(&h1.snapshot()) < TARGET_OPS
        || cluster.max_archive_first_base() == 0
    {
        std::thread::sleep(FAULT_PERIOD);
        cluster.supervise_services();
        match frng.random_range(0..3u8) {
            0 => cluster.kill_and_restart_leader(),
            1 => cluster.crash_and_restart_leader_service(),
            _ => {
                cluster.crash_and_restart_random_follower_service(&mut frng);
                follower_svc_faults += 1;
            }
        }
        faults += 1;
        assert!(
            start.elapsed() < budget,
            "[{label}] budget exhausted: ok0={} ok1={} floor={}",
            History::ok_count(&h0.snapshot()),
            History::ok_count(&h1.snapshot()),
            cluster.max_archive_first_base()
        );
    }
    let elapsed = start.elapsed();
    let (ok0, ok1) = (History::ok_count(&h0.snapshot()), History::ok_count(&h1.snapshot()));
    stop.store(true, Ordering::Relaxed);
    join_workers(handles);
    cluster.stop();
    eprintln!(
        "[{label}] seed={seed} ok0={ok0} ok1={ok1} faults={faults} \
         follower_svc_faults={follower_svc_faults} elapsed={:.1}s",
        elapsed.as_secs_f64()
    );
    assert_eq!(
        equiv_failures.load(Ordering::Relaxed),
        0,
        "[{label}] replication-equivalence violated"
    );
    for (id, h) in [(0u8, h0), (1u8, h1)] {
        let entries = Arc::try_unwrap(h).map(History::into_entries).unwrap_or_else(|a| a.snapshot());
        match check_register(&entries) {
            Verdict::Linearizable => {}
            v => panic!("[{label}] FSM {id}: {v:?} (seed={seed})"),
        }
    }
}

#[test]
fn two_fsm_bounded() {
    run_two_fsm(
        "two_fsm_bounded",
        uc2_node::FsmLag::Bounded(64 * 1024),
        std::env::var("LIN_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(0x14c2),
    );
}
#[test]
fn two_fsm_lockstep() {
    run_two_fsm(
        "two_fsm_lockstep",
        uc2_node::FsmLag::Lockstep,
        std::env::var("LIN_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(0x14c3),
    );
}

/// The oracle must bite: FSM 1 = `Corrupt<RegisterSm>` flips every CAS, so the
/// first CAS `submit_all` disagrees and `equiv_failures` is non-zero.
#[test]
#[should_panic(expected = "replication-equivalence violated")]
fn two_fsm_oracle_bites() {
    let _g = serialize();
    let dir = tempdir();
    let ccfg = ClusterCfg {
        services: lincheck_v2::FsmSet::Two { lag: uc2_node::FsmLag::Bounded(64 * 1024) },
        ..ClusterCfg::default()
    };
    let cluster: LinClusterV2<uc_lincheck::register::RegisterSm, lincheck_v2::Corrupt<uc_lincheck::register::RegisterSm>> =
        LinClusterV2::start_cfg(dir.path(), 3, FaultConfig::default(), ccfg);
    let leader = cluster.await_single_serving(30);
    let client = cluster.client(leader);
    let _: Vec<(u8, CmdResp)> = client.submit_all(&Cmd::Write(1)).unwrap();
    let r: Vec<(u8, CmdResp)> = client.submit_all(&Cmd::Cas { old: 1, new: 2 }).unwrap();
    cluster.stop();
    assert_eq!(r[0].1, r[1].1, "replication-equivalence violated: {r:?}");
}

/// M14c2 T4: the slow-FSM oracle. FSM 1 is `Slow<RegisterSm, 200>` (200 µs per
/// apply — ≥ 10× slower than FSM 0's unthrottled apply on this box, so it is
/// comfortably "the limiter"). No faults; a background sampler reads
/// `(applied_0, applied_1)` off the leader's cnc page every 50 ms and asserts
/// (ruling 2026-08-30, spec §16.3):
///   (i) the lag bound holds at EVERY sample — `Bounded(b)` -> `b`,
///       `Lockstep` -> 288 (one frame: max_payload 256 + 32-byte header);
///   (ii) over the second half of the run, FSM 0's applied-bytes rate is
///        within 10% of FSM 1's — i.e. the faster FSM actually converges to
///        the slow one's pace rather than racing arbitrarily ahead within the
///        bound.
fn run_two_fsm_slow(label: &str, lag: uc2_node::FsmLag, seed: u64) {
    const SECS: u64 = 20;
    const N_WORKERS: u32 = 4;
    let _g = serialize();
    let dir = tempdir();
    let ccfg = ClusterCfg { services: lincheck_v2::FsmSet::Two { lag }, ..ClusterCfg::default() };
    let cluster: LinClusterV2<uc_lincheck::register::RegisterSm, lincheck_v2::Slow<uc_lincheck::register::RegisterSm, 200>> =
        LinClusterV2::start_cfg(dir.path(), 3, FaultConfig::default(), ccfg);
    let leader = cluster.await_single_serving(30);
    let dirs = Arc::new(cluster.dirs());
    let (h0, h1) = (Arc::new(History::default()), Arc::new(History::default()));
    let equiv = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let last_seen = Arc::new(AtomicU64::new(0));
    let handles = lincheck_v2::spawn_workers2(&dirs, &h0, &h1, &equiv, &stop, &last_seen, seed, Duration::ZERO, N_WORKERS);
    // sampler: (t, applied_0, applied_1) on the leader every 50 ms; no faults in this run
    let samples = {
        let stop = Arc::clone(&stop);
        let dir0 = dirs[leader].clone();
        std::thread::spawn(move || {
            let cnc = uc2_log::cnc::CncPage::open_file(&dir0.join("cnc2.dat"), lincheck_v2::APP).expect("cnc");
            let mut v = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                v.push((Instant::now(), cnc.service_slot(0).applied.load_acquire(), cnc.service_slot(1).applied.load_acquire()));
                std::thread::sleep(Duration::from_millis(50));
            }
            v
        })
    };
    std::thread::sleep(Duration::from_secs(SECS));
    stop.store(true, Ordering::Relaxed);
    join_workers(handles);
    let samples = samples.join().unwrap();
    cluster.stop();
    assert_eq!(equiv.load(Ordering::Relaxed), 0, "[{label}] replication-equivalence violated");
    // (i) the bound at every sample
    let bound = match lag { uc2_node::FsmLag::Bounded(b) => b, uc2_node::FsmLag::Lockstep => 288 };
    for (t, a0, a1) in &samples {
        assert!(a0.saturating_sub(*a1) <= bound, "[{label}] lag {} > bound {bound} at {:?}", a0.saturating_sub(*a1), t);
    }
    // (ii) convergence over the second half (ruling 2026-08-30)
    let half = samples.len() / 2;
    let (t0, a0_0, a1_0) = samples[half];
    let (t1, a0_1, a1_1) = *samples.last().unwrap();
    let dt = (t1 - t0).as_secs_f64();
    let (r0, r1) = ((a0_1 - a0_0) as f64 / dt, (a1_1 - a1_0) as f64 / dt);
    assert!(r1 > 0.0, "[{label}] FSM 1 made no progress in the second half");
    let ratio = r0 / r1;
    assert!((0.9..=1.1).contains(&ratio), "[{label}] FSM 0 rate {r0:.0} B/s vs FSM 1 {r1:.0} B/s: ratio {ratio:.3} outside [0.9, 1.1]");
    eprintln!("[{label}] samples={} rate0={r0:.0} rate1={r1:.0} ratio={ratio:.3}", samples.len());
}

#[test] fn two_fsm_slow()          { run_two_fsm_slow("two_fsm_slow",          uc2_node::FsmLag::Bounded(64 * 1024), 0x51); }
#[test] fn two_fsm_slow_lockstep() { run_two_fsm_slow("two_fsm_slow_lockstep", uc2_node::FsmLag::Lockstep,           0x52); }
