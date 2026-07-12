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

use lincheck_v2::{LinClusterV2, join_workers, serialize, spawn_workers};
use uc2_net::fault::FaultConfig;
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
    let cluster = LinClusterV2::start(dir.path(), 3, FaultConfig::default());
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
    let mut cluster = LinClusterV2::start(dir.path(), 3, FaultConfig::default());
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
