//! Directed stale-read hunt (flake-hunt 2026-08-16, safety-signal track).
//!
//! The nightly elle-crypto failover pass produced `incompatible-order`
//! (Aug 9/10 CI) and, locally, `G-single-item-realtime`: linearizable reads
//! returning PRE-FAILOVER state seconds after a fresh leader resumed serving.
//! This rig is the sharpened instrument: one writer acks monotonically
//! increasing register values, a killer crash-restarts the leader in a tight
//! loop, and readers assert every linearizable read returns a value >= the
//! acked frontier captured BEFORE the read was invoked. A single violation
//! aborts with a full evidence dump (per-node cnc commit/applied counters).
//!
//! Ignored by default — it is a hunt tool, not a CI gate. Run:
//!
//! ```bash
//! UC2_CRYPTO=1 STALE_HUNT_SECS=120 cargo test -p uc2_node --release \
//!     --test stale_read_hunt -- --ignored --nocapture
//! ```

mod lincheck_v2;

use lincheck_v2::{read_leader, submit_cmd, ClusterCfg, LinClusterV2, ReadOutcome, WorkerConn};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uc2_net::fault::FaultConfig;
use uc_lincheck::register::{Cmd, CmdResp, RegisterSm};

fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("uc2-stale-hunt-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Dump each node's cnc counters (commit / service_applied / instance flags)
/// for the violation report. Best-effort: a mid-restart node prints an error.
fn dump_cnc(dirs: &[PathBuf]) -> String {
    use uc2_log::cnc::CncPage;
    let mut out = String::new();
    for (i, d) in dirs.iter().enumerate() {
        match CncPage::open_file(&d.join("cnc.dat"), "lincheck-v2") {
            Ok(page) => {
                let c = page.counters();
                let s = page.service();
                out.push_str(&format!(
                    "  node{}: commit={} durable={} append={} applied={} epoch={}\n",
                    i,
                    c.commit.load_acquire(),
                    c.durable.load_acquire(),
                    c.append.load_acquire(),
                    s.service_applied.load_acquire(),
                    s.service_epoch.load_acquire(),
                ));
            }
            Err(e) => out.push_str(&format!("  node{i}: <cnc open failed: {e:?}>\n")),
        }
    }
    out
}

/// One writer thread: Write(v) with v strictly increasing, one in flight at a
/// time; on Ok publish v as the acked frontier. Readers snapshot the frontier
/// BEFORE invoking a linearizable read; any Ok(read) < snapshot is a
/// linearizability violation (the write was acked before the read began).
#[test]
#[ignore = "directed hunt tool, not a CI gate — run with --ignored"]
fn stale_read_hunt() {
    let budget = Duration::from_secs(env_u64("STALE_HUNT_SECS", 120));
    let kill_period = Duration::from_millis(env_u64("STALE_HUNT_KILL_MS", 500));
    let crypto = std::env::var("UC2_CRYPTO").map(|v| v == "1").unwrap_or(true);

    let dir = tempdir();
    let ccfg = ClusterCfg { crypto, ..ClusterCfg::default() };
    let mut cluster = LinClusterV2::<RegisterSm>::start_cfg(dir.path(), 3, FaultConfig::default(), ccfg);
    let dirs = Arc::new(cluster.dirs());

    let acked = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let violated = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();

    // Writer: sequential acked writes = the frontier is exact, not approximate.
    let w_dirs = Arc::clone(&dirs);
    let w_acked = Arc::clone(&acked);
    let w_stop = Arc::clone(&stop);
    let writer = std::thread::spawn(move || {
        let mut conn = WorkerConn::new(w_dirs, 0);
        let mut v: u64 = 0;
        let mut acks: u64 = 0;
        while !w_stop.load(Ordering::Relaxed) {
            v += 1;
            let deadline = Instant::now() + Duration::from_secs(5);
            match submit_cmd::<_, CmdResp>(&mut conn, &Cmd::Write(v), deadline) {
                lincheck_v2::SubmitOutcome::Ok(_) => {
                    w_acked.store(v, Ordering::SeqCst);
                    acks += 1;
                }
                // Indeterminate: the write MAY have landed; the frontier must
                // NOT advance (we only assert against known-acked writes) but
                // v must not be reused either (register would go backwards
                // legitimately). Skip to a fresh value.
                _ => {}
            }
        }
        acks
    });

    // Readers: 2 threads hammering linearizable reads with pre-invoke frontier.
    let mut readers = Vec::new();
    for r in 0..2usize {
        let r_dirs = Arc::clone(&dirs);
        let r_acked = Arc::clone(&acked);
        let r_stop = Arc::clone(&stop);
        let r_violated = Arc::clone(&violated);
        readers.push(std::thread::spawn(move || {
            let mut conn = WorkerConn::new(r_dirs.clone(), 1 + r);
            let mut reads: u64 = 0;
            while !r_stop.load(Ordering::Relaxed) {
                let frontier = r_acked.load(Ordering::SeqCst);
                let invoke = Instant::now();
                let deadline = invoke + Duration::from_secs(5);
                match read_leader::<(), Option<u64>>(&mut conn, &(), deadline) {
                    ReadOutcome::Ok(got) => {
                        reads += 1;
                        let got_v = got.unwrap_or(0);
                        if got_v < frontier {
                            r_violated.store(true, Ordering::SeqCst);
                            r_stop.store(true, Ordering::SeqCst);
                            eprintln!(
                                "\n=== STALE READ CAUGHT (reader {r}, t={:.3}s) ===\n\
                                 acked frontier at invoke : {frontier}\n\
                                 linearizable read got    : {got_v} (staleness {} writes)\n\
                                 read latency             : {:?}\n\
                                 cnc state at detection:\n{}",
                                t0.elapsed().as_secs_f64(),
                                frontier - got_v,
                                invoke.elapsed(),
                                dump_cnc(&r_dirs),
                            );
                            return reads;
                        }
                    }
                    ReadOutcome::Indeterminate => {}
                    ReadOutcome::Fatal(e) => panic!("reader {r} fatal: {e}"),
                }
            }
            reads
        }));
    }

    // Killer (main thread): the elle failover nemesis mix — 50/50 leader
    // node kill+restart vs leader SERVICE-only crash+reattach (the arm the
    // first rig version missed; elle's CI hits ran this mix). Deterministic
    // alternation instead of rng: same 50/50 mass, denser coverage of the
    // service-crash windows, reproducible.
    let mut kills = 0u32;
    while t0.elapsed() < budget && !violated.load(Ordering::SeqCst) {
        std::thread::sleep(kill_period);
        if kills % 2 == 0 {
            eprintln!("[nemesis t={:.1}s] kill_and_restart_leader", t0.elapsed().as_secs_f64());
            cluster.kill_and_restart_leader();
        } else {
            eprintln!(
                "[nemesis t={:.1}s] crash_and_restart_leader_service",
                t0.elapsed().as_secs_f64()
            );
            cluster.crash_and_restart_leader_service();
        }
        kills += 1;
    }
    stop.store(true, Ordering::SeqCst);

    let acks = writer.join().expect("writer join");
    let reads: u64 = readers.into_iter().map(|h| h.join().expect("reader join")).sum();
    eprintln!(
        "stale_read_hunt: budget={budget:?} kills={kills} acked_writes={acks} lin_reads={reads}"
    );
    assert!(
        !violated.load(Ordering::SeqCst),
        "linearizable read returned a value below the acked frontier (see dump above)"
    );
    drop(cluster);
}
