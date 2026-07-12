// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Hard-crash linearizability tests (v2, M5 Task 14, spec §8 "SIGKILL
//! (service and node)"): SIGKILL a real OS process mid-apply while
//! sustained `uc2_client` load drives a concurrent CAS-register workload
//! against a real, single-node `uc2_node` cluster, restart it, and assert
//! the recorded op history stays WGL-linearizable across the
//! crash/restart/reconstruction.
//!
//! Unlike the in-process L3 capstone (`uc2_node/tests/lin_v2.rs` +
//! `lincheck_v2/mod.rs`), which crashes/restarts nodes and services
//! IN-PROCESS (`Node::crash`/`Service::crash`), this drops a `Reap` guard —
//! a true `kill -9` + reap + respawn over a SEPARATE OS process sharing the
//! instance dir — so it proves the same reconstruction/recovery paths
//! survive a real hard crash, not just an in-process handle drop.
//!
//! `RegisterSm` is plain in-memory (persists nothing): a restarted service
//! comes back EMPTY and the node must reconstruct it from the replicated
//! log. That reconstruction is what `linearizable_under_service_sigkill`
//! checks; `node_sigkill_recovery` checks the node side of the same
//! contract.
//!
//! ## Classification discipline (reused from `lincheck_v2::submit_cmd`/
//! `read_leader`)
//!
//! Only `NotLeader`/`BackpressureFull`/`Retry` are GUARANTEED not to have
//! committed — the node's ingress drain proves it: it answers
//! `MSG_V2_NOT_LEADER` (and never appends) while not serving, and refuses
//! admission with `BackpressureFull` before any append; `Retry` is likewise
//! pre-append (query-only, but harmless to fold in for symmetry). So those
//! three alone are safe to retry — the whole point being that the same
//! logical op still commits exactly once.
//!
//! Everything else — most importantly `InstanceRestart` (this crate's real
//! hard-crash trigger: a node restart re-creates the cnc page with a fresh
//! random `instance_id`, so an in-flight request against the old one can
//! never be disambiguated as committed-or-not) — is classified
//! `Indeterminate` and is NEVER retried. Retrying a maybe-committed op can
//! double-apply it (a duplicate CAS turns a later `true` into a `false`: a
//! textbook linearizability violation), which is exactly the class of bug
//! this test exists to catch — so the classifier itself must not
//! manufacture one.
//!
//! Gated behind `hard-crash-tests` because it spawns real OS processes and
//! sends real SIGKILLs.
#![cfg(feature = "hard-crash-tests")]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use uc2_client::{Client, ClientError};
use uc_lincheck::checker::{Verdict, check_register};
use uc_lincheck::history::{History, Outcome};
use uc_lincheck::model::{Op, RegResp};
use uc_lincheck::register::{Cmd, CmdResp};

mod common;
use common::*;

// ------------------------------------------------------ reconnecting conn

/// One worker's client connection to the (single) node instance dir,
/// (re)connected on demand. Mirrors `uc2_node/tests/lincheck_v2/mod.rs`'s
/// `WorkerConn`, minus the multi-node leader-hint routing — this crate only
/// ever runs a single-node cluster, so a fresh `Client::connect` always
/// targets the same `dir`; reconnecting is exactly what's needed to survive
/// a node SIGKILL (a fresh `instance_id`) as well as a service SIGKILL (the
/// client itself never goes stale, but a defensive reconnect is harmless).
struct Conn {
    dir: PathBuf,
    client: Option<Client>,
}

impl Conn {
    fn new(dir: PathBuf) -> Self {
        Self { dir, client: None }
    }
    /// Ensure a client attached to `dir`; `None` if the attach failed (node
    /// mid-restart) — the caller retries.
    fn client(&mut self) -> Option<&Client> {
        if self.client.is_none() {
            self.client = Client::connect(&self.dir, APP_ID).ok();
        }
        self.client.as_ref()
    }
    fn drop_client(&mut self) {
        if let Some(c) = self.client.take() {
            c.shutdown();
        }
    }
}

#[derive(Debug)]
enum SubmitOutcome {
    Ok(CmdResp),
    /// May or may not have committed — the WGL "indeterminate mutation".
    Indeterminate,
    /// A genuine harness/wiring bug (bad codec, wrong app/version) — never a
    /// legitimate operational outcome.
    Fatal(String),
}

enum ReadOutcome {
    Ok(Option<u64>),
    Indeterminate,
    Fatal(String),
}

/// Submit `cmd`, retrying ONLY the guaranteed-not-committed errors
/// (`NotLeader`/`BackpressureFull`/`Retry`) until `deadline`. Every other
/// error is `Indeterminate` and NEVER retried — see the module doc. A
/// connect failure (node mid-restart) rotates through a short sleep and
/// retries the attach, same as a routing retry.
fn submit_cmd(conn: &mut Conn, cmd: &Cmd, deadline: Instant) -> SubmitOutcome {
    loop {
        if Instant::now() > deadline {
            return SubmitOutcome::Indeterminate; // gave up → in-limbo
        }
        let Some(client) = conn.client() else {
            std::thread::sleep(Duration::from_millis(20));
            continue;
        };
        match client.submit::<Cmd, CmdResp>(cmd) {
            Ok(r) => return SubmitOutcome::Ok(r),
            Err(ClientError::NotLeader { .. })
            | Err(ClientError::BackpressureFull)
            | Err(ClientError::Retry) => {
                std::thread::sleep(Duration::from_millis(20));
            }
            // Maybe-committed → indeterminate, never retried (see module
            // doc). Drop the (now stale) client so the NEXT op reconnects
            // to the fresh page: this single-node harness has no surviving
            // peer to rotate to, so a fresh `Client::connect` is the only
            // way back once the node/service is up again. Also dropping on
            // `Timeout`/`ResponseOverwritten` (unlike `lincheck_v2`'s
            // multi-node convention, which instead rotates to a different
            // node on `NotLeader`) matters here specifically for a node
            // restart: a `Timeout`'d client's own cnc-page mmap may itself
            // be of an unlinked, now-stale file (`Node::start` unlinks +
            // recreates every IPC file on boot), so it can never observe
            // the new `instance_id` to self-classify `InstanceRestart` —
            // only a fresh attach can.
            Err(ClientError::InstanceRestart { .. })
            | Err(ClientError::Cnc(_))
            | Err(ClientError::Ring(_))
            | Err(ClientError::Timeout(_))
            | Err(ClientError::ResponseOverwritten) => {
                conn.drop_client();
                return SubmitOutcome::Indeterminate;
            }
            Err(e @ ClientError::Decode(_))
            | Err(e @ ClientError::AppIdMismatch { .. })
            | Err(e @ ClientError::VersionMismatch { .. })
            | Err(e @ ClientError::ShutDown) => return SubmitOutcome::Fatal(format!("{e:?}")),
        }
    }
}

/// Linearizable read, same routing/classification discipline as
/// [`submit_cmd`].
fn read_leader(conn: &mut Conn, deadline: Instant) -> ReadOutcome {
    loop {
        if Instant::now() > deadline {
            return ReadOutcome::Indeterminate;
        }
        let Some(client) = conn.client() else {
            std::thread::sleep(Duration::from_millis(20));
            continue;
        };
        match client.query_linearizable::<(), Option<u64>>(&()) {
            Ok(v) => return ReadOutcome::Ok(v),
            Err(ClientError::NotLeader { .. })
            | Err(ClientError::Retry)
            | Err(ClientError::BackpressureFull) => {
                std::thread::sleep(Duration::from_millis(20));
            }
            // See `submit_cmd`'s doc comment for why `Timeout`/
            // `ResponseOverwritten` also drop the client here.
            Err(ClientError::InstanceRestart { .. })
            | Err(ClientError::Cnc(_))
            | Err(ClientError::Ring(_)) => {
                conn.drop_client();
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(ClientError::Timeout(_)) | Err(ClientError::ResponseOverwritten) => {
                conn.drop_client();
                return ReadOutcome::Indeterminate;
            }
            Err(e @ ClientError::Decode(_))
            | Err(e @ ClientError::AppIdMismatch { .. })
            | Err(e @ ClientError::VersionMismatch { .. })
            | Err(e @ ClientError::ShutDown) => return ReadOutcome::Fatal(format!("{e:?}")),
        }
    }
}

/// One worker: until `stop`, pick a seeded op, route it through its own
/// (reconnecting) `Conn`, classify the outcome, record it. A worker must
/// NEVER panic on an operational error — the node or service WILL be dead
/// mid-op during a fault — only a `Fatal` (harness bug) panics.
fn worker(
    id: u32,
    dir: Arc<PathBuf>,
    history: Arc<History>,
    last_seen: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    mut rng: StdRng,
    throttle: Duration,
) {
    let mut conn = Conn::new((*dir).clone());
    while !stop.load(Ordering::Relaxed) {
        std::thread::sleep(throttle);
        let deadline = Instant::now() + Duration::from_secs(15);
        match rng.random_range(0..3u8) {
            0 => {
                let v = rng.random_range(1..1000u64);
                let inv = history.invoke();
                let outcome = match submit_cmd(&mut conn, &Cmd::Write(v), deadline) {
                    SubmitOutcome::Ok(_) => Outcome::Ok(RegResp::Ack),
                    SubmitOutcome::Indeterminate => Outcome::Indeterminate,
                    SubmitOutcome::Fatal(e) => panic!("fatal submit: {e}"),
                };
                history.record(id, Op::Write(v), inv, outcome);
            }
            1 => {
                let inv = history.invoke();
                let outcome = match read_leader(&mut conn, deadline) {
                    ReadOutcome::Ok(v) => {
                        if let Some(x) = v {
                            last_seen.store(x, Ordering::Relaxed);
                        }
                        Outcome::Ok(RegResp::Value(v))
                    }
                    ReadOutcome::Indeterminate => Outcome::Indeterminate,
                    ReadOutcome::Fatal(e) => panic!("fatal read: {e}"),
                };
                history.record(id, Op::Read, inv, outcome);
            }
            _ => {
                // CAS using a recently-seen value as `old` (so some
                // succeed), sometimes a random old (so some fail).
                let old = if rng.random_bool(0.7) {
                    last_seen.load(Ordering::Relaxed)
                } else {
                    rng.random_range(1..1000u64)
                };
                let new = rng.random_range(1..1000u64);
                let inv = history.invoke();
                let outcome = match submit_cmd(&mut conn, &Cmd::Cas { old, new }, deadline) {
                    SubmitOutcome::Ok(CmdResp::CasResult(b)) => {
                        if b {
                            last_seen.store(new, Ordering::Relaxed);
                        }
                        Outcome::Ok(RegResp::CasOk(b))
                    }
                    SubmitOutcome::Ok(other) => panic!("cas returned non-cas response: {other:?}"),
                    SubmitOutcome::Indeterminate => Outcome::Indeterminate,
                    SubmitOutcome::Fatal(e) => panic!("fatal cas: {e}"),
                };
                history.record(id, Op::Cas { old, new }, inv, outcome);
            }
        }
    }
    conn.drop_client();
}

/// Spawn `n_workers` op-driving threads sharing `dir`/`history`/`last_seen`/
/// `stop`. Each gets its own seeded RNG and its own `Conn` (created inside
/// `worker`).
fn spawn_workers(
    dir: &Arc<PathBuf>,
    history: &Arc<History>,
    last_seen: &Arc<AtomicU64>,
    stop: &Arc<AtomicBool>,
    seed: u64,
    throttle: Duration,
    n_workers: u32,
) -> Vec<std::thread::JoinHandle<()>> {
    (0..n_workers)
        .map(|w| {
            let rng = StdRng::seed_from_u64(seed ^ (w as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let (dir, history, last_seen, stop) =
                (Arc::clone(dir), Arc::clone(history), Arc::clone(last_seen), Arc::clone(stop));
            std::thread::spawn(move || worker(w, dir, history, last_seen, stop, rng, throttle))
        })
        .collect()
}

fn join_workers(handles: Vec<std::thread::JoinHandle<()>>) {
    for h in handles {
        if let Err(e) = h.join() {
            std::panic::resume_unwind(e);
        }
    }
}

/// Record the warm-up write (WGL init=None gotcha, v1 precedent): the
/// model's initial state is `None` (never written), so a DISCARDED warm-up
/// write would leave a phantom value later reads observe but the checker
/// can't account for (a false `Violation`). Recording it as history entry 0
/// instead gives the history a clean, model-consistent start at value 1.
/// Panics if the warm-up itself doesn't commit — a prerequisite for the rest
/// of the test to mean anything.
fn warmup_write(dir: &Path, history: &History, last_seen: &AtomicU64) {
    let mut conn = Conn::new(dir.to_path_buf());
    let inv = history.invoke();
    match submit_cmd(&mut conn, &Cmd::Write(1), Instant::now() + Duration::from_secs(15)) {
        SubmitOutcome::Ok(_) => {}
        other => panic!("warm-up write did not commit: {other:?}"),
    }
    history.record(0, Op::Write(1), inv, Outcome::Ok(RegResp::Ack));
    last_seen.store(1, Ordering::Relaxed);
    conn.drop_client();
}

/// Check the collected history and panic (dumping it) on a real Violation.
/// `dump_prefix` names the per-test dump file
/// (`/tmp/uc2_<dump_prefix>_<tag>.txt`).
fn assert_linearizable(entries: &[uc_lincheck::history::Entry], dump_prefix: &str, tag: &str) {
    match check_register(entries) {
        Verdict::Linearizable => {
            eprintln!("[{dump_prefix}] {tag}: Linearizable");
        }
        Verdict::Inconclusive => {
            // WGL search hit its visited-state budget — acceptable (like the
            // capstone), not a correctness failure.
            eprintln!("[{dump_prefix}] {tag}: Inconclusive (checker budget)");
        }
        Verdict::Violation => {
            // Dump the full history so a Violation is reproducible offline
            // (the checker is deterministic on a captured history even
            // though the crash interleaving is not).
            let path = format!("/tmp/uc2_{dump_prefix}_history_{tag}.txt");
            let mut s = String::new();
            for e in entries {
                s.push_str(&format!("{e:?}\n"));
            }
            let _ = std::fs::write(&path, s);
            eprintln!("[{dump_prefix}] history ({} entries) dumped to {path}", entries.len());
            panic!("{dump_prefix} history NOT linearizable (tag {tag})");
        }
    }
}

// ------------------------------------------------------------- test 1

/// SIGKILL the SERVICE process mid-apply several times while sustained
/// concurrent load drives Write/Read/CAS against the register; assert the
/// full history stays linearizable. The node stays up throughout — this
/// checks service-state reconstruction (spec §8 / task9) survives a real
/// hard crash of the service process, not just a graceful shutdown or an
/// in-process handle drop.
///
/// Run with `LIN_SEED=1|7|42 cargo test ... linearizable_under_service_sigkill`
/// — all three seeds must report `Linearizable`.
#[test]
fn linearizable_under_service_sigkill() {
    shorten_client_timeout();
    let seed: u64 = std::env::var("LIN_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(1);

    let tmp = tempfile::tempdir().unwrap();
    let inst = tmp.path().join("inst");
    std::fs::create_dir_all(&inst).unwrap();

    // Node held for the whole test; service held behind a Mutex<Option<_>>
    // so the fault loop can SIGKILL + respawn it. `Option` (not a bare
    // `Reap`) matters: it lets us explicitly `.take()` (kill + reap the OLD
    // process) BEFORE spawning the new one — `*g = spawn_service(&inst)`
    // would evaluate the RHS (spawn the new child) FIRST and only drop the
    // old `Reap` as part of the assignment, racing the two processes.
    let _node = spawn_node(&inst);
    wait_for_path(&inst.join("cnc2.dat"), Duration::from_secs(10));
    let svc = Arc::new(Mutex::new(Some(spawn_service(&inst))));

    let dir = Arc::new(inst.clone());
    let history = Arc::new(History::default());
    let last_seen = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    warmup_write(&inst, &history, &last_seen);

    const N_WORKERS: u32 = 3;
    let throttle = Duration::from_millis(7);
    let handles = spawn_workers(&dir, &history, &last_seen, &stop, seed, throttle, N_WORKERS);

    // Fault loop: HARD-CRASH (SIGKILL) + restart the service several times.
    // The client(s) stay attached to the same NODE throughout (only the
    // service dies); submits during the down/reconstruct window become
    // Indeterminate. After respawn the node reconstructs the fresh (empty)
    // service from the replicated log.
    //
    // 700ms between crashes is comfortably longer than single-node
    // reconstruction (replay of a tiny register) so the cluster fully
    // recovers and lands committed ops between faults; 5 iterations gives
    // several distinct crash/recover cycles within the test's runtime
    // budget.
    for _ in 0..5 {
        std::thread::sleep(Duration::from_millis(700));
        let mut g = svc.lock().unwrap();
        g.take(); // drop = SIGKILL + reap the OLD service, BEFORE spawning the new one
        *g = Some(spawn_service(&inst));
        drop(g);
    }

    // Let post-recovery ops land so the tail of the history reflects a
    // healthy reconstructed service.
    std::thread::sleep(Duration::from_secs(1));

    stop.store(true, Ordering::Relaxed);
    join_workers(handles);

    let entries = Arc::try_unwrap(history).ok().expect("sole history owner").into_entries();

    let ok = History::ok_count(&entries);
    eprintln!("[hard_crash] seed={seed} ops={} ok={ok} — checking linearizability", entries.len());
    assert!(
        ok >= 50,
        "liveness: only {ok} ops completed Ok (<50) — cluster failed to progress under service SIGKILL"
    );

    assert_linearizable(&entries, "hard_crash", &seed.to_string());
    // _node / svc Reaps dropped here → killed + reaped.
}

// ------------------------------------------------------------- test 2

/// Single-node hard-crash-and-recover test (spec §8 "SIGKILL (service and
/// node)", v2 L3 port): mid-load, SIGKILL the NODE process (not the
/// service), respawn it on the SAME instance dir, then the harness also
/// kills + respawns the service.
///
/// **v2.0 contract, not a shortcut** (decision #9, restated): a node
/// restart invalidates every attachment. `Node::start` re-creates the cnc
/// page with a brand-new random `instance_id` on every boot (see
/// `uc2_node/src/node.rs`'s `Node::start_with_socket` step 3), so an
/// attached client can only ever observe this as
/// `ClientError::InstanceRestart` (this test's workers reconnect through
/// exactly that path via `Conn`/`submit_cmd`/`read_leader` above) — there is
/// no live re-attach today. A service that watches `instance_id` and
/// re-attaches on its own without an external respawn is the M6 polish
/// already named in the deferred list; it is NOT built here. This test's
/// harness playing "external process supervisor" — killing the node,
/// waiting for survivors (there are none, single-node), respawning it, and
/// ONLY THEN respawning the service — IS the v2.0 contract being exercised,
/// honestly scoped, not a workaround for a missing feature.
///
/// Post-recovery: `ok` resumes growing (checked explicitly via
/// `History::snapshot` before/after the kill window) and the full history
/// stays `Linearizable`. Run 3 consecutive times for stability.
#[test]
fn node_sigkill_recovery() {
    for run in 0..3u32 {
        node_sigkill_recovery_once(run);
    }
}

fn node_sigkill_recovery_once(run: u32) {
    shorten_client_timeout();
    let seed: u64 = 0xC0FFEE_u64 ^ (run as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);

    let tmp = tempfile::tempdir().unwrap();
    let inst = tmp.path().join("inst");
    std::fs::create_dir_all(&inst).unwrap();

    // `Mutex<Option<_>>` (not a bare `Reap`), same rationale as the
    // service-kill test above: an explicit `.take()` kills + reaps the OLD
    // process before the new one is spawned. This matters MORE here than
    // for a service respawn: `Node::start` takes an EXCLUSIVE flock on the
    // instance dir, so if the old node's process hasn't actually exited yet
    // when the new one tries to acquire it, the new node fails outright
    // with `AlreadyRunning` instead of merely racing on data.
    let node = Arc::new(Mutex::new(Some(spawn_node(&inst))));
    wait_for_path(&inst.join("cnc2.dat"), Duration::from_secs(10));
    let svc = Arc::new(Mutex::new(Some(spawn_service(&inst))));

    let dir = Arc::new(inst.clone());
    let history = Arc::new(History::default());
    let last_seen = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    warmup_write(&inst, &history, &last_seen);

    const N_WORKERS: u32 = 3;
    let throttle = Duration::from_millis(7);
    let handles = spawn_workers(&dir, &history, &last_seen, &stop, seed, throttle, N_WORKERS);

    // Let some load land before the kill. Capture the pre-kill instance_id
    // (a throwaway probe client) so the post-restart wait can positively
    // confirm a FRESH node, not just a stale leftover cnc2.dat.
    std::thread::sleep(Duration::from_millis(300));
    let ok_before_kill = History::ok_count(&history.snapshot());
    let old_instance_id = connect_with_retry(&inst, Duration::from_secs(10)).instance_id();

    // SIGKILL the node (Reap reassignment), then respawn on the SAME
    // instance dir. Workers riding `Conn` see their in-flight request time
    // out into `ClientError::InstanceRestart` (or a plain connect failure
    // while the process is down) and reconnect on their next op — no
    // separate "reconnect loop" bookkeeping needed; `submit_cmd`/
    // `read_leader`'s normal retry path *is* the reconnect loop.
    {
        let mut g = node.lock().unwrap();
        g.take(); // kill + reap the OLD node, BEFORE spawning the new one (flock!)
        *g = Some(spawn_node(&inst));
    }
    // `wait_for_path(cnc2.dat)` would be VACUOUS here — the file never
    // disappeared (only the process holding it died), so it'd return
    // instantly against the stale leftover from the killed node and let the
    // service below race ahead and attach to a soon-to-be-unlinked page.
    // Wait for a genuinely fresh `instance_id` instead (module doc).
    wait_for_fresh_instance(&inst, old_instance_id, Duration::from_secs(10));

    // The harness ALSO kills + respawns the service, strictly AFTER the
    // node (module doc: the v2.0 contract is external-supervisor-driven,
    // node-first).
    {
        let mut g = svc.lock().unwrap();
        g.take(); // kill + reap the OLD service, BEFORE spawning the new one
        *g = Some(spawn_service(&inst));
    }

    // Let recovery land: single-node self-election is near-instant, but
    // give the reconstruction + resumed load a comfortable window.
    std::thread::sleep(Duration::from_secs(2));
    let ok_after_recovery = History::ok_count(&history.snapshot());

    stop.store(true, Ordering::Relaxed);
    join_workers(handles);

    let entries = Arc::try_unwrap(history).ok().expect("sole history owner").into_entries();
    let ok = History::ok_count(&entries);
    eprintln!(
        "[node_sigkill_recovery] run={run} ops={} ok_before_kill={ok_before_kill} \
         ok_after_recovery={ok_after_recovery} ok_total={ok}",
        entries.len()
    );
    assert!(
        ok >= 20,
        "liveness: only {ok} ops completed Ok (<20) across the node SIGKILL+restart (run {run})"
    );
    assert!(
        ok_after_recovery > ok_before_kill,
        "ok count did not grow after the node SIGKILL+restart (stuck at {ok_before_kill}, run {run}) \
         — the cluster failed to resume serving after recovery"
    );

    assert_linearizable(&entries, "node_sigkill_recovery", &format!("run{run}"));
    // node / svc Reaps dropped here → killed + reaped.
}
