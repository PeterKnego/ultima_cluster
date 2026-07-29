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

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use uc2_client::{Client, ClientError};
use uc2_log::cnc::{AdminReq, CncPage};
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

    // M8 Task 15: UC2_CRYPTO=1 boots this single node with wire crypto
    // Enabled instead of the pre-M8 Disabled default.
    let crypto_on = crypto_from_env();
    let crypto = crypto_on.then(|| provision_crypto(&inst, &[0]));
    let crypto_args =
        crypto.as_ref().map(|m| (m.key_paths[&0].as_path(), m.allowlist_path.as_path()));

    // Node held for the whole test; service held behind a Mutex<Option<_>>
    // so the fault loop can SIGKILL + respawn it. `Option` (not a bare
    // `Reap`) matters: it lets us explicitly `.take()` (kill + reap the OLD
    // process) BEFORE spawning the new one — `*g = spawn_service(&inst)`
    // would evaluate the RHS (spawn the new child) FIRST and only drop the
    // old `Reap` as part of the assignment, racing the two processes.
    let _node = spawn_node_with(&inst, crypto_args);
    wait_for_ready(&inst, Duration::from_secs(10));
    if crypto_on {
        assert_crypto_epoch_active(&inst, Duration::from_secs(10));
    }
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
    eprintln!(
        "[hard_crash] seed={seed} ops={} ok={ok} crypto={crypto_on} — checking linearizability",
        entries.len()
    );
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

    // M8 Task 15: UC2_CRYPTO=1 boots this single node with wire crypto
    // Enabled instead of the pre-M8 Disabled default. Same key/allowlist
    // reused across the restart below (same instance dir, same node id).
    let crypto_on = crypto_from_env();
    let crypto = crypto_on.then(|| provision_crypto(&inst, &[0]));
    let crypto_args =
        crypto.as_ref().map(|m| (m.key_paths[&0].as_path(), m.allowlist_path.as_path()));

    // `Mutex<Option<_>>` (not a bare `Reap`), same rationale as the
    // service-kill test above: an explicit `.take()` kills + reaps the OLD
    // process before the new one is spawned. This matters MORE here than
    // for a service respawn: `Node::start` takes an EXCLUSIVE flock on the
    // instance dir, so if the old node's process hasn't actually exited yet
    // when the new one tries to acquire it, the new node fails outright
    // with `AlreadyRunning` instead of merely racing on data.
    let node = Arc::new(Mutex::new(Some(spawn_node_with(&inst, crypto_args))));
    wait_for_ready(&inst, Duration::from_secs(10));
    if crypto_on {
        assert_crypto_epoch_active(&inst, Duration::from_secs(10));
    }
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
        *g = Some(spawn_node_with(&inst, crypto_args));
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
         ok_after_recovery={ok_after_recovery} ok_total={ok} crypto={crypto_on}",
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

// ============================================================== test 3 (M7)
//
// `sigkill_mid_config_window` — a REAL 3-node, multi-PROCESS cluster (unlike
// every other test in this file, which is single-node): 3 separate
// `uc2-crashtest-node`/`uc2-crashtest-service` process pairs sharing nothing
// but the wire protocol, driven by a concurrent CAS-register workload routed
// leader-hint-style across all 3 (mirrors `uc2_node/tests/lincheck_v2/mod.rs`'s
// `WorkerConn`). Against that live cluster: write an `AddLearner` admin
// request directly into the LEADER's cnc admin slot (the `uc2ctl`/
// `reconfig.rs` pattern, reached here without a client or the bin), then race
// a real SIGKILL of the leader's NODE PROCESS against its own append-to-
// commit window — tight timing, no synchronization with the node's internal
// state beyond polling `config_pending`/`config_version` on the SAME cnc
// mmap. Whichever side of the race the kill lands on is a legitimate outcome
// (`uc2_node/tests/reconfig.rs::truncation_revert_e2e` already proves a
// leader's own admin accept is a LOCAL, optimistic append — genuinely
// revertible until quorum commit) — the invariant under test is that the
// cluster NEVER converges on a MIX: every live node must land on the exact
// same config version (either the pre-race one or pre-race+1), and the WGL
// history recorded by the concurrent workload throughout must stay
// linearizable.

/// Bind a fresh loopback UDP socket purely to learn a free port, then drop it
/// — the child node process binds the real socket itself (mirrors
/// `uc2_node/tests/reconfig.rs`'s pre-bind-then-hand-the-address-to-a-peer
/// pattern, adapted for a separate OS process instead of a `UdpSocket` we
/// could hand over directly).
fn free_addr() -> SocketAddr {
    let s = UdpSocket::bind("127.0.0.1:0").expect("bind probe");
    let addr = s.local_addr().unwrap();
    drop(s);
    addr
}

fn members_arg(members: &[(u32, SocketAddr)]) -> String {
    members.iter().map(|(id, a)| format!("{id}@{a}")).collect::<Vec<_>>().join(",")
}

fn addr_to_wire(addr: SocketAddr) -> (u32, u16) {
    match addr {
        SocketAddr::V4(a) => (u32::from(*a.ip()), a.port()),
        SocketAddr::V6(_) => panic!("this harness only binds IPv4 loopback"),
    }
}

/// Spawn a node process as one voter of an `n`-member cluster (unlike
/// `common::spawn_node`, which always boots a single-node default).
/// `crypto` (M8 Task 15): `--crypto-key`/`--crypto-allowlist`, when the
/// UC2_CRYPTO=1 switch is on.
fn spawn_node_multi(
    instance_dir: &Path,
    id: u32,
    bind: SocketAddr,
    members: &str,
    crypto: Option<(&Path, &Path)>,
) -> Reap {
    let mut cmd = Command::new(NODE_BIN);
    cmd.arg("--instance-dir")
        .arg(instance_dir)
        .arg("--app-id")
        .arg(APP_ID)
        .arg("--id")
        .arg(id.to_string())
        .arg("--bind")
        .arg(bind.to_string())
        .arg("--members")
        .arg(members);
    if let Some((key_path, allowlist_path)) = crypto {
        cmd.arg("--crypto-key").arg(key_path).arg("--crypto-allowlist").arg(allowlist_path);
    }
    let child = cmd
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn node {id}: {e}"));
    Reap(child)
}

/// Open `dir`'s cnc page directly (the `uc2ctl` attach path minus the bin,
/// mirrors `uc2_node/tests/reconfig.rs`'s `open_cnc`) — `None` on any
/// transient failure (mid-restart truncation window, not yet created), which
/// every caller below treats as "try again", never a hard error.
fn open_cnc(dir: &Path) -> Option<Arc<CncPage>> {
    CncPage::open_file(&dir.join("cnc2.dat"), APP_ID).ok()
}

/// Wait for exactly one of `dirs` to report itself the sole serving leader
/// (both `NODE_FLAG_LEADER` and `NODE_FLAG_CAN_SERVE` set), reading every
/// node's cnc directly. Returns its index.
fn await_single_leader_multi(dirs: &[PathBuf], secs: u64) -> usize {
    use uc_protocol::v2::cnc::{NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER};
    let want = NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE;
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let serving: Vec<usize> = (0..dirs.len())
            .filter(|&i| {
                open_cnc(&dirs[i]).is_some_and(|c| c.status().flags.load_acquire() & want == want)
            })
            .collect();
        assert!(serving.len() <= 1, "split-brain: dirs {serving:?} all serve");
        if serving.len() == 1 {
            return serving[0];
        }
        assert!(Instant::now() < deadline, "no single leader among {} dirs within {secs}s", dirs.len());
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Wait until a node respawned on the SAME instance dir presents a FRESH cnc
/// `instance_id` (differing from `old_id`) — mirrors `common::
/// wait_for_fresh_instance`, but reads the raw cnc page directly instead of
/// requiring a full client attach (this test's own `open_cnc`/`CncPage`
/// handles are already in scope). Returns the fresh id.
fn wait_for_fresh_cnc_instance(dir: &Path, old_id: Option<u128>, timeout: Duration) -> u128 {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(id) = open_cnc(dir).and_then(|cnc| cnc.try_instance_id())
            && Some(id) != old_id
        {
            return id;
        }
        assert!(
            Instant::now() < deadline,
            "node restart on {} never presented a fresh cnc instance within {timeout:?}",
            dir.display()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Wait until EVERY dir's `config_version` reads the SAME value, that value
/// being either `v_lo` or `v_hi` (the only two legal outcomes of one
/// in-flight admin op), then re-confirm after a short settle window — a
/// still-propagating majority could transiently look converged one poll
/// before a lagging node catches up. Returns the settled common version.
fn await_config_converged_one_of(dirs: &[PathBuf], v_lo: u64, v_hi: u64, secs: u64) -> u64 {
    let read_all = |dirs: &[PathBuf]| -> Option<Vec<u64>> {
        dirs.iter().map(|d| open_cnc(d).map(|c| c.config_version())).collect()
    };
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if let Some(versions) = read_all(dirs) {
            let v0 = versions[0];
            if versions.iter().all(|&v| v == v0) && (v0 == v_lo || v0 == v_hi) {
                std::thread::sleep(Duration::from_millis(300));
                if read_all(dirs) == Some(versions) {
                    return v0;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "config never converged to a single value in {{{v_lo}, {v_hi}}} within {secs}s (last: {:?})",
            read_all(dirs)
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ---------------------------------------------- multi-node worker routing
//
// Mirrors `uc2_node/tests/lincheck_v2/mod.rs`'s `WorkerConn`/`submit_cmd`/
// `read_leader` (leader-hint routing across several dirs instead of this
// file's single-`dir` `Conn`/`submit_cmd`/`read_leader` above), reusing the
// SAME `SubmitOutcome`/`ReadOutcome` classification types.

struct MultiConn {
    dirs: Arc<Vec<PathBuf>>,
    target: usize,
    client: Option<Client>,
}

impl MultiConn {
    /// `start` is taken mod `dirs.len()` — callers pass a worker id as the
    /// initial routing target (spreading workers across nodes at boot), and
    /// this crate's worker ids are 1-based (id 0 is reserved for the
    /// warm-up write in the WGL history), so an id `>= dirs.len()` must wrap
    /// rather than index out of range.
    fn new(dirs: Arc<Vec<PathBuf>>, start: usize) -> Self {
        let target = start % dirs.len();
        Self { dirs, target, client: None }
    }
    fn client(&mut self) -> Option<&Client> {
        if self.client.is_none() {
            match Client::connect(&self.dirs[self.target], APP_ID) {
                Ok(c) => self.client = Some(c),
                Err(_) => return None,
            }
        }
        self.client.as_ref()
    }
    fn reconnect_to(&mut self, idx: usize) {
        if let Some(c) = self.client.take() {
            c.shutdown();
        }
        self.target = idx % self.dirs.len();
    }
    fn rotate(&mut self) {
        let next = (self.target + 1) % self.dirs.len();
        self.reconnect_to(next);
    }
    fn drop_client(&mut self) {
        if let Some(c) = self.client.take() {
            c.shutdown();
        }
    }
}

fn submit_cmd_multi(conn: &mut MultiConn, cmd: &Cmd, deadline: Instant) -> SubmitOutcome {
    loop {
        if Instant::now() > deadline {
            return SubmitOutcome::Indeterminate;
        }
        let Some(client) = conn.client() else {
            std::thread::sleep(Duration::from_millis(20));
            conn.rotate();
            continue;
        };
        match client.submit::<Cmd, CmdResp>(cmd) {
            Ok(r) => return SubmitOutcome::Ok(r),
            Err(ClientError::NotLeader { hint }) => match hint {
                Some(h) => conn.reconnect_to(h as usize),
                None => {
                    std::thread::sleep(Duration::from_millis(20));
                    conn.rotate();
                }
            },
            Err(ClientError::BackpressureFull) | Err(ClientError::Retry) => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(ClientError::InstanceRestart { .. })
            | Err(ClientError::Cnc(_))
            | Err(ClientError::Ring(_)) => {
                conn.drop_client();
                return SubmitOutcome::Indeterminate;
            }
            Err(ClientError::Timeout(_)) | Err(ClientError::ResponseOverwritten) => {
                return SubmitOutcome::Indeterminate;
            }
            Err(e @ ClientError::Decode(_))
            | Err(e @ ClientError::AppIdMismatch { .. })
            | Err(e @ ClientError::VersionMismatch { .. })
            | Err(e @ ClientError::ShutDown) => return SubmitOutcome::Fatal(format!("{e:?}")),
        }
    }
}

fn read_leader_multi(conn: &mut MultiConn, deadline: Instant) -> ReadOutcome {
    loop {
        if Instant::now() > deadline {
            return ReadOutcome::Indeterminate;
        }
        let Some(client) = conn.client() else {
            std::thread::sleep(Duration::from_millis(20));
            conn.rotate();
            continue;
        };
        match client.query_linearizable::<(), Option<u64>>(&()) {
            Ok(v) => return ReadOutcome::Ok(v),
            Err(ClientError::NotLeader { hint }) => match hint {
                Some(h) => conn.reconnect_to(h as usize),
                None => {
                    std::thread::sleep(Duration::from_millis(20));
                    conn.rotate();
                }
            },
            Err(ClientError::Retry) => std::thread::sleep(Duration::from_millis(15)),
            Err(ClientError::InstanceRestart { .. })
            | Err(ClientError::Cnc(_))
            | Err(ClientError::Ring(_)) => {
                conn.drop_client();
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(ClientError::BackpressureFull) => std::thread::sleep(Duration::from_millis(10)),
            Err(ClientError::Timeout(_)) | Err(ClientError::ResponseOverwritten) => {
                return ReadOutcome::Indeterminate;
            }
            Err(e @ ClientError::Decode(_))
            | Err(e @ ClientError::AppIdMismatch { .. })
            | Err(e @ ClientError::VersionMismatch { .. })
            | Err(e @ ClientError::ShutDown) => return ReadOutcome::Fatal(format!("{e:?}")),
        }
    }
}

fn worker_multi(
    id: u32,
    dirs: Arc<Vec<PathBuf>>,
    history: Arc<History>,
    stop: Arc<AtomicBool>,
    mut rng: StdRng,
    last_seen: Arc<AtomicU64>,
    throttle: Duration,
) {
    let mut conn = MultiConn::new(dirs, id as usize);
    while !stop.load(Ordering::Relaxed) {
        if !throttle.is_zero() {
            std::thread::sleep(throttle);
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        match rng.random_range(0..3u8) {
            0 => {
                let v = rng.random_range(1..1000u64);
                let inv = history.invoke();
                let outcome = match submit_cmd_multi(&mut conn, &Cmd::Write(v), deadline) {
                    SubmitOutcome::Ok(_) => Outcome::Ok(RegResp::Ack),
                    SubmitOutcome::Indeterminate => Outcome::Indeterminate,
                    SubmitOutcome::Fatal(e) => panic!("fatal submit: {e}"),
                };
                history.record(id, Op::Write(v), inv, outcome);
            }
            1 => {
                let inv = history.invoke();
                let outcome = match read_leader_multi(&mut conn, deadline) {
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
                let old = if rng.random_bool(0.7) {
                    last_seen.load(Ordering::Relaxed)
                } else {
                    rng.random_range(1..1000u64)
                };
                let new = rng.random_range(1..1000u64);
                let inv = history.invoke();
                let outcome = match submit_cmd_multi(&mut conn, &Cmd::Cas { old, new }, deadline) {
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

fn spawn_workers_multi(
    dirs: &Arc<Vec<PathBuf>>,
    history: &Arc<History>,
    stop: &Arc<AtomicBool>,
    last_seen: &Arc<AtomicU64>,
    seed: u64,
    throttle: Duration,
    n_workers: u32,
) -> Vec<std::thread::JoinHandle<()>> {
    (1..=n_workers)
        .map(|w| {
            let rng = StdRng::seed_from_u64(seed ^ (w as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let (dirs, history, stop, last_seen) =
                (Arc::clone(dirs), Arc::clone(history), Arc::clone(stop), Arc::clone(last_seen));
            std::thread::spawn(move || worker_multi(w, dirs, history, stop, rng, last_seen, throttle))
        })
        .collect()
}

/// Record the warm-up write against whichever node is currently leader
/// (routing via `MultiConn`) — see the single-node `warmup_write`'s doc for
/// why this must be recorded rather than discarded.
fn warmup_write_multi(dirs: &Arc<Vec<PathBuf>>, history: &History, last_seen: &AtomicU64) {
    let mut conn = MultiConn::new(Arc::clone(dirs), 0);
    let inv = history.invoke();
    match submit_cmd_multi(&mut conn, &Cmd::Write(1), Instant::now() + Duration::from_secs(20)) {
        SubmitOutcome::Ok(_) => {}
        other => panic!("warm-up write did not commit: {other:?}"),
    }
    history.record(0, Op::Write(1), inv, Outcome::Ok(RegResp::Ack));
    last_seen.store(1, Ordering::Relaxed);
    conn.drop_client();
}

/// M7 Task 10 — the scenario itself. A real 3-node cluster; `RUNS` times,
/// race an `AddLearner` admin write against a SIGKILL of the leader's node
/// process, restart it, and prove cluster-wide config-version convergence
/// never straddles two values. Concurrent workers keep driving the WGL
/// workload across the whole test; the final history must stay
/// linearizable.
#[test]
fn sigkill_mid_config_window() {
    shorten_client_timeout();
    let tmp = tempfile::tempdir().unwrap();

    const N: usize = 3;
    let addrs: Vec<SocketAddr> = (0..N).map(|_| free_addr()).collect();
    let members: Vec<(u32, SocketAddr)> = (0..N as u32).map(|i| (i, addrs[i as usize])).collect();
    let members_str = members_arg(&members);

    // M8 Task 15: UC2_CRYPTO=1 boots this real 3-PROCESS cluster with wire
    // crypto Enabled on every node — a genuine multi-process handshake, not
    // the in-process fixture the other capstones use. Only ids 0..N ever
    // boot a real process (the `spare_id`s below name an admin-protocol
    // target that never actually starts a node — see the loop's comment).
    let crypto_on = crypto_from_env();
    let crypto = crypto_on.then(|| provision_crypto(tmp.path(), &(0..N as u32).collect::<Vec<_>>()));
    let crypto_args_for = |id: u32| -> Option<(&Path, &Path)> {
        crypto.as_ref().map(|m| (m.key_paths[&id].as_path(), m.allowlist_path.as_path()))
    };

    let mut dirs: Vec<PathBuf> = Vec::with_capacity(N);
    let mut node_procs: Vec<Option<Reap>> = Vec::with_capacity(N);
    for i in 0..N as u32 {
        let d = tmp.path().join(format!("n{i}"));
        std::fs::create_dir_all(&d).unwrap();
        node_procs.push(Some(spawn_node_multi(&d, i, addrs[i as usize], &members_str, crypto_args_for(i))));
        wait_for_ready(&d, Duration::from_secs(10));
        dirs.push(d);
    }
    let mut svc_procs: Vec<Option<Reap>> = dirs.iter().map(|d| Some(spawn_service(d))).collect();

    let leader0 = await_single_leader_multi(&dirs, 30);
    if crypto_on {
        assert_crypto_epoch_active(&dirs[leader0], Duration::from_secs(10));
    }

    let dirs = Arc::new(dirs);
    let history = Arc::new(History::default());
    let last_seen = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    warmup_write_multi(&dirs, &history, &last_seen);

    const N_WORKERS: u32 = 3;
    let handles =
        spawn_workers_multi(&dirs, &history, &stop, &last_seen, 0xC0FFEE, Duration::from_millis(15), N_WORKERS);

    const RUNS: u32 = 3;
    let mut committed_version = 0u64;
    let mut observed_pending_count = 0u32;
    let mut committed_count = 0u32;

    for run in 0..RUNS {
        let li = await_single_leader_multi(&dirs, 20);

        let spare_id = 100 + run; // fresh-forever: never reused across runs
        let spare_addr = free_addr();
        let (ip, port) = addr_to_wire(spare_addr);
        let target_version = committed_version + 1;

        let leader_cnc = open_cnc(&dirs[li]).expect("leader cnc must open");
        let old_seq = leader_cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0);
        let seq = old_seq + 1;
        let nonce = rand::random::<u64>();
        leader_cnc.write_admin_req(&AdminReq {
            seq,
            nonce,
            op: 1, // AddLearner
            id: spare_id,
            ip,
            port,
        });

        // The race: poll for the append-to-commit window and SIGKILL the
        // instant we see it — or, if we never catch it (the change committed
        // faster than we could observe, or never got appended at all before
        // the poll budget expires), kill anyway. Both sides of the race are
        // valid histories (see the module doc above); `observed_pending`
        // just tells us which side THIS run actually landed on.
        let race_deadline = Instant::now() + Duration::from_millis(1500);
        let mut observed_pending = false;
        while Instant::now() < race_deadline {
            if leader_cnc.config_pending() != 0 {
                observed_pending = true;
                break;
            }
            if leader_cnc.config_version() >= target_version {
                break; // already committed — kill now anyway (the other side)
            }
        }
        // Capture the pre-kill instance id, then DROP our handle to this
        // generation's cnc BEFORE the restart truncates the SAME inode in
        // place — holding a live mmap across that window is the documented
        // "accepted SIGBUS window" in `uc2_log::cnc::CncPage::create_file`'s
        // doc, which explicitly calls out a "stale process" doing exactly
        // this as the at-risk party; dropping first makes every subsequent
        // read go through a FRESH `open_file` (safe: it just gets `None` or
        // a `BadHeader` during the transient window, never a SIGBUS).
        let old_instance_id = leader_cnc.try_instance_id();
        drop(leader_cnc);

        if observed_pending {
            observed_pending_count += 1;
        }

        // SIGKILL the leader NODE now (Reap reassignment), whichever side of
        // the race we landed on. Also drop its SERVICE — the v2.0 external-
        // supervisor contract restated in `node_sigkill_recovery`'s doc above
        // (a service never self-heals across a node restart).
        node_procs[li] = None;
        svc_procs[li] = None;

        // Restart on the SAME id/bind/members; wait for a genuinely FRESH
        // cnc instance (not the stale pre-truncate leftover) before the
        // service reattaches.
        node_procs[li] = Some(spawn_node_multi(
            &dirs[li],
            li as u32,
            addrs[li],
            &members_str,
            crypto_args_for(li as u32),
        ));
        wait_for_fresh_cnc_instance(&dirs[li], old_instance_id, Duration::from_secs(10));
        svc_procs[li] = Some(spawn_service(&dirs[li]));

        // Cluster-wide convergence: EITHER the pre-race version (the add
        // never committed) OR pre-race+1 (it did) — but the identical value
        // on every node, never a straddle.
        let final_version =
            await_config_converged_one_of(&dirs, committed_version, target_version, 30);
        assert!(
            final_version == committed_version || final_version == target_version,
            "run {run}: converged to an unexpected version {final_version} \
             (expected {committed_version} or {target_version})"
        );
        if final_version == target_version {
            committed_count += 1;
        }
        eprintln!(
            "[sigkill_mid_config_window] run={run} observed_pending={observed_pending} \
             final_version={final_version} (pre-race was {committed_version}, target {target_version})"
        );
        committed_version = final_version;

        // Let the workers get some clean traffic through before the NEXT
        // race — otherwise 3 back-to-back kill/restart cycles (each only a
        // few hundred ms) never give the concurrent workload a settled
        // window to actually rack up `Ok`s, and the liveness gate below is
        // starved for reasons that have nothing to do with linearizability.
        std::thread::sleep(Duration::from_millis(800));
    }

    // Same reasoning as above: let the tail of the history reflect a
    // healthy, fully-recovered cluster after the last race.
    std::thread::sleep(Duration::from_secs(1));

    stop.store(true, Ordering::Relaxed);
    join_workers(handles);

    let entries = Arc::try_unwrap(history).ok().expect("sole history owner").into_entries();
    let ok = History::ok_count(&entries);
    eprintln!(
        "[sigkill_mid_config_window] ops={} ok={ok} runs={RUNS} \
         observed_pending={observed_pending_count}/{RUNS} committed={committed_count}/{RUNS} \
         final_config_version={committed_version} crypto={crypto_on}",
        entries.len()
    );
    assert!(
        ok >= 20,
        "liveness: only {ok} ops completed Ok (<20) across the {RUNS} config-window races"
    );

    assert_linearizable(&entries, "sigkill_mid_config_window", "all");
    // node_procs / svc_procs dropped here → killed + reaped.
}
