// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M11 Task 3: the survival crashtest — the spec's acceptance sentence,
//! literally, as a CI test: *"A node is backed up under load, its host
//! destroyed, a new host restored from the backup alone, and it rejoins and
//! converges."*
//!
//! Real multi-process (3 `uc2-crashtest-node`/`uc2-crashtest-service` pairs,
//! mirroring `hard_crash.rs`'s `sigkill_mid_config_window`/
//! `leader_node_sigkill_recovery_multi` 3-process shape, duplicated minimally
//! here rather than restructuring `common/mod.rs`, which only builds
//! single-node setups): a client submits a serial CAS chain against the
//! leader for the whole test, one FOLLOWER is backed up live
//! (`uc2_node::backup::backup_instance`, in-process — this test crate links
//! `uc2_node` directly, no shelling out to `uc2ctl`), that follower's
//! processes are SIGKILLed and its instance dir `rm -rf`'d ("host
//! destroyed"), a fresh dir at a different path ("new host") is restored from
//! the artifact alone (`restore_artifact`) and booted with the SAME node id
//! and SAME bind address (the restored durable `config.state` — not the
//! CLI's `--members`, which is seed-only once a durable record exists — owns
//! membership, so the surviving two nodes' already-built peer-address tables
//! route to it unchanged), and the cluster is proven to converge and to have
//! lost no acknowledged write.
//!
//! # Why a CAS chain, not independent writes
//!
//! `uc-lincheck::register::RegisterSm` is a single-value register: an
//! unconditional `Write` simply overwrites, so "did an earlier acked write
//! survive" is unanswerable from the final value alone (a later write
//! legitimately overwrites an earlier one — that is not loss). A serial
//! `Cas{old, new}` chain, issued by a single client thread with no other
//! writer, makes an unexpected `CasResult(false)` an unambiguous DETECTOR:
//! under a single serial writer, `old` always matches the value that same
//! writer's own previous successful CAS just installed, so a mismatch can
//! only mean the register's committed value reverted or was otherwise lost
//! underneath the client — exactly "acked-write loss." The load thread
//! panics immediately on that signal rather than deferring to a
//! post-hoc check.
#![cfg(feature = "survival-tests")]

use std::net::{SocketAddr, UdpSocket};
use std::panic;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use uc2_client::Client;
use uc2_log::cnc::{AdminReq, AdminResp, CncPage};
use uc2_node::backup::{backup_instance, restore_artifact};
use uc2_node::recovery::force_single_member;
use uc_lincheck::register::{Cmd, CmdResp};
use uc_protocol::v2::cnc::{NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER};

mod common;
use common::*;

/// A tempdir on the ext4 target volume, never `/tmp` (RAM-backed tmpfs, no
/// swap — see CLAUDE.md and `hard_crash.rs`'s identical helper).
fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("uc2-survival-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

// -------------------------------------------------------- 3-node cluster
//
// Mirrors `hard_crash.rs`'s `spawn_node_multi`/`free_addr`/`members_arg`/
// `open_cnc`/`await_single_leader_multi`, without crypto (not needed for
// this bar) — those helpers are private to `hard_crash.rs`'s own test
// binary, so a trimmed copy lives here per the task brief ("extend
// minimally in the test file, don't restructure common/").

fn free_addr() -> SocketAddr {
    let s = UdpSocket::bind("127.0.0.1:0").expect("bind probe");
    let addr = s.local_addr().unwrap();
    drop(s);
    addr
}

fn members_arg(members: &[(u32, SocketAddr)]) -> String {
    members.iter().map(|(id, a)| format!("{id}@{a}")).collect::<Vec<_>>().join(",")
}

fn spawn_node_multi(instance_dir: &Path, id: u32, bind: SocketAddr, members: &str) -> Reap {
    let child = Command::new(NODE_BIN)
        .arg("--instance-dir")
        .arg(instance_dir)
        .arg("--app-id")
        .arg(APP_ID)
        .arg("--id")
        .arg(id.to_string())
        .arg("--bind")
        .arg(bind.to_string())
        .arg("--members")
        .arg(members)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn node {id}: {e}"));
    Reap(child)
}

fn open_cnc(dir: &Path) -> Option<Arc<CncPage>> {
    CncPage::open_file(&dir.join("cnc2.dat"), APP_ID).ok()
}

/// Wait for exactly one of `dirs` to report itself the sole serving leader.
fn await_single_leader(dirs: &[PathBuf], secs: u64) -> usize {
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

/// Any live, non-leader voter — identified via its cnc `flags` word (the
/// leader bit is unset), not by position in `dirs`.
fn find_follower(dirs: &[PathBuf], leader_idx: usize) -> usize {
    for (i, dir) in dirs.iter().enumerate() {
        if i == leader_idx {
            continue;
        }
        if let Some(c) = open_cnc(dir)
            && c.status().flags.load_acquire() & NODE_FLAG_LEADER == 0
        {
            return i;
        }
    }
    panic!("no follower found among {} dirs (leader={leader_idx})", dirs.len());
}

// --------------------------------------------------- serial CAS-chain load

/// A single background thread driving `Cas{old, new}` against one node's
/// (the leader's) `Client`, sequentially: warm the register to `Some(0)`,
/// then repeatedly `Cas{old: n, new: n+1}`. `acked` is the count of
/// successful increments so far — also the exact register value a correctly
/// functioning cluster must hold once every in-flight request has settled.
/// See the module doc for why a serial CAS chain (not independent writes) is
/// the right shape to detect acked-write loss.
struct CasLoad {
    stop: Arc<AtomicBool>,
    acked: Arc<AtomicU64>,
    // `Option` (not a bare `JoinHandle`), same reason as `Reap`'s use
    // elsewhere in this crate's harness: a type with a `Drop` impl (below)
    // can't have a field moved out of it by value, so `stop_and_join` takes
    // it via `.take()` instead.
    handle: Option<std::thread::JoinHandle<()>>,
}

impl CasLoad {
    /// Stop the loop and join it — panics (propagated) if the loop itself
    /// panicked, i.e. if it ever observed an unexpected `CasResult(false)`.
    fn stop_and_join(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take()
            && let Err(e) = h.join()
        {
            panic::resume_unwind(e);
        }
    }
}

/// Fix round 1, Finding 2: best-effort stop on drop, no join. If the test
/// panics (an assertion failure) before reaching `stop_and_join`, the
/// unwind drops `CasLoad` without ever telling the load thread to stop —
/// it keeps hammering a client whose instance dir is about to be torn down
/// by the unwinding `tempdir`, which is pure unwind-path noise (stray
/// errors/log lines racing process teardown), not a correctness issue.
/// Setting the flag quiets that. Deliberately does NOT join: a `Drop`
/// running during a panic must not itself block or panic further, and the
/// flag alone is enough to make the loop exit on its own.
impl Drop for CasLoad {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

fn start_cas_load(client: Client) -> CasLoad {
    let stop = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(AtomicU64::new(0));
    let (stop2, acked2) = (Arc::clone(&stop), Arc::clone(&acked));
    let handle = std::thread::spawn(move || {
        submit_until_ok(&client, &Cmd::Write(0), Instant::now() + Duration::from_secs(15));
        while !stop2.load(Ordering::Relaxed) {
            let cur = acked2.load(Ordering::Relaxed);
            let deadline = Instant::now() + Duration::from_secs(10);
            match submit_until_ok(&client, &Cmd::Cas { old: cur, new: cur + 1 }, deadline) {
                CmdResp::CasResult(true) => acked2.store(cur + 1, Ordering::Relaxed),
                CmdResp::CasResult(false) => panic!(
                    "CAS(old={cur}, new={}) unexpectedly failed under a single serial writer — \
                     acked-write loss or reversion detected",
                    cur + 1
                ),
                other => panic!("unexpected response to Cas: {other:?}"),
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        client.shutdown();
    });
    CasLoad { stop, acked, handle: Some(handle) }
}

/// Fix round 1, Finding 1: `backup_instance` genuinely overlapping live load
/// was true (a reviewer's independent scratch instrumentation measured
/// 54->69 acked CAS increments during a 130ms copy) but was never asserted —
/// a future change that accidentally serialized the backup with the load
/// (e.g. quiescing the cluster first) would pass this test silently. This
/// snapshots `acked` immediately before and after the copy and requires it
/// to have strictly increased somewhere during the call.
///
/// One retry (remove the partial artifact dir, call `backup_instance`
/// again) is allowed if a single attempt happens to land in a rare
/// zero-progress window — plausible on a starved/oversubscribed CI runner
/// where the copy is slower AND the load thread's own submit-and-wait rate
/// is lower, unlike the ~15-ack margin observed locally in 130ms. A SECOND
/// zero-progress attempt is treated as a genuine failure of this test's
/// premise, not tolerated.
fn backup_under_load_with_overlap_proof(
    follower_dir: &Path,
    out: &Path,
    acked: &Arc<AtomicU64>,
) -> uc2_node::backup::BackupReport {
    for attempt in 1..=2 {
        let before = acked.load(Ordering::Relaxed);
        let report = backup_instance(follower_dir, out).expect("backup_instance under load");
        let after = acked.load(Ordering::Relaxed);
        if after > before {
            return report;
        }
        assert!(
            attempt < 2,
            "backup_instance completed with ZERO overlapping CAS progress across 2 attempts \
             (before={before}, after={after}) — the under-load half of this test's bar did not \
             actually happen"
        );
        eprintln!(
            "[survival] backup attempt {attempt} saw zero CAS progress ({before} -> {after}); \
             retrying once (starved-CI grace)"
        );
        std::fs::remove_dir_all(out).expect("clear the artifact dir before the retry attempt");
    }
    unreachable!("loop above always returns or asserts on attempt 2")
}

// ------------------------------------------------------------- the test

/// The spec's acceptance sentence, as a CI test.
#[test]
fn a_follower_backed_up_under_load_restores_onto_a_new_host_and_converges() {
    shorten_client_timeout();
    let tmp = tempdir();

    const N: usize = 3;
    let addrs: Vec<SocketAddr> = (0..N).map(|_| free_addr()).collect();
    let members: Vec<(u32, SocketAddr)> = (0..N as u32).map(|i| (i, addrs[i as usize])).collect();
    let members_str = members_arg(&members);

    let mut dirs: Vec<PathBuf> = Vec::with_capacity(N);
    let mut node_procs: Vec<Option<Reap>> = Vec::with_capacity(N);
    for i in 0..N as u32 {
        let d = tmp.path().join(format!("n{i}"));
        std::fs::create_dir_all(&d).unwrap();
        node_procs.push(Some(spawn_node_multi(&d, i, addrs[i as usize], &members_str)));
        wait_for_ready(&d, Duration::from_secs(10));
        dirs.push(d);
    }
    let mut svc_procs: Vec<Option<Reap>> = dirs.iter().map(|d| Some(spawn_service(d))).collect();

    let leader_idx = await_single_leader(&dirs, 30);

    // A client submitting throughout: a serial CAS chain against the
    // leader, started now and stopped only after the restored follower has
    // converged — spans backup, host-destroy, restore, and rejoin.
    let load_client = connect_with_retry(&dirs[leader_idx], Duration::from_secs(10));
    let cas_load = start_cas_load(load_client);
    // Let the warm-up + a few increments land before touching anything.
    std::thread::sleep(Duration::from_millis(300));

    // 1. Identify a FOLLOWER via the cnc flags; back it up LIVE, under load.
    let follower_idx = find_follower(&dirs, leader_idx);
    let artifact_dir = tmp.path().join("follower-backup-artifact");
    let backup_report = backup_under_load_with_overlap_proof(
        &dirs[follower_idx],
        &artifact_dir,
        &cas_load.acked,
    );
    assert!(
        backup_report.journal_last_pos > 0,
        "backup of a loaded follower recovered no journal position"
    );

    let pre_kill_value = cas_load.acked.load(Ordering::Relaxed);

    // 2. SIGKILL the follower's processes and rm -rf its instance dir ("host
    // destroyed"). Reap's Drop sends SIGKILL and reaps synchronously, so by
    // the time these assignments return the old processes hold no fds/locks
    // under the directory we're about to remove.
    node_procs[follower_idx] = None;
    svc_procs[follower_idx] = None;
    std::fs::remove_dir_all(&dirs[follower_idx]).expect("rm -rf the destroyed host's instance dir");

    // 3. "New host": a different fresh dir path; restore_artifact; start
    // node+service there with the SAME node id and SAME bind addr (the
    // restored durable config.state — not --members, seed-only once a
    // durable record exists — owns membership, and the surviving two nodes'
    // own peer-address tables, built once at their own boot, already expect
    // this id at this address).
    let restored_dir = tmp.path().join(format!("n{follower_idx}-restored-host"));
    let restore_report =
        restore_artifact(&artifact_dir, &restored_dir).expect("restore_artifact onto a new host");
    assert_eq!(
        restore_report.journal_last_pos, backup_report.journal_last_pos,
        "restore must recover exactly what was backed up"
    );

    node_procs[follower_idx] = Some(spawn_node_multi(
        &restored_dir,
        follower_idx as u32,
        addrs[follower_idx],
        &members_str,
    ));
    wait_for_ready(&restored_dir, Duration::from_secs(10));
    svc_procs[follower_idx] = Some(spawn_service(&restored_dir));
    dirs[follower_idx] = restored_dir.clone();

    // 4. Converge, bounded 30s: poll until the restored node's durable
    // reaches the leader's commit (a fixed target snapshotted now — the
    // leader keeps advancing under the live CAS load, but the restored
    // node's replication catch-up is expected to outrun it comfortably
    // within the bound).
    let leader_commit_target =
        open_cnc(&dirs[leader_idx]).expect("leader cnc").counters().commit.load_acquire();
    let converge_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(c) = open_cnc(&restored_dir)
            && c.counters().durable.load_acquire() >= leader_commit_target
        {
            break;
        }
        assert!(
            Instant::now() < converge_deadline,
            "restored node's durable did not reach the leader's pre-converge commit \
             ({leader_commit_target}) within 30s"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // Every response the client received BEFORE the kill reads back via a
    // linearizable read on the leader.
    let leader_read_client = connect_with_retry(&dirs[leader_idx], Duration::from_secs(10));
    let leader_val: Option<u64> =
        leader_read_client.query_linearizable(&()).expect("leader linearizable read");
    assert!(
        leader_val.unwrap_or(0) >= pre_kill_value,
        "leader's linearizable read ({leader_val:?}) is behind the pre-kill acked value \
         ({pre_kill_value}) — acked-write loss"
    );

    // The restored node must also serve a snapshot (non-linearizable, local)
    // read at, or past, its pre-kill applied state — bounded grace for the
    // service's apply agent to catch up to the durable position just
    // reached above.
    let restored_client = connect_with_retry(&restored_dir, Duration::from_secs(10));
    let snapshot_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let v: Option<u64> =
            restored_client.query_snapshot(&()).expect("restored node snapshot read");
        if v.unwrap_or(0) >= pre_kill_value {
            break;
        }
        assert!(
            Instant::now() < snapshot_deadline,
            "restored node's snapshot read never reached its pre-kill applied state \
             ({pre_kill_value}, last saw {v:?})"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    restored_client.shutdown();

    // 5. The client ran the whole time; zero acked-write loss cluster-wide —
    // the CAS loop panics internally on any unexpected failure (see its
    // doc), so a clean join proves it observed none. Confirm the exact
    // final value too: under one serial writer, the leader's committed
    // value must equal precisely the count of acked increments once
    // everything has settled. `acked` is cloned BEFORE `stop_and_join`
    // consumes `cas_load`, so it's still readable after the join.
    let acked_handle = Arc::clone(&cas_load.acked);
    cas_load.stop_and_join();
    std::thread::sleep(Duration::from_millis(300));
    let acked_final = acked_handle.load(Ordering::Relaxed);

    let final_val: Option<u64> =
        leader_read_client.query_linearizable(&()).expect("final leader linearizable read");
    assert_eq!(
        final_val,
        Some(acked_final),
        "final register value must equal the exact count of acked CAS increments — any \
         mismatch is acked-write loss"
    );
    leader_read_client.shutdown();

    // node_procs / svc_procs dropped here -> SIGKILL + reap every survivor.
}

// ------------------------------------------------- M11 Task 4: quorum loss

/// `ip:u32, port:u16` wire pair for a real `SocketAddr` (IPv4 only) — the
/// admin-request wire shape, same conversion `uc2ctl`/`reconfig.rs` use.
fn addr_to_wire(addr: SocketAddr) -> (u32, u16) {
    match addr {
        SocketAddr::V4(a) => (u32::from(*a.ip()), a.port()),
        SocketAddr::V6(_) => panic!("this harness only binds IPv4 loopback"),
    }
}

/// Fix round 1, Minor 10: `PromoteLearner` can legitimately refuse with
/// reason 10 (`NotCaughtUp`) even after the learner's own `config_version`
/// has converged — config convergence and DATA/durable catch-up are two
/// different things, and a single-shot promote right after the version
/// check is a latent race. Retries on `NotCaughtUp` (reason 10) or a `Retry`
/// status (2) until it succeeds or `secs` elapses; any OTHER refusal reason
/// is a real failure, not retried.
fn promote_until_ok(cnc: &CncPage, id: u32, secs: u64) -> AdminResp {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let resp = admin_request(cnc, 2 /* PromoteLearner */, id, 0, 0, 20);
        if resp.status == 0 {
            return resp;
        }
        let retryable = resp.status == 2 || (resp.status == 1 && resp.reason == 10);
        assert!(
            retryable,
            "promote refused for a non-retryable reason: status={} reason={}",
            resp.status, resp.reason
        );
        assert!(
            Instant::now() < deadline,
            "promote never succeeded within {secs}s (last status={} reason={})",
            resp.status,
            resp.reason
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The `uc2ctl` mutating-command flow, minus the bin (same trimmed-copy
/// convention as this file's `spawn_node_multi`/`free_addr` above, and the
/// same choreography `uc2_node/tests/reconfig.rs`'s `admin_request` uses):
/// read the admin band's current seq, write a fresh request (`seq = old_seq +
/// 1`, a random nonce), poll the response line for the echoed seq.
fn admin_request(cnc: &CncPage, op: u32, id: u32, ip: u32, port: u16, secs: u64) -> AdminResp {
    let old_seq = cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0);
    let seq = old_seq + 1;
    let nonce = rand::random::<u64>();
    cnc.write_admin_req(&AdminReq { seq, nonce, op, id, ip, port });
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if let Some(resp) = cnc.read_admin_resp(seq) {
            return resp;
        }
        assert!(Instant::now() < deadline, "admin response timed out for seq {seq}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The M11 Task 4 acceptance scenario: a 3-voter cluster loses quorum (the
/// leader AND one follower are killed permanently, leaving one survivor —
/// below the 2-of-3 majority ANY config over this membership could ever
/// reach), the survivor is forced to a single-member cluster via
/// `uc2_node::recovery::force_single_member` (in-process — this test crate
/// links `uc2_node` directly, same as the backup/restore calls above), comes
/// back up as a sole leader, and is proven to have lost nothing durable
/// (every write the survivor itself had DURABLY recorded before the kill
/// reads back exactly) while being honest about the narrower loss window
/// (writes the OLD quorum acked but that never replicated to this particular
/// survivor MAY be lost — printed, not hidden). Finally, one of the two dead
/// peers is wiped and rejoined as a fresh learner, then promoted, proving the
/// forced cluster is a normal, live, growable cluster afterward — not a
/// dead end.
#[test]
fn a_survivor_forced_single_after_quorum_loss_recovers_and_repairs() {
    shorten_client_timeout();
    let tmp = tempdir();

    const N: usize = 3;
    let addrs: Vec<SocketAddr> = (0..N).map(|_| free_addr()).collect();
    let members: Vec<(u32, SocketAddr)> = (0..N as u32).map(|i| (i, addrs[i as usize])).collect();
    let members_str = members_arg(&members);

    let mut dirs: Vec<PathBuf> = Vec::with_capacity(N);
    let mut node_procs: Vec<Option<Reap>> = Vec::with_capacity(N);
    for i in 0..N as u32 {
        let d = tmp.path().join(format!("n{i}"));
        std::fs::create_dir_all(&d).unwrap();
        node_procs.push(Some(spawn_node_multi(&d, i, addrs[i as usize], &members_str)));
        wait_for_ready(&d, Duration::from_secs(10));
        dirs.push(d);
    }
    let mut svc_procs: Vec<Option<Reap>> = dirs.iter().map(|d| Some(spawn_service(d))).collect();

    let leader_idx = await_single_leader(&dirs, 30);

    // A short, bounded serial CAS chain against the leader — the "acked by
    // the old quorum" upper bound the honest loss print compares against.
    // (No background thread here, unlike the backup-under-load test above:
    // this scenario doesn't need continued load spanning the kill, and a
    // bounded foreground chain makes `acked` an exact, race-free count.)
    let leader_client = connect_with_retry(&dirs[leader_idx], Duration::from_secs(10));
    let deadline = Instant::now() + Duration::from_secs(15);
    submit_until_ok(&leader_client, &Cmd::Write(0), deadline);
    let mut acked: u64 = 0;
    for _ in 0..50u64 {
        match submit_until_ok(&leader_client, &Cmd::Cas { old: acked, new: acked + 1 }, deadline) {
            CmdResp::CasResult(true) => acked += 1,
            other => panic!("unexpected response to warm-up Cas: {other:?}"),
        }
    }
    leader_client.shutdown();

    // Pick the survivor (a follower) and the OTHER, permanently-dead peer
    // BEFORE killing anything.
    let survivor_idx = find_follower(&dirs, leader_idx);
    let dead2_idx = (0..N).find(|&i| i != leader_idx && i != survivor_idx).unwrap();
    let survivor_id = survivor_idx as u32;

    let old_instance_id =
        connect_with_retry(&dirs[survivor_idx], Duration::from_secs(10)).instance_id();

    // Fix round 2 (adjudicated): pin the survivor's own commit/durable/
    // applied frontier to be FLUSH before killing anything — load-bearing,
    // not decoration. A leaderless follower's `commit_seen` only advances
    // via inbound leader gossip (no local-only path closes a commit<durable
    // gap), and the gossip floor is ~100ms; a kill landing inside that
    // window can freeze the survivor with `durable > commit`, and
    // `CommitTracker::new(0,1)` + the post-force `new_term_pos` clamp would
    // then legitimately commit-and-apply that gap on its own — making the
    // post-force read HIGHER than whatever was captured pre-force. Polling
    // to `commit == durable == service_applied` here makes the later
    // `assert_eq!` provably sound instead of merely the race's usual winner.
    let survivor_cnc_pre_kill = open_cnc(&dirs[survivor_idx]).expect("survivor cnc before the kill");
    let pin_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let commit = survivor_cnc_pre_kill.counters().commit.load_acquire();
        let durable = survivor_cnc_pre_kill.counters().durable.load_acquire();
        let applied = survivor_cnc_pre_kill.service().service_applied.load_acquire();
        if commit >= durable && applied == durable {
            break;
        }
        assert!(
            Instant::now() < pin_deadline,
            "survivor never caught commit up to durable before the kill \
             (commit={commit} durable={durable} applied={applied})"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    // 1. SIGKILL the leader AND the other follower, PERMANENTLY — below the
    // 2-of-3 majority this membership needs, so no config over it (short of
    // force) can ever reach quorum again.
    node_procs[leader_idx] = None;
    svc_procs[leader_idx] = None;
    node_procs[dead2_idx] = None;
    svc_procs[dead2_idx] = None;

    // Assert the cluster is genuinely stalled: real submit attempts against
    // the survivor for 2s, none of which may succeed (no quorum exists).
    let stall_client = connect_with_retry(&dirs[survivor_idx], Duration::from_secs(10));
    let stall_deadline = Instant::now() + Duration::from_secs(2);
    let mut committed_during_stall = false;
    while Instant::now() < stall_deadline {
        if let Ok(resp) = stall_client.submit::<Cmd, CmdResp>(&Cmd::Cas { old: acked, new: acked + 1 })
        {
            committed_during_stall = true;
            eprintln!("[survival] unexpected commit during the stall window: {resp:?}");
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        !committed_during_stall,
        "a write committed during quorum loss (only {survivor_idx} of {N} alive) — expected a stall"
    );

    // Snapshot what the survivor itself had durably applied — taken NOW, with
    // the leader and the other follower already dead (so nothing further can
    // ever replicate to the survivor; its state is frozen), NOT earlier —
    // this (not `acked`) is the honest "every write with position <= the
    // survivor's pre-force durable" bound the forced cluster must reproduce
    // exactly. Reusing `stall_client` (rather than opening a fresh one) keeps
    // this read immediately adjacent to the stall assertion above, with no
    // window in which anything could still change.
    let survivor_value_before_force: Option<u64> =
        stall_client.query_snapshot(&()).expect("survivor's own snapshot read, state now frozen");
    stall_client.shutdown();

    // 2. Stop the survivor's own processes — `force_single_member` takes the
    // instance's exclusive flock, so nothing may hold it.
    node_procs[survivor_idx] = None;
    svc_procs[survivor_idx] = None;

    // 3. Force: quorum-loss recovery, in-process.
    let force_report = force_single_member(&dirs[survivor_idx], survivor_id)
        .expect("force_single_member on the survivor");
    assert_eq!(force_report.new_version, force_report.old_version + 1);
    let mut dropped = force_report.dropped_peers.clone();
    dropped.sort_unstable();
    let mut expect_dropped: Vec<u32> = (0..N as u32).filter(|&i| i != survivor_id).collect();
    expect_dropped.sort_unstable();
    assert_eq!(dropped, expect_dropped, "force must drop exactly the two dead peers");

    // 4. Restart the survivor on the SAME instance dir/id/addr — the forced
    // durable config (not `--members`, seed-only once a durable record
    // exists) now owns membership, so it boots as a sole voter and should
    // self-elect immediately.
    node_procs[survivor_idx] =
        Some(spawn_node_multi(&dirs[survivor_idx], survivor_id, addrs[survivor_idx], &members_str));
    wait_for_fresh_instance(&dirs[survivor_idx], old_instance_id, Duration::from_secs(10));
    svc_procs[survivor_idx] = Some(spawn_service(&dirs[survivor_idx]));

    let elect_deadline = Instant::now() + Duration::from_secs(10);
    let want = NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE;
    loop {
        if let Some(c) = open_cnc(&dirs[survivor_idx])
            && c.status().flags.load_acquire() & want == want
        {
            break;
        }
        assert!(Instant::now() < elect_deadline, "forced sole voter never elected/served itself");
        std::thread::sleep(Duration::from_millis(20));
    }

    // 5. Every write with position <= the survivor's pre-force durable reads
    // back EXACTLY (force touches only the config record, never the
    // journal) — and print, honestly, how many of the writes the OLD quorum
    // acked never made it to this particular survivor before the kill.
    let survivor_post_client = connect_with_retry(&dirs[survivor_idx], Duration::from_secs(10));
    let survivor_value_after_force: Option<u64> = survivor_post_client
        .query_linearizable(&())
        .expect("survivor's own linearizable read after force+restart");
    assert_eq!(
        survivor_value_after_force, survivor_value_before_force,
        "force_single_member must not lose or invent any write the survivor already held durably"
    );
    let lost = acked.saturating_sub(survivor_value_before_force.unwrap_or(0));
    eprintln!(
        "[survival] quorum-loss recovery: {lost} acked-above-durable write(s) lost \
         (old-quorum acked={acked}, survivor's own pre-force durable value={:?})",
        survivor_value_before_force
    );

    // Prove the forced sole-voter cluster is genuinely live, not just
    // read-only: one more CAS must commit immediately (quorum of 1).
    let base = survivor_value_after_force.unwrap_or(0);
    let live_deadline = Instant::now() + Duration::from_secs(10);
    match submit_until_ok(&survivor_post_client, &Cmd::Cas { old: base, new: base + 1 }, live_deadline) {
        CmdResp::CasResult(true) => {}
        other => panic!("forced sole-voter cluster refused a live write: {other:?}"),
    }
    survivor_post_client.shutdown();

    // 6. Wipe one dead peer's dir and rejoin it as a FRESH learner (a new id
    // — Global Constraints: dropped peers rejoin as fresh ids, never reusing
    // the old, never-tombstoned-but-dropped one) via the live cnc admin band
    // on the survivor, then promote it — back to a real 2-voter cluster.
    std::fs::remove_dir_all(&dirs[dead2_idx]).expect("wipe the dead peer's instance dir");
    let fresh_id: u32 = N as u32; // 3 — not one of the original 0/1/2 ids.
    let fresh_addr = free_addr();
    let fresh_dir = dirs[dead2_idx].clone();

    let survivor_cnc = open_cnc(&dirs[survivor_idx]).expect("survivor cnc for admin ops");
    let (ip, port) = addr_to_wire(fresh_addr);
    let resp = admin_request(&survivor_cnc, 1 /* AddLearner */, fresh_id, ip, port, 20);
    assert_eq!(resp.status, 0, "add-learner refused: reason={}", resp.reason);

    // Fix round 1, Minor 9 (fix round 2: named precisely, not just
    // "fixture-honesty"): the crashtest node bin has no `--learners` flag —
    // everything in `--members` becomes a VOTER in `NodeConfig.members`, so
    // appending `{fresh_id}@{fresh_addr}` here makes this fresh node's own
    // LOCAL boot-time genesis seed (its instance dir has no config.state
    // yet) a 4-VOTER config {0,1,2,3} at version 0, with itself believing it
    // is a voter, not a learner. That is wrong (and momentary): the real
    // AddLearner change (`resp` above, already committed on the survivor as
    // a MUCH higher version, correctly listing id 3 as a learner) supersedes
    // it the instant this node connects and replicates — `ConfigObserved`'s
    // version-gated adoption overwrites any lower version unconditionally.
    // It is benign even during that brief window: of its own believed 4
    // voters, two (the original leader/dead2 addresses) are unreachable
    // dead processes and the third (the survivor) is already the sole voter
    // of a DIFFERENT, much-higher-version single-voter term — it cannot
    // grant this node a vote toward a phantom 4-voter quorum it isn't even
    // part of — so this node cannot win an election under its own stale
    // local view before the real config arrives and corrects it. Confirmed
    // in practice, not just argued: this test's own log output shows node 3
    // adopting the real config (v2, then v3) promptly, never electing
    // itself. Without this line (the original `--members = members_str`
    // alone, not naming id 3 at all), node 3's local genesis seed would be
    // the 3 voters {0,1,2} — NOT including its own id anywhere in
    // members-or-learners, an even less representative boot-time shape (a
    // real `preflight`-checked node would refuse outright via
    // `SelfNotAMember` rather than boot with a genesis that omits itself).
    let fresh_members_str = format!("{members_str},{fresh_id}@{fresh_addr}");
    node_procs.push(Some(spawn_node_multi(&fresh_dir, fresh_id, fresh_addr, &fresh_members_str)));
    wait_for_ready(&fresh_dir, Duration::from_secs(10));
    svc_procs.push(Some(spawn_service(&fresh_dir)));
    dirs.push(fresh_dir.clone());

    let converge_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(c) = open_cnc(&fresh_dir)
            && c.config_version() >= resp.version
        {
            break;
        }
        assert!(Instant::now() < converge_deadline, "fresh learner never converged its config version");
        std::thread::sleep(Duration::from_millis(50));
    }

    let promote_resp = promote_until_ok(&survivor_cnc, fresh_id, 20);
    assert_eq!(promote_resp.status, 0, "promote refused: reason={}", promote_resp.reason);

    // A 2-voter cluster is genuinely operational: one more CAS must commit.
    let final_client = connect_with_retry(&dirs[survivor_idx], Duration::from_secs(10));
    let cur: Option<u64> = final_client.query_linearizable(&()).expect("final linearizable read");
    let cur = cur.unwrap_or(0);
    let final_deadline = Instant::now() + Duration::from_secs(10);
    match submit_until_ok(&final_client, &Cmd::Cas { old: cur, new: cur + 1 }, final_deadline) {
        CmdResp::CasResult(true) => {}
        other => panic!("repaired 2-voter cluster refused a write: {other:?}"),
    }
    final_client.shutdown();

    // node_procs / svc_procs dropped here -> SIGKILL + reap every survivor.
}
