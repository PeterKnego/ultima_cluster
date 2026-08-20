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
use uc2_log::cnc::CncPage;
use uc2_node::backup::{backup_instance, restore_artifact};
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
    handle: std::thread::JoinHandle<()>,
}

impl CasLoad {
    /// Stop the loop and join it — panics (propagated) if the loop itself
    /// panicked, i.e. if it ever observed an unexpected `CasResult(false)`.
    fn stop_and_join(self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Err(e) = self.handle.join() {
            panic::resume_unwind(e);
        }
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
    CasLoad { stop, acked, handle }
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
    let backup_report =
        backup_instance(&dirs[follower_idx], &artifact_dir).expect("backup_instance under load");
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
