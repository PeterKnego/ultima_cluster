// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Wire-level spec §6.3: the two-binary rolling upgrade, as real processes.
//!
//! Three `uc_crashtest-node` processes boot on the OLD side and serve a
//! concurrent CAS-register workload. They are then rolled to the NEW side
//! (this tree's bin) one at a time: nodes 0 and 1 by an ordinary restart,
//! node 2 by a SIGKILL mid-roll and a restart on the new binary. The history
//! recorded across the whole roll must be linearizable, and the cluster must
//! keep serving.
//!
//! No level is written down here. The test encodes the LEVEL RULE, not a
//! release pair: a cluster at level `L` holds `L` while any member still
//! reports `L`, and once every member reports above it the leader raises to
//! `L + 1` on its own — one level per raise, never more. `L` is read from the
//! old cluster's cnc `wire_level` word after it boots (the old binary wrote
//! its own genesis), and the new side's top is `uc_protocol::WIRE_MAX` of this
//! tree. That makes the test pass for ANY adjacent pair, and it is why the
//! release checklist runs it against the previous release's binary for every
//! release (`docs/how-to/cut-a-release.md` § 1, #178):
//!
//! - **The old side.** `UC2_BASE_NODE_BIN` names a real previous release's
//!   `uc_crashtest-node` (no override env: it speaks its own range). Without
//!   it the old side is synthetic — this tree's bin pinned to
//!   `UC2_WIRE_RANGE=WIRE_MAX,WIRE_MAX` — and the new side is this tree's bin
//!   pinned to `WIRE_MAX,WIRE_MAX+1`, so the nightly exercises a one-level
//!   step on every tree without edits.
//! - **A step** (`new top == L + 1`, the real `2.N+1` against a `2.N` that
//!   bumped the wire minor, and the synthetic pair): while any old member
//!   remains the level must stay `L`; within 30 s of the last roll every
//!   node's word must read `L + 1`; and the old binary, started on node 2's
//!   dir afterwards, must be refused by name — its cluster agent fail-stops
//!   `WireLevelAboveBinary` and the process exits 1.
//! - **Same level** (`new top == L`, a `2.N+1` that kept the wire minor —
//!   the case the every-release rule exists for: a cnc word, a settings
//!   field or a decoder can still break a mixed cluster): the roll completes,
//!   the level never moves, the history is linearizable, and the refusal
//!   clause is SKIPPED with a stated reason, because a same-level binary is
//!   not refused.
//! - **Two or more apart, or the old side above the new**: not shapes the
//!   rule allows (two apart: the new binary's `WIRE_MIN` excludes `L`, so it
//!   would fail-stop `WireLevelBelowBinary` at join — `uc_node/tests/
//!   wire_level.rs` pins that in-process; above: a base binary newer than
//!   this tree is not an upgrade). The test fails at once, naming both tops,
//!   instead of hanging on a raise that cannot come.
//!
//! Gated on both `hard-crash-tests` (real processes, real SIGKILL) and
//! `wire-level-override` (without it the node bin ignores `UC2_WIRE_RANGE`
//! and the synthetic "sides" would be the same binary).
#![cfg(all(feature = "hard-crash-tests", feature = "wire-level-override"))]

use std::io::{Read, Seek, SeekFrom};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use uc_lincheck::history::History;
use uc_protocol::version::WIRE_MAX;

mod common;
use common::*;

/// One side of the roll: the node binary and the env it runs under.
struct Side {
    bin: String,
    env: Vec<(&'static str, String)>,
}

impl Side {
    fn env(&self) -> Vec<(&str, &str)> {
        self.env.iter().map(|(k, v)| (*k, v.as_str())).collect()
    }
}

/// The two sides, and the top of the new side's range (the level a raise
/// would land on, if one is due). With `UC2_BASE_NODE_BIN` the old side is
/// that binary with no override and the new side is this tree's bin at its
/// compiled range; without it both sides are this tree's bin, pinned one
/// level apart.
fn sides() -> (Side, Side, u8) {
    match std::env::var("UC2_BASE_NODE_BIN") {
        Ok(bin) => (
            Side { bin, env: vec![] },
            Side {
                bin: NODE_BIN.to_string(),
                env: vec![],
            },
            WIRE_MAX,
        ),
        Err(_) => (
            Side {
                bin: NODE_BIN.to_string(),
                env: vec![("UC2_WIRE_RANGE", format!("{WIRE_MAX},{WIRE_MAX}"))],
            },
            Side {
                bin: NODE_BIN.to_string(),
                env: vec![("UC2_WIRE_RANGE", format!("{WIRE_MAX},{}", WIRE_MAX + 1))],
            },
            WIRE_MAX + 1,
        ),
    }
}

/// `dir`'s committed wire level as its cnc page reports it; 0 while the page
/// cannot be opened (mid-restart).
fn cnc_level(dir: &Path) -> u64 {
    open_cnc(dir).map(|c| c.wire_level()).unwrap_or(0)
}

#[test]
fn rolling_upgrade_one_node_at_a_time_under_load_is_linearizable_and_raises() {
    shorten_client_timeout();
    let started = Instant::now();
    let tmp = tempdir();

    const N: usize = 3;
    let addrs: Vec<SocketAddr> = (0..N).map(|_| free_addr()).collect();
    let members: Vec<(u32, SocketAddr)> = (0..N as u32).map(|i| (i, addrs[i as usize])).collect();
    let members_str = members_arg(&members);
    let dirs: Vec<PathBuf> = (0..N).map(|i| tmp.path().join(format!("n{i}"))).collect();
    for d in &dirs {
        std::fs::create_dir_all(d).unwrap();
    }
    let (old, new, new_top) = sides();
    let (old_env, new_env) = (old.env(), new.env());

    // Every node before any readiness wait (an attach waits for the node to
    // have joined its cluster, which needs its peers up).
    let mut nodes: Vec<Option<Reap>> = (0..N)
        .map(|i| {
            Some(spawn_node_multi_bin(
                &old.bin,
                &dirs[i],
                i as u32,
                addrs[i],
                &members_str,
                &old_env,
            ))
        })
        .collect();
    for d in &dirs {
        wait_for_ready(d, Duration::from_secs(10));
    }
    let mut svcs: Vec<Option<Reap>> = dirs.iter().map(|d| Some(spawn_service(d))).collect();
    await_single_leader_multi(&dirs, 30);

    // The old cluster's level, read from what the old binary committed at
    // genesis — the one fact every later assertion is relative to.
    let levels: Vec<u64> = dirs.iter().map(|d| cnc_level(d)).collect();
    let level = levels[0];
    assert!(
        levels.iter().all(|&l| l != 0),
        "an old node reports no level (a pre-level binary, or its page was not readable): {levels:?}"
    );
    assert!(
        levels.iter().all(|&l| l == level),
        "the old nodes disagree on the genesis level: {levels:?}"
    );
    let step = match (new_top as u64).checked_sub(level) {
        Some(0) => false,
        Some(1) => true,
        Some(gap) => panic!(
            "the old side is at level {level} and the new side tops out at {new_top}: {gap} \
             apart. The level rule admits adjacent levels only; a stop-all upgrade is the \
             procedure for a cluster this far behind (upgrade-a-cluster.md)"
        ),
        None => panic!("the old side ({level}) is above the new side's top ({new_top})"),
    };
    let target = if step { level + 1 } else { level };
    eprintln!(
        "[rolling_upgrade] old side {} at level {level}; new side {} tops at {new_top}: {}",
        old.bin,
        new.bin,
        if step {
            "a one-level step"
        } else {
            "same level, no raise expected"
        }
    );

    let dirs = Arc::new(dirs);
    let history = Arc::new(History::default());
    let stop = Arc::new(AtomicBool::new(false));
    let last_seen = Arc::new(AtomicU64::new(0));
    warmup_write_multi(&dirs, &history, &last_seen);
    let handles = spawn_workers_multi(
        &dirs,
        &history,
        &stop,
        &last_seen,
        7,
        Duration::from_millis(15),
        3,
    );
    std::thread::sleep(Duration::from_millis(500));
    let ok_before_roll = History::ok_count(&history.snapshot());

    // Roll nodes 0 and 1 by an ordinary restart onto the new side. Node 2 is
    // still on the old side after each, so the level must not move.
    for i in [0usize, 1] {
        let old_id = open_cnc(&dirs[i]).and_then(|c| c.try_instance_id());
        svcs[i] = None;
        nodes[i] = None;
        nodes[i] = Some(spawn_node_multi_bin(
            &new.bin,
            &dirs[i],
            i as u32,
            addrs[i],
            &members_str,
            &new_env,
        ));
        wait_for_fresh_cnc_instance(&dirs[i], old_id, Duration::from_secs(10));
        svcs[i] = Some(spawn_service(&dirs[i]));
        await_single_leader_multi(&dirs, 30);
        std::thread::sleep(Duration::from_secs(2));
        for d in dirs.iter() {
            assert_eq!(
                cnc_level(d),
                level,
                "an old-side member (node 2) still holds the level at {level}, after node {i} \
                 rolled; {} read otherwise",
                d.display()
            );
        }
    }

    // Node 2: SIGKILL mid-roll (not a drop), then back on the new side.
    {
        let old_id = open_cnc(&dirs[2]).and_then(|c| c.try_instance_id());
        nodes[2].as_mut().unwrap().0.kill().expect("SIGKILL node 2");
        svcs[2] = None;
        nodes[2] = None;
        nodes[2] = Some(spawn_node_multi_bin(
            &new.bin,
            &dirs[2],
            2,
            addrs[2],
            &members_str,
            &new_env,
        ));
        wait_for_fresh_cnc_instance(&dirs[2], old_id, Duration::from_secs(10));
        svcs[2] = Some(spawn_service(&dirs[2]));
        await_single_leader_multi(&dirs, 30);
    }

    if step {
        // Every member now reports `level + 1`: the leader raises on its own,
        // and every node's cluster agent applies it.
        let deadline = Instant::now() + Duration::from_secs(30);
        while dirs.iter().any(|d| cnc_level(d) != target) {
            assert!(
                Instant::now() < deadline,
                "the raise to {target} did not land within 30 s of the last node rolling: \
                 wire_level per node {:?}",
                dirs.iter().map(|d| cnc_level(d)).collect::<Vec<_>>()
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    } else {
        // Same level on both sides: nothing to raise to. Give a wrong raise
        // the same window it would need to land, then check nothing moved.
        std::thread::sleep(Duration::from_secs(3));
        for d in dirs.iter() {
            assert_eq!(
                cnc_level(d),
                level,
                "same-level roll must not move the level; {} read otherwise",
                d.display()
            );
        }
    }
    let settled_after = started.elapsed();
    std::thread::sleep(Duration::from_secs(1));
    let ok_after_roll = History::ok_count(&history.snapshot());

    stop.store(true, Ordering::Relaxed);
    join_workers(handles);
    let entries = Arc::try_unwrap(history)
        .ok()
        .expect("sole history owner")
        .into_entries();
    let ok = History::ok_count(&entries);
    eprintln!(
        "[rolling_upgrade] ops={} ok_before_roll={ok_before_roll} ok_after_roll={ok_after_roll} \
         ok_total={ok} level {level} -> {target} settled_at={settled_after:?}",
        entries.len()
    );
    assert!(
        ok_after_roll > ok_before_roll,
        "the cluster stopped serving across the roll (ok stuck at {ok_before_roll})"
    );
    assert_linearizable(&entries, "rolling_upgrade", "run0");

    if !step {
        // The level over the WHOLE run, not only the 3 s window above: a
        // wrong raise that took longer to land would show here.
        for d in dirs.iter() {
            assert_eq!(
                cnc_level(d),
                level,
                "same-level roll moved the level by the end of the run; {} read otherwise",
                d.display()
            );
        }
        eprintln!(
            "[rolling_upgrade] SKIPPED the refusal clause: both sides top out at level {level}, \
             and a same-level binary is not refused"
        );
        eprintln!("[rolling_upgrade] wall {:?}", started.elapsed());
        return;
    }

    // The old binary is now refused by name: start it on node 2's dir.
    svcs[2] = None;
    nodes[2] = None;
    let log = node_stderr_log(&dirs[2]);
    let from = std::fs::metadata(&log).map(|m| m.len()).unwrap_or(0);
    let mut old_again =
        spawn_node_multi_bin(&old.bin, &dirs[2], 2, addrs[2], &members_str, &old_env);
    let status = poll_exit(&mut old_again.0, Duration::from_secs(20));
    let mut err = String::new();
    let mut f = std::fs::File::open(&log).expect("node 2 stderr log");
    f.seek(SeekFrom::Start(from)).unwrap();
    f.read_to_string(&mut err).unwrap();
    let status = status.unwrap_or_else(|| {
        panic!("the old binary must exit within 20 s of starting above its range; stderr:\n{err}")
    });
    assert_eq!(
        status.code(),
        Some(1),
        "the old binary's exit (expected 1, the agent fail-stop exit); stderr:\n{err}"
    );
    assert!(
        err.contains("WireLevelAboveBinary"),
        "the old binary's stderr does not name WireLevelAboveBinary:\n{err}"
    );
    eprintln!("[rolling_upgrade] wall {:?}", started.elapsed());
}
