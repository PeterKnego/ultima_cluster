// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Wire-level spec §6.3: the two-binary rolling upgrade, as real processes.
//!
//! Three `uc_crashtest-node` processes boot on the OLD side — this tree with
//! `UC2_WIRE_RANGE=12,12`, or a real previous release's node bin named by
//! `UC2_BASE_NODE_BIN` (no override env then) — and serve a concurrent
//! CAS-register workload. They are then rolled to the NEW side
//! (`UC2_WIRE_RANGE=12,13`) one at a time: nodes 0 and 1 by an ordinary
//! restart, node 2 by a SIGKILL mid-roll and a restart on the new binary.
//! While any `[12,12]` member remains the committed level must stay 12; once
//! the last one is replaced the leader raises it to 13 on its own, and every
//! node's cnc `wire_level` word must read 13 within 30 s. The history
//! recorded across the whole roll must be linearizable. Finally the old
//! binary, started on node 2's dir after the raise, must be refused by name:
//! its cluster agent fail-stops `WireLevelAboveBinary` and the process exits.
//!
//! Gated on both `hard-crash-tests` (real processes, real SIGKILL) and
//! `wire-level-override` (without it the node bin ignores `UC2_WIRE_RANGE`
//! and both "sides" would be the same `[12,12]` binary).
#![cfg(all(feature = "hard-crash-tests", feature = "wire-level-override"))]

use std::io::{Read, Seek, SeekFrom};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use uc_lincheck::history::History;

mod common;
use common::*;

/// The previous release's range: the synthetic old side of this tree.
const OLD: &str = "12,12";
/// This release's range: speaks 12, accepts a raise to 13.
const NEW: &str = "12,13";

/// The old side of the roll: a real previous-release node binary when
/// `UC2_BASE_NODE_BIN` names one (no override — it speaks its own range),
/// else this tree's bin pinned to the old range.
fn old_side() -> (String, Vec<(&'static str, &'static str)>) {
    match std::env::var("UC2_BASE_NODE_BIN") {
        Ok(bin) => (bin, vec![]),
        Err(_) => (NODE_BIN.to_string(), vec![("UC2_WIRE_RANGE", OLD)]),
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
    let (old_bin, old_env) = old_side();
    let new_env = [("UC2_WIRE_RANGE", NEW)];

    // Every node before any readiness wait (an attach waits for the node to
    // have joined its cluster, which needs its peers up).
    let mut nodes: Vec<Option<Reap>> = (0..N)
        .map(|i| {
            Some(spawn_node_multi_bin(
                &old_bin,
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
    for d in &dirs {
        assert_eq!(cnc_level(d), 12, "genesis level on {}", d.display());
    }

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
    // still `[12,12]` after each, so the level must not move.
    for i in [0usize, 1] {
        let old_id = open_cnc(&dirs[i]).and_then(|c| c.try_instance_id());
        svcs[i] = None;
        nodes[i] = None;
        nodes[i] = Some(spawn_node_multi_bin(
            NODE_BIN,
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
                12,
                "a [12,12] member (node 2) still holds the level, after node {i} rolled; {} read \
                 otherwise",
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
            NODE_BIN,
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

    // Every member now reports 13: the leader raises on its own, and every
    // node's cluster agent applies it.
    let deadline = Instant::now() + Duration::from_secs(30);
    while dirs.iter().any(|d| cnc_level(d) != 13) {
        assert!(
            Instant::now() < deadline,
            "the raise did not land within 30 s of the last node rolling: wire_level per node {:?}",
            dirs.iter().map(|d| cnc_level(d)).collect::<Vec<_>>()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let raised_after = started.elapsed();
    std::thread::sleep(Duration::from_secs(1));
    let ok_after_raise = History::ok_count(&history.snapshot());

    stop.store(true, Ordering::Relaxed);
    join_workers(handles);
    let entries = Arc::try_unwrap(history)
        .ok()
        .expect("sole history owner")
        .into_entries();
    let ok = History::ok_count(&entries);
    eprintln!(
        "[rolling_upgrade] ops={} ok_before_roll={ok_before_roll} ok_after_raise={ok_after_raise} \
         ok_total={ok} raised_at={raised_after:?}",
        entries.len()
    );
    assert!(
        ok_after_raise > ok_before_roll,
        "the cluster stopped serving across the roll (ok stuck at {ok_before_roll})"
    );
    assert_linearizable(&entries, "rolling_upgrade", "run0");

    // The old binary is now refused by name: start it on node 2's dir.
    svcs[2] = None;
    nodes[2] = None;
    let log = node_stderr_log(&dirs[2]);
    let from = std::fs::metadata(&log).map(|m| m.len()).unwrap_or(0);
    let mut old = spawn_node_multi_bin(&old_bin, &dirs[2], 2, addrs[2], &members_str, &old_env);
    let status = poll_exit(&mut old.0, Duration::from_secs(20));
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
